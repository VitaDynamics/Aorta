#!/usr/bin/env python3
"""Fail-closed attack matrix for the external-root lint.

Every case builds a COMPLETE, valid external root in a temp directory and then
perturbs exactly one thing, so a green assertion means "this specific forgery
is what the lint rejects", not "something was broken".

The matrix mirrors the design's attack list: missing evidence input, fragment ↔
physical mismatch, forged `internal` provenance, path escape / symlink, a
canonical slot already owned by an Aorta package, an unknown manifest version,
tampered hashes, and one-sided include / `dep_fbs` edits.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from external_root_lint import (  # noqa: E402
    _check_action_root_type,
    declares_root_type,
    lint_external_root,
    parse_internal_root_allowlist,
)
from external_root_workspace_lint import discover_sources  # noqa: E402
from registry_lint import RegistryLintError, repo_root  # noqa: E402

PACKAGE = "//schemas"
SOURCE_PREFIX = "src"

FRAGMENT = '''"""EDU fixture fragment."""

_AORTA_BASE_FBS = "@aorta//message/schemas/system:aorta_base.fbs"

EDU_SCHEMAS = [
    struct(
        pkg = "//schemas",
        stem = "my_text",
        root_fbs = "my_text.fbs",
        provenance = "external",
        dep_fbs = [_AORTA_BASE_FBS],
        cpp = True,
        python = True,
        rust = True,
        bfbs = True,
        schema_meta_cpp = True,
        schema_meta_rust = True,
        cpp_wrapper = True,
        rust_wrapper = True,
    ),
    struct(
        pkg = "//schemas",
        stem = "my_units",
        root_fbs = "my_units.fbs",
        provenance = "external",
        cpp = True,
        python = True,
        rust = True,
    ),
]
'''

MY_TEXT_FBS = '''include "schemas/system/aorta_base.fbs";

namespace edu.demo;

table MyText {
  aorta_header: aorta.sys.AortaHeader;
  text: string;
}

root_type MyText;
'''

# A MINIMAL action schema in the shape every migrated Aorta action family has:
# the three domain tables plus `root_type <Prefix>Goal`. gen_action_wrapper
# expands it into the 8-table protocol and re-emits exactly this root type, so
# declaring it is legal — and omitting it is equally legal.
DEMO_ACTION_FBS = '''include "schemas/shared/action_common/action_common.fbs";

namespace edu.demo;

table DemoGoal {
  target: string;
}

table DemoResult {
  ok: bool;
}

table DemoFeedbackData {
  progress: ubyte;
}

root_type DemoGoal;
'''

MY_UNITS_FBS = '''namespace edu.demo;

enum MyUnit : byte {
  UNKNOWN = 0,
}
'''

ALLOWLIST = '''"""Aorta-owned internal-root allowlist."""

INTERNAL_EXTERNAL_SCHEMA_ROOTS = [
    struct(
        pkg = "//message/schemas/external_fixture/probe",
        name = "probe",
        source_package = "//message/tools/tests/external_root_internal",
        test_only = True,
    ),
]
'''

REGISTRY_MANIFEST = {
    "manifest_version": 3,
    "schema_definitions": [
        {
            "pkg": "//message/schemas/system",
            "stem": "aorta_base",
            "root_fbs": "aorta_base.fbs",
            "provenance": "public",
        }
    ],
}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ExternalRoot:
    """A valid on-disk external root plus its manifest."""

    def __init__(self, root: Path):
        self.root = root
        self.package_dir = root / "schemas"
        self.source_dir = self.package_dir / SOURCE_PREFIX
        self.source_dir.mkdir(parents=True)
        self.fragment = self.package_dir / "registry.bzl"
        self.fragment.write_text(FRAGMENT, encoding="utf-8")

        self.sources = {}
        self.outputs = {}
        for name, text in (("my_text.fbs", MY_TEXT_FBS), ("my_units.fbs", MY_UNITS_FBS)):
            source = self.source_dir / name
            source.write_text(text, encoding="utf-8")
            output = self.package_dir / name
            output.write_text(text, encoding="utf-8")
            self.sources[name] = source
            self.outputs[name] = output
        self.manifest = self._manifest()

    def _item(self, stem: str, typed: bool) -> dict:
        fields = {
            "pkg": PACKAGE,
            "stem": stem,
            "root_fbs": f"{stem}.fbs",
            "provenance": "external",
            "cpp": True,
            "python": True,
            "rust": True,
        }
        if typed:
            fields.update(
                {
                    "dep_fbs": ["@aorta//message/schemas/system:aorta_base.fbs"],
                    "bfbs": True,
                    "schema_meta_cpp": True,
                    "schema_meta_rust": True,
                    "cpp_wrapper": True,
                    "rust_wrapper": True,
                }
            )
        return {
            "stem": stem,
            "fields": fields,
            "source_label": f"{PACKAGE}:{SOURCE_PREFIX}/{stem}.fbs",
            "canonical_output_label": f"{PACKAGE}:{stem}.fbs",
            "closure_targets": [],
            "targets": {},
        }

    def _manifest(self) -> dict:
        return {
            "external_root_manifest_version": 1,
            "name": "my_texts",
            "package": PACKAGE,
            "source_package": PACKAGE,
            "source_prefix": SOURCE_PREFIX,
            "expected_provenance": "external",
            "allowed_item_fields": sorted(
                {
                    "action_name",
                    "action_schema",
                    "alias_id",
                    "bfbs",
                    "cc_alias",
                    "cpp",
                    "cpp_lib_name",
                    "cpp_wrapper",
                    "cpp_wrapper_name",
                    "dep_fbs",
                    "extra_cc_deps",
                    "pkg",
                    "provenance",
                    "python",
                    "root_fbs",
                    "rust",
                    "rust_wrapper",
                    "rust_wrapper_crate_name",
                    "rust_wrapper_facade_name",
                    "rust_wrapper_name",
                    "schema_meta_cpp",
                    "schema_meta_py",
                    "schema_meta_rust",
                    "stem",
                }
            ),
            "required_item_fields": ["pkg", "provenance", "root_fbs", "stem"],
            "typed_marker_flag": "bfbs",
            "typed_required_flags": [
                "bfbs",
                "cpp",
                "cpp_wrapper",
                "python",
                "rust",
                "rust_wrapper",
                "schema_meta_cpp",
                "schema_meta_rust",
            ],
            "include_only_forbidden_flags": [
                "action_schema",
                "bfbs",
                "cpp_wrapper",
                "rust_wrapper",
                "schema_meta_cpp",
                "schema_meta_py",
                "schema_meta_rust",
            ],
            "raw_language_flags": ["cpp", "python", "rust"],
            # Every declared language is generated, so nothing is
            # refused any more. The MECHANISM is still exercised: see
            # `RefusedFlagMechanismTest`.
            "refused_flags": [],
            "items": [self._item("my_text", True), self._item("my_units", False)],
            "package_targets": {
                "cc": f"{PACKAGE}:my_texts_cc",
                "py": f"{PACKAGE}:my_texts_py",
                "py_tree": f"{PACKAGE}:my_texts_py_tree",
                "canonical_fbs": f"{PACKAGE}:my_texts_canonical_fbs",
                "manifest": f"{PACKAGE}:my_texts_manifest",
                "rs": f"{PACKAGE}:my_texts_rs",
                "rs_crate_name": "my_texts",
            },
            "fragment": {
                "label": ":registry.bzl",
                "symbol": "EDU_SCHEMAS",
                "path": str(self.fragment),
                "basename": "registry.bzl",
                "sha256": _sha256(self.fragment.read_bytes()),
            },
            "source_inventory": [
                {
                    "path": str(path),
                    "basename": path.name,
                    "sha256": _sha256(path.read_bytes()),
                }
                for _, path in sorted(self.sources.items())
            ],
            "imports": [
                {
                    "stem": name[: -len(".fbs")],
                    "source_label": f"{PACKAGE}:{SOURCE_PREFIX}/{name}",
                    "source_path": str(self.sources[name]),
                    "source_sha256": _sha256(self.sources[name].read_bytes()),
                    "canonical_output_label": f"{PACKAGE}:{name}",
                    "canonical_output_path": str(self.outputs[name]),
                    "canonical_output_sha256": _sha256(self.outputs[name].read_bytes()),
                }
                for name in sorted(self.sources)
            ],
            "direct_dep_labels": ["@aorta//message/schemas/system:aorta_base.fbs"],
        }

    def package_census(self) -> list[Path]:
        """What a `glob(["**/*.fbs"])` of the calling package would return."""
        return sorted(self.package_dir.rglob("*.fbs"), key=str)

    def lint(
        self,
        manifest: dict | None = None,
        source_paths: list[Path] | None = None,
        canonical_output_paths: list[Path] | None = None,
        package_census_paths: list[Path] | None = None,
    ) -> list[str]:
        return lint_external_root(
            manifest=manifest if manifest is not None else self.manifest,
            fragment_path=self.fragment,
            source_paths=(
                source_paths
                if source_paths is not None
                else sorted(self.sources.values(), key=str)
            ),
            canonical_output_paths=(
                canonical_output_paths
                if canonical_output_paths is not None
                else sorted(self.outputs.values(), key=str)
            ),
            registry_manifest=REGISTRY_MANIFEST,
            internal_root_allowlist_text=ALLOWLIST,
            package_census_paths=(
                package_census_paths
                if package_census_paths is not None
                else [
                    path
                    for path in self.package_census()
                    # The canonical outputs are GENERATED; a real `glob()` only
                    # ever returns source files, and the fixture writes both
                    # into one directory.
                    if path not in set(self.outputs.values())
                ]
            ),
        )


class ExternalRootLintTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.fixture = ExternalRoot(Path(self._tmp.name))

    def assertRejects(self, errors: list[str], needle: str) -> None:
        self.assertTrue(
            any(needle in error for error in errors),
            f"expected an error containing {needle!r}, got {errors}",
        )


class GreenBaselineTest(ExternalRootLintTestCase):
    def test_valid_root_passes(self):
        self.assertEqual([], self.fixture.lint())


class MissingEvidenceTest(ExternalRootLintTestCase):
    """Negative: the mandatory evidence inputs are missing or empty."""

    def test_manifest_without_fragment_evidence_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        del manifest["fragment"]
        self.assertRejects(self.fixture.lint(manifest), "missing top-level field(s)")

    def test_manifest_without_source_inventory_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        del manifest["source_inventory"]
        self.assertRejects(self.fixture.lint(manifest), "missing top-level field(s)")

    def test_empty_inventory_cannot_cover_the_items(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["source_inventory"] = []
        errors = self.fixture.lint(manifest, source_paths=[], package_census_paths=[])
        self.assertRejects(errors, "without exactly one physical source file")

    def test_unknown_manifest_version_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["external_root_manifest_version"] = 2
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "unknown external_root_manifest_version")
        # And nothing else runs: an uninterpretable document is not linted.
        self.assertEqual(1, len(errors))


class PolicyFloorTest(ExternalRootLintTestCase):
    """Negative: the manifest tries to narrow the policy it is judged by.

    The policy constants travel in the manifest so the Starlark gate and the
    content gate cannot drift; that must not become a way to opt out of them.
    """

    def test_narrowed_typed_flag_set_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["typed_required_flags"] = ["bfbs", "cpp"]
        self.assertRejects(self.fixture.lint(manifest), "cannot narrow the policy")

    def test_dropped_include_only_prohibition_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["include_only_forbidden_flags"] = ["bfbs"]
        self.assertRejects(self.fixture.lint(manifest), "cannot narrow the policy")

    def test_rewritten_typed_marker_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["typed_marker_flag"] = "cpp"
        self.assertRejects(self.fixture.lint(manifest), "typed_marker_flag must be")

    def test_widened_item_field_universe_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["allowed_item_fields"] = manifest["allowed_item_fields"] + ["ship_it"]
        self.assertRejects(self.fixture.lint(manifest), "allows unknown item field(s)")

    def test_edu_public_is_known_but_still_barred_from_an_external_root(self):
        """`edu_public` is a KNOWN field (an internal root's items
        are Aorta registry entries and carry it), so the floor check no longer
        catches it. The ban on consumer (b) claiming Aorta's public surface now
        lives in `_check_flags` — this pins that it did not go missing in the
        move."""
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["allowed_item_fields"] = manifest["allowed_item_fields"] + ["edu_public"]
        manifest["items"][0]["fields"]["edu_public"] = True
        errors = self.fixture.lint(manifest)
        self.assertFalse([e for e in errors if "allows unknown item field(s)" in e])
        self.assertRejects(errors, "must not appear in an EXTERNAL-root fragment")


class FragmentPhysicalMismatchTest(ExternalRootLintTestCase):
    """Negative: the fragment and the physical file set disagree."""

    def test_unregistered_physical_file_is_rejected(self):
        stray = self.fixture.source_dir / "forgotten.fbs"
        stray.write_text("namespace edu.demo;\n", encoding="utf-8")
        errors = self.fixture.lint(
            source_paths=sorted(self.fixture.sources.values(), key=str) + [stray],
        )
        self.assertRejects(errors, "not registered in the fragment")

    def test_item_the_fragment_does_not_declare_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        item = copy.deepcopy(manifest["items"][0])
        item["stem"] = "ghost"
        item["fields"]["stem"] = "ghost"
        item["fields"]["root_fbs"] = "ghost.fbs"
        manifest["items"].append(item)
        self.assertRejects(self.fixture.lint(manifest), "does not declare for")

    def test_fragment_item_dropped_from_the_macro_call_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"] = manifest["items"][:1]
        manifest["imports"] = manifest["imports"][:1]
        self.assertRejects(self.fixture.lint(manifest), "did not materialize")

    def test_field_edited_after_evaluation_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["cpp_wrapper"] = False
        self.assertRejects(self.fixture.lint(manifest), "differs between the fragment")

    def test_fragment_hash_tampering_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["fragment"]["sha256"] = "0" * 64
        self.assertRejects(self.fixture.lint(manifest), "fragment sha256 mismatch")

    def test_non_pure_data_fragment_is_rejected(self):
        self.fixture.fragment.write_text(
            FRAGMENT + "\ndef sneak():\n    return 1\n", encoding="utf-8"
        )
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["fragment"]["sha256"] = _sha256(self.fixture.fragment.read_bytes())
        self.assertRejects(self.fixture.lint(manifest), "fragment AST lint failed")

    def test_unknown_fragment_field_is_rejected(self):
        self.fixture.fragment.write_text(
            FRAGMENT.replace(
                '        stem = "my_units",',
                '        stem = "my_units",\n        edu_public = True,',
            ),
            encoding="utf-8",
        )
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["fragment"]["sha256"] = _sha256(self.fixture.fragment.read_bytes())
        self.assertRejects(self.fixture.lint(manifest), "unknown field(s)")


class CopyIntegrityTest(ExternalRootLintTestCase):
    """Negative: the canonical slot is not a byte-for-byte copy any more."""

    def test_modified_canonical_output_is_rejected(self):
        self.fixture.outputs["my_units.fbs"].write_text(
            MY_UNITS_FBS + "// injected\n", encoding="utf-8"
        )
        self.assertRejects(self.fixture.lint(), "not a byte-for-byte copy")

    def test_output_hash_tampering_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["imports"][0]["canonical_output_sha256"] = "0" * 64
        self.assertRejects(self.fixture.lint(manifest), "sha256 mismatch")

    def test_rewritten_source_label_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["imports"][0]["source_label"] = "//elsewhere:my_text.fbs"
        self.assertRejects(self.fixture.lint(manifest), "does not match source_package")


class IncludeGraphTest(ExternalRootLintTestCase):
    """Negative: include text and `dep_fbs` edited on one side only."""

    def test_include_without_a_dep_edge_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["dep_fbs"] = []
        self.fixture.fragment.write_text(
            FRAGMENT.replace("        dep_fbs = [_AORTA_BASE_FBS],\n", ""),
            encoding="utf-8",
        )
        manifest["fragment"]["sha256"] = _sha256(self.fixture.fragment.read_bytes())
        self.assertRejects(self.fixture.lint(manifest), "declares no matching dep_fbs")

    def test_dep_edge_without_an_include_is_rejected(self):
        text = MY_TEXT_FBS.replace('include "schemas/system/aorta_base.fbs";\n', "")
        text = text.replace("  aorta_header: aorta.sys.AortaHeader;\n", "")
        self.fixture.sources["my_text.fbs"].write_text(text, encoding="utf-8")
        self.fixture.outputs["my_text.fbs"].write_text(text, encoding="utf-8")
        manifest = copy.deepcopy(self.fixture.manifest)
        digest = _sha256(text.encode("utf-8"))
        for entry in manifest["source_inventory"]:
            if entry["basename"] == "my_text.fbs":
                entry["sha256"] = digest
        for record in manifest["imports"]:
            if record["stem"] == "my_text":
                record["source_sha256"] = digest
                record["canonical_output_sha256"] = digest
        self.assertRejects(self.fixture.lint(manifest), "no matching include statement")


class FlagPolicyTest(ExternalRootLintTestCase):
    """Negative: the first-class-language policy is not satisfied."""

    def test_partial_typed_surface_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["python"] = False
        self.assertRejects(self.fixture.lint(manifest), "must be first class")

    def test_typed_item_without_rust_raw_codegen_is_rejected(self):
        # `rust` joined the first-class set later. Before it did, this
        # very manifest was the PASSING shape — which is exactly why the
        # "partial typed set" rule has to name each language explicitly.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["rust"] = False
        self.assertRejects(self.fixture.lint(manifest), "must be first class")

    def test_typed_item_without_rust_wrapper_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["rust_wrapper"] = False
        self.assertRejects(self.fixture.lint(manifest), "must be first class")

    def test_typed_item_without_rust_schema_meta_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["schema_meta_rust"] = False
        self.assertRejects(self.fixture.lint(manifest), "must be first class")

    def test_include_only_item_with_a_typed_flag_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][1]["fields"]["cpp_wrapper"] = True
        self.assertRejects(self.fixture.lint(manifest), "only exists for a root type")

    def test_include_only_item_with_a_rust_typed_flag_is_rejected(self):
        for flag in ("rust_wrapper", "schema_meta_rust"):
            with self.subTest(flag=flag):
                manifest = copy.deepcopy(self.fixture.manifest)
                manifest["items"][1]["fields"][flag] = True
                self.assertRejects(
                    self.fixture.lint(manifest), "only exists for a root type"
                )

    def test_in_root_dependency_without_raw_codegen_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["dep_fbs"] = [":my_units.fbs"]
        manifest["items"][1]["fields"]["cpp"] = False
        manifest["items"][1]["fields"]["python"] = False
        manifest["items"][1]["fields"]["rust"] = False
        self.assertRejects(self.fixture.lint(manifest), "in-root dependency")

    def test_in_root_dependency_without_rust_codegen_is_rejected(self):
        # The Rust-specific shape of the same rule, and the one with the
        # sharpest failure mode: the dependent's wrapper emits `#[path = ...]
        # pub mod my_units_generated;` unconditionally, so a dep without
        # `rust = True` produces a crate referring to a file nobody generates.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["dep_fbs"] = [":my_units.fbs"]
        manifest["items"][1]["fields"]["rust"] = False
        self.assertRejects(self.fixture.lint(manifest), "in-root dependency")

    def test_asymmetric_include_only_raw_codegen_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][1]["fields"]["rust"] = False
        self.assertRejects(self.fixture.lint(manifest), "per-language symmetric")

    def test_target_name_collision_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        # A stem ending in `_msg` makes the schema alias `<stem>_cc` and the
        # typed binding `<message base>_cc` the same label.
        manifest["items"][1]["fields"]["cc_alias"] = "my_text_msg_cc"
        manifest["items"][0]["fields"]["cpp_wrapper_name"] = "my_text_msg_cc"
        self.assertRejects(self.fixture.lint(manifest), "target name collision")

    def test_rust_target_name_collision_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["rust_wrapper_name"] = "my_texts_rs"
        self.assertRejects(self.fixture.lint(manifest), "target name collision")

    def test_crate_name_collision_with_the_root_facade_is_rejected(self):
        # R8: the facade crate and a per-schema crate in one root. rustc would
        # only complain in whichever downstream crate depends on both.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["rust_wrapper_crate_name"] = "my_texts"
        self.assertRejects(self.fixture.lint(manifest), "crate name collision")

    def test_illegal_crate_name_is_rejected(self):
        for crate in ("My-Text", "9lives", "my text"):
            with self.subTest(crate=crate):
                manifest = copy.deepcopy(self.fixture.manifest)
                manifest["items"][0]["fields"]["rust_wrapper_crate_name"] = crate
                self.assertRejects(self.fixture.lint(manifest), "crate name")

    def test_hyphenated_alias_without_facade_override_is_rejected(self):
        # Reviewer's exact tuple: alias_id "foo-bar" with a LEGAL
        # rust_wrapper_crate_name passes the crate gate, but the facade module
        # name defaults to _message_base(alias_id) = "foo-bar_msg" — not a
        # legal Rust identifier — and `pub use ::... as foo-bar_msg;` would be
        # uncompilable.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["alias_id"] = "foo-bar"
        manifest["items"][0]["fields"]["rust_wrapper_crate_name"] = "foo_bar_msg"
        self.assertRejects(self.fixture.lint(manifest), "facade module name")

    def test_keyword_facade_module_name_is_rejected(self):
        # A facade module name that is exactly a Rust keyword needs raw-
        # identifier spelling in `pub use ... as <name>;` — rejected up front.
        for keyword in ("self", "fn", "type", "crate"):
            with self.subTest(keyword=keyword):
                manifest = copy.deepcopy(self.fixture.manifest)
                manifest["items"][0]["fields"]["rust_wrapper_facade_name"] = keyword
                self.assertRejects(self.fixture.lint(manifest), "facade module name")

    def _promote_to_typed(self, manifest, index, extra_fields):
        """Promote the raw-only fixture item to typed (full flag set) and apply
        extra_fields, keeping the fragment text + sha consistent so the only
        errors are the ones under test."""
        fields = manifest["items"][index]["fields"]
        fields.update(
            {
                "bfbs": True,
                "cpp_wrapper": True,
                "rust_wrapper": True,
                "schema_meta_cpp": True,
                "schema_meta_rust": True,
            }
        )
        fields.update(extra_fields)
        stem = manifest["items"][index]["stem"]
        additions = "".join(
            f'\n        {key} = {value!r},' for key, value in extra_fields.items()
        )
        new_fragment = self.fixture.fragment.read_text(encoding="utf-8").replace(
            f'stem = "{stem}",',
            f'stem = "{stem}",'
            + '\n        bfbs = True,\n        cpp_wrapper = True,\n'
            + '        rust_wrapper = True,\n        schema_meta_cpp = True,\n'
            + '        schema_meta_rust = True,'
            + additions,
        )
        self.fixture.fragment.write_text(new_fragment, encoding="utf-8")
        manifest["fragment"]["sha256"] = _sha256(new_fragment.encode("utf-8"))

    def test_duplicate_facade_module_names_via_overrides_are_rejected(self):
        # Two TYPED items whose (overridden) facade module names coincide would
        # silently shadow each other in the root facade — rustc error only in
        # whichever downstream crate depends on both.
        manifest = copy.deepcopy(self.fixture.manifest)
        self._promote_to_typed(manifest, 1, {"rust_wrapper_facade_name": "shared_msg"})
        manifest["items"][0]["fields"]["rust_wrapper_facade_name"] = "shared_msg"
        self.assertRejects(self.fixture.lint(manifest), "facade module name collision")

    def test_facade_override_rescues_hyphenated_alias(self):
        # Positive: the rust_wrapper_facade_name escape hatch (symmetric to
        # rust_wrapper_crate_name) makes a hyphenated alias_id usable. The
        # mutation is applied to BOTH the fragment text and the manifest so the
        # fragment-vs-declared comparison stays consistent.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["alias_id"] = "foo-bar"
        manifest["items"][0]["fields"]["rust_wrapper_crate_name"] = "foo_bar_msg"
        manifest["items"][0]["fields"]["rust_wrapper_facade_name"] = "foo_bar_msg"
        new_fragment = self.fixture.fragment.read_text(encoding="utf-8").replace(
            'stem = "my_text",',
            'stem = "my_text",\n'
            '        alias_id = "foo-bar",\n'
            '        rust_wrapper_crate_name = "foo_bar_msg",\n'
            '        rust_wrapper_facade_name = "foo_bar_msg",',
        )
        self.fixture.fragment.write_text(new_fragment, encoding="utf-8")
        manifest["fragment"]["sha256"] = _sha256(new_fragment.encode("utf-8"))
        self.assertEqual([], self.fixture.lint(manifest))

    def test_duplicate_default_facade_module_names_are_rejected(self):
        # alias_id "foo" and "foo_msg" both default to _message_base "foo_msg"
        # — collision detected even without overrides.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["alias_id"] = "foo"
        self._promote_to_typed(manifest, 1, {"alias_id": "foo_msg"})
        self.assertRejects(self.fixture.lint(manifest), "facade module name collision")


class RefusedFlagMechanismTest(ExternalRootLintTestCase):
    """`refused_flags` is empty in stage 3 — the MECHANISM still has to work.

    Emptying the list is what "Rust is generated now" means, but the list is
    also the only thing standing between a future flag Aorta does not implement
    and a root that silently claims a surface nobody builds. Asserting on an
    empty list proves nothing, so this drives the mechanism with a flag put
    into the list explicitly.
    """

    def test_a_declared_refused_flag_is_still_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["refused_flags"] = ["cpp_wrapper"]
        self.assertRejects(self.fixture.lint(manifest), "this Aorta refuses")

    def test_the_live_policy_refuses_nothing(self):
        # Pins the honest claim: stage 3 removed the Rust refusal rather than
        # leaving a rule that happens never to fire.
        self.assertEqual([], self.fixture.manifest["refused_flags"])
        self.assertEqual([], self.fixture.lint(self.fixture.manifest))


class PackageCensusTest(ExternalRootLintTestCase):
    """Negative: a `.fbs` dropped into the CANONICAL package.

    This is the case every declaration-derived check is blind to: the file is
    in no item, no `source_inventory` and no `<name>_canonical_fbs`, so without
    an independent census it is in no input set either and the whole suite
    stays green while a second physical owner of a canonical slot sits in the
    tree. The census comes from a `glob()` of the calling package.
    """

    def test_stray_fbs_in_the_canonical_package_is_rejected(self):
        stray = self.fixture.package_dir / "undeclared.fbs"
        stray.write_text(MY_UNITS_FBS, encoding="utf-8")
        errors = self.fixture.lint()
        self.assertRejects(errors, "undeclared schema file")
        self.assertRejects(errors, "undeclared.fbs")

    def test_stray_fbs_in_a_shadow_package_is_rejected(self):
        # Cross-module root: the calling package must hold NO tracked .fbs at
        # all, which is the exact shape of the reviewer's mutation on
        # message/schemas/external_fixture/probe.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["source_package"] = "@external_schemas//message/schemas/probe"
        for record in manifest["imports"]:
            record["source_label"] = "{}:{}/{}".format(
                manifest["source_package"], SOURCE_PREFIX, f"{record['stem']}.fbs"
            )
        stray = self.fixture.package_dir / "undeclared.fbs"
        stray.write_text(MY_UNITS_FBS, encoding="utf-8")
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "must contain NO tracked .fbs at all")

    def test_declared_sources_are_not_flagged(self):
        # The EDU layout keeps its sources INSIDE the calling package
        # (`src/`), so the census must not turn them into errors.
        self.assertEqual([], self.fixture.lint())

    def test_census_is_not_derived_from_the_declaration(self):
        # Passing the stray only through the census — never through the
        # inventory, the items or the outputs — still fails. That is the
        # property the package census exists for.
        stray = self.fixture.package_dir / "undeclared.fbs"
        stray.write_text(MY_UNITS_FBS, encoding="utf-8")
        errors = self.fixture.lint(
            package_census_paths=sorted(self.fixture.sources.values(), key=str) + [stray],
        )
        self.assertRejects(errors, "undeclared.fbs")


class RootTypeClosureTest(ExternalRootLintTestCase):
    """Negative: the `bfbs` marker disagrees with the schema's own root_type."""

    def test_hidden_root_type_in_an_include_only_item_is_rejected(self):
        # The reviewer's exact mutation: append a root type to a raw-only
        # schema and leave every typed flag off.
        text = MY_UNITS_FBS + "\ntable HiddenRoot { value:int; }\nroot_type HiddenRoot;\n"
        self.fixture.sources["my_units.fbs"].write_text(text, encoding="utf-8")
        self.fixture.outputs["my_units.fbs"].write_text(text, encoding="utf-8")
        manifest = copy.deepcopy(self.fixture.manifest)
        digest = _sha256(text.encode("utf-8"))
        for entry in manifest["source_inventory"]:
            if entry["basename"] == "my_units.fbs":
                entry["sha256"] = digest
        for record in manifest["imports"]:
            if record["stem"] == "my_units":
                record["source_sha256"] = digest
                record["canonical_output_sha256"] = digest
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "declares a root_type")
        self.assertRejects(errors, "must be first class in every supported language")

    def test_typed_item_without_a_root_type_is_rejected(self):
        text = MY_TEXT_FBS.replace("root_type MyText;\n", "")
        self.fixture.sources["my_text.fbs"].write_text(text, encoding="utf-8")
        self.fixture.outputs["my_text.fbs"].write_text(text, encoding="utf-8")
        manifest = copy.deepcopy(self.fixture.manifest)
        digest = _sha256(text.encode("utf-8"))
        for entry in manifest["source_inventory"]:
            if entry["basename"] == "my_text.fbs":
                entry["sha256"] = digest
        for record in manifest["imports"]:
            if record["stem"] == "my_text":
                record["source_sha256"] = digest
                record["canonical_output_sha256"] = digest
        self.assertRejects(self.fixture.lint(manifest), "declares no root_type")

    def test_commented_out_root_type_does_not_count(self):
        text = MY_UNITS_FBS + "\n// root_type Ghost;\n"
        self.fixture.sources["my_units.fbs"].write_text(text, encoding="utf-8")
        self.fixture.outputs["my_units.fbs"].write_text(text, encoding="utf-8")
        manifest = copy.deepcopy(self.fixture.manifest)
        digest = _sha256(text.encode("utf-8"))
        for entry in manifest["source_inventory"]:
            if entry["basename"] == "my_units.fbs":
                entry["sha256"] = digest
        for record in manifest["imports"]:
            if record["stem"] == "my_units":
                record["source_sha256"] = digest
                record["canonical_output_sha256"] = digest
        self.assertEqual([], self.fixture.lint(manifest))

    def test_action_source_may_declare_its_goal_table_as_root_type(self):
        """The rule is gen_action_wrapper's, not a stricter local one.

        An action source may omit `root_type` OR declare the Goal table (bare
        or namespace-qualified). Every one of Aorta's own migrated action
        schemas uses the second spelling; forbidding it would have made those
        families un-migratable without EDITING the `.fbs`, which is exactly the
        semantic drift a byte-identical move exists to prevent.
        """
        for label, text in (
            ("declared bare", DEMO_ACTION_FBS),
            ("omitted", DEMO_ACTION_FBS.replace("root_type DemoGoal;\n", "")),
            (
                "declared qualified",
                DEMO_ACTION_FBS.replace("root_type DemoGoal;", "root_type edu.demo.DemoGoal;"),
            ),
            (
                "commented out",
                DEMO_ACTION_FBS.replace("root_type DemoGoal;", "// root_type DemoResult;"),
            ),
        ):
            with self.subTest(label):
                errors: list[str] = []
                _check_action_root_type("demo", "demo.fbs", text, errors)
                self.assertEqual([], errors)

    def test_action_root_type_naming_another_table_is_rejected(self):
        errors: list[str] = []
        _check_action_root_type(
            "demo",
            "demo.fbs",
            DEMO_ACTION_FBS.replace("root_type DemoGoal;", "root_type DemoResult;"),
            errors,
        )
        self.assertRejects(errors, "does not name its Goal table")

    def test_action_item_whose_tables_are_no_action_protocol_is_rejected(self):
        """`action_schema = True` on a schema that is not an action protocol is
        an authoring error, and it is reported HERE — against the canonical
        copy, with the schema named — instead of surfacing later as an opaque
        gen_action_wrapper genrule failure."""
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["action_schema"] = True
        manifest["items"][0]["fields"]["action_name"] = "demo/act"
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "does not form an action protocol")

    def test_root_type_detection_is_comment_aware(self):
        self.assertTrue(declares_root_type("table T{}\nroot_type T;\n"))
        self.assertFalse(declares_root_type("// root_type T;\n"))
        self.assertFalse(declares_root_type("/* root_type T; */\n"))


class FirstClassPolicyScopeTest(ExternalRootLintTestCase):
    """The first-class completeness rule binds consumer (b) only.

    An `internal` root's items are Aorta REGISTRY entries that merely live in
    another repository — 86 of today's 220 deliberately are not first class in
    every language — so grading them on the EDU curve would silently change 86
    schemas' generated surface as a side effect of moving a file. The
    protection that replaces it is that `internal` is unforgeable: it is
    granted only by Aorta's own external_roots.bzl allowlist, which
    `ProvenanceForgeryTest` pins.
    """

    def _internal_manifest(self, **field_overrides) -> dict:
        manifest = copy.deepcopy(self.fixture.manifest)
        # The allowlisted in-repo internal root, so the provenance gate passes.
        manifest["package"] = "//message/schemas/external_fixture/probe"
        manifest["name"] = "probe"
        manifest["expected_provenance"] = "internal"
        manifest["source_package"] = "//message/tools/tests/external_root_internal"
        for item in manifest["items"]:
            item["fields"]["pkg"] = manifest["package"]
            item["fields"]["provenance"] = "internal"
        manifest["items"][0]["fields"].update(field_overrides)
        return manifest

    def test_internal_root_may_keep_a_partial_typed_surface(self):
        for flag in ("python", "rust", "rust_wrapper", "schema_meta_rust", "cpp_wrapper"):
            with self.subTest(flag=flag):
                errors = self.fixture.lint(self._internal_manifest(**{flag: False}))
                self.assertFalse(
                    [e for e in errors if "must be first class" in e],
                    f"disabling {flag} on an internal root must not trip owner "
                    f"decision 4; got {errors}",
                )

    def test_external_root_with_the_same_shape_is_still_rejected(self):
        # The control: identical field edit, `external` provenance. If this ever
        # goes green the scoping has leaked into the EDU surface.
        for flag in ("python", "rust", "rust_wrapper", "schema_meta_rust"):
            with self.subTest(flag=flag):
                manifest = copy.deepcopy(self.fixture.manifest)
                manifest["items"][0]["fields"][flag] = False
                self.assertRejects(self.fixture.lint(manifest), "must be first class")

    def test_internal_root_may_omit_or_explicitly_disable_schema_meta_py(self):
        # The Python schema-meta module is part of the every-language surface,
        # so it is scoped the same way. A C++-only internal item has no Python
        # codegen at all: `schema_meta_py` OMITTED already evaluates to False
        # (registry.bzl's `schema_meta_py_enabled` = `python and bfbs`), and
        # spelling that same value out must not become an error.
        cpp_only = {
            "python": False,
            "rust": False,
            "rust_wrapper": False,
            "schema_meta_rust": False,
        }
        for label, overrides in (
            ("omitted", cpp_only),
            ("explicit False", dict(cpp_only, schema_meta_py=False)),
        ):
            with self.subTest(schema_meta_py=label):
                errors = self.fixture.lint(self._internal_manifest(**overrides))
                self.assertFalse(
                    [e for e in errors if "schema_meta_py" in e],
                    f"a C++-only internal item with schema_meta_py {label} must be "
                    f"accepted; got {errors}",
                )

    def test_external_root_disabling_schema_meta_py_is_still_rejected(self):
        # The control for the scoping above: same field edit on an EXTERNAL
        # root, where python+bfbs are both required, so the predicate defaults
        # to True and only an explicit False can turn it off.
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["schema_meta_py"] = False
        self.assertRejects(
            self.fixture.lint(manifest), "is typed but disables schema_meta_py"
        )

    def test_internal_scoping_does_not_disable_the_policy_floor(self):
        manifest = self._internal_manifest()
        manifest["typed_required_flags"] = ["bfbs"]
        self.assertRejects(
            self.fixture.lint(manifest),
            "cannot narrow the policy it is checked against",
        )

    def test_internal_scoping_does_not_disable_the_root_type_closure(self):
        # `bfbs = False` on a schema that really declares a root_type stays an
        # error for BOTH consumers: that check reads the FILE, and it is what
        # stops a schema posing as include-only.
        manifest = self._internal_manifest(bfbs=False)
        self.assertRejects(self.fixture.lint(manifest), "declares a root_type")


class ProvenanceForgeryTest(ExternalRootLintTestCase):
    """Negative: an external root tries to claim Aorta provenance."""

    def test_undeclared_internal_root_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["expected_provenance"] = "internal"
        for item in manifest["items"]:
            item["fields"]["provenance"] = "internal"
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "cannot grant itself")

    def test_internal_root_with_a_rewritten_source_package_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["package"] = "//message/schemas/external_fixture/probe"
        manifest["name"] = "probe"
        manifest["expected_provenance"] = "internal"
        manifest["source_package"] = "//attacker/sources"
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "Aorta's allowlist pins")

    def test_aorta_declared_root_claiming_external_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["package"] = "//message/schemas/external_fixture/probe"
        manifest["name"] = "probe"
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "is declared as an INTERNAL root by Aorta")

    def test_item_provenance_must_match_the_root(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["items"][0]["fields"]["provenance"] = "internal"
        self.assertRejects(self.fixture.lint(manifest), "declares provenance")

    def test_allowlist_must_be_pure_data(self):
        errors = lint_external_root(
            manifest=self.fixture.manifest,
            fragment_path=self.fixture.fragment,
            source_paths=sorted(self.fixture.sources.values(), key=str),
            canonical_output_paths=sorted(self.fixture.outputs.values(), key=str),
            registry_manifest=REGISTRY_MANIFEST,
            internal_root_allowlist_text=ALLOWLIST + "\nload('//x:y.bzl', 'z')\n",
            package_census_paths=sorted(self.fixture.sources.values(), key=str),
        )
        self.assertRejects(errors, "not valid pure data")

    def test_internal_claim_without_an_allowlist_cannot_pass(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["expected_provenance"] = "internal"
        errors = lint_external_root(
            manifest=manifest,
            fragment_path=self.fixture.fragment,
            source_paths=sorted(self.fixture.sources.values(), key=str),
            canonical_output_paths=sorted(self.fixture.outputs.values(), key=str),
            registry_manifest=REGISTRY_MANIFEST,
            internal_root_allowlist_text=None,
            package_census_paths=sorted(self.fixture.sources.values(), key=str),
        )
        self.assertRejects(errors, "cannot be verified without")


class CanonicalSlotTest(ExternalRootLintTestCase):
    """Negative: the root reaches for a slot an Aorta package already owns."""

    def test_slot_owned_by_an_aorta_registry_entry_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["package"] = "//message/schemas/system"
        for item in manifest["items"]:
            item["fields"]["pkg"] = "//message/schemas/system"
        # A DIFFERENT item reaching for the slot `system/aorta_base.fbs`, which
        # the Aorta registry already owns.
        manifest["items"][0]["stem"] = "clone"
        manifest["items"][0]["fields"]["stem"] = "clone"
        manifest["items"][0]["fields"]["root_fbs"] = "aorta_base.fbs"
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "already owned by the Aorta registry entry")

    def test_internal_root_outside_the_canonical_root_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["expected_provenance"] = "internal"
        errors = self.fixture.lint(manifest)
        self.assertRejects(errors, "have no canonical slot")


class PathContractTest(ExternalRootLintTestCase):
    """Negative: path escape, nesting and symlinks in the source root."""

    def test_source_prefix_escape_is_rejected(self):
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["source_prefix"] = "../outside"
        self.assertRejects(self.fixture.lint(manifest), "must not contain")

    def test_source_outside_the_declared_prefix_is_rejected(self):
        nested_dir = self.fixture.source_dir / "nested"
        nested_dir.mkdir()
        nested = nested_dir / "my_units.fbs"
        nested.write_text(MY_UNITS_FBS, encoding="utf-8")
        manifest = copy.deepcopy(self.fixture.manifest)
        manifest["source_inventory"].append(
            {
                "path": str(nested),
                "basename": nested.name,
                "sha256": _sha256(nested.read_bytes()),
            }
        )
        errors = self.fixture.lint(
            manifest,
            source_paths=sorted(self.fixture.sources.values(), key=str) + [nested],
        )
        self.assertRejects(errors, "does not sit directly under the declared")

    def test_workspace_lint_rejects_a_symlinked_source(self):
        # The symlink gate lives in the WORKSPACE lint on purpose: a runfiles
        # tree is itself a forest of symlinks, so only the real checkout can
        # tell an author's symlink from Bazel's own.
        link = self.fixture.source_dir / "linked.fbs"
        os.symlink(self.fixture.sources["my_units.fbs"], link)
        found, errors = discover_sources(self.fixture.source_dir, self.fixture.root)
        self.assertRejects(errors, "symlink rejected in the source root")
        self.assertNotIn(link, found)

    def test_workspace_lint_finds_a_file_the_inventory_forgot(self):
        stray = self.fixture.source_dir / "forgotten.fbs"
        stray.write_text(MY_UNITS_FBS, encoding="utf-8")
        found, errors = discover_sources(self.fixture.source_dir, self.fixture.root)
        self.assertEqual([], errors)
        self.assertIn(stray, found)


class AllowlistGrammarTest(unittest.TestCase):
    """The Aorta-owned allowlist is parsed with the SAME pure-data grammar."""

    def test_valid_allowlist_parses(self):
        entries = parse_internal_root_allowlist(ALLOWLIST, "external_roots.bzl")
        self.assertEqual(1, len(entries))
        self.assertEqual("probe", entries[0]["name"])
        self.assertTrue(entries[0]["test_only"])

    def test_unknown_field_is_rejected(self):
        text = ALLOWLIST.replace(
            "        test_only = True,", "        test_only = True,\n        trusted = True,"
        )
        with self.assertRaises(RegistryLintError):
            parse_internal_root_allowlist(text, "external_roots.bzl")

    def test_entries_must_be_sorted_and_unique(self):
        text = ALLOWLIST.replace(
            "]",
            """    struct(
        pkg = "//a",
        name = "b",
        source_package = "//c",
    ),
]""",
        )
        with self.assertRaises(RegistryLintError):
            parse_internal_root_allowlist(text, "external_roots.bzl")


# ---------------------------------------------------------------------------
# Target-declaration regression
#
# `aorta_external_schema_package` publishes a per-item `targets` map into the
# external-root manifest. Those strings are TEXT, not Bazel dependency edges,
# so the lint's shape check and a green `bazel build` both stay silent when the
# map names a target `aorta_define_schema_targets` never emitted — except for
# the Python schema-meta pair, which the root's `_py_tree` really does depend
# on (that one fails analysis).
#
# This executes the PRODUCTION declaration block out of
# `external_schema_root.bzl` (sliced by source anchors, so an edit that moves
# the logic elsewhere breaks the slice rather than silently passing) together
# with the production helpers it calls, including `schema_meta_py_enabled` from
# `message/schemas/registry.bzl`. The Starlark subset used by that block is
# syntactically Python; only `type()` and `fail()` need shims.
# ---------------------------------------------------------------------------

EXTERNAL_SCHEMA_ROOT_BZL_REL = "message/tools/external_schema_root.bzl"
REGISTRY_BZL_REL = "message/schemas/registry.bzl"
# The shared, load-free derivation module. The
# helpers below used to be duplicated in external_schema_root.bzl and
# registry.bzl; they now have ONE definition here, so this test — which execs
# the PRODUCTION source rather than a copy of it — reads them from here.
SCHEMA_NAMES_BZL_REL = "message/tools/schema_names.bzl"

# The first and last lines of the per-item target-declaration block inside
# `aorta_external_schema_package`'s item loop.
_BLOCK_START = "        typed = _get(item, _TYPED_MARKER_FLAG, False)\n        has_cpp ="
_BLOCK_END = "        declaration = {\n"

_BZL_HELPERS = (
    "_MISSING",
    "_TYPED_MARKER_FLAG",
    "_get",
    "_rust_binding_name",
    "_rust_crate_name",
)

# Pure derivations that moved to schema_names.bzl. `_message_base` and
# `schema_meta_py_enabled` are re-bound below to the names the production block
# uses, so the exec'd block still runs against the REAL implementations.
_SCHEMA_NAMES_HELPERS = (
    "_MISSING",
    "item_get",
    "item_has",
    "schema_message_base",
    "schema_meta_py_enabled",
)

_STARLARK_TYPE_NAMES = {
    dict: "dict",
    list: "list",
    tuple: "tuple",
    str: "string",
    bool: "bool",
    int: "int",
}


def _starlark_type(value):
    return _STARLARK_TYPE_NAMES.get(type(value), type(value).__name__)


def _starlark_fail(*args):
    raise AssertionError(" ".join(str(a) for a in args))


def _extract_top_level(text: str, filename: str, names) -> str:
    """Source of the named top-level `def`s / assignments, in file order."""
    wanted = set(names)
    module = ast.parse(text, filename=filename)
    chunks: list[str] = []
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            wanted.discard(node.name)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in wanted for target in node.targets
        ):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    wanted.discard(target.id)
        else:
            continue
        chunks.append(ast.get_source_segment(text, node))
    if wanted:
        raise AssertionError(f"{filename}: could not find {sorted(wanted)}")
    return "\n\n".join(chunks)


def _declaration_block_source() -> str:
    text = (repo_root() / EXTERNAL_SCHEMA_ROOT_BZL_REL).read_text(encoding="utf-8")
    start = text.find(_BLOCK_START)
    end = text.find(_BLOCK_END, start + 1) if start >= 0 else -1
    if start < 0 or end < 0:
        raise AssertionError(
            f"{EXTERNAL_SCHEMA_ROOT_BZL_REL}: the per-item target-declaration block no "
            "longer starts with _BLOCK_START / ends before _BLOCK_END. Re-point the "
            "anchors at the moved logic — do not delete this test."
        )
    return textwrap.dedent(text[start:end])


def _declaration_namespace() -> dict:
    root = repo_root()
    bzl = (root / EXTERNAL_SCHEMA_ROOT_BZL_REL).read_text(encoding="utf-8")
    names = (root / SCHEMA_NAMES_BZL_REL).read_text(encoding="utf-8")
    namespace: dict = {
        "type": _starlark_type,
        "fail": _starlark_fail,
        "__builtins__": __builtins__,
    }
    # schema_names.bzl FIRST: it is load-free, and external_schema_root.bzl's
    # `_message_base` is an alias onto one of its functions.
    exec(  # noqa: S102 - production Starlark source, executed on purpose
        _extract_top_level(names, SCHEMA_NAMES_BZL_REL, _SCHEMA_NAMES_HELPERS),
        namespace,
    )
    namespace["_schema_message_base"] = namespace["schema_message_base"]
    namespace["_message_base"] = namespace["schema_message_base"]
    exec(  # noqa: S102
        _extract_top_level(bzl, EXTERNAL_SCHEMA_ROOT_BZL_REL, _BZL_HELPERS),
        namespace,
    )
    return namespace


def declare_targets(item: dict, package_label: str) -> dict:
    """Run the production block for one item; returns everything it produced."""
    namespace = _declaration_namespace()
    namespace.update(
        {
            "item": item,
            "stem": item["stem"],
            "alias_id": item.get("alias_id", item["stem"]),
            "package_label": package_label,
            "py_trees": [],
            "py_metas": [],
            "cc_targets": [],
            "rs_facade_entries": [],
        }
    )
    exec(_declaration_block_source(), namespace)  # noqa: S102
    return {
        "targets": namespace["targets"],
        "py_trees": namespace["py_trees"],
        "py_metas": namespace["py_metas"],
        "cc_targets": namespace["cc_targets"],
        "rs_facade_entries": namespace["rs_facade_entries"],
    }


# The real `service/humanoid_recorder` registry entry (internal provenance,
# C++-only typed surface) as it reaches the macro. Kept in sync with
# message/schemas/registry_manifest_v3.golden.json.
RECORDER_PACKAGE = "//message/schemas/service/humanoid_recorder"
RECORDER_ITEM = {
    "pkg": RECORDER_PACKAGE,
    "provenance": "internal",
    "stem": "recorder_control",
    "root_fbs": "recorder_control.fbs",
    "alias_id": "humanoid_recorder_control",
    "dep_fbs": ["//message/schemas/system:aorta_base.fbs"],
    "bfbs": True,
    "cpp": True,
    "cpp_lib": True,
    "cpp_lib_name": "recorder_control_cpp_lib",
    "cc_alias": "recorder_control_cc",
    "cpp_wrapper": True,
    "schema_meta_cpp": True,
    "python": False,
    "rust": False,
    "rust_wrapper": False,
    "schema_meta_rust": False,
}

# Positive control: an every-language external item (the EDU floor shape).
FIRST_CLASS_PACKAGE = "//message/schemas/topic/probe"
FIRST_CLASS_ITEM = {
    "pkg": FIRST_CLASS_PACKAGE,
    "provenance": "external",
    "stem": "probe",
    "root_fbs": "probe.fbs",
    "dep_fbs": ["@aorta//message/schemas/system:aorta_base.fbs"],
    "bfbs": True,
    "cpp": True,
    "cpp_wrapper": True,
    "python": True,
    "rust": True,
    "rust_wrapper": True,
    "schema_meta_cpp": True,
    "schema_meta_rust": True,
}


class TargetDeclarationTest(unittest.TestCase):
    def test_cpp_only_typed_item_declares_no_python_or_rust_metadata(self):
        result = declare_targets(RECORDER_ITEM, RECORDER_PACKAGE)
        targets = result["targets"]

        for key in ("schema_meta_py", "schema_meta_py_lib", "schema_meta_rust",
                    "rust", "rust_closure", "typed_rs", "py", "py_lib", "crate_name"):
            self.assertNotIn(
                key,
                targets,
                f"{key!r} names a target the codegen never emits for {RECORDER_ITEM['stem']}",
            )

        # …and no phantom LABEL may reach the manifest under any other key.
        declared = set(targets.values())
        for phantom in (
            f"{RECORDER_PACKAGE}:recorder_control_schema_meta",
            f"{RECORDER_PACKAGE}:recorder_control_schema_meta_py",
            f"{RECORDER_PACKAGE}:recorder_control_schema_meta_py_lib",
            f"{RECORDER_PACKAGE}:recorder_control_rust",
            f"{RECORDER_PACKAGE}:recorder_control_rust_closure",
            f"{RECORDER_PACKAGE}:recorder_control_py",
            f"{RECORDER_PACKAGE}:recorder_control_py_lib",
            f"{RECORDER_PACKAGE}:humanoid_recorder_control_msg_rs",
        ):
            self.assertNotIn(phantom, declared)

        self.assertEqual([], result["py_metas"])
        self.assertEqual([], result["py_trees"])
        self.assertEqual([], result["rs_facade_entries"])

        # The C++ surface it DOES declare stays intact.
        self.assertEqual(
            f"{RECORDER_PACKAGE}:recorder_control_schema_meta_cpp",
            targets["schema_meta_cpp"],
        )
        self.assertEqual(f"{RECORDER_PACKAGE}:recorder_control_bfbs", targets["bfbs"])
        self.assertEqual(f"{RECORDER_PACKAGE}:recorder_control_cpp", targets["cpp"])
        self.assertEqual(
            f"{RECORDER_PACKAGE}:recorder_control_cpp_lib", targets["cpp_lib"]
        )
        self.assertEqual(f"{RECORDER_PACKAGE}:recorder_control_cc", targets["cc"])
        self.assertEqual(
            f"{RECORDER_PACKAGE}:humanoid_recorder_control_msg_cc", targets["typed_cc"]
        )
        self.assertEqual([":humanoid_recorder_control_msg_cc"], result["cc_targets"])

    def test_first_class_item_still_declares_every_metadata_target(self):
        result = declare_targets(FIRST_CLASS_ITEM, FIRST_CLASS_PACKAGE)
        targets = result["targets"]
        self.assertEqual(
            f"{FIRST_CLASS_PACKAGE}:probe_schema_meta", targets["schema_meta_rust"]
        )
        self.assertEqual(
            f"{FIRST_CLASS_PACKAGE}:probe_schema_meta_cpp", targets["schema_meta_cpp"]
        )
        self.assertEqual(
            f"{FIRST_CLASS_PACKAGE}:probe_schema_meta_py", targets["schema_meta_py"]
        )
        self.assertEqual(
            f"{FIRST_CLASS_PACKAGE}:probe_schema_meta_py_lib",
            targets["schema_meta_py_lib"],
        )
        self.assertEqual([":probe_schema_meta_py"], result["py_metas"])
        self.assertEqual([":probe_py"], result["py_trees"])
        self.assertEqual(f"{FIRST_CLASS_PACKAGE}:probe_msg_rs", targets["typed_rs"])
        self.assertEqual("probe_msg", targets["crate_name"])

    def test_explicit_schema_meta_py_false_drops_only_the_python_metadata(self):
        item = dict(FIRST_CLASS_ITEM, schema_meta_py=False)
        result = declare_targets(item, FIRST_CLASS_PACKAGE)
        targets = result["targets"]
        self.assertNotIn("schema_meta_py", targets)
        self.assertNotIn("schema_meta_py_lib", targets)
        self.assertEqual([], result["py_metas"])
        # Raw Python codegen is a separate flag and stays on.
        self.assertEqual([":probe_py"], result["py_trees"])
        self.assertEqual(f"{FIRST_CLASS_PACKAGE}:probe_py", targets["py"])
        self.assertIn("schema_meta_rust", targets)

    def test_schema_meta_rust_alone_declares_only_the_rust_metadata(self):
        item = dict(RECORDER_ITEM, schema_meta_rust=True, schema_meta_cpp=False)
        targets = declare_targets(item, RECORDER_PACKAGE)["targets"]
        self.assertEqual(
            f"{RECORDER_PACKAGE}:recorder_control_schema_meta",
            targets["schema_meta_rust"],
        )
        self.assertNotIn("schema_meta_cpp", targets)


if __name__ == "__main__":
    unittest.main()
