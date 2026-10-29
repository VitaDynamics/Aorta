#!/usr/bin/env python3

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from registry_lint import RegistryLintError, lint_fragment_text, load_registry_entries, repo_root


class RegistryFragmentAstLintTest(unittest.TestCase):
    def test_repository_fragments_pass(self):
        entries = load_registry_entries(repo_root())
        self.assertTrue(entries)
        self.assertTrue(all(e.provenance in {"public", "fixture"} for e in entries))

    def test_rejects_load(self):
        text = 'load("//x:y.bzl", "Y")\nENTRIES = []\n'
        with self.assertRaisesRegex(RegistryLintError, "assignment"):
            lint_fragment_text(text, "fixture.bzl", "ENTRIES", {"internal"})

    def test_rejects_other_calls(self):
        text = 'ENTRIES = [fail("no")]\n'
        with self.assertRaisesRegex(RegistryLintError, "struct"):
            lint_fragment_text(text, "fixture.bzl", "ENTRIES", {"internal"})

    def test_rejects_comprehension(self):
        text = 'ENTRIES = [struct(pkg = value) for value in []]\n'
        with self.assertRaisesRegex(RegistryLintError, "list literal"):
            lint_fragment_text(text, "fixture.bzl", "ENTRIES", {"internal"})

    def test_rejects_functions_and_conditionals(self):
        cases = (
            "def make_entry():\n    return None\nENTRIES = []\n",
            "if True:\n    ENTRIES = []\n",
        )
        for text in cases:
            with self.subTest(text=text):
                with self.assertRaisesRegex(RegistryLintError, "assignment"):
                    lint_fragment_text(text, "fixture.bzl", "ENTRIES", {"internal"})

    def test_rejects_implicit_computation(self):
        text = (
            'PKG = "//message/schemas/topic/x"\n'
            'ENTRIES = [struct(pkg = PKG + ":x.fbs", stem = "x", root_fbs = "x.fbs", '
            'provenance = "internal")]\n'
        )
        with self.assertRaisesRegex(RegistryLintError, "scalar/list literals"):
            lint_fragment_text(text, "fixture.bzl", "ENTRIES", {"internal"})

    def test_rejects_forward_constant_reference(self):
        text = (
            'ENTRIES = [struct(pkg = PKG, stem = "x", root_fbs = "x.fbs", '
            'provenance = "internal")]\nPKG = "//message/schemas/topic/x"\n'
        )
        with self.assertRaisesRegex(RegistryLintError, "previously declared"):
            lint_fragment_text(text, "fixture.bzl", "ENTRIES", {"internal"})

    def test_rejects_wrong_provenance(self):
        text = (
            'ENTRIES = [struct(pkg = "//message/schemas/topic/x", stem = "x", '
            'root_fbs = "x.fbs", provenance = "public")]\n'
        )
        with self.assertRaisesRegex(RegistryLintError, "provenance"):
            lint_fragment_text(text, "fixture.bzl", "ENTRIES", {"internal"})


if __name__ == "__main__":
    unittest.main()
