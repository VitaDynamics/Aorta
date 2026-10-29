#!/usr/bin/env python3
"""Mirror-state registry bijection test."""

import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from registry_lint import RegistryEntry, lint_fragment_text, repo_root, validate_bijection


class MirrorStateBijectionTest(unittest.TestCase):
    def test_materialized_repository_is_bijective(self):
        root = repo_root()
        relative = "message/schemas/registry_local.bzl"
        values = lint_fragment_text(
            (root / relative).read_text(encoding="utf-8"),
            relative,
            "LOCAL_SCHEMA_DEFINITIONS",
            frozenset({"public", "fixture"}),
        )
        entries = [RegistryEntry(value, relative) for value in values]
        result = validate_bijection(root, entries)
        self.assertEqual(len(entries), result["entries"])
        self.assertEqual(result["entries"], result["physical_files"])
        self.assertEqual(0, result["internal_entries"])


if __name__ == "__main__":
    unittest.main()
