"""Unit tests for core/tools/core_abi.py, the one parser of AORTA_ABI_VERSION."""

import sys
import unittest
from pathlib import Path

_CORE_TOOLS = Path(__file__).resolve().parents[2] / "core" / "tools"
if str(_CORE_TOOLS) not in sys.path:
    sys.path.insert(0, str(_CORE_TOOLS))

import core_abi  # noqa: E402


class ParseCoreAbiTest(unittest.TestCase):
    def test_parses_the_declaration(self):
        text = "stuff\npub const AORTA_ABI_VERSION: u32 = 8;\nmore"
        self.assertEqual(core_abi.parse_core_abi(text), 8)

    def test_rejects_a_missing_declaration(self):
        with self.assertRaisesRegex(ValueError, "found 0"):
            core_abi.parse_core_abi("nothing here")

    def test_rejects_two_declarations(self):
        text = (
            "pub const AORTA_ABI_VERSION: u32 = 8;\n"
            "pub const AORTA_ABI_VERSION: u32 = 9;\n"
        )
        with self.assertRaisesRegex(ValueError, "found 2"):
            core_abi.parse_core_abi(text)

    def test_ignores_prose_mentioning_the_constant(self):
        text = (
            "//! release and evolve via new `_v2` symbols plus an `AORTA_ABI_VERSION`\n"
            "pub const AORTA_ABI_VERSION: u32 = 12;\n"
            "    AORTA_ABI_VERSION\n"
        )
        self.assertEqual(core_abi.parse_core_abi(text), 12)

    def test_reads_the_checked_in_core(self):
        # The real declaration in core/src/ffi/c_api/mod.rs must parse; the
        # bundle gate and the release workflow both rely on this path.
        self.assertGreater(core_abi.read_core_abi(), 0)


class RenderTest(unittest.TestCase):
    def test_replaces_every_placeholder(self):
        rendered = core_abi.render(
            "so.@AORTA_CORE_ABI_VERSION@ v@AORTA_CORE_ABI_VERSION@.0.0", 12
        )
        self.assertEqual(rendered, "so.12 v12.0.0")

    def test_rejects_a_template_without_placeholder(self):
        with self.assertRaisesRegex(ValueError, "AORTA_CORE_ABI_VERSION"):
            core_abi.render("set(Aorta_CORE_ABI_VERSION 10)", 12)


if __name__ == "__main__":
    unittest.main()
