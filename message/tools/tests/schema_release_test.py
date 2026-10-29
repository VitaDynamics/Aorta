"""Exercise real external configured outputs and their portable staged copies."""

import os
from pathlib import Path
import unittest

from schema_release import verify


class ConfiguredReleaseTest(unittest.TestCase):
    def test_external_transitive_staging(self):
        tree = Path(os.environ["RELEASE_TREE"]).resolve()
        manifest = verify(tree, {"module_name": "release_fixture", "module_revision": "fixture-module-revision", "aorta_revision": "fixture-aorta-revision"})
        sources = {row["logical_include_path"]: row for row in manifest["sources"]}
        self.assertEqual(set(sources), {"schemas/system/aorta_base.fbs", "schemas/release/commands.fbs", "schemas/release/release_leaf.fbs", "schemas/release/nested/release_reading.fbs"})
        self.assertEqual(sources["schemas/system/aorta_base.fbs"]["origin_module"], "aorta")
        self.assertEqual(sources["schemas/release/release_leaf.fbs"]["origin_module"], "release_fixture")
        roles = {item["role"] for item in manifest["artifacts"]}
        self.assertTrue({"rust", "rust_wrapper", "rust_support", "bfbs", "schema_meta_rust", "schema_meta_py"} <= roles)
        paths = [tree / a["relative"] for a in manifest["artifacts"]]
        canonical = next(p for p in paths if p.name == "release_reading.bfbs" and "generated" in p.parts)
        local = next(p for p in paths if p.name.endswith("release_reading.bfbs") and "bindings" in p.parts)
        self.assertEqual(canonical.read_bytes(), local.read_bytes())
        self.assertEqual(canonical.read_bytes()[4:8], b"BFBS")
        self.assertIn(b"schemas/release/release_leaf.fbs", (tree / "sources/schemas/release/nested/release_reading.fbs").read_bytes())
        self.assertIn("flatc_sha256", manifest["verified_tools"])
        self.assertTrue((tree / "licenses/aorta/NOTICE").is_file())
        self.assertTrue((tree / "licenses/release_fixture/LICENSE").is_file())
        self.assertFalse(any("bazel-bin" in str(p) for p in manifest["artifacts"]))


if __name__ == "__main__":
    unittest.main()
