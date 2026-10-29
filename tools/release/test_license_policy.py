"""Exercise the release policy with the real pinned cargo-about executable."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


@unittest.skipUnless(shutil.which("cargo-about"), "cargo-about is installed by the release job")
class LicensePolicyTest(unittest.TestCase):
    def test_spdx_policy(self):
        config = Path(__file__).with_name("about.toml").resolve()
        cases = [
            ("MIT", True),
            ("MIT OR GPL-3.0-only", True),
            ("Apache-2.0 WITH LLVM-exception", True),
            ("MIT AND GPL-3.0-only", False),
            ("GPL-3.0-only", False),
            ("AGPL-3.0-only", False),
            ("LGPL-2.1-only", False),
            ("LicenseRef-Unknown-Fixture", False),
            (None, False),
        ]
        for expression, accepted in cases:
            with self.subTest(expression=expression), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / "src").mkdir()
                (root / "src/lib.rs").write_text("")
                manifest = '[package]\nname="license-policy-fixture"\nversion="0.1.0"\nedition="2021"\n'
                if expression is not None:
                    manifest += "license=" + json.dumps(expression) + "\n"
                (root / "Cargo.toml").write_text(manifest)
                subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=root, check=True,
                               capture_output=True)
                (root / "licenses.json").write_text("stale evidence")
                result = subprocess.run([
                    sys.executable, str(Path(__file__).with_name("gen_third_party_notices.py").resolve()),
                    "--collect", "--manifest", str(root / "Cargo.toml"),
                    "--lock", str(root / "Cargo.lock"), "--config", str(config),
                    "--licenses", str(root / "licenses.json"),
                    # The fixture workspace is its own reachability root.
                    "--reachable-root", "license-policy-fixture",
                ], cwd=root, capture_output=True, text=True)
                print(f"LICENSE POLICY {expression or 'MISSING'}: rc={result.returncode}")
                self.assertEqual(result.returncode == 0, accepted, result.stderr)
                if not accepted:
                    self.assertIn("LICENSE REJECT license-policy-fixture", result.stderr)
                    self.assertFalse((root / "licenses.json").exists())
                else:
                    evidence = json.loads((root / "licenses.json").read_text())
                    self.assertEqual(evidence["reachable_root"], "license-policy-fixture")
                    self.assertEqual(
                        {tuple(row) for rows in evidence["reachable"].values() for row in rows},
                        {("license-policy-fixture", "0.1.0", "path")},
                    )


if __name__ == "__main__":
    unittest.main()
