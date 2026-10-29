"""Execute the source gate over real Bazel audit/runfiles inputs."""

import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from message.tools import schema_lint_main


class SurfaceTest(unittest.TestCase):
    def setUp(self):
        self.audit = json.loads(Path(os.environ["AORTA_SURFACE_AUDIT"]).read_text())
        self.adopted = [entry for entry in self.audit["entries"] if entry["source_contract"] is not None]
        self.assertTrue(self.adopted)

    def execute(self, audit):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            path.write_text(json.dumps(audit))
            with mock.patch.object(sys, "argv", ["schema_lint_main", str(path), "source"]):
                schema_lint_main.main()

    def test_real_runfiles(self):
        self.execute(self.audit)

    def test_missing_contract_is_not_legacy(self):
        for entry in self.adopted:
            with self.subTest(source=entry["fbs_label"]):
                audit = copy.deepcopy(self.audit)
                selected = next(e for e in audit["entries"] if e["fbs_label"] == entry["fbs_label"])
                del selected["source_contract"]
                with self.assertRaisesRegex(ValueError, "AORTA_SOURCE_INPUT.*source_contract"):
                    self.execute(audit)

    def test_missing_published_bfbs_is_not_success(self):
        typed = [entry for entry in self.adopted if "bfbs" in entry["output_labels"]]
        self.assertTrue(typed)
        for entry in typed:
            with self.subTest(source=entry["fbs_label"]):
                audit = copy.deepcopy(self.audit)
                audit["source_evidence"][entry["fbs_label"]]["published_bfbs"] = []
                with self.assertRaisesRegex(ValueError, "AORTA_SOURCE_INPUT.*published BFBS"):
                    self.execute(audit)

    def test_missing_output_role_is_not_success(self):
        for entry in self.adopted:
            for role in entry["output_labels"]:
                with self.subTest(source=entry["fbs_label"], role=role):
                    audit = copy.deepcopy(self.audit)
                    del audit["source_evidence"][entry["fbs_label"]]["outputs"][role]
                    with self.assertRaisesRegex(ValueError, "AORTA_SOURCE_INPUT.*output roles"):
                        self.execute(audit)

    def test_missing_reflection_fails(self):
        audit = copy.deepcopy(self.audit)
        del audit["source_evidence"][self.adopted[0]["fbs_label"]]
        with self.assertRaises(KeyError):
            self.execute(audit)

    def test_missing_runfile_mapping_fails(self):
        audit = copy.deepcopy(self.audit)
        del audit["files"][self.adopted[0]["source_identity"]["path"]]
        with self.assertRaises(KeyError):
            self.execute(audit)

    def test_missing_runfile_has_no_checkout_fallback(self):
        audit = copy.deepcopy(self.audit)
        audit["files"][self.adopted[0]["source_identity"]["path"]] = "absent/source.fbs"
        with self.assertRaises(FileNotFoundError):
            self.execute(audit)

    def test_explicit_legacy_needs_no_source_evidence(self):
        audit = copy.deepcopy(self.audit)
        for entry in audit["entries"]:
            entry["source_contract"] = None
        audit["source_evidence"] = {}
        audit["files"] = {}
        self.execute(audit)

    def test_real_adoption_partition_and_legacy_execution(self):
        base = json.loads(Path(os.environ["AORTA_BASE_SURFACE_AUDIT"]).read_text())
        entries = [entry for fragment in base["fragments"] for entry in fragment["audit"]["entries"]]
        adopted = {entry["fbs_label"] for entry in entries if entry["source_contract"] is not None}
        legacy = [entry for entry in entries if entry["source_contract"] is None]
        self.assertTrue(adopted)
        self.assertTrue(legacy)
        self.assertEqual(set(base["source_evidence"]), adopted)
        self.execute(dict(base, entries=entries))
        self.execute(dict(base, entries=legacy, files={}, source_evidence={}))
        # Legacy declarations still participate in L4 and independent census.
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            path.write_text(json.dumps(base))
            with mock.patch.object(sys, "argv", ["schema_lint_main", str(path), "module"]):
                schema_lint_main.main()
        print("adoption: adopted=%d legacy=%d" % (
            len(adopted) + len(self.adopted), len(legacy)))

    def test_legacy_still_requires_include_agreement(self):
        base = json.loads(Path(os.environ["AORTA_BASE_SURFACE_AUDIT"]).read_text())
        entry = next(entry for fragment in base["fragments"] for entry in fragment["audit"]["entries"]
                     if entry["source_contract"] is None and entry["dep_fbs"])
        source = entry["source_identity"]["path"]
        with tempfile.TemporaryDirectory() as directory:
            replacement = Path(directory) / "changed.fbs"
            replacement.write_text("namespace legacy; table Changed {}\n")
            base["files"][source] = str(replacement)
            path = Path(directory) / "audit.json"
            path.write_text(json.dumps(base))
            with mock.patch.object(sys, "argv", ["schema_lint_main", str(path), "module"]):
                with self.assertRaisesRegex(ValueError, "AORTA_INCLUDE_EDGES"):
                    schema_lint_main.main()


if __name__ == "__main__":
    unittest.main()
