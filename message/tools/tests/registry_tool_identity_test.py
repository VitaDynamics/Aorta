import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from message.tools.registry_tool_identity import main


class RegistryToolIdentityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.template = self.root / "template.json"
        self.template.write_text(json.dumps({"flatbuffers_version": "25.12.19", "entries": []}))
        self.output = self.root / "manifest.json"
        self.flatc = self.root / "flatc"
        self.flatc.write_text('#!/bin/sh\necho "flatc version 25.12.19"\n')
        self.flatc.chmod(0o755)
        self.source = self.root / "codegen.bzl"
        self.source.write_text("CODEGEN = 1\n")

    def run_writer(self):
        main([str(self.template), str(self.output), str(self.flatc), str(self.source)])
        return json.loads(self.output.read_text())

    def test_records_actual_compiler_and_codegen_bytes(self):
        result = self.run_writer()
        self.assertEqual([], result["entries"])
        self.assertEqual(hashlib.sha256(self.flatc.read_bytes()).hexdigest(),
                         result["verified_tools"]["flatc_sha256"])
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(),
                         result["verified_tools"]["codegen_sha256"][str(self.source)])

    def test_codegen_change_invalidates_identity(self):
        before = self.run_writer()["verified_tools"]
        self.source.write_text("CODEGEN = 2\n")
        after = self.run_writer()["verified_tools"]
        self.assertNotEqual(before["codegen_sha256"], after["codegen_sha256"])

    def test_wrong_selected_version_does_not_publish_manifest(self):
        self.flatc.write_text('#!/bin/sh\necho "flatc version 0.0.0"\n')
        with self.assertRaisesRegex(ValueError, "FlatBuffers tool version mismatch"):
            self.run_writer()
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
