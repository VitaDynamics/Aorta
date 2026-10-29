"""Refuse incompatible audit metadata using actual fragment runfiles."""

import copy
import json
import os
from pathlib import Path
import unittest

from message.tools.schema_lint import SchemaLintError, check_fragment_agreement


class ApiFreezeTest(unittest.TestCase):
    def setUp(self):
        self.audit = json.loads(Path(os.environ["AORTA_FREEZE_AUDIT"]).read_text())
        self.runfiles = Path(os.environ["TEST_SRCDIR"])

    def check(self, audit):
        check_fragment_agreement(audit, lambda name: (self.runfiles / audit["files"][name]).read_text())

    def test_current_audit(self):
        self.check(self.audit)

    def test_api_one_audit(self):
        audit = copy.deepcopy(self.audit)
        audit["rules_api"] = 1
        with self.assertRaisesRegex(SchemaLintError, "AORTA_LINT_SCHEMA.*rules_api"):
            self.check(audit)

    def test_missing_api_is_not_current(self):
        audit = copy.deepcopy(self.audit)
        audit.pop("rules_api", None)
        with self.assertRaisesRegex(SchemaLintError, "AORTA_LINT_SCHEMA.*rules_api"):
            self.check(audit)

    def test_old_payload_version(self):
        audit = copy.deepcopy(self.audit)
        audit["audit_version"] = 1
        with self.assertRaisesRegex(SchemaLintError, "AORTA_LINT_SCHEMA.*audit_version"):
            self.check(audit)

    def test_stale_field_schema(self):
        audit = copy.deepcopy(self.audit)
        audit["field_names"].remove("request_type")
        with self.assertRaisesRegex(SchemaLintError, "AORTA_LINT_SCHEMA.*field_names"):
            self.check(audit)

    def test_unknown_field_schema(self):
        audit = copy.deepcopy(self.audit)
        audit["field_names"].append("obsolete_field")
        with self.assertRaisesRegex(SchemaLintError, "AORTA_LINT_SCHEMA.*field_names"):
            self.check(audit)


if __name__ == "__main__":
    unittest.main()
