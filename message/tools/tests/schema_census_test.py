import copy
import unittest

from message.tools.schema_lint import SchemaLintError, check_census


class CensusTest(unittest.TestCase):
    def setUp(self):
        self.source = dict(repo="owner", path="schemas/sample.fbs", package="schemas", is_source=True)
        self.record = dict(entry=dict(fbs_label="@@owner//schemas:sample.fbs", publication="public"), file=self.source)
        self.audit = dict(scope=dict(repo="owner", package=None), census=[self.source],
                          fragments=[dict(repo="owner", sources=[self.record])], dependency_entries=[])

    def test_complete_inventory(self):
        check_census(self.audit)

    def test_independent_orphan(self):
        self.audit["census"].append(dict(self.source, path="schemas/orphan.fbs"))
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_UNREGISTERED.*orphan"):
            check_census(self.audit)

    def test_missing_census_source(self):
        self.audit["census"] = []
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_MISSING.*sample"):
            check_census(self.audit)

    def test_omitted_package(self):
        self.audit["census"].append(dict(self.source, package="schemas/child", path="schemas/child/other.fbs"))
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_UNREGISTERED.*child"):
            check_census(self.audit)

    def test_exact_diamond_deduplicates(self):
        self.audit["fragments"].append(copy.deepcopy(self.audit["fragments"][0]))
        self.audit["census"].append(copy.deepcopy(self.source))
        check_census(self.audit)

    def test_distinct_declarations_for_same_file(self):
        for change in [dict(fbs_label="@@owner//schemas:alias.fbs"), dict(publication="internal")]:
            with self.subTest(change=change):
                self.audit["fragments"][0]["sources"] = [self.record, dict(self.record, entry=dict(self.record["entry"], **change))]
                with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_DUPLICATE"):
                    check_census(self.audit)

    def test_foreign_census_same_path(self):
        self.audit["census"].append(dict(self.source, repo="foreign"))
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_FOREIGN"):
            check_census(self.audit)

    def test_foreign_declared_source(self):
        self.record["file"] = dict(self.source, repo="foreign")
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_FOREIGN"):
            check_census(self.audit)

    def test_foreign_fragment(self):
        self.audit["fragments"][0]["repo"] = "foreign"
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_FOREIGN"):
            check_census(self.audit)

    def test_package_scope_excludes_child_owner(self):
        self.audit["scope"]["package"] = "schemas"
        self.record["file"] = dict(self.source, package="schemas/child")
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_FOREIGN"):
            check_census(self.audit)

    def test_generated_census_or_declared_file(self):
        for side in ["census", "declaration"]:
            with self.subTest(side=side):
                audit = copy.deepcopy(self.audit)
                if side == "census":
                    audit["census"] = [dict(self.source, is_source=False)]
                else:
                    audit["fragments"][0]["sources"][0]["file"] = dict(self.source, is_source=False)
                with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_GENERATED"):
                    check_census(audit)

    def test_dependency_metadata_does_not_register_local_source(self):
        self.audit["dependency_entries"] = [self.record["entry"], dict(fbs_label="@@remote//schemas:sample.fbs")]
        check_census(self.audit)
        self.audit["fragments"] = []
        with self.assertRaisesRegex(SchemaLintError, "AORTA_CENSUS_UNREGISTERED"):
            check_census(self.audit)


if __name__ == "__main__":
    unittest.main()
