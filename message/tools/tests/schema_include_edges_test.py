import copy
from pathlib import Path
import tempfile
import unittest

from message.tools.schema_lint import SchemaLintError, check_include_edges


class IncludeEdgesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.types = self.entry("types", "local/schemas/demo/types.fbs", "schemas/demo/types.fbs")
        self.sample = self.entry("sample", "local/schemas/demo/sample.fbs", "schemas/demo/sample.fbs", [self.types])
        self.entries = [self.sample, self.types]
        self.dependencies = []
        self.write(self.sample, 'include "types.fbs";\nnamespace demo; table Sample {}\n')

    def entry(self, name, path, key, deps=()):
        label = "@@" + name + "//:" + name + ".fbs"
        result = dict(fbs_label=label, logical_include_path=key, source_identity=dict(owner=label, path=path),
                      dep_fbs=[d["fbs_label"] for d in deps],
                      dependency_sources={d["fbs_label"]: d["source_identity"] for d in deps})
        self.write(result, "namespace demo; table Type {}\n")
        return result

    def write(self, entry, text):
        path = self.root / entry["source_identity"]["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def check(self):
        check_include_edges(self.entries, self.dependencies, lambda name: self.root / name)

    def test_bare_and_rooted(self):
        for spelling in ["types.fbs", "schemas/demo/types.fbs"]:
            self.write(self.sample, 'include "' + spelling + '";\n')
            self.check()

    def test_source_mutation_is_read_again(self):
        self.check()
        self.write(self.sample, "// source changed after previous check\n")
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_EDGES.*unused"):
            self.check()

    def test_undeclared_include(self):
        self.sample["dep_fbs"] = []
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_EDGES.*undeclared"):
            self.check()

    def test_missing_provider_owner(self):
        self.entries = [self.sample]
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_OWNER"):
            self.check()

    def test_wrong_configured_file(self):
        self.sample["dependency_sources"][self.types["fbs_label"]] = dict(owner="wrong", path="wrong.fbs")
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_FILE"):
            self.check()

    def test_direct_edges_not_transitive_closure(self):
        branch = self.entry("branch", "local/schemas/demo/branch.fbs", "schemas/demo/branch.fbs", [self.types])
        self.write(branch, 'include "types.fbs";\n')
        self.entries.append(branch)
        self.sample["dep_fbs"] = [branch["fbs_label"]]
        self.sample["dependency_sources"] = {branch["fbs_label"]: branch["source_identity"]}
        self.write(self.sample, 'include "branch.fbs"; include "types.fbs";\n')
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_EDGES"):
            self.check()
        self.sample["dep_fbs"].append(self.types["fbs_label"])
        self.sample["dependency_sources"][self.types["fbs_label"]] = self.types["source_identity"]
        self.check()

    def test_foreign_bare_is_not_basename_lookup(self):
        self.types["source_identity"]["path"] = "foreign/schemas/demo/types.fbs"
        self.write(self.types, "namespace foreign; table Types {}\n")
        self.entries = [self.sample]
        self.dependencies = [self.types]
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_UNRESOLVED"):
            self.check()
        self.write(self.sample, 'include "schemas/demo/types.fbs";\n')
        self.check()

    def test_same_basename_distinct_keys(self):
        self.dependencies = [self.entry("remote", "remote/schemas/other/types.fbs", "schemas/other/types.fbs")]
        self.check()

    def test_duplicate_logical_key(self):
        self.dependencies = [self.entry("remote", "remote/schemas/demo/types.fbs", self.types["logical_include_path"])]
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_KEY"):
            self.check()

    def test_first_schemas_component(self):
        self.types["logical_include_path"] = "schemas/first/message/schemas/demo/types.fbs"
        self.write(self.sample, 'include "schemas/demo/types.fbs";\n')
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_UNRESOLVED"):
            self.check()
        self.write(self.sample, 'include "schemas/first/message/schemas/demo/types.fbs";\n')
        self.check()

    def test_repository_relative_key_without_schemas(self):
        self.types["logical_include_path"] = "custom/types.fbs"
        self.write(self.sample, 'include "custom/types.fbs";\n')
        self.check()

    def test_source_relative_subdirectory(self):
        self.types["source_identity"]["path"] = "local/schemas/demo/sub/types.fbs"
        self.write(self.types, "namespace demo; table Types {}\n")
        self.write(self.sample, 'include "sub/types.fbs";\n')
        self.check()

    def test_unknown_or_escaping_path(self):
        for spelling, diagnostic in [("schemas/missing.fbs", "UNRESOLVED"), ("../types.fbs", "PATH"),
                                     ("/types.fbs", "PATH"), ("./types.fbs", "PATH")]:
            with self.subTest(spelling=spelling):
                self.write(self.sample, 'include "' + spelling + '";\n')
                with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_" + diagnostic):
                    self.check()

    def test_comments_repetition_and_exact_diamond(self):
        self.dependencies = [copy.deepcopy(self.types)]
        self.write(self.sample, '// include "fake.fbs";\n/* include "also_fake.fbs"; */\ninclude "types.fbs"; include "types.fbs";\n')
        self.check()

    def test_conflicting_provider_owner(self):
        self.dependencies = [dict(self.types, logical_include_path="other/types.fbs")]
        with self.assertRaisesRegex(SchemaLintError, "AORTA_INCLUDE_OWNER"):
            self.check()


if __name__ == "__main__":
    unittest.main()
