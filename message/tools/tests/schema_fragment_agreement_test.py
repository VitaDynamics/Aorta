import copy
import unittest

from message.tools.schema_lint import SCHEMA_ITEM_FIELDS, SchemaLintError, check_fragment_agreement


class FragmentAgreementTest(unittest.TestCase):
    def setUp(self):
        self.original = dict(pkg="//schemas", stem="sample", root_fbs="sample.fbs",
                             provenance="internal", publication="public")
        self.texts = {"fragment.bzl": self.text(self.original)}
        self.audit = {
            "audit_version": 2,
            "rules_api": 2,
            "field_names": SCHEMA_ITEM_FIELDS,
            "fragments": [dict(file="fragment.bzl", symbol="ENTRIES", packages=None, mode="authored")],
            "declarations": [dict(original=copy.deepcopy(self.original), effective=copy.deepcopy(self.original),
                                  overrides={}, expected=dict(publication="public", dep_fbs=[]))],
            "entries": [dict(pkg="//schemas", stem="sample", publication="public", dep_fbs=[])],
        }

    def text(self, *items):
        return "ENTRIES = [" + ", ".join("struct(" + ", ".join(k + "=" + repr(v) for k, v in sorted(item.items())) + ")" for item in items) + "]\n"

    def check(self):
        check_fragment_agreement(self.audit, self.texts.__getitem__)

    def test_text_mutations_are_not_hidden_by_evaluated_values(self):
        for field, value in [("publication", "internal"), ("cpp", True), ("action_name", "Other"),
                             ("cc_alias", None), ("dep_fbs", [":other.fbs"])]:
            with self.subTest(field=field):
                changed = dict(self.original, **{field: value})
                self.texts["fragment.bzl"] = self.text(changed)
                with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*" + field):
                    self.check()

    def test_same_package_extra_text_entry_is_rejected(self):
        self.texts["fragment.bzl"] = self.text(self.original, dict(self.original, stem="z", root_fbs="z.fbs"))
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*coverage"):
            self.check()

    def test_missing_text_entry_is_rejected(self):
        self.texts["fragment.bzl"] = "ENTRIES = []\n"
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*coverage"):
            self.check()

    def test_descriptor_scope_cannot_omit_declared_package(self):
        self.audit["fragments"][0]["packages"] = ["//other"]
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*coverage"):
            self.check()

    def test_generated_text_is_mutated_independently(self):
        self.audit["fragments"][0]["mode"] = "generated"
        self.texts["fragment.bzl"] = self.text(dict(self.original, publication="internal"))
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*publication"):
            self.check()

    def test_override_does_not_hide_original_text_drift(self):
        record = self.audit["declarations"][0]
        record["overrides"] = {"dep_fbs": [":new.fbs"]}
        record["effective"]["dep_fbs"] = [":new.fbs"]
        self.texts["fragment.bzl"] = self.text(dict(self.original, dep_fbs=[":new.fbs"]))
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*dep_fbs"):
            self.check()

    def test_unrecorded_effective_edit_is_rejected(self):
        self.audit["declarations"][0]["effective"]["publication"] = "internal"
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*effective"):
            self.check()

    def test_provider_projection_drift_is_rejected(self):
        self.audit["entries"][0]["publication"] = "internal"
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*publication"):
            self.check()

    def test_duplicate_direct_edges_are_rejected(self):
        self.audit["entries"][0]["dep_fbs"] = ["@@owner//:x.fbs"] * 2
        self.audit["declarations"][0]["expected"]["dep_fbs"] = self.audit["entries"][0]["dep_fbs"]
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*duplicate direct"):
            self.check()

    def test_comments_constants_and_empty_companion_fragment(self):
        self.texts["fragment.bzl"] = '# comment\nPUB = "public"\n' + self.text(self.original).replace("'public'", "PUB")
        self.texts["empty.bzl"] = "ENTRIES = []\n"
        self.audit["fragments"].append(dict(file="empty.bzl", symbol="ENTRIES", packages=None, mode="authored"))
        self.check()

    def test_complete_package_filter(self):
        other = dict(self.original, pkg="//other")
        self.texts["fragment.bzl"] = self.text(other, self.original)
        self.audit["fragments"][0]["packages"] = ["//schemas"]
        self.check()

    def test_recorded_override_and_empty_python_includes(self):
        for values in [dict(dep_fbs=[":new.fbs"]), dict(dep_fbs=[], python_include_dirs=[])]:
            audit = copy.deepcopy(self.audit)
            record = self.audit["declarations"][0]
            record["original"].update({k: v for k, v in values.items() if k != "dep_fbs"})
            record["effective"].update(values)
            record["overrides"] = {"dep_fbs": values["dep_fbs"]}
            self.texts["fragment.bzl"] = self.text(record["original"])
            self.check()
            self.audit = audit

    def test_existing_parser_refusals(self):
        for text in ["ENTRIES = [struct(**{})]", "ENTRIES = [x for x in []]", "OTHER = []",
                     self.text(dict(self.original, typo=True)), self.text(self.original, self.original),
                     self.text(dict(self.original, stem="z"), self.original)]:
            with self.subTest(text=text):
                self.texts["fragment.bzl"] = text
                with self.assertRaises(SchemaLintError):
                    self.check()

    def test_label_spellings_use_recorded_canonical_identity(self):
        record = self.audit["declarations"][0]
        canonical = "@@owner//schemas:types.fbs"
        record["original"]["dep_fbs"] = [":types.fbs"]
        record["effective"]["dep_fbs"] = [":types.fbs"]
        record["expected"]["dep_fbs"] = [canonical]
        self.audit["entries"][0]["dep_fbs"] = [canonical]
        record["label_bindings"] = {"dep_fbs": {
            ":types.fbs": canonical, "//schemas:types.fbs": canonical,
            canonical: canonical, ":alias.fbs": "@@owner//schemas:alias.fbs",
        }}
        for spelling in ["//schemas:types.fbs", canonical]:
            with self.subTest(spelling=spelling):
                self.texts["fragment.bzl"] = self.text(dict(record["original"], dep_fbs=[spelling]))
                self.check()
        self.texts["fragment.bzl"] = self.text(dict(record["original"], dep_fbs=[":alias.fbs"]))
        with self.assertRaisesRegex(SchemaLintError, "AORTA_L4.*dep_fbs"):
            self.check()


if __name__ == "__main__":
    unittest.main()
