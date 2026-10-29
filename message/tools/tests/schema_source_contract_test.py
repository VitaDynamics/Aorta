import copy
import unittest

from message.tools.schema_lint import SchemaLintError
from message.tools.schema_source_contract import check_output_provenance, check_source_contract


class SourceContractTest(unittest.TestCase):
    def setUp(self):
        self.entry = dict(fbs_label='@@owner//schemas/custom:sample.fbs', logical_include_path='schemas/custom/sample.fbs',
                          source_identity=dict(path='schemas/custom/sample.fbs'), action_name=None,
                          flags=dict(bfbs=True, action_schema=False, rust_wrapper=False),
                          source_contract=dict(schema_kind='message', root_type='test.Sample', namespace='test', file_identifier=''))
        self.text = 'namespace test; table Sample { value:int; } root_type Sample;'
        self.reflection = dict(root_type='test.Sample', file_identifier='', local_namespaces=['test'],
                               tables={'test.Sample': dict(namespace='test', local=True)})

    def check(self):
        check_source_contract(self.entry, self.text, self.reflection)

    def test_message(self):
        self.check()

    def test_root_contract_mutations(self):
        for root in ['', 'test.Other', 'Sample']:
            with self.subTest(root=root), self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_ROOT'):
                self.entry['source_contract']['root_type'] = root
                self.check()

    def test_missing_and_duplicate_source_root(self):
        for text in ['namespace test; table Sample {}', self.text + ' root_type Sample;']:
            with self.subTest(text=text), self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_ROOT'):
                self.text = text
                self.check()

    def test_raw_root(self):
        self.entry['source_contract'].update(schema_kind='include_only', root_type='')
        self.entry['flags']['bfbs'] = False
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_ROOT'):
            self.check()

    def test_missing_contract_field(self):
        del self.entry['source_contract']['file_identifier']
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_CONTRACT'):
            self.check()

    def test_non_service_fields_refused(self):
        self.entry['source_contract']['response_type'] = 'test.Sample'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_CONTRACT'):
            self.check()

    def test_kind_flag_mismatch(self):
        self.entry['flags']['action_schema'] = True
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_KIND'):
            self.check()

    def test_package_kind(self):
        self.entry['logical_include_path'] = 'schemas/service/demo/sample.fbs'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_KIND'):
            self.check()

    def test_package_namespace(self):
        self.entry['source_contract'].update(schema_kind='service', request_type='test.Sample', response_type='test.Sample')
        self.entry['logical_include_path'] = 'schemas/service/demo/sample.fbs'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_NAMESPACE'):
            self.check()

    def test_namespace(self):
        self.entry['source_contract']['namespace'] = 'wrong'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_NAMESPACE'):
            self.check()

    def test_identifier_policies(self):
        for identifier in ['ABCD', 'ATRC', '']:
            with self.subTest(identifier=identifier):
                self.entry['source_contract']['file_identifier'] = identifier
                self.reflection['file_identifier'] = identifier
                self.text = 'namespace test; table Sample {} root_type Sample;'
                if identifier:
                    self.text += f' file_identifier "{identifier}";'
                self.check()

    def test_identifier_mutations(self):
        for identifier in ['BAD', 'wrong', 'ABCD', 'éééé']:
            with self.subTest(identifier=identifier), self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_IDENTIFIER'):
                self.entry['source_contract']['file_identifier'] = identifier
                self.check()

    def test_unexpected_identifier(self):
        self.text += ' file_identifier "ABCD";'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_IDENTIFIER'):
            self.check()

    def test_comments_and_strings_do_not_declare_roots(self):
        self.text = '/* root_type Ghost; */ namespace test; table Sample { value:string = "root_type Ghost;"; } root_type Sample;'
        self.check()

    def test_reflection_root_mismatch(self):
        self.reflection['root_type'] = 'test.Other'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_ROOT'):
            self.check()

    def test_imported_root(self):
        self.reflection['tables']['test.Sample']['local'] = False
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_ROOT'):
            self.check()

    def test_reflection_identifier_mismatch(self):
        self.reflection['file_identifier'] = 'ABCD'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_IDENTIFIER'):
            self.check()

    def test_reflection_error(self):
        self.reflection = dict(error='flatc rejected schema')
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_REFLECTION'):
            self.check()

    def test_service_nonroot_response_and_locality(self):
        self.entry['source_contract'].update(schema_kind='service', request_type='test.Sample', response_type='test.Reply')
        self.reflection['tables']['test.Reply'] = dict(namespace='test', local=True)
        self.check()
        self.reflection['tables']['test.Reply']['local'] = False
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_SERVICE'):
            self.check()
        del self.reflection['tables']['test.Reply']
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_SERVICE'):
            self.check()

    def test_action_both_roots_and_result_refusal(self):
        self.entry['source_contract'].update(schema_kind='action', root_type='')
        self.entry['flags']['action_schema'] = True
        self.text = 'namespace test;\ntable SampleGoal {}\ntable SampleResult {}\ntable SampleFeedbackData {}\n'
        self.reflection['root_type'] = 'test.SampleGoal'
        self.reflection['tables'] = {'test.' + t: dict(namespace='test', local=True) for t in ['SampleGoal', 'SampleResult', 'SampleFeedbackData']}
        self.check()
        self.entry['source_contract']['root_type'] = 'test.SampleGoal'
        self.text += 'root_type test.SampleGoal;'
        self.check()
        self.text = self.text.replace('root_type test.SampleGoal;', 'root_type test.SampleResult;')
        self.entry['source_contract']['root_type'] = 'test.SampleResult'
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_ACTION'):
            self.check()

    def test_output_source_tool_and_generated_identity(self):
        records = dict(bfbs=[dict(path='output.bfbs', generated=True, required_tool='flatc', sources=['flatc', 'schemas/custom/sample.fbs'])])
        check_output_provenance(self.entry, records)
        for key, value in [('generated', False), ('sources', ['flatc']), ('sources', ['schemas/custom/sample.fbs'])]:
            changed = copy.deepcopy(records)
            changed['bfbs'][0][key] = value
            with self.subTest(key=key, value=value), self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_PROVENANCE'):
                check_output_provenance(self.entry, changed)
        with self.assertRaisesRegex(SchemaLintError, 'AORTA_SOURCE_PROVENANCE'):
            check_output_provenance(self.entry, dict(bfbs=[]))


if __name__ == '__main__':
    unittest.main()
