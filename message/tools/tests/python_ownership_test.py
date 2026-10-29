"""Distribution ownership regressions and direct-output assembly contract."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from python_distribution import assemble


class OwnershipTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.rows = []
        self.registry = {'entries': []}

    def schema(self, name, module, files, emitted=None, meta=None):
        direct = self.root / name / 'direct'
        tree = self.root / name / 'tree'
        for directory, entries in ((direct, files), (tree, emitted or files)):
            directory.mkdir(parents=True)
            for install, data in entries.items():
                path = directory / install
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(data)
        row = {'schema': name, 'module': module, 'direct': str(direct), 'trees': [str(tree)]}
        if meta:
            path = self.root / name / meta
            path.write_text('META = True\n')
            row['meta'] = {'path': str(path), 'install': meta}
        self.rows.append(row)
        self.registry['entries'].append({'fbs_label': name, 'origin_module': module, 'output_labels': {'python': name}})
        return row

    def run_assembly(self, module):
        out = self.root / ('installed_' + module)
        manifest = self.root / (module + '.json')
        assemble(self.rows, self.registry, module, out, manifest)
        return {p.relative_to(out).as_posix(): p.read_bytes() for p in out.rglob('*.py')}, json.loads(manifest.read_text())

    def test_independently_assembled_distributions_do_not_share_base_files(self):
        base = {'aorta/sys/AortaHeader.py': 'HEADER = True\n', 'aorta/__init__.py': ''}
        product = {'aorta/sys/product/Reading.py': 'READING = True\n', 'aorta/sys/__init__.py': ''}
        self.schema('base', 'aorta', base)
        self.schema('reading', 'product', product, {**base, **product}, meta='reading_schema_meta.py')
        base_files, base_manifest = self.run_assembly('aorta')
        product_files, manifest = self.run_assembly('product')
        self.assertFalse(base_files.keys() & product_files.keys())
        self.assertEqual(set(base_files), {'aorta/sys/AortaHeader.py'})
        self.assertEqual(set(product_files), {'aorta/sys/product/Reading.py', 'reading_schema_meta.py'})
        self.assertEqual(set(product_files), set(manifest['files']))
        self.assertEqual(manifest['files']['aorta/sys/product/Reading.py'], {'schema': 'reading', 'module': 'product'})
        self.assertEqual(base_manifest['schemas'], ['base'])

    def test_companions_follow_root_ownership_not_include_copies(self):
        base = self.schema('base', 'aorta', {'ns/Base.py': 'BASE = True'})
        product = self.schema('reading', 'product', {'ns/Reading.py': 'READING = True'},
                              {'ns/Base.py': 'BASE = True', 'ns/Reading.py': 'READING = True'})
        for row in (base, product):
            directory = self.root / row['schema'] / 'fastpath'
            directory.mkdir()
            (directory / (row['schema'] + '_fastpath.py')).write_text('COMPANION = True')
            row['fastpath'] = str(directory)
        base_files, _ = self.run_assembly('aorta')
        product_files, manifest = self.run_assembly('product')
        self.assertIn('base_fastpath.py', base_files)
        self.assertIn('reading_fastpath.py', product_files)
        self.assertNotIn('base_fastpath.py', product_files)
        self.assertEqual(manifest['files']['reading_fastpath.py'], {'schema': 'reading', 'module': 'product'})

    def test_runtime_is_base_owned_and_retains_authored_initializer(self):
        self.schema('base', 'aorta', {'ns/Base.py': 'BASE = True'})
        self.schema('reading', 'product', {'ns/Reading.py': 'READING = True'})
        source = self.root / '__init__.py'
        source.write_text('AUTHORED = True')
        runtime = [{'path': str(source), 'install': 'aorta_msgs_runtime/__init__.py'}]
        for module in ('aorta', 'product'):
            out, manifest = self.root / module, self.root / (module + '.json')
            assemble(self.rows, self.registry, module, out, manifest, runtime=runtime)
            owners = json.loads(manifest.read_text())
            self.assertEqual(owners['composition_files'][runtime[0]['install']], {'module': 'aorta', 'kind': 'runtime'})
            self.assertEqual((out / runtime[0]['install']).exists(), module == 'aorta')
            if module == 'aorta':
                self.assertEqual((out / runtime[0]['install']).read_text(), 'AUTHORED = True')

    def test_runtime_cannot_replace_schema_owned_file(self):
        self.schema('reading', 'product', {'aorta_msgs_runtime/_table_vector.py': 'SAME'})
        source = self.root / 'runtime.py'
        source.write_text('SAME')
        with self.assertRaisesRegex(ValueError, 'Python install collision.*_table_vector.py'):
            assemble(self.rows, self.registry, 'product', self.root / 'out', self.root / 'manifest',
                     runtime=[{'path': str(source), 'install': 'aorta_msgs_runtime/_table_vector.py'}])

    def test_companion_cannot_replace_another_schema_owner(self):
        left = self.schema('left', 'aorta', {'ns/Left.py': 'LEFT'})
        self.schema('right', 'product', {'same_fastpath.py': 'SAME'})
        directory = self.root / 'companion'
        directory.mkdir()
        (directory / 'same_fastpath.py').write_text('SAME')
        left['fastpath'] = str(directory)
        with self.assertRaisesRegex(ValueError, 'Python install collision.*same_fastpath.py'):
            self.run_assembly('product')

    def test_sdk_initializers_survive_generated_tree(self):
        files = {p + '/__init__.py': '' for p in ('aorta', 'aorta/action', 'aorta/capture')}
        files['aorta/capture/custom/Frame.py'] = 'FRAME = True\n'
        self.schema('frame', 'custom', files)
        installed, _ = self.run_assembly('custom')
        self.assertEqual(set(installed), {'aorta/capture/custom/Frame.py'})

    def test_distinct_owners_fail_even_for_equal_bytes(self):
        self.schema('left', 'aorta', {'same/Type.py': 'SAME = True'})
        self.schema('right', 'product', {'same/Type.py': 'SAME = True'})
        with self.assertRaisesRegex(ValueError, 'same/Type.py.*left.*right'):
            self.run_assembly('product')

    def test_same_owner_diamond_requires_identical_bytes(self):
        row = self.schema('base', 'aorta', {'same/Type.py': 'SAME = True'})
        self.rows.append(copy.deepcopy(row))
        installed, _ = self.run_assembly('aorta')
        self.assertEqual(set(installed), {'same/Type.py'})
        (Path(row['trees'][0]) / 'same/Type.py').write_text('DIFFERENT = True')
        with self.assertRaisesRegex(ValueError, 'same-owner bytes differ'):
            self.run_assembly('aorta')

    def test_metadata_and_generated_module_collision(self):
        self.schema('left', 'aorta', {'left/Type.py': ''}, meta='right_meta.py')
        self.schema('right', 'product', {'right_meta.py': ''})
        with self.assertRaisesRegex(ValueError, 'right_meta.py.*left.*right'):
            self.run_assembly('product')

    def test_incomplete_plan_rejected(self):
        self.schema('base', 'aorta', {'base/Type.py': ''})
        self.rows.clear()
        with self.assertRaisesRegex(ValueError, 'exactly'):
            self.run_assembly('aorta')

    def test_unowned_emitted_file_rejected(self):
        self.schema('base', 'aorta', {'base/Type.py': ''}, {'foreign/Type.py': ''})
        with self.assertRaisesRegex(ValueError, 'no direct owner'):
            self.run_assembly('aorta')


class WheelPathTest(unittest.TestCase):
    def check_wheel(self, extra, expected, metadata='sample-0.1.0.dist-info'):
        import base64
        import csv
        import hashlib
        import io
        import zipfile
        from python_distribution import verify_wheels

        with tempfile.TemporaryDirectory() as tmp:
            wheel = Path(tmp) / 'sample-0.1.0-py3-none-any.whl'
            files = {metadata + '/METADATA': b'Metadata-Version: 2.1\nName: sample\nVersion: 0.1.0\n',
                     metadata + '/WHEEL': b'Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n',
                     'owned.py': b'OWNED = True\n', **extra}
            rows = [[path, 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode(), str(len(data))]
                    for path, data in files.items()]
            record = metadata + '/RECORD'
            rows.append([record, '', ''])
            text = io.StringIO()
            csv.writer(text, lineterminator='\n').writerows(rows)
            files[record] = text.getvalue().encode()
            with zipfile.ZipFile(wheel, 'w') as archive:
                for path, data in files.items():
                    archive.writestr(path, data)
            return verify_wheels([wheel], [{'files': {'owned.py': {}, **expected}}])

    def test_parent_component_cannot_escape_metadata(self):
        with self.assertRaisesRegex(ValueError, 'Noncanonical wheel path'):
            self.check_wheel({'sample-0.1.0.dist-info/../unowned.py': b''}, {})

    def test_foreign_metadata_is_payload(self):
        with self.assertRaisesRegex(ValueError, 'differs from ownership manifest'):
            self.check_wheel({'foreign.dist-info/unowned.py': b''}, {})

    def test_data_relocation_is_explicitly_rejected(self):
        path = 'sample-0.1.0.data/purelib/unowned.py'
        with self.assertRaisesRegex(ValueError, 'Unsupported wheel relocation'):
            self.check_wheel({path: b''}, {path: {}})

    def test_noncanonical_spellings_are_rejected(self):
        for path in ('./unowned.py', '/unowned.py', 'a//unowned.py', 'a/./unowned.py', 'a\\unowned.py', 'C:/unowned.py'):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, 'Noncanonical wheel path'):
                self.check_wheel({path: b''}, {path: {}})

    def test_metadata_directory_must_match_filename(self):
        with self.assertRaisesRegex(ValueError, 'Wheel metadata directory'):
            self.check_wheel({}, {}, metadata='other-0.1.0.dist-info')

    def test_canonical_wheel_passes(self):
        self.assertEqual(self.check_wheel({}, {}), [{'owned.py': b'OWNED = True\n'}])

    def test_undeclared_metadata_file_is_a_stray(self):
        with self.assertRaisesRegex(ValueError, 'metadata files differ.*top_level.txt'):
            self.check_wheel({'sample-0.1.0.dist-info/top_level.txt': b'owned\n'}, {})

    def test_undeclared_license_file_is_a_stray(self):
        with self.assertRaisesRegex(ValueError, 'metadata files differ.*licenses/LICENSE'):
            self.check_wheel({'sample-0.1.0.dist-info/licenses/LICENSE': b'text\n'}, {})


def _write_wheel(path, files):
    """Write `files` as a wheel with a fresh RECORD, in the wheelmaker's entry shape."""
    import base64
    import csv
    import hashlib
    import io
    import zipfile
    from python_distribution import ZIP_EPOCH

    record = next(name for name in files if name.endswith('.dist-info/RECORD'))
    rows = [[name, '', ''] if name == record else
            [name, 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode(), str(len(data))]
            for name, data in files.items()]
    text = io.StringIO()
    csv.writer(text, lineterminator='\n').writerows(rows)
    files = dict(files, **{record: text.getvalue().encode()})
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            info = zipfile.ZipInfo(name, date_time=ZIP_EPOCH)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100777 << 16
            archive.writestr(info, data)
    return path


def _sample_wheel(directory, name='sample', version='0.1.0', payload=None, metadata_extra=''):
    """A wheelmaker-shaped wheel: payload, WHEEL, METADATA, RECORD."""
    prefix = f'{name}-{version}.dist-info/'
    files = dict(payload or {'owned.py': b'OWNED = True\n'})
    files[prefix + 'WHEEL'] = b'Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
    files[prefix + 'METADATA'] = (f'Metadata-Version: 2.1\nName: {name}\nRequires-Python: >=3.10\n'
                                  f'{metadata_extra}Version: {version}\n\nUNKNOWN\n').encode()
    files[prefix + 'RECORD'] = b''
    return _write_wheel(Path(directory) / f'{name}-{version}-py3-none-any.whl', files)


class LicenseFilesTest(unittest.TestCase):
    """PEP 639 license files ride the metadata channel, never the payload."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = _sample_wheel(self.root / 'in')
        self.texts = {'LICENSE': b'Apache text\n', 'NOTICE': b'Notice text\n',
                      'THIRD_PARTY_LICENSES/upstream.LICENSE': b'MIT text\n'}
        self.files = []
        for name, data in self.texts.items():
            path = self.root / 'src' / name.replace('/', '_')
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            self.files.append((name, path))
        self.manifest = {'files': {'owned.py': {}}}

    def finalize(self, files=None, expression='Apache-2.0', source=None):
        from python_distribution import license_files

        output = self.root / 'out' / self.source.name
        output.parent.mkdir(exist_ok=True)
        license_files(source or self.source, output, self.files if files is None else files, expression)
        return output

    def read(self, wheel):
        import zipfile

        with zipfile.ZipFile(wheel) as archive:
            infos = archive.infolist()
            return [info.filename for info in infos], {info.filename: archive.read(info) for info in infos}, infos

    def test_license_files_land_in_dist_info_licenses_with_metadata(self):
        from python_distribution import verify_wheels

        output = self.finalize()
        names, contents, infos = self.read(output)
        prefix = 'sample-0.1.0.dist-info/'
        for name, data in self.texts.items():
            self.assertEqual(contents[prefix + 'licenses/' + name], data)
        metadata = contents[prefix + 'METADATA'].decode()
        headers, body = metadata.split('\n\n', 1)
        self.assertEqual(headers.splitlines()[0], 'Metadata-Version: 2.4')
        self.assertIn('License-Expression: Apache-2.0', headers.splitlines())
        self.assertEqual([line[len('License-File: '):] for line in headers.splitlines() if line.startswith('License-File: ')],
                         sorted(self.texts))
        self.assertEqual(body, 'UNKNOWN\n')
        self.assertNotIn('License:', headers)
        # Payload is untouched and still the only thing ownership sees.
        self.assertEqual(contents['owned.py'], b'OWNED = True\n')
        self.assertEqual(verify_wheels([output], [self.manifest]), [{'owned.py': b'OWNED = True\n'}])
        # Entry order: the source order, then the license files, then RECORD last.
        source_names, _, _ = self.read(self.source)
        self.assertEqual(names, source_names[:-1] + [prefix + 'licenses/' + name for name in sorted(self.texts)] + [prefix + 'RECORD'])
        for info in infos:
            self.assertEqual(info.date_time, (1980, 1, 1, 0, 0, 0), info.filename)
            self.assertEqual(info.external_attr >> 16, 0o100777, info.filename)

    def test_output_is_deterministic(self):
        first = self.finalize().read_bytes()
        second = self.finalize().read_bytes()
        self.assertEqual(first, second)

    def test_noncanonical_and_duplicate_names_are_rejected(self):
        for name in ('', '/LICENSE', '../LICENSE', 'a/./LICENSE', 'a\\LICENSE', 'LICENSE/'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'license file name'):
                self.finalize([(name, self.files[0][1])])
        with self.assertRaisesRegex(ValueError, 'Duplicate license file'):
            self.finalize([('LICENSE', self.files[0][1]), ('LICENSE', self.files[1][1])])

    def test_wheel_that_already_carries_license_metadata_is_rejected(self):
        source = _sample_wheel(self.root / 'pre', metadata_extra='License: MIT\n')
        with self.assertRaisesRegex(ValueError, 'already declares'):
            self.finalize(source=source)
        source = _sample_wheel(self.root / 'pre2', payload={'owned.py': b'', 'sample-0.1.0.dist-info/licenses/X': b'x'})
        with self.assertRaisesRegex(ValueError, 'already carries'):
            self.finalize(source=source)

    def test_verifier_requires_declared_and_present_to_match(self):
        from python_distribution import verify_wheels

        output = self.finalize()
        prefix = 'sample-0.1.0.dist-info/'
        _, contents, _ = self.read(output)

        def rewrite(mutate, label):
            files = dict(contents)
            mutate(files)
            return _write_wheel(self.root / label / output.name, files)

        # A declared license file that is not shipped.
        missing = rewrite(lambda f: f.pop(prefix + 'licenses/NOTICE'), 'missing')
        with self.assertRaisesRegex(ValueError, 'metadata files differ.*missing.*licenses/NOTICE'):
            verify_wheels([missing], [self.manifest])
        # A shipped file nobody declared.
        stray = rewrite(lambda f: f.__setitem__(prefix + 'licenses/EXTRA', b'x'), 'stray')
        with self.assertRaisesRegex(ValueError, 'metadata files differ.*unexpected.*licenses/EXTRA'):
            verify_wheels([stray], [self.manifest])
        # A declaration that escapes the licenses directory.
        def escape(f):
            f[prefix + 'METADATA'] = f[prefix + 'METADATA'].replace(b'License-File: NOTICE', b'License-File: ../NOTICE')
        with self.assertRaisesRegex(ValueError, 'License-File'):
            verify_wheels([rewrite(escape, 'escape')], [self.manifest])


class NoticesTest(unittest.TestCase):
    def test_notices_list_upstream_derived_modules_and_embed_the_license(self):
        from python_distribution import render_notices

        manifest = {'module': 'aorta', 'files': {
            'foxglove/Pose.py': {'schema': '@@//message/schemas/topic/foxglove_schema:Pose.fbs', 'module': 'aorta'},
            'Pose_schema_meta.py': {'schema': '@@//message/schemas/topic/foxglove_schema:Pose.fbs', 'module': 'aorta'},
            'aorta/sys/AortaHeader.py': {'schema': '@@//message/schemas/system:aorta_base.fbs', 'module': 'aorta'}}}
        text = render_notices(manifest, 'aorta_msgs', 'MIT License\n\nCopyright (c) Upstream\n')
        self.assertIn('aorta_msgs', text)
        self.assertIn('message/schemas/topic/foxglove_schema/Pose.fbs', text)
        self.assertIn('- foxglove/Pose.py', text)
        self.assertIn('- Pose_schema_meta.py', text)
        self.assertNotIn('AortaHeader', text)
        self.assertTrue(text.endswith('MIT License\n\nCopyright (c) Upstream\n'))

    def test_notices_fail_closed_without_derived_files(self):
        from python_distribution import render_notices

        manifest = {'module': 'aorta', 'files': {'aorta/sys/AortaHeader.py': {'schema': '@@//message/schemas/system:aorta_base.fbs', 'module': 'aorta'}}}
        with self.assertRaisesRegex(ValueError, 'no foxglove-derived'):
            render_notices(manifest, 'aorta_msgs', 'MIT\n')


if __name__ == '__main__':
    unittest.main()
