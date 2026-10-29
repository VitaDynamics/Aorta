"""Actual SDK/base/product/custom wheels: RECORD coverage, license files and offline orders."""
from email import message_from_string
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
import zipfile

from python_distribution import license_file_names, verify_wheels


def _license_files(wheel):
    """{name: bytes} of the license files a wheel declares, taken from METADATA."""
    with zipfile.ZipFile(wheel) as archive:
        prefix = next(p for p in archive.namelist() if p.endswith('.dist-info/METADATA'))[:-len('METADATA')]
        metadata = archive.read(prefix + 'METADATA').decode()
        files = {name: archive.read(prefix + 'licenses/' + name) for name in license_file_names(metadata)}
    return metadata, files


def _wheel_path(key):
    # rootpath quotes the {BUILD_EMBED_LABEL} placeholder in the wheel name.
    return Path(shlex.split(os.environ[key])[0])


INSTALLED_SMOKE = """
import importlib, json, os, sys, sysconfig
from pathlib import Path
import flatbuffers
os.environ.pop('AORTA_CORE_FFI_LIB', None)
for name in json.loads(sys.argv[1]):
    importlib.import_module(name)
from aorta._ffi.loader import load_library
from release.fixture import Command, ReleaseLeaf, ReleaseReading
from release.fixture.commands_fastpath import CommandVector
from release.fixture.release_reading_fastpath import read_leaves
from aorta.capture.custom.custom_fastpath import CustomCommandVector
from aorta_msgs_runtime._struct_vector import StructVectorWriter
assert flatbuffers.__version__ == '25.12.19'
assert isinstance(CustomCommandVector, StructVectorWriter)
rows = [(False, 0.0), (True, 1.25), (0.5, -2.0)]
fast = flatbuffers.Builder(1)
fast.Finish(CommandVector.create(fast, rows))
reference = flatbuffers.Builder(1)
reference.StartVector(8, len(rows), 4)
for enabled, value in reversed(rows):
    Command.CreateCommand(reference, enabled, value)
reference.Finish(reference.EndVector())
assert fast.Output() == reference.Output()
builder = flatbuffers.Builder(1)
ReleaseLeaf.Start(builder)
ReleaseLeaf.AddValue(builder, 37)
leaf = ReleaseLeaf.End(builder)
ReleaseReading.StartLeavesVector(builder, 1)
builder.PrependUOffsetTRelative(leaf)
leaves = builder.EndVector()
ReleaseReading.Start(builder)
ReleaseReading.AddLeaves(builder, leaves)
builder.Finish(ReleaseReading.End(builder))
view = ReleaseReading.ReleaseReading.GetRootAs(builder.Output(), 0)
assert read_leaves(view, ['value']) == [(view.Leaves(0).Value(),)] == [(37,)]
assert Path(load_library()._name).resolve().is_relative_to(Path(sysconfig.get_path('purelib')).resolve())
"""


class CompositionTest(unittest.TestCase):
    def assert_license_files(self, wheel, expected):
        """The wheel declares exactly `expected` (name -> source path) with identical bytes."""
        metadata, files = _license_files(wheel)
        headers = metadata.split('\n\n', 1)[0].splitlines()
        self.assertEqual(headers[0], 'Metadata-Version: 2.4', wheel)
        self.assertIn('License-Expression: Apache-2.0', headers, wheel)
        self.assertFalse([line for line in headers if line.startswith('License:')], wheel)
        self.assertEqual(sorted(files), sorted(expected), wheel)
        for name, source in expected.items():
            self.assertEqual(files[name], Path(os.environ[source]).read_bytes(), f'{wheel}: {name}')
        return files

    def test_license_files_ride_the_metadata_channel(self):
        sdk, base = _wheel_path('SDK_WHEEL'), _wheel_path('BASE_WHEEL')
        self.assert_license_files(sdk, {'LICENSE': 'LICENSE_TEXT', 'NOTICE': 'SDK_NOTICE',
                                        'THIRD_PARTY_NOTICES.md': 'SDK_THIRD_PARTY_NOTICES'})
        files = self.assert_license_files(base, {
            'LICENSE': 'LICENSE_TEXT', 'NOTICE': 'BASE_NOTICE', 'THIRD_PARTY_NOTICES.md': 'BASE_THIRD_PARTY_NOTICES',
            'THIRD_PARTY_LICENSES/foxglove-sdk.LICENSE': 'FOXGLOVE_LICENSE'})
        # The base notice is derived from the ownership manifest: every module a
        # foxglove-derived schema owns is named, and the upstream text is embedded.
        manifest = json.loads(Path(os.environ['BASE_MANIFEST']).read_text())
        derived = sorted(p for p, o in manifest['files'].items() if 'foxglove_schema' in o.get('schema', ''))
        self.assertTrue(derived)
        notices = files['THIRD_PARTY_NOTICES.md'].decode()
        for install in derived:
            self.assertIn(f'- {install}\n', notices)
        self.assertTrue(notices.endswith(files['THIRD_PARTY_LICENSES/foxglove-sdk.LICENSE'].decode()))
        # Product and custom wheels are private distributions and declare no license files.
        for key in ('PRODUCT_WHEEL', 'CUSTOM_WHEEL'):
            self.assertEqual(_license_files(_wheel_path(key))[1], {}, key)
        if os.environ.get('RELEASE_NOTICES'):
            # The published SDK wheel: cargo-about notices restricted to the crates
            # linked into libaorta_core, with the bundled Zstandard C library's BSD
            # notice, and without the build-only header generator.
            sdk_notices = _license_files(sdk)[1]['THIRD_PARTY_NOTICES.md'].decode()
            self.assertIn('| zenoh |', sdk_notices)
            self.assertIn('| zstd-sys |', sdk_notices)
            self.assertIn('Meta Platforms', sdk_notices)
            self.assertNotIn('cbindgen', sdk_notices)

    def test_exact_flatbuffers_requirement_in_finished_metadata(self):
        for key in ('BASE_WHEEL', 'PRODUCT_WHEEL', 'CUSTOM_WHEEL'):
            metadata, _ = _license_files(_wheel_path(key))
            requirements = message_from_string(metadata).get_all('Requires-Dist', [])
            self.assertEqual([r.replace(' ', '') for r in requirements if r.startswith('flatbuffers')],
                             ['flatbuffers==25.12.19'], key)

    def test_composed_wheels(self):
        wheels = [_wheel_path(key).resolve() for key in ('SDK_WHEEL', 'BASE_WHEEL', 'PRODUCT_WHEEL', 'CUSTOM_WHEEL')]
        normalized = tempfile.TemporaryDirectory()
        self.addCleanup(normalized.cleanup)
        # Resolve Bazel stamping placeholders before verification and pip installation.
        installable = []
        for wheel in wheels:
            with zipfile.ZipFile(wheel) as archive:
                meta = next(p.split('/')[0][:-10] for p in archive.namelist() if p.endswith('.dist-info/METADATA'))
                tags = archive.read(next(p for p in archive.namelist() if p.endswith('.dist-info/WHEEL'))).decode()
            tag = next(line[5:] for line in tags.splitlines() if line.startswith('Tag: '))
            destination = Path(normalized.name) / (meta + '-' + tag + '.whl')
            destination.write_bytes(wheel.read_bytes())
            installable.append(destination)
        wheels = installable
        versions = [message_from_string(_license_files(wheel)[0])['Version'] for wheel in wheels]
        manifests = [json.loads(Path(os.environ[key]).read_text()) for key in ('SDK_MANIFEST', 'BASE_MANIFEST', 'PRODUCT_MANIFEST', 'CUSTOM_MANIFEST')]
        payloads = verify_wheels(wheels, manifests)
        sdk, base, product, custom = payloads
        reserved = {'aorta/__init__.py', 'aorta/action/__init__.py', 'aorta/capture/__init__.py'}
        self.assertTrue(reserved <= sdk.keys())
        self.assertTrue(all(not reserved & p.keys() for p in (base, product, custom)))
        self.assertIn('aorta/sys/AortaHeader.py', base)
        self.assertIn('release/fixture/ReleaseReading.py', product)
        self.assertIn('aorta/capture/custom/CustomFrame.py', custom)
        self.assertNotIn('release/fixture/__init__.py', product)
        self.assertNotIn('aorta/sys/__init__.py', base)
        runtime = {p for p in base if p.startswith('aorta_msgs_runtime/')}
        self.assertEqual(runtime, {'aorta_msgs_runtime/__init__.py', 'aorta_msgs_runtime/_table_vector.py',
                                   'aorta_msgs_runtime/_struct_vector.py'})
        for path in runtime:
            self.assertEqual(manifests[1]['files'][path], {'module': 'aorta', 'kind': 'runtime'})
        self.assertTrue(any(p.endswith('_fastpath.py') for p in base))
        self.assertIn('release/fixture/commands_fastpath.py', product)
        self.assertIn('release/fixture/release_reading_fastpath.py', product)
        self.assertIn('aorta/capture/custom/custom_fastpath.py', custom)
        self.assertNotIn('release/fixture/commands_fastpath.py', custom)
        self.assertFalse(any(p.endswith('commands_schema_meta.py') for p in product))
        registry = json.loads(Path(os.environ['REGISTRY']).read_text())
        intended_schemas = {e['fbs_label'] for e in registry['entries'] if e['origin_module'] == 'aorta' and 'python' in e['output_labels']}
        self.assertEqual(set(manifests[1]['schemas']), intended_schemas)
        intended_files = {p for p, o in manifests[2]['composition_files'].items() if o['module'] == 'aorta'}
        self.assertEqual(set(base), intended_files)
        for manifest in manifests[1:]:
            self.assertEqual({o['schema'] for o in manifest['files'].values() if 'schema' in o}, set(manifest['schemas']))
        for wheel in wheels[2:]:
            with zipfile.ZipFile(wheel) as archive:
                metadata = archive.read(next(p for p in archive.namelist() if p.endswith('.dist-info/METADATA'))).decode()
            requirements = [r.replace(' ', '') for r in message_from_string(metadata).get_all('Requires-Dist', [])]
            self.assertIn(f'aorta-msgs=={versions[1]}', requirements)
            self.assertIn(f'aorta-sdk=={versions[0]}', requirements)
            self.assertIn('Requires-Dist: flatbuffers', metadata)
        # Reproduce both destructive overlaps using actual wheel payloads.
        import base64
        import csv
        import hashlib
        import io
        with tempfile.TemporaryDirectory() as tmp:
            for source, path in ((base, 'aorta/sys/AortaHeader.py'), (sdk, 'aorta/capture/__init__.py')):
                changed = Path(tmp) / wheels[2].name
                with zipfile.ZipFile(wheels[2]) as original:
                    files = {name: original.read(name) for name in original.namelist()}
                record = next(name for name in files if name.endswith('.dist-info/RECORD'))
                rows = list(csv.reader(io.StringIO(files[record].decode())))
                data = source[path]
                files[path] = data
                digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
                rows.append([path, 'sha256=' + digest, str(len(data))])
                text = io.StringIO()
                csv.writer(text, lineterminator='\n').writerows(rows)
                files[record] = text.getvalue().encode()
                with zipfile.ZipFile(changed, 'w') as archive:
                    for name, data in files.items():
                        archive.writestr(name, data)
                with self.assertRaisesRegex(ValueError, 'Wheel ownership collision'):
                    verify_wheels([wheels[0], wheels[1], changed], manifests[:3])
        missing = dict(manifests[1], files=dict(manifests[1]['files']))
        missing['files'].pop(next(iter(missing['files'])))
        with self.assertRaisesRegex(ValueError, 'differs from ownership manifest'):
            verify_wheels([wheels[1]], [missing])
        dependency = _wheel_path('FLATBUFFERS_WHEEL').resolve()
        (Path(normalized.name) / dependency.name).write_bytes(dependency.read_bytes())
        pip_wheel = _wheel_path('PIP_WHEEL').resolve()
        launch_pip = 'import runpy,sys; sys.path.insert(0,sys.argv[1]); sys.argv=["pip"]+sys.argv[2:]; runpy.run_module("pip",run_name="__main__")'
        # The third environment resolves by distribution names from an empty site-packages.
        fresh = [f'aorta-sdk=={versions[0]}', f'aorta-msgs=={versions[1]}',
                 f'vita-msgs=={versions[2]}', f'custom-msgs=={versions[3]}']
        for order in (wheels, list(reversed(wheels)), fresh):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                subprocess.run([sys.executable, '-m', 'venv', '--without-pip', str(root / 'venv')], check=True, capture_output=True)
                python = root / 'venv/bin/python'
                pip = [str(python), '-I', '-c', launch_pip, str(pip_wheel), '--isolated']
                batches = [order] if order is fresh else [[wheel] for wheel in order]
                for batch in batches:
                    subprocess.run(pip + ['install', '--no-index', '--find-links', normalized.name, '--no-compile'] +
                                   [str(wheel) for wheel in batch], check=True)
                subprocess.run(pip + ['check'], check=True)
                companions = [p[:-3].replace('/', '.') for payload in payloads for p in payload if p.endswith('_fastpath.py')]
                subprocess.run([str(python), '-I', '-c', INSTALLED_SMOKE, json.dumps(companions)], check=True)
                site = Path(subprocess.check_output([str(python), '-c', 'import sysconfig; print(sysconfig.get_path("purelib"))'], text=True).strip())
                for payload in payloads:
                    for path, data in payload.items():
                        self.assertEqual((site / path).read_bytes(), data, path)
                for wheel in wheels:
                    _, files = _license_files(wheel)
                    info = next(p for p in site.glob(wheel.name.split('-')[0] + '-*.dist-info'))
                    for name, data in files.items():
                        self.assertEqual((info / 'licenses' / name).read_bytes(), data, f'{wheel.name}: {name}')
                print('Offline install order passed: ' + ', '.join(str(w) for w in order), flush=True)


if __name__ == '__main__':
    unittest.main()
