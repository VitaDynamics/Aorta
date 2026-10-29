"""Derive install ownership from direct flatc outputs and verify emitted copies.

Also finalizes a wheel's legal files: PEP 639 places them under
`<dist>-<ver>.dist-info/licenses/` and lists each one as a `License-File`
metadata entry, so they ride the metadata channel — installed with the
distribution, covered by RECORD, and never confused with schema-owned payload.
"""
import json
from pathlib import Path
import sys

SDK_INITIALIZERS = {'aorta/__init__.py', 'aorta/action/__init__.py', 'aorta/capture/__init__.py'}
# The wheelmaker's fixed entry timestamp and mode (-rwxrwxrwx regular file).
ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)
ZIP_MODE = 0o100777 << 16
METADATA_FILES = ('METADATA', 'WHEEL', 'RECORD')
LICENSE_DIR = 'licenses/'
# Schemas under this message-relative directory are the foxglove-sdk derived
# ones (MIT); the wheel's third-party notice lists the modules generated from
# them.
FOXGLOVE_SCHEMA_PACKAGE = 'message/schemas/topic/foxglove_schema'


def assemble(rows, registry, module, output, manifest, runtime=()):
    enabled = {e['fbs_label']: e for e in registry['entries'] if 'python' in e['output_labels']}
    if {row['schema'] for row in rows} != set(enabled):
        raise ValueError('Python plan must cover exactly the composed Python-enabled registry')
    owners = {}
    contents = {}
    for row in rows:
        schema = row['schema']
        if row['module'] != enabled[schema]['origin_module']:
            raise ValueError('Python module owner mismatch: ' + schema)
        files = [(p.relative_to(row['direct']).as_posix(), p) for p in Path(row['direct']).rglob('*.py')]
        if 'fastpath' in row:
            files.extend((p.relative_to(row['fastpath']).as_posix(), p) for p in Path(row['fastpath']).rglob('*.py'))
        if 'meta' in row:
            files.append((row['meta']['install'], Path(row['meta']['path'])))
        for install, source in files:
            if install.endswith('/__init__.py') or install == '__init__.py':
                continue
            owner = {'schema': schema, 'module': row['module']}
            if install in owners and owners[install] != owner:
                raise ValueError(f"Python install collision {install}: {owners[install]} and {owner}")
            data = source.read_bytes()
            if install in contents and contents[install] != data:
                raise ValueError(f'Python same-owner bytes differ: {install} ({schema})')
            owners[install] = owner
            contents[install] = data
    for file in runtime:
        install = file['install']
        owner = {'module': 'aorta', 'kind': 'runtime'}
        if install in owners:
            raise ValueError(f"Python install collision {install}: {owners[install]} and {owner}")
        owners[install] = owner
        contents[install] = Path(file['path']).read_bytes()
    # All generated ancestor initializers are omitted: future compositions can introduce
    # another owner below any ancestor without rebuilding an installed base wheel.
    for row in rows:
        for tree in row['trees']:
            for source in Path(tree).rglob('*.py'):
                install = source.relative_to(tree).as_posix()
                if source.name == '__init__.py':
                    continue
                if install not in owners:
                    raise ValueError(f'Python emitted file has no direct owner: {install} ({row["schema"]})')
                if source.read_bytes() != contents[install]:
                    raise ValueError(f'Python same-owner bytes differ: {install} ({owners[install]})')
    if module not in {e['origin_module'] for e in registry['entries']}:
        raise ValueError('Python module absent from composition: ' + module)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    selected = {p: owner for p, owner in owners.items() if owner['module'] == module}
    for install in selected:
        target = output / install
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents[install])
    Path(manifest).write_text(json.dumps({'version': 1, 'module': module, 'files': selected,
        'composition_files': owners, 'schemas': sorted(k for k, e in enabled.items() if e['origin_module'] == module),
        'sdk_initializers': sorted(SDK_INITIALIZERS)}, indent=2, sort_keys=True) + '\n')


def verify_wheels(wheels, manifests):
    """Check final RECORD ownership and complete intended payload coverage."""
    import base64
    import csv
    import hashlib
    import io
    import zipfile

    if len(wheels) != len(manifests):
        raise ValueError("Every wheel requires an ownership manifest")
    # Canonical paths with no relocation map directly to site-packages paths.
    installed = {}
    payloads = []
    for wheel, manifest in zip(wheels, manifests):
        with zipfile.ZipFile(wheel) as archive:
            names = set(archive.namelist())
            if len(names) != len(archive.namelist()):
                raise ValueError(f'Duplicate wheel archive path: {wheel}')
            for path in names:
                if "\\" in path or ":" in path or any(part in ('', '.', '..') for part in path.split('/')):
                    raise ValueError(f'Noncanonical wheel path: {path}')
                if path.split('/')[0].endswith('.data'):
                    raise ValueError(f'Unsupported wheel relocation: {path}')
            filename = Path(wheel).name
            parts = filename[:-4].split('-')
            if not filename.endswith('.whl') or len(parts) not in (5, 6) or not all(parts):
                raise ValueError(f'Invalid wheel filename: {filename}')
            metadata = f'{parts[0]}-{parts[1]}.dist-info/'
            record = metadata + 'RECORD'
            if not {record, metadata + 'METADATA', metadata + 'WHEEL'} <= names:
                raise ValueError(f'Wheel metadata directory does not match filename: {filename}')
            rows = list(csv.reader(io.StringIO(archive.read(record).decode())))
            paths = {row[0] for row in rows}
            if len(paths) != len(rows) or paths != names:
                raise ValueError(f'RECORD does not match wheel files: {wheel}')
            # The metadata directory holds exactly the three standard files plus
            # the license files METADATA declares: nothing undeclared rides along,
            # and every declaration is backed by a file under licenses/.
            declared = license_file_names(archive.read(metadata + 'METADATA').decode())
            expected = {metadata + name for name in METADATA_FILES} | {metadata + LICENSE_DIR + name for name in declared}
            present = {path for path in names if path.startswith(metadata)}
            if present != expected:
                raise ValueError(f'Wheel metadata files differ from the declared license files: {wheel}: '
                                 f'unexpected={sorted(present - expected)}, missing={sorted(expected - present)}')
            for path, digest, size in rows:
                if path in installed:
                    raise ValueError(f'Wheel ownership collision {path}: {installed[path]} and {wheel}')
                installed[path] = str(wheel)
                if path != record:
                    data = archive.read(path)
                    expected = 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
                    if digest != expected or size != str(len(data)):
                        raise ValueError(f'Invalid RECORD digest/size: {path}')
            payload = {p: archive.read(p) for p in paths if not p.startswith(metadata)}
            if manifest is not None and set(payload) != set(manifest['files']):
                raise ValueError(f'Wheel payload differs from ownership manifest: {wheel}')
            payloads.append(payload)
    # Flat SDK modules cannot host generated child packages, even without an
    # identical install path. Reject that shadowing at composition time too.
    for path in installed:
        if path.endswith('.py') and not path.endswith('/__init__.py'):
            prefix = path[:-3] + '/'
            if any(other.startswith(prefix) for other in installed):
                raise ValueError(f'Python module shadows package: {path}')
    return payloads


def _check_license_name(name):
    parts = name.split('/')
    if not name or '\\' in name or ':' in name or any(part in ('', '.', '..') for part in parts):
        raise ValueError(f'Noncanonical license file name: {name!r}')


def license_file_names(metadata):
    """The `License-File` entries of a METADATA text, validated as licenses/-relative paths."""
    headers = metadata.split('\n\n', 1)[0]
    names = [line[len('License-File: '):] for line in headers.splitlines() if line.startswith('License-File: ')]
    for name in names:
        try:
            _check_license_name(name)
        except ValueError as error:
            raise ValueError(f'Invalid License-File entry {name!r}') from error
    if len(set(names)) != len(names):
        raise ValueError(f'Duplicate License-File entries: {names}')
    return names


def license_files(source, output, files, expression):
    """Write `output` = `source` wheel plus PEP 639 license files.

    `files` pairs each licenses/-relative name with the file to ship. METADATA
    gains `License-Expression` and one `License-File` per name (Metadata 2.4);
    RECORD is regenerated; every other entry is copied byte-for-byte in the
    source order, with the wheelmaker's fixed timestamp and mode.
    """
    import base64
    import csv
    import hashlib
    import io
    import zipfile

    names = [name for name, _ in files]
    for name in names:
        try:
            _check_license_name(name)
        except ValueError as error:
            raise ValueError(f'Invalid license file name {name!r}') from error
    if len(set(names)) != len(names):
        raise ValueError(f'Duplicate license file name: {names}')
    if not expression:
        raise ValueError('A license expression is required')
    with zipfile.ZipFile(source) as archive:
        infos = archive.infolist()
        entries = {info.filename: archive.read(info) for info in infos}
    metadata_paths = [name for name in entries if name.endswith('.dist-info/METADATA') and name.count('/') == 1]
    if len(metadata_paths) != 1:
        raise ValueError(f'Wheel must carry exactly one dist-info METADATA: {source}')
    prefix = metadata_paths[0][:-len('METADATA')]
    record = prefix + 'RECORD'
    if not {prefix + name for name in METADATA_FILES} <= set(entries):
        raise ValueError(f'Wheel metadata directory is incomplete: {source}')
    if any(name.startswith(prefix + LICENSE_DIR) for name in entries):
        raise ValueError(f'Wheel already carries license files: {source}')
    metadata = entries[prefix + 'METADATA'].decode()
    headers, body = metadata.split('\n\n', 1)
    lines = headers.split('\n')
    if any(line.split(':', 1)[0] in ('License', 'License-Expression', 'License-File') for line in lines):
        raise ValueError(f'Wheel already declares license metadata: {source}')
    if not lines[0].startswith('Metadata-Version: '):
        raise ValueError(f'Wheel METADATA does not start with Metadata-Version: {source}')
    lines[0] = 'Metadata-Version: 2.4'
    lines.append('License-Expression: ' + expression)
    lines.extend('License-File: ' + name for name in sorted(names))
    entries[prefix + 'METADATA'] = ('\n'.join(lines) + '\n\n' + body).encode()
    licenses = {prefix + LICENSE_DIR + name: Path(path).read_bytes() for name, path in files}

    def digest(data):
        return 'sha256=' + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()

    rows = []
    for row in csv.reader(io.StringIO(entries[record].decode())):
        if row[0] == record:
            continue
        data = entries[row[0]]
        rows.append([row[0], digest(data), str(len(data))])
    rows.extend([name, digest(data), str(len(data))] for name, data in sorted(licenses.items()))
    rows.append([record, '', ''])
    text = io.StringIO()
    csv.writer(text, lineterminator='\n').writerows(rows)
    entries[record] = text.getvalue().encode()

    def entry(name, template=None):
        info = zipfile.ZipInfo(name, date_time=template.date_time if template else ZIP_EPOCH)
        info.compress_type = template.compress_type if template else zipfile.ZIP_DEFLATED
        info.external_attr = template.external_attr if template else ZIP_MODE
        info.create_system = template.create_system if template else 3
        return info

    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for info in infos:
            if info.filename != record:
                archive.writestr(entry(info.filename, info), entries[info.filename])
        for name, data in sorted(licenses.items()):
            archive.writestr(entry(name), data)
        archive.writestr(entry(record, next(info for info in infos if info.filename == record)), entries[record])


def render_notices(manifest, distribution, upstream_license):
    """Third-party notices for a generated-schema wheel, from its ownership manifest.

    The only third-party material in such a wheel is the code generated from
    the foxglove-sdk derived schemas (MIT), so the notice names exactly the
    installed modules those schemas own and reproduces the upstream license.
    """
    derived = {install: owner['schema'] for install, owner in manifest['files'].items()
               if owner.get('schema', '').split('//', 1)[-1].split(':', 1)[0] == FOXGLOVE_SCHEMA_PACKAGE}
    if not derived:
        raise ValueError(f'{distribution}: no foxglove-derived files in the ownership manifest')
    schemas = sorted({FOXGLOVE_SCHEMA_PACKAGE + '/' + schema.rsplit(':', 1)[1] for schema in derived.values()})
    lines = ['# Third-party notices', '',
             f'Generated for the `{distribution}` distribution from its ownership manifest.',
             'The `flatbuffers` runtime is a declared dependency (Requires-Dist), not part',
             'of this distribution.', '',
             '## Foxglove-derived schemas — MIT', '',
             'The modules below are generated from FlatBuffers schemas converted or adapted',
             'from the foxglove-sdk (https://github.com/foxglove/foxglove-sdk,',
             '`schemas/flatbuffer`), Copyright (c) Foxglove Technologies Inc, MIT License.',
             'The verbatim upstream license text follows and is also shipped beside this',
             'file as `THIRD_PARTY_LICENSES/foxglove-sdk.LICENSE`.', '',
             'Schemas:', *[f'- {schema}' for schema in schemas], '',
             'Generated modules:', *[f'- {install}' for install in sorted(derived)], '',
             '### License text', '']
    return '\n'.join(lines) + '\n' + upstream_license


if __name__ == '__main__':
    if sys.argv[1] == 'licenses':
        source, output, expression, *pairs = sys.argv[2:]
        if not pairs or any('=' not in pair for pair in pairs):
            raise ValueError('licenses requires NAME=PATH pairs')
        license_files(source, output, [tuple(pair.split('=', 1)) for pair in pairs], expression)
    elif sys.argv[1] == 'notices':
        manifest, distribution, upstream, output = sys.argv[2:]
        Path(output).write_text(render_notices(json.loads(Path(manifest).read_text()), distribution, Path(upstream).read_text()))
    elif sys.argv[1] == '--verify':
        paths = sys.argv[2:]
        if not paths or len(paths) % 2:
            raise ValueError('--verify requires wheel/ownership-manifest pairs')
        verify_wheels(paths[::2], [json.loads(Path(p).read_text()) for p in paths[1::2]])
    else:
        plan, registry, module, output, manifest = sys.argv[1:]
        plan = json.loads(Path(plan).read_text())
        assemble(plan['schemas'], json.loads(Path(registry).read_text()), module, output, manifest, plan['runtime'])
