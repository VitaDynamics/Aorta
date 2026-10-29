"""Compile private source reflection using the existing staging and action tools."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile

from message.tools.gen_action_wrapper import GenerationError, generate_action_wrapper
from message.tools.schema_reflection import SchemaLoadError, is_root_local, load_bfbs_file


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    with tempfile.TemporaryDirectory(prefix='aorta_source_lint_') as folder:
        root = Path(config['source'])
        try:
            if config['action']:
                expanded = Path(folder) / root.name
                expanded.write_text(generate_action_wrapper(root))
                root = expanded
            proc = subprocess.run([
                config['stage'], str(Path(config['flatc']).resolve()), '--binary --schema --bfbs-builtins',
                folder, str(root), config['slot'], str(len(config['direct'])),
                *config['direct'], *config['closure'],
            ], capture_output=True, text=True)
            if proc.returncode:
                result = {'error': proc.stdout + proc.stderr}
            else:
                bfbs = Path(folder) / (root.stem + '.bfbs')
                loaded = load_bfbs_file(bfbs)
                result = {
                    'root_type': loaded.root_type.qualified_name if loaded.root_type else '',
                    'file_identifier': loaded.file_ident or '',
                    'tables': {name: {'namespace': obj.type_id.namespace, 'local': is_root_local(obj.type_id, loaded)} for name, obj in loaded.tables.items()},
                    'local_namespaces': sorted({obj.type_id.namespace for mapping in (loaded.tables, loaded.structs, loaded.enums, loaded.unions) for obj in mapping.values() if is_root_local(obj.type_id, loaded)}),
                }
                Path(sys.argv[3]).write_bytes(bfbs.read_bytes())
        except (GenerationError, SchemaLoadError) as error:
            result = {'error': str(error)}
        if 'error' in result:
            Path(sys.argv[3]).write_bytes(b'')
        Path(sys.argv[2]).write_text(json.dumps(result, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
