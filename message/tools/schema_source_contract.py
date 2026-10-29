"""Declared source semantics checked against text and independent reflection."""

import re

from message.tools.gen_action_wrapper import GenerationError, derive_action_prefix, parse_table_names
from message.tools.schema_include_utils import strip_fbs_comments
from message.tools.schema_lint import SchemaLintError

_FIELDS = {'schema_kind', 'root_type', 'namespace', 'file_identifier'}
_DECLARATIONS = re.compile(r'\b(namespace|root_type)\s+([A-Za-z0-9_.]+)\s*;')
_IDENTIFIER = re.compile(r'\bfile_identifier\s+"([^"\n]*)"\s*;|"(?:\\.|[^"\\])*"')


def check_source_contract(entry, text, reflection):
    contract = entry.get('source_contract')
    if contract is None:
        return

    def refuse(code, detail):
        raise SchemaLintError(f"AORTA_SOURCE_{code} {entry['fbs_label']}: {detail}")

    kind = contract.get('schema_kind')
    required = _FIELDS | ({'request_type', 'response_type'} if kind == 'service' else set())
    if contract.keys() != required or any(not isinstance(v, str) for v in contract.values()):
        refuse('CONTRACT', 'explicit scalar fields required: ' + ', '.join(sorted(required)))
    if kind not in ('message', 'service', 'action', 'include_only'):
        refuse('KIND', f'unknown schema_kind {kind!r}')
    flags = entry['flags']
    if bool(flags['action_schema']) != (kind == 'action') or bool(flags['bfbs']) != (kind != 'include_only'):
        refuse('KIND', 'kind disagrees with action_schema/bfbs')
    if kind != 'action' and entry.get('action_name') is not None:
        refuse('KIND', 'action_name is action-only')
    if kind == 'action' and entry.get('action_name') == '':
        refuse('ACTION', 'action_name must be nonempty when supplied')
    if kind == 'include_only' and any(flags.get(k, False) for k in ('rust_wrapper', 'cpp_wrapper', 'schema_meta_rust', 'schema_meta_cpp', 'schema_meta_py')):
        refuse('KIND', 'include_only cannot enable typed wrappers or metadata')
    path = entry['logical_include_path'].split('/')
    if len(path) > 2 and path[0] == 'schemas' and path[1] in ('topic', 'service', 'action'):
        expected_kind = {'topic': 'message', 'service': 'service', 'action': 'action'}[path[1]]
        if kind != expected_kind:
            refuse('KIND', 'kind disagrees with schema package domain')
        # example is the reviewed cross-domain fixture collection.
        if path[1] in ('service', 'action') and path[2] != 'example':
            domain = 'services' if path[1] == 'service' else 'action'
            if contract['namespace'] != f'aorta.{domain}.{path[2]}':
                refuse('NAMESPACE', 'namespace disagrees with schema package domain')

    scan = strip_fbs_comments(text)
    namespace = ''
    roots = []
    for token, value in _DECLARATIONS.findall(re.sub(r'"(?:\\.|[^"\\])*"', '""', scan)):
        if token == 'namespace':
            namespace = value
        else:
            roots.append(value if '.' in value or not namespace else namespace + '.' + value)
    expected_root = contract['root_type']
    if len(roots) > 1 or (roots[0] if roots else '') != expected_root:
        refuse('ROOT', f'declared root {expected_root!r} differs from source roots {roots!r}')
    if kind in ('message', 'service') and not expected_root:
        refuse('ROOT', 'typed schema requires a local table root')
    if kind == 'include_only' and expected_root:
        refuse('ROOT', 'include_only must have no root')
    if expected_root and '.' not in expected_root:
        refuse('ROOT', 'root_type must be fully qualified')
    identifiers = [m.group(1) for m in _IDENTIFIER.finditer(scan) if m.group(1) is not None]
    expected_ident = contract['file_identifier']
    if expected_ident and len(expected_ident.encode('utf-8')) != 4:
        refuse('IDENTIFIER', 'file_identifier must be absent or exactly four bytes')
    if len(identifiers) > 1 or (identifiers[0] if identifiers else '') != expected_ident:
        refuse('IDENTIFIER', 'file_identifier differs from source')

    family = []
    if kind == 'action':
        try:
            _, goal, result, feedback = derive_action_prefix(parse_table_names(scan))
        except GenerationError as error:
            refuse('ACTION', str(error))
        family = [contract['namespace'] + '.' + name for name in (goal, result, feedback)]
        if expected_root and expected_root != family[0]:
            refuse('ACTION', 'source action root must be its own Goal table')
    if expected_root and expected_root.rsplit('.', 1)[0] != contract['namespace']:
        refuse('NAMESPACE', 'root namespace differs from expectation')
    if 'error' in reflection:
        refuse('REFLECTION', reflection['error'])
    tables = reflection['tables']
    relevant = family if kind == 'action' else ([expected_root] if expected_root else [])
    if kind == 'service':
        relevant += [contract['request_type'], contract['response_type']]
    for name in relevant:
        if name not in tables or not tables[name]['local']:
            refuse('SERVICE' if kind == 'service' else 'ROOT', f'{name!r} must be a table declared in this source')
        if tables[name]['namespace'] != contract['namespace']:
            refuse('NAMESPACE', f'{name!r} has a different namespace')
    reflected_root = family[0] if kind == 'action' else expected_root
    if reflection['root_type'] != reflected_root:
        refuse('ROOT', 'independent reflection root differs')
    if reflection['file_identifier'] != expected_ident:
        refuse('IDENTIFIER', 'independent reflection identifier differs')
    if kind == 'include_only' and contract['namespace'] not in reflection['local_namespaces']:
        refuse('NAMESPACE', 'expected namespace has no local declaration')


def check_output_provenance(entry, outputs):
    source = entry['source_identity']['path']
    for role, records in outputs.items():
        if role == 'rust_facade' and not entry['flags']['rust_wrapper']:
            continue
        if not records and role not in ('cpp_lib', 'rust_facade'):
            raise SchemaLintError(f"AORTA_SOURCE_PROVENANCE {entry['fbs_label']}:{role}: no generated output")
        for record in records:
            if not record['generated'] or source not in record['sources'] or record['required_tool'] not in record['sources']:
                raise SchemaLintError(f"AORTA_SOURCE_PROVENANCE {entry['fbs_label']}:{role}: {record['path']} lacks the required source/tool generating-action ancestry")
