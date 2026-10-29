import argparse
import importlib
import keyword
import math
from pathlib import Path
import sys

from schema_reflection import is_root_local, load_bfbs, python_scalar_facts

_SCALAR_FORMATS = {'bool': '?', 'byte': 'b', 'ubyte': 'B', 'short': 'h', 'ushort': 'H',
                   'int': 'i', 'uint': 'I', 'long': 'q', 'ulong': 'Q', 'float': 'f', 'double': 'd'}


def _identifier(name):
    return name.isidentifier() and not keyword.iskeyword(name)


def _literal(value):
    if isinstance(value, float) and not math.isfinite(value):
        return 'float({!r})'.format(str(value))
    return repr(value)


def generate(bfbs, getters, out, stem):
    schema = load_bfbs(bfbs.read_bytes())
    groups = {}
    writers = {}
    for name, obj in sorted(schema.structs.items()):
        if not is_root_local(obj.type_id, schema):
            continue
        if any(not (f.shape.startswith('scalar:') or f.shape.startswith('enum:'))
               for f in obj.fields):
            continue
        writers.setdefault(obj.type_id.namespace, []).append(obj)
        groups.setdefault(obj.type_id.namespace, [])
    for parent_name, parent in sorted(schema.tables.items()):
        if not is_root_local(parent.type_id, schema):
            continue
        for field in parent.fields:
            if (field.deprecated or field.offset64 or not field.shape.startswith('vector<table:')
                    or field.target is None):
                continue
            child = schema.tables[field.target.qualified_name]
            if any(f.offset64 for f in child.fields):
                continue
            scalars = [f for f in child.fields if not f.deprecated
                       and (f.shape.startswith('scalar:') or f.shape.startswith('enum:'))]
            if scalars:
                groups.setdefault(parent.type_id.namespace, []).append((parent_name, field, child, scalars))
    sys.path.insert(0, str(getters))
    try:
        outputs = {}
        for namespace, relations in sorted(groups.items()):
            names = {p for p, _, _, _ in relations} | {c.type_id.qualified_name for _, _, c, _ in relations}
            if not _identifier(stem) or any(not _identifier(part) for name in names for part in name.split('.')):
                return
            structs = writers.get(namespace, [])
            functions = (['read_' + f.name for _, f, _, _ in relations]
                         + [obj.type_id.name + 'Vector' for obj in structs])
            if any(not _identifier(n) for n in functions) or len(set(functions)) != len(functions):
                return
            lines = ['# Generated from BFBS and matching pinned flatc getters.',
                     'from aorta_msgs_runtime._table_vector import TableVectorReader',
                     'from aorta_msgs_runtime._struct_vector import StructVectorWriter']
            for obj in structs:
                if any(not _identifier(part) for part in obj.type_id.qualified_name.split('.')):
                    return
                lines += ['', '{}Vector = StructVectorWriter({}, {}, ('.format(
                    obj.type_id.name, obj.bytesize, obj.minalign)]
                for f in obj.fields:
                    shape = (schema.enums[f.target.qualified_name].base_type if f.shape.startswith('enum:')
                             else f.shape.removeprefix('scalar:'))
                    lines.append('    ({!r}, {}, {!r}),'.format(f.name, f.wire_offset, _SCALAR_FORMATS[shape]))
                lines.append('))')
            aliases = {name: '_type_' + str(i) for i, name in enumerate(sorted(names))}
            for name, alias in aliases.items():
                lines.append('from {} import {} as {}'.format(name, name.rsplit('.', 1)[-1], alias))
            readers = {}
            children = {child.type_id.qualified_name: (child, scalars) for _, _, child, scalars in relations}
            for name, (child, scalars) in sorted(children.items()):
                if any(not _identifier(f.name) for f in scalars):
                    return
                try:
                    table = getattr(importlib.import_module(name), child.type_id.name)
                    facts = python_scalar_facts(child, table)
                except (KeyError, AttributeError, ValueError, SyntaxError):
                    return
                reader = '_reader_' + str(len(readers))
                readers[name] = reader
                lines += ['', '{} = TableVectorReader({}, ('.format(reader, aliases[name])]
                for f in scalars:
                    shape = (schema.enums[f.target.qualified_name].base_type if f.shape.startswith('enum:')
                             else f.shape.removeprefix('scalar:'))
                    fact = facts[f.name]
                    lines.append('    ({!r}, {}, {!r}, {}, {}, {}.{}),'.format(
                        f.name, f.wire_offset, _SCALAR_FORMATS[shape], _literal(fact.default),
                        fact.matches_bfbs, aliases[name], fact.accessor))
                lines.append('))')
            for parent_name, field, child, _ in relations:
                lines += ['', '', 'def read_{}(root, fields=None):'.format(field.name),
                          '    if not isinstance(root, {}):'.format(aliases[parent_name]),
                          "        raise TypeError('expected {}')".format(parent_name),
                          '    return {}.rows(root, {}, fields)'.format(readers[child.type_id.qualified_name], field.wire_offset)]
            outputs[Path(*namespace.split('.')) / (stem + '_fastpath.py') if namespace else Path(stem + '_fastpath.py')] = '\n'.join(lines) + '\n'
        for path, source in outputs.items():
            destination = out / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(source)
    finally:
        sys.path.remove(str(getters))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for name in ('bfbs', 'getters', 'out'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--stem', required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    generate(args.bfbs, args.getters, args.out, args.stem)
