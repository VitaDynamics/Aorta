import struct
from functools import lru_cache

_U32 = struct.Struct('<I')
_I32 = struct.Struct('<i')
_U16 = struct.Struct('<H')
_VTABLE = struct.Struct('<HH')


def _bounds(start, size, length):
    if start < 0 or size < 0 or start + size > length:
        raise ValueError('FlatBuffer access outside buffer')


def _table(data, pos):
    length = len(data)
    _bounds(pos, 4, length)
    vt = pos - _I32.unpack_from(data, pos)[0]
    _bounds(vt, 4, length)
    size, object_size = _VTABLE.unpack_from(data, vt)
    if size < 4 or size % 2 or object_size < 4:
        raise ValueError('invalid FlatBuffer vtable')
    _bounds(vt, size, length)
    _bounds(pos, object_size, length)
    return vt, size, object_size


class TableVectorReader:
    def __init__(self, table_class, fields):
        self.table_class = table_class
        # Generated facts: name, slot, format, getter default, BFBS agreement, getter.
        self.fields = {name: (slot, fmt, default, matches, getter)
                       for name, slot, fmt, default, matches, getter in fields}
        self.names = tuple(self.fields)
        self.plan = lru_cache(maxsize=128)(self._plan)

    def _plan(self, signature, names):
        size, object_size = _VTABLE.unpack_from(signature)
        present, defaults = [], []
        fallback = False
        for i, name in enumerate(names):
            slot, fmt, default, matches, _ = self.fields[name]
            offset = _U16.unpack_from(signature, slot)[0] if slot + 2 <= size else 0
            defaults.append(default)
            if not offset and not matches:
                fallback = True
            if offset:
                width = struct.calcsize('<' + fmt)
                if offset < 4 or offset + width > object_size:
                    raise ValueError('scalar outside table object')
                present.append((offset, i, fmt, width))
        present.sort()
        parts, order, cursor = ['<'], [], 0
        for offset, i, fmt, width in present:
            if offset < cursor:
                fallback = True
            parts.extend([str(max(0, offset - cursor)) + 'x', fmt])
            order.append(i)
            cursor = offset + width
        return (None if fallback else struct.Struct(''.join(parts)),
                tuple(order), tuple(defaults), object_size)

    def rows(self, view, slot, fields=None):
        names = self.names if fields is None else tuple(fields)
        if any(name not in self.fields for name in names):
            raise ValueError('projection must contain non-deprecated scalar fields')
        data, root = view._tab.Bytes, view._tab.Pos
        length = len(data)
        vt, size, object_size = _table(data, root)
        offset = _U16.unpack_from(data, vt + slot)[0] if slot + 2 <= size else 0
        if not offset:
            return []
        if offset < 4 or offset + 4 > object_size:
            raise ValueError('vector pointer outside parent table object')
        pointer = root + offset
        relative = _U32.unpack_from(data, pointer)[0]
        if relative < 4:
            raise ValueError('invalid vector offset')
        vector = pointer + relative
        _bounds(vector, 4, length)
        count = _U32.unpack_from(data, vector)[0]
        first = vector + 4
        # Check the complete vector before allocating offsets or result rows.
        _bounds(first, count * 4, length)
        relatives = struct.unpack_from('<' + 'I' * count, data, first)
        layouts, result = {}, []
        for i, relative in enumerate(relatives):
            if relative < 4:
                raise ValueError('invalid table offset')
            pos = first + 4 * i + relative
            _bounds(pos, 4, length)
            vtpos = pos - _I32.unpack_from(data, pos)[0]
            plan = layouts.get(vtpos)
            if plan is None:
                _, size, _ = _table(data, pos)
                signature = bytes(memoryview(data)[vtpos:vtpos + size])
                plan = self.plan(signature, names)
                layouts[vtpos] = plan
            _bounds(pos, plan[3], length)
            packer, order, defaults, _ = plan
            if packer is None:
                obj = self.table_class()
                obj.Init(data, pos)
                result.append(tuple(self.fields[name][4](obj) for name in names))
            else:
                values = list(defaults)
                for index, value in zip(order, packer.unpack_from(data, pos)):
                    values[index] = value
                result.append(tuple(values))
        return result
