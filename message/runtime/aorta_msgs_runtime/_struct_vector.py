import struct
from functools import lru_cache

import flatbuffers
from flatbuffers import number_types


class StructVectorWriter:
    def __init__(self, size, alignment, fields):
        self.size, self.alignment = size, alignment
        self.names = tuple(name for name, _, _ in fields)
        self.bool_positions = tuple(i for i, (_, _, fmt) in enumerate(fields) if fmt == '?')
        parts, cursor = [], 0
        for _, offset, fmt in fields:
            parts.extend([str(offset - cursor) + 'x', fmt])
            cursor = offset + struct.calcsize('<' + fmt)
        parts.append(str(size - cursor) + 'x')
        self.format = ''.join(parts)
        self.packer = lru_cache(maxsize=16)(self.packer)

    def packer(self, count):
        return struct.Struct('<' + self.format * count)

    def create(self, builder, rows):
        if flatbuffers.__version__ != '25.12.19':
            raise RuntimeError('struct-vector writer requires flatbuffers==25.12.19')
        if any(len(row) != len(self.names) for row in rows):
            raise ValueError('wrong struct row width')
        for index in self.bool_positions:
            for row in rows:
                number_types.enforce_number(row[index], number_types.BoolFlags)
        # Pack before mutating the builder; struct validates other scalar ranges.
        payload = self.packer(len(rows)).pack(*(value for row in rows for value in row))
        builder.StartVector(self.size, len(rows), self.alignment)
        end = builder.Head()
        builder.head = end - len(payload)
        builder.Bytes[builder.Head():end] = payload
        return builder.EndVector()
