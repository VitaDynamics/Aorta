import importlib.util
import os
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch

import flatbuffers
from flatbuffers.builder import IsNestedError
from fx_write import AllScalar, EnumCommand, Fixed, LowCmd, MotorCmd, Nested

from gen_python_fastpath import generate


def companion():
    path = Path(os.environ['WRITER_COMPANION'], 'fx_write/writer_common_fastpath.py')
    spec = importlib.util.spec_from_file_location('writer_common_fastpath', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def encode(writer, rows, initial=1, generated=False, prelude=False):
    b = flatbuffers.Builder(initial)
    if prelude:
        b.CreateString('unaligned prefix')
    if generated:
        LowCmd.LowCmdStartMotorCmdVector(b, len(rows))
        for row in reversed(rows):
            MotorCmd.CreateMotorCmd(b, *row)
        vector = b.EndVector()
    else:
        vector = writer.create(b, rows)
    LowCmd.LowCmdStart(b)
    LowCmd.LowCmdAddStamp(b, 123)
    LowCmd.LowCmdAddMotorCmd(b, vector)
    b.Finish(LowCmd.LowCmdEnd(b))
    return bytes(b.Output())


def produce(path):
    fast = companion()
    b = flatbuffers.Builder(1)
    rows = [(i, i + .125, i + .25, i + .5, i + .75, i + .875) for i in range(42)]
    motors = fast.MotorCmdVector.create(b, rows)
    enums = fast.EnumCommandVector.create(b, [(127, -0.0, 2**64-1), (-128, float('inf'), 0)])
    scalars = fast.AllScalarVector.create(b, [
        (True, -128, 255, -32768, 65535, -2**31, 2**32-1, -2**63, 2**64-1, .1, -.125)])
    LowCmd.LowCmdStart(b)
    LowCmd.LowCmdAddStamp(b, 123)
    LowCmd.LowCmdAddMotorCmd(b, motors)
    LowCmd.LowCmdAddEnums(b, enums)
    LowCmd.LowCmdAddScalars(b, scalars)
    b.Finish(LowCmd.LowCmdEnd(b))
    Path(path).write_bytes(b.Output())


class StructVectorTests(unittest.TestCase):
    def setUp(self):
        self.fast = companion()

    def test_counts_growth_order_alignment_padding_and_bytes(self):
        writer = self.fast.MotorCmdVector
        self.assertEqual((writer.size, writer.alignment), (24, 4))
        self.assertEqual(writer.names, ('mode', 'q', 'dq', 'tau', 'kp', 'kd'))
        for count in (0, 1, 2, 42, 43):
            rows = [(i, i + .125, i + .25, i + .5, i + .75, i + .875) for i in range(count)]
            for initial in (1, 1024, 4096):
                for prelude in (False, True):
                    with self.subTest(count=count, initial=initial, prelude=prelude):
                        data = encode(writer, rows, initial, prelude=prelude)
                        self.assertEqual(data, encode(writer, rows, initial, True, prelude))
                        root = LowCmd.LowCmd.GetRootAs(data)
                        self.assertEqual(root.MotorCmdLength(), count)
                        start = root._tab.Vector(root._tab.Offset(6))
                        self.assertEqual(start % 4, 0)
                        for i, row in enumerate(rows):
                            motor = root.MotorCmd(i)
                            self.assertEqual((motor.Mode(), motor.Q(), motor.Dq(), motor.Tau(), motor.Kp(), motor.Kd()), row)
                            self.assertEqual(data[start+24*i+1:start+24*i+4], b'\0'*3)

    def test_float_special_values_rounding_and_overflow(self):
        for value in (float('nan'), float('inf'), -float('inf'), -0.0, .1, 3.4028234663852886e38):
            rows = [(255, value, 0., 0., 0., 0.)]
            data = encode(self.fast.MotorCmdVector, rows)
            self.assertEqual(data, encode(self.fast.MotorCmdVector, rows, generated=True))
            actual = LowCmd.LowCmd.GetRootAs(data).MotorCmd(0).Q()
            self.assertEqual(struct.pack('<f', actual), struct.pack('<f', value))
        with self.assertRaises(OverflowError):
            self.fast.MotorCmdVector.create(flatbuffers.Builder(1), [(0, 1e100, 0., 0., 0., 0.)])

    def test_all_scalar_ranges_enum_and_forced_alignment(self):
        limits = ((0, 1), (-128, 127), (0, 255), (-32768, 32767), (0, 65535),
                  (-2**31, 2**31-1), (0, 2**32-1), (-2**63, 2**63-1), (0, 2**64-1))
        rows = [(bool(side), *(limit[side] for limit in limits[1:]), .1, -0.0) for side in (0, 1)]
        for writer, module, values in (
                (self.fast.AllScalarVector, AllScalar, rows),
                (self.fast.EnumCommandVector, EnumCommand, [(127, .1, 2**64-1), (-128, -0., 0)])):
            creator = getattr(module, 'Create' + module.__name__.rsplit('.', 1)[-1])
            for initial in (1, 1024, 4096):
                b, reference = flatbuffers.Builder(initial), flatbuffers.Builder(initial)
                b.PrependUint8(17)
                reference.PrependUint8(17)
                vector = writer.create(b, values)
                reference.StartVector(writer.size, len(values), writer.alignment)
                for row in reversed(values):
                    creator(reference, *row)
                expected = reference.EndVector()
                self.assertEqual(vector, expected)
                self.assertEqual(b.Bytes[b.Head():], reference.Bytes[reference.Head():])
                self.assertEqual((len(b.Bytes)-vector+4) % writer.alignment, 0)
        # Unknown enum numbers are wire integers, bounded only by the underlying byte.
        for value in (-129, 128):
            with self.assertRaises(struct.error):
                self.fast.EnumCommandVector.create(flatbuffers.Builder(1), [(value, 0., 0)])
        for index, (low, high) in enumerate(limits):
            for value in (low-1, high+1, 1.5):
                row = list(rows[0])
                row[index] = value
                with self.subTest(index=index, value=value), self.assertRaises(TypeError if index == 0 else struct.error):
                    self.fast.AllScalarVector.create(flatbuffers.Builder(1), [row])

    def test_bool_rejection_matches_creator_before_builder_mutation(self):
        base = (True, -128, 255, -32768, 65535, -2**31, 2**32-1,
                -2**63, 2**64-1, .1, -.125)
        for value in (2, -1, 2**64, float('nan'), float('inf'), 'false', None):
            with self.subTest(value=repr(value)):
                row = (value,) + base[1:]
                with self.assertRaises(TypeError):
                    AllScalar.CreateAllScalar(flatbuffers.Builder(1), *row)
                b = flatbuffers.Builder(1)
                b.PrependUint8(17)
                before = (bytes(b.Bytes), b.Head(), b.nested)
                with self.assertRaises(TypeError):
                    self.fast.AllScalarVector.create(b, [base, row])
                self.assertEqual((bytes(b.Bytes), b.Head(), b.nested), before)

    def test_bool_valid_domain_bytes_match_creator(self):
        base = (True, -128, 255, -32768, 65535, -2**31, 2**32-1,
                -2**63, 2**64-1, .1, -.125)
        for value in (False, True, 0, 1, -0.0, 0.0, .25, .5, .75, 1.0):
            with self.subTest(value=value):
                row = (value,) + base[1:]
                b, reference = flatbuffers.Builder(1), flatbuffers.Builder(1)
                vector = self.fast.AllScalarVector.create(b, [row])
                LowCmd.LowCmdStartScalarsVector(reference, 1)
                AllScalar.CreateAllScalar(reference, *row)
                expected = reference.EndVector()
                self.assertEqual(vector, expected)
                self.assertEqual(b.Bytes[b.Head():], reference.Bytes[reference.Head():])

    def test_row_width_rejection_before_builder_mutation(self):
        for rows in ([(1,)], [(0, 1., 2., 3., 4.), (0, 1., 2., 3., 4., 5., 6.)]):
            b = flatbuffers.Builder(1)
            with self.assertRaises(ValueError):
                self.fast.MotorCmdVector.create(b, rows)
            self.assertEqual(b.Head(), 1)
            self.assertFalse(b.nested)

    def test_nesting_rejection_and_runtime_version(self):
        for nesting in ('table', 'vector'):
            b = flatbuffers.Builder(1)
            if nesting == 'table':
                LowCmd.LowCmdStart(b)
            else:
                b.StartVector(4, 1, 4)
            with self.assertRaises(IsNestedError):
                self.fast.MotorCmdVector.create(b, [(0, 0., 0., 0., 0., 0.)])
        self.assertEqual(flatbuffers.__version__, '25.12.19')
        with patch.object(flatbuffers, '__version__', '0.0.0'):
            with self.assertRaisesRegex(RuntimeError, '25.12.19'):
                self.fast.MotorCmdVector.create(flatbuffers.Builder(1), [])

    def test_rootless_ownership_and_unsupported_structs(self):
        self.assertFalse(hasattr(self.fast, 'NestedVector'))
        self.assertFalse(hasattr(self.fast, 'FixedVector'))
        b = flatbuffers.Builder(1)
        offset = Nested.CreateNested(b, 1, .125, .25, .5, .75, .875)
        nested = Nested.Nested()
        nested.Init(b.Bytes, len(b.Bytes)-offset)
        motor = nested.Command(MotorCmd.MotorCmd())
        self.assertEqual((motor.Mode(), motor.Q()), (1, .125))
        offset = Fixed.CreateFixed(b, [3, 5, 7])
        fixed = Fixed.Fixed()
        fixed.Init(b.Bytes, len(b.Bytes)-offset)
        self.assertEqual([fixed.Values(i) for i in range(3)], [3, 5, 7])
        for key, expected in (('COMMON_BFBS', ['fx_write/owner_fastpath.py']), ('CMD_BFBS', [])):
            with tempfile.TemporaryDirectory() as out:
                generate(Path(os.environ[key]), Path(os.environ['WRITER_GETTERS']), Path(out), 'owner')
                self.assertEqual(sorted(str(p.relative_to(out)) for p in Path(out).rglob('*.py')), expected)


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--produce':
        produce(sys.argv[2])
    else:
        unittest.main()
