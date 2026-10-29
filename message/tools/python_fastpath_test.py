import importlib.util
import inspect
import math
import os
from pathlib import Path
import random
import struct
import sys
import unittest
from collections import Counter
from unittest.mock import patch

import flatbuffers

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gen_python_fastpath import generate
from fx_rows import Batch, Sample
from fx_fastpath import ScalarDefaults
spec = importlib.util.spec_from_file_location(
    'fx_rows.fastpath_rows_fastpath',
    Path(os.environ['COMPANION'], 'fx_rows/fastpath_rows_fastpath.py'))
fast = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fast)

MOTOR_NAMES = ('ts_acquired_hw_us', 'ts_ingested_us', 'ts_assembled_us', 'mode',
               'q', 'dq', 'ddq', 'tau_est', 'q_raw', 'dq_raw', 'ddq_raw',
               'temperature', 'lost', 'slot_index', 'slot_role',
               'accumulated_running_sec', 'thermal_integral', 'overload_integral',
               'mos_temperature_deg_c', 'iq_setpoint_a', 'iq_measured_a')


def build(module, vector_slot, entries, force=False):
    b = flatbuffers.Builder(8)
    b.ForceDefaults(force)
    offsets = []
    name = module.__name__.rsplit('.', 1)[-1]
    for fields in entries:
        values = list(fields)
        getattr(module, name + 'Start')(b)
        for slot, value in values:
            if module is Sample:
                formats = ('Int8', 'Uint8', 'Int16', 'Uint16', 'Int32', 'Uint32',
                           'Int64', 'Uint64', 'Float32', 'Float64')
                getattr(b, 'Prepend' + formats[slot] + 'Slot')(slot, value, 0.1 if slot == 8 else 0)
            else:
                methods = {0: 'UlongMax', 1: 'NegativeFloat', 2: 'NegativeDouble',
                           4: 'OptionalFloat', 5: 'Enabled', 6: 'Mode', 7: 'Foo_bar', 8: 'GpsPosition'}
                getattr(module, name + 'Add' + methods[slot])(b, value)
        offsets.append(getattr(module, name + 'End')(b))
    b.StartVector(4, len(offsets), 4)
    for offset in reversed(offsets):
        b.PrependUOffsetTRelative(offset)
    vector = b.EndVector()
    Batch.BatchStart(b)
    b.PrependUOffsetTRelativeSlot(vector_slot, vector, 0)
    b.Finish(Batch.BatchEnd(b))
    return b.Output()


def signature(data, pos):
    vt = pos - struct.unpack_from('<i', data, pos)[0]
    size = struct.unpack_from('<H', data, vt)[0]
    return vt, bytes(data[vt:vt + size])


class FastpathTests(unittest.TestCase):
    def assert_rows(self, view, read, getter, names=None):
        actual = read(view, fields=names)
        child = getattr(view, getter)
        reader = {'Motors': fast._reader_1, 'Samples': fast._reader_2,
                  'Defaults': fast._reader_0}[getter]
        names = reader.names if names is None else names
        expected = [tuple(reader.fields[n][4](child(i)) for n in names)
                    for i in range(getattr(view, getter + 'Length')())]
        self.assertEqual(len(actual), len(expected))
        for row, reference in zip(actual, expected):
            for a, e in zip(row, reference):
                if isinstance(e, float):
                    self.assertTrue(math.isnan(a) if math.isnan(e) else struct.pack('<d', a) == struct.pack('<d', e))
                else:
                    self.assertEqual(a, e)
        return actual

    def test_randomized_presence_order_widths_and_projections(self):
        rng = random.Random(477)
        limits = [(-128, 127), (0, 255), (-32768, 32767), (0, 65535),
                  (-2**31, 2**31-1), (0, 2**32-1), (-2**63, 2**63-1), (0, 2**64-1)]
        for iteration in range(100):
            rows = []
            for _ in range(44):
                fields = [(i, rng.randint(*limit)) for i, limit in enumerate(limits)]
                fields += [(8, rng.uniform(-2, 2)), (9, rng.uniform(-2, 2))]
                rng.shuffle(fields)
                rows.append([f for f in fields if rng.random() > .4])
            view = Batch.Batch.GetRootAs(build(Sample, 2, rows, iteration % 2 == 0))
            self.assert_rows(view, fast.read_samples, 'Samples')
            self.assert_rows(view, fast.read_samples, 'Samples', ('unsigned64', 'real32', 'signed64'))
        self.assertEqual(fast._reader_2.plan.cache_info().currsize, 128)
        self.assertGreater(fast._reader_2.plan.cache_info().misses, 128)

    def test_integer_extremes_and_absent_float_literal(self):
        limits = ((-128, 127), (0, 255), (-32768, 32767), (0, 65535),
                  (-2**31, 2**31-1), (0, 2**32-1), (-2**63, 2**63-1), (0, 2**64-1))
        entries = [[(i, bounds[side]) for i, bounds in enumerate(limits)] for side in (0, 1)]
        view = Batch.Batch.GetRootAs(build(Sample, 2, entries, True))
        rows = self.assert_rows(view, fast.read_samples, 'Samples')
        self.assertEqual(rows[0][6:8], (-2**63, 0))
        self.assertEqual(rows[1][6:8], (2**63-1, 2**64-1))
        self.assertEqual(rows[0][8], 0.1)
        self.assertNotEqual(rows[0][8], struct.unpack('<f', struct.pack('<f', 0.1))[0])

    def test_defaults_oracle_optional_enum_and_names(self):
        data = build(ScalarDefaults, 1, [[], [(0, 2**64-2), (1, .125), (2, -.125), (4, .25), (5, False), (6, 127), (7, 7), (8, 11)]])
        view = Batch.Batch.GetRootAs(data)
        rows = self.assert_rows(view, fast.read_defaults, 'Defaults')
        reader = fast._reader_0
        self.assertNotIn('deprecated_value', reader.names)
        self.assertNotIn('commands', reader.names)
        self.assertEqual(rows[0][reader.names.index('ulong_max')], 2**64-1)
        self.assertIsNone(rows[0][reader.names.index('optional_float')])
        self.assertEqual(reader.fields['foo__bar'][4].__name__, 'Foo_bar')
        self.assertEqual(reader.fields['GPS_position'][4].__name__, 'GpsPosition')
        _, sig = signature(data, view.Defaults(0)._tab.Pos)
        self.assertIsNone(reader.plan(sig, reader.names)[0])
        _, sig = signature(data, view.Defaults(1)._tab.Pos)
        self.assertIsNotNone(reader.plan(sig, reader.names)[0])
        self.assertEqual(fast.read_defaults(view, fields=('mode',))[1], (127,))

    def test_producer_faithful_44_tables(self):
        data = bytearray(Path(os.environ['PRODUCER']).read_bytes())
        view = Batch.Batch.GetRootAs(data)
        counts = Counter(signature(data, view.Motors(i)._tab.Pos)[1] for i in range(44))
        self.assertEqual(sorted(counts.values()), [1, 2, 41])
        self.assertEqual(fast._reader_1.names, tuple(n for n in MOTOR_NAMES if n != 'thermal_integral'))
        reader = fast._reader_1
        reader.plan.cache_clear()
        self.assert_rows(view, fast.read_motors, 'Motors', ('q', 'dq', 'tau_est', 'temperature'))
        self.assertEqual(reader.plan.cache_info().misses, 3)
        self.assert_rows(view, fast.read_motors, 'Motors', ('q', 'dq', 'tau_est', 'temperature'))
        self.assertEqual(reader.plan.cache_info().misses, 3)
        self.assert_rows(view, fast.read_motors, 'Motors')
        for i in range(44):
            self.assertEqual(view.Motors(i).ThermalIntegralLength(), 3)
            self.assertEqual(view.Motors(i).SlotRole(), 1 if i < 42 else 2)

    def test_shared_unshared_negative_displacement_and_overlap_fallback(self):
        data = build(Sample, 2, [[(4, 7), (5, 11)]] * 3)
        view = Batch.Batch.GetRootAs(data)
        reader = fast._reader_2
        reader.plan.cache_clear()
        self.assert_rows(view, fast.read_samples, 'Samples')
        misses = reader.plan.cache_info().misses
        # Identical bytes at different addresses/messages reuse only descriptors.
        self.assert_rows(Batch.Batch.GetRootAs(bytes(data)), fast.read_samples, 'Samples')
        self.assertEqual(reader.plan.cache_info().misses, misses)
        child = view.Samples(0)._tab
        vt, sig = signature(data, child.Pos)
        self.assertLess(len({signature(data, view.Samples(i)._tab.Pos)[0] for i in range(3)}), 3)
        replacement = len(data)
        data.extend(sig)
        struct.pack_into('<i', data, child.Pos, child.Pos - replacement)
        self.assertLess(struct.unpack_from('<i', data, child.Pos)[0], 0)
        self.assert_rows(view, fast.read_samples, 'Samples')
        struct.pack_into('<H', data, replacement + 14, child.Offset(12))
        self.assert_rows(view, fast.read_samples, 'Samples', ('signed32', 'unsigned32'))
        self.assertIsNone(fast._reader_2.plan(bytes(data[replacement:]), ('signed32', 'unsigned32'))[0])

    def test_mutation_short_vtable_and_special_floats(self):
        data = build(Sample, 2, [[(8, v)] for v in (float('nan'), float('inf'), -float('inf'), -0.0)], True)
        for raw in (data, bytes(data), memoryview(data)):
            self.assert_rows(Batch.Batch.GetRootAs(raw), fast.read_samples, 'Samples')
        view = Batch.Batch.GetRootAs(data)
        table = view.Samples(0)._tab
        vt, _ = signature(data, table.Pos)
        struct.pack_into('<f', data, table.Pos + table.Offset(20), .25)
        self.assert_rows(view, fast.read_samples, 'Samples')
        struct.pack_into('<H', data, vt, 4)
        self.assert_rows(view, fast.read_samples, 'Samples')

    def test_missing_empty_vectors_and_projection_boundary(self):
        b = flatbuffers.Builder(8)
        Batch.BatchStart(b)
        b.Finish(Batch.BatchEnd(b))
        self.assertEqual(fast.read_motors(Batch.Batch.GetRootAs(b.Output())), [])
        for count in (0, 1, 42, 44):
            view = Batch.Batch.GetRootAs(build(Sample, 2, [[]] * count))
            self.assert_rows(view, fast.read_samples, 'Samples')
            self.assertEqual(fast.read_samples(view, fields=()), [()] * count)
        with self.assertRaises(ValueError):
            fast.read_samples(view, fields=('missing',))
        with self.assertRaises(TypeError):
            fast.read_samples(view.Samples(0))

    def test_parent_mutations_and_truncations(self):
        data = build(Sample, 2, [[(4, 7)]])
        view = Batch.Batch.GetRootAs(data)
        self.assert_rows(view, fast.read_samples, 'Samples')
        root = view._tab.Pos
        vt, _ = signature(data, root)
        offset = view._tab.Offset(8)
        for location, value in ((vt, 9), (vt, 0), (vt+2, 4), (vt+2, offset+3), (vt+8, 3)):
            bad = bytearray(data)
            struct.pack_into('<H', bad, location, value)
            with self.assertRaises(ValueError):
                fast.read_samples(Batch.Batch.GetRootAs(bad))
        for end in range(root+4, len(data)):
            with self.assertRaises(ValueError):
                fast.read_samples(Batch.Batch.GetRootAs(data[:end]))

    def test_malformed_child_vector_and_fallback_spans(self):
        data = build(Sample, 2, [[(4, 7), (5, 11)]])
        view = Batch.Batch.GetRootAs(data)
        child = view.Samples(0)._tab
        vt, sig = signature(data, child.Pos)
        pointer = view._tab.Pos + view._tab.Offset(8)
        vector = pointer + struct.unpack_from('<I', data, pointer)[0]
        mutations = [(pointer, '<I', 0), (vector, '<I', 0xffffffff),
                     (vector+4, '<I', 0xffffffff), (vector+4, '<I', 0),
                     (child.Pos, '<i', 0x7fffffff), (vt, '<H', 3),
                     (vt+2, '<H', 65535), (vt+12, '<H', 2), (vt+12, '<H', 65535)]
        for location, fmt, value in mutations:
            bad = bytearray(data)
            struct.pack_into(fmt, bad, location, value)
            with self.assertRaises(ValueError):
                fast.read_samples(Batch.Batch.GetRootAs(bad))
        # All projected spans must be checked before overlapping-field fallback.
        bad = bytearray(data)
        struct.pack_into('<H', bad, vt+14, child.Offset(12))
        struct.pack_into('<H', bad, vt+2, child.Offset(12)+3)
        with self.assertRaises(ValueError):
            fast.read_samples(Batch.Batch.GetRootAs(bad))

    def test_emission_ownership_unsupported_and_unverified_names(self):
        import tempfile
        with tempfile.TemporaryDirectory() as out:
            generate(Path(os.environ['ROWS_BFBS']), Path(os.environ['GETTERS']), Path(out), 'rows')
            files = sorted(str(p.relative_to(out)) for p in Path(out).rglob('*.py'))
            self.assertEqual(files, ['fx_rows/rows_fastpath.py'])
            source = Path(out, files[0]).read_text()
            self.assertEqual(source, Path(os.environ['COMPANION'], 'fx_rows/fastpath_rows_fastpath.py').read_text().replace('fastpath_rows', 'rows'))
        with tempfile.TemporaryDirectory() as out:
            generate(Path(os.environ['UNSUPPORTED_BFBS']), Path(os.environ['GETTERS']), Path(out), 'unsupported')
            self.assertEqual(list(Path(out).rglob('*.py')), [])
        with tempfile.TemporaryDirectory() as out:
            generate(Path(os.environ['DEFAULTS_BFBS']), Path(os.environ['GETTERS']), Path(out), 'defaults')
            source = Path(out, 'fx_fastpath/defaults_fastpath.py').read_text()
            self.assertIn('CommandVector = StructVectorWriter(16, 16, (', source)
            self.assertIn("('mode', 0, 'b')", source)
            self.assertNotIn('def read_', source)
        with patch('gen_python_fastpath.python_scalar_facts', side_effect=KeyError('unverified')):
            with tempfile.TemporaryDirectory() as out:
                generate(Path(os.environ['ROWS_BFBS']), Path(os.environ['GETTERS']), Path(out), 'rows')
                self.assertEqual(list(Path(out).rglob('*.py')), [])

    def test_flatc_accessor_collisions_emit_no_companion(self):
        import tempfile
        for key, namespace, accessor in (
                ('ISNONE_BFBS', 'fx_fastpath_isnone_collision', 'FooIsNone'),
                ('LENGTH_BFBS', 'fx_fastpath_length_collision', 'FooLength')):
            with self.subTest(collision=key), tempfile.TemporaryDirectory() as out:
                getters = Path(os.environ['COLLISION_GETTERS'])
                source = (getters / namespace / 'Child.py').read_text()
                self.assertEqual(source.count('def ' + accessor + '('), 2)
                generate(Path(os.environ[key]), getters, Path(out), 'collision')
                self.assertEqual(list(Path(out).rglob('*.py')), [])

    def test_ambiguous_accessor_skips_companion(self):
        import tempfile
        source = inspect.getsource(ScalarDefaults.ScalarDefaults)
        source += '\n    def Foo_bar(self):\n        return self._tab.Offset(18)\n'
        getsource = inspect.getsource
        with patch('schema_reflection.inspect.getsource', side_effect=lambda table:
                   source if table is ScalarDefaults.ScalarDefaults else getsource(table)):
            with tempfile.TemporaryDirectory() as out:
                generate(Path(os.environ['ROWS_BFBS']), Path(os.environ['GETTERS']), Path(out), 'rows')
                self.assertEqual(list(Path(out).rglob('*.py')), [])


if __name__ == '__main__':
    unittest.main()
