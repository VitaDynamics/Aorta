"""Tests for the standalone Python tracing wrapper (``python/tracing.py``).

Runs only when the core FFI library is available (``conftest.py`` discovers the
Bazel-built cdylib and exports ``AORTA_CORE_FFI_LIB``; set it manually
otherwise). Without the library the suite skips, so it does not hard-fail
environments where the FFI lib is not built. Execute with
``python3 -m pytest python/tests/test_tracing.py``.
"""

import os
import sys
import unittest

# `tracing` is a top-level module in python/ (the SDK source root). conftest.py
# already puts that root on sys.path under pytest; keep a fallback for direct
# unittest invocation.
_PYTHON_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PYTHON_ROOT not in sys.path:
    sys.path.insert(0, _PYTHON_ROOT)

try:
    import tracing as trace  # type: ignore

    trace._load_lib()
    _HAVE_LIB = True
except Exception:  # noqa: BLE001 - any load/bind failure means skip
    _HAVE_LIB = False


@unittest.skipUnless(_HAVE_LIB, "core FFI library not available (set AORTA_CORE_FFI_LIB)")
class TraceWrapperTest(unittest.TestCase):
    def test_disabled_span_is_noop(self) -> None:
        trace.set_enabled(False)
        scope = trace.Scope("py.test.off")
        scope.set_enabled(True)  # scope on, but master off
        self.assertFalse(scope.enabled(trace.LogLevel.INFO))
        # A disabled span must not raise and must make no FFI record call.
        with trace.trace_span(scope, trace.LogLevel.INFO, "blk"):
            pass

    def test_scope_timer_records_when_enabled(self) -> None:
        trace.set_enabled(True)
        scope = trace.Scope("py.test.on")
        scope.set_enabled(True)
        self.assertTrue(scope.enabled(trace.LogLevel.INFO))
        with trace.trace_span(scope, trace.LogLevel.INFO, "work"):
            pass

        calls = {"n": 0}

        @trace.traced(scope, trace.LogLevel.INFO, "decorated")
        def do_work() -> int:
            calls["n"] += 1
            return 7

        self.assertEqual(do_work(), 7)
        self.assertEqual(calls["n"], 1)
        trace.set_enabled(False)

    def test_master_gate_toggle_visible(self) -> None:
        scope = trace.Scope("py.test.gate")
        scope.set_enabled(True)
        trace.set_enabled(True)
        self.assertTrue(scope.enabled(trace.LogLevel.ERROR))
        trace.set_enabled(False)
        self.assertFalse(scope.enabled(trace.LogLevel.ERROR))


if __name__ == "__main__":
    unittest.main()
