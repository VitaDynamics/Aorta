"""Aorta Python tracing wrapper (Step 11) — thin, over the core C-ABI via ctypes.

Independent of telemetry/logging. The disabled check calls the FFI predicate
``aorta_trace_enabled`` — the gate's relaxed ATOMIC load happens in Rust. We do
NOT read the gate bytes from Python with a plain (non-atomic) load, which would
race the Rust atomic store on a runtime toggle. This is "cheap, NOT near-zero":
the FFI crossing + Python interpreter floor is tens-to-hundreds of ns, so Python
tracing does not match the Rust/C++ near-zero disabled path (by design — Rust
owns hot emission).

Timing uses ``time.monotonic_ns`` (monotonic). Records go through the C-ABI only
when the ``(scope, level)`` gate passes.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import functools
import os
import threading
import time
from contextlib import contextmanager
from enum import IntEnum
from typing import Optional


class LogLevel(IntEnum):
    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4


_lib: Optional[ctypes.CDLL] = None
_lock = threading.Lock()


def _load_lib() -> ctypes.CDLL:
    global _lib
    if _lib is not None:
        return _lib
    with _lock:
        if _lib is not None:
            return _lib
        candidates = []
        env_path = os.environ.get("AORTA_CORE_FFI_LIB")
        if env_path:
            candidates.append(env_path)
        found = ctypes.util.find_library("aorta_core_ffi")
        if found:
            candidates.append(found)
        candidates += ["libaorta_core_ffi.so", "libaorta_core_ffi.dylib"]
        lib = None
        for cand in candidates:
            try:
                lib = ctypes.CDLL(cand)
                break
            except OSError:
                continue
        if lib is None:
            raise RuntimeError(
                "failed to load Aorta FFI library; set AORTA_CORE_FFI_LIB to its path"
            )
        _bind(lib)
        _lib = lib
        return _lib


def _bind(lib: ctypes.CDLL) -> None:
    lib.aorta_trace_scope.argtypes = [ctypes.c_char_p]
    lib.aorta_trace_scope.restype = ctypes.c_uint16
    lib.aorta_trace_set_enabled.argtypes = [ctypes.c_uint8]
    lib.aorta_trace_set_enabled.restype = None
    lib.aorta_trace_is_enabled.argtypes = []
    lib.aorta_trace_is_enabled.restype = ctypes.c_uint8
    lib.aorta_trace_set_scope_enabled.argtypes = [ctypes.c_uint16, ctypes.c_uint8]
    lib.aorta_trace_set_scope_enabled.restype = None
    lib.aorta_trace_enabled.argtypes = [ctypes.c_uint16, ctypes.c_uint8]
    lib.aorta_trace_enabled.restype = ctypes.c_uint8
    lib.aorta_trace_record.argtypes = [
        ctypes.c_uint16,
        ctypes.c_char_p,
        ctypes.c_uint8,
        ctypes.c_uint64,
    ]
    lib.aorta_trace_record.restype = ctypes.c_int32
    lib.aorta_trace_record_event.argtypes = [
        ctypes.c_uint16,
        ctypes.c_char_p,
        ctypes.c_uint8,
        ctypes.c_uint8,
        ctypes.c_uint64,
    ]
    lib.aorta_trace_record_event.restype = ctypes.c_int32
    lib.aorta_trace_record_stage.argtypes = [ctypes.c_uint8, ctypes.c_uint64]
    lib.aorta_trace_record_stage.restype = ctypes.c_int32
    lib.aorta_trace_init_from_env.argtypes = []
    lib.aorta_trace_init_from_env.restype = ctypes.c_int32
    # restype MUST be set: ctypes defaults to c_int (32-bit signed), which would
    # silently truncate the u64 count and misread on LP64 targets.
    lib.aorta_trace_user_metric_count.argtypes = [ctypes.c_uint16, ctypes.c_char_p]
    lib.aorta_trace_user_metric_count.restype = ctypes.c_uint64
    lib.aorta_trace_master_gate.argtypes = []
    lib.aorta_trace_master_gate.restype = ctypes.c_void_p
    lib.aorta_trace_scope_gate.argtypes = [ctypes.c_uint16]
    lib.aorta_trace_scope_gate.restype = ctypes.c_void_p


def set_enabled(on: bool) -> None:
    _load_lib().aorta_trace_set_enabled(1 if on else 0)


def is_enabled() -> bool:
    return _load_lib().aorta_trace_is_enabled() != 0


def init_from_env() -> None:
    _load_lib().aorta_trace_init_from_env()


class Scope:
    """A cached trace scope. Intern once and reuse (interning is cold)."""

    def __init__(self, name: str):
        lib = _load_lib()
        self._lib = lib
        self.name = name
        self.handle = lib.aorta_trace_scope(name.encode("utf-8"))

    def set_enabled(self, on: bool) -> None:
        self._lib.aorta_trace_set_scope_enabled(self.handle, 1 if on else 0)

    def enabled(self, level: LogLevel) -> bool:
        # Gate via the FFI predicate: the relaxed ATOMIC load (master + per-scope
        # bytes) happens in Rust. We deliberately do NOT read the gate bytes from
        # Python with a plain non-atomic load, which would race the Rust atomic
        # store on a runtime toggle.
        return self._lib.aorta_trace_enabled(self.handle, int(level)) != 0

    def record(self, name: str, level: LogLevel, latency_ns: int) -> None:
        # Gate-checked like instant()/counter(): skip the FFI crossing + name
        # encoding when disabled.
        if self.enabled(level):
            self._lib.aorta_trace_record(
                self.handle, name.encode("utf-8"), int(level), int(latency_ns)
            )

    def instant(self, name: str, level: LogLevel) -> None:
        """EVENT-TYPE: a point instant (occurrence). Gate-checked."""
        if self.enabled(level):
            self._lib.aorta_trace_record_event(
                self.handle, name.encode("utf-8"), 1, int(level), 1
            )

    def counter(self, name: str, level: LogLevel, value: int) -> None:
        """EVENT-TYPE: a counter value. Gate-checked (value evaluated by caller)."""
        if self.enabled(level):
            self._lib.aorta_trace_record_event(
                self.handle, name.encode("utf-8"), 2, int(level), int(value)
            )

    def recorded_count(self, name: str) -> int:
        """Recorded sample count for ``name`` under this scope (0 if never
        recorded). Read-back into the real engine — for tests/diagnostics, NOT
        the hot path. Mirrors the C++ ``Scope::recorded_count``."""
        return int(
            self._lib.aorta_trace_user_metric_count(self.handle, name.encode("utf-8"))
        )


@contextmanager
def trace_span(scope: Scope, level: LogLevel, name: str):
    """Time a block; records the monotonic latency on exit iff enabled."""
    active = scope.enabled(level)
    start = time.monotonic_ns() if active else 0
    try:
        yield
    finally:
        if active:
            scope.record(name, level, time.monotonic_ns() - start)


def traced(scope: Scope, level: LogLevel, name: Optional[str] = None):
    """Decorator: time a function under `(scope, level, name or fn.__name__)`."""

    def decorator(fn):
        metric_name = name or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with trace_span(scope, level, metric_name):
                return fn(*args, **kwargs)

        return wrapper

    return decorator
