"""Timer wrapper + the timer callback trampoline.

Timer ticks fire on a Tokio worker thread, so the same discipline as the
subscriber trampoline (Python SDK design proposal §5.1) applies: ONE
process-global ``TIMER_CB``, a token→callback registry (never pass a PyObject
pointer to Rust), the GIL is held by ctypes on entry, and a Python exception
must never cross the ``extern "C"`` (nounwind) boundary — it is logged and
swallowed.

Parity note: the C++ ``Timer`` exposes only the 13 diagnostic counters (no
control methods). ``pause``/``resume``/``stop``/``is_running``/``period`` are
Rust-SDK parity; ``is_running``/``period`` needed new C-ABI bridge fns added in
Phase 3 (``aorta_timer_is_running`` / ``aorta_timer_get_period_ns``).
"""

from __future__ import annotations

import ctypes
import datetime
import itertools
import threading
from typing import Callable, Optional, Union

from . import errors, logging as _log
from ._ffi import bindings, loader
from .options import TimerOptions, TimerStats, build_timer_options

# Timer tick callback: () -> None
TimerCallback = Callable[[], None]
# Period accepted by create_timer: seconds (float/int) or a timedelta.
Period = Union[float, int, "datetime.timedelta"]

_handlers: dict = {}
_handlers_lock = threading.Lock()
_token_counter = itertools.count(1)


def _register(callback: TimerCallback) -> int:
    token = next(_token_counter)
    with _handlers_lock:
        _handlers[token] = callback
    return token


def _unregister(token: int) -> None:
    with _handlers_lock:
        _handlers.pop(token, None)


def _dispatch(user_data) -> None:
    # Foreign worker thread, GIL held by ctypes. Must never let an exception
    # propagate into Rust (extern "C" is nounwind -> abort).
    callback = _handlers.get(int(user_data)) if user_data else None
    if callback is None:
        return
    try:
        callback()
    except BaseException as exc:  # noqa: BLE001 - swallow: must not cross C boundary
        try:
            _log.error("timer callback raised: %r", exc, scope="aorta_sdk")
        except BaseException:  # pragma: no cover - logging must never re-raise
            pass


# One process-global trampoline, kept alive for the process lifetime.
_TRAMPOLINE = bindings.TIMER_CB(_dispatch)


def _period_to_ns(period: Period) -> int:
    if isinstance(period, datetime.timedelta):
        ns = int(period.total_seconds() * 1_000_000_000)
    else:
        ns = int(float(period) * 1_000_000_000)
    if ns <= 0:
        raise errors.InvalidArgumentError("timer period must be positive")
    return ns


class Timer:
    """A periodic timer. Created via ``Node.create_timer``.

    The callback runs as ``callback()`` on a worker thread (exceptions are logged
    and swallowed). ``auto_start`` (default True) starts it immediately; otherwise
    call ``resume()``.
    """

    def __init__(
        self,
        node,
        period: Period,
        callback: TimerCallback,
        *,
        options: Optional[TimerOptions] = None,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        self._closed = False
        self._period_ns = _period_to_ns(period)
        self._token = _register(callback)
        opts_struct, self._opts_keep = build_timer_options(options)
        opts_arg = ctypes.byref(opts_struct) if opts_struct is not None else None
        handle = bindings.aorta_timer_t(0)
        rc = self._lib.aorta_timer_new_with_options(
            node._handle,
            self._period_ns,
            _TRAMPOLINE,
            ctypes.c_void_p(self._token),
            opts_arg,
            ctypes.byref(handle),
        )
        if rc != 0:
            _unregister(self._token)
            errors.check_rc(self._lib, rc, "aorta_timer_new_with_options")
        self._handle = handle.value

    def pause(self) -> None:
        """Stop firing ticks; the timer can be resumed (Rust ``Timer::pause``)."""
        self._ensure_open()
        rc = self._lib.aorta_timer_cancel(self._handle)
        errors.check_rc(self._lib, rc, "aorta_timer_cancel")

    def resume(self) -> None:
        """Restart a paused timer (resets the next deadline; Rust ``Timer::resume``)."""
        self._ensure_open()
        rc = self._lib.aorta_timer_reset(self._handle)
        errors.check_rc(self._lib, rc, "aorta_timer_reset")

    def is_running(self) -> bool:
        """Whether the timer is started and not paused/stopped."""
        self._ensure_open()
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_timer_is_running(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_timer_is_running")
        return bool(out.value)

    def period(self) -> float:
        """The configured period in **seconds** (float)."""
        return self.period_ns() / 1_000_000_000

    def period_ns(self) -> int:
        """The configured period in nanoseconds (exact)."""
        self._ensure_open()
        out = ctypes.c_uint64(0)
        rc = self._lib.aorta_timer_get_period_ns(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_timer_get_period_ns")
        return int(out.value)

    def stats(self) -> TimerStats:
        """Snapshot the 13 diagnostic counters (C++/Rust ``Timer`` getters)."""
        self._ensure_open()
        values = {}
        out = ctypes.c_uint64(0)
        for name in bindings.TIMER_COUNTERS:
            fn = getattr(self._lib, "aorta_timer_get_" + name)
            rc = fn(self._handle, ctypes.byref(out))
            errors.check_rc(self._lib, rc, "aorta_timer_get_" + name)
            values[name] = int(out.value)
        return TimerStats(**values)

    def close(self) -> None:
        """Permanently stop the timer and drop the callback token. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_timer_free(self._handle)
        _unregister(self._token)

    # Rust ``Timer::stop`` is the permanent stop (called on Drop) — same as close.
    stop = close

    def _ensure_open(self) -> None:
        if self._closed:
            raise errors.AortaError("operation on a closed Timer")

    def __enter__(self) -> "Timer":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
