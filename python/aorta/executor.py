"""Executor — explicit, user-driven executor (C++ ``Executor`` parity).

Most Python programs never touch this. A :class:`~aorta.node.Node` lazily spins a
private single-threaded **auto-executor** the first time a timer / context-watch /
execution-group needs an event queue (see :meth:`aorta.node.Node.ensure_executor`),
and subscribers / services deliver on Zenoh worker threads with no executor at
all. This class is the explicit, manually-driven alternative — the direct mirror
of the C++ ``aorta::Executor`` (``cpp/include/aorta/executor.h``): create one,
:meth:`~Executor.add_node`, then drive it yourself with :meth:`~Executor.spin`
(blocking) or :meth:`~Executor.spin_once` (cooperative), mint executor-scoped
callback groups, read admission/queue stats, and stop it through a detachable
:class:`StopHandle`.

**One model per node.** Do not drive a node with an explicit ``Executor`` *and*
call the auto-executor triggers (``create_timer`` / ``create_context_store().watch``
/ ``create_execution_group``) on the same node — both event loops would pull from
the node and race. Pick one.
"""

from __future__ import annotations

import ctypes
import dataclasses
import datetime
from enum import IntEnum
from typing import Union

from . import errors
from ._ffi import bindings, loader, structs
from .options import CallbackGroup

# spin_once timeout: seconds (float/int) or a timedelta.
Timeout = Union[float, int, datetime.timedelta]


class ExecutorType(IntEnum):
    """``aorta_executor_type_t`` variants constructible via ``aorta_executor_new``.

    ``CURRENT`` (2) is intentionally omitted — the C-ABI constructor only builds
    Single / Multi / Async backends.
    """

    SINGLE_THREADED = bindings.AORTA_EXECUTOR_SINGLE  # 0 — one worker, ordered
    MULTI_THREADED = bindings.AORTA_EXECUTOR_MULTI  # 1 — worker pool
    ASYNC = bindings.AORTA_EXECUTOR_ASYNC  # 3 — pool with a bounded in-flight cap


@dataclasses.dataclass(frozen=True)
class ExecutorAdmissionStats:
    """A snapshot of an executor's event-queue admission counters
    (``aorta_executor_admission_stats_t``)."""

    queued_events: int
    queued_bytes: int
    peak_queued_events: int
    peak_queued_bytes: int
    dropped_queue_full: int
    dropped_queue_closed: int
    dropped_budget_events: int
    dropped_budget_bytes: int

    @classmethod
    def _from_ffi(cls, raw: "structs.AortaExecutorAdmissionStats") -> "ExecutorAdmissionStats":
        return cls(
            queued_events=int(raw.queued_events),
            queued_bytes=int(raw.queued_bytes),
            peak_queued_events=int(raw.peak_queued_events),
            peak_queued_bytes=int(raw.peak_queued_bytes),
            dropped_queue_full=int(raw.dropped_queue_full),
            dropped_queue_closed=int(raw.dropped_queue_closed),
            dropped_budget_events=int(raw.dropped_budget_events),
            dropped_budget_bytes=int(raw.dropped_budget_bytes),
        )


class StopHandle:
    """A detachable handle that stops an :class:`Executor` (C++ ``StopHandle``).

    :meth:`stop` may be called from inside a callback running in the executor's
    :meth:`Executor.spin` (it takes only a brief registry lock); it is **not**
    async-signal-safe — from a signal handler post to a self-pipe and call
    :meth:`stop` from a normal thread. ``close()`` (or the context manager) frees
    the registry entry; freeing twice is safe.
    """

    def __init__(self, lib: ctypes.CDLL, handle: int) -> None:
        self._lib = lib
        self._handle = handle
        self._closed = False

    def stop(self) -> None:
        """Signal the associated executor to stop spinning."""
        rc = self._lib.aorta_stop_handle_stop(self._handle)
        errors.check_rc(self._lib, rc, "aorta_stop_handle_stop")

    def close(self) -> None:
        """Free the handle. Idempotent."""
        if self._closed:
            return
        self._closed = True
        # Best-effort: a freed/never-allocated handle returns NOT_FOUND, which is
        # benign on a teardown path; double-free is safe in Core.
        self._lib.aorta_stop_handle_free(self._handle)

    def __enter__(self) -> "StopHandle":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


def _timeout_to_ms(timeout: Timeout) -> int:
    """Seconds (or a timedelta) → non-negative integer milliseconds."""
    if isinstance(timeout, datetime.timedelta):
        ms = timeout.total_seconds() * 1000.0
    else:
        ms = float(timeout) * 1000.0
    return max(0, int(ms))


class Executor:
    """An explicit executor. Mirror of C++ ``aorta::Executor``.

    Use as a context manager or call :meth:`close` explicitly. The common path is
    the Node auto-executor; reach for this only when you need to drive spinning
    yourself, mint executor-scoped callback groups, or read admission stats.
    """

    def __init__(self, executor_type: ExecutorType = ExecutorType.SINGLE_THREADED) -> None:
        self._lib = bindings.configure(loader.library())
        self._closed = False
        out = bindings.aorta_exec_t(0)
        rc = self._lib.aorta_executor_new(int(executor_type), ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_executor_new")
        self._handle = out.value

    @classmethod
    def create(cls, executor_type: ExecutorType = ExecutorType.SINGLE_THREADED) -> "Executor":
        """C++ ``Executor::Create`` spelling; equivalent to the constructor."""
        return cls(executor_type)

    def handle(self) -> int:
        """The underlying ``aorta_exec_t`` handle."""
        return self._handle

    def add_node(self, node) -> None:
        """Attach ``node`` so its timers / callback-group work spin on this executor."""
        self._ensure_open()
        rc = self._lib.aorta_executor_add_node(self._handle, node._handle)
        errors.check_rc(self._lib, rc, "aorta_executor_add_node")

    def create_callback_group(self, group: CallbackGroup = CallbackGroup.MUTUALLY_EXCLUSIVE) -> int:
        """Mint an executor-scoped callback group; returns its handle (stamp it
        onto a primitive's ``callback_group_handle``)."""
        self._ensure_open()
        out = bindings.aorta_callback_group_handle_t(0)
        rc = self._lib.aorta_executor_create_callback_group(
            self._handle, int(group), ctypes.byref(out)
        )
        errors.check_rc(self._lib, rc, "aorta_executor_create_callback_group")
        return out.value

    def free_callback_group(self, handle: int) -> None:
        """Release a callback group previously minted by :meth:`create_callback_group`."""
        self._ensure_open()
        rc = self._lib.aorta_executor_free_callback_group(self._handle, handle)
        errors.check_rc(self._lib, rc, "aorta_executor_free_callback_group")

    def get_admission_stats(self) -> ExecutorAdmissionStats:
        """Snapshot the event-queue admission counters (C++ ``GetAdmissionStats``)."""
        self._ensure_open()
        raw = structs.AortaExecutorAdmissionStats()
        rc = self._lib.aorta_executor_get_admission_stats(self._handle, ctypes.byref(raw))
        errors.check_rc(self._lib, rc, "aorta_executor_get_admission_stats")
        return ExecutorAdmissionStats._from_ffi(raw)

    def spin(self) -> None:
        """Block, draining the event queue until stopped (ctypes releases the GIL,
        so worker callbacks run). Call from a non-callback thread."""
        self._ensure_open()
        rc = self._lib.aorta_executor_spin(self._handle)
        errors.check_rc(self._lib, rc, "aorta_executor_spin")

    def spin_once(self, timeout: Timeout = 0.0) -> bool:
        """Process at most one event, waiting up to ``timeout`` (seconds or a
        ``timedelta``). Returns ``True`` if an event ran (or the executor was
        stopped), ``False`` on timeout."""
        self._ensure_open()
        rc = self._lib.aorta_executor_spin_once(self._handle, _timeout_to_ms(timeout))
        if rc == int(errors.ResultCode.TIMEOUT):
            return False
        errors.check_rc(self._lib, rc, "aorta_executor_spin_once")
        return True

    def stop(self) -> None:
        """Signal the spin loop to stop (C++ ``Stop``)."""
        self._ensure_open()
        rc = self._lib.aorta_executor_stop(self._handle)
        errors.check_rc(self._lib, rc, "aorta_executor_stop")

    def get_stop_handle(self) -> StopHandle:
        """Obtain a detachable :class:`StopHandle` (C++ ``GetStopHandle``)."""
        self._ensure_open()
        out = bindings.aorta_stop_handle_t(0)
        rc = self._lib.aorta_executor_get_stop_handle(self._handle, ctypes.byref(out))
        errors.check_rc(self._lib, rc, "aorta_executor_get_stop_handle")
        return StopHandle(self._lib, out.value)

    def close(self) -> None:
        """Stop + free the executor (destructor-like). Idempotent, best-effort.

        Wakes any in-progress :meth:`spin` (so the free does not block forever on
        the spin lock), then frees. If you spun on your own thread, stop + join it
        before calling this. A destructor must not raise, so the return codes of
        the underlying stop/free are intentionally ignored."""
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_executor_stop(self._handle)
        self._lib.aorta_executor_free(self._handle)

    def _ensure_open(self) -> None:
        if self._closed:
            raise errors.AortaError("operation on a closed Executor")

    def __enter__(self) -> "Executor":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
