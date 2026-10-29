"""Tests for the explicit :class:`aorta.Executor` (C++ ``Executor`` parity).

Live tests drive a real executor; direct tests cover the pure marshaling
(enum values, admission-stats decode, timeout conversion).
"""

from __future__ import annotations

import datetime
import threading
import time

import pytest

import aorta
from aorta._ffi import structs
from aorta.executor import _timeout_to_ms


@pytest.fixture()
def node(lib):
    n = aorta.Node("py_exec_test", group="py_test")
    yield n
    n.close()


# ── direct / unit (no live session) ───────────────────────────────────────────
def test_executor_type_values():
    assert (
        aorta.ExecutorType.SINGLE_THREADED,
        aorta.ExecutorType.MULTI_THREADED,
        aorta.ExecutorType.ASYNC,
    ) == (0, 1, 3)


def test_timeout_to_ms_seconds_and_clamp():
    assert _timeout_to_ms(0.0) == 0
    assert _timeout_to_ms(1) == 1000
    assert _timeout_to_ms(0.25) == 250
    assert _timeout_to_ms(-5) == 0  # negative clamps to 0


def test_timeout_to_ms_timedelta():
    assert _timeout_to_ms(datetime.timedelta(milliseconds=50)) == 50
    assert _timeout_to_ms(datetime.timedelta(seconds=2)) == 2000


def test_admission_stats_from_ffi():
    raw = structs.AortaExecutorAdmissionStats()
    raw.queued_events = 3
    raw.queued_bytes = 128
    raw.peak_queued_events = 5
    raw.peak_queued_bytes = 256
    raw.dropped_queue_full = 1
    raw.dropped_queue_closed = 2
    raw.dropped_budget_events = 4
    raw.dropped_budget_bytes = 512
    s = aorta.ExecutorAdmissionStats._from_ffi(raw)
    assert (s.queued_events, s.queued_bytes) == (3, 128)
    assert (s.peak_queued_events, s.peak_queued_bytes) == (5, 256)
    assert (s.dropped_queue_full, s.dropped_queue_closed) == (1, 2)
    assert (s.dropped_budget_events, s.dropped_budget_bytes) == (4, 512)


# ── live ───────────────────────────────────────────────────────────────────────
def test_create_via_constructor_and_classmethod(lib):
    ex = aorta.Executor()
    assert ex.handle() != 0
    ex.close()
    ex2 = aorta.Executor.create(aorta.ExecutorType.SINGLE_THREADED)
    assert ex2.handle() != 0
    ex2.close()


def test_add_node_and_spin_once_timeout(node):
    with aorta.Executor() as ex:
        ex.add_node(node)
        # an idle executor has no event within the timeout
        assert ex.spin_once(0.02) is False


def test_stop_makes_spin_once_return_true(node):
    ex = aorta.Executor(aorta.ExecutorType.SINGLE_THREADED)
    ex.add_node(node)
    ex.stop()  # the explicit Stop() (not via a StopHandle)
    assert ex.spin_once(0.05) is True  # Stopped outcome -> True
    ex.close()


def test_spin_blocks_until_stop_handle(node):
    ex = aorta.Executor()
    ex.add_node(node)
    stop = ex.get_stop_handle()
    spun = threading.Thread(target=ex.spin, daemon=True)
    spun.start()
    time.sleep(0.1)
    stop.stop()
    spun.join(timeout=3.0)
    assert not spun.is_alive()  # spin returned after stop
    stop.close()
    ex.close()


def test_stop_handle_context_manager_and_idempotent_close():
    ex = aorta.Executor()
    with ex.get_stop_handle() as stop:
        stop.stop()
    stop.close()  # idempotent (already closed by __exit__)
    ex.close()


def test_create_and_free_callback_group():
    with aorta.Executor() as ex:
        h = ex.create_callback_group(aorta.CallbackGroup.REENTRANT)
        assert h != 0
        ex.free_callback_group(h)


def test_get_admission_stats(node):
    with aorta.Executor() as ex:
        ex.add_node(node)
        stats = ex.get_admission_stats()
        assert isinstance(stats, aorta.ExecutorAdmissionStats)
        assert stats.queued_events >= 0 and stats.dropped_queue_full >= 0


def test_multi_threaded_and_async_types(node):
    with aorta.Executor(aorta.ExecutorType.MULTI_THREADED) as ex:
        ex.add_node(node)
        assert ex.handle() != 0
    with aorta.Executor(aorta.ExecutorType.ASYNC) as ex:
        assert ex.handle() != 0


def test_subscriber_alone_does_not_suppress_auto_executor(node):
    # Guard the has_event_sender() early-return: a subscriber delivers on Zenoh
    # worker threads and does NOT attach an event sender, so a later timer must
    # still get its auto-executor (only an EXPLICIT executor suppresses it).
    assert node._has_event_sender() is False
    node.create_subscriber("/py_test/exec/subonly", lambda p, c: None)
    assert node._has_event_sender() is False  # subscriber did not attach a sender
    ticks = []
    node.create_timer(0.04, lambda: ticks.append(1))
    assert node._executor is not None  # auto-executor WAS created
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and len(ticks) < 2:
        time.sleep(0.02)
    assert len(ticks) >= 2  # timer fired on the auto-executor


def test_explicit_executor_suppresses_auto_executor(node):
    # Parity with C++ Node::EnsureExecutor: once a node is attached to an explicit
    # Executor, create_timer must NOT spin a second auto-executor — the explicit
    # one dispatches the timer.
    ticks = []
    ex = aorta.Executor()
    ex.add_node(node)
    timer = node.create_timer(0.04, lambda: ticks.append(1))
    assert node._executor is None  # auto-executor suppressed (node already attached)
    stop = ex.get_stop_handle()
    spun = threading.Thread(target=ex.spin, daemon=True)
    spun.start()
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and len(ticks) < 2:
        time.sleep(0.02)
    stop.stop()
    spun.join(timeout=3.0)
    stop.close()
    timer.close()
    ex.close()
    assert len(ticks) >= 2  # the explicit executor dispatched the timer


def test_close_idempotent_and_ops_after_close_raise(node):
    ex = aorta.Executor()
    ex.close()
    ex.close()  # idempotent
    for op in (
        lambda: ex.add_node(node),
        lambda: ex.spin(),
        lambda: ex.spin_once(0.0),
        lambda: ex.stop(),
        lambda: ex.get_stop_handle(),
        lambda: ex.create_callback_group(),
        lambda: ex.free_callback_group(1),
        lambda: ex.get_admission_stats(),
    ):
        with pytest.raises(aorta.AortaError, match="closed"):
            op()
