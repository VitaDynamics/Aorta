"""Timer tests — live (against the auto-executor) + direct trampoline-branch
unit tests + option/stats marshaling.
"""

from __future__ import annotations

import datetime
import time

import pytest

import aorta
from aorta import options as O
from aorta import timer as _timer
from aorta._ffi import structs


def _poll_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture()
def node(lib):
    n = aorta.Node("py_timer_test", group="py_test")
    yield n
    n.close()


# ── live ──────────────────────────────────────────────────────────────────────
def test_timer_fires_and_accessors(node):
    ticks = []
    t = node.create_timer(0.05, lambda: ticks.append(1))
    assert t.is_running()
    assert t.period_ns() == 50_000_000
    assert abs(t.period() - 0.05) < 1e-9
    assert _poll_until(lambda: len(ticks) >= 3), f"only {len(ticks)} ticks"


def test_timer_pause_resume(node):
    ticks = []
    t = node.create_timer(0.05, lambda: ticks.append(1))
    assert _poll_until(lambda: len(ticks) >= 2)
    t.pause()
    assert not t.is_running()
    paused_at = len(ticks)
    time.sleep(0.2)
    assert len(ticks) == paused_at  # no ticks while paused
    t.resume()
    assert t.is_running()
    assert _poll_until(lambda: len(ticks) > paused_at)


def test_timer_stats(node):
    t = node.create_timer(0.02, lambda: None)
    assert _poll_until(lambda: t.stats().due_firings_total >= 1)
    st = t.stats()
    assert isinstance(st, aorta.TimerStats)
    assert st.due_firings_total >= 1
    assert st.deadline_slip_max_ns >= 0


def test_timer_auto_start_false(node):
    t = node.create_timer(0.05, lambda: None, options=aorta.TimerOptions(auto_start=False))
    assert not t.is_running()
    t.resume()
    assert t.is_running()


def test_timer_callback_exception_is_swallowed(node):
    ticks = []

    def cb():
        ticks.append(1)
        raise ValueError("boom")  # must not crash the process

    node.create_timer(0.05, cb)
    assert _poll_until(lambda: len(ticks) >= 2)  # keeps firing despite raising


def test_timer_period_timedelta(node):
    t = node.create_timer(datetime.timedelta(milliseconds=40), lambda: None)
    assert t.period_ns() == 40_000_000


def test_timer_invalid_period(node):
    with pytest.raises(aorta.InvalidArgumentError):
        node.create_timer(0, lambda: None)
    with pytest.raises(aorta.InvalidArgumentError):
        node.create_timer(-1.0, lambda: None)


def test_timer_context_manager_and_idempotent_close(node):
    with node.create_timer(0.05, lambda: None) as t:
        assert t.is_running()
    assert t._closed
    t.close()  # idempotent
    t.stop()  # alias for close, also idempotent


def test_timer_ops_on_closed_raise(node):
    t = node.create_timer(0.05, lambda: None)
    t.close()
    for op in (t.pause, t.resume, t.is_running, t.period_ns, t.stats):
        with pytest.raises(aorta.AortaError, match="closed"):
            op()


def test_ensure_executor_is_idempotent(node):
    node.ensure_executor()
    first = node._executor
    node.ensure_executor()
    assert node._executor == first  # reused, not recreated
    # two timers share the one executor
    node.create_timer(0.05, lambda: None)
    node.create_timer(0.05, lambda: None)
    assert node._executor == first


def test_concurrent_ensure_executor_creates_one(node):
    # ensure_executor is lock-guarded (DCL); concurrent callers
    # must end up with a single executor, not one-per-thread.
    import threading

    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()  # release all threads together to maximise contention
        node.ensure_executor()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert node._executor is not None
    ticks = []
    node.create_timer(0.04, lambda: ticks.append(1))
    assert _poll_until(lambda: len(ticks) >= 2)


def test_create_timer_on_closed_node(lib):
    n = aorta.Node("py_timer_closed", group="py_test")
    n.close()
    with pytest.raises(aorta.AortaError, match="closed"):
        n.create_timer(0.05, lambda: None)


def test_timer_create_failure_unregisters_token(lib):
    # Constructing a Timer directly (bypassing create_timer's ensure_executor)
    # fails in Core ("timer requires an executor"); the token must be cleaned up.
    n = aorta.Node("py_timer_noexec", group="py_test")
    before = len(_timer._handlers)
    try:
        with pytest.raises(aorta.AortaError):
            _timer.Timer(n, 0.05, lambda: None)
        assert len(_timer._handlers) == before  # token unregistered, no leak
    finally:
        n.close()


def test_subscriber_runtime_stats(node):
    sub = node.create_subscriber("/py_test/timer/rt", lambda p, c: None)
    rs = sub.runtime_stats()
    assert isinstance(rs, aorta.SubscriberRuntimeStats)
    assert rs.duplicate_drop_count == 0
    assert rs.out_of_order_count == 0


# ── direct trampoline-branch unit tests ──────────────────────────────────────
def test_timer_dispatch_unknown_token():
    _timer._dispatch(99_999_999)  # no handler -> no-op, must not raise
    _timer._dispatch(None)  # null user_data -> no-op


def test_timer_dispatch_happy_path_and_exception():
    fired = []
    token = _timer._register(lambda: fired.append(1))
    try:
        _timer._dispatch(token)
        assert fired == [1]
    finally:
        _timer._unregister(token)
    # exception swallowed
    bad = _timer._register(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        _timer._dispatch(bad)  # must not raise
    finally:
        _timer._unregister(bad)


# ── option/stats marshaling ──────────────────────────────────────────────────
def test_timer_overflow_policy_enum():
    assert (O.TimerOverflowPolicy.COALESCE_LATEST, O.TimerOverflowPolicy.BLOCK) == (0, 1)


def test_build_timer_options_none_and_fields():
    assert O.build_timer_options(None) == (None, ())
    opts = O.TimerOptions(
        callback_group=O.CallbackGroup.REENTRANT,
        auto_start=False,
        invoke_on_missed=True,
        overflow_policy=O.TimerOverflowPolicy.BLOCK,
        overflow_block_timeout_ms=250,
        callback_group_handle=7,
    )
    t, keep = O.build_timer_options(opts)
    assert t.callback_group == int(O.CallbackGroup.REENTRANT)
    assert t.auto_start == 0 and t.invoke_on_missed == 1
    assert t.overflow_policy == int(O.TimerOverflowPolicy.BLOCK)
    assert t.overflow_block_timeout_ms == 250
    assert t.callback_group_handle == 7
    assert t in keep


def test_timer_stats_construction():
    st = aorta.TimerStats(*range(13))
    assert st.coalesced_tick_count == 0
    assert st.coalesce_event_wait_sample_count == 12


def test_subscriber_runtime_stats_from_ffi():
    raw = structs.AortaSubscriberRuntimeStats()
    raw.duplicate_drop_count = 3
    raw.overflow_relay_drop_count = 9
    rs = O.SubscriberRuntimeStats._from_ffi(raw)
    assert rs.duplicate_drop_count == 3
    assert rs.overflow_relay_drop_count == 9
    assert rs.executor_drop_count == 0
