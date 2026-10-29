"""ExecutionGroup tests — creation, option stamping, policy mapping, lifecycle."""

from __future__ import annotations

import time

import pytest

import aorta


@pytest.fixture()
def node(lib):
    n = aorta.Node("py_eg_test", group="py_test")
    yield n
    n.close()


def test_create_and_accessors(node):
    eg = node.create_execution_group(aorta.ExecutionPolicy.SERIALIZED)
    assert eg.handle() != 0
    assert eg.policy() == aorta.ExecutionPolicy.SERIALIZED


def test_default_policy_is_serialized(node):
    eg = node.create_execution_group()
    assert eg.policy() == aorta.ExecutionPolicy.SERIALIZED


def test_timer_options_stamping(node):
    eg = node.create_execution_group(aorta.ExecutionPolicy.CONCURRENT)
    # default base — both the handle AND the callback_group enum are stamped
    t = eg.timer_options()
    assert t.callback_group_handle == eg.handle()
    assert t.callback_group == aorta.CallbackGroup.REENTRANT  # CONCURRENT
    assert t.auto_start is True  # default preserved
    # explicit base preserved + stamped
    base = aorta.TimerOptions(auto_start=False, overflow_block_timeout_ms=9)
    t2 = eg.timer_options(base)
    assert t2.callback_group_handle == eg.handle()
    assert t2.callback_group == aorta.CallbackGroup.REENTRANT
    assert t2.auto_start is False and t2.overflow_block_timeout_ms == 9
    assert base.callback_group_handle == 0  # original not mutated


def test_service_options_stamping(node):
    eg = node.create_execution_group(aorta.ExecutionPolicy.SERIALIZED)
    s = eg.service_options()
    assert s.callback_group_handle == eg.handle()
    assert s.callback_group == aorta.CallbackGroup.MUTUALLY_EXCLUSIVE
    base = aorta.ServiceOptions(rpc_overflow_block_timeout_ms=7)
    s2 = eg.service_options(base)
    assert s2.callback_group_handle == eg.handle()
    assert s2.rpc_overflow_block_timeout_ms == 7


def test_stamping_after_close_raises(node):
    eg = node.create_execution_group()
    eg.close()
    assert eg._handle == 0  # freed handle is zeroed, never re-stamped
    with pytest.raises(aorta.AortaError, match="closed"):
        eg.timer_options()
    with pytest.raises(aorta.AortaError, match="closed"):
        eg.service_options()


def test_subscriber_options_stamping(node):
    eg = node.create_execution_group(aorta.ExecutionPolicy.CONCURRENT)
    s = eg.subscriber_options()
    assert s.callback_group_handle == eg.handle()
    assert s.callback_group == aorta.CallbackGroup.REENTRANT
    base = aorta.SubscriberOptions(enable_dedup=True, dedup_window=64)
    s2 = eg.subscriber_options(base)
    assert s2.callback_group_handle == eg.handle()
    assert s2.enable_dedup is True and s2.dedup_window == 64
    # a subscriber bound to the group
    node.create_subscriber("/py_test/eg/sub", lambda p, c: None,
                           options=eg.subscriber_options())


def test_bound_timer_fires(node):
    eg = node.create_execution_group(aorta.ExecutionPolicy.SERIALIZED)
    ticks = []
    node.create_timer(0.04, lambda: ticks.append(1), options=eg.timer_options())
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and len(ticks) < 2:
        time.sleep(0.02)
    assert len(ticks) >= 2


def test_policy_mapping():
    assert aorta.ExecutionPolicy.SERIALIZED.to_callback_group() == aorta.CallbackGroup.MUTUALLY_EXCLUSIVE
    assert aorta.ExecutionPolicy.CONCURRENT.to_callback_group() == aorta.CallbackGroup.REENTRANT


def test_context_manager_and_idempotent_close(node):
    with node.create_execution_group(aorta.ExecutionPolicy.CONCURRENT) as eg:
        assert eg.handle() != 0
    assert eg._closed
    eg.close()  # idempotent


def test_create_on_closed_node(lib):
    n = aorta.Node("py_eg_closed", group="py_test")
    n.close()
    with pytest.raises(aorta.AortaError, match="closed"):
        n.create_execution_group()
