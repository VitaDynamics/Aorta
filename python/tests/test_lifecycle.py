"""Lifecycle + dispatch-edge tests for Node/Publisher/Subscriber."""

from __future__ import annotations

import pytest

import aorta
from aorta import subscriber as sub_mod


@pytest.fixture()
def open_node(lib):
    n = aorta.Node("py_lifecycle", group="py_test")
    yield n
    n.close()


def test_node_accessors_and_context_manager(lib):
    with aorta.Node("nm", group="grp") as n:
        assert n.name() == "nm"
        # group() is the EFFECTIVE group (Core ignores the ctor arg "grp").
        assert n.group() and n.group() != "grp"
        node_id = n.id()
        assert isinstance(node_id, str) and node_id
        now = n.now()
        assert isinstance(now, int) and now > 0
    # closed -> every operation raises
    for op in (
        lambda: n.create_publisher("/x"),
        lambda: n.create_subscriber("/x", lambda p, c: None),
        lambda: n.create_subscriber_typed(bytes, "/x", lambda m, c: None),
        n.id,
        n.group,
        n.now,
    ):
        with pytest.raises(aorta.errors.AortaError, match="closed"):
            op()


def test_node_double_close_idempotent(lib):
    n = aorta.Node("py_dc")
    n.create_publisher("/py/dc/p")  # a child to exercise the teardown loop
    n.close()
    n.close()  # no raise


def test_publisher_context_manager_and_double_close(open_node):
    pub = open_node.create_publisher("/py/lc/pub")
    assert pub.topic() == "/py/lc/pub"
    with pub:
        pass
    pub.close()  # idempotent


def test_subscriber_context_manager_and_double_close(open_node):
    sub = open_node.create_subscriber("/py/lc/sub", lambda p, c: None)
    assert sub.topic() == "/py/lc/sub"
    with sub:
        pass
    sub.close()  # idempotent


def test_subscriber_ctor_failure_unregisters_token(open_node, monkeypatch):
    before = len(sub_mod._handlers)
    # The ctor routes through aorta_subscriber_new_buffered (the buffering
    # knobs ride the companion struct); force it to fail and assert the token
    # is cleaned up.
    monkeypatch.setattr(
        open_node._lib, "aorta_subscriber_new_buffered", lambda *a: -2
    )
    with pytest.raises(aorta.errors.AortaError):
        open_node.create_subscriber("/py/lc/fail", lambda p, c: None)
    assert len(sub_mod._handlers) == before  # no leaked token


def test_dispatch_unknown_token_is_noop():
    # token not in the registry -> return without touching any handler
    sub_mod._dispatch(None, None, None, 999_999_999)
    sub_mod._dispatch(None, None, None, 0)  # null user_data branch


def test_dispatch_null_view_yields_empty_payload():
    got = []
    token = sub_mod._register(lambda payload, ctx: got.append((payload, ctx)))
    try:
        sub_mod._dispatch(None, None, None, token)
    finally:
        sub_mod._unregister(token)
    assert got[0][0] == b""
    assert got[0][1] == aorta.MessageContext("", 0, 0, "", 0)


def test_dispatch_swallows_handler_exception():
    def boom(payload, ctx):
        raise RuntimeError("must not cross the C boundary")

    token = sub_mod._register(boom)
    try:
        sub_mod._dispatch(None, None, None, token)  # must NOT raise
    finally:
        sub_mod._unregister(token)


def test_typed_decode_dispatch_success():
    got = []
    sub_mod.typed_decode_dispatch(
        lambda p: ("decoded", p), lambda m, c: got.append((m, c)), b"raw", "CTX"
    )
    assert got == [(("decoded", b"raw"), "CTX")]


def test_typed_decode_dispatch_failure_is_skipped():
    got = []

    def bad(payload):
        raise ValueError("nope")

    sub_mod.typed_decode_dispatch(bad, lambda m, c: got.append(m), b"raw", "CTX")
    assert got == []  # decode failed -> handler not called, no raise
