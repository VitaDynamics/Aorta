"""ContextStore tests — live (set/get/watch) + direct decode/trampoline-branch
unit tests + enum/dataclass coverage.
"""

from __future__ import annotations

import time

import pytest

import aorta
from aorta import contextstore as cs_mod
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
    n = aorta.Node("py_ctx_test", group="py_test")
    yield n
    n.close()


@pytest.fixture()
def store(node):
    cs = node.create_context_store()
    cs.declare_owned_prefix("py/ctx/**")
    return cs


# ── live ──────────────────────────────────────────────────────────────────────
def test_declare_and_is_key_declared(store):
    assert store.declared_prefixes() == ["py/ctx/**"]
    assert store.is_key_declared("py/ctx/x") is True
    assert store.is_key_declared("other/y") is False


def test_set_get_all_types(store):
    store.set_string("py/ctx/s", "hello")
    store.set_i64("py/ctx/i", -7)
    store.set_u64("py/ctx/u", 9)
    store.set_bool("py/ctx/b", True)
    store.set_f64("py/ctx/f", 1.5)
    assert _poll_until(lambda: store.get_string("py/ctx/s") is not None)
    rs = store.get_string("py/ctx/s")
    assert rs.value == "hello"
    assert rs.is_fresh is True
    assert rs.value_kind == aorta.ContextKind.STRING
    assert rs.source_node == "py_ctx_test"
    assert rs.sequence >= 1
    assert store.get_i64("py/ctx/i").value == -7
    assert store.get_u64("py/ctx/u").value == 9
    assert store.get_bool("py/ctx/b").value is True
    assert store.get_f64("py/ctx/f").value == 1.5


def test_get_missing_returns_none(store):
    assert store.get_string("py/ctx/absent") is None
    assert store.get_i64("py/ctx/absent") is None


def test_declared_prefixes_exceeding_buffer(node):
    # Declare enough long disjoint prefixes that the newline-joined output
    # exceeds the 4096-byte first-pass buffer, exercising the resize+retry path
    # (guards against silent truncation).
    cs = node.create_context_store()
    # Each keyexpr segment caps at 128 bytes, so use two long segments + an
    # index to stay disjoint; ~250 bytes/prefix x 20 -> ~5KB joined (> 4096).
    prefixes = [f"py/big/{'a' * 120}/{'b' * 120}/{i}/**" for i in range(20)]
    for p in prefixes:
        cs.declare_owned_prefix(p)
    got = cs.declared_prefixes()
    assert len(got) == 20
    assert set(got) == set(prefixes)  # nothing truncated
    cs.close()


def test_set_with_expiry(store):
    store.set_string("py/ctx/exp", "v", expires_at_ns=10**19)  # far future
    assert _poll_until(lambda: store.get_string("py/ctx/exp") is not None)
    r = store.get_string("py/ctx/exp")
    assert r.expires_at_ns == 10**19


def test_set_on_undeclared_prefix_raises(store):
    with pytest.raises(aorta.PrefixNotDeclaredError):
        store.set_string("not/declared/key", "x")


def test_watch_live(store):
    changes = []
    wh = store.watch("py/ctx/**", lambda c: changes.append(c))
    time.sleep(0.3)
    store.set_string("py/ctx/w", "live")
    assert _poll_until(lambda: any(c.kind == aorta.ContextChangeKind.LIVE for c in changes))
    live = [c for c in changes if c.kind == aorta.ContextChangeKind.LIVE]
    assert live[0].key.endswith("py/ctx/w")
    assert live[0].value == "live"
    wh.shutdown()
    wh.shutdown()  # idempotent


def test_watch_with_initial_snapshot(store):
    store.set_string("py/ctx/snap", "before")
    time.sleep(0.2)
    changes = []
    with store.watch_with_initial_snapshot(
        "py/ctx/**", lambda c: changes.append(c),
        options=aorta.WatchSnapshotOptions(snapshot_deadline_ms=1500),
    ) as wh:
        assert _poll_until(lambda: any(c.is_snapshot_done() for c in changes))
        kinds = [c.kind for c in changes]
        assert aorta.ContextChangeKind.SNAPSHOT in kinds
        assert aorta.ContextChangeKind.SNAPSHOT_DONE in kinds
    assert wh._closed


def test_context_store_context_manager_and_closed_ops(node):
    with node.create_context_store() as cs:
        cs.declare_owned_prefix("py/cm/**")
    assert cs._closed
    cs.close()  # idempotent
    for op in (
        lambda: cs.declare_owned_prefix("x/**"),
        lambda: cs.is_key_declared("x"),
        lambda: cs.declared_prefixes(),
        lambda: cs.set_string("x", "y"),
        lambda: cs.get_string("x"),
        lambda: cs.shutdown(),
    ):
        with pytest.raises(aorta.AortaError, match="closed"):
            op()


def test_shutdown_then_close(store):
    store.shutdown()
    store.close()  # close after shutdown is fine


def test_close_closes_open_watches(store):
    wh = store.watch("py/ctx/**", lambda c: None)
    store.close()
    assert wh._closed  # node/store close tears the watch down


def test_watch_invalid_prefix_unregisters(store):
    before = len(cs_mod._watch_handlers)
    with pytest.raises(aorta.AortaError):
        store.watch("", lambda c: None)  # empty keyexpr is rejected by Core
    assert len(cs_mod._watch_handlers) == before  # token cleaned up


# ── direct decode / trampoline-branch unit tests ─────────────────────────────
def _make_change(kind, value_kind, *, snapshot_done_reason=0xFF, i=0, u=0, f=0.0, b=0):
    c = structs.AortaCtxChange()
    c.kind = kind
    c.snapshot_done_reason = snapshot_done_reason
    c.value_kind = value_kind
    c.i64_value = i
    c.u64_value = u
    c.f64_value = f
    c.bool_value = b
    # leave string views NULL/zero (str_from_view -> "")
    return c


def test_change_from_struct_all_value_kinds():
    K = aorta.ContextKind
    assert cs_mod.change_from_struct(_make_change(0, K.BOOL, b=1)).value is True
    assert cs_mod.change_from_struct(_make_change(0, K.I64, i=-3)).value == -3
    assert cs_mod.change_from_struct(_make_change(0, K.U64, u=4)).value == 4
    assert cs_mod.change_from_struct(_make_change(0, K.F64, f=2.5)).value == 2.5
    assert cs_mod.change_from_struct(_make_change(0, K.STRING)).value == ""  # null view
    none_change = cs_mod.change_from_struct(_make_change(0, K.NONE))
    assert none_change.value is None


def test_change_from_struct_snapshot_done():
    c = _make_change(2, 0, snapshot_done_reason=1)  # SNAPSHOT_DONE / DEADLINE_ELAPSED
    change = cs_mod.change_from_struct(c)
    assert change.kind == aorta.ContextChangeKind.SNAPSHOT_DONE
    assert change.snapshot_done_reason == aorta.SnapshotDoneReason.DEADLINE_ELAPSED
    assert change.is_snapshot_done() is True
    # non-snapshot-done -> reason None
    live = cs_mod.change_from_struct(_make_change(0, 0))
    assert live.snapshot_done_reason is None
    assert live.is_snapshot_done() is False


def test_owner_liveliness_watch_reports_alive_and_gone(node):
    """Owner-liveliness surface test: a second store (owner) declares a
    prefix, the watcher sees alive, then shutdown -> gone."""
    owner_node = aorta.Node("py_ctx_liv_owner", group="py_test")
    try:
        owner = owner_node.create_context_store()
        owner.declare_owned_prefix("py/liv/**")

        events = []
        with node.create_context_store() as watcher:
            wh = watcher.watch_ctx_owner_liveliness(lambda e: events.append(e))
            # history(true) replays the already-alive owner token.
            assert _poll_until(lambda: any(e.alive for e in events))
            alive = [e for e in events if e.alive][0]
            assert alive.node_name == "py_ctx_liv_owner"
            assert len(alive.source_instance_id) == 16

            owner.shutdown()
            assert _poll_until(lambda: any(not e.alive for e in events))
            gone = [e for e in events if not e.alive][0]
            assert gone.node_name == "py_ctx_liv_owner"
            assert gone.source_instance_id == alive.source_instance_id

            wh.shutdown()
            wh.shutdown()  # idempotent
    finally:
        owner_node.close()


def test_owner_liveliness_watch_stops_after_shutdown(node):
    """After handle shutdown no further callback fires: a late owner
    shutdown must not produce a gone event."""
    owner_node = aorta.Node("py_ctx_liv_stop", group="py_test")
    try:
        owner = owner_node.create_context_store()
        owner.declare_owned_prefix("py/liv_stop/**")

        events = []
        with node.create_context_store() as watcher:
            wh = watcher.watch_ctx_owner_liveliness(lambda e: events.append(e))
            assert _poll_until(lambda: any(e.alive for e in events))
            wh.shutdown()
            owner.shutdown()
            time.sleep(0.3)
            assert all(e.alive for e in events), "no event may fire after shutdown"
    finally:
        owner_node.close()


def test_watch_dispatch_branches():
    import ctypes

    fired = []
    token = cs_mod._register_watch(lambda c: fired.append(c))
    try:
        c = _make_change(0, aorta.ContextKind.I64, i=11)
        cs_mod._watch_dispatch(ctypes.pointer(c), token)
        assert fired and fired[0].value == 11
    finally:
        cs_mod._unregister_watch(token)
    # unknown token / null ptr / null user_data -> no-op
    cs_mod._watch_dispatch(ctypes.pointer(_make_change(0, 0)), 99_999_999)
    cs_mod._watch_dispatch(None, 1)
    cs_mod._watch_dispatch(ctypes.pointer(_make_change(0, 0)), None)
    # raising callback is swallowed
    bad = cs_mod._register_watch(lambda c: (_ for _ in ()).throw(RuntimeError("x")))
    try:
        cs_mod._watch_dispatch(ctypes.pointer(_make_change(0, 0)), bad)
    finally:
        cs_mod._unregister_watch(bad)


def _make_liv_event(*, node_name=b"n1", instance=b"0123456789abcdef", alive=True):
    import ctypes

    ev = structs.AortaCtxOwnerLivelinessEvent()
    node = ctypes.create_string_buffer(node_name)
    inst = ctypes.create_string_buffer(instance)
    ev.node_name = structs.AortaStringView(ctypes.cast(node, ctypes.c_void_p), len(node_name))
    ev.source_instance_id = structs.AortaStringView(
        ctypes.cast(inst, ctypes.c_void_p), len(instance)
    )
    ev.alive = alive
    return ev


def test_owner_liveliness_dispatch_branches():
    import ctypes

    fired = []
    token = cs_mod._register_watch(lambda e: fired.append(e))
    try:
        ev = _make_liv_event(node_name=b"py_node", instance=b"0123456789abcdef", alive=True)
        cs_mod._owner_liveliness_dispatch(ctypes.pointer(ev), token)
        assert fired and fired[0].node_name == "py_node"
        assert fired[0].source_instance_id == "0123456789abcdef"
        assert fired[0].alive is True
    finally:
        cs_mod._unregister_watch(token)
    # unknown token / null ptr / null user_data -> no-op
    cs_mod._owner_liveliness_dispatch(ctypes.pointer(_make_liv_event()), 99_999_999)
    cs_mod._owner_liveliness_dispatch(None, 1)
    cs_mod._owner_liveliness_dispatch(ctypes.pointer(_make_liv_event()), None)
    # raising callback is swallowed
    bad = cs_mod._register_watch(lambda e: (_ for _ in ()).throw(RuntimeError("x")))
    try:
        cs_mod._owner_liveliness_dispatch(ctypes.pointer(_make_liv_event()), bad)
    finally:
        cs_mod._unregister_watch(bad)


def test_owner_liveliness_watch_context_manager(node):
    """`with handle:` teardown + no event after close."""
    owner_node = aorta.Node("py_ctx_liv_cm", group="py_test")
    try:
        owner = owner_node.create_context_store()
        owner.declare_owned_prefix("py/liv_cm/**")
        events = []
        with node.create_context_store() as watcher:
            with watcher.watch_ctx_owner_liveliness(lambda e: events.append(e)) as wh:
                assert _poll_until(lambda: any(e.alive for e in events))
            assert wh._closed
            owner.shutdown()
            time.sleep(0.3)
            assert all(e.alive for e in events), "no event may fire after close"
    finally:
        owner_node.close()


def test_watch_shutdown_free_error_keeps_token(store):
    """When the FFI free fails (handle already freed -> NOT_FOUND),
    shutdown() must raise AND keep the token registered + stay open —
    the FFI may still own the watch and deliver callbacks."""
    events = []
    wh = store.watch("py/ctx_err/**", lambda c: events.append(c))
    # Free the FFI watch behind the wrapper's back.
    assert wh._lib.aorta_ctx_watch_free(wh._handle) == 0
    with pytest.raises(aorta.AortaError):
        wh.shutdown()
    assert wh._closed is False
    assert wh._token in cs_mod._watch_handlers  # token still registered
    cs_mod._unregister_watch(wh._token)  # test cleanup


def test_owner_liveliness_watch_shutdown_free_error_keeps_token(node):
    """Same for the owner-liveliness watch free path."""
    owner_node = aorta.Node("py_ctx_liv_err", group="py_test")
    try:
        owner = owner_node.create_context_store()
        owner.declare_owned_prefix("py/liv_err/**")
        with node.create_context_store() as watcher:
            wh = watcher.watch_ctx_owner_liveliness(lambda e: None)
            assert wh._lib.aorta_ctx_owner_liveliness_watch_free(wh._handle) == 0
            with pytest.raises(aorta.AortaError):
                wh.shutdown()
            assert wh._closed is False
            assert wh._token in cs_mod._watch_handlers  # token still registered
            cs_mod._unregister_watch(wh._token)  # test cleanup
    finally:
        owner_node.close()


def test_owner_liveliness_watch_stale_store_unregisters(node):
    """rc != 0 (stale store handle) must unregister the callback token."""
    before = len(cs_mod._watch_handlers)
    cs = node.create_context_store()
    # Invalidate the store handle at the FFI boundary (the registry
    # lookup then fails with NOT_FOUND on the watch call).
    cs._lib.aorta_ctx_free(cs._handle)
    cs._handle = 0
    with pytest.raises(aorta.AortaError):
        cs.watch_ctx_owner_liveliness(lambda e: None)
    assert len(cs_mod._watch_handlers) == before  # token cleaned up
    cs._closed = True  # mark so the object never double-frees


def test_cstr_to_str_null():
    assert cs_mod._cstr_to_str(None) == ""


def test_enums():
    assert aorta.ContextKind.STRING == 5
    assert aorta.ContextChangeKind.SNAPSHOT_DONE == 2
    assert aorta.SnapshotDoneReason.QUERY_DRAINED == 0
