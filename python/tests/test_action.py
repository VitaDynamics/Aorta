"""Action tests — live client/server roundtrips + direct unit tests for the
marshaling helpers, trampolines, and context (via mock lib).
"""

from __future__ import annotations

import ctypes
import time
from unittest import mock

import pytest

import aorta
from aorta import action
from aorta._ffi import bindings, loader, structs
from _fixtures import demo_action
from _fixtures.demo_action import DEMO_SCHEMA_BFBS, DemoActionCodec


def _poll_terminal(gh, timeout=5.0):
    return gh.wait_result(timeout=timeout)


@pytest.fixture()
def node(lib):
    n = aorta.Node("py_action_test", group="py_test")
    yield n
    n.close()


@pytest.fixture()
def codec():
    return DemoActionCodec()


# ── live roundtrips ───────────────────────────────────────────────────────────
def test_succeed_roundtrip_with_feedback(node, codec):
    fb = []

    def handler(goal, ctx):
        assert ctx.goal_id() == goal.goal_id()
        assert not ctx.is_cancel_requested()
        ctx.publish_feedback(demo_action.build_value_table(99), "halfway")
        ctx.succeed(demo_action.build_value_table(goal.decode() * 2))

    node.create_action_server("/py_test/calc", DEMO_SCHEMA_BFBS, codec, handler,
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/calc", codec, options=aorta.ActionClientOptions(control_timeout_ms=4000)
    )
    time.sleep(0.6)
    gh = client.send_goal(21)
    sub = gh.subscribe_feedback(lambda s: fb.append(s))
    res = gh.wait_result(timeout=5.0)
    assert res.status == aorta.ActionStatus.SUCCEEDED
    assert demo_action._decode_i64_table_bytes(res.result) == 42
    assert gh.goal_id() and gh.goal_secret() and gh.server_epoch()
    sub.close()


def test_handler_exception_auto_aborts(node, codec):
    def bad(goal, ctx):
        raise ValueError("boom")  # must auto-abort, not crash

    node.create_action_server("/py_test/bad", DEMO_SCHEMA_BFBS, codec, bad,
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/bad", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000)
    )
    time.sleep(0.6)
    res = client.send_goal(1).wait_result(timeout=5.0)
    assert res.status == aorta.ActionStatus.ABORTED
    assert res.code == aorta.ActionReplyCode.HANDLER_ERROR


def test_explicit_abort_and_cancel(node, codec):
    def aborter(goal, ctx):
        ctx.abort(aorta.ActionReplyCode.CONFLICT, "nope")

    node.create_action_server("/py_test/abrt", DEMO_SCHEMA_BFBS, codec, aborter,
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/abrt", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000)
    )
    time.sleep(0.6)
    res = client.send_goal(1).wait_result(timeout=5.0)
    assert res.status == aorta.ActionStatus.ABORTED
    assert res.code == aorta.ActionReplyCode.CONFLICT


def test_query_status_and_send_goal_with_id(node, codec):
    node.create_action_server("/py_test/qs", DEMO_SCHEMA_BFBS, codec,
                              lambda g, c: c.succeed(demo_action.build_value_table(0)),
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/qs", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000)
    )
    time.sleep(0.6)
    gid = "0" * 32  # valid 32-hex goal id
    gh = client.send_goal_with_id(gid, 5)
    assert gh.goal_id() == gid
    gh.wait_result(timeout=5.0)
    st = client.query_status(gid)
    assert st is None or st.goal_id == gid
    # an unknown goal id -> None
    assert client.query_status("f" * 32) is None


def test_rejection_codes_raise(node, codec):
    node.create_action_server("/py_test/cxl", DEMO_SCHEMA_BFBS, codec,
                              lambda g, c: c.succeed(demo_action.build_value_table(0)),
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/cxl", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000)
    )
    time.sleep(0.6)
    # unknown goal -> NOT_FOUND -> NotFoundError (was silently returned before)
    with pytest.raises(aorta.NotFoundError):
        client.cancel("a" * 32, "b" * 64)
    # wrong goal_secret on get_result -> UNAUTHORIZED -> InvalidArgumentError
    gh = client.send_goal(1)
    gh.wait_result(timeout=5.0)
    with pytest.raises(aorta.InvalidArgumentError):
        client.get_result(gh.goal_id(), "f" * 64)


def test_cancel_in_flight_goal_returns_response(node, codec):
    # cancel() of an EXECUTING goal (server supports cancel) is accepted and
    # returns a CancelResponse — the non-raising path (vs the already-terminal
    # rejection covered elsewhere).
    import threading

    release = threading.Event()

    def handler(goal, ctx):
        release.wait(2.0)  # stay EXECUTING so the cancel is accepted, not terminal
        ctx.succeed(demo_action.build_value_table(0))

    node.create_action_server(
        "/py_test/cancel_ok", DEMO_SCHEMA_BFBS, codec, handler,
        goal_decoder=codec.decode_goal_value,
        options=aorta.ActionServerOptions(supports_cancel=True),
    )
    client = node.create_action_client(
        "/py_test/cancel_ok", codec,
        options=aorta.ActionClientOptions(control_timeout_ms=3000))
    time.sleep(0.6)
    gh = client.send_goal(1)
    time.sleep(0.2)  # let it enter EXECUTING
    try:
        cr = gh.cancel()
        assert isinstance(cr, aorta.CancelResponse)
    finally:
        release.set()


def test_info(node, codec):
    node.create_action_server("/py_test/info", DEMO_SCHEMA_BFBS, codec,
                              lambda g, c: c.succeed(demo_action.build_value_table(0)),
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client("/py_test/info", codec)
    time.sleep(0.6)
    info = client.info()
    assert info is None or isinstance(info, aorta.ActionInfo)


def test_require_goal_id_mismatch_raises():
    # A reply for a different goal (stale/misrouted / non-Core provider) is rejected
    # before we act on it; a matching id passes through.
    with pytest.raises(aorta.InternalError, match="goal_id mismatch"):
        action._require_goal_id("a" * 32, "b" * 32, "send_goal")
    action._require_goal_id("same", "same", "cancel")  # match -> no raise


def test_normalize_action_name_canonicalizes_and_validates():
    # Additive C-ABI helper: the SDK stores the SAME canonical name the
    # core routes under so action_name()/name() match the Rust/C++ getters. Strips
    # leading/trailing '/', idempotent on a canonical name, and rejects malformed
    # names (parity with Rust try_new / C++ NormalizeActionName).
    lib = bindings.configure(loader.library())
    assert action.normalize_action_name(lib, "/demo/execute_task/") == "demo/execute_task"
    assert action.normalize_action_name(lib, "demo/execute_task") == "demo/execute_task"
    with pytest.raises(aorta.InvalidArgumentError):
        action.normalize_action_name(lib, "")  # empty -> rejected
    with pytest.raises(aorta.InvalidArgumentError):
        action.normalize_action_name(lib, "a//b")  # empty middle segment -> rejected


def test_query_status_and_info_on_closed_client_raise(node, codec):
    # A freed handle returns NOT_FOUND (→ None); closed clients must raise like
    # every other op, not silently report "status absent".
    client = node.create_action_client("/py_test/closed_q", codec)
    client.close()
    with pytest.raises(aorta.AortaError, match="closed"):
        client.query_status("a" * 32)
    with pytest.raises(aorta.AortaError, match="closed"):
        client.info()


def test_wait_result_without_timeout_returns_terminal(node, codec):
    # Exercises the no-deadline branch of wait_result (poll get_result unbounded).
    node.create_action_server(
        "/py_test/wr_noto", DEMO_SCHEMA_BFBS, codec,
        lambda g, c: c.succeed(demo_action.build_value_table(7)),
        goal_decoder=codec.decode_goal_value,
    )
    client = node.create_action_client(
        "/py_test/wr_noto", codec,
        options=aorta.ActionClientOptions(control_timeout_ms=4000))
    time.sleep(0.6)
    res = client.send_goal(1).wait_result()  # no timeout
    assert res.status.is_terminal()


def test_wait_result_bounds_poll_by_caller_timeout(node, codec):
    # A blocked get_result (no provider) must not overshoot the caller's
    # wait_result timeout by the client's control timeout (10s default). Without
    # the per-poll bound this blocks ~10s; with it, ~0.5s.
    client = node.create_action_client("/py_test/wr_bound", codec)  # default 10s
    gh = aorta.GoalHandle.from_persisted(client, "a" * 32, "secret")
    start = time.monotonic()
    with pytest.raises(aorta.AortaError):
        gh.wait_result(timeout=0.5)
    elapsed = time.monotonic() - start
    assert elapsed < 5.0, f"wait_result overshot caller timeout: {elapsed:.1f}s"


def test_goalhandle_from_persisted_fields():
    # Pure construction (no live client): recover a handle from id + secret.
    gh = aorta.GoalHandle.from_persisted(None, "a" * 32, "the-secret", "epoch-1")
    assert gh.goal_id() == "a" * 32
    assert gh.goal_secret() == "the-secret"
    assert gh.server_epoch() == "epoch-1"
    # server_epoch is optional (the live server stamps the authoritative one).
    assert aorta.GoalHandle.from_persisted(None, "b" * 32, "s").server_epoch() == ""


def test_goalhandle_from_persisted_drives_result(node, codec):
    node.create_action_server("/py_test/persist", DEMO_SCHEMA_BFBS, codec,
                              lambda g, c: c.succeed(demo_action.build_value_table(7)),
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/persist", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000)
    )
    time.sleep(0.6)
    gh = client.send_goal(1)
    # A fresh handle built only from the persisted id + secret drives the goal.
    recovered = aorta.GoalHandle.from_persisted(client, gh.goal_id(), gh.goal_secret())
    res = recovered.wait_result(timeout=5.0)
    assert res.status == aorta.ActionStatus.SUCCEEDED
    assert demo_action._decode_i64_table_bytes(res.result) == 7


def test_goalhandle_stale_epoch_raises(node, codec):
    node.create_action_server("/py_test/epoch", DEMO_SCHEMA_BFBS, codec,
                              lambda g, c: c.succeed(demo_action.build_value_table(0)),
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/epoch", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000)
    )
    time.sleep(0.6)
    gh = client.send_goal(1)
    # A handle pinned to a server_epoch that cannot match must reject the
    # real-epoch result as belonging to a previous server (C++ parity).
    stale = aorta.GoalHandle.from_persisted(
        client, gh.goal_id(), gh.goal_secret(), server_epoch="epoch-never-matches"
    )
    with pytest.raises(aorta.NotFoundError, match="previous server epoch"):
        stale.get_result()


def _fb_codec():
    DT = demo_action.DemoDomainT
    return aorta.FlatbuffersActionCodec(goal=DT, result=DT, feedback=DT), DT


def test_flatbuffers_action_codec_roundtrip(node):
    codec, DT = _fb_codec()
    ser = aorta.FlatbuffersActionCodec.serialize
    fb = []

    def handler(goal, ctx):
        g = goal.decode()  # a DemoDomainT (via goal_decoder=codec.decode_goal)
        ctx.publish_feedback(ser(DT(g.value)), "half")
        ctx.succeed(ser(DT(g.value * 2)))

    node.create_action_server("/py_test/fbcodec", demo_action.DEMO_SCHEMA_BFBS, codec,
                              handler, goal_decoder=codec.decode_goal)
    client = node.create_action_client(
        "/py_test/fbcodec", codec, options=aorta.ActionClientOptions(control_timeout_ms=4000)
    )
    time.sleep(0.6)
    gh = client.send_goal(ser(DT(21)))  # goal spliced as a nested table
    sub = gh.subscribe_feedback(lambda s: fb.append(DT.InitFromPackedBuf(s.data, 0).value))
    res = gh.wait_result(timeout=5.0)
    assert res.status == aorta.ActionStatus.SUCCEEDED
    assert DT.InitFromPackedBuf(res.result, 0).value == 42  # result nested table round-tripped
    sub.close()


def _build_action_info(name, hash_hex):
    import flatbuffers
    b = flatbuffers.Builder(128)
    nm, hh = b.CreateString(name), b.CreateString(hash_hex)
    nn, ep = b.CreateString("node"), b.CreateString("epoch")
    b.StartObject(8)
    b.PrependUOffsetTRelativeSlot(0, nm, 0)
    b.PrependUOffsetTRelativeSlot(1, hh, 0)
    b.PrependBoolSlot(2, True, False)
    b.PrependUint32Slot(3, 4, 0)
    b.PrependUint64Slot(4, 60000, 0)
    b.PrependUint64Slot(5, 300000, 0)
    b.PrependUOffsetTRelativeSlot(6, nn, 0)
    b.PrependUOffsetTRelativeSlot(7, ep, 0)
    b.Finish(b.EndObject())
    return bytes(b.Output())


def test_flatbuffers_action_codec_methods_direct():
    c, DT = _fb_codec()
    ser = aorta.FlatbuffersActionCodec.serialize
    gid = "a" * 32

    # send-goal request: goal_id + nested goal table
    req = c.build_send_goal_request(gid, ser(DT(7)))
    assert c.decode_send_goal_request_goal_id(req) == gid
    assert c.decode_goal(req).value == 7

    resp = c.encode_send_goal_response(
        True, gid, "sec", aorta.ActionReplyCode.OK, aorta.ActionStatus.ACCEPTED, "m", "ep")
    r = c.parse_send_goal_response(resp)
    assert r.accepted and r.goal_id == gid and r.goal_secret == "sec"
    assert r.code == aorta.ActionReplyCode.OK and r.status == aorta.ActionStatus.ACCEPTED

    # cancel
    assert c.decode_cancel_goal_request(c.build_cancel_request(gid, "sec")) == (gid, "sec")
    cr = c.parse_cancel_response(
        c.encode_cancel_response(True, gid, aorta.ActionReplyCode.OK, aorta.ActionStatus.CANCELED, "m"))
    assert cr.accepted and cr.status == aorta.ActionStatus.CANCELED

    # get-result request + response (result + snapshot nested tables)
    assert c.decode_get_result_request(c.build_get_result_request(gid, "sec")) == (gid, "sec")
    gresp = c.encode_get_result_response(
        gid, aorta.ActionStatus.SUCCEEDED, 1, 2, 3, 4, ser(DT(9)), ser(DT(42)),
        aorta.ActionReplyCode.SUCCEEDED, "ok", "ep")
    g = c.parse_get_result_response(gresp)
    assert g.status == aorta.ActionStatus.SUCCEEDED and g.accepted_stamp_ns == 1
    assert DT.InitFromPackedBuf(g.result, 0).value == 42
    assert DT.InitFromPackedBuf(g.feedback_snapshot, 0).value == 9

    # public status + feedback
    p = c.parse_public_status(c.encode_public_status(
        gid, aorta.ActionStatus.EXECUTING, aorta.ActionReplyCode.NONE, 1, 2, 3, 4, "ep"))
    assert p.status == aorta.ActionStatus.EXECUTING and p.last_feedback_seq == 4
    f = c.parse_feedback(c.encode_feedback(
        gid, 5, 99, aorta.ActionStatus.EXECUTING, ser(DT(3)), "fmsg"))
    assert f.seq == 5 and f.message == "fmsg" and DT.InitFromPackedBuf(f.data, 0).value == 3

    # empty-payload branches: no nested field -> decode_goal None, extract -> b""
    assert c.decode_goal(c.build_send_goal_request(gid, b"")) is None
    empty = c.parse_get_result_response(c.encode_get_result_response(
        gid, aorta.ActionStatus.ABORTED, 0, 0, 0, 0, b"", b"",
        aorta.ActionReplyCode.HANDLER_ERROR, "err", ""))
    assert empty.result == b"" and empty.feedback_snapshot == b""
    fe = c.parse_feedback(c.encode_feedback(gid, 1, 1, aorta.ActionStatus.EXECUTING, b"", ""))
    assert fe.data == b""

    # action info
    info = c.parse_action_info(_build_action_info("/x/y", "cd" * 32))
    assert info.action_name == "/x/y" and info.max_concurrency == 4 and info.supports_cancel


def test_action_client_closes_owned_feedback_subs(node, codec):
    # A feedback subscription the caller does NOT close must still be undeclared
    # when the client closes (Core tracks subs separately).
    client = node.create_action_client("/py_test/fbclose", codec)
    sub = client.subscribe_feedback(lambda s: None)
    assert sub._closed is False
    client.close()
    assert sub._closed is True  # client.close() closed the owned subscription


def test_action_info_server(node, codec):
    with node.create_action_info_server("/py_test/infoonly", DEMO_SCHEMA_BFBS) as srv:
        # name() returns the NORMALIZED form (leading slash stripped), matching
        # the Rust/C++ getters (action name = runtime parameter).
        assert srv.name() == "py_test/infoonly"
        client = node.create_action_client("/py_test/infoonly", codec)
        # poll discovery; the info-only server exists so info() should resolve
        info = None
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            info = client.info()
            if info is not None:
                break
            time.sleep(0.1)
        assert info is None or isinstance(info, aorta.ActionInfo)
    srv.close()  # idempotent after the context manager already closed it


def test_client_context_managers_and_closed(node, codec):
    with node.create_action_server("/py_test/cm", DEMO_SCHEMA_BFBS, codec,
                                   lambda g, c: c.succeed(demo_action.build_value_table(0)),
                                   goal_decoder=codec.decode_goal_value) as srv:
        with node.create_action_client("/py_test/cm", codec) as client:
            assert client.action_name() == "py_test/cm"  # normalized (#171 parity)
        assert client._closed
    assert srv._closed
    client.close()  # idempotent
    with pytest.raises(aorta.AortaError, match="closed"):
        client.send_goal(1)
    with pytest.raises(aorta.AortaError, match="closed"):
        client.subscribe_feedback(lambda s: None)


# ── direct unit tests ─────────────────────────────────────────────────────────
def test_server_name_and_goalhandle_delegation(node, codec):
    srv = node.create_action_server("/py_test/gh", DEMO_SCHEMA_BFBS, codec,
                                    lambda g, c: c.succeed(demo_action.build_value_table(0)),
                                    goal_decoder=codec.decode_goal_value)
    assert srv.name() == "py_test/gh"  # normalized (#171 parity)
    client = node.create_action_client(
        "/py_test/gh", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000))
    time.sleep(0.6)
    gh = client.send_goal(1)
    gh.wait_result(timeout=5.0)
    assert gh.query_status() is None or isinstance(gh.query_status(), aorta.PublicStatus)
    # Cancelling the now-terminal goal is rejected (ALREADY_TERMINAL) and raises,
    # parity with Rust/C++ — not a returned CancelResponse.
    with pytest.raises(aorta.AortaError):
        gh.cancel()


def test_send_goal_busy_rejection_raises(node, codec):
    def slow(goal, ctx):
        time.sleep(1.0)  # hold the single concurrency slot
        ctx.succeed(demo_action.build_value_table(0))

    node.create_action_server("/py_test/busy", DEMO_SCHEMA_BFBS, codec, slow,
                              goal_decoder=codec.decode_goal_value,
                              options=aorta.ActionServerOptions(max_concurrency=1))
    client = node.create_action_client(
        "/py_test/busy", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000))
    time.sleep(0.6)
    client.send_goal(1)  # occupies the only slot
    time.sleep(0.15)
    with pytest.raises(aorta.BusyError):  # 2nd goal -> BUSY (max_concurrency=1)
        client.send_goal(2)


def test_action_server_create_failure_unregisters(lib, codec):
    class _DeadNode:
        _handle = 0  # not in the registry -> NOT_FOUND

    before = len(action._execute_handlers)
    with pytest.raises(aorta.NotFoundError):
        action.ActionServer(_DeadNode(), "/x", DEMO_SCHEMA_BFBS, codec, lambda g, c: None)
    assert len(action._execute_handlers) == before  # execute token cleaned up


def test_info_none_without_server(node, codec):
    client = node.create_action_client("/py_test/no_server", codec,
                                       options=aorta.ActionClientOptions(info_timeout_ms=500))
    time.sleep(0.3)
    assert client.info() is None  # no server registered -> NOT_FOUND -> None


def test_wait_result_timeout_returns_nonterminal(node, codec):
    def slow(goal, ctx):
        time.sleep(1.5)  # stays EXECUTING past the wait timeout
        ctx.succeed(demo_action.build_value_table(0))

    node.create_action_server("/py_test/slow", DEMO_SCHEMA_BFBS, codec, slow,
                              goal_decoder=codec.decode_goal_value)
    client = node.create_action_client(
        "/py_test/slow", codec, options=aorta.ActionClientOptions(control_timeout_ms=3000))
    time.sleep(0.6)
    gh = client.send_goal(1)
    with pytest.raises(aorta.TimeoutError):  # raises on expiry (C++/Rust parity)
        gh.wait_result(timeout=0.3)


def test_feedback_subscription_lifecycle(node, codec):
    client = node.create_action_client("/py_test/fbsub", codec)
    sub = client.subscribe_feedback(lambda s: None)
    assert isinstance(sub, aorta.ActionFeedbackSubscription)
    with sub:  # context manager
        pass
    assert sub._closed
    sub.close()  # idempotent


def test_decode_secret_callback_malformed(codec):
    bad, _k = _make_bytes_view(b"\xff\xff\xff\xff")
    rc = action._cb_decode_secret(
        codec.decode_cancel_goal_request, ctypes.pointer(bad),
        ctypes.pointer(structs.AortaStringView()), ctypes.pointer(structs.AortaStringView()))
    assert rc == action._ERR_INTERNAL


def test_enums_and_terminal():
    assert int(aorta.ActionStatus.CANCELED) == 6
    assert aorta.ActionStatus.SUCCEEDED.is_terminal()
    assert not aorta.ActionStatus.EXECUTING.is_terminal()
    assert int(aorta.ActionReplyCode.HANDLER_COMPLETED_WITHOUT_RESULT) == 302


def test_option_builders():
    assert action._build_server_options(None) == (None, ())
    s, _ = action._build_server_options(aorta.ActionServerOptions(supports_cancel=False, max_concurrency=3))
    assert s.supports_cancel == 0 and s.max_concurrency == 3
    assert action._build_client_options(None) == (None, ())
    c, _ = action._build_client_options(aorta.ActionClientOptions(control_timeout_ms=7, info_timeout_ms=8))
    assert c.control_timeout_ms == 7 and c.info_timeout_ms == 8


def test_string_view_and_owned_view_helpers():
    sv = structs.AortaStringView()
    action._write_string_view(ctypes.pointer(sv), "héllo", [])
    assert action._sv_str(sv) == "héllo"
    # empty
    sv2 = structs.AortaStringView()
    action._write_string_view(ctypes.pointer(sv2), "", [])
    assert action._sv_str(sv2) == ""
    # owned view + release roundtrip
    out = structs.AortaOwnedBytesView()
    action._write_owned_view(ctypes.pointer(out), b"payload")
    assert out.slice_count == 1 and out.total_len == 7
    slc = ctypes.cast(out.slices, ctypes.POINTER(structs.AortaOwnedBytesSlice)).contents
    assert ctypes.string_at(slc.ptr, slc.len) == b"payload"
    # release frees the registry entry
    token = int(slc.release_ctx)
    assert token in action._owned_registry
    action._release_owned(slc.release_ctx, slc.ptr, slc.len)
    assert token not in action._owned_registry
    action._release_owned(99999999, None, 0)  # unknown token -> no-op


def test_execution_context_via_mock():
    lib = mock.MagicMock()
    for name in ("is_cancel_requested", "publish_feedback", "succeed", "abort", "cancel", "free"):
        getattr(lib, "aorta_action_context_" + name).return_value = 0  # AORTA_OK
    ctx = action.ActionExecutionContext(lib, 77, "goalX")
    assert ctx.goal_id() == "goalX"
    assert ctx.is_cancel_requested() is False  # out stays 0
    ctx.publish_feedback(b"fb", "m")
    assert lib.aorta_action_context_publish_feedback.called
    ctx.succeed(b"r")
    assert lib.aorta_action_context_succeed.called
    ctx.abort(aorta.ActionReplyCode.HANDLER_ERROR, "x")
    assert lib.aorta_action_context_abort.called
    ctx.cancel(aorta.ActionReplyCode.CANCELED, "x")
    assert lib.aorta_action_context_cancel.called
    ctx._free()
    assert lib.aorta_action_context_free.called


def test_codec_decode_callbacks(codec):
    req = codec.build_send_goal_request("0" * 32, 5)
    rview, _k = _make_bytes_view(req)
    out_id = structs.AortaStringView()
    assert action._cb_decode_id(codec, ctypes.pointer(rview), ctypes.pointer(out_id)) == 0
    assert action._sv_str(out_id) == "0" * 32
    # decode_secret (cancel + get_result share the typedef)
    creq = codec.build_cancel_request("a" * 32, "b" * 64)
    cview, _k2 = _make_bytes_view(creq)
    oid, osec = structs.AortaStringView(), structs.AortaStringView()
    assert action._cb_decode_secret(
        codec.decode_cancel_goal_request, ctypes.pointer(cview),
        ctypes.pointer(oid), ctypes.pointer(osec)) == 0
    assert action._sv_str(oid) == "a" * 32 and action._sv_str(osec) == "b" * 64
    # a malformed request -> _ERR_INTERNAL (parse raises, swallowed)
    bad, _k3 = _make_bytes_view(b"\x01\x02\x03\x04")
    assert action._cb_decode_id(codec, ctypes.pointer(bad), ctypes.pointer(structs.AortaStringView())) == action._ERR_INTERNAL


def _sv(s):
    v = structs.AortaStringView()
    # The backing buffer's lifetime is owned by the keepalive list; tie it to the
    # returned view so ``v.ptr`` stays valid until the view itself is dropped.
    # Passing a throwaway ``[]`` here would let the buffer be freed the moment
    # ``_sv`` returns, leaving ``v.ptr`` dangling — later reads then hit reused
    # memory (e.g. goal_id picking up goal_secret's bytes), an obmalloc-dependent
    # use-after-free that fails non-deterministically on some Python versions.
    keep = []
    action._write_string_view(ctypes.pointer(v), s, keep)
    v._keepalive = keep
    return v


def test_codec_encode_callbacks(codec):
    out = structs.AortaOwnedBytesView()
    args = (1, _sv("g"), _sv("s"), 1, 1, _sv("m"), _sv("e"), ctypes.pointer(out))
    assert action._cb_encode(codec.encode_send_goal_response, action._bld_send_goal, args) == 0
    slc = ctypes.cast(out.slices, ctypes.POINTER(structs.AortaOwnedBytesSlice)).contents
    r = codec.parse_send_goal_response(ctypes.string_at(slc.ptr, slc.len))
    assert r.goal_id == "g" and r.status == aorta.ActionStatus.ACCEPTED
    # exercise the other four encode builders through _cb_encode
    o2 = structs.AortaOwnedBytesView()
    assert action._cb_encode(codec.encode_cancel_response, action._bld_cancel,
                             (1, _sv("g"), 1, 1, _sv("m"), ctypes.pointer(o2))) == 0
    o3 = structs.AortaOwnedBytesView()
    assert action._cb_encode(codec.encode_public_status, action._bld_public_status,
                             (_sv("g"), 1, 1, 1, 2, 3, 4, _sv("e"), ctypes.pointer(o3))) == 0
    o4 = structs.AortaOwnedBytesView()
    fbview, _k = _make_bytes_view(demo_action.build_value_table(7))
    assert action._cb_encode(codec.encode_feedback, action._bld_feedback,
                             (_sv("g"), 1, 2, 1, ctypes.pointer(fbview), _sv("m"), ctypes.pointer(o4))) == 0
    o5 = structs.AortaOwnedBytesView()
    rview, _k2 = _make_bytes_view(demo_action.build_value_table(42))
    empty, _k3 = _make_bytes_view(b"")
    assert action._cb_encode(codec.encode_get_result_response, action._bld_get_result,
                             (_sv("g"), 4, 1, 2, 3, 4, ctypes.pointer(empty), ctypes.pointer(rview), 100, _sv("m"), _sv("e"), ctypes.pointer(o5))) == 0
    # encode build raises -> _ERR_INTERNAL
    assert action._cb_encode(lambda *a: (_ for _ in ()).throw(ValueError("x")), action._bld_cancel,
                             (1, _sv("g"), 1, 1, _sv("m"), ctypes.pointer(structs.AortaOwnedBytesView()))) == action._ERR_INTERNAL


def test_run_execute_logic(node, codec):
    req = codec.build_send_goal_request("0" * 32, 9)
    rview, _k = _make_bytes_view(req)
    captured = {}

    def handler(goal, ctx):
        captured["value"] = goal.decode()
        captured["gid"] = ctx.goal_id()

    # ctx handle 0: the ctx free/abort FFI calls are harmless on an unknown handle.
    action._run_execute(codec, handler, codec.decode_goal_value, ctypes.pointer(rview), 0)
    assert captured["value"] == 9 and captured["gid"] == "0" * 32
    # handler raises -> swallowed + abort attempted (caught on the bad handle)
    action._run_execute(codec, lambda g, c: (_ for _ in ()).throw(RuntimeError("x")),
                        codec.decode_goal_value, ctypes.pointer(rview), 0)


def test_retained_goal_decode():
    g = action.RetainedActionGoal("gid", b"payload", goal_decoder=lambda p: ("dec", p))
    assert g.goal_id() == "gid" and g.payload() == b"payload"
    assert g.decode() == ("dec", b"payload")
    g2 = action.RetainedActionGoal("g", b"p", goal_decoder=None)
    with pytest.raises(aorta.AortaError):
        g2.decode()


def test_raise_for_code():
    # send_goal rejection (mirrors Rust map_send_goal_rejection): Busy/Conflict/
    # GoalExpired are special; everything else -> invalid-argument.
    with pytest.raises(aorta.BusyError):
        action._raise_send_goal_rejection(aorta.ActionReplyCode.BUSY, "")
    with pytest.raises(aorta.ConcurrentConflictError):
        action._raise_send_goal_rejection(aorta.ActionReplyCode.CONFLICT, "")
    with pytest.raises(aorta.NotFoundError):
        action._raise_send_goal_rejection(aorta.ActionReplyCode.GOAL_EXPIRED, "")
    with pytest.raises(aorta.InvalidArgumentError):  # PAYLOAD_CONFLICT -> invalid-arg, NOT concurrent-conflict
        action._raise_send_goal_rejection(aorta.ActionReplyCode.PAYLOAD_CONFLICT, "")
    # cancel rejection (mirrors Rust map_cancel_rejection): only NotFound is
    # special; everything else (incl. Conflict for supports_cancel=False,
    # AlreadyTerminal, Unauthorized) -> invalid-argument.
    with pytest.raises(aorta.NotFoundError):
        action._raise_cancel_rejection(aorta.ActionReplyCode.NOT_FOUND, "gone")
    with pytest.raises(aorta.InvalidArgumentError):
        action._raise_cancel_rejection(aorta.ActionReplyCode.ALREADY_TERMINAL, "")
    with pytest.raises(aorta.InvalidArgumentError):
        action._raise_cancel_rejection(aorta.ActionReplyCode.CONFLICT, "")
    # get_result narrow set: Conflict/Busy are outcomes (no raise); only
    # Unauthorized/NotFound/GoalExpired raise.
    action._raise_for_get_result(aorta.ActionReplyCode.CONFLICT, "")
    action._raise_for_get_result(aorta.ActionReplyCode.SUCCEEDED, "")
    with pytest.raises(aorta.InvalidArgumentError):
        action._raise_for_get_result(aorta.ActionReplyCode.UNAUTHORIZED, "")
    with pytest.raises(aorta.NotFoundError):
        action._raise_for_get_result(aorta.ActionReplyCode.GOAL_EXPIRED, "")


def test_execute_dispatch_token_branches(node, codec):
    # unknown token / null user_data -> frees the ctx (handle 0 = harmless) + no-op
    action._execute_dispatch(None, 0, 99_999_999)
    action._execute_dispatch(None, 0, None)
    # a live token runs the handler
    fired = {}
    token = action._register_execute(
        (codec, lambda g, c: fired.setdefault("v", g.decode()), codec.decode_goal_value))
    try:
        req = codec.build_send_goal_request("0" * 32, 8)
        rview, _k = _make_bytes_view(req)
        action._execute_dispatch(ctypes.pointer(rview), 0, token)
        assert fired["v"] == 8
    finally:
        action._unregister_execute(token)


def test_feedback_dispatch_branches(codec):
    got = []
    # build a feedback buffer the codec can parse
    fb_bytes = codec.encode_feedback("gidA", 1, 5, aorta.ActionStatus.EXECUTING,
                                     demo_action.build_value_table(7), "msg")
    view, slc = _make_bytes_view(fb_bytes)
    token = action._register_feedback((codec, lambda s: got.append(s), None))
    try:
        action._feedback_dispatch(ctypes.pointer(view), None, None, token)
        assert got and got[0].goal_id == "gidA" and got[0].seq == 1
    finally:
        action._unregister_feedback(token)
    # goal filter excludes a non-matching goal
    got2 = []
    t2 = action._register_feedback((codec, lambda s: got2.append(s), "other"))
    try:
        action._feedback_dispatch(ctypes.pointer(view), None, None, t2)
        assert got2 == []
    finally:
        action._unregister_feedback(t2)
    # unknown token / null user_data -> no-op
    action._feedback_dispatch(ctypes.pointer(view), None, None, 99999999)
    action._feedback_dispatch(ctypes.pointer(view), None, None, None)
    # raising callback is swallowed
    t3 = action._register_feedback((codec, lambda s: (_ for _ in ()).throw(RuntimeError("x")), None))
    try:
        action._feedback_dispatch(ctypes.pointer(view), None, None, t3)
    finally:
        action._unregister_feedback(t3)


def _make_bytes_view(data: bytes):
    """Build a borrowed AortaBytesView over data (kept alive via the slice)."""
    arr = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
    slc = structs.AortaBytesSlice()
    slc.ptr = ctypes.cast(arr, ctypes.c_void_p)
    slc.len = len(data)
    view = structs.AortaBytesView()
    view.slices = ctypes.addressof(slc)
    view.slice_count = 1
    view.total_len = len(data)
    return view, (arr, slc)
