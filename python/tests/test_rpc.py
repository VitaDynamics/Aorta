"""RPC (service + client) tests — live in-process roundtrips plus direct
branch-coverage unit tests for the callback trampoline and marshaling.

The live tests exercise the worker-thread service trampoline against a real
Zenoh session; the direct unit tests cover the error branches (unknown token,
handler exception, decode failure, already-replied) deterministically on the
main thread.
"""

from __future__ import annotations

import ctypes
import time

import flatbuffers
import pytest

import aorta
from aorta import client as _client
from aorta import service as _service
from aorta._ffi import bindings, structs
from aorta.header import read_aorta_header


# ── helpers ──────────────────────────────────────────────────────────────────
def _minimal_fill(builder, header_off):
    """Build a message table whose only field (slot 0) is the AortaHeader."""
    builder.StartObject(1)
    builder.PrependUOffsetTRelativeSlot(0, header_off, 0)
    return builder.EndObject()


def _decode_header(payload: bytes):
    """Decode the AortaHeader at field 0 of a finished message buffer."""
    root = flatbuffers.encode.Get(flatbuffers.packer.uoffset, payload, 0)
    tab = flatbuffers.table.Table(payload, root)
    off = tab.Offset(4)  # field 0
    header_pos = tab.Indirect(tab.Pos + off)
    return read_aorta_header(payload, header_pos)


def _poll_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture()
def node(lib):  # the lib fixture guarantees the cdylib is loaded + guarded
    n = aorta.Node("py_rpc_test", group="py_test")
    yield n
    n.close()


class _FakeRequest:
    """Records reply/reply_error for responder unit tests (no live handle)."""

    def __init__(self):
        self.replied = None
        self.error = None
        self._valid = True

    def reply(self, data=b""):
        self.replied = bytes(data)

    def reply_error(self, message):
        self.error = message

    def release(self):
        self._valid = False

    def is_valid(self):
        return self._valid


# ── live: raw service + client ────────────────────────────────────────────────
def test_service_client_echo_roundtrip(node):
    svc = node.create_service(
        "/py_test/echo", lambda data, req: req.reply(b"echo:" + data)
    )
    client = node.create_client(
        "/py_test/echo", options=aorta.ClientOptions(timeout_ms=3000)
    )
    assert svc.name() == "/py_test/echo"
    assert client.service_name() == "/py_test/echo"
    time.sleep(0.5)
    assert client.call(b"hello") == b"echo:hello"
    assert client.call(b"world") == b"echo:world"


def test_reply_error_maps_to_exception(node):
    node.create_service(
        "/py_test/err", lambda data, req: req.reply_error("nope: " + data.decode())
    )
    client = node.create_client(
        "/py_test/err", options=aorta.ClientOptions(timeout_ms=3000)
    )
    time.sleep(0.5)
    with pytest.raises(aorta.AortaError) as ei:
        client.call(b"boom")
    # The error reply surfaces as a mapped error. Core wraps the message text in
    # a ReplyError payload, so the readable string is not preserved verbatim at
    # the client (documented gap) — assert the error indication.
    assert "error" in str(ei.value).lower()


def test_service_and_client_stats(node):
    node.create_service("/py_test/stats", lambda data, req: req.reply(data))
    client = node.create_client(
        "/py_test/stats", options=aorta.ClientOptions(timeout_ms=3000)
    )
    time.sleep(0.5)
    client.call(b"a")
    client.call(b"b")
    assert _poll_until(lambda: client.stats().success_count >= 2)
    cs = client.stats()
    assert isinstance(cs, aorta.ClientStats)
    assert cs.call_count >= 2 and cs.success_count >= 2
    service = node._children[0]  # the service
    # The service bumps reply_ok_count on its own task AFTER the reply has
    # already reached the client, so client.success_count >= 2 does NOT imply
    # the service-side counters have settled — reading them once here races
    # that bump (observed flaky as reply_ok_count == 1). Poll the service
    # stats too so the counters are given time to catch up.
    assert _poll_until(
        lambda: service.stats().reply_ok_count >= 2
        and service.stats().request_received_count >= 2
    )
    ss = service.stats()
    assert isinstance(ss, aorta.ServiceStats)
    assert ss.reply_ok_count >= 2 and ss.request_received_count >= 2


def test_client_call_empty_request_rejected(node):
    client = node.create_client("/py_test/empty")
    with pytest.raises(aorta.InvalidArgumentError):
        client.call(b"")


def test_create_client_with_timeout(node):
    node.create_service("/py_test/cwt", lambda data, req: req.reply(b"pong"))
    client = node.create_client_with_timeout("/py_test/cwt", 3000)
    assert client.service_name() == "/py_test/cwt"
    time.sleep(0.5)
    assert client.call(b"ping") == b"pong"


def test_call_on_closed_client_raises(node):
    client = node.create_client("/py_test/closed")
    client.close()
    client.close()  # idempotent
    with pytest.raises(aorta.AortaError, match="closed"):
        client.call(b"x")


def test_client_call_timeout_no_provider(node):
    client = node.create_client(
        "/py_test/no_such_provider", options=aorta.ClientOptions(timeout_ms=300)
    )
    start = time.monotonic()
    with pytest.raises(aorta.AortaError):
        client.call(b"anyone there?")
    assert time.monotonic() - start < 5.0  # bounded by the 300ms timeout


def test_service_handler_exception_releases_request_and_survives(node):
    calls = {"n": 0}

    def handler(data, req):
        calls["n"] += 1
        if data == b"raise":
            raise ValueError("handler boom")  # must not crash; request released
        req.reply(b"ok")

    node.create_service("/py_test/raises", handler)
    client = node.create_client(
        "/py_test/raises", options=aorta.ClientOptions(timeout_ms=800)
    )
    time.sleep(0.5)
    with pytest.raises(aorta.AortaError):  # released w/o reply -> call fails fast
        client.call(b"raise")
    # the service survived: a normal call still works
    assert client.call(b"ok") == b"ok"
    assert calls["n"] >= 2


def test_service_handler_reply_then_raise_delivers(node):
    # Handler replies, THEN raises: the reply must still reach the client (the
    # request is already consumed -> is_valid() False -> release, not reply_error).
    def handler(data, req):
        req.reply(b"early")
        raise ValueError("after reply")  # swallowed; must not undo the reply

    node.create_service("/py_test/replythrow", handler)
    client = node.create_client(
        "/py_test/replythrow", options=aorta.ClientOptions(timeout_ms=1500)
    )
    time.sleep(0.5)
    assert client.call(b"x") == b"early"


def test_service_request_release_without_reply(node):
    node.create_service("/py_test/drop", lambda data, req: req.release())
    client = node.create_client(
        "/py_test/drop", options=aorta.ClientOptions(timeout_ms=800)
    )
    time.sleep(0.5)
    with pytest.raises(aorta.AortaError):
        client.call(b"please drop me")


@pytest.mark.parametrize("action", ["reply", "reply_error", "release"])
def test_deferred_completion_from_main_thread(node, action):
    """The handler retains the request and completes it later on another thread.

    This exercises the ``reply``/``reply_error``/``release`` success bodies on the
    main thread (the live worker-thread paths are not traced by coverage), and
    proves the deferred-reply pattern works (the finalizer does not fire early).
    """
    import threading

    held = []
    topic = "/py_test/deferred_" + action
    node.create_service(topic, lambda data, req: held.append((data, req)))
    client = node.create_client(topic, options=aorta.ClientOptions(timeout_ms=2000))
    time.sleep(0.5)

    result = {}

    def call():
        try:
            result["resp"] = client.call(b"defer")
        except aorta.AortaError as exc:
            result["err"] = exc

    t = threading.Thread(target=call)
    t.start()
    assert _poll_until(lambda: len(held) >= 1), "request not received"
    _data, req = held[0]
    assert req.is_valid()
    if action == "reply":
        req.reply(b"late:" + _data)
    elif action == "reply_error":
        req.reply_error("deferred error")
    else:
        req.release()
    assert not req.is_valid()  # consumed/released
    t.join(timeout=5)
    if action == "reply":
        assert result.get("resp") == b"late:defer"
    else:
        assert "err" in result  # error reply or release -> client sees an error


def test_service_create_failure_unregisters_token(lib):
    """A failed ``aorta_service_new_with_options`` (here: unknown node handle)
    must unregister the handler token and raise the mapped error (covers the
    creation error branch)."""

    class _DeadNode:
        _handle = 0  # not in the registry -> NOT_FOUND

    before = len(_service._handlers)
    with pytest.raises(aorta.NotFoundError):
        _service.Service(_DeadNode(), "/py_test/dead", lambda d, r: r.reply(d))
    assert len(_service._handlers) == before  # token cleaned up, no leak


def test_service_and_client_context_managers(node):
    with node.create_service("/py_test/cm", lambda d, r: r.reply(d)) as svc:
        with node.create_client(
            "/py_test/cm", options=aorta.ClientOptions(timeout_ms=3000)
        ) as client:
            time.sleep(0.5)
            assert client.call(b"cm") == b"cm"
        assert client._closed
    assert svc._closed


# ── live: typed service + typed client ────────────────────────────────────────
def test_typed_service_view_and_typed_client(node):
    seen = []

    def handler(decoded_header, responder):
        seen.append(decoded_header.sequence)
        responder.reply(_minimal_fill)

    node.create_service_typed_view(_decode_header, "/py_test/typed", handler)
    tc = node.create_client_typed(
        "/py_test/typed", _decode_header, options=aorta.ClientOptions(timeout_ms=3000)
    )
    time.sleep(0.5)
    r0 = tc.call(_minimal_fill)
    r1 = tc.call(_minimal_fill)
    assert isinstance(r0, aorta.TypedClientResponse)
    # request headers carry the per-client monotonic sequence
    assert seen == [0, 1]
    # responses decode to an AortaHeader (service stamps its own sequence)
    assert r0.decode().sequence == 0
    assert r1.decode().sequence == 1
    assert tc.service_name() == "/py_test/typed"
    assert isinstance(tc.stats(), aorta.ClientStats)
    assert tc.raw() is node._children[-1]  # the wrapped raw client


def test_typed_rpc_schema_auto_injection(node):
    import hashlib
    import types

    bfbs = b"demo.echo.service.schema.v1"
    meta = types.SimpleNamespace(
        SCHEMA_BFBS=bfbs,
        SCHEMA_HASH_HEX=hashlib.sha256(bfbs).hexdigest(),
        ROOT_TYPE_NAME="demo.Echo",
    )

    def handler(decoded_header, responder):
        responder.reply(_minimal_fill)

    # Auto-inject the schema on BOTH the typed service and client; the roundtrip
    # must still work (schema metadata is additive — for cross-language discovery).
    node.create_service_typed_view(
        _decode_header, "/py_test/typed_schema", handler, request_schema_meta=meta
    )
    tc = node.create_client_typed(
        "/py_test/typed_schema",
        _decode_header,
        request_schema_meta=meta,
        options=aorta.ClientOptions(timeout_ms=3000),
    )
    time.sleep(0.5)
    assert tc.call(_minimal_fill).decode().sequence == 0


def test_typed_view_decode_failure_sends_error_reply(node):
    def bad_decoder(payload):
        raise ValueError("cannot decode")

    node.create_service_typed_view(
        bad_decoder, "/py_test/typedbad", lambda m, r: r.reply(_minimal_fill)
    )
    client = node.create_client(
        "/py_test/typedbad", options=aorta.ClientOptions(timeout_ms=2000)
    )
    time.sleep(0.5)
    with pytest.raises(aorta.AortaError):  # decode failure -> error reply
        client.call(b"not a valid message")


def test_typed_client_context_manager(node):
    node.create_service_typed_view(
        _decode_header, "/py_test/typedcm", lambda m, r: r.reply(_minimal_fill)
    )
    with node.create_client_typed(
        "/py_test/typedcm", _decode_header, options=aorta.ClientOptions(timeout_ms=3000)
    ) as tc:
        time.sleep(0.5)
        assert tc.call(_minimal_fill).decode().sequence == 0
    assert tc._client._closed
    tc.close()  # idempotent via underlying client


# ── direct unit tests: ServiceRequest branches (no live handle) ───────────────
def test_service_request_already_consumed_branches(lib):
    req = _service.ServiceRequest(lib, 0)  # handle 0 == already consumed/invalid
    assert not req.is_valid()
    with pytest.raises(aorta.AlreadyRepliedError):
        req.reply(b"x")
    with pytest.raises(aorta.AlreadyRepliedError):
        req.reply_error("x")
    req.release()  # no-op on a zero handle


def test_finalize_request_is_silent_noop(lib):
    _service._finalize_request(lib, 0)  # release(0) -> OK, no raise


# ── direct unit tests: service trampoline branches ───────────────────────────
def test_service_dispatch_unknown_token_releases(lib):
    bindings.configure(lib)
    # user_data token not in the registry -> handler None -> req.release() (no-op
    # at handle 0). Must not raise.
    _service._service_dispatch(None, 0, 99_999_999)


def test_service_dispatch_handler_exception_is_swallowed(lib):
    bindings.configure(lib)
    token = _service._register(lambda payload, req: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        # None view -> payload b""; handler raises -> logged + released (no-op).
        _service._service_dispatch(None, 0, token)
    finally:
        _service._unregister(token)


def test_handle_failed_request_replies_or_releases():
    class _FakeReq:
        def __init__(self, valid):
            self._valid = valid
            self.action = None

        def is_valid(self):
            return self._valid

        def reply_error(self, msg):
            self.action = ("reply_error", msg)

        def release(self):
            self.action = "release"

    # unanswered -> error reply (caller fails fast, not a timeout)
    unanswered = _FakeReq(True)
    _service._handle_failed_request(unanswered)
    assert unanswered.action == ("reply_error", "service handler raised an exception")
    # already replied/released -> just release
    consumed = _FakeReq(False)
    _service._handle_failed_request(consumed)
    assert consumed.action == "release"


def test_service_dispatch_happy_path_main_thread(lib):
    bindings.configure(lib)
    got = []
    token = _service._register(lambda payload, req: got.append(payload))
    try:
        view = structs.AortaBytesView()
        slc = structs.AortaBytesSlice()
        buf = ctypes.create_string_buffer(b"ping", 4)
        slc.ptr = ctypes.cast(buf, ctypes.c_void_p)
        slc.len = 4
        view.slices = ctypes.addressof(slc)
        view.slice_count = 1
        view.total_len = 4
        _service._service_dispatch(ctypes.pointer(view), 0, token)
    finally:
        _service._unregister(token)
    assert got == [b"ping"]


# ── direct unit tests: TypedServiceResponder + typed dispatch ─────────────────
def test_typed_responder_methods():
    fake = _FakeRequest()
    seqs = iter([(7, 123, "n1")])
    responder = _service.TypedServiceResponder(fake, lambda: next(seqs))
    assert responder.is_valid()
    assert responder.raw() is fake
    responder.reply(_minimal_fill)
    header = _decode_header(fake.replied)
    assert header.sequence == 7 and header.publish_stamp_ns == 123
    responder.reply_bytes(b"raw")
    assert fake.replied == b"raw"
    responder.reply_error("bad")
    assert fake.error == "bad"


def test_typed_decode_dispatch_success_and_failure():
    # success path
    fake_ok = _FakeRequest()
    seen = []
    _service.typed_service_decode_dispatch(
        lambda payload: ("decoded", payload),
        lambda decoded, responder: seen.append(decoded),
        lambda: (0, 0, None),
        b"body",
        fake_ok,
    )
    assert seen == [("decoded", b"body")]
    # decode failure -> error reply, handler never called
    fake_bad = _FakeRequest()
    called = []
    _service.typed_service_decode_dispatch(
        lambda payload: (_ for _ in ()).throw(ValueError("x")),
        lambda decoded, responder: called.append(decoded),
        lambda: (0, 0, None),
        b"body",
        fake_bad,
    )
    assert called == [] and fake_bad.error == "request payload decode failed"


def test_make_header_stamper_sequence_and_node(lib, monkeypatch):
    bindings.configure(lib)
    stamp = _service.make_header_stamper(lib, "node-x")
    s0, t0, n0 = stamp()
    s1, _t1, _n1 = stamp()
    assert (s0, s1) == (0, 1) and t0 >= 0 and n0 == "node-x"
    # env opt-out drops the node name
    monkeypatch.setenv("AORTA_HEADER_INCLUDE_PUBLISHER_NODE", "0")
    stamp2 = _service.make_header_stamper(lib, "node-y")
    _s, _t, n = stamp2()
    assert n is None


# ── TypedClientResponse unit ──────────────────────────────────────────────────
def test_typed_client_response_payload_and_decode():
    resp = _client.TypedClientResponse(b"\x01\x02", lambda p: ("decoded", p))
    assert resp.payload() == b"\x01\x02"
    assert resp.decode() == ("decoded", b"\x01\x02")


def test_client_options_timeout_ms_range_validated():
    # timeout_ms is a c_uint64; a negative value would wrap to ~2^64 ms (no
    # timeout) — reject it before marshaling. A valid value marshals through.
    from aorta.options import build_client_options

    c, _ = build_client_options(aorta.ClientOptions(timeout_ms=1234))
    assert c.timeout_ms == 1234
    with pytest.raises(aorta.InvalidArgumentError):
        build_client_options(aorta.ClientOptions(timeout_ms=-1))


def test_inject_service_schema_rejects_mismatched_request_response():
    # A typed service has ONE schema identity — request + response must share the
    # same BFBS/hash (parity with C++ InjectTypedServiceSchemaOptions).
    from aorta.options import inject_service_schema

    class _Meta:
        def __init__(self, h):
            self.SCHEMA_BFBS = b"\x00"
            self.SCHEMA_HASH_HEX = h
            self.ROOT_TYPE_NAME = "demo.Chat"

    req = _Meta("a" * 64)
    # Same hash (one .fbs, two root types) -> accepted.
    assert inject_service_schema(None, req, _Meta("a" * 64)) is not None
    # Different artifacts (different hash) -> rejected.
    with pytest.raises(aorta.InvalidArgumentError):
        inject_service_schema(None, req, _Meta("b" * 64))


def test_client_matching_status_tracks_provider(node):
    service = "/py_test/matching_rpc"
    client = node.create_client(service)
    # No provider yet: not matching; a bounded wait times out -> False.
    assert client.matching_status() is False
    assert client.wait_for_matching(50) is False
    # Stand up a service provider; matching converges to True.
    node.create_service(service, lambda data, req: req.reply(data))
    assert _poll_until(lambda: client.matching_status(), timeout=3.0)
    assert client.wait_for_matching(2000) is True


def test_matching_on_closed_client_raises(node):
    client = node.create_client("/py_test/matching_rpc_closed")
    client.close()
    with pytest.raises(aorta.errors.AortaError, match="closed"):
        client.matching_status()
    with pytest.raises(aorta.errors.AortaError, match="closed"):
        client.wait_for_matching(10)


def test_client_wait_for_matching_rejects_out_of_range_timeout(node):
    client = node.create_client("/py_test/matching_rpc_badtimeout")
    # Negative wraps to ~u64::MAX ms under ctypes -> would hang; must be rejected.
    with pytest.raises(aorta.InvalidArgumentError):
        client.wait_for_matching(-1)
