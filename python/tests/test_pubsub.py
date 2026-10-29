"""In-process raw pub/sub smoke tests (real Zenoh delivery on a local session)."""

from __future__ import annotations

import time

import flatbuffers
import pytest

import aorta
from aorta import header as _header


def _read_message_header(buf: bytes) -> _header.AortaHeader:
    """Read the AortaHeader from field 0 of a message buffer."""
    root = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
    tab = flatbuffers.table.Table(buf, root)
    off = tab.Offset(4)  # field 0 = aorta_header
    header_pos = tab.Indirect(tab.Pos + off)
    return _header.read_aorta_header(buf, header_pos)


def _minimal_fill(builder, header_off):
    """Build a message table whose only field (slot 0) is the AortaHeader."""
    builder.StartObject(1)
    builder.PrependUOffsetTRelativeSlot(0, header_off, 0)
    return builder.EndObject()


def _poll_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture()
def node(lib):  # lib fixture guarantees the cdylib is loaded/guarded
    n = aorta.Node("py_pubsub_test", group="py_test")
    yield n
    n.close()


def test_raw_pubsub_roundtrip(node):
    received = []
    topic = "/py_test/chatter/raw"
    node.create_subscriber(topic, lambda payload, ctx: received.append(payload))
    time.sleep(0.4)  # let the subscriber declaration propagate
    pub = node.create_publisher(topic)
    for i in range(3):
        pub.publish_bytes(f"msg-{i}".encode())
    assert _poll_until(lambda: len(received) >= 3), f"only got {received!r}"
    assert received == [b"msg-0", b"msg-1", b"msg-2"]


def test_callback_exception_is_swallowed(node):
    received = []
    topic = "/py_test/chatter/throwing"

    def handler(payload, ctx):
        received.append(payload)
        raise ValueError("boom")  # must NOT cross the C boundary

    node.create_subscriber(topic, handler)
    time.sleep(0.4)
    pub = node.create_publisher(topic)
    pub.publish_bytes(b"x")
    pub.publish_bytes(b"y")
    # process survives and both deliver despite the raising handler
    assert _poll_until(lambda: len(received) >= 2), f"only got {received!r}"
    assert received == [b"x", b"y"]


def test_message_context_delivered(node):
    contexts = []
    topic = "/py_test/chatter/ctx"
    node.create_subscriber(topic, lambda payload, ctx: contexts.append(ctx))
    time.sleep(0.4)
    pub = node.create_publisher(topic)
    pub.publish_bytes(b"hello")
    assert _poll_until(lambda: len(contexts) >= 1)
    ctx = contexts[0]
    assert isinstance(ctx, aorta.MessageContext)
    assert ctx.receive_stamp_ns >= 0  # populated or default-0 per options


def test_subscriber_options_populate_context(node):
    contexts = []
    topic = "/py_test/subopts/ctx"
    node.create_subscriber(
        topic, lambda payload, ctx: contexts.append(ctx),
        options=aorta.SubscriberOptions(
            enable_header_context=True, include_topic_in_context=True,
            include_receive_timestamp=True),
    )
    time.sleep(0.4)
    pub = node.create_publisher(topic)
    pub.publish_bytes(b"x")
    assert _poll_until(lambda: len(contexts) >= 1)
    ctx = contexts[0]
    assert ctx.topic.endswith(topic)  # include_topic_in_context took effect
    assert ctx.receive_stamp_ns > 0   # include_receive_timestamp took effect


def test_publish_on_closed_publisher_raises(node):
    pub = node.create_publisher("/py_test/chatter/closed")
    pub.close()
    with pytest.raises(aorta.errors.AortaError, match="closed"):
        pub.publish_bytes(b"nope")
    with pytest.raises(aorta.errors.AortaError, match="closed"):
        pub.publish_typed(_minimal_fill)


def test_publish_typed_injects_header_with_sequence(node):
    received = []
    topic = "/py_test/typed/chat"
    node.create_subscriber(topic, lambda payload, ctx: received.append(payload))
    time.sleep(0.4)
    pub = node.create_publisher(topic)
    for _ in range(3):
        pub.publish_typed(_minimal_fill)
    assert _poll_until(lambda: len(received) >= 3), f"only got {len(received)}"
    headers = [_read_message_header(buf) for buf in received]
    assert [h.sequence for h in headers] == [0, 1, 2]  # per-publisher monotonic
    assert all(h.publisher_node == node.name() for h in headers)
    assert all(h.publish_stamp_ns > 0 for h in headers)


def test_subscriber_typed_view_decodes(node):
    decoded = []
    topic = "/py_test/typed/view"
    node.create_subscriber_typed(
        _read_message_header, topic, lambda msg, ctx: decoded.append(msg)
    )
    time.sleep(0.4)
    pub = node.create_publisher(topic)
    pub.publish_typed(_minimal_fill)
    pub.publish_typed(_minimal_fill)
    assert _poll_until(lambda: len(decoded) >= 2), f"only got {len(decoded)}"
    assert all(isinstance(h, _header.AortaHeader) for h in decoded)
    assert [h.sequence for h in decoded] == [0, 1]


def test_subscriber_typed_view_decode_failure_is_skipped(node):
    delivered = []
    topic = "/py_test/typed/badview"

    def bad_decoder(payload):
        raise ValueError("cannot decode")  # must be caught -> message skipped

    node.create_subscriber_typed(
        bad_decoder, topic, lambda msg, ctx: delivered.append(msg)
    )
    time.sleep(0.4)
    pub = node.create_publisher(topic)
    pub.publish_typed(_minimal_fill)
    time.sleep(0.5)  # let it be processed (and skipped)
    assert delivered == []  # handler never called; process survived


def test_publisher_matching_status_tracks_subscriber(node):
    topic = "/py_test/chatter/matching"
    pub = node.create_publisher(topic)
    # No subscriber yet: not matching; a bounded wait times out -> False.
    assert pub.matching_status() is False
    assert pub.wait_for_matching(50) is False
    # Declare a matching subscriber; matching converges to True.
    node.create_subscriber(topic, lambda payload, ctx: None)
    assert _poll_until(lambda: pub.matching_status(), timeout=3.0)
    assert pub.wait_for_matching(2000) is True


def test_matching_on_closed_publisher_raises(node):
    pub = node.create_publisher("/py_test/chatter/matching_closed")
    pub.close()
    with pytest.raises(aorta.errors.AortaError, match="closed"):
        pub.matching_status()
    with pytest.raises(aorta.errors.AortaError, match="closed"):
        pub.wait_for_matching(10)


def test_publisher_wait_for_matching_rejects_out_of_range_timeout(node):
    pub = node.create_publisher("/py_test/chatter/matching_badtimeout")
    # Negative wraps to ~u64::MAX ms under ctypes -> would hang; must be rejected.
    with pytest.raises(aorta.InvalidArgumentError):
        pub.wait_for_matching(-1)


# ---- QoS & buffering redesign: Tier A receive_depth + Tier B-pull ----

def test_pull_subscriber_keeps_last_n(node):
    topic = "/py_test/chatter/pull"
    sub = node.create_subscriber_pull(topic, 4)
    assert sub.dropped_count() == 0
    time.sleep(0.4)  # discovery
    pub = node.create_publisher(topic)

    # Publish 0..10 without draining; the ring overflows and keeps the last N.
    for i in range(10):
        pub.publish_bytes(bytes([i]))
        time.sleep(0.025)
    time.sleep(0.3)

    got = []
    while True:
        sample = sub.try_recv()
        if sample is None:
            break
        assert sample.payload  # non-empty
        got.append(sample.payload[0])

    assert got, "pull ring delivered nothing"
    assert len(got) <= 4, f"ring exceeded depth: {got}"
    assert 9 in got, f"ring must retain the newest sample: {got}"
    sub.close()


def test_pull_recv_timeout_returns_none(node):
    sub = node.create_subscriber_pull("/py_test/chatter/pull_empty", 2)
    # Nothing published -> recv with a short timeout yields None, not an error.
    assert sub.recv(timeout=0.05) is None
    assert sub.try_recv() is None
    sub.close()


def test_receive_depth_caps_and_sheds(node):
    topic = "/py_test/chatter/tier_a"
    delivered = []

    def slow(payload, ctx):
        delivered.append(payload)
        time.sleep(0.08)  # block so events pile up against the cap

    sub = node.create_subscriber(
        topic, slow, options=aorta.SubscriberOptions(receive_depth=2)
    )
    pub = node.create_publisher(topic)

    # Warm up discovery.
    deadline = time.monotonic() + 5.0
    while not delivered and time.monotonic() < deadline:
        pub.publish_bytes(b"\x00")
        time.sleep(0.04)
    assert delivered, "subscriber never became ready"

    # Burst far more than the cap, faster than the slow callback can drain.
    for i in range(40):
        pub.publish_bytes(bytes([i % 256]))

    # Let the gate shed and callbacks drain.
    assert _poll_until(lambda: sub.dropped_count() > 0, timeout=3.0), (
        "the receive_depth gate should shed under a 40-message burst at cap 2"
    )


# ---- Pull subscriber: full surface coverage + options-copy fix ----

def test_create_subscriber_pull_rejects_zero_depth(node):
    with pytest.raises(aorta.InvalidArgumentError):
        node.create_subscriber_pull("/py_test/pull/baddepth", 0)


def test_create_subscriber_pull_does_not_mutate_caller_options(node):
    # Forcing pull-mode fields must NOT mutate the caller's object.
    opts = aorta.SubscriberOptions(receive_depth=7)
    sub = node.create_subscriber_pull("/py_test/pull/optscopy", 4, options=opts)
    assert opts.delivery == aorta.DeliveryMode.EXECUTOR  # unchanged
    assert opts.receive_overflow == aorta.ReceiveOverflow.DROP_NEWEST  # unchanged
    assert opts.receive_depth == 7  # unchanged
    sub.close()


def test_create_subscriber_pull_with_qos(node):
    # Exercises the qos!=None branch of PullSubscriber.__init__.
    sub = node.create_subscriber_pull(
        "/py_test/pull/withqos", 4, qos=aorta.QoS.realtime_control()
    )
    assert sub.topic() == "/py_test/pull/withqos"
    assert sub.dropped_count() == 0
    sub.close()


def test_pull_recv_blocking_returns_published(node):
    # recv() with no timeout (the blocking path) returns a delivered sample.
    topic = "/py_test/pull/blocking"
    sub = node.create_subscriber_pull(topic, 4)
    time.sleep(0.4)  # discovery
    pub = node.create_publisher(topic)
    for i in range(5):
        pub.publish_bytes(bytes([i]))
        time.sleep(0.02)
    time.sleep(0.2)  # ensure at least one sample is in the ring before we block
    sample = sub.recv()  # blocking (timeout=None)
    assert sample is not None
    assert len(sample.payload) == 1
    sub.close()


def test_pull_recv_zero_timeout_is_nonblocking(node):
    # recv(timeout=0) takes the explicit-zero branch and is non-blocking.
    sub = node.create_subscriber_pull("/py_test/pull/zerotimeout", 2)
    assert sub.recv(timeout=0) is None  # empty ring, no block
    sub.close()


def test_pull_recv_negative_timeout_raises(node):
    # A negative timeout is rejected: at the C-ABI, negative timeout_ns means
    # block-forever, so silently treating it as non-blocking would invert the
    # convention a C/C++ porter expects. Blocking mode is spelled `None`.
    sub = node.create_subscriber_pull("/py_test/pull/negtimeout", 2)
    with pytest.raises(ValueError, match="block-forever"):
        sub.recv(timeout=-1)
    sub.close()


def test_pull_subscriber_context_manager_and_double_close(node):
    topic = "/py_test/pull/ctxmgr"
    with node.create_subscriber_pull(topic, 2) as sub:
        assert sub.topic() == topic
    # __exit__ closed it; an explicit close is idempotent.
    sub.close()
