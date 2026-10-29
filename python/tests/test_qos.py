"""QoS profile + presets + Publisher.stats() — parity with Rust/C++ ``QoS``.

Presets are read from the C-ABI ``aorta_qos_preset`` (the single source of preset
truth); the expected values mirror ``core/src/qos.rs``.
"""

from __future__ import annotations

import time

import pytest

import aorta
from aorta import CongestionControl, Priority, QoS, Reliability


def _poll_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture()
def node(lib):  # lib fixture guarantees the cdylib is loaded/guarded
    n = aorta.Node("py_qos_test", group="py_test")
    yield n
    n.close()


# ── QoS value object + presets (no live node needed) ──────────────────────────
def test_qos_defaults():
    # QoS & buffering redesign: 4 wired fields, no durability/history.
    q = QoS()
    assert q.priority is Priority.NORMAL
    assert q.reliability is Reliability.RELIABLE
    assert q.congestion_control is CongestionControl.DROP
    assert q.express is False


def test_qos_to_ffi_mirrors_all_four_fields():
    # aorta_qos_t now mirrors QoS 1:1 — _to_ffi carries every field.
    q = QoS(
        priority=Priority.HIGH,
        reliability=Reliability.BEST_EFFORT,
        congestion_control=CongestionControl.BLOCK,
        express=True,
    )
    s = q._to_ffi()
    assert s.priority == int(Priority.HIGH)
    assert s.reliability == int(Reliability.BEST_EFFORT)
    assert s.congestion_control == int(CongestionControl.BLOCK)
    assert s.express == 1


def test_qos_presets_match_core(lib):  # noqa: ARG001 - lib ensures cdylib loaded
    # Values come from the C-ABI preset accessors (mirrors core/src/qos.rs).
    assert QoS.default() == QoS(
        Priority.NORMAL, Reliability.RELIABLE, CongestionControl.DROP, False
    )
    assert QoS.realtime_control() == QoS(
        Priority.REALTIME, Reliability.RELIABLE, CongestionControl.DROP, True
    )
    assert QoS.sensor_data() == QoS(
        Priority.NORMAL, Reliability.BEST_EFFORT, CongestionControl.DROP, False
    )
    assert QoS.state_update() == QoS(
        Priority.HIGH, Reliability.RELIABLE, CongestionControl.DROP, False
    )
    assert QoS.bulk_transfer() == QoS(
        Priority.BACKGROUND, Reliability.RELIABLE, CongestionControl.DROP, False
    )


# ── Live pub/sub with QoS ─────────────────────────────────────────────────────
def test_pubsub_with_qos_roundtrip(node):
    # Custom QoS on BOTH sides (express=False path); Reliable so delivery is sure.
    received = []
    topic = "/py_test/qos/roundtrip"
    qos = QoS(priority=Priority.HIGH)
    node.create_subscriber(topic, lambda p, c: received.append(p), qos=qos)
    time.sleep(0.4)
    pub = node.create_publisher(topic, qos=QoS(priority=Priority.HIGH))
    for i in range(3):
        pub.publish_bytes(f"q-{i}".encode())
    assert _poll_until(lambda: len(received) >= 3), f"only got {received!r}"
    assert received == [b"q-0", b"q-1", b"q-2"]


def test_pubsub_with_block_congestion_control(node):
    # congestion_control=BLOCK must reach the publisher (it rides aorta_qos_t;
    # the pub_options override stays INHERIT). Reliable + Block: lossless on a
    # loopback topic.
    received = []
    topic = "/py_test/qos/block_cc"
    node.create_subscriber(topic, lambda p, c: received.append(p))
    time.sleep(0.4)
    pub = node.create_publisher(
        topic, qos=QoS(congestion_control=CongestionControl.BLOCK)
    )
    for i in range(3):
        pub.publish_bytes(f"b-{i}".encode())
    assert _poll_until(lambda: len(received) >= 3), f"only got {received!r}"


def test_pubsub_with_express_qos_roundtrip(node):
    # express=True path on both pub and sub (realtime_control sets express).
    received = []
    topic = "/py_test/qos/express"
    node.create_subscriber(
        topic, lambda p, c: received.append(p), qos=QoS.realtime_control()
    )
    time.sleep(0.4)
    pub = node.create_publisher(topic, qos=QoS.realtime_control())
    pub.publish_bytes(b"x")
    assert _poll_until(lambda: len(received) >= 1), f"only got {received!r}"


def test_subscriber_typed_with_qos(node):
    # Exercises create_subscriber_typed's qos passthrough (identity decoder).
    decoded = []
    topic = "/py_test/qos/typed"
    node.create_subscriber_typed(
        lambda payload: payload,
        topic,
        lambda msg, ctx: decoded.append(msg),
        qos=QoS(reliability=Reliability.RELIABLE),
    )
    time.sleep(0.4)
    pub = node.create_publisher(topic, qos=QoS())
    pub.publish_bytes(b"hello")
    assert _poll_until(lambda: len(decoded) >= 1)
    assert decoded[0] == b"hello"


# ── Publisher.stats() ─────────────────────────────────────────────────────────
def test_publisher_stats(node):
    # Publisher.stats() exposes aorta_publisher_get_stats — parity with C++
    # Publisher::GetStats(). NOTE: the core currently STUBS these counters
    # (publisher_stats_from_core returns default), so values are 0 on BOTH the
    # C++ and Python C-ABI paths; we assert the SDK surface (shape + no raise),
    # not specific counts. The method returns real data once the core fills it.
    pub = node.create_publisher("/py_test/qos/stats")
    s = pub.stats()
    assert isinstance(s, aorta.PublisherStats)
    for name in (
        "publish_count",
        "oversize_count",
        "last_oversize_size",
        "current_capacity",
        "max_capacity_seen",
        "last_publish_ns",
        "send_error_count",
    ):
        assert getattr(s, name) >= 0
    pub.publish_bytes(b"m")
    assert isinstance(pub.stats(), aorta.PublisherStats)  # still works post-publish
