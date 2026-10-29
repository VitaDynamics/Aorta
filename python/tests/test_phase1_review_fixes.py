"""Regression tests for the Python SDK (effective group, thread-safe
sequence, Core-clock header timestamp, publisher_node env, binary payloads)."""

from __future__ import annotations

import threading
import time

import flatbuffers
import pytest

import aorta
from aorta import header as _header


def _poll_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _read_message_header(buf: bytes) -> _header.AortaHeader:
    root = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
    tab = flatbuffers.table.Table(buf, root)
    off = tab.Offset(4)
    return _header.read_aorta_header(buf, tab.Indirect(tab.Pos + off))


def _minimal_fill(builder, header_off):
    builder.StartObject(1)
    builder.PrependUOffsetTRelativeSlot(0, header_off, 0)
    return builder.EndObject()


@pytest.fixture()
def node(lib):
    n = aorta.Node("py_review_fixes", group="ignored_by_core")
    yield n
    n.close()


# --- High #1: effective group -------------------------------------------

def test_group_ignores_ctor_arg(node):
    # Core ignores the constructor group; group() returns the resolved value.
    assert node.group() and node.group() != "ignored_by_core"


def test_group_resolves_from_env(lib, monkeypatch):
    monkeypatch.setenv("AORTA_GROUP", "fleet1")
    n = aorta.Node("py_grp_env")
    try:
        assert n.group() == "fleet1"
    finally:
        n.close()


# --- High #3: thread-safe sequence --------------------------------------

def test_publish_typed_sequence_is_thread_safe(node, monkeypatch):
    captured = []
    cap_lock = threading.Lock()
    pub = node.create_publisher("/py_test/seq")

    def fake_publish_bytes(data):
        with cap_lock:
            captured.append(bytes(data))

    monkeypatch.setattr(pub, "publish_bytes", fake_publish_bytes)

    n = 60
    threads = [
        threading.Thread(target=pub.publish_typed, args=(_minimal_fill,))
        for _ in range(n)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    seqs = sorted(_read_message_header(b).sequence for b in captured)
    assert seqs == list(range(n))  # unique + complete: no dup, no gap


# --- High #2: Core-clock timestamp --------------------------------------

def test_publish_typed_stamp_from_core_clock(node, monkeypatch):
    captured = []
    pub = node.create_publisher("/py_test/clock")
    monkeypatch.setattr(pub, "publish_bytes", lambda d: captured.append(bytes(d)))
    before = node.now()
    pub.publish_typed(_minimal_fill)
    after = node.now()
    stamp = _read_message_header(captured[0]).publish_stamp_ns
    # stamp came from the Aorta clock, so it sits within the node.now() window.
    assert before <= stamp <= after


# --- Medium #2: publisher_node env parity -------------------------------

def test_publisher_node_included_by_default(node, monkeypatch):
    captured = []
    pub = node.create_publisher("/py_test/withnode")
    monkeypatch.setattr(pub, "publish_bytes", lambda d: captured.append(bytes(d)))
    pub.publish_typed(_minimal_fill)
    assert _read_message_header(captured[0]).publisher_node == node.name()


def test_publisher_node_excluded_by_env(node, monkeypatch):
    monkeypatch.setenv("AORTA_HEADER_INCLUDE_PUBLISHER_NODE", "false")
    captured = []
    pub = node.create_publisher("/py_test/nonode")  # reads env at construction
    monkeypatch.setattr(pub, "publish_bytes", lambda d: captured.append(bytes(d)))
    pub.publish_typed(_minimal_fill)
    assert _read_message_header(captured[0]).publisher_node == ""


# --- Nit: binary payloads (embedded NUL, high bytes, buffer types) ------

def test_binary_payload_roundtrip(node):
    received = []
    topic = "/py_test/binary"
    node.create_subscriber(topic, lambda p, c: received.append(p))
    time.sleep(0.4)
    pub = node.create_publisher(topic)
    pub.publish_bytes(b"a\x00b\xff")
    pub.publish_bytes(bytearray(b"BA\x00R"))
    pub.publish_bytes(memoryview(b"MV\x01"))
    assert _poll_until(lambda: len(received) >= 3), f"got {received!r}"
    assert received == [b"a\x00b\xff", b"BA\x00R", b"MV\x01"]
