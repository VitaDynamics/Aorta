"""Tests for aorta.header — AortaHeader injection byte-identical to flatc."""

from __future__ import annotations

import importlib.util
import os

import flatbuffers
import pytest

from aorta import header as H

_FIXTURE = os.path.join(os.path.dirname(__file__), "_fixtures", "gen_aorta_header.py")


def _generated():
    spec = importlib.util.spec_from_file_location("gen_aorta_header", _FIXTURE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _root_pos(buf: bytes) -> int:
    return flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)


def _build_with_sdk(stamp, node, seq) -> bytes:
    b = flatbuffers.Builder(0)
    off = H.build_aorta_header(
        b, publish_stamp_ns=stamp, publisher_node=node, sequence=seq
    )
    b.Finish(off)
    return bytes(b.Output())


def _build_with_flatc(stamp, node, seq) -> bytes:
    g = _generated()
    b = flatbuffers.Builder(0)
    node_off = b.CreateString(node) if node else None
    g.AortaHeaderStart(b)
    g.AortaHeaderAddPublishStampNs(b, stamp)
    if node_off is not None:
        g.AortaHeaderAddPublisherNode(b, node_off)
    g.AortaHeaderAddSequence(b, seq)
    b.Finish(g.AortaHeaderEnd(b))
    return bytes(b.Output())


@pytest.mark.parametrize(
    "stamp,node,seq",
    [
        (1234567890, "pub_node", 7),
        (0, "", 0),
        (999, None, 42),
        (2**63, "n", 2**64 - 1),
    ],
)
def test_build_header_byte_identical_to_flatc(stamp, node, seq):
    # The whole point: same builder primitives -> same bytes as the generated
    # CreateAortaHeader, which is what Rust/C++ emit -> cross-language wire compat.
    assert _build_with_sdk(stamp, node, seq) == _build_with_flatc(stamp, node, seq)


def test_read_header_roundtrips():
    buf = _build_with_sdk(111, "nodeA", 9)
    hdr = H.read_aorta_header(buf, _root_pos(buf))
    assert hdr == H.AortaHeader(111, "nodeA", 9)


def test_read_header_omitted_node_is_empty():
    buf = _build_with_sdk(5, None, 1)
    hdr = H.read_aorta_header(buf, _root_pos(buf))
    assert hdr == H.AortaHeader(5, "", 1)


def test_read_all_default_header():
    # stamp=0, node=None, seq=0 -> every field elided -> read yields the
    # FlatBuffers defaults (exercises the field-absent read branches).
    buf = _build_with_sdk(0, None, 0)
    hdr = H.read_aorta_header(buf, _root_pos(buf))
    assert hdr == H.AortaHeader(0, "", 0)


def test_publish_stamp_defaults_to_now(monkeypatch):
    monkeypatch.setattr(H.time, "time_ns", lambda: 424242)
    b = flatbuffers.Builder(0)
    b.Finish(H.build_aorta_header(b, sequence=0))
    buf = bytes(b.Output())
    assert H.read_aorta_header(buf, _root_pos(buf)).publish_stamp_ns == 424242
