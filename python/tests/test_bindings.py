"""Unit tests for _ffi.bindings view helpers + context.context_from_ptr."""

from __future__ import annotations

import ctypes

from aorta import context
from aorta._ffi import bindings, structs


def _make_bytes_view(chunks):
    """Build an AortaBytesView over `chunks` (list[bytes]); returns (view, keep)."""
    n = len(chunks)
    arr = (structs.AortaBytesSlice * n)()
    keep = [arr]
    total = 0
    for i, c in enumerate(chunks):
        if c:
            buf = ctypes.create_string_buffer(bytes(c), len(c))
            keep.append(buf)
            arr[i].ptr = ctypes.cast(buf, ctypes.c_void_p)
            arr[i].len = len(c)
        else:
            arr[i].ptr = None
            arr[i].len = 0
        total += len(c)
    view = structs.AortaBytesView()
    view.slices = ctypes.cast(arr, ctypes.c_void_p)
    view.slice_count = n
    view.total_len = total
    return view, keep


def _make_string_view(s):
    if s is None:
        return structs.AortaStringView(None, 0), []
    raw = s.encode("utf-8")
    buf = ctypes.create_string_buffer(raw, len(raw))
    view = structs.AortaStringView(ctypes.cast(buf, ctypes.c_void_p), len(raw))
    return view, [buf]


def test_bytes_from_view_single_slice():
    view, _keep = _make_bytes_view([b"hello"])
    assert bindings.bytes_from_view(view) == b"hello"


def test_bytes_from_view_multi_slice_concatenates():
    view, _keep = _make_bytes_view([b"ab", b"cd", b"ef"])
    assert bindings.bytes_from_view(view) == b"abcdef"


def test_bytes_from_view_empty_slice_count():
    view = structs.AortaBytesView()
    view.slices = None
    view.slice_count = 0
    view.total_len = 0
    assert bindings.bytes_from_view(view) == b""


def test_bytes_from_view_skips_null_or_empty_slice():
    view, _keep = _make_bytes_view([b"x", b"", b"y"])
    assert bindings.bytes_from_view(view) == b"xy"


def test_str_from_view_decodes():
    view, _keep = _make_string_view("aorta/topic")
    assert bindings.str_from_view(view) == "aorta/topic"


def test_str_from_view_null_is_empty():
    view, _keep = _make_string_view(None)
    assert bindings.str_from_view(view) == ""


def test_context_from_null_ptr_is_empty():
    ctx = context.context_from_ptr(None)
    assert ctx == context.MessageContext("", 0, 0, "", 0)


def test_context_from_populated_ptr():
    topic_view, keep1 = _make_string_view("/py/topic")
    node_view, keep2 = _make_string_view("pub_node")
    mc = structs.AortaMsgContext()
    mc.topic = topic_view
    mc.receive_stamp_ns = 123456789
    mc.priority = 2
    mc.publisher_node = node_view
    mc.sequence = 7
    ctx = context.context_from_ptr(ctypes.pointer(mc))
    assert ctx.topic == "/py/topic"
    assert ctx.receive_stamp_ns == 123456789
    assert ctx.priority == 2
    assert ctx.publisher_node == "pub_node"
    assert ctx.sequence == 7
    _ = (keep1, keep2)  # keepalive
