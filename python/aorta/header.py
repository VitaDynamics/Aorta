"""AortaHeader injection + read — byte-identical to the Rust/C++ SDKs.

The system schema (message/schemas/system/aorta_base.fbs) is fixed::

    table AortaHeader {
      publish_stamp_ns: ulong;  // field 0
      publisher_node: string;   // field 1
      sequence: ulong;          // field 2
    }

``build_aorta_header`` replicates exactly the FlatBuffers builder primitives the
flatc-generated ``CreateAortaHeader`` emits (``StartObject(3)`` + the three
slot writes, default elision, string built before the table opens). Because all
three SDKs feed the same primitives to a FlatBuffers builder, the produced bytes
are identical — the per-message header is wire-compatible across languages
(Python SDK design proposal §7.3). The header table is built BEFORE the
enclosing message root opens (FlatBuffers is bottom-up).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import flatbuffers
from flatbuffers import number_types as _N
from flatbuffers import table as _table

# AortaHeader vtable slot indices (declaration order).
_SLOT_PUBLISH_STAMP_NS = 0
_SLOT_PUBLISHER_NODE = 1
_SLOT_SEQUENCE = 2
_NUM_FIELDS = 3
# vtable byte offset for field i = 4 + i*2.
_VT_PUBLISH_STAMP_NS = 4
_VT_PUBLISHER_NODE = 6
_VT_SEQUENCE = 8


def build_aorta_header(
    builder: "flatbuffers.Builder",
    *,
    sequence: int,
    publish_stamp_ns: Optional[int] = None,
    publisher_node: Optional[str] = None,
) -> int:
    """Build an AortaHeader table into ``builder``; return its offset.

    ``publish_stamp_ns`` defaults to ``time.time_ns()`` (wall-clock ns, matching
    ``unix_time_ns()``). ``publisher_node`` is omitted from the table when empty
    (default elision, like Rust/C++). ``sequence`` is the per-publisher,
    per-direction monotonic counter (from 0).
    """
    if publish_stamp_ns is None:
        publish_stamp_ns = time.time_ns()
    # The string must be created before the table is started (bottom-up build).
    node_off = (
        builder.CreateString(publisher_node)
        if publisher_node
        else None
    )
    builder.StartObject(_NUM_FIELDS)
    builder.PrependUint64Slot(_SLOT_PUBLISH_STAMP_NS, int(publish_stamp_ns), 0)
    if node_off is not None:
        builder.PrependUOffsetTRelativeSlot(_SLOT_PUBLISHER_NODE, node_off, 0)
    builder.PrependUint64Slot(_SLOT_SEQUENCE, int(sequence), 0)
    return builder.EndObject()


@dataclass(frozen=True)
class AortaHeader:
    """Decoded AortaHeader. Absent fields take their FlatBuffers defaults."""

    publish_stamp_ns: int
    publisher_node: str
    sequence: int


def read_aorta_header(buf: bytes, table_pos: int) -> AortaHeader:
    """Decode an AortaHeader table located at ``table_pos`` in ``buf``."""
    tab = _table.Table(buf, table_pos)
    stamp = 0
    off = tab.Offset(_VT_PUBLISH_STAMP_NS)
    if off:
        stamp = tab.Get(_N.Uint64Flags, tab.Pos + off)
    node = ""
    off = tab.Offset(_VT_PUBLISHER_NODE)
    if off:
        node = tab.String(tab.Pos + off).decode("utf-8")
    seq = 0
    off = tab.Offset(_VT_SEQUENCE)
    if off:
        seq = tab.Get(_N.Uint64Flags, tab.Pos + off)
    return AortaHeader(int(stamp), node, int(seq))
