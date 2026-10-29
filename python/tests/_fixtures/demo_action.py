"""A hand-written demo ActionCodec for tests — proves the Phase-4 action machinery.

Demo action: Goal{value:i64}, Result{value:i64}, FeedbackData{value:i64}. The
wrapper tables are built/parsed with FlatBuffers primitives (the field slots match
the generated wrapper schema in the Phase-4 contract). The encoding only needs to
be self-consistent between this codec's build and parse sides (Core never parses
the wrapper tables — it only calls the codec to decode goal_id/secret and to
encode responses), so primitives are sufficient; no flatc/object-API is required.

For real actions, users generate a codec from their action schema (a Phase-5
codegen rule); this demonstrates the contract the SDK expects.
"""

from __future__ import annotations

import flatbuffers
from flatbuffers import number_types as N
from flatbuffers import table as _table

from aorta.action import (
    ActionCodec,
    ActionInfo,
    ActionReplyCode,
    ActionStatus,
    CancelResponse,
    FeedbackSample,
    GetResultResponse,
    PublicStatus,
    SendGoalResponse,
)

# Any consistent non-empty bytes: Core stores + sha256-hashes them, never parses.
DEMO_SCHEMA_BFBS = b"aorta.demo.calc.action.wrapper.v1"


def _vt(slot: int) -> int:
    return 4 + slot * 2


def _root(buf: bytes) -> "_table.Table":
    pos = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0)
    return _table.Table(buf, pos)


def _str(tab, slot: int) -> str:
    off = tab.Offset(_vt(slot))
    return tab.String(tab.Pos + off).decode("utf-8") if off else ""


def _scalar(tab, slot: int, flags, default=0):
    off = tab.Offset(_vt(slot))
    return tab.Get(flags, tab.Pos + off) if off else default


def _subtable_i64(tab, slot: int):
    """Read a nested single-i64 table field; return (present, value)."""
    off = tab.Offset(_vt(slot))
    if not off:
        return False, 0
    sub = _table.Table(tab.Bytes, tab.Indirect(tab.Pos + off))
    voff = sub.Offset(_vt(0))
    return True, (sub.Get(N.Int64Flags, sub.Pos + voff) if voff else 0)


def _build_i64_table(b, value: int) -> int:
    b.StartObject(1)
    b.PrependInt64Slot(0, int(value), 0)
    return b.EndObject()


def _decode_i64_table_bytes(payload: bytes):
    """Decode a standalone Goal/Result/FeedbackData{value:i64} buffer → int|None."""
    if not payload:
        return None
    tab = _root(payload)
    voff = tab.Offset(_vt(0))
    return tab.Get(N.Int64Flags, tab.Pos + voff) if voff else 0


def build_value_table(value: int) -> bytes:
    """Serialize a domain table {value:i64} (a Result/FeedbackData payload)."""
    b = flatbuffers.Builder(64)
    b.Finish(_build_i64_table(b, value))
    return bytes(b.Output())


class DemoDomainT:
    """Minimal object-API-compatible class for a ``{value:i64}`` domain table —
    mirrors flatc ``--gen-object-api`` (``Pack`` / ``InitFromBuf``) just enough to
    drive ``aorta.FlatbuffersActionCodec`` in unit tests without a bazel-generated
    dep. A real action uses the generated ``<Goal>T`` / ``<Result>T`` / etc."""

    def __init__(self, value: int = 0) -> None:
        self.value = value

    @classmethod
    def InitFromPackedBuf(cls, buf, pos: int) -> "DemoDomainT":
        tab = _table.Table(buf, pos + flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, pos))
        voff = tab.Offset(_vt(0))
        return cls(tab.Get(N.Int64Flags, tab.Pos + voff) if voff else 0)

    def Pack(self, builder) -> int:
        return _build_i64_table(builder, self.value)


class DemoActionCodec(ActionCodec):
    # ── server decode ────────────────────────────────────────────────────────
    def decode_send_goal_request_goal_id(self, request: bytes) -> str:
        return _str(_root(request), 0)  # SendGoalRequest.goal_id

    def decode_cancel_goal_request(self, request: bytes):
        t = _root(request)
        return _str(t, 0), _str(t, 1)

    def decode_get_result_request(self, request: bytes):
        t = _root(request)
        return _str(t, 0), _str(t, 1)

    def decode_goal_value(self, request: bytes):  # helper for RetainedActionGoal.decode
        present, value = _subtable_i64(_root(request), 1)  # SendGoalRequest.goal
        return value if present else None

    # ── server encode ────────────────────────────────────────────────────────
    def encode_send_goal_response(self, accepted, goal_id, goal_secret, code, status, message, server_epoch):
        b = flatbuffers.Builder(128)
        gid = b.CreateString(goal_id)
        sec = b.CreateString(goal_secret)
        msg = b.CreateString(message)
        ep = b.CreateString(server_epoch)
        b.StartObject(7)
        b.PrependBoolSlot(0, bool(accepted), False)
        b.PrependUOffsetTRelativeSlot(1, gid, 0)
        b.PrependUOffsetTRelativeSlot(2, sec, 0)
        b.PrependInt16Slot(3, int(code), 0)
        b.PrependInt8Slot(4, int(status), 0)
        b.PrependUOffsetTRelativeSlot(5, msg, 0)
        b.PrependUOffsetTRelativeSlot(6, ep, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def encode_cancel_response(self, accepted, goal_id, code, status, message):
        b = flatbuffers.Builder(96)
        gid = b.CreateString(goal_id)
        msg = b.CreateString(message)
        b.StartObject(5)
        b.PrependBoolSlot(0, bool(accepted), False)
        b.PrependUOffsetTRelativeSlot(1, gid, 0)
        b.PrependInt16Slot(2, int(code), 0)
        b.PrependInt8Slot(3, int(status), 0)
        b.PrependUOffsetTRelativeSlot(4, msg, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def encode_public_status(self, goal_id, status, code, accepted_ns, started_ns, finished_ns, last_feedback_seq, server_epoch):
        b = flatbuffers.Builder(96)
        gid = b.CreateString(goal_id)
        ep = b.CreateString(server_epoch)
        b.StartObject(8)
        b.PrependUOffsetTRelativeSlot(0, gid, 0)
        b.PrependInt8Slot(1, int(status), 0)
        b.PrependInt16Slot(2, int(code), 0)
        b.PrependUint64Slot(3, int(accepted_ns), 0)
        b.PrependUint64Slot(4, int(started_ns), 0)
        b.PrependUint64Slot(5, int(finished_ns), 0)
        b.PrependUint64Slot(6, int(last_feedback_seq), 0)
        b.PrependUOffsetTRelativeSlot(7, ep, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def encode_feedback(self, goal_id, seq, stamp_ns, status, feedback_data, message):
        b = flatbuffers.Builder(96)
        value = _decode_i64_table_bytes(feedback_data)
        data_off = _build_i64_table(b, value) if value is not None else None
        gid = b.CreateString(goal_id)
        msg = b.CreateString(message)
        b.StartObject(6)
        b.PrependUOffsetTRelativeSlot(0, gid, 0)
        b.PrependUint64Slot(1, int(seq), 0)
        b.PrependUint64Slot(2, int(stamp_ns), 0)
        b.PrependInt8Slot(3, int(status), 0)
        if data_off is not None:
            b.PrependUOffsetTRelativeSlot(4, data_off, 0)
        b.PrependUOffsetTRelativeSlot(5, msg, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def encode_get_result_response(self, goal_id, status, accepted_ns, started_ns, finished_ns, last_feedback_seq, feedback_snapshot, result_payload, code, message, server_epoch):
        b = flatbuffers.Builder(160)
        snap_value = _decode_i64_table_bytes(feedback_snapshot)
        result_value = _decode_i64_table_bytes(result_payload)
        snap_off = _build_i64_table(b, snap_value) if snap_value is not None else None
        result_off = _build_i64_table(b, result_value) if result_value is not None else None
        gid = b.CreateString(goal_id)
        msg = b.CreateString(message)
        ep = b.CreateString(server_epoch)
        b.StartObject(11)
        b.PrependUOffsetTRelativeSlot(0, gid, 0)
        b.PrependInt8Slot(1, int(status), 0)
        b.PrependUint64Slot(2, int(accepted_ns), 0)
        b.PrependUint64Slot(3, int(started_ns), 0)
        b.PrependUint64Slot(4, int(finished_ns), 0)
        b.PrependUint64Slot(5, int(last_feedback_seq), 0)
        if snap_off is not None:
            b.PrependUOffsetTRelativeSlot(6, snap_off, 0)
        if result_off is not None:
            b.PrependUOffsetTRelativeSlot(7, result_off, 0)
        b.PrependInt16Slot(8, int(code), 0)
        b.PrependUOffsetTRelativeSlot(9, msg, 0)
        b.PrependUOffsetTRelativeSlot(10, ep, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    # ── client build/parse ───────────────────────────────────────────────────
    def build_send_goal_request(self, goal_id: str, goal) -> bytes:
        b = flatbuffers.Builder(96)
        goal_off = _build_i64_table(b, int(goal))  # goal is the demo's int value
        gid = b.CreateString(goal_id)
        b.StartObject(2)
        b.PrependUOffsetTRelativeSlot(0, gid, 0)
        b.PrependUOffsetTRelativeSlot(1, goal_off, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def parse_send_goal_response(self, data: bytes) -> SendGoalResponse:
        t = _root(data)
        return SendGoalResponse(
            accepted=bool(_scalar(t, 0, N.BoolFlags, False)),
            goal_id=_str(t, 1),
            goal_secret=_str(t, 2),
            code=ActionReplyCode(_scalar(t, 3, N.Int16Flags)),
            status=ActionStatus(_scalar(t, 4, N.Int8Flags)),
            message=_str(t, 5),
            server_epoch=_str(t, 6),
        )

    def build_cancel_request(self, goal_id: str, goal_secret: str) -> bytes:
        return self._build_two_string(goal_id, goal_secret)

    def build_get_result_request(self, goal_id: str, goal_secret: str) -> bytes:
        return self._build_two_string(goal_id, goal_secret)

    def _build_two_string(self, a: str, c: str) -> bytes:
        b = flatbuffers.Builder(64)
        ao = b.CreateString(a)
        co = b.CreateString(c)
        b.StartObject(2)
        b.PrependUOffsetTRelativeSlot(0, ao, 0)
        b.PrependUOffsetTRelativeSlot(1, co, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def parse_cancel_response(self, data: bytes) -> CancelResponse:
        t = _root(data)
        return CancelResponse(
            accepted=bool(_scalar(t, 0, N.BoolFlags, False)),
            goal_id=_str(t, 1),
            code=ActionReplyCode(_scalar(t, 2, N.Int16Flags)),
            status=ActionStatus(_scalar(t, 3, N.Int8Flags)),
            message=_str(t, 4),
        )

    def parse_get_result_response(self, data: bytes) -> GetResultResponse:
        t = _root(data)
        _, snap = _subtable_i64(t, 6)
        has_result, result_value = _subtable_i64(t, 7)
        return GetResultResponse(
            goal_id=_str(t, 0),
            status=ActionStatus(_scalar(t, 1, N.Int8Flags)),
            accepted_stamp_ns=_scalar(t, 2, N.Uint64Flags),
            started_stamp_ns=_scalar(t, 3, N.Uint64Flags),
            finished_stamp_ns=_scalar(t, 4, N.Uint64Flags),
            last_feedback_seq=_scalar(t, 5, N.Uint64Flags),
            feedback_snapshot=build_value_table(snap) if _subtable_i64(t, 6)[0] else b"",
            result=build_value_table(result_value) if has_result else b"",
            code=ActionReplyCode(_scalar(t, 8, N.Int16Flags)),
            message=_str(t, 9),
            server_epoch=_str(t, 10),
        )

    def parse_public_status(self, data: bytes) -> PublicStatus:
        t = _root(data)
        return PublicStatus(
            goal_id=_str(t, 0),
            status=ActionStatus(_scalar(t, 1, N.Int8Flags)),
            code=ActionReplyCode(_scalar(t, 2, N.Int16Flags)),
            accepted_stamp_ns=_scalar(t, 3, N.Uint64Flags),
            started_stamp_ns=_scalar(t, 4, N.Uint64Flags),
            finished_stamp_ns=_scalar(t, 5, N.Uint64Flags),
            last_feedback_seq=_scalar(t, 6, N.Uint64Flags),
            server_epoch=_str(t, 7),
        )

    def parse_feedback(self, data: bytes) -> FeedbackSample:
        t = _root(data)
        has_data, value = _subtable_i64(t, 4)
        return FeedbackSample(
            goal_id=_str(t, 0),
            seq=_scalar(t, 1, N.Uint64Flags),
            stamp_ns=_scalar(t, 2, N.Uint64Flags),
            status=ActionStatus(_scalar(t, 3, N.Int8Flags)),
            data=build_value_table(value) if has_data else b"",
            message=_str(t, 5),
        )

    def parse_action_info(self, data: bytes) -> ActionInfo:
        t = _root(data)
        return ActionInfo(
            action_name=_str(t, 0),
            schema_hash_hex=_str(t, 1),
            supports_cancel=bool(_scalar(t, 2, N.BoolFlags, True)),
            max_concurrency=_scalar(t, 3, N.Uint32Flags),
            result_ttl_ms=_scalar(t, 4, N.Uint64Flags),
            dedup_ttl_ms=_scalar(t, 5, N.Uint64Flags),
            server_node_name=_str(t, 6),
            server_epoch=_str(t, 7),
        )
