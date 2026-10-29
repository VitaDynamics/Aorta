"""Action client + server + the 8-fn codec (Phase 4).

The action *protocol* (goal_id/secret/epoch, status/code, public status,
feedback, result) is generic; the *wire encoding* of a specific action's wrapper
FlatBuffers tables is supplied by an :class:`ActionCodec` adapter (hand-written
or generated from the action schema). The SDK wires the adapter into the C-ABI:

- The 8 server codec callbacks are created **per server** as ctypes closures over
  the adapter (the C-ABI codec fns take no user_data, so per-Action statics — as
  C++ does via templates — are the model; in Python each server keeps its own
  closures alive). Crash-safety: every callback try/excepts and returns an error
  code (or aborts the goal) — a Python exception must never cross the C boundary.
- Decode/encode out-params (``aorta_string_view_t`` / ``aorta_owned_bytes_view_t``)
  are read by Core *after* the callback returns, so their backing buffers live on
  a thread-local scratch (the C++ ``thread_local`` pattern), cleared at the start
  of the next codec call on that thread.

``ActionStatus`` is ``i8`` and ``ActionReplyCode`` is ``i16`` — signed, mandatory.
"""

from __future__ import annotations

# Make ``aorta.action`` path-extending for the same reason ``aorta`` itself is (see
# ``aorta/__init__.py``): the generated FlatBuffers modules for ``namespace
# aorta.action.*`` schemas live in a DIFFERENT ``sys.path`` root than this SDK
# package whenever the two are separate Bazel targets (``//python:aorta_py`` plus an
# action schema's ``<stem>_py_lib``). Extending ``aorta.__path__`` alone is not
# enough — once this regular package is selected for ``aorta.action``, its own
# ``__path__`` bounds the search for ``aorta.action.demo`` & friends, so they would
# raise ``ModuleNotFoundError``. Co-installed wheels happen to work without this
# only because pip merges both trees into one physical directory.
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

import ctypes
import itertools
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, List, Optional, Tuple

import flatbuffers
from flatbuffers import number_types as _N
from flatbuffers import table as _fbtable

# This module is a PACKAGE (`aorta/action/__init__.py`), not `aorta/action.py`, so
# `.` resolves to `aorta.action` — the parent-relative `..` is required. The package
# form exists so the generated `aorta.action.*` message modules from the aorta_msgs
# wheel can land as subpackages here instead of shadowing this module; see
# message/README.md "Python Bindings" and python_msgs_tree in
# message/tools/flatbuffers_rules.bzl.
from .. import errors, logging as _log
from .._ffi import bindings, loader, structs

_OK = 0
_ERR_INTERNAL = -5


def normalize_action_name(lib: ctypes.CDLL, action_name: str) -> str:
    """Canonicalize ``action_name`` to the SAME key form the core routes under,
    via the C-ABI ``aorta_normalize_action_name`` (additive). Storing the
    normalized name makes ``ActionClient.action_name()`` / server ``name()`` match
    the Rust/C++ getters, which return the normalized name. The core re-normalizes
    idempotently, so passing the normalized name on to the constructor is a no-op;
    a malformed name raises here (parity with Rust ``try_new`` / C++
    ``NormalizeActionName``) rather than from the constructor. The normalized form
    is never longer than the input, so ``len(input)+1`` bytes always suffice.
    """
    raw = action_name.encode("utf-8")
    buf = ctypes.create_string_buffer(len(raw) + 1)
    out_len = ctypes.c_size_t(0)
    rc = lib.aorta_normalize_action_name(raw, buf, len(buf), ctypes.byref(out_len))
    errors.check_rc(lib, rc, "aorta_normalize_action_name")
    return buf.raw[: out_len.value].decode("utf-8")


class ActionStatus(IntEnum):
    """Goal lifecycle status (``#[repr(i8)]``). Terminal = SUCCEEDED/ABORTED/CANCELED."""

    UNKNOWN = 0
    ACCEPTED = 1
    EXECUTING = 2
    CANCELING = 3
    SUCCEEDED = 4
    ABORTED = 5
    CANCELED = 6

    def is_terminal(self) -> bool:
        return self in (ActionStatus.SUCCEEDED, ActionStatus.ABORTED, ActionStatus.CANCELED)


class ActionReplyCode(IntEnum):
    """Action control reply code (``#[repr(i16)]``)."""

    NONE = 0
    OK = 1
    DUPLICATE_ACCEPTED = 2
    SUCCEEDED = 100
    CANCELED = 101
    BUSY = 200
    PAYLOAD_CONFLICT = 201
    UNAUTHORIZED = 202
    NOT_FOUND = 203
    ALREADY_TERMINAL = 204
    GOAL_EXPIRED = 205
    HANDLER_ERROR = 300
    HANDLER_PANIC = 301
    HANDLER_COMPLETED_WITHOUT_RESULT = 302
    CONTEXT_DROPPED = 303
    SERVER_CRASH = 304
    CONFLICT = 400


# ── result dataclasses (decoded wrapper responses) ───────────────────────────
@dataclass(frozen=True)
class SendGoalResponse:
    accepted: bool
    goal_id: str
    goal_secret: str
    code: ActionReplyCode
    status: ActionStatus
    message: str
    server_epoch: str


@dataclass(frozen=True)
class CancelResponse:
    accepted: bool
    goal_id: str
    code: ActionReplyCode
    status: ActionStatus
    message: str


@dataclass(frozen=True)
class PublicStatus:
    goal_id: str
    status: ActionStatus
    code: ActionReplyCode
    accepted_stamp_ns: int
    started_stamp_ns: int
    finished_stamp_ns: int
    last_feedback_seq: int
    server_epoch: str


@dataclass(frozen=True)
class GetResultResponse:
    goal_id: str
    status: ActionStatus
    accepted_stamp_ns: int
    started_stamp_ns: int
    finished_stamp_ns: int
    last_feedback_seq: int
    feedback_snapshot: bytes  # raw FeedbackData bytes (may be empty)
    result: bytes  # raw Result bytes (empty until terminal-succeeded)
    code: ActionReplyCode
    message: str
    server_epoch: str


@dataclass(frozen=True)
class FeedbackSample:
    goal_id: str
    seq: int
    stamp_ns: int
    status: ActionStatus
    data: bytes  # raw FeedbackData bytes
    message: str


@dataclass(frozen=True)
class ActionInfo:
    action_name: str
    schema_hash_hex: str
    supports_cancel: bool
    max_concurrency: int
    result_ttl_ms: int
    dedup_ttl_ms: int
    server_node_name: str
    server_epoch: str


class ActionCodec(ABC):
    """Adapter that encodes/decodes a specific action's wrapper FlatBuffers tables.

    The SDK calls these with Python-native types; implementations build/parse the
    generated wrapper tables. ``feedback_data``/``result`` are raw domain-table
    bytes that the encode methods must embed as the wrapper's nested table fields.
    """

    # server-side decode (request bytes -> ids)
    @abstractmethod
    def decode_send_goal_request_goal_id(self, request: bytes) -> str: ...
    @abstractmethod
    def decode_cancel_goal_request(self, request: bytes) -> Tuple[str, str]: ...
    @abstractmethod
    def decode_get_result_request(self, request: bytes) -> Tuple[str, str]: ...
    # server-side encode (fields -> wrapper bytes)
    @abstractmethod
    def encode_send_goal_response(self, accepted, goal_id, goal_secret, code, status, message, server_epoch) -> bytes: ...
    @abstractmethod
    def encode_cancel_response(self, accepted, goal_id, code, status, message) -> bytes: ...
    @abstractmethod
    def encode_public_status(self, goal_id, status, code, accepted_ns, started_ns, finished_ns, last_feedback_seq, server_epoch) -> bytes: ...
    @abstractmethod
    def encode_feedback(self, goal_id, seq, stamp_ns, status, feedback_data, message) -> bytes: ...
    @abstractmethod
    def encode_get_result_response(self, goal_id, status, accepted_ns, started_ns, finished_ns, last_feedback_seq, feedback_snapshot, result_payload, code, message, server_epoch) -> bytes: ...
    # client-side build/parse
    @abstractmethod
    def build_send_goal_request(self, goal_id: str, goal: Any) -> bytes: ...
    @abstractmethod
    def parse_send_goal_response(self, data: bytes) -> SendGoalResponse: ...
    @abstractmethod
    def build_cancel_request(self, goal_id: str, goal_secret: str) -> bytes: ...
    @abstractmethod
    def parse_cancel_response(self, data: bytes) -> CancelResponse: ...
    @abstractmethod
    def build_get_result_request(self, goal_id: str, goal_secret: str) -> bytes: ...
    @abstractmethod
    def parse_get_result_response(self, data: bytes) -> GetResultResponse: ...
    @abstractmethod
    def parse_public_status(self, data: bytes) -> PublicStatus: ...
    @abstractmethod
    def parse_feedback(self, data: bytes) -> FeedbackSample: ...
    @abstractmethod
    def parse_action_info(self, data: bytes) -> ActionInfo: ...


# ── Generic FlatBuffers action codec (no hand-written / generated codec) ──────
# The action wrapper tables have a FIXED layout (message/tools/gen_action_wrapper.py):
# only the goal/result/feedback DOMAIN fields are schema-specific, and they are
# real NESTED TABLES. With the flatc object API (`--gen-object-api`, on by default
# in the repo's python codegen) a pre-serialized domain message is spliced as a
# nested table via `<T>.InitFromPackedBuf(bytes).Pack(builder)`, so ONE generic codec —
# parameterized only by the three domain object-API classes — works for every
# action, wire-compatible with the Rust/C++ generated wrappers.


def _avt(slot: int) -> int:
    return 4 + slot * 2


def _aroot(buf: bytes) -> "_fbtable.Table":
    return _fbtable.Table(buf, flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, 0))


def _astr(tab, slot: int) -> str:
    off = tab.Offset(_avt(slot))
    return tab.String(tab.Pos + off).decode("utf-8") if off else ""


def _ascalar(tab, slot: int, flags, default=0):
    off = tab.Offset(_avt(slot))
    return tab.Get(flags, tab.Pos + off) if off else default


def _splice_domain(builder, domain_t, payload: bytes):
    """Embed pre-serialized domain ``payload`` as a NESTED TABLE; offset or None."""
    if not payload:
        return None
    return domain_t.InitFromPackedBuf(bytes(payload), 0).Pack(builder)


def _extract_domain(tab, slot: int, domain_t) -> bytes:
    """Read the nested domain table at ``slot`` back to a standalone buffer."""
    off = tab.Offset(_avt(slot))
    if not off:
        return b""
    # The field position holds the table's uoffset; InitFromBuf(buf, field_pos)
    # follows it exactly like tab.Indirect, so no separate reader class is needed.
    obj = domain_t.InitFromPackedBuf(tab.Bytes, tab.Pos + off)
    b = flatbuffers.Builder(64)
    b.Finish(obj.Pack(b))
    return bytes(b.Output())


class FlatbuffersActionCodec(ActionCodec):
    """A ready-made :class:`ActionCodec` for any action — no hand-written codec.

    Pass the three generated **object-API** domain classes (``<Goal>T`` /
    ``<Result>T`` / ``<FeedbackData>T`` from a ``--gen-object-api`` schema). Domain
    payloads cross the SDK boundary as **bytes** (serialize an object-API message
    with :meth:`serialize`); this codec splices them as the wrapper's nested
    tables, wire-compatible with the Rust/C++ generated codecs.
    """

    def __init__(self, *, goal, result, feedback) -> None:
        self._goal = goal
        self._result = result
        self._feedback = feedback

    @staticmethod
    def serialize(domain_obj) -> bytes:
        """Serialize an object-API domain message (``<Name>T``) to standalone bytes
        (for ``send_goal`` / ``ctx.succeed`` / ``ctx.publish_feedback``)."""
        b = flatbuffers.Builder(64)
        b.Finish(domain_obj.Pack(b))
        return bytes(b.Output())

    def decode_goal(self, request: bytes):
        """``goal_decoder`` helper: the embedded goal as a ``<Goal>T`` object."""
        t = _aroot(request)
        off = t.Offset(_avt(1))
        return self._goal.InitFromPackedBuf(t.Bytes, t.Pos + off) if off else None

    # ── server decode ────────────────────────────────────────────────────────
    def decode_send_goal_request_goal_id(self, request: bytes) -> str:
        return _astr(_aroot(request), 0)

    def decode_cancel_goal_request(self, request: bytes):
        t = _aroot(request)
        return _astr(t, 0), _astr(t, 1)

    def decode_get_result_request(self, request: bytes):
        t = _aroot(request)
        return _astr(t, 0), _astr(t, 1)

    # ── server encode ────────────────────────────────────────────────────────
    def encode_send_goal_response(self, accepted, goal_id, goal_secret, code, status, message, server_epoch):
        b = flatbuffers.Builder(128)
        gid, sec = b.CreateString(goal_id), b.CreateString(goal_secret)
        msg, ep = b.CreateString(message), b.CreateString(server_epoch)
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
        gid, msg = b.CreateString(goal_id), b.CreateString(message)
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
        gid, ep = b.CreateString(goal_id), b.CreateString(server_epoch)
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
        b = flatbuffers.Builder(128)
        data_off = _splice_domain(b, self._feedback, feedback_data)
        gid, msg = b.CreateString(goal_id), b.CreateString(message)
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
        b = flatbuffers.Builder(192)
        snap_off = _splice_domain(b, self._feedback, feedback_snapshot)
        result_off = _splice_domain(b, self._result, result_payload)
        gid, msg, ep = b.CreateString(goal_id), b.CreateString(message), b.CreateString(server_epoch)
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
        b = flatbuffers.Builder(128)
        goal_off = _splice_domain(b, self._goal, goal)
        gid = b.CreateString(goal_id)
        b.StartObject(2)
        b.PrependUOffsetTRelativeSlot(0, gid, 0)
        if goal_off is not None:
            b.PrependUOffsetTRelativeSlot(1, goal_off, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def parse_send_goal_response(self, data: bytes) -> SendGoalResponse:
        t = _aroot(data)
        return SendGoalResponse(
            accepted=bool(_ascalar(t, 0, _N.BoolFlags, False)),
            goal_id=_astr(t, 1), goal_secret=_astr(t, 2),
            code=ActionReplyCode(_ascalar(t, 3, _N.Int16Flags)),
            status=ActionStatus(_ascalar(t, 4, _N.Int8Flags)),
            message=_astr(t, 5), server_epoch=_astr(t, 6),
        )

    def build_cancel_request(self, goal_id, goal_secret):
        return self._two_string(goal_id, goal_secret)

    def build_get_result_request(self, goal_id, goal_secret):
        return self._two_string(goal_id, goal_secret)

    def _two_string(self, a: str, c: str) -> bytes:
        b = flatbuffers.Builder(64)
        ao, co = b.CreateString(a), b.CreateString(c)
        b.StartObject(2)
        b.PrependUOffsetTRelativeSlot(0, ao, 0)
        b.PrependUOffsetTRelativeSlot(1, co, 0)
        b.Finish(b.EndObject())
        return bytes(b.Output())

    def parse_cancel_response(self, data: bytes) -> CancelResponse:
        t = _aroot(data)
        return CancelResponse(
            accepted=bool(_ascalar(t, 0, _N.BoolFlags, False)), goal_id=_astr(t, 1),
            code=ActionReplyCode(_ascalar(t, 2, _N.Int16Flags)),
            status=ActionStatus(_ascalar(t, 3, _N.Int8Flags)), message=_astr(t, 4),
        )

    def parse_get_result_response(self, data: bytes) -> GetResultResponse:
        t = _aroot(data)
        return GetResultResponse(
            goal_id=_astr(t, 0), status=ActionStatus(_ascalar(t, 1, _N.Int8Flags)),
            accepted_stamp_ns=_ascalar(t, 2, _N.Uint64Flags),
            started_stamp_ns=_ascalar(t, 3, _N.Uint64Flags),
            finished_stamp_ns=_ascalar(t, 4, _N.Uint64Flags),
            last_feedback_seq=_ascalar(t, 5, _N.Uint64Flags),
            feedback_snapshot=_extract_domain(t, 6, self._feedback),
            result=_extract_domain(t, 7, self._result),
            code=ActionReplyCode(_ascalar(t, 8, _N.Int16Flags)),
            message=_astr(t, 9), server_epoch=_astr(t, 10),
        )

    def parse_public_status(self, data: bytes) -> PublicStatus:
        t = _aroot(data)
        return PublicStatus(
            goal_id=_astr(t, 0), status=ActionStatus(_ascalar(t, 1, _N.Int8Flags)),
            code=ActionReplyCode(_ascalar(t, 2, _N.Int16Flags)),
            accepted_stamp_ns=_ascalar(t, 3, _N.Uint64Flags),
            started_stamp_ns=_ascalar(t, 4, _N.Uint64Flags),
            finished_stamp_ns=_ascalar(t, 5, _N.Uint64Flags),
            last_feedback_seq=_ascalar(t, 6, _N.Uint64Flags), server_epoch=_astr(t, 7),
        )

    def parse_feedback(self, data: bytes) -> FeedbackSample:
        t = _aroot(data)
        return FeedbackSample(
            goal_id=_astr(t, 0), seq=_ascalar(t, 1, _N.Uint64Flags),
            stamp_ns=_ascalar(t, 2, _N.Uint64Flags),
            status=ActionStatus(_ascalar(t, 3, _N.Int8Flags)),
            data=_extract_domain(t, 4, self._feedback), message=_astr(t, 5),
        )

    def parse_action_info(self, data: bytes) -> ActionInfo:
        t = _aroot(data)
        return ActionInfo(
            action_name=_astr(t, 0), schema_hash_hex=_astr(t, 1),
            supports_cancel=bool(_ascalar(t, 2, _N.BoolFlags, True)),
            max_concurrency=_ascalar(t, 3, _N.Uint32Flags),
            result_ttl_ms=_ascalar(t, 4, _N.Uint64Flags),
            dedup_ttl_ms=_ascalar(t, 5, _N.Uint64Flags),
            server_node_name=_astr(t, 6), server_epoch=_astr(t, 7),
        )


# ── FFI marshaling helpers ───────────────────────────────────────────────────
_scratch = threading.local()


def _scratch_reset() -> List:
    """Clear + return this thread's codec out-param keepalive list."""
    bufs: List = []
    _scratch.bufs = bufs
    return bufs


def _sv_str(sv) -> str:
    """Decode a by-value ``aorta_string_view_t`` argument into str."""
    if sv.ptr and sv.len:
        return ctypes.string_at(sv.ptr, sv.len).decode("utf-8", "replace")
    return ""


def _write_string_view(out_sv_ptr, text: str, keep: List) -> None:
    """Point ``*out_sv_ptr`` at a freshly-copied buffer kept alive in ``keep``."""
    raw = text.encode("utf-8")
    sv = out_sv_ptr.contents
    if raw:
        arr = (ctypes.c_char * len(raw)).from_buffer_copy(raw)
        keep.append(arr)
        sv.ptr = ctypes.cast(arr, ctypes.c_void_p)
        sv.len = len(raw)
    else:
        sv.ptr = None
        sv.len = 0


# Codec encode out-params: Core reads the out-param AFTER the codec callback
# returns, and may call ANOTHER codec fn in between (e.g. encode_public_status
# between encode_send_goal_response and the copy), so a per-call keepalive would
# dangle. Instead Core OWNS the buffer and frees it via a release callback (the
# C++ codec's FillOwnedActionBytesView model). The registry pins (arr, slice)
# until Core releases.
_owned_registry: dict = {}
_owned_lock = threading.Lock()
_owned_counter = itertools.count(1)


def _release_owned(release_ctx, _data, _length) -> None:
    # Runs on a Core thread when the buffer is no longer needed. Must not raise.
    try:
        with _owned_lock:
            _owned_registry.pop(int(release_ctx), None)
    except BaseException:  # pragma: no cover - release must never cross C boundary
        pass


_RELEASE_CB = bindings.RELEASE_BYTES_FN(_release_owned)


def _write_owned_view(out_obv_ptr, data: bytes) -> None:
    """Fill ``*out_obv_ptr`` with a single slice that Core OWNS (freed via the
    release callback). Correct for codec encode out-params (read post-callback)."""
    buf = bytes(data)
    n = len(buf)
    token = next(_owned_counter)
    arr = (ctypes.c_ubyte * n).from_buffer_copy(buf) if n else (ctypes.c_ubyte * 1)()
    slc = structs.AortaOwnedBytesSlice()
    slc.ptr = ctypes.cast(arr, ctypes.c_void_p) if n else None
    slc.len = n
    slc.release = ctypes.cast(_RELEASE_CB, ctypes.c_void_p)
    slc.release_ctx = ctypes.c_void_p(token)
    with _owned_lock:
        _owned_registry[token] = (arr, slc)  # pinned until Core releases
    dst = out_obv_ptr.contents
    dst.slices = ctypes.addressof(slc)
    dst.slice_count = 1
    dst.total_len = n


def _read_owned_response(lib, resp) -> bytes:
    """Copy an ``aorta_owned_bytes_t`` response into bytes + free it."""
    try:
        return bindings.bytes_from_view(resp.view)
    finally:
        lib.aorta_owned_bytes_free(ctypes.byref(resp))


def _opt_cstr(message: Optional[str]):
    return None if message is None else message.encode("utf-8")


# ── ActionExecutionContext (runs inside the execute callback thread) ──────────
class ActionExecutionContext:
    """Handed to the action handler. Drive the goal to a terminal state with
    ``succeed`` / ``abort`` / ``cancel``; publish progress with ``publish_feedback``.
    ``is_cancel_requested`` polls for a cancel (the C-ABI exposes no await form).
    """

    def __init__(self, lib, handle: int, goal_id: str) -> None:
        self._lib = lib
        self._handle = handle
        self._goal_id = goal_id

    def goal_id(self) -> str:
        return self._goal_id

    def is_cancel_requested(self) -> bool:
        out = ctypes.c_uint8(0)
        rc = self._lib.aorta_action_context_is_cancel_requested(
            self._handle, ctypes.byref(out)
        )
        errors.check_rc(self._lib, rc, "aorta_action_context_is_cancel_requested")
        return bool(out.value)

    def publish_feedback(self, feedback_data: bytes, message: str = "") -> None:
        view, _ka = bindings.borrowed_view_from_bytes(feedback_data)
        rc = self._lib.aorta_action_context_publish_feedback(
            self._handle, ctypes.byref(view), _opt_cstr(message)
        )
        errors.check_rc(self._lib, rc, "aorta_action_context_publish_feedback")

    def succeed(self, result_payload: bytes) -> None:
        view, _ka = bindings.borrowed_view_from_bytes(result_payload)
        rc = self._lib.aorta_action_context_succeed(self._handle, ctypes.byref(view))
        errors.check_rc(self._lib, rc, "aorta_action_context_succeed")

    def abort(self, code: ActionReplyCode, message: str = "") -> None:
        rc = self._lib.aorta_action_context_abort(
            self._handle, ctypes.c_int16(int(code)), _opt_cstr(message)
        )
        errors.check_rc(self._lib, rc, "aorta_action_context_abort")

    def cancel(self, code: ActionReplyCode, message: str = "") -> None:
        rc = self._lib.aorta_action_context_cancel(
            self._handle, ctypes.c_int16(int(code)), _opt_cstr(message)
        )
        errors.check_rc(self._lib, rc, "aorta_action_context_cancel")

    def _free(self) -> None:
        self._lib.aorta_action_context_free(self._handle)


class RetainedActionGoal:
    """The goal payload handed to the handler. ``decode`` runs the codec's goal
    decoder (if provided)."""

    def __init__(self, goal_id: str, payload: bytes, goal_decoder=None) -> None:
        self._goal_id = goal_id
        self._payload = payload
        self._goal_decoder = goal_decoder

    def goal_id(self) -> str:
        return self._goal_id

    def payload(self) -> bytes:
        return self._payload

    def decode(self) -> Any:
        if self._goal_decoder is None:
            raise errors.AortaError("no goal decoder configured for this action")
        return self._goal_decoder(self._payload)


# Action handler: (goal: RetainedActionGoal, ctx: ActionExecutionContext) -> None
ActionHandler = Callable[[RetainedActionGoal, ActionExecutionContext], None]


@dataclass
class ActionServerOptions:
    supports_cancel: bool = True
    max_concurrency: int = 1
    result_ttl_ms: int = 60000
    dedup_ttl_ms: int = 300000


@dataclass
class ActionClientOptions:
    control_timeout_ms: int = 10000
    info_timeout_ms: int = 5000


class ActionServer:
    """An action server. Created via ``Node.create_action_server``.

    The handler runs ``handler(goal, ctx)`` on a worker thread. If it returns
    without calling a terminal op, Core auto-aborts (HandlerCompletedWithoutResult);
    if it raises, the trampoline calls ``ctx.abort(HANDLER_ERROR, ...)``.
    """

    def __init__(
        self,
        node,
        action_name: str,
        schema_bfbs: bytes,
        codec: ActionCodec,
        handler: ActionHandler,
        *,
        options: Optional[ActionServerOptions] = None,
        goal_decoder=None,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        action_name = normalize_action_name(self._lib, action_name)
        self._name = action_name
        self._closed = False
        self._codec = codec
        self._handler = handler
        self._goal_decoder = goal_decoder
        # Keep every ctypes callback alive for the server's lifetime.
        self._callbacks: List = []
        self._schema_buf = ctypes.create_string_buffer(bytes(schema_bfbs), len(schema_bfbs))
        opts_struct, self._opts_keep = _build_server_options(options)
        opts_arg = ctypes.byref(opts_struct) if opts_struct is not None else None
        codec_struct = self._build_codec_struct()
        self._codec_keep = codec_struct
        # The execute callback uses a PROCESS-GLOBAL trampoline + token (the
        # C-ABI passes user_data to execute_cb). Core dispatches the handler from
        # a spawned task that aorta_action_server_free does NOT join, so a late
        # dispatch after close() must find a stable pointer + a token that is
        # gone -> no-op (parity with the C++ shutting-down guard). The codec
        # callbacks have no user_data so they stay per-server closures; those ARE
        # drained by aorta_action_server_free (queryable/status tasks).
        self._exec_token = _register_execute((self._codec, self._handler, self._goal_decoder))
        handle = bindings.aorta_action_server_t(0)
        rc = self._lib.aorta_action_server_new_with_options(
            node._handle,
            action_name.encode("utf-8"),
            ctypes.cast(self._schema_buf, ctypes.c_void_p),
            len(schema_bfbs),
            opts_arg,
            ctypes.byref(codec_struct),
            _EXECUTE_TRAMPOLINE,
            ctypes.c_void_p(self._exec_token),
            ctypes.byref(handle),
        )
        if rc != 0:
            _unregister_execute(self._exec_token)
            errors.check_rc(self._lib, rc, "aorta_action_server_new_with_options")
        self._handle = handle.value

    def name(self) -> str:
        return self._name

    def _build_codec_struct(self):
        c = self._codec
        struct = structs.AortaActionServerCodec()
        # Thin lambdas over module-level logic (testable directly; the lambdas
        # run on worker threads which coverage cannot trace).
        cb0 = bindings.ACTION_DECODE_GOAL_ID(lambda rq, oi: _cb_decode_id(c, rq, oi))
        cb1 = bindings.ACTION_DECODE_GOAL_SECRET(
            lambda rq, oi, os: _cb_decode_secret(c.decode_cancel_goal_request, rq, oi, os))
        cb2 = bindings.ACTION_DECODE_GOAL_SECRET(
            lambda rq, oi, os: _cb_decode_secret(c.decode_get_result_request, rq, oi, os))
        cb3 = bindings.ACTION_ENCODE_SEND_GOAL_RESPONSE(
            lambda *a: _cb_encode(c.encode_send_goal_response, _bld_send_goal, a))
        cb4 = bindings.ACTION_ENCODE_CANCEL_RESPONSE(
            lambda *a: _cb_encode(c.encode_cancel_response, _bld_cancel, a))
        cb5 = bindings.ACTION_ENCODE_PUBLIC_STATUS(
            lambda *a: _cb_encode(c.encode_public_status, _bld_public_status, a))
        cb6 = bindings.ACTION_ENCODE_FEEDBACK(
            lambda *a: _cb_encode(c.encode_feedback, _bld_feedback, a))
        cb7 = bindings.ACTION_ENCODE_GET_RESULT_RESPONSE(
            lambda *a: _cb_encode(c.encode_get_result_response, _bld_get_result, a))
        self._callbacks.extend([cb0, cb1, cb2, cb3, cb4, cb5, cb6, cb7])
        struct.decode_send_goal_request_goal_id = ctypes.cast(cb0, ctypes.c_void_p)
        struct.decode_cancel_goal_request = ctypes.cast(cb1, ctypes.c_void_p)
        struct.decode_get_result_request = ctypes.cast(cb2, ctypes.c_void_p)
        struct.encode_send_goal_response = ctypes.cast(cb3, ctypes.c_void_p)
        struct.encode_cancel_goal_response = ctypes.cast(cb4, ctypes.c_void_p)
        struct.encode_public_status = ctypes.cast(cb5, ctypes.c_void_p)
        struct.encode_feedback = ctypes.cast(cb6, ctypes.c_void_p)
        struct.encode_get_result_response = ctypes.cast(cb7, ctypes.c_void_p)
        return struct

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # free drains queryable/status tasks (so the codec callbacks are quiesced);
        # then drop the execute token so a late spawned-handler dispatch no-ops.
        self._lib.aorta_action_server_free(self._handle)
        _unregister_execute(self._exec_token)

    def __enter__(self) -> "ActionServer":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


class ActionInfoServer:
    """An **info-only** action server: it answers the action's discovery query
    (so a client's :meth:`ActionClient.info` resolves and ``enforce_single_provider``
    sees a provider) but accepts no goals — no codec, no execute handler.

    Mirror of C++ ``ActionServer::ServeInfoOnly`` / Rust ``serve_info_only``.
    Created via ``Node.create_action_info_server``. Use as a context manager or
    call :meth:`close`.
    """

    def __init__(
        self,
        node,
        action_name: str,
        schema_bfbs,
        *,
        options: Optional[ActionServerOptions] = None,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        action_name = normalize_action_name(self._lib, action_name)
        self._name = action_name
        self._closed = False
        self._schema_buf = ctypes.create_string_buffer(bytes(schema_bfbs), len(schema_bfbs))
        opts_struct, self._opts_keep = _build_server_options(options)
        opts_arg = ctypes.byref(opts_struct) if opts_struct is not None else None
        handle = bindings.aorta_action_server_t(0)
        rc = self._lib.aorta_action_server_info_new_with_options(
            node._handle,
            action_name.encode("utf-8"),
            ctypes.cast(self._schema_buf, ctypes.c_void_p),
            len(schema_bfbs),
            opts_arg,
            ctypes.byref(handle),
        )
        errors.check_rc(self._lib, rc, "aorta_action_server_info_new_with_options")
        self._handle = handle.value

    def name(self) -> str:
        return self._name

    def close(self) -> None:
        """Free the info server. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_action_server_free(self._handle)

    def __enter__(self) -> "ActionInfoServer":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


# encode builders: map the C-ABI by-value args to the adapter's Python signature.
def _bld_send_goal(meth, args):
    accepted, goal_id, goal_secret, code, status, message, server_epoch = args
    return meth(
        bool(accepted), _sv_str(goal_id), _sv_str(goal_secret),
        ActionReplyCode(int(code)), ActionStatus(int(status)),
        _sv_str(message), _sv_str(server_epoch),
    )


def _bld_cancel(meth, args):
    accepted, goal_id, code, status, message = args
    return meth(bool(accepted), _sv_str(goal_id), ActionReplyCode(int(code)),
                ActionStatus(int(status)), _sv_str(message))


def _bld_public_status(meth, args):
    goal_id, status, code, a_ns, s_ns, f_ns, last_seq, epoch = args
    return meth(_sv_str(goal_id), ActionStatus(int(status)), ActionReplyCode(int(code)),
                int(a_ns), int(s_ns), int(f_ns), int(last_seq), _sv_str(epoch))


def _bld_feedback(meth, args):
    goal_id, seq, stamp_ns, status, fb_ptr, message = args
    fb = bindings.bytes_from_view(fb_ptr.contents) if fb_ptr else b""
    return meth(_sv_str(goal_id), int(seq), int(stamp_ns), ActionStatus(int(status)),
                fb, _sv_str(message))


def _bld_get_result(meth, args):
    goal_id, status, a_ns, s_ns, f_ns, last_seq, snap_ptr, result_ptr, code, message, epoch = args
    snap = bindings.bytes_from_view(snap_ptr.contents) if snap_ptr else b""
    result = bindings.bytes_from_view(result_ptr.contents) if result_ptr else b""
    return meth(_sv_str(goal_id), ActionStatus(int(status)), int(a_ns), int(s_ns), int(f_ns),
                int(last_seq), snap, result, ActionReplyCode(int(code)), _sv_str(message),
                _sv_str(epoch))


def _swallow(where: str, exc: BaseException) -> None:
    try:
        _log.error("%s raised: %r", where, exc, scope="aorta_sdk")
    except BaseException:  # pragma: no cover - logging must never re-raise
        pass


# Module-level codec-callback bodies (called by the per-server lambdas; extracted
# so they're directly unit-testable on the main thread). Each returns an
# aorta_result_t and must never let an exception cross the C boundary.
def _cb_decode_id(codec, request_ptr, out_id) -> int:
    keep = _scratch_reset()
    try:
        req = bindings.bytes_from_view(request_ptr.contents) if request_ptr else b""
        _write_string_view(out_id, codec.decode_send_goal_request_goal_id(req), keep)
        return _OK
    except BaseException as exc:  # noqa: BLE001
        _swallow("decode_send_goal_request_goal_id", exc)
        return _ERR_INTERNAL


def _cb_decode_secret(meth, request_ptr, out_id, out_secret) -> int:
    keep = _scratch_reset()
    try:
        req = bindings.bytes_from_view(request_ptr.contents) if request_ptr else b""
        gid, secret = meth(req)
        _write_string_view(out_id, gid, keep)
        _write_string_view(out_secret, secret, keep)
        return _OK
    except BaseException as exc:  # noqa: BLE001
        _swallow("decode_goal_secret", exc)
        return _ERR_INTERNAL


def _cb_encode(meth, build, args) -> int:
    out = args[-1]
    try:
        _write_owned_view(out, build(meth, args[:-1]))
        return _OK
    except BaseException as exc:  # noqa: BLE001
        _swallow("encode", exc)
        return _ERR_INTERNAL


def _run_execute(codec, handler, goal_decoder, request_ptr, ctx_handle) -> None:
    lib = loader.library()
    ctx = ActionExecutionContext(lib, int(ctx_handle), "")
    try:
        payload = bindings.bytes_from_view(request_ptr.contents) if request_ptr else b""
        goal_id = codec.decode_send_goal_request_goal_id(payload)
        ctx._goal_id = goal_id
        goal = RetainedActionGoal(goal_id, payload, goal_decoder)
        handler(goal, ctx)
    except BaseException as exc:  # noqa: BLE001 - must not cross the C boundary
        _swallow("action handler", exc)
        try:
            ctx.abort(ActionReplyCode.HANDLER_ERROR, "handler raised an exception")
        except BaseException:  # pragma: no cover - abort best-effort
            pass
    finally:
        ctx._free()


# Process-global execute trampoline + token registry: the execute callback's C
# pointer must never move (Core dispatches it from a spawned task that the server
# free does not join), so a late dispatch after close() finds the token gone and
# no-ops. The codec callbacks cannot use this (no user_data) so they stay
# per-server, drained by aorta_action_server_free.
_execute_handlers: dict = {}
_execute_lock = threading.Lock()
_execute_counter = itertools.count(1)


def _register_execute(entry) -> int:
    token = next(_execute_counter)
    with _execute_lock:
        _execute_handlers[token] = entry
    return token


def _unregister_execute(token: int) -> None:
    with _execute_lock:
        _execute_handlers.pop(token, None)


def _execute_dispatch(request_ptr, ctx_handle, user_data) -> None:
    entry = _execute_handlers.get(int(user_data)) if user_data else None
    if entry is None:
        # Server is gone (closed): free the ctx so the goal does not hang, no-op.
        ActionExecutionContext(loader.library(), int(ctx_handle), "")._free()
        return
    codec, handler, goal_decoder = entry
    _run_execute(codec, handler, goal_decoder, request_ptr, ctx_handle)


_EXECUTE_TRAMPOLINE = bindings.ACTION_EXECUTE_CB(_execute_dispatch)


def _build_server_options(opts: Optional[ActionServerOptions]):
    if opts is None:
        return None, ()
    s = structs.AortaActionServerOptions()
    s.supports_cancel = 1 if opts.supports_cancel else 0
    s.max_concurrency = int(opts.max_concurrency)
    s.result_ttl_ms = int(opts.result_ttl_ms)
    s.dedup_ttl_ms = int(opts.dedup_ttl_ms)
    return s, (s,)


def _build_client_options(opts: Optional[ActionClientOptions]):
    if opts is None:
        return None, ()
    c = structs.AortaActionClientOptions()
    c.control_timeout_ms = int(opts.control_timeout_ms)
    c.info_timeout_ms = int(opts.info_timeout_ms)
    return c, (c,)


# ── ActionClient + GoalHandle ────────────────────────────────────────────────
# Map a rejection ActionReplyCode to a Python exception (C++/Rust raise on these).
# Benign codes (Ok/Succeeded/Canceled/DuplicateAccepted/AlreadyTerminal/None) do
# not raise — the caller inspects status/code.
# Rejection-code → exception is PER-OPERATION: the same ActionReplyCode means
# different things for send_goal vs cancel, so each mirrors its Rust counterpart
# (core/src/action_client.rs `map_send_goal_rejection` / `map_cancel_rejection`).
# Both default the remaining rejection codes to InvalidArgumentError (Rust's
# catch-all). These are only called on a REJECTED reply (accepted=false), so they
# always raise — success codes never reach them.
_SEND_GOAL_REJECT = {
    ActionReplyCode.BUSY: errors.BusyError,
    ActionReplyCode.CONFLICT: errors.ConcurrentConflictError,
    ActionReplyCode.GOAL_EXPIRED: errors.NotFoundError,
    # everything else (PayloadConflict, Unauthorized, NotFound, …) → invalid-argument
}
_CANCEL_REJECT = {
    ActionReplyCode.NOT_FOUND: errors.NotFoundError,
    # everything else (Unauthorized, AlreadyTerminal, Conflict, Busy, …) → invalid-argument
}


def _raise_send_goal_rejection(code: "ActionReplyCode", message: str) -> None:
    raise _SEND_GOAL_REJECT.get(code, errors.InvalidArgumentError)(message or code.name)


def _raise_cancel_rejection(code: "ActionReplyCode", message: str) -> None:
    raise _CANCEL_REJECT.get(code, errors.InvalidArgumentError)(message or code.name)


# get_result's `code` field doubles as the goal OUTCOME (a handler may abort with
# any code, incl. Conflict/Busy), so only the unambiguous request-rejection codes
# raise here — Unauthorized (wrong secret), NotFound / GoalExpired (no such goal).
_GET_RESULT_REJECT = {
    ActionReplyCode.UNAUTHORIZED: errors.InvalidArgumentError,
    ActionReplyCode.NOT_FOUND: errors.NotFoundError,
    ActionReplyCode.GOAL_EXPIRED: errors.NotFoundError,
}


def _raise_for_get_result(code: "ActionReplyCode", message: str) -> None:
    exc = _GET_RESULT_REJECT.get(code)
    if exc is not None:
        raise exc(message or code.name)


def _require_goal_id(actual: str, expected: str, op: str) -> None:
    """Reject a reply whose goal_id differs from the one we asked about — a
    stale/misrouted reply or a non-Core provider — before acting on it (parity
    with the Rust/C++ clients, which reject the mismatch as an internal error)."""
    if actual != expected:
        raise errors.InternalError(
            f"action {op} reply goal_id mismatch: requested {expected!r}, got {actual!r}"
        )


class GoalHandle:
    """A submitted goal. Carries goal_id / goal_secret / server_epoch and offers
    query_status / cancel / get_result."""

    def __init__(
        self,
        client: "ActionClient",
        goal_id: str,
        goal_secret: str,
        server_epoch: str = "",
    ) -> None:
        self._client = client
        self._goal_id = goal_id
        self._goal_secret = goal_secret
        self._server_epoch = server_epoch

    @classmethod
    def _from_response(cls, client: "ActionClient", response: SendGoalResponse) -> "GoalHandle":
        return cls(client, response.goal_id, response.goal_secret, response.server_epoch)

    @classmethod
    def from_persisted(
        cls,
        client: "ActionClient",
        goal_id: str,
        goal_secret: str,
        server_epoch: str = "",
    ) -> "GoalHandle":
        """Reconstruct a handle for a goal submitted earlier (e.g. in another
        process or before a restart) from its persisted ``goal_id`` +
        ``goal_secret``. Mirror of the C++/Rust ``GoalHandle::from_persisted``.

        The control plane (``query_status`` / ``cancel`` / ``get_result`` /
        ``subscribe_feedback`` / ``wait_result``) keys off id + secret, so the
        reconstructed handle is fully functional against the same ``client``.
        ``server_epoch`` is optional (the live server stamps the authoritative
        epoch into every status/result it returns)."""
        return cls(client, goal_id, goal_secret, server_epoch)

    def goal_id(self) -> str:
        return self._goal_id

    def goal_secret(self) -> str:
        return self._goal_secret

    def server_epoch(self) -> str:
        return self._server_epoch

    def query_status(self) -> Optional[PublicStatus]:
        return self._client.query_status(self._goal_id)

    def cancel(self) -> CancelResponse:
        return self._client.cancel(self._goal_id, self._goal_secret)

    def get_result(self) -> GetResultResponse:
        return self._get_result()

    def _get_result(self, timeout_ms: Optional[int] = None) -> GetResultResponse:
        resp = self._client._get_result(
            self._goal_id, self._goal_secret, timeout_ms=timeout_ms
        )
        # Client-side stale-epoch guard, parity with C++ ValidateGetResultResponse
        # / ServerEpochMismatch (cpp/include/aorta/action.h): a result stamped by
        # a *different* server epoch belongs to a previous server instance, so it
        # is NotFound. `!expected.empty() && expected != observed` — skipped when
        # this handle carries no epoch (e.g. from_persisted(server_epoch="")), so
        # the live server's authoritative epoch is then taken as-is.
        if self._server_epoch and resp.server_epoch != self._server_epoch:
            raise errors.NotFoundError("action goal belongs to a previous server epoch")
        return resp

    def wait_result(self, timeout: Optional[float] = None) -> GetResultResponse:
        """Block until the goal is terminal and return its result; raise
        :class:`~aorta.errors.TimeoutError` if ``timeout`` seconds elapse first.

        (The C++ ``WaitResult`` subscribes to the public-status topic + a condvar;
        the Python v1 polls ``get_result`` to terminal — simpler, same outcome,
        and raises on timeout like C++/Rust. Documented gap: polling, not
        subscription.)
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if deadline is None:
                res = self._get_result()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise errors.TimeoutError(
                        f"wait_result timed out (goal {self._goal_id} not terminal)"
                    )
                # Bound each poll's RPC by the remaining budget so a blocked
                # get_result (server down / lost reply) can't overshoot the caller
                # timeout by the client's control timeout (10s default).
                res = self._get_result(timeout_ms=max(1, int(remaining * 1000)))
            if res.status.is_terminal():
                return res
            time.sleep(0.02)

    def subscribe_feedback(self, callback) -> "ActionFeedbackSubscription":
        """Subscribe to this goal's feedback (filtered by goal_id)."""
        return self._client.subscribe_feedback(callback, goal_id=self._goal_id)


# ── feedback subscription (raw on_msg trampoline; goal-filtered client-side) ──
_feedback_handlers: dict = {}
_feedback_lock = threading.Lock()
_feedback_counter = itertools.count(1)


def _register_feedback(entry) -> int:
    token = next(_feedback_counter)
    with _feedback_lock:
        _feedback_handlers[token] = entry
    return token


def _unregister_feedback(token: int) -> None:
    with _feedback_lock:
        _feedback_handlers.pop(token, None)


def _feedback_dispatch(view_ptr, _hash_ptr, _ctx_ptr, user_data) -> None:
    # Worker thread, GIL held. Must not let an exception cross the C boundary.
    entry = _feedback_handlers.get(int(user_data)) if user_data else None
    if entry is None:
        return
    codec, callback, goal_filter = entry
    try:
        payload = bindings.bytes_from_view(view_ptr.contents) if view_ptr else b""
        sample = codec.parse_feedback(payload)
        if goal_filter is None or sample.goal_id == goal_filter:
            callback(sample)
    except BaseException as exc:  # noqa: BLE001 - swallow: must not cross C boundary
        _swallow("action feedback callback", exc)


_FEEDBACK_TRAMPOLINE = bindings.ON_MSG(_feedback_dispatch)


class ActionFeedbackSubscription:
    """A live feedback subscription. ``close`` (or context manager) to stop."""

    def __init__(self, lib, handle: int, token: int) -> None:
        self._lib = lib
        self._handle = handle
        self._token = token
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._lib.aorta_action_feedback_sub_free(self._handle)
        _unregister_feedback(self._token)

    def __enter__(self) -> "ActionFeedbackSubscription":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


class ActionClient:
    """An action client. Created via ``Node.create_action_client``."""

    def __init__(
        self,
        node,
        action_name: str,
        codec: ActionCodec,
        *,
        options: Optional[ActionClientOptions] = None,
    ) -> None:
        self._lib = bindings.configure(loader.library())
        action_name = normalize_action_name(self._lib, action_name)
        self._name = action_name
        self._codec = codec
        self._closed = False
        # Feedback subscriptions this client created. Core tracks them in a
        # SEPARATE registry (only aorta_action_feedback_sub_free undeclares them),
        # so close() must close them — freeing just the client would leak the
        # Zenoh subscriber + the callback token.
        self._feedback_subs: List["ActionFeedbackSubscription"] = []
        opts_struct, self._opts_keep = _build_client_options(options)
        opts_arg = ctypes.byref(opts_struct) if opts_struct is not None else None
        handle = bindings.aorta_action_client_t(0)
        rc = self._lib.aorta_action_client_new_with_options(
            node._handle, action_name.encode("utf-8"), opts_arg, ctypes.byref(handle)
        )
        errors.check_rc(self._lib, rc, "aorta_action_client_new_with_options")
        self._handle = handle.value

    def action_name(self) -> str:
        return self._name

    def send_goal(self, goal: Any) -> GoalHandle:
        """Submit a goal (auto goal_id). Returns a :class:`GoalHandle`."""
        return self.send_goal_with_id(uuid.uuid4().hex, goal)

    def send_goal_with_id(self, goal_id: str, goal: Any) -> GoalHandle:
        request = self._codec.build_send_goal_request(goal_id, goal)
        data = self._call_owned("aorta_action_client_send_goal", request)
        resp = self._codec.parse_send_goal_response(data)
        if not resp.accepted and resp.code != ActionReplyCode.DUPLICATE_ACCEPTED:
            _raise_send_goal_rejection(resp.code, resp.message)  # always raises
        _require_goal_id(resp.goal_id, goal_id, "send_goal")
        return GoalHandle._from_response(self, resp)

    def query_status(self, goal_id: str) -> Optional[PublicStatus]:
        # Check closed first: a freed handle returns NOT_FOUND, which we map to
        # None below — without this, a use-after-close would look like "no status"
        # instead of raising like every other ActionClient op.
        if self._closed:
            raise errors.AortaError("query_status on a closed ActionClient")
        resp = structs.AortaOwnedBytes()
        rc = self._lib.aorta_action_client_query_status(
            self._handle, goal_id.encode("utf-8"), ctypes.byref(resp)
        )
        if rc == errors.ResultCode.NOT_FOUND:
            self._lib.aorta_owned_bytes_free(ctypes.byref(resp))
            return None
        errors.check_rc(self._lib, rc, "aorta_action_client_query_status")
        return self._codec.parse_public_status(_read_owned_response(self._lib, resp))

    def cancel(self, goal_id: str, goal_secret: str) -> CancelResponse:
        request = self._codec.build_cancel_request(goal_id, goal_secret)
        data = self._call_owned("aorta_action_client_cancel", request)
        resp = self._codec.parse_cancel_response(data)
        _require_goal_id(resp.goal_id, goal_id, "cancel")
        if not resp.accepted:  # rejection -> map per cancel semantics; else return
            _raise_cancel_rejection(resp.code, resp.message)
        return resp

    def get_result(self, goal_id: str, goal_secret: str) -> GetResultResponse:
        return self._get_result(goal_id, goal_secret)

    def _get_result(
        self, goal_id: str, goal_secret: str, timeout_ms: Optional[int] = None
    ) -> GetResultResponse:
        # timeout_ms (when set) bounds the RPC via aorta_action_client_get_result_
        # with_timeout, so a polling wait_result can cap each query by its remaining
        # budget instead of the client's full control timeout.
        request = self._codec.build_get_result_request(goal_id, goal_secret)
        if timeout_ms is None:
            data = self._call_owned("aorta_action_client_get_result", request)
        else:
            data = self._call_owned(
                "aorta_action_client_get_result_with_timeout",
                request,
                ctypes.c_uint64(timeout_ms),
            )
        resp = self._codec.parse_get_result_response(data)
        _require_goal_id(resp.goal_id, goal_id, "get_result")
        _raise_for_get_result(resp.code, resp.message)  # only request rejections
        return resp

    def info(self) -> Optional[ActionInfo]:
        # See query_status: a freed handle returns NOT_FOUND (→ None); check
        # closed first so use-after-close raises consistently.
        if self._closed:
            raise errors.AortaError("info on a closed ActionClient")
        resp = structs.AortaOwnedBytes()
        rc = self._lib.aorta_action_client_info(self._handle, ctypes.byref(resp))
        if rc == errors.ResultCode.NOT_FOUND:
            self._lib.aorta_owned_bytes_free(ctypes.byref(resp))
            return None
        errors.check_rc(self._lib, rc, "aorta_action_client_info")
        return self._codec.parse_action_info(_read_owned_response(self._lib, resp))

    def subscribe_feedback(self, callback, *, goal_id: Optional[str] = None) -> ActionFeedbackSubscription:
        """Subscribe to feedback samples; ``callback(FeedbackSample)`` runs on a
        worker thread. ``goal_id`` filters to one goal (None = all goals)."""
        if self._closed:
            raise errors.AortaError("subscribe on a closed ActionClient")
        token = _register_feedback((self._codec, callback, goal_id))
        handle = bindings.aorta_action_feedback_sub_t(0)
        rc = self._lib.aorta_action_client_subscribe_feedback(
            self._handle, _FEEDBACK_TRAMPOLINE, ctypes.c_void_p(token), ctypes.byref(handle)
        )
        if rc != 0:  # pragma: no cover - subscribe on a valid client does not fail
            _unregister_feedback(token)
            errors.check_rc(self._lib, rc, "aorta_action_client_subscribe_feedback")
        sub = ActionFeedbackSubscription(self._lib, handle.value, token)
        self._feedback_subs.append(sub)
        return sub

    def _call_owned(self, fn_name: str, request: bytes, *extra_args) -> bytes:
        if self._closed:
            raise errors.AortaError("call on a closed ActionClient")
        # Request views are BORROWED on the Rust side; Core
        # copies synchronously, _keep pins the buffer across the call.
        view, _keep = bindings.borrowed_view_from_bytes(request)
        resp = structs.AortaOwnedBytes()
        fn = getattr(self._lib, fn_name)
        # extra_args (e.g. a c_uint64 timeout_ms) slot between the request view
        # and the owned-bytes out-param, matching the *_with_timeout signatures.
        rc = fn(self._handle, ctypes.byref(view), *extra_args, ctypes.byref(resp))
        try:
            errors.check_rc(self._lib, rc, fn_name)
            return bindings.bytes_from_view(resp.view)
        finally:
            self._lib.aorta_owned_bytes_free(ctypes.byref(resp))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Close owned feedback subscriptions BEFORE freeing the client so the
        # Zenoh subscribers are undeclared (close() is idempotent — a sub the
        # caller already closed is a no-op).
        for sub in self._feedback_subs:
            sub.close()
        self._feedback_subs.clear()
        self._lib.aorta_action_client_free(self._handle)

    def __enter__(self) -> "ActionClient":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
