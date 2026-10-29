"""RPC option + stats dataclasses for the Aorta Python SDK.

Mirrors the Rust SDK ``ServiceOptions``/``ClientOptions`` (rust/src/{service,
client}.rs) and the C++ SDK options (cpp/include/aorta/options.h), restricted to
the fields the L2 C-ABI (``aorta_{service,client}_options_t``) actually consumes.

Notes / documented gaps (see Python SDK design proposal §9.5/§9.6):
- ``ServiceOptions.deadline_ns`` is **RESERVED/no-op** — the field exists in the
  C-ABI struct + C++ options, but Core's FFI builder does not consume it. Kept
  for signature parity; do not rely on deadline semantics.
- ``enforce_single_provider`` and ``allow_retry`` are **tri-state** (``None`` =
  inherit the Core/env default — which for ``allow_retry`` is OFF — ``False`` =
  force off, ``True`` = force on), encoded as the C-ABI u8 0/1/2.
- ``ClientOptions`` does **not** expose the Rust-only ``validate_schema`` /
  ``request_root_type`` / ``response_root_type`` — the C-ABI client options
  struct has no such fields (gap documented in the parity matrix).
"""

from __future__ import annotations

import ctypes
from dataclasses import dataclass, replace
from enum import IntEnum
from typing import Optional, Tuple

from . import errors
from ._ffi import structs
from .schema import SchemaAttachPolicy, SchemaMeta

__all__ = [
    "CallbackGroup",
    "RpcOverflowPolicy",
    "ExecutionPolicy",
    "ServiceOptions",
    "ClientOptions",
    "ServiceStats",
    "ClientStats",
    "SubscriberRuntimeStats",
    "SubscriberOverflowPolicy",
    "DeliveryMode",
    "ReceiveOverflow",
    "SubscriberOptions",
    "TimerOverflowPolicy",
    "TimerOptions",
    "TimerStats",
]


class CallbackGroup(IntEnum):
    """Mirror of ``aorta_callback_group_t``."""

    MUTUALLY_EXCLUSIVE = 0
    REENTRANT = 1


class RpcOverflowPolicy(IntEnum):
    """Mirror of ``aorta_rpc_overflow_policy_t``."""

    FAST_FAIL_BUSY = 0
    BLOCK = 1


class SubscriberOverflowPolicy(IntEnum):
    """Mirror of ``aorta_sub_overflow_policy_t``."""

    DROP = 0
    RELAY = 1


class DeliveryMode(IntEnum):
    """How received samples are delivered.

    Mirrors core ``DeliveryMode`` / ``aorta_sub_buffering_t.delivery_mode``
    (the buffering companion of ``aorta_subscriber_new_buffered`` —
    ``aorta_sub_options_t`` is layout-frozen and carries no buffering knobs).
    ``INLINE_PUSH`` is deferred (rejected at declare) and not offered in Python.
    """

    EXECUTOR = 0  # callback on the shared executor (default); Tier A cap
    INLINE_PULL = 1  # native keep-last ring; drain via PullSubscriber.recv


class ReceiveOverflow(IntEnum):
    """Drop direction when ``receive_depth`` is exceeded."""

    DROP_NEWEST = 0  # reject the newest at the gate (Tier A; cheap)
    DROP_OLDEST = 1  # keep newest N (requires INLINE_PULL / native ring)


class ExecutionPolicy(IntEnum):
    """An :class:`~aorta.executiongroup.ExecutionGroup` scheduling policy.

    Maps to a callback-group type: SERIALIZED → MutuallyExclusive (callbacks in
    the group never run concurrently), CONCURRENT → Reentrant.
    """

    SERIALIZED = 0  # -> CallbackGroup.MUTUALLY_EXCLUSIVE
    CONCURRENT = 1  # -> CallbackGroup.REENTRANT

    def to_callback_group(self) -> "CallbackGroup":
        return (
            CallbackGroup.MUTUALLY_EXCLUSIVE
            if self == ExecutionPolicy.SERIALIZED
            else CallbackGroup.REENTRANT
        )


@dataclass
class ServiceOptions:
    """Options for ``Node.create_service*`` (parity with C++/Rust ServiceOptions)."""

    schema_attach_policy: SchemaAttachPolicy = SchemaAttachPolicy.NEVER
    callback_group: CallbackGroup = CallbackGroup.MUTUALLY_EXCLUSIVE
    # Bind to an ExecutionGroup's callback group (0 = none); stamped by
    # ExecutionGroup.service_options().
    callback_group_handle: int = 0
    rpc_overflow_policy: RpcOverflowPolicy = RpcOverflowPolicy.FAST_FAIL_BUSY
    rpc_overflow_block_timeout_ms: int = 0
    deadline_ns: int = 0  # RESERVED / no-op (Core's FFI builder ignores it)
    # Schema injection — typed services may fill these; raw services leave None.
    service_schema_bfbs: Optional[bytes] = None
    service_schema_hash_hex: Optional[str] = None
    request_root_type: Optional[str] = None
    response_root_type: Optional[str] = None


@dataclass
class ClientOptions:
    """Options for ``Node.create_client*`` (parity with C++/Rust ClientOptions)."""

    timeout_ms: int = 5000
    schema_attach_policy: SchemaAttachPolicy = SchemaAttachPolicy.INHERIT
    enforce_single_provider: Optional[bool] = None  # tri-state
    allow_retry: Optional[bool] = None  # tri-state; None=inherit (OFF)
    multi_reply_check_window_ms: Optional[int] = None
    service_schema_hash_hex: Optional[str] = None


@dataclass(frozen=True)
class ServiceStats:
    """Snapshot of ``aorta_service_stats_t`` (12 real FFI counters)."""

    request_received_count: int
    request_dispatched_count: int
    reply_ok_count: int
    reply_error_count: int
    reply_send_failure_count: int
    request_dropped_count: int
    query_receive_failure_count: int
    task_exit_count: int
    in_flight_requests: int
    last_request_ns: int
    last_reply_ns: int
    #: Deadline-expired queue drops — a SUBSET of ``request_dropped_count``.
    request_dropped_deadline_count: int

    @classmethod
    def _from_ffi(cls, s: "structs.AortaServiceStats") -> "ServiceStats":
        return cls(**{name: getattr(s, name) for name, *_ in s._fields_})


@dataclass(frozen=True)
class ClientStats:
    """Snapshot of ``aorta_client_stats_t`` (9 real FFI counters)."""

    call_count: int
    success_count: int
    timeout_count: int
    concurrent_conflict_count: int
    error_count: int
    retry_count: int
    last_call_started_ns: int
    last_call_finished_ns: int
    last_call_latency_ns: int

    @classmethod
    def _from_ffi(cls, s: "structs.AortaClientStats") -> "ClientStats":
        return cls(**{name: getattr(s, name) for name, *_ in s._fields_})


@dataclass(frozen=True)
class PublisherStats:
    """Snapshot of ``aorta_publisher_stats_t`` (7 real FFI counters)."""

    publish_count: int
    oversize_count: int
    last_oversize_size: int
    current_capacity: int
    max_capacity_seen: int
    last_publish_ns: int
    send_error_count: int

    @classmethod
    def _from_ffi(cls, s: "structs.AortaPublisherStats") -> "PublisherStats":
        return cls(**{name: getattr(s, name) for name, *_ in s._fields_})


@dataclass(frozen=True)
class SubscriberRuntimeStats:
    """Snapshot of ``aorta_subscriber_runtime_stats_t`` (7 drop/reorder counters)."""

    duplicate_drop_count: int
    out_of_order_count: int
    out_of_order_drop_count: int
    executor_drop_count: int
    local_drop_count: int
    overflow_relay_enqueue_count: int
    overflow_relay_drop_count: int

    @classmethod
    def _from_ffi(cls, s: "structs.AortaSubscriberRuntimeStats") -> "SubscriberRuntimeStats":
        return cls(**{name: getattr(s, name) for name, *_ in s._fields_})


# ── FFI marshaling (option dataclass -> ctypes struct + keepalive) ───────────

# Tri-state u8 encoding (core mod.rs optional_bool_from_ffi).
_OPTIONAL_BOOL_INHERIT = 0
_OPTIONAL_BOOL_FALSE = 1
_OPTIONAL_BOOL_TRUE = 2


def _tristate(value: Optional[bool]) -> int:
    if value is None:
        return _OPTIONAL_BOOL_INHERIT
    return _OPTIONAL_BOOL_TRUE if value else _OPTIONAL_BOOL_FALSE


def _ptr_len(keep: list, data: Optional[bytes]):
    """Copy ``data`` into a kept-alive buffer; return ``(c_void_p, len)``.

    ``None``/empty yields ``(None, 0)`` — a NULL pointer (the Core FFI treats a
    null/zero-length field as "unset").
    """
    if not data:
        return None, 0
    buf = ctypes.create_string_buffer(bytes(data), len(data))
    keep.append(buf)
    return ctypes.cast(buf, ctypes.c_void_p), len(data)


def build_service_options(opts: Optional[ServiceOptions]) -> Tuple[object, tuple]:
    """Return ``(AortaServiceOptions, keepalive)``; ``(None, ())`` for defaults.

    The caller passes ``byref(struct)`` (or NULL) into
    ``aorta_service_new_with_options`` and MUST keep ``keepalive`` referenced
    across that call (it pins the hash/bfbs/root-type buffers).
    """
    if opts is None:
        return None, ()
    keep: list = []
    s = structs.AortaServiceOptions()
    s.schema_attach_policy = int(opts.schema_attach_policy)
    s.callback_group = int(opts.callback_group)
    s.callback_group_handle = int(opts.callback_group_handle)
    s.deadline_ns = int(opts.deadline_ns)
    hex_bytes = opts.service_schema_hash_hex.encode("ascii") if opts.service_schema_hash_hex else None
    s.service_schema_hash_hex, s.service_schema_hash_len = _ptr_len(keep, hex_bytes)
    s.service_schema_bfbs, s.service_schema_bfbs_len = _ptr_len(keep, opts.service_schema_bfbs)
    req = opts.request_root_type.encode("utf-8") if opts.request_root_type else None
    s.request_root_type, s.request_root_type_len = _ptr_len(keep, req)
    resp = opts.response_root_type.encode("utf-8") if opts.response_root_type else None
    s.response_root_type, s.response_root_type_len = _ptr_len(keep, resp)
    s.rpc_overflow_policy = int(opts.rpc_overflow_policy)
    s.rpc_overflow_block_timeout_ms = int(opts.rpc_overflow_block_timeout_ms)
    keep.append(s)
    return s, tuple(keep)


def build_client_options(opts: Optional[ClientOptions]) -> Tuple[object, tuple]:
    """Return ``(AortaClientOptions, keepalive)``; ``(None, ())`` for defaults."""
    if opts is None:
        return None, ()
    keep: list = []
    c = structs.AortaClientOptions()
    c.schema_attach_policy = int(opts.schema_attach_policy)
    # Validate BEFORE marshaling: timeout_ms is a c_uint64, so a negative value
    # would wrap to ~2^64 ms (effectively no timeout) instead of being rejected.
    timeout_ms = int(opts.timeout_ms)
    if not 0 <= timeout_ms <= 0xFFFFFFFFFFFFFFFF:
        raise errors.InvalidArgumentError(
            f"ClientOptions.timeout_ms out of range [0, 2^64): {opts.timeout_ms!r}"
        )
    c.timeout_ms = timeout_ms
    c.enforce_single_provider = _tristate(opts.enforce_single_provider)
    c.allow_retry = _tristate(opts.allow_retry)
    c.multi_reply_check_window_ms = int(opts.multi_reply_check_window_ms or 0)
    hex_bytes = opts.service_schema_hash_hex.encode("ascii") if opts.service_schema_hash_hex else None
    c.service_schema_hash_hex, c.service_schema_hash_len = _ptr_len(keep, hex_bytes)
    keep.append(c)
    return c, tuple(keep)


# ── Typed-RPC schema auto-injection (parity with Rust typed_service) ──────────


def _merge_schema_field(base, updates: dict, field: str, value) -> None:
    """Stage ``field=value`` unless ``base`` already set a CONFLICTING explicit
    value (parity with Rust typed_service, which errors on mismatch)."""
    existing = getattr(base, field)
    if existing is not None and existing != value:
        raise errors.InvalidArgumentError(
            f"typed schema-meta conflicts with explicit options on {field!r}: "
            f"options set {existing!r} but the schema-meta provides {value!r}"
        )
    updates[field] = value


def inject_service_schema(
    options: Optional[ServiceOptions],
    request_meta: Optional[SchemaMeta],
    response_meta: Optional[SchemaMeta] = None,
) -> Optional[ServiceOptions]:
    """Auto-fill a typed service's schema fields from schema-meta so peers can
    discover/validate the schema (the schema_by_key queryable). The request
    schema's BFBS + hash become the service schema; request/response root-type
    names come from each meta (response defaults to the request meta). Explicit
    option values win unless they conflict (then raise). No-op if no meta."""
    if request_meta is None and response_meta is None:
        return options
    # One service schema identity: request + response must share the same BFBS/
    # hash (parity with C++ InjectTypedServiceSchemaOptions). Reject mismatched
    # artifacts rather than publishing the request schema with a response root
    # type that isn't in it.
    if (
        request_meta is not None
        and response_meta is not None
        and request_meta.SCHEMA_HASH_HEX != response_meta.SCHEMA_HASH_HEX
    ):
        raise errors.InvalidArgumentError(
            "typed service request/response must use the same service schema hash "
            f"(request={request_meta.SCHEMA_HASH_HEX}, "
            f"response={response_meta.SCHEMA_HASH_HEX})"
        )
    base = options if options is not None else ServiceOptions()
    updates: dict = {}
    if request_meta is not None:
        _merge_schema_field(base, updates, "service_schema_bfbs", request_meta.SCHEMA_BFBS)
        _merge_schema_field(base, updates, "service_schema_hash_hex", request_meta.SCHEMA_HASH_HEX)
        _merge_schema_field(base, updates, "request_root_type", request_meta.ROOT_TYPE_NAME)
    # At least one meta is set (early return above), so `resp` is never None.
    resp = response_meta if response_meta is not None else request_meta
    _merge_schema_field(base, updates, "response_root_type", resp.ROOT_TYPE_NAME)
    return replace(base, **updates)


def inject_client_schema(
    options: Optional[ClientOptions],
    request_meta: Optional[SchemaMeta],
) -> Optional[ClientOptions]:
    """Auto-fill a typed client's ``service_schema_hash_hex`` from the request
    schema-meta (so the client can match the provider's schema). The C-ABI
    ``aorta_client_options_t`` carries no root-type fields (parity with C++),
    so only the hash is injected. Explicit value wins unless it conflicts."""
    if request_meta is None:
        return options
    base = options if options is not None else ClientOptions()
    updates: dict = {}
    _merge_schema_field(base, updates, "service_schema_hash_hex", request_meta.SCHEMA_HASH_HEX)
    return replace(base, **updates)


# ── Timer ────────────────────────────────────────────────────────────────────


class TimerOverflowPolicy(IntEnum):
    """Mirror of ``aorta_timer_overflow_policy_t``."""

    COALESCE_LATEST = 0
    BLOCK = 1


@dataclass
class TimerOptions:
    """Options for ``Node.create_timer`` (parity with C++/Rust TimerOptions)."""

    callback_group: CallbackGroup = CallbackGroup.MUTUALLY_EXCLUSIVE
    auto_start: bool = True
    invoke_on_missed: bool = False
    overflow_policy: TimerOverflowPolicy = TimerOverflowPolicy.COALESCE_LATEST
    overflow_block_timeout_ms: int = 0
    # Bind to an ExecutionGroup's callback group; 0 = none.
    callback_group_handle: int = 0


@dataclass(frozen=True)
class TimerStats:
    """The 13 timer diagnostic counters (C++ ``Timer`` getters / Rust ``Timer``)."""

    coalesced_tick_count: int
    overflow_timeout_count: int
    deadline_slip_total_ns: int
    deadline_slip_max_ns: int
    due_firings_total: int
    due_firings_max: int
    due_sample_count: int
    coalesce_requeue_count: int
    coalesce_inline_dispatch_total: int
    coalesce_inline_dispatch_max: int
    coalesce_event_wait_total_ns: int
    coalesce_event_wait_max_ns: int
    coalesce_event_wait_sample_count: int


@dataclass
class SubscriberOptions:
    """Options for ``Node.create_subscriber`` (parity with C++ SubscriberOptions).

    Defaults match Core's ``SubscriberOptions::default()`` so ``SubscriberOptions()``
    is equivalent to passing no options. ``deadline_ns`` is RESERVED/no-op. The
    ``include_*``/``enable_header_context`` flags opt MessageContext fields in.
    """

    callback_group: CallbackGroup = CallbackGroup.MUTUALLY_EXCLUSIVE
    callback_group_handle: int = 0
    deadline_ns: int = 0  # RESERVED/no-op
    prefetch_schema_on_init: bool = True
    enable_header_context: bool = False
    include_topic_in_context: bool = False
    include_receive_timestamp: bool = False
    include_priority_in_context: bool = False
    include_publisher_timestamp: bool = False
    enable_dedup: bool = False
    dedup_window: int = 1024
    enable_reorder_check: bool = False
    drop_out_of_order: bool = False
    overflow_policy: SubscriberOverflowPolicy = SubscriberOverflowPolicy.DROP
    overflow_relay_capacity: int = 1024
    schema_bfbs: Optional[bytes] = None
    # Per-topic in-flight cap (Tier A) / native
    # ring capacity (Tier B-pull). 0 (default) disables the gate — today's behavior.
    receive_depth: int = 0
    delivery: DeliveryMode = DeliveryMode.EXECUTOR
    receive_overflow: ReceiveOverflow = ReceiveOverflow.DROP_NEWEST


def build_subscriber_options(opts: Optional[SubscriberOptions]) -> Tuple[object, tuple]:
    """Return ``(AortaSubOptions, keepalive)``; ``(None, ())`` for defaults."""
    if opts is None:
        return None, ()
    keep: list = []
    s = structs.AortaSubOptions()
    s.callback_group = int(opts.callback_group)
    s.callback_group_handle = int(opts.callback_group_handle)
    s.deadline_ns = int(opts.deadline_ns)
    s.prefetch_schema_on_init = 1 if opts.prefetch_schema_on_init else 0
    s.enable_header_context = 1 if opts.enable_header_context else 0
    s.include_topic_in_context = 1 if opts.include_topic_in_context else 0
    s.include_receive_timestamp = 1 if opts.include_receive_timestamp else 0
    s.include_priority_in_context = 1 if opts.include_priority_in_context else 0
    s.include_publisher_timestamp = 1 if opts.include_publisher_timestamp else 0
    s.enable_dedup = 1 if opts.enable_dedup else 0
    s.dedup_window = int(opts.dedup_window)
    s.enable_reorder_check = 1 if opts.enable_reorder_check else 0
    s.drop_out_of_order = 1 if opts.drop_out_of_order else 0
    s.overflow_policy = int(opts.overflow_policy)
    s.overflow_relay_capacity = int(opts.overflow_relay_capacity)
    s.schema_bfbs, s.schema_bfbs_len = _ptr_len(keep, opts.schema_bfbs)
    keep.append(s)
    return s, tuple(keep)


def build_sub_buffering(opts: Optional[SubscriberOptions]) -> Optional[object]:
    """Return the ``AortaSubBuffering`` companion for
    ``aorta_subscriber_new_buffered``, or ``None`` for defaults.

    ``aorta_sub_options_t`` is layout-frozen (the Python SDK mirrors it), so the
    QoS & buffering redesign knobs ride this dedicated struct. All-zero (or a
    NULL pointer) reproduces today's behavior.
    """
    if opts is None:
        return None
    b = structs.AortaSubBuffering()
    b.receive_depth = int(opts.receive_depth)
    b.delivery_mode = int(opts.delivery)
    b.receive_overflow = int(opts.receive_overflow)
    return b


def build_timer_options(opts: Optional[TimerOptions]) -> Tuple[object, tuple]:
    """Return ``(AortaTimerOptions, keepalive)``; ``(None, ())`` for defaults."""
    if opts is None:
        return None, ()
    t = structs.AortaTimerOptions()
    t.callback_group = int(opts.callback_group)
    t.callback_group_handle = int(opts.callback_group_handle)
    t.auto_start = 1 if opts.auto_start else 0
    t.invoke_on_missed = 1 if opts.invoke_on_missed else 0
    t.overflow_policy = int(opts.overflow_policy)
    t.overflow_block_timeout_ms = int(opts.overflow_block_timeout_ms)
    return t, (t,)
