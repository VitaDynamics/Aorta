"""ctypes mirrors of the Aorta ``#[repr(C)]`` C-ABI structs (layout-locked).

These mirror the data-plane structs declared in ``core/src/ffi/c_api`` and
size-locked by ``core/src/ffi/abi_assert.rs`` (the Rust compile-time ABI lock)
and ``cpp/test/abi_layout_test.cc`` (the C++ side). The Python side adds a
**runtime** ``sizeof`` guard (called by the loader) plus a **static** drift test
(``tests/test_structs.py``) that re-reads ``abi_assert.rs`` so a layout change in
core is caught the moment it would diverge from this binding (Python SDK design
proposal §6.1/§6.4).

The foundation set (string/bytes views, message context, publish options) comes
first; option/stat structs follow, and the registry + guard mechanism covers all
of them.

Pointers and function pointers are mirrored as pointer-width ``c_void_p`` — the
binding only needs correct *layout* here; typed access happens at the call site.
"""

from __future__ import annotations

import ctypes
from typing import Dict, List, Tuple, Type

__all__ = [
    "AortaStringView",
    "AortaBytesSlice",
    "AortaSchemaHashView",
    "AortaBytesView",
    "AortaOwnedBytesSlice",
    "AortaOwnedBytesView",
    "AortaOwnedBytes",
    "AortaMsgContext",
    "AortaPubOptions",
    "AortaStringKv",
    "AortaNodeTelemetryOptions",
    "AortaNodeOptions",
    "AortaServiceOptions",
    "AortaClientOptions",
    "AortaServiceStats",
    "AortaClientStats",
    "AortaTimerOptions",
    "AortaSubscriberRuntimeStats",
    "AortaCtxRead",
    "AortaCtxChange",
    "AortaCtxOwnerLivelinessEvent",
    "AortaSubOptions",
    "AortaSubBuffering",
    "AortaPullSample",
    "AortaActionClientOptions",
    "AortaActionServerOptions",
    "AortaActionServerCodec",
    "EXPECTED_SIZES",
    "EXPECTED_OFFSETS",
    "MIRRORS",
    "field_offsets",
    "check_layouts",
]


class AortaStringView(ctypes.Structure):
    """``aorta_string_view_t`` — (``*const u8`` ptr, ``usize`` len). Not NUL-terminated."""

    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]


class AortaBytesSlice(ctypes.Structure):
    """``aorta_bytes_slice_t`` — (``*const u8``, ``usize``)."""

    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]


class AortaSchemaHashView(ctypes.Structure):
    """``aorta_schema_hash_view_t`` — borrowed 64-hex schema hash (``*const u8``, ``usize``)."""

    _fields_ = [("hex", ctypes.c_void_p), ("len", ctypes.c_size_t)]


class AortaBytesView(ctypes.Structure):
    """``aorta_bytes_view_t`` — borrowed, possibly multi-slice (Zenoh scatter)."""

    _fields_ = [
        ("slices", ctypes.c_void_p),  # *const aorta_bytes_slice_t
        ("slice_count", ctypes.c_size_t),
        ("total_len", ctypes.c_size_t),
    ]


class AortaOwnedBytesSlice(ctypes.Structure):
    """``aorta_owned_bytes_slice_t`` — (ptr, len, release fn-ptr, release ctx)."""

    _fields_ = [
        ("ptr", ctypes.c_void_p),
        ("len", ctypes.c_size_t),
        ("release", ctypes.c_void_p),  # fn-ptr
        ("release_ctx", ctypes.c_void_p),
    ]


class AortaOwnedBytesView(ctypes.Structure):
    """``aorta_owned_bytes_view_t`` — (``*const owned_slice``, count, total_len)."""

    _fields_ = [
        ("slices", ctypes.c_void_p),
        ("slice_count", ctypes.c_size_t),
        ("total_len", ctypes.c_size_t),
    ]


class AortaOwnedBytes(ctypes.Structure):
    """``aorta_owned_bytes_t`` — owned response buffer (view + owner handle)."""

    _fields_ = [("view", AortaBytesView), ("owner", ctypes.c_void_p)]


class AortaMsgContext(ctypes.Structure):
    """``aorta_msg_context_t`` — opt-in per-message metadata delivered to callbacks."""

    _fields_ = [
        ("topic", AortaStringView),
        ("receive_stamp_ns", ctypes.c_int64),
        ("priority", ctypes.c_int32),  # repr(C) enum; 4 bytes + 4 pad
        ("publisher_node", AortaStringView),
        ("sequence", ctypes.c_uint64),
    ]


class AortaPubOptions(ctypes.Structure):
    """``aorta_pub_options_t`` — the C-ABI-mappable publisher options.

    ``congestion_control`` is the per-publisher OVERRIDE — a raw ``u8`` at
    offset 4 using the ``AORTA_PUB_CONGESTION_*`` encoding (0=inherit, 1=Drop,
    2=Block), distinct from the ``aorta_qos_t`` byte. The Python SDK exposes no
    per-publisher override, so it is left zero-initialized (=inherit) and Core
    resolves it against ``qos.congestion_control``; ``QoS.congestion_control``
    reaches the publisher via ``aorta_qos_t``, not this byte.
    """

    _fields_ = [
        ("schema_attach_policy", ctypes.c_int32),  # enum; offset 0
        ("congestion_control", ctypes.c_uint8),  # offset 4 (+3 pad before ptr)
        ("schema_bfbs", ctypes.c_void_p),  # offset 8
        ("schema_bfbs_len", ctypes.c_size_t),  # offset 16
        ("max_message_size", ctypes.c_size_t),  # offset 24
        ("schema_root_type", ctypes.c_char_p),  # offset 32
    ]


class AortaStringKv(ctypes.Structure):
    """``aorta_string_kv_t`` — (``*const c_char`` key, ``*const c_char`` value)."""

    _fields_ = [("key", ctypes.c_char_p), ("value", ctypes.c_char_p)]


class AortaNodeTelemetryOptions(ctypes.Structure):
    """``aorta_node_telemetry_options_t`` — node identity for telemetry.

    The Python SDK sets ``language="python"`` + sdk name/version here so Core
    does not default the node to ``language="c"``.
    """

    _fields_ = [
        ("executable", ctypes.c_char_p),
        ("language", ctypes.c_char_p),
        ("sdk_name", ctypes.c_char_p),
        ("sdk_version", ctypes.c_char_p),
        ("build_id", ctypes.c_char_p),
        ("container_id", ctypes.c_char_p),
        ("labels", ctypes.c_void_p),  # *const aorta_string_kv_t
        ("labels_len", ctypes.c_size_t),
    ]


class AortaNodeOptions(ctypes.Structure):
    """``aorta_node_options_t`` — ``{ *const aorta_node_telemetry_options_t }``."""

    _fields_ = [("telemetry", ctypes.c_void_p)]


class AortaServiceOptions(ctypes.Structure):
    """``aorta_service_options_t`` (104 bytes) — RPC service creation options.

    The three ``*_policy``/``callback_group`` fields are ``#[repr(C)]`` enums
    (4-byte int). ``deadline_ns`` is RESERVED/no-op (Core's FFI builder does not
    consume it). Hash/bfbs/root-type pointers are borrowed for
    the duration of the ``aorta_service_new_with_options`` call.
    """

    _fields_ = [
        ("schema_attach_policy", ctypes.c_int32),
        ("callback_group", ctypes.c_int32),
        ("callback_group_handle", ctypes.c_uint64),
        ("deadline_ns", ctypes.c_int64),
        ("service_schema_hash_hex", ctypes.c_void_p),
        ("service_schema_hash_len", ctypes.c_size_t),
        ("service_schema_bfbs", ctypes.c_void_p),
        ("service_schema_bfbs_len", ctypes.c_size_t),
        ("request_root_type", ctypes.c_void_p),
        ("request_root_type_len", ctypes.c_size_t),
        ("response_root_type", ctypes.c_void_p),
        ("response_root_type_len", ctypes.c_size_t),
        ("rpc_overflow_policy", ctypes.c_int32),
        ("rpc_overflow_block_timeout_ms", ctypes.c_uint64),  # +4 pad before
    ]


class AortaClientOptions(ctypes.Structure):
    """``aorta_client_options_t`` (48 bytes) — RPC client creation options.

    ``allow_retry`` is tri-state as a ``u8`` (0=inherit Core/env default → OFF,
    1=false, 2=true) packed adjacent to ``enforce_single_provider`` so the
    struct stays 48 bytes (see core mod.rs).
    """

    _fields_ = [
        ("schema_attach_policy", ctypes.c_int32),  # +4 pad before timeout_ms
        ("timeout_ms", ctypes.c_uint64),
        ("enforce_single_provider", ctypes.c_uint8),
        ("allow_retry", ctypes.c_uint8),  # +6 pad before the next u64
        ("multi_reply_check_window_ms", ctypes.c_uint64),
        ("service_schema_hash_hex", ctypes.c_void_p),
        ("service_schema_hash_len", ctypes.c_size_t),
    ]


class AortaServiceStats(ctypes.Structure):
    """``aorta_service_stats_t`` (96 bytes) — 12 × ``u64`` real FFI counters."""

    _fields_ = [
        ("request_received_count", ctypes.c_uint64),
        ("request_dispatched_count", ctypes.c_uint64),
        ("reply_ok_count", ctypes.c_uint64),
        ("reply_error_count", ctypes.c_uint64),
        ("reply_send_failure_count", ctypes.c_uint64),
        ("request_dropped_count", ctypes.c_uint64),
        ("query_receive_failure_count", ctypes.c_uint64),
        ("task_exit_count", ctypes.c_uint64),
        ("in_flight_requests", ctypes.c_uint64),
        ("last_request_ns", ctypes.c_uint64),
        ("last_reply_ns", ctypes.c_uint64),
        # Deadline-expired queue drops — a SUBSET of request_dropped_count
        # (appended at the end; Rust/C++ size locks moved 88 -> 96).
        ("request_dropped_deadline_count", ctypes.c_uint64),
    ]


class AortaClientStats(ctypes.Structure):
    """``aorta_client_stats_t`` (72 bytes) — 9 × ``u64`` real FFI counters."""

    _fields_ = [
        ("call_count", ctypes.c_uint64),
        ("success_count", ctypes.c_uint64),
        ("timeout_count", ctypes.c_uint64),
        ("concurrent_conflict_count", ctypes.c_uint64),
        ("error_count", ctypes.c_uint64),
        ("retry_count", ctypes.c_uint64),
        ("last_call_started_ns", ctypes.c_uint64),
        ("last_call_finished_ns", ctypes.c_uint64),
        ("last_call_latency_ns", ctypes.c_uint64),
    ]


class AortaQoS(ctypes.Structure):
    """``aorta_qos_t`` (12 bytes) — QoS profile passed to publisher/subscriber new.

    Mirrors the Rust ``QoS`` 1:1: ``priority``/``reliability`` are ``#[repr(C)]``
    enums (4-byte int); ``congestion_control`` (``AORTA_CONGESTION_CONTROL_*``)
    and ``express`` are ``u8`` (the redesign folded them in and dropped the
    former ``durability``/``history_depth`` no-op bytes). 2 trailing pad bytes
    bring the size to 12 (align 4).
    """

    _fields_ = [
        ("priority", ctypes.c_int32),
        ("reliability", ctypes.c_int32),
        ("congestion_control", ctypes.c_uint8),
        ("express", ctypes.c_uint8),  # +2 pad to 12 (4-align)
    ]


class AortaPublisherStats(ctypes.Structure):
    """``aorta_publisher_stats_t`` — publish/oversize/capacity FFI counters."""

    _fields_ = [
        ("publish_count", ctypes.c_uint64),
        ("oversize_count", ctypes.c_uint64),
        ("last_oversize_size", ctypes.c_size_t),
        ("current_capacity", ctypes.c_size_t),
        ("max_capacity_seen", ctypes.c_size_t),
        ("last_publish_ns", ctypes.c_uint64),
        ("send_error_count", ctypes.c_uint64),
    ]


class AortaTimerOptions(ctypes.Structure):
    """``aorta_timer_options_t`` (32 bytes) — timer creation options.

    ``callback_group``/``overflow_policy`` are ``#[repr(C)]`` enums (4-byte int).
    """

    _fields_ = [
        ("callback_group", ctypes.c_int32),
        ("callback_group_handle", ctypes.c_uint64),  # +4 pad before
        ("auto_start", ctypes.c_uint8),
        ("invoke_on_missed", ctypes.c_uint8),
        ("overflow_policy", ctypes.c_int32),  # +2 pad before
        ("overflow_block_timeout_ms", ctypes.c_uint64),
    ]


class AortaSubscriberRuntimeStats(ctypes.Structure):
    """``aorta_subscriber_runtime_stats_t`` (56 bytes) — 7 × ``u64`` drop/reorder counters."""

    _fields_ = [
        ("duplicate_drop_count", ctypes.c_uint64),
        ("out_of_order_count", ctypes.c_uint64),
        ("out_of_order_drop_count", ctypes.c_uint64),
        ("executor_drop_count", ctypes.c_uint64),
        ("local_drop_count", ctypes.c_uint64),
        ("overflow_relay_enqueue_count", ctypes.c_uint64),
        ("overflow_relay_drop_count", ctypes.c_uint64),
    ]


class AortaCtxRead(ctypes.Structure):
    """``aorta_ctx_read_t`` (88 bytes) — a ContextStore get() result.

    ``string_value``/``source_node``/``source_instance_id`` are **owned**
    ``*mut c_char`` (NUL-terminated) that the caller MUST release via
    ``aorta_ctx_read_free``. ``value_kind`` is one of ``AORTA_CTX_KIND_*``;
    ``bool_value``/``is_fresh`` are 1-byte bools.
    """

    _fields_ = [
        ("value_kind", ctypes.c_uint8),
        ("bool_value", ctypes.c_uint8),  # +6 pad before i64
        ("i64_value", ctypes.c_int64),
        ("u64_value", ctypes.c_uint64),
        ("f64_value", ctypes.c_double),
        ("string_value", ctypes.c_void_p),  # owned *mut c_char
        ("is_fresh", ctypes.c_uint8),  # +7 pad before stamp_ns
        ("stamp_ns", ctypes.c_uint64),
        ("expires_at_ns", ctypes.c_uint64),
        ("source_node", ctypes.c_void_p),  # owned *mut c_char
        ("source_instance_id", ctypes.c_void_p),  # owned *mut c_char
        ("sequence", ctypes.c_uint64),
    ]


class AortaCtxOwnerLivelinessEvent(ctypes.Structure):
    """``aorta_ctx_owner_liveliness_event_t`` (40 bytes) — an owner-liveliness
    watch event. Both ``aorta_string_view_t`` members are **borrowed** and
    valid only during the callback (ABI generation 10).
    """

    _fields_ = [
        ("node_name", AortaStringView),
        ("source_instance_id", AortaStringView),
        ("alive", ctypes.c_bool),
    ]


class AortaCtxChange(ctypes.Structure):
    """``aorta_ctx_change_t`` (128 bytes) — a watch-callback change event.

    The four ``aorta_string_view_t`` members (key/string_value/source_node/
    source_instance_id) are **borrowed** and valid only during the callback.
    """

    _fields_ = [
        ("kind", ctypes.c_uint8),
        ("snapshot_done_reason", ctypes.c_uint8),  # +6 pad before key
        ("key", AortaStringView),
        ("value_kind", ctypes.c_uint8),
        ("bool_value", ctypes.c_uint8),  # +6 pad before i64
        ("i64_value", ctypes.c_int64),
        ("u64_value", ctypes.c_uint64),
        ("f64_value", ctypes.c_double),
        ("string_value", AortaStringView),
        ("stamp_ns", ctypes.c_uint64),
        ("expires_at_ns", ctypes.c_uint64),
        ("source_node", AortaStringView),
        ("source_instance_id", AortaStringView),
        ("sequence", ctypes.c_uint64),
    ]


class AortaSubOptions(ctypes.Structure):
    """``aorta_sub_options_t`` (72 bytes) — subscriber creation options.

    ``deadline_ns`` is RESERVED/no-op (the FFI builder does not consume it). The
    6 ``include_*``/``enable_header_context`` flags opt MessageContext fields in.

    Layout-frozen (this struct is mirrored on the
    runtime-loaded Python surface, so new fields never grow it). The QoS &
    buffering redesign knobs live in :class:`AortaSubBuffering`, passed to
    ``aorta_subscriber_new_buffered``.
    """

    _fields_ = [
        ("callback_group", ctypes.c_int32),
        ("callback_group_handle", ctypes.c_uint64),  # +4 pad
        ("deadline_ns", ctypes.c_int64),
        ("prefetch_schema_on_init", ctypes.c_uint8),
        ("enable_header_context", ctypes.c_uint8),
        ("include_topic_in_context", ctypes.c_uint8),
        ("include_receive_timestamp", ctypes.c_uint8),
        ("include_priority_in_context", ctypes.c_uint8),
        ("include_publisher_timestamp", ctypes.c_uint8),
        ("enable_dedup", ctypes.c_uint8),  # +1 pad before dedup_window
        ("dedup_window", ctypes.c_size_t),
        ("enable_reorder_check", ctypes.c_uint8),
        ("drop_out_of_order", ctypes.c_uint8),  # +2 pad before overflow_policy
        ("overflow_policy", ctypes.c_int32),
        ("overflow_relay_capacity", ctypes.c_size_t),
        ("schema_bfbs", ctypes.c_void_p),
        ("schema_bfbs_len", ctypes.c_size_t),
    ]


class AortaSubBuffering(ctypes.Structure):
    """``aorta_sub_buffering_t`` (16 bytes) — the QoS & buffering redesign
    knobs, carried by ``aorta_subscriber_new_buffered`` (a new size-asserted
    struct behind a new symbol;
    ``aorta_sub_options_t`` is layout-frozen).

    ``receive_depth`` (Tier A in-flight cap / Tier B-pull ring capacity;
    ``0`` = disabled = today's behavior), ``delivery_mode`` (0=executor,
    1=inline_pull, 2=inline_push) and ``receive_overflow`` (0=drop_newest,
    1=drop_oldest). A zero-initialized struct (or a NULL pointer) reproduces
    today's behavior.
    """

    _fields_ = [
        ("receive_depth", ctypes.c_uint64),
        ("delivery_mode", ctypes.c_uint32),
        ("receive_overflow", ctypes.c_uint32),
    ]


class AortaPullSample(ctypes.Structure):
    """``aorta_pull_sample_t`` (88 bytes) — a sample pulled from an InlinePull
    subscriber. ``payload``/``context`` views borrow from ``internal``, which the
    caller must release via ``aorta_pull_sample_free``.
    """

    _fields_ = [
        ("payload", AortaBytesView),
        ("context", AortaMsgContext),
        ("internal", ctypes.c_void_p),
    ]


class AortaActionClientOptions(ctypes.Structure):
    """``aorta_action_client_options_t`` (16 bytes). 0 → Core default."""

    _fields_ = [
        ("control_timeout_ms", ctypes.c_uint64),
        ("info_timeout_ms", ctypes.c_uint64),
    ]


class AortaActionServerOptions(ctypes.Structure):
    """``aorta_action_server_options_t`` (32 bytes)."""

    _fields_ = [
        ("supports_cancel", ctypes.c_uint8),  # +7 pad before max_concurrency
        ("max_concurrency", ctypes.c_size_t),
        ("result_ttl_ms", ctypes.c_uint64),
        ("dedup_ttl_ms", ctypes.c_uint64),
    ]


class AortaActionServerCodec(ctypes.Structure):
    """``aorta_action_server_codec_t`` (64 bytes) — the 8 codec fn-ptrs in order.

    Held as raw pointer-width slots; the typed CFUNCTYPE callbacks are assigned at
    the call site (and kept alive there).
    """

    _fields_ = [
        ("decode_send_goal_request_goal_id", ctypes.c_void_p),
        ("decode_cancel_goal_request", ctypes.c_void_p),
        ("decode_get_result_request", ctypes.c_void_p),
        ("encode_send_goal_response", ctypes.c_void_p),
        ("encode_cancel_goal_response", ctypes.c_void_p),
        ("encode_public_status", ctypes.c_void_p),
        ("encode_feedback", ctypes.c_void_p),
        ("encode_get_result_response", ctypes.c_void_p),
    ]


class AortaExecutorAdmissionStats(ctypes.Structure):
    """``aorta_executor_admission_stats_t`` (64 bytes) — 8 contiguous u64 queue
    diagnostics read out of an explicit :class:`~aorta.executor.Executor`."""

    _fields_ = [
        ("queued_events", ctypes.c_uint64),
        ("queued_bytes", ctypes.c_uint64),
        ("peak_queued_events", ctypes.c_uint64),
        ("peak_queued_bytes", ctypes.c_uint64),
        ("dropped_queue_full", ctypes.c_uint64),
        ("dropped_queue_closed", ctypes.c_uint64),
        ("dropped_budget_events", ctypes.c_uint64),
        ("dropped_budget_bytes", ctypes.c_uint64),
    ]


# Baked layout baseline (Rust type name → byte size), kept in lockstep with
# core/src/ffi/abi_assert.rs. tests/test_structs.py re-reads abi_assert.rs and
# fails if these drift, so the baseline can never silently go stale.
EXPECTED_SIZES = {
    "aorta_string_view_t": 16,
    "aorta_bytes_slice_t": 16,
    "aorta_schema_hash_view_t": 16,
    "aorta_bytes_view_t": 24,
    "aorta_owned_bytes_slice_t": 32,
    "aorta_owned_bytes_view_t": 24,
    "aorta_owned_bytes_t": 32,
    "aorta_msg_context_t": 56,
    "aorta_pub_options_t": 40,
    "aorta_string_kv_t": 16,
    "aorta_node_telemetry_options_t": 64,
    "aorta_node_options_t": 8,
    "aorta_service_options_t": 104,
    "aorta_client_options_t": 48,
    "aorta_service_stats_t": 96,
    "aorta_client_stats_t": 72,
    "aorta_timer_options_t": 32,
    "aorta_subscriber_runtime_stats_t": 56,
    "aorta_ctx_read_t": 88,
    "aorta_ctx_change_t": 128,
    "aorta_ctx_owner_liveliness_event_t": 40,
    "aorta_sub_options_t": 72,
    "aorta_sub_buffering_t": 16,
    "aorta_pull_sample_t": 88,
    "aorta_action_client_options_t": 16,
    "aorta_action_server_options_t": 32,
    "aorta_action_server_codec_t": 64,
    "aorta_executor_admission_stats_t": 64,
}

# Per-field byte offsets, **keyed by this binding's field name**, in lockstep
# with the authoritative offsetof() lock in cpp/test/abi_layout_test.cc. Keying
# by name (not an ordered list) is what catches a same-size field REORDER in the
# ctypes mirror: an ordered list [0,8,16] is invariant
# under swapping two 8-byte fields, a name→offset map is not.
EXPECTED_OFFSETS = {
    "aorta_string_view_t": {"ptr": 0, "len": 8},
    "aorta_bytes_slice_t": {"ptr": 0, "len": 8},
    "aorta_schema_hash_view_t": {"hex": 0, "len": 8},
    "aorta_bytes_view_t": {"slices": 0, "slice_count": 8, "total_len": 16},
    "aorta_owned_bytes_slice_t": {"ptr": 0, "len": 8, "release": 16, "release_ctx": 24},
    "aorta_owned_bytes_view_t": {"slices": 0, "slice_count": 8, "total_len": 16},
    "aorta_owned_bytes_t": {"view": 0, "owner": 24},
    "aorta_msg_context_t": {
        "topic": 0,
        "receive_stamp_ns": 16,
        "priority": 24,
        "publisher_node": 32,
        "sequence": 48,
    },
    "aorta_pub_options_t": {
        "schema_attach_policy": 0,
        "congestion_control": 4,
        "schema_bfbs": 8,
        "schema_bfbs_len": 16,
        "max_message_size": 24,
        "schema_root_type": 32,
    },
    "aorta_string_kv_t": {"key": 0, "value": 8},
    "aorta_node_telemetry_options_t": {
        "executable": 0,
        "language": 8,
        "sdk_name": 16,
        "sdk_version": 24,
        "build_id": 32,
        "container_id": 40,
        "labels": 48,
        "labels_len": 56,
    },
    "aorta_node_options_t": {"telemetry": 0},
    "aorta_service_options_t": {
        "schema_attach_policy": 0,
        "callback_group": 4,
        "callback_group_handle": 8,
        "deadline_ns": 16,
        "service_schema_hash_hex": 24,
        "service_schema_hash_len": 32,
        "service_schema_bfbs": 40,
        "service_schema_bfbs_len": 48,
        "request_root_type": 56,
        "request_root_type_len": 64,
        "response_root_type": 72,
        "response_root_type_len": 80,
        "rpc_overflow_policy": 88,
        "rpc_overflow_block_timeout_ms": 96,
    },
    "aorta_client_options_t": {
        "schema_attach_policy": 0,
        "timeout_ms": 8,
        "enforce_single_provider": 16,
        "allow_retry": 17,
        "multi_reply_check_window_ms": 24,
        "service_schema_hash_hex": 32,
        "service_schema_hash_len": 40,
    },
    "aorta_service_stats_t": {
        "request_received_count": 0,
        "request_dispatched_count": 8,
        "reply_ok_count": 16,
        "reply_error_count": 24,
        "reply_send_failure_count": 32,
        "request_dropped_count": 40,
        "query_receive_failure_count": 48,
        "task_exit_count": 56,
        "in_flight_requests": 64,
        "last_request_ns": 72,
        "last_reply_ns": 80,
        "request_dropped_deadline_count": 88,
    },
    "aorta_client_stats_t": {
        "call_count": 0,
        "success_count": 8,
        "timeout_count": 16,
        "concurrent_conflict_count": 24,
        "error_count": 32,
        "retry_count": 40,
        "last_call_started_ns": 48,
        "last_call_finished_ns": 56,
        "last_call_latency_ns": 64,
    },
    "aorta_timer_options_t": {
        "callback_group": 0,
        "callback_group_handle": 8,
        "auto_start": 16,
        "invoke_on_missed": 17,
        "overflow_policy": 20,
        "overflow_block_timeout_ms": 24,
    },
    "aorta_subscriber_runtime_stats_t": {
        "duplicate_drop_count": 0,
        "out_of_order_count": 8,
        "out_of_order_drop_count": 16,
        "executor_drop_count": 24,
        "local_drop_count": 32,
        "overflow_relay_enqueue_count": 40,
        "overflow_relay_drop_count": 48,
    },
    "aorta_ctx_read_t": {
        "value_kind": 0,
        "bool_value": 1,
        "i64_value": 8,
        "u64_value": 16,
        "f64_value": 24,
        "string_value": 32,
        "is_fresh": 40,
        "stamp_ns": 48,
        "expires_at_ns": 56,
        "source_node": 64,
        "source_instance_id": 72,
        "sequence": 80,
    },
    "aorta_ctx_change_t": {
        "kind": 0,
        "snapshot_done_reason": 1,
        "key": 8,
        "value_kind": 24,
        "bool_value": 25,
        "i64_value": 32,
        "u64_value": 40,
        "f64_value": 48,
        "string_value": 56,
        "stamp_ns": 72,
        "expires_at_ns": 80,
        "source_node": 88,
        "source_instance_id": 104,
        "sequence": 120,
    },
    "aorta_ctx_owner_liveliness_event_t": {
        "node_name": 0,
        "source_instance_id": 16,
        "alive": 32,
    },
    "aorta_sub_options_t": {
        "callback_group": 0,
        "callback_group_handle": 8,
        "deadline_ns": 16,
        "prefetch_schema_on_init": 24,
        "enable_header_context": 25,
        "include_topic_in_context": 26,
        "include_receive_timestamp": 27,
        "include_priority_in_context": 28,
        "include_publisher_timestamp": 29,
        "enable_dedup": 30,
        "dedup_window": 32,
        "enable_reorder_check": 40,
        "drop_out_of_order": 41,
        "overflow_policy": 44,
        "overflow_relay_capacity": 48,
        "schema_bfbs": 56,
        "schema_bfbs_len": 64,
    },
    "aorta_sub_buffering_t": {
        "receive_depth": 0,
        "delivery_mode": 8,
        "receive_overflow": 12,
    },
    "aorta_pull_sample_t": {
        "payload": 0,
        "context": 24,
        "internal": 80,
    },
    "aorta_action_client_options_t": {
        "control_timeout_ms": 0,
        "info_timeout_ms": 8,
    },
    "aorta_action_server_options_t": {
        "supports_cancel": 0,
        "max_concurrency": 8,
        "result_ttl_ms": 16,
        "dedup_ttl_ms": 24,
    },
    "aorta_action_server_codec_t": {
        "decode_send_goal_request_goal_id": 0,
        "decode_cancel_goal_request": 8,
        "decode_get_result_request": 16,
        "encode_send_goal_response": 24,
        "encode_cancel_goal_response": 32,
        "encode_public_status": 40,
        "encode_feedback": 48,
        "encode_get_result_response": 56,
    },
    "aorta_executor_admission_stats_t": {
        "queued_events": 0,
        "queued_bytes": 8,
        "peak_queued_events": 16,
        "peak_queued_bytes": 24,
        "dropped_queue_full": 32,
        "dropped_queue_closed": 40,
        "dropped_budget_events": 48,
        "dropped_budget_bytes": 56,
    },
}

# Rust type name → ctypes mirror. The single source for the runtime guard and
# the static drift test.
MIRRORS: List[Tuple[str, Type[ctypes.Structure]]] = [
    ("aorta_string_view_t", AortaStringView),
    ("aorta_bytes_slice_t", AortaBytesSlice),
    ("aorta_schema_hash_view_t", AortaSchemaHashView),
    ("aorta_bytes_view_t", AortaBytesView),
    ("aorta_owned_bytes_slice_t", AortaOwnedBytesSlice),
    ("aorta_owned_bytes_view_t", AortaOwnedBytesView),
    ("aorta_owned_bytes_t", AortaOwnedBytes),
    ("aorta_msg_context_t", AortaMsgContext),
    ("aorta_pub_options_t", AortaPubOptions),
    ("aorta_string_kv_t", AortaStringKv),
    ("aorta_node_telemetry_options_t", AortaNodeTelemetryOptions),
    ("aorta_node_options_t", AortaNodeOptions),
    ("aorta_service_options_t", AortaServiceOptions),
    ("aorta_client_options_t", AortaClientOptions),
    ("aorta_service_stats_t", AortaServiceStats),
    ("aorta_client_stats_t", AortaClientStats),
    ("aorta_timer_options_t", AortaTimerOptions),
    ("aorta_subscriber_runtime_stats_t", AortaSubscriberRuntimeStats),
    ("aorta_ctx_read_t", AortaCtxRead),
    ("aorta_ctx_change_t", AortaCtxChange),
    ("aorta_ctx_owner_liveliness_event_t", AortaCtxOwnerLivelinessEvent),
    ("aorta_sub_options_t", AortaSubOptions),
    ("aorta_sub_buffering_t", AortaSubBuffering),
    ("aorta_pull_sample_t", AortaPullSample),
    ("aorta_action_client_options_t", AortaActionClientOptions),
    ("aorta_action_server_options_t", AortaActionServerOptions),
    ("aorta_action_server_codec_t", AortaActionServerCodec),
    ("aorta_executor_admission_stats_t", AortaExecutorAdmissionStats),
]


def field_offsets(ctype: Type[ctypes.Structure]) -> Dict[str, int]:
    """Map each ctypes Structure field name to its byte offset."""
    return {name: getattr(ctype, name).offset for name, *_ in ctype._fields_}


def check_layouts() -> List[str]:
    """Return a list of human-readable layout-mismatch messages (empty = OK).

    For every mirror, asserts ``ctypes.sizeof`` equals the baked
    ``EXPECTED_SIZES`` and the per-field-name offsets equal ``EXPECTED_OFFSETS``
    (so a same-size field reorder is caught — the name→offset map changes even
    when the ordered offset list does not). Used by the loader's runtime ABI
    guard; never raises, so the caller controls the error type.
    """
    mismatches: List[str] = []
    for rust_name, ctype in MIRRORS:
        actual_size = ctypes.sizeof(ctype)
        if actual_size != EXPECTED_SIZES[rust_name]:
            mismatches.append(
                f"{rust_name}: ctypes mirror is {actual_size} bytes, "
                f"expected {EXPECTED_SIZES[rust_name]}"
            )
        actual_offsets = field_offsets(ctype)
        if actual_offsets != EXPECTED_OFFSETS[rust_name]:
            mismatches.append(
                f"{rust_name}: field offsets {actual_offsets}, "
                f"expected {EXPECTED_OFFSETS[rust_name]}"
            )
    return mismatches
