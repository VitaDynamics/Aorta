"""ctypes signatures for the pub/sub + RPC C-ABI surface + payload-view helpers.

Configures ``argtypes``/``restype`` for the node/publisher/subscriber/service/
client entry points once, and defines the ``ON_MSG``/``SERVICE_CB`` callback
prototypes plus helpers to copy a borrowed (possibly multi-slice)
``aorta_bytes_view_t`` into owned Python ``bytes`` and to build an
``aorta_owned_bytes_view_t`` over a Python buffer. Signatures verified against
core/src/ffi/c_api/{node,publisher,subscriber,service,client}.rs.
"""

from __future__ import annotations

import ctypes
import threading

from . import structs

__all__ = [
    "aorta_node_t",
    "aorta_pub_t",
    "aorta_sub_t",
    "aorta_service_t",
    "aorta_client_t",
    "aorta_service_request_t",
    "aorta_timer_t",
    "aorta_exec_t",
    "aorta_stop_handle_t",
    "aorta_callback_group_handle_t",
    "aorta_ctx_t",
    "aorta_ctx_watch_t",
    "aorta_ctx_owner_liveliness_watch_t",
    "CTX_OWNER_LIVELINESS_CB",
    "aorta_action_client_t",
    "aorta_action_server_t",
    "aorta_action_context_t",
    "aorta_action_feedback_sub_t",
    "ON_MSG",
    "SERVICE_CB",
    "TIMER_CB",
    "CTX_WATCH_CB",
    "ACTION_DECODE_GOAL_ID",
    "ACTION_DECODE_GOAL_SECRET",
    "ACTION_ENCODE_SEND_GOAL_RESPONSE",
    "ACTION_ENCODE_CANCEL_RESPONSE",
    "ACTION_ENCODE_PUBLIC_STATUS",
    "ACTION_ENCODE_FEEDBACK",
    "ACTION_ENCODE_GET_RESULT_RESPONSE",
    "ACTION_EXECUTE_CB",
    "RELEASE_BYTES_FN",
    "TIMER_COUNTERS",
    "configure",
    "bytes_from_view",
    "str_from_view",
    "owned_view_from_bytes",
    "borrowed_view_from_bytes",
]

# Opaque registry handles are u64 (cbindgen: uint64_t).
aorta_node_t = ctypes.c_uint64
aorta_pub_t = ctypes.c_uint64
aorta_sub_t = ctypes.c_uint64
aorta_service_t = ctypes.c_uint64
aorta_client_t = ctypes.c_uint64
aorta_service_request_t = ctypes.c_uint64

# aorta_on_msg_cb = fn(bytes_view*, schema_hash_view*, msg_context*, user_data*)
ON_MSG = ctypes.CFUNCTYPE(
    None,
    ctypes.POINTER(structs.AortaBytesView),
    ctypes.POINTER(structs.AortaSchemaHashView),
    ctypes.POINTER(structs.AortaMsgContext),
    ctypes.c_void_p,
)

# aorta_service_callback_t = fn(bytes_view* request, service_request_t, user_data*)
SERVICE_CB = ctypes.CFUNCTYPE(
    None,
    ctypes.POINTER(structs.AortaBytesView),
    aorta_service_request_t,
    ctypes.c_void_p,
)

aorta_timer_t = ctypes.c_uint64
aorta_exec_t = ctypes.c_uint64
aorta_stop_handle_t = ctypes.c_uint64
aorta_callback_group_handle_t = ctypes.c_uint64
aorta_ctx_t = ctypes.c_uint64
aorta_ctx_watch_t = ctypes.c_uint64
aorta_ctx_owner_liveliness_watch_t = ctypes.c_uint64

# aorta_executor_type_t enum values. CURRENT (2) is not supported by
# aorta_executor_new (it only constructs Single/Multi/Async), so it is omitted.
AORTA_EXECUTOR_SINGLE = 0
AORTA_EXECUTOR_MULTI = 1
AORTA_EXECUTOR_ASYNC = 3

# aorta_timer_cb = fn(user_data*)
TIMER_CB = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

# aorta_ctx_watch_callback_t = fn(ctx_change* change, user_data*)
CTX_WATCH_CB = ctypes.CFUNCTYPE(
    None, ctypes.POINTER(structs.AortaCtxChange), ctypes.c_void_p
)

# aorta_ctx_owner_liveliness_callback_t = fn(event*, user_data*)
CTX_OWNER_LIVELINESS_CB = ctypes.CFUNCTYPE(
    None, ctypes.POINTER(structs.AortaCtxOwnerLivelinessEvent), ctypes.c_void_p
)

# ── Action handles + the 8 codec callback prototypes ─────────────────────────
aorta_action_client_t = ctypes.c_uint64
aorta_action_server_t = ctypes.c_uint64
aorta_action_context_t = ctypes.c_uint64
aorta_action_feedback_sub_t = ctypes.c_uint64

_SV = structs.AortaStringView
_BVP = ctypes.POINTER(structs.AortaBytesView)
_SVP = ctypes.POINTER(structs.AortaStringView)
_OBVP = ctypes.POINTER(structs.AortaOwnedBytesView)
_I32 = ctypes.c_int32
# ActionStatus is i8, ActionReplyCode is i16 — SIGNED (mandatory, contract §discriminants).
_I8 = ctypes.c_int8
_I16 = ctypes.c_int16
_U8 = ctypes.c_uint8
_U64 = ctypes.c_uint64

# String views are passed BY VALUE in the encode callbacks (16-byte {ptr,len}).
ACTION_DECODE_GOAL_ID = ctypes.CFUNCTYPE(_I32, _BVP, _SVP)
ACTION_DECODE_GOAL_SECRET = ctypes.CFUNCTYPE(_I32, _BVP, _SVP, _SVP)
ACTION_ENCODE_SEND_GOAL_RESPONSE = ctypes.CFUNCTYPE(
    _I32, _U8, _SV, _SV, _I16, _I8, _SV, _SV, _OBVP
)
ACTION_ENCODE_CANCEL_RESPONSE = ctypes.CFUNCTYPE(_I32, _U8, _SV, _I16, _I8, _SV, _OBVP)
ACTION_ENCODE_PUBLIC_STATUS = ctypes.CFUNCTYPE(
    _I32, _SV, _I8, _I16, _U64, _U64, _U64, _U64, _SV, _OBVP
)
ACTION_ENCODE_FEEDBACK = ctypes.CFUNCTYPE(_I32, _SV, _U64, _U64, _I8, _BVP, _SV, _OBVP)
ACTION_ENCODE_GET_RESULT_RESPONSE = ctypes.CFUNCTYPE(
    _I32, _SV, _I8, _U64, _U64, _U64, _U64, _BVP, _BVP, _I16, _SV, _SV, _OBVP
)
# execute_cb = fn(request_bytes_view*, action_context_t, user_data*)
ACTION_EXECUTE_CB = ctypes.CFUNCTYPE(None, _BVP, aorta_action_context_t, ctypes.c_void_p)

# aorta_release_bytes_fn = fn(release_ctx*, data*, len) — Core calls this to free
# an owned slice it took ownership of (used for codec encode out-params, whose
# lifetime outlives the codec call).
RELEASE_BYTES_FN = ctypes.CFUNCTYPE(
    None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t
)

# The 13 timer diagnostic counters: (TimerStats field == C-ABI getter suffix).
# Used by both configure() (to set argtypes) and Timer.stats().
TIMER_COUNTERS = (
    "coalesced_tick_count",
    "overflow_timeout_count",
    "deadline_slip_total_ns",
    "deadline_slip_max_ns",
    "due_firings_total",
    "due_firings_max",
    "due_sample_count",
    "coalesce_requeue_count",
    "coalesce_inline_dispatch_total",
    "coalesce_inline_dispatch_max",
    "coalesce_event_wait_total_ns",
    "coalesce_event_wait_max_ns",
    "coalesce_event_wait_sample_count",
)

_configured = False
_cfg_lock = threading.Lock()


def configure(lib: ctypes.CDLL) -> ctypes.CDLL:
    """Set argtypes/restype for the pub/sub surface once; return the library."""
    global _configured
    if _configured:
        return lib
    with _cfg_lock:
        if _configured:  # pragma: no cover - race re-check
            return lib
        i32 = ctypes.c_int32
        cstr = ctypes.c_char_p
        vp = ctypes.c_void_p

        lib.aorta_node_new.argtypes = [cstr, cstr, ctypes.POINTER(aorta_node_t)]
        lib.aorta_node_new.restype = i32
        lib.aorta_node_new_with_options.argtypes = [
            cstr, cstr, vp, ctypes.POINTER(aorta_node_t)
        ]
        lib.aorta_node_new_with_options.restype = i32
        lib.aorta_node_free.argtypes = [aorta_node_t]
        lib.aorta_node_free.restype = i32
        lib.aorta_node_now.argtypes = [aorta_node_t, ctypes.POINTER(ctypes.c_int64)]
        lib.aorta_node_now.restype = i32
        lib.aorta_node_has_event_sender.argtypes = [
            aorta_node_t, ctypes.POINTER(ctypes.c_uint8)
        ]
        lib.aorta_node_has_event_sender.restype = i32
        lib.aorta_node_get_id.argtypes = [
            aorta_node_t, cstr, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)
        ]
        lib.aorta_node_get_id.restype = i32
        lib.aorta_node_get_group.argtypes = [
            aorta_node_t, cstr, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)
        ]
        lib.aorta_node_get_group.restype = i32
        lib.aorta_now_ns.argtypes = [ctypes.POINTER(ctypes.c_int64)]
        lib.aorta_now_ns.restype = i32
        lib.aorta_uuid_v7.argtypes = [cstr, ctypes.c_size_t]
        lib.aorta_uuid_v7.restype = i32

        lib.aorta_publisher_new.argtypes = [
            aorta_node_t, cstr, vp, vp, ctypes.POINTER(aorta_pub_t)
        ]
        lib.aorta_publisher_new.restype = i32
        lib.aorta_publisher_publish.argtypes = [aorta_pub_t, cstr, ctypes.c_size_t]
        lib.aorta_publisher_publish.restype = i32
        lib.aorta_publisher_free.argtypes = [aorta_pub_t]
        lib.aorta_publisher_free.restype = i32
        lib.aorta_publisher_get_stats.argtypes = [
            aorta_pub_t, ctypes.POINTER(structs.AortaPublisherStats)
        ]
        lib.aorta_publisher_get_stats.restype = i32
        # Route-readiness gate (matches Rust/C++ matching_status/wait_for_matching).
        # out_matching is *mut u8 (0/1); timeout is u64 ms.
        u8p = ctypes.POINTER(ctypes.c_uint8)
        lib.aorta_publisher_matching_status.argtypes = [aorta_pub_t, u8p]
        lib.aorta_publisher_matching_status.restype = i32
        lib.aorta_publisher_wait_for_matching.argtypes = [
            aorta_pub_t, ctypes.c_uint64, u8p
        ]
        lib.aorta_publisher_wait_for_matching.restype = i32
        # QoS presets — the single C-ABI source of preset truth.
        # aorta_qos_preset returns the FULL aorta_qos_t BY VALUE (priority /
        # reliability / congestion_control / express).
        lib.aorta_qos_preset.argtypes = [i32]
        lib.aorta_qos_preset.restype = structs.AortaQoS

        lib.aorta_subscriber_new.argtypes = [
            aorta_node_t, cstr, vp, vp, ON_MSG, vp, ctypes.POINTER(aorta_sub_t)
        ]
        lib.aorta_subscriber_new.restype = i32
        # Buffered ctor: + *const aorta_sub_buffering_t after opts
        # (aorta_sub_options_t is layout-frozen because the Python SDK mirrors it).
        lib.aorta_subscriber_new_buffered.argtypes = [
            aorta_node_t, cstr, vp, vp, vp, ON_MSG, vp, ctypes.POINTER(aorta_sub_t)
        ]
        lib.aorta_subscriber_new_buffered.restype = i32
        lib.aorta_subscriber_free.argtypes = [aorta_sub_t]
        lib.aorta_subscriber_free.restype = i32
        # Pull (Tier B-pull) consumption. out is *mut aorta_pull_sample_t.
        lib.aorta_sub_recv.argtypes = [
            aorta_sub_t, ctypes.POINTER(structs.AortaPullSample), ctypes.c_int64,
        ]
        lib.aorta_sub_recv.restype = i32
        lib.aorta_sub_try_recv.argtypes = [
            aorta_sub_t, ctypes.POINTER(structs.AortaPullSample),
        ]
        lib.aorta_sub_try_recv.restype = i32
        lib.aorta_sub_dropped_count.argtypes = [aorta_sub_t]
        lib.aorta_sub_dropped_count.restype = ctypes.c_size_t
        lib.aorta_pull_sample_free.argtypes = [
            ctypes.POINTER(structs.AortaPullSample)
        ]
        lib.aorta_pull_sample_free.restype = None

        # ── RPC: service (server) ────────────────────────────────────────────
        # opts is *const aorta_service_options_t (vp accepts byref(opts) | None).
        lib.aorta_service_new_with_options.argtypes = [
            aorta_node_t, cstr, vp, SERVICE_CB, vp, ctypes.POINTER(aorta_service_t)
        ]
        lib.aorta_service_new_with_options.restype = i32
        lib.aorta_service_free.argtypes = [aorta_service_t]
        lib.aorta_service_free.restype = i32
        lib.aorta_service_get_stats.argtypes = [
            aorta_service_t, ctypes.POINTER(structs.AortaServiceStats)
        ]
        lib.aorta_service_get_stats.restype = i32
        # response is *const aorta_owned_bytes_view_t (None => empty reply).
        lib.aorta_service_request_reply.argtypes = [
            aorta_service_request_t, ctypes.POINTER(structs.AortaOwnedBytesView)
        ]
        lib.aorta_service_request_reply.restype = i32
        lib.aorta_service_request_reply_error.argtypes = [aorta_service_request_t, cstr]
        lib.aorta_service_request_reply_error.restype = i32
        lib.aorta_service_request_release.argtypes = [aorta_service_request_t]
        lib.aorta_service_request_release.restype = i32

        # ── RPC: client (sync) ───────────────────────────────────────────────
        lib.aorta_client_new_with_options.argtypes = [
            aorta_node_t, cstr, vp, ctypes.POINTER(aorta_client_t)
        ]
        lib.aorta_client_new_with_options.restype = i32
        lib.aorta_client_call.argtypes = [
            aorta_client_t,
            ctypes.POINTER(structs.AortaOwnedBytesView),
            ctypes.POINTER(structs.AortaOwnedBytes),
        ]
        lib.aorta_client_call.restype = i32
        lib.aorta_client_free.argtypes = [aorta_client_t]
        lib.aorta_client_free.restype = i32
        lib.aorta_client_get_stats.argtypes = [
            aorta_client_t, ctypes.POINTER(structs.AortaClientStats)
        ]
        lib.aorta_client_get_stats.restype = i32
        # Route-readiness gate for the query leg (matches Rust/C++ Client matching).
        lib.aorta_client_matching_status.argtypes = [
            aorta_client_t, ctypes.POINTER(ctypes.c_uint8)
        ]
        lib.aorta_client_matching_status.restype = i32
        lib.aorta_client_wait_for_matching.argtypes = [
            aorta_client_t, ctypes.c_uint64, ctypes.POINTER(ctypes.c_uint8)
        ]
        lib.aorta_client_wait_for_matching.restype = i32
        lib.aorta_owned_bytes_free.argtypes = [ctypes.POINTER(structs.AortaOwnedBytes)]
        lib.aorta_owned_bytes_free.restype = None

        # ── Timer ────────────────────────────────────────────────────────────
        u64p = ctypes.POINTER(ctypes.c_uint64)
        lib.aorta_timer_new_with_options.argtypes = [
            aorta_node_t, ctypes.c_int64, TIMER_CB, vp, vp, ctypes.POINTER(aorta_timer_t)
        ]
        lib.aorta_timer_new_with_options.restype = i32
        lib.aorta_timer_cancel.argtypes = [aorta_timer_t]
        lib.aorta_timer_cancel.restype = i32
        lib.aorta_timer_reset.argtypes = [aorta_timer_t]
        lib.aorta_timer_reset.restype = i32
        lib.aorta_timer_is_running.argtypes = [aorta_timer_t, ctypes.POINTER(ctypes.c_uint8)]
        lib.aorta_timer_is_running.restype = i32
        lib.aorta_timer_get_period_ns.argtypes = [aorta_timer_t, u64p]
        lib.aorta_timer_get_period_ns.restype = i32
        lib.aorta_timer_free.argtypes = [aorta_timer_t]
        lib.aorta_timer_free.restype = i32
        for _name in TIMER_COUNTERS:
            fn = getattr(lib, "aorta_timer_get_" + _name)
            fn.argtypes = [aorta_timer_t, u64p]
            fn.restype = i32

        # ── Subscriber runtime stats ─────────────────────────────────────────
        lib.aorta_subscriber_runtime_stats.argtypes = [
            aorta_sub_t, ctypes.POINTER(structs.AortaSubscriberRuntimeStats)
        ]
        lib.aorta_subscriber_runtime_stats.restype = i32

        # ── Executor + stop handle + callback groups ─────────────────────────
        lib.aorta_executor_new.argtypes = [i32, ctypes.POINTER(aorta_exec_t)]
        lib.aorta_executor_new.restype = i32
        lib.aorta_executor_add_node.argtypes = [aorta_exec_t, aorta_node_t]
        lib.aorta_executor_add_node.restype = i32
        lib.aorta_executor_spin.argtypes = [aorta_exec_t]
        lib.aorta_executor_spin.restype = i32
        lib.aorta_executor_spin_once.argtypes = [aorta_exec_t, ctypes.c_int64]
        lib.aorta_executor_spin_once.restype = i32
        lib.aorta_executor_stop.argtypes = [aorta_exec_t]
        lib.aorta_executor_stop.restype = i32
        lib.aorta_executor_get_stop_handle.argtypes = [
            aorta_exec_t, ctypes.POINTER(aorta_stop_handle_t)
        ]
        lib.aorta_executor_get_stop_handle.restype = i32
        lib.aorta_stop_handle_stop.argtypes = [aorta_stop_handle_t]
        lib.aorta_stop_handle_stop.restype = i32
        lib.aorta_stop_handle_free.argtypes = [aorta_stop_handle_t]
        lib.aorta_stop_handle_free.restype = i32
        lib.aorta_executor_free.argtypes = [aorta_exec_t]
        lib.aorta_executor_free.restype = i32
        lib.aorta_executor_create_callback_group.argtypes = [
            aorta_exec_t, i32, ctypes.POINTER(aorta_callback_group_handle_t)
        ]
        lib.aorta_executor_create_callback_group.restype = i32
        lib.aorta_executor_free_callback_group.argtypes = [
            aorta_exec_t, aorta_callback_group_handle_t
        ]
        lib.aorta_executor_free_callback_group.restype = i32
        lib.aorta_executor_get_admission_stats.argtypes = [
            aorta_exec_t, ctypes.POINTER(structs.AortaExecutorAdmissionStats)
        ]
        lib.aorta_executor_get_admission_stats.restype = i32
        # Node-level callback groups back ExecutionGroup.
        lib.aorta_node_create_callback_group.argtypes = [
            aorta_node_t, i32, ctypes.POINTER(aorta_callback_group_handle_t)
        ]
        lib.aorta_node_create_callback_group.restype = i32
        lib.aorta_node_free_callback_group.argtypes = [
            aorta_node_t, aorta_callback_group_handle_t
        ]
        lib.aorta_node_free_callback_group.restype = i32

        # ── ContextStore ─────────────────────────────────────────────────────
        szp = ctypes.POINTER(ctypes.c_size_t)
        readp = ctypes.POINTER(structs.AortaCtxRead)
        lib.aorta_ctx_new.argtypes = [aorta_node_t, ctypes.POINTER(aorta_ctx_t)]
        lib.aorta_ctx_new.restype = i32
        lib.aorta_ctx_free.argtypes = [aorta_ctx_t]
        lib.aorta_ctx_free.restype = i32
        lib.aorta_ctx_shutdown.argtypes = [aorta_ctx_t]
        lib.aorta_ctx_shutdown.restype = i32
        lib.aorta_ctx_declare_owned_prefix.argtypes = [aorta_ctx_t, cstr]
        lib.aorta_ctx_declare_owned_prefix.restype = i32
        lib.aorta_ctx_is_key_declared.argtypes = [
            aorta_ctx_t, cstr, ctypes.POINTER(ctypes.c_uint8)
        ]
        lib.aorta_ctx_is_key_declared.restype = i32
        lib.aorta_ctx_declared_prefixes.argtypes = [aorta_ctx_t, cstr, ctypes.c_size_t, szp]
        lib.aorta_ctx_declared_prefixes.restype = i32
        # setters: (handle, key, value, expires_at_ns)
        lib.aorta_ctx_set_bool.argtypes = [aorta_ctx_t, cstr, ctypes.c_bool, ctypes.c_uint64]
        lib.aorta_ctx_set_bool.restype = i32
        lib.aorta_ctx_set_i64.argtypes = [aorta_ctx_t, cstr, ctypes.c_int64, ctypes.c_uint64]
        lib.aorta_ctx_set_i64.restype = i32
        lib.aorta_ctx_set_u64.argtypes = [aorta_ctx_t, cstr, ctypes.c_uint64, ctypes.c_uint64]
        lib.aorta_ctx_set_u64.restype = i32
        lib.aorta_ctx_set_f64.argtypes = [aorta_ctx_t, cstr, ctypes.c_double, ctypes.c_uint64]
        lib.aorta_ctx_set_f64.restype = i32
        lib.aorta_ctx_set_string.argtypes = [aorta_ctx_t, cstr, cstr, ctypes.c_uint64]
        lib.aorta_ctx_set_string.restype = i32
        # getters: (handle, key, out_read)
        for _g in ("bool", "i64", "u64", "f64", "string"):
            fn = getattr(lib, "aorta_ctx_get_" + _g)
            fn.argtypes = [aorta_ctx_t, cstr, readp]
            fn.restype = i32
        lib.aorta_ctx_read_free.argtypes = [readp]
        lib.aorta_ctx_read_free.restype = i32
        # watch
        lib.aorta_ctx_watch.argtypes = [
            aorta_ctx_t, cstr, CTX_WATCH_CB, vp, ctypes.POINTER(aorta_ctx_watch_t)
        ]
        lib.aorta_ctx_watch.restype = i32
        lib.aorta_ctx_watch_with_snapshot.argtypes = [
            aorta_ctx_t, cstr, ctypes.c_uint64, CTX_WATCH_CB, vp,
            ctypes.POINTER(aorta_ctx_watch_t),
        ]
        lib.aorta_ctx_watch_with_snapshot.restype = i32
        lib.aorta_ctx_watch_free.argtypes = [aorta_ctx_watch_t]
        lib.aorta_ctx_watch_free.restype = i32
        lib.aorta_ctx_owner_liveliness_watch.argtypes = [
            aorta_ctx_t, CTX_OWNER_LIVELINESS_CB, vp,
            ctypes.POINTER(aorta_ctx_owner_liveliness_watch_t),
        ]
        lib.aorta_ctx_owner_liveliness_watch.restype = i32
        lib.aorta_ctx_owner_liveliness_watch_free.argtypes = [
            aorta_ctx_owner_liveliness_watch_t
        ]
        lib.aorta_ctx_owner_liveliness_watch_free.restype = i32

        # ── Action: client ───────────────────────────────────────────────────
        obytesp = ctypes.POINTER(structs.AortaOwnedBytes)
        # Action request/feedback/result payloads are BORROWED views on the
        # Rust side (`*const aorta_bytes_view_t`) — Core copies synchronously
        # during the call. Declaring the owned-view struct here was a bug
        # (non-crashing only by layout coincidence).
        bviewp = ctypes.POINTER(structs.AortaBytesView)
        # Publish-side action-name normalization (additive C-ABI):
        # (action_name, out_buf, buf_size, out_len) -> rc. Lets the SDK store the
        # SAME canonical name the core routes under, so action_name()/name()
        # match the Rust/C++ normalized getters.
        lib.aorta_normalize_action_name.argtypes = [
            cstr, cstr, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)
        ]
        lib.aorta_normalize_action_name.restype = i32
        lib.aorta_action_client_new_with_options.argtypes = [
            aorta_node_t, cstr, vp, ctypes.POINTER(aorta_action_client_t)
        ]
        lib.aorta_action_client_new_with_options.restype = i32
        lib.aorta_action_client_send_goal.argtypes = [aorta_action_client_t, bviewp, obytesp]
        lib.aorta_action_client_send_goal.restype = i32
        lib.aorta_action_client_query_status.argtypes = [aorta_action_client_t, cstr, obytesp]
        lib.aorta_action_client_query_status.restype = i32
        lib.aorta_action_client_query_status_with_timeout.argtypes = [
            aorta_action_client_t, cstr, ctypes.c_uint64, obytesp
        ]
        lib.aorta_action_client_query_status_with_timeout.restype = i32
        lib.aorta_action_client_cancel.argtypes = [aorta_action_client_t, bviewp, obytesp]
        lib.aorta_action_client_cancel.restype = i32
        lib.aorta_action_client_get_result.argtypes = [aorta_action_client_t, bviewp, obytesp]
        lib.aorta_action_client_get_result.restype = i32
        lib.aorta_action_client_get_result_with_timeout.argtypes = [
            aorta_action_client_t, bviewp, ctypes.c_uint64, obytesp
        ]
        lib.aorta_action_client_get_result_with_timeout.restype = i32
        lib.aorta_action_client_info.argtypes = [aorta_action_client_t, obytesp]
        lib.aorta_action_client_info.restype = i32
        lib.aorta_action_client_free.argtypes = [aorta_action_client_t]
        lib.aorta_action_client_free.restype = i32
        lib.aorta_action_client_subscribe_feedback.argtypes = [
            aorta_action_client_t, ON_MSG, vp, ctypes.POINTER(aorta_action_feedback_sub_t)
        ]
        lib.aorta_action_client_subscribe_feedback.restype = i32
        lib.aorta_action_feedback_sub_free.argtypes = [aorta_action_feedback_sub_t]
        lib.aorta_action_feedback_sub_free.restype = i32

        # ── Action: server + codec + context ─────────────────────────────────
        lib.aorta_action_server_info_new_with_options.argtypes = [
            aorta_node_t, cstr, vp, ctypes.c_size_t, vp,
            ctypes.POINTER(aorta_action_server_t),
        ]
        lib.aorta_action_server_info_new_with_options.restype = i32
        lib.aorta_action_server_new_with_options.argtypes = [
            aorta_node_t, cstr, vp, ctypes.c_size_t, vp,
            ctypes.POINTER(structs.AortaActionServerCodec), ACTION_EXECUTE_CB, vp,
            ctypes.POINTER(aorta_action_server_t),
        ]
        lib.aorta_action_server_new_with_options.restype = i32
        lib.aorta_action_server_free.argtypes = [aorta_action_server_t]
        lib.aorta_action_server_free.restype = i32
        lib.aorta_action_context_is_cancel_requested.argtypes = [
            aorta_action_context_t, ctypes.POINTER(ctypes.c_uint8)
        ]
        lib.aorta_action_context_is_cancel_requested.restype = i32
        lib.aorta_action_context_publish_feedback.argtypes = [
            aorta_action_context_t, bviewp, cstr
        ]
        lib.aorta_action_context_publish_feedback.restype = i32
        lib.aorta_action_context_succeed.argtypes = [aorta_action_context_t, bviewp]
        lib.aorta_action_context_succeed.restype = i32
        lib.aorta_action_context_abort.argtypes = [
            aorta_action_context_t, ctypes.c_int16, cstr
        ]
        lib.aorta_action_context_abort.restype = i32
        lib.aorta_action_context_cancel.argtypes = [
            aorta_action_context_t, ctypes.c_int16, cstr
        ]
        lib.aorta_action_context_cancel.restype = i32
        lib.aorta_action_context_free.argtypes = [aorta_action_context_t]
        lib.aorta_action_context_free.restype = i32

        _configured = True
        return lib


def bytes_from_view(view: structs.AortaBytesView) -> bytes:
    """Copy a borrowed (possibly multi-slice) bytes view into owned ``bytes``.

    Iterates ``slices[0:slice_count]`` and concatenates — never assumes a single
    contiguous slice (Zenoh scatter). Safe to retain after the callback returns.
    """
    if not view.slice_count or not view.slices:
        return b""
    slice_array = ctypes.cast(
        view.slices, ctypes.POINTER(structs.AortaBytesSlice)
    )
    chunks = []
    for i in range(view.slice_count):
        sl = slice_array[i]
        if sl.ptr and sl.len:
            chunks.append(ctypes.string_at(sl.ptr, sl.len))
    return b"".join(chunks)


def str_from_view(view: structs.AortaStringView) -> str:
    """Decode a borrowed UTF-8 string view (empty when null)."""
    if view.ptr and view.len:
        return ctypes.string_at(view.ptr, view.len).decode("utf-8", "replace")
    return ""


def owned_view_from_bytes(payload: bytes):
    """Build a single-slice ``aorta_owned_bytes_view_t`` over ``payload``.

    Used to pass a request/response body into ``aorta_client_call`` /
    ``aorta_service_request_reply``. ``release`` is left NULL: Core *copies* the
    bytes synchronously (core mod.rs ``zbytes_from_owned_ffi_view`` — the
    NULL-release branch does ``to_vec``), so we never need a release callback on
    this copy path (zero-copy ``publish_owned`` is separate). Returns
    ``(view, keepalive)``; the caller MUST keep ``keepalive`` referenced across
    the FFI call so the backing buffer + slice stay alive. Empty payload yields a
    zero-slice view (Core maps it to an empty ``ZBytes``).
    """
    buf = bytes(payload)
    n = len(buf)
    view = structs.AortaOwnedBytesView()
    if n == 0:
        view.slices = None
        view.slice_count = 0
        view.total_len = 0
        return view, ()
    arr = (ctypes.c_ubyte * n).from_buffer_copy(buf)
    slc = structs.AortaOwnedBytesSlice()
    slc.ptr = ctypes.cast(arr, ctypes.c_void_p)
    slc.len = n
    slc.release = None
    slc.release_ctx = None
    view.slices = ctypes.addressof(slc)
    view.slice_count = 1
    view.total_len = n
    # keepalive pins both the data buffer (arr) and the slice struct (slc) whose
    # address `view.slices` points at — neither may be GC'd before the FFI call.
    return view, (arr, slc)


def borrowed_view_from_bytes(payload: bytes):
    """Build a single-slice BORROWED ``aorta_bytes_view_t`` over ``payload``.

    For C-ABI entry points whose Rust signature is ``*const
    aorta_bytes_view_t`` — the six action request/feedback/result paths.
    Core copies the bytes synchronously during the call, so no
    release callback exists; the caller MUST keep ``keepalive`` referenced
    across the FFI call so the backing buffer + slice stay alive. Empty
    payload yields a zero-slice view (Core maps it to empty bytes).
    """
    buf = bytes(payload)
    n = len(buf)
    view = structs.AortaBytesView()
    if n == 0:
        view.slices = None
        view.slice_count = 0
        view.total_len = 0
        return view, ()
    arr = (ctypes.c_ubyte * n).from_buffer_copy(buf)
    slc = structs.AortaBytesSlice()
    slc.ptr = ctypes.cast(arr, ctypes.c_void_p)
    slc.len = n
    view.slices = ctypes.addressof(slc)
    view.slice_count = 1
    view.total_len = n
    return view, (arr, slc)
