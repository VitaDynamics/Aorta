"""Unit tests for aorta.options (RPC option/stats marshaling) + the
owned-bytes-view helper. Pure main-thread tests; no live session needed.
"""

from __future__ import annotations

import ctypes

import pytest

from aorta import options as O
from aorta._ffi import bindings, structs
from aorta.schema import SchemaAttachPolicy


def test_enum_values_match_cabi():
    assert (O.CallbackGroup.MUTUALLY_EXCLUSIVE, O.CallbackGroup.REENTRANT) == (0, 1)
    assert (O.RpcOverflowPolicy.FAST_FAIL_BUSY, O.RpcOverflowPolicy.BLOCK) == (0, 1)


def test_build_service_options_none():
    struct, keep = O.build_service_options(None)
    assert struct is None and keep == ()


def test_build_service_options_defaults():
    s, keep = O.build_service_options(O.ServiceOptions())
    assert s.schema_attach_policy == int(SchemaAttachPolicy.NEVER)
    assert s.callback_group == int(O.CallbackGroup.MUTUALLY_EXCLUSIVE)
    assert s.rpc_overflow_policy == int(O.RpcOverflowPolicy.FAST_FAIL_BUSY)
    assert s.rpc_overflow_block_timeout_ms == 0
    assert s.deadline_ns == 0
    # no schema -> NULL pointers + zero lengths
    assert not s.service_schema_hash_hex and s.service_schema_hash_len == 0
    assert not s.service_schema_bfbs and s.service_schema_bfbs_len == 0
    assert not s.request_root_type and not s.response_root_type
    assert s in keep  # the struct itself is pinned


def test_build_service_options_with_schema():
    opts = O.ServiceOptions(
        schema_attach_policy=SchemaAttachPolicy.ALWAYS,
        callback_group=O.CallbackGroup.REENTRANT,
        rpc_overflow_policy=O.RpcOverflowPolicy.BLOCK,
        rpc_overflow_block_timeout_ms=250,
        deadline_ns=999,
        service_schema_bfbs=b"\x01\x02\x03\x04",
        service_schema_hash_hex="ab" * 32,
        request_root_type="demo.Req",
        response_root_type="demo.Resp",
    )
    s, keep = O.build_service_options(opts)
    assert s.schema_attach_policy == int(SchemaAttachPolicy.ALWAYS)
    assert s.callback_group == int(O.CallbackGroup.REENTRANT)
    assert s.rpc_overflow_policy == int(O.RpcOverflowPolicy.BLOCK)
    assert s.rpc_overflow_block_timeout_ms == 250
    assert s.deadline_ns == 999
    assert s.service_schema_bfbs and s.service_schema_bfbs_len == 4
    assert s.service_schema_hash_hex and s.service_schema_hash_len == 64
    assert s.request_root_type and s.request_root_type_len == len(b"demo.Req")
    assert s.response_root_type and s.response_root_type_len == len(b"demo.Resp")
    assert len(keep) >= 5  # struct + 4 buffers pinned


def test_build_client_options_none():
    struct, keep = O.build_client_options(None)
    assert struct is None and keep == ()


def test_build_client_options_defaults():
    c, _keep = O.build_client_options(O.ClientOptions())
    assert c.timeout_ms == 5000
    assert c.schema_attach_policy == int(SchemaAttachPolicy.INHERIT)
    assert c.enforce_single_provider == 0  # inherit
    assert c.allow_retry == 0  # inherit (OFF)
    assert c.multi_reply_check_window_ms == 0
    assert not c.service_schema_hash_hex and c.service_schema_hash_len == 0


def test_build_client_options_tristate_and_fields():
    c, keep = O.build_client_options(
        O.ClientOptions(
            timeout_ms=1234,
            enforce_single_provider=True,
            allow_retry=False,
            multi_reply_check_window_ms=77,
            service_schema_hash_hex="cd" * 32,
        )
    )
    assert c.timeout_ms == 1234
    assert c.enforce_single_provider == 2  # True
    assert c.allow_retry == 1  # False
    assert c.multi_reply_check_window_ms == 77
    assert c.service_schema_hash_len == 64
    # the other tri-state combination
    c2, _ = O.build_client_options(
        O.ClientOptions(enforce_single_provider=False, allow_retry=True)
    )
    assert c2.enforce_single_provider == 1  # False
    assert c2.allow_retry == 2  # True


def test_service_stats_from_ffi():
    raw = structs.AortaServiceStats()
    raw.request_received_count = 5
    raw.reply_ok_count = 4
    raw.reply_error_count = 1
    raw.in_flight_requests = 2
    s = O.ServiceStats._from_ffi(raw)
    assert isinstance(s, O.ServiceStats)
    assert s.request_received_count == 5
    assert s.reply_ok_count == 4
    assert s.reply_error_count == 1
    assert s.in_flight_requests == 2
    assert s.task_exit_count == 0


def test_client_stats_from_ffi():
    raw = structs.AortaClientStats()
    raw.call_count = 9
    raw.success_count = 7
    raw.timeout_count = 1
    raw.error_count = 1
    raw.last_call_latency_ns = 42
    c = O.ClientStats._from_ffi(raw)
    assert isinstance(c, O.ClientStats)
    assert c.call_count == 9
    assert c.success_count == 7
    assert c.timeout_count == 1
    assert c.error_count == 1
    assert c.last_call_latency_ns == 42


def test_subscriber_overflow_policy_enum():
    assert (O.SubscriberOverflowPolicy.DROP, O.SubscriberOverflowPolicy.RELAY) == (0, 1)


def test_build_subscriber_options_none_and_defaults():
    assert O.build_subscriber_options(None) == (None, ())
    s, keep = O.build_subscriber_options(O.SubscriberOptions())
    # defaults match Core's SubscriberOptions::default()
    assert s.prefetch_schema_on_init == 1
    assert s.enable_header_context == 0 and s.include_topic_in_context == 0
    assert s.enable_dedup == 0 and s.dedup_window == 1024
    assert s.overflow_policy == int(O.SubscriberOverflowPolicy.DROP)
    assert s.overflow_relay_capacity == 1024
    assert not s.schema_bfbs and s.schema_bfbs_len == 0
    assert s in keep


def test_build_subscriber_options_all_fields():
    s, keep = O.build_subscriber_options(O.SubscriberOptions(
        callback_group=O.CallbackGroup.REENTRANT, callback_group_handle=5, deadline_ns=9,
        enable_header_context=True, include_topic_in_context=True,
        include_receive_timestamp=True, include_priority_in_context=True,
        include_publisher_timestamp=True, enable_dedup=True, dedup_window=64,
        enable_reorder_check=True, drop_out_of_order=True,
        overflow_policy=O.SubscriberOverflowPolicy.RELAY, overflow_relay_capacity=99,
        schema_bfbs=b"\x01\x02",
    ))
    assert s.callback_group == int(O.CallbackGroup.REENTRANT) and s.callback_group_handle == 5
    assert s.deadline_ns == 9 and s.enable_header_context == 1
    assert s.include_topic_in_context == 1 and s.include_receive_timestamp == 1
    assert s.include_priority_in_context == 1 and s.include_publisher_timestamp == 1
    assert s.enable_dedup == 1 and s.dedup_window == 64
    assert s.enable_reorder_check == 1 and s.drop_out_of_order == 1
    assert s.overflow_policy == int(O.SubscriberOverflowPolicy.RELAY)
    assert s.overflow_relay_capacity == 99
    assert s.schema_bfbs and s.schema_bfbs_len == 2


def _fake_meta(bfbs=b"\x01\x02\x03", hash_hex="ab" * 32, root="demo.Chat"):
    import types
    return types.SimpleNamespace(
        SCHEMA_BFBS=bfbs, SCHEMA_HASH_HEX=hash_hex, ROOT_TYPE_NAME=root
    )


def test_inject_service_schema_noop_without_meta():
    assert O.inject_service_schema(None, None) is None
    base = O.ServiceOptions()
    assert O.inject_service_schema(base, None, None) is base


def test_inject_service_schema_fills_from_request_meta():
    s = O.inject_service_schema(None, _fake_meta())
    assert s.service_schema_bfbs == b"\x01\x02\x03"
    assert s.service_schema_hash_hex == "ab" * 32
    assert s.request_root_type == "demo.Chat"
    assert s.response_root_type == "demo.Chat"  # defaults to the request meta


def test_inject_service_schema_separate_response_meta():
    s = O.inject_service_schema(
        None, _fake_meta(root="demo.Req"), _fake_meta(root="demo.Resp")
    )
    assert s.request_root_type == "demo.Req"
    assert s.response_root_type == "demo.Resp"


def test_inject_service_schema_response_meta_only():
    # request_meta None, response_meta set -> only response_root_type filled.
    s = O.inject_service_schema(None, None, _fake_meta(root="demo.Resp"))
    assert s.request_root_type is None
    assert s.service_schema_bfbs is None
    assert s.response_root_type == "demo.Resp"


def test_inject_service_schema_explicit_match_ok():
    base = O.ServiceOptions(request_root_type="demo.Chat")
    s = O.inject_service_schema(base, _fake_meta())  # same value -> no conflict
    assert s.request_root_type == "demo.Chat"


def test_inject_service_schema_conflict_raises():
    base = O.ServiceOptions(request_root_type="other.Type")
    with pytest.raises(O.errors.InvalidArgumentError, match="conflict"):
        O.inject_service_schema(base, _fake_meta(root="demo.Chat"))


def test_inject_client_schema():
    assert O.inject_client_schema(None, None) is None
    c = O.inject_client_schema(None, _fake_meta())
    assert c.service_schema_hash_hex == "ab" * 32
    # explicit conflict raises
    with pytest.raises(O.errors.InvalidArgumentError, match="conflict"):
        O.inject_client_schema(O.ClientOptions(service_schema_hash_hex="cd" * 32), _fake_meta())


def test_owned_view_from_bytes_empty():
    view, keep = bindings.owned_view_from_bytes(b"")
    assert view.slice_count == 0 and view.total_len == 0
    assert not view.slices
    assert keep == ()


def test_owned_view_from_bytes_nonempty():
    payload = b"request-bytes"
    view, keep = bindings.owned_view_from_bytes(payload)
    assert view.slice_count == 1
    assert view.total_len == len(payload)
    assert view.slices  # non-null pointer to the slice
    assert len(keep) == 2  # (data buffer, slice struct)
    # read the slice back and confirm it points at a faithful copy
    slc = ctypes.cast(view.slices, ctypes.POINTER(structs.AortaOwnedBytesSlice)).contents
    assert slc.len == len(payload)
    assert ctypes.string_at(slc.ptr, slc.len) == payload
    assert not slc.release  # NULL release -> Core copies


# ---- QoS & buffering redesign: SubscriberOptions buffering knobs ----

def test_delivery_and_overflow_enum_values_match_cabi():
    assert (O.DeliveryMode.EXECUTOR, O.DeliveryMode.INLINE_PULL) == (0, 1)
    assert (O.ReceiveOverflow.DROP_NEWEST, O.ReceiveOverflow.DROP_OLDEST) == (0, 1)


def test_subscriber_options_buffering_defaults():
    opts = O.SubscriberOptions()
    assert opts.receive_depth == 0
    assert opts.delivery == O.DeliveryMode.EXECUTOR
    assert opts.receive_overflow == O.ReceiveOverflow.DROP_NEWEST


def test_build_sub_buffering_defaults():
    # A default SubscriberOptions yields an all-zero buffering companion =
    # today's behavior (aorta_sub_options_t itself is layout-frozen).
    s, keep = O.build_subscriber_options(O.SubscriberOptions())
    assert isinstance(s, structs.AortaSubOptions)
    assert s.overflow_relay_capacity == 1024
    assert s in keep
    b = O.build_sub_buffering(O.SubscriberOptions())
    assert isinstance(b, structs.AortaSubBuffering)
    assert b.receive_depth == 0
    assert b.delivery_mode == 0
    assert b.receive_overflow == 0


def test_build_sub_buffering_none_is_none():
    assert O.build_sub_buffering(None) is None


def test_build_sub_buffering_tier_a_cap():
    opts = O.SubscriberOptions(receive_depth=32)
    b = O.build_sub_buffering(opts)
    assert b.receive_depth == 32
    assert b.delivery_mode == 0  # executor
    assert b.receive_overflow == 0  # drop-newest


def test_build_sub_buffering_pull_ring():
    opts = O.SubscriberOptions(
        receive_depth=8,
        delivery=O.DeliveryMode.INLINE_PULL,
        receive_overflow=O.ReceiveOverflow.DROP_OLDEST,
    )
    b = O.build_sub_buffering(opts)
    assert b.receive_depth == 8
    assert b.delivery_mode == 1
    assert b.receive_overflow == 1


def test_build_subscriber_options_buffering_keeps_schema_buffer_alive():
    # schema_bfbs pointer must stay valid via the keepalive tuple.
    opts = O.SubscriberOptions(schema_bfbs=b"\x01\x02\x03\x04")
    s, keep = O.build_subscriber_options(opts)
    assert s.schema_bfbs_len == 4
    assert s.schema_bfbs  # non-null
    assert ctypes.string_at(s.schema_bfbs, 4) == b"\x01\x02\x03\x04"


def test_borrowed_view_from_bytes_empty():
    view, keep = bindings.borrowed_view_from_bytes(b"")
    assert view.slice_count == 0 and view.total_len == 0
    assert not view.slices
    assert keep == ()


def test_borrowed_view_from_bytes_nonempty():
    payload = b"action-request-bytes"
    view, keep = bindings.borrowed_view_from_bytes(payload)
    assert isinstance(view, structs.AortaBytesView)  # borrowed, not owned
    assert view.slice_count == 1
    assert view.total_len == len(payload)
    assert view.slices  # non-null pointer to the slice
    assert len(keep) == 2  # (data buffer, slice struct)
    # read the borrowed slice back (16-byte {ptr,len} stride, no release fields)
    slc = ctypes.cast(view.slices, ctypes.POINTER(structs.AortaBytesSlice)).contents
    assert slc.len == len(payload)
    assert ctypes.string_at(slc.ptr, slc.len) == payload
    # round-trips through the standard borrowed-view reader
    assert bindings.bytes_from_view(view) == payload
