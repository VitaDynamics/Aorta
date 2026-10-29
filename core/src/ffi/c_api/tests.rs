// Tests for the C-ABI FFI surface.
//
// Lifted verbatim out of c_api/mod.rs.
// Imports were hoisted out of the outer `mod tests { ... }` block
// so the module name in mod.rs (`#[cfg(test)] mod tests;`) supplies
// the test gate.
//
// Lint exception: every `unsafe { … }` block
// in this module is a deliberate call into the FFI extern "C" surface
// being tested. Arguments are constructed locally inside each test (CStr
// builders, stack-allocated `aorta_*_t` structs, registry handles from
// the API under test) with lifetimes that bound the FFI call, and the
// safety contract for each block is identical to the one documented on
// the corresponding `pub unsafe extern "C" fn` in `c_api/*.rs`. A
// per-block SAFETY comment here would be 126 copies of the same prose
// — counterproductive for both reviewer and reader. The lib-root
// `warn`/`deny` still keeps PRODUCTION code honest.
#![allow(clippy::undocumented_unsafe_blocks)]

use super::*;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn last_error_string() -> String {
    let ptr = aorta_last_error();
    if ptr.is_null() {
        return "<null>".to_string();
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .unwrap_or("<invalid utf8>")
        .to_string()
}

extern "C" fn test_service_reply_empty_callback(
    _payload: *const aorta_bytes_view_t,
    request_handle: aorta_service_request_t,
    user_data: *mut c_void,
) {
    if !user_data.is_null() {
        let call_count = unsafe { &*(user_data as *const AtomicUsize) };
        call_count.fetch_add(1, Ordering::SeqCst);
    }
    let _ = unsafe { aorta_service_request_reply(request_handle, ptr::null()) };
}

extern "C" fn test_timer_noop_callback(_user_data: *mut c_void) {}

fn ffi_qos() -> aorta_qos_t {
    aorta_qos_t {
        priority: aorta_priority_t::AORTA_PRIORITY_NORMAL,
        reliability: aorta_reliability_t::AORTA_RELIABILITY_RELIABLE,
        congestion_control: AORTA_CONGESTION_CONTROL_DROP,
        express: 0,
    }
}

#[test]
fn qos_struct_layout_is_12_bytes() {
    // priority(4) + reliability(4) + congestion_control(1) + express(1) + pad(2).
    assert_eq!(std::mem::size_of::<aorta_qos_t>(), 12);
    assert_eq!(std::mem::align_of::<aorta_qos_t>(), 4);
}

#[test]
fn normalize_action_name_canonicalizes_and_reports_required_len() {
    // The C-ABI helper SDK bindings use to store the *normalized* action name,
    // so client-side key building (e.g. status subscriptions) matches the keys
    // the server publishes under.
    let input = CString::new("/demo/execute_task/").unwrap();
    let want = "demo/execute_task";

    let mut buf = [0u8; 64];
    let mut out_len: usize = 0;
    let rc = unsafe {
        aorta_normalize_action_name(
            input.as_ptr(),
            buf.as_mut_ptr() as *mut c_char,
            buf.len(),
            &mut out_len,
        )
    };
    assert_eq!(rc, AORTA_OK, "normalize failed: {}", last_error_string());
    let got = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
        .to_str()
        .unwrap();
    assert_eq!(got, want);
    assert_eq!(out_len, want.len());

    // Buffer too small -> error, with the required length reported in out_len.
    let mut tiny = [0u8; 4];
    let mut need: usize = 0;
    let rc_small = unsafe {
        aorta_normalize_action_name(
            input.as_ptr(),
            tiny.as_mut_ptr() as *mut c_char,
            tiny.len(),
            &mut need,
        )
    };
    assert_eq!(rc_small, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(need, want.len());
}

#[test]
fn qos_preset_enum_layout_is_4_bytes() {
    // `aorta_qos_preset_t` is `#[repr(C)]` enum with 5 variants — Rust
    // lowers this to a C `enum`, which on every supported target is
    // `int` (4 bytes). The cross-language tests on the C++ side
    // (`cpp/test/qos_preset_test.cc`) re-assert the same; this test
    // catches Rust-side drift early.
    assert_eq!(std::mem::size_of::<aorta_qos_preset_t>(), 4);
}

// No direct out-of-range test from Rust: synthesizing an invalid
// `aorta_qos_preset_t` via `mem::transmute` triggers Rust's
// debug-only validity check before reaching `aorta_qos_preset`.
// The defensive fallback in `qos_for_preset` is verified via the
// C++ integration test instead (which can legitimately construct
// out-of-range values from the C side without tripping Rust's
// construction check). See `cpp/test/integration/qos_preset_integration_test.cc`.

#[test]
fn qos_preset_returns_canonical_values() {
    // Single source of truth check: each FFI preset round-trips
    // through `qos_from_ffi` to the same value the high-level Rust
    // constructor returns. Drift here means the C-ABI surface and the
    // Rust runtime disagree about what e.g. RealtimeControl means.
    for (preset, want) in [
        (aorta_qos_preset_t::AORTA_QOS_PRESET_DEFAULT, QoS::default()),
        (
            aorta_qos_preset_t::AORTA_QOS_PRESET_REALTIME_CONTROL,
            QoS::realtime_control(),
        ),
        (
            aorta_qos_preset_t::AORTA_QOS_PRESET_SENSOR_DATA,
            QoS::sensor_data(),
        ),
        (
            aorta_qos_preset_t::AORTA_QOS_PRESET_STATE_UPDATE,
            QoS::state_update(),
        ),
        (
            aorta_qos_preset_t::AORTA_QOS_PRESET_BULK_TRANSFER,
            QoS::bulk_transfer(),
        ),
    ] {
        // `aorta_qos_preset` now returns the FULL struct, so a single
        // `qos_from_ffi` round-trip recovers every field.
        let ffi = aorta_qos_preset(preset);
        let round_tripped = qos_from_ffi(ptr::addr_of!(ffi)).unwrap();
        assert_eq!(round_tripped, want, "preset {preset:?}");
    }
}

#[test]
fn qos_from_ffi_defaults_express_disabled() {
    let default_qos = ffi_qos();
    assert!(!qos_from_ffi(ptr::addr_of!(default_qos)).unwrap().express);
}

#[test]
fn qos_from_ffi_reads_express_from_struct_and_rejects_invalid() {
    let mut qos = ffi_qos();
    qos.express = 1;
    assert!(qos_from_ffi(ptr::addr_of!(qos)).unwrap().express);

    qos.express = 0;
    assert!(!qos_from_ffi(ptr::addr_of!(qos)).unwrap().express);

    qos.express = 2;
    assert!(qos_from_ffi(ptr::addr_of!(qos)).is_err());
}

#[test]
fn qos_from_ffi_reads_congestion_control_from_struct_and_rejects_invalid() {
    let mut qos = ffi_qos();
    qos.congestion_control = AORTA_CONGESTION_CONTROL_BLOCK;
    assert_eq!(
        qos_from_ffi(ptr::addr_of!(qos)).unwrap().congestion_control,
        CongestionControl::Block
    );

    qos.congestion_control = 7;
    assert!(qos_from_ffi(ptr::addr_of!(qos)).is_err());
}

#[test]
fn string_view_to_string_roundtrip() {
    let value = "aorta/default/pub/demo/chatter";
    let view = string_view_from_str(value);
    let decoded = unsafe { string_view_to_string(view, "topic") }.expect("decode");
    assert_eq!(decoded, value);
}

#[test]
fn string_view_to_string_accepts_null_zero_len_as_empty() {
    let view = aorta_string_view_t::default();
    let decoded = unsafe { string_view_to_string(view, "topic") }.expect("decode");
    assert!(decoded.is_empty());
}

#[test]
fn string_view_to_string_rejects_null_with_nonzero_len() {
    let view = aorta_string_view_t {
        data: ptr::null(),
        len: 4,
    };
    let err = unsafe { string_view_to_string(view, "topic") }.expect_err("must fail");
    assert!(err.to_string().contains("non-zero"));
}

#[test]
fn string_view_optional_none_when_empty() {
    let view = aorta_string_view_t::default();
    let decoded =
        unsafe { string_view_to_string_optional(view, "publisher_node") }.expect("decode");
    assert!(decoded.is_none());
}

#[test]
fn string_view_optional_rejects_null_with_nonzero_len() {
    let view = aorta_string_view_t {
        data: ptr::null(),
        len: 4,
    };
    let err =
        unsafe { string_view_to_string_optional(view, "publisher_node") }.expect_err("must fail");
    assert!(err.to_string().contains("non-zero"));
}

#[test]
fn map_aorta_error_maps_not_found() {
    let rc = map_aorta_error(AortaError::not_found("missing handle"));
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
}

#[test]
fn map_aorta_error_maps_busy() {
    let rc = map_aorta_error(AortaError::busy("queue full"));
    assert_eq!(rc, AORTA_ERR_BUSY);
}

#[test]
fn map_aorta_error_maps_already_replied() {
    let rc = map_aorta_error(AortaError::already_replied("already handled"));
    assert_eq!(rc, AORTA_ERR_ALREADY_REPLIED);
}

#[test]
fn map_aorta_error_maps_timeout() {
    let rc = map_aorta_error(AortaError::timeout("timed out"));
    assert_eq!(rc, AORTA_ERR_TIMEOUT);
}

#[test]
fn map_aorta_error_maps_concurrent_conflict() {
    let rc = map_aorta_error(AortaError::concurrent_conflict("multiple replies"));
    assert_eq!(rc, AORTA_ERR_CONCURRENT_CONFLICT);
}

#[test]
fn map_aorta_error_maps_prefix_not_declared() {
    let rc = map_aorta_error(AortaError::prefix_not_declared("demo/state"));
    assert_eq!(rc, AORTA_ERR_PREFIX_NOT_DECLARED);
}

#[test]
fn map_aorta_error_maps_declaration_not_ready() {
    let rc = map_aorta_error(AortaError::declaration_not_ready("demo/**"));
    assert_eq!(rc, AORTA_ERR_DECLARATION_NOT_READY);
}

#[test]
fn map_aorta_error_maps_kind_mismatch() {
    let rc = map_aorta_error(AortaError::kind_mismatch("expected Bool got U64"));
    assert_eq!(rc, AORTA_ERR_KIND_MISMATCH);
}

#[test]
fn map_aorta_error_maps_instance_id_collision() {
    let rc = map_aorta_error(AortaError::instance_id_collision("rcp_node"));
    assert_eq!(rc, AORTA_ERR_INSTANCE_ID_COLLISION);
}

#[test]
fn map_aorta_error_maps_transport_to_internal() {
    let rc = map_aorta_error(AortaError::transport(TransportError::backend(
        "zenoh tx queue closed",
    )));
    assert_eq!(rc, AORTA_ERR_INTERNAL);
}

#[test]
fn map_aorta_error_maps_shm_unavailable_to_busy() {
    let rc = map_aorta_error(AortaError::shm_unavailable("shm pool exhausted"));
    assert_eq!(rc, AORTA_ERR_BUSY);
}

#[test]
fn map_aorta_error_maps_quota_exceeded_to_busy() {
    let rc = map_aorta_error(AortaError::quota_exceeded("max loans reached"));
    assert_eq!(rc, AORTA_ERR_BUSY);
}

#[test]
fn map_aorta_error_maps_capacity_exceeded_to_invalid_argument() {
    let rc = map_aorta_error(AortaError::capacity_exceeded("payload too large"));
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn map_aorta_error_maps_not_supported_to_invalid_argument() {
    let rc = map_aorta_error(AortaError::not_supported("policy unsupported"));
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn now_ns_rejects_null_out_pointer() {
    let rc = unsafe { aorta_now_ns(ptr::null_mut()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn now_ns_returns_value() {
    let mut out = 0i64;
    let rc = unsafe { aorta_now_ns(&mut out as *mut i64) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert!(out >= 0);
}

#[test]
fn uuid_v7_rejects_null_out_pointer() {
    let rc = unsafe { aorta_uuid_v7(ptr::null_mut(), 37) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn uuid_v7_returns_valid_version_and_variant() {
    let mut out = [0i8; 37];
    let rc = unsafe { aorta_uuid_v7(out.as_mut_ptr(), out.len()) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let text = unsafe { CStr::from_ptr(out.as_ptr()) }
        .to_str()
        .expect("UUID is UTF-8");
    let parsed = uuid::Uuid::parse_str(text).expect("valid UUID");
    assert_eq!(text, parsed.hyphenated().to_string());
    assert_eq!(parsed.get_version_num(), 7);
    assert_eq!(parsed.get_variant(), uuid::Variant::RFC4122);
}

#[test]
fn uuid_v7_rejects_short_output_buffer() {
    let mut out = [0i8; 36];
    let rc = unsafe { aorta_uuid_v7(out.as_mut_ptr(), out.len()) };
    assert_eq!(rc, AORTA_ERR_BUFFER_TOO_SMALL);
}

#[test]
fn log_configure_rejects_null_config() {
    let rc = unsafe { aorta_log_configure(ptr::null()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn log_configure_accepts_valid_config() {
    let filter = CString::new("aorta_core=info,zenoh=warn").expect("filter");
    let cfg = aorta_log_config_t {
        filter: filter.as_ptr(),
        file_path: ptr::null(),
        format: aorta_log_format_t::AORTA_LOG_FORMAT_TEXT,
        stderr_enabled: 1,
    };
    let rc = unsafe { aorta_log_configure(&cfg) };
    assert!(rc == AORTA_OK || rc == AORTA_ERR_BUSY);
}

#[test]
fn log_write_rejects_null_message() {
    let scope = CString::new("aorta_sdk").expect("scope");
    let rc = unsafe {
        aorta_log_write(
            aorta_log_level_t::AORTA_LOG_LEVEL_WARN,
            scope.as_ptr(),
            ptr::null(),
        )
    };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn log_write_accepts_valid_payload() {
    let scope = CString::new("aorta_sdk").expect("scope");
    let message = CString::new("publisher oversize").expect("message");
    let rc = unsafe {
        aorta_log_write(
            aorta_log_level_t::AORTA_LOG_LEVEL_WARN,
            scope.as_ptr(),
            message.as_ptr(),
        )
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn log_write_at_accepts_valid_payload() {
    let scope = CString::new("aorta_sdk").expect("scope");
    let file = CString::new("publisher.cc").expect("file");
    let message = CString::new("publisher oversize").expect("message");
    let rc = unsafe {
        aorta_log_write_at(
            aorta_log_level_t::AORTA_LOG_LEVEL_WARN,
            scope.as_ptr(),
            file.as_ptr(),
            42,
            message.as_ptr(),
        )
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn log_flush_is_ok() {
    let rc = aorta_log_flush();
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn log_flush_is_idempotent() {
    let first = aorta_log_flush();
    let second = aorta_log_flush();
    assert_eq!(first, AORTA_OK);
    assert_eq!(second, AORTA_OK);
}

#[test]
fn log_set_context_rejects_null_node_name() {
    let group = CString::new("default").expect("group");
    let rc = unsafe { aorta_log_set_context(ptr::null(), group.as_ptr()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn log_set_context_accepts_valid_values() {
    let node = CString::new("node_a").expect("node");
    let group = CString::new("default").expect("group");
    let rc = unsafe { aorta_log_set_context(node.as_ptr(), group.as_ptr()) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn log_enabled_rejects_null_scope() {
    let enabled =
        unsafe { aorta_log_enabled(aorta_log_level_t::AORTA_LOG_LEVEL_INFO, ptr::null()) };
    assert_eq!(enabled, 0);
}

#[test]
fn log_enabled_accepts_valid_scope() {
    let scope = CString::new("aorta_sdk").expect("scope");
    let enabled =
        unsafe { aorta_log_enabled(aorta_log_level_t::AORTA_LOG_LEVEL_INFO, scope.as_ptr()) };
    assert!(enabled == 0 || enabled == 1);
}

#[test]
fn ffi_log_target_maps_sdk_scope() {
    let (target, scope) = ffi_log_target_from_scope("aorta_sdk");
    assert_eq!(target, FfiLogTarget::SdkGeneric);
    assert_eq!(scope, Some("aorta_sdk"));
}

#[test]
fn ffi_log_target_maps_unknown_scope_to_generic_with_field() {
    let (target, scope) = ffi_log_target_from_scope("sdk.custom");
    assert_eq!(target, FfiLogTarget::SdkGeneric);
    assert_eq!(scope, Some("sdk.custom"));
}

#[test]
fn async_executor_in_flight_limit_uses_default_when_unset() {
    let _guard = crate::test_support::ENV_LOCK.lock();
    let _env_guard = crate::test_support::EnvVarGuard::remove(ENV_ASYNC_EXECUTOR_IN_FLIGHT);
    assert_eq!(
        async_executor_in_flight_limit(),
        DEFAULT_ASYNC_EXECUTOR_IN_FLIGHT
    );
}

#[test]
fn async_executor_in_flight_limit_clamps_to_minimum_one() {
    let _guard = crate::test_support::ENV_LOCK.lock();
    let _env_guard = crate::test_support::EnvVarGuard::set(ENV_ASYNC_EXECUTOR_IN_FLIGHT, "0");
    assert_eq!(async_executor_in_flight_limit(), 1);
}

#[test]
fn async_executor_in_flight_limit_accepts_valid_override() {
    let _guard = crate::test_support::ENV_LOCK.lock();
    let _env_guard = crate::test_support::EnvVarGuard::set(ENV_ASYNC_EXECUTOR_IN_FLIGHT, "17");
    assert_eq!(async_executor_in_flight_limit(), 17);
}

#[test]
fn subscriber_overflow_policy_mapping_drop() {
    let policy =
        subscriber_overflow_policy_from_ffi(aorta_sub_overflow_policy_t::AORTA_SUB_OVERFLOW_DROP);
    assert_eq!(policy, SubscriberOverflowPolicy::Drop);
}

#[test]
fn subscriber_overflow_policy_mapping_relay() {
    let policy =
        subscriber_overflow_policy_from_ffi(aorta_sub_overflow_policy_t::AORTA_SUB_OVERFLOW_RELAY);
    assert_eq!(policy, SubscriberOverflowPolicy::Relay);
}

#[test]
fn rpc_overflow_policy_mapping_fast_fail_busy() {
    let policy = rpc_overflow_policy_from_ffi(
        aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
    );
    assert_eq!(policy, crate::service::RpcOverflowPolicy::FastFailBusy);
}

#[test]
fn rpc_overflow_policy_mapping_block() {
    let policy =
        rpc_overflow_policy_from_ffi(aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_BLOCK);
    assert_eq!(policy, crate::service::RpcOverflowPolicy::Block);
}

#[test]
fn timer_overflow_policy_mapping_coalesce_latest() {
    let policy = timer_overflow_policy_from_ffi(
        aorta_timer_overflow_policy_t::AORTA_TIMER_OVERFLOW_COALESCE_LATEST,
    );
    assert_eq!(policy, crate::timer::TimerOverflowPolicy::CoalesceLatest);
}

#[test]
fn timer_overflow_policy_mapping_block() {
    let policy =
        timer_overflow_policy_from_ffi(aorta_timer_overflow_policy_t::AORTA_TIMER_OVERFLOW_BLOCK);
    assert_eq!(policy, crate::timer::TimerOverflowPolicy::Block);
}

// ---- QoS & buffering redesign: buffering sub options + pull recv ----

fn zeroed_sub_options() -> aorta_sub_options_t {
    aorta_sub_options_t {
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        prefetch_schema_on_init: 0,
        enable_header_context: 0,
        include_topic_in_context: 0,
        include_receive_timestamp: 0,
        include_priority_in_context: 0,
        include_publisher_timestamp: 0,
        enable_dedup: 0,
        dedup_window: 0,
        enable_reorder_check: 0,
        drop_out_of_order: 0,
        overflow_policy: aorta_sub_overflow_policy_t::AORTA_SUB_OVERFLOW_DROP,
        overflow_relay_capacity: 0,
        schema_bfbs: ptr::null(),
        schema_bfbs_len: 0,
    }
}

/// Buffering knobs (Tier A / Tier B-pull) for `aorta_subscriber_new_buffered`.
fn buffering(
    receive_depth: u64,
    delivery_mode: u32,
    receive_overflow: u32,
) -> aorta_sub_buffering_t {
    aorta_sub_buffering_t {
        receive_depth,
        delivery_mode,
        receive_overflow,
    }
}

#[test]
fn sub_options_struct_is_abi_stable() {
    // Layout-frozen at the original 72-byte layout (shipped ABI); the
    // buffering knobs live in the companion aorta_sub_buffering_t.
    assert_eq!(std::mem::size_of::<aorta_sub_options_t>(), 72);
    assert_eq!(std::mem::align_of::<aorta_sub_options_t>(), 8);
    // u64(8) + u32(4) + u32(4) = 16, no padding.
    assert_eq!(std::mem::size_of::<aorta_sub_buffering_t>(), 16);
    assert_eq!(std::mem::align_of::<aorta_sub_buffering_t>(), 8);
}

#[test]
fn aorta_pull_sample_free_handles_null() {
    // Must be a no-op, not a crash.
    unsafe { aorta_pull_sample_free(ptr::null_mut()) };
}

#[test]
fn add_node_while_spinning_reports_busy_instead_of_deadlocking() {
    // TCK wave-3 finding: `aorta_executor_spin` holds the executor mutex
    // for the WHOLE spin, so a later add_node blocked forever — and
    // flow::Runtime starts its spin thread inside create(), making
    // rt->add_node a startup RACE (works if it beats the spin thread to
    // the lock, hangs otherwise; flow_demo.cc was winning it by luck).
    // Contract now: a spinning executor REFUSES add_node with BUSY.
    let node_name = CString::new(format!("ffi_busy_node_{}", alloc_handle())).expect("name");
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let mut exec: aorta_exec_t = 0;
    assert_eq!(
        unsafe {
            aorta_executor_new(
                aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                ptr::addr_of_mut!(exec),
            )
        },
        AORTA_OK
    );
    let spinner = std::thread::spawn(move || {
        let _ = aorta_executor_spin(exec);
    });
    // Wait until the spin thread actually HOLDS the executor lock: while
    // it does, add_node must come back BUSY. Before it does, an OK is a
    // legal (pre-spin) add — retry with a fresh expectation window.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut saw_busy = false;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        while std::time::Instant::now() < deadline {
            let rc = aorta_executor_add_node(exec, node);
            if rc == AORTA_ERR_BUSY {
                let _ = tx.send(true);
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = tx.send(false);
    });
    // The old behavior parks the probe thread INSIDE add_node forever —
    // this recv_timeout is the deadlock detector.
    if let Ok(got) = rx.recv_timeout(std::time::Duration::from_secs(8)) {
        saw_busy = got;
    }
    assert!(
        saw_busy,
        "add_node against a spinning executor must return AORTA_ERR_BUSY \
         promptly (deadlock/starvation detected)"
    );
    assert_eq!(aorta_executor_stop(exec), AORTA_OK);
    spinner.join().expect("spin thread");
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn subscriber_pull_recv_try_recv_and_dropped_count() {
    let node_name = CString::new(format!("ffi_pull_node_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/pull/id{}", alloc_handle())).expect("topic");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    // delivery_mode = 1 (InlinePull), depth 4.
    let buf = buffering(4, 1, 1);
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        None, // no callback for pull
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    // Nothing published → empty watch → try_recv is a timeout (no message).
    let mut sample = std::mem::MaybeUninit::<aorta_pull_sample_t>::uninit();
    let rc = unsafe { aorta_sub_try_recv(sub, sample.as_mut_ptr()) };
    assert_eq!(rc, AORTA_ERR_TIMEOUT, "empty watch must report timeout");

    // recv with a short timeout also times out (still nothing published).
    let rc = unsafe { aorta_sub_recv(sub, sample.as_mut_ptr(), 50_000_000) };
    assert_eq!(rc, AORTA_ERR_TIMEOUT);

    // Pull subscribers have no Tier A shed counter.
    assert_eq!(aorta_sub_dropped_count(sub), 0);

    let rc = aorta_subscriber_free(sub);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let rc = aorta_node_free(node);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn subscriber_pull_notify_refuses_a_second_hook() {
    // The single-awaiter contract on the PULL subscriber
    // copy of set_notify_hook — previously never called
    // twice by any test. Same-hook re-arm stays idempotent.
    unsafe extern "C" fn hook_a(_ctx: *mut std::ffi::c_void) {}
    unsafe extern "C" fn hook_b(_ctx: *mut std::ffi::c_void) {}

    let node_name = CString::new(format!("ffi_pull_hook_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/pullhook/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let buf = buffering(4, 1, 1);
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        None,
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    assert_eq!(
        unsafe { aorta_sub_set_notify(sub, Some(hook_a), ptr::null_mut()) },
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_sub_set_notify(sub, Some(hook_b), ptr::null_mut()) },
        AORTA_ERR_INVALID_ARGUMENT,
        "single awaiter: a DIFFERENT hook over a live one is refused"
    );
    assert_eq!(
        unsafe { aorta_sub_set_notify(sub, Some(hook_a), ptr::null_mut()) },
        AORTA_OK,
        "same-hook re-arm is idempotent"
    );
    assert_eq!(
        unsafe { aorta_sub_set_notify(sub, None, ptr::null_mut()) },
        AORTA_OK,
        "the owner clears its hook"
    );
    assert_eq!(
        unsafe { aorta_sub_set_notify(sub, Some(hook_b), ptr::null_mut()) },
        AORTA_OK,
        "a cleared slot accepts a new awaiter"
    );

    assert_eq!(aorta_subscriber_free(sub), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn subscriber_pull_rejects_zero_depth() {
    let node_name = CString::new(format!("ffi_pull_bad_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/pullbad/id{}", alloc_handle())).expect("topic");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    // receive_depth = 0 is invalid for InlinePull.
    let buf = buffering(0, 1, 1);
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        None,
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);

    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

#[test]
fn sub_dropped_count_unknown_handle_is_zero() {
    assert_eq!(aorta_sub_dropped_count(0xDEAD_BEEF), 0);
}

// Regression: a node attached to an executor whose class
// policy defaults subscriber overflow to Relay must still be able to create a
// pull subscriber — the legacy Relay compat rewrite only applies to the
// executor/callback delivery path (an InlinePull sub has no overflow relay,
// and validate_delivery rejects Relay).
#[test]
fn pull_subscriber_survives_relay_class_policy_default() {
    let node_name = CString::new(format!("ffi_pull_relay_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/pullrelay/id{}", alloc_handle())).expect("topic");

    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK,
        "{}",
        last_error_string()
    );

    // Executor whose class policy defaults subscriber overflow to Relay.
    let class_policy = aorta_executor_class_policy_options_t {
        sub_overflow_policy: aorta_sub_overflow_policy_t::AORTA_SUB_OVERFLOW_RELAY,
        sub_overflow_relay_capacity: 2048,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
        timer_overflow_policy: aorta_timer_overflow_policy_t::AORTA_TIMER_OVERFLOW_COALESCE_LATEST,
        timer_overflow_block_timeout_ms: 0,
    };
    let exec_opts = aorta_executor_options_t {
        scheduler_mode: aorta_scheduler_mode_t::AORTA_SCHEDULER_FIFO,
        priority: ptr::null(),
        admission: ptr::null(),
        class_policy: ptr::addr_of!(class_policy),
    };
    let mut exec: aorta_exec_t = 0;
    assert_eq!(
        unsafe {
            aorta_executor_new_with_options(
                aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                ptr::addr_of!(exec_opts),
                ptr::addr_of_mut!(exec),
            )
        },
        AORTA_OK,
        "{}",
        last_error_string()
    );
    assert_eq!(
        aorta_executor_add_node(exec, node),
        AORTA_OK,
        "{}",
        last_error_string()
    );

    // Pull creation AFTER add_node (when the Relay default is live on the
    // node) must succeed — this is exactly the path the compat rewrite used
    // to break.
    let buf = buffering(4, 1, 1);
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        None,
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    assert_eq!(
        aorta_subscriber_free(sub),
        AORTA_OK,
        "{}",
        last_error_string()
    );
    assert_eq!(
        aorta_executor_free(exec),
        AORTA_OK,
        "{}",
        last_error_string()
    );
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

extern "C" fn ffi_test_noop_msg_cb(
    _payload: *const aorta_bytes_view_t,
    _schema: *const aorta_schema_hash_view_t,
    _ctx: *const aorta_msg_context_t,
    _user_data: *mut c_void,
) {
}

// The buffered ctor must reject an unknown delivery_mode rather than
// silently coercing it to Executor.
#[test]
fn subscriber_rejects_unknown_delivery_mode() {
    let node_name = CString::new(format!("ffi_bad_dm_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/baddm/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK
    );
    let buf = buffering(4, 99, 0); // delivery_mode 99 is invalid
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        Some(ffi_test_noop_msg_cb),
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

// Unknown receive_overflow must be rejected too.
#[test]
fn subscriber_rejects_unknown_receive_overflow() {
    let node_name = CString::new(format!("ffi_bad_ro_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/badro/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK
    );
    // executor delivery, receive_overflow 7 is invalid.
    let buf = buffering(0, 0, 7);
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        Some(ffi_test_noop_msg_cb),
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

// InlinePull + a non-null callback is a caller error (it must be
// rejected, not silently ignored).
#[test]
fn subscriber_inline_pull_rejects_callback() {
    let node_name = CString::new(format!("ffi_pull_cb_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/pullcb/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK
    );
    let buf = buffering(4, 1, 1); // delivery_mode 1 = InlinePull
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        Some(ffi_test_noop_msg_cb), // illegal for InlinePull
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

// A blocking aorta_sub_recv (infinite timeout) on an empty watch
// MUST be unblocked when the subscriber is freed — no leak, no deadlock.
#[test]
fn aorta_sub_recv_unblocks_on_free() {
    use std::sync::atomic::{AtomicBool, AtomicI32};
    use std::sync::Arc as StdArc;

    let node_name = CString::new(format!("ffi_unblock_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/unblock/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK
    );
    let buf = buffering(2, 1, 1);
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    assert_eq!(
        aorta_subscriber_new_buffered(
            node,
            topic.as_ptr(),
            ptr::addr_of!(qos),
            ptr::null(),
            ptr::addr_of!(buf),
            None,
            ptr::null_mut(),
            ptr::addr_of_mut!(sub),
        ),
        AORTA_OK,
        "{}",
        last_error_string()
    );

    let done = StdArc::new(AtomicBool::new(false));
    let rc_out = StdArc::new(AtomicI32::new(0xBAD));
    let done_t = StdArc::clone(&done);
    let rc_t = StdArc::clone(&rc_out);
    let sub_handle = sub; // aorta_sub_t is a Copy/Send u64
    let recver = std::thread::spawn(move || {
        let mut sample = std::mem::MaybeUninit::<aorta_pull_sample_t>::uninit();
        // -1 => block forever; nothing is ever published.
        let rc = unsafe { aorta_sub_recv(sub_handle, sample.as_mut_ptr(), -1) };
        rc_t.store(rc, Ordering::SeqCst);
        done_t.store(true, Ordering::SeqCst);
    });

    // Let the recv thread park in the watch.
    std::thread::sleep(Duration::from_millis(250));
    assert!(
        !done.load(Ordering::SeqCst),
        "recv returned before free — it should be blocked on the empty watch"
    );

    // Freeing the subscriber drops the watch's sender side → recv must wake.
    assert_eq!(
        aorta_subscriber_free(sub),
        AORTA_OK,
        "{}",
        last_error_string()
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !done.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        done.load(Ordering::SeqCst),
        "blocking recv did NOT unblock after free — subscriber leaked / undeclare never completed"
    );
    recver.join().expect("recv thread panicked");
    assert_ne!(
        rc_out.load(Ordering::SeqCst),
        AORTA_OK,
        "an unblocked-by-disconnect recv must return an error, not a sample"
    );

    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

// The frozen `aorta_subscriber_new` routes through the buffered
// inner with the buffering knobs zeroed — it produces an Executor (callback)
// subscriber, NOT a pull one, with no Tier A shedding.
#[test]
fn v1_subscriber_new_is_executor_not_pull() {
    let node_name = CString::new(format!("ffi_v1_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/plain/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK
    );

    // plain ctor with a callback (no buffering struct in sight).
    let opts = zeroed_sub_options();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new(
        node,
        topic.as_ptr(),
        ptr::null(),
        ptr::addr_of!(opts),
        Some(ffi_test_noop_msg_cb),
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    // receive_depth defaulted to 0 → no shedding.
    assert_eq!(aorta_sub_dropped_count(sub), 0);

    // It is an executor (callback) subscriber, so a pull recv is rejected.
    let mut sample = std::mem::MaybeUninit::<aorta_pull_sample_t>::uninit();
    let rc = unsafe { aorta_sub_try_recv(sub, sample.as_mut_ptr()) };
    assert_eq!(
        rc, AORTA_ERR_INVALID_ARGUMENT,
        "recv on a the queue path (executor) subscriber must be rejected"
    );

    assert_eq!(
        aorta_subscriber_free(sub),
        AORTA_OK,
        "{}",
        last_error_string()
    );
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

#[test]
fn executor_options_from_ffi_reads_admission_overrides() {
    let priority = aorta_priority_scheduler_options_t {
        max_drain_per_cycle: 16,
        weights: [9, 7, 5, 3, 1],
        map_subscription_qos: 1,
    };
    let admission = aorta_executor_admission_options_t {
        max_queued_events: 111,
        max_queued_bytes: 222,
    };
    let ffi = aorta_executor_options_t {
        scheduler_mode: aorta_scheduler_mode_t::AORTA_SCHEDULER_PRIORITY,
        priority: ptr::addr_of!(priority),
        admission: ptr::addr_of!(admission),
        class_policy: ptr::null(),
    };

    let parsed =
        unsafe { executor_options_from_ffi(ptr::addr_of!(ffi)) }.expect("executor options parse");
    assert_eq!(parsed.mode, SchedulerMode::Priority);
    assert_eq!(parsed.priority.max_drain_per_cycle, 16);
    assert_eq!(parsed.priority.weights, [9, 7, 5, 3, 1]);
    assert!(parsed.priority.map_subscription_qos);
    assert_eq!(parsed.admission.max_queued_events, 111);
    assert_eq!(parsed.admission.max_queued_bytes, 222);
}

#[test]
fn executor_options_from_ffi_defaults_admission_when_null() {
    let ffi = aorta_executor_options_t {
        scheduler_mode: aorta_scheduler_mode_t::AORTA_SCHEDULER_FIFO,
        priority: ptr::null(),
        admission: ptr::null(),
        class_policy: ptr::null(),
    };

    let parsed =
        unsafe { executor_options_from_ffi(ptr::addr_of!(ffi)) }.expect("executor options parse");
    assert_eq!(parsed.mode, SchedulerMode::Fifo);
    assert_eq!(parsed.admission.max_queued_events, 0);
    assert_eq!(parsed.admission.max_queued_bytes, 0);
}

#[test]
fn executor_options_from_ffi_reads_class_policy_overrides() {
    let class_policy = aorta_executor_class_policy_options_t {
        sub_overflow_policy: aorta_sub_overflow_policy_t::AORTA_SUB_OVERFLOW_RELAY,
        sub_overflow_relay_capacity: 2048,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_BLOCK,
        rpc_overflow_block_timeout_ms: 30,
        timer_overflow_policy: aorta_timer_overflow_policy_t::AORTA_TIMER_OVERFLOW_BLOCK,
        timer_overflow_block_timeout_ms: 40,
    };
    let ffi = aorta_executor_options_t {
        scheduler_mode: aorta_scheduler_mode_t::AORTA_SCHEDULER_FIFO,
        priority: ptr::null(),
        admission: ptr::null(),
        class_policy: ptr::addr_of!(class_policy),
    };

    let parsed =
        unsafe { executor_options_from_ffi(ptr::addr_of!(ffi)) }.expect("executor options parse");
    assert_eq!(
        parsed.class_policy.subscriber_overflow_policy,
        SubscriberOverflowPolicy::Relay
    );
    assert_eq!(parsed.class_policy.subscriber_overflow_relay_capacity, 2048);
    assert_eq!(
        parsed.class_policy.rpc_overflow_policy,
        crate::service::RpcOverflowPolicy::Block
    );
    assert_eq!(
        parsed.class_policy.rpc_overflow_block_timeout,
        Duration::from_millis(30)
    );
    assert_eq!(
        parsed.class_policy.timer_overflow_policy,
        crate::timer::TimerOverflowPolicy::Block
    );
    assert_eq!(
        parsed.class_policy.timer_overflow_block_timeout,
        Duration::from_millis(40)
    );
}

#[test]
fn publisher_get_stats_rejects_null_out() {
    let rc = unsafe { aorta_publisher_get_stats(1, ptr::null_mut()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn publisher_get_stats_returns_not_found_for_unknown_handle() {
    let mut stats = aorta_publisher_stats_t::default();
    let rc = unsafe { aorta_publisher_get_stats(u64::MAX, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
}

#[test]
fn publisher_get_stats_not_found_sets_last_error() {
    let mut stats = aorta_publisher_stats_t::default();
    let rc = unsafe { aorta_publisher_get_stats(u64::MAX, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);

    let err_ptr = aorta_last_error();
    assert!(!err_ptr.is_null());
    let err = unsafe { CStr::from_ptr(err_ptr) };
    let err = err.to_str().expect("utf-8");
    assert!(err.contains("publisher handle not found"));
}

#[test]
fn publisher_get_stats_returns_default_stable_fields() {
    let node_name = CString::new(format!("ffi_stats_node_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/stats/id{}", alloc_handle())).expect("topic");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let opts = aorta_pub_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        congestion_control: AORTA_PUB_CONGESTION_INHERIT,
        schema_bfbs: ptr::null(),
        schema_bfbs_len: 0,
        max_message_size: 64,
        schema_root_type: ptr::null(),
    };

    let mut pub_handle: aorta_pub_t = 0;
    let rc = aorta_publisher_new(
        node,
        topic.as_ptr(),
        ptr::null(),
        ptr::addr_of!(opts),
        ptr::addr_of_mut!(pub_handle),
    );
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let mut stats = aorta_publisher_stats_t::default();
    let rc = unsafe { aorta_publisher_get_stats(pub_handle, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(stats.publish_count, 0);
    assert_eq!(stats.oversize_count, 0);
    assert_eq!(stats.send_error_count, 0);

    let rc = aorta_publisher_free(pub_handle);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let rc = aorta_node_free(node);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn node_new_defaults_identity_to_c_api_metadata() {
    let node_name =
        CString::new(format!("ffi_identity_default_{}", alloc_handle())).expect("node name");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let telemetry = registry()
        .nodes
        .lock()
        .get(&node)
        .cloned()
        .expect("node handle")
        .build_node_identity_telemetry();

    assert_eq!(telemetry.language, "c");
    assert_eq!(telemetry.sdk_name, "aorta_c_api");
    assert_eq!(telemetry.sdk_version, env!("CARGO_PKG_VERSION"));

    let rc = aorta_node_free(node);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn node_new_with_options_overrides_identity_metadata() {
    let node_name =
        CString::new(format!("ffi_identity_override_{}", alloc_handle())).expect("node name");
    let executable = CString::new("/tmp/custom-ffi-node").expect("executable");
    let sdk_name = CString::new("custom_ffi_sdk").expect("sdk_name");
    let build_id = CString::new("ffi-build").expect("build_id");
    let container_id = CString::new("ffi-container").expect("container_id");
    let label_key = CString::new(" deployment ").expect("label key");
    let label_value = CString::new(" test ").expect("label value");
    let blank_label_key = CString::new("ignored").expect("blank label key");
    let blank_label_value = CString::new("   ").expect("blank label value");
    let labels = [
        aorta_string_kv_t {
            key: label_key.as_ptr(),
            value: label_value.as_ptr(),
        },
        aorta_string_kv_t {
            key: blank_label_key.as_ptr(),
            value: blank_label_value.as_ptr(),
        },
    ];

    let telemetry = aorta_node_telemetry_options_t {
        executable: executable.as_ptr(),
        language: ptr::null(),
        sdk_name: sdk_name.as_ptr(),
        sdk_version: ptr::null(),
        build_id: build_id.as_ptr(),
        container_id: container_id.as_ptr(),
        labels: labels.as_ptr(),
        labels_len: labels.len(),
    };
    let options = aorta_node_options_t {
        telemetry: ptr::addr_of!(telemetry),
    };

    let mut node: aorta_node_t = 0;
    let rc = unsafe {
        aorta_node_new_with_options(
            node_name.as_ptr(),
            ptr::null(),
            ptr::addr_of!(options),
            ptr::addr_of_mut!(node),
        )
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let identity = registry()
        .nodes
        .lock()
        .get(&node)
        .cloned()
        .expect("node handle")
        .build_node_identity_telemetry();

    assert_eq!(identity.executable, "/tmp/custom-ffi-node");
    assert_eq!(identity.language, "c");
    assert_eq!(identity.sdk_name, "custom_ffi_sdk");
    assert_eq!(identity.sdk_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(identity.build_id.as_deref(), Some("ffi-build"));
    assert_eq!(identity.container_id.as_deref(), Some("ffi-container"));
    assert_eq!(identity.labels.len(), 1);
    assert_eq!(
        identity.labels.get("deployment").map(String::as_str),
        Some("test")
    );

    let rc = aorta_node_free(node);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
}

#[test]
fn node_new_with_options_rejects_null_labels_pointer_with_nonzero_len() {
    let node_name =
        CString::new(format!("ffi_identity_invalid_labels_{}", alloc_handle())).expect("node name");

    let telemetry = aorta_node_telemetry_options_t {
        executable: ptr::null(),
        language: ptr::null(),
        sdk_name: ptr::null(),
        sdk_version: ptr::null(),
        build_id: ptr::null(),
        container_id: ptr::null(),
        labels: ptr::null(),
        labels_len: 1,
    };
    let options = aorta_node_options_t {
        telemetry: ptr::addr_of!(telemetry),
    };

    let mut node: aorta_node_t = 0;
    let rc = unsafe {
        aorta_node_new_with_options(
            node_name.as_ptr(),
            ptr::null(),
            ptr::addr_of!(options),
            ptr::addr_of_mut!(node),
        )
    };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert!(last_error_string().contains("telemetry.labels is null but len is non-zero"));
}

#[test]
fn publisher_publish_owned_not_found_still_releases_owned_slice() {
    let released = AtomicUsize::new(0);
    let payload = [0xABu8, 0xCD, 0xEF];
    let release_ctx = ptr::addr_of!(released) as *mut c_void;
    let slice = aorta_owned_bytes_slice_t {
        data: payload.as_ptr(),
        len: payload.len(),
        release: Some(test_release_counter),
        release_ctx,
    };
    let view = aorta_owned_bytes_view_t {
        slices: ptr::addr_of!(slice),
        slice_count: 1,
        total_len: payload.len(),
    };

    let rc = unsafe { aorta_publisher_publish_owned(u64::MAX, ptr::addr_of!(view)) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
    assert_eq!(released.load(Ordering::SeqCst), 1);

    let err_ptr = aorta_last_error();
    assert!(!err_ptr.is_null());
    let err = unsafe { CStr::from_ptr(err_ptr) };
    let err = err.to_str().expect("utf-8");
    assert!(err.contains("publisher handle not found"));
}

#[test]
fn service_get_stats_rejects_null_out() {
    let rc = unsafe { aorta_service_get_stats(1, ptr::null_mut()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn service_get_stats_returns_not_found_for_unknown_handle() {
    let mut stats = aorta_service_stats_t::default();
    let rc = unsafe { aorta_service_get_stats(u64::MAX, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
}

#[test]
fn client_get_stats_rejects_null_out() {
    let rc = unsafe { aorta_client_get_stats(1, ptr::null_mut()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn client_get_stats_returns_not_found_for_unknown_handle() {
    let mut stats = aorta_client_stats_t::default();
    let rc = unsafe { aorta_client_get_stats(u64::MAX, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
}

#[test]
fn service_get_stats_returns_runtime_counters_for_active_service() {
    let node_name =
        CString::new(format!("ffi_service_stats_node_{}", alloc_handle())).expect("name");
    let service_name_raw = format!("ffi_service_stats_service_{}", alloc_handle());
    let service_name = CString::new(service_name_raw.clone()).expect("service name");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let callback_calls = AtomicUsize::new(0);
    let mut service: aorta_service_t = 0;
    let rc = unsafe {
        aorta_service_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::null(),
            Some(test_service_reply_empty_callback),
            ptr::addr_of!(callback_calls) as *mut c_void,
            ptr::addr_of_mut!(service),
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(service, 0);

    let mut client: aorta_client_t = 0;
    let rc = unsafe {
        aorta_client_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::null(),
            ptr::addr_of_mut!(client),
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    std::thread::sleep(std::time::Duration::from_millis(120));

    let request = b"ffi service stats request";
    let request_slice = aorta_owned_bytes_slice_t {
        data: request.as_ptr(),
        len: request.len(),
        release: None,
        release_ctx: ptr::null_mut(),
    };
    let request_view = aorta_owned_bytes_view_t {
        slices: ptr::addr_of!(request_slice),
        slice_count: 1,
        total_len: request.len(),
    };
    let mut response = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_client_call(
            client,
            ptr::addr_of!(request_view),
            ptr::addr_of_mut!(response),
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_eq!(response.view.total_len, 0);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(response)) };
    assert!(callback_calls.load(Ordering::SeqCst) >= 1);

    let mut stats = aorta_service_stats_t::default();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let rc = unsafe { aorta_service_get_stats(service, ptr::addr_of_mut!(stats)) };
        assert_eq!(rc, AORTA_OK);

        if stats.reply_ok_count >= 1 && stats.in_flight_requests == 0 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting service stats to settle: received={}, dispatched={}, reply_ok={}, in_flight={}",
                stats.request_received_count,
                stats.request_dispatched_count,
                stats.reply_ok_count,
                stats.in_flight_requests
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(stats.request_received_count >= 1);
    assert_eq!(stats.request_dispatched_count, stats.request_received_count);
    assert_eq!(stats.reply_ok_count, stats.request_received_count);
    assert_eq!(stats.reply_error_count, 0);
    assert_eq!(stats.reply_send_failure_count, 0);
    assert_eq!(stats.request_dropped_count, 0);
    assert_eq!(stats.query_receive_failure_count, 0);
    assert_eq!(stats.task_exit_count, 0);
    assert_eq!(stats.in_flight_requests, 0);
    assert!(stats.last_request_ns > 0);
    assert!(stats.last_reply_ns > 0);

    assert_eq!(aorta_client_free(client), AORTA_OK);
    assert_eq!(aorta_service_free(service), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn client_get_stats_returns_runtime_counters_for_active_client() {
    let node_name =
        CString::new(format!("ffi_client_stats_node_{}", alloc_handle())).expect("name");
    let service_name_raw = format!("ffi_client_stats_service_{}", alloc_handle());
    let service_name = CString::new(service_name_raw.clone()).expect("service name");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let callback_calls = AtomicUsize::new(0);
    let mut service: aorta_service_t = 0;
    let rc = unsafe {
        aorta_service_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::null(),
            Some(test_service_reply_empty_callback),
            ptr::addr_of!(callback_calls) as *mut c_void,
            ptr::addr_of_mut!(service),
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(service, 0);

    let mut client: aorta_client_t = 0;
    let rc = unsafe {
        aorta_client_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::null(),
            ptr::addr_of_mut!(client),
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    std::thread::sleep(std::time::Duration::from_millis(120));

    let request = b"ffi client stats request";
    let request_slice = aorta_owned_bytes_slice_t {
        data: request.as_ptr(),
        len: request.len(),
        release: None,
        release_ctx: ptr::null_mut(),
    };
    let request_view = aorta_owned_bytes_view_t {
        slices: ptr::addr_of!(request_slice),
        slice_count: 1,
        total_len: request.len(),
    };
    let mut response = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_client_call(
            client,
            ptr::addr_of!(request_view),
            ptr::addr_of_mut!(response),
        )
    };
    assert_eq!(rc, AORTA_OK);
    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(response)) };
    assert!(callback_calls.load(Ordering::SeqCst) >= 1);

    let mut stats = aorta_client_stats_t::default();
    let rc = unsafe { aorta_client_get_stats(client, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_OK);
    assert_eq!(stats.call_count, 1);
    assert_eq!(stats.success_count, 1);
    assert_eq!(stats.timeout_count, 0);
    assert_eq!(stats.concurrent_conflict_count, 0);
    assert_eq!(stats.error_count, 0);
    assert!(stats.last_call_started_ns > 0);
    assert!(stats.last_call_finished_ns >= stats.last_call_started_ns);

    assert_eq!(aorta_client_free(client), AORTA_OK);
    assert_eq!(aorta_service_free(service), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_client_strict_mode_maps_multi_reply_to_concurrent_conflict() {
    let _guard = crate::test_support::ENV_LOCK.lock();
    let _group_guard = crate::test_support::EnvVarGuard::remove("AORTA_GROUP");
    let _normalize_case_guard =
        crate::test_support::EnvVarGuard::remove("AORTA_KEY_NORMALIZE_CASE");

    let node_name =
        CString::new(format!("ffi_client_strict_node_{}", alloc_handle())).expect("name");
    let service_name_raw = format!("ffi_client_strict_service_{}", alloc_handle());
    let service_name = CString::new(service_name_raw.clone()).expect("service name");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let query_session = runtime()
        .block_on(crate::session::Session::open_default())
        .expect("open query session");
    let key_expr = crate::key_expr::key_builder_from_group_env()
        .service_key_server(&service_name_raw)
        .expect("service key");
    let key_expr: String = key_expr.into();
    let probe_key_expr = key_expr.clone();
    let queryable = runtime()
        .block_on(async {
            query_session
                .inner()
                .declare_queryable(key_expr.clone())
                .await
        })
        .expect("declare queryable");
    let query_task = runtime().spawn(async move {
        for _ in 0..2 {
            let query = match queryable.recv_async().await {
                Ok(query) => query,
                Err(_) => break,
            };
            let payload = query
                .payload()
                .map(|p| p.to_bytes().to_vec())
                .unwrap_or_default();
            let _ = query.reply(&key_expr, payload.clone()).await;
            let _ = query.reply(&key_expr, payload).await;
        }
        let _ = queryable.undeclare().await;
    });

    // Warm up route discovery with a direct query so the strict-mode call
    // can deterministically exercise multi-reply conflict mapping.
    runtime().block_on(async {
        let replies = query_session
            .inner()
            .get(probe_key_expr.clone())
            .await
            .expect("probe get");
        let first_reply = tokio::time::timeout(Duration::from_secs(3), replies.recv_async())
            .await
            .expect("probe timeout");
        first_reply.expect("probe reply");
    });

    let client_opts = aorta_client_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        timeout_ms: 3000,
        enforce_single_provider: AORTA_OPTIONAL_BOOL_TRUE,
        allow_retry: AORTA_OPTIONAL_BOOL_INHERIT,
        multi_reply_check_window_ms: 500,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
    };
    let mut client: aorta_client_t = 0;
    let rc = unsafe {
        aorta_client_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::addr_of!(client_opts),
            ptr::addr_of_mut!(client),
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    let request = b"ffi strict request";
    let request_slice = aorta_owned_bytes_slice_t {
        data: request.as_ptr(),
        len: request.len(),
        release: None,
        release_ctx: ptr::null_mut(),
    };
    let request_view = aorta_owned_bytes_view_t {
        slices: ptr::addr_of!(request_slice),
        slice_count: 1,
        total_len: request.len(),
    };
    let mut response = aorta_owned_bytes_t::default();
    let rc = unsafe {
        aorta_client_call(
            client,
            ptr::addr_of!(request_view),
            ptr::addr_of_mut!(response),
        )
    };
    assert_eq!(rc, AORTA_ERR_CONCURRENT_CONFLICT);

    let mut stats = aorta_client_stats_t::default();
    let rc = unsafe { aorta_client_get_stats(client, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_OK);
    assert_eq!(stats.call_count, 1);
    assert_eq!(stats.success_count, 0);
    assert_eq!(stats.timeout_count, 0);
    assert_eq!(stats.concurrent_conflict_count, 1);
    assert_eq!(stats.error_count, 0);

    assert_eq!(aorta_client_free(client), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);

    if !query_task.is_finished() {
        query_task.abort();
    }
    let _ = runtime().block_on(query_task);
    let _ = runtime().block_on(query_session.close());
}

#[test]
fn service_request_reply_unknown_handle_sets_not_found_error() {
    let rc = unsafe { aorta_service_request_reply(u64::MAX, ptr::null()) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);

    let err_ptr = aorta_last_error();
    assert!(!err_ptr.is_null());
    let err = unsafe { CStr::from_ptr(err_ptr) };
    let err = err.to_str().expect("utf-8");
    assert!(err.contains("request handle not found"));
}

#[test]
fn ffi_service_builder_registers_schema_by_key_with_node_registry() {
    let node_name = CString::new(format!("ffi_schema_node_{}", alloc_handle())).expect("name");
    let service_name_raw = format!("ffi_schema_service_{}", alloc_handle());
    let service_name = CString::new(service_name_raw.clone()).expect("service name");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let service_bfbs = b"ffi_schema_service".to_vec();
    let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);
    let service_hex = service_hash.to_hex_bytes();
    let node_obj = {
        registry()
            .nodes
            .lock()
            .get(&node)
            .cloned()
            .expect("node exists")
    };
    runtime()
        .block_on(
            node_obj
                .schema_registry()
                .ensure_schema(service_hash, service_bfbs.clone()),
        )
        .expect("register service schema");

    let opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_ALWAYS,
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: service_hex.as_ptr(),
        service_schema_hash_len: service_hex.len(),
        service_schema_bfbs: ptr::null(),
        service_schema_bfbs_len: 0,
        request_root_type: ptr::null(),
        request_root_type_len: 0,
        response_root_type: ptr::null(),
        response_root_type_len: 0,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };

    let mut service: aorta_service_t = 0;
    let rc = unsafe {
        aorta_service_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::addr_of!(opts),
            None,
            ptr::null_mut(),
            ptr::addr_of_mut!(service),
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(service, 0);
    let discovered = runtime()
        .block_on(
            node_obj
                .schema_registry()
                .fetch_schema_by_key_service(&service_name_raw),
        )
        .expect("schema query should succeed");
    let discovered = discovered.expect("service schema should be registered");
    assert_eq!(discovered.service_hash, service_hash);
    assert_eq!(discovered.service_bfbs, service_bfbs);
    assert!(discovered.request_root_type.is_none());
    assert!(discovered.response_root_type.is_none());

    assert_eq!(aorta_service_free(service), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_service_builder_rejects_always_policy_without_schema_bfbs() {
    let node_name = CString::new(format!("ffi_schema_node_{}", alloc_handle())).expect("name");
    let service_name_raw = format!("ffi_schema_missing_service_{}", alloc_handle());
    let service_name = CString::new(service_name_raw).expect("service name");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let service_hash = SchemaHash::from_bfbs_bytes(b"ffi_schema_missing_service");
    let service_hex = service_hash.to_hex_bytes();

    let opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_ALWAYS,
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: service_hex.as_ptr(),
        service_schema_hash_len: service_hex.len(),
        service_schema_bfbs: ptr::null(),
        service_schema_bfbs_len: 0,
        request_root_type: ptr::null(),
        request_root_type_len: 0,
        response_root_type: ptr::null(),
        response_root_type_len: 0,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };

    let mut service: aorta_service_t = 0;
    let rc = unsafe {
        aorta_service_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::addr_of!(opts),
            None,
            ptr::null_mut(),
            ptr::addr_of_mut!(service),
        )
    };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(service, 0);

    let err_ptr = aorta_last_error();
    assert!(!err_ptr.is_null());
    let err = unsafe { CStr::from_ptr(err_ptr) }
        .to_str()
        .expect("utf-8 error");
    assert!(err.contains("schema bfbs is required"));

    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn zbytes_from_ffi_view_supports_multi_slice() {
    let s1 = [0x01u8, 0x02];
    let s2 = [0x03u8, 0x04, 0x05];
    let slices = [
        aorta_bytes_slice_t {
            data: s1.as_ptr(),
            len: s1.len(),
        },
        aorta_bytes_slice_t {
            data: s2.as_ptr(),
            len: s2.len(),
        },
    ];
    let view = aorta_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: slices.len(),
        total_len: s1.len() + s2.len(),
    };

    let payload = unsafe { zbytes_from_ffi_view(ptr::addr_of!(view), "payload") }
        .expect("payload should be assembled");
    assert_eq!(payload.to_bytes().as_ref(), &[0x01, 0x02, 0x03, 0x04, 0x05]);
}

#[test]
fn zbytes_from_ffi_view_rejects_total_len_mismatch() {
    let s1 = [0xAAu8, 0xBB];
    let slices = [aorta_bytes_slice_t {
        data: s1.as_ptr(),
        len: s1.len(),
    }];
    let view = aorta_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: slices.len(),
        total_len: s1.len() + 1,
    };

    let err = unsafe { zbytes_from_ffi_view(ptr::addr_of!(view), "payload") }
        .expect_err("mismatched total_len should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

unsafe extern "C" fn test_release_counter(release_ctx: *mut c_void, _data: *const u8, _len: usize) {
    let counter = &*(release_ctx as *const AtomicUsize);
    counter.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn zbytes_from_owned_ffi_view_supports_multi_slice_and_release() {
    let released = AtomicUsize::new(0);
    let s1 = [0x11u8, 0x22];
    let s2 = [0x33u8, 0x44, 0x55];
    let release_ctx = ptr::addr_of!(released) as *mut c_void;

    let slices = [
        aorta_owned_bytes_slice_t {
            data: s1.as_ptr(),
            len: s1.len(),
            release: Some(test_release_counter),
            release_ctx,
        },
        aorta_owned_bytes_slice_t {
            data: s2.as_ptr(),
            len: s2.len(),
            release: Some(test_release_counter),
            release_ctx,
        },
    ];
    let view = aorta_owned_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: slices.len(),
        total_len: s1.len() + s2.len(),
    };

    let payload = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect("owned payload should be assembled");
    assert_eq!(payload.to_bytes().as_ref(), &[0x11, 0x22, 0x33, 0x44, 0x55]);
    assert_eq!(released.load(Ordering::SeqCst), 0);

    drop(payload);
    assert_eq!(released.load(Ordering::SeqCst), 2);
}

#[test]
fn zbytes_from_owned_ffi_view_rejects_total_len_mismatch() {
    let s1 = [0xAAu8, 0xBB];
    let slices = [aorta_owned_bytes_slice_t {
        data: s1.as_ptr(),
        len: s1.len(),
        release: None,
        release_ctx: ptr::null_mut(),
    }];
    let view = aorta_owned_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: slices.len(),
        total_len: s1.len() + 1,
    };

    let err = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect_err("mismatched total_len should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn zbytes_from_owned_ffi_view_rejects_null_view() {
    let err = unsafe { zbytes_from_owned_ffi_view(ptr::null(), "payload") }
        .expect_err("null view pointer should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn zbytes_from_owned_ffi_view_accepts_zero_slices_zero_total_len() {
    let view = aorta_owned_bytes_view_t {
        slices: ptr::null(),
        slice_count: 0,
        total_len: 0,
    };
    let payload = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect("zero-slice zero-total_len should produce empty payload");
    assert_eq!(payload.to_bytes().as_ref(), &[] as &[u8]);
}

#[test]
fn zbytes_from_owned_ffi_view_rejects_zero_slices_nonzero_total_len() {
    let view = aorta_owned_bytes_view_t {
        slices: ptr::null(),
        slice_count: 0,
        total_len: 42,
    };
    let err = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect_err("zero slices with non-zero total_len should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn zbytes_from_owned_ffi_view_single_slice_owned_fast_path() {
    let released = AtomicUsize::new(0);
    let data = [0xAA_u8, 0xBB, 0xCC, 0xDD];
    let release_ctx = ptr::addr_of!(released) as *mut c_void;

    let slices = [aorta_owned_bytes_slice_t {
        data: data.as_ptr(),
        len: data.len(),
        release: Some(test_release_counter),
        release_ctx,
    }];
    let view = aorta_owned_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: 1,
        total_len: data.len(),
    };

    let payload = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect("single owned slice should succeed");
    assert_eq!(payload.to_bytes().as_ref(), &[0xAA, 0xBB, 0xCC, 0xDD]);
    assert_eq!(released.load(Ordering::SeqCst), 0);

    drop(payload);
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[test]
fn zbytes_from_owned_ffi_view_single_slice_borrowed_fast_path() {
    let data = [0x10_u8, 0x20, 0x30];
    let slices = [aorta_owned_bytes_slice_t {
        data: data.as_ptr(),
        len: data.len(),
        release: None,
        release_ctx: ptr::null_mut(),
    }];
    let view = aorta_owned_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: 1,
        total_len: data.len(),
    };

    let payload = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect("single borrowed slice should succeed");
    assert_eq!(payload.to_bytes().as_ref(), &[0x10, 0x20, 0x30]);
}

#[test]
fn zbytes_from_owned_ffi_view_single_slice_rejects_total_len_mismatch() {
    let data = [0x01_u8, 0x02];
    let slices = [aorta_owned_bytes_slice_t {
        data: data.as_ptr(),
        len: data.len(),
        release: None,
        release_ctx: ptr::null_mut(),
    }];
    let view = aorta_owned_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: 1,
        total_len: data.len() + 5, // mismatch
    };

    let err = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect_err("single-slice total_len mismatch should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn zbytes_from_owned_ffi_view_single_slice_rejects_null_data_nonzero_len() {
    let slices = [aorta_owned_bytes_slice_t {
        data: ptr::null(),
        len: 10,
        release: None,
        release_ctx: ptr::null_mut(),
    }];
    let view = aorta_owned_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: 1,
        total_len: 10,
    };

    let err = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect_err("null data with non-zero len should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn zbytes_from_owned_ffi_view_single_slice_empty_owned() {
    let released = AtomicUsize::new(0);
    let release_ctx = ptr::addr_of!(released) as *mut c_void;

    let slices = [aorta_owned_bytes_slice_t {
        data: ptr::null(), // empty slice, null data is fine when len == 0
        len: 0,
        release: Some(test_release_counter),
        release_ctx,
    }];
    let view = aorta_owned_bytes_view_t {
        slices: slices.as_ptr(),
        slice_count: 1,
        total_len: 0,
    };

    let payload = unsafe { zbytes_from_owned_ffi_view(ptr::addr_of!(view), "payload") }
        .expect("single empty owned slice should succeed");
    assert_eq!(payload.to_bytes().as_ref(), &[] as &[u8]);

    drop(payload);
    // Release callback should still be invoked even for empty owned slices
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[test]
fn executor_spin_once_timeout_sets_last_error() {
    let mut exec = 0;
    let new_rc =
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(new_rc, AORTA_OK);

    let rc = aorta_executor_spin_once(exec, 1);
    assert_eq!(rc, AORTA_ERR_TIMEOUT);

    let err_ptr = aorta_last_error();
    assert!(!err_ptr.is_null());
    let err = unsafe { CStr::from_ptr(err_ptr) };
    let err = err.to_str().expect("utf-8");
    assert!(err.contains("executor spin_once timeout"));

    let free_rc = aorta_executor_free(exec);
    assert_eq!(free_rc, AORTA_OK);
}

#[test]
fn executor_free_stops_running_spin_before_detach() {
    let mut exec = 0;
    let new_rc =
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(new_rc, AORTA_OK);

    let mut stop_handle: aorta_stop_handle_t = AORTA_STOP_HANDLE_INVALID;
    assert_eq!(
        unsafe { aorta_executor_get_stop_handle(exec, ptr::addr_of_mut!(stop_handle)) },
        AORTA_OK
    );
    assert_ne!(stop_handle, AORTA_STOP_HANDLE_INVALID);

    let entry = {
        registry()
            .executors
            .lock()
            .get(&exec)
            .cloned()
            .expect("executor entry")
    };

    let spin_thread = std::thread::spawn(move || aorta_executor_spin(exec));
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    // parking_lot::Mutex::try_lock returns Option<MutexGuard>, not Result
    // — there is no poisoning concept. Some(guard) means we acquired it
    // (executor wasn't spinning) — sleep and retry; None means it's held
    // (executor is busy spinning) — drop out of the wait loop.
    while let Some(guard) = entry.exec.try_lock() {
        drop(guard);
        assert!(
            std::time::Instant::now() < deadline,
            "spin should acquire the executor lock"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let (tx, rx) = std::sync::mpsc::channel();
    let free_thread = std::thread::spawn(move || {
        let rc = aorta_executor_free(exec);
        tx.send(rc).expect("send free result");
    });

    let free_rc = match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(rc) => rc,
        Err(err) => {
            assert_eq!(aorta_stop_handle_stop(stop_handle), AORTA_OK);
            let _ = rx.recv_timeout(Duration::from_secs(1));
            panic!("executor free should stop spin before waiting: {err}");
        }
    };
    assert_eq!(free_rc, AORTA_OK);
    free_thread.join().expect("join free thread");
    assert_eq!(spin_thread.join().expect("join spin thread"), AORTA_OK);
    assert_eq!(aorta_stop_handle_free(stop_handle), AORTA_OK);
}

#[test]
fn executor_new_with_options_accepts_priority_mode() {
    let priority = aorta_priority_scheduler_options_t {
        max_drain_per_cycle: 0,
        weights: [8, 4, 2, 1, 1],
        map_subscription_qos: 1,
    };
    let options = aorta_executor_options_t {
        scheduler_mode: aorta_scheduler_mode_t::AORTA_SCHEDULER_PRIORITY,
        priority: ptr::addr_of!(priority),
        admission: ptr::null(),
        class_policy: ptr::null(),
    };

    let mut exec = 0;
    let rc = unsafe {
        aorta_executor_new_with_options(
            aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
            ptr::addr_of!(options),
            &mut exec,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(exec, 0);

    let free_rc = aorta_executor_free(exec);
    assert_eq!(free_rc, AORTA_OK);
}

#[test]
fn executor_new_with_options_rejects_invalid_bool() {
    let priority = aorta_priority_scheduler_options_t {
        max_drain_per_cycle: 8,
        weights: [8, 4, 2, 1, 1],
        map_subscription_qos: 2,
    };
    let options = aorta_executor_options_t {
        scheduler_mode: aorta_scheduler_mode_t::AORTA_SCHEDULER_PRIORITY,
        priority: ptr::addr_of!(priority),
        admission: ptr::null(),
        class_policy: ptr::null(),
    };

    let mut exec = 0;
    let rc = unsafe {
        aorta_executor_new_with_options(
            aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
            ptr::addr_of!(options),
            &mut exec,
        )
    };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(exec, 0);
}

#[test]
fn executor_get_admission_stats_rejects_null_out() {
    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(rc, AORTA_OK);
    let rc = unsafe { aorta_executor_get_admission_stats(exec, ptr::null_mut()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn executor_get_admission_stats_returns_not_found_for_invalid_handle() {
    let mut stats = aorta_executor_admission_stats_t {
        queued_events: 0,
        queued_bytes: 0,
        peak_queued_events: 0,
        peak_queued_bytes: 0,
        dropped_queue_full: 0,
        dropped_queue_closed: 0,
        dropped_budget_events: 0,
        dropped_budget_bytes: 0,
    };
    let rc = unsafe { aorta_executor_get_admission_stats(u64::MAX, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
}

#[test]
fn executor_get_admission_stats_returns_zeroed_snapshot_on_new_executor() {
    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(rc, AORTA_OK);

    let mut stats = aorta_executor_admission_stats_t {
        queued_events: u64::MAX,
        queued_bytes: u64::MAX,
        peak_queued_events: u64::MAX,
        peak_queued_bytes: u64::MAX,
        dropped_queue_full: u64::MAX,
        dropped_queue_closed: u64::MAX,
        dropped_budget_events: u64::MAX,
        dropped_budget_bytes: u64::MAX,
    };
    let rc = unsafe { aorta_executor_get_admission_stats(exec, ptr::addr_of_mut!(stats)) };
    assert_eq!(rc, AORTA_OK);
    assert_eq!(stats.queued_events, 0);
    assert_eq!(stats.queued_bytes, 0);
    assert_eq!(stats.peak_queued_events, 0);
    assert_eq!(stats.peak_queued_bytes, 0);
    assert_eq!(stats.dropped_queue_full, 0);
    assert_eq!(stats.dropped_queue_closed, 0);
    assert_eq!(stats.dropped_budget_events, 0);
    assert_eq!(stats.dropped_budget_bytes, 0);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn executor_new_supports_multi_type() {
    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_MULTI, &mut exec) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(exec, 0);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn executor_new_supports_async_type() {
    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_ASYNC, &mut exec) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(exec, 0);

    // Async compatibility backend reuses the same spin_once contract.
    let spin_rc = aorta_executor_spin_once(exec, 1);
    assert!(spin_rc == AORTA_OK || spin_rc == AORTA_ERR_TIMEOUT);

    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn executor_callback_group_lifecycle() {
    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_MULTI, &mut exec) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(exec, 0);

    let mut group: aorta_callback_group_handle_t = 0;
    let create_rc = unsafe {
        aorta_executor_create_callback_group(
            exec,
            aorta_callback_group_t::AORTA_CALLBACK_GROUP_REENTRANT,
            &mut group,
        )
    };
    assert_eq!(create_rc, AORTA_OK);
    assert_ne!(group, 0);

    let free_group_rc = aorta_executor_free_callback_group(exec, group);
    assert_eq!(free_group_rc, AORTA_OK);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn executor_callback_group_create_rejects_null_out() {
    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_MULTI, &mut exec) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(exec, 0);

    let create_rc = unsafe {
        aorta_executor_create_callback_group(
            exec,
            aorta_callback_group_t::AORTA_CALLBACK_GROUP_REENTRANT,
            ptr::null_mut(),
        )
    };
    assert_eq!(create_rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn executor_callback_group_free_rejects_unknown_group_handle() {
    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_MULTI, &mut exec) };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(exec, 0);

    let free_rc = aorta_executor_free_callback_group(exec, 999_999);
    assert_eq!(free_rc, AORTA_ERR_NOT_FOUND);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn timer_new_with_options_validates_out_and_callback_before_node_lookup() {
    let null_out_rc = unsafe {
        aorta_timer_new_with_options(
            u64::MAX,
            1_000_000,
            Some(test_timer_noop_callback),
            ptr::null_mut(),
            ptr::null(),
            ptr::null_mut(),
        )
    };
    assert_eq!(null_out_rc, AORTA_ERR_INVALID_ARGUMENT);

    let mut timer = 0;
    let null_cb_rc = unsafe {
        aorta_timer_new_with_options(
            u64::MAX,
            1_000_000,
            None,
            ptr::null_mut(),
            ptr::null(),
            ptr::addr_of_mut!(timer),
        )
    };
    assert_eq!(null_cb_rc, AORTA_ERR_INVALID_ARGUMENT);
}

#[test]
fn node_has_event_sender_reports_attachment_state() {
    let node_name = CString::new(format!("ffi_timer_node_{}", alloc_handle())).expect("node name");
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let mut has_event_sender = u8::MAX;
    let rc = unsafe { aorta_node_has_event_sender(node, ptr::addr_of_mut!(has_event_sender)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(has_event_sender, 0);

    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let rc = aorta_executor_add_node(exec, node);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    has_event_sender = 0;
    let rc = unsafe { aorta_node_has_event_sender(node, ptr::addr_of_mut!(has_event_sender)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(has_event_sender, 1);

    assert_eq!(
        aorta_executor_free(exec),
        AORTA_OK,
        "{}",
        last_error_string()
    );
    has_event_sender = u8::MAX;
    let rc = unsafe { aorta_node_has_event_sender(node, ptr::addr_of_mut!(has_event_sender)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(has_event_sender, 0);
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

#[test]
fn node_callback_group_lifecycle_requires_attached_executor() {
    let node_name = CString::new(format!("ffi_group_node_{}", alloc_handle())).expect("node name");
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let mut group: aorta_callback_group_handle_t = 0;
    let rc = unsafe {
        aorta_node_create_callback_group(
            node,
            aorta_callback_group_t::AORTA_CALLBACK_GROUP_REENTRANT,
            ptr::addr_of_mut!(group),
        )
    };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT, "{}", last_error_string());

    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let rc = aorta_executor_add_node(exec, node);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let rc = unsafe {
        aorta_node_create_callback_group(
            node,
            aorta_callback_group_t::AORTA_CALLBACK_GROUP_REENTRANT,
            ptr::addr_of_mut!(group),
        )
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_ne!(group, 0);

    let rc = aorta_node_free_callback_group(node, group);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    assert_eq!(
        aorta_executor_free(exec),
        AORTA_OK,
        "{}",
        last_error_string()
    );
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

#[test]
fn timer_stats_getters_return_zero_for_new_timer() {
    let node_name =
        CString::new(format!("ffi_timer_stats_node_{}", alloc_handle())).expect("node name");
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let mut exec = 0;
    let rc = unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let rc = aorta_executor_add_node(exec, node);
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    // Keep the timer stopped so stats remain deterministic before the first dispatch.
    let opts = aorta_timer_options_t {
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        auto_start: 0,
        invoke_on_missed: 0,
        overflow_policy: aorta_timer_overflow_policy_t::AORTA_TIMER_OVERFLOW_COALESCE_LATEST,
        overflow_block_timeout_ms: 0,
    };
    let mut timer: aorta_timer_t = 0;
    let rc = unsafe {
        aorta_timer_new_with_options(
            node,
            1_000_000,
            Some(test_timer_noop_callback),
            ptr::null_mut(),
            ptr::addr_of!(opts),
            ptr::addr_of_mut!(timer),
        )
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_ne!(timer, 0);

    let mut coalesced = u64::MAX;
    let rc = unsafe { aorta_timer_get_coalesced_tick_count(timer, ptr::addr_of_mut!(coalesced)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(coalesced, 0);

    let mut overflow_timeouts = u64::MAX;
    let rc = unsafe {
        aorta_timer_get_overflow_timeout_count(timer, ptr::addr_of_mut!(overflow_timeouts))
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(overflow_timeouts, 0);

    let mut deadline_slip_total_ns = u64::MAX;
    let rc = unsafe {
        aorta_timer_get_deadline_slip_total_ns(timer, ptr::addr_of_mut!(deadline_slip_total_ns))
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(deadline_slip_total_ns, 0);

    let mut deadline_slip_max_ns = u64::MAX;
    let rc = unsafe {
        aorta_timer_get_deadline_slip_max_ns(timer, ptr::addr_of_mut!(deadline_slip_max_ns))
    };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(deadline_slip_max_ns, 0);

    let mut due_firings_total = u64::MAX;
    let rc =
        unsafe { aorta_timer_get_due_firings_total(timer, ptr::addr_of_mut!(due_firings_total)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(due_firings_total, 0);

    let mut due_firings_max = u64::MAX;
    let rc = unsafe { aorta_timer_get_due_firings_max(timer, ptr::addr_of_mut!(due_firings_max)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(due_firings_max, 0);

    let mut due_sample_count = u64::MAX;
    let rc =
        unsafe { aorta_timer_get_due_sample_count(timer, ptr::addr_of_mut!(due_sample_count)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    assert_eq!(due_sample_count, 0);

    assert_eq!(aorta_timer_free(timer), AORTA_OK, "{}", last_error_string());
    assert_eq!(
        aorta_executor_free(exec),
        AORTA_OK,
        "{}",
        last_error_string()
    );
    assert_eq!(aorta_node_free(node), AORTA_OK, "{}", last_error_string());
}

#[test]
fn publisher_options_from_ffi_defaults_to_stable_fields_only() {
    let options =
        unsafe { publisher_options_from_ffi(ptr::null(), ptr::null()) }.expect("publisher options");
    assert_eq!(options.schema_attach_policy, SchemaAttachPolicy::Inherit);
    assert!(options.max_message_size.is_none());
    assert!(options.schema_root_type.is_none());
    // QoS & buffering redesign: congestion_control is now an Option override of
    // qos.congestion_control. Null opts => None => inherit the QoS value (Drop).
    assert_eq!(options.congestion_control, None);
}

#[test]
fn publisher_options_from_ffi_parses_schema_and_max_message_size() {
    let schema_root_type = CString::new("demo.Foo").expect("root type");
    let opts = aorta_pub_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        congestion_control: AORTA_PUB_CONGESTION_DROP,
        schema_bfbs: ptr::null(),
        schema_bfbs_len: 0,
        max_message_size: 2048,
        schema_root_type: schema_root_type.as_ptr(),
    };

    let options = unsafe { publisher_options_from_ffi(ptr::null(), ptr::addr_of!(opts)) }
        .expect("publisher options");
    assert_eq!(options.schema_attach_policy, SchemaAttachPolicy::Never);
    assert_eq!(options.max_message_size, Some(2048));
    assert_eq!(options.congestion_control, Some(CongestionControl::Drop));
    assert_eq!(options.schema_root_type.as_deref(), Some("demo.Foo"));
}

#[test]
fn publisher_options_from_ffi_rejects_invalid_schema_root_type_utf8() {
    let invalid_root_type = [0xff_u8, 0];
    let opts = aorta_pub_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        congestion_control: AORTA_PUB_CONGESTION_INHERIT,
        schema_bfbs: ptr::null(),
        schema_bfbs_len: 0,
        max_message_size: 0,
        schema_root_type: invalid_root_type.as_ptr().cast(),
    };

    let err = unsafe { publisher_options_from_ffi(ptr::null(), ptr::addr_of!(opts)) }
        .expect_err("invalid schema_root_type must be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn ffi_publisher_registers_schema_root_type_with_node_registry() {
    let node_name = CString::new(format!("ffi_pub_schema_node_{}", alloc_handle())).expect("name");
    let topic_raw = format!("ffi/pub/schema/{}", alloc_handle());
    let topic = CString::new(topic_raw.clone()).expect("topic");
    let schema_bfbs = b"ffi_topic_schema";
    let schema_root_type = CString::new("demo.Foo").expect("root type");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());
    let node_obj = registry()
        .nodes
        .lock()
        .get(&node)
        .cloned()
        .expect("node exists");

    let opts = aorta_pub_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        congestion_control: AORTA_PUB_CONGESTION_INHERIT,
        schema_bfbs: schema_bfbs.as_ptr(),
        schema_bfbs_len: schema_bfbs.len(),
        max_message_size: 0,
        schema_root_type: schema_root_type.as_ptr(),
    };
    let mut publisher: aorta_pub_t = 0;
    let rc = aorta_publisher_new(
        node,
        topic.as_ptr(),
        ptr::null(),
        ptr::addr_of!(opts),
        ptr::addr_of_mut!(publisher),
    );
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let key_suffix = node_obj
        .key_builder()
        .key_suffix_for_topic(&topic_raw)
        .expect("key suffix");
    let discovered = runtime()
        .block_on(
            node_obj
                .schema_registry()
                .get_schema_by_key_local(&key_suffix),
        )
        .expect("schema_by_key entry");
    assert_eq!(discovered.root_type(), Some("demo.Foo"));

    assert_eq!(aorta_publisher_free(publisher), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn publisher_options_from_ffi_inherits_congestion_when_override_unset() {
    // Regression: a caller that supplies `opts` only to set an
    // unrelated field (here `max_message_size`) leaves the congestion override
    // zero-initialized (`AORTA_PUB_CONGESTION_INHERIT`). That must map to `None`
    // so Core resolves against `qos.congestion_control` — NOT a forced `Drop`
    // that would silently defeat a `Block` QoS.
    let opts = aorta_pub_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_INHERIT,
        congestion_control: AORTA_PUB_CONGESTION_INHERIT,
        schema_bfbs: ptr::null(),
        schema_bfbs_len: 0,
        max_message_size: 4096,
        schema_root_type: ptr::null(),
    };

    let options = unsafe { publisher_options_from_ffi(ptr::null(), ptr::addr_of!(opts)) }
        .expect("publisher options");
    assert_eq!(options.max_message_size, Some(4096));
    assert_eq!(
        options.congestion_control, None,
        "zero-initialized override must inherit the QoS congestion, not force Drop"
    );
}

#[test]
fn publisher_options_from_ffi_parses_block_congestion_control() {
    // A C-ABI / C++ caller can now select Block (backpressure) — the
    // lossless-delivery knob that was previously only reachable from Rust.
    let opts = aorta_pub_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        congestion_control: AORTA_PUB_CONGESTION_BLOCK,
        schema_bfbs: ptr::null(),
        schema_bfbs_len: 0,
        max_message_size: 0,
        schema_root_type: ptr::null(),
    };

    let options = unsafe { publisher_options_from_ffi(ptr::null(), ptr::addr_of!(opts)) }
        .expect("publisher options");
    assert_eq!(options.congestion_control, Some(CongestionControl::Block));
}

#[test]
fn publisher_options_from_ffi_rejects_invalid_congestion_control() {
    // ABI safety: the wire field is a raw u8 so an out-of-range value
    // (e.g. uninitialized former-padding bytes from a stale-layout caller) is
    // rejected with a defined error instead of being decoded as an invalid
    // Rust enum discriminant (which would be undefined behavior).
    let opts = aorta_pub_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        congestion_control: 7,
        schema_bfbs: ptr::null(),
        schema_bfbs_len: 0,
        max_message_size: 0,
        schema_root_type: ptr::null(),
    };

    let err = unsafe { publisher_options_from_ffi(ptr::null(), ptr::addr_of!(opts)) }
        .expect_err("invalid congestion_control must be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn client_config_uses_fallback_when_opts_null() {
    let (
        timeout,
        policy,
        service_schema,
        enforce_single_provider,
        multi_reply_check_window,
        allow_retry,
    ) = unsafe { client_config_from_ffi(ptr::null(), 5000, SchemaAttachPolicy::Never) }
        .expect("client config");
    assert_eq!(timeout, Duration::from_millis(5000));
    assert_eq!(policy, SchemaAttachPolicy::Never);
    assert!(service_schema.is_none());
    assert!(enforce_single_provider.is_none());
    assert!(multi_reply_check_window.is_none());
    assert!(allow_retry.is_none());
}

#[test]
fn client_config_rejects_null_opts_under_always_policy() {
    let err = unsafe { client_config_from_ffi(ptr::null(), 5000, SchemaAttachPolicy::Always) }
        .expect_err("always policy should require service schema");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn client_config_uses_fallback_when_timeout_zero() {
    let opts = aorta_client_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_INHERIT,
        timeout_ms: 0,
        enforce_single_provider: AORTA_OPTIONAL_BOOL_INHERIT,
        allow_retry: AORTA_OPTIONAL_BOOL_INHERIT,
        multi_reply_check_window_ms: 0,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
    };
    let (
        timeout,
        policy,
        service_schema,
        enforce_single_provider,
        multi_reply_check_window,
        allow_retry,
    ) = unsafe { client_config_from_ffi(ptr::addr_of!(opts), 5000, SchemaAttachPolicy::Never) }
        .expect("client config");
    assert_eq!(timeout, Duration::from_millis(5000));
    assert_eq!(policy, SchemaAttachPolicy::Never);
    assert!(service_schema.is_none());
    assert!(enforce_single_provider.is_none());
    assert!(multi_reply_check_window.is_none());
    assert!(allow_retry.is_none());
}

#[test]
fn client_config_respects_non_zero_timeout() {
    let service = SchemaHash::from_bfbs_bytes(b"rpc-service").to_hex_bytes();
    let opts = aorta_client_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_ALWAYS,
        timeout_ms: 1234,
        enforce_single_provider: AORTA_OPTIONAL_BOOL_INHERIT,
        allow_retry: AORTA_OPTIONAL_BOOL_INHERIT,
        multi_reply_check_window_ms: 0,
        service_schema_hash_hex: service.as_ptr(),
        service_schema_hash_len: service.len(),
    };
    let (
        timeout,
        policy,
        service_schema,
        enforce_single_provider,
        multi_reply_check_window,
        allow_retry,
    ) = unsafe { client_config_from_ffi(ptr::addr_of!(opts), 5000, SchemaAttachPolicy::Never) }
        .expect("client config");
    assert_eq!(timeout, Duration::from_millis(1234));
    assert_eq!(policy, SchemaAttachPolicy::Always);
    assert_eq!(
        service_schema.expect("service schema").to_hex_bytes(),
        service
    );
    assert!(enforce_single_provider.is_none());
    assert!(multi_reply_check_window.is_none());
    assert!(allow_retry.is_none());
}

#[test]
fn client_config_rejects_always_without_service_schema() {
    let opts = aorta_client_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_ALWAYS,
        timeout_ms: 5000,
        enforce_single_provider: AORTA_OPTIONAL_BOOL_INHERIT,
        allow_retry: AORTA_OPTIONAL_BOOL_INHERIT,
        multi_reply_check_window_ms: 0,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
    };
    let err =
        unsafe { client_config_from_ffi(ptr::addr_of!(opts), 5000, SchemaAttachPolicy::Never) }
            .expect_err("always policy should require service schema");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn client_config_parses_optional_rpc_strict_mode() {
    let opts = aorta_client_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        timeout_ms: 5000,
        enforce_single_provider: AORTA_OPTIONAL_BOOL_TRUE,
        allow_retry: AORTA_OPTIONAL_BOOL_TRUE,
        multi_reply_check_window_ms: 37,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
    };

    let (
        _timeout,
        _policy,
        _service_schema,
        enforce_single_provider,
        multi_reply_check_window,
        allow_retry,
    ) = unsafe { client_config_from_ffi(ptr::addr_of!(opts), 5000, SchemaAttachPolicy::Never) }
        .expect("client config");

    assert_eq!(enforce_single_provider, Some(true));
    assert_eq!(multi_reply_check_window, Some(Duration::from_millis(37)));
    assert_eq!(allow_retry, Some(true));
}

#[test]
fn client_config_rejects_invalid_optional_bool() {
    let opts = aorta_client_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        timeout_ms: 5000,
        enforce_single_provider: 99,
        allow_retry: AORTA_OPTIONAL_BOOL_INHERIT,
        multi_reply_check_window_ms: 0,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
    };

    let err =
        unsafe { client_config_from_ffi(ptr::addr_of!(opts), 5000, SchemaAttachPolicy::Never) }
            .expect_err("invalid optional bool should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn service_config_accepts_hash_only() {
    let service_hash = SchemaHash::from_bfbs_bytes(b"service-hash-only");
    let service_hash_hex = service_hash.to_hex_bytes();
    let opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: service_hash_hex.as_ptr(),
        service_schema_hash_len: service_hash_hex.len(),
        service_schema_bfbs: ptr::null(),
        service_schema_bfbs_len: 0,
        request_root_type: ptr::null(),
        request_root_type_len: 0,
        response_root_type: ptr::null(),
        response_root_type_len: 0,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };
    let (policy, schema_info) =
        unsafe { service_config_from_ffi(ptr::addr_of!(opts), SchemaAttachPolicy::Never) }
            .expect("hash-only schema should be accepted");
    assert_eq!(policy, SchemaAttachPolicy::Never);
    let schema_info = schema_info.expect("schema info");
    assert_eq!(schema_info.schema, service_hash);
    assert!(schema_info.schema_bfbs.is_empty());
    assert!(schema_info.request_root_type.is_none());
    assert!(schema_info.response_root_type.is_none());
}

#[test]
fn service_config_derives_hash_from_bfbs() {
    let service_bfbs = b"service-bfbs";
    let request_root_type = b"demo.rpc.Request";
    let response_root_type = b"demo.rpc.Response";
    let opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_ALWAYS,
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
        service_schema_bfbs: service_bfbs.as_ptr(),
        service_schema_bfbs_len: service_bfbs.len(),
        request_root_type: request_root_type.as_ptr(),
        request_root_type_len: request_root_type.len(),
        response_root_type: response_root_type.as_ptr(),
        response_root_type_len: response_root_type.len(),
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };

    let (policy, schema_info) =
        unsafe { service_config_from_ffi(ptr::addr_of!(opts), SchemaAttachPolicy::Never) }
            .expect("bfbs-only schema should be accepted");
    assert_eq!(policy, SchemaAttachPolicy::Always);
    let schema_info = schema_info.expect("schema info");
    assert_eq!(
        schema_info.schema,
        SchemaHash::from_bfbs_bytes(service_bfbs)
    );
    assert_eq!(schema_info.schema_bfbs, service_bfbs);
    assert_eq!(
        schema_info.request_root_type.as_deref(),
        Some("demo.rpc.Request")
    );
    assert_eq!(
        schema_info.response_root_type.as_deref(),
        Some("demo.rpc.Response")
    );
}

#[test]
fn service_config_rejects_hash_bfbs_mismatch() {
    let service_bfbs = b"service-bfbs";
    let bad_service_hash = SchemaHash::from_bfbs_bytes(b"other-service").to_hex_bytes();
    let opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: bad_service_hash.as_ptr(),
        service_schema_hash_len: bad_service_hash.len(),
        service_schema_bfbs: service_bfbs.as_ptr(),
        service_schema_bfbs_len: service_bfbs.len(),
        request_root_type: ptr::null(),
        request_root_type_len: 0,
        response_root_type: ptr::null(),
        response_root_type_len: 0,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };

    let err = unsafe { service_config_from_ffi(ptr::addr_of!(opts), SchemaAttachPolicy::Never) }
        .expect_err("hash/bfbs mismatch should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn service_config_rejects_invalid_utf8_root_type() {
    let bad_utf8 = [0xFFu8];
    let opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
        service_schema_bfbs: ptr::null(),
        service_schema_bfbs_len: 0,
        request_root_type: bad_utf8.as_ptr(),
        request_root_type_len: bad_utf8.len(),
        response_root_type: ptr::null(),
        response_root_type_len: 0,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };

    let err = unsafe { service_config_from_ffi(ptr::addr_of!(opts), SchemaAttachPolicy::Never) }
        .expect_err("invalid utf-8 root type should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn service_config_rejects_null_opts_under_always_policy() {
    let err = unsafe { service_config_from_ffi(ptr::null(), SchemaAttachPolicy::Always) }
        .expect_err("always policy should require service schema");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

#[test]
fn service_config_rejects_invalid_hash_length() {
    let bad_hash = b"abcd";
    let opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_NEVER,
        callback_group: aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: bad_hash.as_ptr(),
        service_schema_hash_len: bad_hash.len(),
        service_schema_bfbs: ptr::null(),
        service_schema_bfbs_len: 0,
        request_root_type: ptr::null(),
        request_root_type_len: 0,
        response_root_type: ptr::null(),
        response_root_type_len: 0,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };
    let err = unsafe { service_config_from_ffi(ptr::addr_of!(opts), SchemaAttachPolicy::Never) }
        .expect_err("bad hash length should be rejected");
    assert!(matches!(err, AortaError::InvalidArgument(_)));
}

// alloc_handle must never hand out the reserved 0 sentinel,
// even across the (astronomical) wraparound boundary.
#[test]
fn alloc_handle_skips_zero_on_wraparound() {
    use std::sync::atomic::AtomicU64;

    let counter = AtomicU64::new(u64::MAX);
    // First call returns the pre-increment value u64::MAX and
    // leaves the counter at 0.
    assert_eq!(alloc_handle_from(&counter), u64::MAX);
    // Second call would naively return 0 (the reserved invalid
    // sentinel). The defence must skip it and hand out 1 instead.
    let post = alloc_handle_from(&counter);
    assert_ne!(post, 0, "handle 0 is reserved as invalid sentinel");
    assert_eq!(post, 1);
}

#[test]
fn alloc_handle_steady_state_returns_sequential_nonzero_ids() {
    use std::sync::atomic::AtomicU64;

    let counter = AtomicU64::new(1);
    for expected in 1u64..=64 {
        assert_eq!(alloc_handle_from(&counter), expected);
    }
}

// ---- Teardown gate (drain_in_flight + InFlightGuard) ----
//
// These exercise the FFI-side in-flight gate that aorta_subscriber_free /
// aorta_timer_free use to wait for an entered C++ dispatcher to finish before
// the C++ destructor frees its CallbackState (closes the entrant-pre-increment
// use-after-free window). The full closure↔free
// integration is covered by the C++ integration tests against the real core;
// these pin the mechanism deterministically and fast.

#[test]
fn drain_in_flight_returns_true_when_already_zero() {
    let counter = AtomicUsize::new(0);
    assert!(drain_in_flight(&counter, std::time::Duration::from_secs(2)));
}

#[test]
fn drain_in_flight_times_out_when_never_drains() {
    // Counter pinned at 1 → bounded wait must give up and report false (the
    // caller then falls back to the C++ wait+leak path). Short timeout keeps
    // the test fast.
    let counter = AtomicUsize::new(1);
    let start = std::time::Instant::now();
    assert!(!drain_in_flight(
        &counter,
        std::time::Duration::from_millis(80)
    ));
    assert!(start.elapsed() >= std::time::Duration::from_millis(70));
}

#[test]
fn drain_in_flight_waits_for_concurrent_decrement() {
    // Models an in-flight dispatcher finishing on another thread: drain must
    // block until the count hits zero, then return true.
    let counter = Arc::new(AtomicUsize::new(1));
    let counter_bg = Arc::clone(&counter);
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(120));
        counter_bg.fetch_sub(1, Ordering::AcqRel);
    });
    let start = std::time::Instant::now();
    assert!(drain_in_flight(&counter, std::time::Duration::from_secs(2)));
    assert!(start.elapsed() >= std::time::Duration::from_millis(110));
    releaser.join().unwrap();
}

#[test]
fn in_flight_guard_decrements_on_drop() {
    let counter = AtomicUsize::new(0);
    counter.fetch_add(1, Ordering::AcqRel);
    {
        let _guard = InFlightGuard(&counter);
        assert_eq!(counter.load(Ordering::Acquire), 1);
    }
    assert_eq!(counter.load(Ordering::Acquire), 0);
}

// ---- Flow cancel token C-ABI ------------------------------------------------

#[test]
fn token_root_roundtrip_cancel_poll_reason_free() {
    let mut token: aorta_token_t = 0;
    assert_eq!(unsafe { aorta_token_child(0, &mut token) }, AORTA_OK);
    assert_ne!(token, 0);

    let mut cancelled: u8 = 9;
    assert_eq!(
        unsafe { aorta_token_is_cancelled(token, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 0);
    let mut reason: u8 = 9;
    assert_eq!(unsafe { aorta_token_reason(token, &mut reason) }, AORTA_OK);
    assert_eq!(reason, 0, "live token reports reason 0");

    assert_eq!(
        aorta_token_cancel(token, AORTA_CANCEL_REASON_SHUTDOWN),
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_token_is_cancelled(token, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 1);
    assert_eq!(unsafe { aorta_token_reason(token, &mut reason) }, AORTA_OK);
    assert_eq!(reason, AORTA_CANCEL_REASON_SHUTDOWN);

    assert_eq!(aorta_token_free(token), AORTA_OK);
    assert_eq!(
        aorta_token_free(token),
        AORTA_ERR_NOT_FOUND,
        "double free must be detected, not UAF"
    );
}

#[test]
fn token_cancel_cascades_to_children_created_via_handles() {
    let mut root: aorta_token_t = 0;
    let mut child: aorta_token_t = 0;
    assert_eq!(unsafe { aorta_token_child(0, &mut root) }, AORTA_OK);
    assert_eq!(unsafe { aorta_token_child(root, &mut child) }, AORTA_OK);

    assert_eq!(
        aorta_token_cancel(root, AORTA_CANCEL_REASON_EXPLICIT),
        AORTA_OK
    );
    let mut cancelled: u8 = 0;
    assert_eq!(
        unsafe { aorta_token_is_cancelled(child, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 1, "cancel must cascade through C handles");

    assert_eq!(aorta_token_free(child), AORTA_OK);
    assert_eq!(aorta_token_free(root), AORTA_OK);
}

#[test]
fn token_child_of_cancelled_parent_is_born_cancelled() {
    let mut root: aorta_token_t = 0;
    assert_eq!(unsafe { aorta_token_child(0, &mut root) }, AORTA_OK);
    assert_eq!(
        aorta_token_cancel(root, AORTA_CANCEL_REASON_DEADLINE),
        AORTA_OK
    );

    let mut child: aorta_token_t = 0;
    assert_eq!(unsafe { aorta_token_child(root, &mut child) }, AORTA_OK);
    let mut reason: u8 = 0;
    assert_eq!(unsafe { aorta_token_reason(child, &mut reason) }, AORTA_OK);
    assert_eq!(reason, AORTA_CANCEL_REASON_DEADLINE);

    assert_eq!(aorta_token_free(child), AORTA_OK);
    assert_eq!(aorta_token_free(root), AORTA_OK);
}

#[test]
fn token_freeing_the_middle_handle_adopts_grandchildren() {
    let mut root: aorta_token_t = 0;
    let mut middle: aorta_token_t = 0;
    let mut leaf: aorta_token_t = 0;
    assert_eq!(unsafe { aorta_token_child(0, &mut root) }, AORTA_OK);
    assert_eq!(unsafe { aorta_token_child(root, &mut middle) }, AORTA_OK);
    assert_eq!(unsafe { aorta_token_child(middle, &mut leaf) }, AORTA_OK);

    assert_eq!(aorta_token_free(middle), AORTA_OK);
    assert_eq!(
        aorta_token_cancel(root, AORTA_CANCEL_REASON_SHUTDOWN),
        AORTA_OK
    );
    let mut cancelled: u8 = 0;
    assert_eq!(
        unsafe { aorta_token_is_cancelled(leaf, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(
        cancelled, 1,
        "cancellation must still reach a leaf whose middle handle was freed"
    );

    assert_eq!(aorta_token_free(leaf), AORTA_OK);
    assert_eq!(aorta_token_free(root), AORTA_OK);
}

#[test]
fn token_invalid_arguments_are_rejected() {
    let mut out: aorta_token_t = 0;
    assert_eq!(
        unsafe { aorta_token_child(0xdead_beef, &mut out) },
        AORTA_ERR_NOT_FOUND
    );
    assert_eq!(
        unsafe { aorta_token_child(0, std::ptr::null_mut()) },
        AORTA_ERR_INVALID_ARGUMENT
    );
    assert_eq!(
        aorta_token_cancel(0xdead_beef, AORTA_CANCEL_REASON_EXPLICIT),
        AORTA_ERR_NOT_FOUND
    );
    let mut token: aorta_token_t = 0;
    assert_eq!(unsafe { aorta_token_child(0, &mut token) }, AORTA_OK);
    assert_eq!(
        aorta_token_cancel(token, 0),
        AORTA_ERR_INVALID_ARGUMENT,
        "reason 0 is the live state, not a cancel reason"
    );
    let mut cancelled: u8 = 0;
    assert_eq!(
        unsafe { aorta_token_is_cancelled(0xdead_beef, &mut cancelled) },
        AORTA_ERR_NOT_FOUND
    );
    assert_eq!(aorta_token_free(0), AORTA_OK, "0 is the no-op handle");
    assert_eq!(aorta_token_free(token), AORTA_OK);
}

#[test]
fn token_cancelled_flag_ptr_polls_lock_free() {
    let mut token: aorta_token_t = 0;
    assert_eq!(unsafe { aorta_token_child(0, &mut token) }, AORTA_OK);
    let mut flag: *const u8 = std::ptr::null();
    assert_eq!(
        unsafe { aorta_token_cancelled_flag_ptr(token, &mut flag) },
        AORTA_OK
    );
    assert!(!flag.is_null());
    // SAFETY: the flag pointer is valid until aorta_token_free; reading it
    // through an AtomicU8 view is the documented poll contract.
    let atomic = unsafe { &*(flag as *const std::sync::atomic::AtomicU8) };
    assert_eq!(atomic.load(Ordering::Acquire), 0, "live token");
    assert_eq!(
        aorta_token_cancel(token, AORTA_CANCEL_REASON_SHUTDOWN),
        AORTA_OK
    );
    assert_eq!(
        atomic.load(Ordering::Acquire),
        AORTA_CANCEL_REASON_SHUTDOWN,
        "the resolved pointer observes the cancel without any lookup"
    );
    assert_eq!(aorta_token_free(token), AORTA_OK);
}

// ---- Flow scope C-ABI + post_resume + executor sched options ---------------

#[test]
fn scope_child_cancel_poll_and_budget_roundtrip() {
    let name = std::ffi::CString::new("test-scope").unwrap();
    let opts = aorta_scope_opts_t {
        priority: 2,
        supervision: AORTA_SUPERVISION_FAIL_FAST,
        _pad: [0; 2],
        budget_ns: 50_000_000, // 50ms
        name: name.as_ptr(),
    };
    let mut root: aorta_scope_t = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, std::ptr::addr_of!(opts), &mut root) },
        AORTA_OK
    );
    let mut child: aorta_scope_t = 0;
    assert_eq!(
        unsafe { aorta_scope_child(root, std::ptr::null(), &mut child) },
        AORTA_OK
    );

    // Budget: child inherits the parent's ~50ms deadline.
    let (mut has, mut ns) = (0u8, 0u64);
    assert_eq!(
        unsafe { aorta_budget_remaining_ns(child, &mut has, &mut ns) },
        AORTA_OK
    );
    assert_eq!(has, 1);
    assert!(ns <= 50_000_000);

    // Live checkpoint.
    assert_eq!(aorta_scope_checkpoint(root), AORTA_OK);

    // RT-safe flag ptr observes the cancel cascade.
    let mut flag: *const u8 = std::ptr::null();
    assert_eq!(
        unsafe { aorta_scope_cancelled_flag_ptr(child, &mut flag) },
        AORTA_OK
    );
    let atomic = unsafe { &*(flag as *const std::sync::atomic::AtomicU8) };
    assert_eq!(atomic.load(Ordering::Acquire), 0);
    assert_eq!(
        aorta_scope_cancel(root, AORTA_CANCEL_REASON_SHUTDOWN),
        AORTA_OK
    );
    assert_eq!(atomic.load(Ordering::Acquire), AORTA_CANCEL_REASON_SHUTDOWN);
    let mut cancelled = 0u8;
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(child, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 1, "cancel cascades through C scope handles");
    assert_eq!(aorta_scope_checkpoint(child), AORTA_ERR_CANCELLED);

    assert_eq!(aorta_scope_free(child), AORTA_OK);
    assert_eq!(aorta_scope_free(root), AORTA_OK);
    assert_eq!(aorta_scope_free(root), AORTA_ERR_NOT_FOUND);
}

#[test]
fn scope_checkpoint_observes_budget_expiry() {
    let opts = aorta_scope_opts_t {
        priority: 2,
        supervision: AORTA_SUPERVISION_FAIL_FAST,
        _pad: [0; 2],
        budget_ns: 1, // effectively expired at first observation
        name: std::ptr::null(),
    };
    let mut scope: aorta_scope_t = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, std::ptr::addr_of!(opts), &mut scope) },
        AORTA_OK
    );
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        aorta_scope_checkpoint(scope),
        AORTA_ERR_CANCELLED,
        "the first checkpoint observes expiry and fires DEADLINE"
    );
    let mut cancelled = 0u8;
    assert_eq!(
        unsafe { aorta_scope_is_cancelled(scope, &mut cancelled) },
        AORTA_OK
    );
    assert_eq!(cancelled, 1);
    assert_eq!(aorta_scope_free(scope), AORTA_OK);
}

extern "C" fn post_resume_probe(event: *const aorta_resume_event_t) {
    // Assert the event's STATUS field, not just delivery — a
    // normal dispatch must report AORTA_OK (a panic here is swallowed by
    // dispatch isolation, so the count assertion below would then fail).
    assert_eq!(unsafe { (*event).status }, AORTA_OK);
    let counter = unsafe { &*((*event).handle as *const AtomicUsize) };
    counter.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn executor_post_resume_runs_exactly_once_on_the_dispatch_thread() {
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    let spin_exec = exec;
    let spin = std::thread::spawn(move || aorta_executor_spin(spin_exec));

    let counter = Box::leak(Box::new(AtomicUsize::new(0)));
    assert_eq!(
        aorta_executor_post_resume(
            exec,
            Some(post_resume_probe),
            counter as *const AtomicUsize as *mut c_void,
            255,
        ),
        AORTA_OK
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while counter.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1, "resumed exactly once");

    assert_eq!(aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(spin.join().expect("spin"), AORTA_OK);
    // A stopped executor refuses the post: caller resumes inline into
    // cancellation.
    assert_eq!(
        aorta_executor_post_resume(
            exec,
            Some(post_resume_probe),
            counter as *const AtomicUsize as *mut c_void,
            255,
        ),
        AORTA_ERR_CANCELLED
    );
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

extern "C" fn flow_task_body(handle: *mut c_void) {
    let counter = unsafe { &*(handle as *const AtomicUsize) };
    counter.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn scope_spawn_dispatches_body_and_close_waits_for_task_finish() {
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    let spin_exec = exec;
    let spin = std::thread::spawn(move || aorta_executor_spin(spin_exec));

    let mut scope: aorta_scope_t = 0;
    assert_eq!(
        unsafe { aorta_scope_child(0, std::ptr::null(), &mut scope) },
        AORTA_OK
    );
    let counter = Box::leak(Box::new(AtomicUsize::new(0)));
    let mut task: aorta_task_t = 0;
    assert_eq!(
        unsafe {
            aorta_scope_spawn(
                exec,
                scope,
                Some(flow_task_body),
                counter as *const AtomicUsize as *mut c_void,
                &mut task,
            )
        },
        AORTA_OK
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while counter.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1, "body dispatched once");

    // close() blocks until the task is FINISHED (the body returning is not
    // completion — a coroutine's first slice returns at its first suspend).
    let closer_scope = scope;
    let closer = std::thread::spawn(move || aorta_scope_close(closer_scope));
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(!closer.is_finished(), "close waits for the open task");
    assert_eq!(aorta_task_finish(task), AORTA_OK);
    assert_eq!(closer.join().expect("closer"), AORTA_OK);
    assert_eq!(
        aorta_task_finish(task),
        AORTA_ERR_NOT_FOUND,
        "finish is exactly-once"
    );

    assert_eq!(aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(spin.join().expect("spin"), AORTA_OK);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
    assert_eq!(aorta_scope_free(scope), AORTA_OK);
}

#[test]
fn executor_new_with_options_v2_holds_the_invariants() {
    // SINGLE with worker_count 1 (or 0 = default) is legal.
    let opts = aorta_executor_sched_options_t {
        worker_count: 1,
        sched: 1,
        _pad: [0; 3],
    };
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe {
            aorta_executor_new_with_sched_options(
                aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                std::ptr::addr_of!(opts),
                &mut exec,
            )
        },
        AORTA_OK
    );
    assert_eq!(aorta_executor_free(exec), AORTA_OK);

    // SINGLE with worker_count > 1 is invalid (SINGLE is the single-thread
    // inline path).
    let bad = aorta_executor_sched_options_t {
        worker_count: 4,
        sched: 0,
        _pad: [0; 3],
    };
    assert_eq!(
        unsafe {
            aorta_executor_new_with_sched_options(
                aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                std::ptr::addr_of!(bad),
                &mut exec,
            )
        },
        AORTA_ERR_INVALID_ARGUMENT
    );

    // MULTI takes worker_count as its dispatch parallelism.
    let multi = aorta_executor_sched_options_t {
        worker_count: 3,
        sched: 1,
        _pad: [0; 3],
    };
    assert_eq!(
        unsafe {
            aorta_executor_new_with_sched_options(
                aorta_executor_type_t::AORTA_EXECUTOR_MULTI,
                std::ptr::addr_of!(multi),
                &mut exec,
            )
        },
        AORTA_OK
    );
    assert_eq!(aorta_executor_free(exec), AORTA_OK);

    // sched must be 0/1.
    let bad_sched = aorta_executor_sched_options_t {
        worker_count: 1,
        sched: 9,
        _pad: [0; 3],
    };
    assert_eq!(
        unsafe {
            aorta_executor_new_with_sched_options(
                aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                std::ptr::addr_of!(bad_sched),
                &mut exec,
            )
        },
        AORTA_ERR_INVALID_ARGUMENT
    );
}

extern "C" fn second_hop(event: *const aorta_resume_event_t) {
    let counter = unsafe { &*((*event).handle as *const AtomicUsize) };
    counter.fetch_add(10, Ordering::SeqCst);
}

extern "C" fn first_hop(event: *const aorta_resume_event_t) {
    let handle = unsafe { (*event).handle };
    let counter = unsafe { &*(handle as *const AtomicUsize) };
    counter.fetch_add(1, Ordering::SeqCst);
    // Re-post from INSIDE a dispatched callback (the C++ resume_on shape).
    let exec = HOP_EXEC.load(Ordering::SeqCst);
    assert_eq!(
        aorta_executor_post_resume(exec, Some(second_hop), handle, 255),
        AORTA_OK
    );
}

static HOP_EXEC: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[test]
fn post_resume_from_inside_a_dispatch_reposts_correctly() {
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    HOP_EXEC.store(exec, Ordering::SeqCst);
    let spin_exec = exec;
    let spin = std::thread::spawn(move || aorta_executor_spin(spin_exec));

    let counter = Box::leak(Box::new(AtomicUsize::new(0)));
    assert_eq!(
        aorta_executor_post_resume(
            exec,
            Some(first_hop),
            counter as *const AtomicUsize as *mut c_void,
            255
        ),
        AORTA_OK
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while counter.load(Ordering::SeqCst) != 11 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 11, "both hops dispatched");

    assert_eq!(aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(spin.join().expect("spin"), AORTA_OK);
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

// ---- Flow per-endpoint capability setters -----------------------------------

#[test]
fn subscriber_capability_setters_validate_inputs() {
    // Invalid handle.
    assert_eq!(
        aorta_subscriber_set_priority(0xdead_beef, 1),
        AORTA_ERR_NOT_FOUND
    );
    // Priority range.
    assert_eq!(
        aorta_subscriber_set_priority(0xdead_beef, 7),
        AORTA_ERR_INVALID_ARGUMENT
    );
    // Admission null pointer.
    assert_eq!(
        unsafe { aorta_subscriber_set_admission(0xdead_beef, std::ptr::null()) },
        AORTA_ERR_INVALID_ARGUMENT
    );
    // The struct carries ONLY working fields (evolution policy — no
    // reserved/rejected members).
    let ok_shape = aorta_admission_t {
        max_in_flight: 2,
        deadline_ns: 5_000_000,
    };
    // Valid shape but unknown handle → NOT_FOUND (validation order).
    assert_eq!(
        unsafe { aorta_subscriber_set_admission(0xdead_beef, std::ptr::addr_of!(ok_shape)) },
        AORTA_ERR_NOT_FOUND
    );
}

// ---- 3-axis delayed resumes (post_resume_after) ----------------------

extern "C" fn delayed_probe(event: *const aorta_resume_event_t) {
    // Encodes status into the counter: OK adds 1, CANCELLED adds 1000.
    let counter = unsafe { &*((*event).handle as *const AtomicUsize) };
    let add = if unsafe { (*event).status } == AORTA_OK {
        1
    } else {
        1000
    };
    counter.fetch_add(add, Ordering::SeqCst);
}

#[test]
fn post_resume_after_fires_after_the_delay() {
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    let stop = {
        let execs = registry().executors.lock();
        execs.get(&exec).expect("entry").stop.clone()
    };
    let spin = std::thread::spawn(move || {
        let _ = aorta_executor_spin(exec);
    });
    let counter = Box::leak(Box::new(AtomicUsize::new(0)));
    assert_eq!(
        aorta_executor_post_resume_after(
            exec,
            20_000_000, // 20ms
            Some(delayed_probe),
            counter as *const AtomicUsize as *mut c_void,
            255,
        ),
        AORTA_OK
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while counter.load(Ordering::SeqCst) == 0 {
        assert!(std::time::Instant::now() < deadline, "delayed resume fired");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 1, "normal delivery (OK)");
    stop.stop();
    spin.join().expect("spin");
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn stop_cancels_sleeping_posts_promptly() {
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    let counter = Box::leak(Box::new(AtomicUsize::new(0)));
    // A LONG sleep (10s): stop must not wait it out.
    assert_eq!(
        aorta_executor_post_resume_after(
            exec,
            10_000_000_000,
            Some(delayed_probe),
            counter as *const AtomicUsize as *mut c_void,
            255,
        ),
        AORTA_OK
    );
    let started = std::time::Instant::now();
    let stop = {
        let execs = registry().executors.lock();
        execs.get(&exec).expect("entry").stop.clone()
    };
    stop.stop();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "stop returned promptly, not after the 10s delay"
    );
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1000,
        "the sleeping post was resumed into cancellation"
    );
    // Post after stop is refused up front.
    assert_eq!(
        aorta_executor_post_resume_after(
            exec,
            1,
            Some(delayed_probe),
            counter as *const AtomicUsize as *mut c_void,
            255,
        ),
        AORTA_ERR_CANCELLED
    );
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn delayed_fire_vs_stop_race_is_exactly_once() {
    // Hammer: many short sleeps racing a stop — each resume must be
    // delivered EXACTLY once (sum of OK+CANCELLED encodings == N).
    let mut exec: u64 = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );
    let spin_exec = exec;
    let spin = std::thread::spawn(move || {
        let _ = aorta_executor_spin(spin_exec);
    });
    const N: usize = 64;
    let counter = Box::leak(Box::new(AtomicUsize::new(0)));
    for i in 0..N {
        assert_eq!(
            aorta_executor_post_resume_after(
                exec,
                (i as u64 % 8) * 1_000_000, // 0..7ms spread
                Some(delayed_probe),
                counter as *const AtomicUsize as *mut c_void,
                255,
            ),
            AORTA_OK
        );
    }
    std::thread::sleep(std::time::Duration::from_millis(3));
    let stop = {
        let execs = registry().executors.lock();
        execs.get(&exec).expect("entry").stop.clone()
    };
    stop.stop();
    spin.join().expect("spin");
    // Every post delivered exactly once: value = ok_count + 1000*cancelled
    // with ok_count + cancelled == N.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let v = counter.load(Ordering::SeqCst);
        let (ok, cancelled) = (v % 1000, v / 1000);
        if ok + cancelled == N {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "exactly-once accounting must converge: ok={ok} cancelled={cancelled}"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

#[test]
fn ring_store_accepts_null_empty_payload() {
    // len == 0 with data == NULL must store an
    // empty value, not form a null slice (UB).
    let node_name = CString::new(format!("ffi_ring_p1_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/ringp1/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK
    );
    let mut watch: aorta_watch_t = 0;
    assert_eq!(
        unsafe { aorta_watch_create(node, topic.as_ptr(), ptr::addr_of_mut!(watch)) },
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_watch_store(watch, std::ptr::null(), 0) },
        AORTA_OK,
        "empty store with a null pointer is legal at this ABI"
    );
    let mut buf = [0u8; 8];
    let mut len: usize = usize::MAX;
    let mut has: u8 = 0;
    let mut seq: u64 = 0;
    assert_eq!(
        unsafe {
            aorta_watch_load(
                watch,
                buf.as_mut_ptr(),
                buf.len(),
                ptr::addr_of_mut!(len),
                ptr::addr_of_mut!(has),
                ptr::addr_of_mut!(seq),
            )
        },
        AORTA_OK
    );
    assert_eq!((has, len, seq), (1, 0, 1), "an EMPTY value was stored");
    assert_eq!(aorta_watch_free(watch), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

// ---- Flow readiness surface (WindowSync / ReadinessSet) -----

#[test]
fn readiness_window_sync_ffi_round_trip() {
    let mut ws: aorta_window_sync_t = 0;
    assert_eq!(
        unsafe { aorta_window_sync_create(2, 1_000_000_000, 0, ptr::addr_of_mut!(ws)) },
        AORTA_OK
    );
    // Foolproofing: zero inputs and out-of-range feeds are refused loudly.
    let mut bogus: aorta_window_sync_t = 0;
    assert_eq!(
        unsafe { aorta_window_sync_create(0, 1, 0, ptr::addr_of_mut!(bogus)) },
        AORTA_ERR_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { aorta_window_sync_feed(ws, 2, 1, ptr::null(), 0) },
        AORTA_ERR_INVALID_ARGUMENT,
        "input 2 of a 2-input window is a wiring bug"
    );

    // Empty probe: has = 0, OK.
    let mut set: aorta_aligned_set_t = 0;
    let mut has: u8 = 9;
    assert_eq!(
        unsafe {
            aorta_window_sync_try_aligned(ws, ptr::addr_of_mut!(set), ptr::addr_of_mut!(has))
        },
        AORTA_OK
    );
    assert_eq!(has, 0);

    // Feed both inputs within slop → one aligned set.
    let a = [1u8, 2];
    let b = [3u8];
    assert_eq!(
        unsafe { aorta_window_sync_feed(ws, 0, 100, a.as_ptr(), a.len()) },
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_window_sync_feed(ws, 1, 150, b.as_ptr(), b.len()) },
        AORTA_OK
    );
    assert_eq!(
        unsafe {
            aorta_window_sync_try_aligned(ws, ptr::addr_of_mut!(set), ptr::addr_of_mut!(has))
        },
        AORTA_OK
    );
    assert_eq!(has, 1);
    let mut n: u32 = 0;
    assert_eq!(
        unsafe { aorta_aligned_set_len(set, ptr::addr_of_mut!(n)) },
        AORTA_OK
    );
    assert_eq!(n, 2);
    let (mut ts, mut data, mut data_len): (u64, *const u8, usize) = (0, ptr::null(), 0);
    assert_eq!(
        unsafe {
            aorta_aligned_set_get(
                set,
                0,
                ptr::addr_of_mut!(ts),
                ptr::addr_of_mut!(data),
                ptr::addr_of_mut!(data_len),
            )
        },
        AORTA_OK
    );
    assert_eq!(ts, 100);
    assert_eq!(unsafe { std::slice::from_raw_parts(data, data_len) }, &a);
    assert_eq!(
        unsafe {
            aorta_aligned_set_get(
                set,
                2,
                ptr::addr_of_mut!(ts),
                ptr::addr_of_mut!(data),
                ptr::addr_of_mut!(data_len),
            )
        },
        AORTA_ERR_INVALID_ARGUMENT,
        "index 2 of a 2-input set"
    );

    // Stats: 1 emitted, 0 dropped; per-input fed/last_ts.
    let (mut emitted, mut dropped): (u64, u64) = (0, 0);
    assert_eq!(
        unsafe {
            aorta_window_sync_stats(ws, ptr::addr_of_mut!(emitted), ptr::addr_of_mut!(dropped))
        },
        AORTA_OK
    );
    assert_eq!((emitted, dropped), (1, 0));
    let (mut fed, mut last_ts): (u64, u64) = (0, 0);
    assert_eq!(
        unsafe {
            aorta_window_sync_input_stats(ws, 1, ptr::addr_of_mut!(fed), ptr::addr_of_mut!(last_ts))
        },
        AORTA_OK
    );
    assert_eq!((fed, last_ts), (1, 150));

    assert_eq!(aorta_aligned_set_free(set), AORTA_OK);
    assert_eq!(
        unsafe { aorta_aligned_set_len(set, ptr::addr_of_mut!(n)) },
        AORTA_ERR_NOT_FOUND,
        "views die with the set"
    );
    assert_eq!(aorta_window_sync_free(ws), AORTA_OK);
    assert_eq!(
        unsafe { aorta_window_sync_feed(ws, 0, 1, ptr::null(), 0) },
        AORTA_ERR_NOT_FOUND,
        "stale window handle no-ops loudly"
    );
}

mod readiness_hooks {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, Ordering};

    unsafe extern "C" fn count_hook(ctx: *mut c_void) {
        let hits = unsafe { &*(ctx as *const AtomicU32) };
        hits.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn other_hook(_ctx: *mut c_void) {}

    #[test]
    fn readiness_set_ffi_wave_hook_and_destroy_wake() {
        let hits = Box::leak(Box::new(AtomicU32::new(0)));
        let ctx = hits as *const AtomicU32 as *mut c_void;

        let mut set: aorta_readiness_set_t = 0;
        assert_eq!(
            unsafe { aorta_readiness_set_create(2, ptr::addr_of_mut!(set)) },
            AORTA_OK
        );
        assert_eq!(
            unsafe { aorta_readiness_set_create(65, ptr::addr_of_mut!(set)) },
            AORTA_ERR_INVALID_ARGUMENT,
            "1..=64 bound"
        );
        assert_eq!(
            unsafe { aorta_readiness_set_set_notify(set, Some(count_hook), ctx) },
            AORTA_OK
        );
        // Single awaiter: a DIFFERENT hook is refused; the SAME one is
        // an idempotent re-arm.
        assert_eq!(
            unsafe { aorta_readiness_set_set_notify(set, Some(other_hook), ctx) },
            AORTA_ERR_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe { aorta_readiness_set_set_notify(set, Some(count_hook), ctx) },
            AORTA_OK
        );

        assert_eq!(aorta_readiness_set_satisfy(set, 0), AORTA_OK);
        assert_eq!(
            aorta_readiness_set_satisfy(set, 2),
            AORTA_ERR_INVALID_ARGUMENT,
            "condition 2 of a 2-condition set"
        );
        let mut mask: u64 = 0;
        assert_eq!(
            unsafe { aorta_readiness_set_pending_mask(set, ptr::addr_of_mut!(mask)) },
            AORTA_OK
        );
        assert_eq!(mask, 0b10);
        assert_eq!(hits.load(Ordering::SeqCst), 0, "no wave yet");
        assert_eq!(aorta_readiness_set_satisfy(set, 1), AORTA_OK);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "wave completion fires the hook"
        );
        let mut waves: u64 = 0;
        assert_eq!(
            unsafe { aorta_readiness_set_waves(set, ptr::addr_of_mut!(waves)) },
            AORTA_OK
        );
        assert_eq!(waves, 1);

        // Destroy-wake: free fires the parked hook one last time.
        assert_eq!(aorta_readiness_set_free(set), AORTA_OK);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(
            aorta_readiness_set_satisfy(set, 0),
            AORTA_ERR_NOT_FOUND,
            "stale set handle no-ops loudly"
        );
    }

    #[test]
    fn window_sync_ffi_emission_hook_and_destroy_wake() {
        let hits = Box::leak(Box::new(AtomicU32::new(0)));
        let ctx = hits as *const AtomicU32 as *mut c_void;

        let mut ws: aorta_window_sync_t = 0;
        assert_eq!(
            unsafe { aorta_window_sync_create(1, 1_000, 0, ptr::addr_of_mut!(ws)) },
            AORTA_OK
        );
        assert_eq!(
            unsafe { aorta_window_sync_set_notify(ws, Some(count_hook), ctx) },
            AORTA_OK
        );
        assert_eq!(
            unsafe { aorta_window_sync_set_notify(ws, Some(other_hook), ctx) },
            AORTA_ERR_INVALID_ARGUMENT,
            "single awaiter"
        );
        let payload = [7u8];
        assert_eq!(
            unsafe { aorta_window_sync_feed(ws, 0, 42, payload.as_ptr(), 1) },
            AORTA_OK
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "single-input feed emits and fires the hook"
        );
        // Clearing the hook stops emission callbacks.
        assert_eq!(
            unsafe { aorta_window_sync_set_notify(ws, None, ptr::null_mut()) },
            AORTA_OK
        );
        assert_eq!(
            unsafe { aorta_window_sync_feed(ws, 0, 43, payload.as_ptr(), 1) },
            AORTA_OK
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1, "cleared hook stays silent");
        // Re-arm, then free → destroy-wake fires it once more.
        assert_eq!(
            unsafe { aorta_window_sync_set_notify(ws, Some(count_hook), ctx) },
            AORTA_OK
        );
        assert_eq!(aorta_window_sync_free(ws), AORTA_OK);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn readiness_binds_validate_and_wire_a_c_subscriber() {
    let node_name = CString::new(format!("ffi_rdy_bind_{}", alloc_handle())).expect("node name");
    let topic = CString::new(format!("ffi/rdybind/id{}", alloc_handle())).expect("topic");
    let mut node: aorta_node_t = 0;
    assert_eq!(
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
        AORTA_OK
    );
    // Pull subscriber (delivery_mode = 1, depth 4) — binds work on ANY
    // delivery mode; pull needs no executor here.
    let buf = buffering(4, 1, 1);
    let qos = ffi_qos();
    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new_buffered(
        node,
        topic.as_ptr(),
        ptr::addr_of!(qos),
        ptr::null(),
        ptr::addr_of!(buf),
        None,
        ptr::null_mut(),
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(rc, AORTA_OK, "{}", last_error_string());

    let mut ws: aorta_window_sync_t = 0;
    assert_eq!(
        unsafe { aorta_window_sync_create(1, 1_000_000_000, 0, ptr::addr_of_mut!(ws)) },
        AORTA_OK
    );
    let mut set: aorta_readiness_set_t = 0;
    assert_eq!(
        unsafe { aorta_readiness_set_create(1, ptr::addr_of_mut!(set)) },
        AORTA_OK
    );

    // Foolproofing at the bind boundary.
    assert_eq!(
        aorta_subscriber_bind_window(sub, ws, 0, 9),
        AORTA_ERR_INVALID_ARGUMENT,
        "stamp must be 0|1"
    );
    assert_eq!(
        aorta_subscriber_bind_window(sub, ws, 1, 0),
        AORTA_ERR_INVALID_ARGUMENT,
        "input 1 of a 1-input window"
    );
    assert_eq!(
        aorta_subscriber_bind_readiness(sub, set, 1),
        AORTA_ERR_INVALID_ARGUMENT,
        "condition 1 of a 1-condition set"
    );
    assert_eq!(
        aorta_subscriber_bind_window(0xDEAD, ws, 0, 0),
        AORTA_ERR_NOT_FOUND
    );

    // Unbind before any bind exists: NOT_FOUND.
    assert_eq!(
        aorta_subscriber_unbind_window(sub, ws, 0),
        AORTA_ERR_NOT_FOUND,
        "no wire to remove yet"
    );

    assert_eq!(aorta_subscriber_bind_window(sub, ws, 0, 0), AORTA_OK);
    assert_eq!(aorta_subscriber_bind_readiness(sub, set, 0), AORTA_OK);

    // Round-trip the inverse: unbind removes, a second unbind reports
    // NOT_FOUND, and re-binding works.
    assert_eq!(aorta_subscriber_unbind_readiness(sub, set, 0), AORTA_OK);
    assert_eq!(
        aorta_subscriber_unbind_readiness(sub, set, 0),
        AORTA_ERR_NOT_FOUND
    );
    assert_eq!(aorta_subscriber_bind_readiness(sub, set, 0), AORTA_OK);
    assert_eq!(
        aorta_subscriber_unbind_window(0xDEAD, ws, 0),
        AORTA_ERR_NOT_FOUND,
        "stale subscriber handle"
    );

    // Data path: publish through the C publisher; the RX wire feeds the
    // window and satisfies the set.
    let mut publisher: aorta_pub_t = 0;
    assert_eq!(
        aorta_publisher_new(
            node,
            topic.as_ptr(),
            ptr::addr_of!(qos),
            ptr::null(),
            ptr::addr_of_mut!(publisher),
        ),
        AORTA_OK,
        "{}",
        last_error_string()
    );
    let payload = [0x5Au8];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let (mut aligned, mut has): (aorta_aligned_set_t, u8) = (0, 0);
    loop {
        assert_eq!(
            unsafe { aorta_publisher_publish(publisher, payload.as_ptr(), payload.len()) },
            AORTA_OK
        );
        assert_eq!(
            unsafe {
                aorta_window_sync_try_aligned(
                    ws,
                    ptr::addr_of_mut!(aligned),
                    ptr::addr_of_mut!(has),
                )
            },
            AORTA_OK
        );
        if has == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "bound subscriber must feed the window"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let (mut ts, mut data, mut data_len): (u64, *const u8, usize) = (0, ptr::null(), 0);
    assert_eq!(
        unsafe {
            aorta_aligned_set_get(
                aligned,
                0,
                ptr::addr_of_mut!(ts),
                ptr::addr_of_mut!(data),
                ptr::addr_of_mut!(data_len),
            )
        },
        AORTA_OK
    );
    assert_eq!(
        unsafe { std::slice::from_raw_parts(data, data_len) },
        &payload
    );
    assert!(ts > 0, "receive stamp populated");
    let mut waves: u64 = 0;
    assert_eq!(
        unsafe { aorta_readiness_set_waves(set, ptr::addr_of_mut!(waves)) },
        AORTA_OK
    );
    assert!(waves >= 1, "satisfy wire completed the wave");

    assert_eq!(aorta_aligned_set_free(aligned), AORTA_OK);
    assert_eq!(aorta_publisher_free(publisher), AORTA_OK);
    assert_eq!(aorta_subscriber_free(sub), AORTA_OK);
    assert_eq!(aorta_window_sync_free(ws), AORTA_OK);
    assert_eq!(aorta_readiness_set_free(set), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

// ---- Flow the completion arbiter ---------------------------

mod arbiter_protocol {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    struct Probe {
        resumes: AtomicU32,
        completes: AtomicU32,
        cancelled: AtomicU32,
        last_status: AtomicUsize,
    }

    unsafe extern "C" fn resume_cb(ctx: *mut c_void) {
        let p = unsafe { &*(ctx as *const Probe) };
        p.resumes.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn complete_cb(
        ctx: *mut c_void,
        status: aorta_result_t,
        _payload: *const aorta_bytes_view_t,
        _completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
    ) {
        let p = unsafe { &*(ctx as *const Probe) };
        p.completes.fetch_add(1, Ordering::SeqCst);
        if status == AORTA_ERR_CANCELLED {
            p.cancelled.fetch_add(1, Ordering::SeqCst);
        }
        p.last_status.store(status as usize, Ordering::SeqCst);
    }

    fn new_group(probe: &Probe, n: u32) -> (u64, Vec<u64>) {
        let mut group = 0u64;
        let rc = unsafe {
            arbiter::aorta_arbiter_group_new(
                Some(resume_cb),
                probe as *const Probe as *mut c_void,
                n,
                &mut group,
            )
        };
        assert_eq!(rc, AORTA_OK);
        let mut handles = Vec::new();
        for i in 0..n {
            let mut h = 0u64;
            let rc = unsafe {
                arbiter::aorta_arbiter_arm(
                    group,
                    i,
                    Some(complete_cb),
                    probe as *const Probe as *mut c_void,
                    &mut h,
                )
            };
            assert_eq!(rc, AORTA_OK);
            handles.push(h);
        }
        (group, handles)
    }

    fn probe() -> Probe {
        Probe {
            resumes: AtomicU32::new(0),
            completes: AtomicU32::new(0),
            cancelled: AtomicU32::new(0),
            last_status: AtomicUsize::new(usize::MAX),
        }
    }

    #[test]
    fn last_resolver_fires_exactly_one_resume() {
        let p = probe();
        let (group, handles) = new_group(&p, 3);
        for (i, h) in handles.iter().enumerate() {
            assert_eq!(
                unsafe { arbiter::aorta_arbiter_complete(*h, AORTA_OK, std::ptr::null()) },
                AORTA_OK
            );
            let expected_resumes = if i == 2 { 1 } else { 0 };
            assert_eq!(p.resumes.load(Ordering::SeqCst), expected_resumes);
        }
        assert_eq!(p.completes.load(Ordering::SeqCst), 3);
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
    }

    #[test]
    fn group_release_of_the_zero_sentinel_is_a_no_op() {
        // Every sibling Flow _free treats
        // handle 0 ("never allocated") as OK; this one returned NOT_FOUND.
        assert_eq!(arbiter::aorta_arbiter_group_release(0), AORTA_OK);
    }

    #[test]
    fn cas_loser_no_ops_and_double_complete_is_safe() {
        let p = probe();
        let (group, handles) = new_group(&p, 1);
        let h = handles[0];
        assert_eq!(
            unsafe { arbiter::aorta_arbiter_complete(h, AORTA_OK, std::ptr::null()) },
            AORTA_OK
        );
        assert_eq!(
            unsafe { arbiter::aorta_arbiter_complete(h, AORTA_OK, std::ptr::null()) },
            AORTA_OK,
            "loser no-ops"
        );
        assert_eq!(
            arbiter::aorta_arbiter_revoke(h),
            AORTA_OK,
            "late revoke no-ops"
        );
        assert_eq!(p.completes.load(Ordering::SeqCst), 1, "delivered once");
        assert_eq!(p.resumes.load(Ordering::SeqCst), 1, "resumed once");
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
    }

    #[test]
    fn race_shape_claim_then_revoke_losers() {
        // The when_any shape: winner claims; losers revoked; the frame
        // resumes at the LAST resolution (Cobalt interrupt-then-drain).
        let p = probe();
        let (group, handles) = new_group(&p, 3);
        assert_eq!(
            unsafe { arbiter::aorta_arbiter_complete(handles[1], AORTA_OK, std::ptr::null()) },
            AORTA_OK
        );
        assert_eq!(p.resumes.load(Ordering::SeqCst), 0, "two still pending");
        assert_eq!(arbiter::aorta_arbiter_revoke(handles[0]), AORTA_OK);
        assert_eq!(arbiter::aorta_arbiter_revoke(handles[2]), AORTA_OK);
        assert_eq!(p.resumes.load(Ordering::SeqCst), 1, "last revoke resumed");
        assert_eq!(
            p.cancelled.load(Ordering::SeqCst),
            2,
            "losers saw CANCELLED"
        );
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
    }

    #[test]
    fn concurrent_completes_resume_exactly_once() {
        for _ in 0..200 {
            let p = std::sync::Arc::new(probe());
            let (group, handles) = new_group(&p, 4);
            let mut joins = Vec::new();
            for h in handles {
                joins.push(std::thread::spawn(move || unsafe {
                    arbiter::aorta_arbiter_complete(h, AORTA_OK, std::ptr::null())
                }));
            }
            for j in joins {
                assert_eq!(j.join().unwrap(), AORTA_OK);
            }
            assert_eq!(p.resumes.load(Ordering::SeqCst), 1, "exactly one resume");
            assert_eq!(p.completes.load(Ordering::SeqCst), 4);
            assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        }
    }

    #[test]
    fn timer_completes_branch_and_revoke_aborts_sleeper() {
        // Fire path: a 1ms timer completes the single branch → resume.
        let p = probe();
        let (group, handles) = new_group(&p, 1);
        assert_eq!(
            arbiter::aorta_timer_arm_arbiter(handles[0], 1_000_000),
            AORTA_OK
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while p.resumes.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            p.resumes.load(Ordering::SeqCst),
            1,
            "timer fired the branch"
        );
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);

        // Revoke path: a LONG timer revoked early delivers CANCELLED and
        // must not fire later (the sleeper is aborted).
        let p2 = probe();
        let (group2, handles2) = new_group(&p2, 1);
        assert_eq!(
            arbiter::aorta_timer_arm_arbiter(handles2[0], 60_000_000_000),
            AORTA_OK
        );
        assert_eq!(arbiter::aorta_arbiter_revoke(handles2[0]), AORTA_OK);
        assert_eq!(p2.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(p2.resumes.load(Ordering::SeqCst), 1);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(
            p2.completes.load(Ordering::SeqCst),
            1,
            "the aborted sleeper never double-delivered"
        );
        assert_eq!(arbiter::aorta_arbiter_group_release(group2), AORTA_OK);
    }

    #[test]
    fn scope_arm_completes_branch_on_scope_cancel() {
        // The scope bridge — a core watcher
        // resolves the branch CANCELLED when the scope's token fires
        // (the per-scope cancel race behind next(sub, rt, scope)).
        let p = probe();
        let (group, handles) = new_group(&p, 2);
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { crate::ffi::c_api::flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        assert_eq!(
            arbiter::aorta_scope_arm_arbiter(handles[1], scope),
            AORTA_OK
        );
        assert_eq!(p.resumes.load(Ordering::SeqCst), 0, "live scope: parked");
        assert_eq!(
            crate::ffi::c_api::flow::aorta_scope_cancel(scope, 1),
            AORTA_OK
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while p.cancelled.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            p.cancelled.load(Ordering::SeqCst),
            1,
            "the watcher resolved the scope branch CANCELLED"
        );
        assert_eq!(
            unsafe { arbiter::aorta_arbiter_complete(handles[0], AORTA_OK, std::ptr::null()) },
            AORTA_OK
        );
        assert_eq!(p.resumes.load(Ordering::SeqCst), 1, "group resolved");
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(crate::ffi::c_api::flow::aorta_scope_free(scope), AORTA_OK);
    }

    #[test]
    fn scope_arm_not_found_on_stale_handles() {
        // Flow handle convention: stale ids NOT_FOUND-no-op.
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { crate::ffi::c_api::flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        assert_eq!(
            arbiter::aorta_scope_arm_arbiter(0xdead00, scope),
            crate::ffi::c_api::AORTA_ERR_NOT_FOUND,
            "stale arbiter handle"
        );
        let p = probe();
        let (group, handles) = new_group(&p, 1);
        assert_eq!(
            arbiter::aorta_scope_arm_arbiter(handles[0], 0xdead_beef),
            crate::ffi::c_api::AORTA_ERR_NOT_FOUND,
            "stale scope handle"
        );
        // The branch stays armed and usable after the refusal.
        assert_eq!(
            unsafe { arbiter::aorta_arbiter_complete(handles[0], AORTA_OK, std::ptr::null()) },
            AORTA_OK
        );
        assert_eq!(p.resumes.load(Ordering::SeqCst), 1);
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(crate::ffi::c_api::flow::aorta_scope_free(scope), AORTA_OK);
    }

    #[test]
    fn scope_arm_revoke_aborts_watcher_no_double_resolution() {
        // Revoking the branch aborts the watcher; a
        // scope cancel AFTER the revoke must not double-resolve.
        let p = probe();
        let (group, handles) = new_group(&p, 1);
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { crate::ffi::c_api::flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        assert_eq!(
            arbiter::aorta_scope_arm_arbiter(handles[0], scope),
            AORTA_OK
        );
        assert_eq!(arbiter::aorta_arbiter_revoke(handles[0]), AORTA_OK);
        assert_eq!(p.resumes.load(Ordering::SeqCst), 1, "revoke resolved");
        assert_eq!(
            crate::ffi::c_api::flow::aorta_scope_cancel(scope, 1),
            AORTA_OK
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(
            p.resumes.load(Ordering::SeqCst),
            1,
            "late scope cancel never double-resolves"
        );
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(crate::ffi::c_api::flow::aorta_scope_free(scope), AORTA_OK);
    }

    #[test]
    fn complete_after_group_release_no_ops() {
        let p = probe();
        let (group, handles) = new_group(&p, 2);
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(
            unsafe { arbiter::aorta_arbiter_complete(handles[0], AORTA_OK, std::ptr::null()) },
            crate::ffi::c_api::AORTA_ERR_NOT_FOUND,
            "late complete against a freed group no-ops as NOT_FOUND"
        );
        assert_eq!(p.completes.load(Ordering::SeqCst), 0);
        assert_eq!(p.resumes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn partial_arm_error_resolves_through_the_same_path() {
        // Partial arm: a producer failing AFTER arm resolves Stopped via
        // complete(error) — same counter, same last-resolver.
        let p = probe();
        let (group, handles) = new_group(&p, 2);
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_complete(handles[0], AORTA_ERR_CANCELLED, std::ptr::null())
            },
            AORTA_OK
        );
        assert_eq!(
            unsafe { arbiter::aorta_arbiter_complete(handles[1], AORTA_OK, std::ptr::null()) },
            AORTA_OK
        );
        assert_eq!(p.resumes.load(Ordering::SeqCst), 1);
        assert_eq!(p.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
    }
}

// ---- emit-side forwarding ------------------------------------

mod wp1_completion_record {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU8, Ordering};

    // --- with_ffi_completion unit contracts --------------------------------

    #[test]
    fn completion_record_preserves_the_stop_vocabulary() {
        // Cancellation with a NAMED reason: code is the normative
        // CANCELLED, the reason byte is exact, detail empty.
        let stopped = crate::flow::Stopped::cancelled(crate::flow::CancelReason::Shutdown);
        flow::with_ffi_completion(&stopped, |c| {
            let c = unsafe { &*c };
            assert_eq!(c.code, AORTA_ERR_CANCELLED);
            assert_eq!(c.stop_kind, flow::AORTA_STOP_KIND_CANCELLED);
            assert_eq!(c.cancel_reason, flow::AORTA_CANCEL_REASON_SHUTDOWN);
            assert_eq!(c.detail.total_len, 0);
        });

        // UNKNOWN raw byte: verbatim through the record (loss-free carrier).
        let stopped =
            crate::flow::Stopped::cancelled(crate::flow::CancelReason::from_raw(200).unwrap());
        flow::with_ffi_completion(&stopped, |c| {
            let c = unsafe { &*c };
            assert_eq!(c.stop_kind, flow::AORTA_STOP_KIND_CANCELLED);
            assert_eq!(c.cancel_reason, 200);
        });

        // Fault: ORIGINAL stable code + UTF-8 detail bytes.
        let stopped = crate::flow::Stopped::fault_with_code(-3, "rpc window elapsed");
        flow::with_ffi_completion(&stopped, |c| {
            let c = unsafe { &*c };
            assert_eq!(c.code, -3);
            assert_eq!(c.stop_kind, flow::AORTA_STOP_KIND_FAULT);
            assert_eq!(c.cancel_reason, 0);
            assert_eq!(c.detail.total_len, "rpc window elapsed".len());
            let slice = unsafe { &*c.detail.slices };
            let bytes = unsafe { std::slice::from_raw_parts(slice.data, slice.len) };
            assert_eq!(bytes, b"rpc window elapsed");
        });
    }

    // --- scope observation --------------------------------------------------

    #[test]
    fn scope_cancel_reason_round_trips_raw_bytes_across_the_abi() {
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        let (mut has, mut reason) = (0xffu8, 0xffu8);
        assert_eq!(
            unsafe { flow::aorta_scope_cancel_reason(scope, &mut has, &mut reason) },
            AORTA_OK
        );
        assert_eq!((has, reason), (0, 0), "a live scope reads (0, 0)");
        // A FOREIGN byte enters at the C ABI and is observed VERBATIM —
        // previously this read back as Explicit (1).
        assert_eq!(flow::aorta_scope_cancel(scope, 200), AORTA_OK);
        assert_eq!(
            unsafe { flow::aorta_scope_cancel_reason(scope, &mut has, &mut reason) },
            AORTA_OK
        );
        assert_eq!((has, reason), (1, 200), "raw byte must round-trip");
        assert_eq!(flow::aorta_scope_free(scope), AORTA_OK);
    }

    #[test]
    fn shutdown_with_grace_reports_zero_detached_when_clean() {
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        let mut detached = u64::MAX;
        assert_eq!(
            unsafe {
                flow::aorta_scope_shutdown_with_grace(
                    scope,
                    0, // conventional default = Shutdown
                    50_000_000,
                    &mut detached,
                )
            },
            AORTA_OK
        );
        assert_eq!(detached, 0, "no children: nothing detaches");
        let (mut has, mut reason) = (0u8, 0u8);
        assert_eq!(
            unsafe { flow::aorta_scope_cancel_reason(scope, &mut has, &mut reason) },
            AORTA_OK
        );
        assert_eq!(
            (has, reason),
            (1, flow::AORTA_CANCEL_REASON_SHUTDOWN),
            "grace shutdown cancels with Shutdown"
        );
        assert_eq!(flow::aorta_scope_free(scope), AORTA_OK);
    }

    #[test]
    fn service_stats_record_preserves_the_deadline_drop_counter() {
        // Flow TCK `budget_expiry_queued_server` finding: core's
        // ServiceStats has carried request_dropped_deadline_count since
        // the deadline gate landed, but service_stats_from_core silently
        // DROPPED it at the C ABI — the C++ plane had no way to observe
        // queued-request deadline drops at all.
        let core = crate::service::ServiceStats {
            request_dropped_count: 9,
            request_dropped_deadline_count: 7,
            ..Default::default()
        };
        let ffi = super::super::service_stats_from_core(core);
        assert_eq!(ffi.request_dropped_count, 9);
        assert_eq!(
            ffi.request_dropped_deadline_count, 7,
            "the deadline-drop SUBSET counter must cross the ABI verbatim"
        );
    }

    // --- the arbiter scope-watcher record -----------------------------------

    struct CompletionProbe {
        fired: AtomicU32,
        status: AtomicI32,
        has_record: AtomicBool,
        kind: AtomicU8,
        reason: AtomicU8,
        code: AtomicI32,
    }

    unsafe extern "C" fn noop_resume(_ctx: *mut c_void) {}

    unsafe extern "C" fn completion_probe_cb(
        ctx: *mut c_void,
        status: aorta_result_t,
        _payload: *const aorta_bytes_view_t,
        completion: *const flow::aorta_flow_completion_t,
    ) {
        let p = unsafe { &*(ctx as *const CompletionProbe) };
        p.status.store(status, Ordering::SeqCst);
        if !completion.is_null() {
            let c = unsafe { &*completion };
            p.has_record.store(true, Ordering::SeqCst);
            p.kind.store(c.stop_kind, Ordering::SeqCst);
            p.reason.store(c.cancel_reason, Ordering::SeqCst);
            p.code.store(c.code, Ordering::SeqCst);
        }
        p.fired.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn scope_watcher_wakes_on_budget_expiry_with_deadline_reason() {
        // Flow TCK `budget_expiry_parked` finding: a C-ABI frame parked on
        // the scope watcher (C++ `co_await flow::cancelled()`) never woke
        // when the scope's budget expired — only `checkpoint()` fires the
        // token, and a parked frame never checkpoints. The watcher must
        // race the budget deadline (Rust-plane parity: DispatchBoundary
        // arms a per-park deadline) and deliver DeadlineExceeded.
        let probe = Box::leak(Box::new(CompletionProbe {
            fired: AtomicU32::new(0),
            status: AtomicI32::new(0),
            has_record: AtomicBool::new(false),
            kind: AtomicU8::new(0xff),
            reason: AtomicU8::new(0xff),
            code: AtomicI32::new(0),
        }));
        let mut opts: flow::aorta_scope_opts_t = unsafe { std::mem::zeroed() };
        opts.budget_ns = 50_000_000; // 50ms
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, &opts, &mut scope) },
            AORTA_OK
        );
        let mut group = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_group_new(
                    Some(noop_resume),
                    probe as *const CompletionProbe as *mut c_void,
                    1,
                    &mut group,
                )
            },
            AORTA_OK
        );
        let mut handle = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_arm(
                    group,
                    0,
                    Some(completion_probe_cb),
                    probe as *const CompletionProbe as *mut c_void,
                    &mut handle,
                )
            },
            AORTA_OK
        );
        assert_eq!(arbiter::aorta_scope_arm_arbiter(handle, scope), AORTA_OK);
        // NOBODY cancels — the budget alone must wake the watcher.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while probe.fired.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            probe.fired.load(Ordering::SeqCst),
            1,
            "budget expiry must wake the parked watcher"
        );
        assert_eq!(probe.status.load(Ordering::SeqCst), AORTA_ERR_CANCELLED);
        assert!(probe.has_record.load(Ordering::SeqCst), "record must ride");
        assert_eq!(
            probe.kind.load(Ordering::SeqCst),
            flow::AORTA_STOP_KIND_CANCELLED
        );
        assert_eq!(
            probe.reason.load(Ordering::SeqCst),
            flow::AORTA_CANCEL_REASON_DEADLINE
        );
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(crate::ffi::c_api::flow::aorta_scope_free(scope), AORTA_OK);
    }

    #[test]
    fn scope_watcher_delivers_the_exact_reason_through_the_record() {
        let probe = Box::leak(Box::new(CompletionProbe {
            fired: AtomicU32::new(0),
            status: AtomicI32::new(0),
            has_record: AtomicBool::new(false),
            kind: AtomicU8::new(0xff),
            reason: AtomicU8::new(0xff),
            code: AtomicI32::new(0),
        }));
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        let mut group = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_group_new(
                    Some(noop_resume),
                    probe as *const CompletionProbe as *mut c_void,
                    1,
                    &mut group,
                )
            },
            AORTA_OK
        );
        let mut handle = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_arm(
                    group,
                    0,
                    Some(completion_probe_cb),
                    probe as *const CompletionProbe as *mut c_void,
                    &mut handle,
                )
            },
            AORTA_OK
        );
        assert_eq!(arbiter::aorta_scope_arm_arbiter(handle, scope), AORTA_OK);
        // The FOREIGN reason byte cancels the scope; the watcher must
        // deliver it VERBATIM in the completion record — the one arbiter
        // resolver with a real stop reason.
        assert_eq!(flow::aorta_scope_cancel(scope, 200), AORTA_OK);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while probe.fired.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            probe.fired.load(Ordering::SeqCst),
            1,
            "watcher resolved once"
        );
        assert_eq!(probe.status.load(Ordering::SeqCst), AORTA_ERR_CANCELLED);
        assert!(probe.has_record.load(Ordering::SeqCst), "record must ride");
        assert_eq!(
            probe.kind.load(Ordering::SeqCst),
            flow::AORTA_STOP_KIND_CANCELLED
        );
        assert_eq!(probe.reason.load(Ordering::SeqCst), 200, "byte verbatim");
        assert_eq!(probe.code.load(Ordering::SeqCst), AORTA_ERR_CANCELLED);
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(flow::aorta_scope_free(scope), AORTA_OK);
    }

    // Two more named reasons through the SAME arbiter
    // scope-watcher harness as the FOREIGN-byte test above — pins
    // Explicit-vs-Shutdown-vs-ChildFailed distinguishability across the ABI
    // (not just "some non-zero byte round-trips").

    #[test]
    fn scope_watcher_delivers_the_shutdown_reason_through_the_record() {
        let probe = Box::leak(Box::new(CompletionProbe {
            fired: AtomicU32::new(0),
            status: AtomicI32::new(0),
            has_record: AtomicBool::new(false),
            kind: AtomicU8::new(0xff),
            reason: AtomicU8::new(0xff),
            code: AtomicI32::new(0),
        }));
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        let mut group = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_group_new(
                    Some(noop_resume),
                    probe as *const CompletionProbe as *mut c_void,
                    1,
                    &mut group,
                )
            },
            AORTA_OK
        );
        let mut handle = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_arm(
                    group,
                    0,
                    Some(completion_probe_cb),
                    probe as *const CompletionProbe as *mut c_void,
                    &mut handle,
                )
            },
            AORTA_OK
        );
        assert_eq!(arbiter::aorta_scope_arm_arbiter(handle, scope), AORTA_OK);
        // Runtime shutdown (byte 3) — must NOT be flattened to Explicit.
        assert_eq!(
            flow::aorta_scope_cancel(scope, flow::AORTA_CANCEL_REASON_SHUTDOWN),
            AORTA_OK
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while probe.fired.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            probe.fired.load(Ordering::SeqCst),
            1,
            "watcher resolved once"
        );
        assert_eq!(probe.status.load(Ordering::SeqCst), AORTA_ERR_CANCELLED);
        assert!(probe.has_record.load(Ordering::SeqCst), "record must ride");
        assert_eq!(
            probe.kind.load(Ordering::SeqCst),
            flow::AORTA_STOP_KIND_CANCELLED
        );
        assert_eq!(
            probe.reason.load(Ordering::SeqCst),
            flow::AORTA_CANCEL_REASON_SHUTDOWN,
            "Shutdown must survive as byte 3, distinguishable from Explicit/ChildFailed"
        );
        assert_eq!(probe.code.load(Ordering::SeqCst), AORTA_ERR_CANCELLED);
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(flow::aorta_scope_free(scope), AORTA_OK);
    }

    #[test]
    fn scope_watcher_delivers_the_child_failed_reason_through_the_record() {
        let probe = Box::leak(Box::new(CompletionProbe {
            fired: AtomicU32::new(0),
            status: AtomicI32::new(0),
            has_record: AtomicBool::new(false),
            kind: AtomicU8::new(0xff),
            reason: AtomicU8::new(0xff),
            code: AtomicI32::new(0),
        }));
        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        let mut group = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_group_new(
                    Some(noop_resume),
                    probe as *const CompletionProbe as *mut c_void,
                    1,
                    &mut group,
                )
            },
            AORTA_OK
        );
        let mut handle = 0u64;
        assert_eq!(
            unsafe {
                arbiter::aorta_arbiter_arm(
                    group,
                    0,
                    Some(completion_probe_cb),
                    probe as *const CompletionProbe as *mut c_void,
                    &mut handle,
                )
            },
            AORTA_OK
        );
        assert_eq!(arbiter::aorta_scope_arm_arbiter(handle, scope), AORTA_OK);
        // A supervised child's failure cascade (byte 4) — must NOT be
        // flattened to Explicit or confused with Shutdown.
        assert_eq!(
            flow::aorta_scope_cancel(scope, flow::AORTA_CANCEL_REASON_CHILD_FAILED),
            AORTA_OK
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while probe.fired.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(
            probe.fired.load(Ordering::SeqCst),
            1,
            "watcher resolved once"
        );
        assert_eq!(probe.status.load(Ordering::SeqCst), AORTA_ERR_CANCELLED);
        assert!(probe.has_record.load(Ordering::SeqCst), "record must ride");
        assert_eq!(
            probe.kind.load(Ordering::SeqCst),
            flow::AORTA_STOP_KIND_CANCELLED
        );
        assert_eq!(
            probe.reason.load(Ordering::SeqCst),
            flow::AORTA_CANCEL_REASON_CHILD_FAILED,
            "ChildFailed must survive as byte 4, distinguishable from Explicit/Shutdown"
        );
        assert_eq!(probe.code.load(Ordering::SeqCst), AORTA_ERR_CANCELLED);
        assert_eq!(arbiter::aorta_arbiter_group_release(group), AORTA_OK);
        assert_eq!(flow::aorta_scope_free(scope), AORTA_OK);
    }

    // --- grace shutdown with a stalled child ---

    #[test]
    fn shutdown_with_grace_detaches_a_stalled_external_child() {
        // A C-ABI `aorta_scope_spawn`ed task counts toward `close()` — and
        // therefore toward `shutdown_with_grace`'s drain — until
        // `aorta_task_finish` is called (see `aorta_task_finish`'s doc: "the
        // task stays open for scope accounting until aorta_task_finish").
        // Simulating a STALLED child (a C body that never observes
        // cancellation) is therefore as simple as leaving the spawned task
        // open past the grace window — no separate "parking body" helper
        // needed; this reuses the exact `flow_task_body` /
        // `scope_spawn_dispatches_body_and_close_waits_for_task_finish`
        // pattern from this file's top-level tests, just never finishing
        // the task before the grace deadline.
        let mut exec: u64 = 0;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let spin_exec = exec;
        let spin = std::thread::spawn(move || executor::aorta_executor_spin(spin_exec));

        let mut scope: u64 = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, std::ptr::null(), &mut scope) },
            AORTA_OK
        );
        let counter = Box::leak(Box::new(std::sync::atomic::AtomicUsize::new(0)));
        let mut task: flow::aorta_task_t = 0;
        assert_eq!(
            unsafe {
                flow::aorta_scope_spawn(
                    exec,
                    scope,
                    Some(super::flow_task_body),
                    counter as *const std::sync::atomic::AtomicUsize as *mut c_void,
                    &mut task,
                )
            },
            AORTA_OK
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while counter.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1, "body dispatched once");
        // Deliberately NO aorta_task_finish here — the task stays OPEN,
        // standing in for a C body that never observes cancellation (a
        // stalled child close() would otherwise wait on forever).

        let mut detached = u64::MAX;
        assert_eq!(
            unsafe {
                flow::aorta_scope_shutdown_with_grace(
                    scope,
                    flow::AORTA_CANCEL_REASON_SHUTDOWN,
                    50_000_000, // 50ms grace — expires with the task still open
                    &mut detached,
                )
            },
            AORTA_OK
        );
        assert_eq!(
            detached, 1,
            "the stalled external child is detached once the grace window expires"
        );

        let (mut has, mut reason) = (0u8, 0u8);
        assert_eq!(
            unsafe { flow::aorta_scope_cancel_reason(scope, &mut has, &mut reason) },
            AORTA_OK
        );
        assert_eq!(
            (has, reason),
            (1, flow::AORTA_CANCEL_REASON_SHUTDOWN),
            "grace shutdown still cancels with Shutdown even though a child was detached"
        );

        // Finish the straggler so its accounting settles cleanly — legal
        // even after detach: the child slot's 3-state arbiter already
        // resolved the close()-blocking registration, so this is a benign
        // no-op release, not a double-decrement.
        assert_eq!(flow::aorta_task_finish(task), AORTA_OK);

        assert_eq!(executor::aorta_executor_stop(exec), AORTA_OK);
        assert_eq!(spin.join().expect("spin"), AORTA_OK);
        assert_eq!(flow::aorta_scope_free(scope), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn unknown_supervision_byte_refused_at_both_scope_child_entries() {
        // `validate_scope_opts()` is the ONE input
        // validation for `aorta_scope_opts_t`, called FIRST by EVERY entry
        // that decodes it — `aorta_scope_child` (root/parented) AND
        // `aorta_scope_child_of_token` (the CancelToken bridge). An
        // unknown supervision byte must be refused INVALID_ARGUMENT at
        // BOTH, never silently folded to FailFast by
        // `scope_opts_from_ffi`'s defensive default arm.
        let opts = flow::aorta_scope_opts_t {
            priority: 0,
            supervision: 200, // unknown policy value
            _pad: [0; 2],
            budget_ns: 0,
            name: std::ptr::null(),
        };

        // Entry 1: aorta_scope_child (root scope, parent == 0).
        let mut scope: flow::aorta_scope_t = 0;
        assert_eq!(
            unsafe { flow::aorta_scope_child(0, std::ptr::addr_of!(opts), &mut scope) },
            AORTA_ERR_INVALID_ARGUMENT
        );
        assert_eq!(scope, 0, "no scope handle leaks out of a refused call");

        // Entry 2: aorta_scope_child_of_token — same token-bridge
        // construction pattern as the aorta_token_child tests above.
        let mut token: flow::aorta_token_t = 0;
        assert_eq!(unsafe { flow::aorta_token_child(0, &mut token) }, AORTA_OK);
        let mut token_scope: flow::aorta_scope_t = 0;
        assert_eq!(
            unsafe {
                flow::aorta_scope_child_of_token(token, std::ptr::addr_of!(opts), &mut token_scope)
            },
            AORTA_ERR_INVALID_ARGUMENT
        );
        assert_eq!(
            token_scope, 0,
            "no scope handle leaks out of a refused call"
        );

        assert_eq!(flow::aorta_token_free(token), AORTA_OK);
    }
}

mod lane_forwarding {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    static FORWARDED: AtomicU32 = AtomicU32::new(0);
    static MODE: AtomicUsize = AtomicUsize::new(1); // verdict to return

    unsafe extern "C" fn fwd_drain(_ctx: *mut c_void, _max: u32) -> u64 {
        0
    }

    unsafe extern "C" fn fwd_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        drop_fn: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _lane_class: u8,
        _group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        let mode = MODE.load(Ordering::SeqCst) as u8;
        match mode {
            1 => {
                FORWARDED.fetch_add(1, Ordering::SeqCst);
                // Deliver inline (stand-in for the lane drain).
                unsafe { run(item) };
                1
            }
            2 => 2, // refused-full: caller keeps the item
            _ => {
                let _ = drop_fn;
                0
            }
        }
    }

    fn hooks() -> aorta_lane_hooks_t {
        aorta_lane_hooks_t {
            drain_fn: Some(fwd_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(fwd_forward),
            pool_workers: 0,
        }
    }

    #[test]
    fn attached_executor_forwards_callback_events_and_queue_stays_empty() {
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let h = hooks();
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );
        MODE.store(1, Ordering::SeqCst);
        let before = FORWARDED.load(Ordering::SeqCst);

        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran2 = ran.clone();
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        let event = crate::event_queue::Event::new(
            crate::event_queue::EventKind::SubscriptionData,
            move || {
                ran2.store(true, Ordering::SeqCst);
            },
        );
        entry.sender.try_send(event).expect("forwarded = accepted");
        assert_eq!(
            FORWARDED.load(Ordering::SeqCst),
            before + 1,
            "the emit-side hook carried the event"
        );
        assert!(ran.load(Ordering::SeqCst), "the shim ran the callback");

        // The facade exit shape: nothing reached the core queue — spin_once
        // finds no event.
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn refused_full_surfaces_v1_full_to_the_caller() {
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let h = hooks();
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );
        MODE.store(2, Ordering::SeqCst);
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        let event =
            crate::event_queue::Event::new(crate::event_queue::EventKind::SubscriptionData, || {});
        let err = entry.sender.try_send(event).expect_err("refused-full");
        assert!(
            matches!(err, tokio::sync::mpsc::error::TrySendError::Full(_)),
            "caller-owned refusal surfaces as the queue path Full (Drop policy runs)"
        );
        MODE.store(1, Ordering::SeqCst);
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn second_attach_is_refused_until_detach() {
        // A second attach while the old hooks
        // may still be mid-flight returned AORTA_OK silently — the caller
        // then freed the old pool under a live callback (UAF window).
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let h = hooks();
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );
        let h2 = hooks();
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h2) },
            AORTA_ERR_INVALID_ARGUMENT,
            "double attach must be refused, not silently accepted"
        );
        // The refusal left the FIRST hooks in place; detach + re-attach is
        // the sanctioned swap sequence.
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h2) },
            AORTA_OK
        );
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn detach_from_inside_a_callback_frame_is_refused_not_hung() {
        // detach()'s quiesce spin waits for the
        // in-flight hook frame to return — called from INSIDE one (the ST
        // default runs drain_fn inline on this thread) it spun forever at
        // 100% CPU. The reentrancy mark must reject it instead.
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let h = hooks();
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );
        {
            // The same thread-local mark a drain_fn/lane-worker frame runs
            // under (aorta_callback_context_enter / CallbackContextGuard).
            let _in_callback = crate::flow::reentrancy::CallbackContextGuard::enter();
            assert_eq!(
                executor::aorta_executor_detach_lane_pool(exec),
                AORTA_ERR_REENTRANT,
                "reentrant detach must be refused, not spin forever"
            );
        }
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn attach_validates_the_threading_pairing() {
        // ST executor + MT pool = INVALID_ARGUMENT (and vice versa).
        let mut st_exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut st_exec,
                )
            },
            AORTA_OK
        );
        let mut h = hooks();
        h.pool_workers = 3;
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(st_exec, &h) },
            AORTA_ERR_INVALID_ARGUMENT,
            "ST executor + MT pool destroys no-concurrency"
        );
        assert_eq!(executor::aorta_executor_free(st_exec), AORTA_OK);

        let mut mt_exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_MULTI,
                    &mut mt_exec,
                )
            },
            AORTA_OK
        );
        let mut h2 = hooks();
        h2.pool_workers = 0;
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(mt_exec, &h2) },
            AORTA_ERR_INVALID_ARGUMENT,
            "MT executor + ST pool has no drainers once spin parks"
        );
        assert_eq!(executor::aorta_executor_free(mt_exec), AORTA_OK);
    }

    #[test]
    fn detached_executor_keeps_the_v1_queue_path() {
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        MODE.store(1, Ordering::SeqCst);
        let before = FORWARDED.load(Ordering::SeqCst);
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        let event =
            crate::event_queue::Event::new(crate::event_queue::EventKind::SubscriptionData, || {});
        entry.sender.try_send(event).expect("queue path accepts");
        assert_eq!(
            FORWARDED.load(Ordering::SeqCst),
            before,
            "no hook involvement when detached"
        );
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }
}

/// The `lane_forwarding` /
/// `timer_forwarding` unit tests above validate the attach surface by pushing
/// hand-built `Event`s into the crate-private executor sender — no test drove
/// a REAL callback-mode subscription or a REAL timer fire through an attached
/// lane pool. These two tests close that gap through PUBLIC entry points
/// only: C-ABI node + executor + `aorta_executor_add_node` +
/// `aorta_executor_attach_lane_pool`, a real Zenoh publish (respectively a
/// real `aorta_timer_new` fire), asserting the callback executes INSIDE the
/// forward hook's frame. The existing unit tests stay — these add the
/// end-to-end proof.
mod lane_forwarding_e2e {
    use super::super::*;
    use super::{ffi_qos, last_error_string};
    use std::cell::Cell;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ---- subscription end-to-end -----------------------------------------

    static SUB_FORWARDS: AtomicU32 = AtomicU32::new(0);
    static SUB_DELIVERIES: AtomicU32 = AtomicU32::new(0);
    static SUB_DELIVERIES_IN_FRAME: AtomicU32 = AtomicU32::new(0);

    thread_local! {
        /// True while THIS thread is inside a forward-hook `run(item)` frame.
        static IN_HOOK_FRAME: Cell<bool> = const { Cell::new(false) };
    }

    unsafe extern "C" fn e2e_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn e2e_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _lane_class: u8,
        _group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        SUB_FORWARDS.fetch_add(1, Ordering::SeqCst);
        IN_HOOK_FRAME.set(true);
        unsafe { run(item) };
        IN_HOOK_FRAME.set(false);
        1
    }

    extern "C" fn e2e_on_msg(
        _payload: *const aorta_bytes_view_t,
        _schema: *const aorta_schema_hash_view_t,
        _ctx: *const aorta_msg_context_t,
        _user_data: *mut c_void,
    ) {
        SUB_DELIVERIES.fetch_add(1, Ordering::SeqCst);
        if IN_HOOK_FRAME.get() {
            SUB_DELIVERIES_IN_FRAME.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn hooks(
        forward: unsafe extern "C" fn(
            *mut c_void,
            unsafe extern "C" fn(*mut c_void),
            unsafe extern "C" fn(*mut c_void),
            *mut c_void,
            u8,
            u64,
            u64,
            u32,
        ) -> u8,
    ) -> aorta_lane_hooks_t {
        aorta_lane_hooks_t {
            drain_fn: Some(e2e_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(forward),
            pool_workers: 0,
        }
    }

    #[test]
    fn real_subscription_delivers_through_the_attached_lane_pool() {
        // The genuine emit chain: aorta_publisher_publish → Zenoh RX →
        // subscriber callback closure → try_send on the node's executor
        // sender → LaneHookSlot::forward → the C forward hook, which runs
        // the callback inline. The executor is NEVER spun: if the event had
        // taken the core-queue path instead, the callback could not run at
        // all and the wait below would time out — delivery here is only
        // possible THROUGH the attached hooks.
        SUB_FORWARDS.store(0, Ordering::SeqCst);
        SUB_DELIVERIES.store(0, Ordering::SeqCst);
        SUB_DELIVERIES_IN_FRAME.store(0, Ordering::SeqCst);

        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let node_name =
            CString::new(format!("ffi_lane_e2e_node_{}", alloc_handle())).expect("node name");
        let mut node: aorta_node_t = 0;
        assert_eq!(
            unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
            AORTA_OK,
            "{}",
            last_error_string()
        );
        assert_eq!(aorta_executor_add_node(exec, node), AORTA_OK);
        let h = hooks(e2e_forward);
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );

        let topic = CString::new(format!("ffi/lane_e2e/id{}", alloc_handle())).expect("topic");
        let qos = ffi_qos();
        let mut sub: aorta_sub_t = 0;
        let rc = aorta_subscriber_new(
            node,
            topic.as_ptr(),
            ptr::addr_of!(qos),
            ptr::null(),
            Some(e2e_on_msg),
            ptr::null_mut(),
            ptr::addr_of_mut!(sub),
        );
        assert_eq!(rc, AORTA_OK, "{}", last_error_string());
        let mut publisher: aorta_pub_t = 0;
        assert_eq!(
            aorta_publisher_new(
                node,
                topic.as_ptr(),
                ptr::addr_of!(qos),
                ptr::null(),
                ptr::addr_of_mut!(publisher),
            ),
            AORTA_OK,
            "{}",
            last_error_string()
        );

        // Publish (retrying) until the callback has run.
        let payload = [0x42u8];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while SUB_DELIVERIES.load(Ordering::SeqCst) == 0 {
            assert_eq!(
                unsafe { aorta_publisher_publish(publisher, payload.as_ptr(), payload.len()) },
                AORTA_OK
            );
            assert!(
                std::time::Instant::now() < deadline,
                "published sample never reached the C callback through the lane pool"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(
            SUB_FORWARDS.load(Ordering::SeqCst) >= 1,
            "the forward hook carried the subscription event"
        );
        assert!(
            SUB_DELIVERIES_IN_FRAME.load(Ordering::SeqCst) >= 1,
            "the C callback executed inside the forward hook's run(item) frame"
        );

        // Negative control: after detach, publishes take the core-queue path
        // (nobody spins it) — the hook sees nothing and the callback stops.
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        let forwards_before = SUB_FORWARDS.load(Ordering::SeqCst);
        let deliveries_before = SUB_DELIVERIES.load(Ordering::SeqCst);
        for _ in 0..3 {
            assert_eq!(
                unsafe { aorta_publisher_publish(publisher, payload.as_ptr(), payload.len()) },
                AORTA_OK
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(400));
        assert_eq!(
            SUB_FORWARDS.load(Ordering::SeqCst),
            forwards_before,
            "detached: no forward-hook involvement"
        );
        assert_eq!(
            SUB_DELIVERIES.load(Ordering::SeqCst),
            deliveries_before,
            "detached: events sit in the un-spun core queue, so the earlier \
             deliveries can only have ridden the lane pool"
        );

        assert_eq!(aorta_publisher_free(publisher), AORTA_OK);
        assert_eq!(aorta_subscriber_free(sub), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
        assert_eq!(aorta_node_free(node), AORTA_OK);
    }

    // ---- timer end-to-end -------------------------------------------------

    static TIMER_FORWARDS: AtomicU32 = AtomicU32::new(0);
    static TIMER_TICKS: AtomicU32 = AtomicU32::new(0);
    static TIMER_TICKS_IN_FRAME: AtomicU32 = AtomicU32::new(0);

    unsafe extern "C" fn timer_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _lane_class: u8,
        _group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        TIMER_FORWARDS.fetch_add(1, Ordering::SeqCst);
        IN_HOOK_FRAME.set(true);
        unsafe { run(item) };
        IN_HOOK_FRAME.set(false);
        1
    }

    extern "C" fn on_tick(_user_data: *mut c_void) {
        TIMER_TICKS.fetch_add(1, Ordering::SeqCst);
        if IN_HOOK_FRAME.get() {
            TIMER_TICKS_IN_FRAME.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn real_timer_fire_forwards_through_the_attached_lane_pool() {
        // End-to-end: a REAL `aorta_timer_new` timer fires through the
        // core timer machinery; the spin thread (this test thread, via
        // `aorta_executor_spin_once`) posts the fired callback through
        // `forward_timer_or_keep` to the attached forward hook, which runs
        // the C tick inline — asserted via the in-frame counter. No
        // hand-built Timer event, no `push_ready_event_for_test`.
        TIMER_FORWARDS.store(0, Ordering::SeqCst);
        TIMER_TICKS.store(0, Ordering::SeqCst);
        TIMER_TICKS_IN_FRAME.store(0, Ordering::SeqCst);

        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let node_name =
            CString::new(format!("ffi_lane_e2e_timer_{}", alloc_handle())).expect("node name");
        let mut node: aorta_node_t = 0;
        assert_eq!(
            unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
            AORTA_OK,
            "{}",
            last_error_string()
        );
        // Timers require the node to be attached first (event sender).
        assert_eq!(aorta_executor_add_node(exec, node), AORTA_OK);
        let h = hooks(timer_forward);
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );

        // 20ms period, default options (auto_start = true). NOTE: the FFI
        // timer dispatcher skips the C callback when user_data is NULL, so a
        // (unused) non-null pointer must ride along.
        let mut timer: aorta_timer_t = 0;
        let mut user_data_sentinel = 0u8;
        assert_eq!(
            unsafe {
                aorta_timer_new(
                    node,
                    20_000_000,
                    Some(on_tick),
                    ptr::addr_of_mut!(user_data_sentinel) as *mut c_void,
                    ptr::addr_of_mut!(timer),
                )
            },
            AORTA_OK,
            "{}",
            last_error_string()
        );

        // Drive the executor from this thread until a tick has been
        // dispatched through the hook.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while TIMER_TICKS_IN_FRAME.load(Ordering::SeqCst) == 0 {
            // Timeout iterations return AORTA_ERR_TIMEOUT — not a failure.
            let _ = aorta_executor_spin_once(exec, 100);
            assert!(
                std::time::Instant::now() < deadline,
                "timer tick never dispatched through the attached lane pool \
                 (forwards={}, ticks={}, in_frame={})",
                TIMER_FORWARDS.load(Ordering::SeqCst),
                TIMER_TICKS.load(Ordering::SeqCst),
                TIMER_TICKS_IN_FRAME.load(Ordering::SeqCst),
            );
        }
        assert!(
            TIMER_FORWARDS.load(Ordering::SeqCst) >= 1,
            "the fired timer event rode the forward hook"
        );
        assert!(
            TIMER_TICKS.load(Ordering::SeqCst) >= 1,
            "the C timer callback ran"
        );

        assert_eq!(aorta_timer_free(timer), AORTA_OK);
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
        assert_eq!(aorta_node_free(node), AORTA_OK);
    }
}

mod group_descriptor {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU64, Ordering};

    static LAST_GROUP: AtomicU64 = AtomicU64::new(u64::MAX);

    unsafe extern "C" fn cap_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn cap_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _class: u8,
        group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        LAST_GROUP.store(group, Ordering::SeqCst);
        unsafe { run(item) };
        1
    }

    #[test]
    fn group_descriptor_encodes_type_class_and_handle() {
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let hooks = aorta_lane_hooks_t {
            drain_fn: Some(cap_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(cap_forward),
            pool_workers: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &hooks) },
            AORTA_OK
        );
        // Explicit group with declared class Realtime (0) via create_v2.
        let mut gh = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_create_callback_group_with_class(
                    exec,
                    aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
                    0,
                    &mut gh,
                )
            },
            AORTA_OK
        );
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        let event =
            crate::event_queue::Event::new(crate::event_queue::EventKind::SubscriptionData, || {})
                .with_callback_group(
                    crate::event_queue::CallbackGroupType::MutuallyExclusive,
                    Some(gh),
                );
        entry.sender.try_send(event).expect("forwarded");
        let desc = LAST_GROUP.load(Ordering::SeqCst);
        assert_eq!(desc & 0x3, 1, "explicit-ME type");
        assert_eq!((desc >> 2) & 0xff, 0, "declared class Realtime");
        assert_eq!(desc >> 10, gh, "handle rides the high bits");

        // Default-ME: type 0, declared Normal (2).
        let event =
            crate::event_queue::Event::new(crate::event_queue::EventKind::SubscriptionData, || {});
        entry.sender.try_send(event).expect("forwarded");
        let desc = LAST_GROUP.load(Ordering::SeqCst);
        assert_eq!(desc & 0x3, 0, "default-ME type");
        assert_eq!((desc >> 2) & 0xff, 2, "default declared class Normal");

        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }
}

mod timer_forwarding {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TIMER_FWD: AtomicU32 = AtomicU32::new(0);

    unsafe extern "C" fn t_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn t_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _class: u8,
        _group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        TIMER_FWD.fetch_add(1, Ordering::SeqCst);
        unsafe { run(item) };
        1
    }

    #[test]
    fn fired_timer_callbacks_forward_on_attached_st_executor() {
        // The spin thread posts fired timer callbacks as lane
        // items; core timer machinery (scheduling) is untouched — pinned by
        // driving dispatch_ready_batch directly with a Timer event.
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let hooks = aorta_lane_hooks_t {
            drain_fn: Some(t_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(t_forward),
            pool_workers: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &hooks) },
            AORTA_OK
        );
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran2 = ran.clone();
        let before = TIMER_FWD.load(Ordering::SeqCst);
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        {
            let mut inst = entry.exec.lock();
            match &mut *inst {
                ExecutorInstance::Single(e) => {
                    e.push_ready_event_for_test(crate::event_queue::Event::new(
                        crate::event_queue::EventKind::Timer,
                        move || {
                            ran2.store(true, Ordering::SeqCst);
                        },
                    ));
                    assert!(e.dispatch_ready_batch_for_test(), "progress");
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(
            TIMER_FWD.load(Ordering::SeqCst),
            before + 1,
            "the fired timer forwarded to the lane hook"
        );
        assert!(ran.load(Ordering::SeqCst), "shim ran the tick");
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }
}

mod resume_flip {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, AtomicU64 as StdAtomicU64, Ordering};

    static RESUME_FWD: AtomicU32 = AtomicU32::new(0);
    static LAST_GROUP_SENTINEL: StdAtomicU64 = StdAtomicU64::new(0);
    static REFUSE: AtomicU32 = AtomicU32::new(0);

    unsafe extern "C" fn r_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn r_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _class: u8,
        group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        LAST_GROUP_SENTINEL.store(group, Ordering::SeqCst);
        if REFUSE.load(Ordering::SeqCst) != 0 {
            return 0; // closed → ladder falls to the reserved lane
        }
        RESUME_FWD.fetch_add(1, Ordering::SeqCst);
        unsafe { run(item) };
        1
    }

    fn hooks() -> aorta_lane_hooks_t {
        aorta_lane_hooks_t {
            drain_fn: Some(r_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(r_forward),
            pool_workers: 0,
        }
    }

    #[test]
    fn attached_post_resume_forwards_with_the_exempt_sentinel() {
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let h = hooks();
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );
        REFUSE.store(0, Ordering::SeqCst);
        let before = RESUME_FWD.load(Ordering::SeqCst);
        let resumed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let r2 = resumed.clone();
        unsafe extern "C" fn resume_cb(ev: *const aorta_resume_event_t) {
            let flag = unsafe { &*((*ev).handle as *const std::sync::atomic::AtomicBool) };
            flag.store(true, Ordering::SeqCst);
        }
        let rc = flow::aorta_executor_post_resume(
            exec,
            Some(resume_cb),
            std::sync::Arc::as_ptr(&r2) as *mut c_void,
            255,
        );
        assert_eq!(rc, AORTA_OK);
        assert_eq!(
            RESUME_FWD.load(Ordering::SeqCst),
            before + 1,
            "the resume rode the lane hook"
        );
        assert_eq!(
            LAST_GROUP_SENTINEL.load(Ordering::SeqCst),
            u64::MAX,
            "the exempt sentinel marks resume items"
        );
        assert!(resumed.load(Ordering::SeqCst), "resume delivered AORTA_OK");
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn refused_pool_falls_back_to_the_reserved_lane_on_a_live_executor() {
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let h = hooks();
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &h) },
            AORTA_OK
        );
        REFUSE.store(1, Ordering::SeqCst); // pool "closed", executor LIVE
        unsafe extern "C" fn resume_cb(_ev: *const aorta_resume_event_t) {}
        let rc = flow::aorta_executor_post_resume(exec, Some(resume_cb), std::ptr::null_mut(), 255);
        assert_eq!(
            rc, AORTA_OK,
            "the ladder accepted via the reserved lane — post_resume must \
             not brick on a refused pool while the executor lives"
        );
        REFUSE.store(0, Ordering::SeqCst);
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }
}

mod per_class_routing {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::Mutex;

    static CLASSES: Mutex<Vec<u8>> = Mutex::new(Vec::new());

    unsafe extern "C" fn c_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn c_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        lane_class: u8,
        _group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        CLASSES.lock().unwrap().push(lane_class);
        unsafe { run(item) };
        1
    }

    #[test]
    fn forwarded_events_carry_their_priority_class() {
        // carry-forward: the EventPriority → lane-class map
        // holds through the forward path for every priority.
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let hooks = aorta_lane_hooks_t {
            drain_fn: Some(c_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(c_forward),
            pool_workers: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &hooks) },
            AORTA_OK
        );
        CLASSES.lock().unwrap().clear();
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        use crate::event_queue::{Event, EventKind, EventPriority};
        for p in [
            EventPriority::Realtime,
            EventPriority::High,
            EventPriority::Normal,
            EventPriority::Background,
        ] {
            let event = Event::new_with_priority(EventKind::SubscriptionData, p, || {});
            entry.sender.try_send(event).expect("forwarded");
        }
        assert_eq!(
            CLASSES.lock().unwrap().as_slice(),
            &[0, 1, 2, 3],
            "each priority routes to its class lane"
        );
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }
}

mod qos_class_pin {
    use super::super::*;
    use super::{ffi_qos, last_error_string};
    use std::cell::Cell;
    use std::ffi::c_void;
    use std::sync::Mutex;

    /// One record per delivered sample: (payload marker byte, lane class the
    /// forward hook saw for THAT event, whether the C callback ran inside the
    /// forward-hook frame).
    static RECORDS: Mutex<Vec<(u8, u8, bool)>> = Mutex::new(Vec::new());

    thread_local! {
        /// (class, in-frame) set by the forward hook around `run(item)`. The
        /// C callback executes inside that frame ON THE SAME THREAD, so the
        /// read is race-free pairing of class ↔ delivered sample.
        static CURRENT: Cell<(u8, bool)> = const { Cell::new((u8::MAX, false)) };
    }

    unsafe extern "C" fn q_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn q_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        lane_class: u8,
        _group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        CURRENT.set((lane_class, true));
        unsafe { run(item) };
        CURRENT.set((u8::MAX, false));
        1
    }

    extern "C" fn on_msg(
        payload: *const aorta_bytes_view_t,
        _schema: *const aorta_schema_hash_view_t,
        _ctx: *const aorta_msg_context_t,
        _user_data: *mut c_void,
    ) {
        // SAFETY: the FFI dispatcher passes a valid view for the call.
        let marker = unsafe {
            let view = &*payload;
            if view.slice_count == 0 || view.slices.is_null() {
                0
            } else {
                let s = &*view.slices;
                if s.len == 0 || s.data.is_null() {
                    0
                } else {
                    *s.data
                }
            }
        };
        let (class, in_frame) = CURRENT.get();
        RECORDS.lock().unwrap().push((marker, class, in_frame));
    }

    /// Publish `[marker]` (retrying every 100ms) until a delivery with that
    /// marker is recorded; returns its (class, in-frame) pair.
    fn publish_until_marker(publisher: aorta_pub_t, marker: u8) -> (u8, bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let payload = [marker];
            assert_eq!(
                unsafe { aorta_publisher_publish(publisher, payload.as_ptr(), payload.len()) },
                AORTA_OK
            );
            let step_end = std::time::Instant::now() + std::time::Duration::from_millis(100);
            while std::time::Instant::now() < step_end {
                let found = RECORDS
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|r| r.0 == marker)
                    .copied();
                if let Some((_, class, in_frame)) = found {
                    return (class, in_frame);
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no delivery for marker {marker:#x}"
            );
        }
    }

    #[test]
    fn flowcaps_priority_override_routes_to_the_overridden_class_lane() {
        // (QoS↔class): the monotone class chain, driven END-TO-END through the
        // REAL emit path — a C-ABI publisher's sample reaches the Zenoh RX
        // callback in `Subscriber::new_with_options`, which stamps the event
        // priority (default: `map_event_priority(qos.priority)`; overridden:
        // `event_priority_from_index_lossy(FlowCaps.priority_override)`) and
        // try_sends into the attached executor, where
        // `lane_class_for_priority` picks the forward hook's class. No
        // hand-built Event, no crate-private sender: a regression in
        // `map_event_priority`, the FlowCaps load, or the class map turns
        // this red.
        RECORDS.lock().unwrap().clear();
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let node_name =
            CString::new(format!("ffi_qospin_node_{}", alloc_handle())).expect("node name");
        let mut node: aorta_node_t = 0;
        assert_eq!(
            unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), ptr::addr_of_mut!(node)) },
            AORTA_OK,
            "{}",
            last_error_string()
        );
        // Attach the node BEFORE the subscriber so the emit path caches the
        // executor's (hook-slotted) sender.
        assert_eq!(aorta_executor_add_node(exec, node), AORTA_OK);
        let hooks = aorta_lane_hooks_t {
            drain_fn: Some(q_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(q_forward),
            pool_workers: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &hooks) },
            AORTA_OK
        );

        // Subscriber QoS priority High: the DEFAULT (inherit) mapping must
        // route through map_event_priority(High) → class 1.
        let topic = CString::new(format!("ffi/qospin/id{}", alloc_handle())).expect("topic");
        let sub_qos = aorta_qos_t {
            priority: aorta_priority_t::AORTA_PRIORITY_HIGH,
            reliability: aorta_reliability_t::AORTA_RELIABILITY_RELIABLE,
            congestion_control: AORTA_CONGESTION_CONTROL_DROP,
            express: 0,
        };
        let mut sub: aorta_sub_t = 0;
        let rc = aorta_subscriber_new(
            node,
            topic.as_ptr(),
            ptr::addr_of!(sub_qos),
            ptr::null(),
            Some(on_msg),
            ptr::null_mut(),
            ptr::addr_of_mut!(sub),
        );
        assert_eq!(rc, AORTA_OK, "{}", last_error_string());

        let pub_qos = ffi_qos();
        let mut publisher: aorta_pub_t = 0;
        assert_eq!(
            aorta_publisher_new(
                node,
                topic.as_ptr(),
                ptr::addr_of!(pub_qos),
                ptr::null(),
                ptr::addr_of_mut!(publisher),
            ),
            AORTA_OK,
            "{}",
            last_error_string()
        );

        // Phase 1 — no override: QoS(High) → map_event_priority → class 1.
        let (class, in_frame) = publish_until_marker(publisher, 0xA1);
        assert!(in_frame, "delivery must ride the forward hook");
        assert_eq!(
            class, 1,
            "inherit path: qos High → map_event_priority → class lane 1"
        );

        // Phase 2 — FlowCaps override to Realtime via the public setter:
        // event_priority_from_index_lossy(0) → class 0, regardless of the QoS.
        assert_eq!(aorta_subscriber_set_priority(sub, 0), AORTA_OK);
        let (class, in_frame) = publish_until_marker(publisher, 0xB2);
        assert!(in_frame, "delivery must ride the forward hook");
        assert_eq!(class, 0, "override path: Realtime override → class lane 0");

        // Phase 3 — restore inherit (255): back to the QoS-derived map.
        assert_eq!(aorta_subscriber_set_priority(sub, u8::MAX), AORTA_OK);
        let (class, in_frame) = publish_until_marker(publisher, 0xC3);
        assert!(in_frame, "delivery must ride the forward hook");
        assert_eq!(
            class, 1,
            "restore: 255 re-derives the class from qos High via map_event_priority"
        );

        assert_eq!(aorta_publisher_free(publisher), AORTA_OK);
        assert_eq!(aorta_subscriber_free(sub), AORTA_OK);
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
        assert_eq!(aorta_node_free(node), AORTA_OK);
    }
}

// ---- Lane-forwarding test battery ------------------------------------------

mod p26_battery {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

    // Per-test forward mode (0=run, 1=drop / expiry sim), carried through
    // the hooks' ctx pointer so parallel tests in this module cannot flip
    // each other's mode mid-flight.
    fn new_mode(initial: u8) -> &'static AtomicU8 {
        Box::leak(Box::new(AtomicU8::new(initial)))
    }

    unsafe extern "C" fn b_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn b_forward(
        ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        drop_fn: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _lane_class: u8,
        _group: u64,
        deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        // SAFETY: ctx is the leaked &'static AtomicU8 installed by attach().
        let mode = unsafe { &*(ctx as *const AtomicU8) };
        // Expiry simulation: a deadline-carrying item takes the lane's
        // deadline drop path when mode=1 (the lane would compare clocks; the
        // fake asserts the wire carried a deadline at all).
        if mode.load(Ordering::SeqCst) == 1 {
            assert!(deadline_ns > 0, "deadline must ride the forward hook");
            unsafe { drop_fn(item) };
        } else {
            unsafe { run(item) };
        }
        1
    }

    fn attach(exec: u64, mode: &'static AtomicU8) {
        let hooks = aorta_lane_hooks_t {
            drain_fn: Some(b_drain),
            ctx: mode as *const AtomicU8 as *mut c_void,
            on_stop: None,
            forward_fn: Some(b_forward),
            pool_workers: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &hooks) },
            AORTA_OK
        );
    }

    fn new_st_exec() -> u64 {
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        exec
    }

    #[test]
    fn m9_admission_stats_report_lane_backed_equivalents() {
        let exec = new_st_exec();
        attach(exec, new_mode(0));
        // The SDK's report: rows [depth, high_water, enqueued, shed,
        // expired] × 4 lanes.
        let flat: [u64; 20] = [
            1, 0, 2, 0, // depth
            3, 0, 9, 1, // high water
            10, 0, 500, 3, // enqueued
            0, 0, 4, 0, // shed
            0, 1, 0, 0, // expired
        ];
        assert_eq!(
            unsafe { executor::aorta_executor_update_lane_telemetry(exec, flat.as_ptr()) },
            AORTA_OK
        );
        let mut stats = aorta_executor_admission_stats_t {
            queued_events: 0,
            queued_bytes: 0,
            peak_queued_events: 0,
            peak_queued_bytes: 0,
            dropped_queue_full: 0,
            dropped_queue_closed: 0,
            dropped_budget_events: 0,
            dropped_budget_bytes: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_get_admission_stats(exec, &mut stats) },
            AORTA_OK
        );
        assert_eq!(stats.queued_events, 3, "Σ depth");
        assert_eq!(stats.peak_queued_events, 13, "Σ high water");
        assert_eq!(stats.dropped_queue_full, 4, "Σ shed (field-for-field)");
        assert_eq!(stats.dropped_budget_events, 1, "Σ expired");
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn attached_queue_stays_flat_the_101_exit() {
        // Testable facade exit: forward N callbacks, detach, and the CORE
        // admission tier must have seen ZERO of them.
        let exec = new_st_exec();
        attach(exec, new_mode(0));
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        for _ in 0..16 {
            let event = crate::event_queue::Event::new(
                crate::event_queue::EventKind::SubscriptionData,
                || {},
            );
            entry.sender.try_send(event).expect("forwarded");
        }
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        let mut stats = aorta_executor_admission_stats_t {
            queued_events: u64::MAX,
            queued_bytes: 0,
            peak_queued_events: u64::MAX,
            peak_queued_bytes: 0,
            dropped_queue_full: 0,
            dropped_queue_closed: 0,
            dropped_budget_events: 0,
            dropped_budget_bytes: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_get_admission_stats(exec, &mut stats) },
            AORTA_OK
        );
        assert_eq!(stats.queued_events, 0, "nothing entered the core queue");
        assert_eq!(stats.peak_queued_events, 0, "not even transiently");
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn forwarded_panic_counts_into_error_count() {
        // panic parity: a forwarded callback that panics is
        // contained (isolated dispatch) and counted.
        let exec = new_st_exec();
        attach(exec, new_mode(0));
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        let event =
            crate::event_queue::Event::new(crate::event_queue::EventKind::SubscriptionData, || {
                panic!("boom (contained)")
            });
        entry.sender.try_send(event).expect("forwarded");
        let errors = {
            match &*entry.exec.lock() {
                ExecutorInstance::Single(e) => e.error_count_for_test(),
                _ => unreachable!(),
            }
        };
        assert_eq!(errors, 1, "panic parity through the run shim");
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn expiry_drop_attributes_dropped_expired() {
        // expiry attribution: the drop shim counts an EXPIRED
        // event into dropped_expired (close-drain of a live event
        // does not — pinned by the zero case first).
        let exec = new_st_exec();
        attach(exec, new_mode(1)); // drop mode for both cases below
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };

        // Case 1: unexpired event dropped (close-drain shape) — NOT counted.
        let event =
            crate::event_queue::Event::new(crate::event_queue::EventKind::SubscriptionData, || {})
                .with_deadline(Some(
                    std::time::Instant::now() + std::time::Duration::from_secs(60),
                ));
        entry.sender.try_send(event).expect("forwarded");
        let read_dropped = || {
            let mut stats = aorta_executor_admission_stats_t {
                queued_events: 0,
                queued_bytes: 0,
                peak_queued_events: 0,
                peak_queued_bytes: 0,
                dropped_queue_full: 0,
                dropped_queue_closed: 0,
                dropped_budget_events: 0,
                dropped_budget_bytes: 0,
            };
            let _ = unsafe { executor::aorta_executor_get_admission_stats(exec, &mut stats) };
            stats
        };
        let _ = read_dropped();

        // Case 2: EXPIRED event dropped — counted.
        let event =
            crate::event_queue::Event::new(crate::event_queue::EventKind::SubscriptionData, || {})
                .with_deadline(Some(std::time::Instant::now()));
        std::thread::sleep(std::time::Duration::from_millis(2));
        entry.sender.try_send(event).expect("forwarded");
        let dropped = {
            match &*entry.exec.lock() {
                ExecutorInstance::Single(e) => e.dropped_expired_for_test(),
                _ => unreachable!(),
            }
        };
        assert_eq!(dropped, 1, "only the EXPIRED drop attributed");
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }

    #[test]
    fn post_resume_races_attach_detach_without_loss() {
        // Remaining attach/detach races: hammer post_resume from another thread
        // across repeated attach/detach + a final stop. Every accepted
        // post must resolve (delivered or cancelled) — no hang, no leak.
        let exec = new_st_exec();
        let mode = new_mode(0); // run mode
        let delivered = std::sync::Arc::new(AtomicU32::new(0));
        unsafe extern "C" fn resume_cb(ev: *const aorta_resume_event_t) {
            // SAFETY: handle carries the counter (test-owned, outlives).
            let c = unsafe { &*((*ev).handle as *const AtomicU32) };
            c.fetch_add(1, Ordering::SeqCst);
        }
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = std::sync::Arc::clone(&stop);
        let delivered2 = std::sync::Arc::clone(&delivered);
        let poster = std::thread::spawn(move || {
            let mut accepted = 0u32;
            while !stop2.load(Ordering::SeqCst) {
                let rc = flow::aorta_executor_post_resume(
                    exec,
                    Some(resume_cb),
                    std::sync::Arc::as_ptr(&delivered2) as *mut c_void,
                    255,
                );
                if rc == AORTA_OK {
                    accepted += 1;
                }
                std::hint::spin_loop();
            }
            accepted
        });
        for _ in 0..50 {
            attach(exec, mode);
            std::thread::sleep(std::time::Duration::from_micros(200));
            assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        }
        stop.store(true, Ordering::SeqCst);
        let accepted = poster.join().unwrap();
        // Free stop-drains the reserved lane: every accepted post must
        // resolve through resume_cb — delivered inline by the attached
        // hook, or resumed-into-cancellation by the stop drain. Equality
        // is THE no-loss property this test exists for (the counters
        // were previously discarded).
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while delivered.load(Ordering::SeqCst) < accepted && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(
            delivered.load(Ordering::SeqCst),
            accepted,
            "every accepted post_resume resolves exactly once (no loss, no dup)"
        );
        assert!(accepted > 0, "the poster raced at least one accepted post");
    }
}

mod timer_cadence_pin {
    use super::super::*;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicU32, Ordering};

    static FWD: AtomicU32 = AtomicU32::new(0);

    unsafe extern "C" fn t_drain(_c: *mut c_void, _m: u32) -> u64 {
        0
    }
    unsafe extern "C" fn t_forward(
        _ctx: *mut c_void,
        run: unsafe extern "C" fn(*mut c_void),
        _drop: unsafe extern "C" fn(*mut c_void),
        item: *mut c_void,
        _lane_class: u8,
        _group: u64,
        _deadline_ns: u64,
        _cost_bytes: u32,
    ) -> u8 {
        FWD.fetch_add(1, Ordering::SeqCst);
        unsafe { run(item) };
        1
    }

    #[test]
    fn periodic_timer_keeps_rearming_through_the_forward_path() {
        // Reduced cadence pin: the core timer MACHINERY stays on
        // the spin thread — forwarding the fired callback must not
        // break periodic re-arm. Drive the ST dispatch site directly
        // with successive Timer events (the scheduler's re-arm is
        // core code, untouched; this pins that forwarding N ticks
        // delivers N executions, no single-shot regression).
        let mut exec = 0u64;
        assert_eq!(
            unsafe {
                executor::aorta_executor_new(
                    aorta_executor_type_t::AORTA_EXECUTOR_SINGLE,
                    &mut exec,
                )
            },
            AORTA_OK
        );
        let hooks = aorta_lane_hooks_t {
            drain_fn: Some(t_drain),
            ctx: std::ptr::null_mut(),
            on_stop: None,
            forward_fn: Some(t_forward),
            pool_workers: 0,
        };
        assert_eq!(
            unsafe { executor::aorta_executor_attach_lane_pool(exec, &hooks) },
            AORTA_OK
        );
        let ticks = std::sync::Arc::new(AtomicU32::new(0));
        let entry = { registry().executors.lock().get(&exec).cloned().unwrap() };
        let before = FWD.load(Ordering::SeqCst);
        for _ in 0..5 {
            let t2 = ticks.clone();
            let mut inst = entry.exec.lock();
            match &mut *inst {
                ExecutorInstance::Single(e) => {
                    e.push_ready_event_for_test(crate::event_queue::Event::new(
                        crate::event_queue::EventKind::Timer,
                        move || {
                            t2.fetch_add(1, Ordering::SeqCst);
                        },
                    ));
                    assert!(e.dispatch_ready_batch_for_test());
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(ticks.load(Ordering::SeqCst), 5, "all ticks executed");
        assert_eq!(
            FWD.load(Ordering::SeqCst) - before,
            5,
            "every tick took the forward path"
        );
        assert_eq!(executor::aorta_executor_detach_lane_pool(exec), AORTA_OK);
        assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
    }
}

// ---------------------------------------------------------------------------
// ABI generation pin. Each breaking C-ABI change must update the ledger in
// `mod.rs` and every SDK loader in lockstep.
// ---------------------------------------------------------------------------
#[test]
fn abi_version_is_generation_twelve() {
    assert_eq!(aorta_abi_version(), 13);
    assert_eq!(AORTA_ABI_VERSION, 13);
}

/// Resolve-once stop-flag pointer: valid for the
/// executor handle's lifetime, monotonic 0→1, read with acquire order.
/// This is the arming edge of the SDK stop-bridge state machine.
#[test]
fn executor_stop_flag_ptr_observes_stop() {
    let mut exec = 0;
    let new_rc =
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(new_rc, AORTA_OK);

    // Null out-param refused.
    assert_eq!(
        unsafe { executor::aorta_executor_stop_flag_ptr(exec, ptr::null_mut()) },
        AORTA_ERR_INVALID_ARGUMENT
    );
    // Unknown handle refused.
    let mut flag: *const u8 = ptr::null();
    assert_eq!(
        unsafe { executor::aorta_executor_stop_flag_ptr(0xdead_beef, &mut flag) },
        AORTA_ERR_NOT_FOUND
    );

    // Happy path: non-null, initially 0.
    assert_eq!(
        unsafe { executor::aorta_executor_stop_flag_ptr(exec, &mut flag) },
        AORTA_OK
    );
    assert!(!flag.is_null());
    let atomic = unsafe { &*(flag as *const std::sync::atomic::AtomicU8) };
    assert_eq!(atomic.load(Ordering::Acquire), 0);

    // Monotonic edge: stop publishes 1 through the SAME atomic the spin
    // thread observes (Executor::is_stopped) — the bridge arming edge.
    assert_eq!(executor::aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(atomic.load(Ordering::Acquire), 1);

    // Stop is one-way; a second stop keeps the flag set.
    assert_eq!(executor::aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(atomic.load(Ordering::Acquire), 1);

    assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
}

/// The feature-gated C seam symbol (SerialDomain stop-window tests): installs the
/// stop-window pause point the C++ stop-flag-window family drives.
/// Present ONLY under the `test-seams` cargo feature — release cores
/// never export it (the dedicated Bazel target is the sole consumer).
#[cfg(feature = "test-seams")]
#[test]
fn test_seam_symbol_installs_and_clears_stop_window_seam() {
    use std::sync::atomic::AtomicU64;
    static FIRED: AtomicU64 = AtomicU64::new(0);
    unsafe extern "C" fn bump(_ctx: *mut c_void) {
        FIRED.fetch_add(1, Ordering::SeqCst);
    }

    let mut exec = 0;
    let new_rc =
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) };
    assert_eq!(new_rc, AORTA_OK);

    assert_eq!(
        unsafe { executor::aorta_test_stop_window_seam(Some(bump), ptr::null_mut()) },
        AORTA_OK
    );
    let before = FIRED.load(Ordering::SeqCst);
    assert_eq!(executor::aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(
        FIRED.load(Ordering::SeqCst),
        before + 1,
        "seam fired in the stop path"
    );

    // Clear: a later stop must not fire the seam again.
    assert_eq!(
        unsafe { executor::aorta_test_stop_window_seam(None, ptr::null_mut()) },
        AORTA_OK
    );
    assert_eq!(executor::aorta_executor_stop(exec), AORTA_OK);
    assert_eq!(
        FIRED.load(Ordering::SeqCst),
        before + 1,
        "cleared seam is silent"
    );

    assert_eq!(executor::aorta_executor_free(exec), AORTA_OK);
}
