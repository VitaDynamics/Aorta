// Lint exception: same rationale as
// `ffi_action_tests.rs` — integration tests deliberately exercise the
// FFI extern "C" surface with locally-constructed arguments.
#![allow(clippy::undocumented_unsafe_blocks)]

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::ffi::c_api::{
    aorta_bytes_view_t, aorta_client_call, aorta_client_free, aorta_client_new_with_options,
    aorta_client_options_t, aorta_client_t, aorta_node_free, aorta_node_new, aorta_node_t,
    aorta_owned_bytes_free, aorta_owned_bytes_slice_t, aorta_owned_bytes_t,
    aorta_owned_bytes_view_t, aorta_rpc_overflow_policy_t, aorta_schema_attach_policy_t,
    aorta_service_free, aorta_service_new_with_options, aorta_service_options_t,
    aorta_service_request_reply, aorta_service_request_t, aorta_service_t, AORTA_OK,
    AORTA_OPTIONAL_BOOL_INHERIT,
};
use crate::test_support::{EnvVarGuard, ENV_LOCK};

static CALLBACK_COUNT: AtomicUsize = AtomicUsize::new(0);
static CALLBACK_RC: AtomicI32 = AtomicI32::new(i32::MIN);
static UNIQUE_ID: AtomicU64 = AtomicU64::new(0);

extern "C" fn empty_response_cb(
    _payload: *const aorta_bytes_view_t,
    request: aorta_service_request_t,
    _user_data: *mut c_void,
) {
    CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
    let rc = unsafe { aorta_service_request_reply(request, ptr::null()) };
    CALLBACK_RC.store(rc, Ordering::SeqCst);
}

#[test]
fn ffi_service_allows_empty_response() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _normalize_case_guard = EnvVarGuard::remove("AORTA_KEY_NORMALIZE_CASE");

    let unique_id = UNIQUE_ID.fetch_add(1, Ordering::SeqCst);
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    let suffix = format!("{}_{}_{}", std::process::id(), unique_id, now_ns);
    let group = format!("f{}{}", std::process::id(), unique_id);
    let node_name = CString::new(format!("ffi_empty_response_node_{}", suffix)).unwrap();
    let service_name = CString::new(format!("ffi_empty_response_service_{}", suffix)).unwrap();

    let mut node: aorta_node_t = 0;
    let rc = {
        let _group_guard = EnvVarGuard::set("AORTA_GROUP", &group);
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), &mut node) }
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(node, 0);

    let mut service: aorta_service_t = 0;
    let service_opts = aorta_service_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_INHERIT,
        callback_group:
            crate::ffi::c_api::aorta_callback_group_t::AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
        callback_group_handle: 0,
        deadline_ns: 0,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
        service_schema_bfbs: ptr::null(),
        service_schema_bfbs_len: 0,
        request_root_type: ptr::null(),
        request_root_type_len: 0,
        response_root_type: ptr::null(),
        response_root_type_len: 0,
        rpc_overflow_policy: aorta_rpc_overflow_policy_t::AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
        rpc_overflow_block_timeout_ms: 0,
    };
    let rc = unsafe {
        aorta_service_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::addr_of!(service_opts),
            Some(empty_response_cb),
            ptr::null_mut(),
            &mut service,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(service, 0);

    std::thread::sleep(Duration::from_millis(500));

    let mut client: aorta_client_t = 0;
    let client_opts = aorta_client_options_t {
        schema_attach_policy: aorta_schema_attach_policy_t::AORTA_SCHEMA_ATTACH_INHERIT,
        timeout_ms: 2000,
        enforce_single_provider: AORTA_OPTIONAL_BOOL_INHERIT,
        allow_retry: AORTA_OPTIONAL_BOOL_INHERIT,
        multi_reply_check_window_ms: 0,
        service_schema_hash_hex: ptr::null(),
        service_schema_hash_len: 0,
    };
    let rc = unsafe {
        aorta_client_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::addr_of!(client_opts),
            &mut client,
        )
    };
    assert_eq!(rc, AORTA_OK);
    assert_ne!(client, 0);

    CALLBACK_COUNT.store(0, Ordering::SeqCst);
    CALLBACK_RC.store(i32::MIN, Ordering::SeqCst);

    let request = [0x01u8, 0x02, 0x03];
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
    assert_eq!(response.view.slice_count, 0);

    let cb_rc = CALLBACK_RC.load(Ordering::SeqCst);
    assert_eq!(cb_rc, AORTA_OK);
    assert!(CALLBACK_COUNT.load(Ordering::SeqCst) > 0);

    unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(response)) };
    assert_eq!(aorta_client_free(client), AORTA_OK);
    assert_eq!(aorta_service_free(service), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

static TRACE_CTX_RC: AtomicI32 = AtomicI32::new(i32::MIN);
static TRACE_CTX_SPAN_ID: AtomicU64 = AtomicU64::new(0);
static TRACE_CTX_ID_NONZERO: AtomicUsize = AtomicUsize::new(0);

extern "C" fn trace_context_capture_cb(
    _payload: *const aorta_bytes_view_t,
    request: aorta_service_request_t,
    _user_data: *mut c_void,
) {
    let mut trace_id = [0u8; 16];
    let mut span_id: u64 = 0;
    let got = unsafe {
        crate::ffi::c_api::aorta_service_request_trace_context(
            request,
            trace_id.as_mut_ptr(),
            &mut span_id,
        )
    };
    TRACE_CTX_RC.store(i32::from(got), Ordering::SeqCst);
    TRACE_CTX_SPAN_ID.store(span_id, Ordering::SeqCst);
    TRACE_CTX_ID_NONZERO.store(usize::from(trace_id != [0u8; 16]), Ordering::SeqCst);
    let rc = unsafe { aorta_service_request_reply(request, ptr::null()) };
    CALLBACK_RC.store(rc, Ordering::SeqCst);
}

/// The deferred-reply trace accessor: `0` (outputs untouched) while tracing is
/// disabled, `1` + a usable `(trace_id, span_id)` once tracing is on — read
/// from inside the service callback against the live request handle.
#[test]
fn ffi_service_request_trace_context_follows_the_gate() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _normalize_case_guard = EnvVarGuard::remove("AORTA_KEY_NORMALIZE_CASE");

    let unique_id = UNIQUE_ID.fetch_add(1, Ordering::SeqCst);
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    let suffix = format!("{}_{}_{}", std::process::id(), unique_id, now_ns);
    let group = format!("t{}{}", std::process::id(), unique_id);
    let node_name = CString::new(format!("ffi_trace_ctx_node_{}", suffix)).unwrap();
    let service_name = CString::new(format!("ffi_trace_ctx_service_{}", suffix)).unwrap();

    let mut node: aorta_node_t = 0;
    let rc = {
        let _group_guard = EnvVarGuard::set("AORTA_GROUP", &group);
        unsafe { aorta_node_new(node_name.as_ptr(), ptr::null(), &mut node) }
    };
    assert_eq!(rc, AORTA_OK);

    let mut service: aorta_service_t = 0;
    let rc = unsafe {
        aorta_service_new_with_options(
            node,
            service_name.as_ptr(),
            ptr::null(),
            Some(trace_context_capture_cb),
            ptr::null_mut(),
            &mut service,
        )
    };
    assert_eq!(rc, AORTA_OK);

    std::thread::sleep(Duration::from_millis(500));

    let mut client: aorta_client_t = 0;
    let rc = unsafe {
        aorta_client_new_with_options(node, service_name.as_ptr(), ptr::null(), &mut client)
    };
    assert_eq!(rc, AORTA_OK);

    let request = [0xAAu8, 0xBB];
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

    let call = |label: &str| {
        TRACE_CTX_RC.store(i32::MIN, Ordering::SeqCst);
        TRACE_CTX_SPAN_ID.store(0, Ordering::SeqCst);
        TRACE_CTX_ID_NONZERO.store(0, Ordering::SeqCst);
        let mut response = aorta_owned_bytes_t::default();
        let rc = unsafe {
            aorta_client_call(
                client,
                ptr::addr_of!(request_view),
                ptr::addr_of_mut!(response),
            )
        };
        assert_eq!(rc, AORTA_OK, "client call must succeed ({label})");
        unsafe { aorta_owned_bytes_free(ptr::addr_of_mut!(response)) };
    };

    // Serialize the master-gate toggle against the rest of the trace suite.
    let _trace_guard = crate::trace_test_support::lock();

    crate::trace::set_trace_enabled(false);
    call("tracing off");
    assert_eq!(
        TRACE_CTX_RC.load(Ordering::SeqCst),
        0,
        "no trace context may be reported while tracing is disabled"
    );

    crate::trace::set_trace_enabled(true);
    call("tracing on");
    crate::trace::set_trace_enabled(false);
    assert_eq!(TRACE_CTX_RC.load(Ordering::SeqCst), 1);
    assert_ne!(
        TRACE_CTX_SPAN_ID.load(Ordering::SeqCst),
        0,
        "the reported span id must be the minted server root"
    );
    assert_eq!(
        TRACE_CTX_ID_NONZERO.load(Ordering::SeqCst),
        1,
        "the reported trace id must be non-zero"
    );

    assert_eq!(aorta_client_free(client), AORTA_OK);
    assert_eq!(aorta_service_free(service), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}
