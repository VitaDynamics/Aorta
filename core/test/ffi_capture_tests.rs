//! C-ABI tests for the capture + schema-resolver surface. Plain sync `#[test]`
//! (the FFI `block_on` must not run nested inside a tokio runtime).

// FFI integration tests exercise the extern "C" surface with locally
// constructed args; per-block SAFETY comments would be the same prose repeated.
// The lib-root deny keeps production honest; test files opt out as a class
// (same convention as ffi_action_tests.rs / ffi_service_tests.rs).
#![allow(clippy::undocumented_unsafe_blocks)]

use crate::ffi::c_api::{
    aorta_capture_selector_t, aorta_capture_subscriber_free, aorta_capture_subscriber_new,
    aorta_captured_sample_t, aorta_node_free, aorta_node_new, aorta_publisher_free,
    aorta_publisher_new, aorta_publisher_publish, aorta_resolved_schema_free,
    aorta_resolved_schema_t, aorta_schema_hash_view_t, aorta_schema_resolver_free,
    aorta_schema_resolver_new, aorta_schema_resolver_opts_t, aorta_schema_resolver_resolve_by_hash,
    aorta_schema_resolver_resolve_by_key, AORTA_CAPTURE_KIND_LIVE, AORTA_CAPTURE_SELECTOR_TOPIC,
    AORTA_ERR_INVALID_ARGUMENT, AORTA_ERR_NOT_FOUND, AORTA_OK,
};
use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Default)]
struct CapSink {
    key: Mutex<String>,
    suffix: Mutex<String>,
    payload: Mutex<Vec<u8>>,
    kind: AtomicI32,
    has_schema_hash: AtomicBool,
    recv_nonzero: AtomicBool,
    count: AtomicUsize,
}

extern "C" fn on_capture(
    sample: *const aorta_captured_sample_t,
    is_snapshot_done: bool,
    _done_reason: i32,
    user_data: *mut c_void,
) {
    // `is_snapshot_done` is authoritative; a live subscriber never sets it.
    if is_snapshot_done || sample.is_null() {
        return;
    }
    // SAFETY: `user_data` is the `&CapSink` we passed to subscriber_new; `sample`
    // is valid for this call.
    let sink = unsafe { &*(user_data as *const CapSink) };
    let s = unsafe { &*sample };
    let key = unsafe { std::slice::from_raw_parts(s.key.data, s.key.len) };
    *sink.key.lock().unwrap() = String::from_utf8_lossy(key).into_owned();
    let suffix = unsafe { std::slice::from_raw_parts(s.key_suffix.data, s.key_suffix.len) };
    *sink.suffix.lock().unwrap() = String::from_utf8_lossy(suffix).into_owned();
    if s.payload.slice_count >= 1 && !s.payload.slices.is_null() {
        let sl = unsafe { &*s.payload.slices };
        let payload = unsafe { std::slice::from_raw_parts(sl.data, sl.len) };
        *sink.payload.lock().unwrap() = payload.to_vec();
    }
    sink.kind.store(s.kind, Ordering::SeqCst);
    sink.has_schema_hash
        .store(s.has_schema_hash, Ordering::SeqCst);
    sink.recv_nonzero
        .store(s.recv_time_ns > 0, Ordering::SeqCst);
    sink.count.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn ffi_capture_subscriber_receives_live_publish() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env = schema_attach_never_guard();

    let group = CString::new("default").unwrap();
    let sub_name = CString::new("ffi_cap_sub").unwrap();
    let pub_name = CString::new("ffi_cap_pub").unwrap();
    let mut sub_node = 0u64;
    let mut pub_node = 0u64;
    // SAFETY: NUL-terminated names + writable out handles.
    assert_eq!(
        unsafe { aorta_node_new(sub_name.as_ptr(), group.as_ptr(), &mut sub_node) },
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_node_new(pub_name.as_ptr(), group.as_ptr(), &mut pub_node) },
        AORTA_OK
    );

    let sink = Box::new(CapSink::default());
    let sink_ptr = (&*sink as *const CapSink) as *mut c_void;

    let pattern = CString::new("ffi_cap/chatter").unwrap();
    let selector = aorta_capture_selector_t {
        kind: AORTA_CAPTURE_SELECTOR_TOPIC,
        pattern: pattern.as_ptr(),
    };
    let mut cap_handle = 0u64;
    assert_eq!(
        aorta_capture_subscriber_new(
            sub_node,
            &selector,
            1,
            Some(on_capture),
            sink_ptr,
            &mut cap_handle
        ),
        AORTA_OK
    );

    std::thread::sleep(Duration::from_millis(300));

    let topic = CString::new("ffi_cap/chatter").unwrap();
    let mut pub_handle = 0u64;
    assert_eq!(
        aorta_publisher_new(
            pub_node,
            topic.as_ptr(),
            ptr::null(),
            ptr::null(),
            &mut pub_handle
        ),
        AORTA_OK
    );
    let data = b"ffi hello";
    // SAFETY: valid publisher handle + contiguous buffer.
    assert_eq!(
        unsafe { aorta_publisher_publish(pub_handle, data.as_ptr(), data.len()) },
        AORTA_OK
    );

    let deadline = Instant::now() + Duration::from_secs(3);
    while sink.count.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        sink.count.load(Ordering::SeqCst) > 0,
        "FFI capture should receive the live publish"
    );
    assert_eq!(*sink.suffix.lock().unwrap(), "pub/ffi_cap/chatter");
    assert!(sink.key.lock().unwrap().ends_with("/pub/ffi_cap/chatter"));
    assert_eq!(*sink.payload.lock().unwrap(), b"ffi hello");
    assert_eq!(sink.kind.load(Ordering::SeqCst), AORTA_CAPTURE_KIND_LIVE);
    assert!(!sink.has_schema_hash.load(Ordering::SeqCst));
    assert!(sink.recv_nonzero.load(Ordering::SeqCst));

    // Free the subscriber (drains in-flight callbacks) BEFORE `sink` drops.
    assert_eq!(aorta_capture_subscriber_free(cap_handle), AORTA_OK);
    assert_eq!(aorta_publisher_free(pub_handle), AORTA_OK);
    assert_eq!(aorta_node_free(pub_node), AORTA_OK);
    assert_eq!(aorta_node_free(sub_node), AORTA_OK);
}

#[test]
fn ffi_schema_resolver_error_and_lifecycle_paths() {
    let _guard = ENV_LOCK.lock().expect("lock");

    let group = CString::new("default").unwrap();
    let name = CString::new("ffi_resolver_node").unwrap();
    let mut node = 0u64;
    // SAFETY: NUL-terminated name + writable out.
    assert_eq!(
        unsafe { aorta_node_new(name.as_ptr(), group.as_ptr(), &mut node) },
        AORTA_OK
    );

    let opts = aorta_schema_resolver_opts_t {
        cache_max_entries: 16,
        by_hash_timeout_ms: 150,
        by_key_timeout_ms: 150,
    };
    let mut resolver = 0u64;
    assert_eq!(
        aorta_schema_resolver_new(node, &opts, &mut resolver),
        AORTA_OK
    );

    let mut out = std::mem::MaybeUninit::<aorta_resolved_schema_t>::zeroed();

    // Non-hex hash view → INVALID_ARGUMENT.
    let bad = b"not-a-hash";
    let bad_view = aorta_schema_hash_view_t {
        hex: bad.as_ptr(),
        len: bad.len(),
    };
    assert_eq!(
        aorta_schema_resolver_resolve_by_hash(resolver, bad_view, out.as_mut_ptr()),
        AORTA_ERR_INVALID_ARGUMENT
    );

    // Unknown key → NOT_FOUND (network query drains with no replies).
    let missing = CString::new("pub/does/not/exist").unwrap();
    assert_eq!(
        aorta_schema_resolver_resolve_by_key(resolver, missing.as_ptr(), out.as_mut_ptr()),
        AORTA_ERR_NOT_FOUND
    );

    // Null out → INVALID_ARGUMENT.
    assert_eq!(
        aorta_schema_resolver_resolve_by_key(resolver, missing.as_ptr(), ptr::null_mut()),
        AORTA_ERR_INVALID_ARGUMENT
    );

    // Freeing a null resolved-schema is a guarded INVALID_ARGUMENT (not UB).
    assert_eq!(
        aorta_resolved_schema_free(ptr::null_mut()),
        AORTA_ERR_INVALID_ARGUMENT
    );

    // Resolver free is idempotent-safe: second free → NOT_FOUND, not UAF.
    assert_eq!(aorta_schema_resolver_free(resolver), AORTA_OK);
    assert_eq!(aorta_schema_resolver_free(resolver), AORTA_ERR_NOT_FOUND);

    assert_eq!(aorta_node_free(node), AORTA_OK);
}
