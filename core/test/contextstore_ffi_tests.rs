//! Rust-side smoke tests for the ContextStore C ABI.
//!
//! Mirrors the pattern in ffi_action_tests.rs: drive the FFI surface
//! from Rust by calling the `extern "C"` symbols and verifying the
//! out-parameters and registry teardown behaviour. The FFI functions
//! internally use `runtime().block_on(...)`, so these tests use plain
//! `#[test]` (NOT `#[tokio::test]`) — nesting a tokio runtime inside
//! the FFI's runtime panics with "Cannot start a runtime from within a
//! runtime."

// Lint exception: same rationale as
// `ffi_action_tests.rs` — integration tests deliberately exercise the
// FFI extern "C" surface with locally-constructed arguments. Per-block
// SAFETY comments would be dozens of copies of the same prose.
#![allow(clippy::undocumented_unsafe_blocks)]

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::ffi::c_api::{
    aorta_ctx_change_t, aorta_ctx_declare_owned_prefix, aorta_ctx_free, aorta_ctx_get_bool,
    aorta_ctx_get_i64, aorta_ctx_get_string, aorta_ctx_get_u64, aorta_ctx_new,
    aorta_ctx_owner_liveliness_event_t, aorta_ctx_owner_liveliness_watch,
    aorta_ctx_owner_liveliness_watch_free, aorta_ctx_owner_liveliness_watch_t, aorta_ctx_read_free,
    aorta_ctx_read_t, aorta_ctx_set_bool, aorta_ctx_set_i64, aorta_ctx_set_string,
    aorta_ctx_set_u64, aorta_ctx_shutdown, aorta_ctx_t, aorta_ctx_watch, aorta_ctx_watch_free,
    aorta_ctx_watch_t, aorta_node_free, aorta_node_new, aorta_node_t, AORTA_CTX_CHANGE_LIVE,
    AORTA_CTX_KIND_BOOL, AORTA_CTX_KIND_I64, AORTA_CTX_KIND_STRING, AORTA_CTX_KIND_U64,
    AORTA_CTX_NO_EXPIRY, AORTA_ERR_INTERNAL, AORTA_ERR_INVALID_ARGUMENT, AORTA_ERR_KIND_MISMATCH,
    AORTA_ERR_NOT_FOUND, AORTA_ERR_PREFIX_NOT_DECLARED, AORTA_OK, TEST_PANIC_AFTER_WATCH_REMOVE,
};

fn new_ffi_node(name: &str) -> aorta_node_t {
    let cname = CString::new(name).unwrap();
    let cgroup = CString::new("default").unwrap();
    let mut handle: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(cname.as_ptr(), cgroup.as_ptr(), &mut handle) };
    assert_eq!(rc, AORTA_OK, "aorta_node_new failed: {}", rc);
    handle
}

fn new_ctx(node: aorta_node_t) -> aorta_ctx_t {
    let mut handle: aorta_ctx_t = 0;
    let rc = unsafe { aorta_ctx_new(node, &mut handle) };
    assert_eq!(rc, AORTA_OK, "aorta_ctx_new failed: {}", rc);
    handle
}

fn declare(ctx: aorta_ctx_t, prefix: &str) {
    let cprefix = CString::new(prefix).unwrap();
    let rc = unsafe { aorta_ctx_declare_owned_prefix(ctx, cprefix.as_ptr()) };
    assert_eq!(rc, AORTA_OK, "declare_owned_prefix failed: {}", rc);
}

unsafe fn read_owned_string(p: *mut std::os::raw::c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}

#[test]
fn ffi_new_free_roundtrip() {
    let node = new_ffi_node("ctxD_ffi_lifecycle");
    let ctx = new_ctx(node);
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_ctx_free(ctx), AORTA_ERR_NOT_FOUND);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_new_with_invalid_node_returns_not_found() {
    let mut handle: aorta_ctx_t = 0;
    let rc = unsafe { aorta_ctx_new(0xdead_beef, &mut handle) };
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
}

#[test]
fn ffi_new_with_null_out_returns_invalid_argument() {
    let node = new_ffi_node("ctxD_ffi_null_out");
    let rc = unsafe { aorta_ctx_new(node, ptr::null_mut()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_set_get_bool_roundtrip() {
    let node = new_ffi_node("ctxD_ffi_bool");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_bool/**");
    let key = CString::new("system_ffi_bool/flag").unwrap();
    assert_eq!(
        unsafe { aorta_ctx_set_bool(ctx, key.as_ptr(), true, AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );

    let mut read = aorta_ctx_read_t::default();
    assert_eq!(
        unsafe { aorta_ctx_get_bool(ctx, key.as_ptr(), &mut read) },
        AORTA_OK
    );
    assert_eq!(read.value_kind, AORTA_CTX_KIND_BOOL);
    assert!(read.bool_value);
    assert!(read.is_fresh);
    assert_eq!(read.sequence, 1);
    let source_node = unsafe { read_owned_string(read.source_node) };
    let instance = unsafe { read_owned_string(read.source_instance_id) };
    assert_eq!(source_node, "ctxD_ffi_bool");
    assert_eq!(instance.len(), 16);

    assert_eq!(unsafe { aorta_ctx_read_free(&mut read) }, AORTA_OK);
    // Idempotent: free again is OK; pointers were nulled out.
    assert_eq!(unsafe { aorta_ctx_read_free(&mut read) }, AORTA_OK);

    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_set_get_i64_u64_roundtrip() {
    let node = new_ffi_node("ctxD_ffi_ints");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_int/**");
    let key_i = CString::new("system_ffi_int/i").unwrap();
    let key_u = CString::new("system_ffi_int/u").unwrap();
    assert_eq!(
        unsafe { aorta_ctx_set_i64(ctx, key_i.as_ptr(), -42, AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_ctx_set_u64(ctx, key_u.as_ptr(), 999, AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );

    let mut read = aorta_ctx_read_t::default();
    assert_eq!(
        unsafe { aorta_ctx_get_i64(ctx, key_i.as_ptr(), &mut read) },
        AORTA_OK
    );
    assert_eq!(read.value_kind, AORTA_CTX_KIND_I64);
    assert_eq!(read.i64_value, -42);
    unsafe { aorta_ctx_read_free(&mut read) };

    let mut read = aorta_ctx_read_t::default();
    assert_eq!(
        unsafe { aorta_ctx_get_u64(ctx, key_u.as_ptr(), &mut read) },
        AORTA_OK
    );
    assert_eq!(read.value_kind, AORTA_CTX_KIND_U64);
    assert_eq!(read.u64_value, 999);
    unsafe { aorta_ctx_read_free(&mut read) };

    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_set_get_f64_roundtrip() {
    let node = new_ffi_node("ctxD_ffi_f64");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_f64/**");
    let key = CString::new("system_ffi_f64/temp").unwrap();
    let rc = unsafe {
        crate::ffi::c_api::aorta_ctx_set_f64(ctx, key.as_ptr(), 36.5, AORTA_CTX_NO_EXPIRY)
    };
    assert_eq!(rc, AORTA_OK);

    let mut read = aorta_ctx_read_t::default();
    let rc = unsafe { crate::ffi::c_api::aorta_ctx_get_f64(ctx, key.as_ptr(), &mut read) };
    assert_eq!(rc, AORTA_OK);
    assert_eq!(read.value_kind, crate::ffi::c_api::AORTA_CTX_KIND_F64);
    assert!((read.f64_value - 36.5).abs() < 1e-9);
    unsafe { aorta_ctx_read_free(&mut read) };

    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_set_get_string_empty_value_roundtrip() {
    let node = new_ffi_node("ctxD_ffi_str_empty");
    let ctx = new_ctx(node);
    declare(ctx, "rcp_ffi_str_empty/**");
    let key = CString::new("rcp_ffi_str_empty/req").unwrap();
    let value = CString::new("").unwrap();
    assert_eq!(
        unsafe { aorta_ctx_set_string(ctx, key.as_ptr(), value.as_ptr(), AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );

    let mut read = aorta_ctx_read_t::default();
    assert_eq!(
        unsafe { aorta_ctx_get_string(ctx, key.as_ptr(), &mut read) },
        AORTA_OK
    );
    let s = unsafe { read_owned_string(read.string_value) };
    assert_eq!(s, "");
    unsafe { aorta_ctx_read_free(&mut read) };

    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_set_get_string_roundtrip() {
    let node = new_ffi_node("ctxD_ffi_str");
    let ctx = new_ctx(node);
    declare(ctx, "rcp_ffi_str/**");
    let key = CString::new("rcp_ffi_str/request_id").unwrap();
    let value = CString::new("task-xyz-001").unwrap();
    assert_eq!(
        unsafe { aorta_ctx_set_string(ctx, key.as_ptr(), value.as_ptr(), AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );

    let mut read = aorta_ctx_read_t::default();
    assert_eq!(
        unsafe { aorta_ctx_get_string(ctx, key.as_ptr(), &mut read) },
        AORTA_OK
    );
    assert_eq!(read.value_kind, AORTA_CTX_KIND_STRING);
    let s = unsafe { read_owned_string(read.string_value) };
    assert_eq!(s, "task-xyz-001");
    unsafe { aorta_ctx_read_free(&mut read) };

    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_kind_mismatch_returned_through_c_abi() {
    let node = new_ffi_node("ctxD_ffi_mismatch");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_mm/**");
    let key = CString::new("system_ffi_mm/flag").unwrap();
    assert_eq!(
        unsafe { aorta_ctx_set_bool(ctx, key.as_ptr(), true, AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );
    let mut read = aorta_ctx_read_t::default();
    let rc = unsafe { aorta_ctx_get_string(ctx, key.as_ptr(), &mut read) };
    assert_eq!(rc, AORTA_ERR_KIND_MISMATCH);
    unsafe { aorta_ctx_read_free(&mut read) };
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_set_without_declare_returns_prefix_not_declared() {
    let node = new_ffi_node("ctxD_ffi_undecl");
    let ctx = new_ctx(node);
    let key = CString::new("rcp_ffi_undecl/state").unwrap();
    let rc = unsafe { aorta_ctx_set_u64(ctx, key.as_ptr(), 1, AORTA_CTX_NO_EXPIRY) };
    assert_eq!(rc, AORTA_ERR_PREFIX_NOT_DECLARED);
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_shutdown_then_set_returns_prefix_not_declared() {
    let node = new_ffi_node("ctxD_ffi_shutdown");
    let ctx = new_ctx(node);
    declare(ctx, "rcp_ffi_shut/**");
    let key = CString::new("rcp_ffi_shut/state").unwrap();
    assert_eq!(
        unsafe { aorta_ctx_set_u64(ctx, key.as_ptr(), 1, AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );
    assert_eq!(aorta_ctx_shutdown(ctx), AORTA_OK);
    let rc = unsafe { aorta_ctx_set_u64(ctx, key.as_ptr(), 2, AORTA_CTX_NO_EXPIRY) };
    assert_eq!(rc, AORTA_ERR_PREFIX_NOT_DECLARED);
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

// ----- watch ---------------------------------------------------------------

struct WatchCounter {
    live_count: AtomicUsize,
    last_key_len: AtomicUsize,
    last_u64_value: AtomicU64,
    last_kind: AtomicU8,
}

unsafe extern "C" fn count_watch_callback(
    change: *const aorta_ctx_change_t,
    user_data: *mut c_void,
) {
    let counter = &*(user_data as *const WatchCounter);
    let c = &*change;
    if c.kind == AORTA_CTX_CHANGE_LIVE {
        counter.live_count.fetch_add(1, Ordering::SeqCst);
        counter.last_key_len.store(c.key.len, Ordering::SeqCst);
        counter.last_u64_value.store(c.u64_value, Ordering::SeqCst);
        counter.last_kind.store(c.value_kind, Ordering::SeqCst);
    }
}

#[test]
fn ffi_watch_callback_receives_self_set_event() {
    let node = new_ffi_node("ctxD_ffi_watch");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_watch/**");

    let counter = Arc::new(WatchCounter {
        live_count: AtomicUsize::new(0),
        last_key_len: AtomicUsize::new(0),
        last_u64_value: AtomicU64::new(0),
        last_kind: AtomicU8::new(0),
    });
    let user_data = Arc::as_ptr(&counter) as *mut c_void;

    let mut watch_handle: aorta_ctx_watch_t = 0;
    let prefix = CString::new("system_ffi_watch/**").unwrap();
    let rc = unsafe {
        aorta_ctx_watch(
            ctx,
            prefix.as_ptr(),
            Some(count_watch_callback),
            user_data,
            &mut watch_handle,
        )
    };
    assert_eq!(rc, AORTA_OK);

    let key = CString::new("system_ffi_watch/state").unwrap();
    assert_eq!(
        unsafe { aorta_ctx_set_u64(ctx, key.as_ptr(), 7, AORTA_CTX_NO_EXPIRY) },
        AORTA_OK
    );

    // Wait for the live event using a busy-wait (no async runtime here).
    let deadline = Instant::now() + Duration::from_millis(2_000);
    while counter.live_count.load(Ordering::SeqCst) == 0 {
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(counter.live_count.load(Ordering::SeqCst), 1);
    assert_eq!(counter.last_kind.load(Ordering::SeqCst), AORTA_CTX_KIND_U64);
    assert_eq!(counter.last_u64_value.load(Ordering::SeqCst), 7);
    assert!(counter.last_key_len.load(Ordering::SeqCst) > 0);

    assert_eq!(aorta_ctx_watch_free(watch_handle), AORTA_OK);
    assert_eq!(aorta_ctx_watch_free(watch_handle), AORTA_ERR_NOT_FOUND);
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_watch_with_null_callback_rejected() {
    let node = new_ffi_node("ctxD_ffi_watch_null");
    let ctx = new_ctx(node);
    let prefix = CString::new("rcp_ffi_watch_null/**").unwrap();
    let mut watch_handle: aorta_ctx_watch_t = 0;
    let rc = unsafe {
        aorta_ctx_watch(
            ctx,
            prefix.as_ptr(),
            None,
            ptr::null_mut(),
            &mut watch_handle,
        )
    };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_read_free_rejects_null() {
    let rc = unsafe { aorta_ctx_read_free(ptr::null_mut()) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
}

// ----- owner liveliness watch -----------------------------------------------

/// Collects owner-liveliness events delivered through the C ABI.
struct OwnerLivelinessCounter {
    alive_count: AtomicUsize,
    gone_count: AtomicUsize,
    last_node_name: AtomicU64,
    last_instance_id: AtomicU64,
    last_alive: AtomicU8,
}

unsafe extern "C" fn owner_liveliness_callback(
    event: *const aorta_ctx_owner_liveliness_event_t,
    user_data: *mut c_void,
) {
    let counter = &*(user_data as *const OwnerLivelinessCounter);
    if event.is_null() {
        return;
    }
    // Copy the borrowed views into owned strings immediately (the views
    // are valid only for the callback duration) and stash their lengths
    // as a proxy for content correctness.
    let node_name = (*event).node_name;
    let instance = (*event).source_instance_id;
    let node_len = node_name.len as u64;
    let instance_len = instance.len as u64;
    let node_bytes = std::slice::from_raw_parts(node_name.data, node_name.len);
    let instance_bytes = std::slice::from_raw_parts(instance.data, instance.len);
    // The node name is "ctxD_ffi_liv" — assert its exact bytes here.
    let node_str = String::from_utf8_lossy(node_bytes);
    let instance_str = String::from_utf8_lossy(instance_bytes);
    assert_eq!(node_str, "ctxD_ffi_liv");
    assert_eq!(
        instance_str.len(),
        16,
        "source_instance_id must be 16 hex chars"
    );
    counter.last_node_name.store(node_len, Ordering::SeqCst);
    counter
        .last_instance_id
        .store(instance_len, Ordering::SeqCst);
    counter
        .last_alive
        .store((*event).alive as u8, Ordering::SeqCst);
    if (*event).alive {
        counter.alive_count.fetch_add(1, Ordering::SeqCst);
    } else {
        counter.gone_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn ffi_owner_liveliness_watch_receives_alive_and_gone() {
    let node = new_ffi_node("ctxD_ffi_liv");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_liv/**");

    // Watch AFTER the owner exists: history(true) replays the token as
    // an alive event, then shutdown withdraws it as a gone event.
    let counter = Arc::new(OwnerLivelinessCounter {
        alive_count: AtomicUsize::new(0),
        gone_count: AtomicUsize::new(0),
        last_node_name: AtomicU64::new(0),
        last_instance_id: AtomicU64::new(0),
        last_alive: AtomicU8::new(0),
    });
    let user_data = Arc::as_ptr(&counter) as *mut c_void;

    let mut watch_handle: aorta_ctx_owner_liveliness_watch_t = 0;
    let rc = unsafe {
        aorta_ctx_owner_liveliness_watch(
            ctx,
            Some(owner_liveliness_callback),
            user_data,
            &mut watch_handle,
        )
    };
    assert_eq!(rc, AORTA_OK);

    let deadline = Instant::now() + Duration::from_millis(2_000);
    while counter.alive_count.load(Ordering::SeqCst) == 0 {
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        counter.alive_count.load(Ordering::SeqCst),
        1,
        "expected one alive event (history replay)"
    );
    assert_eq!(counter.last_alive.load(Ordering::SeqCst), 1);
    assert!(counter.last_node_name.load(Ordering::SeqCst) > 0);
    assert_eq!(counter.last_instance_id.load(Ordering::SeqCst), 16);

    // Deterministic shutdown -> token withdrawn -> gone event.
    assert_eq!(aorta_ctx_shutdown(ctx), AORTA_OK);
    let deadline = Instant::now() + Duration::from_millis(2_000);
    while counter.gone_count.load(Ordering::SeqCst) == 0 {
        if Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        counter.gone_count.load(Ordering::SeqCst),
        1,
        "expected one gone event after shutdown"
    );
    assert_eq!(counter.last_alive.load(Ordering::SeqCst), 0);

    assert_eq!(
        aorta_ctx_owner_liveliness_watch_free(watch_handle),
        AORTA_OK
    );
    assert_eq!(
        aorta_ctx_owner_liveliness_watch_free(watch_handle),
        AORTA_ERR_NOT_FOUND
    );
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_owner_liveliness_watch_null_callback_rejected() {
    let node = new_ffi_node("ctxD_ffi_liv_null");
    let ctx = new_ctx(node);
    let mut watch_handle: aorta_ctx_owner_liveliness_watch_t = 0;
    let rc =
        unsafe { aorta_ctx_owner_liveliness_watch(ctx, None, ptr::null_mut(), &mut watch_handle) };
    assert_eq!(rc, AORTA_ERR_INVALID_ARGUMENT);
    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_watch_free_panic_after_remove_leaves_entry_gone() {
    // Counterexample semantics: a panicking free
    // (AORTA_ERR_INTERNAL from ffi_guard) removes the registry entry
    // BEFORE the panic, so a retry sees NOT_FOUND — while the watch is
    // only best-effort-dropped during unwind and a callback may still
    // be in flight. This is exactly why the C++ wrapper must retain
    // callback state on EVERY non-OK result, including NOT_FOUND: the
    // registry's absence proves nothing about callback drain (the
    // callback closure holds user_data directly; dispatch never
    // consults the registry).
    let node = new_ffi_node("ctxD_ffi_watch_panic");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_watch_panic/**");

    let counter = Arc::new(WatchCounter {
        live_count: AtomicUsize::new(0),
        last_key_len: AtomicUsize::new(0),
        last_u64_value: AtomicU64::new(0),
        last_kind: AtomicU8::new(0),
    });
    let user_data = Arc::as_ptr(&counter) as *mut c_void;
    let mut watch_handle: aorta_ctx_watch_t = 0;
    let prefix = CString::new("system_ffi_watch_panic/**").unwrap();
    let rc = unsafe {
        aorta_ctx_watch(
            ctx,
            prefix.as_ptr(),
            Some(count_watch_callback),
            user_data,
            &mut watch_handle,
        )
    };
    assert_eq!(rc, AORTA_OK);

    // Arm the panic seam: the next free panics after removing the entry.
    TEST_PANIC_AFTER_WATCH_REMOVE.store(true, Ordering::SeqCst);
    assert_eq!(aorta_ctx_watch_free(watch_handle), AORTA_ERR_INTERNAL);
    // The entry is gone: the retry sees NOT_FOUND.
    assert_eq!(aorta_ctx_watch_free(watch_handle), AORTA_ERR_NOT_FOUND);

    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

unsafe extern "C" fn liveliness_count_callback(
    _event: *const aorta_ctx_owner_liveliness_event_t,
    user_data: *mut c_void,
) {
    // Deliberately panic-free: an assertion failure inside an
    // `extern "C"` callback aborts the process (nounwind), and these
    // tests only need the watch to exist.
    let counter = &*(user_data as *const OwnerLivelinessCounter);
    counter.alive_count.fetch_add(1, Ordering::SeqCst);
}

#[test]
fn ffi_owner_liveliness_watch_free_panic_after_remove_leaves_entry_gone() {
    // Same counterexample semantics for the owner-liveliness watch.
    let node = new_ffi_node("ctxD_ffi_liv_panic");
    let ctx = new_ctx(node);
    declare(ctx, "system_ffi_liv_panic/**");

    let counter = Arc::new(OwnerLivelinessCounter {
        alive_count: AtomicUsize::new(0),
        gone_count: AtomicUsize::new(0),
        last_node_name: AtomicU64::new(0),
        last_instance_id: AtomicU64::new(0),
        last_alive: AtomicU8::new(0),
    });
    let user_data = Arc::as_ptr(&counter) as *mut c_void;
    let mut watch_handle: aorta_ctx_owner_liveliness_watch_t = 0;
    let rc = unsafe {
        aorta_ctx_owner_liveliness_watch(
            ctx,
            Some(liveliness_count_callback),
            user_data,
            &mut watch_handle,
        )
    };
    assert_eq!(rc, AORTA_OK);

    TEST_PANIC_AFTER_WATCH_REMOVE.store(true, Ordering::SeqCst);
    assert_eq!(
        aorta_ctx_owner_liveliness_watch_free(watch_handle),
        AORTA_ERR_INTERNAL
    );
    assert_eq!(
        aorta_ctx_owner_liveliness_watch_free(watch_handle),
        AORTA_ERR_NOT_FOUND
    );

    assert_eq!(aorta_ctx_free(ctx), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

#[test]
fn ffi_owner_liveliness_watch_free_unknown_handle_not_found() {
    let rc = aorta_ctx_owner_liveliness_watch_free(12345);
    assert_eq!(rc, AORTA_ERR_NOT_FOUND);
}
