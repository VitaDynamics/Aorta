//! Regression tests for the FFI reentrancy contract.
//!
//! These exercise the `ffi_block_on!` macro through the public C ABI,
//! proving that calling a creation / mutation FFI entry point from
//! inside an active Tokio runtime returns `AORTA_ERR_REENTRANT` instead
//! of panicking with `Cannot start a runtime from within a runtime` (a
//! panic which under Rust ≥ 1.81 aborts the process across the
//! `extern "C"` boundary, bypassing even `ffi_guard`).
//!
//! End-to-end coverage of "FFI called from inside a subscriber /
//! service / action callback dispatched by the FFI runtime itself"
//! lives in the C++ SDK reentrancy tests, where we can construct a
//! real callback path from C++. From inside Rust we simulate the same
//! condition by running the FFI entry under `#[tokio::test]` — the
//! macro's `Handle::try_current()` check fires identically for a Tokio
//! worker thread regardless of whether the runtime was spun up by C++
//! or by tokio's test harness.

// Lint exception: same rationale as
// `ffi_action_tests.rs` — integration tests deliberately exercise the
// FFI extern "C" surface with locally-constructed arguments.
#![allow(clippy::undocumented_unsafe_blocks)]

use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::ffi::c_api::{
    aorta_bytes_view_t, aorta_callback_group_t, aorta_executor_add_node,
    aorta_executor_admission_stats_t, aorta_executor_create_callback_group, aorta_executor_free,
    aorta_executor_free_callback_group, aorta_executor_get_admission_stats, aorta_executor_spin,
    aorta_executor_spin_once, aorta_msg_context_t, aorta_node_free, aorta_node_new, aorta_node_t,
    aorta_pub_t, aorta_publisher_free, aorta_publisher_new, aorta_publisher_publish,
    aorta_reentrant_context, aorta_schema_hash_view_t, aorta_sub_t, aorta_subscriber_free,
    aorta_subscriber_new, AORTA_ERR_REENTRANT, AORTA_OK,
};

fn make_name(label: &str) -> CString {
    CString::new(format!("ffi_reentrancy_{}_{}", label, std::process::id())).expect("CString")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn node_new_from_runtime_returns_reentrant_error() {
    let name = make_name("node_new");
    let group = CString::new("default").expect("CString");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(name.as_ptr(), group.as_ptr(), ptr::addr_of_mut!(node)) };

    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "creating a node from inside a tokio runtime must return AORTA_ERR_REENTRANT, got {}",
        rc
    );
    assert_eq!(
        node, 0,
        "out parameter must be left unchanged on a reentrant rejection"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn node_free_from_runtime_succeeds_via_spawn_and_detach() {
    // FREE paths take a different policy from CREATE paths: returning
    // REENTRANT would orphan the registry-removed handle (the caller
    // has no way to retry the free since they no longer hold a valid
    // handle). Instead they spawn-and-detach the async close, matching
    // the existing protected sites at `aorta_service_free` /
    // `shutdown_ffi_action_server`. The user-visible contract is
    // "free always succeeds; close completes asynchronously".
    let name = make_name("node_free");
    let group = CString::new("default").expect("CString");
    let name_ptr = name.as_ptr() as usize;
    let group_ptr = group.as_ptr() as usize;

    let node = std::thread::spawn(move || {
        let mut node: aorta_node_t = 0;
        let rc = unsafe {
            aorta_node_new(
                name_ptr as *const std::os::raw::c_char,
                group_ptr as *const std::os::raw::c_char,
                ptr::addr_of_mut!(node),
            )
        };
        assert_eq!(rc, AORTA_OK, "node_new on a clean thread must succeed");
        assert_ne!(node, 0);
        node
    })
    .join()
    .expect("setup thread");

    let rc = aorta_node_free(node);
    assert_eq!(
        rc, AORTA_OK,
        "freeing a node from inside a tokio runtime must succeed via the spawn-and-detach path, got {}",
        rc
    );

    // Second free finds nothing in the registry — proves the first free
    // actually consumed the handle.
    std::thread::spawn(move || {
        let rc = aorta_node_free(node);
        assert_ne!(rc, AORTA_OK, "second free should fail with NOT_FOUND");
    })
    .join()
    .expect("teardown thread");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_spin_from_runtime_returns_reentrant_error_early() {
    // `aorta_executor_spin` is a long-running blocking entry; it carries
    // an explicit `in_async_context()` check at the top of the function
    // body (not via `ffi_block_on!`) because the helper's contract is
    // "block exactly once" — `spin` blocks forever. Pass a sentinel
    // invalid handle: the early-return path runs BEFORE the handle
    // lookup, so we never see AORTA_ERR_NOT_FOUND.
    let rc = aorta_executor_spin(0xdead_beef);
    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "executor_spin must early-return AORTA_ERR_REENTRANT inside a runtime, got {}",
        rc
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_spin_once_from_runtime_returns_reentrant_error_early() {
    // Same contract as `aorta_executor_spin`: the reentrancy check must
    // run BEFORE `entry.exec.lock()` so a nested call from inside the
    // outer spin's callback doesn't deadlock on the executor mutex.
    // Sentinel invalid handle proves the early-return precedes the
    // handle lookup (otherwise we'd see AORTA_ERR_NOT_FOUND).
    let rc = aorta_executor_spin_once(0xdead_beef, 100);
    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "executor_spin_once must early-return AORTA_ERR_REENTRANT inside a runtime, got {}",
        rc
    );
}

#[test]
fn node_new_outside_runtime_succeeds() {
    // Baseline: on a plain test thread (no tokio runtime), the same call
    // must still succeed — the helper must not over-reject.
    let name = make_name("node_new_baseline");
    let group = CString::new("default").expect("CString");

    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(name.as_ptr(), group.as_ptr(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "expected AORTA_OK from a non-runtime thread");
    assert_ne!(node, 0);
    assert_eq!(aorta_node_free(node), AORTA_OK);
}

// ---------------------------------------------------------------------------
// Executor mutation/inspection entry points must also early-return REENTRANT
// before touching `entry.exec.lock()`. These mirror the spin / spin_once
// contracts above — without the guard a callback that reaches into the
// executor (e.g. `add_node`, `create_callback_group`) would deadlock on the
// non-reentrant parking_lot mutex held by the outer spin loop.
// All tests use the 0xdead_beef sentinel handle, so the early-return path
// runs BEFORE the handle lookup; a missing guard surfaces as
// AORTA_ERR_NOT_FOUND, not REENTRANT.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_add_node_from_runtime_returns_reentrant_error_early() {
    let rc = aorta_executor_add_node(0xdead_beef, 0xdead_beef);
    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "executor_add_node must early-return AORTA_ERR_REENTRANT inside a runtime, got {}",
        rc
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_free_from_runtime_returns_reentrant_error_early() {
    // Unlike `aorta_node_free` (spawn-and-detach), `aorta_executor_free`
    // body is fully synchronous — REENTRANT is the only sensible response
    // when the outer spin still holds `entry.exec.lock()`.
    let rc = aorta_executor_free(0xdead_beef);
    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "executor_free must early-return AORTA_ERR_REENTRANT inside a runtime, got {}",
        rc
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_create_callback_group_from_runtime_returns_reentrant_error_early() {
    let mut out: u64 = 0;
    let rc = unsafe {
        aorta_executor_create_callback_group(
            0xdead_beef,
            aorta_callback_group_t::AORTA_CALLBACK_GROUP_REENTRANT,
            ptr::addr_of_mut!(out),
        )
    };
    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "executor_create_callback_group must early-return AORTA_ERR_REENTRANT inside a runtime, got {}",
        rc
    );
    assert_eq!(out, 0, "out parameter must be left unchanged on REENTRANT");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_free_callback_group_from_runtime_returns_reentrant_error_early() {
    let rc = aorta_executor_free_callback_group(0xdead_beef, 0xdead_beef);
    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "executor_free_callback_group must early-return AORTA_ERR_REENTRANT inside a runtime, got {}",
        rc
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_get_admission_stats_from_runtime_returns_reentrant_error_early() {
    let mut out = aorta_executor_admission_stats_t {
        queued_events: 0,
        queued_bytes: 0,
        peak_queued_events: 0,
        peak_queued_bytes: 0,
        dropped_queue_full: 0,
        dropped_queue_closed: 0,
        dropped_budget_events: 0,
        dropped_budget_bytes: 0,
    };
    let rc = unsafe { aorta_executor_get_admission_stats(0xdead_beef, ptr::addr_of_mut!(out)) };
    assert_eq!(
        rc, AORTA_ERR_REENTRANT,
        "executor_get_admission_stats must early-return AORTA_ERR_REENTRANT inside a runtime, got {}",
        rc
    );
}

// ---------------------------------------------------------------------------
// aorta_reentrant_context() — the exported explicit detector.
// 1 iff the current thread is inside a dispatched Aorta callback or nested
// inside an aorta-owned block_on; 0 everywhere else. Never initializes the
// runtime.
// ---------------------------------------------------------------------------

/// Records `aorta_reentrant_context()` at dispatch time; sentinel `u8::MAX`
/// until the first message arrives.
extern "C" fn reentrancy_probe_cb(
    _payload: *const aorta_bytes_view_t,
    _schema: *const aorta_schema_hash_view_t,
    _ctx: *const aorta_msg_context_t,
    user_data: *mut c_void,
) {
    // SAFETY: `user_data` is the `&AtomicU8` probe we passed to
    // `aorta_subscriber_new`; it outlives the subscriber (freed before the
    // probe drops).
    let probe = unsafe { &*(user_data as *const AtomicU8) };
    probe.store(aorta_reentrant_context(), Ordering::SeqCst);
}

#[test]
fn reentrant_context_is_one_inside_dispatched_subscriber_callback() {
    // Live FFI pub/sub round trip: a default (`DeliveryMode::Executor`)
    // subscriber dispatches on_msg through the event queue, whose single
    // dispatch point (`Event::dispatch_isolated`) runs under
    // `CallbackContextGuard` — so the exported detector must read exactly 1
    // inside the callback.
    let group = CString::new("default").expect("CString");
    let node_name = make_name("sub_ctx_node");
    let mut node: aorta_node_t = 0;
    let rc = unsafe { aorta_node_new(node_name.as_ptr(), group.as_ptr(), ptr::addr_of_mut!(node)) };
    assert_eq!(rc, AORTA_OK, "node_new on a plain thread must succeed");

    let topic = make_name("sub_ctx_topic");
    let probe = Box::new(AtomicU8::new(u8::MAX));
    let probe_ptr = (&*probe as *const AtomicU8) as *mut c_void;

    let mut sub: aorta_sub_t = 0;
    let rc = aorta_subscriber_new(
        node,
        topic.as_ptr(),
        ptr::null(),
        ptr::null(),
        Some(reentrancy_probe_cb),
        probe_ptr,
        ptr::addr_of_mut!(sub),
    );
    assert_eq!(
        rc, AORTA_OK,
        "subscriber_new on a plain thread must succeed"
    );

    // Give the subscriber time to match before publishing.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let pub_node_name = make_name("sub_ctx_pub_node");
    let mut pub_node: aorta_node_t = 0;
    let rc = unsafe {
        aorta_node_new(
            pub_node_name.as_ptr(),
            group.as_ptr(),
            ptr::addr_of_mut!(pub_node),
        )
    };
    assert_eq!(rc, AORTA_OK);

    let mut pub_handle: aorta_pub_t = 0;
    let rc = aorta_publisher_new(
        pub_node,
        topic.as_ptr(),
        ptr::null(),
        ptr::null(),
        ptr::addr_of_mut!(pub_handle),
    );
    assert_eq!(rc, AORTA_OK);

    let payload = b"reentrancy probe";
    // SAFETY: valid publisher handle + contiguous buffer.
    let rc = unsafe { aorta_publisher_publish(pub_handle, payload.as_ptr(), payload.len()) };
    assert_eq!(rc, AORTA_OK);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while probe.load(Ordering::SeqCst) == u8::MAX && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert_eq!(
        probe.load(Ordering::SeqCst),
        1,
        "aorta_reentrant_context must read 1 inside a dispatched subscriber callback"
    );

    // Free the subscriber (drains in-flight callbacks) BEFORE the probe drops.
    assert_eq!(aorta_subscriber_free(sub), AORTA_OK);
    assert_eq!(aorta_publisher_free(pub_handle), AORTA_OK);
    assert_eq!(aorta_node_free(pub_node), AORTA_OK);
    assert_eq!(aorta_node_free(node), AORTA_OK);
    drop(probe);
}

#[test]
fn reentrant_context_is_zero_on_a_plain_thread() {
    // Plain test thread: carries neither `BlockOnGuard` nor
    // `CallbackContextGuard`.
    assert_eq!(aorta_reentrant_context(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn reentrant_context_is_zero_on_spawn_blocking_thread() {
    // spawn_blocking threads carry neither reentrancy flag, so their legal
    // `block_on` stays on the success path.
    let observed = tokio::task::spawn_blocking(|| aorta_reentrant_context())
        .await
        .expect("spawn_blocking join");
    assert_eq!(observed, 0);
}
