// Lint exception: same rationale as
// `ffi_action_tests.rs` — integration tests deliberately exercise the
// FFI extern "C" surface with locally-constructed arguments.
#![allow(clippy::undocumented_unsafe_blocks)]

//! Regression tests for `aorta_stop_handle_*` lifetime safety.
//!
//! Before this fix, the stop handle was a raw
//! `Box<StopHandle>` pointer returned from `Box::into_raw`. Calling
//! `aorta_stop_handle_free` twice on the same handle triggered use-after-free.
//! After the fix, handles are u64 keys into `registry().stop_handles`, and
//! double-free is detected and returns `AORTA_ERR_NOT_FOUND`.

use std::time::Duration;

use crate::ffi::c_api::{
    aorta_executor_free, aorta_executor_get_stop_handle, aorta_executor_new, aorta_executor_type_t,
    aorta_stop_handle_free, aorta_stop_handle_stop, aorta_stop_handle_t,
    AORTA_ERR_INVALID_ARGUMENT, AORTA_ERR_NOT_FOUND, AORTA_OK,
};

/// Sanity: get / stop / free in a single thread completes cleanly.
#[test]
fn stop_handle_basic_lifecycle() {
    let mut exec = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );

    let mut sh: aorta_stop_handle_t = 0;
    assert_eq!(
        unsafe { aorta_executor_get_stop_handle(exec, &mut sh) },
        AORTA_OK
    );
    // Non-zero / non-null sentinel for the valid handle
    assert!(
        stop_handle_is_valid(sh),
        "handle should be non-default after get"
    );

    assert_eq!(aorta_stop_handle_stop(sh), AORTA_OK);
    assert_eq!(aorta_stop_handle_free(sh), AORTA_OK);

    // Cleanup
    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

/// CORE REGRESSION: double-free must NOT cause UAF / heap corruption.
///
/// Before C-4 fix: second `aorta_stop_handle_free` calls `Box::from_raw`
/// on an already-freed pointer → use-after-free / heap corruption.
///
/// After C-4 fix: handles are u64 keys; second free finds nothing and
/// returns AORTA_ERR_NOT_FOUND.
#[test]
fn stop_handle_double_free_returns_not_found() {
    let mut exec = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );

    let mut sh: aorta_stop_handle_t = 0;
    assert_eq!(
        unsafe { aorta_executor_get_stop_handle(exec, &mut sh) },
        AORTA_OK
    );

    // First free: OK
    assert_eq!(aorta_stop_handle_free(sh), AORTA_OK);

    // Second free: must NOT crash; must report NOT_FOUND
    assert_eq!(
        aorta_stop_handle_free(sh),
        AORTA_ERR_NOT_FOUND,
        "double free must surface as NOT_FOUND, not UAF"
    );

    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

/// After free, calling stop on a stale handle must also surface NOT_FOUND
/// (rather than dereferencing freed memory).
#[test]
fn stop_handle_use_after_free_returns_not_found() {
    let mut exec = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );

    let mut sh: aorta_stop_handle_t = 0;
    assert_eq!(
        unsafe { aorta_executor_get_stop_handle(exec, &mut sh) },
        AORTA_OK
    );

    assert_eq!(aorta_stop_handle_free(sh), AORTA_OK);

    // Stop on freed handle must not crash; must report NOT_FOUND
    assert_eq!(
        aorta_stop_handle_stop(sh),
        AORTA_ERR_NOT_FOUND,
        "stop on freed handle must surface as NOT_FOUND, not UAF"
    );

    assert_eq!(aorta_executor_free(exec), AORTA_OK);
}

/// Invalid (zero / null) handles are rejected without panic or UAF.
#[test]
fn stop_handle_invalid_handle_rejected() {
    let invalid: aorta_stop_handle_t = crate::ffi::c_api::AORTA_STOP_HANDLE_INVALID;

    // Both stop and free on an unknown handle return NOT_FOUND or INVALID_ARGUMENT.
    let rc_stop = aorta_stop_handle_stop(invalid);
    assert!(
        rc_stop == AORTA_ERR_NOT_FOUND || rc_stop == AORTA_ERR_INVALID_ARGUMENT,
        "stop on invalid handle should be safely rejected; got {rc_stop}"
    );
    let rc_free = aorta_stop_handle_free(invalid);
    assert!(
        rc_free == AORTA_OK || rc_free == AORTA_ERR_INVALID_ARGUMENT,
        "free on invalid handle should be a safe no-op; got {rc_free}"
    );
}

/// Multiple stop handles from the same executor are independent.
#[test]
fn stop_handle_multiple_per_executor() {
    let mut exec = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );

    let mut sh1: aorta_stop_handle_t = 0;
    let mut sh2: aorta_stop_handle_t = 0;
    assert_eq!(
        unsafe { aorta_executor_get_stop_handle(exec, &mut sh1) },
        AORTA_OK
    );
    assert_eq!(
        unsafe { aorta_executor_get_stop_handle(exec, &mut sh2) },
        AORTA_OK
    );
    assert_ne!(sh1, sh2, "two handles for the same executor must differ");

    // Free in any order; each must succeed once and only once.
    assert_eq!(aorta_stop_handle_free(sh1), AORTA_OK);
    assert_eq!(aorta_stop_handle_free(sh2), AORTA_OK);
    assert_eq!(aorta_stop_handle_free(sh1), AORTA_ERR_NOT_FOUND);
    assert_eq!(aorta_stop_handle_free(sh2), AORTA_ERR_NOT_FOUND);

    // Cancel the timer test so executor_free doesn't hang on Stop().
    let _ = aorta_executor_free(exec);
}

/// Stop after the executor is freed must not crash (the executor's StopHandle
/// is held via Arc inside the registry, so the StopHandle can outlive the
/// executor reference).
#[test]
fn stop_handle_outlives_executor() {
    let mut exec = 0;
    assert_eq!(
        unsafe { aorta_executor_new(aorta_executor_type_t::AORTA_EXECUTOR_SINGLE, &mut exec) },
        AORTA_OK
    );

    let mut sh: aorta_stop_handle_t = 0;
    assert_eq!(
        unsafe { aorta_executor_get_stop_handle(exec, &mut sh) },
        AORTA_OK
    );

    // Drop the executor first.
    assert_eq!(aorta_executor_free(exec), AORTA_OK);

    // The stop handle's StopHandle is still valid; calling stop() on it
    // must not crash even though the executor is gone.
    let rc = aorta_stop_handle_stop(sh);
    assert_eq!(rc, AORTA_OK, "stop on outlived handle should still succeed");

    assert_eq!(aorta_stop_handle_free(sh), AORTA_OK);
    // Sleep tiny bit to allow any deferred cleanup.
    std::thread::sleep(Duration::from_millis(5));
}

/// Helper: a stop handle is "valid" if non-zero (u64 sentinel).
#[inline]
fn stop_handle_is_valid(h: aorta_stop_handle_t) -> bool {
    h != crate::ffi::c_api::AORTA_STOP_HANDLE_INVALID
}
