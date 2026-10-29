//! Tests for the tracing C-ABI surface (Step 10): scope interning, gate
//! toggling, gated recording, and the master gate-byte pointer.

// Lint exception: same rationale as
// `ffi_action_tests.rs` — integration tests deliberately exercise the
// FFI extern "C" surface with locally-constructed arguments. Per-block
// SAFETY comments would be dozens of copies of the same prose.
#![allow(clippy::undocumented_unsafe_blocks)]

use crate::ffi::c_api::trace_ffi::{
    aorta_trace_enabled, aorta_trace_master_gate, aorta_trace_metric, aorta_trace_record,
    aorta_trace_record_by_handle, aorta_trace_scope, aorta_trace_set_enabled,
    aorta_trace_user_metric_count,
};
use crate::ffi::c_api::AORTA_OK;
use crate::trace::{all_user_snapshots, set_scope_enabled, set_trace_enabled, LogLevel};
use std::ffi::CString;

fn guard<T>(f: impl FnOnce() -> T) -> T {
    crate::trace_test_support::guarded(f)
}

fn metric_count(scope: u16, name: &str) -> u64 {
    all_user_snapshots()
        .into_iter()
        .find(|(s, n, _, _)| *s == scope && n == name)
        .map(|(_, _, _, snap)| snap.count)
        .unwrap_or(0)
}

#[test]
fn ffi_record_user_populates_metric_when_enabled() {
    guard(|| {
        set_trace_enabled(true);
        let sname = CString::new("ffi.scope").unwrap();
        let scope = unsafe { aorta_trace_scope(sname.as_ptr()) };
        assert_ne!(scope, 0, "interned scope handle must be non-sentinel");
        set_scope_enabled(scope, true);

        let mname = CString::new("ffi.metric").unwrap();
        let before = metric_count(scope, "ffi.metric");
        let r = unsafe { aorta_trace_record(scope, mname.as_ptr(), LogLevel::Info as u8, 1234) };
        assert_eq!(r, AORTA_OK);
        assert_eq!(metric_count(scope, "ffi.metric") - before, 1);
    });
}

#[test]
fn ffi_enabled_reflects_master_and_scope() {
    guard(|| {
        let sname = CString::new("ffi.gate.scope").unwrap();
        let scope = unsafe { aorta_trace_scope(sname.as_ptr()) };
        set_scope_enabled(scope, true);

        aorta_trace_set_enabled(0);
        assert_eq!(
            aorta_trace_enabled(scope, LogLevel::Info as u8),
            0,
            "master off"
        );
        aorta_trace_set_enabled(1);
        assert_eq!(
            aorta_trace_enabled(scope, LogLevel::Info as u8),
            1,
            "master on + scope on"
        );

        set_scope_enabled(scope, false);
        assert_eq!(
            aorta_trace_enabled(scope, LogLevel::Info as u8),
            0,
            "scope off disables even with master on"
        );
        // sentinel scope is never enabled
        assert_eq!(aorta_trace_enabled(0, LogLevel::Error as u8), 0);
    });
}

#[test]
fn ffi_master_gate_pointer_tracks_toggle() {
    guard(|| {
        let p = aorta_trace_master_gate();
        assert!(!p.is_null());
        aorta_trace_set_enabled(1);
        assert_ne!(
            unsafe { *p },
            0,
            "gate byte reflects enabled (SDK inline-read path)"
        );
        aorta_trace_set_enabled(0);
        assert_eq!(unsafe { *p }, 0, "gate byte reflects disabled");
    });
}

#[test]
fn ffi_user_metric_count_reads_back_recorded() {
    guard(|| {
        set_trace_enabled(true);
        let sname = CString::new("ffi.readback.scope").unwrap();
        let scope = unsafe { aorta_trace_scope(sname.as_ptr()) };
        set_scope_enabled(scope, true);
        let mname = CString::new("ffi.readback.metric").unwrap();

        let before = unsafe { aorta_trace_user_metric_count(scope, mname.as_ptr()) };
        let _ = unsafe { aorta_trace_record(scope, mname.as_ptr(), LogLevel::Info as u8, 500) };
        let _ = unsafe { aorta_trace_record(scope, mname.as_ptr(), LogLevel::Info as u8, 700) };
        let after = unsafe { aorta_trace_user_metric_count(scope, mname.as_ptr()) };

        assert_eq!(
            after - before,
            2,
            "read-back count must match records made via the C-ABI"
        );
        // sentinel scope and unknown metric both read 0
        let unknown = CString::new("ffi.readback.absent").unwrap();
        assert_eq!(
            unsafe { aorta_trace_user_metric_count(scope, unknown.as_ptr()) },
            0
        );
        assert_eq!(
            unsafe { aorta_trace_user_metric_count(0, mname.as_ptr()) },
            0
        );
    });
}

#[test]
fn ffi_metric_handle_records_lock_free() {
    guard(|| {
        set_trace_enabled(true);
        let sname = CString::new("ffi.handle.scope").unwrap();
        let scope = unsafe { aorta_trace_scope(sname.as_ptr()) };
        set_scope_enabled(scope, true);
        let mname = CString::new("ffi.handle.metric").unwrap();

        let h = unsafe { aorta_trace_metric(scope, mname.as_ptr(), 0) };
        assert!(
            !h.is_null(),
            "metric handle must be non-null for a valid scope"
        );
        // The handle is stable per (scope, name) — the SDK caches it once.
        let h2 = unsafe { aorta_trace_metric(scope, mname.as_ptr(), 0) };
        assert_eq!(h, h2, "metric handle must be stable per (scope, name)");

        let before = unsafe { aorta_trace_user_metric_count(scope, mname.as_ptr()) };
        unsafe {
            aorta_trace_record_by_handle(h, 100);
            aorta_trace_record_by_handle(h, 200);
        }
        let after = unsafe { aorta_trace_user_metric_count(scope, mname.as_ptr()) };
        assert_eq!(
            after - before,
            2,
            "record_by_handle must bump the same histogram"
        );

        // A null handle and the sentinel scope are safe no-ops / NULL.
        unsafe { aorta_trace_record_by_handle(std::ptr::null(), 1) };
        assert!(unsafe { aorta_trace_metric(0, mname.as_ptr(), 0) }.is_null());
    });
}

#[test]
fn ffi_record_is_noop_when_master_off() {
    guard(|| {
        set_trace_enabled(false);
        let sname = CString::new("ffi.off.scope").unwrap();
        let scope = unsafe { aorta_trace_scope(sname.as_ptr()) };
        set_scope_enabled(scope, true); // scope on, but master off
        let mname = CString::new("ffi.off.metric").unwrap();
        let before = metric_count(scope, "ffi.off.metric");
        let _ = unsafe { aorta_trace_record(scope, mname.as_ptr(), LogLevel::Info as u8, 99) };
        assert_eq!(
            metric_count(scope, "ffi.off.metric"),
            before,
            "no-op when master off"
        );
    });
}
