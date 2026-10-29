//! Regression tests for FFI panic isolation guards.
//!
//! Covers the helpers in `crate::ffi::guard` in isolation: happy path
//! returns, panic catch + AORTA_ERR_INTERNAL, fallback values, void
//! variant, non-string panic payloads, and re-entrant nesting.
//!
//! End-to-end "panic dispatched through the FFI" testing was attempted
//! but is not feasible at this layer: Rust ≥ 1.81 marks every
//! `extern "C"` function as `nounwind`, so a panic that originates
//! inside a user-supplied callback aborts the process before any
//! surrounding `catch_unwind` (including `ffi_guard_void`) can observe
//! the unwind. See `crate::ffi::guard` module docs for the contract
//! split. C++ end-to-end coverage lives in the C++ SDK's
//! action-throwing-callback test suite.

use std::cell::Cell;
use std::ffi::CString;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};

use crate::ffi::c_api::{
    aorta_last_error, aorta_node_free, aorta_node_new, aorta_node_t, aorta_result_t,
    set_last_error, AORTA_ERR_INTERNAL, AORTA_OK,
};
use crate::ffi::guard::{ffi_guard, ffi_guard_value, ffi_guard_void};

const TLS_TEARDOWN_CHILD_ENV: &str = "AORTA_TEST_FFI_TLS_TEARDOWN_CHILD";
const TLS_TEARDOWN_CHILD_TEST: &str =
    "ffi_panic_isolation_tests::ffi_calls_survive_destroyed_thread_locals_child";

static TLS_TEARDOWN_NODE_FREE_RC: AtomicI32 = AtomicI32::new(i32::MIN);
static TLS_TEARDOWN_LAST_ERROR_EMPTY: AtomicBool = AtomicBool::new(false);
static TLS_TEARDOWN_TRACE_EMPTY: AtomicBool = AtomicBool::new(false);

struct TlsTeardownProbe {
    node: Cell<aorta_node_t>,
}

impl Drop for TlsTeardownProbe {
    fn drop(&mut self) {
        // This destructor was registered before Aorta's Drop-bearing TLS. The
        // calls therefore run after LAST_ERROR and trace STATE were destroyed,
        // matching the main-thread window between TLS and static destructors.
        set_last_error("ignored after TLS teardown");
        TLS_TEARDOWN_LAST_ERROR_EMPTY.store(aorta_last_error().is_null(), Ordering::SeqCst);

        crate::trace::set_trace_context([0x5a; 16], 7);
        TLS_TEARDOWN_TRACE_EMPTY.store(
            crate::trace::current_outbound_context().is_none(),
            Ordering::SeqCst,
        );

        TLS_TEARDOWN_NODE_FREE_RC.store(aorta_node_free(self.node.get()), Ordering::SeqCst);
    }
}

thread_local! {
    static TLS_TEARDOWN_PROBE: TlsTeardownProbe = const {
        TlsTeardownProbe { node: Cell::new(0) }
    };
}

#[test]
fn ffi_calls_survive_destroyed_thread_locals_child() {
    if std::env::var_os(TLS_TEARDOWN_CHILD_ENV).is_none() {
        return;
    }

    let node = std::thread::spawn(|| {
        let name = CString::new("ffi-tls-teardown-child").unwrap();
        let mut node = 0;
        assert_eq!(
            // SAFETY: `name` is a live NUL-terminated string, group is optional,
            // and `node` is writable for the duration of the call.
            unsafe { aorta_node_new(name.as_ptr(), std::ptr::null(), &mut node) },
            AORTA_OK
        );
        node
    })
    .join()
    .expect("node creation thread must return normally");

    std::thread::spawn(move || {
        // Register the probe destructor first so every Aorta TLS initialized
        // below is destroyed before the probe calls back into the FFI. Node
        // creation happens on another thread so this one does not pre-register
        // Tokio's runtime-context TLS before entering the teardown window.
        TLS_TEARDOWN_PROBE.with(|probe| probe.node.set(node));

        // Force both Drop-bearing TLS slots to initialize after the probe.
        set_last_error("initialize LAST_ERROR before thread exit");
        crate::trace::set_trace_context([0x11; 16], 3);
    })
    .join()
    .expect("TLS teardown thread must return normally");

    assert!(TLS_TEARDOWN_LAST_ERROR_EMPTY.load(Ordering::SeqCst));
    assert!(TLS_TEARDOWN_TRACE_EMPTY.load(Ordering::SeqCst));
    assert_eq!(TLS_TEARDOWN_NODE_FREE_RC.load(Ordering::SeqCst), AORTA_OK);
}

#[test]
// Miri cannot spawn the child process; regular cargo test keeps this coverage.
#[cfg_attr(miri, ignore)]
fn ffi_calls_survive_destroyed_thread_locals() {
    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", TLS_TEARDOWN_CHILD_TEST, "--nocapture"])
        .env(TLS_TEARDOWN_CHILD_ENV, "1")
        .output()
        .expect("run TLS teardown child process");

    assert!(
        output.status.success(),
        "TLS teardown child failed with {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn ffi_guard_passes_ok_through_when_body_does_not_panic() {
    let rc: aorta_result_t = ffi_guard(|| AORTA_OK);
    assert_eq!(rc, AORTA_OK);
}

#[test]
fn ffi_guard_passes_error_codes_through_unchanged() {
    // Body returns a non-zero error — guard must NOT remap successful error
    // returns to AORTA_ERR_INTERNAL; only actual panics get that treatment.
    let rc: aorta_result_t = ffi_guard(|| -42);
    assert_eq!(rc, -42);
}

#[test]
fn ffi_guard_catches_panic_and_returns_internal_error() {
    let rc: aorta_result_t = ffi_guard(|| {
        panic!("intentional test panic");
    });
    assert_eq!(rc, AORTA_ERR_INTERNAL);
}

#[test]
fn ffi_guard_void_runs_body_on_happy_path() {
    let counter = AtomicU32::new(0);
    ffi_guard_void(|| {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn ffi_guard_void_swallows_panic_and_returns_normally() {
    // The `ran` flag defends against false-pass if a future refactor
    // accidentally makes ffi_guard_void short-circuit before calling the
    // closure: without an observable side-effect from inside the closure,
    // a no-op implementation would also "swallow" the panic (vacuously)
    // and let us reach the post-call line.
    //
    // If the panic propagated PAST ffi_guard_void, the test thread itself
    // would abort and we would never reach the assertions below.
    let ran = AtomicBool::new(false);
    ffi_guard_void(|| {
        ran.store(true, Ordering::SeqCst);
        panic!("intentional test panic");
    });
    assert!(
        ran.load(Ordering::SeqCst),
        "ffi_guard_void must have invoked the closure"
    );
}

#[test]
fn ffi_guard_value_returns_inner_value_on_happy_path() {
    let v: u64 = ffi_guard_value(0u64, || 42);
    assert_eq!(v, 42);
}

#[test]
fn ffi_guard_value_returns_fallback_on_panic() {
    let v: u64 = ffi_guard_value(0u64, || {
        panic!("intentional test panic");
    });
    assert_eq!(v, 0);
}

#[test]
fn ffi_guard_handles_panic_with_non_string_payload() {
    // catch_unwind preserves any panic payload type, not just &str. Make
    // sure our guard isn't accidentally specializing on str payloads.
    let rc: aorta_result_t = ffi_guard(|| {
        std::panic::panic_any(42i32);
    });
    assert_eq!(rc, AORTA_ERR_INTERNAL);
}

#[test]
fn ffi_guard_is_re_entrant_safe() {
    // A guarded body that itself calls ffi_guard must still trap an inner
    // panic and surface AORTA_ERR_INTERNAL all the way out.
    let rc: aorta_result_t = ffi_guard(|| {
        ffi_guard(|| {
            panic!("inner panic");
        })
    });
    assert_eq!(rc, AORTA_ERR_INTERNAL);
}

// `serde_json::to_*` indirectly enters the FFI
// call stack via `core/src/telemetry/serde.rs` (the telemetry
// publisher / inventory / runtime modules all route through
// `to_json_zbytes`, which can be triggered through logging and topic
// publication paths that originate at FFI entry points). The serde
// library itself doesn't panic on well-formed types, but a user
// type's custom `Serialize` impl can — that's a common business-code
// footgun. ffi_guard must trap such a panic just like any other
// callback panic.

struct PanickingSerialize;

impl serde::Serialize for PanickingSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        panic!("intentional panic from PanickingSerialize::serialize");
    }
}

#[test]
fn ffi_guard_catches_panic_originating_in_serde_serialize_impl() {
    // Direct path: a panicking Serialize impl reached via
    // crate::telemetry::serde must be caught by ffi_guard. This is
    // the path exercised whenever an FFI entry triggers telemetry
    // emission with a user-typed payload.
    let rc: aorta_result_t = ffi_guard(|| {
        let _ = crate::telemetry::serde::to_json_string(&PanickingSerialize);
        AORTA_OK
    });
    assert_eq!(rc, AORTA_ERR_INTERNAL);
}

#[test]
fn ffi_guard_catches_panic_through_zbytes_path() {
    // Same coverage for the to_json_zbytes path used by the live
    // telemetry publisher (telemetry/publisher.rs:178 et al).
    let rc: aorta_result_t = ffi_guard(|| {
        let _ = crate::telemetry::serde::to_json_zbytes(&PanickingSerialize);
        AORTA_OK
    });
    assert_eq!(rc, AORTA_ERR_INTERNAL);
}

/// Read the thread-local last-error slot as a Rust string.
fn last_error_string() -> Option<String> {
    let ptr = crate::ffi::c_api::aorta_last_error();
    if ptr.is_null() {
        return None;
    }
    // SAFETY: aorta_last_error returns a valid NUL-terminated C string
    // owned by the thread-local slot; we copy it out immediately.
    Some(
        unsafe { std::ffi::CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned(),
    )
}

// After ANY caught panic, a caller that checks
// aorta_last_error() must see a message describing the panic — not NULL
// and not a stale message from an earlier, unrelated failure.

#[test]
fn ffi_guard_panic_updates_last_error() {
    crate::ffi::c_api::set_last_error("stale message from a previous failure");
    let rc: aorta_result_t = ffi_guard(|| {
        panic!("boom-core-01 marker");
    });
    assert_eq!(rc, AORTA_ERR_INTERNAL);
    let msg = last_error_string().expect("last_error must be set after a caught panic");
    assert!(
        msg.contains("boom-core-01 marker"),
        "last_error should carry the panic message, got: {}",
        msg
    );
}

#[test]
fn ffi_guard_value_panic_updates_last_error() {
    crate::ffi::c_api::set_last_error("stale message from a previous failure");
    let v: u64 = ffi_guard_value(0u64, || {
        panic!("boom-guard-value marker");
    });
    assert_eq!(v, 0);
    let msg = last_error_string().expect("last_error must be set after a caught panic");
    assert!(
        msg.contains("boom-guard-value marker"),
        "last_error should carry the panic message, got: {}",
        msg
    );
}

#[test]
fn ffi_guard_panic_with_non_string_payload_still_updates_last_error() {
    crate::ffi::c_api::set_last_error("stale message from a previous failure");
    let rc: aorta_result_t = ffi_guard(|| {
        std::panic::panic_any(42i32);
    });
    assert_eq!(rc, AORTA_ERR_INTERNAL);
    let msg = last_error_string().expect("last_error must be set after a caught panic");
    assert!(
        !msg.contains("stale message"),
        "last_error must not remain stale after a caught panic, got: {}",
        msg
    );
    assert!(
        msg.contains("panic"),
        "last_error should identify the failure as a caught panic, got: {}",
        msg
    );
}

// An interior NUL byte in an error message
// (reachable from a foreign peer's reply_err payload since the
// switch to raw lossy-UTF8 reply text) must not silently destroy the
// entire message for C/C++/Python callers.

#[test]
fn set_last_error_preserves_message_with_interior_nul() {
    crate::ffi::c_api::set_last_error("prefix-part\0suffix-part");
    let msg = last_error_string().expect("last_error must be set");
    assert!(
        msg.contains("prefix-part") && msg.contains("suffix-part"),
        "both sides of the interior NUL must survive, got: {}",
        msg
    );
    assert!(
        !msg.contains("invalid error message"),
        "message must not be replaced by the generic fallback, got: {}",
        msg
    );
}
