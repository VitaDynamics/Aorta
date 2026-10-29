//! FFI panic isolation utilities.
//!
//! Wraps every `pub extern "C"` entry point so a Rust panic never crosses the
//! FFI boundary. Modern Rust toolchains abort the process when a panic
//! unwinds through an `extern "C"` frame — this would turn any internal
//! invariant violation in `aorta_core` into a hard crash of the host
//! process (the embedding C++/Python application). Catching the panic
//! here, surfacing `AORTA_ERR_INTERNAL` to the caller, and continuing
//! lets the host application observe the failure and decide its own
//! recovery policy.
//!
//! # What these helpers DO catch
//!
//! - Panics raised by Aorta-internal Rust code while executing the body
//!   of a guarded `extern "C" fn` (lock-acquisition failure, integer
//!   overflow in handle arithmetic, an `expect()` inside a helper, an
//!   `unreachable!()` in a match arm, etc.).
//! - Panics raised by Aorta-internal Rust code in the dispatcher
//!   closure that invokes a user callback (`bytes_view_from_zbytes_*`
//!   panicking on a malformed payload, a registry lookup that blew up,
//!   etc.) — i.e. the wrap covers everything in the closure body
//!   *except* the user's own callback function call itself.
//!
//! # What these helpers do NOT catch
//!
//! - Panics that ORIGINATE inside a user-supplied `extern "C"` callback.
//!   Rust ≥ 1.81 marks every `extern "C"` function as `nounwind` at the
//!   LLVM level; a panic that tries to escape such a function aborts the
//!   process before the surrounding `catch_unwind` ever sees the unwind.
//!   That is a contract the user must hold up — if their callback can
//!   panic, they need to wrap their own body in `catch_unwind` or
//!   declare the function as `extern "C-unwind"`. The C++ SDK enforces
//!   this contract by wrapping every callback dispatcher in `try/catch`.
//!
//! The helpers are intentionally tiny and dependency-free so that
//! wiring them into every guarded entry point (~170 at the time of
//! writing — recount with
//! `grep -c 'pub \(unsafe \)\?extern "C"' core/src/ffi/c_api/*.rs`
//! rather than trusting this comment) adds negligible code size and the
//! guard itself is unlikely to introduce new panic paths. Any panic
//! raised inside the best-effort `eprintln!` notification is itself
//! swallowed by a nested `catch_unwind` — the guard's safety property
//! must hold even if stderr is closed.

use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::ffi::c_api::{aorta_result_t, set_last_error, AORTA_ERR_INTERNAL};

/// Wraps an FFI body that returns [`aorta_result_t`]. Catches any Rust panic
/// that escapes the closure and returns [`AORTA_ERR_INTERNAL`] instead of
/// letting the panic propagate across the FFI boundary.
///
/// Use at the top of every `pub extern "C" fn` that returns
/// `aorta_result_t`:
///
/// ```ignore
/// pub unsafe extern "C" fn aorta_publisher_new(...) -> aorta_result_t {
///     ffi_guard(|| {
///         // body that may panic
///         AORTA_OK
///     })
/// }
/// ```
#[inline]
pub fn ffi_guard<F>(f: F) -> aorta_result_t
where
    F: FnOnce() -> aorta_result_t,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(rc) => rc,
        Err(payload) => {
            record_panic_last_error(payload.as_ref());
            log_panic_caught("aorta::ffi", "returning AORTA_ERR_INTERNAL");
            AORTA_ERR_INTERNAL
        }
    }
}

/// Variant for entries that return a non-[`aorta_result_t`] value such as a
/// handle or length. Returns the supplied `fallback` on panic.
///
/// Use for getters that signal failure with a sentinel value (e.g. handle
/// equal to `0` or length equal to `usize::MAX`):
///
/// ```ignore
/// pub unsafe extern "C" fn aorta_last_error() -> *const c_char {
///     ffi_guard_value(std::ptr::null(), || { /* ... */ })
/// }
/// ```
#[inline]
pub fn ffi_guard_value<R, F>(fallback: R, f: F) -> R
where
    F: FnOnce() -> R,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(payload) => {
            record_panic_last_error(payload.as_ref());
            log_panic_caught("aorta::ffi", "returning fallback value");
            fallback
        }
    }
}

/// Wraps a user-callback dispatch site. The callback signature has no
/// return value, so a panic is simply caught and logged; the surrounding
/// dispatcher continues as if the callback returned cleanly.
///
/// Used at every site where Aorta invokes a function pointer supplied by
/// the C/C++/Python caller:
///
/// ```ignore
/// ffi_guard_void(|| unsafe {
///     (cb)(&ctx_view, payload_ptr, payload_len, user_data);
/// });
/// ```
#[inline]
pub fn ffi_guard_void<F>(f: F)
where
    F: FnOnce(),
{
    if let Err(payload) = catch_unwind(AssertUnwindSafe(f)) {
        record_panic_last_error(payload.as_ref());
        log_panic_caught("aorta::ffi::callback", "swallowing panic (void return)");
    }
}

/// Record a caught panic into the thread-local last-error slot so a caller
/// checking `aorta_last_error()` after an `AORTA_ERR_INTERNAL` (or fallback
/// value) sees what actually happened instead of NULL or a stale message
/// from an earlier, unrelated failure.
///
/// `set_last_error` uses fallible TLS access because a panic raised by a bare
/// `LocalKey::with` can enter the panic hook before this nested catch gets a
/// chance to recover. The nested catch remains for non-TLS failures such as an
/// allocation panic while formatting the diagnostic.
fn record_panic_last_error(payload: &(dyn std::any::Any + Send)) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
            *s
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.as_str()
        } else {
            "<non-string panic payload>"
        };
        set_last_error(format!("panic caught at FFI boundary: {msg}"));
    }));
}

/// Best-effort panic notification. Deliberately uses `eprintln!` rather than
/// the project's `crate::logging` machinery: logging itself can panic during
/// init/teardown (e.g. tracing subscriber not yet installed, file logger
/// errors), and the guard MUST NOT propagate a second panic out of its
/// outer `catch_unwind`. The whole call is wrapped in another
/// `catch_unwind` so even if `eprintln!` somehow panics (closed stderr,
/// OOM, etc.) the guard remains safe.
///
/// The `action` parameter describes what the guard does next (e.g.
/// "returning AORTA_ERR_INTERNAL" / "returning fallback value" /
/// "swallowing panic (void return)") so the operator looking at logs
/// can tell which control-flow path was taken. The previous
/// unconditional "returning AORTA_ERR_INTERNAL"
/// suffix was misleading for `ffi_guard_value` / `ffi_guard_void`
/// call sites.
fn log_panic_caught(target: &'static str, action: &'static str) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        eprintln!("[{target}] panic caught at FFI boundary; {action}");
    }));
}
