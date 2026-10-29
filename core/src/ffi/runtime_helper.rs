//! Block-on guard for FFI entry points.
//!
//! FFI entry points drive Aorta from C/C++/Python. Most of them need to
//! poll an async future to completion before they can return a value to
//! the caller, so the canonical pattern is `runtime().block_on(fut)`.
//! This is safe on any thread that isn't already driving the runtime's
//! async scheduler: non-runtime threads, and Tokio's own
//! `spawn_blocking` pool both work — only worker threads currently
//! polling an async task hit Tokio's
//! "Cannot start a runtime from within a runtime" panic.
//!
//! The footgun is **reentrancy** from a worker thread. When an FFI
//! entry point is invoked from inside an Aorta callback that runs on a
//! worker (subscriber `on_msg`, service handler, context-store watch
//! change), the calling thread is currently polling a task. Calling
//! `block_on` panics, and per Rust 1.81+ that panic crosses the
//! `extern "C"` boundary and aborts the process. The `ffi_guard`
//! cannot catch it.
//!
//! `ffi_block_on!` catches that exact panic with
//! `std::panic::catch_unwind` and converts it into
//! `Err(NestedRuntimeError)`. The FFI entry point then maps it to
//! `AORTA_ERR_REENTRANT`. This works even though we can't directly
//! distinguish worker threads from `spawn_blocking` threads through
//! Tokio's public API — the FFI raw action handler (which runs on a
//! dedicated per-goal thread — see `offload_dedicated` in
//! `core/src/flow/offload.rs`) and any future callback running under
//! `spawn_blocking` hit the `runtime().block_on`
//! success path normally, because tokio's internal blocking-context
//! tracker is only set on worker threads.
//!
//! The macro form is deliberate — a generic `fn run_blocking<F:
//! Future>` would force `F: 'static`, which most call sites can't
//! satisfy because they borrow `&str` arguments straight from the C
//! caller's stack.
//!
//! See the SDK reentrancy contract block in `cpp/include/aorta/aorta.h`
//! for the user-facing rules.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Returned by [`ffi_block_on`] when the caller is already inside a
/// Tokio runtime. FFI entry points convert it to `AORTA_ERR_REENTRANT`.
pub struct NestedRuntimeError;

// The explicit reentrancy detector lives in `crate::flow::reentrancy`
// as thread-local flags set (a) while this thread is inside
// an aorta `block_on` — e.g. a C thread parked in `aorta_executor_spin`,
// whose dispatched callbacks run on this same thread — and (b) while this
// thread is inside a dispatched callback (`Event::dispatch_isolated`, the
// single dispatch point). Either flag makes reentrancy exact: no panic, no
// string matching. Foreign runtimes (an application's own Tokio) still fall
// back to the probe/classifier path below. `spawn_blocking` threads carry
// neither flag, so their (legal) `block_on` stays on the success path.

/// Returns the process-wide FFI runtime. Initialized on first use with
/// two worker threads — enough for the small number of concurrent
/// blocking calls FFI clients typically issue without leaving the
/// runtime starved during nested cleanup.
///
/// Marked `pub` (with `#[doc(hidden)]`) because the `#[macro_export]`'d
/// `ffi_block_on!` macro expands to call it via `$crate::...` — a
/// downstream consumer of the macro would otherwise hit a visibility
/// error at the call site. The function is not intended as part of the
/// public API surface and is hidden from rustdoc accordingly.
#[doc(hidden)]
pub fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("create tokio runtime")
    })
}

/// Returns `true` iff blocking on the FFI runtime from the current thread
/// would be reentrant: inside an aorta `block_on`, inside a dispatched
/// aorta callback (the explicit Flow reentrancy detector), or currently
/// driving an async task on a Tokio runtime worker (the probe fallback).
/// Used by FFI entry points whose signature can't go through
/// `ffi_block_on!` (e.g. `aorta_executor_spin`, which blocks indefinitely
/// and can't carry a deferred reentrancy error) and by free/teardown paths
/// choosing between block-here and spawn-and-detach.
///
/// Implementation: probe by running a minimal
/// `runtime().block_on(async { 0 })` through
/// [`run_or_classify_reentrant`]. On a worker thread the probe panics
/// with Tokio's nested-runtime marker, which the classifier maps to
/// `Err(NestedRuntimeError)`. On a `spawn_blocking` thread, a
/// non-runtime thread, or before the runtime is initialised, the probe
/// completes silently. Any *other* panic — out-of-memory during
/// runtime init, etc. — propagates so `ffi_guard` can convert it to
/// `AORTA_ERR_INTERNAL`; swallowing it as "in async context" would
/// silently mis-route the FREE paths to spawn-and-detach.
pub(crate) fn in_async_context() -> bool {
    // Explicit detector first: inside an aorta block_on
    // or a dispatched callback — exact, panic-free.
    if crate::flow::reentrancy::explicitly_reentrant() {
        return true;
    }
    // Cheap pre-filter: if no Tokio runtime is reachable at all, we
    // can't be on a worker. Saves spinning up the static runtime in
    // the common "C caller, not in any runtime" path.
    if tokio::runtime::Handle::try_current().is_err() {
        return false;
    }
    match run_or_classify_reentrant(|| runtime().block_on(async { 0u8 })) {
        Ok(_) => false,
        Err(NestedRuntimeError) => true,
    }
}

/// Helper used by [`ffi_block_on`]: runs `f` under `catch_unwind` and
/// classifies a caught panic as `NestedRuntimeError` if its message
/// contains Tokio's known reentrancy marker, or re-raises otherwise.
///
/// Hidden from the public surface via doc(hidden) — it's only exposed
/// because the macro needs to call it across `$crate::ffi::*`.
#[doc(hidden)]
pub fn run_or_classify_reentrant<R>(f: impl FnOnce() -> R) -> Result<R, NestedRuntimeError> {
    // Explicit detector first: a thread inside an aorta
    // block_on or a dispatched callback is reentrant by construction —
    // return the error without constructing and classifying a panic. The
    // probe/classifier below remains as the fallback for foreign runtime
    // contexts the flags cannot see.
    if crate::flow::reentrancy::explicitly_reentrant() {
        return Err(NestedRuntimeError);
    }
    let _guard = crate::flow::reentrancy::BlockOnGuard::enter();
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(value) => Ok(value),
        Err(payload) => {
            if is_runtime_nested_panic(&payload) {
                Err(NestedRuntimeError)
            } else {
                // Some other panic — re-raise so the outer `ffi_guard`
                // can convert it to AORTA_ERR_INTERNAL the usual way.
                std::panic::resume_unwind(payload)
            }
        }
    }
}

fn is_runtime_nested_panic(payload: &Box<dyn Any + Send + 'static>) -> bool {
    let msg: &str = if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        return false;
    };
    // Tokio 1.x emits this exact prefix from
    // `runtime::context::enter_runtime` and from `Handle::enter`.
    // We match a stable substring so a minor wording tweak doesn't
    // silently turn the protection back off.
    msg.contains("Cannot start a runtime from within a runtime")
        || msg.contains("cannot block the current thread from within a runtime")
}

/// Evaluates `$fut` on the FFI runtime, returning the result wrapped
/// in `Ok(..)`. If Tokio detects we're already driving an async task
/// on this thread and panics with the "Cannot start a runtime from
/// within a runtime" marker, the panic is caught and converted to
/// `Err(NestedRuntimeError)` so the FFI entry point can return
/// `AORTA_ERR_REENTRANT` to the C caller.
///
/// Other panics propagate so `ffi_guard` can convert them to
/// `AORTA_ERR_INTERNAL`.
///
/// The macro form preserves borrow lifetimes: the future `$fut` is
/// evaluated in-place, so callers can pass futures that borrow `&str`
/// arguments straight from the C ABI (`store.set_string(&key,
/// &value)`), which a generic `fn run_blocking<F: Future + 'static>`
/// cannot accept.
///
/// # Example
///
/// ```ignore
/// match crate::ffi_block_on!(publisher.undeclare()) {
///     Ok(Ok(())) => AORTA_OK,
///     Ok(Err(err)) => map_aorta_error(err),
///     Err(_) => AORTA_ERR_REENTRANT,
/// }
/// ```
#[macro_export]
macro_rules! ffi_block_on {
    ($fut:expr) => {{
        let fut = $fut;
        $crate::ffi::runtime_helper::run_or_classify_reentrant(move || {
            $crate::ffi::runtime_helper::runtime().block_on(fut)
        })
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_outside_runtime_evaluates_future() {
        let result: Result<i32, NestedRuntimeError> = ffi_block_on!(async { 7 });
        match result {
            Ok(v) => assert_eq!(v, 7),
            Err(_) => panic!("expected Ok outside runtime"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn block_on_inside_runtime_returns_nested_error() {
        let result: Result<i32, NestedRuntimeError> = ffi_block_on!(async { 7 });
        assert!(
            result.is_err(),
            "expected NestedRuntimeError inside runtime"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn in_async_context_true_inside_runtime() {
        assert!(in_async_context());
    }

    #[test]
    fn in_async_context_false_outside_runtime() {
        assert!(!in_async_context());
    }
}
