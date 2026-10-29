//! Shared support for the trace test suite.
//!
//! The tracing engine is process-global (the master gate, the scope registry,
//! and the stage + user histograms all live in `static`s), so tests that toggle
//! it must run serially. Every trace test file routes its local guard through the
//! ONE lock here, so the whole suite serializes even under a default
//! multi-threaded `cargo test` run — not only under `--test-threads=1`.

use std::sync::{Mutex, MutexGuard, OnceLock};

static TRACE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Acquire the one shared trace-test lock (poison-tolerant — a panicking test
/// must not wedge the rest of the suite).
pub fn lock() -> MutexGuard<'static, ()> {
    TRACE_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Run `f` while holding the shared lock, restoring the master switch to its
/// prior state afterward so tests do not perturb each other.
pub fn guarded<T>(f: impl FnOnce() -> T) -> T {
    let _g = lock();
    let prev = crate::trace::trace_enabled();
    let r = f();
    crate::trace::set_trace_enabled(prev);
    r
}
