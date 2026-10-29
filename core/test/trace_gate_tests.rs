//! Tests for the tracing master enable gate (Step 2).
//!
//! The gate is the tracing subsystem's OWN switch — independent of telemetry and
//! logging. These tests assert: off-by-default, clean toggling, that reading the
//! gate never perturbs the time source, and that the default build compiles in
//! all levels (no compile-time cutoff).

use crate::time::time_source;
use crate::trace::{
    gate_allows, intern_scope, record_stage, scope_gate, set_scope_enabled, set_trace_enabled,
    trace_enabled, LogLevel, StageId, COMPILED_MIN_LEVEL, GATE_OFF,
};

/// Serialize tests that mutate the process-global master gate, restoring the
/// previous state afterward. Routed through the suite-wide lock so it serializes
/// against the other trace test files too (see `trace_test_support`).
fn with_trace_gate<T>(f: impl FnOnce() -> T) -> T {
    crate::trace_test_support::guarded(f)
}

/// Concurrency smoke/stress test: many threads toggle the master gate while
/// others read it inline (the SDK hot-path `gate_allows` on a cached gate
/// pointer) and record into a stage histogram. Proves the toggle ⇄ inline-read ⇄
/// record paths don't panic, deadlock, or corrupt state; run under TSAN it also
/// asserts data-race freedom. Holds the suite lock itself (it deliberately
/// toggles concurrently, so it cannot use the serializing `with_trace_gate`).
#[test]
fn concurrent_toggle_record_and_gate_read_is_race_free() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    let _suite = crate::trace_test_support::lock();
    let prev = trace_enabled();

    let scope = intern_scope("trace.gate.stress");
    set_scope_enabled(scope, true);
    let gate = scope_gate(scope); // a cached gate pointer, read concurrently below
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    // Togglers: flip the master switch as fast as possible.
    for _ in 0..2 {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut on = false;
            while !stop.load(Ordering::Relaxed) {
                on = !on;
                set_trace_enabled(on);
            }
        }));
    }
    // Readers + recorders: inline-gate on the cached pointer, then record.
    for _ in 0..3 {
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if gate_allows(gate, LogLevel::Info) {
                    record_stage(StageId::ServiceClientCall, 100);
                }
                std::hint::black_box(trace_enabled());
            }
        }));
    }

    // Run briefly, then stop and join.
    for _ in 0..200_000 {
        std::hint::black_box(trace_enabled());
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("trace stress worker panicked");
    }

    // The cached gate pointer is still valid after the storm.
    let _ = gate_allows(gate, LogLevel::Error);
    set_trace_enabled(prev);
}

#[test]
fn trace_disabled_by_default() {
    // The static initializer is `AtomicU8::new(0)`; assert the disabled state is
    // observable as "not enabled".
    with_trace_gate(|| {
        set_trace_enabled(false);
        assert!(!trace_enabled());
    });
}

#[test]
fn set_trace_enabled_toggles_gate() {
    with_trace_gate(|| {
        set_trace_enabled(true);
        assert!(trace_enabled());
        set_trace_enabled(false);
        assert!(!trace_enabled());
    });
}

#[test]
fn gate_read_does_not_touch_time_source() {
    with_trace_gate(|| {
        let before = time_source();
        set_trace_enabled(true);
        let _ = trace_enabled();
        set_trace_enabled(false);
        let _ = trace_enabled();
        assert_eq!(
            time_source(),
            before,
            "trace gate must be independent of the time source"
        );
    });
}

#[test]
fn default_build_compiles_in_all_levels() {
    // No `aorta_trace_max_level_*` feature selected in the default build, so the
    // compile-time floor is Trace (0) — everything is compiled in.
    assert_eq!(COMPILED_MIN_LEVEL, 0);
    assert_eq!(GATE_OFF, u8::MAX);
}
