//! Tests for the public user instrumentation API (Step 8): the
//! `aorta_trace_span!` / `aorta_trace_record!` macros + `StageTimer`.
//!
//! The load-bearing property is GATE-BEFORE-EVAL: a disabled call must not
//! evaluate its argument expression (proving zero work past the gate). Run
//! single-threaded (global gate state).

use crate::trace::{
    all_user_snapshots, intern_scope, register_user_metric, set_scope_enabled, set_trace_enabled,
    EventType, LogLevel,
};
use std::sync::atomic::{AtomicUsize, Ordering};

static EVAL_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Side-effecting "expensive" latency expression used to prove the macro only
/// evaluates its argument when enabled.
fn expensive() -> u64 {
    EVAL_COUNT.fetch_add(1, Ordering::SeqCst);
    42
}

fn guard<T>(f: impl FnOnce() -> T) -> T {
    crate::trace_test_support::guarded(f)
}

fn user_metric_count(scope: u16, name: &str) -> u64 {
    all_user_snapshots()
        .into_iter()
        .find(|(s, n, _, _)| *s == scope && n == name)
        .map(|(_, _, _, snap)| snap.count)
        .unwrap_or(0)
}

#[test]
fn record_macro_gates_before_evaluating_args() {
    guard(|| {
        let h = intern_scope("test.user.gate");

        set_trace_enabled(false);
        let before = EVAL_COUNT.load(Ordering::SeqCst);
        crate::aorta_trace_record!(LogLevel::Info, "test.user.gate", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before,
            "disabled must NOT evaluate the latency expression"
        );

        set_trace_enabled(true);
        set_scope_enabled(h, true);
        crate::aorta_trace_record!(LogLevel::Info, "test.user.gate", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before + 1,
            "enabled must evaluate the latency expression exactly once"
        );
    });
}

#[test]
fn record_macro_respects_level_threshold() {
    guard(|| {
        let h = intern_scope("test.user.level");
        set_trace_enabled(true);
        // Enable only Warn+; an Info record must be gated out (no eval).
        crate::trace::set_scope_threshold(h, LogLevel::Warn as u8);
        let before = EVAL_COUNT.load(Ordering::SeqCst);
        crate::aorta_trace_record!(LogLevel::Info, "test.user.level", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before,
            "below-threshold gated"
        );
        crate::aorta_trace_record!(LogLevel::Error, "test.user.level", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before + 1,
            "at/above-threshold runs"
        );
    });
}

#[test]
fn span_timer_records_on_drop_when_enabled() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("test.user.timer");
        set_scope_enabled(h, true);
        let before = user_metric_count(h, "work");
        {
            let _t = crate::aorta_trace_span!(LogLevel::Info, "test.user.timer", "work");
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
        let after = user_metric_count(h, "work");
        assert_eq!(
            after - before,
            1,
            "RAII timer records exactly one sample on drop"
        );
    });
}

#[test]
fn span_timer_noop_when_disabled() {
    guard(|| {
        set_trace_enabled(false);
        let h = intern_scope("test.user.timer.off");
        let before = user_metric_count(h, "work");
        {
            let _t = crate::aorta_trace_span!(LogLevel::Info, "test.user.timer.off", "work");
        }
        let after = user_metric_count(h, "work");
        assert_eq!(after, before, "disabled span records nothing");
    });
}

#[test]
fn register_user_metric_is_idempotent() {
    let h = intern_scope("test.user.idem");
    let a = register_user_metric(h, "metric", EventType::Span);
    let b = register_user_metric(h, "metric", EventType::Span);
    assert!(
        std::ptr::eq(a, b),
        "same (scope,name) returns the same histogram"
    );
}

fn user_metric(scope: u16, name: &str) -> Option<(EventType, u64, u64)> {
    all_user_snapshots()
        .into_iter()
        .find(|(s, n, _, _)| *s == scope && n == name)
        .map(|(_, _, et, snap)| (et, snap.count, snap.max_ns))
}

#[test]
fn instant_macro_counts_occurrences() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("test.user.instant");
        set_scope_enabled(h, true);
        let before = user_metric(h, "ev").map(|(_, c, _)| c).unwrap_or(0);
        crate::aorta_trace_instant!(LogLevel::Info, "test.user.instant", "ev");
        crate::aorta_trace_instant!(LogLevel::Info, "test.user.instant", "ev");
        let (et, count, _) = user_metric(h, "ev").expect("instant metric registered");
        assert_eq!(et, EventType::Instant);
        assert_eq!(count - before, 2, "each instant records one occurrence");
    });
}

#[test]
fn counter_macro_records_values() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("test.user.counter");
        set_scope_enabled(h, true);
        crate::aorta_trace_counter!(LogLevel::Info, "test.user.counter", "depth", 1000u64);
        let (et, count, max) = user_metric(h, "depth").expect("counter metric registered");
        assert_eq!(et, EventType::Counter);
        assert!(count >= 1);
        assert!(max >= 1000, "counter records the value into the histogram");
    });
}

#[test]
fn counter_macro_does_not_eval_value_when_disabled() {
    guard(|| {
        let h = intern_scope("test.user.counter.gate");
        set_trace_enabled(false);
        let before = EVAL_COUNT.load(Ordering::SeqCst);
        crate::aorta_trace_counter!(LogLevel::Info, "test.user.counter.gate", "c", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before,
            "disabled counter must not eval value"
        );
        set_trace_enabled(true);
        set_scope_enabled(h, true);
        crate::aorta_trace_counter!(LogLevel::Info, "test.user.counter.gate", "c", expensive());
        assert_eq!(EVAL_COUNT.load(Ordering::SeqCst), before + 1);
    });
}

#[test]
fn disabled_span_does_not_register_metric() {
    guard(|| {
        set_trace_enabled(false); // master off
        let h = intern_scope("test.user.noreg");
        {
            let _t = crate::aorta_trace_span!(LogLevel::Info, "test.user.noreg", "blk_noreg");
        }
        let registered = all_user_snapshots()
            .into_iter()
            .any(|(s, n, _, _)| s == h && n == "blk_noreg");
        assert!(
            !registered,
            "a disabled span must not register a user metric (no lock/alloc on the disabled path)"
        );
    });
}
