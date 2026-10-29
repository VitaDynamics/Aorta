//! aorta_sdk-LEVEL trace tests.
//!
//! These prove the SDK RE-EXPORT path reaches the SAME process-global tracing
//! engine as aorta_core: the public user macros (`aorta_trace_span!` /
//! `record!` / `instant!` / `counter!`) and the `aorta_sdk::trace::*` surface,
//! invoked exactly as a downstream user would. Distinct from the core tests,
//! which exercise the engine directly.
//!
//! Run single-threaded — the master gate is a process-global `AtomicU8` shared
//! with core and across all tests; each gate-touching test runs inside `guard()`
//! (save/restore) and uses a unique scope name + count deltas.

use crate::trace::{
    all_user_snapshots, intern_scope, set_scope_enabled, set_scope_threshold, set_trace_enabled,
    stage_snapshot, trace_enabled, EventType, LogLevel, StageId, COMPILED_MIN_LEVEL, GATE_OFF,
    STAGE_COUNT,
};
use crate::{aorta_trace_counter, aorta_trace_instant, aorta_trace_record, aorta_trace_span};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

static EVAL_COUNT: AtomicUsize = AtomicUsize::new(0);

fn expensive() -> u64 {
    EVAL_COUNT.fetch_add(1, Ordering::SeqCst);
    7
}

fn guard<T>(f: impl FnOnce() -> T) -> T {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let g = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let prev = trace_enabled();
    let r = f();
    set_trace_enabled(prev);
    drop(g);
    r
}

fn user_metric(scope: u16, name: &str) -> Option<(EventType, u64, u64)> {
    all_user_snapshots()
        .into_iter()
        .find(|(s, n, _, _)| *s == scope && n == name)
        .map(|(_, _, et, snap)| (et, snap.count, snap.max_ns))
}

#[test]
fn sdk_set_trace_enabled_reaches_the_same_engine() {
    guard(|| {
        set_trace_enabled(true);
        assert!(
            trace_enabled(),
            "SDK re-export toggles the same process-global gate"
        );
        set_trace_enabled(false);
        assert!(!trace_enabled());
    });
}

#[test]
fn sdk_span_macro_records_on_drop() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("sdk.test.span");
        set_scope_enabled(h, true);
        let before = user_metric(h, "work").map(|(_, c, _)| c).unwrap_or(0);
        {
            let _t = aorta_trace_span!(LogLevel::Info, "sdk.test.span", "work");
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
        let after = user_metric(h, "work").map(|(_, c, _)| c).unwrap_or(0);
        assert_eq!(
            after - before,
            1,
            "SDK span macro records one sample on drop"
        );
    });
}

#[test]
fn sdk_span_macro_emits_ring_records_for_the_waterfall() {
    guard(|| {
        use crate::trace::chunk::{ChunkEventType, RingRecord};
        set_trace_enabled(true);
        let h = intern_scope("sdk.test.span.ring");
        set_scope_enabled(h, true);
        // Clear any stragglers, then run a span and a nested span.
        let mut scratch: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut scratch);
        {
            let _outer = aorta_trace_span!(LogLevel::Info, "sdk.test.span.ring", "outer");
            let _inner = aorta_trace_span!(LogLevel::Info, "sdk.test.span.ring", "inner");
        }
        let mut recs: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut recs);
        let ours: Vec<_> = recs.into_iter().filter(|r| r.scope_id == h).collect();
        // 2 spans × (begin + end) = 4 ring records — the waterfall side of the
        // SAME macro that fed the aggregate histogram.
        assert_eq!(
            ours.len(),
            4,
            "unified span macro emits begin/end to the ring"
        );
        let begins = ours
            .iter()
            .filter(|r| ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin)
            .count();
        assert_eq!(begins, 2);
    });
}

#[test]
fn sdk_span_macro_noop_when_disabled() {
    guard(|| {
        set_trace_enabled(false);
        let h = intern_scope("sdk.test.span.off");
        {
            let _t = aorta_trace_span!(LogLevel::Info, "sdk.test.span.off", "work");
        }
        assert!(
            user_metric(h, "work").is_none(),
            "a disabled SDK span must not register a metric"
        );
    });
}

#[test]
fn sdk_record_macro_gates_before_evaluating_args() {
    guard(|| {
        let h = intern_scope("sdk.test.record");
        set_trace_enabled(false);
        let before = EVAL_COUNT.load(Ordering::SeqCst);
        aorta_trace_record!(LogLevel::Info, "sdk.test.record", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before,
            "disabled must not eval the arg"
        );
        set_trace_enabled(true);
        set_scope_enabled(h, true);
        aorta_trace_record!(LogLevel::Info, "sdk.test.record", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before + 1,
            "enabled evaluates once"
        );
    });
}

#[test]
fn sdk_instant_macro_counts_occurrences() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("sdk.test.instant");
        set_scope_enabled(h, true);
        let before = user_metric(h, "ev").map(|(_, c, _)| c).unwrap_or(0);
        aorta_trace_instant!(LogLevel::Info, "sdk.test.instant", "ev");
        aorta_trace_instant!(LogLevel::Info, "sdk.test.instant", "ev");
        let (et, count, _) = user_metric(h, "ev").expect("instant metric registered");
        assert_eq!(et, EventType::Instant);
        assert_eq!(count - before, 2);
    });
}

#[test]
fn sdk_counter_macro_records_value() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("sdk.test.counter");
        set_scope_enabled(h, true);
        aorta_trace_counter!(LogLevel::Info, "sdk.test.counter", "depth", 2048u64);
        let (et, count, max) = user_metric(h, "depth").expect("counter metric registered");
        assert_eq!(et, EventType::Counter);
        assert!(count >= 1 && max >= 2048);
    });
}

#[test]
fn sdk_level_threshold_gates_via_macros() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("sdk.test.level");
        set_scope_threshold(h, LogLevel::Warn as u8);
        let before = EVAL_COUNT.load(Ordering::SeqCst);
        aorta_trace_record!(LogLevel::Info, "sdk.test.level", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before,
            "below-threshold is gated out"
        );
        aorta_trace_record!(LogLevel::Error, "sdk.test.level", "m", expensive());
        assert_eq!(
            EVAL_COUNT.load(Ordering::SeqCst),
            before + 1,
            "at/above-threshold runs"
        );
    });
}

#[test]
fn sdk_stage_snapshot_and_stageid_accessible() {
    // The SDK can query the built-in service/action stage histograms + taxonomy.
    let _ = stage_snapshot(StageId::ServiceClientCall).count; // must not panic
    assert_eq!(StageId::ALL.len(), STAGE_COUNT);
    for (i, s) in StageId::ALL.iter().enumerate() {
        assert_eq!(*s as u8, i as u8);
        assert_eq!(StageId::from_u8(i as u8), Some(*s));
    }
}

#[test]
fn sdk_default_build_compiles_in_all_levels() {
    // aorta_sdk sets no aorta_trace_max_level_* feature, so the compile-time
    // floor is Trace (0). A future feature flip should make this fail on purpose.
    assert_eq!(COMPILED_MIN_LEVEL, 0);
    assert_eq!(GATE_OFF, u8::MAX);
}
