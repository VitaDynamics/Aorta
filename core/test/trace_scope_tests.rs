//! Tests for the trace scope registry + per-scope gating (Step 4).

use crate::trace::{
    intern_scope, level_value, scope_active, scope_enabled, scope_gate, scope_threshold,
    set_scope_enabled, set_scope_threshold, set_trace_enabled, trace_enabled, LogLevel, GATE_OFF,
    SCOPE_NONE,
};

/// Serialize tests that mutate the global master switch, restoring it afterward.
/// Scope names are unique per test, so the (leaked, never-removed) per-scope
/// gates do not interfere.
fn guard<T>(f: impl FnOnce() -> T) -> T {
    crate::trace_test_support::guarded(f)
}

#[test]
fn intern_returns_stable_handle_for_same_name() {
    let a = intern_scope("trace.scope.stable");
    let b = intern_scope("trace.scope.stable");
    assert_eq!(a, b);
    assert!(std::ptr::eq(scope_gate(a), scope_gate(b)));
}

#[test]
fn intern_skips_zero_sentinel() {
    assert_eq!(SCOPE_NONE, 0);
    assert_ne!(intern_scope("trace.scope.skipzero"), SCOPE_NONE);
}

#[test]
fn distinct_names_get_distinct_handles() {
    let a = intern_scope("trace.scope.distinct.a");
    let b = intern_scope("trace.scope.distinct.b");
    assert_ne!(a, b);
}

#[test]
fn per_scope_gate_independent() {
    guard(|| {
        set_trace_enabled(true);
        let a = intern_scope("trace.scope.indep.a");
        let b = intern_scope("trace.scope.indep.b");
        set_scope_enabled(a, true);
        set_scope_enabled(b, false);
        assert!(scope_enabled(a, LogLevel::Trace));
        assert!(!scope_enabled(b, LogLevel::Trace));
    });
}

#[test]
fn scope_gate_respects_master_gate() {
    guard(|| {
        let a = intern_scope("trace.scope.master.a");
        set_scope_enabled(a, true);

        set_trace_enabled(false);
        assert!(
            !scope_enabled(a, LogLevel::Error),
            "master off disables scope"
        );
        assert!(!scope_active(a));

        set_trace_enabled(true);
        assert!(
            scope_enabled(a, LogLevel::Error),
            "master on re-enables scope"
        );
        assert!(scope_active(a));
    });
}

#[test]
fn unregistered_handle_is_failsafe_not_panic() {
    guard(|| {
        set_trace_enabled(true);
        // Reads on the sentinel / a fabricated handle are fail-safe: they read
        // GATE_OFF, never panic, and never report enabled.
        let bogus = u16::MAX; // never interned
        assert_eq!(scope_threshold(bogus), GATE_OFF);
        assert_eq!(scope_threshold(SCOPE_NONE), GATE_OFF);
        assert!(!scope_enabled(bogus, LogLevel::Error));
        assert!(!scope_active(bogus));
        // Writes through a bad handle must no-op — crucially they must NOT corrupt
        // the shared OFF fallback into an enabled state.
        set_scope_threshold(bogus, level_value(LogLevel::Trace));
        set_scope_enabled(bogus, true);
        assert_eq!(
            scope_threshold(bogus),
            GATE_OFF,
            "bad-handle write must not stick"
        );
        assert_eq!(
            scope_threshold(SCOPE_NONE),
            GATE_OFF,
            "OFF fallback uncorrupted"
        );
        // A real scope is unaffected and still fully functional.
        let real = intern_scope("trace.scope.failsafe.real");
        set_scope_enabled(real, true);
        assert!(scope_enabled(real, LogLevel::Info));
    });
}

#[test]
fn apply_spec_scope_off_does_not_enable_master() {
    guard(|| {
        set_trace_enabled(false);
        // `scope=off` silences just that scope; it must NOT flip the master on
        // (which would surprise-enable every other scope at an enabled threshold).
        crate::trace::scope::apply_spec("trace.scope.spec.off=off");
        assert!(
            !trace_enabled(),
            "scope=off must not turn the master switch on"
        );
        // A positive `scope=level` DOES enable the master.
        crate::trace::scope::apply_spec("trace.scope.spec.on=info");
        assert!(trace_enabled(), "scope=level enables the master");
        let h = intern_scope("trace.scope.spec.on");
        assert!(scope_enabled(h, LogLevel::Info));
        assert!(!scope_enabled(h, LogLevel::Debug));
    });
}

#[test]
fn scope_threshold_filters_by_level() {
    guard(|| {
        set_trace_enabled(true);
        let a = intern_scope("trace.scope.level.a");
        set_scope_threshold(a, level_value(LogLevel::Info));
        assert!(scope_enabled(a, LogLevel::Info));
        assert!(scope_enabled(a, LogLevel::Warn));
        assert!(scope_enabled(a, LogLevel::Error));
        assert!(!scope_enabled(a, LogLevel::Debug));
        assert!(!scope_enabled(a, LogLevel::Trace));
    });
}

#[test]
fn phase2_tier_aliases_map_onto_the_level_gate() {
    guard(|| {
        // `always` (T1) → Info floor: only Info+ passes (errors/warns always do).
        set_trace_enabled(false);
        crate::trace::scope::apply_spec("always");
        assert!(trace_enabled(), "a tier enables the master");
        let s = intern_scope("trace.tier.always");
        assert!(scope_enabled(s, LogLevel::Info), "T1 admits always");
        assert!(scope_enabled(s, LogLevel::Error), "errors always pass");
        assert!(!scope_enabled(s, LogLevel::Debug), "T2 filtered at T1");
        assert!(!scope_enabled(s, LogLevel::Trace), "T3 filtered at T1");

        // `standard` (T2) lowers the floor to Debug.
        crate::trace::scope::apply_spec("standard");
        assert!(scope_enabled(s, LogLevel::Debug), "T2 admitted at standard");
        assert!(!scope_enabled(s, LogLevel::Trace), "T3 still filtered");

        // `verbose` (T3) admits everything.
        crate::trace::scope::apply_spec("verbose");
        assert!(scope_enabled(s, LogLevel::Trace), "T3 admitted at verbose");
    });
}

#[test]
fn off_directive_disables_master() {
    guard(|| {
        crate::trace::scope::apply_spec("always");
        assert!(trace_enabled());
        crate::trace::scope::apply_spec("off");
        assert!(!trace_enabled(), "off disables the master");
    });
}
