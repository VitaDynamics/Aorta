//! Aorta tracing — a low-overhead, cross-language distributed-tracing subsystem.
//!
//! # Independent from telemetry and logging
//!
//! Tracing is a **separate purpose** from telemetry and logging and shares no
//! lifecycle, cadence, transport, storage, or enable gate with them:
//!
//! - **Telemetry** (`crate::telemetry`) is anomaly/heartbeat/snapshot oriented
//!   (latest-value queryables on a slow cadence). Its gates are the
//!   `AORTA_TELEMETRY_*` per-family env bools read once at start.
//! - **Logging** (`crate::logging`) is human-readable event text via the
//!   `tracing` crate, gated by `AORTA_LOG`.
//! - **Tracing** (this module) is low-overhead structured stage/span emission
//!   for latency analysis and cross-host correlation. Its gate is **its own**
//!   process-static [`set_trace_enabled`] master switch plus per-scope bytes
//!   (see [`scope`]); it is driven by `AORTA_TRACE`, never a shared env var,
//!   shared control key, or umbrella "observability off" switch.
//!
//! This module **must not** import `crate::telemetry`, and `crate::telemetry`
//! must not import this module (enforced by a guard test). The only permitted
//! coupling is the `trace_id` join key, exposed elsewhere as a by-id reference.
//!
//! # Low overhead is the #1 goal
//!
//! When tracing is disabled (or a call's level is below its scope threshold),
//! the call site must cost a single relaxed atomic load + a predicted-not-taken
//! branch, evaluate no arguments, read no clock, and allocate nothing. Two tiers
//! deliver this:
//!
//! - **Compile-time** ([`COMPILED_MIN_LEVEL`]): the `aorta_trace_max_level_*`
//!   cargo features raise a `const` floor; calls below it fold to a constant
//!   `false` and the optimizer deletes the call site (true zero cost).
//! - **Runtime** ([`trace_enabled`] + per-scope gate): a relaxed `AtomicU8`
//!   read. Default is **off**.
//!
//! Stage/span timing uses [`crate::time::monotonic_ns`] — never the wall clock
//! ([`crate::time::unix_time_ns`]), which can absorb NTP/DST steps and produce
//! negative or skewed latencies.

use std::sync::atomic::{AtomicU8, Ordering};

/// The EVENT-TYPE axis of the user API: a timed `Span` (latency), a point
/// `Instant` (occurrence), or a `Counter` (a recorded value). FLOW/causal links
/// are separate (they require cross-process trace-context propagation).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Span = 0,
    Instant = 1,
    Counter = 2,
}

impl EventType {
    /// Stable lowercase name for export labels.
    pub const fn as_str(self) -> &'static str {
        match self {
            EventType::Span => "span",
            EventType::Instant => "instant",
            EventType::Counter => "counter",
        }
    }

    /// Reconstruct from a `u8` discriminant (for the C-ABI); unknown -> `Span`.
    pub const fn from_u8(v: u8) -> EventType {
        match v {
            1 => EventType::Instant,
            2 => EventType::Counter,
            _ => EventType::Span,
        }
    }
}

pub mod chunk;
pub mod export;
pub mod histogram;
pub mod intern;
pub mod ring;
pub mod runtime;
pub mod scope;
pub mod sink;
pub mod span;
pub mod stage;
pub mod user;

pub use crate::logging::LogLevel;

// Tracing reuses `logging::LogLevel` as the single level taxonomy (DRY), and
// depends on its EXACT `repr(u8)` discriminants: the per-scope gate byte stores
// `level as u8` (see `level_value`) and the C-ABI maps `u8 -> LogLevel`
// (`trace_ffi::level_from_u8`). Pin them here so any reordering or renumbering in
// `logging.rs` breaks THIS compile rather than silently shifting gate semantics
// across the FFI boundary.
const _: () = {
    assert!(level_value(LogLevel::Trace) == 0);
    assert!(level_value(LogLevel::Debug) == 1);
    assert!(level_value(LogLevel::Info) == 2);
    assert!(level_value(LogLevel::Warn) == 3);
    assert!(level_value(LogLevel::Error) == 4);
};

pub use chunk::{ChunkEventType, RingRecord};
pub use histogram::{FixedHistogram, HistogramSnapshot};
pub use intern::{intern_event, thread_cpu_ns};
pub use scope::{
    gate_allows, intern_scope, scope_active, scope_enabled, scope_gate, scope_name,
    scope_threshold, set_scope_enabled, set_scope_threshold, SCOPE_NONE,
};
pub use span::{
    adopt_remote_context, clear_trace_context, counter, current_outbound_context,
    emit_completed_span, emit_completed_span_linked, emit_completed_span_linked_with_thread_delta,
    inbound_root_link, instant, mint_span_id, outbound_link, set_thread_time_enabled,
    set_trace_context, span_begin, span_end, thread_time_enabled, AdoptedContext, SpanGuard,
    SpanLink, SpanToken, TraceParent, TRACEPARENT_SAMPLED, TRACEPARENT_WIRE_LEN,
};
pub use stage::{
    all_stage_snapshots, emit_stage_span_at, emit_stage_span_under, flow_counter, flow_instant,
    flow_trace_active, marker_event_for, record_sched_callback, record_sched_wait, record_stage,
    record_stage_elapsed, record_stage_elapsed_as, record_stage_elapsed_linked,
    record_stage_elapsed_under, record_topic_span, sched_counter, sched_instant,
    sched_scope_enabled, stage_event_for, stage_snapshot, topic_scope_enabled, StageId,
    SCHED_SCOPE_NAME, STAGE_COUNT, TOPIC_SCOPE_NAME,
};
pub use user::{
    all_user_snapshots, record_user, register_user_metric, user_metric_count, StageTimer,
};

/// Public user instrumentation: time a scope/RAII block. Returns a [`SpanGuard`]
/// that on drop emits a Perfetto **begin/end span** to the waterfall ring AND
/// records the elapsed latency into the `(scope, name)` aggregate histogram —
/// **one site, both outputs**. The master gate is checked FIRST, so a
/// disabled call-site is a single relaxed load + branch and never interns the
/// scope, reads the clock, or allocates — not even on its first call. Once
/// enabled, the scope handle, gate, event id, and histogram are interned once and
/// cached per call-site; the level/tier filter (`gate_allows`) decides whether
/// this site records at all.
///
/// ```ignore
/// let _t = aorta_trace_span!(aorta_core::trace::LogLevel::Info, "app.planner", "replan");
/// ```
#[macro_export]
macro_rules! aorta_trace_span {
    ($level:expr, $scope:expr, $name:expr) => {{
        // Master gate FIRST — `$scope`/`$level`/the histogram are only touched
        // when tracing is on, so a disabled span never interns or allocates.
        if $crate::trace::trace_enabled() {
            static SCOPE: ::std::sync::OnceLock<u16> = ::std::sync::OnceLock::new();
            static GATE: ::std::sync::OnceLock<&'static ::std::sync::atomic::AtomicU8> =
                ::std::sync::OnceLock::new();
            static EVENT: ::std::sync::OnceLock<u16> = ::std::sync::OnceLock::new();
            static HIST: ::std::sync::OnceLock<&'static $crate::trace::FixedHistogram> =
                ::std::sync::OnceLock::new();
            let scope = *SCOPE.get_or_init(|| $crate::trace::intern_scope($scope));
            let gate = *GATE.get_or_init(|| $crate::trace::scope_gate(scope));
            if $crate::trace::gate_allows(gate, $level) {
                let event = *EVENT.get_or_init(|| $crate::trace::intern_event($name));
                let hist = *HIST.get_or_init(|| {
                    $crate::trace::register_user_metric(
                        scope,
                        $name,
                        $crate::trace::EventType::Span,
                    )
                });
                let token =
                    $crate::trace::span_begin(scope, event, $crate::trace::level_value($level), 0);
                $crate::trace::SpanGuard::new(token, Some(hist))
            } else {
                $crate::trace::SpanGuard::inert()
            }
        } else {
            $crate::trace::SpanGuard::inert()
        }
    }};
}

/// Public user instrumentation: record a pre-computed latency (ns) for a
/// `(scope, name)` metric at `level`. The `$ns` expression is evaluated **only
/// when** `(scope, level)` is enabled, so any cost to compute it is skipped on
/// the disabled path (one branch after a few cached relaxed loads).
///
/// ```ignore
/// aorta_trace_record!(aorta_core::trace::LogLevel::Info, "app.io", "flush", elapsed_ns);
/// ```
#[macro_export]
macro_rules! aorta_trace_record {
    ($level:expr, $scope:expr, $name:expr, $ns:expr) => {{
        // Master gate FIRST: a disabled call-site never interns, allocates, or
        // evaluates `$ns`/`$scope`/`$level`.
        if $crate::trace::trace_enabled() {
            static SCOPE: ::std::sync::OnceLock<u16> = ::std::sync::OnceLock::new();
            static GATE: ::std::sync::OnceLock<&'static ::std::sync::atomic::AtomicU8> =
                ::std::sync::OnceLock::new();
            static HIST: ::std::sync::OnceLock<&'static $crate::trace::FixedHistogram> =
                ::std::sync::OnceLock::new();
            let scope = *SCOPE.get_or_init(|| $crate::trace::intern_scope($scope));
            let gate = *GATE.get_or_init(|| $crate::trace::scope_gate(scope));
            if $crate::trace::gate_allows(gate, $level) {
                let hist = *HIST.get_or_init(|| {
                    $crate::trace::register_user_metric(
                        scope,
                        $name,
                        $crate::trace::EventType::Span,
                    )
                });
                hist.record($ns);
            }
        }
    }};
}

/// Public user instrumentation: an INSTANT event — record one occurrence of
/// `(scope, name)` at `level`. Gated; nothing happens when disabled.
///
/// ```ignore
/// aorta_trace_instant!(aorta_core::trace::LogLevel::Info, "app.fsm", "state_change");
/// ```
#[macro_export]
macro_rules! aorta_trace_instant {
    ($level:expr, $scope:expr, $name:expr) => {{
        // Master gate FIRST: a disabled call-site never interns or allocates.
        if $crate::trace::trace_enabled() {
            static SCOPE: ::std::sync::OnceLock<u16> = ::std::sync::OnceLock::new();
            static GATE: ::std::sync::OnceLock<&'static ::std::sync::atomic::AtomicU8> =
                ::std::sync::OnceLock::new();
            static EVENT: ::std::sync::OnceLock<u16> = ::std::sync::OnceLock::new();
            static HIST: ::std::sync::OnceLock<&'static $crate::trace::FixedHistogram> =
                ::std::sync::OnceLock::new();
            let scope = *SCOPE.get_or_init(|| $crate::trace::intern_scope($scope));
            let gate = *GATE.get_or_init(|| $crate::trace::scope_gate(scope));
            if $crate::trace::gate_allows(gate, $level) {
                // Both tiers: aggregate count (histogram) + timeline event (ring).
                let hist = *HIST.get_or_init(|| {
                    $crate::trace::register_user_metric(
                        scope,
                        $name,
                        $crate::trace::EventType::Instant,
                    )
                });
                hist.record(1);
                let event = *EVENT.get_or_init(|| $crate::trace::intern_event($name));
                $crate::trace::instant(scope, event, $crate::trace::level_value($level));
            }
        }
    }};
}

/// Public user instrumentation: a COUNTER — record a `$value` (u64) for
/// `(scope, name)` at `level`. The `$value` is evaluated only when enabled.
///
/// ```ignore
/// aorta_trace_counter!(aorta_core::trace::LogLevel::Info, "app.io", "queue_depth", depth);
/// ```
#[macro_export]
macro_rules! aorta_trace_counter {
    ($level:expr, $scope:expr, $name:expr, $value:expr) => {{
        // Master gate FIRST: a disabled call-site never interns, allocates, or
        // evaluates `$value`.
        if $crate::trace::trace_enabled() {
            static SCOPE: ::std::sync::OnceLock<u16> = ::std::sync::OnceLock::new();
            static GATE: ::std::sync::OnceLock<&'static ::std::sync::atomic::AtomicU8> =
                ::std::sync::OnceLock::new();
            static EVENT: ::std::sync::OnceLock<u16> = ::std::sync::OnceLock::new();
            static HIST: ::std::sync::OnceLock<&'static $crate::trace::FixedHistogram> =
                ::std::sync::OnceLock::new();
            let scope = *SCOPE.get_or_init(|| $crate::trace::intern_scope($scope));
            let gate = *GATE.get_or_init(|| $crate::trace::scope_gate(scope));
            if $crate::trace::gate_allows(gate, $level) {
                // Both tiers: aggregate distribution (histogram) + timeline
                // counter sample (ring). `$value` is evaluated once.
                let value = $value;
                let hist = *HIST.get_or_init(|| {
                    $crate::trace::register_user_metric(
                        scope,
                        $name,
                        $crate::trace::EventType::Counter,
                    )
                });
                hist.record(value);
                let event = *EVENT.get_or_init(|| $crate::trace::intern_event($name));
                $crate::trace::counter(scope, event, $crate::trace::level_value($level), value);
            }
        }
    }};
}

/// Gate byte value meaning "off" — no level passes, since levels are `0..=4`.
pub const GATE_OFF: u8 = u8::MAX;

/// Process-global MASTER tracing switch. `0` = disabled (the default).
///
/// This is the tracing subsystem's OWN gate; it is independent of telemetry's
/// `AORTA_TELEMETRY_*` switches and of logging's `AORTA_LOG`. Toggling telemetry
/// or logging has no effect here, and vice-versa.
static TRACE_ENABLED: AtomicU8 = AtomicU8::new(0);

/// Is tracing globally enabled? A single relaxed atomic load — the hot-path
/// master gate. Per-scope/level filtering layers on top (see [`scope`]).
#[inline(always)]
pub fn trace_enabled() -> bool {
    TRACE_ENABLED.load(Ordering::Relaxed) != 0
}

/// Enable or disable tracing globally. Cold path (config/control), not the hot
/// path. Does not touch the time source, logging, or telemetry.
#[inline]
pub fn set_trace_enabled(on: bool) {
    TRACE_ENABLED.store(on as u8, Ordering::Relaxed);
}

/// Address of the process-global master gate byte, for the C-ABI / SDK inline
/// near-zero-off check. The pointee is a single `u8` (`0` = off) at a stable
/// process-lifetime address; callers MUST read it atomically (relaxed). This is
/// the C-ABI-safe gate handoff: a plain byte, not a Rust `AtomicU8` exposed by
/// type.
pub fn master_gate_ptr() -> *const u8 {
    (&TRACE_ENABLED as *const AtomicU8).cast::<u8>()
}

/// Numeric verbosity of a level (`Trace=0 .. Error=4`) for gate comparisons.
#[inline(always)]
pub const fn level_value(level: LogLevel) -> u8 {
    level as u8
}

/// Compile-time verbosity floor. Calls strictly below this level are dead-code-
/// eliminated at the gate (true zero cost). Controlled by the
/// `aorta_trace_max_level_*` cargo features; the default (no feature) is `0`, so
/// every level is compiled in (and gated at runtime).
pub const COMPILED_MIN_LEVEL: u8 = compiled_min_level();

const fn compiled_min_level() -> u8 {
    // At most one max-level feature should be set; checked most-restrictive
    // first. `off` removes all levels.
    if cfg!(feature = "aorta_trace_max_level_off") {
        GATE_OFF
    } else if cfg!(feature = "aorta_trace_max_level_error") {
        4
    } else if cfg!(feature = "aorta_trace_max_level_warn") {
        3
    } else if cfg!(feature = "aorta_trace_max_level_info") {
        2
    } else if cfg!(feature = "aorta_trace_max_level_debug") {
        1
    } else {
        0
    }
}

/// Is `level` compiled in at all? Folds to a compile-time constant so sub-floor
/// call sites are eliminated entirely.
//
// `COMPILED_MIN_LEVEL` is feature-conditional: in the default build it is `0`
// (so the comparison is intentionally always-true — every level compiled in),
// but with `aorta_trace_max_level_*` it is a real floor that eliminates call
// sites. clippy's absurd-extreme-comparisons lint only sees the default-build
// constant, so it is allowed here by design.
#[allow(clippy::absurd_extreme_comparisons)]
#[inline(always)]
pub const fn level_compiled_in(level: LogLevel) -> bool {
    level_value(level) >= COMPILED_MIN_LEVEL
}
