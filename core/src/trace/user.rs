//! User-defined trace metrics — the backing for the public instrumentation API
//! (Step 8).
//!
//! Aorta-internal stages use the fixed [`crate::trace::StageId`] table; *user*
//! instrumentation records arbitrary `(scope, name)` latency metrics here. Each
//! `(scope, name)` gets a leaked, process-lifetime [`FixedHistogram`] (so a
//! call-site can cache the reference and record lock-free). Registration is
//! cold (locks once per call-site); recording is lock-free and gated.
//!
//! The ergonomic surface is the [`crate::aorta_trace_span`] /
//! [`crate::aorta_trace_record`] macros + [`StageTimer`]; this module is their
//! engine. Tracing-owned; no telemetry import.

use crate::trace::{
    gate_allows, scope_gate, EventType, FixedHistogram, HistogramSnapshot, LogLevel,
};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Per-metric entry: its histogram + the event type it was first registered as.
type Entry = (&'static FixedHistogram, EventType);
/// Metrics within one scope, keyed by name. Nested under the scope handle so the
/// per-name lookup borrows `&str` (via `String: Borrow<str>`) and allocates
/// nothing on the already-registered path — important because the C-ABI record
/// path (`record_user`) re-resolves the metric on every enabled call.
type ScopeMetrics = HashMap<String, Entry>;

static METRICS: OnceLock<Mutex<HashMap<u16, ScopeMetrics>>> = OnceLock::new();

fn metrics() -> &'static Mutex<HashMap<u16, ScopeMetrics>> {
    METRICS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Intern a `(scope, name)` user metric of the given [`EventType`] and return its
/// stable histogram reference. Idempotent; cold (locks once); first registration
/// fixes the event type. Call-sites cache the result so the hot path is
/// lock-free. The histogram is leaked (process-lifetime), mirroring scope gates.
/// The already-registered path borrows `name` (no allocation); only first
/// registration allocates the owned key.
pub fn register_user_metric(
    scope: u16,
    name: &str,
    event_type: EventType,
) -> &'static FixedHistogram {
    let mut map = metrics().lock().expect("trace user metrics poisoned");
    let inner = map.entry(scope).or_default();
    if let Some(&(h, _)) = inner.get(name) {
        return h;
    }
    let hist: &'static FixedHistogram = Box::leak(Box::new(FixedHistogram::new()));
    inner.insert(name.to_string(), (hist, event_type));
    hist
}

/// Record a user value by `(scope, name, event_type, level)` (convenience;
/// prefer the macros, which cache the gate + histogram per call-site). Gated: a
/// no-op when the scope/level is disabled. For a `Span` the value is a latency;
/// for an `Instant` use `1` (occurrence); for a `Counter` the recorded value.
pub fn record_user(scope: u16, name: &str, event_type: EventType, level: LogLevel, value: u64) {
    if gate_allows(scope_gate(scope), level) {
        register_user_metric(scope, name, event_type).record(value);
    }
}

/// Recorded sample count for a `(scope, name)` user metric (`0` if not
/// registered). Backs the C-ABI read-back so the C++/Python SDKs (and their
/// tests) can verify the real engine actually recorded.
pub fn user_metric_count(scope: u16, name: &str) -> u64 {
    let map = metrics().lock().expect("trace user metrics poisoned");
    map.get(&scope)
        .and_then(|inner| inner.get(name))
        .map(|(hist, _)| hist.count())
        .unwrap_or(0)
}

/// Snapshot every registered user metric as `(scope_handle, name, event_type,
/// snapshot)` for export/analysis.
pub fn all_user_snapshots() -> Vec<(u16, String, EventType, HistogramSnapshot)> {
    let map = metrics().lock().expect("trace user metrics poisoned");
    map.iter()
        .flat_map(|(scope, inner)| {
            inner.iter().map(move |(name, (hist, event_type))| {
                (*scope, name.clone(), *event_type, hist.snapshot())
            })
        })
        .collect()
}

/// RAII span timer: captures the monotonic start time on construction and
/// records the elapsed latency into its histogram on drop — but ONLY when
/// active. An inactive timer (`hist == None`) reads no clock, holds no
/// histogram, and does nothing on drop. Crucially, the macro passes `None` when
/// disabled WITHOUT registering a histogram, so a disabled span never locks or
/// allocates (not even on first use).
pub struct StageTimer {
    hist: Option<&'static FixedHistogram>,
    start: u64,
}

impl StageTimer {
    /// Start a timer. `hist` is `Some` only when the gate passed (the macro
    /// resolves/registers the histogram lazily, so disabled = no registration).
    #[inline]
    pub fn start(hist: Option<&'static FixedHistogram>) -> Self {
        Self {
            start: if hist.is_some() {
                crate::time::monotonic_ns()
            } else {
                0
            },
            hist,
        }
    }
}

impl Drop for StageTimer {
    #[inline]
    fn drop(&mut self) {
        if let Some(hist) = self.hist {
            hist.record(crate::time::monotonic_ns().saturating_sub(self.start));
        }
    }
}
