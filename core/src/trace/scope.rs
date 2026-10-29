//! Trace scope registry (Step 4).
//!
//! A *scope* is a provider/category string (e.g. `"aorta.service"`,
//! `"app.planner"`) — the unit of enable/disable and the SCOPE axis of the user
//! API. Scopes are interned once to a stable `u16` handle (handle `0` is a
//! reserved sentinel meaning "no scope", so real handles start at `1`).
//!
//! Each scope owns a per-scope gate byte storing its **minimum enabled level**
//! (a [`LogLevel`] value `0..=4`, or [`GATE_OFF`] = disabled). The byte lives at
//! a stable process-lifetime address (leaked), so the hot path can cache the
//! gate reference once and read it inline, and so the C-ABI (Step 10) can hand a
//! C-compatible pointer to it. The default for a new scope is the registry
//! default threshold (off unless configured/enabled).
//!
//! Effective enablement is **master AND scope AND compiled-in**:
//! `trace_enabled() && level_compiled_in(level) && level >= scope_threshold`.

use crate::trace::{
    level_compiled_in, level_value, set_trace_enabled, trace_enabled, LogLevel, GATE_OFF,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{OnceLock, RwLock};

/// Reserved sentinel handle meaning "no scope". Real handles start at `1`.
pub const SCOPE_NONE: u16 = 0;

/// Fallback gate for the sentinel / an unregistered handle. Always reads
/// [`GATE_OFF`], so a fabricated handle arriving over the C-ABI fails *safe*
/// (tracing disabled) instead of panicking on an out-of-bounds index — and it is
/// never written through (writes no-op on bad handles), so it can never be
/// corrupted into an "enabled" state.
static OFF_GATE: AtomicU8 = AtomicU8::new(GATE_OFF);

struct Inner {
    by_name: HashMap<String, u16>,
    /// `gates[handle - 1]` — leaked so the address is stable for the process.
    gates: Vec<&'static AtomicU8>,
    names: Vec<String>,
    /// Threshold applied to scopes registered without an explicit per-scope
    /// override. Defaults to [`GATE_OFF`] (tracing off until enabled).
    default_threshold: u8,
    /// Thresholds configured before a scope was registered (e.g. from the
    /// environment); applied on registration.
    pending: HashMap<String, u8>,
}

/// Process-global scope registry.
pub struct ScopeRegistry {
    inner: RwLock<Inner>,
}

impl ScopeRegistry {
    fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                by_name: HashMap::new(),
                gates: Vec::new(),
                names: Vec::new(),
                default_threshold: GATE_OFF,
                pending: HashMap::new(),
            }),
        }
    }

    /// Intern a scope name (idempotent) and return its stable handle (`>= 1`).
    pub fn intern(&self, name: &str) -> u16 {
        // (overflow WARN helper defined at module scope below)
        {
            let inner = self.inner.read().expect("trace scope registry poisoned");
            if let Some(&h) = inner.by_name.get(name) {
                return h;
            }
        }
        let mut inner = self.inner.write().expect("trace scope registry poisoned");
        if let Some(&h) = inner.by_name.get(name) {
            return h;
        }
        // Never abort — on exhaustion return 0,
        // the EXISTING "no scope" sentinel: the overflowing scope simply
        // traces as disabled, which is the right degraded mode for a
        // diagnostics subsystem.
        let Ok(handle) = u16::try_from(inner.gates.len() + 1) else {
            scope_overflow_warn();
            return 0;
        };
        let threshold = inner
            .pending
            .get(name)
            .copied()
            .unwrap_or(inner.default_threshold);
        let gate: &'static AtomicU8 = Box::leak(Box::new(AtomicU8::new(threshold)));
        inner.by_name.insert(name.to_string(), handle);
        inner.gates.push(gate);
        inner.names.push(name.to_string());
        handle
    }

    /// Stable gate reference for a handle. For a valid handle this is the scope's
    /// own leaked gate (the hot path caches it). For the sentinel (`0`) or an
    /// unregistered handle — which the C-ABI may be handed from C — it returns the
    /// shared read-only [`OFF_GATE`] (fail-safe: reads as disabled, never panics).
    #[inline]
    pub fn gate(&self, handle: u16) -> &'static AtomicU8 {
        if handle == SCOPE_NONE {
            return &OFF_GATE;
        }
        let inner = self.inner.read().expect("trace scope registry poisoned");
        inner
            .gates
            .get(handle as usize - 1)
            .copied()
            .unwrap_or(&OFF_GATE)
    }

    /// Store a threshold for a handle, ignoring the sentinel or an unregistered
    /// handle (gates only ever grow, so a handle that is in range stays valid).
    /// The bounds check keeps a fabricated C-ABI handle from writing through the
    /// shared [`OFF_GATE`] fallback.
    pub fn set_threshold_by_handle(&self, handle: u16, threshold: u8) {
        if handle == SCOPE_NONE {
            return;
        }
        let inner = self.inner.read().expect("trace scope registry poisoned");
        if let Some(gate) = inner.gates.get(handle as usize - 1) {
            gate.store(threshold, Ordering::Relaxed);
        }
    }

    /// Scope name for a handle, if registered. The sentinel (`0` — "no
    /// scope", also the overflow return) resolves to `None` instead
    /// of underflowing the index (debug builds would panic on `0 - 1`).
    pub fn name(&self, handle: u16) -> Option<String> {
        if handle == SCOPE_NONE {
            return None;
        }
        let inner = self.inner.read().expect("trace scope registry poisoned");
        inner.names.get(handle as usize - 1).cloned()
    }

    /// Set a scope's threshold by name. If the scope is not yet registered the
    /// threshold is remembered and applied on registration.
    pub fn set_threshold(&self, name: &str, threshold: u8) {
        let mut inner = self.inner.write().expect("trace scope registry poisoned");
        if let Some(&h) = inner.by_name.get(name) {
            inner.gates[h as usize - 1].store(threshold, Ordering::Relaxed);
        } else {
            inner.pending.insert(name.to_string(), threshold);
        }
    }

    /// Set the default threshold for future scopes AND apply it to every
    /// existing scope (a level floor for all scopes). Does not touch the master
    /// switch.
    pub fn set_all(&self, threshold: u8) {
        let mut inner = self.inner.write().expect("trace scope registry poisoned");
        inner.default_threshold = threshold;
        for gate in &inner.gates {
            gate.store(threshold, Ordering::Relaxed);
        }
    }

    /// Number of registered scopes.
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .expect("trace scope registry poisoned")
            .gates
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

static REGISTRY: OnceLock<ScopeRegistry> = OnceLock::new();

/// The process-global scope registry.
pub fn registry() -> &'static ScopeRegistry {
    REGISTRY.get_or_init(ScopeRegistry::new)
}

/// Intern (idempotently) a scope name and return its stable handle.
#[inline]
pub fn intern_scope(name: &str) -> u16 {
    registry().intern(name)
}

/// Stable gate reference for a handle — cache this once on the hot path.
#[inline]
pub fn scope_gate(handle: u16) -> &'static AtomicU8 {
    registry().gate(handle)
}

/// Scope name for a handle, if registered (for export labels).
pub fn scope_name(handle: u16) -> Option<String> {
    registry().name(handle)
}

/// Current threshold byte (minimum enabled level, or [`GATE_OFF`]).
#[inline]
pub fn scope_threshold(handle: u16) -> u8 {
    scope_gate(handle).load(Ordering::Relaxed)
}

/// Set a scope's minimum enabled level (or [`GATE_OFF`] to disable it). A
/// no-op for the sentinel / an unregistered handle.
#[inline]
pub fn set_scope_threshold(handle: u16, threshold: u8) {
    registry().set_threshold_by_handle(handle, threshold);
}

/// Convenience: enable a scope at all levels (`Trace`) or disable it.
#[inline]
pub fn set_scope_enabled(handle: u16, on: bool) {
    set_scope_threshold(
        handle,
        if on {
            level_value(LogLevel::Trace)
        } else {
            GATE_OFF
        },
    );
}

/// The load-bearing hot-path check, taking an already-cached gate reference.
/// Order matters for zero-overhead: the compile-time `level_compiled_in` const
/// folds first, then the master switch, then the per-scope byte. Two relaxed
/// loads in the enabled-build case; the disabled path is a predicted-not-taken
/// branch.
#[inline(always)]
pub fn gate_allows(gate: &AtomicU8, level: LogLevel) -> bool {
    level_compiled_in(level)
        && trace_enabled()
        && level_value(level) >= gate.load(Ordering::Relaxed)
}

/// Convenience hot/cold check by handle (looks the gate up; prefer caching the
/// gate via [`scope_gate`] on the hot path).
#[inline]
pub fn scope_enabled(handle: u16, level: LogLevel) -> bool {
    gate_allows(scope_gate(handle), level)
}

/// Is the scope active at all (master on and not [`GATE_OFF`]), ignoring a
/// specific level?
#[inline]
pub fn scope_active(handle: u16) -> bool {
    trace_enabled() && scope_gate(handle).load(Ordering::Relaxed) != GATE_OFF
}

fn parse_level(s: &str) -> Option<LogLevel> {
    match s.trim().to_ascii_lowercase().as_str() {
        "trace" => Some(LogLevel::Trace),
        "debug" => Some(LogLevel::Debug),
        "info" => Some(LogLevel::Info),
        "warn" | "warning" => Some(LogLevel::Warn),
        "error" => Some(LogLevel::Error),
        // Tier aliases: the 3 tiers map onto the level gate so
        // `level >= threshold` admits the right tiers. `always` (T1, the
        // default) is the HIGHEST-priority tier and maps to the `Info` floor;
        // `standard` (T2) lowers the floor to `Debug`; `verbose` (T3) to `Trace`.
        // Errors/warnings sit above `always`, so they always pass at any tier.
        "always" => Some(LogLevel::Info),
        "standard" => Some(LogLevel::Debug),
        "verbose" => Some(LogLevel::Trace),
        _ => None,
    }
}

fn parse_threshold(s: &str) -> u8 {
    if s.trim().eq_ignore_ascii_case("off") {
        GATE_OFF
    } else {
        parse_level(s).map(level_value).unwrap_or(GATE_OFF)
    }
}

/// Apply an `AORTA_TRACE` directive string (comma-separated):
/// - `off` — disable the master switch;
/// - `scope=level` — set one scope's threshold and turn the master on;
/// - `scope=off` — silence just that scope (does NOT touch the master switch);
/// - a bare level (`info`) — set the floor for all scopes and turn the master on.
///
/// This is the tracing subsystem's OWN config grammar — independent of
/// `AORTA_LOG` and `AORTA_TELEMETRY_*`.
pub fn apply_spec(spec: &str) {
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if part.eq_ignore_ascii_case("off") {
            set_trace_enabled(false);
        } else if let Some((name, level)) = part.split_once('=') {
            set_threshold_and_enable(name.trim(), parse_threshold(level));
        } else if let Some(level) = parse_level(part) {
            registry().set_all(level_value(level));
            set_trace_enabled(true);
        }
    }
}

fn set_threshold_and_enable(name: &str, threshold: u8) {
    registry().set_threshold(name, threshold);
    // A `scope=off` directive only silences that scope; it must not turn the
    // master switch ON (which would surprise-enable every other scope sitting at
    // an enabled threshold). Only a positive `scope=level` enables the master.
    if threshold != GATE_OFF {
        set_trace_enabled(true);
    }
}

/// Initialize trace gating from the `AORTA_TRACE` environment variable. Absent or
/// empty leaves tracing off (the default). Separate from `AORTA_LOG` /
/// `AORTA_TELEMETRY_*`.
pub fn init_from_env() {
    if let Ok(spec) = std::env::var("AORTA_TRACE") {
        apply_spec(&spec);
    }
}

/// One-shot exhaustion WARN for the scope registry.
fn scope_overflow_warn() {
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        crate::aorta_core_warn!(
            "Aorta WARN: trace scope registry exhausted its u16 handle space — further scopes trace as disabled (the process continues)"
        );
    }
}
