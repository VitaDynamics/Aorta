//! Alert evaluators.
//!
//! Pure functions that turn a freshly-collected [`Snapshot`] plus the
//! relevant thresholds from [`Config`] into zero-or-more [`Alert`]
//! records. The daemon publishes each `Alert` on the alert topic
//! (separate from the snapshot topic).
//!
//! Each sub-module covers one [`AlertKind`]:
//!  - [`psi`] — PSI stall_ratio threshold crossings.
//!  - [`process`] — top-K process exceeding `cpu_percent_host` /
//!    `rss_bytes` / `fd_count` thresholds.
//!  - `collector` — collector errors are passed through verbatim from
//!    the snapshot's `collector_errors` field. The pass-through lives
//!    inline in [`evaluate`] because it has no per-rule state.

pub mod process;
pub mod psi;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::snapshot::Snapshot;

/// Severity tier emitted on the wire (mirrors the schema enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warn,
    Error,
}

impl AlertSeverity {
    /// Parse one of `info` / `warn` / `error` (case-insensitive) from
    /// config. Unknown strings fall back to `Warn` rather than
    /// panicking — config drift should be visible but not fatal.
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "info" => Self::Info,
            "warn" => Self::Warn,
            "error" => Self::Error,
            _ => Self::Warn,
        }
    }
}

/// One alert event, ready to publish.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub severity: AlertSeverity,
    pub observed_at_ns: u64,
    pub monotonic_ns: u64,
    pub payload: AlertPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertPayload {
    Psi(PsiAlert),
    ProcessTop(ProcessTopAlert),
    Collector(CollectorAlert),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsiAlert {
    pub resource: String,
    pub line: String,
    pub stall_ratio: f32,
    pub threshold: f32,
    pub window_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessTopAlert {
    pub pid: i32,
    pub comm: String,
    pub metric: String,
    pub value: f64,
    pub threshold: f64,
    pub top_k: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorAlert {
    pub family: String,
    pub detail: String,
}

/// Evaluate every alert rule against a freshly-collected snapshot.
///
/// `observed_at_ns` / `monotonic_ns` are stamped on every emitted
/// alert and should be captured at tick start so all alerts produced
/// from the same snapshot share an identity.
pub fn evaluate(
    snap: &Snapshot,
    cfg: &Config,
    observed_at_ns: u64,
    monotonic_ns: u64,
) -> Vec<Alert> {
    let mut out: Vec<Alert> = Vec::new();
    out.extend(psi::evaluate(snap, cfg, observed_at_ns, monotonic_ns));
    out.extend(process::evaluate(snap, cfg, observed_at_ns, monotonic_ns));
    // Collector errors are surfaced 1:1 from the snapshot and
    // tracked at INFO severity; once the snapshot collector
    // actually starts populating `collector_errors`, this loop emits
    // one alert per row. Today the field is always empty, so this is
    // a forward-compat hook.
    for err in &snap.capabilities {
        if let Some(reason) = err.degraded_reason.as_deref() {
            // Skip the "configured off" / "not implemented"
            // placeholder reasons — those are intentional, not errors.
            if !reason.is_empty()
                && reason != "configured off"
                && !reason.contains("not implemented")
                && err.runtime_status == crate::capability::RuntimeStatus::Degraded
            {
                out.push(Alert {
                    severity: AlertSeverity::Warn,
                    observed_at_ns,
                    monotonic_ns,
                    payload: AlertPayload::Collector(CollectorAlert {
                        family: err.family.clone(),
                        detail: reason.to_string(),
                    }),
                });
            }
        }
    }
    out
}
