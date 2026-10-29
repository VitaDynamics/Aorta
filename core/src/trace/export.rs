//! Histogram export for analysis & visualization (Step 9).
//!
//! There is no wire format and no on-robot collector; this serializes the
//! tracing-OWNED histograms (internal stages + user metrics) to:
//! - **Prometheus** text exposition — scrape into Prometheus/Grafana for
//!   over-time p50/p95/p99 dashboards, and
//! - **JSON** — offline analysis / later Perfetto conversion.
//!
//! Output is local/text only — NOT MCAP, NOT a Zenoh queryable, and entirely
//! independent of telemetry (this module does not import `crate::telemetry`).

use crate::trace::histogram::bucket_upper_bound;
use crate::trace::{all_stage_snapshots, all_user_snapshots, scope_name, HistogramSnapshot};
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::io;
use std::path::Path;

/// One cumulative histogram bucket (`le` = inclusive upper bound, ns).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketExport {
    pub le_ns: u64,
    pub cumulative_count: u64,
}

/// Export view of one metric's histogram.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricExport {
    pub metric: String,
    /// Event type ("span" | "instant" | "counter").
    pub kind: String,
    pub count: u64,
    pub sum_ns: u64,
    pub max_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub buckets: Vec<BucketExport>,
}

impl MetricExport {
    fn from_snapshot(metric: String, kind: &str, snap: &HistogramSnapshot) -> Self {
        let mut buckets = Vec::new();
        let mut cumulative = 0u64;
        for (k, &c) in snap.buckets.iter().enumerate() {
            // Skip leading all-empty buckets for compactness; emit once any data
            // has appeared so the cumulative series is meaningful.
            if c == 0 && cumulative == 0 {
                continue;
            }
            cumulative += c;
            buckets.push(BucketExport {
                le_ns: bucket_upper_bound(k),
                cumulative_count: cumulative,
            });
        }
        Self {
            metric,
            kind: kind.to_string(),
            count: snap.count,
            sum_ns: snap.sum_ns,
            max_ns: snap.max_ns,
            p50_ns: snap.p50_ns(),
            p95_ns: snap.p95_ns(),
            p99_ns: snap.p99_ns(),
            buckets,
        }
    }
}

/// Collect every tracing histogram (internal stages + user metrics) as export
/// records. Internal stages use their dotted name; user metrics use
/// `"{scope}/{name}"`. Tracing-owned; never reads telemetry.
pub fn collect() -> Vec<MetricExport> {
    let mut out = Vec::new();
    for (name, snap) in all_stage_snapshots() {
        // Internal service/action stages are duration spans.
        out.push(MetricExport::from_snapshot(name.to_string(), "span", &snap));
    }
    for (scope, name, event_type, snap) in all_user_snapshots() {
        let scope_label = scope_name(scope).unwrap_or_else(|| format!("scope{scope}"));
        out.push(MetricExport::from_snapshot(
            format!("{scope_label}/{name}"),
            event_type.as_str(),
            &snap,
        ));
    }
    out
}

/// Pretty JSON array of all metrics (offline analysis / conversion).
pub fn json() -> String {
    serde_json::to_string_pretty(&collect()).unwrap_or_else(|_| "[]".to_string())
}

/// Escape a Prometheus label value (`\` -> `\\`, `"` -> `\"`, newline -> `\n`).
/// Scope/metric names are arbitrary user strings, so this prevents invalid
/// exposition / label injection.
fn escape_label(s: &str) -> std::borrow::Cow<'_, str> {
    if s.bytes().any(|b| b == b'\\' || b == b'"' || b == b'\n') {
        let mut out = String::with_capacity(s.len() + 8);
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                other => out.push(other),
            }
        }
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// Prometheus text exposition. One histogram per metric (cumulative `le`
/// buckets, `+Inf`, `_count`, `_sum`) plus p50/p95/p99 gauges for direct
/// dashboards. Label values are escaped.
pub fn prometheus_text() -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "# HELP aorta_trace_latency_ns Aorta trace stage/user latency (nanoseconds)."
    );
    let _ = writeln!(s, "# TYPE aorta_trace_latency_ns histogram");
    for m in collect() {
        let metric = escape_label(&m.metric);
        let kind = &m.kind; // fixed enum string (span|instant|counter), no escaping needed
        for b in &m.buckets {
            let _ = writeln!(
                s,
                "aorta_trace_latency_ns_bucket{{metric=\"{metric}\",kind=\"{kind}\",le=\"{}\"}} {}",
                b.le_ns, b.cumulative_count
            );
        }
        let _ = writeln!(
            s,
            "aorta_trace_latency_ns_bucket{{metric=\"{metric}\",kind=\"{kind}\",le=\"+Inf\"}} {}",
            m.count
        );
        let _ = writeln!(
            s,
            "aorta_trace_latency_ns_count{{metric=\"{metric}\",kind=\"{kind}\"}} {}",
            m.count
        );
        let _ = writeln!(
            s,
            "aorta_trace_latency_ns_sum{{metric=\"{metric}\",kind=\"{kind}\"}} {}",
            m.sum_ns
        );
        for (suffix, value) in [("p50", m.p50_ns), ("p95", m.p95_ns), ("p99", m.p99_ns)] {
            let _ = writeln!(
                s,
                "aorta_trace_latency_{suffix}_ns{{metric=\"{metric}\",kind=\"{kind}\"}} {value}"
            );
        }
    }
    s
}

/// Write the Prometheus exposition to a local file — the "file" exporter
/// (local only; NOT MCAP, NOT a Zenoh queryable). Callers own rotation/retention.
pub fn write_prometheus_file(path: impl AsRef<Path>) -> io::Result<()> {
    std::fs::write(path, prometheus_text())
}
