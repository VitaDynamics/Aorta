//! Tests for histogram export (Step 9) + the telemetry/trace separation guard.

use crate::trace::export::{collect, json, prometheus_text, MetricExport};
use crate::trace::{
    intern_scope, record_user, set_scope_enabled, set_trace_enabled, EventType, LogLevel,
};
use std::fs;
use std::path::{Path, PathBuf};

fn guard<T>(f: impl FnOnce() -> T) -> T {
    crate::trace_test_support::guarded(f)
}

#[test]
fn export_collects_and_json_roundtrips_a_user_metric() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("test.export.scope");
        set_scope_enabled(h, true);
        // Record a known distribution.
        for _ in 0..10 {
            record_user(h, "flush", EventType::Span, LogLevel::Info, 1000);
        }
        let metric_name = "test.export.scope/flush";

        // collect() must include the metric with the right count.
        let found = collect()
            .into_iter()
            .find(|m| m.metric == metric_name)
            .expect("collected metrics must include the recorded user metric");
        assert!(found.count >= 10);
        assert_eq!(
            found.p50_ns, 1023,
            "10x 1000ns -> p50 in bucket 10 (ub 1023)"
        );
        assert!(!found.buckets.is_empty());

        // JSON must parse back and contain the metric.
        let parsed: Vec<MetricExport> =
            serde_json::from_str(&json()).expect("export json must be valid");
        let pj = parsed
            .into_iter()
            .find(|m| m.metric == metric_name)
            .expect("json export must include the metric");
        assert_eq!(pj.count, found.count);
        assert_eq!(pj.p99_ns, found.p99_ns);
    });
}

#[test]
fn prometheus_text_is_well_formed() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("test.export.prom");
        set_scope_enabled(h, true);
        record_user(h, "io", EventType::Span, LogLevel::Info, 2000);

        let text = prometheus_text();
        assert!(text.contains("# TYPE aorta_trace_latency_ns histogram"));
        assert!(text.contains(
            "aorta_trace_latency_ns_count{metric=\"test.export.prom/io\",kind=\"span\"}"
        ));
        assert!(text
            .contains("aorta_trace_latency_p99_ns{metric=\"test.export.prom/io\",kind=\"span\"}"));
        assert!(text.contains("le=\"+Inf\""));
        // Every non-comment, non-empty line must look like a sample (have a value).
        for line in text.lines() {
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            assert!(
                line.rsplit(' ')
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some(),
                "malformed prometheus line: {line}"
            );
        }
    });
}

#[test]
fn write_prometheus_file_roundtrips() {
    guard(|| {
        set_trace_enabled(true);
        let h = intern_scope("test.export.file");
        set_scope_enabled(h, true);
        record_user(h, "step", EventType::Span, LogLevel::Info, 3000);

        let mut path = std::env::temp_dir();
        path.push(format!("aorta_trace_export_{}.prom", std::process::id()));
        crate::trace::export::write_prometheus_file(&path).expect("write export file");
        let contents = fs::read_to_string(&path).expect("read export file");
        assert!(contents.contains(
            "aorta_trace_latency_ns_count{metric=\"test.export.file/step\",kind=\"span\"}"
        ));
        let _ = fs::remove_file(&path);
    });
}

// ----------------------------------------------------------------------------
// Telemetry <-> trace separation guard (hard architectural invariant): the
// trace module must NEVER import telemetry, and telemetry must never import
// trace. A future `use crate::telemetry::...` in trace code (or vice-versa)
// fails this test.
// ----------------------------------------------------------------------------

fn rs_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(rs_files(&path));
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
    out
}

/// True if a non-comment line of `src` contains `needle`.
fn references_outside_comments(src: &str, needle: &str) -> bool {
    src.lines().any(|line| {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") {
            return false; // line/doc comment
        }
        line.contains(needle)
    })
}

#[test]
fn trace_module_does_not_import_telemetry_and_vice_versa() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));

    let trace_dir = root.join("src/trace");
    for f in rs_files(&trace_dir) {
        let src = fs::read_to_string(&f).expect("read trace source");
        assert!(
            !references_outside_comments(&src, "crate::telemetry"),
            "{} references crate::telemetry — trace must be independent of telemetry",
            f.display()
        );
    }

    let telemetry_dir = root.join("src/telemetry");
    for f in rs_files(&telemetry_dir) {
        let src = fs::read_to_string(&f).expect("read telemetry source");
        assert!(
            !references_outside_comments(&src, "crate::trace"),
            "{} references crate::trace — telemetry must be independent of trace",
            f.display()
        );
    }
}
