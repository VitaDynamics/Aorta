//! Process top-K evaluator.
//!
//! For each `[[process_threshold]]` row, sort all process detail rows
//! by the named metric descending, take the top K, and emit an alert
//! for every process whose metric value is `>= threshold`.
//!
//! Supported metric names match the corresponding fields of
//! `ProcessStat`:
//!   - `cpu_percent_host`
//!   - `rss_bytes`
//!   - `fd_count`
//!
//! Unknown metric names are skipped silently (matches the "unknown
//! family in family-mode config" pattern used by the snapshot path).

use crate::collector::process::ProcessStat;
use crate::config::{Config, ProcessThreshold};
use crate::snapshot::Snapshot;

use super::{Alert, AlertPayload, AlertSeverity, ProcessTopAlert};

pub fn evaluate(
    snap: &Snapshot,
    cfg: &Config,
    observed_at_ns: u64,
    monotonic_ns: u64,
) -> Vec<Alert> {
    let mut out = Vec::new();
    for rule in &cfg.process_thresholds {
        out.extend(check(rule, snap, observed_at_ns, monotonic_ns));
    }
    out
}

fn check(
    rule: &ProcessThreshold,
    snap: &Snapshot,
    observed_at_ns: u64,
    monotonic_ns: u64,
) -> Vec<Alert> {
    let extractor = match metric_extractor(&rule.metric) {
        Some(f) => f,
        None => return Vec::new(),
    };

    let mut ranked: Vec<(usize, f64, &ProcessStat)> = snap
        .processes
        .iter()
        .enumerate()
        .map(|(i, p)| (i, extractor(p), p))
        .collect();

    // Sort by metric DESC with deterministic tie-break on pid (so a tied
    // group is sliced the same way on every tick — matches the
    // determinism guarantee we added to the snapshot top-N sort).
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.2.pid.cmp(&b.2.pid))
    });

    let k = rule.top_k.max(1) as usize;
    let severity = AlertSeverity::parse(&rule.severity);
    let mut alerts = Vec::new();
    for (rank, (_, value, proc)) in ranked.iter().take(k).enumerate() {
        if *value < rule.threshold {
            // Sorted DESC — once we dip below threshold no later row
            // can exceed it either, so break early.
            break;
        }
        alerts.push(Alert {
            severity,
            observed_at_ns,
            monotonic_ns,
            payload: AlertPayload::ProcessTop(ProcessTopAlert {
                pid: proc.pid,
                comm: proc.comm.clone(),
                metric: rule.metric.clone(),
                value: *value,
                threshold: rule.threshold,
                top_k: (rank as u32) + 1,
            }),
        });
    }
    alerts
}

fn metric_extractor(name: &str) -> Option<fn(&ProcessStat) -> f64> {
    match name {
        "cpu_percent_host" => Some(|p| p.cpu_percent_host as f64),
        "rss_bytes" => Some(|p| p.rss_bytes as f64),
        "fd_count" => Some(|p| {
            // Unknown / EACCES is reported as -1 and must NOT be
            // promoted into a "high fd_count" alert.
            if p.fd_count < 0 {
                f64::NEG_INFINITY
            } else {
                p.fd_count as f64
            }
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::process::ProcessStat;
    use crate::config::{Config, ProcessThreshold};

    fn snap_with(processes: Vec<ProcessStat>) -> Snapshot {
        Snapshot {
            host_name: "h".into(),
            host_id: "h".into(),
            host_name_key: "h".into(),
            host_id_key: "h".into(),
            kernel: String::new(),
            preempt_model: String::new(),
            cgroup_version: 0,
            online_cpus: 1,
            interval_ms: 1000,
            sequence: 0,
            capabilities: Vec::new(),
            cpu: Default::default(),
            memory: Default::default(),
            disks: Vec::new(),
            net_ifs: Vec::new(),
            psi: Vec::new(),
            processes,
        }
    }

    fn proc(pid: i32, comm: &str, cpu_pct: f32, rss: u64, fds: i64) -> ProcessStat {
        ProcessStat {
            pid,
            comm: comm.into(),
            cpu_percent_host: cpu_pct,
            rss_bytes: rss,
            fd_count: fds,
            ..Default::default()
        }
    }

    fn rule(metric: &str, threshold: f64, top_k: u32) -> ProcessThreshold {
        ProcessThreshold {
            metric: metric.into(),
            threshold,
            top_k,
            severity: "warn".into(),
        }
    }

    #[test]
    fn fires_for_top_k_exceeding_threshold() {
        let snap = snap_with(vec![
            proc(1, "low", 5.0, 100, 10),
            proc(2, "mid", 50.0, 1000, 50),
            proc(3, "hi", 95.0, 8000, 200),
        ]);
        let cfg = Config {
            process_thresholds: vec![rule("cpu_percent_host", 80.0, 2)],
            ..Config::default()
        };
        let alerts = evaluate(&snap, &cfg, 0, 0);
        assert_eq!(alerts.len(), 1);
        match &alerts[0].payload {
            AlertPayload::ProcessTop(p) => {
                assert_eq!(p.pid, 3);
                assert_eq!(p.metric, "cpu_percent_host");
                assert!((p.value - 95.0).abs() < 1e-6);
                assert_eq!(p.top_k, 1);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn unknown_fd_count_is_not_flagged() {
        let snap = snap_with(vec![
            proc(1, "denied", 0.0, 0, -1), // EACCES
            proc(2, "open", 0.0, 0, 5),
        ]);
        let cfg = Config {
            process_thresholds: vec![rule("fd_count", 100.0, 2)],
            ..Config::default()
        };
        assert!(evaluate(&snap, &cfg, 0, 0).is_empty());
    }

    #[test]
    fn ties_break_on_pid_for_determinism() {
        let snap = snap_with(vec![
            proc(7, "a", 50.0, 0, 0),
            proc(3, "b", 50.0, 0, 0),
            proc(11, "c", 50.0, 0, 0),
        ]);
        let cfg = Config {
            process_thresholds: vec![rule("cpu_percent_host", 50.0, 2)],
            ..Config::default()
        };
        let alerts = evaluate(&snap, &cfg, 0, 0);
        // top_k=2 picks the two with the LOWEST pid (3, 7).
        assert_eq!(alerts.len(), 2);
        let pids: Vec<i32> = alerts
            .iter()
            .map(|a| match &a.payload {
                AlertPayload::ProcessTop(p) => p.pid,
                _ => -1,
            })
            .collect();
        assert_eq!(pids, vec![3, 7]);
    }

    #[test]
    fn unknown_metric_silent() {
        let snap = snap_with(vec![proc(1, "x", 99.0, 0, 0)]);
        let cfg = Config {
            process_thresholds: vec![rule("not_a_metric", 0.0, 1)],
            ..Config::default()
        };
        assert!(evaluate(&snap, &cfg, 0, 0).is_empty());
    }
}
