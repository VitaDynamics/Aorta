//! PSI threshold evaluator.
//!
//! For each `[[psi_threshold]]` row in config, find a matching
//! `PsiLine` in the snapshot and compare its `stall_ratio` against
//! the threshold. When PSI is unavailable on this host the snapshot
//! `psi` vector is empty and no alert fires — that's the "silently
//! disabled" path the design calls for. The capability layer already
//! exposes "PSI unavailable" via `MetricCapability.runtime_status`,
//! so a downstream consumer can still detect the suppression.

use crate::config::{Config, PsiThreshold};
use crate::snapshot::Snapshot;

use super::{Alert, AlertPayload, AlertSeverity, PsiAlert};

pub fn evaluate(
    snap: &Snapshot,
    cfg: &Config,
    observed_at_ns: u64,
    monotonic_ns: u64,
) -> Vec<Alert> {
    let mut out = Vec::new();
    for rule in &cfg.psi_thresholds {
        if let Some(alert) = check(rule, snap, observed_at_ns, monotonic_ns) {
            out.push(alert);
        }
    }
    out
}

fn check(
    rule: &PsiThreshold,
    snap: &Snapshot,
    observed_at_ns: u64,
    monotonic_ns: u64,
) -> Option<Alert> {
    // Find the matching `(resource, line)` PsiLine — names are matched
    // exactly so a typo in config does NOT silently fire on a different
    // line. Case-insensitive compare is intentional: kernel emits
    // lowercase, config example uses lowercase, but operators may
    // capitalize.
    let line = snap.psi.iter().find(|p| {
        p.resource.eq_ignore_ascii_case(&rule.resource) && p.line.eq_ignore_ascii_case(&rule.line)
    })?;
    if line.stall_ratio < rule.stall_ratio {
        return None;
    }
    Some(Alert {
        severity: AlertSeverity::Warn,
        observed_at_ns,
        monotonic_ns,
        payload: AlertPayload::Psi(PsiAlert {
            resource: line.resource.clone(),
            line: line.line.clone(),
            stall_ratio: line.stall_ratio,
            threshold: rule.stall_ratio,
            window_ms: rule.window_ms,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::psi::PsiLine;
    use crate::config::{Config, PsiThreshold};

    fn snap_with_psi(lines: Vec<PsiLine>) -> Snapshot {
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
            psi: lines,
            processes: Vec::new(),
        }
    }

    fn rule(resource: &str, line: &str, ratio: f32) -> PsiThreshold {
        PsiThreshold {
            resource: resource.into(),
            line: line.into(),
            stall_ratio: ratio,
            window_ms: 1000,
        }
    }

    #[test]
    fn fires_when_ratio_meets_threshold() {
        let snap = snap_with_psi(vec![PsiLine {
            resource: "io".into(),
            line: "full".into(),
            stall_ratio: 0.25,
            ..Default::default()
        }]);
        let cfg = Config {
            psi_thresholds: vec![rule("io", "full", 0.10)],
            ..Config::default()
        };
        let alerts = evaluate(&snap, &cfg, 0, 0);
        assert_eq!(alerts.len(), 1);
        match &alerts[0].payload {
            AlertPayload::Psi(p) => {
                assert_eq!(p.resource, "io");
                assert!((p.stall_ratio - 0.25).abs() < 1e-6);
                assert!((p.threshold - 0.10).abs() < 1e-6);
            }
            other => panic!("unexpected payload: {:?}", other),
        }
    }

    #[test]
    fn silent_when_psi_unavailable() {
        let snap = snap_with_psi(Vec::new());
        let cfg = Config {
            psi_thresholds: vec![rule("io", "full", 0.10)],
            ..Config::default()
        };
        assert!(evaluate(&snap, &cfg, 0, 0).is_empty());
    }

    #[test]
    fn silent_when_below_threshold() {
        let snap = snap_with_psi(vec![PsiLine {
            resource: "cpu".into(),
            line: "some".into(),
            stall_ratio: 0.05,
            ..Default::default()
        }]);
        let cfg = Config {
            psi_thresholds: vec![rule("cpu", "some", 0.10)],
            ..Config::default()
        };
        assert!(evaluate(&snap, &cfg, 0, 0).is_empty());
    }

    #[test]
    fn unrelated_line_does_not_fire() {
        let snap = snap_with_psi(vec![PsiLine {
            resource: "cpu".into(),
            line: "some".into(),
            stall_ratio: 0.99,
            ..Default::default()
        }]);
        let cfg = Config {
            psi_thresholds: vec![rule("io", "full", 0.10)],
            ..Config::default()
        };
        assert!(evaluate(&snap, &cfg, 0, 0).is_empty());
    }
}
