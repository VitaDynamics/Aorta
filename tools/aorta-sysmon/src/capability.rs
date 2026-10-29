//! Runtime capability probe.
//!
//! Decides whether each metric family is `Active`, `Unavailable`, or
//! `Degraded` based on what files actually parse on this host. The probe
//! covers procfs/PSI/process families; the `perf` row is a placeholder
//! that `PerfState::render_capabilities` replaces, and `zenoh_topology` is
//! reported only when configured `On` (the default is `Off`).

use std::fs;

use serde::{Deserialize, Serialize};

use crate::config::{Config, FamilyMode};
use crate::procfs_root::ProcfsRoot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatus {
    Active,
    Unavailable,
    Degraded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub family: String,
    pub configured_mode: FamilyMode,
    pub runtime_status: RuntimeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded_reason: Option<String>,
    // Per-counter perf-event fields. Empty / zero for non-perf
    // rows; populated by `PerfState::render_capabilities` for the
    // five perf counters. Kept inline on `Capability` (vs in a
    // separate table) because the existing wire schema already has
    // these slots on `MetricCapability` and the producer-side struct
    // mirrors the wire shape one-to-one.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub perf_event_name: String,
    #[serde(skip_serializing_if = "is_zero_u64", default)]
    pub perf_raw_count: u64,
    #[serde(skip_serializing_if = "is_zero_u64", default)]
    pub perf_time_enabled_ns: u64,
    #[serde(skip_serializing_if = "is_zero_u64", default)]
    pub perf_time_running_ns: u64,
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

impl Capability {
    fn active(family: &str, mode: FamilyMode) -> Self {
        Self {
            family: family.into(),
            configured_mode: mode,
            runtime_status: RuntimeStatus::Active,
            degraded_reason: None,
            perf_event_name: String::new(),
            perf_raw_count: 0,
            perf_time_enabled_ns: 0,
            perf_time_running_ns: 0,
        }
    }

    fn unavailable(family: &str, mode: FamilyMode, reason: impl Into<String>) -> Self {
        Self {
            family: family.into(),
            configured_mode: mode,
            runtime_status: RuntimeStatus::Unavailable,
            degraded_reason: Some(reason.into()),
            perf_event_name: String::new(),
            perf_raw_count: 0,
            perf_time_enabled_ns: 0,
            perf_time_running_ns: 0,
        }
    }

    fn off(family: &str) -> Self {
        Self {
            family: family.into(),
            configured_mode: FamilyMode::Off,
            runtime_status: RuntimeStatus::Unavailable,
            degraded_reason: Some("configured off".into()),
            perf_event_name: String::new(),
            perf_raw_count: 0,
            perf_time_enabled_ns: 0,
            perf_time_running_ns: 0,
        }
    }

    /// One row per perf counter. `family` is always
    /// `"perf"`, `perf_event_name` carries the wire name from
    /// `COUNTER_NAMES`, and the three numeric slots are populated
    /// from a successful `read_count_and_time` (or left at 0 when
    /// the read failed).
    pub fn with_perf_fields(
        family: &str,
        mode: FamilyMode,
        status: RuntimeStatus,
        reason: Option<String>,
        perf_event_name: &str,
        reading: Option<crate::collector::perf::CounterReading>,
    ) -> Self {
        Self {
            family: family.into(),
            configured_mode: mode,
            runtime_status: status,
            degraded_reason: reason,
            perf_event_name: perf_event_name.into(),
            perf_raw_count: reading.map(|r| r.count).unwrap_or(0),
            perf_time_enabled_ns: reading.map(|r| r.time_enabled_ns).unwrap_or(0),
            perf_time_running_ns: reading.map(|r| r.time_running_ns).unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelKnobs {
    pub kernel: String,
    pub preempt_model: String,
    pub cgroup_version: u8,
    pub online_cpus: u32,
}

impl KernelKnobs {
    pub fn probe(root: &ProcfsRoot) -> Self {
        // `/proc/version` looks like
        //   "Linux version 6.1.83-DR-PL5.2_V1.1.0 (root@host) (gcc ...) #1 SMP PREEMPT Thu Apr 9 ..."
        // The PREEMPT marker can appear *after* the first few tokens, so
        // run preempt detection against the full string before truncating
        // the display field down to the first three tokens.
        let full_version =
            fs::read_to_string(root.proc_path("version")).unwrap_or_else(|_| String::new());
        // When `/proc/version` is unreadable / empty (very locked-down
        // containers, missing procfs mount), reporting `preempt_model =
        // "voluntary"` would mis-classify "unknown" as a real model.
        // Mirror the `kernel` field's `"unknown"` fallback so the two
        // stay in sync.
        let (kernel, preempt_model) = if full_version.is_empty() {
            ("unknown".to_string(), "unknown".to_string())
        } else {
            let kernel = full_version
                .split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" ");
            (kernel, detect_preempt_model(&full_version))
        };

        let cgroup_version = detect_cgroup_version(root);
        let online_cpus = detect_online_cpus(root);

        Self {
            kernel,
            preempt_model,
            cgroup_version,
            online_cpus,
        }
    }
}

fn detect_preempt_model(version: &str) -> String {
    if version.contains("PREEMPT_RT") {
        "preempt_rt".into()
    } else if version.contains("PREEMPT_DYNAMIC") {
        "preempt_dynamic".into()
    } else if version.contains("PREEMPT") {
        "preempt".into()
    } else {
        "voluntary".into()
    }
}

fn detect_cgroup_version(root: &ProcfsRoot) -> u8 {
    // cgroup v2 unified hierarchy mounts `cgroup.controllers` at the
    // root; v1 does not. If both exist we report 2 ("hybrid mode favors
    // v2 for the metrics we care about").
    let v2 = root.sys_path("fs/cgroup/cgroup.controllers").exists();
    if v2 {
        2
    } else {
        1
    }
}

fn detect_online_cpus(root: &ProcfsRoot) -> u32 {
    let stat = match fs::read_to_string(root.proc_path("stat")) {
        Ok(s) => s,
        Err(_) => return 1,
    };
    let count = stat
        .lines()
        .filter(|line| {
            // Lines like `cpu0 ...`, `cpu1 ...` — exclude the aggregate `cpu ` line.
            line.starts_with("cpu") && line.as_bytes().get(3).is_some_and(|b| b.is_ascii_digit())
        })
        .count() as u32;
    // `cpu_percent_host` divides by `online_cpus`. Clamp to ≥1 so a
    // malformed /proc/stat with no `cpuN` rows cannot produce NaN/Inf
    // downstream.
    count.max(1)
}

/// Run a one-shot capability probe.
pub fn probe(cfg: &Config, root: &ProcfsRoot) -> Vec<Capability> {
    let mut caps = Vec::with_capacity(10);

    caps.push(probe_file_family("cpu", cfg.metrics.cpu, root, "stat"));
    caps.push(probe_file_family(
        "memory",
        cfg.metrics.memory,
        root,
        "meminfo",
    ));
    caps.push(probe_file_family(
        "disk",
        cfg.metrics.disk,
        root,
        "diskstats",
    ));
    caps.push(probe_file_family(
        "network",
        cfg.metrics.network,
        root,
        "net/dev",
    ));

    // PSI: one row per resource.
    for resource in ["cpu", "memory", "io"] {
        caps.push(probe_psi(resource, cfg.metrics.psi, root));
    }

    // Process — always probe self/stat.
    caps.push(probe_file_family(
        "process",
        cfg.metrics.process,
        root,
        "self/stat",
    ));

    // perf — placeholder row replaced by `PerfState::render_capabilities`
    // in the daemon (`publisher::run_daemon`) and one-shot snapshot
    // (`run_snapshot` in main.rs) paths. If neither of those splices it
    // out, the family is "configured but no counters opened on this
    // tick" — surface that as Unavailable rather than silently dropping
    // the row.
    caps.push(Capability::unavailable(
        "perf",
        cfg.metrics.perf,
        "perf state not initialized for this caller",
    ));

    // zenoh_topology: Off until a topology collector is implemented.
    let zenoh = match cfg.metrics.zenoh_topology {
        FamilyMode::Off => Capability::off("zenoh_topology"),
        _ => Capability::unavailable(
            "zenoh_topology",
            cfg.metrics.zenoh_topology,
            "not implemented",
        ),
    };
    caps.push(zenoh);

    caps
}

fn probe_file_family(family: &str, mode: FamilyMode, root: &ProcfsRoot, rel: &str) -> Capability {
    if mode == FamilyMode::Off {
        return Capability::off(family);
    }
    let path = root.proc_path(rel);
    match fs::metadata(&path) {
        Ok(_) => Capability::active(family, mode),
        Err(e) => Capability::unavailable(family, mode, format!("{}: {}", path.display(), e)),
    }
}

fn probe_psi(resource: &str, mode: FamilyMode, root: &ProcfsRoot) -> Capability {
    let family = format!("psi_{}", resource);
    if mode == FamilyMode::Off {
        return Capability::off(&family);
    }
    let path = root.proc_path(format!("pressure/{}", resource));
    match fs::read_to_string(&path) {
        Ok(body) if body.contains("some") || body.contains("full") => {
            Capability::active(&family, mode)
        }
        Ok(_) => Capability::unavailable(&family, mode, "unrecognized PSI format"),
        Err(_) => Capability::unavailable(&family, mode, "/proc/pressure/* missing"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preempt_rt_classifies_correctly() {
        assert_eq!(
            detect_preempt_model("Linux 6.1.112 PREEMPT_RT"),
            "preempt_rt"
        );
        assert_eq!(detect_preempt_model("Linux 6.1.83 PREEMPT"), "preempt");
        assert_eq!(detect_preempt_model("Linux 5.15 voluntary"), "voluntary");
    }

    #[test]
    fn preempt_marker_is_found_in_full_version_line() {
        // Real /proc/version content from a production target: the PREEMPT
        // marker appears AFTER the first three whitespace-separated
        // tokens, so the probe must scan the full line, not just the
        // truncated display field.
        let full = "Linux version 6.1.83-DR-PL5.2_V1.1.0 (root@host) (gcc 11.4.0) #1 SMP PREEMPT Thu Apr 9 10:11:06 UTC 2026";
        assert_eq!(detect_preempt_model(full), "preempt");
    }

    #[test]
    fn missing_proc_version_reports_unknown_not_voluntary() {
        // Fixture without `proc/version` — KernelKnobs::probe must
        // surface "unknown" for both `kernel` and `preempt_model` so
        // an unreadable /proc/version is not misclassified as a real
        // voluntary-preempt kernel.
        let tmp = std::env::temp_dir().join("aorta_sysmon_test_missing_version");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(tmp.join("proc")).unwrap();
        // /proc/stat must exist so `online_cpus` doesn't bail.
        std::fs::write(tmp.join("proc/stat"), "cpu  0 0 0 0\ncpu0 0 0 0 0\n").unwrap();
        let root = ProcfsRoot::fixture(&tmp);
        let knobs = KernelKnobs::probe(&root);
        assert_eq!(knobs.kernel, "unknown");
        assert_eq!(knobs.preempt_model, "unknown");
    }

    #[test]
    fn online_cpus_from_fixture() {
        // Inline fixture: cpu/cpu0/cpu1/cpu2 lines.
        let tmp = std::env::temp_dir().join("aorta_sysmon_test_cpus");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(tmp.join("proc")).unwrap();
        std::fs::write(
            tmp.join("proc/stat"),
            "cpu  0 0 0 0\ncpu0 0 0 0 0\ncpu1 0 0 0 0\ncpu2 0 0 0 0\nctxt 0\n",
        )
        .unwrap();
        let root = ProcfsRoot::fixture(&tmp);
        assert_eq!(detect_online_cpus(&root), 3);
    }
}
