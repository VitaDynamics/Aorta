//! perf-event2 collector.
//!
//! Opens a small set of per-self counters at daemon start-up and reads
//! them on every tick. Each counter's open status, raw count, and
//! `time_enabled` / `time_running` are surfaced through the snapshot's
//! `capabilities` vector as one `MetricCapability` row per counter
//! (family = `"perf"`, `perf_event_name = "context-switches"` etc.).
//!
//! Linux-only — the module body is gated behind
//! `cfg(target_os = "linux")` and the [`PerfState`] type is `pub`
//! everywhere so callers can keep their cfg blocks tidy. On non-Linux
//! builds [`PerfState::try_open`] returns an `Unavailable` state and
//! produces zero counter rows.

use std::fs;

use serde::{Deserialize, Serialize};

use crate::capability::{Capability, RuntimeStatus};
use crate::config::FamilyMode;
use crate::procfs_root::ProcfsRoot;

/// The fixed counter set this collector samples.
///
/// The string IDs are the wire-format names downstream consumers see
/// in `MetricCapability.perf_event_name`. They mirror the canonical
/// `perf` tool names so existing dashboards interpret them without a
/// remapping table.
const COUNTER_NAMES: &[&str] = &[
    "context-switches",
    "page-faults",
    "cpu-migrations",
    "cache-references",
    "cache-misses",
];

/// Kernel knob snapshot used to interpret degraded modes.
///
/// Carried separately from per-counter rows so a consumer can still see
/// the kernel posture even when every counter failed to open
/// (e.g. paranoid≥3 on a non-root process).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerfKnobs {
    pub paranoid: Option<i32>,
    pub mlock_kb: Option<i64>,
    pub max_sample_rate: Option<i64>,
}

impl PerfKnobs {
    pub fn probe(root: &ProcfsRoot) -> Self {
        fn read<T: std::str::FromStr>(root: &ProcfsRoot, rel: &str) -> Option<T> {
            fs::read_to_string(root.proc_path(rel))
                .ok()
                .and_then(|s| s.trim().parse::<T>().ok())
        }
        Self {
            paranoid: read::<i32>(root, "sys/kernel/perf_event_paranoid"),
            mlock_kb: read::<i64>(root, "sys/kernel/perf_event_mlock_kb"),
            max_sample_rate: read::<i64>(root, "sys/kernel/perf_event_max_sample_rate"),
        }
    }

    /// Render the knob set as a single key=value string suitable for
    /// stuffing into a `degraded_reason` slot. Returns `""` if no
    /// knobs were readable (very old kernels / odd containers).
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if let Some(v) = self.paranoid {
            parts.push(format!("paranoid={v}"));
        }
        if let Some(v) = self.mlock_kb {
            parts.push(format!("mlock_kb={v}"));
        }
        if let Some(v) = self.max_sample_rate {
            parts.push(format!("max_sample_rate={v}"));
        }
        parts.join(" ")
    }
}

/// One counter's cumulative state after a read.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CounterReading {
    pub count: u64,
    pub time_enabled_ns: u64,
    pub time_running_ns: u64,
}

/// Per-counter status; mirrors `RuntimeStatus` but carries the wire-
/// level numbers + reason inline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterStatus {
    pub name: &'static str,
    pub status: RuntimeStatus,
    pub reason: Option<String>,
    pub reading: Option<CounterReading>,
}

#[cfg(target_os = "linux")]
mod imp {
    use super::{CounterReading, CounterStatus, COUNTER_NAMES};
    use crate::capability::RuntimeStatus;
    use perf_event::events::{Hardware, Software};
    use perf_event::{Builder, Counter};

    /// One open counter slot. `Ok(c)` means the counter opened cleanly
    /// at start-up; `Err(reason)` records the open-time error so every
    /// subsequent tick can surface a stable `Unavailable` row instead
    /// of silently dropping the counter.
    pub struct Slot {
        pub name: &'static str,
        pub counter: Result<Counter, String>,
    }

    pub struct LinuxPerfState {
        slots: Vec<Slot>,
    }

    impl LinuxPerfState {
        pub fn try_open() -> Self {
            let mut slots = Vec::with_capacity(COUNTER_NAMES.len());
            for &name in COUNTER_NAMES {
                let counter = match build_one(name) {
                    Ok(mut c) => {
                        // Best-effort enable — perf-event2 leaves
                        // counters disabled at build time.
                        let _ = c.enable();
                        Ok(c)
                    }
                    Err(e) => Err(e.to_string()),
                };
                slots.push(Slot { name, counter });
            }
            Self { slots }
        }

        pub fn read_all(&mut self) -> Vec<CounterStatus> {
            let mut out = Vec::with_capacity(self.slots.len());
            for slot in &mut self.slots {
                match &mut slot.counter {
                    Ok(counter) => match counter.read_count_and_time() {
                        Ok(c) => {
                            // `time_running == 0` means the kernel
                            // never actually scheduled the counter
                            // since the last read; surface as Degraded
                            // so downstream consumers can decide.
                            let status = if c.time_running == 0 {
                                RuntimeStatus::Degraded
                            } else {
                                RuntimeStatus::Active
                            };
                            let reason = if c.time_running == 0 {
                                Some("no runtime".to_string())
                            } else if c.time_running < c.time_enabled {
                                Some(format!(
                                    "kernel-multiplexed (enabled={} running={})",
                                    c.time_enabled, c.time_running
                                ))
                            } else {
                                None
                            };
                            out.push(CounterStatus {
                                name: slot.name,
                                status,
                                reason,
                                reading: Some(CounterReading {
                                    count: c.count,
                                    time_enabled_ns: c.time_enabled,
                                    time_running_ns: c.time_running,
                                }),
                            });
                        }
                        Err(e) => {
                            out.push(CounterStatus {
                                name: slot.name,
                                status: RuntimeStatus::Degraded,
                                reason: Some(format!("read failed: {e}")),
                                reading: None,
                            });
                        }
                    },
                    Err(open_err) => {
                        out.push(CounterStatus {
                            name: slot.name,
                            status: RuntimeStatus::Unavailable,
                            reason: Some(open_err.clone()),
                            reading: None,
                        });
                    }
                }
            }
            out
        }
    }

    fn build_one(name: &'static str) -> std::io::Result<Counter> {
        // Map the wire-format names to perf-event2 enum variants.
        // Unknown names are a programmer error (the list is fixed by
        // `COUNTER_NAMES`), so we panic via `unreachable!()` rather
        // than silently dropping the counter.
        match name {
            "context-switches" => Builder::new(Software::CONTEXT_SWITCHES)
                .observe_self()
                .build(),
            "page-faults" => Builder::new(Software::PAGE_FAULTS).observe_self().build(),
            "cpu-migrations" => Builder::new(Software::CPU_MIGRATIONS)
                .observe_self()
                .build(),
            "cache-references" => Builder::new(Hardware::CACHE_REFERENCES)
                .observe_self()
                .build(),
            "cache-misses" => Builder::new(Hardware::CACHE_MISSES).observe_self().build(),
            _ => unreachable!("unknown perf counter name: {name}"),
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{CounterStatus, COUNTER_NAMES};
    use crate::capability::RuntimeStatus;

    /// Non-Linux stub: every counter is reported as Unavailable with a
    /// stable reason. Keeps the snapshot shape consistent across
    /// platforms so dashboards don't need to special-case macOS dev
    /// builds.
    pub struct LinuxPerfState;

    impl LinuxPerfState {
        pub fn try_open() -> Self {
            Self
        }

        pub fn read_all(&mut self) -> Vec<CounterStatus> {
            COUNTER_NAMES
                .iter()
                .map(|&n| CounterStatus {
                    name: n,
                    status: RuntimeStatus::Unavailable,
                    reason: Some("perf-event2 unavailable on this OS".to_string()),
                    reading: None,
                })
                .collect()
        }
    }
}

/// Public handle the daemon owns across ticks.
pub struct PerfState {
    inner: imp::LinuxPerfState,
    knobs: PerfKnobs,
}

impl PerfState {
    /// Probe kernel knobs and try to open the five counters in
    /// [`COUNTER_NAMES`] against the calling process. The returned
    /// state is always usable — counters that failed to open are
    /// recorded as `Unavailable` rows with the open-time error in
    /// `reason`.
    pub fn try_open(root: &ProcfsRoot) -> Self {
        Self {
            inner: imp::LinuxPerfState::try_open(),
            knobs: PerfKnobs::probe(root),
        }
    }

    /// Read every counter and convert each to a [`MetricCapability`]
    /// row tagged `family = "perf"`. `degraded_reason` is augmented
    /// with the kernel-knob summary so downstream consumers can see
    /// the posture even when individual counters fail.
    pub fn render_capabilities(&mut self, mode: FamilyMode) -> Vec<Capability> {
        let knob_summary = self.knobs.summary();
        let statuses = self.inner.read_all();
        statuses
            .into_iter()
            .map(|s| {
                let mut reason_parts = Vec::new();
                if let Some(r) = s.reason {
                    reason_parts.push(r);
                }
                if !knob_summary.is_empty() {
                    reason_parts.push(knob_summary.clone());
                }
                let degraded_reason = if reason_parts.is_empty() {
                    None
                } else {
                    Some(reason_parts.join("; "))
                };
                Capability::with_perf_fields(
                    "perf",
                    mode,
                    s.status,
                    degraded_reason,
                    s.name,
                    s.reading,
                )
            })
            .collect()
    }

    pub fn knobs(&self) -> &PerfKnobs {
        &self.knobs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn knobs_summary_skips_missing_fields() {
        let k = PerfKnobs {
            paranoid: Some(2),
            mlock_kb: None,
            max_sample_rate: Some(100000),
        };
        let s = k.summary();
        assert!(s.contains("paranoid=2"));
        assert!(s.contains("max_sample_rate=100000"));
        assert!(!s.contains("mlock_kb"));
    }

    #[test]
    fn knobs_all_missing_yields_empty_string() {
        let k = PerfKnobs::default();
        assert_eq!(k.summary(), "");
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn non_linux_reports_unavailable_for_every_counter() {
        let mut state = PerfState::try_open(&ProcfsRoot::fixture("/tmp/nope"));
        let caps = state.render_capabilities(FamilyMode::Auto);
        assert_eq!(caps.len(), COUNTER_NAMES.len());
        for c in &caps {
            assert_eq!(c.runtime_status, RuntimeStatus::Unavailable);
        }
    }
}
