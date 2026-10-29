//! Host CPU collector.

use std::fs;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::procfs_root::ProcfsRoot;

use super::{parse_u64_col, per_sec};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuTimes {
    pub user: u64,
    pub nice: u64,
    pub system: u64,
    pub idle: u64,
    pub iowait: u64,
    pub irq: u64,
    pub softirq: u64,
    pub steal: u64,
}

impl CpuTimes {
    fn parse_line(line: &str) -> Result<Self> {
        // /proc/stat cpu line: cpu  user nice system idle iowait irq softirq steal ...
        // Required fields (user..idle) must be present and parseable.
        // Optional fields (iowait..steal) may be absent on older kernels
        // — `None` means "kernel doesn't report it" and falls back to 0 —
        // but if they ARE present and fail to parse, that's corruption
        // and must surface as an error.
        let mut it = line.split_whitespace();
        let _label = it.next();
        let required = |it: &mut std::str::SplitWhitespace<'_>| -> Result<u64> {
            it.next()
                .ok_or_else(|| anyhow::anyhow!("short cpu line: {:?}", line))?
                .parse::<u64>()
                .context("parse cpu field")
        };
        let optional = |it: &mut std::str::SplitWhitespace<'_>| -> Result<u64> {
            match it.next() {
                None => Ok(0),
                Some(s) => s.parse::<u64>().context("parse optional cpu field"),
            }
        };
        Ok(Self {
            user: required(&mut it)?,
            nice: required(&mut it)?,
            system: required(&mut it)?,
            idle: required(&mut it)?,
            iowait: optional(&mut it)?,
            irq: optional(&mut it)?,
            softirq: optional(&mut it)?,
            steal: optional(&mut it)?,
        })
    }

    fn total(&self) -> u64 {
        self.user
            .saturating_add(self.nice)
            .saturating_add(self.system)
            .saturating_add(self.idle)
            .saturating_add(self.iowait)
            .saturating_add(self.irq)
            .saturating_add(self.softirq)
            .saturating_add(self.steal)
    }

    fn busy(&self) -> u64 {
        self.total()
            .saturating_sub(self.idle)
            .saturating_sub(self.iowait)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuSample {
    pub aggregate: CpuTimes,
    pub per_core: Vec<CpuTimes>,
    pub ctxt: u64,
    pub procs_running: u32,
    pub procs_blocked: u32,
}

impl CpuSample {
    pub fn read(root: &ProcfsRoot) -> Result<Self> {
        let body = fs::read_to_string(root.proc_path("stat")).context("read /proc/stat")?;
        Self::parse(&body)
    }

    pub fn parse(body: &str) -> Result<Self> {
        let mut aggregate: Option<CpuTimes> = None;
        let mut per_core = Vec::new();
        let mut ctxt = 0u64;
        let mut procs_running = 0u32;
        let mut procs_blocked = 0u32;

        for line in body.lines() {
            if line.starts_with("cpu ") {
                aggregate = Some(CpuTimes::parse_line(line)?);
            } else if line.starts_with("cpu") {
                if let Some(b) = line.as_bytes().get(3) {
                    if b.is_ascii_digit() {
                        per_core.push(CpuTimes::parse_line(line)?);
                    }
                }
            } else if let Some(v) = line.strip_prefix("ctxt ") {
                ctxt = v.trim().parse().context("parse /proc/stat ctxt")?;
            } else if let Some(v) = line.strip_prefix("procs_running ") {
                procs_running = v.trim().parse().context("parse procs_running")?;
            } else if let Some(v) = line.strip_prefix("procs_blocked ") {
                procs_blocked = v.trim().parse().context("parse procs_blocked")?;
            }
        }

        let aggregate =
            aggregate.ok_or_else(|| anyhow::anyhow!("/proc/stat missing aggregate `cpu` line"))?;

        Ok(Self {
            aggregate,
            per_core,
            ctxt,
            procs_running,
            procs_blocked,
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostCpu {
    pub usage_percent_total: f32,
    pub usage_percent_per_core: Vec<f32>,
    pub context_switches_per_sec: u64,
    pub procs_running: u32,
    pub procs_blocked: u32,
}

pub fn delta(prev: &CpuSample, curr: &CpuSample, interval_ms: u64) -> HostCpu {
    let usage_percent_total = busy_pct(&prev.aggregate, &curr.aggregate);
    let usage_percent_per_core = prev
        .per_core
        .iter()
        .zip(curr.per_core.iter())
        .map(|(p, c)| busy_pct(p, c))
        .collect();
    let context_switches_per_sec = per_sec(super::delta_u64(prev.ctxt, curr.ctxt), interval_ms);

    HostCpu {
        usage_percent_total,
        usage_percent_per_core,
        context_switches_per_sec,
        procs_running: curr.procs_running,
        procs_blocked: curr.procs_blocked,
    }
}

fn busy_pct(prev: &CpuTimes, curr: &CpuTimes) -> f32 {
    let total = curr.total().saturating_sub(prev.total());
    if total == 0 {
        return 0.0;
    }
    let busy = curr.busy().saturating_sub(prev.busy());
    100.0 * busy as f32 / total as f32
}

// `parse_u64_col` is shared via super; suppress unused-import warning
// when this module ends up not needing it (kept as a re-export hint).
#[allow(dead_code)]
fn _keep_import(line: &str) -> Option<u64> {
    parse_u64_col(line, 0).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAT_PREV: &str = "\
cpu  100 0 50 850 0 0 0 0 0 0
cpu0 50 0 25 425 0 0 0 0
cpu1 50 0 25 425 0 0 0 0
ctxt 10000
procs_running 1
procs_blocked 0
";
    const STAT_CURR: &str = "\
cpu  200 0 100 1700 0 0 0 0 0 0
cpu0 100 0 50 850 0 0 0 0
cpu1 100 0 50 850 0 0 0 0
ctxt 15000
procs_running 2
procs_blocked 1
";

    #[test]
    fn cpu_delta_50pct() {
        let p = CpuSample::parse(STAT_PREV).unwrap();
        let c = CpuSample::parse(STAT_CURR).unwrap();
        let d = delta(&p, &c, 1000);
        // Busy increase = 150 (user+system), total increase = 1000, so 15%.
        assert!((d.usage_percent_total - 15.0).abs() < 0.01);
        assert_eq!(d.context_switches_per_sec, 5000);
        assert_eq!(d.procs_running, 2);
        assert_eq!(d.procs_blocked, 1);
        assert_eq!(d.usage_percent_per_core.len(), 2);
    }
}
