//! Host memory collector.

use std::collections::HashMap;
use std::fs;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::procfs_root::ProcfsRoot;

use super::per_sec;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemorySample {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub free_bytes: u64,
    pub buffers_bytes: u64,
    pub cached_bytes: u64,
    pub swap_total_bytes: u64,
    pub swap_free_bytes: u64,
    pub huge_pages_total: u64,
    pub huge_pages_free: u64,
    /// Cumulative since boot.
    pub pgfault: u64,
    pub pgmajfault: u64,
    pub oom_kill: u64,
}

impl MemorySample {
    pub fn read(root: &ProcfsRoot) -> Result<Self> {
        let meminfo =
            fs::read_to_string(root.proc_path("meminfo")).context("read /proc/meminfo")?;
        let vmstat = fs::read_to_string(root.proc_path("vmstat")).unwrap_or_default();
        Self::parse(&meminfo, &vmstat)
    }

    pub fn parse(meminfo: &str, vmstat: &str) -> Result<Self> {
        let mi = parse_meminfo(meminfo);
        let huge_total = mi.get("HugePages_Total").copied().unwrap_or(0);
        let huge_free = mi.get("HugePages_Free").copied().unwrap_or(0);
        // /proc/meminfo numeric fields are in kB except HugePages_* which are counts.
        let kb_to_bytes = |k: u64| k.saturating_mul(1024);
        let vm = parse_vmstat(vmstat);
        Ok(Self {
            total_bytes: kb_to_bytes(*mi.get("MemTotal").unwrap_or(&0)),
            available_bytes: kb_to_bytes(*mi.get("MemAvailable").unwrap_or(&0)),
            free_bytes: kb_to_bytes(*mi.get("MemFree").unwrap_or(&0)),
            buffers_bytes: kb_to_bytes(*mi.get("Buffers").unwrap_or(&0)),
            cached_bytes: kb_to_bytes(*mi.get("Cached").unwrap_or(&0)),
            swap_total_bytes: kb_to_bytes(*mi.get("SwapTotal").unwrap_or(&0)),
            swap_free_bytes: kb_to_bytes(*mi.get("SwapFree").unwrap_or(&0)),
            huge_pages_total: huge_total,
            huge_pages_free: huge_free,
            pgfault: *vm.get("pgfault").unwrap_or(&0),
            pgmajfault: *vm.get("pgmajfault").unwrap_or(&0),
            oom_kill: *vm.get("oom_kill").unwrap_or(&0),
        })
    }
}

fn parse_meminfo(body: &str) -> HashMap<&str, u64> {
    let mut out = HashMap::new();
    for line in body.lines() {
        // "MemTotal:  16329136 kB"
        let mut it = line.split(':');
        let key = match it.next() {
            Some(k) => k.trim(),
            None => continue,
        };
        let rest = match it.next() {
            Some(r) => r.trim(),
            None => continue,
        };
        let num_str = rest.split_whitespace().next().unwrap_or("0");
        if let Ok(n) = num_str.parse() {
            out.insert(key, n);
        }
    }
    out
}

fn parse_vmstat(body: &str) -> HashMap<&str, u64> {
    let mut out = HashMap::new();
    for line in body.lines() {
        let mut it = line.split_whitespace();
        let key = match it.next() {
            Some(k) => k,
            None => continue,
        };
        let val = match it.next() {
            Some(v) => v,
            None => continue,
        };
        if let Ok(n) = val.parse() {
            out.insert(key, n);
        }
    }
    out
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostMemory {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub free_bytes: u64,
    pub buffers_bytes: u64,
    pub cached_bytes: u64,
    pub swap_total_bytes: u64,
    pub swap_free_bytes: u64,
    pub huge_pages_total: u64,
    pub huge_pages_free: u64,
    pub pgfault_per_sec: u64,
    pub pgmajfault_per_sec: u64,
    pub oom_kill_count: u64,
}

pub fn delta(prev: &MemorySample, curr: &MemorySample, interval_ms: u64) -> HostMemory {
    HostMemory {
        total_bytes: curr.total_bytes,
        available_bytes: curr.available_bytes,
        free_bytes: curr.free_bytes,
        buffers_bytes: curr.buffers_bytes,
        cached_bytes: curr.cached_bytes,
        swap_total_bytes: curr.swap_total_bytes,
        swap_free_bytes: curr.swap_free_bytes,
        huge_pages_total: curr.huge_pages_total,
        huge_pages_free: curr.huge_pages_free,
        pgfault_per_sec: per_sec(super::delta_u64(prev.pgfault, curr.pgfault), interval_ms),
        pgmajfault_per_sec: per_sec(
            super::delta_u64(prev.pgmajfault, curr.pgmajfault),
            interval_ms,
        ),
        oom_kill_count: curr.oom_kill,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMINFO: &str = "\
MemTotal:        16329136 kB
MemFree:          2000000 kB
MemAvailable:     8000000 kB
Buffers:           100000 kB
Cached:           4000000 kB
SwapTotal:        2000000 kB
SwapFree:         2000000 kB
HugePages_Total:        0
HugePages_Free:         0
";

    #[test]
    fn meminfo_parses_units_kb() {
        let m = MemorySample::parse(MEMINFO, "").unwrap();
        assert_eq!(m.total_bytes, 16329136 * 1024);
        assert_eq!(m.available_bytes, 8000000 * 1024);
    }

    #[test]
    fn pgfault_delta_per_sec() {
        let prev = MemorySample {
            pgfault: 100,
            pgmajfault: 5,
            ..Default::default()
        };
        let curr = MemorySample {
            pgfault: 1100,
            pgmajfault: 25,
            ..Default::default()
        };
        let d = delta(&prev, &curr, 1000);
        assert_eq!(d.pgfault_per_sec, 1000);
        assert_eq!(d.pgmajfault_per_sec, 20);
    }
}
