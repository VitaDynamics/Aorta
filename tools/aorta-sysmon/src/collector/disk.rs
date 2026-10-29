//! Disk collector.
//!
//! `/proc/diskstats` columns (1-indexed):
//!   1 major  2 minor  3 device
//!   4 reads_completed  5 reads_merged  6 sectors_read  7 read_ticks(ms)
//!   8 writes_completed 9 writes_merged 10 sectors_written 11 write_ticks(ms)
//!   12 ios_in_flight   13 io_ticks(ms) 14 weighted_io_ticks(ms)
//!   15 discards_completed 16 discards_merged 17 sectors_discarded
//!
//! Sector counts are **always 512-byte units** regardless of the device's
//! logical block size (see `Documentation/admin-guide/iostats.rst`).

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::Filters;
use crate::procfs_root::ProcfsRoot;

use super::{delta_u64, glob_match, per_sec};

const SECTOR_BYTES: u64 = 512;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskCounters {
    pub reads_completed: u64,
    pub reads_merged: u64,
    pub sectors_read: u64,
    pub read_ticks_ms: u64,
    pub writes_completed: u64,
    pub writes_merged: u64,
    pub sectors_written: u64,
    pub write_ticks_ms: u64,
    pub io_ticks_ms: u64,
    pub weighted_io_ticks_ms: u64,
    pub discards_completed: u64,
    pub sectors_discarded: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskSample {
    pub devices: HashMap<String, DiskCounters>,
}

impl DiskSample {
    pub fn read(root: &ProcfsRoot) -> Result<Self> {
        let body =
            fs::read_to_string(root.proc_path("diskstats")).context("read /proc/diskstats")?;
        Ok(Self::parse(&body))
    }

    pub fn parse(body: &str) -> Self {
        let mut devices = HashMap::new();
        for line in body.lines() {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.len() < 14 {
                continue;
            }
            let device = cols[2].to_string();
            let get = |i: usize| -> u64 { cols.get(i).and_then(|s| s.parse().ok()).unwrap_or(0) };
            let counters = DiskCounters {
                reads_completed: get(3),
                reads_merged: get(4),
                sectors_read: get(5),
                read_ticks_ms: get(6),
                writes_completed: get(7),
                writes_merged: get(8),
                sectors_written: get(9),
                write_ticks_ms: get(10),
                io_ticks_ms: get(12),
                weighted_io_ticks_ms: get(13),
                discards_completed: get(14),
                sectors_discarded: get(16),
            };
            devices.insert(device, counters);
        }
        Self { devices }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskStat {
    pub device: String,
    pub read_bytes_per_sec: u64,
    pub write_bytes_per_sec: u64,
    pub read_ios_per_sec: u64,
    pub write_ios_per_sec: u64,
    pub discard_bytes_per_sec: u64,
    pub discard_ios_per_sec: u64,
    pub inflight_reads: u64,
    pub inflight_writes: u64,
    pub await_ms: f32,
    pub util_percent: f32,
    pub avg_queue_length: f32,
    pub logical_block_size: u32,
}

pub fn delta(
    prev: &DiskSample,
    curr: &DiskSample,
    interval_ms: u64,
    filters: &Filters,
    root: &ProcfsRoot,
) -> Vec<DiskStat> {
    let mut out = Vec::new();
    for (device, c) in &curr.devices {
        if !accept(device, filters) {
            continue;
        }
        let p = match prev.devices.get(device) {
            Some(p) => p,
            None => continue,
        };
        out.push(compute(device, p, c, interval_ms, root));
    }
    out.sort_by(|a, b| a.device.cmp(&b.device));
    out
}

fn accept(device: &str, filters: &Filters) -> bool {
    let included = filters.disk_include.is_empty()
        || filters.disk_include.iter().any(|p| glob_match(p, device));
    let excluded = filters.disk_exclude.iter().any(|p| glob_match(p, device));
    included && !excluded
}

fn compute(
    device: &str,
    prev: &DiskCounters,
    curr: &DiskCounters,
    interval_ms: u64,
    root: &ProcfsRoot,
) -> DiskStat {
    let read_sectors = delta_u64(prev.sectors_read, curr.sectors_read);
    let write_sectors = delta_u64(prev.sectors_written, curr.sectors_written);
    let discard_sectors = delta_u64(prev.sectors_discarded, curr.sectors_discarded);
    let reads = delta_u64(prev.reads_completed, curr.reads_completed);
    let writes = delta_u64(prev.writes_completed, curr.writes_completed);
    let discards = delta_u64(prev.discards_completed, curr.discards_completed);
    let read_ticks = delta_u64(prev.read_ticks_ms, curr.read_ticks_ms);
    let write_ticks = delta_u64(prev.write_ticks_ms, curr.write_ticks_ms);
    let io_ticks = delta_u64(prev.io_ticks_ms, curr.io_ticks_ms);
    let weighted_ticks = delta_u64(prev.weighted_io_ticks_ms, curr.weighted_io_ticks_ms);

    let await_ms = if reads + writes > 0 {
        (read_ticks + write_ticks) as f32 / (reads + writes) as f32
    } else {
        0.0
    };
    let util_percent = if interval_ms > 0 {
        let raw = io_ticks as f32 * 100.0 / interval_ms as f32;
        raw.min(100.0)
    } else {
        0.0
    };
    let avg_queue_length = if interval_ms > 0 {
        weighted_ticks as f32 / interval_ms as f32
    } else {
        0.0
    };

    let (inflight_reads, inflight_writes) = read_inflight(root, device);
    let logical_block_size = read_logical_block_size(root, device);

    DiskStat {
        device: device.to_string(),
        read_bytes_per_sec: per_sec(read_sectors.saturating_mul(SECTOR_BYTES), interval_ms),
        write_bytes_per_sec: per_sec(write_sectors.saturating_mul(SECTOR_BYTES), interval_ms),
        read_ios_per_sec: per_sec(reads, interval_ms),
        write_ios_per_sec: per_sec(writes, interval_ms),
        discard_bytes_per_sec: per_sec(discard_sectors.saturating_mul(SECTOR_BYTES), interval_ms),
        discard_ios_per_sec: per_sec(discards, interval_ms),
        inflight_reads,
        inflight_writes,
        await_ms,
        util_percent,
        avg_queue_length,
        logical_block_size,
    }
}

fn read_inflight(root: &ProcfsRoot, device: &str) -> (u64, u64) {
    let path = root.sys_path(format!("block/{}/inflight", device));
    let body = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return (0, 0),
    };
    let mut it = body.split_whitespace();
    let r = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let w = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    (r, w)
}

fn read_logical_block_size(root: &ProcfsRoot, device: &str) -> u32 {
    let path = root.sys_path(format!("block/{}/queue/logical_block_size", device));
    read_u32(&path).unwrap_or(512)
}

fn read_u32(path: &Path) -> Option<u32> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn util_capped_at_100() {
        let p = DiskCounters {
            io_ticks_ms: 0,
            ..Default::default()
        };
        let c = DiskCounters {
            io_ticks_ms: 5000,
            ..Default::default()
        };
        // 5000ms busy in 1000ms wall → would be 500% raw, must cap at 100.
        let stat = compute("sda", &p, &c, 1000, &ProcfsRoot::fixture("/tmp/nope"));
        assert!((stat.util_percent - 100.0).abs() < 0.01);
    }

    #[test]
    fn await_iostat_formula() {
        let p = DiskCounters::default();
        let c = DiskCounters {
            reads_completed: 100,
            writes_completed: 100,
            read_ticks_ms: 1000,
            write_ticks_ms: 1000,
            ..Default::default()
        };
        let stat = compute("sda", &p, &c, 1000, &ProcfsRoot::fixture("/tmp/nope"));
        // (1000 + 1000) / (100 + 100) = 10 ms per IO.
        assert!((stat.await_ms - 10.0).abs() < 0.01);
    }

    #[test]
    fn bytes_use_fixed_512_sectors() {
        let p = DiskCounters::default();
        let c = DiskCounters {
            sectors_read: 2048, // 2048 * 512 = 1 MiB
            ..Default::default()
        };
        let stat = compute("sda", &p, &c, 1000, &ProcfsRoot::fixture("/tmp/nope"));
        assert_eq!(stat.read_bytes_per_sec, 2048 * 512);
    }

    #[test]
    fn await_zero_when_no_ios() {
        let p = DiskCounters::default();
        let c = DiskCounters::default();
        let stat = compute("sda", &p, &c, 1000, &ProcfsRoot::fixture("/tmp/nope"));
        assert_eq!(stat.await_ms, 0.0);
    }

    #[test]
    fn filter_excludes_loop_devices() {
        let filters = Filters::default();
        assert!(accept("sda", &filters));
        assert!(accept("nvme0n1", &filters));
        assert!(!accept("loop0", &filters));
        assert!(!accept("dm-0", &filters));
    }
}
