//! Process collector.
//!
//! Process selection: sysmon's own PID, plus any user-listed
//! `include_pids` / `include_executables`. The `include_aorta_nodes`
//! flag is recognized in config but not yet wired (it needs an
//! aorta_core queryable).

use std::collections::HashMap;
use std::fs;

use serde::{Deserialize, Serialize};

use crate::config::ProcessConfig;
use crate::procfs_root::ProcfsRoot;

use super::{delta_u64, per_sec};

/// Microseconds-per-tick computed once from `sysconf(_SC_CLK_TCK)`.
fn ticks_to_us() -> u64 {
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if hz <= 0 {
        return 10_000; // fall back to 100 Hz.
    }
    1_000_000 / hz as u64
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessCounters {
    pub pid: i32,
    pub comm: String,
    pub state: String,
    pub starttime_ticks: u64,
    pub thread_count: u32,
    pub utime_ticks: u64,
    pub stime_ticks: u64,
    pub cutime_ticks: u64,
    pub cstime_ticks: u64,
    pub minor_faults: u64,
    pub major_faults: u64,
    pub voluntary_ctxt_switches: u64,
    pub nonvoluntary_ctxt_switches: u64,
    pub rss_bytes: u64,
    pub vss_bytes: u64,
    pub rss_hwm_bytes: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub cancelled_write_bytes: u64,
    pub cmdline: String,
    pub exe: String,
    pub cgroup_path: String,
    pub fd_count: Option<u64>,
    pub oom_score: i32,
    pub oom_score_adj: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessSample {
    pub processes: HashMap<i32, ProcessCounters>,
}

impl ProcessSample {
    pub fn read_for(root: &ProcfsRoot, pids: &[i32]) -> Self {
        let mut processes = HashMap::new();
        for pid in pids {
            if let Some(counters) = read_one(root, *pid) {
                processes.insert(*pid, counters);
            }
        }
        Self { processes }
    }
}

fn read_one(root: &ProcfsRoot, pid: i32) -> Option<ProcessCounters> {
    let base = root.proc_path(format!("{}", pid));
    let stat_body = fs::read_to_string(base.join("stat")).ok()?;
    let (comm, state, fields) = parse_stat(&stat_body)?;

    let status_body = fs::read_to_string(base.join("status")).unwrap_or_default();
    let status = parse_kv(&status_body);

    let io_body = fs::read_to_string(base.join("io")).unwrap_or_default();
    let io = parse_kv(&io_body);

    let cmdline = fs::read_to_string(base.join("cmdline"))
        .map(|s| s.replace('\0', " ").trim().to_string())
        .unwrap_or_default();

    let exe = fs::read_link(base.join("exe"))
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();

    let cgroup_path = fs::read_to_string(base.join("cgroup"))
        .map(|s| first_v2_path(&s))
        .unwrap_or_default();

    let oom_score = fs::read_to_string(base.join("oom_score"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    let oom_score_adj = fs::read_to_string(base.join("oom_score_adj"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);

    let fd_count = match fs::read_dir(base.join("fd")) {
        Ok(it) => Some(it.filter_map(|e| e.ok()).count() as u64),
        // EACCES → Unknown (None).
        Err(_) => None,
    };

    let kb_to_bytes = |k: u64| k.saturating_mul(1024);

    Some(ProcessCounters {
        pid,
        comm,
        state,
        // /proc/[pid]/stat columns (1-indexed) → token offsets into
        // `fields` (0-indexed, against tokens AFTER `state`):
        //   col 4 ppid → fields[0]; col 10 minflt → fields[6];
        //   col 12 majflt → fields[8]; col 14 utime → fields[10];
        //   col 15 stime → fields[11]; col 16/17 cutime/cstime → 12/13;
        //   col 20 num_threads → 16; col 22 starttime → 18;
        //   col 23 vsize → 19; col 24 rss (pages) → 20.
        minor_faults: fields.get(6).copied().unwrap_or(0),
        major_faults: fields.get(8).copied().unwrap_or(0),
        utime_ticks: fields.get(10).copied().unwrap_or(0),
        stime_ticks: fields.get(11).copied().unwrap_or(0),
        cutime_ticks: fields.get(12).copied().unwrap_or(0),
        cstime_ticks: fields.get(13).copied().unwrap_or(0),
        thread_count: fields.get(16).copied().unwrap_or(0) as u32,
        starttime_ticks: fields.get(18).copied().unwrap_or(0),
        vss_bytes: fields.get(19).copied().unwrap_or(0),
        rss_bytes: fields
            .get(20)
            .copied()
            .unwrap_or(0)
            .saturating_mul(page_size()),
        rss_hwm_bytes: kb_to_bytes(status.get("VmHWM").copied().unwrap_or(0)),
        voluntary_ctxt_switches: status.get("voluntary_ctxt_switches").copied().unwrap_or(0),
        nonvoluntary_ctxt_switches: status
            .get("nonvoluntary_ctxt_switches")
            .copied()
            .unwrap_or(0),
        read_bytes: io.get("read_bytes").copied().unwrap_or(0),
        write_bytes: io.get("write_bytes").copied().unwrap_or(0),
        cancelled_write_bytes: io.get("cancelled_write_bytes").copied().unwrap_or(0),
        cmdline,
        exe,
        cgroup_path,
        fd_count,
        oom_score,
        oom_score_adj,
    })
}

/// `/proc/[pid]/stat` parser robust to spaces inside `comm`.
///
/// Returns `(comm, state, fields_after_state_as_u64)`. The returned
/// `fields` vector is 0-indexed against the tokens AFTER `state` —
/// i.e. `fields[0]` corresponds to column 4 of stat (ppid).
fn parse_stat(body: &str) -> Option<(String, String, Vec<u64>)> {
    let open = body.find('(')?;
    let close = body.rfind(')')?;
    if close <= open {
        return None;
    }
    let comm = body[open + 1..close].to_string();
    let tail = body[close + 1..].trim_start();
    let mut it = tail.split_whitespace();
    let state = it.next()?.to_string();
    let fields: Vec<u64> = it.map(|s| s.parse().unwrap_or(0)).collect();
    Some((comm, state, fields))
}

fn parse_kv(body: &str) -> HashMap<&str, u64> {
    let mut out = HashMap::new();
    for line in body.lines() {
        let mut it = line.split(':');
        let key = match it.next() {
            Some(k) => k.trim(),
            None => continue,
        };
        let rest = match it.next() {
            Some(r) => r.trim(),
            None => continue,
        };
        if let Some(num_str) = rest.split_whitespace().next() {
            if let Ok(n) = num_str.parse() {
                out.insert(key, n);
            }
        }
    }
    out
}

fn first_v2_path(cgroup_body: &str) -> String {
    // cgroup v2 line shape: "0::/system.slice/..."
    for line in cgroup_body.lines() {
        if let Some(rest) = line.strip_prefix("0::") {
            return rest.to_string();
        }
    }
    cgroup_body.lines().next().unwrap_or("").to_string()
}

fn page_size() -> u64 {
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if v <= 0 {
        4096
    } else {
        v as u64
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessStat {
    pub pid: i32,
    pub comm: String,
    pub state: String,
    pub cmdline: String,
    pub exe: String,
    pub cgroup_path: String,
    pub starttime_ticks: u64,
    pub thread_count: u32,
    pub cpu_user_us_delta: u64,
    pub cpu_kernel_us_delta: u64,
    pub cpu_total_us_delta: u64,
    pub cpu_children_us_delta: u64,
    pub cpu_percent_host: f32,
    pub cpu_percent_per_core: f32,
    pub vss_bytes: u64,
    pub rss_bytes: u64,
    pub rss_hwm_bytes: u64,
    pub minor_faults_per_sec: u64,
    pub major_faults_per_sec: u64,
    pub voluntary_ctxt_switches_per_sec: u64,
    pub nonvoluntary_ctxt_switches_per_sec: u64,
    pub read_bytes_per_sec: u64,
    pub write_bytes_per_sec: u64,
    pub cancelled_write_bytes: u64,
    /// `-1` reported as unknown / EACCES.
    pub fd_count: i64,
    pub oom_score: i32,
    pub oom_score_adj: i32,
    pub exit_marker: bool,
}

pub fn delta(
    prev: &ProcessSample,
    curr: &ProcessSample,
    interval_ms: u64,
    online_cpus: u32,
    cfg: &ProcessConfig,
) -> Vec<ProcessStat> {
    let us_per_tick = ticks_to_us();
    let interval_us = interval_ms.saturating_mul(1000) as f32;
    let cores = online_cpus.max(1) as f32;

    let mut out: Vec<ProcessStat> = Vec::new();

    for (pid, c) in &curr.processes {
        let p_match = prev.processes.get(pid);
        // PID-reuse detection: if starttime changed, emit one exit_marker
        // row for the previous lifetime, then the fresh row. The
        // collector only surfaces the marker; a long-running consumer should
        // drop the stale entry from its in-memory state.
        let reused = match p_match {
            Some(p) => p.starttime_ticks != 0 && p.starttime_ticks != c.starttime_ticks,
            None => false,
        };
        if reused {
            let p = p_match.expect("reused implies prev exists");
            out.push(make_exit_marker(p));
        }

        let prev_for_delta = if reused { None } else { p_match };
        let utime_delta_ticks = match prev_for_delta {
            Some(p) => delta_u64(p.utime_ticks, c.utime_ticks),
            None => 0,
        };
        let stime_delta_ticks = match prev_for_delta {
            Some(p) => delta_u64(p.stime_ticks, c.stime_ticks),
            None => 0,
        };
        let cutime_delta_ticks = match prev_for_delta {
            Some(p) => delta_u64(p.cutime_ticks, c.cutime_ticks),
            None => 0,
        };
        let cstime_delta_ticks = match prev_for_delta {
            Some(p) => delta_u64(p.cstime_ticks, c.cstime_ticks),
            None => 0,
        };
        let minor_d = match prev_for_delta {
            Some(p) => delta_u64(p.minor_faults, c.minor_faults),
            None => 0,
        };
        let major_d = match prev_for_delta {
            Some(p) => delta_u64(p.major_faults, c.major_faults),
            None => 0,
        };
        let vol_d = match prev_for_delta {
            Some(p) => delta_u64(p.voluntary_ctxt_switches, c.voluntary_ctxt_switches),
            None => 0,
        };
        let nonvol_d = match prev_for_delta {
            Some(p) => delta_u64(p.nonvoluntary_ctxt_switches, c.nonvoluntary_ctxt_switches),
            None => 0,
        };
        let read_d = match prev_for_delta {
            Some(p) => delta_u64(p.read_bytes, c.read_bytes),
            None => 0,
        };
        let write_d = match prev_for_delta {
            Some(p) => delta_u64(p.write_bytes, c.write_bytes),
            None => 0,
        };

        let cpu_user_us = utime_delta_ticks.saturating_mul(us_per_tick);
        let cpu_kernel_us = stime_delta_ticks.saturating_mul(us_per_tick);
        let cpu_total_us = cpu_user_us.saturating_add(cpu_kernel_us);
        let cpu_children_us = cutime_delta_ticks
            .saturating_add(cstime_delta_ticks)
            .saturating_mul(us_per_tick);

        let cpu_percent_host = if interval_us > 0.0 {
            100.0 * cpu_total_us as f32 / (interval_us * cores)
        } else {
            0.0
        };
        let cpu_percent_per_core = if interval_us > 0.0 {
            100.0 * cpu_total_us as f32 / interval_us
        } else {
            0.0
        };

        out.push(ProcessStat {
            pid: c.pid,
            comm: c.comm.clone(),
            state: c.state.clone(),
            cmdline: c.cmdline.clone(),
            exe: c.exe.clone(),
            cgroup_path: c.cgroup_path.clone(),
            starttime_ticks: c.starttime_ticks,
            thread_count: c.thread_count,
            cpu_user_us_delta: cpu_user_us,
            cpu_kernel_us_delta: cpu_kernel_us,
            cpu_total_us_delta: cpu_total_us,
            cpu_children_us_delta: cpu_children_us,
            cpu_percent_host,
            cpu_percent_per_core,
            vss_bytes: c.vss_bytes,
            rss_bytes: c.rss_bytes,
            rss_hwm_bytes: c.rss_hwm_bytes,
            minor_faults_per_sec: per_sec(minor_d, interval_ms),
            major_faults_per_sec: per_sec(major_d, interval_ms),
            voluntary_ctxt_switches_per_sec: per_sec(vol_d, interval_ms),
            nonvoluntary_ctxt_switches_per_sec: per_sec(nonvol_d, interval_ms),
            read_bytes_per_sec: per_sec(read_d, interval_ms),
            write_bytes_per_sec: per_sec(write_d, interval_ms),
            cancelled_write_bytes: c.cancelled_write_bytes,
            fd_count: c.fd_count.map(|v| v as i64).unwrap_or(-1),
            oom_score: c.oom_score,
            oom_score_adj: c.oom_score_adj,
            exit_marker: false,
        });
    }

    // Bound detail rows by `max_listed`; sort by CPU% descending so the
    // truncation is meaningful for top-process views. The HashMap that
    // backed `processes` has non-deterministic iteration order, so
    // primary CPU% ties must be broken on stable keys — otherwise
    // `truncate` would silently flip which process gets dropped between
    // runs and the snapshot would jitter.
    out.sort_by(|a, b| {
        b.cpu_percent_host
            .partial_cmp(&a.cpu_percent_host)
            .unwrap_or(std::cmp::Ordering::Equal)
            // exit_marker rows go LAST among ties so we don't drop fresh
            // entries in favor of stale ones.
            .then_with(|| a.exit_marker.cmp(&b.exit_marker))
            .then_with(|| a.pid.cmp(&b.pid))
            .then_with(|| a.starttime_ticks.cmp(&b.starttime_ticks))
    });
    if (cfg.max_listed as usize) < out.len() {
        out.truncate(cfg.max_listed as usize);
    }
    out
}

fn make_exit_marker(p: &ProcessCounters) -> ProcessStat {
    ProcessStat {
        pid: p.pid,
        comm: p.comm.clone(),
        state: "X".into(),
        cmdline: p.cmdline.clone(),
        exe: p.exe.clone(),
        cgroup_path: p.cgroup_path.clone(),
        starttime_ticks: p.starttime_ticks,
        thread_count: p.thread_count,
        exit_marker: true,
        fd_count: -1,
        ..Default::default()
    }
}

/// Resolve which PIDs to sample, given config.
pub fn discover_pids(_root: &ProcfsRoot, cfg: &ProcessConfig) -> Vec<i32> {
    let mut pids: Vec<i32> = Vec::new();
    if cfg.include_self {
        let pid = unsafe { libc::getpid() };
        pids.push(pid);
    }
    pids.extend(cfg.include_pids.iter().copied());
    // include_executables matching is best-effort: it would sniff
    // /proc/[pid]/comm for the listed names. To keep things bounded
    // without enumerating all PIDs, this implementation skips
    // executable matching for now.
    pids.sort_unstable();
    pids.dedup();
    pids
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAT_PID42: &str = "\
42 (myproc) S 1 42 42 0 -1 4194304 100 0 5 0 1000 500 0 0 20 0 1 0 5000 \
102400 256 18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0 0 0 0 0 0 0 0 0
";

    #[test]
    fn stat_parser_extracts_comm_and_fields() {
        let (comm, state, fields) = parse_stat(STAT_PID42).expect("parse");
        assert_eq!(comm, "myproc");
        assert_eq!(state, "S");
        // utime is column 14 of stat → fields[10] (0-indexed after state).
        assert_eq!(fields[10], 1000);
        // stime is column 15 → fields[11].
        assert_eq!(fields[11], 500);
    }

    #[test]
    fn stat_parser_handles_comm_with_spaces_and_parens() {
        // Linux allows comm to contain spaces and even nested parens; we
        // anchor on the last `)` to recover.
        let body = "7 (very weird (comm)) R 1 7 7 0 -1 0 0 0 0 0 1 2 3 4 20 0 1 0 99 0 0";
        let (comm, state, fields) = parse_stat(body).expect("parse");
        assert_eq!(comm, "very weird (comm)");
        assert_eq!(state, "R");
        // utime (col 14) → fields[10] = 1.
        assert_eq!(fields[10], 1);
    }

    #[test]
    fn child_cpu_excluded_from_process_cpu() {
        let us = ticks_to_us();
        let prev = ProcessCounters {
            pid: 1,
            starttime_ticks: 100,
            utime_ticks: 0,
            stime_ticks: 0,
            cutime_ticks: 0,
            cstime_ticks: 0,
            ..Default::default()
        };
        let curr = ProcessCounters {
            pid: 1,
            starttime_ticks: 100,
            // Parent stayed idle; only children moved.
            utime_ticks: 0,
            stime_ticks: 0,
            cutime_ticks: 1000,
            cstime_ticks: 500,
            ..Default::default()
        };
        let prev_sample = ProcessSample {
            processes: [(1, prev)].into_iter().collect(),
        };
        let curr_sample = ProcessSample {
            processes: [(1, curr)].into_iter().collect(),
        };
        let cfg = ProcessConfig::default();
        let stats = delta(&prev_sample, &curr_sample, 1000, 1, &cfg);
        let p = &stats[0];
        // Process-only CPU stays at 0 even though children consumed time.
        assert_eq!(p.cpu_user_us_delta, 0);
        assert_eq!(p.cpu_kernel_us_delta, 0);
        assert_eq!(p.cpu_total_us_delta, 0);
        // Children CPU appears in the dedicated field.
        assert_eq!(p.cpu_children_us_delta, 1500 * us);
    }

    #[test]
    fn cpu_percent_host_normalized_by_cores() {
        let us = ticks_to_us();
        // Pin one full core for one second (in ticks).
        let one_core_ticks_per_sec = 1_000_000 / us;
        let prev = ProcessCounters {
            pid: 1,
            starttime_ticks: 100,
            ..Default::default()
        };
        let curr = ProcessCounters {
            pid: 1,
            starttime_ticks: 100,
            utime_ticks: one_core_ticks_per_sec,
            ..Default::default()
        };
        let prev_sample = ProcessSample {
            processes: [(1, prev)].into_iter().collect(),
        };
        let curr_sample = ProcessSample {
            processes: [(1, curr)].into_iter().collect(),
        };
        let cfg = ProcessConfig::default();
        let stats = delta(&prev_sample, &curr_sample, 1000, 8, &cfg);
        let p = &stats[0];
        // 1 core out of 8 → 12.5% host.
        assert!((p.cpu_percent_host - 12.5).abs() < 0.5);
        // Same workload on per-core scale → ~100%.
        assert!((p.cpu_percent_per_core - 100.0).abs() < 0.5);
    }

    #[test]
    fn top_n_truncation_is_deterministic_on_ties() {
        // 4 processes all idle (cpu_percent_host = 0). The HashMap that
        // backs ProcessSample iterates in non-deterministic order, but
        // after sorting + truncating to top-2 we must always keep the
        // SAME two pids — otherwise downstream consumers see the
        // process list flicker between snapshots.
        let mut processes = HashMap::new();
        for pid in [100, 50, 200, 75] {
            processes.insert(
                pid,
                ProcessCounters {
                    pid,
                    starttime_ticks: 1,
                    ..Default::default()
                },
            );
        }
        let cfg = ProcessConfig {
            max_listed: 2,
            ..ProcessConfig::default()
        };
        let curr = ProcessSample {
            processes: processes.clone(),
        };
        let prev = ProcessSample { processes };

        // Run delta 5 times; expect identical pid lists every iteration.
        let baseline: Vec<i32> = delta(&prev, &curr, 1000, 1, &cfg)
            .iter()
            .map(|p| p.pid)
            .collect();
        assert_eq!(baseline.len(), 2);
        for _ in 0..4 {
            let again: Vec<i32> = delta(&prev, &curr, 1000, 1, &cfg)
                .iter()
                .map(|p| p.pid)
                .collect();
            assert_eq!(again, baseline);
        }
        // Tiebreak is ascending pid → expect pids 50 and 75.
        assert_eq!(baseline, vec![50, 75]);
    }

    #[test]
    fn pid_reuse_emits_exit_marker_then_fresh() {
        let prev = ProcessSample {
            processes: [(
                1,
                ProcessCounters {
                    pid: 1,
                    comm: "old".into(),
                    starttime_ticks: 100,
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
        };
        let curr = ProcessSample {
            processes: [(
                1,
                ProcessCounters {
                    pid: 1,
                    comm: "new".into(),
                    starttime_ticks: 999, // different starttime → PID reuse
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
        };
        let cfg = ProcessConfig::default();
        let stats = delta(&prev, &curr, 1000, 1, &cfg);
        assert_eq!(stats.len(), 2);
        // Top of the list after sort is exit_marker (cpu_percent_host == 0 ties; we accept either order)
        let exit = stats.iter().find(|s| s.exit_marker).expect("exit marker");
        assert_eq!(exit.comm, "old");
        let fresh = stats.iter().find(|s| !s.exit_marker).expect("fresh");
        assert_eq!(fresh.comm, "new");
    }
}
