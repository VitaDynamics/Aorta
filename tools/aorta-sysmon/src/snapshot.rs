//! Snapshot aggregator — orchestrates one round of collection.
//!
//! `collect_once` produces a `Snapshot` from two procfs reads spaced by
//! `interval_ms`. The JSON renderer used by `aorta-sysmon snapshot
//! --pretty` is a debug-only stdout view; the format is **not
//! stable** and downstream consumers must rely on the daemon's typed
//! FlatBuffers publication instead.

use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::capability::{self, Capability, KernelKnobs};
use crate::collector::{
    cpu::{self, CpuSample, HostCpu},
    disk::{self, DiskSample, DiskStat},
    memory::{self, HostMemory, MemorySample},
    network::{self, NetIfStat, NetSample},
    process::{self, ProcessSample, ProcessStat},
    psi::{self, PsiLine, PsiSample},
};
use crate::config::Config;
use crate::identity::HostIdentity;
use crate::procfs_root::ProcfsRoot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub host_name: String,
    pub host_id: String,
    pub host_name_key: String,
    pub host_id_key: String,
    pub kernel: String,
    pub preempt_model: String,
    pub cgroup_version: u8,
    pub online_cpus: u32,
    pub interval_ms: u64,
    pub sequence: u64,
    pub capabilities: Vec<Capability>,
    pub cpu: HostCpu,
    pub memory: HostMemory,
    pub disks: Vec<DiskStat>,
    pub net_ifs: Vec<NetIfStat>,
    pub psi: Vec<PsiLine>,
    pub processes: Vec<ProcessStat>,
}

/// Collect one snapshot. Takes two procfs samples spaced by `interval_ms`.
pub fn collect_once(
    cfg: &Config,
    identity: &HostIdentity,
    root: &ProcfsRoot,
    interval_ms: u64,
) -> Result<Snapshot> {
    let knobs = KernelKnobs::probe(root);
    let capabilities = capability::probe(cfg, root);

    let pids = process::discover_pids(root, &cfg.process);

    // First sample (t=0).
    let cpu_p = CpuSample::read(root)?;
    let mem_p = MemorySample::read(root)?;
    let disk_p = DiskSample::read(root)?;
    let net_p = NetSample::read(root)?;
    let psi_p = PsiSample::read(root);
    let proc_p = ProcessSample::read_for(root, &pids);

    let t0 = Instant::now();
    thread::sleep(Duration::from_millis(interval_ms));
    let elapsed_ms = t0.elapsed().as_millis() as u64;
    // Guard against host clock weirdness — use the requested interval if
    // the actual elapsed is implausible (e.g. test injects 0).
    let effective_ms = elapsed_ms.max(1);

    // Second sample (t=interval).
    let cpu_c = CpuSample::read(root)?;
    let mem_c = MemorySample::read(root)?;
    let disk_c = DiskSample::read(root)?;
    let net_c = NetSample::read(root)?;
    let psi_c = PsiSample::read(root);
    let proc_c = ProcessSample::read_for(root, &pids);

    Ok(Snapshot {
        host_name: identity.host_name.clone(),
        host_id: identity.host_id.clone(),
        host_name_key: identity.host_name_key.clone(),
        host_id_key: identity.host_id_key.clone(),
        kernel: knobs.kernel,
        preempt_model: knobs.preempt_model,
        cgroup_version: knobs.cgroup_version,
        online_cpus: knobs.online_cpus,
        interval_ms: effective_ms,
        sequence: 0,
        capabilities,
        cpu: cpu::delta(&cpu_p, &cpu_c, effective_ms),
        memory: memory::delta(&mem_p, &mem_c, effective_ms),
        disks: disk::delta(&disk_p, &disk_c, effective_ms, &cfg.filter, root),
        net_ifs: network::delta(&net_p, &net_c, effective_ms, &cfg.filter, root),
        psi: psi::delta(&psi_p, &psi_c, effective_ms),
        processes: process::delta(
            &proc_p,
            &proc_c,
            effective_ms,
            knobs.online_cpus,
            &cfg.process,
        ),
    })
}

/// Render a snapshot as JSON for `--pretty` debug output.
pub fn render_json(snapshot: &Snapshot, pretty: bool) -> Result<String> {
    if pretty {
        Ok(serde_json::to_string_pretty(snapshot)?)
    } else {
        Ok(serde_json::to_string(snapshot)?)
    }
}
