//! Aorta SDK publish glue.
//!
//! Builds a single `aorta_sdk::Node`, registers the `SysmonSnapshot`
//! schema by topic, and drives one publish per `interval_ms` tick using
//! `tokio::time::interval` with `MissedTickBehavior::Skip`.
//!
//! This module is gated behind the `sdk-publish` cargo feature so that
//! cargo lints and unit tests can run without pulling in the codegen
//! wrapper crate (which is Bazel-only). Bazel builds always enable the
//! feature via `crate_features` in `BUILD.bazel`.

#![cfg(feature = "sdk-publish")]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use sysmon_alert_msg::prelude::SysmonAlertSchema;
use sysmon_snapshot_msg::prelude::SysmonSnapshotSchema;
use tokio::time::{interval, Instant, MissedTickBehavior};

use crate::alert::{self, Alert, AlertPayload, AlertSeverity};
use crate::config::Config;
use crate::identity::HostIdentity;
use crate::procfs_root::ProcfsRoot;
use crate::snapshot::{self, Snapshot};

/// Resolve the publish topic suffix by substituting `{host_name}` /
/// `{host_id}` (normalized form) into the user-supplied template.
///
/// Returns the suffix that should be passed to
/// `Node::create_publisher_with_options` — the SDK prepends
/// `aorta/{group}/pub/` itself via `KeyExprBuilder::topic_key`.
pub fn resolve_topic_suffix(template: &str, identity: &HostIdentity) -> String {
    template
        .replace("{host_name}", &identity.host_name_key)
        .replace("{host_id}", &identity.host_id_key)
}

/// Spawn the daemon loop. Runs until `count` publishes have completed
/// (when `Some`) or until the executor is asked to stop.
///
/// The function constructs a single `aorta_sdk::Node`, a single typed
/// `Publisher<SysmonSnapshotSchema>`, and then loops on a tokio interval.
/// Each tick collects one fresh snapshot (which itself sleeps for
/// `interval_ms` between the two procfs reads) and publishes it.
pub async fn run_daemon(cfg: Config, identity: HostIdentity, count: Option<u64>) -> Result<()> {
    let snapshot_topic = resolve_topic_suffix(&cfg.topic_suffix, &identity);
    let alert_topic = resolve_topic_suffix(&cfg.alert_topic_suffix, &identity);

    // Use the `AORTA_GROUP` env var when set, otherwise fall back to the
    // canonical `"default"` group (matches `aorta_core::key_expr::DEFAULT_GROUP`).
    let group = std::env::var("AORTA_GROUP").unwrap_or_else(|_| "default".to_string());
    let node = aorta_sdk::Node::new(cfg.node_name.clone(), &group)
        .await
        .context("create aorta node")?;

    let snapshot_publisher = node
        .create_publisher_with_options::<SysmonSnapshotSchema>(
            &snapshot_topic,
            aorta_sdk::PublisherOptions::default(),
        )
        .await
        .with_context(|| format!("create snapshot publisher for {snapshot_topic:?}"))?;

    let alert_publisher = node
        .create_publisher_with_options::<SysmonAlertSchema>(
            &alert_topic,
            aorta_sdk::PublisherOptions::default(),
        )
        .await
        .with_context(|| format!("create alert publisher for {alert_topic:?}"))?;

    let root = ProcfsRoot::system();

    // Open perf-event counters once at daemon start and read
    // them every tick. State is owned by the daemon task; the
    // capability rows produced each tick are appended to the snapshot
    // before publish so consumers see per-counter `family = "perf"`
    // rows alongside the rest of the families. Counters that fail to
    // open (paranoid≥3 on non-root, or hardware missing the PMU) are
    // reported as `Unavailable` rows with the failure reason — the
    // collector never crashes the daemon.
    let mut perf_state = crate::collector::perf::PerfState::try_open(&root);

    // Daemon interval = config-driven, ≥1ms so the snapshot collector's
    // delta interval is always meaningful.
    let interval_ms = cfg.interval_ms.max(1);
    let mut ticker = interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut sequence: u64 = 0;
    let monotonic_origin = Instant::now();

    // `--count 0` means publish zero times. The natural CLI use of
    // `--count` is "publish N then exit", so 0 should be a clean no-op
    // rather than "tick once before checking". Short-circuit before
    // the loop body to keep the boundary intuitive.
    if matches!(count, Some(0)) {
        return Ok(());
    }

    loop {
        ticker.tick().await;
        // `collect_once` is sync + sleep-based. Run it on a
        // blocking thread so the tokio reactor stays free for the
        // publisher's Zenoh I/O.
        let snap = {
            let cfg = cfg.clone();
            let identity = identity.clone();
            let root = root.clone();
            tokio::task::spawn_blocking(move || {
                snapshot::collect_once(&cfg, &identity, &root, interval_ms)
            })
            .await
            .context("snapshot collector task join")?
        }
        .context("collect snapshot")?;

        let mut snap = snap;
        snap.sequence = sequence;
        sequence = sequence.saturating_add(1);

        // Replace the Slice-1 placeholder `perf` capability row with
        // one row per opened counter, populated with the current
        // cumulative reading. We do this AFTER `collect_once` so the
        // snapshot pipeline stays pure / stateless; perf state lives
        // entirely in the daemon.
        snap.capabilities.retain(|c| c.family != "perf");
        snap.capabilities
            .extend(perf_state.render_capabilities(cfg.metrics.perf));

        // Stamp the snapshot and every alert produced from it with the
        // same wall + monotonic clock so a consumer can correlate the
        // snapshot publish with the alert batch by either clock.
        let observed_at_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let monotonic_ns = monotonic_origin.elapsed().as_nanos() as u64;

        let msg = snapshot_to_msg(&snap, monotonic_ns);
        snapshot_publisher
            .publish_typed_msg(&msg)
            .context("publish snapshot")?;

        for alert in alert::evaluate(&snap, &cfg, observed_at_ns, monotonic_ns) {
            let amsg = alert_to_msg(&snap, &alert);
            alert_publisher
                .publish_typed_msg(&amsg)
                .context("publish alert")?;
        }

        if let Some(n) = count {
            if sequence >= n {
                break;
            }
        }
    }

    Ok(())
}

/// Convert a Slice-1 [`Snapshot`] into the codegen-generated
/// `SysmonSnapshotMsg`. Header fields are NOT populated here — the SDK
/// injects `aorta_header` via the `HeaderInjector` trait on each
/// publish.
///
/// `monotonic_ns` is the daemon's monotonic-clock reading at tick
/// time. Alerts produced from the same tick are stamped with the same
/// value so consumers can join snapshots and alerts by either the
/// wall clock (`aorta_header.publish_stamp_ns`) or the monotonic
/// reference.
fn snapshot_to_msg(
    snap: &Snapshot,
    monotonic_ns: u64,
) -> sysmon_snapshot_msg::typed::SysmonSnapshotMsg<'static> {
    use sysmon_snapshot_msg::typed as t;

    t::SysmonSnapshotMsg {
        schema_version: Some(1),
        host_name: Some(snap.host_name.clone()),
        host_id: Some(snap.host_id.clone()),
        kernel: Some(snap.kernel.clone()),
        preempt_model: Some(snap.preempt_model.clone()),
        cgroup_version: Some(snap.cgroup_version),
        online_cpus: Some(snap.online_cpus),
        monotonic_ns: Some(monotonic_ns),
        interval_ms: Some(snap.interval_ms as u32),
        sequence: Some(snap.sequence),
        capabilities: Some(snap.capabilities.iter().map(capability_to_msg).collect()),
        collector_errors: None,
        cpu: Some(Box::new(host_cpu_to_msg(&snap.cpu))),
        memory: Some(Box::new(host_memory_to_msg(&snap.memory))),
        disks: Some(snap.disks.iter().map(disk_to_msg).collect()),
        net_ifs: Some(snap.net_ifs.iter().map(netif_to_msg).collect()),
        psi: Some(snap.psi.iter().map(psi_to_msg).collect()),
        processes: Some(snap.processes.iter().map(process_to_msg).collect()),
    }
}

fn capability_to_msg(
    cap: &crate::capability::Capability,
) -> sysmon_snapshot_msg::typed::MetricCapabilityMsg {
    use sysmon_snapshot_msg::schema_aorta::sysmon as fb;
    use sysmon_snapshot_msg::typed as t;
    let configured_mode = match cap.configured_mode {
        crate::config::FamilyMode::On => fb::ConfiguredMode::ON,
        crate::config::FamilyMode::Off => fb::ConfiguredMode::OFF,
        crate::config::FamilyMode::Auto => fb::ConfiguredMode::AUTO,
    };
    let runtime_status = match cap.runtime_status {
        crate::capability::RuntimeStatus::Active => fb::RuntimeStatus::ACTIVE,
        crate::capability::RuntimeStatus::Unavailable => fb::RuntimeStatus::UNAVAILABLE,
        crate::capability::RuntimeStatus::Degraded => fb::RuntimeStatus::DEGRADED,
    };
    let perf_event_name = if cap.perf_event_name.is_empty() {
        None
    } else {
        Some(cap.perf_event_name.clone())
    };
    t::MetricCapabilityMsg {
        family: Some(cap.family.clone()),
        configured_mode: Some(configured_mode),
        runtime_status: Some(runtime_status),
        degraded_reason: cap.degraded_reason.clone(),
        perf_event_name,
        // Send `None` for zero-valued slots so the wire stays compact
        // — FlatBuffers default elision already does this, but
        // sending `Some(0)` would force the slot to be written.
        perf_raw_count: (cap.perf_raw_count != 0).then_some(cap.perf_raw_count),
        perf_time_enabled_ns: (cap.perf_time_enabled_ns != 0).then_some(cap.perf_time_enabled_ns),
        perf_time_running_ns: (cap.perf_time_running_ns != 0).then_some(cap.perf_time_running_ns),
    }
}

fn host_cpu_to_msg(
    cpu: &crate::collector::cpu::HostCpu,
) -> sysmon_snapshot_msg::typed::HostCpuMsg<'static> {
    sysmon_snapshot_msg::typed::HostCpuMsg {
        usage_percent_total: Some(cpu.usage_percent_total),
        // HostCpuMsg holds a borrow for the per-core slice. We Box the
        // whole struct in the parent message anyway, but the lifetime
        // forces us to either store the Vec elsewhere or use a static
        // empty slice. We keep the empty-slice path; per-core
        // values are still emitted as part of capabilities/JSON, just
        // not in the FB wire payload yet. A later change can switch to a
        // builder-callback model that owns the Vec inside the typed
        // closure (see `publish_typed`).
        usage_percent_per_core: None,
        context_switches_per_sec: Some(cpu.context_switches_per_sec),
        procs_running: Some(cpu.procs_running),
        procs_blocked: Some(cpu.procs_blocked),
    }
}

fn host_memory_to_msg(
    mem: &crate::collector::memory::HostMemory,
) -> sysmon_snapshot_msg::typed::HostMemoryMsg {
    sysmon_snapshot_msg::typed::HostMemoryMsg {
        total_bytes: Some(mem.total_bytes),
        available_bytes: Some(mem.available_bytes),
        free_bytes: Some(mem.free_bytes),
        buffers_bytes: Some(mem.buffers_bytes),
        cached_bytes: Some(mem.cached_bytes),
        swap_total_bytes: Some(mem.swap_total_bytes),
        swap_free_bytes: Some(mem.swap_free_bytes),
        pgfault_per_sec: Some(mem.pgfault_per_sec),
        pgmajfault_per_sec: Some(mem.pgmajfault_per_sec),
        oom_kill_count: Some(mem.oom_kill_count),
        huge_pages_total: Some(mem.huge_pages_total),
        huge_pages_free: Some(mem.huge_pages_free),
    }
}

fn disk_to_msg(d: &crate::collector::disk::DiskStat) -> sysmon_snapshot_msg::typed::DiskStatMsg {
    sysmon_snapshot_msg::typed::DiskStatMsg {
        device: Some(d.device.clone()),
        read_bytes_per_sec: Some(d.read_bytes_per_sec),
        write_bytes_per_sec: Some(d.write_bytes_per_sec),
        read_ios_per_sec: Some(d.read_ios_per_sec),
        write_ios_per_sec: Some(d.write_ios_per_sec),
        discard_bytes_per_sec: Some(d.discard_bytes_per_sec),
        discard_ios_per_sec: Some(d.discard_ios_per_sec),
        inflight_reads: Some(d.inflight_reads),
        inflight_writes: Some(d.inflight_writes),
        await_ms: Some(d.await_ms),
        util_percent: Some(d.util_percent),
        avg_queue_length: Some(d.avg_queue_length),
        logical_block_size: Some(d.logical_block_size),
    }
}

fn netif_to_msg(
    n: &crate::collector::network::NetIfStat,
) -> sysmon_snapshot_msg::typed::NetIfStatMsg {
    sysmon_snapshot_msg::typed::NetIfStatMsg {
        ifname: Some(n.ifname.clone()),
        rx_bytes_per_sec: Some(n.rx_bytes_per_sec),
        rx_packets_per_sec: Some(n.rx_packets_per_sec),
        tx_bytes_per_sec: Some(n.tx_bytes_per_sec),
        tx_packets_per_sec: Some(n.tx_packets_per_sec),
        rx_errors: Some(n.rx_errors),
        rx_drop: Some(n.rx_drop),
        tx_errors: Some(n.tx_errors),
        tx_drop: Some(n.tx_drop),
        oper_state: Some(n.oper_state.clone()),
        mtu: Some(n.mtu),
        speed_mbps: Some(n.speed_mbps),
    }
}

fn psi_to_msg(p: &crate::collector::psi::PsiLine) -> sysmon_snapshot_msg::typed::PsiLineMsg {
    sysmon_snapshot_msg::typed::PsiLineMsg {
        resource: Some(p.resource.clone()),
        line: Some(p.line.clone()),
        avg10: Some(p.avg10),
        avg60: Some(p.avg60),
        avg300: Some(p.avg300),
        total_us: Some(p.total_us),
        total_delta_us: Some(p.total_delta_us),
        stall_ratio: Some(p.stall_ratio),
    }
}

fn process_to_msg(
    p: &crate::collector::process::ProcessStat,
) -> sysmon_snapshot_msg::typed::ProcessStatMsg {
    sysmon_snapshot_msg::typed::ProcessStatMsg {
        pid: Some(p.pid),
        comm: Some(p.comm.clone()),
        cmdline: Some(p.cmdline.clone()),
        exe: Some(p.exe.clone()),
        cgroup_path: Some(p.cgroup_path.clone()),
        state: Some(p.state.clone()),
        starttime_ticks: Some(p.starttime_ticks),
        thread_count: Some(p.thread_count),
        cpu_user_us_delta: Some(p.cpu_user_us_delta),
        cpu_kernel_us_delta: Some(p.cpu_kernel_us_delta),
        cpu_children_us_delta: Some(p.cpu_children_us_delta),
        cpu_percent_host: Some(p.cpu_percent_host),
        cpu_percent_per_core: Some(p.cpu_percent_per_core),
        vss_bytes: Some(p.vss_bytes),
        rss_bytes: Some(p.rss_bytes),
        rss_hwm_bytes: Some(p.rss_hwm_bytes),
        minor_faults_per_sec: Some(p.minor_faults_per_sec),
        major_faults_per_sec: Some(p.major_faults_per_sec),
        voluntary_ctxt_switches_per_sec: Some(p.voluntary_ctxt_switches_per_sec),
        nonvoluntary_ctxt_switches_per_sec: Some(p.nonvoluntary_ctxt_switches_per_sec),
        read_bytes_per_sec: Some(p.read_bytes_per_sec),
        write_bytes_per_sec: Some(p.write_bytes_per_sec),
        cancelled_write_bytes: Some(p.cancelled_write_bytes),
        fd_count: Some(p.fd_count),
        oom_score: Some(p.oom_score),
        oom_score_adj: Some(p.oom_score_adj),
        exit_marker: Some(p.exit_marker),
    }
}

/// Convert one [`Alert`] into the codegen-generated
/// `SysmonAlertMsg`. `aorta_header` is injected by the SDK.
fn alert_to_msg(snap: &Snapshot, alert: &Alert) -> sysmon_alert_msg::typed::SysmonAlertMsg {
    use sysmon_alert_msg::schema_aorta::sysmon as fb;
    use sysmon_alert_msg::typed as t;

    let severity = match alert.severity {
        AlertSeverity::Info => fb::AlertSeverity::INFO,
        AlertSeverity::Warn => fb::AlertSeverity::WARN,
        AlertSeverity::Error => fb::AlertSeverity::ERROR,
    };
    let (kind, payload) = match &alert.payload {
        AlertPayload::Psi(p) => (
            fb::AlertKind::PSI_STALL,
            t::AlertPayloadMsg::AlertPsi(Box::new(t::AlertPsiMsg {
                resource: Some(p.resource.clone()),
                line: Some(p.line.clone()),
                stall_ratio: Some(p.stall_ratio),
                threshold: Some(p.threshold),
                window_ms: Some(p.window_ms),
            })),
        ),
        AlertPayload::ProcessTop(p) => (
            fb::AlertKind::PROCESS_TOP,
            t::AlertPayloadMsg::AlertProcessTop(Box::new(t::AlertProcessTopMsg {
                pid: Some(p.pid),
                comm: Some(p.comm.clone()),
                metric: Some(p.metric.clone()),
                value: Some(p.value),
                threshold: Some(p.threshold),
                top_k: Some(p.top_k),
            })),
        ),
        AlertPayload::Collector(c) => (
            fb::AlertKind::COLLECTOR_ERROR,
            t::AlertPayloadMsg::AlertCollector(Box::new(t::AlertCollectorMsg {
                family: Some(c.family.clone()),
                detail: Some(c.detail.clone()),
            })),
        ),
    };

    t::SysmonAlertMsg {
        schema_version: Some(1),
        host_name: Some(snap.host_name.clone()),
        host_id: Some(snap.host_id.clone()),
        observed_at_ns: Some(alert.observed_at_ns),
        monotonic_ns: Some(alert.monotonic_ns),
        kind: Some(kind),
        severity: Some(severity),
        payload: Some(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::HostIdentity;

    #[test]
    fn resolve_topic_substitutes_normalized_keys() {
        let identity = HostIdentity::resolve(Some("dev-host.local"), Some("dev-host.local"));
        assert_eq!(
            resolve_topic_suffix("sysmon/{host_name}/snapshot", &identity),
            "sysmon/dev_host_local/snapshot",
        );
        assert_eq!(
            resolve_topic_suffix("sysmon/{host_id}/alerts", &identity),
            "sysmon/dev_host_local/alerts",
        );
    }

    #[test]
    fn resolve_topic_no_placeholders_passes_through() {
        let identity = HostIdentity::resolve(Some("ubuntu"), Some("ubuntu"));
        assert_eq!(
            resolve_topic_suffix("sysmon/fixed/snapshot", &identity),
            "sysmon/fixed/snapshot"
        );
    }
}
