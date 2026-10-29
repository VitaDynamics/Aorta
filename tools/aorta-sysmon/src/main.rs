//! `aorta-sysmon` binary entry point.
//!
//! `snapshot` takes one sample and prints JSON. `daemon` publishes over
//! the Aorta SDK and needs the `sdk-publish` feature; without it the
//! subcommand bails out with a clear error.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

use aorta_sysmon::config::{Config, FamilyMode};
use aorta_sysmon::identity::HostIdentity;
use aorta_sysmon::procfs_root::ProcfsRoot;
use aorta_sysmon::snapshot;

#[derive(Debug, Parser)]
#[command(name = "aorta-sysmon", about = "Aorta system monitor", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Take a single snapshot and print JSON to stdout.
    Snapshot(SnapshotArgs),
    /// Run as a daemon that publishes snapshots (requires the `sdk-publish` feature).
    Daemon(DaemonArgs),
}

#[derive(Debug, Args)]
struct SnapshotArgs {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long)]
    host_name: Option<String>,
    #[arg(long)]
    host_id: Option<String>,
    /// Default 1000 ms for snapshot mode (daemon defaults to 5000 ms).
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[arg(long, value_name = "MODE")]
    with_cpu: Option<FamilyMode>,
    #[arg(long, value_name = "MODE")]
    with_memory: Option<FamilyMode>,
    #[arg(long, value_name = "MODE")]
    with_disk: Option<FamilyMode>,
    #[arg(long, value_name = "MODE")]
    with_network: Option<FamilyMode>,
    #[arg(long, value_name = "MODE")]
    with_psi: Option<FamilyMode>,
    #[arg(long, value_name = "MODE")]
    with_process: Option<FamilyMode>,
    #[arg(long = "include-pid", value_name = "PID")]
    include_pids: Vec<i32>,
    /// Cap on detail rows. Default lives in `Config::max_listed` so that
    /// file > env precedence is honored when the flag is omitted.
    #[arg(long)]
    max_processes: Option<u32>,
    /// Pretty-print the JSON output. Format is **not stable**.
    #[arg(long)]
    pretty: bool,
    /// Take one sample and exit (default; flag kept for CLI parity).
    #[arg(long)]
    #[allow(dead_code)]
    once: bool,
    /// Read procfs from a fixture directory instead of `/proc` and `/sys`.
    /// Convenience for integration tests.
    #[arg(long, value_name = "DIR", hide = true)]
    procfs_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct DaemonArgs {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    #[arg(long)]
    host_name: Option<String>,
    #[arg(long)]
    host_id: Option<String>,
    #[arg(long)]
    topic_suffix: Option<String>,
    #[arg(long)]
    alert_topic_suffix: Option<String>,
    #[arg(long)]
    interval_ms: Option<u64>,
    /// Exit after N publishes (for `--count 1` smoke tests).
    #[arg(long)]
    count: Option<u64>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Snapshot(args) => run_snapshot(args),
        Command::Daemon(args) => run_daemon(args),
    }
}

#[cfg(feature = "sdk-publish")]
fn run_daemon(args: DaemonArgs) -> Result<()> {
    let mut cfg = Config::load(args.config.as_deref()).context("load config")?;
    if let Some(v) = args.topic_suffix {
        cfg.topic_suffix = v;
    }
    if let Some(v) = args.alert_topic_suffix {
        cfg.alert_topic_suffix = v;
    }
    if let Some(v) = args.interval_ms {
        cfg.interval_ms = v;
    }

    let identity = HostIdentity::resolve(
        args.host_name.as_deref().or(Some(cfg.host_name.as_str())),
        args.host_id.as_deref().or(Some(cfg.host_id.as_str())),
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("create tokio runtime")?;
    rt.block_on(aorta_sysmon::publisher::run_daemon(
        cfg, identity, args.count,
    ))
}

#[cfg(not(feature = "sdk-publish"))]
fn run_daemon(_args: DaemonArgs) -> Result<()> {
    anyhow::bail!(
        "daemon subcommand requires the `sdk-publish` feature; build with \
         Bazel (`bazel build //tools/aorta-sysmon:aorta-sysmon`) or with \
         `cargo build --features sdk-publish` once the codegen wrapper is \
         resolvable to cargo"
    );
}

fn run_snapshot(args: SnapshotArgs) -> Result<()> {
    // File < env < CLI; Config::load already does file + env.
    let mut cfg = Config::load(args.config.as_deref()).context("load config")?;

    if let Some(v) = args.with_cpu {
        cfg.metrics.cpu = v;
    }
    if let Some(v) = args.with_memory {
        cfg.metrics.memory = v;
    }
    if let Some(v) = args.with_disk {
        cfg.metrics.disk = v;
    }
    if let Some(v) = args.with_network {
        cfg.metrics.network = v;
    }
    if let Some(v) = args.with_psi {
        cfg.metrics.psi = v;
    }
    if let Some(v) = args.with_process {
        cfg.metrics.process = v;
    }
    if !args.include_pids.is_empty() {
        cfg.process.include_pids.extend(args.include_pids);
    }
    if let Some(v) = args.max_processes {
        cfg.process.max_listed = v;
    }

    let identity = HostIdentity::resolve(
        args.host_name.as_deref().or(Some(cfg.host_name.as_str())),
        args.host_id.as_deref().or(Some(cfg.host_id.as_str())),
    );

    let root = match args.procfs_root {
        Some(p) => ProcfsRoot::fixture(p),
        None => ProcfsRoot::system(),
    };

    // Open perf counters BEFORE the collector's interval-gap
    // sleep so the cumulative reads we splice in below cover the same
    // window as the procfs deltas. On non-Linux builds `PerfState`
    // falls through to a stub that emits "Unavailable" rows — see
    // `collector/perf.rs`.
    let mut perf_state = aorta_sysmon::collector::perf::PerfState::try_open(&root);
    let mut snap = snapshot::collect_once(&cfg, &identity, &root, args.interval_ms)
        .context("collect snapshot")?;
    snap.capabilities.retain(|c| c.family != "perf");
    snap.capabilities
        .extend(perf_state.render_capabilities(cfg.metrics.perf));

    let body = snapshot::render_json(&snap, args.pretty)?;
    println!("{}", body);
    Ok(())
}
