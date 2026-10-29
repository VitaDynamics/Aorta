//! `aorta-play` — MCAP playback for Aorta.
//!
//! Authoritative design: `spec/Aorta_MCAP_Player_Proposal.md` (v10).
//!
//! Layering (§4): a pure L3 tool over the high-level `aorta_core` Rust
//! API — never `aorta_core::ffi::*` — with only the narrowly-scoped Core
//! timeout-classifier export required by schema preflight.
//!
//! **Pipeline.** `pass 0` private snapshot ([`snapshot`]) → `pass 1` planning
//! ([`plan`], over the [`reader`] adapter) → the live engine ([`live`]): the
//! §6.3 queryable gate, the §6.9 `AORTA_GROUP` ownership, the Node and
//! publisher map under managed ownership, the schema preflight
//! ([`preflight`]), the §6.4 matching barrier, `pass 2` timed replay
//! ([`replay`]) and the §6.7 bounded teardown.
//!
//! **Process contract.** The library installs no signal handlers and never
//! calls `process::exit` (§6.6) — [`signal`] is a *wrapper* helper. It
//! does own `AORTA_GROUP` process-wide while its Node exists (§6.9),
//! which is also why concurrent player runs in one process are unsupported;
//! and after a teardown-deadline expiry the process is Aorta-tainted
//! ([`group::is_tainted`]) and no further run is accepted.

#[cfg(test)]
mod fixture;
mod group;
mod live;
#[cfg(test)]
mod live_tests;
pub mod plan;
mod preflight;
pub mod reader;
mod replay;
pub mod signal;
mod snapshot;
pub mod timeline;
mod validate;

use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub use group::is_tainted;

pub use validate::{
    ValidatedLimits, DEFAULT_BUFFER_BYTES, DEFAULT_MATCH_TIMEOUT_SECS, DEFAULT_MAX_CHUNK_BYTES,
    DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_PREFLIGHT_TIMEOUT_SECS, DEFAULT_RATE, MAX_RATE,
};

/// Multi-typed-schema policy for a replayed topic (MCAP Player Proposal §5,
/// §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaPolicy {
    /// A topic carrying more than one typed schema is a hard error.
    #[default]
    Strict,
    /// Register only the newest channel's schema and replay all typed bytes
    /// through it (opt-in rolling-upgrade semantics).
    Latest,
}

impl std::str::FromStr for SchemaPolicy {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self> {
        match raw {
            "strict" => Ok(Self::Strict),
            "latest" => Ok(Self::Latest),
            other => Err(anyhow!(
                "invalid --schema-policy '{}' (expected strict|latest)",
                other
            )),
        }
    }
}

impl SchemaPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Latest => "latest",
        }
    }
}

/// Log verbosity, mirroring the recorder's `--log-level` surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl std::str::FromStr for LogLevel {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self> {
        match raw {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => Err(anyhow!(
                "invalid --log-level '{}' (expected trace|debug|info|warn|error)",
                other
            )),
        }
    }
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Install the player log subscriber once. Embedders may already own the
/// process-wide subscriber, so a second installation is intentionally ignored.
pub fn init_logging(level: LogLevel) {
    let max_level = match level {
        LogLevel::Trace => tracing::Level::TRACE,
        LogLevel::Debug => tracing::Level::DEBUG,
        LogLevel::Info => tracing::Level::INFO,
        LogLevel::Warn => tracing::Level::WARN,
        LogLevel::Error => tracing::Level::ERROR,
    };
    let _ = tracing_subscriber::fmt()
        .with_max_level(max_level)
        .with_target(false)
        .try_init();
}

/// The full MCAP Player Proposal §5 CLI surface, in library form. `aorta-play`
/// and `aorta-cli play` both build one of these and hand it to [`run`].
#[derive(Debug, Clone, PartialEq)]
pub struct PlayerConfig {
    /// One or more `.mcap` files and/or directories holding a rotated segment
    /// set (non-recursive `*.mcap` glob).
    pub inputs: Vec<PathBuf>,
    /// Playback rate multiplier (`0 < rate <= 1e6`).
    pub rate: f64,
    /// Skip the first N seconds of the effective timeline.
    pub start_secs: f64,
    /// Stop after N seconds of effective recorded time.
    pub duration_secs: Option<f64>,
    /// Only replay these topics (topic name, key suffix, or full key).
    pub topics: Vec<String>,
    /// Skip these topics; applied after `topics`.
    pub exclude: Vec<String>,
    /// Loop forever until cancelled.
    pub loop_playback: bool,
    /// Matched-subscriber barrier bound in seconds (`0` disables).
    pub match_timeout_secs: f64,
    /// Per-topic schema-preflight get+drain deadline in seconds (`> 0`).
    pub preflight_timeout_secs: f64,
    /// Multi-typed-schema policy.
    pub schema_policy: SchemaPolicy,
    /// Play a rotated set with missing/duplicate segment indices.
    pub allow_gaps: bool,
    /// Reject larger message payloads (must be `<= buffer_bytes`).
    pub max_message_bytes: u64,
    /// Reject larger declared chunk / generic record sizes.
    pub max_chunk_bytes: u64,
    /// In-flight byte budget of the reader queue.
    pub buffer_bytes: u64,
    /// Replay into this group instead of the recorded one (MCAP Player Proposal
    /// §6.9).
    pub group: Option<String>,
    /// Explicit Zenoh config file.
    pub zenoh_config: Option<PathBuf>,
    /// Force the resolved Zenoh session into client mode by default.
    pub client_mode_override: bool,
    /// Log verbosity.
    pub log_level: LogLevel,
}

impl Default for PlayerConfig {
    fn default() -> Self {
        Self {
            inputs: Vec::new(),
            rate: DEFAULT_RATE,
            start_secs: 0.0,
            duration_secs: None,
            topics: Vec::new(),
            exclude: Vec::new(),
            loop_playback: false,
            match_timeout_secs: DEFAULT_MATCH_TIMEOUT_SECS,
            preflight_timeout_secs: DEFAULT_PREFLIGHT_TIMEOUT_SECS,
            schema_policy: SchemaPolicy::Strict,
            allow_gaps: false,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_chunk_bytes: DEFAULT_MAX_CHUNK_BYTES,
            buffer_bytes: DEFAULT_BUFFER_BYTES,
            group: None,
            zenoh_config: None,
            client_mode_override: true,
            log_level: LogLevel::Info,
        }
    }
}

/// Non-pub channels skipped by v1 (MCAP Player Proposal §6.10).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkippedChannels {
    pub total: u64,
    pub ctx: u64,
    pub sys: u64,
}

/// Truncation / recovery report (MCAP Player Proposal §5, §6.1).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TruncationReport {
    /// Inputs that were reopened with `IgnoreEndMagic` and replayed from their
    /// verified prefix.
    pub truncated_inputs: Vec<PathBuf>,
    /// Tail chunks discarded because they failed verification — always exact.
    pub discarded_tail_chunks: u64,
    /// Tail messages discarded. `None` = `unknown`: a torn prevalidated chunk
    /// yields no messages to count and its message index was never written.
    pub discarded_tail_messages: Option<u64>,
}

/// Teardown report (MCAP Player Proposal §5, §6.6, §6.7). Populated when
/// teardown does not complete or a resource close returns an error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TeardownReport {
    /// True when the absolute teardown deadline expired.
    pub deadline_expired: bool,
    /// The stage that did not complete (e.g. `node_close`).
    pub stuck_stage: Option<String>,
    /// Residual counts per stage (detached threads/tasks, un-rolled-back
    /// schema puts, ...).
    pub residual: BTreeMap<String, u64>,
    /// A non-timeout teardown error, such as a failed `Node::close()`.
    pub error: Option<String>,
}

/// End-of-run summary (MCAP Player Proposal §5). Also printed on Ctrl-C.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunSummary {
    /// Messages published, i.e. *enqueued* (MCAP Player Proposal §6.4).
    pub published_messages: u64,
    /// Per replayed channel key → published count.
    pub published_per_channel: BTreeMap<String, u64>,
    /// Messages published later than their target + late threshold.
    pub late_messages: u64,
    /// Largest observed lateness.
    pub max_lateness: Duration,
    /// Messages whose `log_time` went backwards and was clamped (MCAP Player
    /// Proposal §6.5).
    pub clamped_messages: u64,
    /// Failed publish calls (counted, rate-limit-logged, non-fatal).
    pub publish_errors: u64,
    /// Planned topics with no matched subscriber at the barrier deadline.
    pub unmatched_topics: Vec<String>,
    /// Non-`pub/**` channels excluded by v1.
    pub skipped_channels: SkippedChannels,
    /// Torn-tail recovery accounting.
    pub truncation: TruncationReport,
    /// Teardown accounting.
    pub teardown: TeardownReport,
    /// Wall-clock duration of the whole run.
    pub wall_duration: Duration,
}

/// How the run ended (MCAP Player Proposal §6.6). Precedence, highest first:
/// `Err` > `Interrupted` > `CompletedWithTeardownTimeout` >
/// `CompletedWithPublishErrors` > `Completed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    Completed,
    CompletedWithPublishErrors,
    /// Replay finished; Node teardown did not complete within its deadline.
    CompletedWithTeardownTimeout,
    Interrupted,
}

impl Termination {
    /// Wrapper exit-code mapping (MCAP Player Proposal §6.6). `Err(_)` maps to
    /// 1 at the wrapper, which is why it has no variant here.
    pub fn exit_code(self) -> u8 {
        match self {
            Self::Completed => 0,
            Self::CompletedWithPublishErrors | Self::CompletedWithTeardownTimeout => 1,
            Self::Interrupted => 130,
        }
    }
}

/// The result of a completed (or interrupted) run.
#[derive(Debug, Clone, PartialEq)]
pub struct RunOutcome {
    pub summary: RunSummary,
    pub termination: Termination,
}

/// Format the wrapper-facing end-of-run summary. The wrappers own the output
/// stream so errors can print the same data to stderr.
pub fn format_summary(summary: &RunSummary) -> String {
    use std::fmt::Write as _;
    let mut output = String::new();
    writeln!(
        output,
        "Play summary: published={} late={} (max_lateness={:?}) clamped={} publish_errors={} wall={:?}",
        summary.published_messages,
        summary.late_messages,
        summary.max_lateness,
        summary.clamped_messages,
        summary.publish_errors,
        summary.wall_duration
    )
    .expect("writing a String cannot fail");
    for (channel, count) in &summary.published_per_channel {
        writeln!(output, "  channel {}: {}", channel, count).expect("writing a String cannot fail");
    }
    if !summary.unmatched_topics.is_empty() {
        writeln!(
            output,
            "  unmatched topics: {}",
            summary.unmatched_topics.join(", ")
        )
        .expect("writing a String cannot fail");
    }
    if summary.skipped_channels.total > 0 {
        writeln!(
            output,
            "  skipped {} non-pub channels (ctx: {}, sys: {}) — v1 replays pub/** only",
            summary.skipped_channels.total,
            summary.skipped_channels.ctx,
            summary.skipped_channels.sys
        )
        .expect("writing a String cannot fail");
    }
    if !summary.truncation.truncated_inputs.is_empty() {
        let discarded_messages = match summary.truncation.discarded_tail_messages {
            Some(count) => count.to_string(),
            None => "unknown".to_string(),
        };
        writeln!(
            output,
            "  truncated inputs: {} (discarded tail chunks: {}, tail messages: {})",
            summary
                .truncation
                .truncated_inputs
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", "),
            summary.truncation.discarded_tail_chunks,
            discarded_messages
        )
        .expect("writing a String cannot fail");
    }
    if summary.teardown.deadline_expired || summary.teardown.error.is_some() {
        let label = if summary.teardown.deadline_expired {
            "teardown deadline expired at stage"
        } else {
            "teardown failed at stage"
        };
        writeln!(
            output,
            "  {} {}",
            label,
            summary.teardown.stuck_stage.as_deref().unwrap_or("unknown")
        )
        .expect("writing a String cannot fail");
        for (stage, count) in &summary.teardown.residual {
            writeln!(output, "    residual {}: {}", stage, count)
                .expect("writing a String cannot fail");
        }
        if let Some(error) = &summary.teardown.error {
            writeln!(output, "    error: {error}").expect("writing a String cannot fail");
        }
    }
    output
}

/// Format a wrapper-owned hard error with the summary first, as MCAP Player
/// Proposal §6.6 requires: the summary-so-far reaches the wrappers on `Err`.
pub fn format_player_error(prefix: &str, error: &PlayerError) -> String {
    format!(
        "{}{}: {:#}\n",
        format_summary(error.summary()),
        prefix,
        error
    )
}

/// A hard run error together with the complete summary accumulated before it.
/// Wrappers print both parts before mapping the error to exit code 1.
#[derive(Debug)]
pub struct PlayerError {
    pub summary: RunSummary,
    source: anyhow::Error,
}

impl PlayerError {
    pub fn summary(&self) -> &RunSummary {
        &self.summary
    }
}

impl std::fmt::Display for PlayerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for PlayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

enum ManagedEvent<T> {
    Completed(Result<T>),
    Panicked(String),
    Vanished,
}

struct ManagedThread<T> {
    stop: Arc<AtomicBool>,
    terminal: tokio::sync::oneshot::Receiver<ManagedEvent<T>>,
}

enum ManagedWait<T> {
    Finished(ManagedEvent<T>),
    Cancelled(ManagedThread<T>),
}

const PASS_JOIN_DEADLINE: Duration = Duration::from_millis(100);

#[cfg(feature = "fault-injection")]
pub(crate) const TEST_PASS0_ERROR_AFTER_CANCEL_ENV: &str =
    "AORTA_PLAY_TEST_PASS0_ERROR_AFTER_CANCEL";
#[cfg(feature = "fault-injection")]
pub(crate) const TEST_PASS1_ERROR_AFTER_CANCEL_ENV: &str =
    "AORTA_PLAY_TEST_PASS1_ERROR_AFTER_CANCEL";
#[cfg(feature = "fault-injection")]
pub(crate) static TEST_PASS0_ERROR_ARMED: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "fault-injection")]
pub(crate) static TEST_PASS1_ERROR_ARMED: AtomicBool = AtomicBool::new(false);

fn spawn_managed_thread<T, F>(name: &str, work: F) -> Result<ManagedThread<T>>
where
    T: Send + 'static,
    F: FnOnce(Arc<AtomicBool>) -> Result<T> + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let event = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                work(thread_stop)
            })) {
                Ok(result) => ManagedEvent::Completed(result),
                Err(payload) => ManagedEvent::Panicked(panic_message(payload)),
            };
            let _ = terminal_tx.send(event);
        })
        .map_err(|err| anyhow!("cannot spawn {name} managed thread: {err}"))?;
    Ok(ManagedThread {
        stop,
        terminal: terminal_rx,
    })
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_string())
}

async fn wait_managed<T>(worker: ManagedThread<T>, token: &CancellationToken) -> ManagedWait<T> {
    let ManagedThread { stop, terminal } = worker;
    let mut terminal = terminal;
    tokio::select! {
        biased;
        _ = token.cancelled() => ManagedWait::Cancelled(ManagedThread {
            stop,
            terminal,
        }),
        event = &mut terminal => ManagedWait::Finished(
            event.unwrap_or(ManagedEvent::Vanished)
        ),
    }
}

async fn stop_and_join_managed<T>(
    worker: ManagedThread<T>,
    deadline: Duration,
) -> Option<ManagedEvent<T>> {
    worker.stop.store(true, Ordering::SeqCst);
    match tokio::time::timeout(deadline, worker.terminal).await {
        Ok(Ok(event)) => Some(event),
        Ok(Err(_)) => Some(ManagedEvent::Vanished),
        Err(_) => None,
    }
}

fn managed_failure(stage: &str, event: ManagedEvent<impl Send + 'static>) -> anyhow::Error {
    match event {
        ManagedEvent::Completed(Ok(_)) => {
            anyhow!("{stage} managed thread completed without a result")
        }
        ManagedEvent::Completed(Err(err)) => err,
        ManagedEvent::Panicked(message) => anyhow!("{stage} managed thread panicked: {message}"),
        ManagedEvent::Vanished => {
            anyhow!("{stage} managed thread vanished before its terminal event")
        }
    }
}

/// Live counters behind the wrapper's progress line (MCAP Player Proposal §5).
/// The library only publishes numbers; the 4 Hz TTY formatting is a wrapper
/// concern.
#[derive(Debug, Default)]
pub struct ProgressHandle {
    published: AtomicU64,
    elapsed_ns: AtomicU64,
    span_ns: AtomicU64,
    /// Rate × 1e6, so the whole handle stays lock-free.
    rate_micros: AtomicU64,
    active: AtomicBool,
}

/// A point-in-time read of [`ProgressHandle`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Progress {
    /// True while pass 2 is running.
    pub active: bool,
    pub published: u64,
    /// Recorded-time position inside the selection.
    pub elapsed: Duration,
    /// Recorded-time span of the selection.
    pub span: Duration,
    pub rate: f64,
}

impl ProgressHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Progress {
        Progress {
            active: self.active.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            elapsed: Duration::from_nanos(self.elapsed_ns.load(Ordering::Relaxed)),
            span: Duration::from_nanos(self.span_ns.load(Ordering::Relaxed)),
            rate: self.rate_micros.load(Ordering::Relaxed) as f64 / 1e6,
        }
    }

    pub(crate) fn begin(&self, rate: f64, span_ns: u64) {
        self.rate_micros
            .store((rate * 1e6) as u64, Ordering::Relaxed);
        self.span_ns.store(span_ns, Ordering::Relaxed);
        self.active.store(true, Ordering::Relaxed);
    }

    pub(crate) fn observe(&self, published: u64, elapsed_ns: u64, span_ns: u64) {
        self.published.store(published, Ordering::Relaxed);
        self.elapsed_ns.store(elapsed_ns, Ordering::Relaxed);
        self.span_ns.store(span_ns, Ordering::Relaxed);
    }

    pub(crate) fn finish(&self) {
        self.active.store(false, Ordering::Relaxed);
    }
}

/// Replay `config` with no external cancellation handle.
pub async fn run(config: PlayerConfig) -> std::result::Result<RunOutcome, PlayerError> {
    run_with_shutdown(config, CancellationToken::new()).await
}

/// Replay `config`, observing `shutdown` at every player-controlled decision
/// point (MCAP Player Proposal §6.7). The library installs no signal handlers
/// and never calls `process::exit` — that is the wrapper's job (§6.6, §6.7).
///
/// # Process-wide effects (§6.9, §6.7)
///
/// - **`AORTA_GROUP` is owned by this call** from before the player's Node is
///   constructed (the Node reads the variable while constructing) until its
///   close completes. The prior value is saved and restored on every return
///   path where teardown completed — success, startup error, mid-replay error
///   and cancellation alike. Concurrent player runs, or concurrent Node
///   construction, in one process are therefore **unsupported**: save/restore
///   cannot make concurrent mutation of a process-global variable safe.
/// - **If the teardown deadline expires**, the variable is deliberately *not*
///   restored (live core tasks may still read it), the run reports
///   [`Termination::CompletedWithTeardownTimeout`] with a populated
///   [`TeardownReport`], and the process becomes *Aorta-tainted*
///   ([`is_tainted`]): further calls fail closed and an embedding caller must
///   exit.
pub async fn run_with_shutdown(
    config: PlayerConfig,
    shutdown: CancellationToken,
) -> std::result::Result<RunOutcome, PlayerError> {
    run_with_shutdown_reporting(config, shutdown, Arc::new(ProgressHandle::new())).await
}

/// [`run_with_shutdown`] plus the live counters the wrapper's progress line
/// reads (MCAP Player Proposal §5).
pub async fn run_with_shutdown_reporting(
    config: PlayerConfig,
    shutdown: CancellationToken,
    progress: Arc<ProgressHandle>,
) -> std::result::Result<RunOutcome, PlayerError> {
    let started = Instant::now();

    let mut summary = RunSummary::default();
    match run_with_shutdown_inner(config, shutdown, progress, started, &mut summary).await {
        Ok(mut outcome) => {
            outcome.summary.wall_duration = started.elapsed();
            Ok(outcome)
        }
        Err(source) => {
            summary.wall_duration = started.elapsed();
            Err(PlayerError { summary, source })
        }
    }
}

async fn run_with_shutdown_inner(
    config: PlayerConfig,
    shutdown: CancellationToken,
    progress: Arc<ProgressHandle>,
    started: Instant,
    summary: &mut RunSummary,
) -> Result<RunOutcome> {
    // MCAP Player Proposal §6.7/§6.9 — a teardown-deadline expiry taints the
    // process for good.
    group::ensure_not_tainted()?;

    // MCAP Player Proposal §6.12 — every numeric input is checked before
    // anything is opened.
    let limits = validate::validate(&config)?;
    // MCAP Player Proposal §5 — files and/or directories (non-recursive *.mcap
    // glob).
    let inputs = snapshot::collect_input_files(&config.inputs)?;

    tracing::info!(
        input_files = inputs.len(),
        rate = limits.rate,
        buffer_bytes = limits.buffer_bytes,
        max_chunk_bytes = limits.max_chunk_bytes,
        max_message_bytes = limits.max_message_bytes,
        "aorta-play: starting pass 0 (private snapshot)"
    );

    // Pass 0 (MCAP Player Proposal §6.1): a managed OS thread owns the copy,
    // while the async owner selects cancellation independently of a wedged
    // filesystem call.
    let temp_dir = snapshot_temp_dir();
    let copy_worker = spawn_managed_thread("aorta-play-pass0", {
        let inputs = inputs.clone();
        let temp_dir = temp_dir.clone();
        move |stop| {
            #[cfg(feature = "fault-injection")]
            if std::env::var(TEST_PASS0_ERROR_AFTER_CANCEL_ENV)
                .ok()
                .as_deref()
                == Some("1")
            {
                TEST_PASS0_ERROR_ARMED.store(true, Ordering::SeqCst);
                while !stop.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
                return Err(anyhow!("test pass-0 hard error after cancellation"));
            }
            snapshot::snapshot_inputs(&inputs, &temp_dir, &stop, &snapshot::StatvfsSpaceProbe)
        }
    })?;
    let snapshots = match wait_managed(copy_worker, &shutdown).await {
        ManagedWait::Finished(ManagedEvent::Completed(Ok(snapshots))) => {
            let _ = std::fs::remove_dir(&temp_dir);
            snapshots
        }
        ManagedWait::Finished(ManagedEvent::Completed(Err(err)))
            if snapshot::is_cancelled(&err) =>
        {
            tracing::warn!("aorta-play: cancelled during pass-0 snapshot");
            let _ = std::fs::remove_dir(&temp_dir);
            return Ok(interrupted_outcome(summary, started));
        }
        ManagedWait::Finished(event) => {
            let _ = std::fs::remove_dir(&temp_dir);
            return Err(managed_failure("pass-0", event));
        }
        ManagedWait::Cancelled(worker) => {
            let joined = stop_and_join_managed(worker, PASS_JOIN_DEADLINE).await;
            match joined {
                Some(ManagedEvent::Completed(Err(err))) if !snapshot::is_cancelled(&err) => {
                    let _ = std::fs::remove_dir(&temp_dir);
                    return Err(err);
                }
                Some(ManagedEvent::Panicked(message)) => {
                    let _ = std::fs::remove_dir(&temp_dir);
                    return Err(anyhow!("pass-0 managed thread panicked: {message}"));
                }
                Some(ManagedEvent::Vanished) => {
                    let _ = std::fs::remove_dir(&temp_dir);
                    return Err(anyhow!(
                        "pass-0 managed thread vanished before its terminal event"
                    ));
                }
                None => {
                    summary
                        .teardown
                        .residual
                        .insert("detached_pass0_threads".to_string(), 1);
                    tracing::warn!("aorta-play: detached pass-0 managed thread at join deadline");
                }
                Some(ManagedEvent::Completed(Ok(_))) | Some(ManagedEvent::Completed(Err(_))) => {}
            }
            let _ = std::fs::remove_dir(&temp_dir);
            return Ok(interrupted_outcome(summary, started));
        }
    };

    for snapshot in &snapshots {
        tracing::debug!(
            source = %snapshot.source.display(),
            bytes = snapshot.len,
            "aorta-play: snapshot ready"
        );
    }
    let snapshot_bytes: u64 = snapshots.iter().map(|s| s.len).sum();
    tracing::info!(
        snapshots = snapshots.len(),
        snapshot_bytes,
        "aorta-play: pass 0 complete"
    );

    // Pass 1 (MCAP Player Proposal §6.1–§6.3, §6.5): segment ordering,
    // verification, the effective timeline, the selection bits and the publish
    // plan. It has its own stop flag and terminal event so cancellation is
    // independently observable.
    let plan_options = plan::PlanOptions::from_config(&config, &limits);
    let plan_worker = spawn_managed_thread("aorta-play-pass1", {
        move |stop| {
            #[cfg(feature = "fault-injection")]
            if std::env::var(TEST_PASS1_ERROR_AFTER_CANCEL_ENV)
                .ok()
                .as_deref()
                == Some("1")
            {
                TEST_PASS1_ERROR_ARMED.store(true, Ordering::SeqCst);
                while !stop.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
                return Err(anyhow!("test pass-1 hard error after cancellation"));
            }
            let plan = {
                let inputs: Vec<plan::PlanInput<'_>> = snapshots
                    .iter()
                    .map(|snapshot| plan::PlanInput {
                        source: snapshot.source.clone(),
                        reader: &snapshot.file,
                    })
                    .collect();
                plan::build_plan(&inputs, &plan_options, &stop)
            };
            // The snapshots outlive pass 1: pass 2 re-walks the same fds.
            plan.map(|plan| (snapshots, plan))
        }
    })?;
    let plan = match wait_managed(plan_worker, &shutdown).await {
        ManagedWait::Finished(ManagedEvent::Completed(result)) => result,
        ManagedWait::Finished(event) => return Err(managed_failure("pass-1", event)),
        ManagedWait::Cancelled(worker) => {
            match stop_and_join_managed(worker, PASS_JOIN_DEADLINE).await {
                Some(ManagedEvent::Completed(Err(err))) if !snapshot::is_cancelled(&err) => {
                    return Err(err);
                }
                Some(ManagedEvent::Panicked(message)) => {
                    return Err(anyhow!("pass-1 managed thread panicked: {message}"));
                }
                Some(ManagedEvent::Vanished) => {
                    return Err(anyhow!(
                        "pass-1 managed thread vanished before its terminal event"
                    ));
                }
                None => {
                    summary
                        .teardown
                        .residual
                        .insert("detached_pass1_threads".to_string(), 1);
                    tracing::warn!("aorta-play: detached pass-1 managed thread at join deadline");
                }
                Some(ManagedEvent::Completed(Ok(_))) | Some(ManagedEvent::Completed(Err(_))) => {}
            }
            return Ok(interrupted_outcome(summary, started));
        }
    };

    let (snapshots, plan) = match plan {
        Ok(planned) => planned,
        Err(err) if snapshot::is_cancelled(&err) => {
            tracing::warn!("aorta-play: cancelled during pass-1 planning");
            return Ok(interrupted_outcome(summary, started));
        }
        Err(err) => return Err(err),
    };

    tracing::info!(
        segments = plan.segments.len(),
        topics = plan.topics.len(),
        messages = plan.timeline.total_messages,
        selected = plan.timeline.selected_messages,
        clamped = plan.timeline.clamped_messages,
        retained_bytes = plan.retained_bytes,
        "aorta-play: pass 1 complete"
    );

    *summary = RunSummary {
        clamped_messages: plan.timeline.clamped_messages,
        skipped_channels: plan.skipped_channels.clone(),
        truncation: plan.truncation.clone(),
        wall_duration: started.elapsed(),
        ..RunSummary::default()
    };

    // Empty final selection → zero-count `Completed`: no publishers, no
    // preflight, and `--loop` exits after this one pass (MCAP Player Proposal
    // §6.1, §6.5).
    if plan.is_empty_selection() {
        tracing::info!(
            "aorta-play: nothing to replay (window ∩ filters selected 0 of {} recorded \
             messages)",
            plan.timeline.total_messages
        );
        return Ok(RunOutcome {
            summary: summary.clone(),
            termination: Termination::Completed,
        });
    }

    // The live half (MCAP Player Proposal §6.3, §6.4, §6.5, §6.7, §6.9). The
    // summary is filled in as the run progresses so it survives every return
    // path, including errors.
    let termination = live::run_live(
        plan,
        snapshots,
        &config,
        &limits,
        &shutdown,
        summary,
        progress.as_ref(),
    )
    .await;
    summary.wall_duration = started.elapsed();

    match termination {
        Ok(termination) => Ok(RunOutcome {
            summary: summary.clone(),
            termination,
        }),
        Err(err) => {
            // MCAP Player Proposal §6.6 — mid-replay errors report the summary
            // so far before the error propagates (printing is the wrapper's
            // job; the library logs it).
            tracing::error!(
                published = summary.published_messages,
                publish_errors = summary.publish_errors,
                late = summary.late_messages,
                wall_ms = summary.wall_duration.as_millis() as u64,
                "aorta-play: run failed — summary so far"
            );
            Err(err)
        }
    }
}

fn interrupted_outcome(summary: &RunSummary, started: Instant) -> RunOutcome {
    let mut summary = summary.clone();
    summary.wall_duration = started.elapsed();
    RunOutcome {
        summary,
        termination: Termination::Interrupted,
    }
}

/// Per-run private temp directory for the MCAP Player Proposal §6.1 snapshots.
/// Concurrent player runs in one process are unsupported (§6.9), but the
/// counter keeps the name unique anyway so a stale directory can never be
/// reused.
fn snapshot_temp_dir() -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("aorta-play-{}-{}", std::process::id(), seq))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn exit_codes_follow_the_spec_mapping() {
        assert_eq!(Termination::Completed.exit_code(), 0);
        assert_eq!(Termination::CompletedWithPublishErrors.exit_code(), 1);
        assert_eq!(Termination::CompletedWithTeardownTimeout.exit_code(), 1);
        assert_eq!(Termination::Interrupted.exit_code(), 130);
    }

    #[test]
    fn player_error_is_send_sync_and_formats_summary_before_error() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PlayerError>();

        let error = PlayerError {
            summary: RunSummary::default(),
            source: anyhow!("hard failure"),
        };
        let output = format_player_error("aorta-play", &error);
        assert!(output.starts_with("Play summary:"), "got: {output}");
        assert!(
            output.ends_with("aorta-play: hard failure\n"),
            "got: {output}"
        );
    }

    #[tokio::test]
    async fn cancelled_managed_receiver_disappearance_is_a_hard_error() {
        let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
        drop(terminal_tx);
        let token = CancellationToken::new();
        token.cancel();
        let worker: ManagedThread<()> = ManagedThread {
            stop: Arc::new(AtomicBool::new(false)),
            terminal: terminal_rx,
        };
        let ManagedWait::Cancelled(worker) = wait_managed(worker, &token).await else {
            panic!("cancellation must own the managed worker");
        };
        let event = stop_and_join_managed(worker, Duration::from_millis(100)).await;
        assert!(
            matches!(event, Some(ManagedEvent::Vanished)),
            "a closed terminal channel must be a vanished event"
        );
        let error = managed_failure("test", event.expect("vanished event"));
        assert!(error.to_string().contains("vanished"), "got: {error}");
    }

    #[test]
    fn schema_policy_parses_only_the_documented_values() {
        assert_eq!(
            SchemaPolicy::from_str("strict").unwrap(),
            SchemaPolicy::Strict
        );
        assert_eq!(
            SchemaPolicy::from_str("latest").unwrap(),
            SchemaPolicy::Latest
        );
        assert!(SchemaPolicy::from_str("newest").is_err());
        assert_eq!(SchemaPolicy::default().as_str(), "strict");
    }

    #[test]
    fn log_level_parses_only_the_documented_values() {
        assert_eq!(LogLevel::from_str("warn").unwrap(), LogLevel::Warn);
        assert!(LogLevel::from_str("verbose").is_err());
        assert_eq!(LogLevel::default().as_str(), "info");
    }

    #[test]
    fn player_logging_initialization_is_idempotent() {
        init_logging(LogLevel::Debug);
        init_logging(LogLevel::Info);
    }

    #[test]
    fn default_config_matches_the_documented_cli_defaults() {
        let config = PlayerConfig::default();
        assert_eq!(config.rate, 1.0);
        assert!(config.client_mode_override);
        assert_eq!(config.match_timeout_secs, 5.0);
        assert_eq!(config.preflight_timeout_secs, 5.0);
        assert_eq!(config.max_message_bytes, 256 * 1024 * 1024);
        assert_eq!(config.max_chunk_bytes, 512 * 1024 * 1024);
        assert_eq!(config.buffer_bytes, 256 * 1024 * 1024);
        assert!(!config.loop_playback);
        assert!(!config.allow_gaps);
    }

    #[tokio::test]
    async fn run_rejects_an_invalid_config_before_touching_the_filesystem() {
        let config = PlayerConfig {
            rate: 0.0,
            inputs: vec![PathBuf::from("/nonexistent/aorta-play.mcap")],
            ..PlayerConfig::default()
        };
        let err = run(config).await.unwrap_err();
        assert!(err.to_string().contains("--rate"), "got: {err}");
    }

    /// Self-cleaning scratch directory holding one real MCAP fixture file.
    struct RunFixture {
        dir: PathBuf,
        input: PathBuf,
    }

    impl RunFixture {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "aorta-play-run-{}-{}-{:?}",
                std::process::id(),
                tag,
                std::thread::current().id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("scratch dir");
            let input = dir.join("a.mcap");
            std::fs::write(
                &input,
                fixture::simple_file(fixture::FixtureOptions::default()),
            )
            .expect("fixture");
            Self { dir, input }
        }
    }

    impl Drop for RunFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn run_refuses_to_start_in_a_tainted_process() {
        // MCAP Player Proposal §6.7: after a teardown-deadline expiry no
        // further player run is supported in the process, and the error says so
        // before anything is opened.
        let _lock = group::EnvLock::acquire();
        let fixture = RunFixture::new("tainted");
        group::mark_tainted();
        let config = PlayerConfig {
            inputs: vec![fixture.input.clone()],
            ..PlayerConfig::default()
        };
        let err = run(config).await.unwrap_err();
        group::clear_taint_for_test();
        assert!(err.to_string().contains("Aorta-tainted"), "got: {err}");
    }

    #[tokio::test]
    async fn run_rejects_a_snapshot_that_is_not_an_mcap_file() {
        let dir = std::env::temp_dir().join(format!("aorta-play-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let input = dir.join("a.mcap");
        std::fs::write(&input, b"not a real mcap at all").expect("fixture");

        let config = PlayerConfig {
            inputs: vec![input],
            ..PlayerConfig::default()
        };
        let err = run(config).await.unwrap_err();
        assert!(err.to_string().contains("not an MCAP"), "got: {err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_completes_with_zero_counts_when_the_selection_is_empty() {
        // MCAP Player Proposal §6.1/§6.5: empty final selection → zero-count
        // `Completed`, no publishers, no preflight — and `--loop` exits after
        // one pass rather than spinning forever on an empty plan.
        let fixture = RunFixture::new("empty");
        let config = PlayerConfig {
            inputs: vec![fixture.input.clone()],
            topics: vec!["pub/nothing/here".to_string()],
            loop_playback: true,
            ..PlayerConfig::default()
        };
        let outcome = run(config)
            .await
            .expect("an empty selection is not an error");
        assert_eq!(outcome.termination, Termination::Completed);
        assert_eq!(outcome.termination.exit_code(), 0);
        assert_eq!(outcome.summary.published_messages, 0);
        assert!(outcome.summary.published_per_channel.is_empty());
        assert_eq!(outcome.summary.late_messages, 0);
        assert_eq!(outcome.summary.publish_errors, 0);
    }

    #[tokio::test]
    async fn run_reports_interrupted_when_cancelled_before_the_snapshot() {
        let dir = std::env::temp_dir().join(format!("aorta-play-cancel-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let input = dir.join("a.mcap");
        std::fs::write(&input, vec![0u8; 4 * 1024 * 1024]).expect("fixture");

        let config = PlayerConfig {
            inputs: vec![input],
            ..PlayerConfig::default()
        };
        let token = CancellationToken::new();
        token.cancel();
        let outcome = run_with_shutdown(config, token)
            .await
            .expect("cancellation is not an error");
        assert_eq!(outcome.termination, Termination::Interrupted);
        assert_eq!(outcome.termination.exit_code(), 130);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
