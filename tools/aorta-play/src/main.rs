//! `aorta-play` — thin clap wrapper over the `aorta_play` engine.
//!
//! The wrapper owns process concerns the library refuses to take (MCAP Player
//! Proposal §6.6, §6.7): signal handling, the end-of-run summary, and the
//! exit-code mapping `Completed → 0`, `CompletedWithPublishErrors` /
//! `CompletedWithTeardownTimeout` / `Err(_) → 1`, `Interrupted → 130`.

use anyhow::Result;
use aorta_play::{
    format_summary, init_logging, run_with_shutdown_reporting, signal, LogLevel, PlayerConfig,
    ProgressHandle, SchemaPolicy, DEFAULT_BUFFER_BYTES, DEFAULT_MATCH_TIMEOUT_SECS,
    DEFAULT_MAX_CHUNK_BYTES, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_PREFLIGHT_TIMEOUT_SECS,
    DEFAULT_RATE,
};
use clap::Parser;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(name = "aorta-play")]
#[command(about = "Aorta MCAP player", long_about = None)]
struct Args {
    #[arg(
        required = true,
        value_name = "INPUT",
        help = "One or more .mcap files, or directories holding a rotated segment set \
                (non-recursive *.mcap glob). Every input is copied into private temp \
                storage before replay, so free temp space equal to the total input \
                size is required."
    )]
    inputs: Vec<PathBuf>,

    #[arg(short = 'r', long, default_value_t = DEFAULT_RATE, help = "Playback rate multiplier")]
    rate: f64,

    #[arg(
        short = 's',
        long,
        default_value_t = 0.0,
        help = "Skip the first SECS seconds of the effective timeline"
    )]
    start: f64,

    #[arg(
        short = 'd',
        long,
        help = "Stop after SECS seconds of effective recorded time"
    )]
    duration: Option<f64>,

    #[arg(
        long,
        num_args = 1..,
        value_name = "T",
        help = "Only replay these topics (topic name, key suffix, or full key)"
    )]
    topics: Vec<String>,

    #[arg(
        long,
        num_args = 1..,
        value_name = "T",
        help = "Skip these topics (applied after --topics)"
    )]
    exclude: Vec<String>,

    #[arg(short = 'l', long = "loop", help = "Loop forever (Ctrl-C to stop)")]
    loop_playback: bool,

    #[arg(long, default_value_t = DEFAULT_MATCH_TIMEOUT_SECS, help = "Matched-subscriber barrier bound in seconds (0 disables)")]
    match_timeout: f64,

    #[arg(long, default_value_t = DEFAULT_PREFLIGHT_TIMEOUT_SECS, help = "Per-topic schema-preflight get+drain deadline in seconds (must be > 0)")]
    preflight_timeout: f64,

    #[arg(
        long,
        value_parser = ["strict", "latest"],
        default_value = "strict",
        help = "Policy for topics carrying multiple typed schemas"
    )]
    schema_policy: String,

    #[arg(
        long,
        help = "Play a rotated set with missing/duplicate segment indices"
    )]
    allow_gaps: bool,

    #[arg(long, default_value_t = DEFAULT_MAX_MESSAGE_BYTES, help = "Reject larger message payloads (must be <= --buffer-bytes)")]
    max_message_bytes: u64,

    #[arg(long, default_value_t = DEFAULT_MAX_CHUNK_BYTES, help = "Reject larger declared chunk / generic record sizes")]
    max_chunk_bytes: u64,

    #[arg(long, default_value_t = DEFAULT_BUFFER_BYTES, help = "In-flight byte budget of the reader queue")]
    buffer_bytes: u64,

    #[arg(long, help = "Replay into this group instead of the recorded one")]
    group: Option<String>,

    #[arg(long, help = "Zenoh config file path")]
    zenoh_config: Option<PathBuf>,

    #[arg(
        long,
        help = "Do not override Zenoh session mode to client; keep the resolved config mode"
    )]
    no_client_override: bool,

    #[arg(
        long,
        value_parser = ["trace", "debug", "info", "warn", "error"],
        default_value = "info",
        help = "Log level"
    )]
    log_level: String,
}

impl Args {
    fn into_config(self) -> Result<PlayerConfig> {
        Ok(PlayerConfig {
            inputs: self.inputs,
            rate: self.rate,
            start_secs: self.start,
            duration_secs: self.duration,
            topics: self.topics,
            exclude: self.exclude,
            loop_playback: self.loop_playback,
            match_timeout_secs: self.match_timeout,
            preflight_timeout_secs: self.preflight_timeout,
            schema_policy: SchemaPolicy::from_str(&self.schema_policy)?,
            allow_gaps: self.allow_gaps,
            max_message_bytes: self.max_message_bytes,
            max_chunk_bytes: self.max_chunk_bytes,
            buffer_bytes: self.buffer_bytes,
            group: self.group,
            zenoh_config: self.zenoh_config,
            client_mode_override: !self.no_client_override,
            log_level: LogLevel::from_str(&self.log_level)?,
        })
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let config = match args.into_config() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("aorta-play: {:#}", err);
            return ExitCode::from(1);
        }
    };
    init_logging(config.log_level);

    // Signal ownership (MCAP Player Proposal §6.7): installed *before* the
    // engine is entered. The first SIGINT cancels the token (graceful teardown
    // → `Interrupted` → exit 130 through the mapping below); any further
    // delivery `_exit(130)`s from inside the OS handler, which is the only
    // bound while a synchronous transport publish is wedged. `tokio::signal` is
    // prohibited here.
    let shutdown = CancellationToken::new();
    let _signal_owner = match signal::install(shutdown.clone()) {
        Ok(owner) => owner,
        Err(err) => {
            eprintln!("aorta-play: {:#}", err);
            return ExitCode::from(1);
        }
    };

    // TTY-only 4 Hz progress line (MCAP Player Proposal §5), formatted by the
    // wrapper over the library's lock-free counters.
    let progress = Arc::new(ProgressHandle::new());
    let progress_task = spawn_progress_line(progress.clone(), shutdown.clone());

    let outcome = run_with_shutdown_reporting(config, shutdown, progress).await;
    if let Some(task) = progress_task {
        task.abort();
        eprintln!();
    }

    match outcome {
        Ok(outcome) => {
            print!("{}", format_summary(&outcome.summary));
            ExitCode::from(outcome.termination.exit_code())
        }
        Err(err) => {
            eprint!("{}", aorta_play::format_player_error("aorta-play", &err));
            ExitCode::from(1)
        }
    }
}

/// `[t=12.4s / 87.1s] [rate 1.0x] 4821 msgs`, redrawn 4 times a second on a
/// terminal and never emitted at all when stderr is redirected.
fn spawn_progress_line(
    progress: Arc<ProgressHandle>,
    shutdown: CancellationToken,
) -> Option<tokio::task::JoinHandle<()>> {
    if !std::io::stderr().is_terminal() {
        return None;
    }
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let snapshot = progress.snapshot();
            if !snapshot.active {
                continue;
            }
            let mut stderr = std::io::stderr();
            let _ = write!(
                stderr,
                "\r[t={:.1}s / {:.1}s] [rate {}x] {} msgs ",
                snapshot.elapsed.as_secs_f64(),
                snapshot.span.as_secs_f64(),
                snapshot.rate,
                snapshot.published
            );
            let _ = stderr.flush();
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_no_client_override_flag_is_accepted() {
        let config = Args::try_parse_from(["aorta-play", "--no-client-override", "/tmp/test.mcap"])
            .expect("standalone player must accept --no-client-override")
            .into_config()
            .expect("standalone arguments must build a player config");
        assert!(!config.client_mode_override);
    }
}
