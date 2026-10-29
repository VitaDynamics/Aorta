use clap::{Parser, Subcommand};
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

mod cmd;
mod output;
mod util;

use output::OutputFormat;

/// Version identity stamped by the release build.
///
/// `--stamp --embed_label=<version>` injects the release label together with
/// the git commit (`tools/bazel/workspace_status.sh`). Without stamping,
/// rules_rust leaves the literal `{...}` placeholders in place: that means
/// "not a release build", not a Cargo version. Cargo builds (no injected
/// env) surface the same unstamped form.
fn version_string() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            let commit = usable_stamp(option_env!("STABLE_GIT_COMMIT"))
                .map(|sha| sha.chars().take(12).collect())
                .unwrap_or_else(|| "unknown".to_string());
            match usable_stamp(option_env!("AORTA_RELEASE_VERSION")) {
                Some(release) => format!("{release} ({commit})"),
                None => format!("0.0.0+unstamped ({commit})"),
            }
        })
        .as_str()
}

/// `None` for absent values, the literal `{...}` placeholder rules_rust
/// leaves behind without `--stamp`, and the status script's `unknown` fallback.
fn usable_stamp(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty() && !value.starts_with('{') && *value != "unknown")
}

#[derive(Parser, Debug)]
#[command(name = "aorta", version = version_string())]
#[command(about = "Aorta CLI - Diagnostic and management tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, help = "Aorta group")]
    group: Option<String>,

    #[arg(long, global = true, help = "Zenoh config file path")]
    zenoh_config: Option<String>,

    #[arg(
        long,
        global = true,
        help = "Do not override Zenoh session mode to client. \
            By default the CLI connects in client mode for faster startup. \
            Pass this flag to use the mode from the config file as-is. \
            Future: use a dedicated client-mode config file instead."
    )]
    no_client_override: bool,

    #[arg(
        long,
        value_enum,
        global = true,
        default_value_t = OutputFormat::Text,
        help = "Output format: text (human banners on stderr, data on stdout) or json"
    )]
    format: OutputFormat,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(
        visible_alias = "edu-directory",
        about = "Serve an allowlisted interface directory using a trusted session"
    )]
    InterfaceDirectory {
        #[arg(long, help = "JSON allowlist containing topic and service names")]
        allowlist: PathBuf,
    },
    Action(ActionArgs),
    Topic(TopicArgs),
    Service(ServiceArgs),
    Schema(SchemaArgs),
    Nodes(NodesArgs),
    Record(RecordArgs),
    Play(PlayArgs),
    Ctx(CtxArgs),
    Clean(CleanArgs),
    Stats(StatsArgs),
    Telemetry(TelemetryArgs),
}

#[derive(Parser, Debug)]
#[command(about = "Action operations")]
struct ActionArgs {
    #[command(subcommand)]
    command: ActionCommands,
}

#[derive(Subcommand, Debug)]
enum ActionCommands {
    #[command(about = "List active actions")]
    List,
    #[command(about = "Show action server details")]
    Info {
        #[arg(help = "Action name")]
        action: String,
    },
    #[command(about = "List retained public goal statuses")]
    Goals {
        #[arg(help = "Action name")]
        action: String,
        #[arg(long, help = "Include terminal goals that have not expired yet")]
        all: bool,
    },
    #[command(about = "Show public status for one goal")]
    Status {
        #[arg(help = "Action name")]
        action: String,
        #[arg(long, help = "Goal UUID")]
        goal_id: String,
    },
    #[command(about = "Stream action feedback")]
    Feedback {
        #[arg(help = "Action name")]
        action: String,
        #[arg(long, help = "Optional goal UUID filter")]
        goal_id: Option<String>,
        #[arg(long, short, help = "Stop after N feedback samples")]
        count: Option<usize>,
        #[arg(long, help = "Overall deadline in seconds")]
        timeout: Option<u64>,
    },
    #[command(about = "Send one action goal")]
    Send {
        #[arg(help = "Action name")]
        action: String,
        #[arg(long, help = "Goal JSON or @file")]
        json: String,
        #[arg(long, help = "Optional caller-supplied goal UUID")]
        goal_id: Option<String>,
        #[arg(
            long,
            help = "Return immediately after goal acceptance without waiting for feedback/result"
        )]
        non_blocking: bool,
    },
    #[command(about = "Fetch private action result snapshot")]
    Result {
        #[arg(help = "Action name")]
        action: String,
        #[arg(long, help = "Goal UUID")]
        goal_id: String,
        #[arg(long, help = "Goal secret returned by send")]
        goal_secret: String,
    },
    #[command(about = "Wait for terminal status, then fetch private result")]
    Wait {
        #[arg(help = "Action name")]
        action: String,
        #[arg(long, help = "Goal UUID")]
        goal_id: String,
        #[arg(long, help = "Goal secret returned by send")]
        goal_secret: String,
        #[arg(long, help = "Timeout in seconds", default_value = "300")]
        timeout: u64,
    },
    #[command(about = "Request goal cancellation")]
    Cancel {
        #[arg(help = "Action name")]
        action: String,
        #[arg(long, help = "Goal UUID")]
        goal_id: String,
        #[arg(long, help = "Goal secret returned by send")]
        goal_secret: String,
    },
}

#[derive(Parser, Debug)]
#[command(about = "Topic operations")]
struct TopicArgs {
    #[command(subcommand)]
    command: TopicCommands,
}

#[derive(Subcommand, Debug)]
enum TopicCommands {
    #[command(about = "List active topics")]
    List {
        #[arg(long, short, help = "Timeout in seconds", default_value = "2")]
        timeout: u64,
        #[arg(long, help = "Show provider endpoint/node details")]
        verbose: bool,
        #[arg(long, help = "Filter by provider node name or node id")]
        node: Option<String>,
    },
    #[command(about = "Echo messages from a topic")]
    Echo {
        #[arg(help = "Topic name")]
        topic: String,
        #[arg(long, short, help = "Number of messages to print")]
        count: Option<usize>,
        #[arg(long, help = "Print raw hex")]
        raw: bool,
    },
    #[command(about = "Show message frequency")]
    Hz {
        #[arg(help = "Topic name")]
        topic: String,
        #[arg(
            long,
            short = 'w',
            help = "Sliding window size (number of intervals)",
            default_value = "8192"
        )]
        window: usize,
    },
    #[command(about = "Show message bandwidth (msg/s + bytes/s + payload size)")]
    Bw {
        #[arg(help = "Topic name")]
        topic: String,
        #[arg(
            long,
            short = 'w',
            help = "Sliding window size (number of samples)",
            default_value = "8192"
        )]
        window: usize,
    },
    #[command(about = "Show topic info")]
    Info {
        #[arg(help = "Topic name")]
        topic: String,
    },
}

#[derive(Parser, Debug)]
#[command(about = "Service operations")]
struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommands,
}

#[derive(Subcommand, Debug)]
enum ServiceCommands {
    #[command(about = "List active services")]
    List {
        #[arg(long, short, help = "Timeout in seconds", default_value = "2")]
        timeout: u64,
        #[arg(long, help = "Show provider endpoint/node details")]
        verbose: bool,
        #[arg(long, help = "Filter by provider node name or node id")]
        node: Option<String>,
    },
    #[command(about = "Show service provider details")]
    Info {
        #[arg(help = "Service name")]
        service: String,
    },
    #[command(about = "Sample approximate client-observed latency percentiles (p50/p95/p99)")]
    Latency {
        #[arg(help = "Service name")]
        service: String,
        #[arg(
            long,
            short = 'w',
            help = "Sampling window in seconds",
            default_value = "5"
        )]
        window_secs: u64,
    },
    #[command(about = "Call a service")]
    Call {
        #[arg(help = "Service name")]
        service: String,
        #[arg(help = "Request JSON")]
        request: String,
        #[arg(long, short, help = "Timeout in seconds")]
        timeout: Option<u64>,
    },
}

#[derive(Parser, Debug)]
#[command(about = "Schema operations")]
struct SchemaArgs {
    #[command(subcommand)]
    command: SchemaCommands,
}

#[derive(Subcommand, Debug)]
enum SchemaCommands {
    #[command(about = "Get a schema by key, or by --hash")]
    Get {
        #[arg(help = "Schema key (omit when using --hash)")]
        key: Option<String>,
        #[arg(
            long,
            conflicts_with = "key",
            help = "Look up by SHA-256 schema hash (64 hex chars)"
        )]
        hash: Option<String>,
        #[arg(long, help = "Output bfbs to file")]
        dump_bfbs: Option<String>,
        #[arg(long, help = "Print detailed schema definitions (key lookup only)")]
        describe: bool,
    },
    #[command(about = "List all registered schemas (topic + service)")]
    List,
}

#[derive(Parser, Debug)]
#[command(about = "Node operations")]
struct NodesArgs {
    #[command(subcommand)]
    command: NodesCommands,
}

#[derive(Subcommand, Debug)]
enum NodesCommands {
    #[command(about = "List online nodes")]
    List {
        #[arg(long, short, help = "Watch for changes")]
        watch: bool,
        #[arg(long, help = "Show owner endpoint details")]
        verbose: bool,
        #[arg(long, help = "Show candidate topic/service counts per node")]
        with_resources: bool,
        #[arg(long, short, help = "With --watch: stop after N change events")]
        count: Option<usize>,
        #[arg(long, help = "With --watch: overall deadline in seconds")]
        timeout: Option<u64>,
    },
    #[command(about = "Show node providers (topics/services)")]
    Info {
        #[arg(help = "Node name or node id")]
        node: String,
    },
}

#[derive(Parser, Debug)]
#[command(about = "Cleanup operations")]
struct CleanArgs {
    #[command(subcommand)]
    command: CleanCommands,
}

#[derive(Parser, Debug)]
#[command(about = "Record operations")]
struct RecordArgs {
    #[arg(long, help = "Config file (.yaml/.yml/.json/.toml)")]
    config: Option<PathBuf>,

    #[arg(short = 'o', long, help = "Output MCAP file")]
    output: Option<String>,

    #[arg(long, help = "Zenoh config file path")]
    zenoh_config: Option<String>,

    #[arg(long, help = "Enable RPC record stream capture")]
    rpc_record: bool,

    #[arg(long, help = "Recording duration in seconds")]
    duration: Option<u64>,

    #[arg(long, help = "Stop after receiving max messages")]
    max_messages: Option<u64>,

    #[arg(long = "include", help = "Include topic suffix keyexpr (repeatable)")]
    include_suffix_keyexpr: Vec<String>,

    #[arg(
        long = "blacklist",
        help = "Blacklist topic suffix keyexpr (repeatable)"
    )]
    blacklist_suffix_keyexpr: Vec<String>,

    #[arg(long = "blacklist-full", help = "Blacklist full keyexpr (repeatable)")]
    blacklist_full_keyexpr: Vec<String>,

    #[arg(long, value_parser = ["block", "drop_newest"], help = "Queue backpressure policy")]
    drop_policy: Option<String>,

    #[arg(long, value_parser = ["none", "zstd", "lz4"], help = "MCAP compression algorithm")]
    compression: Option<String>,

    #[arg(long, help = "MCAP compression level (fast|default|best or numeric)")]
    compression_level: Option<String>,

    #[arg(long, help = "MCAP chunk size in bytes")]
    chunk_size: Option<u64>,

    #[arg(long, hide = true, help = "Disable MCAP chunking (diagnostics only)")]
    no_chunking: bool,

    #[arg(
        long,
        hide = true,
        help = "Disable MCAP summary section (diagnostics only)"
    )]
    no_summary: bool,

    #[arg(
        long,
        help = "Enable file rotation/splitting into timestamped segments"
    )]
    rotate: bool,

    #[arg(long, help = "Per-segment split period in minutes (>= 1)")]
    split_minutes: Option<u64>,

    #[arg(
        long,
        help = "Per-segment size cap in MiB (size-only unless --split-minutes is set)"
    )]
    rotate_size_mb: Option<u64>,

    #[arg(long, help = "Retention: keep at most this many finalized segments")]
    rotate_max_files: Option<usize>,

    #[arg(long, help = "Retention: aggregate on-disk cap across segments in MiB")]
    rotate_max_total_mb: Option<u64>,

    #[arg(
        long,
        help = "Segment filename template (placeholders {ts} {stem} {ext} {group} {pid})"
    )]
    rotate_name_template: Option<String>,

    #[arg(long, value_parser = ["utc", "local"], help = "Segment timestamp zone (utc only in this release)")]
    rotate_timestamp: Option<String>,

    #[arg(long, value_parser = ["trace", "debug", "info", "warn", "error"], help = "Log level")]
    log_level: Option<String>,
}

#[derive(Parser, Debug)]
#[command(about = "Play MCAP recordings")]
struct PlayArgs {
    #[arg(
        required = true,
        value_name = "INPUT",
        help = "One or more .mcap files, or directories holding a rotated segment set"
    )]
    inputs: Vec<PathBuf>,

    #[arg(short = 'r', long, default_value_t = aorta_play::DEFAULT_RATE, help = "Playback rate multiplier")]
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

    #[arg(
        long,
        default_value_t = aorta_play::DEFAULT_MATCH_TIMEOUT_SECS,
        help = "Matched-subscriber barrier bound in seconds (0 disables)"
    )]
    match_timeout: f64,

    #[arg(
        long,
        default_value_t = aorta_play::DEFAULT_PREFLIGHT_TIMEOUT_SECS,
        help = "Per-topic schema-preflight get+drain deadline in seconds (must be > 0)"
    )]
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

    #[arg(
        long,
        default_value_t = aorta_play::DEFAULT_MAX_MESSAGE_BYTES,
        help = "Reject larger message payloads (must be <= --buffer-bytes)"
    )]
    max_message_bytes: u64,

    #[arg(
        long,
        default_value_t = aorta_play::DEFAULT_MAX_CHUNK_BYTES,
        help = "Reject larger declared chunk / generic record sizes"
    )]
    max_chunk_bytes: u64,

    #[arg(
        long,
        default_value_t = aorta_play::DEFAULT_BUFFER_BYTES,
        help = "In-flight byte budget of the reader queue"
    )]
    buffer_bytes: u64,

    #[arg(long, help = "Zenoh config file path")]
    zenoh_config: Option<String>,

    #[arg(
        long,
        value_parser = ["trace", "debug", "info", "warn", "error"],
        default_value = "info",
        help = "Log level"
    )]
    log_level: String,
}

impl PlayArgs {
    fn into_config(
        self,
        explicit_global_group: Option<String>,
        global_zenoh_config: Option<String>,
        client_mode_override: bool,
    ) -> anyhow::Result<aorta_play::PlayerConfig> {
        Ok(aorta_play::PlayerConfig {
            inputs: self.inputs,
            rate: self.rate,
            start_secs: self.start,
            duration_secs: self.duration,
            topics: self.topics,
            exclude: self.exclude,
            loop_playback: self.loop_playback,
            match_timeout_secs: self.match_timeout,
            preflight_timeout_secs: self.preflight_timeout,
            schema_policy: self.schema_policy.parse()?,
            allow_gaps: self.allow_gaps,
            max_message_bytes: self.max_message_bytes,
            max_chunk_bytes: self.max_chunk_bytes,
            buffer_bytes: self.buffer_bytes,
            // Only the explicit global flag is a CLI override. The player
            // resolves recorded metadata when this is None; ambient
            // AORTA_GROUP is deliberately not passed through.
            group: explicit_global_group,
            zenoh_config: self.zenoh_config.or(global_zenoh_config).map(PathBuf::from),
            client_mode_override,
            log_level: self.log_level.parse()?,
        })
    }
}

#[derive(Subcommand, Debug)]
enum CleanCommands {
    #[command(about = "Clean orphaned SHM segments")]
    Shm {
        #[arg(long, help = "Stale timeout in seconds", default_value = "5")]
        stale_timeout: u64,
    },
}

#[derive(Parser, Debug)]
#[command(about = "ContextStore operations (Phase 4+)")]
struct CtxArgs {
    #[command(subcommand)]
    command: CtxCommands,
}

#[derive(Subcommand, Debug)]
enum CtxCommands {
    #[command(
        about = "List context keys reachable under a prefix",
        long_about = "Issues a wildcard get and prints every owner Queryable reply, grouped by key. Two rows for the same key with different source_instance_id are tagged with a conflict marker."
    )]
    List {
        #[arg(help = "Optional prefix relative to ctx/ (e.g. demo/**)")]
        prefix: Option<String>,
        #[arg(long, default_value_t = 2_000, help = "Get deadline in ms")]
        deadline: u64,
    },
    #[command(about = "Get a single context key with metadata")]
    Get {
        #[arg(help = "Relative key, e.g. demo/state")]
        key: String,
        #[arg(long, default_value_t = 2_000, help = "Get deadline in ms")]
        deadline: u64,
    },
    #[command(about = "Stream live context updates under a prefix")]
    Watch {
        #[arg(help = "Prefix relative to ctx/ (e.g. demo/**)")]
        prefix: String,
        #[arg(long, short, help = "Stop after N samples")]
        count: Option<usize>,
        #[arg(long, help = "Overall deadline in seconds")]
        timeout: Option<u64>,
    },
    #[command(
        about = "Aggregate owner_conflict telemetry and multi-instance observations",
        long_about = "Combines two data sources: the aorta/{group}/sys/telemetry/vital/ctx/owner_conflict telemetry stream emitted by owners that detected a foreign writer, and live multi-instance observations from a watch on the prefix. The two sources are not always symmetric — see spec §15.2."
    )]
    Conflicts {
        #[arg(help = "Optional prefix relative to ctx/ (e.g. demo/**)")]
        prefix: Option<String>,
        #[arg(long, default_value_t = 30_000, help = "Collection window in ms")]
        window: u64,
    },
    #[command(
        about = "DEBUG: raw Zenoh put bypass for diagnostics (spec §15.3)",
        long_about = "Issues a raw ContextValue put to ctx/{group}/{key}. Does NOT declare ownership and does NOT route through the typed SDK; real owners under the prefix will detect this as a context.owner_conflict (intentional). Use only on test deployments."
    )]
    Set {
        #[arg(help = "Relative key, e.g. demo/state")]
        key: String,
        #[arg(long, value_parser = ["bool", "i64", "u64", "f64", "string"], help = "Value kind")]
        kind: String,
        #[arg(help = "Value to encode (parsed per --kind)")]
        value: String,
    },
}

#[derive(Parser, Debug)]
#[command(about = "Traffic/stats counters (router byte & message totals)")]
struct StatsArgs {
    #[command(subcommand)]
    command: Option<StatsCommands>,
}

#[derive(Subcommand, Debug)]
enum StatsCommands {
    #[command(about = "Show per-router tx/rx byte and message counters")]
    Routers,
}

#[derive(Parser, Debug)]
#[command(about = "System telemetry plane operations")]
struct TelemetryArgs {
    #[command(subcommand)]
    command: TelemetryCommands,
}

#[derive(Subcommand, Debug)]
enum TelemetryCommands {
    #[command(about = "Stream live node telemetry (vital snapshot + runtime metrics) as JSON")]
    Watch {
        #[arg(long, help = "Filter to a single node id (trailing key segment)")]
        node: Option<String>,
        #[arg(
            long,
            value_parser = ["vital", "runtime"],
            help = "Restrict to one telemetry family"
        )]
        kind: Option<String>,
        #[arg(long, short, help = "Stop after N messages")]
        count: Option<usize>,
        #[arg(long, help = "Print raw hex instead of decoded JSON")]
        raw: bool,
    },
}

fn apply_global_cli_env(group: Option<&str>, zenoh_config: Option<&str>) {
    if let Some(group) = group {
        std::env::set_var("AORTA_GROUP", group);
    }
    if let Some(zenoh_config) = zenoh_config {
        std::env::set_var(aorta_core::Session::SESSION_CONFIG_ENV, zenoh_config);
    }
}

fn resolve_record_zenoh_config(
    record_zenoh_config: Option<String>,
    global_zenoh_config: Option<String>,
) -> Option<String> {
    record_zenoh_config.or(global_zenoh_config)
}

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let cli = Cli::parse();
    // Preserve provenance before the existing flag -> ambient-env merge below.
    // `aorta-cli play` must pass only this explicit value to the player; the
    // player itself resolves recorded metadata when it is absent.
    let explicit_global_group = cli.group.clone();
    let global_zenoh_config = cli.zenoh_config.clone();

    apply_global_cli_env(cli.group.as_deref(), global_zenoh_config.as_deref());
    cmd::set_client_mode_override(!cli.no_client_override);
    output::set_output_format(cli.format);

    // Trace this CLI's service/action calls (Phase-2). Shares the same env config
    // as every other Aorta process (AORTA_TRACE / AORTA_TRACE_DIR / ...); the
    // drain flushes on exit (atexit), so a one-shot invocation still persists its
    // chunk. `AORTA_TRACE=off` disables it.
    aorta_core::trace::runtime::init();

    let group = cli.group.or_else(|| std::env::var("AORTA_GROUP").ok());

    match cli.command {
        Commands::InterfaceDirectory { allowlist } => {
            cmd::interface_directory::serve(&allowlist, group.as_deref()).await?
        }
        Commands::Action(args) => match args.command {
            ActionCommands::List => cmd::action::list(group.as_deref()).await?,
            ActionCommands::Info { action } => cmd::action::info(&action, group.as_deref()).await?,
            ActionCommands::Goals { action, all } => {
                cmd::action::goals(&action, all, group.as_deref()).await?
            }
            ActionCommands::Status { action, goal_id } => {
                cmd::action::status(&action, &goal_id, group.as_deref()).await?
            }
            ActionCommands::Feedback {
                action,
                goal_id,
                count,
                timeout,
            } => {
                cmd::action::feedback(
                    &action,
                    goal_id.as_deref(),
                    count,
                    timeout,
                    group.as_deref(),
                )
                .await?
            }
            ActionCommands::Send {
                action,
                json,
                goal_id,
                non_blocking,
            } => {
                cmd::action::send(
                    &action,
                    &json,
                    goal_id.as_deref(),
                    non_blocking,
                    group.as_deref(),
                )
                .await?
            }
            ActionCommands::Result {
                action,
                goal_id,
                goal_secret,
            } => cmd::action::result(&action, &goal_id, &goal_secret, group.as_deref()).await?,
            ActionCommands::Wait {
                action,
                goal_id,
                goal_secret,
                timeout,
            } => {
                cmd::action::wait(
                    &action,
                    &goal_id,
                    &goal_secret,
                    std::time::Duration::from_secs(timeout),
                    group.as_deref(),
                )
                .await?
            }
            ActionCommands::Cancel {
                action,
                goal_id,
                goal_secret,
            } => cmd::action::cancel(&action, &goal_id, &goal_secret, group.as_deref()).await?,
        },
        Commands::Topic(args) => match args.command {
            TopicCommands::List {
                timeout,
                verbose,
                node,
            } => cmd::topic::list(group.as_deref(), timeout, verbose, node.as_deref()).await?,
            TopicCommands::Echo { topic, count, raw } => {
                cmd::topic::echo(&topic, count, raw, group.as_deref()).await?
            }
            TopicCommands::Hz { topic, window } => {
                cmd::topic::hz(&topic, window, group.as_deref()).await?
            }
            TopicCommands::Bw { topic, window } => {
                cmd::topic::bw(&topic, window, group.as_deref()).await?
            }
            TopicCommands::Info { topic } => cmd::topic::info(&topic, group.as_deref()).await?,
        },
        Commands::Service(args) => match args.command {
            ServiceCommands::List {
                timeout,
                verbose,
                node,
            } => cmd::service::list(group.as_deref(), timeout, verbose, node.as_deref()).await?,
            ServiceCommands::Info { service } => {
                cmd::service::info(&service, group.as_deref()).await?
            }
            ServiceCommands::Latency {
                service,
                window_secs,
            } => cmd::service::latency(&service, window_secs, group.as_deref()).await?,
            ServiceCommands::Call {
                service,
                request,
                timeout,
            } => cmd::service::call(&service, &request, timeout, group.as_deref()).await?,
        },
        Commands::Schema(args) => match args.command {
            SchemaCommands::Get {
                key,
                hash,
                dump_bfbs,
                describe,
            } => match (key, hash) {
                (Some(key), None) => {
                    cmd::schema::get(&key, dump_bfbs.as_deref(), describe, group.as_deref()).await?
                }
                (None, Some(hash)) => {
                    cmd::schema::get_by_hash(&hash, dump_bfbs.as_deref(), group.as_deref()).await?
                }
                (None, None) => {
                    return Err(anyhow::anyhow!("provide a schema key or --hash <sha256>"));
                }
                // clap `conflicts_with` rejects key + --hash together before we get here.
                (Some(_), Some(_)) => unreachable!("clap conflicts_with prevents key + --hash"),
            },
            SchemaCommands::List => cmd::schema::list(group.as_deref()).await?,
        },
        Commands::Nodes(args) => match args.command {
            NodesCommands::List {
                watch,
                verbose,
                with_resources,
                count,
                timeout,
            } => {
                cmd::nodes::list(
                    watch,
                    verbose,
                    with_resources,
                    count,
                    timeout,
                    group.as_deref(),
                )
                .await?
            }
            NodesCommands::Info { node } => cmd::nodes::info(&node, group.as_deref()).await?,
        },
        Commands::Ctx(args) => match args.command {
            CtxCommands::List { prefix, deadline } => {
                cmd::ctx::list(group.as_deref(), prefix.as_deref(), deadline).await?
            }
            CtxCommands::Get { key, deadline } => {
                cmd::ctx::get(group.as_deref(), &key, deadline).await?
            }
            CtxCommands::Watch {
                prefix,
                count,
                timeout,
            } => cmd::ctx::watch(group.as_deref(), &prefix, count, timeout).await?,
            CtxCommands::Conflicts { prefix, window } => {
                cmd::ctx::conflicts(group.as_deref(), prefix.as_deref(), window).await?
            }
            CtxCommands::Set { key, kind, value } => {
                cmd::ctx::set(group.as_deref(), &key, &kind, &value).await?
            }
        },
        Commands::Clean(args) => match args.command {
            CleanCommands::Shm { stale_timeout } => cmd::clean::shm(stale_timeout).await?,
        },
        Commands::Stats(args) => match args.command.unwrap_or(StatsCommands::Routers) {
            StatsCommands::Routers => cmd::stats::routers(group.as_deref()).await?,
        },
        Commands::Telemetry(args) => match args.command {
            TelemetryCommands::Watch {
                node,
                kind,
                count,
                raw,
            } => {
                cmd::telemetry::watch(
                    group.as_deref(),
                    node.as_deref(),
                    kind.as_deref(),
                    count,
                    raw,
                )
                .await?
            }
        },
        Commands::Record(args) => {
            let file_config = if let Some(path) = args.config.as_ref() {
                Some(aorta_record::config::load_file_config(path)?)
            } else {
                None
            };

            let mut overrides = aorta_record::config::ConfigOverrides::default();
            overrides.group = group.clone();
            overrides.zenoh_config_file =
                resolve_record_zenoh_config(args.zenoh_config, global_zenoh_config.clone());
            // DRY-04: `aorta record` honours the same global `--no-client-override`
            // as every other subcommand (recorder default = client mode, like
            // cmd::open_session). Only push an explicit disable so a config-file
            // `zenoh.client_mode_override` stays authoritative otherwise.
            if cli.no_client_override {
                overrides.client_mode_override = Some(false);
            }
            overrides.output = args.output;
            overrides.rpc_record = if args.rpc_record { Some(true) } else { None };
            overrides.duration_sec = args.duration;
            overrides.max_messages = args.max_messages;
            overrides.include_suffix_keyexpr = args.include_suffix_keyexpr;
            overrides.blacklist_suffix_keyexpr = args.blacklist_suffix_keyexpr;
            overrides.blacklist_full_keyexpr = args.blacklist_full_keyexpr;
            if let Some(raw) = args.drop_policy {
                overrides.drop_policy = Some(aorta_record::config::DropPolicy::from_str(&raw)?);
            }
            if let Some(raw) = args.compression {
                overrides.compression =
                    Some(aorta_record::config::CompressionKind::from_str(&raw)?);
            }
            overrides.compression_level = args.compression_level;
            overrides.chunk_size = args.chunk_size;
            if args.no_chunking {
                overrides.chunking = Some(false);
            }
            if args.no_summary {
                overrides.summary = Some(false);
            }
            overrides.rotate_enabled = if args.rotate { Some(true) } else { None };
            overrides.split_minutes = args.split_minutes;
            overrides.rotate_max_file_size_mb = args.rotate_size_mb;
            overrides.rotate_max_files = args.rotate_max_files;
            overrides.rotate_max_total_size_mb = args.rotate_max_total_mb;
            overrides.rotate_name_template = args.rotate_name_template;
            overrides.rotate_timestamp = args.rotate_timestamp;
            if let Some(raw) = args.log_level {
                overrides.log_level = Some(aorta_record::config::LogLevel::from_str(&raw)?);
            }

            let record_config = aorta_record::config::build_config(file_config, overrides)?;
            aorta_record::run(record_config).await?;
        }
        Commands::Play(args) => {
            let config = args.into_config(
                explicit_global_group,
                global_zenoh_config.clone(),
                !cli.no_client_override,
            )?;
            return run_play(config).await;
        }
    }

    Ok(ExitCode::SUCCESS)
}

async fn run_play(config: aorta_play::PlayerConfig) -> anyhow::Result<ExitCode> {
    aorta_play::init_logging(config.log_level);
    let shutdown = CancellationToken::new();
    let _signal_owner = aorta_play::signal::install(shutdown.clone())
        .map_err(|err| anyhow::anyhow!("failed to install play SIGINT handler: {err:#}"))?;
    let progress = Arc::new(aorta_play::ProgressHandle::new());
    let progress_task = spawn_progress_line(progress.clone(), shutdown.clone());
    let outcome = aorta_play::run_with_shutdown_reporting(config, shutdown, progress).await;
    if let Some(task) = progress_task {
        task.abort();
        eprintln!();
    }

    match outcome {
        Ok(outcome) => {
            print!("{}", aorta_play::format_summary(&outcome.summary));
            Ok(ExitCode::from(outcome.termination.exit_code()))
        }
        Err(err) => {
            eprint!(
                "{}",
                aorta_play::format_player_error("aorta-cli play", &err)
            );
            Ok(ExitCode::from(1))
        }
    }
}

fn spawn_progress_line(
    progress: Arc<aorta_play::ProgressHandle>,
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
    use super::{
        apply_global_cli_env, resolve_record_zenoh_config, ActionCommands, Cli, Commands,
        CtxCommands, NodesCommands, OutputFormat, PlayArgs, ServiceCommands,
    };
    use clap::Parser;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn apply_global_cli_env_sets_aorta_group_and_zenoh_config() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        std::env::remove_var(aorta_core::Session::SESSION_CONFIG_ENV);

        apply_global_cli_env(Some("fleet_test"), Some("/tmp/zenoh_test.json5"));

        assert_eq!(
            std::env::var("AORTA_GROUP").ok().as_deref(),
            Some("fleet_test")
        );
        assert_eq!(
            std::env::var(aorta_core::Session::SESSION_CONFIG_ENV)
                .ok()
                .as_deref(),
            Some("/tmp/zenoh_test.json5")
        );

        std::env::remove_var("AORTA_GROUP");
        std::env::remove_var(aorta_core::Session::SESSION_CONFIG_ENV);
    }

    #[test]
    fn resolve_record_zenoh_config_prefers_record_arg_over_global_arg() {
        let resolved = resolve_record_zenoh_config(
            Some("/tmp/record.json5".to_string()),
            Some("/tmp/global.json5".to_string()),
        );
        assert_eq!(resolved.as_deref(), Some("/tmp/record.json5"));
    }

    #[test]
    fn resolve_record_zenoh_config_falls_back_to_global_arg() {
        let resolved = resolve_record_zenoh_config(None, Some("/tmp/global.json5".to_string()));
        assert_eq!(resolved.as_deref(), Some("/tmp/global.json5"));
    }

    #[test]
    fn cli_parses_action_status_command() {
        let cli = Cli::try_parse_from([
            "aorta",
            "action",
            "status",
            "/demo/execute_task",
            "--goal-id",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .expect("parse");

        match cli.command {
            Commands::Action(args) => match args.command {
                ActionCommands::Status { action, goal_id } => {
                    assert_eq!(action, "/demo/execute_task");
                    assert_eq!(goal_id, "550e8400-e29b-41d4-a716-446655440000");
                }
                other => panic!("unexpected action command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_action_goals_all_flag() {
        let cli = Cli::try_parse_from(["aorta", "action", "goals", "demo/execute_task", "--all"])
            .expect("parse");

        match cli.command {
            Commands::Action(args) => match args.command {
                ActionCommands::Goals { action, all } => {
                    assert_eq!(action, "demo/execute_task");
                    assert!(all);
                }
                other => panic!("unexpected action command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_action_send_command() {
        let cli = Cli::try_parse_from([
            "aorta",
            "action",
            "send",
            "demo/execute_task",
            "--json",
            "@/tmp/goal.json",
            "--goal-id",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .expect("parse");

        match cli.command {
            Commands::Action(args) => match args.command {
                ActionCommands::Send {
                    action,
                    json,
                    goal_id,
                    non_blocking,
                } => {
                    assert_eq!(action, "demo/execute_task");
                    assert_eq!(json, "@/tmp/goal.json");
                    assert_eq!(
                        goal_id.as_deref(),
                        Some("550e8400-e29b-41d4-a716-446655440000")
                    );
                    assert!(!non_blocking);
                }
                other => panic!("unexpected action command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_action_send_non_blocking_flag() {
        let cli = Cli::try_parse_from([
            "aorta",
            "action",
            "send",
            "demo/execute_task",
            "--json",
            "@/tmp/goal.json",
            "--non-blocking",
        ])
        .expect("parse");

        match cli.command {
            Commands::Action(args) => match args.command {
                ActionCommands::Send {
                    action,
                    json,
                    goal_id,
                    non_blocking,
                } => {
                    assert_eq!(action, "demo/execute_task");
                    assert_eq!(json, "@/tmp/goal.json");
                    assert_eq!(goal_id, None);
                    assert!(non_blocking);
                }
                other => panic!("unexpected action command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_action_wait_timeout() {
        let cli = Cli::try_parse_from([
            "aorta",
            "action",
            "wait",
            "demo/execute_task",
            "--goal-id",
            "550e8400-e29b-41d4-a716-446655440000",
            "--goal-secret",
            "secret",
            "--timeout",
            "42",
        ])
        .expect("parse");

        match cli.command {
            Commands::Action(args) => match args.command {
                ActionCommands::Wait {
                    action,
                    goal_id,
                    goal_secret,
                    timeout,
                } => {
                    assert_eq!(action, "demo/execute_task");
                    assert_eq!(goal_id, "550e8400-e29b-41d4-a716-446655440000");
                    assert_eq!(goal_secret, "secret");
                    assert_eq!(timeout, 42);
                }
                other => panic!("unexpected action command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_action_feedback_with_optional_goal_filter() {
        let cli = Cli::try_parse_from([
            "aorta",
            "action",
            "feedback",
            "demo/execute_task",
            "--goal-id",
            "550e8400-e29b-41d4-a716-446655440000",
        ])
        .expect("parse");

        match cli.command {
            Commands::Action(args) => match args.command {
                ActionCommands::Feedback {
                    action, goal_id, ..
                } => {
                    assert_eq!(action, "demo/execute_task");
                    assert_eq!(
                        goal_id.as_deref(),
                        Some("550e8400-e29b-41d4-a716-446655440000")
                    );
                }
                other => panic!("unexpected action command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_no_client_override_defaults_to_false() {
        let cli = Cli::try_parse_from(["aorta", "service", "list"]).expect("parse");
        assert!(!cli.no_client_override);
    }

    #[test]
    fn cli_no_client_override_flag_is_parsed() {
        let cli = Cli::try_parse_from(["aorta", "--no-client-override", "service", "list"])
            .expect("parse");
        assert!(cli.no_client_override);
    }

    fn parse_record(extra: &[&str]) -> super::RecordArgs {
        let mut argv: Vec<&str> = vec!["aorta", "record", "-o", "/tmp/test.mcap"];
        argv.extend_from_slice(extra);
        let cli = Cli::try_parse_from(argv).expect("parse");
        match cli.command {
            Commands::Record(args) => args,
            _ => panic!("expected record subcommand"),
        }
    }

    fn parse_play(extra: &[&str]) -> PlayArgs {
        let mut argv: Vec<&str> = vec!["aorta", "play", "/tmp/test.mcap"];
        argv.extend_from_slice(extra);
        let cli = Cli::try_parse_from(argv).expect("parse");
        match cli.command {
            Commands::Play(args) => args,
            _ => panic!("expected play subcommand"),
        }
    }

    #[test]
    fn play_args_wire_the_full_player_surface() {
        let args = parse_play(&[
            "--rate",
            "2",
            "--start",
            "1.5",
            "--duration",
            "4",
            "--topics",
            "sensor/imu",
            "sensor/lidar",
            "--exclude",
            "sensor/lidar",
            "--loop",
            "--match-timeout",
            "0",
            "--preflight-timeout",
            "1",
            "--schema-policy",
            "latest",
            "--allow-gaps",
            "--max-message-bytes",
            "1024",
            "--max-chunk-bytes",
            "2048",
            "--buffer-bytes",
            "4096",
            "--zenoh-config",
            "/tmp/play.json5",
            "--log-level",
            "debug",
        ]);
        assert_eq!(args.inputs, vec![PathBuf::from("/tmp/test.mcap")]);
        assert_eq!(args.rate, 2.0);
        assert_eq!(args.topics, vec!["sensor/imu", "sensor/lidar"]);
        assert_eq!(args.exclude, vec!["sensor/lidar"]);
        assert!(args.loop_playback);
        assert_eq!(args.schema_policy, "latest");
        assert!(args.allow_gaps);
        assert_eq!(args.zenoh_config.as_deref(), Some("/tmp/play.json5"));
    }

    #[tokio::test]
    async fn play_rejects_zero_preflight_timeout_before_opening_inputs() {
        let config = parse_play(&["--preflight-timeout", "0"])
            .into_config(None, None, true)
            .expect("CLI arguments are syntactically valid");
        let err = aorta_play::run(config)
            .await
            .expect_err("zero preflight timeout must be rejected");
        assert!(
            err.to_string().contains("--preflight-timeout"),
            "got: {err}"
        );
    }

    #[test]
    fn play_explicit_flag_beats_ambient_group() {
        let _guard = env_lock().lock().expect("lock");
        std::env::set_var("AORTA_GROUP", "ambient");
        let config = parse_play(&[])
            .into_config(Some("explicit".to_string()), None, true)
            .expect("config");
        assert_eq!(config.group.as_deref(), Some("explicit"));
        std::env::remove_var("AORTA_GROUP");
    }

    #[test]
    fn play_ambient_env_only_is_not_a_group_override() {
        let _guard = env_lock().lock().expect("lock");
        std::env::set_var("AORTA_GROUP", "ambient");
        let config = parse_play(&[])
            .into_config(None, None, true)
            .expect("config");
        assert!(
            config.group.is_none(),
            "the engine must consult recording metadata"
        );
        std::env::remove_var("AORTA_GROUP");
    }

    #[test]
    fn play_recorded_metadata_only_leaves_group_resolution_to_the_engine() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let config = parse_play(&[])
            .into_config(None, None, true)
            .expect("config");
        assert!(
            config.group.is_none(),
            "a recording with aorta.group metadata supplies the effective group"
        );
    }

    #[test]
    fn play_no_client_override_maps_to_player_config() {
        let cli = Cli::try_parse_from(["aorta", "--no-client-override", "play", "/tmp/test.mcap"])
            .expect("play CLI arguments must parse");
        let client_mode_override = !cli.no_client_override;
        let args = match cli.command {
            Commands::Play(args) => args,
            _ => panic!("expected play subcommand"),
        };
        let config = args
            .into_config(None, None, client_mode_override)
            .expect("play CLI arguments must build a config");
        assert!(!config.client_mode_override);
    }

    #[test]
    fn record_args_default_storage_flags_are_unset() {
        let args = parse_record(&[]);
        assert!(args.compression.is_none());
        assert!(args.compression_level.is_none());
        assert!(args.chunk_size.is_none());
        assert!(!args.no_chunking);
        assert!(!args.no_summary);
    }

    #[test]
    fn record_args_parses_compression_flag() {
        assert_eq!(
            parse_record(&["--compression", "zstd"])
                .compression
                .as_deref(),
            Some("zstd")
        );
        assert_eq!(
            parse_record(&["--compression", "lz4"])
                .compression
                .as_deref(),
            Some("lz4")
        );
        assert_eq!(
            parse_record(&["--compression", "none"])
                .compression
                .as_deref(),
            Some("none")
        );
    }

    #[test]
    fn record_args_parses_compression_level_alias_and_numeric() {
        assert_eq!(
            parse_record(&["--compression-level", "best"])
                .compression_level
                .as_deref(),
            Some("best")
        );
        assert_eq!(
            parse_record(&["--compression-level", "19"])
                .compression_level
                .as_deref(),
            Some("19")
        );
    }

    #[test]
    fn record_args_parses_chunk_size_and_diagnostic_flags() {
        let args = parse_record(&["--chunk-size", "4096", "--no-chunking", "--no-summary"]);
        assert_eq!(args.chunk_size, Some(4096));
        assert!(args.no_chunking);
        assert!(args.no_summary);
    }

    #[test]
    fn cli_format_defaults_to_text() {
        let cli = Cli::try_parse_from(["aorta", "topic", "list"]).expect("parse");
        assert_eq!(cli.format, OutputFormat::Text);
    }

    #[test]
    fn cli_format_json_flag_is_parsed_as_global() {
        // `--format` is a global flag: it can appear after the subcommand.
        let cli =
            Cli::try_parse_from(["aorta", "topic", "list", "--format", "json"]).expect("parse");
        assert_eq!(cli.format, OutputFormat::Json);
    }

    #[test]
    fn cli_format_rejects_unknown_value() {
        let err = Cli::try_parse_from(["aorta", "topic", "list", "--format", "yaml"])
            .expect_err("parse should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("yaml") || msg.contains("possible values"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn cli_parses_ctx_watch_count_and_timeout() {
        let cli = Cli::try_parse_from([
            "aorta",
            "ctx",
            "watch",
            "demo/**",
            "--count",
            "3",
            "--timeout",
            "10",
        ])
        .expect("parse");
        match cli.command {
            Commands::Ctx(args) => match args.command {
                CtxCommands::Watch {
                    prefix,
                    count,
                    timeout,
                } => {
                    assert_eq!(prefix, "demo/**");
                    assert_eq!(count, Some(3));
                    assert_eq!(timeout, Some(10));
                }
                other => panic!("unexpected ctx command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_nodes_list_watch_count_and_timeout() {
        let cli = Cli::try_parse_from([
            "aorta",
            "nodes",
            "list",
            "--watch",
            "--count",
            "5",
            "--timeout",
            "30",
        ])
        .expect("parse");
        match cli.command {
            Commands::Nodes(args) => match args.command {
                NodesCommands::List {
                    watch,
                    count,
                    timeout,
                    ..
                } => {
                    assert!(watch);
                    assert_eq!(count, Some(5));
                    assert_eq!(timeout, Some(30));
                }
                other => panic!("unexpected nodes command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_action_feedback_count_and_timeout() {
        let cli = Cli::try_parse_from([
            "aorta",
            "action",
            "feedback",
            "demo/execute_task",
            "--count",
            "2",
            "--timeout",
            "15",
        ])
        .expect("parse");
        match cli.command {
            Commands::Action(args) => match args.command {
                ActionCommands::Feedback {
                    action,
                    count,
                    timeout,
                    ..
                } => {
                    assert_eq!(action, "demo/execute_task");
                    assert_eq!(count, Some(2));
                    assert_eq!(timeout, Some(15));
                }
                other => panic!("unexpected action command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_service_list_timeout_with_default() {
        let default_cli = Cli::try_parse_from(["aorta", "service", "list"]).expect("parse");
        match default_cli.command {
            Commands::Service(args) => match args.command {
                ServiceCommands::List { timeout, .. } => assert_eq!(timeout, 2),
                other => panic!("unexpected service command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }

        let cli =
            Cli::try_parse_from(["aorta", "service", "list", "--timeout", "7"]).expect("parse");
        match cli.command {
            Commands::Service(args) => match args.command {
                ServiceCommands::List { timeout, .. } => assert_eq!(timeout, 7),
                other => panic!("unexpected service command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn cli_parses_service_latency_window_with_default() {
        let default_cli =
            Cli::try_parse_from(["aorta", "service", "latency", "nav/plan"]).expect("parse");
        match default_cli.command {
            Commands::Service(args) => match args.command {
                ServiceCommands::Latency {
                    service,
                    window_secs,
                } => {
                    assert_eq!(service, "nav/plan");
                    assert_eq!(window_secs, 5, "default window is 5s");
                }
                other => panic!("unexpected service command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }

        let cli = Cli::try_parse_from(["aorta", "service", "latency", "nav/plan", "-w", "12"])
            .expect("parse");
        match cli.command {
            Commands::Service(args) => match args.command {
                ServiceCommands::Latency {
                    service,
                    window_secs,
                } => {
                    assert_eq!(service, "nav/plan");
                    assert_eq!(window_secs, 12);
                }
                other => panic!("unexpected service command: {:?}", other),
            },
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn record_args_rejects_unknown_compression() {
        let err = Cli::try_parse_from([
            "aorta",
            "record",
            "-o",
            "/tmp/t.mcap",
            "--compression",
            "gzip",
        ])
        .expect_err("parse should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("gzip") || msg.contains("possible values"),
            "unexpected error: {msg}"
        );
    }
}
