use anyhow::Result;
use aorta_record::config::{
    build_config, load_file_config, CompressionKind, ConfigOverrides, DropPolicy, LogLevel,
};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "aorta-record")]
#[command(about = "Aorta MCAP recorder", long_about = None)]
struct Args {
    #[arg(long, help = "Config file (.yaml/.yml/.json/.toml)")]
    config: Option<PathBuf>,

    #[arg(long, help = "Aorta group")]
    group: Option<String>,

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

    #[arg(
        long,
        help = "Disable the initial ctx wildcard-GET snapshot at record start"
    )]
    no_ctx_snapshot: bool,

    #[arg(long = "include", help = "Include topic suffix keyexpr (repeatable)")]
    include_suffix_keyexpr: Vec<String>,

    #[arg(
        long = "blacklist",
        help = "Blacklist topic suffix keyexpr (repeatable)"
    )]
    blacklist_suffix_keyexpr: Vec<String>,

    #[arg(long = "blacklist-full", help = "Blacklist full keyexpr (repeatable)")]
    blacklist_full_keyexpr: Vec<String>,

    #[arg(long, help = "Schema by-hash timeout in ms")]
    by_hash_timeout_ms: Option<u64>,

    #[arg(long, help = "Schema by-key timeout in ms")]
    by_key_timeout_ms: Option<u64>,

    #[arg(
        long,
        help = "Negative schema-cache TTL in ms (0 disables; bounds the per-topic \
                cost of an unregistered schema to one query per window)"
    )]
    negative_cache_ttl_ms: Option<u64>,

    #[arg(
        long,
        help = "Periodic stats log interval in ms (accepted/dropped/queue depths; 0 disables)"
    )]
    stats_interval_ms: Option<u64>,

    #[arg(
        long,
        help = "Do not force zenoh mode=client (observer default); keep the resolved config's mode"
    )]
    no_client_override: bool,

    #[arg(long, help = "Writer queue capacity")]
    queue_capacity: Option<usize>,

    #[arg(long, help = "Batch max messages before flush")]
    batch_max_messages: Option<usize>,

    #[arg(long, help = "Batch max bytes before flush")]
    batch_max_bytes: Option<usize>,

    #[arg(long, help = "Flush interval in ms")]
    flush_interval_ms: Option<u64>,

    #[arg(long, help = "Shutdown drain timeout in ms")]
    shutdown_timeout_ms: Option<u64>,

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
        help = "Segment filename template (placeholders {ts} {stem} {ext} {group} {pid} {index})"
    )]
    rotate_name_template: Option<String>,

    #[arg(long, value_parser = ["utc", "local"], help = "Segment timestamp zone (utc only in this release)")]
    rotate_timestamp: Option<String>,

    #[arg(long, value_parser = ["trace", "debug", "info", "warn", "error"], help = "Log level")]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let file_config = if let Some(path) = args.config.as_ref() {
        Some(load_file_config(path)?)
    } else {
        None
    };

    let mut overrides = ConfigOverrides::default();
    overrides.group = args.group;
    overrides.zenoh_config_file = args.zenoh_config;
    overrides.output = args.output;
    overrides.rpc_record = if args.rpc_record { Some(true) } else { None };
    overrides.duration_sec = args.duration;
    overrides.max_messages = args.max_messages;
    overrides.ctx_snapshot = if args.no_ctx_snapshot {
        Some(false)
    } else {
        None
    };
    overrides.include_suffix_keyexpr = args.include_suffix_keyexpr;
    overrides.blacklist_suffix_keyexpr = args.blacklist_suffix_keyexpr;
    overrides.blacklist_full_keyexpr = args.blacklist_full_keyexpr;
    overrides.by_hash_timeout_ms = args.by_hash_timeout_ms;
    overrides.by_key_timeout_ms = args.by_key_timeout_ms;
    overrides.negative_cache_ttl_ms = args.negative_cache_ttl_ms;
    overrides.stats_interval_ms = args.stats_interval_ms;
    if args.no_client_override {
        overrides.client_mode_override = Some(false);
    }
    overrides.queue_capacity = args.queue_capacity;
    overrides.batch_max_messages = args.batch_max_messages;
    overrides.batch_max_bytes = args.batch_max_bytes;
    overrides.flush_interval_ms = args.flush_interval_ms;
    overrides.shutdown_timeout_ms = args.shutdown_timeout_ms;

    if let Some(raw) = args.drop_policy {
        overrides.drop_policy = Some(DropPolicy::from_str(&raw)?);
    }
    if let Some(raw) = args.compression {
        overrides.compression = Some(CompressionKind::from_str(&raw)?);
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
        overrides.log_level = Some(LogLevel::from_str(&raw)?);
    }

    let config = build_config(file_config, overrides)?;
    aorta_record::run(config).await?;

    Ok(())
}
