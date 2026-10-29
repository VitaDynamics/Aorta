use anyhow::Result;
use aorta_foxglove_bridge::config::{
    build_config, load_file_config, ConfigOverrides, DropPolicy, LogLevel,
};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "aorta-foxglove-bridge")]
#[command(about = "Aorta ↔ Foxglove Studio live WebSocket bridge", long_about = None)]
struct Args {
    #[arg(long, help = "Config file (.yaml/.yml/.json/.toml)")]
    config: Option<PathBuf>,

    #[arg(long, help = "Aorta group")]
    group: Option<String>,

    #[arg(long, help = "Zenoh config file path")]
    zenoh_config: Option<String>,

    #[arg(long, help = "WebSocket bind host")]
    host: Option<String>,

    #[arg(long, help = "WebSocket bind port")]
    port: Option<u16>,

    #[arg(long, help = "Advertised Foxglove server name")]
    server_name: Option<String>,

    #[arg(long = "include", help = "Suffix keyexpr to forward (repeatable)")]
    include_suffix_keyexpr: Vec<String>,

    #[arg(long = "blacklist", help = "Suffix keyexpr to drop (repeatable)")]
    blacklist_suffix_keyexpr: Vec<String>,

    #[arg(long = "blacklist-full", help = "Full keyexpr to drop (repeatable)")]
    blacklist_full_keyexpr: Vec<String>,

    #[arg(long, help = "Schema by-hash query timeout in ms")]
    by_hash_timeout_ms: Option<u64>,

    #[arg(long, help = "Schema by-key query timeout in ms")]
    by_key_timeout_ms: Option<u64>,

    #[arg(long, help = "Schema resolver cache cap")]
    cache_max_entries: Option<usize>,

    #[arg(
        long,
        help = "Negative schema-cache TTL in ms (0 disables; bounds re-query cost for unregistered topics)"
    )]
    negative_cache_ttl_ms: Option<u64>,

    #[arg(long, help = "Admin-space discovery poll interval in ms")]
    discovery_interval_ms: Option<u64>,

    #[arg(
        long,
        help = "Periodic ctx/** snapshot interval in ms (0 disables; keeps context state visible for late-connecting clients)"
    )]
    ctx_snapshot_interval_ms: Option<u64>,

    #[arg(long, help = "Forwarder queue capacity")]
    queue_capacity: Option<usize>,

    #[arg(
        long,
        value_parser = ["block", "drop_newest"],
        help = "Queue-full policy (default drop_newest: a live viewer prefers fresh frames)"
    )]
    drop_policy: Option<String>,

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

    let drop_policy = args
        .drop_policy
        .as_deref()
        .map(DropPolicy::from_str)
        .transpose()?;
    let log_level = args
        .log_level
        .as_deref()
        .map(LogLevel::from_str)
        .transpose()?;

    let overrides = ConfigOverrides {
        group: args.group,
        zenoh_config_file: args.zenoh_config,
        host: args.host,
        port: args.port,
        server_name: args.server_name,
        include_suffix_keyexpr: args.include_suffix_keyexpr,
        blacklist_suffix_keyexpr: args.blacklist_suffix_keyexpr,
        blacklist_full_keyexpr: args.blacklist_full_keyexpr,
        by_hash_timeout_ms: args.by_hash_timeout_ms,
        by_key_timeout_ms: args.by_key_timeout_ms,
        cache_max_entries: args.cache_max_entries,
        negative_cache_ttl_ms: args.negative_cache_ttl_ms,
        discovery_interval_ms: args.discovery_interval_ms,
        ctx_snapshot_interval_ms: args.ctx_snapshot_interval_ms,
        queue_capacity: args.queue_capacity,
        drop_policy,
        log_level,
    };

    let config = build_config(file_config, overrides)?;
    aorta_foxglove_bridge::run(config).await?;
    Ok(())
}
