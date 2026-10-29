pub mod config;
pub mod discovery;
pub mod filter;
pub mod forwarder;
pub mod server;

use anyhow::{anyhow, Result};
use aorta_core::schema_resolve::{SchemaResolver, SchemaResolverConfig};
use aorta_core::{KeyExprBuilder, Session};
use config::{BridgeConfig, LogLevel};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::sync::{broadcast, mpsc};

#[derive(Debug, Default, Clone)]
pub struct RunSummary {
    pub topics_advertised: u64,
    pub messages_forwarded: u64,
    pub bytes_forwarded: u64,
    pub dropped_messages: u64,
    pub schema_miss_count: u64,
    pub by_hash_lookup_failures: u64,
    pub by_key_lookup_failures: u64,
    pub ctx_snapshot_values: u64,
}

pub async fn run(config: BridgeConfig) -> Result<RunSummary> {
    logln(
        config.log_level,
        LogLevel::Info,
        &format!(
            "aorta-foxglove-bridge starting: group={} listen={}:{} include={:?}",
            config.group, config.server.host, config.server.port, config.topics.include_suffix
        ),
    );

    let session = open_session(&config).await?;
    let key_builder = KeyExprBuilder::new(&config.group)
        .map_err(|e| anyhow!("invalid group '{}': {}", config.group, e))?;

    let server = Arc::new(server::Server::start(&config).await?);

    // Canonical core resolver (Foxglove Bridge Design §3.1), shared by the forwarder
    // and the discovery loop so positive AND negative caches are common.
    // `negative_ttl` bounds the cost of an unregistered topic to one query
    // per TTL window instead of one per message.
    let resolver = Arc::new(SchemaResolver::new(
        (*session).clone(),
        config.group.clone(),
        SchemaResolverConfig {
            cache_max_entries: config.schema.cache_max_entries,
            by_hash_timeout: Duration::from_millis(config.schema.by_hash_timeout_ms),
            by_key_timeout: Duration::from_millis(config.schema.by_key_timeout_ms),
            negative_ttl: Duration::from_millis(config.schema.negative_cache_ttl_ms),
        },
    ));

    let dropped_messages = Arc::new(AtomicU64::new(0));
    let (sample_tx, sample_rx) =
        mpsc::channel::<forwarder::Sample>(config.forwarder.queue_capacity);
    let (stop_tx, _) = broadcast::channel::<()>(1);

    let filter = filter::TopicFilter::from_config(&config.topics)?;

    let subscription_mgr = Arc::new(discovery::SubscriptionManager::new(
        session.clone(),
        key_builder.clone(),
        filter.clone(),
        stop_tx.clone(),
        config.forwarder.drop_policy,
        dropped_messages.clone(),
        config.log_level,
    ));
    subscription_mgr
        .attach_wildcards(&config.group, &config.topics, &sample_tx)
        .await?;

    let ctx_snapshot_values = Arc::new(AtomicU64::new(0));
    let discovery_handle = {
        let session = session.clone();
        let key_builder = key_builder.clone();
        let server = server.clone();
        let resolver = resolver.clone();
        let filter = filter.clone();
        let ctx_counter = ctx_snapshot_values.clone();
        let stop_rx = stop_tx.subscribe();
        let group = config.group.clone();
        let interval = Duration::from_millis(config.discovery.interval_ms.max(100));
        // 0 disables the periodic ctx snapshot.
        let ctx_snapshot_interval = match config.discovery.ctx_snapshot_interval_ms {
            0 => None,
            ms => Some(Duration::from_millis(ms.max(500))),
        };
        let log_level = config.log_level;
        tokio::spawn(async move {
            discovery::run_discovery_loop(
                session,
                &group,
                key_builder,
                server,
                resolver,
                filter,
                interval,
                ctx_snapshot_interval,
                ctx_counter,
                stop_rx,
                log_level,
            )
            .await;
        })
    };

    // Drop the LAST non-task sender clone (the manager takes it by
    // reference only); from here the channel stays open exactly as long
    // as at least one subscriber task is alive, so the forwarder's recv
    // loop — and the dead-data-plane detection below — reacts when they
    // all exit.
    drop(sample_tx);

    let mut forwarder_handle = {
        let inputs = forwarder::Inputs {
            server: server.clone(),
            resolver: resolver.clone(),
            sample_rx,
            log_level: config.log_level,
        };
        let stop_rx = stop_tx.subscribe();
        tokio::spawn(async move { forwarder::run(inputs, stop_rx).await })
    };

    // Wait for either a termination signal or the forwarder finishing on
    // its own (all subscriber tasks gone, e.g. the Zenoh session died).
    // Without the second arm a dead forwarder would leave a zombie bridge:
    // WebSocket server up, channels frozen, no data.
    let early_join = tokio::select! {
        signal = shutdown_signal() => {
            logln(
                config.log_level,
                LogLevel::Info,
                &format!("Stopping bridge ({})...", signal),
            );
            None
        }
        join_result = &mut forwarder_handle => {
            logln(
                config.log_level,
                LogLevel::Warn,
                "forwarder exited before shutdown was requested; stopping bridge",
            );
            Some(join_result)
        }
    };

    let _ = stop_tx.send(());
    subscription_mgr.shutdown().await;

    let join_result = match early_join {
        Some(join_result) => Some(join_result),
        None => tokio::time::timeout(Duration::from_secs(5), forwarder_handle)
            .await
            .ok(),
    };

    let mut summary = match join_result {
        Some(Ok(Ok(mut summary))) => {
            summary.dropped_messages = dropped_messages.load(Ordering::Relaxed);
            summary
        }
        Some(Ok(Err(err))) => {
            logln(
                config.log_level,
                LogLevel::Error,
                &format!("forwarder error: {}", err),
            );
            summary_with_drops(&dropped_messages)
        }
        Some(Err(join_err)) => {
            logln(
                config.log_level,
                LogLevel::Error,
                &format!("forwarder task panicked: {}", join_err),
            );
            summary_with_drops(&dropped_messages)
        }
        None => {
            logln(
                config.log_level,
                LogLevel::Warn,
                "forwarder did not finish within shutdown timeout",
            );
            summary_with_drops(&dropped_messages)
        }
    };

    let _ = tokio::time::timeout(Duration::from_secs(2), discovery_handle).await;
    server.shutdown().await;

    summary.ctx_snapshot_values = ctx_snapshot_values.load(Ordering::Relaxed);

    logln(
        config.log_level,
        LogLevel::Info,
        &format!(
            "bridge summary: topics_advertised={} messages_forwarded={} bytes_forwarded={} dropped_messages={} ctx_snapshot_values={} schema_miss={} by_hash_failures={} by_key_failures={}",
            summary.topics_advertised,
            summary.messages_forwarded,
            summary.bytes_forwarded,
            summary.dropped_messages,
            summary.ctx_snapshot_values,
            summary.schema_miss_count,
            summary.by_hash_lookup_failures,
            summary.by_key_lookup_failures,
        ),
    );

    Ok(summary)
}

/// Resolve on the first termination signal, naming the one that arrived.
///
/// SIGINT alone is not enough for a supervised deployment: systemd's default
/// `KillSignal` is SIGTERM, and an unhandled SIGTERM terminates the process
/// outright — skipping the subscriber teardown, the forwarder flush and the
/// run summary. Both signals share the one graceful path.
///
/// Registration deliberately stays here, inside the shutdown `select!`,
/// rather than being hoisted above the startup awaits to also cover the
/// startup window. Measured, not assumed: with the handlers installed up
/// front and `open_session` awaiting an unreachable router, a SIGTERM sent
/// 2 s in was still unobserved 13 s later — the task never got polled and
/// the signal stream never became ready, so the process only died when
/// zenoh's own connect timeout expired. Startup is therefore *worse* off
/// with an installed handler (default disposition kills it instantly, and
/// there is nothing to flush yet) than without one. Covering that window
/// properly needs the OS-level escape `aorta-play`'s `signal` module
/// documents — a `signal-hook` handler that can exit while the runtime is
/// wedged — not a `tokio::select!` arm.
async fn shutdown_signal() -> &'static str {
    // Registration fails only when the process signal setup is already
    // broken; degrade to SIGINT-only rather than refusing to shut down.
    let mut sigterm = unix_signal(SignalKind::terminate()).ok();

    match sigterm.as_mut() {
        Some(sigterm) => tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        },
        None => {
            let _ = tokio::signal::ctrl_c().await;
            "SIGINT"
        }
    }
}

fn summary_with_drops(dropped: &AtomicU64) -> RunSummary {
    RunSummary {
        dropped_messages: dropped.load(Ordering::Relaxed),
        ..RunSummary::default()
    }
}

async fn open_session(config: &BridgeConfig) -> Result<Arc<zenoh::Session>> {
    let zenoh_config = if let Some(path) = &config.zenoh_config_file {
        zenoh::Config::from_file(path)
            .map_err(|e| anyhow!("failed to load zenoh config '{}': {}", path, e))?
    } else {
        Session::config_from_env()?.unwrap_or_default()
    };

    let session = zenoh::open(zenoh_config)
        .await
        .map_err(|e| anyhow!("failed to open zenoh session: {}", e))?;
    Ok(Arc::new(session))
}

pub(crate) fn logln(config_level: LogLevel, level: LogLevel, message: &str) {
    if (level as u8) >= (config_level as u8) {
        println!("{}", message);
    }
}
