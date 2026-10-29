pub mod config;
mod filter;
pub mod record_sink;
mod rotation;
mod writer;
mod writer_thread;

use anyhow::{anyhow, Result};
use aorta_core::attachment::extract_attachment_schema_hash;
use aorta_core::contextstore_fb::{
    context_value_schema_hash, root_as_context_value, CONTEXT_VALUE_BFBS,
};
use aorta_core::schema::SchemaHash;
use aorta_core::schema_resolve::{SchemaResolver, SchemaResolverConfig};
use aorta_core::{unix_time_ns, KeyExprBuilder, Session};
use config::{DropPolicy, LogLevel, RecorderConfig};
use filter::{build_subscription_key, TopicFilter};
use rotation::{RollInfo, RotatingWriter};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use writer::{RecordMessage, RecordSchema};
use writer_thread::WriterThread;
use zenoh::query::{ConsolidationMode, QueryTarget};

/// Deadline for the initial ctx wildcard GET. Mirrors `aorta ctx list`'s
/// default — long enough for owner Queryables to reply, short
/// enough to not stall the record loop start.
const CTX_SNAPSHOT_DEADLINE_MS: u64 = 2000;

#[derive(Debug, Default, Clone)]
pub struct RunSummary {
    pub accepted_messages: u64,
    pub accepted_bytes: u64,
    pub dropped_messages: u64,
    pub schema_miss_count: u64,
    pub by_hash_lookup_failures: u64,
    pub by_key_lookup_failures: u64,
    pub flush_count: u64,
    pub final_file_size_bytes: u64,
    pub segment_count: u64,
    pub deleted_segments: u64,
    pub retained_bytes: u64,
}

/// A segment that has been fully written and atomically promoted from `.part`
/// to its final path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFinalized {
    pub recording_id: String,
    pub index: u64,
    pub path: PathBuf,
    pub size_bytes: u64,
}

pub(crate) type SegmentCallback = Arc<dyn Fn(SegmentFinalized) -> Result<()> + Send + Sync>;

#[derive(Debug, Clone)]
struct IngressEvent {
    full_key: String,
    key_suffix: String,
    attachment_hash: Option<String>,
    payload: Vec<u8>,
    log_time_ns: u64,
    publish_time_ns: u64,
}

#[derive(Debug)]
struct RateLimitedWarn {
    last_emit: HashMap<String, std::time::Instant>,
    suppressed: HashMap<String, u64>,
    interval: Duration,
}

impl RateLimitedWarn {
    fn new() -> Self {
        Self {
            last_emit: HashMap::new(),
            suppressed: HashMap::new(),
            interval: Duration::from_secs(10),
        }
    }

    fn warn(&mut self, key: &str, message: &str, level: LogLevel) {
        let now = std::time::Instant::now();
        let last = self.last_emit.get(key).copied();
        if last
            .map(|ts| now.duration_since(ts) < self.interval)
            .unwrap_or(false)
        {
            let count = self.suppressed.entry(key.to_string()).or_insert(0);
            *count = count.saturating_add(1);
            return;
        }

        let suppressed = self.suppressed.remove(key).unwrap_or(0);
        self.last_emit.insert(key.to_string(), now);

        if suppressed > 0 {
            logln(
                level,
                LogLevel::Warn,
                &format!(
                    "WARN: {} (suppressed {} similar warnings)",
                    message, suppressed
                ),
            );
        } else {
            logln(level, LogLevel::Warn, &format!("WARN: {}", message));
        }
    }
}

pub async fn run(config: RecorderConfig) -> Result<RunSummary> {
    run_inner(config, None).await
}

/// Run the recorder and synchronously notify `on_segment` after each segment
/// is finalized. The callback runs on the writer thread and should only persist
/// or enqueue the event; slow work such as upload must happen elsewhere.
///
/// A callback consumer owns finalized-file retention, so Aorta retention must
/// be disabled when using this API.
pub async fn run_with_segment_callback<F>(
    config: RecorderConfig,
    on_segment: F,
) -> Result<RunSummary>
where
    F: Fn(SegmentFinalized) -> Result<()> + Send + Sync + 'static,
{
    run_inner(config, Some(Arc::new(on_segment))).await
}

async fn run_inner(
    config: RecorderConfig,
    segment_callback: Option<SegmentCallback>,
) -> Result<RunSummary> {
    validate_queue_capacity(config.queue_capacity)?;
    if segment_callback.is_some()
        && (config.rotation.max_files.is_some() || config.rotation.max_total_size_bytes.is_some())
    {
        return Err(anyhow!(
            "segment callbacks require Aorta retention to be disabled; \
             the callback consumer owns finalized-file retention"
        ));
    }
    std::env::set_var("AORTA_GROUP", &config.group);

    let session = open_session(&config).await?;
    let key_builder = KeyExprBuilder::new(&config.group)
        .map_err(|e| anyhow!("invalid group '{}': {}", config.group, e))?;

    let mut subscriptions = build_topic_subscriptions(&config)?;
    if config.rpc_record {
        subscriptions.push(format!("aorta/{}/sys/record/srv/**", config.group));
    }

    if subscriptions.is_empty() {
        return Err(anyhow!("no subscriptions configured"));
    }

    logln(
        config.log_level,
        LogLevel::Info,
        &format!("Recorder output: {}", config.output.display()),
    );
    logln(
        config.log_level,
        LogLevel::Info,
        &format!("Group: {}", config.group),
    );
    logln(
        config.log_level,
        LogLevel::Info,
        &format!("Subscriptions: {}", subscriptions.join(", ")),
    );

    let filter = TopicFilter::new(
        config.blacklist_suffix_keyexpr.clone(),
        config.blacklist_full_keyexpr.clone(),
    );

    let dropped_messages = Arc::new(AtomicU64::new(0));
    let (tx, mut rx) = mpsc::channel::<IngressEvent>(config.queue_capacity);
    let (stop_tx, _) = broadcast::channel::<()>(1);

    let mut handles = Vec::new();

    for selector in subscriptions {
        let subscriber = session
            .declare_subscriber(&selector)
            .await
            .map_err(|e| anyhow!("failed to declare subscriber '{}': {}", selector, e))?;

        let tx = tx.clone();
        let key_builder = key_builder.clone();
        let dropped = dropped_messages.clone();
        let filter = filter.clone();
        let drop_policy = config.drop_policy;
        let mut stop_rx = stop_tx.subscribe();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_rx.recv() => {
                        break;
                    }
                    sample = subscriber.recv_async() => {
                        let sample = match sample {
                            Ok(sample) => sample,
                            Err(_) => break,
                        };

                        let full_key = sample.key_expr().as_str().to_string();
                        let key_suffix = match key_builder.key_suffix_from_full_key(&full_key) {
                            Some(suffix) => suffix,
                            None => continue,
                        };

                        // `blacklist_suffix_keyexpr` is rooted under "pub/"
                        // (legacy semantics in filter.rs); `blacklist_full_keyexpr`
                        // matches the FULL key — including ctx/** and the
                        // owner_conflict telemetry stream that the recorder
                        // now subscribes by default. Always run the filter so
                        // blacklist_full is honoured on those streams too;
                        // the suffix path is a no-op when topic_suffix is empty.
                        let topic_suffix = key_suffix.strip_prefix("pub/").unwrap_or("");
                        if !filter.accepts(topic_suffix, &full_key) {
                            continue;
                        }

                        let ingest_ns = unix_time_ns();
                        let event = IngressEvent {
                            full_key,
                            key_suffix,
                            attachment_hash: extract_attachment_schema_hash(&sample),
                            payload: sample.payload().to_bytes().to_vec(),
                            log_time_ns: ingest_ns,
                            publish_time_ns: resolve_publish_time_ns(
                                sample.timestamp().map(|ts| ts.get_time().as_nanos()),
                                ingest_ns,
                            ),
                        };

                        match drop_policy {
                            DropPolicy::Block => {
                                if tx.send(event).await.is_err() {
                                    break;
                                }
                            }
                            DropPolicy::DropNewest => {
                                if tx.try_send(event).is_err() {
                                    dropped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    drop(tx);

    let mut writer = RotatingWriter::create_with_segment_callback(&config, segment_callback)?;

    // Initial context snapshot. The live subscriber only captures ctx puts
    // that arrive *after* it is declared, so a key whose owner stays quiet for
    // the whole window would otherwise be absent from the MCAP. Issue one
    // wildcard GET on `ctx/**` at t0 and write each current value into the
    // SAME typed channel a live put uses (channel name = full ctx key, schema =
    // the static ContextValue — see `ctx_record_schema`) — snapshot and live
    // streams are byte-identical, so downstream needs no special-casing.
    // Best-effort: a snapshot failure never aborts
    // the recording (live ctx puts are still captured).
    if config.ctx_snapshot {
        let snapshot_ns = unix_time_ns();
        match capture_ctx_snapshot(
            &session,
            &key_builder,
            &filter,
            &config.group,
            Duration::from_millis(CTX_SNAPSHOT_DEADLINE_MS),
        )
        .await
        {
            Ok(entries) => {
                // Snapshot messages count toward `max_messages` just like live
                // puts: `write_ctx_snapshot` stops once the cap is reached so a
                // t0 snapshot can never exceed it. If the cap is hit here, the
                // live loop is skipped below (see `stop_requested`).
                let written =
                    write_ctx_snapshot(&mut writer, &entries, snapshot_ns, config.max_messages)?;
                if written < entries.len() {
                    logln(
                        config.log_level,
                        LogLevel::Info,
                        &format!(
                            "Initial ctx snapshot: captured {} of {} key(s) (max_messages cap reached)",
                            written,
                            entries.len()
                        ),
                    );
                } else {
                    logln(
                        config.log_level,
                        LogLevel::Info,
                        &format!("Initial ctx snapshot: captured {} key(s)", written),
                    );
                }
            }
            Err(err) => {
                logln(
                    config.log_level,
                    LogLevel::Warn,
                    &format!(
                        "WARN: initial ctx snapshot skipped ({}); live ctx puts still recorded",
                        err
                    ),
                );
            }
        }
    }

    // Canonical core resolver: no dependency on the tool layer.
    // `resolve()` prefers the per-message attachment hash, else falls back to
    // by-key — equivalent to the prior behavior (`attachment_fallback` was a
    // no-op: both error arms returned `None`). `negative_ttl` bounds the cost
    // of an unregistered topic to one query per TTL window instead of one per
    // message.
    let resolver = SchemaResolver::new(
        session.clone(),
        config.group.clone(),
        SchemaResolverConfig {
            cache_max_entries: config.cache_max_entries,
            by_hash_timeout: Duration::from_millis(config.by_hash_timeout_ms),
            by_key_timeout: Duration::from_millis(config.by_key_timeout_ms),
            negative_ttl: Duration::from_millis(config.negative_cache_ttl_ms),
        },
    );
    let mut warnings = RateLimitedWarn::new();
    let schema_miss = Arc::new(AtomicU64::new(0));
    let mut flush_tick =
        tokio::time::interval(Duration::from_millis(config.flush_interval_ms.max(1)));
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stats_enabled = config.stats_interval_ms > 0;
    let mut stats_tick =
        tokio::time::interval(Duration::from_millis(config.stats_interval_ms.max(1)));
    stats_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    stats_tick.reset(); // don't fire immediately at t0

    // Stop signals as pollable STATE: the sliced send loop
    // below must be able to observe Ctrl-C / duration-elapsed between slices
    // while the writer queue is full, so a select! arm alone is not enough.
    let interrupted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let interrupt_notify = Arc::new(tokio::sync::Notify::new());
    {
        let interrupted = interrupted.clone();
        let interrupt_notify = interrupt_notify.clone();
        tokio::spawn(async move {
            // Keep listening so repeated Ctrl-C during shutdown stays swallowed
            // (parity with the old per-iteration ctrl_c() arm).
            while tokio::signal::ctrl_c().await.is_ok() {
                interrupted.store(true, Ordering::Relaxed);
                interrupt_notify.notify_one();
            }
        });
    }
    let run_deadline = config
        .duration_sec
        .map(|sec| tokio::time::Instant::now() + Duration::from_secs(sec));
    let run_deadline_elapsed = |deadline: Option<tokio::time::Instant>| {
        deadline.is_some_and(|d| tokio::time::Instant::now() >= d)
    };

    // If the initial ctx snapshot already reached `max_messages`, skip the live
    // loop entirely instead of blocking on the next event/Ctrl-C/duration.
    let mut stop_requested = max_messages_reached(writer.accepted_messages(), config.max_messages);
    if stop_requested {
        logln(
            config.log_level,
            LogLevel::Info,
            "Stopping recorder (max messages reached during initial snapshot)...",
        );
    }

    // Hand the writer to its dedicated thread: from here
    // on, all MCAP I/O is off this drain task. Disk stalls surface as
    // `writer_queue` depth (stats line) + ingress backpressure, never as a
    // stalled subscriber.
    let writer_thread = WriterThread::spawn(
        writer,
        &config,
        schema_miss.clone(),
        dropped_messages.clone(),
    );
    // Set when the writer thread stops early (I/O error): the drain loop exits
    // and the real error is surfaced by `finish()` below.
    let mut writer_stopped = false;

    while !stop_requested {
        if interrupted.load(Ordering::Relaxed) {
            logln(
                config.log_level,
                LogLevel::Info,
                "Stopping recorder (Ctrl-C)...",
            );
            break;
        }
        tokio::select! {
            _ = interrupt_notify.notified() => {
                logln(config.log_level, LogLevel::Info, "Stopping recorder (Ctrl-C)...");
                stop_requested = true;
            }
            _ = flush_tick.tick() => {
                // A dead writer thread (I/O error) must stop an idle recording
                // too — without this, a run with no incoming events would hang
                // until Ctrl-C (the inline writer surfaced the error via
                // `writer.flush()?` on this same tick).
                if writer_thread.is_stopped() {
                    writer_stopped = true;
                    stop_requested = true;
                    continue;
                }
                // Periodic flush + roll check, executed writer-side (covers the
                // time trigger, including idle segments). Best-effort: a full
                // writer queue skips the tick — queued writes batch-flush anyway.
                writer_thread.try_tick();
            }
            _ = stats_tick.tick(), if stats_enabled => {
                let resolver_stats = resolver.stats();
                logln(
                    config.log_level,
                    LogLevel::Info,
                    &format!(
                        "Recorder stats: accepted={} dropped={} schema_miss={} ingress_queue={}/{} writer_queue={}/{} schema_queries={{by_hash:{},by_key:{}}}",
                        writer_thread.accepted_messages(),
                        dropped_messages.load(Ordering::Relaxed),
                        schema_miss.load(Ordering::Relaxed),
                        rx.len(),
                        config.queue_capacity,
                        writer_thread.queue_depth(),
                        config.queue_capacity,
                        resolver_stats.by_hash_network_queries,
                        resolver_stats.by_key_network_queries,
                    ),
                );
            }
            _ = tokio::time::sleep_until(
                run_deadline.unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600))
            ), if run_deadline.is_some() => {
                logln(config.log_level, LogLevel::Info, "Stopping recorder (duration reached)...");
                stop_requested = true;
            }
            event = rx.recv() => {
                let Some(event) = event else {
                    stop_requested = true;
                    continue;
                };

                let (schema, unresolved_schema_hash) = if is_ctx_key(&event.key_suffix) {
                    // ctx values share one static schema; no runtime resolve.
                    (ctx_record_schema(&event.payload), None)
                } else if should_resolve_schema(&event.key_suffix) {
                    let attachment_hash =
                        event.attachment_hash.as_deref().and_then(SchemaHash::from_hex_str);
                    match resolver
                        .resolve(&event.key_suffix, attachment_hash.as_ref())
                        .await
                    {
                        Some(found) => (
                            Some(RecordSchema {
                                hash_hex: found.schema_hash.to_hex(),
                                bfbs: found.bfbs,
                            }),
                            None,
                        ),
                        None => {
                            schema_miss.fetch_add(1, Ordering::Relaxed);
                            warnings.warn(
                                &format!("schema_miss:{}", event.key_suffix),
                                &format!("schema not found for key suffix '{}'", event.key_suffix),
                                config.log_level,
                            );
                            (None, event.attachment_hash.clone())
                        }
                    }
                } else {
                    (None, None)
                };

                // Sliced enqueue: never await the writer queue unconditionally —
                // between 100ms slices, re-check Ctrl-C and the run deadline so
                // a full queue over a stuck-but-alive writer cannot make the
                // stop paths unreachable. On a stop during
                // backpressure the message is counted as dropped.
                let mut pending = Box::new(RecordMessage {
                    topic_key: event.full_key,
                    key_suffix: event.key_suffix,
                    schema,
                    unresolved_schema_hash,
                    payload: event.payload,
                    log_time_ns: event.log_time_ns,
                    publish_time_ns: event.publish_time_ns,
                });
                loop {
                    match writer_thread
                        .send_timeout(pending, Duration::from_millis(100))
                        .await
                    {
                        writer_thread::SendOutcome::Sent => break,
                        writer_thread::SendOutcome::Stopped => {
                            writer_stopped = true;
                            stop_requested = true;
                            break;
                        }
                        writer_thread::SendOutcome::Timeout(message) => {
                            if interrupted.load(Ordering::Relaxed)
                                || run_deadline_elapsed(run_deadline)
                            {
                                dropped_messages.fetch_add(1, Ordering::Relaxed);
                                logln(
                                    config.log_level,
                                    LogLevel::Info,
                                    "Stopping recorder (stop requested while writer queue full)...",
                                );
                                stop_requested = true;
                                break;
                            }
                            pending = message;
                        }
                    }
                }
                if stop_requested {
                    continue;
                }

                if writer_thread.cap_reached() {
                    logln(
                        config.log_level,
                        LogLevel::Info,
                        "Stopping recorder (max messages reached)...",
                    );
                    stop_requested = true;
                }
            }
        }
    }

    let _ = stop_tx.send(());

    let shutdown_deadline =
        tokio::time::Instant::now() + Duration::from_millis(config.shutdown_timeout_ms.max(1));
    while !writer_stopped && tokio::time::Instant::now() < shutdown_deadline {
        if writer_thread.cap_reached() {
            break;
        }
        let remaining = shutdown_deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(event)) => {
                let (schema, unresolved_schema_hash) = if is_ctx_key(&event.key_suffix) {
                    (ctx_record_schema(&event.payload), None)
                } else if should_resolve_schema(&event.key_suffix) {
                    let attachment_hash = event
                        .attachment_hash
                        .as_deref()
                        .and_then(SchemaHash::from_hex_str);
                    let schema = resolver
                        .resolve(&event.key_suffix, attachment_hash.as_ref())
                        .await
                        .map(|found| RecordSchema {
                            hash_hex: found.schema_hash.to_hex(),
                            bfbs: found.bfbs,
                        });
                    let unresolved_schema_hash = schema
                        .is_none()
                        .then(|| event.attachment_hash.clone())
                        .flatten();
                    (schema, unresolved_schema_hash)
                } else {
                    (None, None)
                };

                // Bounded by the shutdown deadline too: a
                // full queue must not let the drain phase overshoot it.
                let slice = shutdown_deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(Duration::from_millis(100))
                    .max(Duration::from_millis(1));
                match writer_thread
                    .send_timeout(
                        Box::new(RecordMessage {
                            topic_key: event.full_key,
                            key_suffix: event.key_suffix,
                            schema,
                            unresolved_schema_hash,
                            payload: event.payload,
                            log_time_ns: event.log_time_ns,
                            publish_time_ns: event.publish_time_ns,
                        }),
                        slice,
                    )
                    .await
                {
                    writer_thread::SendOutcome::Sent => {}
                    writer_thread::SendOutcome::Timeout(_) => {
                        if tokio::time::Instant::now() >= shutdown_deadline {
                            dropped_messages.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        // Slice elapsed but budget remains; drop this event and
                        // keep draining (backpressure during shutdown).
                        dropped_messages.fetch_add(1, Ordering::Relaxed);
                    }
                    writer_thread::SendOutcome::Stopped => break,
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    for handle in handles {
        let _ =
            tokio::time::timeout(Duration::from_millis(config.shutdown_timeout_ms), handle).await;
    }

    // Close the command channel and wait for the final flush, end-metadata
    // write, and retention pass — bounded by a fresh shutdown window so queue
    // drain + finalize cannot hang the process on stuck I/O (the drain phase
    // above may have consumed the first window entirely on a healthy run). An
    // early writer I/O error resurfaces here.
    let dropped = dropped_messages.load(Ordering::Relaxed);
    let finalize_deadline =
        tokio::time::Instant::now() + Duration::from_millis(config.shutdown_timeout_ms.max(1));
    let outcome = writer_thread
        .finish_with_deadline(finalize_deadline)
        .await?;

    let summary = RunSummary {
        accepted_messages: outcome.accepted_messages,
        accepted_bytes: outcome.accepted_bytes,
        dropped_messages: dropped,
        schema_miss_count: schema_miss.load(Ordering::Relaxed),
        by_hash_lookup_failures: resolver.stats().by_hash_failures,
        by_key_lookup_failures: resolver.stats().by_key_failures,
        flush_count: outcome.flush_count,
        final_file_size_bytes: outcome.final_file_size_bytes,
        segment_count: outcome.segment_count,
        deleted_segments: outcome.deleted_segments,
        retained_bytes: outcome.retained_bytes,
    };

    logln(
        config.log_level,
        LogLevel::Info,
        &format!(
            "Recorder summary: accepted_messages={}, dropped_messages={}, accepted_bytes={}, schema_miss_count={}, by_hash_failures={}, by_key_failures={}, flush_count={}, file_size_bytes={}, segments={}, deleted_segments={}, retained_bytes={}",
            summary.accepted_messages,
            summary.dropped_messages,
            summary.accepted_bytes,
            summary.schema_miss_count,
            summary.by_hash_lookup_failures,
            summary.by_key_lookup_failures,
            summary.flush_count,
            summary.final_file_size_bytes,
            summary.segment_count,
            summary.deleted_segments,
            summary.retained_bytes
        ),
    );

    Ok(summary)
}

/// Session bootstrap, unified with aorta-cli/aorta-tui:
/// an explicit `--zenoh-config` file wins; otherwise the shared env-or-local-
/// default resolution (which also logs the config source, incl. the
/// insecure-fallback WARN) — no longer the narrower env-only path. The
/// client-mode override (default on, `--no-client-override` / config
/// `zenoh.client_mode_override=false` to disable) applies to both sources,
/// matching the CLI's observer-tool semantics.
async fn open_session(config: &RecorderConfig) -> Result<zenoh::Session> {
    let mut zenoh_config = if let Some(path) = &config.zenoh_config_file {
        zenoh::Config::from_file(path)
            .map_err(|e| anyhow!("failed to load zenoh config '{}': {}", path, e))?
    } else {
        Session::config_from_env_or_local_default()
            .await
            .map_err(|e| anyhow!("failed to build zenoh config: {}", e))?
    };

    if config.client_mode_override {
        Session::apply_mode_override(&mut zenoh_config, "client")
            .map_err(|e| anyhow!("failed to apply client mode override: {}", e))?;
    }

    zenoh::open(zenoh_config)
        .await
        .map_err(|e| anyhow!("failed to open zenoh session: {}", e))
}

/// Keys whose schema the recorder resolves so it can write a typed `flatbuffer`
/// MCAP channel: published topics under `pub/`, plus the telemetry records that
/// carry a schema-hash attachment. Current (tiered telemetry): the vital snapshot
/// (`vital/snapshot/`) and the bulk runtime stream (`bulk/runtime/`). The
/// legacy per-family keys (`node_identity/`, `resource_inventory/`, `runtime/`)
/// are kept too so a new recorder still writes typed channels for *older* nodes'
/// attached-schema puts during a rolling upgrade (the recorder resolves by hash,
/// so the changed BFBS bytes are irrelevant). The raw `…/ctx/owner_conflict`
/// alarm is intentionally excluded: it publishes without a schema-hash attachment
/// and has no by-key registration, so resolving it would only stall the recorder
/// on a by-key GET timeout before recording it raw anyway.
fn should_resolve_schema(key_suffix: &str) -> bool {
    key_suffix.starts_with("pub/")
        // Tiered telemetry (current):
        || key_suffix.starts_with("sys/telemetry/vital/snapshot/")
        || key_suffix.starts_with("sys/telemetry/bulk/runtime/")
        // Legacy per-family telemetry (mixed-version recording):
        || key_suffix.starts_with("sys/telemetry/node_identity/")
        || key_suffix.starts_with("sys/telemetry/resource_inventory/")
        || key_suffix.starts_with("sys/telemetry/runtime/")
}

/// Context keys (`aorta/{group}/ctx/**`). Every value under this prefix is the
/// single static `aorta.ctx.ContextValue` FlatBuffer (see [`ctx_record_schema`]).
fn is_ctx_key(key_suffix: &str) -> bool {
    key_suffix.starts_with("ctx/")
}

/// Schema for a context value. Unlike `pub/` topics there is nothing to resolve
/// at runtime: ctx puts carry no schema-hash attachment (`ContextStore::set`
/// does a bare `put`), but they all share one compile-time-known schema —
/// `aorta.ctx.ContextValue`. Embedding its committed BFBS makes the recorder
/// write a typed `flatbuffer` MCAP channel that readers like Foxglove can
/// parse, instead of the opaque `aorta_raw_bytes` encoding they reject.
///
/// Returns `None` (→ raw channel) when the payload does not parse as a
/// `ContextValue`, so a future non-ContextValue ctx key (or a corrupt put) is
/// recorded faithfully as raw rather than mislabeled with a schema that would
/// mis-decode it.
fn ctx_record_schema(payload: &[u8]) -> Option<RecordSchema> {
    root_as_context_value(payload).map(|_| RecordSchema {
        hash_hex: context_value_schema_hash().to_hex(),
        bfbs: CONTEXT_VALUE_BFBS.to_vec(),
    })
}

fn build_topic_subscriptions(config: &RecorderConfig) -> Result<Vec<String>> {
    let mut subscriptions = Vec::new();
    if config.include_suffix_keyexpr.is_empty() {
        subscriptions.push(format!("aorta/{}/pub/**", config.group));
    } else {
        for pattern in &config.include_suffix_keyexpr {
            subscriptions.push(build_subscription_key(&config.group, pattern)?);
        }
    }
    // Always include canonical context and the conflict telemetry stream
    // so MCAP captures full robot state plus any owner_conflict events
    // that fired during the recording window: the recorder subscribes to
    // aorta/{group}/ctx/**.
    subscriptions.push(format!("aorta/{}/ctx/**", config.group));
    // `sys/telemetry/**` captures both telemetry tiers: the vital snapshot
    // (`vital/snapshot/`) and the bulk runtime stream (`bulk/runtime/`), written
    // as typed FlatBuffers channels via their schema-hash attachments, plus the
    // raw `vital/ctx/owner_conflict` alarm.
    subscriptions.push(format!("aorta/{}/sys/telemetry/**", config.group));
    subscriptions.sort();
    subscriptions.dedup();
    Ok(subscriptions)
}

/// One context key/value captured by the initial wildcard-GET snapshot.
#[derive(Debug, Clone)]
struct CtxSnapshotEntry {
    full_key: String,
    key_suffix: String,
    payload: Vec<u8>,
}

/// Wrap a snapshot get+drain future with a wall-clock timeout so a hung getter
/// cannot stall recording start. On elapse we degrade to best-effort by
/// returning an error, which the caller already turns into "warn + continue
/// recording". The Zenoh `.timeout(deadline)` only bounds how long owners may
/// answer; it does NOT bound the get future itself, so without this wrapper a
/// transport that stalls before yielding the reply receiver would block start.
/// Mirrors the reference pattern in `core/src/contextstore.rs`
/// (`run_snapshot_task` / `watch_with_initial_snapshot`), which likewise wraps
/// the get future with `tokio::time::timeout` and degrades on elapse.
async fn snapshot_get_with_timeout<F>(deadline: Duration, fut: F) -> Result<Vec<CtxSnapshotEntry>>
where
    F: std::future::Future<Output = Result<Vec<CtxSnapshotEntry>>>,
{
    match tokio::time::timeout(deadline, fut).await {
        Ok(res) => res,
        Err(_elapsed) => Err(anyhow!("ctx snapshot get timed out after {:?}", deadline)),
    }
}

/// Issue a single wildcard GET on `aorta/{group}/ctx/**` and collect the
/// current value of every reachable context key. Uses the same query
/// semantics as `aorta ctx list` (all owners, no consolidation) and mirrors
/// the live ingress filter so `blacklist_full_keyexpr` applies identically to
/// snapshot and live messages. The reply payload is the raw `ContextValue`
/// FlatBuffer — byte-identical to a live put — so it can be written verbatim.
async fn capture_ctx_snapshot(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    filter: &TopicFilter,
    group: &str,
    deadline: Duration,
) -> Result<Vec<CtxSnapshotEntry>> {
    let selector = format!("aorta/{}/ctx/**", group);
    // Wrap the whole get+drain so a hung getter cannot block recording start;
    // on elapse we degrade to best-effort and the caller warns + continues.
    snapshot_get_with_timeout(deadline, async {
        let replies = session
            .get(&selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(deadline)
            .await
            .map_err(|e| anyhow!("ctx snapshot get failed: {}", e))?;

        let mut entries = Vec::new();
        let start = std::time::Instant::now();
        loop {
            let remaining = deadline.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    let Ok(sample) = reply.result() else {
                        continue;
                    };
                    let full_key = sample.key_expr().as_str().to_string();
                    let Some(key_suffix) = key_builder.key_suffix_from_full_key(&full_key) else {
                        continue;
                    };
                    // Mirror the live ingress filter (see the subscriber loop):
                    // the suffix path is a no-op on ctx keys, but blacklist_full
                    // must apply to snapshot and live messages alike.
                    let topic_suffix = key_suffix.strip_prefix("pub/").unwrap_or("");
                    if !filter.accepts(topic_suffix, &full_key) {
                        continue;
                    }
                    entries.push(CtxSnapshotEntry {
                        full_key,
                        key_suffix,
                        payload: sample.payload().to_bytes().to_vec(),
                    });
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
        Ok(entries)
    })
    .await
}

/// Write the captured ctx snapshot into the MCAP, honoring `max_messages` as a
/// hard cap: snapshot messages count toward the cap just like live puts, so
/// writing stops as soon as the cap is reached. Returns the number of entries
/// actually written (`<= entries.len()`); a short count means the cap truncated
/// the snapshot.
fn write_ctx_snapshot(
    writer: &mut RotatingWriter,
    entries: &[CtxSnapshotEntry],
    snapshot_ns: u64,
    max_messages: Option<u64>,
) -> Result<usize> {
    let mut written = 0usize;
    for entry in entries {
        if max_messages_reached(writer.accepted_messages(), max_messages) {
            break;
        }
        writer.write_message(RecordMessage {
            topic_key: entry.full_key.clone(),
            key_suffix: entry.key_suffix.clone(),
            // Type the snapshot value the same way a live ctx put is typed, so
            // the t0 snapshot and later live puts land on one shared
            // `flatbuffer` channel (byte-identical stream) instead of splitting
            // into a raw channel here and a typed one later.
            schema: ctx_record_schema(&entry.payload),
            unresolved_schema_hash: None,
            payload: entry.payload.clone(),
            log_time_ns: snapshot_ns,
            publish_time_ns: snapshot_ns,
        })?;
        written += 1;
    }
    Ok(written)
}

fn resolve_publish_time_ns(sample_timestamp_ns: Option<u64>, ingest_ns: u64) -> u64 {
    sample_timestamp_ns.unwrap_or(ingest_ns)
}

pub(crate) fn max_messages_reached(accepted_messages: u64, max_messages: Option<u64>) -> bool {
    max_messages
        .map(|max| accepted_messages >= max)
        .unwrap_or(false)
}

fn validate_queue_capacity(queue_capacity: usize) -> Result<()> {
    if queue_capacity == 0 {
        return Err(anyhow!(
            "queue_capacity must be greater than 0, got {}",
            queue_capacity
        ));
    }
    Ok(())
}

pub(crate) fn logln(config_level: LogLevel, level: LogLevel, message: &str) {
    if level_enabled(config_level, level) {
        println!("{}", message);
    }
}

pub(crate) fn log_roll(config_level: LogLevel, info: &RollInfo) {
    let deleted = if info.deleted_segments > 0 {
        format!(
            " (retention deleted {} old segment(s))",
            info.deleted_segments
        )
    } else {
        String::new()
    };
    logln(
        config_level,
        LogLevel::Info,
        &format!(
            "Rotated segment (reason={}): {}{}",
            info.reason.as_str(),
            info.segment.path.display(),
            deleted
        ),
    );
}

fn level_enabled(config_level: LogLevel, level: LogLevel) -> bool {
    rank(level) >= rank(config_level)
}

fn rank(level: LogLevel) -> u8 {
    match level {
        LogLevel::Trace => 0,
        LogLevel::Debug => 1,
        LogLevel::Info => 2,
        LogLevel::Warn => 3,
        LogLevel::Error => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_topic_subscriptions, ctx_record_schema, is_ctx_key, max_messages_reached,
        resolve_publish_time_ns, should_resolve_schema, snapshot_get_with_timeout,
        validate_queue_capacity, write_ctx_snapshot, CtxSnapshotEntry,
    };
    use crate::config::{build_config, ConfigOverrides, FileConfig};
    use aorta_core::contextstore_fb::{
        context_value_schema_hash, encode_value_string, ContextValueMeta, CONTEXT_VALUE_BFBS,
    };
    use std::time::Duration;

    #[test]
    fn build_topic_subscriptions_uses_default_when_empty() {
        let mut overrides = ConfigOverrides::default();
        overrides.group = Some("default".to_string());
        overrides.output = Some("/tmp/aorta-test.mcap".to_string());
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        let subscriptions = build_topic_subscriptions(&config).expect("subscriptions");
        assert_eq!(
            subscriptions,
            vec![
                "aorta/default/ctx/**".to_string(),
                "aorta/default/pub/**".to_string(),
                "aorta/default/sys/telemetry/**".to_string(),
            ]
        );
    }

    #[test]
    fn build_topic_subscriptions_uses_includes() {
        let mut overrides = ConfigOverrides::default();
        overrides.group = Some("default".to_string());
        overrides.output = Some("/tmp/aorta-test.mcap".to_string());
        overrides.include_suffix_keyexpr = vec!["demo/**".to_string(), "foo/bar".to_string()];
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        let subscriptions = build_topic_subscriptions(&config).expect("subscriptions");
        // ctx and sys/telemetry/** are always appended; pub/** includes are
        // honoured.
        assert_eq!(
            subscriptions,
            vec![
                "aorta/default/ctx/**".to_string(),
                "aorta/default/pub/demo/**".to_string(),
                "aorta/default/pub/foo/bar".to_string(),
                "aorta/default/sys/telemetry/**".to_string(),
            ]
        );
    }

    #[test]
    fn build_topic_subscriptions_dedups_when_user_includes_ctx() {
        // If the user explicitly added ctx/** via include_suffix, the
        // default ctx/** append should dedup rather than appear twice.
        let mut overrides = ConfigOverrides::default();
        overrides.group = Some("default".to_string());
        overrides.output = Some("/tmp/aorta-test.mcap".to_string());
        overrides.include_suffix_keyexpr = vec!["pub/demo/**".to_string()];
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        let subscriptions = build_topic_subscriptions(&config).expect("subscriptions");
        // The exact entries don't double-up.
        let count_ctx = subscriptions
            .iter()
            .filter(|s| s == &"aorta/default/ctx/**")
            .count();
        let count_telem = subscriptions
            .iter()
            .filter(|s| s == &"aorta/default/sys/telemetry/**")
            .count();
        assert_eq!(count_ctx, 1);
        assert_eq!(count_telem, 1);
    }

    #[test]
    fn should_resolve_schema_targets_pub_and_attached_telemetry_only() {
        assert!(should_resolve_schema("pub/demo/topic"));
        assert!(should_resolve_schema("sys/telemetry/vital/snapshot/abc"));
        assert!(should_resolve_schema("sys/telemetry/bulk/runtime/abc"));
        // Legacy per-family keys stay resolvable for mixed-version recording.
        assert!(should_resolve_schema("sys/telemetry/node_identity/abc"));
        assert!(should_resolve_schema(
            "sys/telemetry/resource_inventory/abc"
        ));
        assert!(should_resolve_schema("sys/telemetry/runtime/abc"));
        // owner_conflict publishes without a schema-hash attachment; resolving
        // it would only time out on a by-key GET, so it is recorded raw.
        assert!(!should_resolve_schema(
            "sys/telemetry/vital/ctx/owner_conflict"
        ));
        // ctx is typed via the static ContextValue schema (see ctx_record_schema),
        // not via the runtime resolver, so should_resolve_schema stays false.
        assert!(!should_resolve_schema("ctx/foo"));
    }

    #[test]
    fn is_ctx_key_matches_only_ctx_prefix() {
        assert!(is_ctx_key("ctx/system/estop"));
        assert!(is_ctx_key("ctx/foo"));
        assert!(!is_ctx_key("pub/demo/topic"));
        assert!(!is_ctx_key("sys/telemetry/vital/ctx/owner_conflict"));
    }

    #[test]
    fn ctx_record_schema_types_valid_context_value() {
        // A real ContextValue payload must be typed with the committed
        // ContextValue bfbs + its hash, so the recorder writes a `flatbuffer`
        // MCAP channel (parseable by Foxglove) instead of `aorta_raw_bytes`.
        let meta = ContextValueMeta {
            stamp_ns: 1,
            expires_at_ns: 0,
            source_node: "app_agent_ctrl".to_string(),
            source_instance_id: "77dcb62ca95d4d70".to_string(),
            sequence: 1,
        };
        let payload = encode_value_string("[\"follow\"]", &meta);
        let schema = ctx_record_schema(&payload).expect("valid ContextValue must be typed");
        assert_eq!(schema.hash_hex, context_value_schema_hash().to_hex());
        assert_eq!(schema.bfbs, CONTEXT_VALUE_BFBS);
    }

    #[test]
    fn ctx_record_schema_falls_back_to_raw_for_non_context_value() {
        // Bytes that do not parse as a ContextValue must NOT be mislabeled with
        // the ContextValue schema; they fall back to raw (None) so the recorder
        // records them faithfully rather than handing Foxglove a mis-decode.
        assert!(ctx_record_schema(&[]).is_none());
        assert!(ctx_record_schema(&[0xff, 0xff, 0xff, 0xff]).is_none());
    }

    #[test]
    fn resolve_publish_time_prefers_sample_timestamp() {
        let ingest_ns = 200;
        let publish_ns = resolve_publish_time_ns(Some(100), ingest_ns);
        assert_eq!(publish_ns, 100);
    }

    #[test]
    fn resolve_publish_time_falls_back_to_ingest_timestamp() {
        let ingest_ns = 200;
        let publish_ns = resolve_publish_time_ns(None, ingest_ns);
        assert_eq!(publish_ns, 200);
    }

    #[test]
    fn max_messages_reached_returns_false_when_unbounded() {
        assert!(!max_messages_reached(10, None));
    }

    #[test]
    fn max_messages_reached_returns_true_at_or_above_limit() {
        assert!(max_messages_reached(10, Some(10)));
        assert!(max_messages_reached(11, Some(10)));
        assert!(!max_messages_reached(9, Some(10)));
    }

    fn snapshot_test_writer(tag: &str) -> (crate::rotation::RotatingWriter, std::path::PathBuf) {
        use crate::config::{build_config, CompressionKind, ConfigOverrides, FileConfig};
        use crate::rotation::RotatingWriter;

        let mut path = std::env::temp_dir();
        path.push(format!("aorta-record-{}-{}.mcap", tag, std::process::id()));
        // Rotation disabled (default) => single-segment pass-through writing the
        // literal path, so the existing read-back assertions still hold.
        let mut overrides = ConfigOverrides::default();
        overrides.group = Some("default".to_string());
        overrides.output = Some(path.to_string_lossy().to_string());
        overrides.compression = Some(CompressionKind::None);
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        let writer = RotatingWriter::create(&config).expect("writer");
        (writer, path)
    }

    fn sample_ctx_entries(n: usize) -> Vec<CtxSnapshotEntry> {
        (0..n)
            .map(|i| CtxSnapshotEntry {
                full_key: format!("aorta/default/ctx/system/k{}", i),
                key_suffix: format!("ctx/system/k{}", i),
                payload: vec![i as u8],
            })
            .collect()
    }

    /// Read back an mcap file and return its recorded message count.
    fn read_snapshot_back(path: &std::path::Path) -> usize {
        let bytes = std::fs::read(path).expect("read mcap");
        mcap::MessageStream::new(&bytes)
            .expect("message stream")
            .map(|m| m.expect("message"))
            .count()
    }

    #[test]
    fn write_ctx_snapshot_truncates_at_max_messages() {
        // A small cap must stop the snapshot mid-write so it never exceeds the
        // configured max (writer count == cap, so the live loop is then
        // skipped).
        let (mut writer, path) = snapshot_test_writer("snapshot-cap");
        let entries = sample_ctx_entries(3);
        let written = write_ctx_snapshot(&mut writer, &entries, 42, Some(1)).expect("write");
        assert_eq!(written, 1, "cap of 1 truncates the 3-key snapshot");
        assert_eq!(writer.accepted_messages(), 1);
        writer.finish(0, 0).expect("finish");

        let msg_count = read_snapshot_back(&path);
        assert_eq!(msg_count, 1, "only the capped message reaches the stream");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn write_ctx_snapshot_writes_all_without_cap() {
        let (mut writer, path) = snapshot_test_writer("snapshot-nocap");
        let entries = sample_ctx_entries(3);
        let written = write_ctx_snapshot(&mut writer, &entries, 42, None).expect("write");
        assert_eq!(written, 3, "no cap writes every captured key");
        assert_eq!(writer.accepted_messages(), 3);
        writer.finish(0, 0).expect("finish");

        let msg_count = read_snapshot_back(&path);
        assert_eq!(msg_count, 3, "all messages reach the stream");
        let _ = std::fs::remove_file(path);
    }

    // A hung snapshot get must NOT stall recording start: the timeout wrapper
    // degrades to best-effort by returning Err, which the caller turns into
    // "warn + continue recording" (the same path as a GET failure).
    #[tokio::test]
    async fn snapshot_get_with_timeout_elapses_to_err() {
        let deadline = Duration::from_millis(20);
        let never = std::future::pending::<Result<Vec<CtxSnapshotEntry>, anyhow::Error>>();
        let res = snapshot_get_with_timeout(deadline, never).await;
        assert!(res.is_err(), "hung get must time out to Err, not block");
    }

    // A fast get must pass through unchanged so the timeout wrapper is
    // transparent on the happy path.
    #[tokio::test]
    async fn snapshot_get_with_timeout_passes_fast_future() {
        let deadline = Duration::from_secs(5);
        let entries = sample_ctx_entries(2);
        let expected = entries.len();
        let res = snapshot_get_with_timeout(deadline, async { Ok(entries) }).await;
        assert_eq!(
            res.expect("fast get must pass through").len(),
            expected,
            "fast get result must be returned unchanged"
        );
    }

    #[test]
    fn validate_queue_capacity_rejects_zero() {
        assert!(validate_queue_capacity(0).is_err());
    }

    #[test]
    fn validate_queue_capacity_accepts_positive_value() {
        assert!(validate_queue_capacity(1).is_ok());
    }
}
