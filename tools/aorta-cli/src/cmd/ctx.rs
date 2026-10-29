//! `aorta ctx` subcommands. Implements the ContextStore CLI surface:
//!
//! - `ctx list [<prefix>]`     — wildcard get → owner replies grouped by key,
//!                                multi-instance rows tagged with ⚠.
//! - `ctx get <key>`           — single key + full metadata.
//! - `ctx watch <prefix>`      — streaming live events with source_node and
//!                                source_instance_id columns.
//! - `ctx conflicts [<prefix>]` — owner_conflict telemetry stream + multi-instance
//!                                live observations (see `ConflictRow`).
//! - `ctx set <key> <value>`   — debug-only raw Zenoh put bypass.
//!
//! All commands open a single Zenoh session via `crate::cmd::open_session`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use aorta_core::contextstore_fb::{
    encode_value_bool, encode_value_f64, encode_value_i64, encode_value_string, encode_value_u64,
    root_as_context_value, ContextKind, ContextValueMeta,
};
use aorta_core::owner_conflict_fb::root_as_owner_conflict_event;
use aorta_core::KeyExprBuilder;
use uuid::Uuid;
use zenoh::query::{ConsolidationMode, QueryTarget};

// Defaults are wired directly via clap `default_value_t` on the
// CtxCommands enum in main.rs. Kept here for reference if the dispatch
// is ever inverted.
const _DEFAULT_LIST_DEADLINE_MS: u64 = 2_000;
const _DEFAULT_CONFLICTS_WINDOW_MS: u64 = 30_000;

fn key_builder(group: Option<&str>) -> Result<KeyExprBuilder> {
    let g = group.unwrap_or("default");
    KeyExprBuilder::new(g).map_err(|e| anyhow!("invalid group '{}': {}", g, e))
}

fn ctx_root_for_group(group: &str, prefix: Option<&str>) -> String {
    match prefix {
        Some(p) => format!("aorta/{}/ctx/{}", group, p.trim_matches('/')),
        None => format!("aorta/{}/ctx/**", group),
    }
}

#[derive(Clone, Debug)]
struct OwnerReply {
    key: String,
    value_kind: ContextKind,
    bool_value: bool,
    i64_value: i64,
    u64_value: u64,
    f64_value: f64,
    string_value: Option<String>,
    stamp_ns: u64,
    expires_at_ns: u64,
    source_node: String,
    source_instance_id: String,
    sequence: u64,
}

fn parse_reply_payload(key: &str, payload: &[u8]) -> Option<OwnerReply> {
    let parsed = root_as_context_value(payload)?;
    Some(OwnerReply {
        key: key.to_string(),
        value_kind: parsed.kind(),
        bool_value: parsed.bool_value(),
        i64_value: parsed.i64_value(),
        u64_value: parsed.u64_value(),
        f64_value: parsed.f64_value(),
        string_value: parsed.string_value().map(|s| s.to_string()),
        stamp_ns: parsed.stamp_ns(),
        expires_at_ns: parsed.expires_at_ns(),
        source_node: parsed.source_node().unwrap_or("").to_string(),
        source_instance_id: parsed.source_instance_id().unwrap_or("").to_string(),
        sequence: parsed.sequence(),
    })
}

fn render_value(reply: &OwnerReply) -> String {
    match reply.value_kind {
        ContextKind::None => "<none>".to_string(),
        ContextKind::Bool => format!("{}", reply.bool_value),
        ContextKind::I64 => format!("{}", reply.i64_value),
        ContextKind::U64 => format!("{}", reply.u64_value),
        ContextKind::F64 => format!("{}", reply.f64_value),
        ContextKind::String => reply
            .string_value
            .clone()
            .unwrap_or_default()
            .chars()
            .take(64)
            .collect(),
    }
}

fn render_freshness(expires_at_ns: u64, now_ns: u64) -> &'static str {
    if expires_at_ns == 0 {
        "fresh"
    } else if now_ns < expires_at_ns {
        "fresh"
    } else {
        "stale"
    }
}

fn now_ns() -> u64 {
    let ns = aorta_core::time::now_ns();
    if ns < 0 {
        0
    } else {
        ns as u64
    }
}

fn truncate_instance(instance: &str) -> &str {
    let take = instance.len().min(8);
    &instance[..take]
}

pub async fn list(group: Option<&str>, prefix: Option<&str>, deadline_ms: u64) -> Result<()> {
    let session = crate::cmd::open_session().await?;
    let g = group.unwrap_or("default");
    let target = ctx_root_for_group(g, prefix);
    let deadline = Duration::from_millis(deadline_ms);

    let replies = session
        .get(&target)
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .timeout(deadline)
        .await
        .map_err(|e| anyhow!("zenoh get failed: {}", e))?;

    let mut grouped: BTreeMap<String, Vec<OwnerReply>> = BTreeMap::new();
    let start = Instant::now();
    loop {
        // saturating_sub avoids a clock-skew / scheduler-latency window
        // where `start.elapsed() > deadline` would underflow Duration
        // arithmetic between the loop check and the subtraction.
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, replies.recv_async()).await {
            Ok(Ok(reply)) => {
                if let Ok(sample) = reply.result() {
                    let key = sample.key_expr().as_str().to_string();
                    let payload = sample.payload().to_bytes();
                    if let Some(parsed) = parse_reply_payload(&key, &payload) {
                        grouped.entry(parsed.key.clone()).or_default().push(parsed);
                    }
                }
            }
            Ok(Err(_)) | Err(_) => break,
        }
    }

    if grouped.is_empty() {
        println!(
            "No context keys reachable under {} (deadline {} ms).",
            target, deadline_ms
        );
        return Ok(());
    }

    println!(
        "{:50} {:6} {:14} {:24} {:8} {:6}",
        "key", "kind", "value", "source_node", "instance", "fresh"
    );
    let now = now_ns();
    for (_key, replies) in grouped.iter() {
        let conflict = replies.len() > 1
            && replies
                .iter()
                .map(|r| r.source_instance_id.as_str())
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 1;
        for reply in replies {
            let kind = reply.value_kind.name();
            let value = render_value(reply);
            let fresh = render_freshness(reply.expires_at_ns, now);
            let mark = if conflict { " ⚠" } else { "" };
            println!(
                "{:50} {:6} {:14} {:24} {:8} {:6}{}",
                reply.key,
                kind,
                value,
                reply.source_node,
                truncate_instance(&reply.source_instance_id),
                fresh,
                mark
            );
        }
    }
    Ok(())
}

pub async fn get(group: Option<&str>, key: &str, deadline_ms: u64) -> Result<()> {
    let kb = key_builder(group)?;
    let keyexpr = kb
        .context_key(key)
        .map_err(|e| anyhow!("invalid context key '{}': {}", key, e))?;

    let session = crate::cmd::open_session().await?;
    let deadline = Duration::from_millis(deadline_ms);
    let replies = session
        .get(&keyexpr)
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .timeout(deadline)
        .await
        .map_err(|e| anyhow!("zenoh get failed: {}", e))?;

    match tokio::time::timeout(deadline, replies.recv_async()).await {
        Ok(Ok(reply)) => {
            let sample = reply
                .result()
                .map_err(|e| anyhow!("reply error: {}", aorta_core::reply_error_text(e)))?;
            let payload = sample.payload().to_bytes();
            let parsed = parse_reply_payload(sample.key_expr().as_str(), &payload)
                .ok_or_else(|| anyhow!("malformed ContextValue payload"))?;
            print_reply_detail(&parsed);
            Ok(())
        }
        Ok(Err(_)) | Err(_) => {
            println!("No reply for {} within {} ms.", key, deadline_ms);
            Ok(())
        }
    }
}

fn print_reply_detail(reply: &OwnerReply) {
    let now = now_ns();
    println!("key:                 {}", reply.key);
    println!("kind:                {}", reply.value_kind.name());
    println!("value:               {}", render_value(reply));
    println!("source_node:         {}", reply.source_node);
    println!("source_instance_id:  {}", reply.source_instance_id);
    println!("sequence:            {}", reply.sequence);
    println!("stamp_ns:            {}", reply.stamp_ns);
    println!(
        "expires_at_ns:       {} ({})",
        reply.expires_at_ns,
        render_freshness(reply.expires_at_ns, now)
    );
}

pub async fn watch(
    group: Option<&str>,
    prefix: &str,
    count: Option<usize>,
    timeout_secs: Option<u64>,
) -> Result<()> {
    let session = crate::cmd::open_session().await?;
    let g = group.unwrap_or("default");
    let target = ctx_root_for_group(g, Some(prefix));

    let subscriber = session
        .declare_subscriber(&target)
        .await
        .map_err(|e| anyhow!("subscribe failed: {}", e))?;

    eprintln!("Watching {} (Ctrl-C to stop)", target);
    println!(
        "{:24} {:50} {:6} {:14} {:24} {:8} {:6}",
        "stamp_ns", "key", "kind", "value", "source_node", "instance", "seq"
    );
    let deadline = timeout_secs.map(|secs| Instant::now() + Duration::from_secs(secs));
    let mut printed = 0usize;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        // When a deadline is set, race the subscriber recv against the remaining
        // time; otherwise it can only end on Ctrl-C or subscriber close.
        let recv = subscriber.recv_async();
        let res = if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            tokio::select! {
                _ = &mut ctrl_c => break,
                _ = tokio::time::sleep(remaining) => break,
                res = recv => res,
            }
        } else {
            tokio::select! {
                _ = &mut ctrl_c => break,
                res = recv => res,
            }
        };

        match res {
            Ok(sample) => {
                let key = sample.key_expr().as_str().to_string();
                let payload = sample.payload().to_bytes();
                if let Some(reply) = parse_reply_payload(&key, &payload) {
                    println!(
                        "{:24} {:50} {:6} {:14} {:24} {:8} {:6}",
                        reply.stamp_ns,
                        reply.key,
                        reply.value_kind.name(),
                        render_value(&reply),
                        reply.source_node,
                        truncate_instance(&reply.source_instance_id),
                        reply.sequence
                    );
                    printed += 1;
                    if let Some(limit) = count {
                        if printed >= limit {
                            break;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ConflictRow {
    /// Source of the row:
    /// - `true` => emitted as `context.owner_conflict` telemetry by an
    ///   owner that detected a foreign writer.
    /// - `false` => observed as a multi-instance live put on the
    ///   prefix (no owner is declaring the prefix, or the owner just
    ///   hasn't published telemetry yet).
    via_telemetry: bool,
    key: String,
    self_node: String,
    self_instance: String,
    foreign_node: String,
    foreign_instance: String,
    foreign_seq: u64,
}

pub async fn conflicts(group: Option<&str>, prefix: Option<&str>, window_ms: u64) -> Result<()> {
    let session = Arc::new(crate::cmd::open_session().await?);
    let g = group.unwrap_or("default");
    let kb = key_builder(group)?;
    let telemetry_key = kb
        .ctx_owner_conflict_telemetry_key()
        .map_err(|e| anyhow!("telemetry key build failed: {}", e))?;
    let watch_target = ctx_root_for_group(g, prefix);
    // Telemetry events carry the conflicted key as a concrete
    // ContextStore key (e.g. `aorta/{group}/ctx/demo/state`). When the
    // operator scoped the report to a prefix — including a wildcard
    // one like `demo/**` — we keep only the events whose key falls
    // under that scope. Use Zenoh's keyexpr `intersects` so the
    // wildcard semantics match what the live subscription path uses;
    // a naive `starts_with` would drop every event when the prefix
    // contains literal `**`.
    let telemetry_key_filter: Option<zenoh::key_expr::KeyExpr<'static>> = match prefix {
        Some(p) => {
            let s = format!("aorta/{}/ctx/{}", g, p.trim_matches('/'));
            Some(
                zenoh::key_expr::KeyExpr::new(s.clone())
                    .map_err(|e| anyhow!("invalid prefix keyexpr '{}': {}", s, e))?
                    .into_owned(),
            )
        }
        None => None,
    };
    let telemetry_filter_for_task = telemetry_key_filter.clone();

    let telem_sub = session
        .declare_subscriber(&telemetry_key)
        .await
        .map_err(|e| anyhow!("telemetry subscribe failed: {}", e))?;
    let watch_sub = session
        .declare_subscriber(&watch_target)
        .await
        .map_err(|e| anyhow!("watch subscribe failed: {}", e))?;

    let captured: Arc<std::sync::Mutex<Vec<ConflictRow>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let key_to_instances: Arc<
        std::sync::Mutex<BTreeMap<String, std::collections::BTreeSet<String>>>,
    > = Arc::new(std::sync::Mutex::new(BTreeMap::new()));

    eprintln!(
        "Collecting conflicts on {} for {} ms (telemetry={})",
        watch_target, window_ms, telemetry_key
    );

    let deadline = Instant::now() + Duration::from_millis(window_ms);
    let captured_t = Arc::clone(&captured);
    let captured_w = Arc::clone(&captured);
    let key_map_w = Arc::clone(&key_to_instances);

    let telem_task = tokio::spawn(async move {
        loop {
            match telem_sub.recv_async().await {
                Ok(sample) => {
                    let bytes = sample.payload().to_bytes();
                    if let Some(evt) = root_as_owner_conflict_event(&bytes) {
                        let key = evt.key().unwrap_or("").to_string();
                        if let Some(filter) = telemetry_filter_for_task.as_ref() {
                            // The conflicted key carried by telemetry is
                            // a concrete keyexpr; match it against the
                            // scope using Zenoh keyexpr intersects so
                            // wildcard scopes (`demo/**`) work correctly.
                            match zenoh::key_expr::KeyExpr::new(key.as_str()) {
                                Ok(evt_ke) => {
                                    if !filter.intersects(&evt_ke) {
                                        continue;
                                    }
                                }
                                Err(_) => continue,
                            }
                        }
                        captured_t.lock().unwrap().push(ConflictRow {
                            via_telemetry: true,
                            key,
                            self_node: evt.self_node().unwrap_or("").to_string(),
                            self_instance: evt.self_instance_id().unwrap_or("").to_string(),
                            foreign_node: evt.foreign_node().unwrap_or("").to_string(),
                            foreign_instance: evt.foreign_instance_id().unwrap_or("").to_string(),
                            foreign_seq: evt.foreign_seq(),
                        });
                    }
                }
                Err(_) => break,
            }
        }
    });

    let watch_task = tokio::spawn(async move {
        loop {
            match watch_sub.recv_async().await {
                Ok(sample) => {
                    let key = sample.key_expr().as_str().to_string();
                    let bytes = sample.payload().to_bytes();
                    if let Some(parsed) = root_as_context_value(&bytes) {
                        let instance = parsed.source_instance_id().unwrap_or("").to_string();
                        if instance.is_empty() {
                            continue;
                        }
                        let mut map = key_map_w.lock().unwrap();
                        let set = map.entry(key.clone()).or_default();
                        let inserted = set.insert(instance.clone());
                        if inserted && set.len() >= 2 {
                            captured_w.lock().unwrap().push(ConflictRow {
                                via_telemetry: false,
                                key,
                                self_node: String::new(),
                                self_instance: String::new(),
                                foreign_node: parsed.source_node().unwrap_or("").to_string(),
                                foreign_instance: instance,
                                foreign_seq: parsed.sequence(),
                            });
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });

    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    telem_task.abort();
    watch_task.abort();
    // Awaiting JoinError from `abort` is best-effort: we want the
    // task to release its subscriber + finish any in-flight push to
    // the captured Vec before we clone it for printing.
    let _ = telem_task.await;
    let _ = watch_task.await;

    let events = captured.lock().unwrap().clone();
    if events.is_empty() {
        println!("No conflicts observed.");
        return Ok(());
    }
    println!(
        "{:50} {:9} {:24} {:8} {:24} {:8} {:5}",
        "key", "source", "self_node", "self_id", "foreign_node", "foreign_id", "f_seq"
    );
    for row in &events {
        let source = if row.via_telemetry {
            "telemetry"
        } else {
            "observed"
        };
        println!(
            "{:50} {:9} {:24} {:8} {:24} {:8} {:5}",
            row.key,
            source,
            if row.self_node.is_empty() {
                "-".to_string()
            } else {
                row.self_node.clone()
            },
            if row.self_instance.is_empty() {
                "-".to_string()
            } else {
                truncate_instance(&row.self_instance).to_string()
            },
            row.foreign_node,
            truncate_instance(&row.foreign_instance),
            row.foreign_seq
        );
    }
    Ok(())
}

pub async fn set(group: Option<&str>, key: &str, kind: &str, value: &str) -> Result<()> {
    // Raw zenoh put bypass with synthetic ContextValue.
    let kb = key_builder(group)?;
    let keyexpr = kb
        .context_key(key)
        .map_err(|e| anyhow!("invalid context key '{}': {}", key, e))?;
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".to_string());
    let source_node = format!("cli@{}", host);
    let instance = Uuid::new_v4().simple().to_string()[..16].to_string();
    let stamp = now_ns();
    let meta = ContextValueMeta {
        stamp_ns: stamp,
        expires_at_ns: 0,
        source_node: source_node.clone(),
        source_instance_id: instance.clone(),
        sequence: 0,
    };

    let payload = match kind.to_ascii_lowercase().as_str() {
        "bool" => {
            let v: bool = value
                .parse()
                .map_err(|_| anyhow!("'{}' is not a valid bool", value))?;
            encode_value_bool(v, &meta)
        }
        "i64" => {
            let v: i64 = value
                .parse()
                .map_err(|_| anyhow!("'{}' is not a valid i64", value))?;
            encode_value_i64(v, &meta)
        }
        "u64" => {
            let v: u64 = value
                .parse()
                .map_err(|_| anyhow!("'{}' is not a valid u64", value))?;
            encode_value_u64(v, &meta)
        }
        "f64" => {
            let v: f64 = value
                .parse()
                .map_err(|_| anyhow!("'{}' is not a valid f64", value))?;
            encode_value_f64(v, &meta)
        }
        "string" => encode_value_string(value, &meta),
        other => bail!(
            "unknown kind '{}'; expected one of bool|i64|u64|f64|string",
            other
        ),
    };

    let session = crate::cmd::open_session().await?;
    session
        .put(&keyexpr, payload)
        .await
        .map_err(|e| anyhow!("zenoh put failed: {}", e))?;
    println!(
        "Put {} = {} (kind={}, source_instance_id={}). Note: real owners under this prefix will detect this as a context.owner_conflict.",
        keyexpr.as_str(),
        value,
        kind,
        instance
    );
    Ok(())
}
