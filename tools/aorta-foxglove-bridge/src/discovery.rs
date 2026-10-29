//! Topic discovery and subscription management.
//!
//! Two discovery channels run in parallel:
//!
//! * **Wildcard subscribers** (`SubscriptionManager`) provide the data
//!   plane. They mirror `aorta-record::build_topic_subscriptions`: one
//!   subscriber per `--include` pattern (defaulting to
//!   `aorta/{group}/pub/**`), plus `aorta/{group}/ctx/**` and
//!   `aorta/{group}/sys/telemetry/**` unconditionally, so every sample
//!   the recorder would capture also reaches the forwarder. Zenoh hands a
//!   sample to every local subscriber whose selector matches it, so when
//!   selectors overlap only the earliest attached one forwards it.
//! * **Admin-space probe** (`run_discovery_loop`) provides the control
//!   plane. It polls `IntrospectClient::list_topics` and calls
//!   `Server::ensure_channel` for every topic returned, so idle but
//!   declared publishers are advertised to Foxglove Studio before
//!   their first sample arrives — the behavior users expect from
//!   `ros2 launch foxglove_bridge`.

use anyhow::{anyhow, Result};
use aorta_core::schema_resolve::SchemaResolver;
use aorta_core::{unix_time_ns, KeyExprBuilder};
use aorta_introspect::client::{IntrospectClient, IntrospectOptions, TopicListOptions};
use aorta_introspect::ids::GroupName;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use zenoh::key_expr::{keyexpr, OwnedKeyExpr};

use crate::config::{DropPolicy, LogLevel, TopicsConfig};
use crate::filter::{build_subscription_key, TopicFilter};
use crate::forwarder::Sample;
use crate::logln;
use crate::server::{ChannelSchema, Server};

pub struct SubscriptionManager {
    session: Arc<zenoh::Session>,
    key_builder: KeyExprBuilder,
    filter: TopicFilter,
    stop_tx: broadcast::Sender<()>,
    drop_policy: DropPolicy,
    dropped: Arc<AtomicU64>,
    handles: Mutex<Attached>,
    log_level: LogLevel,
}

#[derive(Default)]
struct Attached {
    tasks: HashMap<String, JoinHandle<()>>,
    /// Selectors in attach order; decides which subscriber owns a sample.
    order: Vec<OwnedKeyExpr>,
}

impl SubscriptionManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: Arc<zenoh::Session>,
        key_builder: KeyExprBuilder,
        filter: TopicFilter,
        stop_tx: broadcast::Sender<()>,
        drop_policy: DropPolicy,
        dropped: Arc<AtomicU64>,
        log_level: LogLevel,
    ) -> Self {
        Self {
            session,
            key_builder,
            filter,
            stop_tx,
            drop_policy,
            dropped,
            handles: Mutex::new(Attached::default()),
            log_level,
        }
    }

    // `sample_tx` is deliberately a parameter, NOT a manager field: the only
    // long-lived senders must be the per-subscriber task clones, so that when
    // every task exits (e.g. the Zenoh session dies) the forwarder's
    // `sample_rx.recv()` returns None and the run loop notices the dead data
    // plane. A sender parked in this struct would keep the channel open
    // forever and revive the zombie-bridge failure mode.
    pub async fn attach_wildcards(
        &self,
        group: &str,
        topics: &TopicsConfig,
        sample_tx: &mpsc::Sender<Sample>,
    ) -> Result<()> {
        let subscriptions = build_wildcard_subscriptions(group, topics)?;
        if subscriptions.is_empty() {
            return Err(anyhow!("no subscriptions configured"));
        }
        for selector in subscriptions {
            self.attach(&selector, sample_tx).await?;
        }
        Ok(())
    }

    pub async fn attach(&self, selector: &str, sample_tx: &mpsc::Sender<Sample>) -> Result<()> {
        let mut handles = self.handles.lock().await;
        if handles.tasks.contains_key(selector) {
            return Ok(());
        }
        let selector_ke = OwnedKeyExpr::autocanonize(selector.to_string())
            .map_err(|e| anyhow!("invalid subscription selector '{}': {}", selector, e))?;
        let earlier = overlapping_earlier(&handles.order, &selector_ke);

        let subscriber = self
            .session
            .declare_subscriber(selector)
            .await
            .map_err(|e| anyhow!("failed to declare subscriber '{}': {}", selector, e))?;

        logln(
            self.log_level,
            LogLevel::Debug,
            &format!("subscribed to '{}'", selector),
        );

        let sample_tx = sample_tx.clone();
        let filter = self.filter.clone();
        let key_builder = self.key_builder.clone();
        let drop_policy = self.drop_policy;
        let dropped = self.dropped.clone();
        let mut stop_rx = self.stop_tx.subscribe();
        let log_level = self.log_level;
        let selector_owned = selector.to_string();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_rx.recv() => break,
                    sample = subscriber.recv_async() => {
                        let sample = match sample {
                            Ok(sample) => sample,
                            Err(_) => break,
                        };

                        if owned_by_earlier(&earlier, sample.key_expr()) {
                            continue;
                        }

                        let full_key = sample.key_expr().as_str().to_string();
                        let key_suffix = match key_builder.key_suffix_from_full_key(&full_key) {
                            Some(suffix) => suffix,
                            None => continue,
                        };

                        // `blacklist_suffix_keyexpr` is rooted under "pub/"
                        // (recorder semantics); `blacklist_full_keyexpr`
                        // matches the FULL key — including ctx/** and
                        // sys/telemetry/**. The suffix path is a no-op when
                        // topic_suffix is empty.
                        let topic_suffix = key_suffix.strip_prefix("pub/").unwrap_or("");
                        if !filter.accepts(topic_suffix, &full_key) {
                            continue;
                        }

                        let ingest_ns = unix_time_ns();
                        let publish_time_ns = sample
                            .timestamp()
                            .map(|ts| ts.get_time().as_nanos())
                            .unwrap_or(ingest_ns);

                        let attachment_hash = aorta_introspect::topic_schema::extract_attachment_schema_hash(&sample);
                        let event = Sample {
                            full_key,
                            key_suffix,
                            attachment_hash,
                            payload: sample.payload().to_bytes().to_vec(),
                            log_time_ns: ingest_ns,
                            publish_time_ns,
                        };

                        match drop_policy {
                            DropPolicy::Block => {
                                if sample_tx.send(event).await.is_err() {
                                    break;
                                }
                            }
                            DropPolicy::DropNewest => {
                                if sample_tx.try_send(event).is_err() {
                                    dropped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            }
            logln(
                log_level,
                LogLevel::Debug,
                &format!("subscriber task exiting: {}", selector_owned),
            );
        });

        handles.tasks.insert(selector.to_string(), handle);
        handles.order.push(selector_ke);
        Ok(())
    }

    pub async fn shutdown(&self) {
        let mut handles = self.handles.lock().await;
        let drained: Vec<_> = handles.tasks.drain().map(|(_, h)| h).collect();
        handles.order.clear();
        drop(handles);
        for handle in drained {
            let _ = tokio::time::timeout(Duration::from_millis(500), handle).await;
        }
    }
}

/// Selectors attached before `selector` that can match any of its keys.
fn overlapping_earlier(attached: &[OwnedKeyExpr], selector: &keyexpr) -> Vec<OwnedKeyExpr> {
    attached
        .iter()
        .filter(|earlier| earlier.intersects(selector))
        .cloned()
        .collect()
}

/// Zenoh hands a sample to every local subscriber whose selector intersects
/// its key, so an earlier selector that intersects the key has received it
/// too; only the earliest one forwards it.
fn owned_by_earlier(earlier: &[OwnedKeyExpr], key: &keyexpr) -> bool {
    earlier.iter().any(|selector| selector.intersects(key))
}

/// Build the wildcard Zenoh selectors a bridge subscribes to at startup.
/// Matches `aorta-record::build_topic_subscriptions`: `--include` patterns
/// narrow the `pub/**` set only; canonical context and the telemetry
/// streams are always included (blacklist them away if unwanted).
pub fn build_wildcard_subscriptions(group: &str, topics: &TopicsConfig) -> Result<Vec<String>> {
    let mut out = Vec::new();
    if topics.include_suffix.is_empty() {
        out.push(format!("aorta/{}/pub/**", group));
    } else {
        for pattern in &topics.include_suffix {
            out.push(build_subscription_key(group, pattern)?);
        }
    }
    out.push(format!("aorta/{}/ctx/**", group));
    out.push(format!("aorta/{}/sys/telemetry/**", group));
    out.sort();
    out.dedup();
    Ok(out)
}

/// Periodically poll the Zenoh router admin space for declared
/// publishers and pre-advertise a Foxglove channel for each, so idle
/// topics show up in Foxglove Studio without waiting for the first
/// sample. Also refreshes the `ctx/**` snapshot on its own interval so
/// context state is visible without waiting for the next put (a live
/// client that connects late would otherwise stare at empty ctx
/// channels — the recorder solves the same problem with its t0
/// snapshot; a periodic refresh is the live-viewer equivalent).
#[allow(clippy::too_many_arguments)]
pub async fn run_discovery_loop(
    session: Arc<zenoh::Session>,
    group: &str,
    key_builder: KeyExprBuilder,
    server: Arc<Server>,
    resolver: Arc<SchemaResolver>,
    filter: TopicFilter,
    interval: Duration,
    ctx_snapshot_interval: Option<Duration>,
    ctx_snapshot_values: Arc<AtomicU64>,
    mut stop_rx: broadcast::Receiver<()>,
    log_level: LogLevel,
) {
    let group_name = match GroupName::new(group) {
        Ok(g) => g,
        Err(err) => {
            logln(
                log_level,
                LogLevel::Warn,
                &format!(
                    "discovery loop disabled — invalid group '{}': {}",
                    group, err
                ),
            );
            return;
        }
    };

    let options = IntrospectOptions::new(group_name);
    let client = IntrospectClient::from_session(session.clone(), options);

    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // When disabled (interval 0 → None), park the snapshot arm on a
    // far-future timer so the select! stays uniform.
    let mut ctx_tick = tokio::time::interval(
        ctx_snapshot_interval.unwrap_or(Duration::from_secs(60 * 60 * 24 * 365)),
    );
    ctx_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let ctx_enabled = ctx_snapshot_interval.is_some();

    let mut admin_warned = false;
    loop {
        tokio::select! {
            _ = stop_rx.recv() => break,
            _ = tick.tick() => {
                pre_advertise_once(
                    &client,
                    group,
                    server.as_ref(),
                    resolver.as_ref(),
                    &filter,
                    &mut admin_warned,
                    log_level,
                )
                .await;
            }
            _ = ctx_tick.tick(), if ctx_enabled => {
                let logged = snapshot_ctx_once(
                    session.as_ref(),
                    &key_builder,
                    &filter,
                    group,
                    server.as_ref(),
                    log_level,
                )
                .await;
                ctx_snapshot_values.fetch_add(logged, Ordering::Relaxed);
            }
        }
    }
}

/// One wildcard GET over `aorta/{group}/ctx/**`, forwarding each current
/// value onto its (typed, static-`ContextValue`-schema) channel with the
/// snapshot wall-clock as `log_time`. Mirrors the recorder's
/// `capture_ctx_snapshot` GET shape; best-effort — a failed or empty
/// snapshot never disturbs the live path.
async fn snapshot_ctx_once(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    filter: &TopicFilter,
    group: &str,
    server: &Server,
    log_level: LogLevel,
) -> u64 {
    const CTX_SNAPSHOT_DEADLINE: Duration = Duration::from_secs(2);
    let selector = format!("aorta/{}/ctx/**", group);
    let replies = match session
        .get(&selector)
        .target(zenoh::query::QueryTarget::All)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(CTX_SNAPSHOT_DEADLINE)
        .await
    {
        Ok(replies) => replies,
        Err(err) => {
            logln(
                log_level,
                LogLevel::Debug,
                &format!("ctx snapshot get failed: {}", err),
            );
            return 0;
        }
    };

    let mut logged = 0u64;
    let start = std::time::Instant::now();
    loop {
        let remaining = CTX_SNAPSHOT_DEADLINE.saturating_sub(start.elapsed());
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
                // Mirror the live ingress filter: suffix path is a no-op on
                // ctx keys, but blacklist_full applies to snapshots too.
                if !filter.accepts("", &full_key) {
                    continue;
                }
                let payload = sample.payload().to_bytes().to_vec();
                let schema = crate::forwarder::ctx_channel_schema(&payload);
                let snapshot_ns = unix_time_ns();
                match server
                    .forward(&full_key, &key_suffix, &payload, snapshot_ns, schema)
                    .await
                {
                    Ok(_) => logged += 1,
                    Err(err) => {
                        logln(
                            log_level,
                            LogLevel::Debug,
                            &format!("ctx snapshot forward failed for '{}': {}", full_key, err),
                        );
                    }
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }
    logged
}

async fn pre_advertise_once(
    client: &IntrospectClient,
    group: &str,
    server: &Server,
    resolver: &SchemaResolver,
    filter: &TopicFilter,
    admin_warned: &mut bool,
    log_level: LogLevel,
) {
    let snapshot = match client
        .list_topics(TopicListOptions {
            include_candidate_nodes: false,
            node_filter: None,
        })
        .await
    {
        Ok(list) => list,
        Err(err) => {
            // First failure is an operator-visible WARN (an
            // unreachable admin space silently empties discovery); the
            // bridge keeps running because the data plane is independent —
            // router-less peer setups are legitimate, topics then appear
            // on first sample instead of pre-advertising.
            if !*admin_warned {
                *admin_warned = true;
                logln(
                    log_level,
                    LogLevel::Warn,
                    &format!(
                        "admin-space topic discovery unavailable ({}); \
                         pre-advertise disabled — topics appear on first sample. \
                         Check router connectivity / admin-space ACL.",
                        err
                    ),
                );
            } else {
                logln(
                    log_level,
                    LogLevel::Debug,
                    &format!("admin-space topic discovery failed: {}", err),
                );
            }
            return;
        }
    };
    if *admin_warned {
        *admin_warned = false;
        logln(
            log_level,
            LogLevel::Info,
            "admin-space topic discovery recovered; pre-advertise active",
        );
    }

    for topic in snapshot {
        let topic_suffix = topic.topic.as_str();
        if topic_suffix.is_empty() {
            continue;
        }
        let key_suffix = format!("pub/{}", topic_suffix);
        let full_key = format!("aorta/{}/{}", group, key_suffix);
        if !filter.accepts(topic_suffix, &full_key) {
            continue;
        }

        // Shared resolver: positive + negative caches are common with the
        // forwarder, so a resolve that failed on the data path is not
        // re-paid here (and vice versa). A miss pre-advertises raw; the
        // registry upgrades raw → typed when a sample resolves, and never
        // downgrades back (see `server::plan_channel_action`).
        let schema = resolver
            .resolve_by_key(&key_suffix)
            .await
            .ok()
            .map(|resolved| ChannelSchema {
                hash_hex: resolved.schema_hash.to_hex(),
                bfbs: resolved.bfbs,
            });

        match server.ensure_channel(&full_key, &key_suffix, schema).await {
            Ok(true) => {
                logln(
                    log_level,
                    LogLevel::Debug,
                    &format!("pre-advertised foxglove channel for '{}'", full_key),
                );
            }
            Ok(false) => {}
            Err(err) => {
                logln(
                    log_level,
                    LogLevel::Warn,
                    &format!("failed to pre-advertise '{}': {}", full_key, err),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_subscriptions_default_to_pub_ctx_and_telemetry() {
        let topics = TopicsConfig {
            include_suffix: vec![],
            blacklist_suffix: vec![],
            blacklist_full: vec![],
        };
        let subs = build_wildcard_subscriptions("default", &topics).expect("subs");
        assert_eq!(
            subs,
            vec![
                "aorta/default/ctx/**".to_string(),
                "aorta/default/pub/**".to_string(),
                "aorta/default/sys/telemetry/**".to_string(),
            ]
        );
    }

    fn owned(selectors: &[&str]) -> Vec<OwnedKeyExpr> {
        selectors
            .iter()
            .map(|s| OwnedKeyExpr::autocanonize(s.to_string()).unwrap())
            .collect()
    }

    #[test]
    fn overlapping_subscriptions_keep_earlier_owner_only() {
        let attached = owned(&["aorta/default/ctx/**", "aorta/default/pub/**"]);
        let cam = OwnedKeyExpr::autocanonize("aorta/default/pub/cam/*".to_string()).unwrap();
        let earlier = overlapping_earlier(&attached, &cam);
        assert_eq!(earlier, owned(&["aorta/default/pub/**"]));

        let front = keyexpr::new("aorta/default/pub/cam/front").unwrap();
        assert!(owned_by_earlier(&earlier, front));
    }

    #[test]
    fn disjoint_subscriptions_have_no_earlier_owner() {
        let attached = owned(&["aorta/default/ctx/**", "aorta/default/pub/status"]);
        let imu = OwnedKeyExpr::autocanonize("aorta/default/pub/imu_raw".to_string()).unwrap();
        assert!(overlapping_earlier(&attached, &imu).is_empty());
    }

    fn topics(include: &[&str]) -> TopicsConfig {
        TopicsConfig {
            include_suffix: include.iter().map(|s| s.to_string()).collect(),
            blacklist_suffix: vec![],
            blacklist_full: vec![],
        }
    }

    fn manager(
        session: &Arc<zenoh::Session>,
        topics: &TopicsConfig,
    ) -> (SubscriptionManager, broadcast::Sender<()>) {
        let (stop_tx, _) = broadcast::channel(1);
        let mgr = SubscriptionManager::new(
            session.clone(),
            KeyExprBuilder::new("default").unwrap(),
            TopicFilter::from_config(topics).unwrap(),
            stop_tx.clone(),
            DropPolicy::Block,
            Arc::new(AtomicU64::new(0)),
            LogLevel::Error,
        );
        (mgr, stop_tx)
    }

    async fn local_session() -> Arc<zenoh::Session> {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        Arc::new(zenoh::open(config).await.unwrap())
    }

    /// Receives until `expected` samples arrived (or the deadline passed),
    /// then keeps listening briefly so a late duplicate is still counted.
    async fn collect(rx: &mut mpsc::Receiver<Sample>, expected: usize) -> HashMap<String, usize> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while counts.values().sum::<usize>() < expected {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(sample)) => *counts.entry(sample.full_key).or_default() += 1,
                _ => break,
            }
        }
        while let Ok(Some(sample)) =
            tokio::time::timeout(Duration::from_millis(300), rx.recv()).await
        {
            *counts.entry(sample.full_key).or_default() += 1;
        }
        counts
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overlapping_includes_forward_each_sample_once() {
        let session = local_session().await;
        let topics = topics(&["**", "status"]);
        let (mgr, stop_tx) = manager(&session, &topics);
        let (sample_tx, mut sample_rx) = mpsc::channel(64);
        mgr.attach_wildcards("default", &topics, &sample_tx)
            .await
            .unwrap();
        drop(sample_tx);

        for key in [
            "aorta/default/pub/status",
            "aorta/default/pub/my_app/chatter",
        ] {
            for _ in 0..3 {
                session.put(key, b"x".to_vec()).await.unwrap();
            }
        }

        let counts = collect(&mut sample_rx, 6).await;
        let _ = stop_tx.send(());
        mgr.shutdown().await;
        session.close().await.unwrap();

        assert_eq!(counts.get("aorta/default/pub/status"), Some(&3));
        assert_eq!(counts.get("aorta/default/pub/my_app/chatter"), Some(&3));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wildcard_sample_key_is_forwarded_once() {
        let session = local_session().await;
        let topics = topics(&["cam/a", "cam/a$*"]);
        let (mgr, stop_tx) = manager(&session, &topics);
        let (sample_tx, mut sample_rx) = mpsc::channel(64);
        mgr.attach_wildcards("default", &topics, &sample_tx)
            .await
            .unwrap();
        drop(sample_tx);

        session
            .put("aorta/default/pub/cam/a$*", b"x".to_vec())
            .await
            .unwrap();

        let counts = collect(&mut sample_rx, 1).await;
        let _ = stop_tx.send(());
        mgr.shutdown().await;
        session.close().await.unwrap();

        assert_eq!(counts.get("aorta/default/pub/cam/a$*"), Some(&1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reattach_after_shutdown_forwards_again() {
        let session = local_session().await;
        let topics = topics(&[]);
        let (mgr, stop_tx) = manager(&session, &topics);
        let selector = "aorta/default/pub/**";

        let (sample_tx, _first_rx) = mpsc::channel(64);
        mgr.attach(selector, &sample_tx).await.unwrap();
        let _ = stop_tx.send(());
        mgr.shutdown().await;

        let (sample_tx, mut sample_rx) = mpsc::channel(64);
        mgr.attach(selector, &sample_tx).await.unwrap();
        drop(sample_tx);
        session
            .put("aorta/default/pub/status", b"x".to_vec())
            .await
            .unwrap();

        let counts = collect(&mut sample_rx, 1).await;
        let _ = stop_tx.send(());
        mgr.shutdown().await;
        session.close().await.unwrap();

        assert_eq!(counts.get("aorta/default/pub/status"), Some(&1));
    }

    /// Deployment shape: the router ACL rejects the bridge's wide `pub/**`
    /// declaration and accepts the exact one. Samples pulled by the exact
    /// subscriber still reach the wide one locally and are forwarded once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acl_rejected_wide_include_does_not_duplicate() {
        const ROUTER_ACL: &str = r#"{
          mode: "router",
          listen: { endpoints: ["tcp/127.0.0.1:0"] },
          scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
          transport: { shared_memory: { enabled: false } },
          access_control: {
            enabled: true,
            default_permission: "deny",
            rules: [{
              id: "exact_only",
              messages: ["put", "declare_subscriber"],
              flows: ["ingress", "egress"],
              permission: "allow",
              key_exprs: ["aorta/default/pub/status"],
            }],
            subjects: [{ id: "tcp", link_protocols: ["tcp"] }],
            policies: [{ rules: ["exact_only"], subjects: ["tcp"] }],
          },
        }"#;
        let router = zenoh::open(zenoh::Config::from_json5(ROUTER_ACL).unwrap())
            .await
            .unwrap();
        let locator = router
            .info()
            .locators()
            .await
            .into_iter()
            .map(|l| l.to_string())
            .find(|l| l.starts_with("tcp/"))
            .expect("router locator");
        let client = || {
            let config = format!(
                r#"{{ mode: "client", connect: {{ endpoints: ["{locator}"] }},
                     scouting: {{ multicast: {{ enabled: false }} }},
                     transport: {{ shared_memory: {{ enabled: false }} }} }}"#
            );
            zenoh::open(zenoh::Config::from_json5(&config).unwrap())
        };
        let bridge = Arc::new(client().await.unwrap());
        let publisher = client().await.unwrap();
        let wide_only = client().await.unwrap();

        let topics = topics(&["**", "status"]);
        let (mgr, stop_tx) = manager(&bridge, &topics);
        let (sample_tx, mut sample_rx) = mpsc::channel(64);
        mgr.attach_wildcards("default", &topics, &sample_tx)
            .await
            .unwrap();
        drop(sample_tx);

        let local_wide = Arc::new(AtomicU64::new(0));
        let counter = local_wide.clone();
        let _local_wide_sub = bridge
            .declare_subscriber("aorta/default/pub/**")
            .callback(move |_| {
                counter.fetch_add(1, Ordering::Relaxed);
            })
            .await
            .unwrap();
        let remote_wide = Arc::new(AtomicU64::new(0));
        let counter = remote_wide.clone();
        let _remote_wide_sub = wide_only
            .declare_subscriber("aorta/default/pub/**")
            .callback(move |_| {
                counter.fetch_add(1, Ordering::Relaxed);
            })
            .await
            .unwrap();

        // Wait until the exact declaration reached the publisher's side.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let matching = publisher
            .declare_publisher("aorta/default/pub/status")
            .await
            .unwrap();
        while !matching.matching_status().await.unwrap().matching() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "exact subscription never reached the publisher"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for _ in 0..5 {
            matching.put(b"x".to_vec()).await.unwrap();
        }

        let counts = collect(&mut sample_rx, 5).await;
        let _ = stop_tx.send(());
        mgr.shutdown().await;

        assert_eq!(counts.get("aorta/default/pub/status"), Some(&5));
        assert_eq!(
            local_wide.load(Ordering::Relaxed),
            5,
            "the rejected wide selector still sees the samples locally"
        );
        assert_eq!(
            remote_wide.load(Ordering::Relaxed),
            0,
            "the router ACL rejects a wide-only subscriber"
        );
        for session in [publisher, wide_only] {
            session.close().await.unwrap();
        }
        bridge.close().await.unwrap();
        router.close().await.unwrap();
    }

    #[test]
    fn include_list_narrows_pub_but_keeps_ctx_and_telemetry() {
        let topics = TopicsConfig {
            include_suffix: vec!["demo/**".to_string(), "cam/*".to_string()],
            blacklist_suffix: vec![],
            blacklist_full: vec![],
        };
        let subs = build_wildcard_subscriptions("default", &topics).expect("subs");
        assert_eq!(
            subs,
            vec![
                "aorta/default/ctx/**".to_string(),
                "aorta/default/pub/cam/*".to_string(),
                "aorta/default/pub/demo/**".to_string(),
                "aorta/default/sys/telemetry/**".to_string(),
            ]
        );
    }
}
