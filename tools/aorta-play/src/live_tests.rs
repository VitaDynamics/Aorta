//! MCAP Player Proposal §7 item 4 integration suite — fixture →
//! [`run_with_shutdown`] → in-process subscriber, single process, real zenoh.
//!
//! Isolation: every test opens its own in-process zenoh **router** on an
//! ephemeral loopback port with multicast and gossip disabled, and every
//! participant (the player's node via `--zenoh-config`, the subscriber node,
//! any fake schema responder) connects to that router as a client. Groups and
//! topics are unique per test, so nothing crosses between tests or between
//! developer machines on a shared LAN.
//!
//! Timing: assertions are one-sided and generous (the defiant-drain flake
//! rule) — "at least the recorded span ÷ rate", never an upper bound on
//! scheduling.

use crate::fixture;
use crate::group::ENV_AORTA_GROUP;
#[cfg(feature = "fault-injection")]
use crate::live::{
    TEST_CLOSE_BEFORE_REPLAY_ENV, TEST_NODE_CLOSE_ERROR_ENV, TEST_NODE_ERROR_AFTER_CANCEL_ENV,
    TEST_NODE_ERROR_ARMED, TEST_NODE_OPEN_STALL_ARMED, TEST_PUBLISHER_DELAY_MS_ENV,
    TEST_PUBLISHER_ERROR_AFTER_CANCEL_ENV, TEST_PUBLISHER_ERROR_ARMED, TEST_STALL_NODE_CLOSE_ENV,
    TEST_STALL_NODE_OPEN_ENV, TEST_STALL_PUBLISHER_ENV, TEST_TEARDOWN_DEADLINE_MS_ENV,
};
#[cfg(feature = "fault-injection")]
use crate::plan::TEST_PLAN_STALL;
#[cfg(feature = "fault-injection")]
use crate::snapshot::TEST_COPY_STALL;
use crate::{run_with_shutdown, PlayerConfig, Termination};
#[cfg(feature = "fault-injection")]
use crate::{
    TEST_PASS0_ERROR_AFTER_CANCEL_ENV, TEST_PASS0_ERROR_ARMED, TEST_PASS1_ERROR_AFTER_CANCEL_ENV,
    TEST_PASS1_ERROR_ARMED,
};
use aorta_core::node::Node;
use aorta_core::schema::SchemaHash;
use aorta_core::schema_registry::SchemaByKeyValue;
use aorta_core::telemetry::fb::{TELEMETRY_NODE_IDENTITY_BFBS, TELEMETRY_RUNTIME_METRICS_BFBS};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use zenoh::config::WhatAmI;
#[cfg(feature = "fault-injection")]
use zenoh::query::{ConsolidationMode, QueryTarget};
use zenoh::Wait;

const ROUTER_CONFIG: &str = r#"
{
  mode: "router",
  listen: { endpoints: ["tcp/127.0.0.1:0"] },
  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
  transport: { shared_memory: { enabled: false } }
}
"#;

const TOPIC_SUFFIX: &str = "sensor/imu";
const SCHEMA_BFBS: &[u8] = TELEMETRY_NODE_IDENTITY_BFBS;
const SCHEMA_NAME: &str = "aorta.telemetry.NodeIdentity";

fn unique(tag: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "p{}t{}{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
        tag
    )
}

/// Router + scratch directory + a per-test group, torn down on drop.
struct Harness {
    dir: PathBuf,
    group: String,
    config_path: PathBuf,
    locator: String,
    _router: zenoh::Session,
}

impl Harness {
    async fn new(tag: &str) -> Self {
        let mut config = zenoh::Config::from_json5(ROUTER_CONFIG).expect("router config");
        config.set_mode(Some(WhatAmI::Router)).expect("router mode");
        let router = zenoh::open(config).await.expect("router session");

        let deadline = Instant::now() + Duration::from_secs(10);
        let locator = loop {
            let locators = router.info().locators().await;
            if let Some(found) = locators
                .iter()
                .map(|locator| locator.to_string())
                .find(|locator| locator.starts_with("tcp/"))
            {
                break found;
            }
            assert!(
                Instant::now() < deadline,
                "router never published a locator"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        let group = unique(tag);
        let dir = std::env::temp_dir().join(format!("aorta-play-live-{group}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let config_path = dir.join("zenoh.json5");
        std::fs::write(&config_path, client_config_text(&locator)).expect("client config");

        Self {
            dir,
            group,
            config_path,
            locator,
            _router: router,
        }
    }

    fn client_config(&self) -> zenoh::Config {
        zenoh::Config::from_json5(&client_config_text(&self.locator)).expect("client config")
    }

    fn topic_key(&self) -> String {
        format!("aorta/{}/pub/{}", self.group, TOPIC_SUFFIX)
    }

    fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.dir.join(name);
        std::fs::write(&path, bytes).expect("fixture");
        path
    }

    /// A typed single-topic recording with the given `log_time`s.
    fn typed_fixture(&self, name: &str, log_times: &[u64]) -> PathBuf {
        let mut builder = fixture::FixtureBuilder::new(fixture::FixtureOptions::default())
            .recorder_start(&self.group, 1_000, None)
            .typed_channel(&self.topic_key(), SCHEMA_NAME, SCHEMA_BFBS);
        for (index, log_time) in log_times.iter().enumerate() {
            builder = builder.message(0, *log_time, format!("payload-{index}").as_bytes());
        }
        self.write(name, &builder.build())
    }

    fn raw_fixture(&self, name: &str, log_times: &[u64]) -> PathBuf {
        let mut builder = fixture::FixtureBuilder::new(fixture::FixtureOptions::default())
            .recorder_start(&self.group, 1_000, None)
            .raw_channel(&self.topic_key());
        for (index, log_time) in log_times.iter().enumerate() {
            builder = builder.message(0, *log_time, format!("raw-{index}").as_bytes());
        }
        self.write(name, &builder.build())
    }

    fn config(&self, input: PathBuf) -> PlayerConfig {
        PlayerConfig {
            inputs: vec![input],
            zenoh_config: Some(self.config_path.clone()),
            // Nothing in this suite depends on the barrier; keep it short so a
            // subscriber-less test does not sit for the 5 s default.
            match_timeout_secs: 1.0,
            preflight_timeout_secs: 2.0,
            ..PlayerConfig::default()
        }
    }

    /// A subscriber node in this harness's group, collecting payloads verbatim.
    async fn subscriber(&self) -> (Node, Arc<Mutex<Vec<Vec<u8>>>>) {
        let _group = EnvVar::set(ENV_AORTA_GROUP, &self.group);
        let node = Node::with_zenoh_config(unique("sub"), &self.group, self.client_config())
            .await
            .expect("subscriber node");
        let received = Arc::new(Mutex::new(Vec::new()));
        let sink = received.clone();
        let subscriber = node
            .create_subscriber(TOPIC_SUFFIX, move |payload, _context| {
                sink.lock()
                    .expect("received")
                    .push(payload.to_bytes().to_vec());
            })
            .await
            .expect("subscriber");
        // The subscriber must outlive the run; leak it into the node's lifetime.
        std::mem::forget(subscriber);
        (node, received)
    }

    /// Declare a fake `schema_by_key` responder for this harness's topic.
    async fn schema_responder(&self, bfbs: &[u8], skip_first_query: bool) -> ResponderHandle {
        let session = zenoh::open(self.client_config())
            .await
            .expect("responder session");
        let key = format!(
            "aorta/{}/sys/schema_by_key/pub/{}",
            self.group, TOPIC_SUFFIX
        );
        let hash = SchemaHash::from_bfbs_bytes(bfbs);
        let payload = SchemaByKeyValue::topic(hash, bfbs.to_vec(), None).encode();
        let reply_key = key.clone();
        let skip = Arc::new(AtomicBool::new(skip_first_query));
        let queries = Arc::new(Mutex::new(Vec::new()));
        let queryable = session
            .declare_queryable(&key)
            .callback(move |query| {
                if skip.swap(false, Ordering::SeqCst) {
                    // Dropped without a reply: the query finalizes cleanly, so
                    // the first scan sees an honest "unclaimed".
                    return;
                }
                let _ = query.reply(reply_key.clone(), payload.clone()).wait();
            })
            .await
            .expect("queryable");
        std::mem::forget(queryable);
        ResponderHandle {
            _session: session,
            _queries: queries,
        }
    }

    /// A responder that never answers and never lets its query finalize —
    /// the MCAP Player Proposal §6.3 "deadline before drain" shape.
    async fn stalling_responder(&self) -> ResponderHandle {
        let session = zenoh::open(self.client_config())
            .await
            .expect("responder session");
        let key = format!(
            "aorta/{}/sys/schema_by_key/pub/{}",
            self.group, TOPIC_SUFFIX
        );
        let queries: Arc<Mutex<Vec<zenoh::query::Query>>> = Arc::new(Mutex::new(Vec::new()));
        let held = queries.clone();
        let queryable = session
            .declare_queryable(&key)
            .callback(move |query| held.lock().expect("held").push(query))
            .await
            .expect("queryable");
        std::mem::forget(queryable);
        ResponderHandle {
            _session: session,
            _queries: queries,
        }
    }

    #[cfg(feature = "fault-injection")]
    async fn schema_state(&self) -> (Vec<String>, Vec<String>) {
        let session = zenoh::open(self.client_config())
            .await
            .expect("schema observer");
        let by_key_selector = format!(
            "aorta/{}/sys/schema_by_key/pub/{}",
            self.group, TOPIC_SUFFIX
        );
        let by_key_replies = session
            .get(&by_key_selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(Duration::from_millis(500))
            .await
            .expect("schema query");
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut by_key_hashes = Vec::new();
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, by_key_replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    if let Ok(sample) = reply.into_result() {
                        if let Some(value) =
                            SchemaByKeyValue::decode(sample.payload().to_bytes().as_ref())
                        {
                            by_key_hashes.push(value.hash().to_hex());
                        }
                    }
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
        let expected_hash = SchemaHash::from_bfbs_bytes(SCHEMA_BFBS).to_hex();
        let by_hash_selector = format!("aorta/sys/schema/{expected_hash}");
        let by_hash_replies = session
            .get(&by_hash_selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(Duration::from_millis(500))
            .await
            .expect("schema by-hash query");
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut by_hash_hashes = Vec::new();
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, by_hash_replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    if let Ok(sample) = reply.into_result() {
                        if SchemaHash::from_bfbs_bytes(sample.payload().to_bytes().as_ref())
                            .to_hex()
                            == expected_hash
                        {
                            by_hash_hashes.push(expected_hash.clone());
                        }
                    }
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
        let _ = session.close().await;
        (by_key_hashes, by_hash_hashes)
    }

    #[cfg(feature = "fault-injection")]
    async fn first_scan_unclaimed_then_stall(&self) -> ResponderHandle {
        let session = zenoh::open(self.client_config())
            .await
            .expect("responder session");
        let key = format!(
            "aorta/{}/sys/schema_by_key/pub/{}",
            self.group, TOPIC_SUFFIX
        );
        let queries: Arc<Mutex<Vec<zenoh::query::Query>>> = Arc::new(Mutex::new(Vec::new()));
        let held = queries.clone();
        let calls = Arc::new(AtomicU64::new(0));
        let call_count = calls.clone();
        let queryable = session
            .declare_queryable(&key)
            .callback(move |query| {
                if call_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    return;
                }
                held.lock().expect("held").push(query);
            })
            .await
            .expect("queryable");
        std::mem::forget(queryable);
        ResponderHandle {
            _session: session,
            _queries: queries,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct ResponderHandle {
    _session: zenoh::Session,
    _queries: Arc<Mutex<Vec<zenoh::query::Query>>>,
}

fn client_config_text(locator: &str) -> String {
    format!(
        r#"
{{
  mode: "client",
  connect: {{ endpoints: ["{locator}"] }},
  scouting: {{ multicast: {{ enabled: false }}, gossip: {{ enabled: false }} }},
  transport: {{ shared_memory: {{ enabled: false }} }}
}}
"#
    )
}

/// Scoped environment variable, restored on drop.
struct EnvVar {
    key: &'static str,
    prior: Option<String>,
}

impl EnvVar {
    fn set(key: &'static str, value: &str) -> Self {
        let prior = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, prior }
    }
}

impl Drop for EnvVar {
    fn drop(&mut self) {
        match self.prior.take() {
            Some(prior) => std::env::set_var(self.key, prior),
            None => std::env::remove_var(self.key),
        }
    }
}

async fn wait_for(received: &Arc<Mutex<Vec<Vec<u8>>>>, count: usize, bound: Duration) {
    let deadline = Instant::now() + bound;
    loop {
        if received.lock().expect("received").len() >= count || Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Delivery, ordering, summary, group restoration
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_typed_recording_replays_byte_identically_and_in_order() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("delivery").await;
    let input = harness.typed_fixture("a.mcap", &[1_000_000, 2_000_000, 3_000_000]);
    let (node, received) = harness.subscriber().await;

    // The player owns AORTA_GROUP only for the duration of the run (MCAP Player
    // Proposal §6.9).
    let _sentinel = EnvVar::set(ENV_AORTA_GROUP, "play_test_sentinel");
    let outcome = run_with_shutdown(harness.config(input), CancellationToken::new())
        .await
        .expect("a clean replay is not an error");

    assert_eq!(outcome.termination, Termination::Completed);
    assert_eq!(outcome.summary.published_messages, 3);
    assert_eq!(
        outcome
            .summary
            .published_per_channel
            .get(&harness.topic_key())
            .copied(),
        Some(3),
        "per-channel counts are keyed by the recorded key"
    );
    assert_eq!(outcome.summary.publish_errors, 0);
    assert_eq!(outcome.summary.clamped_messages, 0);
    assert!(
        outcome.summary.unmatched_topics.is_empty(),
        "the subscriber was up before the barrier: {:?}",
        outcome.summary.unmatched_topics
    );
    assert_eq!(
        std::env::var(ENV_AORTA_GROUP).ok().as_deref(),
        Some("play_test_sentinel"),
        "AORTA_GROUP is restored once teardown completes (MCAP Player Proposal §6.9)"
    );

    wait_for(&received, 3, Duration::from_secs(10)).await;
    let payloads = received.lock().expect("received").clone();
    assert_eq!(
        payloads,
        vec![
            b"payload-0".to_vec(),
            b"payload-1".to_vec(),
            b"payload-2".to_vec()
        ],
        "verbatim bytes, in recorded order (MCAP Player Proposal §6.11)"
    );
    node.close().await.expect("close subscriber node");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_replay_is_paced_by_the_effective_timeline_and_the_rate() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("pacing").await;
    // 600 ms of recorded time at rate 2.0 → at least ~300 ms of wall time.
    let input = harness.typed_fixture("pace.mcap", &[0, 300_000_000, 600_000_000]);
    let mut config = harness.config(input);
    config.rate = 2.0;
    config.match_timeout_secs = 0.0;

    let started = Instant::now();
    let outcome = run_with_shutdown(config, CancellationToken::new())
        .await
        .expect("replay");
    let elapsed = started.elapsed();

    assert_eq!(outcome.summary.published_messages, 3);
    assert!(
        elapsed >= Duration::from_millis(250),
        "the replay must not run ahead of its schedule (recorded span 600 ms ÷ rate 2 = \
         300 ms), took {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topics_with_no_subscriber_are_reported_and_replayed_anyway() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("unmatched").await;
    let input = harness.typed_fixture("u.mcap", &[1_000, 2_000]);
    let mut config = harness.config(input);
    config.match_timeout_secs = 0.2;

    let outcome = run_with_shutdown(config, CancellationToken::new())
        .await
        .expect("an unmatched topic is not an error (MCAP Player Proposal §6.4)");
    assert_eq!(outcome.termination, Termination::Completed);
    assert_eq!(
        outcome.summary.unmatched_topics,
        vec![harness.topic_key()],
        "the barrier proves nothing beyond ≥1 subscriber; unmatched topics are reported"
    );
    assert_eq!(outcome.summary.published_messages, 2, "fire anyway");
}

// ---------------------------------------------------------------------------
// Queryable gate (MCAP Player Proposal §6.3)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_replay_fails_closed_when_the_schema_queryables_are_disabled() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("gate").await;
    let input = harness.typed_fixture("g.mcap", &[1_000]);
    let _disabled = EnvVar::set("AORTA_SCHEMA_QUERYABLE", "0");
    let _sentinel = EnvVar::set(ENV_AORTA_GROUP, "play_test_sentinel");

    let err = run_with_shutdown(harness.config(input), CancellationToken::new())
        .await
        .expect_err("typed replay with disabled queryables must fail closed");
    let text = format!("{err:#}");
    assert!(text.contains("AORTA_SCHEMA_QUERYABLE"), "got: {text}");
    assert_eq!(
        std::env::var(ENV_AORTA_GROUP).ok().as_deref(),
        Some("play_test_sentinel"),
        "the gate trips before the group is ever taken over (MCAP Player Proposal §6.3)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_only_replay_still_proceeds_with_the_queryables_disabled() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("rawgate").await;
    let input = harness.raw_fixture("r.mcap", &[1_000, 2_000]);
    let _disabled = EnvVar::set("AORTA_SCHEMA_QUERYABLE", "0");

    let mut config = harness.config(input);
    config.match_timeout_secs = 0.0;
    let outcome = run_with_shutdown(config, CancellationToken::new())
        .await
        .expect("raw-only replay has nothing to discover (MCAP Player Proposal §6.3)");
    assert_eq!(outcome.termination, Termination::Completed);
    assert_eq!(outcome.summary.published_messages, 2);
}

// ---------------------------------------------------------------------------
// Schema preflight (MCAP Player Proposal §6.3)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_foreign_schema_on_the_same_key_fails_the_first_scan() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("conflict").await;
    let input = harness.typed_fixture("c.mcap", &[1_000]);
    // Two responders on one key, one of them serving a different schema:
    // `ConsolidationMode::None` is what keeps the second one visible.
    let _honest = harness.schema_responder(SCHEMA_BFBS, false).await;
    let _foreign = harness
        .schema_responder(TELEMETRY_RUNTIME_METRICS_BFBS, false)
        .await;
    let _sentinel = EnvVar::set(ENV_AORTA_GROUP, "play_test_sentinel");

    let err = run_with_shutdown(harness.config(input), CancellationToken::new())
        .await
        .expect_err("a conflicting live schema is a hard error");
    let text = format!("{err:#}");
    assert_eq!(
        std::env::var(ENV_AORTA_GROUP).ok().as_deref(),
        Some("play_test_sentinel"),
        "a startup error after the group takeover still restores it (MCAP Player Proposal §6.9)"
    );
    assert!(
        text.contains("already claimed by a different schema"),
        "got: {text}"
    );
    assert!(
        text.contains(&SchemaHash::from_bfbs_bytes(TELEMETRY_RUNTIME_METRICS_BFBS).to_hex())
            && text.contains(&SchemaHash::from_bfbs_bytes(SCHEMA_BFBS).to_hex()),
        "the error names both hashes: {text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_responder_appearing_after_the_first_scan_fails_the_second() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("divergence").await;
    let input = harness.typed_fixture("d.mcap", &[1_000]);
    // Silent for the first scan (unclaimed → proceed), answering with a foreign
    // schema for the second — the narrowed race MCAP Player Proposal §6.3
    // exists to catch.
    let _late = harness
        .schema_responder(TELEMETRY_RUNTIME_METRICS_BFBS, true)
        .await;

    let err = run_with_shutdown(harness.config(input), CancellationToken::new())
        .await
        .expect_err("second-scan divergence is a hard error");
    let text = format!("{err:#}");
    assert!(
        text.contains("second scan") && text.contains("already claimed by a different schema"),
        "got: {text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_that_never_drains_is_inconclusive_not_unclaimed() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("stall").await;
    let input = harness.typed_fixture("s.mcap", &[1_000]);
    let _stalling = harness.stalling_responder().await;
    let mut config = harness.config(input);
    config.preflight_timeout_secs = 0.5;

    let err = run_with_shutdown(config, CancellationToken::new())
        .await
        .expect_err("an undrained preflight is never read as 'unclaimed'");
    let text = format!("{err:#}");
    assert!(text.contains("inconclusive"), "got: {text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_during_preflight_get_and_drain_returns_interrupted() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("cancelpreflight").await;
    let input = harness.typed_fixture("cancel-preflight.mcap", &[1_000]);
    let _stalling = harness.stalling_responder().await;
    let mut config = harness.config(input);
    config.preflight_timeout_secs = 5.0;
    let token = CancellationToken::new();
    let canceller = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        canceller.cancel();
    });

    // No wall-clock bound around the whole run: harness start-up on a loaded
    // runner can alone exceed a fixed budget. The stalling responder never
    // drains, so if cancellation did not win, preflight would run into its
    // own 5 s timeout and return an "inconclusive" error, not Interrupted.
    let outcome = run_with_shutdown(config, token)
        .await
        .expect("preflight cancellation is an interrupted outcome");
    assert_eq!(outcome.termination, Termination::Interrupted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_during_matching_returns_interrupted() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("cancelmatching").await;
    let input = harness.raw_fixture("cancel-matching.mcap", &[1_000]);
    let mut config = harness.config(input);
    config.match_timeout_secs = 5.0;
    let token = CancellationToken::new();
    let canceller = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        canceller.cancel();
    });

    // Same reasoning as the preflight case above: without a responder the
    // matcher would run into its own 5 s timeout and return a non-Interrupted
    // error, so the Interrupted assertion alone proves cancellation won.
    let outcome = run_with_shutdown(config, token)
        .await
        .expect("matching cancellation is an interrupted outcome");
    assert_eq!(outcome.termination, Termination::Interrupted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_during_a_long_already_due_run_returns_interrupted() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("cancelduerun").await;
    let input = harness.raw_fixture("cancel-due.mcap", &vec![1_000; 20_000]);
    let mut config = harness.config(input);
    config.match_timeout_secs = 0.0;
    config.loop_playback = true;
    let token = CancellationToken::new();
    let progress = Arc::new(crate::ProgressHandle::new());

    let mut run = Box::pin(crate::run_with_shutdown_reporting(
        config,
        token.clone(),
        progress.clone(),
    ));
    // Startup is outside the cancellation contract. Wait for a publish with
    // no startup deadline, and keep the observer in this task so it cannot
    // panic independently and leave a looping player behind. Bazel's suite
    // watchdog catches a replay loop that fails to observe cancellation.
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = async {
            while progress.snapshot().published == 0 {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    token.cancel();
    let outcome = run
        .await
        .expect("already-due cancellation is an interrupted outcome");
    assert_eq!(outcome.termination, Termination::Interrupted);
    assert!(
        outcome.summary.published_messages > 0,
        "the cancellation must land during replay, not before the first publish"
    );
}

// ---------------------------------------------------------------------------
// Cancellation (MCAP Player Proposal §6.7)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_during_the_paced_replay_returns_interrupted() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("cancel").await;
    // The next message is an hour away, beyond the suite watchdog. Once the
    // first publish is observed, only cancellation can finish this paced run.
    let input = harness.typed_fixture("x.mcap", &[0, 3_600_000_000_000, 7_200_000_000_000]);
    let mut config = harness.config(input);
    config.match_timeout_secs = 0.0;

    let token = CancellationToken::new();
    let progress = Arc::new(crate::ProgressHandle::new());
    let mut run = Box::pin(crate::run_with_shutdown_reporting(
        config,
        token.clone(),
        progress.clone(),
    ));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = async {
            while progress.snapshot().published == 0 {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    token.cancel();
    let outcome = run.await.expect("cancellation is not an error");

    assert_eq!(outcome.termination, Termination::Interrupted);
    assert_eq!(outcome.termination.exit_code(), 130);
    assert_eq!(
        outcome.summary.published_messages, 1,
        "the first message is due immediately; the rest are beyond the suite watchdog"
    );
    assert!(
        !outcome.summary.teardown.deadline_expired,
        "teardown after a cancelled replay stays inside its deadline"
    );
    assert!(!crate::is_tainted(), "a clean teardown never taints");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_before_the_node_leaves_the_group_untouched() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("cancelnode").await;
    let input = harness.typed_fixture("n.mcap", &[1_000]);
    let _sentinel = EnvVar::set(ENV_AORTA_GROUP, "play_test_sentinel");

    let token = CancellationToken::new();
    token.cancel();
    let outcome = run_with_shutdown(harness.config(input), token)
        .await
        .expect("cancellation is not an error");
    assert_eq!(outcome.termination, Termination::Interrupted);
    assert_eq!(
        std::env::var(ENV_AORTA_GROUP).ok().as_deref(),
        Some("play_test_sentinel"),
        "every completed-teardown path restores AORTA_GROUP (MCAP Player Proposal §6.9)"
    );
}

// ---------------------------------------------------------------------------
// Managed ownership fault injection (MCAP Player Proposal §7 item 4)
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
pub(crate) struct ThreadStall {
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(feature = "fault-injection")]
impl ThreadStall {
    pub(crate) fn wait(self) {
        let _ = self.entered.send(());
        // Stay blocked until the test observes detachment. Dropping the sender
        // also releases the worker if the test panics.
        let _ = self.release.recv();
    }
}

#[cfg(feature = "fault-injection")]
async fn wait_for_fault_arm(flag: &'static AtomicBool) {
    // Generous on purpose: loaded CI runners can take seconds to reach the
    // fault hook; a longer ceiling only delays failure reporting.
    tokio::time::timeout(Duration::from_secs(10), async {
        while !flag.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("fault hook did not arm");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn pass_zero_hard_error_wins_over_cancellation() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("pass0error").await;
    let input = harness.raw_fixture("pass0-error.mcap", &[1_000]);
    let _hook = EnvVar::set(TEST_PASS0_ERROR_AFTER_CANCEL_ENV, "1");
    TEST_PASS0_ERROR_ARMED.store(false, Ordering::SeqCst);
    let token = CancellationToken::new();
    let mut run = Box::pin(run_with_shutdown(harness.config(input), token.clone()));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = wait_for_fault_arm(&TEST_PASS0_ERROR_ARMED) => {}
    }
    token.cancel();
    let err = run
        .await
        .expect_err("pass-0 hard error must outrank Interrupted");
    assert!(err.to_string().contains("pass-0 hard error"), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn pass_one_hard_error_wins_over_cancellation() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("pass1error").await;
    let input = harness.raw_fixture("pass1-error.mcap", &[1_000]);
    let _hook = EnvVar::set(TEST_PASS1_ERROR_AFTER_CANCEL_ENV, "1");
    TEST_PASS1_ERROR_ARMED.store(false, Ordering::SeqCst);
    let token = CancellationToken::new();
    let mut run = Box::pin(run_with_shutdown(harness.config(input), token.clone()));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = wait_for_fault_arm(&TEST_PASS1_ERROR_ARMED) => {}
    }
    token.cancel();
    let err = run
        .await
        .expect_err("pass-1 hard error must outrank Interrupted");
    assert!(err.to_string().contains("pass-1 hard error"), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn node_construction_hard_error_wins_over_cancellation() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("nodeerror").await;
    let input = harness.raw_fixture("node-error.mcap", &[1_000]);
    let _hook = EnvVar::set(TEST_NODE_ERROR_AFTER_CANCEL_ENV, "1");
    TEST_NODE_ERROR_ARMED.store(false, Ordering::SeqCst);
    let token = CancellationToken::new();
    let mut run = Box::pin(run_with_shutdown(harness.config(input), token.clone()));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = wait_for_fault_arm(&TEST_NODE_ERROR_ARMED) => {}
    }
    token.cancel();
    let err = run
        .await
        .expect_err("node construction error must outrank Interrupted");
    assert!(
        err.to_string().contains("node construction hard error"),
        "got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn publisher_construction_hard_error_wins_over_cancellation() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("publishererror").await;
    let input = harness.typed_fixture("publisher-error.mcap", &[1_000]);
    let _hook = EnvVar::set(TEST_PUBLISHER_ERROR_AFTER_CANCEL_ENV, "1");
    TEST_PUBLISHER_ERROR_ARMED.store(false, Ordering::SeqCst);
    let token = CancellationToken::new();
    let mut run = Box::pin(run_with_shutdown(harness.config(input), token.clone()));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = wait_for_fault_arm(&TEST_PUBLISHER_ERROR_ARMED) => {}
    }
    token.cancel();
    let err = run
        .await
        .expect_err("publisher construction error must outrank Interrupted");
    assert!(
        err.to_string()
            .contains("publisher construction hard error"),
        "got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn node_close_error_is_a_hard_error_with_restored_group() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("closeerror").await;
    let input = harness.raw_fixture("close-error.mcap", &[1_000]);
    let _hook = EnvVar::set(TEST_NODE_CLOSE_ERROR_ENV, "1");
    let _prior_group = EnvVar::set(ENV_AORTA_GROUP, "before-close");
    let mut config = harness.config(input);
    config.match_timeout_secs = 0.0;

    let result = run_with_shutdown(config, CancellationToken::new()).await;
    let group_after = std::env::var(ENV_AORTA_GROUP).ok();
    crate::group::clear_taint_for_test();
    let err = result.expect_err("a completed close error must be a hard error");
    assert_eq!(group_after.as_deref(), Some("before-close"));
    assert_eq!(
        err.summary().teardown.stuck_stage.as_deref(),
        Some("node_close")
    );
    assert_eq!(
        err.summary()
            .teardown
            .residual
            .get("detached_node_resources"),
        None
    );
    assert!(err
        .summary()
        .teardown
        .error
        .as_deref()
        .is_some_and(|error| { error.contains("test node close error") }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn cancelled_node_close_error_is_still_a_hard_error() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("cancelcloseerror").await;
    let input = harness.raw_fixture("cancel-close-error.mcap", &[1_000, 10_000_000_000]);
    let _hook = EnvVar::set(TEST_NODE_CLOSE_ERROR_ENV, "1");
    // This tests error precedence, not teardown latency. Let Bazel's 300 s
    // suite watchdog detect hangs; dedicated tests cover teardown deadlines.
    let _deadline = EnvVar::set(TEST_TEARDOWN_DEADLINE_MS_ENV, "300000");
    let _prior_group = EnvVar::set(ENV_AORTA_GROUP, "before-cancel-close");
    let mut config = harness.config(input);
    config.match_timeout_secs = 0.0;
    config.loop_playback = true;
    let token = CancellationToken::new();
    let progress = Arc::new(crate::ProgressHandle::new());
    let mut run = Box::pin(crate::run_with_shutdown_reporting(
        config,
        token.clone(),
        progress.clone(),
    ));
    // A completed publish proves the node exists. Looping keeps replay alive
    // even if the observer is descheduled beyond the recorded span.
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = async {
            while progress.snapshot().published == 0 {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    token.cancel();

    let result = run.await;
    let group_after = std::env::var(ENV_AORTA_GROUP).ok();
    crate::group::clear_taint_for_test();
    let err = result.expect_err("close error must outrank Interrupted");
    assert!(err.summary().published_messages > 0);
    assert_eq!(group_after.as_deref(), Some("before-cancel-close"));
    assert!(err
        .summary()
        .teardown
        .error
        .as_deref()
        .is_some_and(|error| { error.contains("test node close error") }));
    assert_eq!(
        err.summary()
            .teardown
            .residual
            .get("detached_node_resources"),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn stalled_publisher_construction_detaches_at_the_deadline_with_residuals() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("stallpublisher").await;
    let input = harness.typed_fixture("stall-publisher.mcap", &[1_000]);
    let _stall = EnvVar::set(TEST_STALL_PUBLISHER_ENV, "1");
    let _deadline = EnvVar::set(TEST_TEARDOWN_DEADLINE_MS_ENV, "100");
    let _sentinel = EnvVar::set(ENV_AORTA_GROUP, "play_test_sentinel");

    let token = CancellationToken::new();
    let canceller = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        canceller.cancel();
    });

    let mut config = harness.config(input);
    config.preflight_timeout_secs = 0.2;
    config.match_timeout_secs = 0.0;
    let result = run_with_shutdown(config, token).await;

    // A teardown timeout taints the process by design. Reset the test-only
    // latch after the assertions' input has been captured so other live tests
    // in this process can continue independently.
    let outcome = result.expect("stalled construction remains a reported outcome");
    let termination = outcome.termination;
    let report = outcome.summary.teardown;
    crate::group::clear_taint_for_test();

    assert_eq!(termination, Termination::Interrupted);
    assert!(report.deadline_expired);
    assert_eq!(report.stuck_stage.as_deref(), Some("pending_construction"));
    assert_eq!(report.residual.get("stages_not_completed"), Some(&1));
    assert_eq!(report.residual.get("pending_construction_tasks"), Some(&1));
    assert_eq!(report.residual.get("schema_puts_not_rolled_back"), Some(&1));
    let (by_key, by_hash) = harness.schema_state().await;
    let expected_hash = SchemaHash::from_bfbs_bytes(SCHEMA_BFBS).to_hex();
    assert!(
        by_key.contains(&expected_hash),
        "the cancelled registration left its by-key schema state visible: {by_key:?}"
    );
    assert!(
        by_hash.contains(&expected_hash),
        "the cancelled registration left its by-hash schema state visible: {by_hash:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn startup_phase_arithmetic_observes_publisher_delay_and_second_scan_deadline() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("startupmath").await;
    let input = harness.typed_fixture("startup-math.mcap", &[1_000]);
    let _staged = harness.first_scan_unclaimed_then_stall().await;
    let _delay = EnvVar::set(TEST_PUBLISHER_DELAY_MS_ENV, "100");

    let mut config = harness.config(input);
    config.preflight_timeout_secs = 0.15;
    config.match_timeout_secs = 0.0;
    let started = Instant::now();
    let err = run_with_shutdown(config, CancellationToken::new())
        .await
        .expect_err("the stalled second scan must be inconclusive");
    let elapsed = started.elapsed();
    assert!(err.to_string().contains("second scan"), "got: {err}");
    assert!(
        elapsed >= Duration::from_millis(200),
        "startup must include publisher delay plus the second scan deadline: {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn snapshot_copy_cancellation_detaches_the_managed_thread() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("snapshotcancel").await;
    let input = harness.typed_fixture("snapshot-cancel.mcap", &[1_000]);
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    *TEST_COPY_STALL.lock().expect("copy stall") = Some(ThreadStall {
        entered: entered_tx,
        release: wait,
    });
    let token = CancellationToken::new();
    let mut run = Box::pin(run_with_shutdown(harness.config(input), token.clone()));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        armed = entered => armed.expect("copy stall did not arm"),
    }
    token.cancel();
    // The worker cannot finish before this returns, regardless of scheduling.
    // Bazel's suite watchdog catches a broken cancellation/join path.
    let result = run.await;
    drop(release);
    let outcome = result.expect("snapshot cancellation is an interrupted outcome");
    assert_eq!(outcome.termination, Termination::Interrupted);
    assert_eq!(
        outcome
            .summary
            .teardown
            .residual
            .get("detached_pass0_threads"),
        Some(&1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn pass_one_cancellation_detaches_the_managed_thread() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("stallplan").await;
    let input = harness.raw_fixture("plan-cancel.mcap", &[1_000]);
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    *TEST_PLAN_STALL.lock().expect("plan stall") = Some(ThreadStall {
        entered: entered_tx,
        release: wait,
    });
    let token = CancellationToken::new();
    let mut run = Box::pin(run_with_shutdown(harness.config(input), token.clone()));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        armed = entered => armed.expect("plan stall did not arm"),
    }
    token.cancel();
    let result = run.await;
    drop(release);
    let outcome = result.expect("pass-1 cancellation is an interrupted outcome");
    assert_eq!(outcome.termination, Termination::Interrupted);
    assert_eq!(
        outcome
            .summary
            .teardown
            .residual
            .get("detached_pass1_threads"),
        Some(&1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn completed_replay_reports_teardown_timeout_and_residual_state() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("stallclose").await;
    let input = harness.raw_fixture("stall-close.mcap", &[1_000]);
    let _stall = EnvVar::set(TEST_STALL_NODE_CLOSE_ENV, "1");
    // The node_close stall parks forever (future::pending), so the deadline
    // always expires inside node_close no matter how large it is — but the
    // teardown stages BEFORE node_close (reader-thread join, …) must finish
    // within it, or stuck_stage reports the wrong stage. 100 ms was not
    // enough on a loaded CI runner (observed stuck_stage="reader_thread");
    // 2 s only costs wall-clock, not semantics.
    let _deadline = EnvVar::set(TEST_TEARDOWN_DEADLINE_MS_ENV, "2000");
    let _sentinel = EnvVar::set(ENV_AORTA_GROUP, "play_test_sentinel");

    let mut config = harness.config(input);
    config.match_timeout_secs = 0.0;
    let result = run_with_shutdown(config, CancellationToken::new()).await;

    let outcome = result.expect("teardown timeout is a terminal outcome");
    let termination = outcome.termination;
    let report = outcome.summary.teardown;
    crate::group::clear_taint_for_test();

    assert_eq!(termination, Termination::CompletedWithTeardownTimeout);
    assert!(report.deadline_expired);
    assert_eq!(report.stuck_stage.as_deref(), Some("node_close"));
    assert_eq!(report.residual.get("stages_not_completed"), Some(&1));
    assert_eq!(report.residual.get("detached_node_resources"), Some(&1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn publish_errors_and_teardown_timeout_are_reported_end_to_end() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("publisherrorclose").await;
    let input = harness.raw_fixture("publish-error-close.mcap", &[1_000]);
    let _publish_error = EnvVar::set(TEST_CLOSE_BEFORE_REPLAY_ENV, "1");
    let _stall = EnvVar::set(TEST_STALL_NODE_CLOSE_ENV, "1");
    let _deadline = EnvVar::set(TEST_TEARDOWN_DEADLINE_MS_ENV, "100");
    let _sentinel = EnvVar::set(ENV_AORTA_GROUP, "play_test_sentinel");

    let mut config = harness.config(input);
    config.match_timeout_secs = 0.0;
    let outcome = run_with_shutdown(config, CancellationToken::new())
        .await
        .expect("publish errors remain a terminal outcome");
    let report = outcome.summary.teardown;
    let publish_errors = outcome.summary.publish_errors;
    crate::group::clear_taint_for_test();

    assert_eq!(
        outcome.termination,
        Termination::CompletedWithTeardownTimeout
    );
    assert!(report.deadline_expired);
    assert!(
        publish_errors > 0,
        "the closed transport must report publish errors"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
async fn stalled_node_construction_detaches_at_the_deadline() {
    let _lock = crate::group::EnvLock::acquire();
    let harness = Harness::new("stallnode").await;
    let input = harness.raw_fixture("stall-node.mcap", &[1_000]);
    let _stall = EnvVar::set(TEST_STALL_NODE_OPEN_ENV, "1");
    let _deadline = EnvVar::set(TEST_TEARDOWN_DEADLINE_MS_ENV, "100");

    // Cancel only once the run has demonstrably entered node construction
    // (the stall hook arms the flag right before parking): a blind 10 ms
    // timer cancelled earlier stages on loaded CI runners, which never
    // produces the pending_construction residual this test pins.
    TEST_NODE_OPEN_STALL_ARMED.store(false, Ordering::SeqCst);
    let token = CancellationToken::new();
    let mut run = Box::pin(run_with_shutdown(harness.config(input), token.clone()));
    tokio::select! {
        result = &mut run => panic!("player finished before cancellation: {result:?}"),
        _ = wait_for_fault_arm(&TEST_NODE_OPEN_STALL_ARMED) => {}
    }
    token.cancel();
    let outcome = run
        .await
        .expect("stalled node construction remains a reported outcome");
    let report = outcome.summary.teardown;
    crate::group::clear_taint_for_test();

    assert_eq!(outcome.termination, Termination::Interrupted);
    assert!(report.deadline_expired);
    assert_eq!(report.stuck_stage.as_deref(), Some("pending_construction"));
    assert_eq!(report.residual.get("stages_not_completed"), Some(&1));
    assert_eq!(report.residual.get("pending_construction_tasks"), Some(&1));
}
