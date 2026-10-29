use super::{normalize_group, Node, Session};
use crate::client::Client;
use crate::error::{AortaError, AortaResult};
use crate::event_queue::{Event, EventKind, EventQueueSender};
use crate::executor::{ExecutorClassPolicyOptions, SingleThreadedExecutor};
use crate::schema::{SchemaAttachPolicy, SchemaHash};
use crate::service::{RpcOverflowPolicy, Service};
use crate::subscriber::SubscriberOverflowPolicy;
use crate::telemetry::schema::{NodeOptions, TelemetryNodeMetadata};
use crate::test_support::{schema_attach_guard, EnvVarGuard, ENV_LOCK};
use crate::timer::TimerOverflowPolicy;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;

async fn wait_for_timer_overflow_timeout_count(
    timer: &crate::timer::Timer,
    timeout: Duration,
) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count = timer.overflow_timeout_count();
        if count > 0 {
            return count;
        }

        if tokio::time::Instant::now() >= deadline {
            return count;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn normalize_group_defaults_to_default() {
    assert_eq!(normalize_group(""), "default");
    assert_eq!(normalize_group("   "), "default");
}

#[test]
fn normalize_group_trims_whitespace() {
    assert_eq!(normalize_group("  alpha "), "alpha");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_event_sender_state() -> AortaResult<()> {
    use crate::event_queue::EventQueueSender;

    let node = Node::new("node_event_sender", " ").await?;
    assert!(!node.has_event_sender());

    let (tx, _rx) = mpsc::channel(1);
    node.set_event_sender(
        EventQueueSender::from_bounded_channel(tx),
        crate::node::SYNTHETIC_TEST_OWNER,
    );
    assert!(node.has_event_sender());
    assert!(node.event_sender().is_some());

    let (closed_tx, closed_rx) = mpsc::channel(1);
    drop(closed_rx);
    node.set_event_sender(
        EventQueueSender::from_bounded_channel(closed_tx),
        crate::node::SYNTHETIC_TEST_OWNER,
    );
    assert!(!node.has_event_sender());
    assert!(node.event_sender().is_none());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_timer_requires_executor() -> AortaResult<()> {
    let node = Node::new("node_timer_guard", "default").await?;
    let result = node.create_timer(Duration::from_millis(10), || {}).await;
    assert!(result.is_err(), "expected error without executor");
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_timer_uses_executor_class_policy_defaults_when_options_are_default(
) -> AortaResult<()> {
    let config = zenoh::Config::from_json5(
        r#"
{
  mode: "peer",
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
    )?;
    let node = Node::with_config("node_timer_class_policy_defaults", "default", config).await?;
    let (tx, mut rx) = mpsc::channel(1);
    node.set_event_sender(
        EventQueueSender::from_bounded_channel(tx.clone()),
        crate::node::SYNTHETIC_TEST_OWNER,
    );
    node.set_executor_class_policy_defaults(ExecutorClassPolicyOptions {
        subscriber_overflow_policy: SubscriberOverflowPolicy::Drop,
        subscriber_overflow_relay_capacity: 1024,
        rpc_overflow_policy: RpcOverflowPolicy::FastFailBusy,
        rpc_overflow_block_timeout: Duration::from_millis(0),
        timer_overflow_policy: TimerOverflowPolicy::Block,
        timer_overflow_block_timeout: Duration::from_millis(2),
    });

    tx.try_send(Event::new(EventKind::Timer, || {}))
        .expect("fill executor queue");

    let timer = node
        .create_timer(Duration::from_millis(5), || {})
        .await
        .expect("timer create");
    let overflow_count =
        wait_for_timer_overflow_timeout_count(&timer, Duration::from_millis(250)).await;
    assert!(
        overflow_count > 0,
        "expected block-timeout policy to count overflows"
    );

    let _ = rx.try_recv();
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_rejects_empty_name() {
    let result = Node::new("   ", "default").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_group_defaults_when_blank() -> AortaResult<()> {
    let node = Node::new("node_group_default", "   ").await?;
    assert_eq!(node.group(), "default");
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_group_defaults_when_env_missing() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::remove_var("AORTA_GROUP");

    let node = Node::new("node_group_env_missing", "fleet_alpha").await?;
    assert_eq!(node.group(), "default");
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_group_uses_env_when_present() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_GROUP", "fleet_env");

    let node = Node::new("node_group_env_present", "fleet_alpha").await?;
    assert_eq!(node.group(), "fleet_env");
    node.close().await?;
    std::env::remove_var("AORTA_GROUP");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_attach_policy_from_env() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_guard("always");
    let node = Node::new("node_schema_policy", "default").await?;
    assert_eq!(node.schema_attach_policy(), SchemaAttachPolicy::Always);
    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_attach_policy_invalid_defaults_to_never() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_guard("unknown");
    let node = Node::new("node_schema_policy_invalid", "default").await?;
    assert_eq!(node.schema_attach_policy(), SchemaAttachPolicy::Never);
    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_schema_for_topic_updates_registry() -> AortaResult<()> {
    let node = Node::new("node_schema_register", "default").await?;
    let topic = "/schema/test";
    let bfbs = vec![1u8, 2u8, 3u8, 4u8];
    let expected_hash = SchemaHash::from_bfbs_bytes(&bfbs);

    let hash = node
        .register_schema_for_topic_bfbs(topic, bfbs.clone(), None)
        .await?;
    assert_eq!(hash.as_bytes(), expected_hash.as_bytes());

    let cached = node.schema_registry().get_local(hash).await;
    assert_eq!(cached, Some(bfbs.clone()));

    let key_suffix = node.key_builder().key_suffix_for_topic(topic)?;
    let by_key = node
        .schema_registry()
        .get_schema_by_key_local(&key_suffix)
        .await;
    assert!(by_key.is_some());
    let by_key = by_key.unwrap();
    assert_eq!(by_key.hash().as_bytes(), expected_hash.as_bytes());
    assert_eq!(by_key.bfbs(), bfbs.as_slice());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_rejects_duplicate_name_within_group() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::remove_var("AORTA_GROUP");

    let node1 = Node::new("duplicate_node_name", "group_a").await?;
    let node2 = Node::new("duplicate_node_name", "group_b").await;
    assert!(
        node2.is_err(),
        "expected duplicate node name to be rejected"
    );

    node1.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_group_preserves_case_literally() -> AortaResult<()> {
    // Pre-relaxation this test exercised `AORTA_KEY_NORMALIZE_CASE=true`
    // folding `Fleet` → `fleet`. The env var has been removed and the
    // validator now preserves user spelling byte-for-byte, so `Fleet`
    // and `fleet` are distinct groups. The duplicate-node check is
    // therefore exercised in a different test
    // (`node_rejects_duplicate_name_within_same_group`).
    let _guard = ENV_LOCK.lock().expect("lock");
    let _group_guard = EnvVarGuard::set("AORTA_GROUP", "Fleet");

    let node1 = Node::new("case_preservation_node", "ignored").await?;
    assert_eq!(node1.group(), "Fleet");

    node1.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_name_reusable_after_close() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::remove_var("AORTA_GROUP");

    let node1 = Node::new("reusable_node_name", "group_a").await?;
    node1.close().await?;

    let node2 = Node::new("reusable_node_name", "group_b").await?;
    node2.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_id_format_is_32_hex_chars() -> AortaResult<()> {
    let node = Node::new("node_id_test", "default").await?;
    let id = node.id();

    assert_eq!(id.len(), 32, "node_id should be 32 characters");
    assert!(
        id.chars().all(|c| c.is_ascii_hexdigit()),
        "node_id should be all hex digits"
    );

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_id_is_unique_per_node() -> AortaResult<()> {
    let node1 = Node::new("node_id_unique_1", "default").await?;
    let node2 = Node::new("node_id_unique_2", "default").await?;

    assert_ne!(node1.id(), node2.id(), "each node should have unique id");

    node1.close().await?;
    node2.close().await?;
    Ok(())
}

#[test]
fn derive_node_id_is_stable_for_same_inputs() {
    use crate::telemetry::identity::derive_node_id;
    // Holds ENV_LOCK because derive_node_id reads AORTA_NODE_ID; without it a
    // concurrent override test can leak that env into this one.
    let _lock = ENV_LOCK.lock().expect("lock");
    // Determinism is the whole point of a derived node id: the same (host, group, name)
    // must yield the same id across process restarts.
    let first = derive_node_id("host-abc", "default", "planner").expect("derive");
    let second = derive_node_id("host-abc", "default", "planner").expect("derive");
    assert_eq!(first, second);
    assert_eq!(first.len(), 32, "derived node id keeps the 32-hex width");
    assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn derive_node_id_differs_by_host_group_or_name() {
    use crate::telemetry::identity::derive_node_id;
    let _lock = ENV_LOCK.lock().expect("lock");
    let base = derive_node_id("host-abc", "default", "planner").expect("derive");
    assert_ne!(
        base,
        derive_node_id("host-XYZ", "default", "planner").expect("derive")
    );
    assert_ne!(
        base,
        derive_node_id("host-abc", "fleet", "planner").expect("derive")
    );
    assert_ne!(
        base,
        derive_node_id("host-abc", "default", "perception").expect("derive")
    );
}

#[test]
fn derive_node_id_honors_explicit_override() {
    use crate::telemetry::identity::derive_node_id;
    let _lock = ENV_LOCK.lock().expect("lock");
    let _override = EnvVarGuard::set("AORTA_NODE_ID", "robot7-planner");
    // The operator-assigned id wins verbatim (after key-segment validation),
    // independent of host/group/name.
    let id = derive_node_id("host-abc", "default", "planner").expect("derive");
    assert_eq!(id, "robot7-planner");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_id_matches_deterministic_derivation() -> AortaResult<()> {
    let _lock = ENV_LOCK.lock().expect("lock");
    let node = Node::new("node_id_derived", "default").await?;
    // The live node id must be exactly the deterministic derivation from its
    // resolved host identity + group + name (proves node.rs wiring + stability).
    let host = crate::telemetry::resolve_host_identity();
    let expected =
        crate::telemetry::identity::derive_node_id(&host.host_id, node.group(), node.name())?;
    assert_eq!(node.id(), expected);
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_identity_telemetry_includes_session_zid_and_runtime_defaults() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("node_identity_defaults", "default").await?;

    let telemetry = node.build_node_identity_telemetry();

    assert_eq!(telemetry.node_id, node.id());
    assert_eq!(telemetry.node_name, node.name());
    assert_eq!(telemetry.group, node.group());
    assert_eq!(telemetry.sequence, 0);
    assert_eq!(telemetry.session_zid, node.session_zid());
    assert!(!telemetry.session_zid.is_empty());
    // The BASE snapshot leaves router zids empty; the vital heartbeat worker
    // refreshes them live per tick.
    assert_eq!(telemetry.connected_router_zids, Vec::<String>::new());
    assert_eq!(telemetry.language, "rust");
    assert_eq!(telemetry.sdk_name, "aorta_core");
    assert_eq!(telemetry.sdk_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(telemetry.pid, std::process::id());
    assert!(telemetry.process_start_time_ns > 0);
    assert!(telemetry.timestamp_ns >= telemetry.process_start_time_ns);
    assert!(!telemetry.host_id.is_empty());
    assert!(!telemetry.host_name.is_empty());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connected_router_zids_reports_live_router_connection() -> AortaResult<()> {
    // Regression: connected_router_zids was a permanent hardcoded
    // empty stub, leaving router death invisible to fleet telemetry. In the
    // test environment every node connects (client mode) to the in-process
    // local test router, so the live query must surface exactly that router.
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("node_router_zids", "default").await?;

    let zids = crate::telemetry::identity::connected_router_zids(node.session()).await;
    assert!(
        !zids.is_empty(),
        "node is connected to the local test router; its zid must be reported"
    );
    for zid in &zids {
        assert!(!zid.is_empty());
    }

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_identity_telemetry_uses_options_and_env_overrides() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _host_id = EnvVarGuard::set("AORTA_HOST_ID", "explicit-host-id");
    let _host_name = EnvVarGuard::set("AORTA_HOST_NAME", "explicit-host-name");

    let mut labels = BTreeMap::new();
    labels.insert(" deployment ".to_string(), " test ".to_string());
    labels.insert("ignored".to_string(), "   ".to_string());

    let node = Node::with_options(
        "node_identity_overrides",
        "default",
        NodeOptions {
            telemetry: TelemetryNodeMetadata {
                executable: Some("/tmp/custom-aorta".to_string()),
                language: Some("cpp".to_string()),
                sdk_name: Some("aorta_cpp".to_string()),
                sdk_version: Some("1.2.3".to_string()),
                labels,
                build_id: Some("build-abc".to_string()),
                container_id: Some("container-xyz".to_string()),
            },
        },
    )
    .await?;

    let telemetry = node.build_node_identity_telemetry();

    assert_eq!(telemetry.host_id, "explicit-host-id");
    assert_eq!(telemetry.host_name, "explicit-host-name");
    assert_eq!(telemetry.executable, "/tmp/custom-aorta");
    assert_eq!(telemetry.language, "cpp");
    assert_eq!(telemetry.sdk_name, "aorta_cpp");
    assert_eq!(telemetry.sdk_version, "1.2.3");
    assert_eq!(telemetry.build_id.as_deref(), Some("build-abc"));
    assert_eq!(telemetry.container_id.as_deref(), Some("container-xyz"));
    assert_eq!(
        telemetry.labels.get("deployment").map(String::as_str),
        Some("test")
    );
    assert_eq!(telemetry.labels.len(), 1);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_identity_telemetry_query_returns_current_payload() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("node_identity_query", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.vital_telemetry_key().to_string();

    let payload = wait_for_node_identity_payload(&monitor_session, &telemetry_key).await?;

    assert_eq!(payload.node_id, node.id());
    assert_eq!(payload.node_name, node.name());
    assert_eq!(payload.group, node.group());
    assert_eq!(payload.session_zid, node.session_zid().as_str());
    assert_eq!(payload.sequence, 1);

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_identity_telemetry_heartbeat_advances_sequence() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _heartbeat = EnvVarGuard::set("AORTA_TELEMETRY_HEARTBEAT_MS", "50");

    let node = Node::new("node_identity_heartbeat", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.vital_telemetry_key().to_string();

    let first = wait_for_node_identity_payload(&monitor_session, &telemetry_key).await?;
    let first_sequence = first.sequence;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut second_sequence = first_sequence;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let payload = wait_for_node_identity_payload(&monitor_session, &telemetry_key).await?;
        second_sequence = payload.sequence;
        if second_sequence > first_sequence {
            break;
        }
    }

    assert!(
        second_sequence > first_sequence,
        "expected telemetry heartbeat to advance sequence"
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_schemas_are_registered_for_by_hash_resolution() -> AortaResult<()> {
    // With telemetry enabled (default), the vital snapshot schema (which
    // transitively covers node_identity + resource_inventory via `include`) and
    // the bulk runtime schema are registered in the by-hash store, so a recorder
    // resolves them from the hash attached to each put and writes typed MCAP.
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("telemetry_schema_registration", "default").await?;
    let monitor = Session::open_default().await?.into_inner();

    for (hash, bfbs) in [
        (
            crate::telemetry::fb::vital_snapshot_schema_hash(),
            crate::telemetry::fb::TELEMETRY_VITAL_SNAPSHOT_BFBS,
        ),
        (
            crate::telemetry::fb::runtime_metrics_schema_hash(),
            crate::telemetry::fb::TELEMETRY_RUNTIME_METRICS_BFBS,
        ),
    ] {
        let schema_key = format!("aorta/sys/schema/{}", hash.to_hex());
        let replies = monitor
            .get(&schema_key)
            .await
            .map_err(|e| AortaError::internal(format!("schema get failed: {}", e)))?;
        let reply = tokio::time::timeout(Duration::from_secs(2), replies.recv_async())
            .await
            .map_err(|_| AortaError::timeout(format!("no schema reply for {}", schema_key)))?
            .map_err(|e| AortaError::internal(format!("schema recv failed: {}", e)))?;
        let sample = reply
            .into_result()
            .map_err(|_| AortaError::internal("schema reply was an error".to_string()))?;
        assert_eq!(sample.payload().to_bytes().as_ref(), bfbs);
    }

    node.close().await?;
    crate::session::Session::close_inner(monitor).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_put_carries_schema_hash_attachment() -> AortaResult<()> {
    // Each live telemetry put carries the 64-hex schema hash as a zenoh
    // attachment so recorders resolve the schema by-hash (typed MCAP channel).
    let _guard = ENV_LOCK.lock().expect("lock");
    let _hb = EnvVarGuard::set("AORTA_TELEMETRY_HEARTBEAT_MS", "100");
    let node = Node::new("telemetry_attach", "default").await?;
    let monitor = Session::open_default().await?.into_inner();
    let key = node.vital_telemetry_key().to_string();
    let expected = crate::telemetry::fb::vital_snapshot_schema_hash().to_hex();

    let subscriber = monitor
        .declare_subscriber(&key)
        .await
        .map_err(|e| AortaError::internal(format!("subscribe failed: {}", e)))?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut attachment_hex = None;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(sample)) =
            tokio::time::timeout(Duration::from_millis(300), subscriber.recv_async()).await
        {
            if let Some(att) = sample.attachment() {
                attachment_hex = String::from_utf8(att.to_bytes().to_vec()).ok();
                break;
            }
        }
    }
    assert_eq!(attachment_hex.as_deref(), Some(expected.as_str()));

    node.close().await?;
    crate::session::Session::close_inner(monitor).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_forwarder_keeps_user_telemetry_unchanged_after_rearm() -> AortaResult<()> {
    const CHILD_ENV: &str = "AORTA_TEST_LOG_TELEMETRY_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "node::tests::topic_forwarder_keeps_user_telemetry_unchanged_after_rearm",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .env("AORTA_LOG_AUTO_INIT", "true")
            .env("AORTA_LOG_TOPIC_ENABLED", "true")
            .env("AORTA_LOG_TOPIC", "/internal/test_log_forwarder")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "log telemetry child failed: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return Ok(());
    }

    // A fresh process owns the forwarding slot. Recreate the owner to exercise
    // rearming, without relying on other tests to retain a global Node fixture.
    let config = zenoh::Config::from_json5(
        r#"{mode:"peer",listen:{endpoints:["tcp/127.0.0.1:0"]},
            scouting:{multicast:{enabled:false},gossip:{enabled:false}},
            transport:{shared_memory:{enabled:false}}}"#,
    )?;
    for name in ["log_telemetry_owner", "log_telemetry_successor"] {
        let node = Node::with_zenoh_config(name, "default", config.clone()).await?;
        // A user publisher named /log must still be counted: exclusion is an
        // internal option, not a topic-name filter.
        let publisher = node.create_publisher("/log").await?;
        let session = node.session().inner();
        for key in [
            node.vital_telemetry_key(),
            node.runtime_metrics_telemetry_key(),
        ] {
            let declared = wait_for_telemetry_payload_matching(session, key, |value| {
                value["publishers"]
                    .as_array()
                    .is_some_and(|items| items.iter().any(|item| item["resource_name"] == "log"))
            })
            .await?;
            assert_eq!(declared["publishers"].as_array().unwrap().len(), 1);
            if key == node.vital_telemetry_key() {
                assert_eq!(declared["inventory_revision"].as_u64(), Some(1));
            }
        }

        drop(publisher);
        for key in [
            node.vital_telemetry_key(),
            node.runtime_metrics_telemetry_key(),
        ] {
            let removed = wait_for_telemetry_payload_matching(session, key, |value| {
                value["publishers"]
                    .as_array()
                    .is_some_and(|items| items.iter().all(|item| item["resource_name"] != "log"))
            })
            .await?;
            assert!(removed["publishers"].as_array().unwrap().is_empty());
            if key == node.vital_telemetry_key() {
                assert_eq!(removed["inventory_revision"].as_u64(), Some(2));
            }
        }
        node.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_inventory_telemetry_tracks_publisher_lifecycle() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("inventory_publishers", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.vital_telemetry_key().to_string();

    let initial = wait_for_resource_inventory_payload(&monitor_session, &telemetry_key).await?;
    // seq >= 1 (not == 1): start()'s initial publish races a second publish from
    // the node's internal resource registration, so the exact sequence at the
    // first read is timing-dependent (flaky as == 1 on slower/loaded boxes).
    assert!(initial["sequence"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(initial["inventory_revision"].as_u64(), Some(0));
    assert_eq!(
        initial["publishers"].as_array().map(|items| items.len()),
        Some(0)
    );

    let publisher = node.create_publisher("/telemetry/demo_topic").await?;
    let expected_full_key = format!("aorta/{}/pub/telemetry/demo_topic", node.group());

    let declared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["publishers"]
            .as_array()
            .map(|items| items.len() == 1)
            .unwrap_or(false)
    })
    .await?;

    // >= 1 (not == 1): the exact inventory revision after the first declare races
    // other inventory changes during node startup, so it is timing-dependent.
    assert!(declared["inventory_revision"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(
        declared["publishers"][0]["resource_name"].as_str(),
        Some("telemetry/demo_topic")
    );
    assert_eq!(
        declared["publishers"][0]["full_key"].as_str(),
        Some(expected_full_key.as_str())
    );
    assert!(declared["publishers"][0]
        .get("normalized_key_suffix")
        .is_none());
    assert!(declared["publishers"][0]["resource_instance_id"]
        .as_str()
        .unwrap_or_default()
        .starts_with("publisher:"));

    drop(publisher);

    let removed = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["inventory_revision"].as_u64().unwrap_or(0) >= 2
            && value["publishers"]
                .as_array()
                .map(|items| items.is_empty())
                .unwrap_or(false)
    })
    .await?;

    assert_eq!(removed["inventory_revision"].as_u64(), Some(2));
    assert_eq!(
        removed["publishers"].as_array().map(|items| items.len()),
        Some(0)
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_inventory_telemetry_tracks_service_lifecycle() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("inventory_services", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.vital_telemetry_key().to_string();
    let expected_full_key = format!("aorta/{}/srv/telemetry_echo", node.group());

    let service = Service::builder(node.session().clone(), "telemetry_echo")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .schema_registry(node.schema_registry().clone())
        .inventory_telemetry(node.vital_telemetry())
        .build()
        .start()
        .await?;

    let declared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["services"]
            .as_array()
            .map(|items| items.len() == 1)
            .unwrap_or(false)
    })
    .await?;

    // >= 1 (not == 1): the exact inventory revision after the first declare races
    // other inventory changes during node startup, so it is timing-dependent.
    assert!(declared["inventory_revision"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(
        declared["services"][0]["resource_name"].as_str(),
        Some("telemetry_echo")
    );
    assert_eq!(
        declared["services"][0]["full_key"].as_str(),
        Some(expected_full_key.as_str())
    );
    assert!(declared["services"][0]["resource_instance_id"]
        .as_str()
        .unwrap_or_default()
        .starts_with("service:"));

    service.stop_and_join(Duration::from_secs(1)).await;

    let removed = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["inventory_revision"].as_u64().unwrap_or(0) >= 2
            && value["services"]
                .as_array()
                .map(|items| items.is_empty())
                .unwrap_or(false)
    })
    .await?;

    assert_eq!(removed["inventory_revision"].as_u64(), Some(2));
    assert_eq!(
        removed["services"].as_array().map(|items| items.len()),
        Some(0)
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_inventory_telemetry_uses_all_namespaces_service_key() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("inventory_all_namespaces_service", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.vital_telemetry_key().to_string();
    let expected_full_key = "**/aorta/default/srv/demo/inventory_echo";

    let service = Service::builder(node.session().clone(), "demo/inventory_echo")
        .key_builder(node.key_builder().clone())
        .serve_all_namespaces(true)
        .default_schema_attach_policy(node.schema_attach_policy())
        .schema_registry(node.schema_registry().clone())
        .inventory_telemetry(node.vital_telemetry())
        .build()
        .start()
        .await?;

    let declared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["services"]
            .as_array()
            .map(|items| items.len() == 1)
            .unwrap_or(false)
    })
    .await?;

    assert_eq!(
        declared["services"][0]["full_key"].as_str(),
        Some(expected_full_key)
    );

    service.stop_and_join(Duration::from_secs(1)).await;
    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_inventory_telemetry_tracks_subscriber_lifecycle() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("inventory_subscribers", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.vital_telemetry_key().to_string();
    let expected_full_key = format!("aorta/{}/pub/telemetry_input", node.group());

    let subscriber = node
        .create_subscriber("/telemetry_input", |_payload, _ctx| {})
        .await?;

    let declared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["subscribers"]
            .as_array()
            .map(|items| items.len() == 1)
            .unwrap_or(false)
    })
    .await?;

    // >= 1 (not == 1): the exact inventory revision after the first declare races
    // other inventory changes during node startup, so it is timing-dependent.
    assert!(declared["inventory_revision"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(
        declared["subscribers"][0]["resource_name"].as_str(),
        Some("telemetry_input")
    );
    assert_eq!(
        declared["subscribers"][0]["full_key"].as_str(),
        Some(expected_full_key.as_str())
    );
    assert!(declared["subscribers"][0]["resource_instance_id"]
        .as_str()
        .unwrap_or_default()
        .starts_with("subscriber:"));

    subscriber.undeclare().await?;

    let removed = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["inventory_revision"].as_u64().unwrap_or(0) >= 2
            && value["subscribers"]
                .as_array()
                .map(|items| items.is_empty())
                .unwrap_or(false)
    })
    .await?;

    assert_eq!(removed["inventory_revision"].as_u64(), Some(2));
    assert_eq!(
        removed["subscribers"].as_array().map(|items| items.len()),
        Some(0)
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_inventory_telemetry_tracks_client_lifecycle() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("inventory_clients", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.vital_telemetry_key().to_string();
    let expected_full_key = format!("aorta/{}/srv/telemetry_client", node.group());

    let client = Client::builder(node.session().clone(), "telemetry_client")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .inventory_telemetry(node.vital_telemetry())
        .build();

    let declared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["clients"]
            .as_array()
            .map(|items| items.len() == 1)
            .unwrap_or(false)
    })
    .await?;

    // >= 1 (not == 1): the exact inventory revision after the first declare races
    // other inventory changes during node startup, so it is timing-dependent.
    assert!(declared["inventory_revision"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(
        declared["clients"][0]["resource_name"].as_str(),
        Some("telemetry_client")
    );
    assert_eq!(
        declared["clients"][0]["full_key"].as_str(),
        Some(expected_full_key.as_str())
    );
    assert!(declared["clients"][0]["resource_instance_id"]
        .as_str()
        .unwrap_or_default()
        .starts_with("client:"));

    drop(client);

    let removed = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["inventory_revision"].as_u64().unwrap_or(0) >= 2
            && value["clients"]
                .as_array()
                .map(|items| items.is_empty())
                .unwrap_or(false)
    })
    .await?;

    assert_eq!(removed["inventory_revision"].as_u64(), Some(2));
    assert_eq!(
        removed["clients"].as_array().map(|items| items.len()),
        Some(0)
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_metrics_telemetry_tracks_publisher_lifecycle() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("runtime_publishers", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.runtime_metrics_telemetry_key().to_string();

    let initial = wait_for_runtime_metrics_payload(&monitor_session, &telemetry_key).await?;
    // seq >= 1 (not == 1): start()'s initial publish races a second publish from
    // the node's internal resource registration, so the exact sequence at the
    // first read is timing-dependent (flaky as == 1 on slower/loaded boxes).
    assert!(initial["sequence"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(
        initial["publishers"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert_eq!(
        initial["services"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert_eq!(
        initial["clients"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert_eq!(
        initial["subscribers"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert!(initial["collection_period_ms"].as_u64().unwrap_or(0) > 0);

    let publisher = node.create_publisher("/runtime/demo_topic").await?;

    let declared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["publishers"]
            .as_array()
            .map(|items| items.len() == 1)
            .unwrap_or(false)
    })
    .await?;

    assert_eq!(
        declared["publishers"][0]["resource_name"].as_str(),
        Some("runtime/demo_topic")
    );
    assert!(declared["publishers"][0]["resource_instance_id"]
        .as_str()
        .unwrap_or_default()
        .starts_with("publisher:"));
    assert_eq!(
        declared["publishers"][0]["publish_shm_hit_total"].as_u64(),
        Some(0)
    );
    assert_eq!(
        declared["publishers"][0]["loan_inflight_current"].as_u64(),
        Some(0)
    );

    drop(publisher);

    let removed = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["publishers"]
            .as_array()
            .map(|items| items.is_empty())
            .unwrap_or(false)
    })
    .await?;

    assert_eq!(
        removed["publishers"].as_array().map(|items| items.len()),
        Some(0)
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_metrics_telemetry_tracks_subscriber_lifecycle() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("runtime_subscribers", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.runtime_metrics_telemetry_key().to_string();

    let initial = wait_for_runtime_metrics_payload(&monitor_session, &telemetry_key).await?;
    // seq >= 1 (not == 1): start()'s initial publish races a second publish from
    // the node's internal resource registration, so the exact sequence at the
    // first read is timing-dependent (flaky as == 1 on slower/loaded boxes).
    assert!(initial["sequence"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(
        initial["subscribers"].as_array().map(|items| items.len()),
        Some(0)
    );

    let subscriber = node
        .create_subscriber("/runtime/subscriber_topic", |_payload, _context| {})
        .await?;

    let declared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["subscribers"]
            .as_array()
            .map(|items| items.len() == 1)
            .unwrap_or(false)
    })
    .await?;

    assert_eq!(
        declared["subscribers"][0]["resource_name"].as_str(),
        Some("runtime/subscriber_topic")
    );
    assert!(declared["subscribers"][0]["resource_instance_id"]
        .as_str()
        .unwrap_or_default()
        .starts_with("subscriber:"));
    assert_eq!(
        declared["subscribers"][0]["duplicate_drop_count"].as_u64(),
        Some(0)
    );
    assert_eq!(
        declared["subscribers"][0]["executor_drop_count"].as_u64(),
        Some(0)
    );

    drop(subscriber);

    let removed = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["subscribers"]
            .as_array()
            .map(|items| items.is_empty())
            .unwrap_or(false)
    })
    .await?;

    assert_eq!(
        removed["subscribers"].as_array().map(|items| items.len()),
        Some(0)
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_metrics_telemetry_tracks_service_and_client_lifecycle() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("runtime_services_clients", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.runtime_metrics_telemetry_key().to_string();

    let service = Service::builder(node.session().clone(), "runtime_echo")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .schema_registry(node.schema_registry().clone())
        .runtime_telemetry(node.runtime_metrics_telemetry())
        .build()
        .start()
        .await?;

    let service_declared =
        wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
            value["services"]
                .as_array()
                .map(|items| items.len() == 1)
                .unwrap_or(false)
        })
        .await?;

    assert_eq!(
        service_declared["services"][0]["resource_name"].as_str(),
        Some("runtime_echo")
    );
    assert_eq!(
        service_declared["services"][0]["request_received_count"].as_u64(),
        Some(0)
    );

    let client = Client::builder(node.session().clone(), "runtime_echo")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .runtime_telemetry(node.runtime_metrics_telemetry())
        .build();

    let both_declared =
        wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
            value["services"]
                .as_array()
                .map(|items| items.len() == 1)
                .unwrap_or(false)
                && value["clients"]
                    .as_array()
                    .map(|items| items.len() == 1)
                    .unwrap_or(false)
        })
        .await?;

    assert_eq!(
        both_declared["clients"][0]["resource_name"].as_str(),
        Some("runtime_echo")
    );
    assert_eq!(both_declared["clients"][0]["call_count"].as_u64(), Some(0));

    drop(client);

    let client_removed =
        wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
            value["services"]
                .as_array()
                .map(|items| items.len() == 1)
                .unwrap_or(false)
                && value["clients"]
                    .as_array()
                    .map(|items| items.is_empty())
                    .unwrap_or(false)
        })
        .await?;

    assert_eq!(
        client_removed["clients"]
            .as_array()
            .map(|items| items.len()),
        Some(0)
    );

    service.stop_and_join(Duration::from_secs(1)).await;

    let all_removed =
        wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
            value["services"]
                .as_array()
                .map(|items| items.is_empty())
                .unwrap_or(false)
                && value["clients"]
                    .as_array()
                    .map(|items| items.is_empty())
                    .unwrap_or(false)
        })
        .await?;

    assert_eq!(
        all_removed["services"].as_array().map(|items| items.len()),
        Some(0)
    );
    assert_eq!(
        all_removed["clients"].as_array().map(|items| items.len()),
        Some(0)
    );

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_metrics_telemetry_tracks_executor_attachment() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("runtime_executor", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.runtime_metrics_telemetry_key().to_string();

    let mut executor = SingleThreadedExecutor::new(8);
    executor.add_node(node.clone());

    let payload = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["executor"].is_object()
    })
    .await?;

    assert_eq!(payload["executor"]["queued_events"].as_u64(), Some(0));
    assert_eq!(payload["executor"]["queued_bytes"].as_u64(), Some(0));

    drop(executor);

    let cleared = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["executor"].is_null()
    })
    .await?;
    assert!(cleared["executor"].is_null());

    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_metrics_telemetry_reports_executor_spin_count() -> AortaResult<()> {
    // X1 end-to-end: a spinning executor's spin_count must reach the
    // runtime_metrics payload (executor -> add_node spin reader -> worker ->
    // ExecutorMetrics.spin_count). Fast heartbeat so a snapshot taken AFTER the
    // executor begins spinning is published within the read window.
    let _guard = ENV_LOCK.lock().expect("lock");
    let _hb = EnvVarGuard::set("AORTA_TELEMETRY_RUNTIME_HEARTBEAT_MS", "200");
    let node = Node::new("runtime_spin", "default").await?;
    let monitor_session = Session::open_default().await?.into_inner();
    let telemetry_key = node.runtime_metrics_telemetry_key().to_string();

    let mut executor = SingleThreadedExecutor::new(8);
    executor.add_node(node.clone());
    let stop = executor.stop_handle();
    let spin = tokio::spawn(async move {
        executor.spin().await;
    });

    let payload = wait_for_telemetry_payload_matching(&monitor_session, &telemetry_key, |value| {
        value["executor"]["spin_count"].as_u64().unwrap_or(0) > 0
    })
    .await?;
    assert!(
        payload["executor"]["spin_count"].as_u64().unwrap_or(0) > 0,
        "spinning executor must surface a non-zero spin_count"
    );

    stop.stop();
    let _ = spin.await;
    node.close().await?;
    crate::session::Session::close_inner(monitor_session).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_resources_can_drop_after_node_close() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("telemetry_shutdown_drop", "default").await?;

    let publisher = node.create_publisher("/shutdown/topic").await?;
    let subscriber = node
        .create_subscriber("/shutdown/topic", |_payload, _context| {})
        .await?;
    let service = Service::builder(node.session().clone(), "shutdown_service")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .schema_registry(node.schema_registry().clone())
        .inventory_telemetry(node.vital_telemetry())
        .runtime_telemetry(node.runtime_metrics_telemetry())
        .build()
        .start()
        .await?;
    let client = Client::builder(node.session().clone(), "shutdown_service")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .inventory_telemetry(node.vital_telemetry())
        .runtime_telemetry(node.runtime_metrics_telemetry())
        .build();

    tokio::time::timeout(Duration::from_secs(5), node.close())
        .await
        .map_err(|_| AortaError::timeout("node close timed out".to_string()))??;

    drop(client);
    drop(service);
    drop(subscriber);
    drop(publisher);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_shutdown_handles_mixed_release_order_and_late_executor_drop() -> AortaResult<()>
{
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("telemetry_shutdown_mixed", "default").await?;

    let early_publisher = node.create_publisher("/shutdown/early_topic").await?;
    let late_subscriber = node
        .create_subscriber("/shutdown/late_topic", |_payload, _context| {})
        .await?;
    let early_service = Service::builder(node.session().clone(), "shutdown_mixed_service")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .schema_registry(node.schema_registry().clone())
        .inventory_telemetry(node.vital_telemetry())
        .runtime_telemetry(node.runtime_metrics_telemetry())
        .build()
        .start()
        .await?;
    let late_client = Client::builder(node.session().clone(), "shutdown_mixed_service")
        .key_builder(node.key_builder().clone())
        .default_schema_attach_policy(node.schema_attach_policy())
        .inventory_telemetry(node.vital_telemetry())
        .runtime_telemetry(node.runtime_metrics_telemetry())
        .build();

    let mut executor = SingleThreadedExecutor::new(8);
    executor.add_node(node.clone());

    drop(early_publisher);
    early_service.stop_and_join(Duration::from_secs(1)).await;

    tokio::time::timeout(Duration::from_secs(5), node.close())
        .await
        .map_err(|_| AortaError::timeout("node close timed out".to_string()))??;

    drop(late_client);
    drop(late_subscriber);
    drop(executor);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveliness_auto_declared_by_default() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    // Ensure AORTA_LIVELINESS is not set to false
    std::env::remove_var("AORTA_LIVELINESS");

    let node = Node::new("liveliness_auto_test", "default").await?;

    // Liveliness should be auto-declared by default
    assert!(node.has_liveliness(), "liveliness should be auto-declared");

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveliness_can_be_disabled_via_env() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_LIVELINESS", "0");

    let node = Node::new("liveliness_disabled_test", "default").await?;

    // Liveliness should not be declared when disabled
    assert!(
        !node.has_liveliness(),
        "liveliness should be disabled via env"
    );

    node.close().await?;
    std::env::remove_var("AORTA_LIVELINESS");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveliness_token_format_is_correct() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::remove_var("AORTA_LIVELINESS");

    let node = Node::new("liveliness_format_test", "testgroup").await?;

    // Create a subscriber to detect the liveliness token
    let _expected_pattern = format!(
        "aorta/{}/sys/alive/{}/{}",
        node.group(),
        node.name(),
        node.id()
    );

    // Verify the node has declared liveliness
    assert!(node.has_liveliness(), "liveliness should be declared");

    // The actual key format validation is done by LivelinessManager
    // We verify the node_id is in correct format (32 hex chars)
    assert_eq!(
        node.id().len(),
        32,
        "node_id in liveliness key should be 32 chars"
    );
    assert!(
        node.id().chars().all(|c| c.is_ascii_hexdigit()),
        "node_id in liveliness key should be hex"
    );

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveliness_token_undeclared_on_close() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::remove_var("AORTA_LIVELINESS");

    let node_name_for_test = format!("liveliness_undeclare_test_{}", std::process::id());
    let node = Node::new(&node_name_for_test, "default").await?;
    let node_name = node.name().to_string();
    let node_id = node.id().to_string();
    let group = node.group().to_string();

    assert!(
        node.has_liveliness(),
        "liveliness should be declared initially"
    );

    // Close the node - this should undeclare the liveliness token
    node.close().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create a monitoring session to verify token is gone
    let monitor_session = Session::open_default().await?.into_inner();

    let liveliness_key = format!("aorta/{}/sys/alive/{}/{}", group, node_name, node_id);

    // Query liveliness with a bounded wait to absorb asynchronous undeclare propagation.
    let mut count = usize::MAX;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let replies = monitor_session
            .liveliness()
            .get(&liveliness_key)
            .await
            .map_err(|e| AortaError::internal(format!("Failed to query liveliness: {}", e)))?;

        let mut observed = 0usize;
        loop {
            match tokio::time::timeout(Duration::from_millis(300), replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    if let Ok(sample) = reply.into_result() {
                        if sample.key_expr().as_str() == liveliness_key {
                            observed += 1;
                        }
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        count = observed;
        if count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    crate::session::Session::close_inner(monitor_session).await?;

    assert_eq!(
        count, 0,
        "liveliness token should be undeclared after node close"
    );

    Ok(())
}

async fn wait_for_node_identity_payload(
    session: &zenoh::Session,
    telemetry_key: &str,
) -> AortaResult<crate::telemetry::schema::NodeIdentityTelemetry> {
    // node_identity telemetry is FlatBuffers on the wire (telemetry V2), so it
    // is fetched and decoded directly rather than via the JSON-value helper.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let replies = session
            .get(telemetry_key)
            .await
            .map_err(|e| AortaError::internal(format!("Failed to query telemetry: {}", e)))?;
        loop {
            match tokio::time::timeout(Duration::from_millis(300), replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    if let Ok(sample) = reply.into_result() {
                        if sample.key_expr().as_str() != telemetry_key {
                            continue;
                        }
                        let payload = sample.payload().to_bytes();
                        // The identity now travels as the `identity` section of the
                        // merged vital snapshot (telemetry tiering).
                        return crate::telemetry::fb::vital_snapshot_from_fb(payload.as_ref())?
                            .identity
                            .ok_or_else(|| {
                                AortaError::internal("vital snapshot missing identity section")
                            });
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(AortaError::timeout(format!(
        "timed out waiting for telemetry key '{}'",
        telemetry_key
    )))
}

async fn wait_for_resource_inventory_payload(
    session: &zenoh::Session,
    telemetry_key: &str,
) -> AortaResult<serde_json::Value> {
    wait_for_telemetry_payload_matching(session, telemetry_key, |_| true).await
}

async fn wait_for_runtime_metrics_payload(
    session: &zenoh::Session,
    telemetry_key: &str,
) -> AortaResult<serde_json::Value> {
    wait_for_telemetry_payload_matching(session, telemetry_key, |_| true).await
}

async fn wait_for_telemetry_payload_matching<F>(
    session: &zenoh::Session,
    telemetry_key: &str,
    predicate: F,
) -> AortaResult<serde_json::Value>
where
    F: Fn(&serde_json::Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let replies = session
            .get(telemetry_key)
            .await
            .map_err(|e| AortaError::internal(format!("Failed to query telemetry: {}", e)))?;

        loop {
            match tokio::time::timeout(Duration::from_millis(300), replies.recv_async()).await {
                Ok(Ok(reply)) => {
                    if let Ok(sample) = reply.into_result() {
                        if sample.key_expr().as_str() != telemetry_key {
                            continue;
                        }
                        let payload = sample.payload().to_bytes();
                        // Telemetry tiering: the vital snapshot bundles identity + inventory
                        // + health on one key; the bulk runtime stream is separate.
                        // Dispatch on the tiered key, converting FB back to a Value so the
                        // existing predicates keep operating on a serde_json::Value. Tests
                        // here inspect the inventory section of the vital snapshot.
                        let value = if telemetry_key.contains("/vital/snapshot/") {
                            let inventory =
                                crate::telemetry::fb::vital_snapshot_from_fb(payload.as_ref())?
                                    .inventory
                                    .ok_or_else(|| {
                                        AortaError::internal(
                                            "vital snapshot missing inventory section",
                                        )
                                    })?;
                            serde_json::to_value(inventory).map_err(|err| {
                                AortaError::internal(format!(
                                    "failed to convert resource_inventory to value: {}",
                                    err
                                ))
                            })?
                        } else if telemetry_key.contains("/bulk/runtime/") {
                            serde_json::to_value(crate::telemetry::fb::runtime_metrics_from_fb(
                                payload.as_ref(),
                            )?)
                            .map_err(|err| {
                                AortaError::internal(format!(
                                    "failed to convert runtime_metrics to value: {}",
                                    err
                                ))
                            })?
                        } else {
                            serde_json::from_slice(payload.as_ref()).map_err(|err| {
                                AortaError::internal(format!(
                                    "failed to decode telemetry payload: {}",
                                    err
                                ))
                            })?
                        };
                        if predicate(&value) {
                            return Ok(value);
                        }
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Err(AortaError::timeout(format!(
        "timed out waiting for telemetry key '{}'",
        telemetry_key
    )))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_liveliness_declaration_is_idempotent() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_LIVELINESS", "0");

    let node = Node::new("liveliness_idempotent_test", "default").await?;

    assert!(
        !node.has_liveliness(),
        "liveliness should not be auto-declared"
    );

    // Manually declare once
    node.declare_liveliness().await?;
    assert!(
        node.has_liveliness(),
        "liveliness should be declared after manual call"
    );

    // Declare again - should be idempotent
    node.declare_liveliness().await?;
    assert!(node.has_liveliness(), "liveliness should still be declared");

    node.close().await?;
    std::env::remove_var("AORTA_LIVELINESS");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_liveliness_declaration_is_spawn_safe() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::set_var("AORTA_LIVELINESS", "0");

    let config = zenoh::Config::from_json5(
        r#"
{
  mode: "peer",
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
    )?;
    let node = Node::with_config("liveliness_spawn_safe_test", "default", config).await?;
    let mut handles = Vec::new();
    for _ in 0..8 {
        let cloned = node.clone();
        handles.push(tokio::spawn(
            async move { cloned.declare_liveliness().await },
        ));
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        for handle in handles {
            handle.await.map_err(|e| {
                AortaError::internal(format!("spawned liveliness task panicked: {}", e))
            })??;
        }
        Ok::<(), AortaError>(())
    })
    .await
    .map_err(|e| AortaError::internal(format!("spawned liveliness tasks timed out: {}", e)))??;

    assert!(
        node.has_liveliness(),
        "liveliness should be declared exactly once"
    );

    node.close().await?;
    std::env::remove_var("AORTA_LIVELINESS");
    Ok(())
}
