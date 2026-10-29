use super::{
    should_log_policy_event, AllocationPolicy, HeapLoanAllocator, LoanTracker, Publisher,
    PublisherOptions, PublisherRuntimeStatsInner, PublisherShared,
};
use crate::error::AortaResult;
use crate::node::Node;
use crate::payload::AortaPayload;
use crate::qos::{CongestionControl, Priority, QoS, Reliability};
use crate::schema::{SchemaAttachPolicy, SchemaHash, SchemaMeta};
use crate::schema_registry::SchemaByKeyValue;
use crate::telemetry::TelemetryResourceGuard;
use crate::test_support::{schema_attach_guard, ENV_LOCK};
use crate::transport::TransportPublisher;
use crate::ZBytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

struct DummySchema;

fn unique_name(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

impl SchemaMeta for DummySchema {
    fn schema_bfbs() -> &'static [u8] {
        b"dummy_schema_bfbs"
    }

    fn root_type() -> Option<&'static str> {
        Some("demo.DummySchema")
    }
}

#[test]
fn publisher_options_defaults() {
    let options = PublisherOptions::default();
    assert_eq!(options.schema_attach_policy, SchemaAttachPolicy::Inherit);
    assert!(options.schema_bfbs.is_none());
    assert!(options.schema_root_type.is_none());
    assert_eq!(options.qos.priority, Priority::Normal);
    assert_eq!(options.qos.reliability, Reliability::Reliable);
    assert_eq!(options.qos.congestion_control, CongestionControl::Drop);
    assert_eq!(options.congestion_control, None);
    assert!(options.max_message_size.is_none());
    assert_eq!(options.allocation_policy, AllocationPolicy::HeapOnly);
    assert!(options.max_loaned_samples.is_none());
    assert!(options.include_publish_timestamp);
}

#[test]
fn publisher_options_builder_methods() {
    let qos = QoS::default().with_priority(Priority::High);
    let options = PublisherOptions::new()
        .with_qos(qos)
        .with_max_message_size(4096)
        .with_allocation_policy(AllocationPolicy::BestEffort)
        .with_max_loaned_samples(32);

    assert_eq!(options.qos.priority, Priority::High);
    assert_eq!(options.max_message_size, Some(4096));
    assert_eq!(options.allocation_policy, AllocationPolicy::BestEffort);
    assert_eq!(options.max_loaned_samples, Some(32));
}

#[test]
fn should_log_policy_event_logs_first_and_interval_boundaries() {
    assert!(should_log_policy_event(1));
    assert!(!should_log_policy_event(2));
    assert!(!should_log_policy_event(1023));
    assert!(should_log_policy_event(1024));
    assert!(!should_log_policy_event(1025));
    assert!(should_log_policy_event(2048));
}

#[derive(Default)]
struct MockTransportPublisher {
    publish_zbytes_calls: AtomicU64,
    publish_payload_calls: AtomicU64,
    last_zbytes: Mutex<Option<Vec<u8>>>,
}

impl TransportPublisher for MockTransportPublisher {
    fn publish_zbytes(&self, payload: ZBytes, _attachment: Option<ZBytes>) -> AortaResult<()> {
        self.publish_zbytes_calls.fetch_add(1, Ordering::Relaxed);
        *self.last_zbytes.lock().expect("lock") = Some(payload.to_bytes().into_owned());
        Ok(())
    }

    fn publish(&self, payload: AortaPayload, _attachment: Option<ZBytes>) -> AortaResult<()> {
        self.publish_payload_calls.fetch_add(1, Ordering::Relaxed);
        match payload {
            AortaPayload::Owned(owned) => {
                *self.last_zbytes.lock().expect("lock") = Some(owned.payload().to_vec());
            }
            AortaPayload::Shm(_) => {
                *self.last_zbytes.lock().expect("lock") = None;
            }
        }
        Ok(())
    }

    fn undeclare(&self) -> AortaResult<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_heap_only_publish_uses_direct_zbytes_without_shm_hit_stats() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_heap_direct"), "default").await?;
    let transport = Arc::new(MockTransportPublisher::default());
    let publisher = Publisher {
        shared: Arc::new(PublisherShared {
            transport: transport.clone(),
            session: node.session().clone(),
            topic: "test/heap_direct".to_string(),
            schema_hash: None,
            schema_attachment: None,
            topic_publish_event: 0,
            allocation_policy: AllocationPolicy::HeapOnly,
            max_message_size: None,
            loan_tracker: Arc::new(LoanTracker::new(None)),
            heap_allocator: Arc::new(HeapLoanAllocator::default()),
            runtime_stats: Arc::new(PublisherRuntimeStatsInner::default()),
            inventory_guard: TelemetryResourceGuard::disabled(),
            runtime_guard: crate::telemetry::RuntimeTelemetryGuard::disabled(),
        }),
        clone_token: Arc::new(()),
    };

    let mut writer = ZBytes::writer();
    writer.append(ZBytes::from(vec![1u8, 2u8]));
    writer.append(ZBytes::from(vec![3u8, 4u8]));
    let payload = writer.finish();

    publisher.publish(payload)?;

    assert_eq!(transport.publish_zbytes_calls.load(Ordering::Relaxed), 1);
    assert_eq!(transport.publish_payload_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        transport.last_zbytes.lock().expect("lock").clone(),
        Some(vec![1u8, 2u8, 3u8, 4u8])
    );
    let stats = publisher.runtime_stats();
    assert_eq!(stats.publish_shm_hit_total, 0);
    assert_eq!(stats.publish_shm_fallback_total, 0);
    assert_eq!(stats.publish_shm_required_fail_total, 0);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_best_effort_publish_falls_back_to_heap_payload_when_shm_unavailable(
) -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_direct_bridge_fallback"), "default").await?;
    let transport = Arc::new(MockTransportPublisher::default());
    let publisher = Publisher {
        shared: Arc::new(PublisherShared {
            transport: transport.clone(),
            session: node.session().clone(),
            topic: "test/direct_bridge_fallback".to_string(),
            schema_hash: None,
            schema_attachment: None,
            topic_publish_event: 0,
            allocation_policy: AllocationPolicy::BestEffort,
            max_message_size: Some(64),
            loan_tracker: Arc::new(LoanTracker::new(None)),
            heap_allocator: Arc::new(HeapLoanAllocator::default()),
            runtime_stats: Arc::new(PublisherRuntimeStatsInner::default()),
            inventory_guard: TelemetryResourceGuard::disabled(),
            runtime_guard: crate::telemetry::RuntimeTelemetryGuard::disabled(),
        }),
        clone_token: Arc::new(()),
    };

    let mut writer = ZBytes::writer();
    writer.append(ZBytes::from(vec![1u8, 2u8]));
    writer.append(ZBytes::from(vec![3u8, 4u8]));
    let payload = writer.finish();

    publisher.publish(payload)?;

    assert_eq!(transport.publish_zbytes_calls.load(Ordering::Relaxed), 0);
    assert_eq!(transport.publish_payload_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        transport.last_zbytes.lock().expect("lock").clone(),
        Some(vec![1u8, 2u8, 3u8, 4u8])
    );
    let stats = publisher.runtime_stats();
    assert_eq!(stats.publish_shm_hit_total, 0);
    assert_eq!(stats.publish_shm_fallback_total, 1);
    assert_eq!(stats.publish_shm_required_fail_total, 0);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_shm_required_direct_bridge_returns_error_when_shm_unavailable() -> AortaResult<()>
{
    let node = Node::new(&unique_name("pub_direct_bridge_required"), "default").await?;
    let transport = Arc::new(MockTransportPublisher::default());
    let publisher = Publisher {
        shared: Arc::new(PublisherShared {
            transport: transport.clone(),
            session: node.session().clone(),
            topic: "test/direct_bridge_required".to_string(),
            schema_hash: None,
            schema_attachment: None,
            topic_publish_event: 0,
            allocation_policy: AllocationPolicy::ShmRequired,
            max_message_size: Some(64),
            loan_tracker: Arc::new(LoanTracker::new(None)),
            heap_allocator: Arc::new(HeapLoanAllocator::default()),
            runtime_stats: Arc::new(PublisherRuntimeStatsInner::default()),
            inventory_guard: TelemetryResourceGuard::disabled(),
            runtime_guard: crate::telemetry::RuntimeTelemetryGuard::disabled(),
        }),
        clone_token: Arc::new(()),
    };

    let payload = ZBytes::from(vec![1u8, 2u8, 3u8, 4u8]);
    let result = publisher.publish(payload);
    assert!(matches!(
        result,
        Err(crate::error::AortaError::ShmUnavailable(_))
    ));
    assert_eq!(transport.publish_zbytes_calls.load(Ordering::Relaxed), 0);
    assert_eq!(transport.publish_payload_calls.load(Ordering::Relaxed), 0);
    let stats = publisher.runtime_stats();
    assert_eq!(stats.publish_shm_hit_total, 0);
    assert_eq!(stats.publish_shm_fallback_total, 0);
    assert_eq!(stats.publish_shm_required_fail_total, 1);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_requires_schema_when_attach_always() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_schema_required"), "default").await?;
    let topic = unique_name("schema_required_topic");
    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Always,
        schema_bfbs: None,
        ..Default::default()
    };

    let result = node.create_publisher_with_options(&topic, options).await;
    assert!(result.is_err());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_shm_required_needs_max_message_size() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_shm_required"), "default").await?;
    let topic = unique_name("shm_required_topic");
    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Never,
        allocation_policy: AllocationPolicy::ShmRequired,
        max_message_size: None,
        ..Default::default()
    };

    let result = node.create_publisher_with_options(&topic, options).await;
    assert!(result.is_err());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_shm_required_with_max_message_size_succeeds() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_shm_required_ok"), "default").await?;
    let topic = unique_name("shm_required_ok_topic");
    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Never,
        allocation_policy: AllocationPolicy::ShmRequired,
        max_message_size: Some(4096),
        ..Default::default()
    };

    let publisher = node.create_publisher_with_options(&topic, options).await?;
    assert_eq!(publisher.topic(), topic);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_inherit_policy_uses_node_default() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_guard("always");
    let node = Node::new(&unique_name("pub_inherit_policy"), "default").await?;
    let topic = unique_name("schema_inherit_topic");

    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Inherit,
        schema_bfbs: None,
        ..Default::default()
    };

    let result = node.create_publisher_with_options(&topic, options).await;
    assert!(result.is_err());

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_with_schema_bfbs_registers_hash() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_schema_register"), "default").await?;
    let topic = unique_name("schema_with_bfbs_topic");
    let bfbs = vec![9u8, 8u8, 7u8, 6u8];
    let expected = SchemaHash::from_bfbs_bytes(&bfbs);

    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Always,
        schema_bfbs: Some(bfbs.clone()),
        ..Default::default()
    };

    let publisher = node.create_publisher_with_options(&topic, options).await?;
    assert_eq!(
        publisher.schema_hash().map(|h| h.as_bytes().to_vec()),
        Some(expected.as_bytes().to_vec())
    );

    let cached = node.schema_registry().get_local(expected).await;
    assert_eq!(cached, Some(bfbs.clone()));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_schema_by_key_conflict_logs_and_succeeds() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_schema_conflict"), "default").await?;
    let topic = unique_name("schema_conflict_topic");
    let key_suffix = node.key_builder().key_suffix_for_topic(&topic)?;

    let bfbs_existing = vec![1u8, 1u8, 1u8];
    let hash_existing = SchemaHash::from_bfbs_bytes(&bfbs_existing);
    let value = SchemaByKeyValue::topic(hash_existing, bfbs_existing.clone(), None);
    node.schema_registry()
        .insert_schema_by_key_local(key_suffix.clone(), value)
        .await;

    let bfbs_new = vec![2u8, 2u8, 2u8];
    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Always,
        schema_bfbs: Some(bfbs_new),
        ..Default::default()
    };

    let result = node.create_publisher_with_options(&topic, options).await;
    assert!(result.is_ok());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_schema_by_key_match_succeeds() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_schema_match"), "default").await?;
    let topic = unique_name("schema_match_topic");
    let key_suffix = node.key_builder().key_suffix_for_topic(&topic)?;

    let bfbs = vec![3u8, 3u8, 3u8];
    let hash = SchemaHash::from_bfbs_bytes(&bfbs);
    let value = SchemaByKeyValue::topic(hash, bfbs.clone(), None);
    node.schema_registry()
        .insert_schema_by_key_local(key_suffix.clone(), value)
        .await;

    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Always,
        schema_bfbs: Some(bfbs.clone()),
        ..Default::default()
    };

    let publisher = node.create_publisher_with_options(&topic, options).await?;
    assert_eq!(
        publisher.schema_hash().map(|h| h.as_bytes().to_vec()),
        Some(hash.as_bytes().to_vec())
    );

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_create_publisher_with_schema_registers_cache() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_schema_create"), "default").await?;
    let topic = unique_name("schema_create_with_schema_topic");
    let expected = DummySchema::schema_hash();

    let publisher = node
        .create_publisher_with_schema::<DummySchema>(&topic)
        .await?;
    assert_eq!(
        publisher.schema_hash().map(|h| h.as_bytes().to_vec()),
        Some(expected.as_bytes().to_vec())
    );

    let cached = node.schema_registry().get_local(expected).await;
    assert_eq!(cached, Some(DummySchema::schema_bfbs().to_vec()));

    let key_suffix = node.key_builder().key_suffix_for_topic(&topic)?;
    let by_key = node
        .schema_registry()
        .get_schema_by_key_local(&key_suffix)
        .await;
    assert!(by_key.is_some());
    let by_key = by_key.unwrap();
    assert_eq!(by_key.hash().as_bytes(), expected.as_bytes());
    assert_eq!(by_key.bfbs(), DummySchema::schema_bfbs());
    assert_eq!(by_key.root_type(), Some("demo.DummySchema"));

    node.close().await?;
    Ok(())
}

/// Functional check for the matching readiness gate (the fix for the
/// publish-before-route-known race). Matching resolves via `.await`, never
/// zenoh's sync `.wait()` (which would `block_in_place`-panic on the SDK's
/// current_thread callback executor). A zenoh session requires a multi_thread
/// runtime (zenoh rejects current_thread at `open`), so this runs on
/// multi_thread; the "matching never calls `.wait()`" property is structural.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_status_tracks_subscriber_presence() -> AortaResult<()> {
    let node = Node::new(&unique_name("pub_match_fn"), "default").await?;
    let topic = unique_name("match_fn_topic");
    let publisher = node.create_publisher(&topic).await?;

    // No subscriber yet: not matching; a bounded wait times out -> Ok(false).
    assert!(!publisher.matching_status().await?);
    assert!(
        !publisher
            .wait_for_matching(std::time::Duration::from_millis(50))
            .await?,
        "no subscriber declared; expected Ok(false) after timeout"
    );

    // Declare a matching subscriber; matching must converge to true.
    let subscriber = node.create_subscriber(&topic, |_data, _ctx| {}).await?;
    assert!(
        publisher
            .wait_for_matching(std::time::Duration::from_secs(2))
            .await?,
        "subscriber declared; matching should become true"
    );
    assert!(publisher.matching_status().await?);

    drop(subscriber);
    node.close().await?;
    Ok(())
}
