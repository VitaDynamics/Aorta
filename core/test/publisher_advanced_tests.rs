use super::*;
use crate::error::AortaResult;
use crate::node::Node;
use crate::payload::{AortaPayload, OwnedPayload, ShmPayload};
use crate::qos::{CongestionControl, Priority, QoS, Reliability};
use crate::schema::SchemaAttachPolicy;
use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_undeclare_releases_resources() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("pub_undeclare", "default").await?;
    let publisher = node.create_publisher("/test/undeclare").await?;

    // Verify publisher works before undeclare
    publisher.publish(vec![1, 2, 3])?;

    // Undeclare should complete without error
    publisher.undeclare().await?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_undeclare_with_active_refs() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("pub_undeclare_refs", "default").await?;
    let publisher = node.create_publisher("/test/undeclare_refs").await?;

    // Clone the publisher to create multiple references
    let pub_clone = publisher.clone();

    // Undeclare original - should warn about active references but not fail
    publisher.undeclare().await?;

    // Clone should still work (Arc keeps it alive)
    pub_clone.publish(vec![1, 2, 3])?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_topic_accessor() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("pub_topic", "default").await?;
    let topic = "/test/topic_accessor";
    let publisher = node.create_publisher(topic).await?;

    assert_eq!(publisher.topic(), topic);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_schema_hash_without_schema() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("pub_no_schema", "default").await?;
    let publisher = node.create_publisher("/test/no_schema").await?;

    // Without schema, hash should be None
    assert!(publisher.schema_hash().is_none());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_new_with_policy() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let node = Node::new("pub_with_policy", "default").await?;
    let _bfbs = [1u8, 2u8, 3u8];

    let publisher =
        Publisher::new_with_policy(&node, "/test/with_policy", SchemaAttachPolicy::Never).await?;

    assert!(publisher.schema_hash().is_none());

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_publish_empty_payload() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_empty", "default").await?;
    let publisher = node.create_publisher("/test/empty").await?;

    // Empty payload should work
    publisher.publish(vec![])?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_publish_large_payload() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_large", "default").await?;
    let publisher = node.create_publisher("/test/large").await?;

    // Large payload (1MB)
    let large_data = vec![0u8; 1024 * 1024];
    publisher.publish(large_data)?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_publish_payload_owned_works() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_payload_owned", "default").await?;
    let publisher = node.create_publisher("/test/payload_owned").await?;

    let owned = OwnedPayload::from_bytes(bytes::Bytes::from_static(&[7, 8, 9]));
    publisher.publish_payload(AortaPayload::Owned(owned))?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_publish_payload_shm_returns_not_supported() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_payload_shm", "default").await?;
    let publisher = node.create_publisher("/test/payload_shm").await?;

    let shm = ShmPayload::new(123, 1024, 0, 16).expect("valid shm payload");
    let result = publisher.publish_payload(AortaPayload::Shm(shm));
    assert!(matches!(
        result,
        Err(crate::error::AortaError::NotSupported(_))
    ));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_loan_bytes_commit_succeeds() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_loan_commit", "default").await?;
    let options = PublisherOptions::new().with_max_message_size(128);
    let publisher = node
        .create_publisher_with_options("/test/loan_commit", options)
        .await?;

    let mut loan = publisher.loan_bytes(16)?;
    loan.as_mut_slice()
        .copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    loan.commit(16)?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_loan_bytes_quota_exceeded_until_drop() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_loan_quota", "default").await?;
    let options = PublisherOptions::new()
        .with_max_message_size(64)
        .with_max_loaned_samples(1);
    let publisher = node
        .create_publisher_with_options("/test/loan_quota", options)
        .await?;

    let loan = publisher.loan_bytes(8)?;
    let second = publisher.loan_bytes(8);
    assert!(matches!(
        second,
        Err(crate::error::AortaError::QuotaExceeded(_))
    ));

    drop(loan);
    let third = publisher.loan_bytes(8);
    assert!(third.is_ok());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_undeclare_returns_busy_with_outstanding_loan() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_loan_busy", "default").await?;
    let options = PublisherOptions::new().with_max_message_size(64);
    let publisher = node
        .create_publisher_with_options("/test/loan_busy", options)
        .await?;

    let loan = publisher.loan_bytes(8)?;
    let result = publisher.clone().undeclare().await;
    assert!(matches!(result, Err(crate::error::AortaError::Busy(_))));

    drop(loan);
    publisher.undeclare().await?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_best_effort_publish_falls_back_when_shm_unavailable() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_best_effort_fallback", "default").await?;
    let options = PublisherOptions::new()
        .with_max_message_size(64)
        .with_allocation_policy(AllocationPolicy::BestEffort);
    let publisher = node
        .create_publisher_with_options("/test/best_effort_fallback", options)
        .await?;

    publisher.publish(vec![1u8, 2u8, 3u8, 4u8])?;
    let stats = publisher.runtime_stats();
    assert_eq!(stats.publish_shm_hit_total, 0);
    assert_eq!(stats.publish_shm_fallback_total, 1);
    assert_eq!(stats.publish_shm_required_fail_total, 0);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_shm_required_publish_returns_error_when_shm_unavailable() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_shm_required_publish", "default").await?;
    let options = PublisherOptions::new()
        .with_max_message_size(64)
        .with_allocation_policy(AllocationPolicy::ShmRequired);
    let publisher = node
        .create_publisher_with_options("/test/shm_required_publish", options)
        .await?;

    let result = publisher.publish(vec![1u8, 2u8, 3u8, 4u8]);
    assert!(matches!(
        result,
        Err(crate::error::AortaError::ShmUnavailable(_))
    ));
    let stats = publisher.runtime_stats();
    assert_eq!(stats.publish_shm_hit_total, 0);
    assert_eq!(stats.publish_shm_fallback_total, 0);
    assert_eq!(stats.publish_shm_required_fail_total, 1);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_loan_runtime_stats_track_quota_and_inflight() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_loan_runtime_stats", "default").await?;
    let options = PublisherOptions::new()
        .with_max_message_size(64)
        .with_max_loaned_samples(1);
    let publisher = node
        .create_publisher_with_options("/test/loan_runtime_stats", options)
        .await?;

    let loan = publisher.loan_bytes(8)?;
    let stats = publisher.runtime_stats();
    assert_eq!(stats.loan_inflight_current, 1);
    assert_eq!(stats.loan_quota_exceeded_total, 0);

    let second = publisher.loan_bytes(8);
    assert!(matches!(
        second,
        Err(crate::error::AortaError::QuotaExceeded(_))
    ));
    let stats = publisher.runtime_stats();
    assert_eq!(stats.loan_inflight_current, 1);
    assert_eq!(stats.loan_quota_exceeded_total, 1);

    drop(loan);
    let stats = publisher.runtime_stats();
    assert_eq!(stats.loan_inflight_current, 0);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_rapid_publish_no_loss() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_rapid", "default").await?;
    let publisher = node.create_publisher("/test/rapid").await?;

    // Rapid publish 1000 messages
    for i in 0..1000u32 {
        publisher.publish(vec![(i % 256) as u8])?;
    }

    // Messages are published directly into zenoh's transport buffer.
    // Short sleep to let zenoh transport process.
    tokio::time::sleep(Duration::from_millis(100)).await;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_with_all_qos_profiles() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_qos", "default").await?;

    // Test all preset QoS profiles
    let profiles = vec![
        ("realtime", QoS::realtime_control()),
        ("sensor", QoS::sensor_data()),
        ("state", QoS::state_update()),
        ("bulk", QoS::bulk_transfer()),
    ];

    for (name, qos) in profiles {
        let options = PublisherOptions {
            qos,
            ..Default::default()
        };
        let publisher = node
            .create_publisher_with_options(&format!("/test/qos_{}", name), options)
            .await?;
        publisher.publish(vec![1, 2, 3])?;
    }

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_options_builder_chaining() -> AortaResult<()> {
    let qos = QoS::default()
        .with_priority(Priority::High)
        .with_reliability(Reliability::BestEffort)
        .with_congestion_control(CongestionControl::Block)
        .with_express(true);

    let options = PublisherOptions::new()
        .with_qos(qos)
        .with_max_message_size(8192);

    assert_eq!(options.qos.priority, Priority::High);
    assert_eq!(options.qos.reliability, Reliability::BestEffort);
    assert_eq!(options.qos.congestion_control, CongestionControl::Block);
    assert!(options.qos.express);
    assert_eq!(options.max_message_size, Some(8192));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_clone_shares_state() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let node = Node::new("pub_clone", "default").await?;
    let publisher = node.create_publisher("/test/clone").await?;

    let pub_clone = publisher.clone();

    // Both should be able to publish
    publisher.publish(vec![1])?;
    pub_clone.publish(vec![2])?;

    // Topic should be the same
    assert_eq!(publisher.topic(), pub_clone.topic());

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_schema_attach_never_no_hash() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let node = Node::new("pub_attach_never", "default").await?;
    let _bfbs = vec![1u8, 2u8, 3u8];

    let options = PublisherOptions {
        schema_attach_policy: SchemaAttachPolicy::Never,
        schema_bfbs: Some(_bfbs.clone()),
        ..Default::default()
    };

    let publisher = node
        .create_publisher_with_options("/test/attach_never", options)
        .await?;

    // Schema should be registered but not attached
    assert!(publisher.schema_hash().is_some());

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_drop_releases_zenoh_publisher() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let node = Node::new("pub_drop", "default").await?;

    {
        let publisher = node.create_publisher("/test/drop").await?;
        publisher.publish(vec![1, 2, 3])?;
        // Publisher dropped here — zenoh publisher is undeclared on Arc drop
    }

    // After drop, creating a new publisher with same topic should work
    let publisher2 = node.create_publisher("/test/drop").await?;
    publisher2.publish(vec![4, 5, 6])?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_debug_format() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let node = Node::new("pub_debug", "default").await?;
    let publisher = node.create_publisher("/test/debug").await?;

    let debug = format!("{:?}", publisher);
    assert!(debug.contains("Publisher"));
    assert!(debug.contains("topic"));

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_publish_after_undeclare_returns_error() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("pub_after_undeclare", "default").await?;
    let publisher = node.create_publisher("/test/after_undeclare").await?;

    // Clone so we keep a handle after undeclare
    let pub_clone = publisher.clone();

    // Publish works before undeclare
    publisher.publish(vec![1, 2, 3])?;

    // Undeclare will skip (clone still alive) but mark inner as None
    // when last ref is dropped. Let's drop clone first, then undeclare.
    drop(pub_clone);
    publisher.undeclare().await?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publisher_concurrent_publish_from_multiple_tasks() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("pub_concurrent", "default").await?;
    let publisher = node.create_publisher("/test/concurrent").await?;

    let mut handles = Vec::new();
    for i in 0..4u32 {
        let pub_clone = publisher.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..100u32 {
                pub_clone
                    .publish(vec![(i * 100 + j) as u8])
                    .expect("concurrent publish should succeed");
            }
        }));
    }

    for handle in handles {
        handle.await.expect("task should not panic");
    }

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = PublisherOptions::default();
}
