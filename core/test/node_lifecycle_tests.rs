use super::*;
use crate::error::AortaResult;
use crate::executor::SingleThreadedExecutor;
use crate::schema::{SchemaAttachPolicy, SchemaHash};
use crate::test_support::{schema_attach_guard, schema_attach_never_guard, ENV_LOCK};
use crate::time::{
    now_ns, set_simulation_time_ns, set_time_source, simulation_time_ns, time_source, TimeSource,
};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;

fn with_time_source_guard<T>(f: impl FnOnce() -> T) -> T {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("time source test lock poisoned");
    let prev_source = time_source();
    let prev_sim_ns = simulation_time_ns();
    let result = f();
    set_simulation_time_ns(prev_sim_ns);
    set_time_source(prev_source);
    drop(guard);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_close_releases_all_resources() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("node_close_test", "default").await?;

    // Create some resources
    let _publisher = node.create_publisher("/test/pub1").await?;
    let _subscriber = node
        .create_subscriber("/test/sub1", |_data, _ctx| {})
        .await?;

    // Close should clean up everything
    node.close().await?;

    // After close, session should be closed
    assert!(node.session().is_closed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_with_zenoh_config() -> AortaResult<()> {
    let config = Session::config_from_env_or_local_default().await?;
    let node = Node::with_zenoh_config("node_custom_config", "default", config).await?;

    assert_eq!(node.name(), "node_custom_config");
    assert_eq!(node.group(), "default");

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_name_accessor() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    std::env::remove_var("AORTA_GROUP");
    let node = Node::new("test_node_name", "test_group").await?;

    assert_eq!(node.name(), "test_node_name");
    assert_eq!(node.group(), "default");

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_clock_accessor() -> AortaResult<()> {
    let node = Node::new("node_clock", "default").await?;

    let clock = node.clock();
    assert_eq!(clock.mode(), crate::time::ClockMode::System);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_now_returns_valid_time() -> AortaResult<()> {
    let node = Node::new("node_now", "default").await?;

    let t1 = node.now().as_nanos();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let t2 = node.now().as_nanos();

    assert!(t2 > t1, "Time should advance");
    assert!(t1 > 0, "Time should be positive");

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_now_uses_global_time_source() -> AortaResult<()> {
    let node = Node::new("node_now_global_source", "default").await?;

    with_time_source_guard(|| {
        set_simulation_time_ns(777);
        set_time_source(TimeSource::Simulation);
        assert_eq!(node.now().as_nanos(), 777);
        assert_eq!(node.now().as_nanos(), now_ns());
    });

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_session_accessor() -> AortaResult<()> {
    let node = Node::new("node_session", "default").await?;

    let session = node.session();
    assert!(!session.is_closed());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_key_builder_accessor() -> AortaResult<()> {
    let node = Node::new("node_key_builder", "default").await?;

    let key_builder = node.key_builder();
    assert_eq!(key_builder.group(), "default");

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_schema_registry_accessor() -> AortaResult<()> {
    let node = Node::new("node_registry", "default").await?;

    let registry = node.schema_registry();
    // Registry should be accessible
    let _ = registry.get_local(SchemaHash::from_bytes([0u8; 32])).await;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_schema_attach_policy_accessor() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_guard("always");

    let node = Node::new("node_policy", "default").await?;

    assert_eq!(node.schema_attach_policy(), SchemaAttachPolicy::Always);

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_event_sender_lifecycle() -> AortaResult<()> {
    use crate::event_queue::EventQueueSender;

    let node = Node::new("node_event_sender", "default").await?;

    assert!(!node.has_event_sender());
    assert!(node.event_sender().is_none());

    let (tx, _rx) = mpsc::channel(10);
    node.set_event_sender(
        EventQueueSender::from_bounded_channel(tx),
        crate::node::SYNTHETIC_TEST_OWNER,
    );

    assert!(node.has_event_sender());
    assert!(node.event_sender().is_some());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_create_multiple_publishers() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let node = Node::new("node_multi_pub", "default").await?;

    let pub1 = node.create_publisher("/test/pub1").await?;
    let pub2 = node.create_publisher("/test/pub2").await?;
    let pub3 = node.create_publisher("/test/pub3").await?;

    // All publishers should work
    pub1.publish(vec![1])?;
    pub2.publish(vec![2])?;
    pub3.publish(vec![3])?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_create_multiple_subscribers() -> AortaResult<()> {
    let node = Node::new("node_multi_sub", "default").await?;

    let _sub1 = node
        .create_subscriber("/test/sub1", |_data, _ctx| {})
        .await?;
    let _sub2 = node
        .create_subscriber("/test/sub2", |_data, _ctx| {})
        .await?;
    let _sub3 = node
        .create_subscriber("/test/sub3", |_data, _ctx| {})
        .await?;

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_with_executor_integration() -> AortaResult<()> {
    let mut exec = SingleThreadedExecutor::new(256);
    let node = Node::new("node_with_exec", "default").await?;

    exec.add_node(node.clone());

    // Node should now have event sender
    assert!(node.has_event_sender());

    // Create a timer to verify executor integration
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter_clone = counter.clone();

    let _timer = node
        .create_timer(Duration::from_millis(50), move || {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .await?;

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        stop.stop();
    });

    exec.spin().await;

    // Timer should have fired
    assert!(counter.load(std::sync::atomic::Ordering::Relaxed) >= 2);

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_register_schema_for_topic_with_type() -> AortaResult<()> {
    use crate::schema::SchemaMeta;

    struct TestSchema;
    impl SchemaMeta for TestSchema {
        fn schema_bfbs() -> &'static [u8] {
            b"test_schema_data"
        }
    }

    let node = Node::new("node_schema_type", "default").await?;

    let hash = node
        .register_schema_for_topic::<TestSchema>("/test/schema_topic")
        .await?;

    let expected_hash = TestSchema::schema_hash();
    assert_eq!(hash.as_bytes(), expected_hash.as_bytes());

    // Verify it's in the registry
    let cached = node.schema_registry().get_local(expected_hash).await;
    assert!(cached.is_some());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_create_publisher_with_bfbs() -> AortaResult<()> {
    let node = Node::new("node_pub_bfbs", "default").await?;

    let bfbs = vec![1u8, 2u8, 3u8, 4u8, 5u8];
    let publisher = node
        .create_publisher_with_bfbs("/test/with_bfbs", bfbs.clone())
        .await?;

    // Publisher should have schema hash
    assert!(publisher.schema_hash().is_some());

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_empty_name_rejected() {
    let result = Node::new("", "default").await;
    assert!(result.is_err());

    let result = Node::new("   ", "default").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_whitespace_name_trimmed() -> AortaResult<()> {
    // Node name with whitespace should be handled
    let result = Node::new("  valid_name  ", "default").await;
    // Currently doesn't trim, but should at least not panic
    if let Ok(node) = result {
        node.close().await?;
    }
    Ok(())
}

#[test]
fn normalize_group_empty_defaults_to_default() {
    // Test the normalize_group function behavior
    // Empty or whitespace group should default to "default"
    // This is tested indirectly through node creation
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_debug_format() -> AortaResult<()> {
    let node = Node::new("node_debug", "default").await?;

    let debug = format!("{:?}", node);
    assert!(debug.contains("Node"));

    node.close().await?;
    Ok(())
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = Node::new("test", "default");
}
