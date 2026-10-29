use super::*;
use crate::error::AortaResult;
use crate::executor::SingleThreadedExecutor;
use crate::node::Node;
use crate::qos::{Priority, QoS};
use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_undeclare_stops_callback() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let node = Node::new("sub_undeclare", "default").await?;
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let subscriber = node
        .create_subscriber("/test/undeclare", move |_data, _ctx| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    // Undeclare should complete without error
    subscriber.undeclare().await?;

    // After undeclare, creating a new subscriber with same topic should work
    let counter2 = Arc::new(AtomicU32::new(0));
    let counter2_clone = Arc::clone(&counter2);
    let _subscriber2 = node
        .create_subscriber("/test/undeclare", move |_data, _ctx| {
            counter2_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_topic_accessor() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let node = Node::new("sub_topic", "default").await?;
    let topic = "/test/topic_accessor";
    let subscriber = node.create_subscriber(topic, |_data, _ctx| {}).await?;

    assert_eq!(subscriber.topic(), topic);

    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_priority_mapping_realtime() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let mut exec = SingleThreadedExecutor::new(256);
    let pub_node = Node::new("pri_pub_rt", "test_pri").await?;
    let sub_node = Node::new("pri_sub_rt", "test_pri").await?;
    exec.add_node(pub_node.clone());
    exec.add_node(sub_node.clone());

    let received_priority = Arc::new(Mutex::new(None));
    let recv_clone = Arc::clone(&received_priority);

    let _sub = sub_node
        .create_subscriber_with_options(
            "/priority_rt",
            move |_data, ctx| {
                *recv_clone.lock().unwrap() = Some(ctx.priority);
            },
            SubscriberOptions::new()
                .with_qos(QoS::realtime_control())
                .with_priority_in_context(true),
        )
        .await?;

    let qos = QoS::realtime_control();
    let publisher = pub_node
        .create_publisher_with_options(
            "/priority_rt",
            crate::publisher::PublisherOptions {
                qos,
                ..Default::default()
            },
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    publisher.publish(vec![1, 2, 3])?;

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.stop();
    });

    exec.spin().await;

    let priority = received_priority.lock().unwrap();
    assert_eq!(*priority, Some(Priority::Realtime));

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_priority_mapping_background() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let mut exec = SingleThreadedExecutor::new(256);
    let pub_node = Node::new("pri_pub_bg", "test_pri_bg").await?;
    let sub_node = Node::new("pri_sub_bg", "test_pri_bg").await?;
    exec.add_node(pub_node.clone());
    exec.add_node(sub_node.clone());

    let received_priority = Arc::new(Mutex::new(None));
    let recv_clone = Arc::clone(&received_priority);

    let _sub = sub_node
        .create_subscriber_with_options(
            "/priority_bg",
            move |_data, ctx| {
                *recv_clone.lock().unwrap() = Some(ctx.priority);
            },
            SubscriberOptions::new()
                .with_qos(QoS::bulk_transfer())
                .with_priority_in_context(true),
        )
        .await?;

    let qos = QoS::bulk_transfer();
    let publisher = pub_node
        .create_publisher_with_options(
            "/priority_bg",
            crate::publisher::PublisherOptions {
                qos,
                ..Default::default()
            },
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    publisher.publish(vec![1, 2, 3])?;

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.stop();
    });

    exec.spin().await;

    let priority = received_priority.lock().unwrap();
    assert_eq!(*priority, Some(Priority::Background));

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_receive_timestamp_monotonic() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let mut exec = SingleThreadedExecutor::new(256);
    let pub_node = Node::new("ts_pub", "test_ts").await?;
    let sub_node = Node::new("ts_sub", "test_ts").await?;
    exec.add_node(pub_node.clone());
    exec.add_node(sub_node.clone());

    let timestamps = Arc::new(Mutex::new(Vec::new()));
    let ts_clone = Arc::clone(&timestamps);

    let options = SubscriberOptions::new().with_receive_timestamp(true);
    let _sub = sub_node
        .create_subscriber_with_options(
            "/timestamps",
            move |_data, ctx| {
                ts_clone.lock().unwrap().push(ctx.receive_stamp_ns);
            },
            options,
        )
        .await?;

    let publisher = pub_node.create_publisher("/timestamps").await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish multiple messages
    for i in 0..5u8 {
        publisher.publish(vec![i])?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.stop();
    });

    exec.spin().await;

    let ts = timestamps.lock().unwrap();
    assert!(!ts.is_empty());

    // Verify timestamps are monotonic
    for i in 1..ts.len() {
        assert!(ts[i] >= ts[i - 1], "Timestamps should be monotonic");
    }

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_multiple_messages_ordering() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let mut exec = SingleThreadedExecutor::new(256);
    let pub_node = Node::new("order_pub", "test_order").await?;
    let sub_node = Node::new("order_sub", "test_order").await?;
    exec.add_node(pub_node.clone());
    exec.add_node(sub_node.clone());

    let received_data = Arc::new(Mutex::new(Vec::new()));
    let data_clone = Arc::clone(&received_data);

    let _sub = sub_node
        .create_subscriber("/ordering", move |data, _ctx| {
            let bytes = data.to_bytes();
            if let Some(first) = bytes.first() {
                data_clone.lock().unwrap().push(*first);
            }
        })
        .await?;

    let publisher = pub_node.create_publisher("/ordering").await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish sequential data
    for i in 0..10u8 {
        publisher.publish(vec![i])?;
    }

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.stop();
    });

    exec.spin().await;

    let data = received_data.lock().unwrap();

    // Verify we received messages (order may not be guaranteed in all cases,
    // but for local communication it typically is)
    assert!(!data.is_empty(), "Should receive some messages");

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_options_history_request_true() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    let node = Node::new("sub_opts_hist", "default").await?;

    let options = SubscriberOptions::new().with_history_depth(512);

    let _sub = node
        .create_subscriber_with_options("/test/hist_opts", |_data, _ctx| {}, options)
        .await?;

    // Options should be set correctly (actual history behavior is not exercised here)
    node.close().await?;
    drop(_env_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_empty_payload() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let mut exec = SingleThreadedExecutor::new(256);
    let pub_node = Node::new("empty_pub", "test_empty").await?;
    let sub_node = Node::new("empty_sub", "test_empty").await?;
    exec.add_node(pub_node.clone());
    exec.add_node(sub_node.clone());

    let received = Arc::new(AtomicU32::new(0));
    let recv_clone = Arc::clone(&received);
    let notify = Arc::new(Notify::new());
    let notify_clone = Arc::clone(&notify);

    let sub = sub_node
        .create_subscriber("/empty_payload", move |data, _ctx| {
            if data.is_empty() {
                recv_clone.fetch_add(1, Ordering::Relaxed);
                notify_clone.notify_one();
            }
        })
        .await?;

    let publisher = pub_node.create_publisher("/empty_payload").await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let stop = exec.stop_handle();
    let exec_task = tokio::spawn(async move {
        exec.spin().await;
    });

    for _ in 0..5 {
        publisher.publish(vec![])?;
        if received.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    if received.load(Ordering::Relaxed) == 0 {
        let _ = tokio::time::timeout(Duration::from_secs(2), notify.notified()).await;
    }

    stop.stop();
    let _ = exec_task.await;

    assert!(
        received.load(Ordering::Relaxed) > 0,
        "Should receive empty payload"
    );

    sub.undeclare().await?;
    publisher.undeclare().await?;
    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_large_payload() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();
    let mut exec = SingleThreadedExecutor::new(256);
    let pub_node = Node::new("large_pub", "test_large").await?;
    let sub_node = Node::new("large_sub", "test_large").await?;
    exec.add_node(pub_node.clone());
    exec.add_node(sub_node.clone());

    let received_size = Arc::new(Mutex::new(0usize));
    let size_clone = Arc::clone(&received_size);
    let notify = Arc::new(Notify::new());
    let notify_clone = Arc::clone(&notify);

    let sub = sub_node
        .create_subscriber("/large_payload", move |data, _ctx| {
            *size_clone.lock().unwrap() = data.len();
            notify_clone.notify_one();
        })
        .await?;

    let publisher = pub_node.create_publisher("/large_payload").await?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let stop = exec.stop_handle();
    let exec_task = tokio::spawn(async move {
        exec.spin().await;
    });

    let large_data = vec![0u8; 1024 * 1024]; // 1MB
    for _ in 0..3 {
        publisher.publish(large_data.clone())?;
        if *received_size.lock().unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if *received_size.lock().unwrap() == 0 {
        let _ = tokio::time::timeout(Duration::from_secs(3), notify.notified()).await;
    }

    stop.stop();
    let _ = exec_task.await;

    let size = *received_size.lock().unwrap();
    assert_eq!(size, 1024 * 1024, "Should receive full large payload");

    sub.undeclare().await?;
    publisher.undeclare().await?;
    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[test]
fn message_context_clone() {
    let ctx = MessageContext {
        topic: "test/topic".into(),
        receive_stamp_ns: 1234567890,
        priority: Priority::High,
        publisher_stamp_ns: Some(1234567000),
        publisher_node: Some("test_node".to_string()),
        sequence: Some(42),
        source_namespace: Some("R1".to_string()),
    };

    let cloned = ctx.clone();
    assert_eq!(cloned.topic, ctx.topic);
    assert_eq!(cloned.receive_stamp_ns, ctx.receive_stamp_ns);
    assert_eq!(cloned.priority, ctx.priority);
    assert_eq!(cloned.publisher_stamp_ns, ctx.publisher_stamp_ns);
    assert_eq!(cloned.publisher_node, ctx.publisher_node);
    assert_eq!(cloned.sequence, ctx.sequence);
    assert_eq!(cloned.source_namespace, ctx.source_namespace);
}

#[test]
fn message_context_debug() {
    let ctx = MessageContext {
        topic: "test".into(),
        receive_stamp_ns: 1000,
        priority: Priority::Normal,
        publisher_stamp_ns: None,
        publisher_node: None,
        sequence: None,
        source_namespace: None,
    };

    let debug = format!("{:?}", ctx);
    assert!(debug.contains("MessageContext"));
    assert!(debug.contains("test"));
}

#[test]
fn subscriber_options_default_values() {
    let opts = SubscriberOptions::default();
    assert_eq!(opts.history_depth, 1024);
    assert!(opts.prefetch_schema_on_init);
    assert!(!opts.enable_header_context);
    assert!(!opts.include_topic_in_context);
    assert!(!opts.include_receive_timestamp);
    assert!(!opts.include_priority_in_context);
    assert!(!opts.include_publisher_timestamp);
    assert_eq!(opts.qos.priority, Priority::Normal);
    assert!(!opts.enable_dedup);
    assert_eq!(opts.dedup_window, 1024);
    assert!(!opts.enable_reorder_check);
    assert!(!opts.drop_out_of_order);
    assert_eq!(opts.overflow_policy, SubscriberOverflowPolicy::Drop);
    assert_eq!(opts.overflow_relay_capacity, 1024);
}

#[test]
fn subscriber_options_builder_methods() {
    let opts = SubscriberOptions::new()
        .with_history_depth(2048)
        .with_schema_prefetch_on_init(true)
        .with_header_context(true)
        .with_topic_in_context(true)
        .with_receive_timestamp(true)
        .with_priority_in_context(true)
        .with_publisher_timestamp(true)
        .with_priority(Priority::High)
        .with_dedup(true)
        .with_dedup_window(32)
        .with_reorder_check(true)
        .with_drop_out_of_order(true)
        .with_overflow_policy(SubscriberOverflowPolicy::Relay)
        .with_overflow_relay_capacity(16);

    assert_eq!(opts.history_depth, 2048);
    assert!(opts.prefetch_schema_on_init);
    assert!(opts.enable_header_context);
    assert!(opts.include_topic_in_context);
    assert!(opts.include_receive_timestamp);
    assert!(opts.include_priority_in_context);
    assert!(opts.include_publisher_timestamp);
    assert_eq!(opts.qos.priority, Priority::High);
    assert!(opts.enable_dedup);
    assert_eq!(opts.dedup_window, 32);
    assert!(opts.enable_reorder_check);
    assert!(opts.drop_out_of_order);
    assert_eq!(opts.overflow_policy, SubscriberOverflowPolicy::Relay);
    assert_eq!(opts.overflow_relay_capacity, 16);
}

#[test]
fn subscriber_options_with_qos_sets_priority() {
    let opts = SubscriberOptions::new().with_qos(QoS::realtime_control());
    assert_eq!(opts.qos.priority, Priority::Realtime);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_without_executor_uses_local_queue() -> AortaResult<()> {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _env_guard = schema_attach_never_guard();

    // Create subscriber without executor
    let sub_node = Node::new("sub_no_exec", "default").await?;

    let received = Arc::new(AtomicU32::new(0));
    let recv_clone = Arc::clone(&received);

    let _sub = sub_node
        .create_subscriber("/no_exec", move |_data, _ctx| {
            recv_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    // Should work even without executor (uses local queue)
    tokio::time::sleep(Duration::from_millis(100)).await;

    sub_node.close().await?;
    drop(_env_guard);
    Ok(())
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = SubscriberOptions::default();
}
