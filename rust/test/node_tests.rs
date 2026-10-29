use crate::{
    AortaError, AortaResult, ExecutionPolicy, Node, NodeOptions, Priority, QoS, SchemaMeta,
    SubscriberOptions, TelemetryNodeMetadata, TypedRetainedSample, TypedSubscribeMessage,
};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const NODE_CLOSE_TIMEOUT: Duration = Duration::from_secs(12);

async fn wait_for_tick_count(ticks: &AtomicU32, timeout: Duration) -> u32 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let observed = ticks.load(Ordering::Relaxed);
        if observed > 0 {
            return observed;
        }

        if tokio::time::Instant::now() >= deadline {
            return observed;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn unique_topic(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("sdk/test/n{}/{}_{}", std::process::id(), base, id)
}

struct DummyTypedSubSchema;

#[derive(Clone, Copy)]
struct DummyTypedSubView<'a> {
    marker: u8,
    payload: &'a [u8],
}

impl SchemaMeta for DummyTypedSubSchema {
    fn schema_bfbs() -> &'static [u8] {
        b"dummy_typed_sub_schema"
    }
}

impl TypedSubscribeMessage for DummyTypedSubSchema {
    type Root<'a> = DummyTypedSubView<'a>;

    fn decode_typed_root<'a>(payload: &'a [u8]) -> AortaResult<Self::Root<'a>> {
        let marker = payload
            .first()
            .copied()
            .ok_or_else(|| AortaError::invalid_argument("typed payload is empty"))?;
        if marker != 0xAB {
            return Err(AortaError::invalid_argument(
                "typed payload marker mismatch",
            ));
        }
        Ok(DummyTypedSubView { marker, payload })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_can_create_and_close() -> AortaResult<()> {
    let node = Node::new("sdk_node_basic", "sdk_test").await?;
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_identity_telemetry_defaults_to_rust_sdk_metadata() -> AortaResult<()> {
    let node = Node::new("sdk_node_identity_defaults", "sdk_test").await?;
    let telemetry = node.inner().build_node_identity_telemetry();

    assert_eq!(telemetry.language, "rust");
    assert_eq!(telemetry.sdk_name, "aorta_rust");
    assert_eq!(telemetry.sdk_version, env!("CARGO_PKG_VERSION"));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_with_options_preserves_rust_sdk_defaults_for_missing_fields() -> AortaResult<()> {
    let node = Node::with_options(
        "sdk_node_identity_override",
        "sdk_test",
        NodeOptions {
            telemetry: TelemetryNodeMetadata {
                executable: Some("/tmp/custom-sdk".to_string()),
                sdk_name: Some("custom_rust_sdk".to_string()),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?;
    let telemetry = node.inner().build_node_identity_telemetry();

    assert_eq!(telemetry.executable, "/tmp/custom-sdk");
    assert_eq!(telemetry.language, "rust");
    assert_eq!(telemetry.sdk_name, "custom_rust_sdk");
    assert_eq!(telemetry.sdk_version, env!("CARGO_PKG_VERSION"));

    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_auto_executor_ticks() -> AortaResult<()> {
    let node = Node::new("sdk_node_timer", "sdk_test").await?;
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_clone = Arc::clone(&ticks);

    let _timer = node
        .create_timer(Duration::from_millis(20), move || {
            ticks_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    let observed = wait_for_tick_count(ticks.as_ref(), Duration::from_millis(500)).await;
    assert!(
        observed > 0,
        "expected timer to tick, observed {}",
        observed
    );

    node.close().await?;
    Ok(())
}

// Dropping the last Node clone without close() must
// stop the auto-executor thread (previously everything leaked forever —
// thread, runtime, session).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_ticks_stop_after_last_node_clone_dropped_without_close() -> AortaResult<()> {
    let node = Node::new("sdk_node_drop_stops_exec", "sdk_test").await?;
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_clone = Arc::clone(&ticks);

    let timer = node
        .create_timer(Duration::from_millis(20), move || {
            ticks_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    let observed = wait_for_tick_count(ticks.as_ref(), Duration::from_millis(500)).await;
    assert!(observed > 0, "timer must tick before the drop");

    // Drop WITHOUT close(): Node::drop stops and joins the auto-executor
    // thread synchronously, so no callback can dispatch afterwards.
    drop(node);
    let after_drop = ticks.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        ticks.load(Ordering::Relaxed),
        after_drop,
        "executor must stop dispatching once the last Node clone is dropped"
    );

    drop(timer);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn timer_auto_executor_does_not_starve_runtime() -> AortaResult<()> {
    let node = Node::new("sdk_node_timer_starve", "sdk_test").await?;
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_clone = Arc::clone(&ticks);

    let timer = node
        .create_timer(Duration::from_millis(1), move || {
            ticks_clone.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(5));
        })
        .await?;

    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::time::sleep(Duration::from_millis(60)),
    )
    .await
    .expect("internal executor should not starve caller runtime");

    let observed = wait_for_tick_count(ticks.as_ref(), Duration::from_millis(500)).await;
    assert!(observed > 0, "expected hot timer callback to execute");

    drop(timer);

    tokio::time::timeout(NODE_CLOSE_TIMEOUT, node.close())
        .await
        .expect("closing node after stopping hot timer should not hang")?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_publisher_rejects_invalid_topic_name() -> AortaResult<()> {
    let node = Node::new("sdk_node_invalid_topic", "sdk_test").await?;
    // Pre-PR-#90 input was `bad-topic` (hyphen rejected); post-relaxation
    // hyphen is accepted, so use a Zenoh-reserved char that the new
    // validator still rejects.
    let result = node.create_publisher_raw("bad#topic").await;
    match result {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(
                msg.contains("reserved by Zenoh") || msg.contains("'#'"),
                "unexpected error message: {}",
                msg
            );
        }
        Ok(_) => panic!("expected InvalidArgument, got Ok"),
        Err(other) => panic!("expected InvalidArgument, got {}", other),
    }
    node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_service_rejects_invalid_service_name() -> AortaResult<()> {
    let node = Node::new("sdk_node_invalid_service", "sdk_test").await?;
    let result = node.create_service("bad#service", |_req| {}).await;
    match result {
        Err(AortaError::InvalidArgument(msg)) => {
            assert!(
                msg.contains("reserved by Zenoh") || msg.contains("'#'"),
                "unexpected error message: {}",
                msg
            );
        }
        Ok(_) => panic!("expected InvalidArgument, got Ok"),
        Err(other) => panic!("expected InvalidArgument, got {}", other),
    }
    assert!(
        !node.inner().has_event_sender(),
        "invalid service creation should not auto-attach an executor"
    );
    tokio::time::timeout(NODE_CLOSE_TIMEOUT, node.close())
        .await
        .expect("closing node after invalid service creation should not hang")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_auto_executor_attaches_node() -> AortaResult<()> {
    let node = Node::new("sdk_node_sub_auto_exec", "sdk_test").await?;
    let topic = unique_topic("sub_auto_exec");

    node.prepare_subscriber_creation(&topic)?;

    assert!(
        node.inner().has_event_sender(),
        "subscriber creation path should auto-attach an executor"
    );
    tokio::time::timeout(NODE_CLOSE_TIMEOUT, node.close())
        .await
        .expect("closing node after subscriber prepare should not hang")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_auto_executor_attaches_node() -> AortaResult<()> {
    let node = Node::new("sdk_node_service_auto_exec", "sdk_test").await?;
    let service_name = format!("/{}", unique_topic("service_auto_exec"));
    let mut options = crate::ServiceOptions::new();

    node.prepare_service_creation(&service_name, &mut options)?;

    assert!(
        node.inner().has_event_sender(),
        "service creation path should auto-attach an executor"
    );
    tokio::time::timeout(NODE_CLOSE_TIMEOUT, node.close())
        .await
        .expect("closing node after service prepare should not hang")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_group_binds_group_handle_into_options() -> AortaResult<()> {
    let node = Node::new("sdk_node_execution_group", "sdk_test").await?;
    let group = node.create_execution_group(ExecutionPolicy::Concurrent)?;

    let sub_opts = group.subscriber_options(SubscriberOptions::new());
    assert_eq!(
        sub_opts.callback_group,
        aorta_core::event_queue::CallbackGroupType::Reentrant
    );
    assert_eq!(sub_opts.callback_group_handle, Some(group.handle()));

    let timer_opts = group.timer_options(aorta_core::timer::TimerOptions::new());
    assert_eq!(
        timer_opts.callback_group,
        aorta_core::event_queue::CallbackGroupType::Reentrant
    );
    assert_eq!(timer_opts.callback_group_handle, Some(group.handle()));

    let svc_opts = group.service_options(crate::ServiceOptions::new());
    assert_eq!(
        svc_opts.callback_group,
        aorta_core::event_queue::CallbackGroupType::Reentrant
    );
    assert_eq!(svc_opts.callback_group_handle, Some(group.handle()));

    // Failure-report bound, not a promptness SLA: close() legitimately
    // includes zenoh session teardown with its own bounded drains, which
    // a contended CI runner can stretch past 1s (observed 2026-07-15) —
    // the guard's value is "completes at all" vs a real hang.
    tokio::time::timeout(Duration::from_secs(30), node.close())
        .await
        .expect("closing node after execution group creation should not hang")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_subscriber_with_qos_applies_priority() -> AortaResult<()> {
    let pub_node = Node::new("sdk_node_sub_qos_pub", "sdk_test").await?;
    let sub_node = Node::new("sdk_node_sub_qos_sub", "sdk_test").await?;
    let topic = unique_topic("sub_qos");

    let seen_priority = Arc::new(AtomicU32::new(0));
    let seen_priority_clone = Arc::clone(&seen_priority);
    let _sub = sub_node
        .create_subscriber_with_options(
            &topic,
            move |_data, ctx| {
                let mapped = match ctx.priority {
                    Priority::Realtime => 5,
                    Priority::High => 4,
                    Priority::Normal => 3,
                    Priority::Low => 2,
                    Priority::Background => 1,
                };
                seen_priority_clone.store(mapped, Ordering::Relaxed);
            },
            SubscriberOptions::new()
                .with_qos(QoS::default().with_priority(Priority::High))
                .with_priority_in_context(true),
        )
        .await?;

    let publisher = pub_node.create_publisher_raw(&topic).await?;
    let wait_result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            publisher.publish(vec![1, 2, 3])?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            if seen_priority.load(Ordering::Relaxed) == 4 {
                break;
            }
        }
        Ok::<(), AortaError>(())
    })
    .await;

    assert!(
        wait_result.is_ok(),
        "subscriber did not observe expected priority before timeout"
    );
    wait_result.expect("wait result should contain callback result")?;

    assert_eq!(
        seen_priority.load(Ordering::Relaxed),
        4,
        "subscriber callback should observe priority from subscriber qos"
    );

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_subscriber_typed_view_delivers_decoded_message() -> AortaResult<()> {
    let pub_node = Node::new("sdk_node_typed_view_pub", "sdk_test").await?;
    let sub_node = Node::new("sdk_node_typed_view_sub", "sdk_test").await?;
    let topic = unique_topic("sub_typed_view");

    let seen_marker = Arc::new(AtomicU32::new(0));
    let seen_len = Arc::new(AtomicU32::new(0));
    let marker_clone = Arc::clone(&seen_marker);
    let len_clone = Arc::clone(&seen_len);

    let _sub = sub_node
        .create_subscriber_typed_view::<DummyTypedSubSchema, _>(&topic, move |msg, _ctx| {
            marker_clone.store(msg.marker as u32, Ordering::Relaxed);
            len_clone.store(msg.payload.len() as u32, Ordering::Relaxed);
        })
        .await?;

    let publisher = pub_node.create_publisher_raw(&topic).await?;
    let wait_result = tokio::time::timeout(Duration::from_secs(3), async {
        while seen_marker.load(Ordering::Relaxed) == 0 {
            publisher.publish(vec![0xAB, 1, 2, 3])?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<(), AortaError>(())
    })
    .await;

    assert!(
        wait_result.is_ok(),
        "typed view subscriber did not receive message before timeout"
    );
    wait_result.expect("typed view wait result")?;
    assert_eq!(seen_marker.load(Ordering::Relaxed), 0xABu32);
    assert_eq!(seen_len.load(Ordering::Relaxed), 4u32);

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_subscriber_typed_retained_keeps_payload_after_callback() -> AortaResult<()> {
    let pub_node = Node::new("sdk_node_typed_retained_pub", "sdk_test").await?;
    let sub_node = Node::new("sdk_node_typed_retained_sub", "sdk_test").await?;
    let topic = unique_topic("sub_typed_retained");

    let stored: Arc<Mutex<Option<TypedRetainedSample<DummyTypedSubSchema>>>> =
        Arc::new(Mutex::new(None));
    let stored_clone = Arc::clone(&stored);

    let _sub = sub_node
        .create_subscriber_typed_retained_with_options::<DummyTypedSubSchema, _>(
            &topic,
            move |sample| {
                let mut guard = stored_clone.lock().expect("typed retained sample lock");
                *guard = Some(sample);
            },
            SubscriberOptions::new().with_topic_in_context(true),
        )
        .await?;

    let publisher = pub_node.create_publisher_raw(&topic).await?;
    let wait_result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            {
                let guard = stored.lock().expect("typed retained sample lock");
                if guard.is_some() {
                    break;
                }
            }
            publisher.publish(vec![0xAB, 9, 8, 7])?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<(), AortaError>(())
    })
    .await;

    assert!(
        wait_result.is_ok(),
        "typed retained subscriber did not receive message before timeout"
    );
    wait_result.expect("typed retained wait result")?;

    let sample = {
        let mut guard = stored.lock().expect("typed retained sample lock");
        guard.take().expect("typed retained sample must exist")
    };

    assert_eq!(sample.payload(), &[0xAB, 9, 8, 7]);
    let decoded = sample.decode()?;
    assert_eq!(decoded.marker, 0xAB);
    assert_eq!(decoded.payload, &[0xAB, 9, 8, 7]);
    let ctx_topic = sample.context().topic.as_ref();
    assert!(
        ctx_topic.ends_with(topic.as_str()),
        "context topic '{}' should end with raw topic '{}'",
        ctx_topic,
        topic
    );

    pub_node.close().await?;
    sub_node.close().await?;
    Ok(())
}
