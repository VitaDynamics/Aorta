// Integration tests for QoS, Publisher, Subscriber, Timer interactions
// These tests verify that all components work together correctly

#[cfg(test)]
mod qos_integration_tests {
    use crate::error::{AortaError, AortaResult};
    use crate::executor::SingleThreadedExecutor;
    use crate::node::Node;
    use crate::publisher::PublisherOptions;
    use crate::qos::{Priority, QoS};
    use crate::session::Session;
    use crate::subscriber::SubscriberOptions;
    use crate::test_support::{schema_attach_never_guard, ENV_LOCK};
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use zenoh::config::{EndPoint, WhatAmI};

    const ROUTER_CONFIG: &str = include_str!("zenoh_router_config.json5");

    async fn open_router_session() -> AortaResult<(Session, EndPoint)> {
        let mut config = zenoh::Config::from_json5(ROUTER_CONFIG)
            .map_err(|err| AortaError::internal(format!("router config error: {:?}", err)))?;
        config
            .set_mode(Some(WhatAmI::Router))
            .map_err(|err| AortaError::internal(format!("router mode error: {:?}", err)))?;
        let session = Session::open(config).await?;

        // Router locator publication is asynchronous. Poll until a tcp locator appears.
        let started = tokio::time::Instant::now();
        loop {
            let locators = session.inner().info().locators().await;
            if let Some(locator) = locators
                .iter()
                .find(|loc| loc.to_string().starts_with("tcp/"))
            {
                return Ok((session, EndPoint::from(locator.clone())));
            }
            if started.elapsed() >= Duration::from_secs(5) {
                return Err(AortaError::timeout(
                    "router locator not found within 5s".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn peer_config(endpoint: &EndPoint) -> AortaResult<zenoh::Config> {
        let mut config = zenoh::Config::from_json5(
            r#"
{
  mode: "client",
  scouting: {
    multicast: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
        )
        .map_err(|err| AortaError::internal(format!("peer config error: {:?}", err)))?;
        config
            .connect
            .endpoints
            .set(vec![endpoint.clone().into()])
            .map_err(|err| AortaError::internal(format!("peer connect error: {:?}", err)))?;
        Ok(config)
    }

    fn unique_tag(base: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}_p{}_{}", base, std::process::id(), id)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_qos_realtime_pub_sub() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("qos_realtime");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = Arc::clone(&received);

        // Create subscriber first (priority comes from subscriber QoS/options).
        let sub_opts = SubscriberOptions::new()
            .with_qos(QoS::realtime_control())
            .with_priority_in_context(true);
        let _sub = sub_node
            .create_subscriber_with_options(
                &topic,
                move |data, ctx| {
                    received_clone
                        .lock()
                        .unwrap()
                        .push((data.to_bytes().into_owned(), ctx.priority));
                },
                sub_opts,
            )
            .await?;

        // Create publisher with realtime QoS
        let qos = QoS::realtime_control();
        let options = PublisherOptions {
            qos,
            ..Default::default()
        };
        let publisher = pub_node
            .create_publisher_with_options(&topic, options)
            .await?;

        // Wait for discovery
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Process events
        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        // Warm up discovery: require at least one delivery before the assertion phase.
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < ready_deadline {
            publisher.publish(vec![0xAA])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
            if !received.lock().unwrap().is_empty() {
                break;
            }
        }
        assert!(
            !received.lock().unwrap().is_empty(),
            "subscriber did not become ready before assertion phase"
        );
        received.lock().unwrap().clear();

        publisher.publish(vec![1, 2, 3])?;
        publisher.publish(vec![4, 5, 6])?;
        tokio::time::sleep(Duration::from_millis(800)).await;
        stop.stop();
        let _ = spin_task.await;

        // Verify messages received with realtime priority
        let msgs = received.lock().unwrap();
        assert!(
            msgs.len() >= 2,
            "Expected at least 2 messages, got {}",
            msgs.len()
        );
        for (_, priority) in msgs.iter() {
            assert_eq!(*priority, Priority::Realtime);
        }

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_qos_priority_levels() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let pid = std::process::id();
        let group = format!("test_priority_{}", pid);
        let topic_realtime = format!("/realtime_{}", pid);
        let topic_sensor = format!("/sensor_{}", pid);
        let topic_state = format!("/state_{}", pid);
        let topic_bulk = format!("/bulk_{}", pid);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::with_zenoh_config(
            &format!("priority_pub_{}", pid),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("priority_sub_{}", pid),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received_priorities = Arc::new(Mutex::new(Vec::new()));

        // Create all subscribers first
        let mut subscribers = Vec::new();

        let recv1 = Arc::clone(&received_priorities);
        let topic_realtime_cb = topic_realtime.clone();
        let sub1 = sub_node
            .create_subscriber_with_options(
                &topic_realtime,
                move |_data, ctx| {
                    recv1
                        .lock()
                        .unwrap()
                        .push((topic_realtime_cb.clone(), ctx.priority));
                },
                SubscriberOptions::new()
                    .with_qos(QoS::realtime_control())
                    .with_priority_in_context(true),
            )
            .await?;
        subscribers.push(sub1);

        let recv2 = Arc::clone(&received_priorities);
        let topic_sensor_cb = topic_sensor.clone();
        let sub2 = sub_node
            .create_subscriber_with_options(
                &topic_sensor,
                move |_data, ctx| {
                    recv2
                        .lock()
                        .unwrap()
                        .push((topic_sensor_cb.clone(), ctx.priority));
                },
                SubscriberOptions::new()
                    .with_qos(QoS::sensor_data())
                    .with_priority_in_context(true),
            )
            .await?;
        subscribers.push(sub2);

        let recv3 = Arc::clone(&received_priorities);
        let topic_state_cb = topic_state.clone();
        let sub3 = sub_node
            .create_subscriber_with_options(
                &topic_state,
                move |_data, ctx| {
                    recv3
                        .lock()
                        .unwrap()
                        .push((topic_state_cb.clone(), ctx.priority));
                },
                SubscriberOptions::new()
                    .with_qos(QoS::state_update())
                    .with_priority_in_context(true),
            )
            .await?;
        subscribers.push(sub3);

        let recv4 = Arc::clone(&received_priorities);
        let topic_bulk_cb = topic_bulk.clone();
        let sub4 = sub_node
            .create_subscriber_with_options(
                &topic_bulk,
                move |_data, ctx| {
                    recv4
                        .lock()
                        .unwrap()
                        .push((topic_bulk_cb.clone(), ctx.priority));
                },
                SubscriberOptions::new()
                    .with_qos(QoS::bulk_transfer())
                    .with_priority_in_context(true),
            )
            .await?;
        subscribers.push(sub4);

        // Wait for subscribers to be ready
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Test different QoS presets
        let presets = vec![
            (
                topic_realtime.clone(),
                QoS::realtime_control(),
                Priority::Realtime,
            ),
            (topic_sensor.clone(), QoS::sensor_data(), Priority::Normal),
            (topic_state.clone(), QoS::state_update(), Priority::High),
            (
                topic_bulk.clone(),
                QoS::bulk_transfer(),
                Priority::Background,
            ),
        ];

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;

        for (topic, qos, _expected_priority) in presets {
            let options = PublisherOptions {
                qos,
                ..Default::default()
            };
            let pub_temp = pub_node
                .create_publisher_with_options(&topic, options)
                .await?;
            let baseline = received_priorities.lock().unwrap().len();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while tokio::time::Instant::now() < deadline {
                pub_temp.publish([1, 2, 3])?;
                tokio::time::sleep(Duration::from_millis(80)).await;
                if received_priorities.lock().unwrap().len() > baseline {
                    break;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.stop();
        let _ = spin_task.await;

        // Verify priorities received (may not receive all 4 due to timing)
        let priorities = received_priorities.lock().unwrap();
        assert!(
            !priorities.is_empty(),
            "Expected at least 1 message, got {}",
            priorities.len()
        );

        // Verify each received message has correct priority
        for (topic, priority) in priorities.iter() {
            if topic == &topic_realtime {
                assert_eq!(*priority, Priority::Realtime);
            } else if topic == &topic_sensor {
                assert_eq!(*priority, Priority::Normal);
            } else if topic == &topic_state {
                assert_eq!(*priority, Priority::High);
            } else if topic == &topic_bulk {
                assert_eq!(*priority, Priority::Background);
            } else {
                panic!("Unexpected topic: {}", topic);
            }
        }

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_subscriber_options_history_depth() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("history_depth");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(4); // Small queue
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received_count = Arc::new(AtomicU32::new(0));
        let recv_clone = Arc::clone(&received_count);

        // Create subscriber with small history depth
        let options = SubscriberOptions::new().with_history_depth(4);
        let _sub = sub_node
            .create_subscriber_with_options(
                &topic,
                move |_data, _ctx| {
                    recv_clone.fetch_add(1, Ordering::Relaxed);
                },
                options,
            )
            .await?;

        let publisher = pub_node.create_publisher(&topic).await?;

        tokio::time::sleep(Duration::from_millis(400)).await;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Warm up discovery and require at least one observed delivery first.
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < ready_deadline
            && received_count.load(Ordering::Relaxed) == 0
        {
            publisher.publish(vec![0xAA])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        assert!(
            received_count.load(Ordering::Relaxed) > 0,
            "subscriber did not become ready before history-depth assertion phase"
        );
        received_count.store(0, Ordering::Relaxed);

        // Publish multiple messages rapidly to exercise limited history depth.
        for i in 0..10 {
            publisher.publish(vec![i])?;
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        tokio::time::sleep(Duration::from_millis(900)).await;
        stop.stop();
        let _ = spin_task.await;

        // With small queue (4) and rapid publishing (10 msgs), some may be dropped
        let count = received_count.load(Ordering::Relaxed);
        assert!(count > 0, "Should receive at least some messages");
        // Can't assert exact count due to timing, but verify functionality works

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_message_context_metadata() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("metadata");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let metadata = Arc::new(Mutex::new(Vec::new()));
        let meta_clone = Arc::clone(&metadata);

        let _sub = sub_node
            .create_subscriber_with_options(
                &topic,
                move |_data, ctx| {
                    meta_clone.lock().unwrap().push((
                        ctx.topic.clone(),
                        ctx.priority,
                        ctx.receive_stamp_ns,
                        ctx.publisher_stamp_ns,
                    ));
                },
                SubscriberOptions::new()
                    .with_qos(QoS::default().with_priority(Priority::High))
                    .with_topic_in_context(true)
                    .with_priority_in_context(true)
                    .with_receive_timestamp(true),
            )
            .await?;

        let qos = QoS::default().with_priority(Priority::High);
        let options = PublisherOptions {
            qos,
            ..Default::default()
        };
        let publisher = pub_node
            .create_publisher_with_options(&topic, options)
            .await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        publisher.publish(vec![1, 2, 3])?;

        // Process events
        let stop = exec.stop_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            stop.stop();
        });

        exec.spin().await;

        // Verify metadata
        let meta = metadata.lock().unwrap();
        assert!(!meta.is_empty(), "Expected at least 1 message");

        let (topic, priority, recv_ts, _pub_ts) = &meta[0];
        assert!(topic.contains("topic_"));
        assert_eq!(*priority, Priority::High);
        assert!(*recv_ts > 0, "Receive timestamp should be set");
        // Note: Publisher timestamp may not always be set in local network without HLC enabled

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_subscribers_same_topic() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("multi_subs");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub1_node = Node::with_zenoh_config(
            &format!("sub1_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub2_node = Node::with_zenoh_config(
            &format!("sub2_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;

        exec.add_node(pub_node.clone());
        exec.add_node(sub1_node.clone());
        exec.add_node(sub2_node.clone());

        let count1 = Arc::new(AtomicU32::new(0));
        let count2 = Arc::new(AtomicU32::new(0));
        let c1 = Arc::clone(&count1);
        let c2 = Arc::clone(&count2);

        // Two subscribers on same topic
        let _sub1 = sub1_node
            .create_subscriber(&topic, move |_data, _ctx| {
                c1.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        let _sub2 = sub2_node
            .create_subscriber(&topic, move |_data, _ctx| {
                c2.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        let publisher = pub_node.create_publisher(&topic).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < ready_deadline {
            publisher.publish(vec![0xAA])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
            if count1.load(Ordering::Relaxed) > 0 && count2.load(Ordering::Relaxed) > 0 {
                break;
            }
        }
        assert!(
            count1.load(Ordering::Relaxed) > 0 && count2.load(Ordering::Relaxed) > 0,
            "both subscribers must become ready before assertion phase"
        );
        count1.store(0, Ordering::Relaxed);
        count2.store(0, Ordering::Relaxed);

        publisher.publish(vec![1, 2, 3])?;
        tokio::time::sleep(Duration::from_millis(700)).await;
        stop.stop();
        let _ = spin_task.await;

        assert!(
            count1.load(Ordering::Relaxed) >= 1,
            "Sub1 should receive at least one message"
        );
        assert!(
            count2.load(Ordering::Relaxed) >= 1,
            "Sub2 should receive at least one message"
        );

        pub_node.close().await?;
        sub1_node.close().await?;
        sub2_node.close().await?;
        router_session.close().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_qos_reliability_best_effort() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("best_effort");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received = Arc::new(AtomicU32::new(0));
        let recv_clone = Arc::clone(&received);

        let _sub = sub_node
            .create_subscriber(&topic, move |_data, _ctx| {
                recv_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        // Use best-effort QoS
        let qos = QoS::sensor_data(); // BestEffort reliability
        let options = PublisherOptions {
            qos,
            ..Default::default()
        };
        let publisher = pub_node
            .create_publisher_with_options(&topic, options)
            .await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Process events.
        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        // Warm up discovery: require at least one delivery before assertion phase.
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < ready_deadline {
            publisher.publish(vec![0xAA])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
            if received.load(Ordering::Relaxed) > 0 {
                break;
            }
        }
        assert!(
            received.load(Ordering::Relaxed) > 0,
            "subscriber did not become ready before assertion phase"
        );
        received.store(0, Ordering::Relaxed);

        // Publish messages.
        for i in 0..5 {
            publisher.publish(vec![i])?;
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
        stop.stop();
        let _ = spin_task.await;

        // With BestEffort, we should still receive messages in local network
        let count = received.load(Ordering::Relaxed);
        assert!(count > 0, "Should receive messages even with BestEffort");

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_qos_durability_transient_local() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("durability");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        // state_update preset (no durability replay — that's ContextStore now).
        let qos = QoS::state_update();
        let options = PublisherOptions {
            qos,
            ..Default::default()
        };
        let publisher = pub_node
            .create_publisher_with_options(&topic, options)
            .await?;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        // Publish BEFORE subscriber joins
        tokio::time::sleep(Duration::from_millis(150)).await;
        publisher.publish(vec![1, 2, 3])?;

        // Now create late-joining subscriber
        let received = Arc::new(AtomicU32::new(0));
        let recv_clone = Arc::clone(&received);

        // Note: This tests durability configuration; actual late-joiner delivery
        // requires a Zenoh queryable implementation
        let _sub = sub_node
            .create_subscriber(&topic, move |_data, _ctx| {
                recv_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Publish after subscriber joins, and wait for at least one delivery.
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < ready_deadline && received.load(Ordering::Relaxed) == 0
        {
            publisher.publish(vec![4, 5, 6])?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.stop();
        let _ = spin_task.await;

        // Should receive at least the message published after joining
        let count = received.load(Ordering::Relaxed);
        assert!(
            count >= 1,
            "Should receive messages published after joining"
        );

        // A full durability test requires that queryable implementation

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;

        Ok(())
    }

    /// Tier A (`receive_depth`): the per-subscriber gate caps in-flight events
    /// and sheds the newest under load — never exceeding the cap, never leaking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_tier_a_receive_depth_caps_inflight_and_sheds() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("tier_a");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(2048);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        const CAP: usize = 3;
        let received = Arc::new(AtomicU32::new(0));
        let recv_clone = Arc::clone(&received);
        // A deliberately slow callback so events pile up against the cap.
        let sub_opts = SubscriberOptions::new().with_receive_depth(CAP);
        let sub = sub_node
            .create_subscriber_with_options(
                &topic,
                move |_data, _ctx| {
                    recv_clone.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(120));
                },
                sub_opts,
            )
            .await?;

        let publisher = pub_node.create_publisher(&topic).await?;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        // Warm up discovery.
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < ready_deadline && received.load(Ordering::Relaxed) == 0
        {
            publisher.publish(vec![0])?;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        assert!(
            received.load(Ordering::Relaxed) >= 1,
            "subscriber never became ready"
        );

        // Burst far more than the cap, faster than the callback can drain.
        for i in 0..40u8 {
            publisher.publish(vec![i])?;
        }

        // Invariant: in-flight count must NEVER exceed the cap. Poll while it drains.
        let mut max_inflight = 0usize;
        let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < poll_deadline {
            max_inflight = max_inflight.max(sub.inflight());
            assert!(
                sub.inflight() <= CAP,
                "in-flight {} exceeded receive_depth {}",
                sub.inflight(),
                CAP
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        stop.stop();
        let _ = spin_task.await;

        // Let any final in-flight callbacks complete and verify clean accounting.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(max_inflight >= 1, "gate never admitted anything");
        assert!(
            sub.dropped_for_depth() > 0,
            "the gate should have shed events under a 40-message burst at cap {CAP}"
        );
        assert_eq!(
            sub.inflight(),
            0,
            "no in-flight events may leak after drain (got {})",
            sub.inflight()
        );

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;
        Ok(())
    }

    /// Tier B-pull (`InlinePull` + native `RingChannel`): the ring keeps the
    /// newest N samples and the application drains them via `try_recv`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tier_b_pull_keeps_last_n() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("tier_b_pull");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        // Pull subscribers do not need an executor (they own a native ring).
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;

        const DEPTH: usize = 4;
        let sub = sub_node.create_subscriber_pull(&topic, DEPTH).await?;
        assert!(sub.is_pull());
        // recv on the wrong (non-pull) kind is rejected — sanity-check the guard.
        let publisher = pub_node.create_publisher(&topic).await?;

        // Let the pull subscriber be discovered before publishing (volatile).
        tokio::time::sleep(Duration::from_millis(700)).await;

        // Publish 0..10 WITHOUT draining, so the ring overflows and keeps last N.
        for i in 0..10u8 {
            publisher.publish(vec![i])?;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Drain the ring.
        let mut got: Vec<u8> = Vec::new();
        while let Some(sample) = sub.try_recv()? {
            got.push(sample.payload.to_bytes()[0]);
        }

        assert!(!got.is_empty(), "pull ring delivered nothing");
        assert!(
            got.len() <= DEPTH,
            "ring kept {} > depth {DEPTH}: {got:?}",
            got.len()
        );
        assert!(
            got.contains(&9),
            "ring must retain the newest sample (9); got {got:?}"
        );
        assert!(
            got.windows(2).all(|w| w[0] < w[1]),
            "ring must deliver in order without duplicates; got {got:?}"
        );

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;
        Ok(())
    }

    /// Two capped subscribers on the SAME executor each shed
    /// independently and never exceed their OWN cap — per-subscriber isolation
    /// of the Tier A gate (supported regime: Σ caps ≤ shared queue capacity).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_tier_a_two_subscribers_isolated_caps() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("tier_a_two");
        let group = format!("group_{}", tag);
        let topic_a = format!("/topic_a_{}", tag);
        let topic_b = format!("/topic_b_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(2048);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        const CAP: usize = 3;
        let recv_a = Arc::new(AtomicU32::new(0));
        let recv_b = Arc::new(AtomicU32::new(0));
        let ra = Arc::clone(&recv_a);
        let rb = Arc::clone(&recv_b);
        let sub_a = sub_node
            .create_subscriber_with_options(
                &topic_a,
                move |_d, _c| {
                    ra.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(100));
                },
                SubscriberOptions::new().with_receive_depth(CAP),
            )
            .await?;
        let sub_b = sub_node
            .create_subscriber_with_options(
                &topic_b,
                move |_d, _c| {
                    rb.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(100));
                },
                SubscriberOptions::new().with_receive_depth(CAP),
            )
            .await?;

        let pub_a = pub_node.create_publisher(&topic_a).await?;
        let pub_b = pub_node.create_publisher(&topic_b).await?;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        // Warm up both subscriptions.
        let ready = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < ready
            && (recv_a.load(Ordering::Relaxed) == 0 || recv_b.load(Ordering::Relaxed) == 0)
        {
            pub_a.publish(vec![0])?;
            pub_b.publish(vec![0])?;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        assert!(recv_a.load(Ordering::Relaxed) >= 1 && recv_b.load(Ordering::Relaxed) >= 1);

        // Burst both topics far past their caps.
        for i in 0..40u8 {
            pub_a.publish(vec![i])?;
            pub_b.publish(vec![i])?;
        }

        // Each gate caps its OWN subscriber independently — never exceeded.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            assert!(
                sub_a.inflight() <= CAP,
                "A inflight {} > {CAP}",
                sub_a.inflight()
            );
            assert!(
                sub_b.inflight() <= CAP,
                "B inflight {} > {CAP}",
                sub_b.inflight()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        stop.stop();
        let _ = spin_task.await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Both shed under their own burst, independently; neither leaks.
        assert!(sub_a.dropped_for_depth() > 0, "A never shed");
        assert!(sub_b.dropped_for_depth() > 0, "B never shed");
        assert_eq!(sub_a.inflight(), 0, "A leaked");
        assert_eq!(sub_b.inflight(), 0, "B leaked");

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;
        Ok(())
    }

    /// Undeclaring a capped subscriber WHILE its callbacks are
    /// in flight must complete (bounded) without panic/deadlock — the
    /// `InflightGuard`s ride the queued events and drop cleanly on teardown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_tier_a_undeclare_while_in_flight() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("tier_a_teardown");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(2048);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received = Arc::new(AtomicU32::new(0));
        let rc = Arc::clone(&received);
        let sub = sub_node
            .create_subscriber_with_options(
                &topic,
                move |_d, _c| {
                    rc.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(120));
                },
                SubscriberOptions::new().with_receive_depth(4),
            )
            .await?;
        let publisher = pub_node.create_publisher(&topic).await?;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        let ready = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < ready && received.load(Ordering::Relaxed) == 0 {
            publisher.publish(vec![0])?;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        assert!(received.load(Ordering::Relaxed) >= 1);
        for i in 0..40u8 {
            publisher.publish(vec![i])?;
        }
        // Wait until events are actually in flight, then tear down mid-flight.
        let inflight_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while sub.inflight() == 0 && tokio::time::Instant::now() < inflight_deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            sub.inflight() > 0,
            "expected in-flight events before teardown"
        );

        // Bounded — must not panic or hang.
        let undeclare = tokio::time::timeout(Duration::from_secs(10), sub.undeclare()).await;
        assert!(
            undeclare.is_ok(),
            "undeclare timed out under in-flight load"
        );
        undeclare.unwrap()?;

        stop.stop();
        let _ = spin_task.await;
        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;
        Ok(())
    }

    // T4 (end-to-end): `undeclare()` on a callback subscriber must
    // return quickly. Pre-fix it burned the full 2s drain timeout on every
    // call because the local-queue drain task never exited (the queue
    // self-holds a sender), which made every C++ `Subscriber` destructor
    // block ~2s synchronously. Post-fix the drain task exits via an explicit
    // shutdown signal; assert well under the old pathological latency.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_subscriber_undeclare_returns_quickly_with_local_queue() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("undeclare_fast");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let mut exec = SingleThreadedExecutor::new(256);
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        exec.add_node(pub_node.clone());
        exec.add_node(sub_node.clone());

        let received = Arc::new(Mutex::new(0usize));
        let received_clone = Arc::clone(&received);
        let sub = sub_node
            .create_subscriber_with_options(
                &topic,
                move |_data, _ctx| {
                    *received_clone.lock().unwrap() += 1;
                },
                SubscriberOptions::new(),
            )
            .await?;

        let stop = exec.stop_handle();
        let spin_task = tokio::spawn(async move {
            exec.spin().await;
        });

        // Exercise the local-queue path with a little real traffic.
        let publisher = pub_node
            .create_publisher_with_options(
                &topic,
                PublisherOptions {
                    qos: QoS::default(),
                    ..Default::default()
                },
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(400)).await;
        for _ in 0..4 {
            publisher.publish(vec![0x42u8])?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while *received.lock().unwrap() == 0 && tokio::time::Instant::now() < ready_deadline {
            publisher.publish(vec![0x43u8])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        assert!(
            *received.lock().unwrap() > 0,
            "subscriber did not receive any message before undeclare"
        );

        let started = tokio::time::Instant::now();
        sub.undeclare().await?;
        let elapsed = started.elapsed();
        eprintln!("T4 undeclare() latency: {:?}", elapsed);
        assert!(
            elapsed < Duration::from_millis(200),
            "undeclare() took {:?}; pre-fix it always burned the full 2s \
             drain timeout",
            elapsed
        );

        stop.stop();
        let _ = spin_task.await;
        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;
        Ok(())
    }

    // T3: implicit `Subscriber` drop (no
    // `undeclare()`) must still terminate the local-queue drain task —
    // otherwise the task leaks forever (parked on `recv()`, holding the
    // queue). `Subscriber` has no `Drop` impl, so this relies on the
    // `local_queue_shutdown: Option<oneshot::Sender<()>>` field's natural
    // drop.
    //
    // Detection (identity-attributed): the drain task holds the only strong
    // reference to a sentinel `Arc<()>`; the test holds a `Weak` obtained
    // from the `Subscriber` before the drop. When the drain task ends, the
    // Arc drops and the Weak upgrades to None — that is direct proof THIS
    // task ended. No global task counters (zenoh background cleanup tasks
    // exit at arbitrary times and mask leaks), no timing assumptions beyond
    // a generous bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_subscriber_drop_without_undeclare_terminates_drain_task() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("drop_no_undeclare");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let sub = sub_node.create_subscriber(&topic, |_data, _ctx| {}).await?;
        let liveness = sub
            .drain_task_liveness()
            .expect("callback subscriber owns a local-queue drain task");
        assert!(
            liveness.upgrade().is_some(),
            "drain task must be alive while the subscriber is alive"
        );

        // Let the runtime settle (discovery, schema queryable …), then drop
        // the Subscriber without undeclaring.
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(sub); // implicit drop, no undeclare()

        // The drain task must end: its sentinel Arc drops, so the Weak
        // upgrades to None. Bounded wait for CI scheduling slack.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while liveness.upgrade().is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "local-queue drain task leaked after implicit Subscriber drop"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        sub_node.close().await?;
        router_session.close().await?;
        Ok(())
    }

    // `Subscriber::undeclare()` must actually reach its
    // drain-timeout branch and force-stop dispatch — a callback blocked
    // synchronously past the drain timeout, with a second event backlogged
    // behind it. After undeclare() returns (stop flag set + abort),
    // releasing the blocked callback must NOT start the backlogged event.
    // This walks the REAL timeout path: deleting the `stop.store(true)` in
    // undeclare()'s timeout branch turns this red (abort alone cannot
    // preempt the synchronous dispatch loop, so E2 would run).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_subscriber_undeclare_timeout_stops_backlogged_callback() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = schema_attach_never_guard();
        let tag = unique_tag("undeclare_timeout");
        let group = format!("group_{}", tag);
        let topic = format!("/topic_{}", tag);
        let (router_session, router_endpoint) = open_router_session().await?;
        let sub_node = Node::with_zenoh_config(
            &format!("sub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;
        let pub_node = Node::with_zenoh_config(
            &format!("pub_{}", tag),
            &group,
            peer_config(&router_endpoint)?,
        )
        .await?;

        let latch = Arc::new(AtomicBool::new(false));
        let callbacks = Arc::new(AtomicU64::new(0));

        // E1: blocks synchronously on the latch (uninterruptible by abort).
        let latch_sub = Arc::clone(&latch);
        let cb_sub = Arc::clone(&callbacks);
        let sub = sub_node
            .create_subscriber(&topic, move |_data, _ctx| {
                cb_sub.fetch_add(1, Ordering::SeqCst);
                while !latch_sub.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
            })
            .await?;
        let publisher = pub_node.create_publisher(&topic).await?;

        // Two messages: the first blocks (E1), the second is backlogged.
        tokio::time::sleep(Duration::from_millis(400)).await;
        publisher.publish(vec![0xE1u8])?;
        publisher.publish(vec![0xE2u8])?;

        let started = tokio::time::Instant::now();
        while callbacks.load(Ordering::SeqCst) == 0 {
            assert!(
                tokio::time::Instant::now() - started < Duration::from_secs(5),
                "E1 did not start"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // undeclare() must burn the full drain timeout (E1 is stuck), then
        // set the stop flag and abort.
        let t0 = tokio::time::Instant::now();
        sub.undeclare().await?;
        let elapsed = t0.elapsed();
        assert!(
            elapsed >= Duration::from_secs(2),
            "undeclare must have hit the drain timeout, took {elapsed:?}"
        );

        // Release E1: the stop flag's admission check must refuse E2.
        latch.store(true, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            callbacks.load(Ordering::SeqCst),
            1,
            "backlogged callback must not start after undeclare()'s timeout \
             stop"
        );

        pub_node.close().await?;
        sub_node.close().await?;
        router_session.close().await?;
        Ok(())
    }
}
