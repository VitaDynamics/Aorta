//! End-to-end integration tests for cloud cross-namespace addressing (Cloud
//! Cross-Namespace Addressing Proposal §5/§11). A "robot" node runs with a
//! Zenoh session namespace (= its SN), so its keys hit the wire as
//! `{SN}/aorta/{group}/...`. A namespace-free "cloud" node addresses it via
//! the application-level namespace prefix (`SubscriberOptions::with_namespace` /
//! a namespaced service `Client`), proving
//! a single cloud session can reach any robot and recover the source namespace.

#[cfg(test)]
mod tests {
    use crate::client::Client;
    use crate::error::{AortaError, AortaResult};
    use crate::executor::SingleThreadedExecutor;
    use crate::key_expr::KeyExprBuilder;
    use crate::node::Node;
    use crate::service::Service;
    use crate::session::Session;
    use crate::subscriber::SubscriberOptions;
    use crate::test_support::ENV_LOCK;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use zenoh::config::{EndPoint, WhatAmI};

    const ROUTER_CONFIG: &str = include_str!("zenoh_router_config.json5");

    fn unique_tag(base: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}_p{}_{}", base, std::process::id(), id)
    }

    async fn open_router_session() -> AortaResult<(Session, EndPoint)> {
        let mut config = zenoh::Config::from_json5(ROUTER_CONFIG)
            .map_err(|err| AortaError::internal(format!("router config error: {:?}", err)))?;
        config
            .set_mode(Some(WhatAmI::Router))
            .map_err(|err| AortaError::internal(format!("router mode error: {:?}", err)))?;
        let session = Session::open(config).await?;

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

    /// A client-mode peer config connected to `endpoint`. When `namespace` is
    /// set, the Zenoh session prefixes every key with it on the wire — this is
    /// exactly how a robot is deployed (namespace = SN).
    fn peer_config(endpoint: &EndPoint, namespace: Option<&str>) -> AortaResult<zenoh::Config> {
        let ns_line = namespace
            .map(|ns| format!("  namespace: \"{ns}\",\n"))
            .unwrap_or_default();
        let json5 = format!(
            r#"{{
  mode: "client",
{ns_line}  scouting: {{ multicast: {{ enabled: false }} }},
  transport: {{ shared_memory: {{ enabled: false }} }}
}}"#
        );
        let mut config = zenoh::Config::from_json5(&json5)
            .map_err(|err| AortaError::internal(format!("peer config error: {:?}", err)))?;
        config
            .connect
            .endpoints
            .set(vec![endpoint.clone().into()])
            .map_err(|err| AortaError::internal(format!("peer connect error: {:?}", err)))?;
        Ok(config)
    }

    type Received = Arc<Mutex<Vec<(Option<String>, Vec<u8>)>>>;

    // ----------------------------------------------------------------------
    // Topic: a namespace-free cloud subscriber addresses a single robot by SN.
    // ----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_namespace_topic_subscribe_concrete() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // Group is read from AORTA_GROUP; remove it so both nodes agree on
        // "default" deterministically regardless of ambient env.
        std::env::remove_var("AORTA_GROUP");

        let tag = unique_tag("xns_topic");
        let topic = format!("demo/state_{tag}");
        let (router_session, endpoint) = open_router_session().await?;

        let robot = Node::with_zenoh_config(
            format!("robot_{tag}"),
            "default",
            peer_config(&endpoint, Some("R1"))?,
        )
        .await?;
        let cloud = Node::with_zenoh_config(
            format!("cloud_{tag}"),
            "default",
            peer_config(&endpoint, None)?,
        )
        .await?;

        let mut exec = SingleThreadedExecutor::new(256);
        exec.add_node(cloud.clone());

        let received: Received = Arc::new(Mutex::new(Vec::new()));
        let received_cb = Arc::clone(&received);
        let _sub = cloud
            .create_subscriber_with_options(
                &topic,
                move |data, ctx| {
                    received_cb
                        .lock()
                        .unwrap()
                        .push((ctx.source_namespace.clone(), data.to_bytes().into_owned()));
                },
                SubscriberOptions::new().with_namespace("R1"),
            )
            .await?;

        let publisher = robot.create_publisher(&topic).await?;

        let stop = exec.stop_handle();
        let spin = tokio::spawn(async move { exec.spin().await });

        // Publish until the first delivery lands (discovery is async).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        while tokio::time::Instant::now() < deadline {
            publisher.publish(vec![1, 2, 3])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
            if !received.lock().unwrap().is_empty() {
                break;
            }
        }
        stop.stop();
        let _ = spin.await;
        drop(router_session);

        let got = received.lock().unwrap();
        assert!(!got.is_empty(), "cloud subscriber received no message");
        assert_eq!(got[0].0.as_deref(), Some("R1"), "source_namespace mismatch");
        assert_eq!(got[0].1, vec![1, 2, 3], "payload mismatch");
        Ok(())
    }

    // ----------------------------------------------------------------------
    // Fan-out: one `*` subscription covers the fleet; source_namespace
    // distinguishes which robot each sample came from.
    // ----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_namespace_fanout_distinguishes_source() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::remove_var("AORTA_GROUP");

        let tag = unique_tag("xns_fanout");
        let topic = format!("demo/state_{tag}");
        let (router_session, endpoint) = open_router_session().await?;

        let robot1 = Node::with_zenoh_config(
            format!("robot1_{tag}"),
            "default",
            peer_config(&endpoint, Some("R1"))?,
        )
        .await?;
        let robot2 = Node::with_zenoh_config(
            format!("robot2_{tag}"),
            "default",
            peer_config(&endpoint, Some("R2"))?,
        )
        .await?;
        let cloud = Node::with_zenoh_config(
            format!("cloud_{tag}"),
            "default",
            peer_config(&endpoint, None)?,
        )
        .await?;

        let mut exec = SingleThreadedExecutor::new(256);
        exec.add_node(cloud.clone());

        let received: Received = Arc::new(Mutex::new(Vec::new()));
        let received_cb = Arc::clone(&received);
        let _sub = cloud
            .create_subscriber_with_options(
                &topic,
                move |data, ctx| {
                    received_cb
                        .lock()
                        .unwrap()
                        .push((ctx.source_namespace.clone(), data.to_bytes().into_owned()));
                },
                // `*` matches the namespace segment of every robot on the fleet.
                SubscriberOptions::new().with_namespace("*"),
            )
            .await?;

        let pub1 = robot1.create_publisher(&topic).await?;
        let pub2 = robot2.create_publisher(&topic).await?;

        let stop = exec.stop_handle();
        let spin = tokio::spawn(async move { exec.spin().await });

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            pub1.publish(vec![0xA1])?;
            pub2.publish(vec![0xA2])?;
            tokio::time::sleep(Duration::from_millis(80)).await;
            let sources: HashSet<Option<String>> = received
                .lock()
                .unwrap()
                .iter()
                .map(|(ns, _)| ns.clone())
                .collect();
            if sources.contains(&Some("R1".to_string()))
                && sources.contains(&Some("R2".to_string()))
            {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        stop.stop();
        let _ = spin.await;
        drop(router_session);

        let sources: HashSet<Option<String>> = received
            .lock()
            .unwrap()
            .iter()
            .map(|(ns, _)| ns.clone())
            .collect();
        assert!(
            sources.contains(&Some("R1".to_string())),
            "missing samples from R1; saw {sources:?}"
        );
        assert!(
            sources.contains(&Some("R2".to_string())),
            "missing samples from R2; saw {sources:?}"
        );
        Ok(())
    }

    // ----------------------------------------------------------------------
    // Service: a namespace-free cloud client calls a service on one robot via a
    // namespaced key_builder (the injection point `Client::builder.key_builder`).
    // ----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_namespace_service_call() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::remove_var("AORTA_GROUP");

        let tag = unique_tag("xns_srv");
        let service_name = format!("echo_{tag}");
        let (router_session, endpoint) = open_router_session().await?;

        let robot = Node::with_zenoh_config(
            format!("robot_{tag}"),
            "default",
            peer_config(&endpoint, Some("R1"))?,
        )
        .await?;
        let cloud = Node::with_zenoh_config(
            format!("cloud_{tag}"),
            "default",
            peer_config(&endpoint, None)?,
        )
        .await?;

        // Robot provides the service on its (namespaced) session — relative key
        // aorta/default/srv/echo, on the wire R1/aorta/default/srv/echo.
        let service = Service::builder(robot.session().clone(), &service_name)
            .handler(|req| {
                let payload = req.payload().to_bytes().into_owned();
                let _ = tokio::spawn(async move {
                    let _ = req.reply(&payload).await;
                });
            })
            .build();
        let _running = service.start().await?;

        // Cloud client: namespace-free session, but a key_builder pinned to R1.
        let namespaced_kb = KeyExprBuilder::new("default")?.with_namespace("R1")?;
        let client = Client::builder(cloud.session().clone(), &service_name)
            .key_builder(namespaced_kb)
            .timeout_ms(10_000)
            .build();

        // Wait for the queryable to be discovered, then assert the echo.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        let mut response = None;
        while tokio::time::Instant::now() < deadline {
            if let Ok(reply) = client.call_with_timeout(&[7, 8, 9], 400).await {
                response = Some(reply.to_bytes().into_owned());
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        drop(router_session);

        assert_eq!(
            response.as_deref(),
            Some([7u8, 8, 9].as_slice()),
            "cross-namespace service echo failed"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_namespaces_service_roundtrips_and_reports_caller_namespace() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::remove_var("AORTA_GROUP");

        let tag = unique_tag("xns_srv_all");
        let service_name = format!("echo_{tag}");
        let (router_session, endpoint) = open_router_session().await?;
        let server_session = Session::open(peer_config(&endpoint, None)?).await?;
        let client_session = Session::open(peer_config(&endpoint, None)?).await?;

        let service = Service::builder(server_session.clone(), &service_name)
            .serve_all_namespaces(true)
            .handler(|req| {
                let caller_namespace = req
                    .caller_namespace()
                    .unwrap_or("<none>")
                    .as_bytes()
                    .to_vec();
                drop(tokio::spawn(async move {
                    let _ = req.reply(caller_namespace).await;
                }));
            })
            .build();
        let _running = service.start().await?;

        async fn call_until(client: &Client, expected: &[u8]) -> AortaResult<Vec<u8>> {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
            loop {
                if let Ok(reply) = client.call_with_timeout(&[1], 400).await {
                    let bytes = reply.to_bytes().into_owned();
                    if bytes == expected {
                        return Ok(bytes);
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(AortaError::timeout(format!(
                        "all-namespaces service did not return {:?}",
                        expected
                    )));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let r1_client = Client::builder(client_session.clone(), &service_name)
            .key_builder(KeyExprBuilder::new("default")?.with_namespace("R1")?)
            .build();
        assert_eq!(call_until(&r1_client, b"R1").await?, b"R1");

        let multi_client = Client::builder(client_session.clone(), &service_name)
            .key_builder(KeyExprBuilder::new("default")?.with_namespace("region/site/R1")?)
            .build();
        assert_eq!(
            call_until(&multi_client, b"region/site/R1").await?,
            b"region/site/R1"
        );

        let namespace_free_client = Client::builder(client_session, &service_name).build();
        assert_eq!(
            call_until(&namespace_free_client, b"<none>").await?,
            b"<none>"
        );

        drop(router_session);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_namespaces_service_rejects_namespaced_zenoh_session() -> AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::remove_var("AORTA_GROUP");

        let tag = unique_tag("xns_srv_guard");
        let service_name = format!("guard_{tag}");
        let (router_session, endpoint) = open_router_session().await?;
        let namespaced_session = Session::open(peer_config(&endpoint, Some("R1"))?).await?;

        let result = Service::builder(namespaced_session, &service_name)
            .serve_all_namespaces(true)
            .build()
            .start()
            .await;
        let err = match result {
            Ok(_) => panic!("all-namespaces service unexpectedly started"),
            Err(err) => err,
        };
        assert!(
            matches!(err, AortaError::InvalidArgument(_)),
            "expected invalid_argument, got {err:?}"
        );
        assert!(
            err.to_string().contains("Zenoh session namespace"),
            "guard error should explain the session namespace conflict: {err}"
        );

        drop(router_session);
        Ok(())
    }
}
