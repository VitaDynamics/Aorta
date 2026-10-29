//! Cross-session integration tests for Service/Client.
//! Tests Service and Client on DIFFERENT Sessions to verify Zenoh discovery works correctly.

use crate::client::Client;
use crate::error::{AortaError, AortaResult};
use crate::service::Service;
use crate::session::Session;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use zenoh::config::{EndPoint, WhatAmI};

const ROUTER_CONFIG: &str = include_str!("zenoh_router_config.json5");

fn unique_name(base: &str) -> String {
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

async fn open_router_context() -> AortaResult<(Session, EndPoint, String)> {
    let (router_session, router_endpoint) = open_router_session().await?;
    let router_locator = router_endpoint.to_string();
    Ok((router_session, router_endpoint, router_locator))
}

async fn open_peer_session(endpoint: &EndPoint) -> AortaResult<Session> {
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
    Session::open(config).await
}

async fn wait_for_service_ready(
    client: &Client,
    service_name: &str,
    router_locator: &str,
    timeout: Duration,
) -> AortaResult<()> {
    let started = tokio::time::Instant::now();
    let mut last_err_detail = String::from("unknown");
    while started.elapsed() < timeout {
        // Use a short per-attempt timeout so the overall readiness timeout is meaningful.
        match client.call_with_timeout(&[0xEE], 250).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_err_detail = format!("{:?}", err);
                tokio::time::sleep(Duration::from_millis(120)).await;
            }
        }
    }
    Err(AortaError::timeout(format!(
        "service readiness probe timed out after {:?}: service='{}' router={} last_err={}",
        timeout, service_name, router_locator, last_err_detail
    )))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_basic_echo() -> AortaResult<()> {
    let (router_session, router_endpoint, router_locator) = open_router_context().await?;
    let service_session = open_peer_session(&router_endpoint).await?;
    let client_session = open_peer_session(&router_endpoint).await?;
    let service_name = unique_name("cross_session_echo");

    let service = Service::builder(service_session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    let client = Client::builder(client_session, &service_name)
        .timeout_ms(10_000)
        .build();
    wait_for_service_ready(
        &client,
        &service_name,
        &router_locator,
        Duration::from_secs(6),
    )
    .await?;
    let request = vec![10, 20, 30, 40, 50];
    let response = match client.call(&request).await {
        Ok(response) => response,
        Err(err) => {
            return Err(AortaError::internal(format!(
                "cross_session_basic_echo failed (router={}): {:?}",
                router_locator, err
            )));
        }
    };

    drop(router_session);
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_multiple_requests() -> AortaResult<()> {
    let (router_session, router_endpoint, router_locator) = open_router_context().await?;
    let service_session = open_peer_session(&router_endpoint).await?;
    let client_session = open_peer_session(&router_endpoint).await?;
    let service_name = unique_name("cross_session_multi");

    let request_count = Arc::new(AtomicU32::new(0));
    let count_clone = request_count.clone();

    let service = Service::builder(service_session.clone(), &service_name)
        .handler(move |req| {
            let payload = req.payload().to_bytes().into_owned();
            // Ignore readiness probes (single-byte payload) for this test's accounting.
            if payload.len() == 10 {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    let client = Client::builder(client_session, &service_name)
        .timeout_ms(10_000)
        .build();
    wait_for_service_ready(
        &client,
        &service_name,
        &router_locator,
        Duration::from_secs(6),
    )
    .await?;
    let baseline = request_count.load(Ordering::SeqCst);
    for i in 0u8..5 {
        let request = vec![i; 10];
        let response = match client.call(&request).await {
            Ok(response) => response,
            Err(err) => {
                return Err(AortaError::internal(format!(
                    "cross_session_multiple_requests failed (router={}): {:?}",
                    router_locator, err
                )));
            }
        };
        assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    }

    drop(router_session);
    assert_eq!(request_count.load(Ordering::SeqCst) - baseline, 5);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_large_message() -> AortaResult<()> {
    let (router_session, router_endpoint, router_locator) = open_router_context().await?;
    let service_session = open_peer_session(&router_endpoint).await?;
    let client_session = open_peer_session(&router_endpoint).await?;
    let service_name = unique_name("cross_session_large");

    let service = Service::builder(service_session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    let client = Client::builder(client_session, &service_name)
        .timeout_ms(10000)
        .build();
    wait_for_service_ready(
        &client,
        &service_name,
        &router_locator,
        Duration::from_secs(6),
    )
    .await?;

    let request: Vec<u8> = (0..32768).map(|i| (i % 256) as u8).collect();
    let response = match client.call(&request).await {
        Ok(response) => response,
        Err(err) => {
            return Err(AortaError::internal(format!(
                "cross_session_large_message failed (router={}): {:?}",
                router_locator, err
            )));
        }
    };

    assert_eq!(response.len(), 32768);
    drop(router_session);
    assert_eq!(response.to_bytes().as_ref(), request.as_slice());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_session_multiple_clients() -> AortaResult<()> {
    let (router_session, router_endpoint, router_locator) = open_router_context().await?;
    let service_session = open_peer_session(&router_endpoint).await?;
    let service_name = unique_name("cross_session_multi_client");

    let request_count = Arc::new(AtomicU32::new(0));
    let count_clone = request_count.clone();

    let service = Service::builder(service_session.clone(), &service_name)
        .handler(move |req| {
            let payload = req.payload().to_bytes().into_owned();
            // Ignore readiness probes (single-byte payload) for this test's accounting.
            if payload.len() == 5 {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();

    let _running = service.start().await?;

    let mut handles = vec![];
    for i in 0..3 {
        let router_endpoint = router_endpoint.clone();
        let router_locator = router_locator.clone();
        let service_name = service_name.clone();
        let handle = tokio::spawn(async move {
            let client_session = open_peer_session(&router_endpoint).await?;
            let client = Client::builder(client_session, &service_name).build();
            wait_for_service_ready(
                &client,
                &service_name,
                &router_locator,
                Duration::from_secs(6),
            )
            .await?;
            let request = vec![i as u8; 5];
            client.call(&request).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.expect("task panicked");
        assert!(
            result.is_ok(),
            "call failed (router={}): {:?}",
            router_locator,
            result.err()
        );
    }

    drop(router_session);
    assert_eq!(request_count.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_service_discovery() -> AortaResult<()> {
    let (router_session, router_endpoint, router_locator) = open_router_context().await?;
    let session1 = open_peer_session(&router_endpoint).await?;
    let session2 = open_peer_session(&router_endpoint).await?;
    let client_session = open_peer_session(&router_endpoint).await?;
    let service_name_1 = unique_name("discovery_svc_1");
    let service_name_2 = unique_name("discovery_svc_2");

    let service1 = Service::builder(session1.clone(), &service_name_1)
        .handler(|req| {
            let _ = tokio::spawn(async move {
                let _ = req.reply(b"service1").await;
            });
        })
        .build();

    let service2 = Service::builder(session2.clone(), &service_name_2)
        .handler(|req| {
            let _ = tokio::spawn(async move {
                let _ = req.reply(b"service2").await;
            });
        })
        .build();

    let _running1 = service1.start().await?;
    let _running2 = service2.start().await?;

    let client1 = Client::builder(client_session.clone(), &service_name_1).build();
    let client2 = Client::builder(client_session, &service_name_2).build();
    wait_for_service_ready(
        &client1,
        &service_name_1,
        &router_locator,
        Duration::from_secs(6),
    )
    .await?;
    wait_for_service_ready(
        &client2,
        &service_name_2,
        &router_locator,
        Duration::from_secs(6),
    )
    .await?;

    let response1 = match client1.call(b"ping").await {
        Ok(response) => response,
        Err(err) => {
            return Err(AortaError::internal(format!(
                "cross_session_service_discovery (svc1) failed (router={}): {:?}",
                router_locator, err
            )));
        }
    };
    let response2 = match client2.call(b"ping").await {
        Ok(response) => response,
        Err(err) => {
            return Err(AortaError::internal(format!(
                "cross_session_service_discovery (svc2) failed (router={}): {:?}",
                router_locator, err
            )));
        }
    };

    assert_eq!(response1.to_bytes().as_ref(), b"service1");
    drop(router_session);
    assert_eq!(response2.to_bytes().as_ref(), b"service2");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_late_service_registration() -> AortaResult<()> {
    let (router_session, router_endpoint, router_locator) = open_router_context().await?;
    let client_session = open_peer_session(&router_endpoint).await?;
    let service_session = open_peer_session(&router_endpoint).await?;
    let service_name = unique_name("late_service");

    let client = Client::builder(client_session, &service_name)
        .timeout_ms(3000)
        .build();

    let service_session_for_task = service_session.clone();
    let service_name_for_task = service_name.clone();
    let service_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;

        let service = Service::builder(service_session_for_task, &service_name_for_task)
            .handler(|req| {
                let payload = req.payload().to_bytes().into_owned();
                let _ = tokio::spawn(async move {
                    let _ = req.reply(&payload).await;
                });
            })
            .build();

        service.start().await
    });

    let _running = service_handle.await.expect("service task panicked")?;
    wait_for_service_ready(
        &client,
        &service_name,
        &router_locator,
        Duration::from_secs(6),
    )
    .await?;

    let response = match client.call(&[1, 2, 3]).await {
        Ok(response) => response,
        Err(err) => {
            return Err(AortaError::internal(format!(
                "cross_session_late_service_registration failed (router={}): {:?}",
                router_locator, err
            )));
        }
    };
    drop(router_session);
    assert_eq!(response.to_bytes().as_ref(), &[1, 2, 3]);
    Ok(())
}

fn echo_handler(req: crate::service::ServiceRequest) {
    let payload = req.payload().to_bytes().into_owned();
    let _ = tokio::spawn(async move {
        let _ = req.reply(&payload).await;
    });
}

/// When a second node claims a service already owned by another
/// node, the network-level single-provider check must observe the foreign owner
/// token (advisory, off the hot path). A node that started alone must not see a
/// conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_single_provider_owner_conflict_detected() -> AortaResult<()> {
    let (router_session, router_endpoint, _router_locator) = open_router_context().await?;
    let session_a = open_peer_session(&router_endpoint).await?;
    let session_b = open_peer_session(&router_endpoint).await?;
    let service_name = unique_name("owner_conflict");

    // Provider A claims the service first (stable node id "owner-node-a").
    let running_a = Service::builder(session_a.clone(), &service_name)
        .node_id("owner-node-a")
        .handler(echo_handler)
        .build()
        .start()
        .await?;

    // A's owner check runs alone; wait past its query window before B appears so
    // the "started alone -> no conflict" assertion is deterministic.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert_eq!(
        running_a.single_provider_conflict_count(),
        0,
        "provider A started alone and must not observe a conflict"
    );

    // Provider B claims the SAME service with a DIFFERENT node id.
    let running_b = Service::builder(session_b.clone(), &service_name)
        .node_id("owner-node-b")
        .handler(echo_handler)
        .build()
        .start()
        .await?;

    // B's background owner check must discover A's foreign owner token.
    let mut detected = 0;
    for _ in 0..50 {
        detected = running_b.single_provider_conflict_count();
        if detected > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        detected >= 1,
        "provider B must detect provider A as a second owner of '{}'",
        service_name
    );

    drop(running_a);
    drop(running_b);
    drop(router_session);
    Ok(())
}

/// Stopping a service must release its owner token even
/// if the `RunningService` handle is kept, so a stop/restart does not leave a
/// stale claim that makes the replacement provider report a false conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_owner_token_released_on_stop() -> AortaResult<()> {
    let (router_session, router_endpoint, _router_locator) = open_router_context().await?;
    let session_a = open_peer_session(&router_endpoint).await?;
    let probe_session = open_peer_session(&router_endpoint).await?;
    let service_name = unique_name("owner_release");
    let key_builder = crate::key_expr::KeyExprBuilder::new("default").expect("key builder");
    let query = key_builder
        .service_owner_query(&service_name)
        .expect("owner query");

    let owns = |keys: &[String]| {
        keys.iter()
            .any(|k| crate::key_expr::service_owner_key_node_id(k) == Some("owner-node-a"))
    };

    let mut running_a = Service::builder(session_a.clone(), &service_name)
        .node_id("owner-node-a")
        .handler(echo_handler)
        .build()
        .start()
        .await?;

    // A's owner token becomes visible while running.
    let mut present = false;
    for _ in 0..30 {
        let keys = crate::liveliness::liveliness_get(
            probe_session.inner(),
            &query,
            Duration::from_millis(500),
        )
        .await?;
        if owns(&keys) {
            present = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(present, "A's owner token must be visible while running");

    // Stop A but KEEP the handle: the token must be released, not held until drop.
    running_a.stop();
    let mut released = false;
    for _ in 0..50 {
        let keys = crate::liveliness::liveliness_get(
            probe_session.inner(),
            &query,
            Duration::from_millis(500),
        )
        .await?;
        if !owns(&keys) {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        released,
        "stopping the service must release its owner token even with the handle kept"
    );

    drop(running_a);
    drop(router_session);
    Ok(())
}
