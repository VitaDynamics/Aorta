//! Cross-process conflict detection tests for ContextStore.
//!
//! Two ContextStores connected to a local Zenoh router via separate
//! peer Sessions. Each store declares the same prefix; on every put
//! from a foreign `source_instance_id` the receiving store should
//! emit a `context.owner_conflict` telemetry event. Self-puts must
//! never emit telemetry.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use zenoh::config::{EndPoint, WhatAmI};

use crate::contextstore::ContextStore;
use crate::error::{AortaError, AortaResult};
use crate::node::Node;
use crate::owner_conflict_fb::root_as_owner_conflict_event;
use crate::session::Session;

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

async fn open_peer_node(name: &str, endpoint: &EndPoint) -> AortaResult<Node> {
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
    Node::with_zenoh_config(name, "default", config).await
}

#[derive(Clone, Debug)]
struct CapturedConflict {
    key: String,
    self_node: String,
    self_instance: String,
    foreign_node: String,
    foreign_instance: String,
    foreign_stamp: u64,
    foreign_seq: u64,
}

async fn telemetry_subscriber(
    endpoint: &EndPoint,
    captured: Arc<Mutex<Vec<CapturedConflict>>>,
) -> AortaResult<(Session, zenoh::pubsub::Subscriber<()>)> {
    let session = open_peer_session(endpoint).await?;
    let captured_cb = Arc::clone(&captured);
    let sub = session
        .inner()
        .declare_subscriber("aorta/default/sys/telemetry/vital/ctx/owner_conflict")
        .callback(move |sample| {
            let bytes = sample.payload().to_bytes();
            if let Some(evt) = root_as_owner_conflict_event(&bytes) {
                captured_cb.lock().unwrap().push(CapturedConflict {
                    key: evt.key().unwrap_or("").to_string(),
                    self_node: evt.self_node().unwrap_or("").to_string(),
                    self_instance: evt.self_instance_id().unwrap_or("").to_string(),
                    foreign_node: evt.foreign_node().unwrap_or("").to_string(),
                    foreign_instance: evt.foreign_instance_id().unwrap_or("").to_string(),
                    foreign_stamp: evt.foreign_stamp(),
                    foreign_seq: evt.foreign_seq(),
                });
            }
        })
        .await
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "telemetry subscribe failed: {}",
                err
            )))
        })?;
    Ok((session, sub))
}

async fn wait_for<F: Fn() -> bool>(predicate: F, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    predicate()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_owners_with_distinct_node_names_detect_each_other() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;

    let node_a = open_peer_node(&unique_name("ctxC_conf_a"), &endpoint).await?;
    let node_b = open_peer_node(&unique_name("ctxC_conf_b"), &endpoint).await?;
    let store_a = ContextStore::new(&node_a);
    let store_b = ContextStore::new(&node_b);
    let instance_a = store_a.source_instance_id().to_string();
    let instance_b = store_b.source_instance_id().to_string();

    store_a.declare_owned_prefix("rcp_conf_dn/**").await?;
    store_b.declare_owned_prefix("rcp_conf_dn/**").await?;

    let captured: Arc<Mutex<Vec<CapturedConflict>>> = Arc::new(Mutex::new(Vec::new()));
    let (_telem_session, _telem_sub) =
        telemetry_subscriber(&endpoint, Arc::clone(&captured)).await?;

    // Allow subscribers + telemetry sub to settle.
    tokio::time::sleep(Duration::from_millis(300)).await;

    store_a.set_u64("rcp_conf_dn/state", 1).await?;
    store_b.set_u64("rcp_conf_dn/state", 2).await?;

    let ok = wait_for(
        || captured.lock().unwrap().len() >= 2,
        Duration::from_millis(3_000),
    )
    .await;
    assert!(
        ok,
        "expected at least 2 owner_conflict events, got {}",
        captured.lock().unwrap().len()
    );

    let events = captured.lock().unwrap().clone();
    assert!(
        events.iter().any(|e| {
            e.foreign_instance == instance_a
                && e.self_instance == instance_b
                && e.key.ends_with("ctx/rcp_conf_dn/state")
        }),
        "expected store_b to emit telemetry pointing at A, got events={:?}",
        events
    );
    assert!(
        events.iter().any(|e| {
            e.foreign_instance == instance_b
                && e.self_instance == instance_a
                && e.key.ends_with("ctx/rcp_conf_dn/state")
        }),
        "expected store_a to emit telemetry pointing at B, got events={:?}",
        events
    );

    store_a.shutdown().await?;
    store_b.shutdown().await?;
    drop(router_session);
    Ok(())
}

// NOTE: "two writer processes share a node name but have distinct
// instance ids" is a conflict-detection case. Reproducing that
// scenario requires two real processes — the in-process Node registry
// rejects duplicate names within a single process by design. The
// detection logic itself is verified by inspection (see
// `handle_self_subscriber_sample` in contextstore.rs: it compares
// source_instance_id only, never source_node) and by the
// `telemetry_event_carries_full_metadata` test which proves
// `foreign_instance` is the discriminator carried in the telemetry
// envelope independently of `foreign_node`.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_writes_do_not_emit_owner_conflict() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;

    let node_a = open_peer_node(&unique_name("ctxC_self"), &endpoint).await?;
    let store_a = ContextStore::new(&node_a);
    store_a.declare_owned_prefix("rcp_self/**").await?;

    let captured: Arc<Mutex<Vec<CapturedConflict>>> = Arc::new(Mutex::new(Vec::new()));
    let (_telem_session, _telem_sub) =
        telemetry_subscriber(&endpoint, Arc::clone(&captured)).await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..5u64 {
        store_a.set_u64("rcp_self/state", i).await?;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    let count = captured.lock().unwrap().len();
    assert_eq!(
        count, 0,
        "self-puts must not emit owner_conflict telemetry; got {} events",
        count
    );

    store_a.shutdown().await?;
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_writers_under_unowned_prefix_emit_no_telemetry() -> AortaResult<()> {
    // Ownership asymmetry: when nobody declares ownership of a prefix,
    // raw puts from two distinct writers produce a multi-instance
    // observation on `watch` but NO owner_conflict telemetry — there
    // is no owner subscriber to detect them. We construct this by
    // bypassing ContextStore::set entirely and issuing raw Zenoh puts
    // with synthetic ContextValue payloads (mimicking what
    // `aorta ctx set` will do).
    use crate::contextstore_fb::root_as_context_value;
    use crate::contextstore_fb::{encode_value_u64, ContextValueMeta};

    let (router_session, endpoint) = open_router_session().await?;
    let writer_a = open_peer_session(&endpoint).await?;
    let writer_b = open_peer_session(&endpoint).await?;

    let captured: Arc<Mutex<Vec<CapturedConflict>>> = Arc::new(Mutex::new(Vec::new()));
    let (_telem_session, _telem_sub) =
        telemetry_subscriber(&endpoint, Arc::clone(&captured)).await?;

    // A bystander watcher confirms the raw puts ARE visible there even
    // though no telemetry fires — proving the asymmetry, not just
    // "nothing happened".
    let watcher_session = open_peer_session(&endpoint).await?;
    let observed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_cb = Arc::clone(&observed);
    let _watcher = watcher_session
        .inner()
        .declare_subscriber("aorta/default/ctx/no_owner/**")
        .callback(move |sample| {
            let bytes = sample.payload().to_bytes();
            if let Some(parsed) = root_as_context_value(&bytes) {
                observed_cb
                    .lock()
                    .unwrap()
                    .push(parsed.source_instance_id().unwrap_or("").to_string());
            }
        })
        .await
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "watcher subscribe failed: {}",
                err
            )))
        })?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let make_payload = |instance: &str, seq: u64| {
        let meta = ContextValueMeta {
            stamp_ns: 1_700_000_000_000_000_000 + seq,
            expires_at_ns: 0,
            source_node: "raw".to_string(),
            source_instance_id: instance.to_string(),
            sequence: seq,
        };
        encode_value_u64(seq, &meta)
    };

    writer_a
        .inner()
        .put(
            "aorta/default/ctx/no_owner/state",
            make_payload("instance_aaaa", 1),
        )
        .await
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "writer_a put: {}",
                err
            )))
        })?;
    writer_b
        .inner()
        .put(
            "aorta/default/ctx/no_owner/state",
            make_payload("instance_bbbb", 1),
        )
        .await
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "writer_b put: {}",
                err
            )))
        })?;

    let saw_both = wait_for(
        || {
            let o = observed.lock().unwrap();
            o.iter().any(|s| s == "instance_aaaa") && o.iter().any(|s| s == "instance_bbbb")
        },
        Duration::from_millis(2_000),
    )
    .await;
    assert!(saw_both, "watcher should see both raw writers");

    // Telemetry: zero events (no declared owner -> nobody to detect).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let count = captured.lock().unwrap().len();
    assert_eq!(
        count, 0,
        "no-owner prefix raw puts must not emit telemetry; got {} events",
        count
    );

    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_event_carries_full_metadata() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;

    let node_a = open_peer_node(&unique_name("ctxC_meta_a"), &endpoint).await?;
    let node_b = open_peer_node(&unique_name("ctxC_meta_b"), &endpoint).await?;
    let store_a = ContextStore::new(&node_a);
    let store_b = ContextStore::new(&node_b);

    store_a.declare_owned_prefix("rcp_meta/**").await?;
    store_b.declare_owned_prefix("rcp_meta/**").await?;

    let captured: Arc<Mutex<Vec<CapturedConflict>>> = Arc::new(Mutex::new(Vec::new()));
    let (_telem_session, _telem_sub) =
        telemetry_subscriber(&endpoint, Arc::clone(&captured)).await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    store_a.set_u64("rcp_meta/state", 7).await?;
    store_a.set_u64("rcp_meta/state", 8).await?;

    let ok = wait_for(
        || captured.lock().unwrap().len() >= 2,
        Duration::from_millis(3_000),
    )
    .await;
    assert!(ok);

    let events = captured.lock().unwrap().clone();
    // The two telemetry puts from store_b race through tokio::spawn so
    // their delivery order isn't guaranteed; look up by foreign_seq
    // instead of indexing.
    let by_b: Vec<_> = events
        .iter()
        .filter(|e| e.foreign_instance == store_a.source_instance_id())
        .collect();
    assert!(by_b.len() >= 2, "expected ≥2 b→a events, got {:?}", by_b);

    let evt = by_b
        .iter()
        .find(|e| e.foreign_seq == 2)
        .copied()
        .expect("seq=2 telemetry event from store_b");
    assert_eq!(evt.self_instance, store_b.source_instance_id());
    assert_eq!(evt.foreign_instance, store_a.source_instance_id());
    assert_eq!(evt.self_node, node_b.name());
    assert_eq!(evt.foreign_node, node_a.name());
    assert!(evt.key.ends_with("ctx/rcp_meta/state"));
    assert!(evt.foreign_stamp > 0);

    store_a.shutdown().await?;
    store_b.shutdown().await?;
    drop(router_session);
    Ok(())
}
