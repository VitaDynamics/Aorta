//! `watch_with_initial_snapshot` tests.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use zenoh::config::{EndPoint, WhatAmI};

use crate::contextstore::{
    ContextChange, ContextChangeKind, ContextStore, SnapshotDoneReason, WatchSnapshotOptions,
};
use crate::error::{AortaError, AortaResult};
use crate::node::Node;
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

fn snapshot_done(events: &[ContextChange]) -> Option<&ContextChange> {
    events
        .iter()
        .find(|e| e.kind == ContextChangeKind::SnapshotDone)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_query_drained_with_active_owner() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;
    let owner_node = open_peer_node(&unique_name("ctxC_snap_owner"), &endpoint).await?;
    let reader_node = open_peer_node(&unique_name("ctxC_snap_reader"), &endpoint).await?;

    let owner = ContextStore::new(&owner_node);
    let reader = ContextStore::new(&reader_node);
    owner.declare_owned_prefix("rcp_snap_drain/**").await?;
    owner.set_u64("rcp_snap_drain/k1", 1).await?;
    owner.set_u64("rcp_snap_drain/k2", 2).await?;
    owner.set_u64("rcp_snap_drain/k3", 3).await?;

    // Allow Zenoh routing to settle so the reader's get sees the owner.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let received: Arc<Mutex<Vec<ContextChange>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = reader
        .watch_with_initial_snapshot(
            "rcp_snap_drain/**",
            WatchSnapshotOptions::default(),
            move |c| {
                received_cb.lock().unwrap().push(c);
            },
        )
        .await?;

    let ok = wait_for(
        || snapshot_done(&received.lock().unwrap()).is_some(),
        Duration::from_millis(3_000),
    )
    .await;
    assert!(ok, "did not see SnapshotDone within deadline");

    let events = received.lock().unwrap().clone();
    let done_idx = events
        .iter()
        .position(|e| e.kind == ContextChangeKind::SnapshotDone)
        .expect("SnapshotDone present");
    let done = &events[done_idx];

    assert_eq!(
        done.snapshot_done_reason,
        Some(SnapshotDoneReason::QueryDrained)
    );

    // Every event before SnapshotDone must be a Snapshot (not Live).
    for event in &events[..done_idx] {
        assert_eq!(
            event.kind,
            ContextChangeKind::Snapshot,
            "pre-done event was {:?}",
            event.kind
        );
    }

    // Owner published 3 keys; we expect 3 unique snapshot keys.
    let snap_keys: std::collections::BTreeSet<String> =
        events[..done_idx].iter().map(|e| e.key.clone()).collect();
    assert_eq!(
        snap_keys.len(),
        3,
        "expected 3 unique snapshot keys, got {:?}",
        snap_keys
    );

    owner.shutdown().await?;
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_deadline_elapsed_with_no_owner() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;
    let reader_node = open_peer_node(&unique_name("ctxC_snap_no_owner"), &endpoint).await?;
    let reader = ContextStore::new(&reader_node);

    let received: Arc<Mutex<Vec<ContextChange>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = reader
        .watch_with_initial_snapshot(
            "rcp_snap_no_owner/**",
            WatchSnapshotOptions {
                snapshot_deadline: Duration::from_millis(250),
            },
            move |c| {
                received_cb.lock().unwrap().push(c);
            },
        )
        .await?;

    let ok = wait_for(
        || snapshot_done(&received.lock().unwrap()).is_some(),
        Duration::from_millis(2_000),
    )
    .await;
    assert!(ok);

    let events = received.lock().unwrap().clone();
    let done = snapshot_done(&events).expect("SnapshotDone");
    // Either reason is acceptable: Zenoh may close the receiver as
    // soon as it observes that no Queryables match (QueryDrained), or
    // the local tokio deadline may fire first (DeadlineElapsed). The
    // contract is that SnapshotDone fires with SOME reason and zero
    // snapshot events.
    let reason = done.snapshot_done_reason.expect("reason set");
    assert!(
        reason == SnapshotDoneReason::DeadlineElapsed || reason == SnapshotDoneReason::QueryDrained,
        "unexpected reason: {:?}",
        reason
    );
    let snap_count = events
        .iter()
        .filter(|e| e.kind == ContextChangeKind::Snapshot)
        .count();
    assert_eq!(
        snap_count, 0,
        "no owner online -> snapshot must be empty, got {}",
        snap_count
    );

    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_then_live_ordering() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;
    let owner_node = open_peer_node(&unique_name("ctxC_snap_live_owner"), &endpoint).await?;
    let reader_node = open_peer_node(&unique_name("ctxC_snap_live_reader"), &endpoint).await?;
    let owner = ContextStore::new(&owner_node);
    let reader = ContextStore::new(&reader_node);

    owner.declare_owned_prefix("rcp_snap_live/**").await?;
    owner.set_u64("rcp_snap_live/k_initial", 1).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let received: Arc<Mutex<Vec<ContextChange>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = reader
        .watch_with_initial_snapshot(
            "rcp_snap_live/**",
            WatchSnapshotOptions::default(),
            move |c| {
                received_cb.lock().unwrap().push(c);
            },
        )
        .await?;

    // After SnapshotDone, an additional put should arrive as Live.
    let ok = wait_for(
        || snapshot_done(&received.lock().unwrap()).is_some(),
        Duration::from_millis(3_000),
    )
    .await;
    assert!(ok);

    owner.set_u64("rcp_snap_live/k_post", 2).await?;
    let ok = wait_for(
        || {
            received
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.kind == ContextChangeKind::Live && e.key.ends_with("k_post"))
        },
        Duration::from_millis(2_000),
    )
    .await;
    assert!(ok, "expected post-snapshot put to arrive as Live");

    let events = received.lock().unwrap().clone();
    let done_idx = events
        .iter()
        .position(|e| e.kind == ContextChangeKind::SnapshotDone)
        .unwrap();

    // No Live events before SnapshotDone (k_post was set after).
    for event in &events[..done_idx] {
        assert_ne!(event.kind, ContextChangeKind::Live);
    }
    // No Snapshot events after SnapshotDone.
    for event in &events[done_idx + 1..] {
        assert_ne!(event.kind, ContextChangeKind::Snapshot);
    }

    owner.shutdown().await?;
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_events_arrive_after_snapshot_done_with_no_owner() -> AortaResult<()> {
    // Replies arriving after the deadline marker are
    // discarded — they are NOT promoted to live events. We can't
    // construct a controllable slow Queryable, but the snapshot task
    // owns the reply receiver and drops it on exit, so a late reply
    // would simply be dropped at the receiver. This test confirms the
    // observable side: after SnapshotDone with no live owner, no
    // additional events surface to the user callback.
    let (router_session, endpoint) = open_router_session().await?;
    let reader_node = open_peer_node(&unique_name("ctxC_snap_post"), &endpoint).await?;
    let reader = ContextStore::new(&reader_node);

    let received: Arc<Mutex<Vec<ContextChange>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = reader
        .watch_with_initial_snapshot(
            "ghost_post/**",
            WatchSnapshotOptions {
                snapshot_deadline: Duration::from_millis(200),
            },
            move |c| {
                received_cb.lock().unwrap().push(c);
            },
        )
        .await?;

    let ok = wait_for(
        || snapshot_done(&received.lock().unwrap()).is_some(),
        Duration::from_millis(2_000),
    )
    .await;
    assert!(ok);
    let count_at_done = received.lock().unwrap().len();

    // Wait well beyond the deadline so any late Zenoh reply or stray
    // worker activity has time to surface — none should.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let count_later = received.lock().unwrap().len();
    assert_eq!(
        count_at_done, count_later,
        "no events should arrive after SnapshotDone when no owner is online"
    );

    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_owners_yield_distinct_snapshot_replies_per_instance() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;
    let owner_a = open_peer_node(&unique_name("ctxC_snap_overlap_a"), &endpoint).await?;
    let owner_b = open_peer_node(&unique_name("ctxC_snap_overlap_b"), &endpoint).await?;
    let reader_node = open_peer_node(&unique_name("ctxC_snap_overlap_r"), &endpoint).await?;

    let store_a = ContextStore::new(&owner_a);
    let store_b = ContextStore::new(&owner_b);
    let reader = ContextStore::new(&reader_node);

    store_a.declare_owned_prefix("rcp_overlap/**").await?;
    store_b.declare_owned_prefix("rcp_overlap/**").await?;
    // Both write the same key with different values.
    store_a.set_u64("rcp_overlap/state", 100).await?;
    store_b.set_u64("rcp_overlap/state", 200).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let received: Arc<Mutex<Vec<ContextChange>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = reader
        .watch_with_initial_snapshot(
            "rcp_overlap/**",
            WatchSnapshotOptions::default(),
            move |c| {
                received_cb.lock().unwrap().push(c);
            },
        )
        .await?;

    let ok = wait_for(
        || snapshot_done(&received.lock().unwrap()).is_some(),
        Duration::from_millis(3_000),
    )
    .await;
    assert!(ok);

    let events = received.lock().unwrap().clone();
    let snapshots: Vec<&ContextChange> = events
        .iter()
        .filter(|e| e.kind == ContextChangeKind::Snapshot)
        .collect();
    // Both owners reply with the same key; reader should see two
    // snapshot events for `rcp_overlap/state` distinguishable by
    // source_instance_id.
    let overlap_replies: Vec<&&ContextChange> = snapshots
        .iter()
        .filter(|e| e.key.ends_with("ctx/rcp_overlap/state"))
        .collect();
    assert_eq!(
        overlap_replies.len(),
        2,
        "expected 2 snapshot replies for overlapping key, got {}",
        overlap_replies.len()
    );
    let instances: std::collections::BTreeSet<String> = overlap_replies
        .iter()
        .map(|e| e.source_instance_id.clone())
        .collect();
    assert_eq!(instances.len(), 2);
    assert!(instances.contains(store_a.source_instance_id()));
    assert!(instances.contains(store_b.source_instance_id()));

    store_a.shutdown().await?;
    store_b.shutdown().await?;
    drop(router_session);
    Ok(())
}
