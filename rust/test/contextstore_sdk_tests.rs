//! Rust SDK smoke tests for ContextStore via `Node::create_context_store`.
//!
//! Core-level behaviour (declare bootstrap, set/get/watch, conflict
//! detection, snapshot, telemetry) is exhaustively tested in
//! `aorta_core`; this file just validates the SDK wiring — that
//! `Node::create_context_store()` and the re-exported types reach the
//! same primitive end-to-end.

use crate::contextstore::{ContextChangeKind, ContextKind, WatchSnapshotOptions};
use crate::{AortaResult, Node};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_create_context_store_set_get_roundtrip() -> AortaResult<()> {
    let node = Node::new("sdk_ctx_basic", "sdk_test").await?;
    let store = node.create_context_store();
    store.declare_owned_prefix("rcp_sdk_basic/**").await?;

    store.set_u64("rcp_sdk_basic/state", 5).await?;
    let read = store.get_u64("rcp_sdk_basic/state").await?;
    assert_eq!(read.value, 5);
    assert!(read.is_fresh);
    assert_eq!(read.source_node, "sdk_ctx_basic");
    assert_eq!(read.source_instance_id.len(), 16);
    assert_eq!(read.sequence, 1);

    store.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_typed_set_get_for_each_kind() -> AortaResult<()> {
    let node = Node::new("sdk_ctx_kinds", "sdk_test").await?;
    let store = node.create_context_store();
    store.declare_owned_prefix("system_sdk_kinds/**").await?;

    store.set_bool("system_sdk_kinds/b", true).await?;
    store.set_i64("system_sdk_kinds/i", -7).await?;
    store.set_u64("system_sdk_kinds/u", 7).await?;
    store.set_f64("system_sdk_kinds/f", 1.5).await?;
    store.set_string("system_sdk_kinds/s", "alpha").await?;

    assert!(store.get_bool("system_sdk_kinds/b").await?.value);
    assert_eq!(store.get_i64("system_sdk_kinds/i").await?.value, -7);
    assert_eq!(store.get_u64("system_sdk_kinds/u").await?.value, 7);
    assert!((store.get_f64("system_sdk_kinds/f").await?.value - 1.5).abs() < 1e-9);
    assert_eq!(
        store.get_string("system_sdk_kinds/s").await?.value,
        "alpha".to_string()
    );

    store.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_distinct_stores_have_distinct_instance_ids() -> AortaResult<()> {
    let node = Node::new("sdk_ctx_distinct", "sdk_test").await?;
    let a = node.create_context_store();
    let b = node.create_context_store();
    assert_ne!(a.source_instance_id(), b.source_instance_id());
    assert_eq!(a.source_node(), b.source_node());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_watch_with_initial_snapshot_marker() -> AortaResult<()> {
    let node = Node::new("sdk_ctx_snap", "sdk_test").await?;
    let store = node.create_context_store();
    store.declare_owned_prefix("rcp_sdk_snap/**").await?;
    store.set_u64("rcp_sdk_snap/state", 3).await?;

    let received: Arc<Mutex<Vec<crate::contextstore::ContextChange>>> =
        Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = store
        .watch_with_initial_snapshot(
            "rcp_sdk_snap/**",
            WatchSnapshotOptions {
                snapshot_deadline: Duration::from_millis(500),
            },
            move |c| received_cb.lock().unwrap().push(c),
        )
        .await?;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(2_000);
    while tokio::time::Instant::now() < deadline {
        if received
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.kind == ContextChangeKind::SnapshotDone)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let events = received.lock().unwrap();
    assert!(events
        .iter()
        .any(|c| c.kind == ContextChangeKind::SnapshotDone));
    drop(events);
    store.shutdown().await?;
    Ok(())
}

#[test]
fn sdk_re_exports_resolve() {
    // Smoke: confirm the SDK re-exports expose every type a downstream
    // crate is expected to use, without hitting Zenoh runtime.
    let _ = ContextKind::Bool;
    let _ = ContextKind::String;
    let _ = WatchSnapshotOptions::default();
}
