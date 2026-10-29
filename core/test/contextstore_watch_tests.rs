use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::contextstore::{ContextChangeKind, ContextStore};
use crate::contextstore_fb::ContextKind;
use crate::error::AortaResult;
use crate::node::Node;

async fn wait_for<F: Fn() -> bool>(predicate: F, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    predicate()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_receives_self_set_events() -> AortaResult<()> {
    let node = Node::new("ctxB_watch_self", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_watch_self/**").await?;

    let received: Arc<Mutex<Vec<crate::contextstore::ContextChange>>> =
        Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = store
        .watch("system_watch_self/**", move |change| {
            received_cb.lock().unwrap().push(change);
        })
        .await?;

    store.set_bool("system_watch_self/flag", true).await?;

    let ok = wait_for(
        || !received.lock().unwrap().is_empty(),
        Duration::from_millis(1_500),
    )
    .await;
    assert!(ok, "watch did not receive event in time");

    let events = received.lock().unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.kind, ContextChangeKind::Live);
    assert!(
        event.key.ends_with("ctx/system_watch_self/flag"),
        "unexpected key: {}",
        event.key
    );
    assert_eq!(event.value_kind, ContextKind::Bool);
    assert!(event.bool_value);
    assert_eq!(event.source_node, "ctxB_watch_self");
    assert_eq!(event.source_instance_id, store.source_instance_id());
    assert_eq!(event.sequence, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_receives_typed_field_for_each_kind() -> AortaResult<()> {
    let node = Node::new("ctxB_watch_kinds", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_watch_kinds/**").await?;

    let received: Arc<Mutex<Vec<crate::contextstore::ContextChange>>> =
        Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = store
        .watch("system_watch_kinds/**", move |change| {
            received_cb.lock().unwrap().push(change);
        })
        .await?;

    store.set_bool("system_watch_kinds/b", true).await?;
    store.set_i64("system_watch_kinds/i", -100).await?;
    store.set_u64("system_watch_kinds/u", 100).await?;
    store.set_f64("system_watch_kinds/f", 1.25).await?;
    store.set_string("system_watch_kinds/s", "hello").await?;

    let ok = wait_for(
        || received.lock().unwrap().len() >= 5,
        Duration::from_millis(2_000),
    )
    .await;
    assert!(ok, "watch did not receive 5 events in time");

    let events = received.lock().unwrap();
    let by_kind: std::collections::HashMap<_, _> =
        events.iter().map(|e| (e.value_kind, e.clone())).collect();

    let bool_e = by_kind.get(&ContextKind::Bool).expect("bool event");
    assert!(bool_e.bool_value);

    let i64_e = by_kind.get(&ContextKind::I64).expect("i64 event");
    assert_eq!(i64_e.i64_value, -100);

    let u64_e = by_kind.get(&ContextKind::U64).expect("u64 event");
    assert_eq!(u64_e.u64_value, 100);

    let f64_e = by_kind.get(&ContextKind::F64).expect("f64 event");
    assert!((f64_e.f64_value - 1.25).abs() < 1e-9);

    let str_e = by_kind.get(&ContextKind::String).expect("string event");
    assert_eq!(str_e.string_value.as_deref(), Some("hello"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_drop_stops_callbacks() -> AortaResult<()> {
    let node = Node::new("ctxB_watch_drop", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_watch_drop/**").await?;

    let received: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let received_cb = Arc::clone(&received);
    let watch = store
        .watch("system_watch_drop/**", move |_change| {
            *received_cb.lock().unwrap() += 1;
        })
        .await?;

    store.set_bool("system_watch_drop/key", true).await?;
    let ok = wait_for(
        || *received.lock().unwrap() >= 1,
        Duration::from_millis(1_500),
    )
    .await;
    assert!(ok, "did not receive first event");
    let baseline = *received.lock().unwrap();
    assert_eq!(baseline, 1);

    drop(watch);
    tokio::time::sleep(Duration::from_millis(150)).await;

    store.set_bool("system_watch_drop/key", false).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let after = *received.lock().unwrap();
    assert_eq!(after, baseline, "callback fired after WatchHandle dropped");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_filters_by_prefix() -> AortaResult<()> {
    let node = Node::new("ctxB_watch_filter", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("rcp_filt/**").await?;
    store.declare_owned_prefix("dag_filt/**").await?;

    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = Arc::clone(&received);
    let _watch = store
        .watch("rcp_filt/**", move |change| {
            received_cb.lock().unwrap().push(change.key);
        })
        .await?;

    store.set_bool("dag_filt/state", true).await?;
    store.set_bool("rcp_filt/state", true).await?;

    let ok = wait_for(
        || !received.lock().unwrap().is_empty(),
        Duration::from_millis(1_500),
    )
    .await;
    assert!(ok);

    tokio::time::sleep(Duration::from_millis(200)).await;
    let events = received.lock().unwrap();
    assert_eq!(events.len(), 1, "only rcp_filt key should be visible");
    assert!(
        events[0].ends_with("ctx/rcp_filt/state"),
        "unexpected event key: {}",
        events[0]
    );
    Ok(())
}
