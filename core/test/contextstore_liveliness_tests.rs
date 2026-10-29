//! Owner-liveliness tests for ContextStore.
//!
//! Owner side: the FIRST successful `declare_owned_prefix` on a store
//! declares exactly one Zenoh liveliness token at
//! `aorta/{group}/sys/alive/ctx/{node_name}/{source_instance_id}`;
//! `shutdown` (and store drop / process death) withdraws it.
//!
//! Reader side: `watch_ctx_owner_liveliness` is a thin passthrough over
//! Zenoh's liveliness subscriber with `history(true)` — replay of
//! current tokens, then live join/leave. No mapping keys, no merge
//! protocol (both withdrawn in the proposal's Appendix A).
//!
//! Isolation: every test spins up its OWN router + peer sessions (the
//! conflict-tests pattern) so history replay can never observe tokens
//! left mid-withdrawal by earlier tests on the shared local router —
//! the "exactly one token" and "no stray events" assertions require a
//! hermetic network.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use zenoh::config::{EndPoint, WhatAmI};

use crate::contextstore::{ContextStore, OwnerLivelinessEvent};
use crate::error::{AortaError, AortaResult};
use crate::key_expr::KeyExprBuilder;
use crate::liveliness::liveliness_get;
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

/// Raw liveliness subscriber on the group's token query with
/// `history(true)`. Returns the subscriber plus a channel receiving
/// `(key, is_put)` observations.
async fn raw_token_subscriber(
    node: &Node,
) -> AortaResult<(
    zenoh::pubsub::Subscriber<()>,
    std::sync::mpsc::Receiver<(String, bool)>,
)> {
    let query = node.key_builder().ctx_owner_liveliness_query()?;
    let (tx, rx) = std::sync::mpsc::channel();
    let sub: zenoh::pubsub::Subscriber<()> = node
        .session()
        .inner()
        .liveliness()
        .declare_subscriber(query)
        .history(true)
        .callback(move |sample: zenoh::sample::Sample| {
            let _ = tx.send((
                sample.key_expr().as_str().to_string(),
                sample.kind() == zenoh::sample::SampleKind::Put,
            ));
        })
        .await
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "raw liveliness subscribe failed: {}",
                err
            )))
        })?;
    Ok((sub, rx))
}

/// Wait for an observation matching `predicate`; panics on timeout.
fn wait_for<F: Fn(&(String, bool)) -> bool>(
    rx: &std::sync::mpsc::Receiver<(String, bool)>,
    predicate: F,
    what: &str,
) -> (String, bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match rx.try_recv() {
            Ok(obs) if predicate(&obs) => return obs,
            Ok(_) => {}
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        }
    }
    panic!("timed out waiting for {}", what);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_owned_prefix_declares_exactly_one_token() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_token"), &endpoint).await?;
    let store = ContextStore::new(&node);
    let expected_key = node
        .key_builder()
        .ctx_owner_liveliness_key(node.name(), store.source_instance_id())?;

    let (_sub, rx) = raw_token_subscriber(&node).await?;

    // First declaration must bring the token alive.
    store.declare_owned_prefix("system_liv_tok/**").await?;
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && *is_put,
        "token Put for the first declared prefix",
    );
    assert_eq!(key, expected_key.as_str());
    assert!(is_put);

    // A second (disjoint) prefix on the SAME store must NOT declare a
    // second token: exactly one token per store. Drain a bounded window
    // and assert no observation for any other key arrives.
    store.declare_owned_prefix("system_liv_tok2/**").await?;
    let deadline = std::time::Instant::now() + Duration::from_millis(300);
    let mut extra = Vec::new();
    while std::time::Instant::now() < deadline {
        match rx.try_recv() {
            Ok(obs) if obs.0 != expected_key.as_str() => extra.push(obs),
            Ok(_) => {}
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    assert!(
        extra.is_empty(),
        "multi-prefix store must declare exactly one token, got extra observations: {:?}",
        extra
    );

    store.shutdown().await?;
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_liveliness_subscriber_sees_existing_owner() -> AortaResult<()> {
    // Replay semantics: a subscriber declared AFTER the owner exists
    // must still observe the token via history(true).
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_replay"), &endpoint).await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_liv_replay/**").await?;

    let (_sub, rx) = raw_token_subscriber(&node).await?;
    let expected_key = node
        .key_builder()
        .ctx_owner_liveliness_key(node.name(), store.source_instance_id())?;

    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && *is_put,
        "history replay of the existing token",
    );
    assert_eq!(key, expected_key.as_str());
    assert!(is_put, "replay of a live token must be a Put sample");

    store.shutdown().await?;
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_shutdown_withdraws_token() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_gone"), &endpoint).await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_liv_gone/**").await?;
    let expected_key = node
        .key_builder()
        .ctx_owner_liveliness_key(node.name(), store.source_instance_id())?;

    let (_sub, rx) = raw_token_subscriber(&node).await?;
    wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && *is_put,
        "token Put after declare",
    );

    // Deterministic shutdown must withdraw the token (Delete sample).
    store.shutdown().await?;
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && !*is_put,
        "token Delete after shutdown",
    );
    assert_eq!(key, expected_key.as_str());
    assert!(!is_put);
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_ctx_owner_liveliness_reports_alive_and_gone() -> AortaResult<()> {
    // Reader API end-to-end across two peer nodes: watch on one store's
    // session, owner on another.
    let (router_session, endpoint) = open_router_session().await?;
    let owner_node = open_peer_node(&unique_name("ctxE_liv_owner"), &endpoint).await?;
    let owner = ContextStore::new(&owner_node);
    let watcher_node = open_peer_node(&unique_name("ctxE_liv_watcher"), &endpoint).await?;
    let watcher = ContextStore::new(&watcher_node);

    let events: Arc<Mutex<Vec<OwnerLivelinessEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let events_cb = Arc::clone(&events);
    let handle = watcher
        .watch_ctx_owner_liveliness(move |event| {
            events_cb.lock().unwrap().push(event);
        })
        .await?;

    // No owner yet — with a hermetic network nothing may fire.
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        events.lock().unwrap().is_empty(),
        "no events expected before any owner declares"
    );

    // Owner declares -> alive event with the right identity.
    owner.declare_owned_prefix("system_liv_watch/**").await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(e) = events
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.alive && e.source_instance_id == owner.source_instance_id())
        {
            assert_eq!(e.node_name, owner_node.name());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for alive event"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Owner shutdown -> gone event for the same instance.
    owner.shutdown().await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(e) = events
            .lock()
            .unwrap()
            .iter()
            .find(|e| !e.alive && e.source_instance_id == owner.source_instance_id())
        {
            assert_eq!(e.node_name, owner_node.name());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for gone event"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    handle.shutdown().await?;
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_handle_shutdown_stops_callbacks() -> AortaResult<()> {
    let (router_session, endpoint) = open_router_session().await?;
    let owner_node = open_peer_node(&unique_name("ctxE_liv_stop"), &endpoint).await?;
    let owner = ContextStore::new(&owner_node);
    let watcher_node = open_peer_node(&unique_name("ctxE_liv_stop_w"), &endpoint).await?;
    let watcher = ContextStore::new(&watcher_node);

    let count = Arc::new(AtomicUsize::new(0));
    let count_cb = Arc::clone(&count);
    let handle = watcher
        .watch_ctx_owner_liveliness(move |_event| {
            count_cb.fetch_add(1, Ordering::SeqCst);
        })
        .await?;

    owner.declare_owned_prefix("system_liv_stop/**").await?;
    // Let the alive event land.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while count.load(Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the first event"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // Deterministic teardown: after shutdown() no further callback can
    // fire, so the owner's token withdrawal must NOT be observed.
    handle.shutdown().await?;
    owner.shutdown().await?;
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "no callbacks may fire after handle shutdown"
    );
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_redisclared_after_shutdown() -> AortaResult<()> {
    // shutdown() withdraws the token; a subsequent declare on the same
    // store brings a fresh token back (store reuse after teardown).
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_reuse"), &endpoint).await?;
    let store = ContextStore::new(&node);
    let expected_key = node
        .key_builder()
        .ctx_owner_liveliness_key(node.name(), store.source_instance_id())?;

    store.declare_owned_prefix("system_liv_reuse/**").await?;
    store.shutdown().await?;
    store.declare_owned_prefix("system_liv_reuse/**").await?;

    let (_sub, rx) = raw_token_subscriber(&node).await?;
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && *is_put,
        "token Put after re-declare post-shutdown",
    );
    assert_eq!(key, expected_key.as_str());
    assert!(is_put);

    store.shutdown().await?;
    drop(router_session);
    Ok(())
}

// ── Shutdown must wait for in-flight callbacks ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_shutdown_waits_for_in_flight_callback() -> AortaResult<()> {
    // The pinned zenoh 1.9.0 defaults wait_callbacks=false, so an
    // undeclare WITHOUT wait_callbacks() can return while a callback is
    // still running — the C++ / C-ABI free paths free callback state
    // immediately after, which is a use-after-free. shutdown() must
    // block until the in-flight callback exits.
    let (router_session, endpoint) = open_router_session().await?;
    let owner_node = open_peer_node(&unique_name("ctxE_liv_p0"), &endpoint).await?;
    let owner = ContextStore::new(&owner_node);
    let watcher_node = open_peer_node(&unique_name("ctxE_liv_p0_w"), &endpoint).await?;
    let watcher = ContextStore::new(&watcher_node);

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    // The watch callback must be Send + Sync; std mpsc endpoints are
    // not Sync, so park them behind Arc<Mutex<...>>.
    let entered_tx = Arc::new(Mutex::new(entered_tx));
    let release_rx = Arc::new(Mutex::new(release_rx));
    let entered_tx_cb = Arc::clone(&entered_tx);
    let release_rx_cb = Arc::clone(&release_rx);
    let handle = watcher
        .watch_ctx_owner_liveliness(move |_event| {
            // Block inside the callback until the test releases us.
            let _ = entered_tx_cb.lock().unwrap().send(());
            let _ = release_rx_cb.lock().unwrap().recv();
        })
        .await?;

    owner.declare_owned_prefix("system_liv_p0/**").await?;
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("callback never entered its blocking section");

    // shutdown() must NOT return while the callback is still running.
    let shutdown_task = tokio::spawn(async move { handle.shutdown().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !shutdown_task.is_finished(),
        "shutdown returned while a callback was still in flight (wait_callbacks)"
    );

    // Release the callback; shutdown must then complete.
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), shutdown_task)
        .await
        .expect("shutdown did not complete after the callback exited")
        .expect("shutdown task panicked")?;

    drop(router_session);
    Ok(())
}

// ── Failed declare after token creation withdraws the token ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_declare_after_token_creation_withdraws_token() -> AortaResult<()> {
    // Subscriber failure: the token is created first, then the
    // Subscriber declaration fails. The declare must withdraw the token
    // it created — no ghost token (store reports alive with zero
    // successful prefixes).
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_fail"), &endpoint).await?;
    let watcher_node = open_peer_node(&unique_name("ctxE_liv_fail_w"), &endpoint).await?;
    let store = ContextStore::new(&node);

    store.test_fail_subscriber_declare();
    let result = store.declare_owned_prefix("system_liv_fail/**").await;
    assert!(
        matches!(result, Err(AortaError::Transport(_))),
        "declare must fail, got {:?}",
        result
    );
    assert!(
        store.declared_prefixes().is_empty(),
        "no partial prefix state after a failed declare"
    );

    // No ghost token: the router's liveliness state must settle to
    // empty (a transient Put/Delete pair may still be in flight, so
    // query the authoritative current state rather than counting raw
    // observations).
    let query = watcher_node.key_builder().ctx_owner_liveliness_query()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let tokens = liveliness_get(
            watcher_node.session().inner(),
            &query,
            Duration::from_millis(200),
        )
        .await?;
        if tokens.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ghost token still alive after failed declare: {:?}",
            tokens
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A retry without the fault succeeds and the token comes alive.
    let (_sub, rx) = raw_token_subscriber(&watcher_node).await?;
    store.declare_owned_prefix("system_liv_fail/**").await?;
    let expected_key = node
        .key_builder()
        .ctx_owner_liveliness_key(node.name(), store.source_instance_id())?;
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && *is_put,
        "token Put after retry",
    );
    assert_eq!(key, expected_key.as_str());
    assert!(is_put);

    store.shutdown().await?;
    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_queryable_declare_withdraws_token() -> AortaResult<()> {
    // Queryable failure: same rollback for the Queryable
    // declaration failure.
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_failq"), &endpoint).await?;
    let watcher_node = open_peer_node(&unique_name("ctxE_liv_failq_w"), &endpoint).await?;
    let store = ContextStore::new(&node);

    store.test_fail_queryable_declare();
    let result = store.declare_owned_prefix("system_liv_failq/**").await;
    assert!(
        matches!(result, Err(AortaError::Transport(_))),
        "declare must fail, got {:?}",
        result
    );
    assert!(store.declared_prefixes().is_empty());

    // No ghost token: the router's liveliness state must settle to
    // empty (a transient Put/Delete pair may still be in flight, so
    // query the authoritative current state rather than counting raw
    // observations).
    let query = watcher_node.key_builder().ctx_owner_liveliness_query()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let tokens = liveliness_get(
            watcher_node.session().inner(),
            &query,
            Duration::from_millis(200),
        )
        .await?;
        if tokens.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "ghost token still alive after failed declare: {:?}",
            tokens
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let (_sub, rx) = raw_token_subscriber(&watcher_node).await?;
    store.declare_owned_prefix("system_liv_failq/**").await?;
    let expected_key = node
        .key_builder()
        .ctx_owner_liveliness_key(node.name(), store.source_instance_id())?;
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && *is_put,
        "token Put after retry",
    );
    assert_eq!(key, expected_key.as_str());
    assert!(is_put);

    store.shutdown().await?;
    drop(router_session);
    Ok(())
}

// ── declare_owned_prefix and shutdown are linearized ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_and_shutdown_are_linearized() -> AortaResult<()> {
    // Concurrent declare/shutdown must linearize: after both
    // complete, either (prefix declared AND token alive) or (nothing
    // AND no token) — never a prefix without its token. A lifecycle
    // mutex held by declare across precheck->install->rollback and by
    // shutdown from entry provides this; before the fix, shutdown could
    // withdraw a just-created token before the prefix install.
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_lin"), &endpoint).await?;
    let probe_node = open_peer_node(&unique_name("ctxE_liv_lin_p"), &endpoint).await?;
    let query = node.key_builder().ctx_owner_liveliness_query()?;

    for i in 0..20 {
        let store = ContextStore::new(&node);
        let store_d = store.clone();
        let store_s = store.clone();
        let declare =
            tokio::spawn(async move { store_d.declare_owned_prefix("system_liv_lin/**").await });
        let shutdown = tokio::spawn(async move { store_s.shutdown().await });
        let declare_result = declare.await.expect("declare task panicked");
        let shutdown_result = shutdown.await.expect("shutdown task panicked");
        assert!(
            declare_result.is_ok(),
            "declare failed on iteration {}: {:?}",
            i,
            declare_result
        );
        assert!(
            shutdown_result.is_ok(),
            "shutdown failed on iteration {}",
            i
        );

        // Poll until the liveliness state has propagated: the end state
        // must be consistent (prefix present iff token alive).
        let prefixes = store.declared_prefixes();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let tokens = liveliness_get(
                probe_node.session().inner(),
                &query,
                Duration::from_millis(200),
            )
            .await?;
            let consistent = prefixes.is_empty() == tokens.is_empty();
            if consistent || std::time::Instant::now() >= deadline {
                assert!(
                    consistent,
                    "iteration {}: prefixes={:?} tokens={:?} — declare/shutdown not linearized",
                    i, prefixes, tokens
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    drop(router_session);
    Ok(())
}

// ── Last store clone drop withdraws the token ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_withdrawn_when_last_store_clone_drops() -> AortaResult<()> {
    // The Queryable/Subscriber callbacks must hold Weak<Inner>.
    // With a strong capture, the Inner -> session -> callback -> Inner
    // cycle keeps Inner (and its liveliness token) alive after the last
    // store clone drops, so the token would never be withdrawn.
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_drop"), &endpoint).await?;
    let watcher_node = open_peer_node(&unique_name("ctxE_liv_drop_w"), &endpoint).await?;

    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_liv_drop/**").await?;
    let expected_key = node
        .key_builder()
        .ctx_owner_liveliness_key(node.name(), store.source_instance_id())?;

    let (_sub, rx) = raw_token_subscriber(&watcher_node).await?;
    wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && *is_put,
        "token Put after declare",
    );

    // Drop the last store clone WITHOUT shutdown: the node (and its
    // session) stays alive, so only Inner dropping can withdraw the
    // token.
    drop(store);
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k == expected_key.as_str() && !*is_put,
        "token Delete after last clone drop",
    );
    assert_eq!(key, expected_key.as_str());
    assert!(!is_put);

    drop(router_session);
    Ok(())
}

// ── Node-name constraint enforced at the ContextStore boundary ──────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declare_rejects_invalid_owner_node_names() -> AortaResult<()> {
    // `Node::new` only rejects empty names, so the ContextStore
    // boundary must reject names that cannot form a single-segment
    // owner-liveliness token key — with a precise error naming the
    // constraint, before any state is created.
    for (name, needle) in [
        ("foo/bar", "single key segment"),
        ("has space", "whitespace"),
        ("node$name", "reserved"),
        ("node?name", "reserved"),
    ] {
        let node = Node::new(name, "default").await?;
        let store = ContextStore::new(&node);
        let err = store
            .declare_owned_prefix("system_liv_bad/**")
            .await
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("owner-liveliness identity invalid"),
            "unexpected error for {:?}: {}",
            name,
            msg
        );
        assert!(
            msg.contains(needle),
            "error for {:?} must name the constraint ({}): {}",
            name,
            needle,
            msg
        );
        assert!(
            store.declared_prefixes().is_empty(),
            "no state may be created for {:?}",
            name
        );
    }
    Ok(())
}

// ── Process death withdraws the token (kill cell) ───────────────────────────
//
// The kill cell: a real owner process is SIGKILLed and
// the liveliness observer must see the token withdrawn. Detection
// latency is recorded in the test log (no SLO — the thin API has
// none). For a LOCAL kill the OS closes the child's TCP socket
// immediately, so the router notices at once (~tens of ms); the
// keepalive/lease config (`transport/link/tx/lease` + `keep_alive`,
// default lease 10 s) governs the network-partition case where the
// link goes silent without closing. This test configures a short lease
// (1000 ms) on both sides so a silent-death cell would also complete
// quickly; production detection latency follows the deployment's zenoh
// config, not this test's.

const SHORT_LEASE_ROUTER_CONFIG: &str = r#"
{
  mode: "router",
  listen: { endpoints: ["tcp/127.0.0.1:0"] },
  scouting: { multicast: { enabled: false }, gossip: { enabled: false } },
  transport: {
    shared_memory: { enabled: false },
    link: { tx: { lease: 1000, keep_alive: 4 } }
  }
}
"#;

const SHORT_LEASE_CLIENT_CONFIG: &str = r#"
{
  mode: "client",
  scouting: { multicast: { enabled: false } },
  transport: {
    shared_memory: { enabled: false },
    link: { tx: { lease: 1000, keep_alive: 4 } }
  }
}
"#;

/// Child half of `process_death_withdraws_token`: connects to the
/// parent's router, declares a prefix (token alive), and then stays
/// alive until the parent SIGKILLs it. In the normal suite (without the
/// marker env) it is a trivial pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_liveliness_child_probe() -> AortaResult<()> {
    let Ok(endpoint_str) = std::env::var("AORTA_OWNER_LIVELINESS_CHILD_ROUTER") else {
        return Ok(());
    };
    let mut config = zenoh::Config::from_json5(SHORT_LEASE_CLIENT_CONFIG)
        .map_err(|err| AortaError::internal(format!("child config error: {:?}", err)))?;
    let endpoint: zenoh::config::EndPoint = endpoint_str
        .parse()
        .map_err(|err| AortaError::internal(format!("child endpoint error: {:?}", err)))?;
    config
        .connect
        .endpoints
        .set(vec![endpoint.into()])
        .map_err(|err| AortaError::internal(format!("child connect error: {:?}", err)))?;
    let node = Node::with_zenoh_config(
        &format!("ctxE_liv_child_p{}", std::process::id()),
        "default",
        config,
    )
    .await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_liv_child/**").await?;
    // Stay alive until killed: process death => the router notices the
    // dead link after the lease and withdraws the token.
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_death_withdraws_token() -> AortaResult<()> {
    let mut config = zenoh::Config::from_json5(SHORT_LEASE_ROUTER_CONFIG)
        .map_err(|err| AortaError::internal(format!("router config error: {:?}", err)))?;
    config
        .set_mode(Some(WhatAmI::Router))
        .map_err(|err| AortaError::internal(format!("router mode error: {:?}", err)))?;
    let router = Session::open(config).await?;
    let endpoint = {
        let started = tokio::time::Instant::now();
        loop {
            let locators = router.inner().info().locators().await;
            if let Some(locator) = locators
                .iter()
                .find(|loc| loc.to_string().starts_with("tcp/"))
            {
                break EndPoint::from(locator.clone());
            }
            if started.elapsed() >= Duration::from_secs(5) {
                return Err(AortaError::timeout(
                    "router locator not found within 5s".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };

    // Watch the token plane on the router session itself.
    let query = KeyExprBuilder::new("default")?.ctx_owner_liveliness_query()?;
    let (tx, rx) = std::sync::mpsc::channel();
    let sub = router
        .inner()
        .liveliness()
        .declare_subscriber(query)
        .history(true)
        .callback(move |sample: zenoh::sample::Sample| {
            let _ = tx.send((
                sample.key_expr().as_str().to_string(),
                sample.kind() == zenoh::sample::SampleKind::Put,
            ));
        })
        .await
        .map_err(|err| {
            AortaError::transport(crate::error::TransportError::backend(format!(
                "router liveliness subscribe failed: {}",
                err
            )))
        })?;

    // Spawn a real owner child process and wait for its token. The
    // guard kills the child on ANY exit path (including a panicking
    // assertion), so a failed test never leaks an orphan holding the
    // test's output pipe.
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let exe = std::env::current_exe()
        .map_err(|err| AortaError::internal(format!("current_exe failed: {}", err)))?;
    let mut child = ChildGuard(
        std::process::Command::new(exe)
            .args([
                "--exact",
                "contextstore_liveliness_tests::owner_liveliness_child_probe",
                "--nocapture",
            ])
            .env("AORTA_OWNER_LIVELINESS_CHILD_ROUTER", endpoint.to_string())
            .spawn()
            .map_err(|err| AortaError::internal(format!("child spawn failed: {}", err)))?,
    );
    let child_pid = child.0.id();

    // The token key carries two trailing segments:
    // `ctxE_liv_child_p{pid}/{source_instance_id}` — only the node-name
    // part is predictable.
    let expected_prefix = format!("aorta/default/sys/alive/ctx/ctxE_liv_child_p{}", child_pid);
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k.starts_with(&expected_prefix) && *is_put,
        "child token Put",
    );
    assert!(is_put);
    assert!(
        key.starts_with(&format!("{}/", expected_prefix)),
        "child token key must carry node name + instance id: {}",
        key
    );

    // Kill the child (SIGKILL on Unix) and measure the detection
    // latency: how long until the liveliness observer sees the Delete.
    let killed_at = std::time::Instant::now();
    child
        .0
        .kill()
        .map_err(|err| AortaError::internal(format!("child kill failed: {}", err)))?;
    let (key, is_put) = wait_for(
        &rx,
        |(k, is_put)| k.starts_with(&expected_prefix) && !*is_put,
        "child token Delete after SIGKILL",
    );
    assert!(!is_put);
    assert!(
        key.starts_with(&format!("{}/", expected_prefix)),
        "delete key must match the put key: {}",
        key
    );
    let latency_ms = killed_at.elapsed().as_millis();
    // Latency is RECORDED, not asserted (no SLO in the thin API): the
    // log line is the deliverable. Governed by the zenoh
    // transport/link/tx lease (1000 ms here) + keep_alive (4).
    println!(
        "owner liveliness process-death detection latency: {} ms (governed by zenoh transport/link/tx lease=1000ms keep_alive=4)",
        latency_ms
    );

    sub.undeclare().await.map_err(|err| {
        AortaError::transport(crate::error::TransportError::backend(format!(
            "router liveliness undeclare failed: {}",
            err
        )))
    })?;
    Ok(())
}

// ── P2-a: repeat shutdown keeps returning the error after an undeclare failure ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveliness_shutdown_repeat_keeps_undeclare_error() -> AortaResult<()> {
    // P2-a: after an undeclare failure the subscriber was already
    // take()n — a second shutdown() must NOT return a false Ok. The
    // 'drain unconfirmed' state is persisted so every repeat call keeps
    // returning the error (the drain can never be confirmed).
    let (router_session, endpoint) = open_router_session().await?;
    let node = open_peer_node(&unique_name("ctxE_liv_drain"), &endpoint).await?;
    let watcher = ContextStore::new(&node);

    let handle = watcher.watch_ctx_owner_liveliness(|_event| {}).await?;

    crate::contextstore::TEST_FAIL_NEXT_UNDECLARE.store(true, std::sync::atomic::Ordering::SeqCst);
    let first = handle.shutdown().await;
    assert!(
        matches!(first, Err(AortaError::Transport(_))),
        "injected undeclare failure must surface as a transport error, got {:?}",
        first
    );

    // Repeat shutdown: the drain was never confirmed — keep failing.
    let second = handle.shutdown().await;
    assert!(
        matches!(second, Err(AortaError::Transport(_))),
        "repeat shutdown after an undeclare failure must keep returning the error, got {:?}",
        second
    );
    let third = handle.shutdown().await;
    assert!(matches!(third, Err(AortaError::Transport(_))));

    drop(router_session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_watch_shutdown_repeat_keeps_undeclare_error() -> AortaResult<()> {
    // P2-a: same repeat-error contract for WatchHandle.
    let node = Node::new("ctxE_ctx_drain", "default").await?;
    let store = ContextStore::new(&node);
    store.declare_owned_prefix("system_ctx_drain/**").await?;
    let handle = store.watch("system_ctx_drain/**", |_c| {}).await?;

    crate::contextstore::TEST_FAIL_NEXT_UNDECLARE.store(true, std::sync::atomic::Ordering::SeqCst);
    let first = handle.shutdown().await;
    assert!(matches!(first, Err(AortaError::Transport(_))));
    let second = handle.shutdown().await;
    assert!(
        matches!(second, Err(AortaError::Transport(_))),
        "repeat shutdown after an undeclare failure must keep returning the error, got {:?}",
        second
    );
    Ok(())
}
