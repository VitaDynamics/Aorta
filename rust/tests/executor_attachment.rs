//! Executor attachment contract tests.
//!
//! `Node::create_subscriber` starts the SDK auto-executor on a dedicated
//! `aorta-sdk-exec-<node>` OS thread. Before this change, attaching the node
//! to the application's own executor afterwards silently left every existing
//! subscriber wired to the auto-executor: the application's executor spun
//! forever without ever receiving a subscription event, and its configured
//! queue depth / callback groups were inert.
//!
//! The contract these tests pin down:
//!
//! * The FIRST executor attached to a node OWNS it. A second, different
//!   executor is REFUSED loudly instead of being half-attached.
//! * `NodeOptions::auto_executor(false)` lets the application declare
//!   ownership up front, so the correct wiring does not depend on call order.
//! * Under that opt-out, callback-capable primitives created before an
//!   executor is attached fail with a clear error instead of silently
//!   starting a second executor.
//!
//! Node construction uses the in-process local test router
//! (`AORTA_TEST_LOCAL_ROUTER`), so no peer is required.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aorta_sdk::{
    spawn_executor_thread, ExecutorNodeAttachExt, ExecutorOptions, Node, NodeOptions,
    SingleThreadedExecutor,
};

fn local_router() {
    std::env::set_var("AORTA_TEST_LOCAL_ROUTER", "1");
}

/// Order A — subscriber first, then the application attaches its own
/// executor. The auto-executor already owns the node, so the attach must be
/// REFUSED rather than silently re-pointing the node cell and leaving the
/// application's executor inert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attaching_second_executor_after_subscriber_is_refused() {
    local_router();
    let node = Node::new("exec_attach_order_a", "default")
        .await
        .expect("node create");

    let _sub = node
        .create_subscriber("exec_attach/order_a", |_payload, _ctx| {})
        .await
        .expect("subscriber create");

    assert!(
        node.has_internal_executor(),
        "create_subscriber must have started the SDK auto-executor"
    );

    let mut exec_b = SingleThreadedExecutor::new(64);
    let err = exec_b
        .try_attach_sdk_node(&node)
        .expect_err("attaching a second executor to a live node must be refused");
    let message = err.to_string();
    assert!(
        message.contains("exec_attach_order_a"),
        "refusal must name the node, got: {message}"
    );

    // The refusal must leave the node coherent: still owned by the
    // auto-executor, and exec_b must not be half-attached.
    assert!(
        node.has_internal_executor(),
        "a refused attach must not retire the auto-executor"
    );

    let _ = node.close().await;
}

/// The same refusal must hold for the raw core path
/// (`SingleThreadedExecutor::add_node`), which bypasses the SDK extension
/// trait. This is the path the benchmark used, and the one that silently
/// produced the inert-executor state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_add_node_after_subscriber_is_refused() {
    local_router();
    let node = Node::new("exec_attach_raw", "default")
        .await
        .expect("node create");

    let _sub = node
        .create_subscriber("exec_attach/raw", |_payload, _ctx| {})
        .await
        .expect("subscriber create");

    let mut exec_b = SingleThreadedExecutor::new(64);
    let err = exec_b
        .try_add_node(node.inner().clone())
        .expect_err("raw add_node on a live node must be refused");
    assert!(
        err.to_string().contains("exec_attach_raw"),
        "refusal must name the node, got: {err}"
    );

    let _ = node.close().await;
}

/// Order B under the explicit opt-out — the application owns the executor,
/// attaches it first, and the subscriber it creates afterwards delivers to
/// THAT executor. Proven by only spinning `exec_b`: if the event had been
/// routed anywhere else the callback would never run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_executor_receives_subscriber_events() {
    local_router();
    let node = Node::with_options(
        "exec_attach_owned",
        "default",
        NodeOptions::default().with_auto_executor(false),
    )
    .await
    .expect("node create");

    let mut exec_b = SingleThreadedExecutor::new(64);
    exec_b
        .try_attach_sdk_node(&node)
        .expect("attach before any primitive must succeed");

    assert!(
        !node.has_internal_executor(),
        "no SDK auto-executor may exist when the application owns the executor"
    );

    let hits = Arc::new(AtomicUsize::new(0));
    let hits_cb = Arc::clone(&hits);
    let _sub = node
        .create_subscriber("exec_attach/owned", move |_payload, _ctx| {
            hits_cb.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("subscriber create");

    assert!(
        !node.has_internal_executor(),
        "create_subscriber must not start an auto-executor once one is attached"
    );

    let publisher = node
        .create_publisher_raw("exec_attach/owned")
        .await
        .expect("publisher create");
    tokio::time::sleep(Duration::from_millis(200)).await;
    publisher.publish(b"hello".to_vec()).expect("publish");

    // Nothing may run the callback until the application's executor spins:
    // that is what proves exec_b — not a second, hidden executor — owns the
    // subscription event.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "callback ran without exec_b spinning — a second executor is dispatching"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hits.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        exec_b.spin_once(Duration::from_millis(50)).await;
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the application's executor must receive the subscription event"
    );

    let _ = node.close().await;
}

/// With `auto_executor(false)` and no executor attached, a callback-capable
/// primitive must fail loudly instead of silently starting a second executor
/// (or silently dropping into the local fallback queue).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_executor_opt_out_rejects_primitives_before_attach() {
    local_router();
    let node = Node::with_options(
        "exec_attach_optout",
        "default",
        NodeOptions::default().with_auto_executor(false),
    )
    .await
    .expect("node create");

    let err = node
        .create_subscriber("exec_attach/optout", |_payload, _ctx| {})
        .await
        .map(|_| ())
        .expect_err("subscriber creation without an executor must fail");
    let message = err.to_string();
    assert!(
        message.contains("auto_executor"),
        "the error must point at the opt-out that caused it, got: {message}"
    );

    assert!(
        !node.has_internal_executor(),
        "the opt-out must never start the SDK auto-executor"
    );

    let _ = node.close().await;
}

/// A node detached from its executor may be legitimately re-attached, and the
/// subscriber created against the FIRST executor must follow the node to the
/// second one — the construction-time sender snapshot must not outlive the
/// attachment it was taken from.
///
/// `exec_a` is deliberately kept ALIVE for the whole test: a dropped executor
/// closes its queue, and the closed-channel fallback would mask a stale
/// snapshot. Only a live-but-detached predecessor exercises the generation
/// check itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_follows_node_across_detach_and_reattach() {
    local_router();
    let node = Node::with_options(
        "exec_attach_reattach",
        "default",
        NodeOptions::default().with_auto_executor(false),
    )
    .await
    .expect("node create");

    let mut exec_a = SingleThreadedExecutor::new(64);
    exec_a.try_attach_sdk_node(&node).expect("first attach");

    let hits = Arc::new(AtomicUsize::new(0));
    let hits_cb = Arc::clone(&hits);
    let _sub = node
        .create_subscriber("exec_attach/reattach", move |_payload, _ctx| {
            hits_cb.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("subscriber create");

    // Hand the node over to a second executor the sanctioned way. exec_a stays
    // alive (and never spins) so its queue remains OPEN — an event routed there
    // by a stale snapshot would be swallowed, not bounced back by a closed
    // channel.
    exec_a.detach_nodes();
    let mut exec_b = SingleThreadedExecutor::new(64);
    exec_b
        .try_attach_sdk_node(&node)
        .expect("re-attach after detach must be allowed");

    let publisher = node
        .create_publisher_raw("exec_attach/reattach")
        .await
        .expect("publisher create");
    tokio::time::sleep(Duration::from_millis(200)).await;
    publisher.publish(b"hello".to_vec()).expect("publish");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hits.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        exec_b.spin_once(Duration::from_millis(50)).await;
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "a subscriber created under exec_a must follow the node to exec_b"
    );

    drop(exec_a);
    let _ = node.close().await;
}

/// `spawn_executor_thread` is the supported way to get the auto-executor's
/// runtime placement (a dedicated OS thread, not the application's shared
/// tokio pool) for an application-owned executor. It must actually dispatch,
/// and it must refuse an executor whose queue a `current_thread` runtime
/// cannot serve rather than panicking inside the spawned thread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_executor_thread_dispatches_off_the_shared_runtime() {
    local_router();
    let node = Node::with_options(
        "exec_attach_spawned",
        "default",
        NodeOptions::default().with_auto_executor(false),
    )
    .await
    .expect("node create");

    let mut executor = SingleThreadedExecutor::new_with_options(
        64,
        ExecutorOptions {
            force_tokio_queue: true,
            ..ExecutorOptions::default()
        },
    );
    executor.try_attach_sdk_node(&node).expect("attach");

    let hits = Arc::new(AtomicUsize::new(0));
    let hits_cb = Arc::clone(&hits);
    let _sub = node
        .create_subscriber("exec_attach/spawned", move |_payload, _ctx| {
            hits_cb.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("subscriber create");

    let mut spin = spawn_executor_thread("exec-attach-spawned", executor).expect("spawn");

    let publisher = node
        .create_publisher_raw("exec_attach/spawned")
        .await
        .expect("publisher create");
    tokio::time::sleep(Duration::from_millis(200)).await;
    publisher.publish(b"hello".to_vec()).expect("publish");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hits.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the spawned executor thread must dispatch the subscription event"
    );

    spin.stop();
    let _ = node.close().await;
}

#[test]
fn spawn_executor_thread_refuses_a_queue_a_current_thread_runtime_cannot_serve() {
    // Default options may select the crossbeam+eventfd backend, which parks via
    // block_in_place — unsupported on the current_thread runtime the helper
    // builds. Refuse up front instead of panicking inside the spawned thread.
    let executor = SingleThreadedExecutor::new(64);
    let err = spawn_executor_thread("exec-attach-bad-queue", executor)
        .map(|_| ())
        .expect_err("an executor without force_tokio_queue must be refused");
    assert!(
        err.to_string().contains("force_tokio_queue"),
        "the refusal must name the option to set, got: {err}"
    );
}

/// Regression: `ensure_executor` used the infallible `add_node`, so a
/// REFUSED attachment still spawned the executor, stored its handle and
/// reported success. The subscriber created next then found no event sender on
/// the node and fell back to its local queue — callbacks split away from the
/// executor the SDK had just promised — while the stored handle made every
/// later `ensure_executor` a no-op, so the node could never recover even after
/// the blocking producer was dropped.
///
/// The reachable refusal after the P2-A owner fix is a CROSS-executor one: the
/// auto-executor is a brand-new instance, so it is never the node's last owner,
/// and a live timer still pins the node to the executor it was detached from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_creation_fails_when_the_auto_executor_cannot_attach() {
    local_router();
    let node = Node::new("exec_auto_refused", "default")
        .await
        .expect("node create");

    // Application executor owns the node, and a timer pins its queue.
    let mut exec_a = SingleThreadedExecutor::new(64);
    exec_a.try_attach_sdk_node(&node).expect("attach");
    let timer = node
        .create_timer(Duration::from_millis(50), || {})
        .await
        .expect("timer");

    // Released, but the timer still holds exec_a's queue, so no OTHER executor
    // may take the node — including the SDK's auto-executor.
    exec_a.detach_nodes();
    assert!(
        !node.inner().has_event_sender(),
        "precondition: the node must be detached"
    );

    let err = node
        .create_subscriber("exec_auto_refused/topic", |_payload, _ctx| {})
        .await
        .map(|_| ())
        .expect_err(
            "creating a subscriber must FAIL when the auto-executor cannot attach — \
             silently falling back to the local queue is the split #334 exists to prevent",
        );
    let message = err.to_string();
    assert!(
        message.contains("exec_auto_refused"),
        "the error must name the node, got: {message}"
    );

    assert!(
        !node.has_internal_executor(),
        "a refused attach must not leave a spun-up executor handle behind: it dispatches \
         nothing and blocks every later ensure_executor from retrying"
    );

    drop(timer);
    let _ = node.close().await;
}

/// The refusal must be recoverable: once the blocking producer is gone, the
/// same node must be able to start its auto-executor. This is what the stored
/// dead handle used to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_executor_retries_after_the_blocking_producer_is_dropped() {
    local_router();
    let node = Node::new("exec_auto_retry", "default")
        .await
        .expect("node create");

    let mut exec_a = SingleThreadedExecutor::new(64);
    exec_a.try_attach_sdk_node(&node).expect("attach");
    let timer = node
        .create_timer(Duration::from_millis(50), || {})
        .await
        .expect("timer");
    exec_a.detach_nodes();

    assert!(
        node.create_subscriber("exec_auto_retry/topic", |_payload, _ctx| {})
            .await
            .map(|_| ())
            .is_err(),
        "blocked while the timer is live"
    );

    drop(timer);

    let hits = Arc::new(AtomicUsize::new(0));
    let hits_cb = Arc::clone(&hits);
    let _sub = node
        .create_subscriber("exec_auto_retry/topic", move |_payload, _ctx| {
            hits_cb.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("once the producer is dropped the auto-executor must start");
    assert!(
        node.has_internal_executor(),
        "the retry must actually start the auto-executor"
    );
    assert!(
        node.inner().has_event_sender(),
        "and the node must be attached to it, not left on a local-queue fallback"
    );

    let publisher = node
        .create_publisher_raw("exec_auto_retry/topic")
        .await
        .expect("publisher");
    tokio::time::sleep(Duration::from_millis(200)).await;
    publisher.publish(b"hello".to_vec()).expect("publish");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hits.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the recovered executor must actually dispatch"
    );

    let _ = node.close().await;
}
