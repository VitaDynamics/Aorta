//! Flow: `Scope` lifetime rules, `when_all`/`when_any`, `offload`, and
//! `Client::call_in`.

use super::scope::{Scope, ScopeOpts, TaskHandle};
use super::{offload, when_all, when_any, CancelReason, StopReason, Stopped};
use crate::event_queue::EventPriority;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn root() -> Scope {
    Scope::new_root(ScopeOpts::default())
}

// ---- spawn / join / close ---------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_and_join_returns_the_value() {
    let scope = root();
    let handle = scope.spawn(async { 41 + 1 });
    assert_eq!(handle.join().await, Ok(42));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_waits_for_every_child() {
    let scope = root();
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..4 {
        let done = Arc::clone(&done);
        // Deliberate fire-and-forget: close() below is the join point.
        let _detached = scope.spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            done.fetch_add(1, Ordering::SeqCst);
        });
    }
    assert!(scope.live_children() > 0);
    scope.close().await;
    assert_eq!(done.load(Ordering::SeqCst), 4, "rule 1: no orphan tasks");
    assert_eq!(scope.live_children(), 0);
}

// ---- fail-fast ---------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_panic_cancels_siblings_fail_fast() {
    let scope = root();
    let sibling_saw_cancel = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&sibling_saw_cancel);
    let sibling_scope = scope.clone();
    let sibling = scope.spawn(async move {
        sibling_scope.cancelled().await;
        // Flow: this line is UNREACHABLE — the dispatch boundary
        // resolves the sibling BEFORE its body observes the cancel. Code
        // needing to run under cancellation belongs in a shield.
        flag.store(true, Ordering::SeqCst);
    });
    let panicker = scope.spawn(async {
        panic!("boom");
    });

    let err = panicker.join().await.expect_err("panicker fails");
    assert!(matches!(err, Stopped(StopReason::Fault { .. })));
    let sibling_stop = sibling
        .join()
        .await
        .expect_err("fail-fast resolves the sibling AT the boundary");
    assert_eq!(
        sibling_stop.reason(),
        &StopReason::Cancelled(CancelReason::ChildFailed)
    );
    assert!(
        !sibling_saw_cancel.load(Ordering::SeqCst),
        "boundary preempts the body's own cancel handling"
    );
    assert_eq!(
        scope.cancel_reason(),
        Some(CancelReason::ChildFailed),
        "a child error cancels the scope"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_scope_confines_the_failure() {
    let scope = Scope::new_root(ScopeOpts {
        supervision: crate::flow::Supervision::Isolate,
        ..Default::default()
    });
    let panicker = scope.spawn(async {
        panic!("contained");
    });
    let err = panicker.join().await.expect_err("failure surfaces");
    assert!(matches!(err, Stopped(StopReason::Fault { .. })));
    assert!(
        !scope.is_cancelled(),
        "a supervised scope confines the failure to the failing child"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_child_does_not_trip_fail_fast() {
    // Cancellation is not an error: a hard-stopped child (the
    // when_any loser shape) must not cancel the scope.
    let scope = root();
    let child = scope.spawn(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
    });
    child.abort();
    let err = child.join().await.expect_err("aborted");
    assert!(matches!(err, Stopped(StopReason::Cancelled(_))));
    assert!(!scope.is_cancelled());
}

// ---- when_all / when_any ----------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_all_collects_results_in_input_order() {
    let scope = root();
    let handles: Vec<TaskHandle<usize>> = (0..5)
        .map(|i| {
            scope.spawn(async move {
                // Reverse completion order to prove output order is by input.
                tokio::time::sleep(Duration::from_millis(50 - 10 * i as u64)).await;
                i
            })
        })
        .collect();
    assert_eq!(when_all(handles).await, Ok(vec![0, 1, 2, 3, 4]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_all_reports_the_fault_over_a_settled_cascade_cancel() {
    // TCK when_all_fault flake on a slow runner:
    // under FailFast, the faulting child's cascade cancels its parked
    // sibling; if the SIBLING (input index 0) settles first, the old
    // first-error-in-input-order pick reported Cancelled(ChildFailed)
    // instead of the fault that CAUSED it. Contract (C++ parity):
    // fault outranks cancel across ALL slots. Both handles are
    // fully settled BEFORE when_all first polls, so the adverse order is
    // deterministic, not a race.
    let scope = Scope::new_root(ScopeOpts::named("when-all-fault-pick"));
    let parked = scope.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    let failing = scope.spawn(async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        panic!("pick the fault");
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !(parked.is_finished() && failing.is_finished()) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let err = when_all(vec![parked, failing]).await.expect_err("fails");
    assert!(
        matches!(err, Stopped(StopReason::Fault { .. })),
        "fault must outrank the sibling's cascade cancel, got {err:?}"
    );
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_all_fails_fast_and_aborts_the_rest() {
    let scope = Scope::new_root(ScopeOpts {
        // Supervised so the panic's scope-cancel doesn't mask the
        // combinator's own abort behavior under test.
        supervision: crate::flow::Supervision::Isolate,
        ..Default::default()
    });
    let slow_finished = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&slow_finished);
    let slow = scope.spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        flag.store(true, Ordering::SeqCst);
    });
    let failing = scope.spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        panic!("fail fast");
    });
    let started = std::time::Instant::now();
    let err = when_all(vec![slow, failing]).await.expect_err("fails");
    assert!(matches!(err, Stopped(StopReason::Fault { .. })));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "fail-fast must not wait out the slow sibling"
    );
    scope.close().await;
    assert!(!slow_finished.load(Ordering::SeqCst), "slow was aborted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_any_first_wins_and_cancels_losers() {
    let scope = root();
    let loser_finished = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&loser_finished);
    let loser = scope.spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        flag.store(true, Ordering::SeqCst);
        1usize
    });
    let winner = scope.spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        2usize
    });
    let (idx, value) = when_any(vec![loser, winner]).await.expect("a winner");
    assert_eq!((idx, value), (1, 2));
    scope.close().await;
    assert!(!loser_finished.load(Ordering::SeqCst), "loser was aborted");
    assert!(
        !scope.is_cancelled(),
        "cancelled losers do not trip fail-fast"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_any_reports_all_failed() {
    let scope = Scope::new_root(ScopeOpts {
        supervision: crate::flow::Supervision::Isolate,
        ..Default::default()
    });
    let a = scope.spawn(async {
        panic!("a");
        #[allow(unreachable_code)]
        0usize
    });
    let b = scope.spawn(async {
        panic!("b");
        #[allow(unreachable_code)]
        0usize
    });
    let err = when_any(vec![a, b]).await.expect_err("all failed");
    assert!(matches!(err, Stopped(StopReason::Fault { .. })));
}

// ---- budget narrowing / checkpoint -------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_scope_narrows_budget_and_never_raises_priority() {
    let parent = Scope::new_root(ScopeOpts {
        priority: Some(EventPriority::Normal),
        budget: Some(Duration::from_millis(50)),
        ..Default::default()
    });
    let child = parent.child(ScopeOpts {
        priority: Some(EventPriority::Realtime), // must clamp down to Normal
        budget: Some(Duration::from_secs(60)),   // must narrow to ≤ parent's
        ..Default::default()
    });
    assert_eq!(
        child.priority(),
        EventPriority::Normal,
        "a child may narrow but never raise priority"
    );
    assert!(
        child.remaining().expect("deadline") <= Duration::from_millis(50),
        "a child budget never widens the parent's"
    );
    let lower = parent.child(ScopeOpts {
        priority: Some(EventPriority::Background),
        ..Default::default()
    });
    assert_eq!(lower.priority(), EventPriority::Background);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_fires_deadline_cancel_on_expiry() {
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::ZERO),
        ..Default::default()
    });
    assert!(!scope.is_cancelled(), "expiry is observed, not delivered");
    let err = scope.checkpoint().expect_err("expired");
    assert_eq!(err, Stopped::cancelled(CancelReason::DeadlineExceeded));
    assert!(scope.is_cancelled(), "the first observer fires the token");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_on_an_expired_budget_births_into_a_cancelled_scope() {
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::ZERO),
        ..Default::default()
    });
    // Flow: the boundary resolves a born-cancelled child BEFORE its
    // first poll — the body never runs (the pre-Flow path ran it to completion).
    let observer = scope.clone();
    let child = scope.spawn(async move { observer.is_cancelled() });
    let stopped = child.join().await.expect_err("born stopped");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::DeadlineExceeded)
    );
}

// ---- offload -----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_runs_off_the_runtime_worker_and_returns() {
    let caller_thread = std::thread::current().id();
    let (offload_thread, value) = offload(move || (std::thread::current().id(), 7)).await;
    assert_eq!(value, 7);
    assert_ne!(
        offload_thread, caller_thread,
        "the blocking segment must not run on the awaiting thread"
    );
    let (workers, _idle) = super::offload::worker_counts();
    assert!(workers >= 1, "the pool spawned lazily");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_does_not_stall_concurrent_async_work() {
    // A genuinely-blocking segment on the pool must not block the runtime:
    // a concurrent async task keeps making progress.
    let ticked = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ticked);
    let ticker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        flag.store(true, Ordering::SeqCst);
    });
    offload(|| std::thread::sleep(Duration::from_millis(120))).await;
    assert!(
        ticked.load(Ordering::SeqCst),
        "async work progressed while the blocking segment ran"
    );
    ticker.await.expect("ticker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_panic_propagates_to_the_awaiter() {
    let scope = Scope::new_root(ScopeOpts {
        supervision: crate::flow::Supervision::Isolate,
        ..Default::default()
    });
    let handle = scope.spawn(async {
        offload(|| panic!("blocking segment failed")).await;
    });
    let err = handle.join().await.expect_err("panic surfaces");
    assert!(matches!(err, Stopped(StopReason::Fault { .. })));
}

// ---- call_in (Flow Concepts §6.1) -------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_in_short_circuits_on_a_cancelled_scope() {
    let session = crate::session::Session::open_default()
        .await
        .expect("session");
    let client = crate::client::Client::builder(session, "flow_call_in_cancelled").build();
    let scope = root();
    scope.cancel(CancelReason::Explicit);
    let err = client
        .call_in(&scope, vec![1u8])
        .await
        .expect_err("cancelled scope must refuse the call");
    assert!(err.is_cancel(), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_in_reports_deadline_on_an_expired_budget() {
    let session = crate::session::Session::open_default()
        .await
        .expect("session");
    let client = crate::client::Client::builder(session, "flow_call_in_expired").build();
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::ZERO),
        ..Default::default()
    });
    let err = client
        .call_in(&scope, vec![1u8])
        .await
        .expect_err("expired budget must refuse the call");
    assert!(err.is_cancel(), "got: {err}");
    assert_eq!(scope.cancel_reason(), Some(CancelReason::DeadlineExceeded));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_in_roundtrips_and_cancel_interrupts_a_pending_call() {
    let session = crate::session::Session::open_default()
        .await
        .expect("session");
    let service_name = format!("flow_call_in_rt_{}", std::process::id());

    // Echo service.
    let service = crate::service::Service::builder(session.clone(), &service_name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();
    let _running = service.start().await.expect("service start");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = crate::client::Client::builder(session.clone(), &service_name).build();
    let scope = root();
    let response = client
        .call_in(&scope, vec![9u8, 9, 9])
        .await
        .expect("call_in roundtrip");
    assert_eq!(response.to_bytes().as_ref(), &[9u8, 9, 9]);

    // A pending call against a service that ACCEPTS but never replies
    // resolves promptly on scope cancel (cooperative: the query itself is
    // bounded by the narrowed timeout, but cancel must not wait for it).
    // (A nonexistent service is the wrong prop here: its query fails in
    // milliseconds — no queryable matches — beating any cancel.)
    let silent_name = format!("{service_name}_silent");
    let silent_service = crate::service::Service::builder(session.clone(), &silent_name)
        .handler(|req| {
            // Swallow the request: keep the query pending until timeout.
            std::mem::forget(req);
        })
        .build();
    let _silent_running = silent_service.start().await.expect("silent service");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let silent_client = crate::client::Client::builder(session, &silent_name).build();
    let cancel_scope = root();
    let canceller = cancel_scope.clone();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        canceller.cancel(CancelReason::Explicit);
    });
    let started = std::time::Instant::now();
    let err = silent_client
        .call_in(&cancel_scope, vec![1u8])
        .await
        .expect_err("cancel interrupts");
    assert!(err.is_cancel(), "got: {err}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "cancel must interrupt promptly, not ride out the query timeout"
    );
    cancel_task.await.expect("cancel task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_cancelled_interrupts_a_select_isolation_probe() {
    let scope = root();
    let canceller = scope.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        canceller.cancel(CancelReason::Explicit);
    });
    let started = std::time::Instant::now();
    let outcome = tokio::select! {
        _ = scope.cancelled() => "cancelled",
        _ = tokio::time::sleep(Duration::from_secs(10)) => "slept",
    };
    assert_eq!(outcome, "cancelled");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_bars_new_spawns_no_child_outlives_it() {
    // close() must be a one-way closing gate, not a
    // point-in-time wait — a spawn racing (or following) it may never run.
    let scope = root();
    scope.close().await;
    assert!(scope.is_closing());
    let ran = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ran);
    let late = scope.spawn(async move {
        flag.store(true, Ordering::SeqCst);
    });
    let err = late.join().await.expect_err("late spawn is refused");
    assert!(matches!(err, Stopped(StopReason::Cancelled(_))));
    assert!(!ran.load(Ordering::SeqCst), "the refused future never runs");
    assert_eq!(scope.live_children(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_deadline_sheds_stale_events_when_dispatch_is_delayed() {
    // End-to-end: a per-event deadline set through the dynamic
    // capability surface drops stale events at DEQUEUE when the
    // executor gets to them too late.
    use crate::executor::SingleThreadedExecutor;
    let topic = format!("flow_deadline_{}", std::process::id());
    let pub_node = crate::node::Node::new(format!("dl_pub_{}", std::process::id()), "default")
        .await
        .expect("pub node");
    let sub_node = crate::node::Node::new(format!("dl_sub_{}", std::process::id()), "default")
        .await
        .expect("sub node");

    let delivered = Arc::new(AtomicUsize::new(0));
    let delivered_cb = Arc::clone(&delivered);
    let subscriber = sub_node
        .create_subscriber(&topic, move |_payload: &crate::ZBytes, _ctx| {
            delivered_cb.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("subscriber");
    subscriber
        .set_flow_admission(crate::subscriber::FlowAdmission::gated(
            0,
            Some(Duration::from_millis(30)),
        ))
        .expect("set flow admission");

    let mut exec = SingleThreadedExecutor::new(64);
    exec.add_node(sub_node.clone());

    let publisher = pub_node.create_publisher(&topic).await.expect("publisher");
    tokio::time::sleep(Duration::from_millis(300)).await; // discovery

    // Determinism: deadlines are stamped at RX-callback
    // ARRIVAL, not at publish — so the test only sleeps past the deadline
    // AFTER observing the burst enqueued (queued_events), and never spins
    // while a sample could still be fresh. Publish stays fire-and-forget
    // (a pre-discovery burst can be lost), so un-observed bursts retry.
    let admission = exec.queue().admission_reader();
    let overall = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        for _ in 0..3 {
            publisher.publish(vec![1u8]).expect("publish");
        }
        // Wait for the burst to be observably ENQUEUED (deadline stamped).
        let enqueue_wait = std::time::Instant::now() + Duration::from_millis(500);
        while admission.snapshot().queued_events < 3 && std::time::Instant::now() < enqueue_wait {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if admission.snapshot().queued_events >= 3 {
            // All queued samples' 30ms deadlines expire BEFORE any dispatch.
            tokio::time::sleep(Duration::from_millis(120)).await;
            for _ in 0..5 {
                let _ = exec.spin_once(Duration::from_millis(20)).await;
            }
            if exec.dropped_expired() >= 3 {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < overall,
            "the deadline shed must become observable; dropped={} queued={}",
            exec.dropped_expired(),
            admission.snapshot().queued_events
        );
    }
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        0,
        "expired events must be dropped at dequeue, not delivered"
    );
    assert!(
        subscriber.dropped_for_deadline() >= 3,
        "B7: the per-subscriber deadline-drop counter attributes the shed"
    );

    pub_node.close().await.expect("close pub");
    sub_node.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pull_mode_rejects_flow_capability_setters() {
    // The pull-mode rejection must live in CORE (Rust SDK
    // callers reach these setters directly), not just the
    // C-ABI — one semantic, both surfaces.
    let node = crate::node::Node::new(format!("pull_caps_{}", std::process::id()), "default")
        .await
        .expect("node");
    let topic = format!("pull_caps_topic_{}", std::process::id());
    let subscriber = node
        .create_subscriber_pull(&topic, 4)
        .await
        .expect("pull subscriber");

    let err = subscriber
        .set_flow_priority(Some(crate::event_queue::EventPriority::High))
        .expect_err("pull mode must reject the priority override");
    assert!(err.to_string().contains("pull"), "got: {err}");
    let err = subscriber
        .set_flow_admission(crate::subscriber::FlowAdmission::gated(
            1,
            Some(Duration::from_millis(5)),
        ))
        .expect_err("pull mode must reject the Flow gate");
    assert!(err.to_string().contains("pull"), "got: {err}");
    // The getter reports the immutable ring capacity, not the (unchanged)
    // gate atomic.
    assert_eq!(subscriber.receive_depth(), 4);

    node.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_opts_default_inherits_the_parent_priority() {
    // A child created with plain defaults must NOT be
    // silently demoted from its parent's priority.
    let parent = Scope::new_root(ScopeOpts::named("prio-parent").priority(EventPriority::High));
    let child = parent.child(ScopeOpts::named("prio-child"));
    assert_eq!(child.priority(), EventPriority::High, "None = inherit");

    // An explicit request still clamps (never raises).
    let clamped = child.child(ScopeOpts::named("wants-rt").priority(EventPriority::Realtime));
    assert_eq!(clamped.priority(), EventPriority::High);
    let lowered = child.child(ScopeOpts::named("goes-low").priority(EventPriority::Low));
    assert_eq!(lowered.priority(), EventPriority::Low);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_handle_is_a_future() {
    // `handle.await` is the tokio JoinHandle idiom.
    let scope = Scope::new_root(ScopeOpts::named("await-handle"));
    let handle = scope.spawn(async { 21 * 2 });
    let value = handle.await.expect("join via await");
    assert_eq!(value, 42);
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_admission_struct_round_trips() {
    let node = crate::node::Node::new(format!("adm_{}", std::process::id()), "default")
        .await
        .expect("node");
    let sub = node
        .create_subscriber("adm_topic", |_p: &crate::ZBytes, _c| {})
        .await
        .expect("sub");
    sub.set_flow_admission(crate::subscriber::FlowAdmission::default())
        .expect("default admission is valid (ungated)");
    assert_eq!(sub.receive_depth(), 0);
    sub.set_flow_admission(crate::subscriber::FlowAdmission::gated(
        3,
        Some(Duration::from_millis(10)),
    ))
    .expect("gated admission");
    assert_eq!(sub.receive_depth(), 3, "live cap reflects the setter");
    node.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_cancels_then_drains() {
    let scope = Scope::new_root(ScopeOpts::named("shutdown-pair"));
    // Flow: the child parks forever; shutdown's cancel resolves it
    // AT the dispatch boundary — its outcome carries the exact reason.
    let handle = scope.spawn(async {
        std::future::pending::<()>().await;
    });
    scope.shutdown(CancelReason::Shutdown).await;
    assert_eq!(scope.live_children(), 0, "shutdown awaited the drain");
    let stopped = handle.await.expect_err("resolved by the boundary");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::Shutdown),
        "child observed Shutdown via its outcome"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_until_cancelled_returns_none_on_cancel() {
    let scope = Scope::new_root(ScopeOpts::named("ruc"));
    scope.cancel(CancelReason::Explicit);
    let out = scope
        .run_until_cancelled(async { tokio::time::sleep(Duration::from_secs(60)).await })
        .await;
    assert!(out.is_none(), "pre-cancelled scope wins the race");

    let live = Scope::new_root(ScopeOpts::named("ruc-live"));
    let out = live.run_until_cancelled(async { 7 }).await;
    assert_eq!(out, Some(7), "completed future wins on a live scope");
    live.close().await;
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_in_refuses_on_cancelled_scope() {
    let scope = Scope::new_root(ScopeOpts::named("offload-gate"));
    scope.cancel(CancelReason::Explicit);
    let refused = crate::flow::offload_in(&scope, || 1 + 1).await;
    assert_eq!(refused, Err(Stopped::cancelled(CancelReason::Explicit)));
    let live = Scope::new_root(ScopeOpts::named("offload-live"));
    assert_eq!(crate::flow::offload_in(&live, || 21 * 2).await, Ok(42));
    live.close().await;
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_flat_collapses_the_nested_result() {
    let scope = Scope::new_root(ScopeOpts::named("flat"));
    let handle = scope.spawn_with(|scope| async move {
        scope.checkpoint()?;
        Ok::<_, Stopped>(5)
    });
    assert_eq!(handle.join_flat().await, Ok(5));
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_all_handles_without_close_is_survivable() {
    // Debug drop-check smoke: dropping every user handle with a live
    // child emits the debug warning (visually verified via logs) and must
    // never panic or wedge; the orphan still runs to completion.
    let ran = Arc::new(AtomicBool::new(false));
    {
        let scope = Scope::new_root(ScopeOpts::named("orphaned"));
        let ran_c = Arc::clone(&ran);
        let handle = scope.spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            ran_c.store(true, Ordering::SeqCst);
        });
        drop(handle); // user handle #2 (holds its own Scope clone)
        drop(scope); // last user handle -> debug warning fires here
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(ran.load(Ordering::SeqCst), "orphan still completed");
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_warning_decision_is_assertable() {
    // Review (delta): assert the drop-warning LOGIC, not just survive it.
    let scope = Scope::new_root(ScopeOpts::named("warn-logic"));
    let handle = scope.spawn_with(|scope| async move {
        scope.cancelled().await;
    });

    // Held handles keep user_handles above 1 (original + TaskHandle's
    // parent clone; the spawn_with body now owns a clone of its own
    // CHILD scope, which counts on the child's ledger,
    // not this one): a drop of the ORIGINAL is not the last, so no
    // warning path is taken.
    assert_eq!(scope.inner.debug_user_handles(), 2);
    // With a live child and no close(), a hypothetical last drop WOULD warn:
    assert_eq!(scope.inner.would_orphan_on_last_drop(), Some(1));

    // After shutdown, the decision flips to None (closing + drained); the
    // parent handle count is unchanged (the finished body's clone was on
    // the child's ledger).
    scope.shutdown(CancelReason::Shutdown).await;
    assert_eq!(scope.inner.would_orphan_on_last_drop(), None);
    assert_eq!(scope.inner.debug_user_handles(), 2);
    let _ = handle.await;
    assert_eq!(
        scope.inner.debug_user_handles(),
        1,
        "handle released its clone"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_returns_promptly_when_children_exit() {
    // Regression: the watchdog must not change normal-close behavior.
    let scope = Scope::new_root(ScopeOpts::named("prompt-close"));
    let _h = scope.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    let started = std::time::Instant::now();
    scope.shutdown(CancelReason::Shutdown).await;
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "close must not wait for a watchdog tick when children exit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_close_is_detected_from_inside_a_child() {
    // The WARN path is exercised (visually verified via logs); the
    // assertable part is the task-local chain — including grandchildren.
    let scope = Scope::new_root(ScopeOpts::named("self-close"));
    let inner_addr = std::sync::Arc::as_ptr(&scope.inner) as usize;
    let h = scope.spawn_with(move |scope| async move {
        // Direct child sees its parent on the chain.
        let direct = crate::flow::scope::scope_stack_contains(inner_addr);
        // Grandchild sees the ANCESTOR too.
        let g = scope.spawn(async move { crate::flow::scope::scope_stack_contains(inner_addr) });
        let nested = g.await.unwrap_or(false);
        (direct, nested)
    });
    let (direct, nested) = h.await.expect("join");
    assert!(direct, "direct child on the chain");
    assert!(nested, "grandchild inherits the ancestor chain");
    scope.shutdown(CancelReason::Shutdown).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_trace_paths_run_under_both_gate_states() {
    // The aorta.flow instant/counter paths execute cleanly
    // with tracing ON (gate active → instants emitted) and stay one-branch
    // no-ops with tracing OFF. (Serialized via the shared trace-test lock;
    // master switch restored.)
    let prev = crate::trace::trace_enabled();
    let flow_scope = crate::trace::intern_scope("aorta.flow");
    let prev_threshold = crate::trace::scope_threshold(flow_scope);
    crate::trace::set_trace_enabled(true);
    // Force the aorta.flow gate ON so the ON leg is ASSERTABLE
    // (relying on the gate default made this test pass unchanged if
    // instrumentation were deleted).
    crate::trace::set_scope_enabled(flow_scope, true);
    assert!(
        crate::trace::flow_trace_active(),
        "master switch + open gate => flow tracing ACTIVE"
    );
    let scope = Scope::new_root(ScopeOpts::named("traced-scope"));
    let h = scope.spawn(async { 1 });
    let _ = h.await;
    scope.cancel(CancelReason::Explicit);
    scope.close().await;
    crate::trace::set_trace_enabled(false);
    assert!(!crate::trace::flow_trace_active());
    let quiet = Scope::new_root(ScopeOpts::named("untraced-scope"));
    quiet.cancel(CancelReason::Explicit);
    quiet.close().await;
    crate::trace::set_scope_threshold(flow_scope, prev_threshold);
    crate::trace::set_trace_enabled(prev);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_distinguishes_window_expiry_semantics() {
    // (run_for deleted; scope.timeout replaces it): a window expiry
    // resolves Stopped(DeadlineExceeded) WITHOUT cancelling the scope; a
    // completed future passes through; scope cancel resolves Stopped.
    let scope = Scope::new_root(ScopeOpts::named("timeout-sem"));
    let out = scope
        .timeout(Duration::from_millis(10), async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            7
        })
        .await;
    let stopped = out.expect_err("window expired");
    assert!(stopped.is_deadline());
    assert!(
        !scope.is_cancelled(),
        "a window expiry is LOCAL — it must not cancel the scope"
    );

    let out = scope.timeout(Duration::from_millis(50), async { 7 }).await;
    assert_eq!(out.expect("completed inside the window"), 7);

    scope.cancel(CancelReason::Explicit);
    let out = scope
        .timeout(Duration::from_secs(60), async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        })
        .await;
    assert!(out.expect_err("scope cancel wins").is_cancel());
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_warning_machinery_is_release_mode_too() {
    // The user_handles counter is no longer cfg(debug_assertions) —
    // this test compiles and passes in EVERY profile.
    let scope = Scope::new_root(ScopeOpts::named("release-orphan"));
    assert_eq!(scope.inner.debug_user_handles(), 1);
    let clone = scope.clone();
    assert_eq!(scope.inner.debug_user_handles(), 2);
    drop(clone);
    assert_eq!(scope.inner.debug_user_handles(), 1);
    assert_eq!(scope.inner.would_orphan_on_last_drop(), None, "no children");
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn introspect_dump_reflects_live_scopes() {
    // Names/counts/reasons visible once enabled; disabled = no
    // registration (asserted structurally: scopes made before enable()
    // never appear).
    let before = Scope::new_root(ScopeOpts::named("made-before-enable"));
    crate::flow::introspect::enable();
    let scope = Scope::new_root(ScopeOpts::named("introspected"));
    let _h = scope.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    let dump = crate::flow::introspect::dump();
    assert!(
        dump.iter()
            .any(|s| s.name == "introspected" && s.live_children == 1),
        "live scope visible with its child count: {dump:?}"
    );
    assert!(
        !dump.iter().any(|s| s.name == "made-before-enable"),
        "pre-enable scopes were never registered"
    );
    scope.shutdown(CancelReason::Shutdown).await;
    let dump = crate::flow::introspect::dump();
    if let Some(info) = dump.iter().find(|s| s.name == "introspected") {
        assert_eq!(info.live_children, 0);
        assert!(info.closing);
        assert!(matches!(info.cancelled, Some(CancelReason::Shutdown)));
    }
    before.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_in_submits_at_the_scope_priority() {
    // Wiring: offload_in passes scope.priority() into the pool.
    // Class ORDERING is the WRR unit test's job — the pool is process-
    // global and fair (not strict), so an e2e ordering assert would race
    // the rest of the suite. Submission happens on the calling task's
    // thread (before the first await), so the thread-local seam is stable.
    let hi = Scope::new_root(ScopeOpts::named("off-hi").priority(EventPriority::High));
    let lo = Scope::new_root(ScopeOpts::named("off-lo").priority(EventPriority::Low));
    crate::flow::offload_in(&hi, || ()).await.expect("hi");
    assert_eq!(
        crate::flow::offload::LAST_SUBMITTED_PRIORITY.with(|c| c.get()),
        Some(EventPriority::High)
    );
    crate::flow::offload_in(&lo, || ()).await.expect("lo");
    assert_eq!(
        crate::flow::offload::LAST_SUBMITTED_PRIORITY.with(|c| c.get()),
        Some(EventPriority::Low)
    );
    crate::flow::offload(|| ()).await;
    assert_eq!(
        crate::flow::offload::LAST_SUBMITTED_PRIORITY.with(|c| c.get()),
        Some(EventPriority::Normal),
        "plain offload stays Normal"
    );
    hi.close().await;
    lo.close().await;
}

// ---- Subtree accounting in close() ------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_close_waits_for_grandchild_scope_tasks() {
    // A task spawned on a CHILD scope must not outlive the parent's
    // close() — the no-orphan guarantee covers the whole scope tree.
    let parent = Scope::new_root(ScopeOpts::named("p1-parent"));
    let child = parent.child(ScopeOpts::named("p1-child"));
    let grandchild = child.child(ScopeOpts::named("p1-grandchild"));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_probe = Arc::clone(&done);
    let _h = grandchild.spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        done_probe.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    parent.close().await;
    assert!(
        done.load(std::sync::atomic::Ordering::SeqCst),
        "parent.close() returned before a grandchild-scope task finished"
    );
    child.close().await;
    grandchild.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shielded_checkpoint_reports_the_recorded_reason_not_deadline() {
    // Regression: the shielded checkpoint branch
    // reported DeadlineExceeded unconditionally on budget expiry,
    // discarding the token's ACTUAL recorded reason (e.g. an unrelated
    // Shutdown that landed first) — corrupting `Stopped::is_deadline()`
    // branching. Same rule as DispatchBoundary::poll().
    // The branch is only reachable when the budget expires DURING a
    // shielded poll (any later poll is short-circuited at the dispatch
    // boundary, budget pierces shields there): the body blocks in-poll
    // past the deadline while the test task records Shutdown.
    let scope =
        Scope::new_root(ScopeOpts::named("shield-reason").budget(Duration::from_millis(40)));
    let in_shield = Arc::new(AtomicBool::new(false));
    let in_shield_probe = Arc::clone(&in_shield);
    let handle = scope.spawn_with(move |scope| async move {
        let body_scope = scope.clone();
        scope
            .shield(async move {
                let scope = body_scope;
                // Guard the test's own premise: without a live shield the
                // UNSHIELDED branch would return the same reason and this
                // test would stop discriminating.
                assert!(crate::flow::ambient::poll_shielded(), "shield is live");
                in_shield_probe.store(true, Ordering::SeqCst);
                // Block INSIDE this poll until the budget is expired (the
                // test task records Shutdown meanwhile).
                std::thread::sleep(Duration::from_millis(90));
                scope.checkpoint()
            })
            .await
    });
    while !in_shield.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    scope.cancel(CancelReason::Shutdown); // recorded BEFORE the checkpoint runs
    let out = handle.join().await;
    assert!(
        matches!(
            out,
            Ok(Err(Stopped(StopReason::Cancelled(CancelReason::Shutdown))))
        ),
        "shielded checkpoint must surface the token's recorded reason, got {out:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_intermediate_scope_handle_keeps_subtree_accounted() {
    // The canonical nested-.child() idiom —
    // a pass-through stage scope whose handle goes out of scope without
    // close() — must NOT silently detach the live subtree from ancestor
    // accounting. The grandchild task must still block root.close().
    let root = Scope::new_root(ScopeOpts::named("p0-root"));
    let done = Arc::new(AtomicBool::new(false));
    {
        let stage = root.child(ScopeOpts::named("p0-stage"));
        let real = stage.child(ScopeOpts::named("p0-real"));
        let done_probe = Arc::clone(&done);
        let _h = real.spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            done_probe.store(true, Ordering::SeqCst);
        });
        // `stage`, `real`, and the handle all drop here without close().
    }
    root.close().await;
    assert!(
        done.load(Ordering::SeqCst),
        "root.close() returned while a grandchild task was still running \
         (subtree silently orphaned by an intermediate handle drop)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_intermediate_scope_handle_keeps_subtree_cancellable() {
    // P0 companion: shutdown_with_grace must still reach (cancel + drain)
    // a grandchild whose intermediate scope handle was dropped — and its
    // returned 0 must mean "clean drain", not "subtree silently pruned".
    let root = Scope::new_root(ScopeOpts::named("p0c-root"));
    let entered = Arc::new(AtomicBool::new(false));
    let handle;
    {
        let stage = root.child(ScopeOpts::named("p0c-stage"));
        let real = stage.child(ScopeOpts::named("p0c-real"));
        let entered_probe = Arc::clone(&entered);
        handle = real.spawn_with(|scope| async move {
            entered_probe.store(true, Ordering::SeqCst);
            // Parks forever unless cancellation actually reaches it.
            let _ = scope
                .run_until_cancelled(std::future::pending::<()>())
                .await;
        });
        // `stage` and `real` drop here without close().
    }
    while !entered.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let detached = root
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_secs(2))
        .await;
    assert_eq!(
        detached, 0,
        "grace expired: cancel never cascaded to the orphaned grandchild"
    );
    let joined = tokio::time::timeout(Duration::from_secs(3), handle).await;
    assert!(
        matches!(joined, Ok(Err(Stopped(StopReason::Cancelled(_))))),
        "grandchild task never resolved after shutdown — cancel did not \
         reach it through the dropped intermediate scope: {joined:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descendants_refuse_spawns_once_parent_close_begins() {
    // The closing bar cascades: after parent.close() returns, a child
    // scope may NOT accept new work.
    let parent = Scope::new_root(ScopeOpts::named("bar-parent"));
    let child = parent.child(ScopeOpts::named("bar-child"));
    parent.close().await;
    assert!(child.is_closing(), "closing bar cascaded to the child");
    let h = child.spawn(async { 1 });
    assert!(
        matches!(h.await, Err(Stopped(StopReason::Cancelled(_)))),
        "spawn on a closed-ancestor scope is refused"
    );
    // A child CREATED after the parent closed is born closing.
    let late = parent.child(ScopeOpts::named("bar-late"));
    assert!(late.is_closing(), "late child born closing");
    let h = late.spawn(async { 2 });
    assert!(matches!(h.await, Err(Stopped(StopReason::Cancelled(_)))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_child_scopes_do_not_wedge_parent_close() {
    // Dead weak entries prune; an idle-and-dropped child never blocks.
    let parent = Scope::new_root(ScopeOpts::named("prune-parent"));
    for i in 0..8 {
        let child = parent.child(ScopeOpts::named("prune-child"));
        let h = child.spawn(async move { i });
        let _ = h.await;
        child.close().await;
        drop(child);
    }
    let started = std::time::Instant::now();
    parent.close().await;
    assert!(started.elapsed() < Duration::from_secs(2));
}

// ---- Scope-tree reclamation ------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn churned_child_scopes_do_not_grow_the_parent_registry() {
    // The canonical per-request idiom (root.child → work → shutdown → drop,
    // per flow_demo.rs) must leave the long-lived parent's registry at a
    // small constant, NOT proportional to churn count.
    let root = Scope::new_root(ScopeOpts::named("arch03-root"));
    const N: usize = 512;
    for i in 0..N {
        let request = root.child(ScopeOpts::named("arch03-request"));
        let h = request.spawn(async move { i });
        let _ = h.await;
        request.shutdown(CancelReason::Shutdown).await;
        drop(request);
    }
    let len = root.inner.debug_scope_children_len();
    assert!(
        len <= 16,
        "parent registry must stay bounded under churn: {len} entries after {N} children"
    );
    root.close().await;
}

// ---- Bounded root-scope drain in Node::close ------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_close_drains_cooperative_root_scope_children() {
    // A cooperative child unwinds INSIDE the drain window — before the
    // session dies — and node close stays prompt.
    let node = crate::node::Node::new(format!("arch11-coop-{}", std::process::id()), "default")
        .await
        .expect("node");
    let scope = node.root_scope();
    // Flow: the cooperative child resolves at the dispatch boundary
    // inside the drain window; its outcome (not an in-body probe, which
    // the boundary preempts) proves it unwound before session teardown.
    let h = scope.spawn(async {
        std::future::pending::<()>().await;
    });
    let started = std::time::Instant::now();
    node.close().await.expect("close");
    let stopped = h.await.expect_err("resolved inside the drain window");
    assert!(stopped.is_cancel(), "unwound via Shutdown: {stopped:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_close_bounds_the_drain_for_defiant_children() {
    // A defiant child cannot wedge node close: the 500ms window elapses,
    // the WARN fires (visually verified), close proceeds.
    let node = crate::node::Node::new(format!("arch11-defiant-{}", std::process::id()), "default")
        .await
        .expect("node");
    let scope = node.root_scope();
    // Flow: a `loop {{ sleep.await }}` child is no longer able to
    // defy — the dispatch boundary resolves it at its next wake. A truly
    // defiant child is a MID-POLL staller (never yields); the release
    // flag lets it exit after the assertion so the runtime drops cleanly.
    static RELEASED: AtomicBool = AtomicBool::new(false);
    let entered = Arc::new(AtomicBool::new(false));
    let entered_probe = Arc::clone(&entered);
    let _h = scope.spawn(async move {
        entered_probe.store(true, Ordering::SeqCst);
        loop {
            std::thread::sleep(Duration::from_millis(20));
            if RELEASED.load(Ordering::SeqCst) {
                return;
            }
        }
    });
    // The child must be MID-POLL before close() cancels the scope: on a
    // loaded runner the spawned task can still be queued (never polled)
    // when the cancel lands, and the dispatch boundary then resolves it
    // at entry — nothing defies, the drain returns in ~ms, and the
    // >=450ms assertion below fails (observed on GH-hosted CI).
    while !entered.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let started = std::time::Instant::now();
    let close_result = node.close().await;
    RELEASED.store(true, Ordering::SeqCst);
    close_result.expect("close");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(450),
        "the drain window was honored: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "bounded: close must proceed past defiant children: {elapsed:?}"
    );
}

// ---- handle-boundary reason fidelity ----------------------------------

/// Pins the EXACT reason/payload through `handle.await` — a `poll_join`
/// regression that collapses flavors or drops the panic message fails here,
/// not just in the direct `Stopped` constructor tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_await_preserves_exact_reason_and_payload() {
    // Panic payload, verbatim, through the handle boundary.
    let scope = Scope::new_root(ScopeOpts::default().supervised());
    let h = scope.spawn(async {
        panic!("wp-r1 payload 0xBEEF");
    });
    let stopped = h.await.expect_err("panicked child");
    match stopped.reason() {
        StopReason::Fault { message: msg, .. } => {
            assert!(msg.contains("wp-r1 payload 0xBEEF"), "payload lost: {msg}")
        }
        other => panic!("expected fault, got {other:?}"),
    }
    scope.close().await;

    // Exact cancel flavor (Shutdown, not just Cancelled(_)) through the
    // abort path: the pre-Flow runtime has no ambient cancellation, so the
    // child is hard-stopped and poll_join must surface the SCOPE's exact
    // reason, not a generic Explicit.
    let scope = Scope::new_root(ScopeOpts::default());
    let h = scope.spawn(async {
        std::future::pending::<()>().await;
    });
    scope.cancel(CancelReason::Shutdown);
    h.abort();
    let stopped = h.await.expect_err("cancelled child");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::Shutdown),
        "flavor must survive the handle boundary"
    );
    scope.close().await;
}

// ---- ambient cancellation at the dispatch boundary -------------------

/// THE core scenario: a task sleeping in a third-party await must NOT run
/// its post-await side effect once the scope is cancelled — the poll
/// wrapper short-circuits at the dispatch boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_short_circuits_at_dispatch_boundary() {
    let side_effect = Arc::new(AtomicBool::new(false));
    let scope = Scope::new_root(ScopeOpts::named("ambient"));
    let flag = side_effect.clone();
    let h = scope.spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        flag.store(true, Ordering::SeqCst); // must never run
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    scope.cancel(CancelReason::Shutdown);
    let stopped = tokio::time::timeout(Duration::from_secs(2), h)
        .await
        .expect("handle must resolve promptly on cancel")
        .expect_err("cancelled at the boundary");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::Shutdown)
    );
    assert!(
        !side_effect.load(Ordering::SeqCst),
        "post-await side effect ran"
    );
    scope.close().await;
}

/// A pending-forever child resolves COOPERATIVELY on cancel — no abort().
/// (This exact shape hung before Flow: no ambient cancellation.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_child_resolves_on_cancel_without_abort() {
    let scope = Scope::new_root(ScopeOpts::default());
    let h = scope.spawn(async {
        std::future::pending::<()>().await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    scope.cancel(CancelReason::Explicit);
    let stopped = tokio::time::timeout(Duration::from_secs(2), h)
        .await
        .expect("cancel must WAKE parked children, not just gate polls")
        .expect_err("stopped");
    assert!(stopped.is_cancel());
    scope.close().await;
}

/// Budget expiry reaches a parked child through the same boundary: the
/// wrapper observes the expired budget, fires the token, short-circuits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_expiry_short_circuits_parked_child() {
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::from_millis(30)),
        ..Default::default()
    });
    let h = scope.spawn(async {
        std::future::pending::<()>().await;
    });
    let stopped = tokio::time::timeout(Duration::from_secs(2), h)
        .await
        .expect("expiry must resolve the child")
        .expect_err("stopped");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::DeadlineExceeded)
    );
    scope.close().await;
}

/// checkpoint() and the wrapper agree (same effective view).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_agrees_with_wrapper_verdict() {
    let scope = Scope::new_root(ScopeOpts::default());
    let h = scope.spawn_with(|scope| async move {
        loop {
            if scope.checkpoint().is_err() {
                return "observed";
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(15)).await;
    scope.cancel(CancelReason::Explicit);
    // Either the checkpoint or the boundary wins — both must yield a
    // cancel verdict, never a hang and never Ok.
    let outcome = tokio::time::timeout(Duration::from_secs(2), h)
        .await
        .expect("resolved");
    match outcome {
        Ok(s) => assert_eq!(s, "observed"),
        Err(stopped) => assert!(stopped.is_cancel()),
    }
    scope.close().await;
}

// ---- stall-WARN --------------------------------------------------------

/// A mid-poll staller (never yields) triggers exactly one WARN after the
/// threshold; the counter is a cfg(test) hook. Relies on the suite's
/// --test-threads=1 convention (global counter, delta-asserted).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stall_warn_fires_once_for_mid_poll_staller() {
    use super::scope::STALL_WARN_NAMES;
    static RELEASE: AtomicBool = AtomicBool::new(false);
    let count = |name: &str| {
        STALL_WARN_NAMES
            .lock()
            .unwrap()
            .iter()
            .filter(|n| **n == name)
            .count()
    };
    let scope = Scope::new_root(ScopeOpts::named("wp-r2-staller-unique"));
    let _h = scope.spawn(async {
        loop {
            std::thread::sleep(Duration::from_millis(10));
            if RELEASE.load(Ordering::SeqCst) {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await; // child mid-poll
    scope.cancel(CancelReason::Explicit);
    tokio::time::sleep(Duration::from_millis(400)).await; // > threshold
    assert_eq!(
        count("wp-r2-staller-unique"),
        1,
        "exactly one WARN for the stall"
    );
    RELEASE.store(true, Ordering::SeqCst);
    scope.close().await;
    assert_eq!(count("wp-r2-staller-unique"), 1, "one-shot latch held");
}

/// Prompt children: cancel resolves them at the boundary well inside the
/// threshold — no WARN. And no watchdog exists before any cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_stall_warn_for_prompt_children() {
    use super::scope::STALL_WARN_NAMES;
    let scope = Scope::new_root(ScopeOpts::named("wp-r2-prompt-unique"));
    let h = scope.spawn(async {
        std::future::pending::<()>().await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    scope.cancel(CancelReason::Explicit);
    let _ = h.await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mine = STALL_WARN_NAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|n| **n == "wp-r2-prompt-unique")
        .count();
    assert_eq!(mine, 0, "prompt boundary resolution must not WARN");
    scope.close().await;
}

// ---- shield-drop recheck + checkpoint-armed watchdog ----

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A shield that drops INSIDE a poll, with the child then
/// parking on a NEW await, must not strand a spent cancel wake — the
/// boundary re-observes after the child's poll.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shield_drop_mid_poll_does_not_strand_cancel() {
    use std::sync::atomic::AtomicU32;
    static POLLS: AtomicU32 = AtomicU32::new(0);
    let entered = Arc::new(tokio::sync::Notify::new());
    let entered_tx = entered.clone();

    // Hand-rolled "shield lifecycle" future: poll 1 enters a live shield
    // and parks (no self-wake — the CANCEL provides the next wake);
    // poll 2 exits the shield and parks forever with NO waker. Only the
    // boundary's post-poll recheck can resolve the task after that.
    struct ShieldThenPark {
        entered: Arc<tokio::sync::Notify>,
        shielded: bool,
    }
    impl Future for ShieldThenPark {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            match POLLS.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert!(crate::flow::ambient::enter_live_shield());
                    self.shielded = true;
                    self.entered.notify_one();
                    Poll::Pending // parked; the cancel wake re-polls us
                }
                _ => {
                    if self.shielded {
                        crate::flow::ambient::exit_live_shield();
                        self.shielded = false;
                    }
                    Poll::Pending // parked FOREVER, no waker registered
                }
            }
        }
    }

    let scope = Scope::new_root(ScopeOpts::named("shield-drop-recheck"));
    let h = scope.spawn(ShieldThenPark {
        entered: entered_tx,
        shielded: false,
    });
    entered.notified().await; // task is parked inside the live shield
    scope.cancel(CancelReason::Explicit); // wake #2: boundary skips (shielded)
    let stopped = tokio::time::timeout(Duration::from_secs(2), h)
        .await
        .expect("post-poll recheck must resolve the task, not strand it")
        .expect_err("stopped");
    assert!(stopped.is_cancel());
    scope.close().await;
}

/// A checkpoint-fired deadline cancel arms the stall
/// watchdog — a sibling mid-poll staller gets reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_fired_expiry_arms_stall_warn() {
    use super::scope::STALL_WARN_NAMES;
    static RELEASE2: AtomicBool = AtomicBool::new(false);
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::from_millis(20)),
        ..ScopeOpts::named("wp-r2-ckpt-arm-unique")
    });
    let _staller = scope.spawn(async {
        loop {
            std::thread::sleep(Duration::from_millis(10));
            if RELEASE2.load(Ordering::SeqCst) {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(40)).await; // budget expired
    let _ = scope.checkpoint(); // fires DeadlineExceeded + arms watchdog
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mine = STALL_WARN_NAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|n| **n == "wp-r2-ckpt-arm-unique")
        .count();
    assert_eq!(mine, 1, "checkpoint-fired cancel armed the watchdog");
    RELEASE2.store(true, Ordering::SeqCst);
    scope.close().await;
}

// ---- in-task lazy combinators ----------------------------------------

/// Borrowing legality: the constituents borrow LOCALS across .await —
/// impossible with spawn-based combinators ('static bound).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_borrows_locals_zero_spawns() {
    let scope = Scope::new_root(ScopeOpts::named("join-borrow"));
    let data = [1u32, 2, 3];
    let name = String::from("sensor");
    let before = scope.live_children();
    let (sum, tag) = scope
        .join(
            async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                data.iter().sum::<u32>() // borrows `data`
            },
            async { format!("{name}-ok") }, // borrows `name`
        )
        .await
        .expect("both complete");
    assert_eq!(sum, 6);
    assert_eq!(tag, "sensor-ok");
    assert_eq!(scope.live_children(), before, "ZERO spawns");
    assert_eq!(data.len(), 3, "locals still usable after");
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn race_drops_the_loser_in_place() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let scope = Scope::new_root(ScopeOpts::named("race-loser"));
    let loser_dropped = Arc::new(AtomicBool::new(false));
    let guard = DropFlag(loser_dropped.clone());
    let won = scope
        .race(
            async {
                tokio::time::sleep(Duration::from_millis(2)).await;
                "fast"
            },
            async {
                let _g = guard; // dropped when the loser future drops
                tokio::time::sleep(Duration::from_secs(60)).await;
                "slow"
            },
        )
        .await
        .expect("fast side wins");
    assert_eq!(won, "fast");
    assert!(
        loser_dropped.load(Ordering::SeqCst),
        "loser torn down in place (Rust drop = cancel)"
    );
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn race2_reports_which_side_won() {
    use crate::flow::Either;
    let scope = Scope::new_root(ScopeOpts::named("race2"));
    let out = scope
        .race2(
            async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                1u32
            },
            async { "right side" },
        )
        .await
        .expect("right wins");
    assert_eq!(out, Either::Right("right side"));
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_join_fail_fasts_on_first_stopped() {
    let scope = Scope::new_root(ScopeOpts::named("try-join"));
    let slow_side_dropped = Arc::new(AtomicBool::new(false));
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let guard = DropFlag(slow_side_dropped.clone());
    let started = std::time::Instant::now();
    let out: crate::flow::Outcome<(u32, u32)> = scope
        .try_join(
            async {
                tokio::time::sleep(Duration::from_millis(2)).await;
                Err(Stopped::cancelled(CancelReason::Explicit))
            },
            async {
                let _g = guard;
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(7)
            },
        )
        .await;
    assert!(out.expect_err("fail-fast").is_cancel());
    assert!(slow_side_dropped.load(Ordering::SeqCst), "sibling dropped");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "fail-fast is IMMEDIATE — the 60s sibling must not run its course"
    );
    scope.close().await;
}

/// A cancel mid-join WAKES the combinator (waker registration) — both
/// constituents dropped, single Stopped with the scope reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_mid_join_wakes_and_stops() {
    let scope = Scope::new_root(ScopeOpts::named("join-cancel"));
    let scope2 = scope.clone();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        scope2.cancel(CancelReason::Shutdown);
    });
    let out = scope
        .join(
            async {
                std::future::pending::<()>().await;
            },
            async {
                std::future::pending::<()>().await;
            },
        )
        .await;
    let stopped = out.expect_err("cancelled mid-join");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::Shutdown)
    );
    canceller.await.unwrap();
    scope.close().await;
}

/// Nested timeouts: the ambient cap composes by MIN (never widens) —
/// observable via ambient::budget_cap() inside the innermost poll.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_caps_narrow_and_never_widen() {
    // The ambient cap lives in the flow task_local — combinators must run
    // INSIDE a flow-spawned task to propagate it (plain tasks degrade to
    // local-race semantics).
    let scope = Scope::new_root(ScopeOpts::named("timeout-nest"));
    let task_scope = scope.clone();
    let observed = scope
        .spawn(async move {
            let inner = task_scope.clone();
            task_scope
                .timeout(Duration::from_millis(50), async {
                    // Inner WIDER window must not widen the effective cap.
                    inner
                        .timeout(Duration::from_secs(60), async {
                            crate::flow::ambient::budget_cap()
                        })
                        .await
                        .expect("inner completes")
                })
                .await
                .expect("outer completes")
        })
        .await
        .expect("task completes")
        .expect("a cap is visible");
    let remaining = observed.saturating_duration_since(std::time::Instant::now());
    assert!(
        remaining <= Duration::from_millis(50),
        "effective cap is the OUTER (narrower) window: {remaining:?}"
    );
    scope.close().await;
}

/// The cap is poll-scoped: a task SPAWNED inside a timeout window gets
/// the scope budget, not the window cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_cap_does_not_cross_spawn() {
    // Inside a flow task (ambient seeded — a plain test task would make
    // BudgetCapGuard a no-op and prove nothing).
    let scope = Scope::new_root(ScopeOpts::named("cap-spawn"));
    let task_scope = scope.clone();
    let (parent_cap, child_cap) = scope
        .spawn(async move {
            let inner = task_scope.clone();
            task_scope
                .timeout(Duration::from_millis(50), async move {
                    let parent_cap = crate::flow::ambient::budget_cap();
                    let h = inner.spawn(async { crate::flow::ambient::budget_cap() });
                    (parent_cap, h.await.expect("child ran"))
                })
                .await
                .expect("completed inside the window")
        })
        .await
        .expect("task completed");
    assert!(
        parent_cap.is_some(),
        "the ambient cap IS visible inside the window (same task)"
    );
    assert_eq!(child_cap, None, "spawned child sees NO window cap");
    scope.close().await;
}

/// checkpoint() inside a timeout window: the window alone does not make
/// checkpoint fail (it is scope-level), and a live scope stays Ok.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_inside_timeout_window_stays_scope_level() {
    let scope = Scope::new_root(ScopeOpts::named("ckpt-window"));
    let inner_scope = scope.clone();
    let out = scope
        .timeout(Duration::from_millis(50), async move {
            inner_scope.checkpoint().is_ok()
        })
        .await
        .expect("completed");
    assert!(out, "window does not poison scope-level checkpoint");
    scope.close().await;
}

// ---- budget-bound timeout + arity + cap-spawn pin ---------------------

/// The timer is bounded by the SCOPE BUDGET, not just the
/// window — a plain-tokio caller (no ambient, no boundary timer) with a
/// short budget and a long window must wake AT the budget, observe the
/// expiry (scope cancelled), and resolve Stopped(Deadline).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_bounded_by_scope_budget() {
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::from_millis(20)),
        ..ScopeOpts::named("timeout-budget-bound")
    });
    let started = std::time::Instant::now();
    let out = scope
        .timeout(Duration::from_secs(60), async {
            std::future::pending::<()>().await;
        })
        .await;
    let stopped = out.expect_err("budget bound fired");
    assert!(stopped.is_deadline());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "woke at the budget, not the 60s window: {:?}",
        started.elapsed()
    );
    assert!(
        scope.is_cancelled(),
        "budget-bound expiry observes like checkpoint (fires the token)"
    );
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join3_and_join_all_in_task() {
    let scope = Scope::new_root(ScopeOpts::named("arity"));
    let before = scope.live_children();
    let (a, b, c) = scope
        .join3(async { 1u32 }, async { "two" }, async { 3.0f32 })
        .await
        .expect("all three");
    assert_eq!((a, b, c), (1, "two", 3.0));
    let vals = scope
        .join_all((0..5u32).map(|i| async move { i * 2 }))
        .await
        .expect("all five");
    assert_eq!(vals, vec![0, 2, 4, 6, 8]);
    assert_eq!(scope.live_children(), before, "zero spawns");

    // Scope stop drops the whole set.
    scope.cancel(CancelReason::Explicit);
    let out = scope.join_all(vec![std::future::pending::<()>()]).await;
    assert!(out.expect_err("stopped").is_cancel());
    scope.close().await;
}

// ---- shield -----------------------------------------------------------

/// THE cleanup contract: a shielded multi-step cleanup runs to
/// completion despite a scope cancel; the task then resolves Stopped at
/// its next dispatch boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shield_cleanup_completes_despite_cancel() {
    let steps = Arc::new(AtomicUsize::new(0));
    let scope = Scope::new_root(ScopeOpts::named("shield-cleanup"));
    let task_scope = scope.clone();
    let steps2 = steps.clone();
    let h = scope.spawn(async move {
        task_scope
            .shield(async {
                for _ in 0..3 {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    steps2.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;
        // Past the shield: the next boundary resolves us.
        std::future::pending::<()>().await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    scope.cancel(CancelReason::Shutdown); // mid-cleanup
    let stopped = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved after the shield")
        .expect_err("stopped at the post-shield boundary");
    assert!(stopped.is_cancel());
    assert_eq!(steps.load(Ordering::SeqCst), 3, "cleanup ran to completion");
    scope.close().await;
}

/// Regression scenario: run_until_cancelled INSIDE a
/// shield runs the wrapped future to completion on a cancelled scope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_until_cancelled_inside_shield_completes() {
    let scope = Scope::new_root(ScopeOpts::named("shield-ruc"));
    let task_scope = scope.clone();
    let h = scope.spawn(async move {
        let inner = task_scope.clone();
        task_scope
            .shield(async move {
                inner
                    .run_until_cancelled(async {
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        "flushed"
                    })
                    .await
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    scope.cancel(CancelReason::Explicit);
    let out = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved");
    // The shield masked the cancel arm: the flush COMPLETED.
    assert_eq!(
        out.expect("body finished").expect("ruc completed"),
        "flushed"
    );
    scope.close().await;
}

/// Budget pierces shields a shielded pending future still
/// stops at the scope budget's hard bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_expiry_stops_a_shielded_future() {
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::from_millis(25)),
        ..ScopeOpts::named("shield-budget")
    });
    let task_scope = scope.clone();
    let h = scope.spawn(async move {
        task_scope
            .shield(async {
                std::future::pending::<()>().await;
            })
            .await;
    });
    let stopped = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("budget bound resolved the task THROUGH the shield")
        .expect_err("stopped");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::DeadlineExceeded)
    );
    scope.close().await;
}

/// Shields do not cross spawn: a child spawned INSIDE a shield observes
/// cancellation normally while the shielded parent continues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shield_does_not_cross_spawn() {
    let scope = Scope::new_root(ScopeOpts::named("shield-spawn"));
    let task_scope = scope.clone();
    let h = scope.spawn(async move {
        let inner = task_scope.clone();
        task_scope
            .shield(async move {
                let child = inner.spawn(async {
                    std::future::pending::<()>().await;
                });
                // Child must resolve Stopped on cancel DESPITE our shield.
                let child_stop = child.await.expect_err("child observed cancel");
                assert!(child_stop.is_cancel());
                "shield finished"
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(15)).await;
    scope.cancel(CancelReason::Explicit);
    let out = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved");
    assert_eq!(out.expect("shielded body completed"), "shield finished");
    scope.close().await;
}

/// checkpoint() inside a shield: cancel masked, budget reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_inside_shield_masks_cancel_not_budget() {
    let scope = Scope::new_root(ScopeOpts::named("shield-ckpt"));
    let task_scope = scope.clone();
    let h = scope.spawn(async move {
        let inner = task_scope.clone();
        task_scope
            .shield(async move {
                // Wait until the cancel has landed…
                while !inner.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                // …then checkpoint: masked under the shield.
                inner.checkpoint().is_ok()
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    scope.cancel(CancelReason::Explicit);
    let masked = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved")
        .expect("body completed");
    assert!(masked, "checkpoint returned Ok inside the shield");
    scope.close().await;
}

/// Budget REPORTS through a shielded checkpoint — the
/// direct assertion (the boundary-piercing test covers a different
/// entry point).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_inside_shield_reports_expired_budget() {
    let scope = Scope::new_root(ScopeOpts {
        budget: Some(Duration::from_millis(10)),
        ..ScopeOpts::named("shield-ckpt-budget")
    });
    let task_scope = scope.clone();
    let h = scope.spawn(async move {
        let inner = task_scope.clone();
        task_scope
            .shield(async move {
                tokio::time::sleep(Duration::from_millis(30)).await; // expire
                inner.checkpoint()
            })
            .await
    });
    let out = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved");
    match out {
        // The shield let the body run to completion; checkpoint reported
        // the deadline THROUGH the shield.
        Ok(ckpt) => assert_eq!(
            ckpt.expect_err("deadline reported"),
            Stopped::cancelled(CancelReason::DeadlineExceeded)
        ),
        // Or the boundary's hard bound resolved the task first (budget
        // pierces) — also a deadline. Either way the budget was honored.
        Err(stopped) => assert!(stopped.is_deadline()),
    }
    scope.close().await;
}

// ---- shutdown_with_grace ----------------------------------------------

/// Cooperative children drain inside the grace window: clean exit,
/// nothing detached, no WARN.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_shutdown_clean_when_children_cooperate() {
    let scope = Scope::new_root(ScopeOpts::named("grace-clean"));
    for _ in 0..4 {
        let _h = scope.spawn(async {
            std::future::pending::<()>().await; // boundary resolves on cancel
        });
    }
    let detached = scope
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_secs(5))
        .await;
    assert_eq!(detached, 0, "boundary drained everyone inside grace");
    assert_eq!(scope.live_children(), 0);
    assert_eq!(scope.detached_children(), 0);
}

/// A shielded cleanup finishing at grace/2 exits cleanly (the grace
/// window exists FOR shielded work).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_shutdown_waits_for_shielded_cleanup() {
    let scope = Scope::new_root(ScopeOpts::named("grace-shield"));
    let task_scope = scope.clone();
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = cleaned.clone();
    let _h = scope.spawn(async move {
        task_scope
            .shield(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                flag.store(true, Ordering::SeqCst);
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let detached = scope
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(500))
        .await;
    assert_eq!(detached, 0, "shielded cleanup finished inside grace");
    assert!(cleaned.load(Ordering::SeqCst), "cleanup ran");
}

/// A mid-poll staller forces the grace expiry: detach + WARN + prompt
/// return; the straggler's eventual exit settles the detached counter
/// (no double-decrement).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grace_shutdown_detaches_stragglers_and_settles() {
    static RELEASE3: AtomicBool = AtomicBool::new(false);
    let scope = Scope::new_root(ScopeOpts::named("grace-detach"));
    let _h = scope.spawn(async {
        loop {
            std::thread::sleep(Duration::from_millis(15));
            if RELEASE3.load(Ordering::SeqCst) {
                return;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started = std::time::Instant::now();
    let detached = scope
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(100))
        .await;
    assert_eq!(detached, 1, "the staller was detached");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "returned promptly at grace: {:?}",
        started.elapsed()
    );
    assert_eq!(scope.live_children(), 0, "in_flight is drained");
    assert_eq!(scope.detached_children(), 1, "tracked until self-resolve");
    RELEASE3.store(true, Ordering::SeqCst);
    // The straggler's departure settles the detached side exactly once.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while scope.detached_children() > 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(scope.detached_children(), 0, "settled on self-resolve");
}

/// Spawn AFTER the grace bar is refused: the closing bar goes
/// up as step 1 and outlives the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_after_grace_shutdown_is_refused() {
    let scope = Scope::new_root(ScopeOpts::named("grace-bar"));
    let _ = scope
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(50))
        .await;
    let h = scope.spawn(async { 1u32 });
    let stopped = h.await.expect_err("refused by the closing bar");
    assert_eq!(
        stopped.reason(),
        &StopReason::Cancelled(CancelReason::Shutdown)
    );
}

/// Detach-vs-completion race hammer: SHIELDED children completing right
/// at the grace edge (the shield masks the cancel, so completion times
/// genuinely straddle the deadline — an unshielded sleep resolves at
/// the boundary instant and never enters the race) never
/// double-decrement, and detach really happens across the rounds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detach_completion_race_never_double_decrements() {
    let mut total_detached = 0u64;
    for round in 0..10 {
        let scope = Scope::new_root(ScopeOpts::named("grace-race"));
        for i in 0..8u64 {
            let task_scope = scope.clone();
            let _h = scope.spawn(async move {
                task_scope
                    .shield(async move {
                        // Straddle the 45ms grace edge from both sides.
                        let ms = 30 + i * 5; // 30..65ms
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                    })
                    .await;
            });
        }
        tokio::time::sleep(Duration::from_millis(5)).await; // all shielded
        let detached = scope
            .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(45))
            .await;
        total_detached += detached;
        // Everything settles: both counters reach exactly zero.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while (scope.live_children() > 0 || scope.detached_children() > 0)
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(scope.live_children(), 0, "round {round}");
        assert_eq!(scope.detached_children(), 0, "round {round}");
    }
    assert!(
        total_detached > 0,
        "the hammer must actually enter the detach race (got 0 across 10 rounds)"
    );
}

/// shutdown(reason) keeps its original semantics: full (unbounded) drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_shutdown_still_drains_fully() {
    let scope = Scope::new_root(ScopeOpts::named("plain-shutdown"));
    let h = scope.spawn(async {
        std::future::pending::<()>().await;
    });
    scope.shutdown(CancelReason::Shutdown).await;
    assert_eq!(scope.live_children(), 0);
    let stopped = h.await.expect_err("drained via the boundary");
    assert!(stopped.is_cancel());
}

// ---- supervision (Restart) --------------------------------------------

fn restart_opts(max: u32, window: Duration, backoff: Duration) -> ScopeOpts {
    ScopeOpts::named("restart").supervision(crate::flow::Supervision::Restart {
        max_restarts: max,
        window,
        backoff,
    })
}

/// Faults restart via the factory until success; the handle resolves the
/// SUCCESSFUL run's value and the scope is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_recovers_after_faults() {
    let scope = Scope::new_root(restart_opts(
        3,
        Duration::from_secs(10),
        Duration::from_millis(5),
    ));
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let h = scope.spawn_supervised(move || {
        let n = a.fetch_add(1, Ordering::SeqCst);
        async move {
            if n < 2 {
                panic!("flaky attempt {n}");
            }
            42u32
        }
    });
    let out = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved");
    assert_eq!(out.expect("third attempt succeeded"), 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(!scope.is_cancelled(), "recovered — no fail-fast");
    scope.close().await;
}

/// Exhausting max_restarts inside the window escalates to fail-fast:
/// scope cancelled ChildFailed, handle carries the final fault payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_exhaustion_escalates_to_fail_fast() {
    let scope = Scope::new_root(restart_opts(
        2,
        Duration::from_secs(10),
        Duration::from_millis(2),
    ));
    let h = scope.spawn_supervised(|| async {
        panic!("permanent fault");
        #[allow(unreachable_code)]
        0u32
    });
    let stopped = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved")
        .expect_err("escalated");
    assert!(stopped.is_fault());
    match stopped.reason() {
        StopReason::Fault { message: msg, .. } => {
            assert!(msg.contains("permanent fault"), "payload: {msg}")
        }
        other => panic!("expected fault: {other:?}"),
    }
    assert_eq!(
        scope.cancel_reason(),
        Some(CancelReason::ChildFailed),
        "escalation IS fail-fast"
    );
    scope.close().await;
}

/// Slow faults (spaced wider than the window) never accumulate to the
/// cap — the window resets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_faults_reset_the_restart_window() {
    let scope = Scope::new_root(restart_opts(
        1,
        Duration::from_millis(40),
        Duration::from_millis(1),
    ));
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let h = scope.spawn_supervised(move || {
        let n = a.fetch_add(1, Ordering::SeqCst);
        async move {
            match n {
                // Two faults, spaced past the window by the sleep below.
                0 | 1 => {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    panic!("slow fault {n}");
                }
                _ => 7u32,
            }
        }
    });
    let out = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved");
    assert_eq!(out.expect("no escalation"), 7);
    assert!(!scope.is_cancelled(), "window reset between slow faults");
    scope.close().await;
}

/// Backoff actually waits, exponentially. Real clock with small bases
/// (tokio test-util/paused clock is not in the workspace features —
/// adding it would force a bazel crate-universe repin): 30+60+120 ms of
/// mandatory backoff before the 4th attempt; assert the lower bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_backoff_waits_exponentially() {
    let scope = Scope::new_root(restart_opts(
        3,
        Duration::from_secs(600),
        Duration::from_millis(30),
    ));
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let started = std::time::Instant::now();
    let h = scope.spawn_supervised(move || {
        let n = a.fetch_add(1, Ordering::SeqCst);
        async move {
            if n < 3 {
                panic!("fault {n}");
            }
            1u32
        }
    });
    let out = tokio::time::timeout(Duration::from_secs(10), h)
        .await
        .expect("resolved");
    let elapsed = started.elapsed();
    assert_eq!(out.expect("recovered"), 1);
    assert!(
        elapsed >= Duration::from_millis(200),
        "30+60+120ms of exponential backoff must have elapsed: {elapsed:?}"
    );
    scope.close().await;
}

/// Cancel during a LONG backoff returns promptly (the dispatch boundary
/// short-circuits the wrapper) — and a stopped scope never restarts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_during_backoff_returns_promptly() {
    let scope = Scope::new_root(restart_opts(
        5,
        Duration::from_secs(600),
        Duration::from_secs(600), // absurd backoff: only cancel can end it
    ));
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let h = scope.spawn_supervised(move || {
        a.fetch_add(1, Ordering::SeqCst);
        async move {
            panic!("first fault");
            #[allow(unreachable_code)]
            0u32
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await; // in backoff now
    let started = std::time::Instant::now();
    scope.cancel(CancelReason::Shutdown);
    let stopped = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("prompt")
        .expect_err("stopped, not restarted");
    assert!(stopped.is_cancel(), "cancellation is NOT a fault");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "no restart after stop");
    scope.close().await;
}

/// A panic inside the FACTORY itself counts as a fault of the child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_panic_counts_as_fault() {
    let scope = Scope::new_root(restart_opts(
        2,
        Duration::from_secs(10),
        Duration::from_millis(2),
    ));
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let h = scope.spawn_supervised(move || {
        let n = a.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            panic!("factory exploded");
        }
        async move { 9u32 }
    });
    let out = tokio::time::timeout(Duration::from_secs(5), h)
        .await
        .expect("resolved");
    assert_eq!(out.expect("second factory call succeeded"), 9);
    scope.close().await;
}

/// Under FailFast/Isolate, spawn_supervised degrades to a single run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_supervised_single_run_without_restart_policy() {
    let scope = Scope::new_root(ScopeOpts::named("no-restart").supervised()); // Isolate
    let attempts = Arc::new(AtomicUsize::new(0));
    let a = attempts.clone();
    let h = scope.spawn_supervised(move || {
        a.fetch_add(1, Ordering::SeqCst);
        async move {
            panic!("one shot");
            #[allow(unreachable_code)]
            0u32
        }
    });
    let stopped = h.await.expect_err("isolated fault");
    assert!(stopped.is_fault());
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "no restart");
    assert!(!scope.is_cancelled(), "isolated — siblings untouched");
    scope.close().await;
}

// ---- call_in conformance ----------------------------------------------

/// The timeout window reaches the WIRE: call_in inside scope.timeout
/// uses the narrowed cap as its RPC deadline (dual-guard), so a silent
/// service resolves at the window, not at the client's default timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_cap_reaches_call_in_wire_budget() {
    let session = crate::session::Session::open_default()
        .await
        .expect("session");
    let name = format!("flow_r7_silent_{}", std::process::id());
    let service = crate::service::Service::builder(session.clone(), &name)
        .handler(|req| {
            std::mem::forget(req); // never reply
        })
        .build();
    let _running = service.start().await.expect("service");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client default timeout is FAR larger than the window.
    let client = crate::client::Client::builder(session, &name)
        .timeout_ms(30_000)
        .build();
    let scope = Scope::new_root(ScopeOpts::named("r7-wire-cap"));
    let task_scope = scope.clone();
    let started = std::time::Instant::now();
    let client = std::sync::Arc::new(client);
    let c2 = client.clone();
    let out = scope
        .spawn(async move {
            let inner = task_scope.clone();
            task_scope
                .timeout(Duration::from_millis(300), async move {
                    c2.call_in(&inner, vec![1u8]).await
                })
                .await
        })
        .await
        .expect("task completed");
    let elapsed = started.elapsed();
    // Either the window resolved the combinator (Stopped) or the call's
    // narrowed wire budget fired first (AortaError timeout) — both are
    // the deadline doing its job; what must NOT happen is the 30s ride.
    match out {
        Err(stopped) => assert!(stopped.is_deadline(), "{stopped:?}"),
        Ok(Err(err)) => {
            let msg = err.to_string();
            assert!(
                msg.contains("timed out") || msg.contains("cancelled"),
                "unexpected error: {msg}"
            );
        }
        Ok(Ok(_)) => panic!("silent service cannot reply"),
    }
    assert!(
        elapsed < Duration::from_secs(5),
        "resolved at the window, not the 30s client timeout: {elapsed:?}"
    );
    scope.close().await;
}

/// A shielded cleanup RPC completes on a CANCELLED scope the
/// entry checkpoint and the in-flight cancel race are both shield-aware.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shield_allows_cleanup_rpc_through() {
    let session = crate::session::Session::open_default()
        .await
        .expect("session");
    let name = format!("flow_r7_echo_{}", std::process::id());
    let service = crate::service::Service::builder(session.clone(), &name)
        .handler(|req| {
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let _ = req.reply(&payload).await;
            });
        })
        .build();
    let _running = service.start().await.expect("service");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = std::sync::Arc::new(crate::client::Client::builder(session, &name).build());
    let scope = Scope::new_root(ScopeOpts::named("r7-shield-rpc"));
    let task_scope = scope.clone();
    let c2 = client.clone();
    let h = scope.spawn(async move {
        let inner = task_scope.clone();
        // THE cleanup idiom: the wait-for-cancel AND the cleanup both
        // live INSIDE the shield — the raw token future is deliberately
        // shield-transparent (it resolves), while the boundary/checkpoint
        // masking keeps the task alive to run the cleanup. Waiting
        // OUTSIDE the shield would let the dispatch boundary resolve the
        // task before the cleanup is reached.
        task_scope
            .shield(async move {
                inner.cancelled().await;
                c2.call_in(&inner, vec![7u8, 7]).await
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    scope.cancel(CancelReason::Shutdown);
    let out = tokio::time::timeout(Duration::from_secs(10), h)
        .await
        .expect("resolved");
    let reply = out
        .expect("shielded body ran to completion")
        .expect("cleanup RPC succeeded on a cancelled scope");
    assert_eq!(reply.to_bytes().as_ref(), &[7u8, 7]);
    scope.close().await;
}

/// The REAL race-loser test: racing a fast echo against a
/// silent call — the winner returns promptly; the dropped loser does
/// NOT abort anything server-side (documented contract) and drops
/// without disturbing the winner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn race_with_real_call_in_loser() {
    let session = crate::session::Session::open_default()
        .await
        .expect("session");
    let echo_name = format!("flow_r7_race_echo_{}", std::process::id());
    let silent_name = format!("flow_r7_race_silent_{}", std::process::id());
    // Determinism (CI-caught flake): the contract under test is that a
    // loser query ALREADY RECEIVED server-side runs to completion after
    // the race drops the loser future. Nothing used to guarantee the
    // loser's query hit the wire before the fast echo resolved the race
    // on a fast runner — so the echo now withholds its reply until the
    // silent side confirms receipt (3s fallback keeps failures loud).
    let loser_received = Arc::new(AtomicBool::new(false));
    let loser_received_echo = loser_received.clone();
    let echo = crate::service::Service::builder(session.clone(), &echo_name)
        .handler(move |req| {
            let received = loser_received_echo.clone();
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                let deadline = std::time::Instant::now() + Duration::from_secs(3);
                while !received.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let _ = req.reply(&payload).await;
            });
        })
        .build();
    // Delayed-reply loser: replies AFTER the race resolves and bumps a
    // counter — observable proof the dropped loser's query ran to
    // completion server-side.
    let loser_completed = Arc::new(AtomicUsize::new(0));
    let loser_probe = loser_completed.clone();
    let loser_received_silent = loser_received.clone();
    let silent = crate::service::Service::builder(session.clone(), &silent_name)
        .handler(move |req| {
            loser_received_silent.store(true, Ordering::SeqCst);
            let probe = loser_probe.clone();
            let payload = req.payload().to_bytes().into_owned();
            let _ = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = req.reply(&payload).await;
                probe.fetch_add(1, Ordering::SeqCst);
            });
        })
        .build();
    let _e = echo.start().await.expect("echo");
    let _s = silent.start().await.expect("silent");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let fast =
        std::sync::Arc::new(crate::client::Client::builder(session.clone(), &echo_name).build());
    let slow = std::sync::Arc::new(crate::client::Client::builder(session, &silent_name).build());
    let scope = Scope::new_root(ScopeOpts::named("r7-race"));
    let started = std::time::Instant::now();
    let won = scope
        .race2(async { fast.call_in(&scope, vec![1u8]).await }, async {
            slow.call_in(&scope, vec![2u8]).await
        })
        .await
        .expect("race resolved");
    match won {
        crate::flow::Either::Left(reply) => {
            assert_eq!(reply.expect("echo replied").to_bytes().as_ref(), &[1u8]);
        }
        crate::flow::Either::Right(_) => panic!("silent side cannot win"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "winner returned promptly; loser dropped in place"
    );
    // The dropped loser's query completes SERVER-SIDE regardless (the
    // documented contract: drop does not abort the router-side query).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while loser_completed.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        loser_completed.load(Ordering::SeqCst),
        1,
        "loser handler ran to completion server-side"
    );
    scope.close().await;
}

// ---- ExternalChild pins + real wire-cap pin ----------------------------------

/// The FFI grace contract, pinned end-to-end — an unfinished
/// ExternalChild is detached at grace, its later finish() settles the
/// detached counter (not in_flight), and the withdraw path leaves the
/// counters untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_child_detaches_at_grace_and_settles_on_finish() {
    let scope = Scope::new_root(ScopeOpts::named("ext-grace"));
    let ext = scope
        .begin_external_child()
        .expect("live scope accepts external children");
    assert_eq!(scope.live_children(), 1);
    let detached = scope
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(50))
        .await;
    assert_eq!(detached, 1, "the defiant external child was detached");
    assert_eq!(scope.live_children(), 0, "in_flight released at grace");
    assert_eq!(scope.detached_children(), 1, "tracked until finish");
    ext.finish(); // late C-plane completion settles the DETACHED side
    assert_eq!(scope.detached_children(), 0, "settled exactly once");
    assert_eq!(scope.live_children(), 0, "no double-decrement");
}

/// begin_external_child on a closing scope withdraws
/// cleanly — counters untouched, no live-looking slot left behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_child_withdraws_under_closing_scope() {
    let scope = Scope::new_root(ScopeOpts::named("ext-withdraw"));
    let _ = scope
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(10))
        .await;
    assert!(scope.begin_external_child().is_none(), "closing bar holds");
    assert_eq!(scope.live_children(), 0);
    assert_eq!(scope.detached_children(), 0);
    // A subsequent grace shutdown must find nothing to detach (a
    // live-looking Weak from the withdraw would show up here).
    let detached = scope
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(10))
        .await;
    assert_eq!(detached, 0);
}

/// The DIRECT wire-cap pin — an ambient cap with NO enclosing
/// timeout combinator must narrow call_in's wire deadline. Deleting the
/// cap consult in call_in turns this test red (the silent service would
/// ride the 30s client timeout).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambient_cap_alone_narrows_call_in_wire_deadline() {
    let session = crate::session::Session::open_default()
        .await
        .expect("session");
    let name = format!("flow_r7_cap_direct_{}", std::process::id());
    let service = crate::service::Service::builder(session.clone(), &name)
        .handler(|req| {
            std::mem::forget(req); // never reply
        })
        .build();
    let _running = service.start().await.expect("service");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = crate::client::Client::builder(session, &name)
        .timeout_ms(30_000)
        .build();
    let scope = Scope::new_root(ScopeOpts::named("r7-cap-direct"));
    let started = std::time::Instant::now();
    let err = crate::flow::ambient::scoped(async {
        let _cap = crate::flow::ambient::BudgetCapGuard::install(
            std::time::Instant::now() + Duration::from_millis(300),
        );
        client.call_in(&scope, vec![1u8]).await
    })
    .await
    .expect_err("silent service cannot reply");
    let elapsed = started.elapsed();
    assert!(
        err.is_deadline(),
        "wire deadline expiry must resolve Stopped(DeadlineExceeded): {err}"
    );
    assert!(
        scope.cancel_reason().is_none(),
        "the wire deadline must not have cancelled the scope itself"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the cap narrowed the wire deadline (30s client timeout did not apply): {elapsed:?}"
    );
    scope.close().await;
}

// ---- abort() subtree cascade ------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_cancels_nested_scopes_created_from_the_spawn_with_scope() {
    // A spawn_with body builds a nested scope from ITS scope and parks a
    // child on it. abort() must tear the whole subtree down — pre-fix
    // the nested child stayed parked forever (orphaned, warn-only).
    struct SetOnDrop(Arc<AtomicBool>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let scope = root();
    let inner_dropped = Arc::new(AtomicBool::new(false));
    let started = Arc::new(tokio::sync::Notify::new());
    let dropped2 = Arc::clone(&inner_dropped);
    let started2 = Arc::clone(&started);
    let handle = scope.spawn_with(move |task_scope| async move {
        let nested = task_scope.child(ScopeOpts::default());
        let guard = SetOnDrop(dropped2);
        let _inner = nested.spawn(async move {
            let _guard = guard;
            started2.notify_one();
            std::future::pending::<()>().await;
        });
        std::future::pending::<()>().await;
    });
    started.notified().await;
    handle.abort();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !inner_dropped.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        inner_dropped.load(Ordering::SeqCst),
        "abort() cancelled the task's own subtree — the nested child resolved"
    );
    scope.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_with_hands_the_body_a_real_child_scope() {
    // The doc always promised a child scope; cancelling the task scope
    // must not cancel the parent, and parent cancel must cascade in.
    let scope = root();
    let handle = scope.spawn_with(|task_scope| async move {
        task_scope.cancel(CancelReason::Explicit);
        task_scope.is_cancelled()
    });
    assert_eq!(handle.join().await, Ok(true));
    assert!(
        !scope.is_cancelled(),
        "cancelling the task's scope never cancels the parent"
    );
    scope.close().await;
}
