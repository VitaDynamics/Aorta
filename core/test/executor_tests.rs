use super::{
    ExecutorClassPolicyOptions, ExecutorOptions, SchedulerMode, SingleThreadedExecutor, SpinOutcome,
};
use crate::event_queue::{Event, EventKind};
use crate::service::RpcOverflowPolicy;
use crate::subscriber::{SubscriberOptions, SubscriberOverflowPolicy};
use crate::test_support::{EnvVarGuard, ENV_LOCK};
use crate::timer::TimerOverflowPolicy;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn spin_once_executes_event() {
    let mut exec = SingleThreadedExecutor::new(4);
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    exec.queue()
        .push(Event::new(EventKind::Timer, move || {
            *flag_clone.lock().expect("lock") = true;
        }))
        .await
        .expect("push");

    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert!(*flag.lock().expect("lock"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spin_loop_increments_spin_count() {
    // X1: the executor's main spin loop must increment the monotonic spin
    // counter that runtime_metrics telemetry exposes (executor liveness). The
    // first iteration alone increments before the loop blocks on the queue, so
    // a brief spin then stop is enough.
    let mut exec = SingleThreadedExecutor::new(8);
    let spin = Arc::clone(&exec.spin_count);
    let stop = exec.stop_handle();
    assert_eq!(spin.load(Ordering::Relaxed), 0, "counter starts at zero");

    let handle = tokio::spawn(async move {
        exec.spin().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.stop();
    handle.await.expect("spin task join");

    assert!(
        spin.load(Ordering::Relaxed) > 0,
        "the spin loop must have incremented spin_count"
    );
}

#[tokio::test]
async fn dispatch_increments_error_count_on_caught_panic() {
    // X1 follow-up: a panicking callback is isolated (the executor keeps
    // running) AND counted into the executor's cumulative error_count, which
    // feeds the vital health pulse (RuntimeHealth.error_count). One panic ->
    // one increment.
    let mut exec = SingleThreadedExecutor::new(4);
    assert_eq!(
        exec.error_count.load(Ordering::Relaxed),
        0,
        "starts at zero"
    );

    exec.queue()
        .push(Event::new(EventKind::SubscriptionData, || {
            panic!("boom from a subscriber callback")
        }))
        .await
        .expect("push");

    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert_eq!(
        exec.error_count.load(Ordering::Relaxed),
        1,
        "a caught callback panic must increment error_count exactly once"
    );
}

#[tokio::test]
async fn dispatch_does_not_count_error_on_clean_callback() {
    // A normally-returning callback must NOT bump error_count — the counter is
    // a genuine fault signal, not a dispatch tally.
    let mut exec = SingleThreadedExecutor::new(4);
    exec.queue()
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");

    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert_eq!(
        exec.error_count.load(Ordering::Relaxed),
        0,
        "a clean callback must leave error_count at zero"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_executor_clears_node_error_reader() {
    // A Node is a SHARED handle — the executor holds a
    // clone in `self.nodes`, so the caller's handle can outlive an executor dropped
    // via RAII (no FFI `detach_nodes()` / `Node::close()`). The executor Drop must
    // release the error reader it installed in `add_node`, or the node keeps
    // reporting the dead executor's frozen `health.error_count` instead of the
    // honest no-reader/zero state. (The vital error-reader clear is synchronous, so
    // the strong-count check is deterministic; the bulk spin reader clears via an
    // async command and is not asserted here.)
    use crate::node::Node;

    let node = Node::new("executor_drop_reader_test", "default")
        .await
        .expect("node");
    let mut exec = SingleThreadedExecutor::new(4);
    let error = Arc::clone(&exec.error_count);

    exec.add_node(node.clone());
    assert!(
        Arc::strong_count(&error) > 1,
        "add_node must install the error reader on the node"
    );

    drop(exec);
    assert_eq!(
        Arc::strong_count(&error),
        1,
        "dropping the executor must clear the node's error reader"
    );

    node.close().await.expect("close");
}

#[test]
fn executor_multi_ingress_admits_events_without_async_add_node_context() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _multi_ingress_guard = EnvVarGuard::set("AORTA_EXECUTOR_MULTI_INGRESS", "1");

    let mut exec = SingleThreadedExecutor::new(8);
    let sender = exec.queue().sender();
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    sender
        .try_send(Event::new(EventKind::SubscriptionData, move || {
            *flag_clone.lock().expect("lock") = true;
        }))
        .expect("admit event into multi-ingress queue");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let outcome = runtime.block_on(async { exec.spin_once(Duration::from_millis(20)).await });

    assert_eq!(outcome, SpinOutcome::Event);
    assert!(*flag.lock().expect("lock"));
}

#[test]
fn executor_queue_honors_max_queued_events_env_budget() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _max_events_guard = EnvVarGuard::set("AORTA_EXECUTOR_MAX_QUEUED_EVENTS", "1");
    let _admission_observability_guard =
        EnvVarGuard::set("AORTA_EXECUTOR_ADMISSION_OBSERVABILITY", "1");

    let exec = SingleThreadedExecutor::new(8);
    let sender = exec.queue().sender();
    sender
        .try_send(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(1))
        .expect("first event should be admitted");
    let second =
        sender.try_send(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(1));
    assert!(
        matches!(second, Err(tokio::sync::mpsc::error::TrySendError::Full(_))),
        "second event should hit admission budget"
    );

    let stats = exec.queue().admission_stats();
    assert_eq!(stats.dropped_budget_events, 1);
}

#[tokio::test]
async fn spin_once_times_out_when_empty() {
    let mut exec = SingleThreadedExecutor::new(1);
    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Timeout);
}

#[tokio::test]
async fn spin_once_zero_timeout_still_executes_ready_event_fifo() {
    let mut exec = SingleThreadedExecutor::new(2);
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    exec.queue()
        .push(Event::new(EventKind::SubscriptionData, move || {
            *flag_clone.lock().expect("lock") = true;
        }))
        .await
        .expect("push");

    let outcome = exec.spin_once(Duration::from_millis(0)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert!(*flag.lock().expect("lock"));
}

#[tokio::test]
async fn spin_once_zero_timeout_still_executes_ready_event_priority() {
    let mut exec = SingleThreadedExecutor::new_with_options(
        2,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            ..ExecutorOptions::default()
        },
    );
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    exec.queue()
        .push(Event::new(EventKind::SubscriptionData, move || {
            *flag_clone.lock().expect("lock") = true;
        }))
        .await
        .expect("push");

    let outcome = exec.spin_once(Duration::from_millis(0)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert!(*flag.lock().expect("lock"));
}

#[tokio::test]
async fn spin_exits_on_stop() {
    let mut exec = SingleThreadedExecutor::new(1);
    let stop = exec.stop_handle();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        stop.stop();
    });

    exec.spin().await;
    assert!(exec.stop_handle().is_stopped());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_with_timer_integration() {
    use crate::node::Node;
    use crate::timer::TimerOptions;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    let mut exec = SingleThreadedExecutor::new(128);
    let node = Node::new("timer_test", "default")
        .await
        .expect("create node");
    exec.add_node(node.clone());

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    // Create timer with 50ms period
    let _timer = node
        .create_timer_with_options(
            Duration::from_millis(50),
            move || {
                counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
            },
            TimerOptions::default(),
        )
        .await
        .expect("create timer");

    // Run executor for ~200ms to capture ~4 timer ticks
    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(220)).await;
        stop.stop();
    });

    exec.spin().await;

    // Verify timer executed multiple times (should be ~4, allow 3-5 range for timing variance)
    let count = counter.load(AtomicOrdering::Relaxed);
    assert!(
        (3..=5).contains(&count),
        "expected 3-5 timer ticks, got {}",
        count
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn coalesce_timer_enqueues_single_pending_event_before_spin() {
    use crate::node::Node;
    use crate::timer::TimerOptions;

    let mut exec = SingleThreadedExecutor::new(128);
    let node = Node::new("executor_owned_timer_queue", "default")
        .await
        .expect("create node");
    exec.add_node(node.clone());

    let _timer = node
        .create_timer_with_options(Duration::from_millis(10), || {}, TimerOptions::default())
        .await
        .expect("create timer");

    tokio::time::sleep(Duration::from_millis(40)).await;

    assert_eq!(
        exec.queue().len(),
        1,
        "dedicated coalesce timer should enqueue a single pending event before spin"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn coalesce_timer_records_due_samples_before_first_dispatch() {
    use crate::node::Node;
    use crate::timer::TimerOptions;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    let mut exec = SingleThreadedExecutor::new(128);
    let node = Node::new("executor_owned_timer_coalesce_stats", "default")
        .await
        .expect("create node");
    exec.add_node(node.clone());

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let timer = node
        .create_timer_with_options(
            Duration::from_millis(10),
            move || {
                counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
            },
            TimerOptions::default(),
        )
        .await
        .expect("create timer");

    tokio::time::sleep(Duration::from_millis(55)).await;
    assert_eq!(exec.queue().len(), 1);
    assert!(
        timer.coalesced_tick_count() > 0,
        "dedicated coalesce timer should count skipped firings while one callback is pending"
    );

    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert!(
        counter.load(AtomicOrdering::Relaxed) >= 1,
        "coalesced timer should execute at least one callback once drained"
    );
    assert!(
        timer.deadline_slip_total_ns() > 0,
        "dedicated coalesce timer should record wake-up deadline slip"
    );
    assert!(
        timer.deadline_slip_max_ns() > 0,
        "dedicated coalesce timer should record maximum deadline slip"
    );
    assert!(
        timer.due_firings_total() > 0,
        "dedicated coalesce timer should record accumulated due firings"
    );
    assert!(
        timer.due_firings_max() > 0,
        "dedicated coalesce timer should record due firings per wake-up"
    );
    assert!(
        timer.due_sample_count() > 0,
        "dedicated coalesce timer should record due-firing samples"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn executor_with_paused_timer() {
    use crate::node::Node;
    use crate::timer::TimerOptions;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    let mut exec = SingleThreadedExecutor::new(128);
    let node = Node::new("paused_timer", "default")
        .await
        .expect("create node");
    exec.add_node(node.clone());

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    // Create timer but don't auto-start
    let timer = node
        .create_timer_with_options(
            Duration::from_millis(50),
            move || {
                counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
            },
            TimerOptions::new().with_auto_start(false),
        )
        .await
        .expect("create timer");

    // Verify timer is not running
    assert!(!timer.is_running());

    // Start the timer before running executor
    timer.resume();
    assert!(timer.is_running());

    // Run executor until the resumed timer delivers a few callbacks, with a
    // timeout to avoid hanging if the timer never starts.
    let stop = exec.stop_handle();
    let stop_counter = Arc::clone(&counter);
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        loop {
            if stop_counter.load(AtomicOrdering::Relaxed) >= 3 {
                stop.stop();
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                stop.stop();
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    exec.spin().await;

    // The resumed timer should execute several callbacks once the executor is spinning.
    let count = counter.load(AtomicOrdering::Relaxed);
    assert!(
        count >= 3,
        "expected paused timer to deliver at least 3 ticks after resume, got {}",
        count
    );
}

#[tokio::test]
async fn spin_once_fifo_drains_multiple_ready_events() {
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    let mut exec = SingleThreadedExecutor::new(64);
    let counter = Arc::new(AtomicU32::new(0));

    // Push 5 events before calling spin_once.
    for _ in 0..5 {
        let counter_clone = Arc::clone(&counter);
        exec.queue()
            .push(Event::new(EventKind::SubscriptionData, move || {
                counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
            }))
            .await
            .expect("push");
    }

    // A single spin_once should drain all 5 events (max_drain_per_cycle=32 by default).
    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert_eq!(
        counter.load(AtomicOrdering::Relaxed),
        5,
        "spin_once should batch-drain all ready events in Fifo mode"
    );
}

#[tokio::test]
async fn spin_once_priority_drains_multiple_ready_events() {
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    let mut exec = SingleThreadedExecutor::new_with_options(
        64,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            ..ExecutorOptions::default()
        },
    );
    let counter = Arc::new(AtomicU32::new(0));

    // Push 5 events before calling spin_once.
    for _ in 0..5 {
        let counter_clone = Arc::clone(&counter);
        exec.queue()
            .push(Event::new(EventKind::SubscriptionData, move || {
                counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
            }))
            .await
            .expect("push");
    }

    // A single spin_once should drain all 5 events from the priority batch.
    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert_eq!(
        counter.load(AtomicOrdering::Relaxed),
        5,
        "spin_once should batch-drain all ready events in Priority mode"
    );
}

#[test]
fn class_policy_subscriber_defaults_inherit_executor_policy() {
    let defaults = ExecutorClassPolicyOptions {
        subscriber_overflow_policy: SubscriberOverflowPolicy::Relay,
        subscriber_overflow_relay_capacity: 2048,
        ..ExecutorClassPolicyOptions::default()
    };
    let mut options = SubscriberOptions::default();
    defaults.apply_subscriber_overflow_compat(&mut options);
    assert_eq!(options.overflow_policy, SubscriberOverflowPolicy::Relay);
    assert_eq!(options.overflow_relay_capacity, 2048);
}

#[test]
fn class_policy_rpc_timeout_only_maps_to_block_for_compat() {
    let defaults = ExecutorClassPolicyOptions::default();
    let (policy, timeout) = defaults
        .resolve_rpc_overflow_compat(RpcOverflowPolicy::FastFailBusy, Duration::from_millis(9));
    assert_eq!(policy, RpcOverflowPolicy::Block);
    assert_eq!(timeout, Duration::from_millis(9));
}

#[test]
fn class_policy_rpc_default_inherits_executor_policy() {
    let defaults = ExecutorClassPolicyOptions {
        rpc_overflow_policy: RpcOverflowPolicy::Block,
        rpc_overflow_block_timeout: Duration::from_millis(25),
        ..ExecutorClassPolicyOptions::default()
    };
    let (policy, timeout) = defaults
        .resolve_rpc_overflow_compat(RpcOverflowPolicy::FastFailBusy, Duration::from_millis(0));
    assert_eq!(policy, RpcOverflowPolicy::Block);
    assert_eq!(timeout, Duration::from_millis(25));
}

#[test]
fn class_policy_timer_timeout_only_maps_to_block_for_compat() {
    let defaults = ExecutorClassPolicyOptions::default();
    let (policy, timeout) = defaults.resolve_timer_overflow_compat(
        TimerOverflowPolicy::CoalesceLatest,
        Duration::from_millis(11),
    );
    assert_eq!(policy, TimerOverflowPolicy::Block);
    assert_eq!(timeout, Duration::from_millis(11));
}

#[test]
fn class_policy_timer_default_inherits_executor_policy() {
    let defaults = ExecutorClassPolicyOptions {
        timer_overflow_policy: TimerOverflowPolicy::Block,
        timer_overflow_block_timeout: Duration::from_millis(15),
        ..ExecutorClassPolicyOptions::default()
    };
    let (policy, timeout) = defaults.resolve_timer_overflow_compat(
        TimerOverflowPolicy::CoalesceLatest,
        Duration::from_millis(0),
    );
    assert_eq!(policy, TimerOverflowPolicy::Block);
    assert_eq!(timeout, Duration::from_millis(15));
}

#[tokio::test]
async fn spin_once_fifo_single_event_still_works() {
    // Regression: verify that a single event still returns SpinOutcome::Event
    // and doesn't break when the drain loop finds no additional events.
    let mut exec = SingleThreadedExecutor::new(4);
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    exec.queue()
        .push(Event::new(EventKind::Timer, move || {
            *flag_clone.lock().expect("lock") = true;
        }))
        .await
        .expect("push");

    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert!(*flag.lock().expect("lock"));
}

#[test]
fn stop_handle_closes_the_reserved_lane() {
    // Stop is one-way — nothing will dispatch the reserved
    // lane after it — so stop must CLOSE the lane. Otherwise a CoroutineResume
    // pushed into a stopped-but-not-yet-freed executor would stall silently
    // (neither delivered, nor failed over, nor warned) until the eventual
    // free.
    let exec = SingleThreadedExecutor::new(4);
    let sender = exec.queue().sender();
    sender
        .try_send(Event::new(EventKind::CoroutineResume, || {}))
        .expect("lane push while running");
    exec.stop_handle().stop();
    let err = sender
        .try_send(Event::new(EventKind::CoroutineResume, || {}))
        .expect_err("lane must be closed after stop");
    assert!(
        matches!(err, tokio::sync::mpsc::error::TrySendError::Closed(_)),
        "producers must observe Closed and fail over"
    );
}

#[test]
fn request_stop_closes_the_reserved_lane() {
    let exec = SingleThreadedExecutor::new(4);
    let sender = exec.queue().sender();
    exec.request_stop();
    assert!(
        matches!(
            sender.try_send(Event::new(EventKind::CoroutineResume, || {})),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_))
        ),
        "request_stop must close the reserved lane like StopHandle::stop"
    );
}

#[test]
fn priority_bucket_breaks_ties_by_earliest_deadline() {
    // Within one bucket, deadline-carrying events dispatch in
    // earliest-deadline order (and ahead of deadline-free peers); dispatch
    // across buckets stays priority round-robin.
    let mut exec = SingleThreadedExecutor::new_with_options(
        8,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            ..Default::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let now = std::time::Instant::now();
    let mk = |tag: &'static str, deadline_ms: Option<u64>| {
        let order = Arc::clone(&order);
        Event::new(EventKind::SubscriptionData, move || {
            order.lock().expect("lock").push(tag);
        })
        .with_deadline(deadline_ms.map(|ms| now + Duration::from_millis(1000 + ms)))
    };
    // Same bucket (Normal), insertion order: none, late, early.
    exec.queue().try_push(mk("none", None)).expect("push");
    exec.queue().try_push(mk("late", Some(500))).expect("push");
    exec.queue().try_push(mk("early", Some(10))).expect("push");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    // One spin may batch several dispatches; loop until all three ran.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while order.lock().expect("lock").len() < 3 {
        let _ = rt.block_on(exec.spin_once(Duration::from_millis(50)));
        assert!(std::time::Instant::now() < deadline, "events must dispatch");
    }
    assert_eq!(
        order.lock().expect("lock").as_slice(),
        &["early", "late", "none"],
        "EDF within the bucket; deadline-free items follow"
    );
}

#[test]
fn expired_events_are_dropped_at_dequeue() {
    let mut exec = SingleThreadedExecutor::new_with_options(
        8,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            ..Default::default()
        },
    );
    let ran = Arc::new(Mutex::new(false));
    let ran_cb = Arc::clone(&ran);
    let expired = Event::new(EventKind::SubscriptionData, move || {
        *ran_cb.lock().expect("lock") = true;
    })
    .with_deadline(Some(std::time::Instant::now() - Duration::from_millis(1)));
    exec.queue().try_push(expired).expect("push");
    // The expired event is dropped at dequeue; spin_once times out.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let outcome = rt.block_on(exec.spin_once(Duration::from_millis(50)));
    assert_eq!(outcome, SpinOutcome::Timeout);
    assert!(!*ran.lock().expect("lock"), "expired event must not run");
    assert_eq!(exec.dropped_expired(), 1, "shed is observable");
}

// ---- Wake-path guards (Tier-1: the queue path fires ONE wake, not two) ----
//
// `EventQueueSender::try_send_queue` no longer fires the executor's outer
// `Notify` — the channel send already wakes the receiver the spin loop parks
// on. These two tests pin both halves of that claim: a queue push must still
// break an already-parked spin loop, and the `Notify` must still be the wake
// primitive for the timer scheduler, which has no channel send to ride on.

/// A `try_push` issued while the spin loop is ALREADY parked must dispatch
/// promptly on the mpsc waker alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_push_wakes_an_already_parked_spin_loop() {
    let mut exec = SingleThreadedExecutor::new(16);
    let sender = exec.queue().sender();
    let stop = exec.stop_handle();
    let ran = Arc::new(Mutex::new(false));
    let ran_cb = Arc::clone(&ran);

    let pusher = tokio::spawn(async move {
        // Long enough that the spin loop is parked inside `pop()` (no timer is
        // registered, so the park has no timeout to fall back on).
        tokio::time::sleep(Duration::from_millis(200)).await;
        sender
            .try_send(Event::new(EventKind::SubscriptionData, move || {
                *ran_cb.lock().expect("lock") = true;
            }))
            .expect("try_send");
        tokio::time::sleep(Duration::from_millis(200)).await;
        stop.stop();
    });

    exec.spin().await;
    pusher.await.expect("pusher");
    assert!(
        *ran.lock().expect("lock"),
        "a queue push must break the park through the channel's own waker"
    );
}

/// A timer registered AFTER the spin loop parked must still fire: the timer
/// scheduler keeps the executor `Notify` precisely because it has no channel
/// send of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_registered_after_park_still_wakes_the_spin_loop() {
    use crate::node::Node;
    use crate::timer::TimerOptions;
    use std::sync::atomic::AtomicU32;

    let mut exec = SingleThreadedExecutor::new(16);
    let node = Node::new("timer_wake_after_park", "default")
        .await
        .expect("create node");
    exec.add_node(node.clone());

    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_cb = Arc::clone(&ticks);
    let stop = exec.stop_handle();

    let arming = tokio::spawn(async move {
        // Park the spin loop first: no node timers, no queue traffic.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let timer = node
            .create_timer_with_options(
                Duration::from_millis(20),
                move || {
                    ticks_cb.fetch_add(1, Ordering::Relaxed);
                },
                TimerOptions::default(),
            )
            .await
            .expect("create timer");
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.stop();
        drop(timer);
    });

    exec.spin().await;
    arming.await.expect("arming task");
    assert!(
        ticks.load(Ordering::Relaxed) > 0,
        "a timer armed while the executor was parked must break the park"
    );
}

// --- executor attachment ownership ---------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_node_refuses_a_node_already_owned_by_another_executor() {
    use crate::node::Node;

    let node = Node::new("exec_attach_core_refuse", "default")
        .await
        .expect("node");
    let mut exec_a = SingleThreadedExecutor::new(4);
    exec_a.try_add_node(node.clone()).expect("first attach");
    let generation = node.executor_attachment_generation();

    let mut exec_b = SingleThreadedExecutor::new(4);
    let err = exec_b
        .try_add_node(node.clone())
        .expect_err("a second executor must be refused");
    assert!(
        err.to_string().contains("exec_attach_core_refuse"),
        "refusal must name the node, got: {err}"
    );
    assert_eq!(
        node.executor_attachment_generation(),
        generation,
        "a refused attach must leave the node's attachment untouched"
    );

    // Re-attaching the SAME executor is an idempotent refresh, not a conflict.
    exec_a
        .try_add_node(node.clone())
        .expect("re-attaching the owning executor must be allowed");

    exec_a.detach_nodes();
    node.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_detached_executor_drop_does_not_unwind_the_new_attachment() {
    use crate::node::Node;

    // The predecessor executor still holds a clone of the node in `self.nodes`.
    // Its detach and its Drop must both leave the SUCCESSOR's wiring alone,
    // otherwise handing a node over silently strips the new executor's readers
    // and event sender.
    let node = Node::new("exec_attach_core_handover", "default")
        .await
        .expect("node");
    let mut exec_a = SingleThreadedExecutor::new(4);
    exec_a.try_add_node(node.clone()).expect("first attach");
    exec_a.detach_nodes();

    let mut exec_b = SingleThreadedExecutor::new(4);
    exec_b.try_add_node(node.clone()).expect("re-attach");
    let error = Arc::clone(&exec_b.error_count);
    let generation = node.executor_attachment_generation();

    exec_a.detach_nodes(); // stale detach: must be a no-op now
    drop(exec_a);

    assert!(
        node.has_event_sender(),
        "the predecessor's teardown must not clear the successor's event sender"
    );
    assert_eq!(
        node.executor_attachment_generation(),
        generation,
        "the predecessor's teardown must not touch the attachment generation"
    );
    assert!(
        Arc::strong_count(&error) > 1,
        "the predecessor's Drop must not clear the successor's error reader"
    );

    exec_b.detach_nodes();
    node.close().await.expect("close");
}

// --- concurrency of the attachment transition -----------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_attach_of_one_node_admits_exactly_one_executor() {
    use crate::node::Node;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Barrier;

    // `has_event_sender()` then `set_event_sender()` is a
    // check-then-set. Two threads attaching the same cloned node to different
    // executors could both observe it unowned, both "succeed", and the last
    // writer silently take ownership — recreating the very defect the ownership check fixes.
    // The claim must be ONE atomic node-level transition.
    let node = Node::new("exec_attach_race_claim", "default")
        .await
        .expect("node");

    const RACERS: usize = 4;
    const ROUNDS: usize = 40;

    for round in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(RACERS));
        let wins = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(RACERS);

        for _ in 0..RACERS {
            let node = node.clone();
            let barrier = Arc::clone(&barrier);
            let wins = Arc::clone(&wins);
            handles.push(std::thread::spawn(move || {
                let mut exec = SingleThreadedExecutor::new(4);
                barrier.wait();
                if exec.try_add_node(node).is_ok() {
                    wins.fetch_add(1, Ordering::SeqCst);
                }
                // Return the executor rather than detaching here: detaching
                // inside the racing thread frees the node again, so a racer
                // scheduled later would win legitimately and the round would
                // count serial claims instead of concurrent ones.
                exec
            }));
        }
        let executors: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().expect("racer thread"))
            .collect();

        assert_eq!(
            wins.load(Ordering::SeqCst),
            1,
            "round {round}: exactly one executor may claim the node, \
             but the claim was granted {} times",
            wins.load(Ordering::SeqCst)
        );

        // Hand the node back for the next round; only the owner's detach has
        // any effect.
        for exec in &executors {
            exec.detach_nodes();
        }
        drop(executors);
    }

    node.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detach_racing_a_successor_attach_never_leaves_a_torn_attachment() {
    use crate::node::Node;
    use std::sync::Barrier;

    // A's `detach_nodes()` could pass `still_attached()`, clear the
    // event sender, and then B could install its whole wiring before A carried
    // on clearing the timer scheduler / callback-group registry / readers. The
    // node would then advertise B's live sender with parts of B's
    // configuration already stripped. Detach cleanup and successor attachment
    // must share one transaction.
    let node = Node::new("exec_attach_race_detach", "default")
        .await
        .expect("node");

    // Widen the detach window deterministically: without serialization the
    // successor lands squarely inside it. With serialization it simply blocks
    // on the node's attachment transaction, so the seam only costs time.
    crate::executor::detach_window_seam::set_pause_micros(2_000);
    struct SeamReset;
    impl Drop for SeamReset {
        fn drop(&mut self) {
            crate::executor::detach_window_seam::set_pause_micros(0);
        }
    }
    let _seam_reset = SeamReset;

    for round in 0..40 {
        let mut exec_a = SingleThreadedExecutor::new(4);
        exec_a
            .try_add_node(node.clone())
            .expect("predecessor attach");

        let barrier = Arc::new(Barrier::new(2));
        let detacher = {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                exec_a.detach_nodes();
                exec_a
            })
        };
        let attacher = {
            let node = node.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut exec_b = SingleThreadedExecutor::new(4);
                barrier.wait();
                let attached = exec_b.try_add_node(node).is_ok();
                (exec_b, attached)
            })
        };

        let exec_a = detacher.join().expect("detacher thread");
        let (exec_b, attached) = attacher.join().expect("attacher thread");

        // The invariant: the executor-owned slots move as ONE unit. A node
        // that advertises a live event sender must also carry the timer
        // scheduler installed alongside it.
        assert_eq!(
            node.has_event_sender(),
            node.timer_scheduler().is_some(),
            "round {round}: torn attachment — has_event_sender={} but \
             timer_scheduler present={} (attach {}succeeded)",
            node.has_event_sender(),
            node.timer_scheduler().is_some(),
            if attached { "" } else { "did not " }
        );

        drop(exec_a);
        exec_b.detach_nodes();
        drop(exec_b);
    }

    node.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handoff_is_refused_while_a_snapshot_producer_is_live() {
    use crate::node::Node;
    use std::time::Duration;

    // Subscribers re-validate against the attachment generation,
    // but timers and services keep the sender they snapshotted at
    // construction. A detach + re-attach would therefore split one node's
    // callbacks across two executors — the exact hazard that made silent
    // back-fill the wrong design. The handoff must be refused while such a
    // producer is alive.
    let node = Node::new("exec_attach_handoff_timer", "default")
        .await
        .expect("node");

    let mut exec_a = SingleThreadedExecutor::new(8);
    exec_a.try_add_node(node.clone()).expect("first attach");

    let timer = node
        .create_timer(Duration::from_millis(50), || {})
        .await
        .expect("timer");
    exec_a.detach_nodes();

    let mut exec_b = SingleThreadedExecutor::new(8);
    let err = exec_b
        .try_add_node(node.clone())
        .expect_err("handoff with a live timer must be refused");
    let message = err.to_string();
    assert!(
        message.contains("exec_attach_handoff_timer") && message.contains("timer"),
        "refusal must name the node and the blocking producer kind, got: {message}"
    );

    // Once the producer is gone the handoff is allowed again.
    drop(timer);
    exec_b
        .try_add_node(node.clone())
        .expect("handoff must succeed once no snapshot producer is live");

    exec_b.detach_nodes();
    node.close().await.expect("close");
}

// --- Same-executor reattach vs cross-executor handoff -------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_executor_reattach_after_detach_is_allowed_with_live_producers() {
    use crate::node::Node;
    use std::time::Duration;

    // The live-producer refusal exists to stop a node's callbacks being SPLIT
    // across two executors. Reattaching to the executor the node just left
    // cannot split anything: the timer captured a sender to THIS executor's
    // queue, `self.queue` is never reassigned, and `detach_nodes()` only
    // clears the node's cell — so producer and node end up on the same queue
    // again. Refusing here would strand the node until every timer/service is
    // destroyed, for no safety gain.
    let node = Node::new("exec_reattach_same", "default")
        .await
        .expect("node");
    let mut exec = SingleThreadedExecutor::new(8);
    exec.try_add_node(node.clone()).expect("first attach");

    let _timer = node
        .create_timer(Duration::from_millis(50), || {})
        .await
        .expect("timer");
    assert_eq!(node.live_snapshot_producer_count(), 1);

    exec.detach_nodes();
    assert!(!node.has_event_sender(), "detach must release the node");

    exec.try_add_node(node.clone()).expect(
        "reattaching to the SAME executor must be allowed: the live timer already \
                 holds this executor's queue, so no cross-executor split is possible",
    );
    assert!(node.has_event_sender(), "the node must be attached again");

    exec.detach_nodes();
    node.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_executor_handoff_with_live_producers_is_still_refused() {
    use crate::node::Node;
    use std::time::Duration;

    // The other direction must not regress: a DIFFERENT executor still cannot
    // take a node whose timers/services hold the previous executor's queue.
    let node = Node::new("exec_reattach_cross", "default")
        .await
        .expect("node");
    let mut exec_a = SingleThreadedExecutor::new(8);
    exec_a.try_add_node(node.clone()).expect("first attach");

    let timer = node
        .create_timer(Duration::from_millis(50), || {})
        .await
        .expect("timer");
    exec_a.detach_nodes();

    let mut exec_b = SingleThreadedExecutor::new(8);
    let err = exec_b
        .try_add_node(node.clone())
        .expect_err("a DIFFERENT executor must still be refused while a producer is live");
    assert!(
        err.to_string().contains("exec_reattach_cross") && err.to_string().contains("timer"),
        "refusal must name the node and the blocking producer kind, got: {err}"
    );

    drop(timer);
    exec_b
        .try_add_node(node.clone())
        .expect("handoff must succeed once no snapshot producer is live");

    exec_b.detach_nodes();
    node.close().await.expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_predecessor_cannot_reclaim_a_node_whose_producers_moved_on() {
    use crate::node::Node;
    use std::time::Duration;

    // The trap in "allow reattach if this executor still lists the node":
    // exec_a keeps its `self.nodes` entry forever, so after the node has
    // legitimately moved to exec_b AND a producer captured exec_b's queue,
    // exec_a would look like an "original executor" and be let back in —
    // splitting the node's callbacks exactly the way the refusal exists to
    // prevent. The predicate must be "most recent owner", not "has an entry".
    let node = Node::new("exec_reattach_stale", "default")
        .await
        .expect("node");

    let mut exec_a = SingleThreadedExecutor::new(8);
    exec_a.try_add_node(node.clone()).expect("attach to a");
    exec_a.detach_nodes(); // released with no producers alive

    let mut exec_b = SingleThreadedExecutor::new(8);
    exec_b.try_add_node(node.clone()).expect("handoff to b");

    // This timer captures exec_B's queue.
    let _timer = node
        .create_timer(Duration::from_millis(50), || {})
        .await
        .expect("timer");
    exec_b.detach_nodes();

    // exec_a still has a (stale) entry for this node from its own attachment.
    let err = exec_a.try_add_node(node.clone()).expect_err(
        "a stale predecessor must NOT reclaim a node whose live producers \
                     hold a different executor's queue",
    );
    assert!(
        err.to_string().contains("exec_reattach_stale"),
        "refusal must name the node, got: {err}"
    );

    exec_b.detach_nodes();
    node.close().await.expect("close");
}
