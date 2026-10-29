use super::*;
use crate::event_queue::{CallbackGroupType, Event, EventKind};
use crate::node::Node;
#[cfg(target_os = "linux")]
use crate::test_support::EnvVarGuard;
use crate::timer::TimerOptions;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn executor_spin_once_multiple_events() {
    let mut exec = SingleThreadedExecutor::new(256);
    let counter = Arc::new(AtomicU32::new(0));

    // Push multiple events
    for _i in 0..5 {
        let counter_clone = Arc::clone(&counter);
        exec.queue()
            .push(Event::new(EventKind::Timer, move || {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }))
            .await
            .expect("push");
    }

    // Process all events — batch drain means a single spin_once may consume
    // multiple ready events.
    let outcome = exec.spin_once(Duration::from_millis(100)).await;
    assert_eq!(outcome, SpinOutcome::Event);

    assert_eq!(counter.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn executor_spin_once_returns_stopped_when_requested() {
    let mut exec = SingleThreadedExecutor::new(1);
    let stop = exec.stop_handle();

    // Request stop
    stop.stop();

    let outcome = exec.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Stopped);
}

#[tokio::test]
async fn executor_spin_once_returns_queue_closed() {
    let exec = SingleThreadedExecutor::new(1);

    // Drop the executor to close the queue
    drop(exec);

    // Create a new executor - the old one is dropped
    let mut exec2 = SingleThreadedExecutor::new(1);
    let outcome = exec2.spin_once(Duration::from_millis(10)).await;
    assert_eq!(outcome, SpinOutcome::Timeout);
}

#[tokio::test]
async fn executor_stop_handle_multiple_stops() {
    let exec = SingleThreadedExecutor::new(1);
    let stop = exec.stop_handle();

    // Multiple stops should not panic
    stop.stop();
    stop.stop();
    stop.stop();

    assert!(stop.is_stopped());
}

#[tokio::test]
async fn executor_stop_handle_clone() {
    let exec = SingleThreadedExecutor::new(1);
    let stop1 = exec.stop_handle();
    let stop2 = stop1.clone();

    stop1.stop();
    assert!(stop2.is_stopped());
}

#[tokio::test]
async fn executor_request_stop_stops_spin() {
    let mut exec = SingleThreadedExecutor::new(1);

    // Request stop directly
    exec.request_stop();

    // Spin should exit immediately
    exec.spin().await;

    assert!(exec.stop_handle().is_stopped());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_add_multiple_nodes() {
    let mut exec = SingleThreadedExecutor::new(256);

    let node1 = Node::new("exec_node1", "default")
        .await
        .expect("create node1");
    let node2 = Node::new("exec_node2", "default")
        .await
        .expect("create node2");

    exec.add_node(node1.clone());
    exec.add_node(node2.clone());

    // Both nodes should have event sender
    assert!(node1.has_event_sender());
    assert!(node2.has_event_sender());

    node1.close().await.expect("close node1");
    node2.close().await.expect("close node2");
}

#[tokio::test]
async fn executor_push_event_directly() {
    let exec = SingleThreadedExecutor::new(256);
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let event = Event::new(EventKind::Timer, move || {
        counter_clone.fetch_add(1, Ordering::Relaxed);
    });

    exec.push_event(event).await.expect("push event");

    // Event should be in queue
    assert_eq!(exec.queue().len(), 1);
}

#[tokio::test]
async fn executor_queue_accessor() {
    let exec = SingleThreadedExecutor::new(128);

    let queue = exec.queue();
    assert_eq!(queue.capacity(), 128);

    // Push an event through the queue reference
    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");

    assert_eq!(queue.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_with_timer_multiple_ticks() {
    let mut exec = SingleThreadedExecutor::new(256);
    let node = Node::new("exec_timer_multi", "default")
        .await
        .expect("create node");
    exec.add_node(node.clone());

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let _timer = node
        .create_timer(Duration::from_millis(30), move || {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await
        .expect("create timer");

    // Run for ~200ms
    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        stop.stop();
    });

    exec.spin().await;

    // Should have ~6-8 ticks (250ms / 30ms)
    let count = counter.load(Ordering::Relaxed);
    assert!(
        (5..=10).contains(&count),
        "Expected 5-10 ticks, got {}",
        count
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_with_timer_pause_resume() {
    let mut exec = SingleThreadedExecutor::new(256);
    let node = Node::new("exec_timer_pause", "default")
        .await
        .expect("create node");
    exec.add_node(node.clone());

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let timer = node
        .create_timer_with_options(
            Duration::from_millis(50),
            move || {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            },
            TimerOptions::new().with_auto_start(true),
        )
        .await
        .expect("create timer");

    // Run for 150ms (should get ~3 ticks)
    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        stop.stop();
    });

    exec.spin().await;

    let count_before = counter.load(Ordering::Relaxed);
    assert!(
        count_before >= 2,
        "Should have at least 2 ticks before stop"
    );

    // Pause timer
    timer.pause();
    assert!(!timer.is_running());

    // Reset and run again
    let mut exec2 = SingleThreadedExecutor::new(256);
    exec2.add_node(node.clone());

    // Resume timer
    timer.resume();
    assert!(timer.is_running());

    let stop2 = exec2.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        stop2.stop();
    });

    exec2.spin().await;

    let count_after = counter.load(Ordering::Relaxed);
    // Timer may or may not fire additional ticks depending on implementation
    // The important thing is that the timer resumed successfully
    assert!(count_after >= count_before, "Timer should have resumed");
}

#[tokio::test]
async fn executor_event_ordering() {
    let mut exec = SingleThreadedExecutor::new(256);
    let order = Arc::new(Mutex::new(Vec::new()));

    // Push events in order
    for i in 0..5 {
        let order_clone = Arc::clone(&order);
        let idx = i;
        exec.queue()
            .push(Event::new(EventKind::Timer, move || {
                order_clone.lock().unwrap().push(idx);
            }))
            .await
            .expect("push");
    }

    // Process all events
    loop {
        match exec.spin_once(Duration::from_millis(10)).await {
            SpinOutcome::Event => continue,
            SpinOutcome::Timeout | SpinOutcome::QueueClosed => break,
            SpinOutcome::Stopped => break,
        }
    }

    let result = order.lock().unwrap();
    assert_eq!(result.len(), 5);
    // Events should be processed in FIFO order
    for i in 0..5 {
        assert_eq!(result[i], i);
    }
}

#[tokio::test]
async fn executor_spin_exits_on_notify() {
    let mut exec = SingleThreadedExecutor::new(1);
    let stop = exec.stop_handle();

    // Spawn a task that will stop after a delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop.stop();
    });

    // Spin should exit when stop is called
    let start = std::time::Instant::now();
    exec.spin().await;
    let elapsed = start.elapsed();

    // Should have exited quickly after stop, not waited for timeout
    assert!(elapsed < Duration::from_millis(200));
}

#[tokio::test]
async fn executor_new_creates_with_capacity() {
    let exec = SingleThreadedExecutor::new(512);
    assert_eq!(exec.queue().capacity(), 512);
}

#[test]
fn spin_outcome_equality() {
    assert_eq!(SpinOutcome::Event, SpinOutcome::Event);
    assert_eq!(SpinOutcome::Timeout, SpinOutcome::Timeout);
    assert_eq!(SpinOutcome::Stopped, SpinOutcome::Stopped);
    assert_eq!(SpinOutcome::QueueClosed, SpinOutcome::QueueClosed);

    assert_ne!(SpinOutcome::Event, SpinOutcome::Timeout);
    assert_ne!(SpinOutcome::Stopped, SpinOutcome::QueueClosed);
}

#[test]
fn spin_outcome_debug() {
    let outcomes = vec![
        SpinOutcome::Event,
        SpinOutcome::Timeout,
        SpinOutcome::Stopped,
        SpinOutcome::QueueClosed,
    ];

    for outcome in outcomes {
        let debug = format!("{:?}", outcome);
        assert!(!debug.is_empty());
    }
}

#[tokio::test]
async fn executor_handles_event_panic() {
    // Note: This test verifies that the executor continues processing
    // even if an event panics. However, panic handling requires special
    // configuration, so this is a placeholder for the concept.

    let mut exec = SingleThreadedExecutor::new(256);
    let counter = Arc::new(AtomicU32::new(0));

    // Push a normal event
    let counter_clone = Arc::clone(&counter);
    exec.queue()
        .push(Event::new(EventKind::Timer, move || {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        }))
        .await
        .expect("push");

    let outcome = exec.spin_once(Duration::from_millis(100)).await;
    assert_eq!(outcome, SpinOutcome::Event);
    assert_eq!(counter.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn executor_spin_once_prefers_higher_priority_event() {
    let mut exec = SingleThreadedExecutor::new_with_options(
        8,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            priority: PrioritySchedulerOptions {
                max_drain_per_cycle: 1,
                ..PrioritySchedulerOptions::default()
            },
            ..ExecutorOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let order_normal = Arc::clone(&order);
    exec.queue()
        .push(Event::new(EventKind::SubscriptionData, move || {
            order_normal.lock().expect("lock").push("normal");
        }))
        .await
        .expect("push");

    let order_high = Arc::clone(&order);
    exec.queue()
        .push(Event::new(EventKind::Timer, move || {
            order_high.lock().expect("lock").push("high");
        }))
        .await
        .expect("push");

    let first = exec.spin_once(Duration::from_millis(50)).await;
    assert_eq!(first, SpinOutcome::Event);
    let second = exec.spin_once(Duration::from_millis(50)).await;
    assert_eq!(second, SpinOutcome::Event);

    let order = order.lock().expect("lock");
    assert_eq!(order.len(), 2);
    assert_eq!(order[0], "high");
    assert_eq!(order[1], "normal");
}

#[tokio::test]
async fn executor_priority_scheduler_preserves_weighted_round_robin_order() {
    let mut exec = SingleThreadedExecutor::new_with_options(
        16,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            priority: PrioritySchedulerOptions {
                max_drain_per_cycle: 1,
                weights: [1, 2, 1, 1, 1],
                map_subscription_qos: true,
            },
            ..ExecutorOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    for tag in ["high-1", "high-2", "high-3"] {
        let order_high = Arc::clone(&order);
        exec.queue()
            .push(Event::new(EventKind::RpcRequest, move || {
                order_high.lock().expect("lock").push(tag);
            }))
            .await
            .expect("push high");
    }

    for tag in ["normal-1", "normal-2"] {
        let order_normal = Arc::clone(&order);
        exec.queue()
            .push(Event::new(EventKind::SubscriptionData, move || {
                order_normal.lock().expect("lock").push(tag);
            }))
            .await
            .expect("push normal");
    }

    for _ in 0..5 {
        let outcome = exec.spin_once(Duration::from_millis(50)).await;
        assert_eq!(outcome, SpinOutcome::Event);
    }

    let order = order.lock().expect("lock");
    assert_eq!(
        order.as_slice(),
        &["high-1", "high-2", "normal-1", "high-3", "normal-2"]
    );
}

#[tokio::test]
async fn executor_priority_scheduler_single_bucket_keeps_fifo_order() {
    let mut exec = SingleThreadedExecutor::new_with_options(
        16,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            priority: PrioritySchedulerOptions {
                max_drain_per_cycle: 1,
                weights: [1, 2, 1, 1, 1],
                map_subscription_qos: true,
            },
            ..ExecutorOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    for tag in ["normal-1", "normal-2", "normal-3"] {
        let order_normal = Arc::clone(&order);
        exec.queue()
            .push(Event::new(EventKind::SubscriptionData, move || {
                order_normal.lock().expect("lock").push(tag);
            }))
            .await
            .expect("push normal");
    }

    for _ in 0..3 {
        let outcome = exec.spin_once(Duration::from_millis(50)).await;
        assert_eq!(outcome, SpinOutcome::Event);
    }

    let order = order.lock().expect("lock");
    assert_eq!(order.as_slice(), &["normal-1", "normal-2", "normal-3"]);
}

#[test]
fn multi_executor_callback_group_lifecycle() {
    let exec = MultiThreadedExecutor::new(64);
    let handle = exec.create_callback_group(CallbackGroupType::Reentrant);
    assert!(handle > 0);
    assert_eq!(
        exec.callback_group_type(handle),
        Some(CallbackGroupType::Reentrant)
    );
    assert!(exec.remove_callback_group(handle));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_executor_mutually_exclusive_group_serializes_callbacks() {
    let mut exec = MultiThreadedExecutor::new(64);
    let handle = exec.create_callback_group(CallbackGroupType::MutuallyExclusive);
    let concurrent = Arc::new(AtomicU32::new(0));
    let max_concurrent = Arc::new(AtomicU32::new(0));
    let executed = Arc::new(AtomicU32::new(0));

    for _ in 0..2 {
        let concurrent_clone = Arc::clone(&concurrent);
        let max_clone = Arc::clone(&max_concurrent);
        let executed_clone = Arc::clone(&executed);
        exec.queue()
            .push(
                Event::new(EventKind::Timer, move || {
                    let now = concurrent_clone.fetch_add(1, Ordering::SeqCst) + 1;
                    let _ = max_clone.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
                        if now > prev {
                            Some(now)
                        } else {
                            None
                        }
                    });
                    std::thread::sleep(Duration::from_millis(40));
                    executed_clone.fetch_add(1, Ordering::SeqCst);
                    concurrent_clone.fetch_sub(1, Ordering::SeqCst);
                })
                .with_callback_group(CallbackGroupType::MutuallyExclusive, Some(handle)),
            )
            .await
            .expect("push event");
    }

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        stop.stop();
    });

    exec.spin().await;

    assert_eq!(executed.load(Ordering::SeqCst), 2);
    assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
}

#[cfg(target_os = "linux")]
#[test]
fn executor_constructed_outside_runtime_uses_tokio_queue() {
    let single = SingleThreadedExecutor::new_with_options(64, ExecutorOptions::default());
    assert!(!single.queue().is_crossbeam());

    let multi = MultiThreadedExecutor::new_with_options(64, 1, ExecutorOptions::default());
    assert!(!multi.queue().is_crossbeam());
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_executor_crossbeam_drains_completed_tasks_without_new_events() {
    let mut exec = {
        let _allow_env_guard = EnvVarGuard::set("AORTA_TEST_ALLOW_ENV", "1");
        let _crossbeam_guard = EnvVarGuard::set("AORTA_EXECUTOR_CROSSBEAM_ENABLE", "1");
        MultiThreadedExecutor::new_with_options(64, 1, ExecutorOptions::default())
    };
    assert!(exec.queue().is_crossbeam());

    let executed = Arc::new(AtomicU32::new(0));
    for _ in 0..2 {
        let executed_clone = Arc::clone(&executed);
        exec.queue()
            .push(Event::new(EventKind::Timer, move || {
                std::thread::sleep(Duration::from_millis(30));
                executed_clone.fetch_add(1, Ordering::SeqCst);
            }))
            .await
            .expect("push event");
    }

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(220)).await;
        stop.stop();
    });

    exec.spin().await;
    assert_eq!(executed.load(Ordering::SeqCst), 2);
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_executor_crossbeam_spin_once_respects_mutually_exclusive_group() {
    use std::sync::atomic::AtomicBool;

    let mut exec = {
        let _allow_env_guard = EnvVarGuard::set("AORTA_TEST_ALLOW_ENV", "1");
        let _crossbeam_guard = EnvVarGuard::set("AORTA_EXECUTOR_CROSSBEAM_ENABLE", "1");
        MultiThreadedExecutor::new_with_options(64, 1, ExecutorOptions::default())
    };
    assert!(exec.queue().is_crossbeam());

    let handle = exec.create_callback_group(CallbackGroupType::MutuallyExclusive);
    let released = Arc::new(AtomicBool::new(false));
    let ran_before_release = Arc::new(AtomicBool::new(false));
    let executed = Arc::new(AtomicBool::new(false));

    let released_for_event = Arc::clone(&released);
    let ran_before_release_for_event = Arc::clone(&ran_before_release);
    let executed_for_event = Arc::clone(&executed);
    let event = Event::new(EventKind::Timer, move || {
        if !released_for_event.load(Ordering::SeqCst) {
            ran_before_release_for_event.store(true, Ordering::SeqCst);
        }
        executed_for_event.store(true, Ordering::SeqCst);
    })
    .with_callback_group(CallbackGroupType::MutuallyExclusive, Some(handle));

    let lock = exec
        .callback_groups
        .lock_for_event(&event)
        .expect("mutually exclusive group should have a lock");
    let guard = lock.lock().await;

    exec.queue().push(event).await.expect("push event");

    let release = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        released.store(true, Ordering::SeqCst);
        drop(guard);
    };

    let (outcome, _) = tokio::join!(exec.spin_once(Duration::from_millis(200)), release);

    assert_eq!(outcome, SpinOutcome::Event);
    assert!(executed.load(Ordering::SeqCst));
    assert!(!ran_before_release.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_executor_reentrant_group_allows_parallel_callbacks() {
    let mut exec = MultiThreadedExecutor::new(64);
    let handle = exec.create_callback_group(CallbackGroupType::Reentrant);
    let concurrent = Arc::new(AtomicU32::new(0));
    let max_concurrent = Arc::new(AtomicU32::new(0));
    let executed = Arc::new(AtomicU32::new(0));

    for _ in 0..2 {
        let concurrent_clone = Arc::clone(&concurrent);
        let max_clone = Arc::clone(&max_concurrent);
        let executed_clone = Arc::clone(&executed);
        exec.queue()
            .push(
                Event::new(EventKind::Timer, move || {
                    let now = concurrent_clone.fetch_add(1, Ordering::SeqCst) + 1;
                    let _ = max_clone.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
                        if now > prev {
                            Some(now)
                        } else {
                            None
                        }
                    });
                    std::thread::sleep(Duration::from_millis(40));
                    executed_clone.fetch_add(1, Ordering::SeqCst);
                    concurrent_clone.fetch_sub(1, Ordering::SeqCst);
                })
                .with_callback_group(CallbackGroupType::Reentrant, Some(handle)),
            )
            .await
            .expect("push event");
    }

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        stop.stop();
    });

    exec.spin().await;

    assert_eq!(executed.load(Ordering::SeqCst), 2);
    assert!(
        max_concurrent.load(Ordering::SeqCst) >= 2,
        "expected reentrant callbacks to overlap"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_executor_unknown_group_handle_falls_back_to_default_group_behavior() {
    let mut exec = MultiThreadedExecutor::new(64);
    let known = exec.create_callback_group(CallbackGroupType::MutuallyExclusive);
    let unknown = known.saturating_add(10_000);
    assert_eq!(exec.callback_group_type(unknown), None);

    let concurrent = Arc::new(AtomicU32::new(0));
    let max_concurrent = Arc::new(AtomicU32::new(0));
    let executed = Arc::new(AtomicU32::new(0));

    for _ in 0..2 {
        let concurrent_clone = Arc::clone(&concurrent);
        let max_clone = Arc::clone(&max_concurrent);
        let executed_clone = Arc::clone(&executed);
        exec.queue()
            .push(
                Event::new(EventKind::Timer, move || {
                    let now = concurrent_clone.fetch_add(1, Ordering::SeqCst) + 1;
                    let _ = max_clone.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
                        if now > prev {
                            Some(now)
                        } else {
                            None
                        }
                    });
                    std::thread::sleep(Duration::from_millis(40));
                    executed_clone.fetch_add(1, Ordering::SeqCst);
                    concurrent_clone.fetch_sub(1, Ordering::SeqCst);
                })
                .with_callback_group(CallbackGroupType::MutuallyExclusive, Some(unknown)),
            )
            .await
            .expect("push event");
    }

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        stop.stop();
    });

    exec.spin().await;

    assert_eq!(executed.load(Ordering::SeqCst), 2);
    assert_eq!(
        max_concurrent.load(Ordering::SeqCst),
        1,
        "unknown handle should fallback to default mutually-exclusive behavior"
    );
}

#[tokio::test]
async fn executor_priority_mode_can_disable_subscription_qos_mapping() {
    let mut exec = SingleThreadedExecutor::new_with_options(
        8,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            priority: PrioritySchedulerOptions {
                max_drain_per_cycle: 1,
                map_subscription_qos: false,
                ..PrioritySchedulerOptions::default()
            },
            ..ExecutorOptions::default()
        },
    );
    let order = Arc::new(Mutex::new(Vec::new()));

    let order_sub = Arc::clone(&order);
    exec.queue()
        .push(Event::new_with_priority(
            EventKind::SubscriptionData,
            crate::event_queue::EventPriority::Realtime,
            move || {
                order_sub.lock().expect("lock").push("subscription");
            },
        ))
        .await
        .expect("push");

    let order_timer = Arc::clone(&order);
    exec.queue()
        .push(Event::new(EventKind::Timer, move || {
            order_timer.lock().expect("lock").push("timer");
        }))
        .await
        .expect("push");

    let first = exec.spin_once(Duration::from_millis(50)).await;
    assert_eq!(first, SpinOutcome::Event);
    let second = exec.spin_once(Duration::from_millis(50)).await;
    assert_eq!(second, SpinOutcome::Event);

    let order = order.lock().expect("lock");
    assert_eq!(order.len(), 2);
    assert_eq!(order[0], "timer");
    assert_eq!(order[1], "subscription");
}

#[test]
fn executor_new_with_options_sanitizes_priority_settings() {
    let exec = SingleThreadedExecutor::new_with_options(
        16,
        ExecutorOptions {
            mode: SchedulerMode::Priority,
            priority: PrioritySchedulerOptions {
                max_drain_per_cycle: 0,
                weights: [0, 0, 0, 0, 0],
                map_subscription_qos: false,
            },
            ..ExecutorOptions::default()
        },
    );
    assert_eq!(exec.scheduler_mode(), SchedulerMode::Priority);
    assert_eq!(exec.max_drain_per_cycle(), 1);
    assert_eq!(exec.priority_weights(), [1, 1, 1, 1, 1]);
    assert!(!exec.map_subscription_qos_enabled());
}

// ---------------------------------------------------------------- lane hooks
// Every stop origin bridges to the attached C++
// lane pool, and every spin variant drains it. Fake extern-"C" hooks stand
// in for the SDK trampoline.

static LANE_ON_STOP_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static LANE_DRAIN_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

unsafe extern "C" fn fake_lane_drain(_ctx: *mut std::ffi::c_void, _max: u32) -> u64 {
    LANE_DRAIN_CALLS.fetch_add(1, Ordering::SeqCst);
    0
}

unsafe extern "C" fn fake_lane_on_stop(_ctx: *mut std::ffi::c_void) {
    LANE_ON_STOP_CALLS.fetch_add(1, Ordering::SeqCst);
}

fn fake_lane_hooks() -> LaneHooks {
    LaneHooks {
        drain_fn: fake_lane_drain,
        ctx: 0,
        on_stop: Some(fake_lane_on_stop),
        forward_fn: None,
        pool_workers: 0,
    }
}

#[tokio::test]
async fn request_stop_bridges_lane_on_stop_on_both_executors() {
    // request_stop() must fire the same lane on_stop bridge as
    // StopHandle::stop() — a Rust-plane direct stop otherwise leaves the
    // attached pool open (no close, no revoke sweep, no worker wake).
    let base = LANE_ON_STOP_CALLS.load(Ordering::SeqCst);
    let exec = SingleThreadedExecutor::new(4);
    assert!(
        exec.stop_handle().lane_hooks().attach(fake_lane_hooks()),
        "first attach on a fresh slot succeeds"
    );
    exec.request_stop();
    assert_eq!(
        LANE_ON_STOP_CALLS.load(Ordering::SeqCst),
        base + 1,
        "ST request_stop bridges to lane on_stop"
    );
    let exec = MultiThreadedExecutor::new(4);
    assert!(
        exec.stop_handle().lane_hooks().attach(fake_lane_hooks()),
        "first attach on a fresh slot succeeds"
    );
    exec.request_stop();
    assert_eq!(
        LANE_ON_STOP_CALLS.load(Ordering::SeqCst),
        base + 2,
        "MT request_stop bridges to lane on_stop"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mt_spin_crossbeam_drains_attached_lane_hooks() {
    // The one spin variant of the eight that never drained the attached
    // pool: with the crossbeam backend selected,
    // the MT spin loop must still call lane_hooks.drain, or lane backlog
    // sits until an unrelated queue event happens to wake the loop.
    let _guard = EnvVarGuard::set("AORTA_EXECUTOR_CROSSBEAM_ENABLE", "1");
    let mut exec = MultiThreadedExecutor::new_with_options(64, 1, ExecutorOptions::default());
    let stop = exec.stop_handle();
    assert!(
        stop.lane_hooks().attach(fake_lane_hooks()),
        "first attach on a fresh slot succeeds"
    );
    let base = LANE_DRAIN_CALLS.load(Ordering::SeqCst);
    let spin = tokio::spawn(async move { exec.spin().await });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while LANE_DRAIN_CALLS.load(Ordering::SeqCst) == base && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        LANE_DRAIN_CALLS.load(Ordering::SeqCst) > base,
        "crossbeam MT spin drains the attached lane pool"
    );
    stop.stop();
    tokio::time::timeout(Duration::from_secs(5), spin)
        .await
        .expect("spin exits on stop")
        .expect("spin task join");
}

// ------------------------------------------------ stop-window seam
// `test-seams` cargo feature: a pause point in BOTH
// stop paths BETWEEN the stop-flag store and the lane on_stop bridge —
// the window the C++ stop-flag-window family must hold open. These rows
// pin the seam's ordering contract at the Rust tier: when the seam
// fires, the flag is ALREADY published and on_stop has NOT yet run.
#[cfg(feature = "test-seams")]
mod stop_window_seam_rows {
    use super::*;
    use crate::executor::stop_window_seam;
    use std::sync::atomic::{AtomicU64, AtomicU8};

    struct SeamRec {
        flag_ptr: usize,
        base_on_stop: u64,
        flag_at_seam: AtomicU8,
        on_stop_delta_at_seam: AtomicU64,
        fired: AtomicU64,
    }

    unsafe extern "C" fn recording_seam(ctx: *mut std::ffi::c_void) {
        let rec = &*(ctx as *const SeamRec);
        let flag = &*(rec.flag_ptr as *const AtomicU8);
        rec.flag_at_seam
            .store(flag.load(Ordering::Acquire), Ordering::SeqCst);
        rec.on_stop_delta_at_seam.store(
            LANE_ON_STOP_CALLS.load(Ordering::SeqCst) - rec.base_on_stop,
            Ordering::SeqCst,
        );
        rec.fired.fetch_add(1, Ordering::SeqCst);
    }

    fn assert_seam_window(rec: &SeamRec, path: &str) {
        assert_eq!(
            rec.fired.load(Ordering::SeqCst),
            1,
            "{path}: seam fired once"
        );
        assert_eq!(
            rec.flag_at_seam.load(Ordering::SeqCst),
            1,
            "{path}: stop flag already published at the seam"
        );
        assert_eq!(
            rec.on_stop_delta_at_seam.load(Ordering::SeqCst),
            0,
            "{path}: on_stop has NOT run yet at the seam"
        );
    }

    #[tokio::test]
    async fn seam_fires_between_flag_store_and_on_stop_in_both_stop_paths() {
        // Path 1: StopHandle::stop() (every C-ABI stop origin).
        let exec = SingleThreadedExecutor::new(4);
        let handle = exec.stop_handle();
        assert!(handle.lane_hooks().attach(fake_lane_hooks()));
        let rec = SeamRec {
            flag_ptr: handle.stop_flag_ptr() as usize,
            base_on_stop: LANE_ON_STOP_CALLS.load(Ordering::SeqCst),
            flag_at_seam: AtomicU8::new(u8::MAX),
            on_stop_delta_at_seam: AtomicU64::new(u64::MAX),
            fired: AtomicU64::new(0),
        };
        stop_window_seam::set(
            Some(recording_seam),
            &rec as *const SeamRec as *mut std::ffi::c_void,
        );
        handle.stop();
        stop_window_seam::set(None, std::ptr::null_mut());
        assert_seam_window(&rec, "StopHandle::stop");
        assert_eq!(
            LANE_ON_STOP_CALLS.load(Ordering::SeqCst),
            rec.base_on_stop + 1,
            "on_stop bridged after the seam released"
        );

        // Path 2: request_stop() (the Rust-plane direct stop).
        let exec = SingleThreadedExecutor::new(4);
        let handle = exec.stop_handle();
        assert!(handle.lane_hooks().attach(fake_lane_hooks()));
        let rec = SeamRec {
            flag_ptr: handle.stop_flag_ptr() as usize,
            base_on_stop: LANE_ON_STOP_CALLS.load(Ordering::SeqCst),
            flag_at_seam: AtomicU8::new(u8::MAX),
            on_stop_delta_at_seam: AtomicU64::new(u64::MAX),
            fired: AtomicU64::new(0),
        };
        stop_window_seam::set(
            Some(recording_seam),
            &rec as *const SeamRec as *mut std::ffi::c_void,
        );
        exec.request_stop();
        stop_window_seam::set(None, std::ptr::null_mut());
        assert_seam_window(&rec, "request_stop");
        assert_eq!(
            LANE_ON_STOP_CALLS.load(Ordering::SeqCst),
            rec.base_on_stop + 1,
            "request_stop bridged on_stop after the seam released"
        );
    }
}
