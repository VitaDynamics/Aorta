use super::{
    event_priority_from_index, event_priority_from_index_lossy, lane_class_for_priority,
    priority_index, AdmissionDropReason, AdmissionState, Event, EventKind, EventPriority,
    EventQueue, EventQueueOptions, EventQueueSender, InflightGuard, MultiIngressOptions,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

// ---- Priority index / lane-class drift guard ----

/// Pins the FULL literal priority-mapping tables (the authoritative
/// table). The C++ twin of the lane fold is pinned by
/// `LaneRuntime.ClassMappingMatchesSpecPriorityTable` /
/// `.ClassByteMappingMatchesSpecPriorityTable` in
/// cpp/test/lane_runtime_test.cc — the two implementations must agree
/// forever; change one only together with the other AND these literal tables.
#[test]
fn priority_index_and_lane_class_tables_pinned() {
    // EventPriority → index (0..=4): the C-ABI priority byte + WRR bucket id.
    assert_eq!(priority_index(EventPriority::Realtime), 0);
    assert_eq!(priority_index(EventPriority::High), 1);
    assert_eq!(priority_index(EventPriority::Normal), 2);
    assert_eq!(priority_index(EventPriority::Low), 3);
    assert_eq!(priority_index(EventPriority::Background), 4);

    // Strict inverse: 0..=4 decode, everything else (incl. the 255 inherit
    // sentinel) is None — sentinel decoding belongs to the call sites.
    assert_eq!(event_priority_from_index(0), Some(EventPriority::Realtime));
    assert_eq!(event_priority_from_index(1), Some(EventPriority::High));
    assert_eq!(event_priority_from_index(2), Some(EventPriority::Normal));
    assert_eq!(event_priority_from_index(3), Some(EventPriority::Low));
    assert_eq!(
        event_priority_from_index(4),
        Some(EventPriority::Background)
    );
    assert_eq!(event_priority_from_index(5), None);
    assert_eq!(event_priority_from_index(254), None);
    assert_eq!(event_priority_from_index(255), None);

    // Lossy inverse: identical on 0..=4, out-of-range clamps to Normal.
    assert_eq!(event_priority_from_index_lossy(0), EventPriority::Realtime);
    assert_eq!(event_priority_from_index_lossy(1), EventPriority::High);
    assert_eq!(event_priority_from_index_lossy(2), EventPriority::Normal);
    assert_eq!(event_priority_from_index_lossy(3), EventPriority::Low);
    assert_eq!(
        event_priority_from_index_lossy(4),
        EventPriority::Background
    );
    assert_eq!(event_priority_from_index_lossy(5), EventPriority::Normal);
    assert_eq!(event_priority_from_index_lossy(255), EventPriority::Normal);

    // The 5→4 lane-class fold: Background ≡ Low at the lane plane (class 3);
    // the Rust core-queue plane keeps all 5 WRR buckets.
    assert_eq!(lane_class_for_priority(EventPriority::Realtime), 0);
    assert_eq!(lane_class_for_priority(EventPriority::High), 1);
    assert_eq!(lane_class_for_priority(EventPriority::Normal), 2);
    assert_eq!(lane_class_for_priority(EventPriority::Low), 3);
    assert_eq!(lane_class_for_priority(EventPriority::Background), 3);
}

// ---- Tier A `InflightGuard` accounting ----

#[test]
fn inflight_guard_decrements_on_drop() {
    let counter = Arc::new(AtomicUsize::new(1));
    {
        let _guard = InflightGuard::new(Arc::clone(&counter));
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "guard drop must decrement the in-flight counter exactly once"
    );
}

#[test]
fn inflight_guard_decrements_when_event_runs() {
    let counter = Arc::new(AtomicUsize::new(1));
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_cb = Arc::clone(&ran);
    let event = Event::new(EventKind::SubscriptionData, move || {
        ran_cb.fetch_add(1, Ordering::SeqCst);
    })
    .with_inflight_guard(Some(InflightGuard::new(Arc::clone(&counter))));
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    event.run();
    assert_eq!(ran.load(Ordering::SeqCst), 1, "callback must run");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "running an event must drop its guard and decrement the counter"
    );
}

#[test]
fn inflight_guard_decrements_when_event_dropped_unrun() {
    let counter = Arc::new(AtomicUsize::new(1));
    let event = Event::new(EventKind::SubscriptionData, || {})
        .with_inflight_guard(Some(InflightGuard::new(Arc::clone(&counter))));
    assert_eq!(counter.load(Ordering::Relaxed), 1);
    drop(event); // e.g. a Full/Closed rejection — never runs
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "dropping an unrun event must still release its in-flight slot"
    );
}

#[test]
fn inflight_guard_decrements_when_event_panics() {
    let counter = Arc::new(AtomicUsize::new(1));
    let event = Event::new(EventKind::SubscriptionData, || panic!("boom"))
        .with_inflight_guard(Some(InflightGuard::new(Arc::clone(&counter))));
    // `run()` isolates the panic and returns true; the guard still drops.
    let panicked = event.run();
    assert!(panicked, "panic must be isolated and reported");
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "a panicking callback must still release its in-flight slot"
    );
}

#[tokio::test]
async fn event_queue_preserves_order() {
    let mut queue = EventQueue::new(4);
    queue
        .push(Event::new(EventKind::SubscriptionData, || {}))
        .await
        .expect("push");
    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");

    let first = queue.pop().await.expect("event");
    assert_eq!(first.kind(), EventKind::SubscriptionData);
    let second = queue.pop().await.expect("event");
    assert_eq!(second.kind(), EventKind::Timer);
}

#[tokio::test]
async fn event_queue_runs_callback() {
    let mut queue = EventQueue::new(1);
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);
    queue
        .push(Event::new(EventKind::Timer, move || {
            *flag_clone.lock().expect("lock") = true;
        }))
        .await
        .expect("push");

    let event = queue.pop().await.expect("event");
    event.run();
    assert!(*flag.lock().expect("lock"));
}

#[tokio::test]
async fn event_queue_try_push_rejects_full() {
    let queue = EventQueue::new(1);
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect("try_push");
    let err = queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect_err("queue should reject full");
    assert!(matches!(err, crate::error::AortaError::Busy(_)));
}

#[tokio::test]
async fn event_queue_timeout_empty() {
    let mut queue = EventQueue::new(1);
    let event = queue.pop_timeout(Duration::from_millis(10)).await;
    assert!(event.is_none());
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossbeam_sender_send_waits_for_capacity() {
    let mut queue = EventQueue::new_crossbeam(1);
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect("fill queue");

    let sender = queue.sender();
    let send_task = tokio::spawn(async move {
        sender
            .send(Event::new(EventKind::SubscriptionData, || {}))
            .await
            .expect("send should wait for capacity instead of failing when full");
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !send_task.is_finished(),
        "send should still be waiting while the crossbeam queue is full"
    );

    let _ = queue.pop().await.expect("first queued event");
    send_task.await.expect("send task");
    assert_eq!(queue.len(), 1);

    let _ = queue.pop().await.expect("second queued event");
    assert_eq!(queue.len(), 0);
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossbeam_sender_send_reports_closed_queue() {
    let queue = EventQueue::new_crossbeam(1);
    let sender = queue.sender();
    drop(queue);

    let err = sender
        .send(Event::new(EventKind::SubscriptionData, || {}))
        .await
        .expect_err("send should fail once the queue is closed");
    assert_eq!(err.0.kind(), EventKind::SubscriptionData);
}

#[cfg(target_os = "linux")]
#[test]
fn crossbeam_waker_unblocks_sync_timeout() {
    let mut queue = EventQueue::new_crossbeam(1);
    let waker = queue.waker().expect("crossbeam queue has an eventfd waker");
    let started = Instant::now();

    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        waker.wake();
    });

    assert!(queue.pop_timeout_sync(Duration::from_secs(2)).is_none());
    thread.join().expect("waker thread");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "waker should interrupt the eventfd wait before timeout"
    );
}

#[tokio::test]
async fn event_queue_len_tracks_items() {
    let mut queue = EventQueue::new(2);
    assert_eq!(queue.len(), 0);
    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");
    assert_eq!(queue.len(), 1);
    let _ = queue.pop().await.expect("event");
    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn event_queue_unbounded_creation() {
    let queue = EventQueue::new_unbounded();
    assert_eq!(queue.capacity(), 0);
    assert!(queue.is_unbounded());
}

#[tokio::test]
async fn event_queue_unbounded_no_backpressure() {
    let queue = EventQueue::new_unbounded();

    for i in 0..1000 {
        queue
            .push(Event::new(EventKind::Timer, move || {
                let _ = i;
            }))
            .await
            .expect("push should succeed on unbounded queue");
    }
}

#[tokio::test]
async fn event_queue_unbounded_preserves_order() {
    let mut queue = EventQueue::new_unbounded();

    queue
        .push(Event::new(EventKind::SubscriptionData, || {}))
        .await
        .expect("push");
    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");
    queue
        .push(Event::new(EventKind::RpcRequest, || {}))
        .await
        .expect("push");

    let first = queue.pop().await.expect("event");
    assert_eq!(first.kind(), EventKind::SubscriptionData);
    let second = queue.pop().await.expect("event");
    assert_eq!(second.kind(), EventKind::Timer);
    let third = queue.pop().await.expect("event");
    assert_eq!(third.kind(), EventKind::RpcRequest);
}

#[tokio::test]
async fn event_queue_unbounded_runs_callbacks() {
    let mut queue = EventQueue::new_unbounded();
    let counter = Arc::new(Mutex::new(0));

    for _ in 0..10 {
        let counter_clone = Arc::clone(&counter);
        queue
            .push(Event::new(EventKind::Timer, move || {
                *counter_clone.lock().expect("lock") += 1;
            }))
            .await
            .expect("push");
    }

    for _ in 0..10 {
        let event = queue.pop().await.expect("event");
        event.run();
    }

    assert_eq!(*counter.lock().expect("lock"), 10);
}

#[tokio::test]
async fn event_queue_bounded_capacity_check() {
    let queue = EventQueue::new(5);
    assert_eq!(queue.capacity(), 5);
    assert!(!queue.is_unbounded());
}

#[tokio::test]
async fn event_queue_zero_capacity_panics() {
    let result = std::panic::catch_unwind(|| {
        let _ = EventQueue::new(0);
    });
    assert!(result.is_err());
}

#[test]
fn event_default_priority_mapping_is_stable() {
    assert_eq!(
        Event::new(EventKind::Timer, || {}).priority(),
        EventPriority::Realtime
    );
    assert_eq!(
        Event::new(EventKind::RpcRequest, || {}).priority(),
        EventPriority::High
    );
    assert_eq!(
        Event::new(EventKind::RpcResponse, || {}).priority(),
        EventPriority::High
    );
    assert_eq!(
        Event::new(EventKind::ClockUpdate, || {}).priority(),
        EventPriority::High
    );
    assert_eq!(
        Event::new(EventKind::SubscriptionData, || {}).priority(),
        EventPriority::Normal
    );
    assert_eq!(
        Event::new(EventKind::CoroutineResume, || {}).priority(),
        EventPriority::High
    );
}

#[test]
fn event_can_override_priority_explicitly() {
    let event = Event::new_with_priority(EventKind::SubscriptionData, EventPriority::Low, || {});
    assert_eq!(event.priority(), EventPriority::Low);
}

#[test]
fn multi_ingress_sender_can_be_created_without_runtime() {
    let sink = EventQueue::new(4);
    let sender =
        EventQueueSender::try_new_multi_ingress(sink.sender(), MultiIngressOptions::default())
            .expect("multi-ingress sender should support non-runtime creation");
    sender
        .try_send(Event::new(EventKind::SubscriptionData, || {}))
        .expect("try_send via multi-ingress");

    let mut sink = sink;
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let event = runtime.block_on(async { sink.pop_timeout(Duration::from_millis(200)).await });
    assert!(event.is_some(), "bridged event should reach main queue");
}

#[tokio::test]
async fn multi_ingress_sender_bridges_events_to_main_queue() {
    let mut sink = EventQueue::new(16);
    let sender = EventQueueSender::try_new_multi_ingress(
        sink.sender(),
        MultiIngressOptions::with_capacity(4),
    )
    .expect("create multi-ingress sender");

    sender
        .send(Event::new(EventKind::SubscriptionData, || {}))
        .await
        .expect("send subscription event");
    sender
        .send(Event::new(EventKind::RpcRequest, || {}))
        .await
        .expect("send rpc request event");
    sender
        .send(Event::new(EventKind::Timer, || {}))
        .await
        .expect("send timer event");

    let mut kinds = vec![];
    for _ in 0..3 {
        let event = sink
            .pop_timeout(Duration::from_millis(200))
            .await
            .expect("bridged event");
        kinds.push(event.kind());
    }

    assert!(kinds.contains(&EventKind::SubscriptionData));
    assert!(kinds.contains(&EventKind::RpcRequest));
    assert!(kinds.contains(&EventKind::Timer));
}

#[tokio::test]
async fn direct_multi_ingress_queue_routes_events_without_bridge_sender() {
    let mut queue = EventQueue::new_multi_ingress(4);
    let sender = queue.sender();

    sender
        .send(Event::new(EventKind::SubscriptionData, || {}))
        .await
        .expect("send sub_data");
    sender
        .send(Event::new(EventKind::RpcRequest, || {}))
        .await
        .expect("send rpc_request");
    sender
        .send(Event::new(EventKind::Timer, || {}))
        .await
        .expect("send timer");

    let mut kinds = vec![];
    for _ in 0..3 {
        let event = queue
            .pop_timeout(Duration::from_millis(200))
            .await
            .expect("event should be delivered from direct multi-ingress queue");
        kinds.push(event.kind());
    }

    assert!(kinds.contains(&EventKind::SubscriptionData));
    assert!(kinds.contains(&EventKind::RpcRequest));
    assert!(kinds.contains(&EventKind::Timer));
}

#[tokio::test]
async fn admission_stats_track_queued_and_peaks() {
    let mut queue = EventQueue::new_with_options(
        4,
        EventQueueOptions {
            max_queued_events: 0,
            max_queued_bytes: 0,
            admission_observability: true,
        },
    );
    queue
        .push(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(128))
        .await
        .expect("push");
    queue
        .push(Event::new(EventKind::RpcRequest, || {}).with_estimated_bytes(64))
        .await
        .expect("push");

    let stats = queue.admission_stats();
    assert_eq!(stats.queued_events, 2);
    assert_eq!(stats.queued_bytes, 192);
    assert_eq!(stats.peak_queued_events, 2);
    assert_eq!(stats.peak_queued_bytes, 192);

    let _ = queue.pop().await.expect("event");
    let _ = queue.pop().await.expect("event");
    let drained = queue.admission_stats();
    assert_eq!(drained.queued_events, 0);
    assert_eq!(drained.queued_bytes, 0);
}

#[tokio::test]
async fn admission_reader_tracks_queue_lifecycle() {
    let mut queue = EventQueue::new_with_options(
        4,
        EventQueueOptions {
            max_queued_events: 0,
            max_queued_bytes: 0,
            admission_observability: true,
        },
    );
    let reader = queue.admission_reader();

    queue
        .push(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(64))
        .await
        .expect("push");
    let mid = reader.snapshot();
    assert_eq!(mid.queued_events, 1);
    assert_eq!(mid.queued_bytes, 64);
    assert_eq!(mid.peak_queued_events, 1);
    assert_eq!(mid.peak_queued_bytes, 64);

    let _ = queue.pop().await.expect("pop");
    let end = reader.snapshot();
    assert_eq!(end.queued_events, 0);
    assert_eq!(end.queued_bytes, 0);
    assert_eq!(end.peak_queued_events, 1);
    assert_eq!(end.peak_queued_bytes, 64);
}

#[tokio::test]
async fn admission_budget_events_rejects_try_push() {
    let queue = EventQueue::new_with_options(
        8,
        EventQueueOptions {
            max_queued_events: 1,
            max_queued_bytes: 0,
            admission_observability: true,
        },
    );
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(1))
        .expect("first push should pass");
    let err = queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(1))
        .expect_err("second push should be blocked by max_queued_events");
    assert!(matches!(err, crate::error::AortaError::Busy(_)));

    let stats = queue.admission_stats();
    assert_eq!(stats.dropped_budget_events, 1);
    assert_eq!(stats.dropped_budget_bytes, 0);
}

#[tokio::test]
async fn admission_budget_bytes_rejects_try_push() {
    let queue = EventQueue::new_with_options(
        8,
        EventQueueOptions {
            max_queued_events: 0,
            max_queued_bytes: 100,
            admission_observability: true,
        },
    );
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(80))
        .expect("first push should pass");
    let err = queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(40))
        .expect_err("second push should be blocked by max_queued_bytes");
    assert!(matches!(err, crate::error::AortaError::Busy(_)));

    let stats = queue.admission_stats();
    assert_eq!(stats.dropped_budget_events, 0);
    assert_eq!(stats.dropped_budget_bytes, 1);
}

#[tokio::test]
async fn admission_observability_can_be_disabled() {
    let queue = EventQueue::new_with_options(
        4,
        EventQueueOptions {
            max_queued_events: 0,
            max_queued_bytes: 0,
            admission_observability: false,
        },
    );

    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(16))
        .expect("push should pass");
    let stats = queue.admission_stats();
    assert_eq!(stats.queued_events, 1);
    assert_eq!(stats.queued_bytes, 16);
    assert_eq!(stats.peak_queued_events, 0);
    assert_eq!(stats.peak_queued_bytes, 0);
    assert_eq!(stats.dropped_queue_full, 0);
    assert_eq!(stats.dropped_budget_events, 0);
}

#[test]
fn on_dequeued_underflow_does_not_wrap_event_counter() {
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 10,
        max_queued_bytes: 0,
        admission_observability: true,
    });

    // Counter starts at 0; calling on_dequeued must not wrap to usize::MAX.
    state.on_dequeued(0);

    let stats = state.snapshot();
    assert_eq!(
        stats.queued_events, 0,
        "queued_events must stay 0 after underflow, not wrap to usize::MAX"
    );

    // Admission must still accept new events after the spurious dequeue.
    assert!(
        state.try_reserve(0).is_ok(),
        "try_reserve must succeed when queue is empty (queued_events == 0)"
    );
}

#[test]
fn on_dequeued_underflow_does_not_wrap_byte_counter() {
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 0,
        max_queued_bytes: 1024,
        admission_observability: true,
    });

    // Counter starts at 0; calling on_dequeued with bytes must not wrap.
    state.on_dequeued(128);

    let stats = state.snapshot();
    assert_eq!(
        stats.queued_bytes, 0,
        "queued_bytes must stay 0 after underflow, not wrap"
    );

    // Admission must still accept new events after the spurious dequeue.
    assert!(
        state.try_reserve(512).is_ok(),
        "try_reserve must succeed when byte budget is not exhausted"
    );
}

#[test]
fn on_dequeued_underflow_does_not_block_concurrent_admission() {
    use std::sync::Barrier;
    use std::thread;

    let state = Arc::new(AdmissionState::new(EventQueueOptions {
        max_queued_events: 10,
        max_queued_bytes: 0,
        admission_observability: false,
    }));

    // Enqueue 1 event so counter is 1.
    state.try_reserve(0).expect("first reservation");
    assert_eq!(state.queued_events(), 1);

    // Dequeue it back to 0.
    state.on_dequeued(0);
    assert_eq!(state.queued_events(), 0);

    // Now fire many parallel dequeues (simulating bookkeeping drift) while
    // another thread continuously probes admission. Checking and reserving are
    // one atomic step now, so a bare probe is reserve-then-release: it leaves
    // occupancy exactly where it was. No probe should ever observe
    // queued_events >= max_queued_events.
    let barrier = Arc::new(Barrier::new(2));
    let admit_state = Arc::clone(&state);
    let admit_barrier = Arc::clone(&barrier);

    let admit_handle = thread::spawn(move || {
        admit_barrier.wait();
        let mut rejections = 0u64;
        for _ in 0..10_000 {
            match admit_state.try_reserve(0) {
                Ok(()) => admit_state.release_reservation(0),
                Err(_) => rejections += 1,
            }
        }
        rejections
    });

    let dequeue_state = Arc::clone(&state);
    let dequeue_barrier = Arc::clone(&barrier);
    let dequeue_handle = thread::spawn(move || {
        dequeue_barrier.wait();
        for _ in 0..10_000 {
            dequeue_state.on_dequeued(0);
        }
    });

    dequeue_handle.join().expect("dequeue thread panicked");
    let rejections = admit_handle.join().expect("admit thread panicked");

    assert_eq!(
        rejections, 0,
        "admission must never reject when the queue is logically empty; \
         got {rejections} false rejections (likely caused by counter wrapping to usize::MAX)"
    );
}

#[test]
fn counter_tracking_disabled_skips_atomic_updates() {
    // When no budget is configured and observability is off, event and byte
    // counters still track accurately (required by len() and snapshot()),
    // but peak tracking is skipped.
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 0,
        max_queued_bytes: 0,
        admission_observability: false,
    });

    state.try_reserve(1024).expect("budget admits this event");
    state.try_reserve(2048).expect("budget admits this event");
    assert_eq!(
        state.queued_events(),
        2,
        "queued_events must always be tracked (required by len())"
    );
    assert_eq!(
        state.snapshot().queued_bytes,
        3072,
        "queued_bytes must always be tracked (required by snapshot())"
    );
    // Peak counters stay at 0 because observability is off.
    assert_eq!(state.snapshot().peak_queued_events, 0);
    assert_eq!(state.snapshot().peak_queued_bytes, 0);

    state.on_dequeued(1024);
    assert_eq!(
        state.queued_events(),
        1,
        "queued_events must always be decremented on dequeue"
    );
    assert_eq!(
        state.snapshot().queued_bytes,
        2048,
        "queued_bytes must reflect dequeue"
    );

    // Reservation must always succeed when no budget is configured, even for
    // an implausibly large event.
    //
    // Not `usize::MAX` any more: checking and reserving are one step now, so
    // this call *counts* the bytes into the monotonic total, and a saturating
    // value overflows that `fetch_add` (debug builds panic). Event byte counts
    // are payload sizes, so the overflow is only reachable from a synthetic
    // test — the assertion's intent is "no budget means no refusal", which a
    // merely enormous value exercises just as well.
    assert!(state.try_reserve(usize::MAX / 4).is_ok());
}

#[test]
fn counter_tracking_enabled_with_budget() {
    // When a budget is configured, counters must update normally.
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 100,
        max_queued_bytes: 0,
        admission_observability: false,
    });

    state.try_reserve(0).expect("budget admits this event");
    state.try_reserve(0).expect("budget admits this event");
    assert_eq!(state.queued_events(), 2);

    state.on_dequeued(0);
    assert_eq!(state.queued_events(), 1);
}

#[test]
fn counter_tracking_enabled_with_observability_only() {
    // When only observability is on (no budget), counters must still update
    // so that peak tracking works.
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 0,
        max_queued_bytes: 0,
        admission_observability: true,
    });

    state.try_reserve(512).expect("budget admits this event");
    assert_eq!(state.queued_events(), 1);

    let stats = state.snapshot();
    assert_eq!(stats.peak_queued_events, 1);
    assert_eq!(stats.peak_queued_bytes, 512);
}

#[test]
fn smallbox_event_callback_runs_inline() {
    // Small closure should be stored inline (not on heap).
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    let event = Event::new(EventKind::SubscriptionData, move || {
        *flag_clone.lock().unwrap() = true;
    });

    // Verify callback is stored inline (not heap-allocated).
    assert!(
        !event.callback.is_heap(),
        "small closure should be stored inline in SmallBox"
    );

    event.run();
    assert!(*flag.lock().unwrap(), "callback should have been invoked");
}

#[test]
fn smallbox_event_callback_large_closure_falls_back_to_heap() {
    // Force a large capture that exceeds S16 (128 bytes) inline capacity.
    let large_data = [0u8; 2048];
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    let event = Event::new(EventKind::Timer, move || {
        // Use large_data to prevent the compiler from optimizing it away.
        let _ = large_data[0];
        *flag_clone.lock().unwrap() = true;
    });

    // Large closure should spill to heap.
    assert!(
        event.callback.is_heap(),
        "large closure should fall back to heap allocation in SmallBox"
    );

    event.run();
    assert!(*flag.lock().unwrap(), "callback should have been invoked");
}

#[test]
fn smallbox_event_callback_invoked_exactly_once() {
    // Verify the FnMut wrapper's Option<F> pattern ensures single invocation.
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let event = Event::new(EventKind::SubscriptionData, move || {
        counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });

    event.run();
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "callback must be invoked exactly once"
    );
}

#[test]
fn run_isolates_panicking_callback() {
    // A panicking callback must NOT unwind out of Event::run — that
    // would kill the executor dispatch loop and take down every timer /
    // subscription / service on the node. The panic is caught and contained.
    let event = Event::new(EventKind::Timer, || {
        panic!("boom from a user timer callback")
    });
    // Must return normally (not unwind) AND report the isolated panic so the
    // executor can count it into the runtime-health error_count.
    assert!(
        event.run(),
        "an isolated callback panic must be reported to the caller"
    );
}

#[test]
fn run_returns_false_on_clean_callback() {
    // The dispatch-error signal: run() reports false when the callback returns
    // normally, so the executor only counts genuine isolated panics.
    let event = Event::new(EventKind::Timer, || {});
    assert!(
        !event.run(),
        "a clean callback must not be reported as a dispatch error"
    );
}

#[test]
fn run_and_measure_isolates_panic_and_still_returns() {
    // The measured-dispatch path gets the same isolation and still returns a
    // duration (it must not propagate the unwind to the metrics caller).
    let event = Event::new(EventKind::SubscriptionData, || panic!("boom"));
    let _elapsed_ns = event.run_and_measure(); // must return, not unwind
}

#[test]
fn run_continues_dispatch_after_a_panicking_callback() {
    // Simulate the executor draining a batch: a panicking event in the middle
    // must not stop the following event from running.
    let ran_after = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ran_after_clone = Arc::clone(&ran_after);

    Event::new(EventKind::Timer, || panic!("boom")).run();
    Event::new(EventKind::Timer, move || {
        ran_after_clone.store(true, std::sync::atomic::Ordering::SeqCst);
    })
    .run();

    assert!(
        ran_after.load(std::sync::atomic::Ordering::SeqCst),
        "dispatch must continue after an isolated panic"
    );
}

// into_parts: the consumer holds NO sender, so pop()'s None branch is real
// — once every handed-out sender clone is dropped, pop() returns None.
// (On a plain EventQueue the self-held sender makes that None branch
// unreachable, which is the pitfall this split closes at the
// type level.)
#[tokio::test]
async fn into_parts_consumer_pop_returns_none_after_sender_dropped() {
    use super::EventKind;

    let (sender, mut consumer) = EventQueue::new(4).into_parts();
    sender
        .try_send(Event::new(EventKind::Timer, || {}))
        .expect("queue has capacity");
    assert!(
        consumer.try_pop().is_some(),
        "queued event must be consumed"
    );
    assert!(consumer.try_pop().is_none(), "queue is empty");
    drop(sender);
    assert!(
        consumer.pop().await.is_none(),
        "pop() must return None once every sender clone is dropped"
    );
    assert!(
        consumer.try_pop().is_none(),
        "try_pop() must return None once the backend is closed"
    );
}

// into_parts must not leak the queue's own AdmissionState Arc: the
// consumer does not hold it (each receiver variant keeps its own clone),
// so the queue's copy must be dropped explicitly — ManuallyDrop
// suppresses EventQueue::drop and the field destructor would never run.
#[tokio::test]
async fn into_parts_drops_admission_arc() {
    let queue = EventQueue::new(4);
    let admission_weak = Arc::downgrade(&queue.admission);
    let (sender, consumer) = queue.into_parts();
    assert!(
        admission_weak.upgrade().is_some(),
        "admission Arc must stay alive while the consumer (and its receiver clone) lives"
    );
    drop(consumer);
    drop(sender);
    assert!(
        admission_weak.upgrade().is_none(),
        "admission Arc must be fully released after into_parts + consumer drop \
         (leak regression)"
    );
}

// Crossbeam queues must refuse into_parts: the consumer's generic pop
// races the reserved lane's Notify against the blocking crossbeam recv
// (EventQueue keeps a dedicated 10ms-slice pop for that backend). Enforced
// by debug_assert; linux-only (crossbeam backend is cfg(target_os =
// "linux")) — NOT runnable on this macOS host, documented as such.
#[cfg(target_os = "linux")]
#[test]
#[should_panic(expected = "into_parts is unsupported on a crossbeam queue")]
fn into_parts_rejects_crossbeam_queue() {
    let _ = EventQueue::new_crossbeam(4).into_parts();
}

// ---- PERF layout guards (Tier-1 event-queue micro-optimisations) ----

/// The producer (zenoh RX / acceptance thread) writes only `enqueued`, the
/// consumer (executor thread) writes only `dequeued`, and `can_admit` reads
/// `config` on every enqueue. Any two of those sharing a cache line reinstates
/// the per-message ping-pong this split exists to remove.
#[test]
fn admission_state_keeps_producer_and_consumer_counters_off_one_cache_line() {
    use super::{AdmissionState, ADMISSION_CACHE_LINE};
    use std::mem::offset_of;

    let config = offset_of!(AdmissionState, config);
    let enqueued = offset_of!(AdmissionState, enqueued);
    let dequeued = offset_of!(AdmissionState, dequeued);
    let observability = offset_of!(AdmissionState, observability);

    for (name, lhs, rhs) in [
        ("config/enqueued", config, enqueued),
        ("enqueued/dequeued", enqueued, dequeued),
        ("dequeued/observability", dequeued, observability),
    ] {
        assert!(
            rhs.abs_diff(lhs) >= ADMISSION_CACHE_LINE,
            "{name} must be at least one cache line ({ADMISSION_CACHE_LINE} B) apart, \
             got {lhs} and {rhs}"
        );
    }
    assert_eq!(
        std::mem::align_of::<AdmissionState>(),
        ADMISSION_CACHE_LINE,
        "AdmissionState must be cache-line aligned, or the Arc allocation can \
         straddle the split it pays for"
    );
}

/// Occupancy is derived from two monotonic totals, so a dequeue that was never
/// matched by an enqueue must clamp at zero rather than wrap — the invariant
/// the old `fetch_update` CAS loop enforced by hand.
#[test]
fn admission_occupancy_saturates_instead_of_wrapping() {
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 0,
        max_queued_bytes: 0,
        admission_observability: true,
    });

    state.try_reserve(64).expect("budget admits this event");
    state.on_dequeued(64);
    state.on_dequeued(64);
    state.on_dequeued(64);

    let stats = state.snapshot();
    assert_eq!(stats.queued_events, 0);
    assert_eq!(stats.queued_bytes, 0);
    assert_eq!(stats.peak_queued_events, 1);
    assert_eq!(stats.peak_queued_bytes, 64);

    // A later enqueue is still counted from the true occupancy, not from a
    // wrapped one.
    state.try_reserve(32).expect("budget admits this event");
    let stats = state.snapshot();
    assert_eq!(stats.queued_events, 0, "one enqueue against three dequeues");
    assert_eq!(stats.queued_bytes, 0);
}

/// PERF contract for [`EVENT_CALLBACK_INLINE_WORDS`]: the pub/sub hot-path
/// closure must stay inline. If this fails, every received message pays a
/// malloc on the RX thread and a cross-thread free on the executor thread.
///
/// `SubscriptionCaptureMirror` mirrors the capture set of the subscription
/// closure in `core/src/subscriber.rs` — update it together with that closure.
#[test]
fn event_callback_budget_fits_subscription_capture() {
    use super::{wrap_fn_once, EVENT_CALLBACK_INLINE_BYTES};

    #[allow(dead_code)]
    struct SubscriptionCaptureMirror {
        shared_for_event: Arc<()>,
        payload: zenoh::bytes::ZBytes,
        cb_link: Option<crate::trace::SpanLink>,
        source_identity: Option<String>,
        source_namespace: Option<String>,
        attachment_hash: Option<u64>,
        publisher_stamp_ns: Option<i64>,
        receive_stamp_ns: u64,
        context_priority: crate::qos::Priority,
        parse_header_for_strategy: bool,
        include_topic_in_context: bool,
        enable_header_context: bool,
    }

    let mirror_bytes = std::mem::size_of::<SubscriptionCaptureMirror>();
    assert!(
        mirror_bytes <= EVENT_CALLBACK_INLINE_BYTES,
        "the subscription closure's capture set is {mirror_bytes} B but the inline \
         callback budget is only {EVENT_CALLBACK_INLINE_BYTES} B — every message would \
         heap-allocate on the RX thread and free on the executor thread"
    );

    let capture = SubscriptionCaptureMirror {
        shared_for_event: Arc::new(()),
        payload: zenoh::bytes::ZBytes::from(vec![0u8; 64]),
        cb_link: None,
        source_identity: None,
        source_namespace: None,
        attachment_hash: None,
        publisher_stamp_ns: None,
        receive_stamp_ns: 0,
        context_priority: crate::qos::Priority::Normal,
        parse_header_for_strategy: false,
        include_topic_in_context: false,
        enable_header_context: false,
    };
    let callback = wrap_fn_once(move || {
        let _ = &capture;
    });
    assert!(
        !callback.is_heap(),
        "a callback with the subscription capture shape must stay inline"
    );
}

/// The dequeue-wins race: the event is published to the backend channel BEFORE
/// `on_enqueued` runs, so a fast consumer can dequeue and count it first. This
/// pins both halves of what that must mean.
#[test]
fn dequeue_winning_the_race_keeps_occupancy_exact_and_peak_at_least_one() {
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 0,
        max_queued_bytes: 0,
        admission_observability: true,
    });

    // Replay the race 1000 times: each event is dequeued (and counted) before
    // its producer records the enqueue.
    for _ in 0..1000 {
        state.on_dequeued(64);
        state.try_reserve(64).expect("budget admits this event");
    }

    let stats = state.snapshot();
    // Occupancy must return to exactly zero. The pre-split counter could not do
    // this: its `saturating_sub` DISCARDED the decrement whenever the dequeue
    // arrived first, so every raced pair leaked +1 permanently, and with a
    // budget configured that drift eventually rejected every admission.
    assert_eq!(
        stats.queued_events, 0,
        "a dequeue that wins the race must still be counted, not clamped away"
    );
    assert_eq!(stats.queued_bytes, 0);

    // Peak is a high-water diagnostic: the event WAS queued, however briefly,
    // so the mark cannot read zero.
    assert!(
        stats.peak_queued_events >= 1,
        "peak_queued_events must not under-report to zero when dequeue wins the race"
    );
    assert!(
        stats.peak_queued_bytes >= 64,
        "peak_queued_bytes must account for at least the event's own bytes"
    );
}

// ---- Admission budget must bound concurrent producers ----

/// The racy producer interleaving, made deterministic.
///
/// Producer A publishes A to the channel and is descheduled before it records
/// the enqueue. The consumer pops A and records the dequeue first. A second
/// producer then reserves and publishes B. At that instant `enqueued` and
/// `dequeued` are equal — A's dequeue cancels B's enqueue — so the derived
/// occupancy reads zero even though B is genuinely queued, and a third
/// producer is admitted past a budget of one.
///
/// Loom is not an option here: it is driven by a global `--cfg loom` that
/// recompiles every crate, and tokio's loom configuration drops the runtime API
/// `aorta_core` depends on — which is exactly why the loom-verified primitives
/// live in the separate tokio-free `aorta_sync` crate. So the interleaving is
/// pinned directly instead of searched for.
#[test]
fn admission_budget_holds_when_a_dequeue_outruns_its_enqueue() {
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 1,
        max_queued_bytes: 0,
        admission_observability: false,
    });

    // Producer A: reserved and published A, then descheduled.
    state
        .try_reserve(0)
        .expect("A is the first event against a budget of 1");
    // Consumer: pops A and records the dequeue.
    state.on_dequeued(0);
    // Producer B: reserves and publishes B. B now occupies the only slot.
    state
        .try_reserve(0)
        .expect("B may take the slot A just vacated");
    // Producer C: MUST be refused — B is still queued.
    assert_eq!(
        state.try_reserve(0),
        Err(AdmissionDropReason::BudgetEvents),
        "a budget of 1 admitted a second concurrently-queued event"
    );
}

/// A reservation taken for a send that then fails must be given back, or the
/// budget leaks a slot per failed send and eventually refuses everything.
#[test]
fn a_failed_send_releases_its_admission_reservation() {
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 1,
        max_queued_bytes: 64,
        admission_observability: false,
    });

    for _ in 0..100 {
        state.try_reserve(64).expect("slot is free each round");
        // The channel refused the entry (Full/Closed): hand the slot back.
        state.release_reservation(64);
    }

    let stats = state.snapshot();
    assert_eq!(
        stats.queued_events, 0,
        "released reservations must not leak"
    );
    assert_eq!(stats.queued_bytes, 0);
    state
        .try_reserve(64)
        .expect("the budget must still admit after 100 failed sends");
}

/// The byte budget is reserved and released on the same path as the event
/// budget, so an oversized event cannot slip through by racing.
#[test]
fn byte_budget_reservation_rolls_back_when_the_byte_limit_is_exceeded() {
    let state = AdmissionState::new(EventQueueOptions {
        max_queued_events: 0,
        max_queued_bytes: 100,
        admission_observability: false,
    });

    state.try_reserve(80).expect("80 <= 100");
    assert_eq!(
        state.try_reserve(40),
        Err(AdmissionDropReason::BudgetBytes),
        "80 + 40 exceeds the 100-byte budget"
    );
    // The refused attempt must not have consumed budget.
    let stats = state.snapshot();
    assert_eq!(
        stats.queued_bytes, 80,
        "a refused reservation must roll back"
    );
    state.try_reserve(20).expect("80 + 20 == 100 still fits");
}

/// Many producers against a budget of N, with a consumer draining: the derived
/// occupancy must never be observed above the budget.
#[test]
fn concurrent_producers_never_exceed_the_event_budget() {
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::thread;

    const BUDGET: usize = 4;
    let state = Arc::new(AdmissionState::new(EventQueueOptions {
        max_queued_events: BUDGET,
        max_queued_bytes: 0,
        admission_observability: false,
    }));
    let stop = Arc::new(AtomicBool::new(false));
    let live = Arc::new(AtomicU64::new(0));
    let worst = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let state = Arc::clone(&state);
        let stop = Arc::clone(&stop);
        let live = Arc::clone(&live);
        let worst = Arc::clone(&worst);
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if state.try_reserve(0).is_ok() {
                    // `live` counts events that are reserved-and-not-yet-drained,
                    // i.e. the real occupancy the budget is meant to bound.
                    let now = live.fetch_add(1, Ordering::AcqRel) + 1;
                    worst.fetch_max(now, Ordering::AcqRel);
                    std::thread::yield_now();
                    live.fetch_sub(1, Ordering::AcqRel);
                    state.on_dequeued(0);
                }
            }
        }));
    }
    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("producer thread panicked");
    }

    let peak = worst.load(Ordering::Acquire);
    assert!(peak > 0, "the stress loop never reserved anything");
    assert!(
        peak <= BUDGET as u64,
        "budget of {BUDGET} was exceeded: {peak} events were concurrently admitted"
    );
}

// ---- Every send arm must balance its reservation ----

/// Reservation balance across EVERY backend and BOTH send paths.
///
/// `try_reserve` charges the budget before the entry is published, so every
/// path that then fails to publish owes a `release_reservation`. A missed arm
/// does not fail loudly — it silently inflates `queued_events`/`queued_bytes`
/// forever, and with a budget configured it eventually refuses everything.
/// A later review found exactly such an arm (the async unbounded close path)
/// that an earlier sweep missed, so this pins all of them at once rather than
/// trusting another read-through.
///
/// Each case drops the consumer first, so the backend receiver is closed and
/// every send must fail; occupancy must therefore return to exactly zero.
#[tokio::test]
async fn every_send_arm_releases_its_reservation_on_a_closed_channel() {
    const BYTES: usize = 64;

    async fn assert_balanced(queue: EventQueue, label: &str) {
        let (sender, consumer) = queue.into_parts();
        // Closing the consumer closes the backend receiver: every send below
        // must fail, and must therefore give its reservation back.
        drop(consumer);

        for _ in 0..25 {
            let event = Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(BYTES);
            sender
                .try_send(event)
                .expect_err("try_send must fail on a closed channel");
        }
        for _ in 0..25 {
            let event = Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(BYTES);
            sender
                .send(event)
                .await
                .expect_err("send must fail on a closed channel");
        }

        let stats = sender.admission_stats();
        assert_eq!(
            stats.queued_events, 0,
            "{label}: {} phantom events left by failed sends",
            stats.queued_events
        );
        assert_eq!(
            stats.queued_bytes, 0,
            "{label}: {} phantom bytes left by failed sends",
            stats.queued_bytes
        );
    }

    assert_balanced(EventQueue::new(8), "bounded").await;
    assert_balanced(EventQueue::new_unbounded(), "unbounded").await;
    assert_balanced(EventQueue::new_multi_ingress(8), "multi-ingress").await;
}

/// The Full path (as opposed to Closed): a bounded queue at capacity must
/// refuse without consuming budget, leaving occupancy at exactly the number of
/// entries actually sitting in the channel.
#[tokio::test]
async fn a_full_bounded_channel_releases_the_refused_reservation() {
    const BYTES: usize = 32;
    let queue = EventQueue::new(2);
    let (sender, _consumer) = queue.into_parts();

    // Fill the channel to capacity — these two stay queued.
    for _ in 0..2 {
        sender
            .try_send(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(BYTES))
            .expect("channel has room");
    }
    // Every further attempt must be refused AND must not consume budget.
    for _ in 0..25 {
        sender
            .try_send(Event::new(EventKind::SubscriptionData, || {}).with_estimated_bytes(BYTES))
            .expect_err("channel is full");
    }

    let stats = sender.admission_stats();
    assert_eq!(
        stats.queued_events, 2,
        "only the 2 entries actually in the channel may be charged"
    );
    assert_eq!(stats.queued_bytes, 2 * BYTES as u64);
}
