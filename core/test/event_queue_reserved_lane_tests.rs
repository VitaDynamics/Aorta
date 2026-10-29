//! Reserved `CoroutineResume` lane.
//!
//! The lane is the admission-bypassing ingress class for result-push wakes:
//! a `CoroutineResume` event must never be rejected by the aggregate admission
//! budget or a full backend channel (a failed send is a bug, not a policy —
//! the only legal failure is a closed queue), must dequeue ahead of backend
//! classes, and keeps its own occupancy/high-water counters
//! instead of participating in the admission budget.

use super::{Event, EventKind, EventPriority, EventQueue, EventQueueOptions, EventQueueSender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn coroutine_resume_event() -> Event {
    Event::new(EventKind::CoroutineResume, || {})
}

#[test]
fn coroutine_resume_default_priority_is_high() {
    // The reserved class gets its own default-high bucket.
    assert_eq!(coroutine_resume_event().priority(), EventPriority::High);
}

#[test]
fn reserved_lane_bypasses_admission_budget() {
    // Budget of one queued event, already consumed by a backend event: the
    // budget must reject further backend events but never a CoroutineResume.
    let queue = EventQueue::new_with_options(
        8,
        EventQueueOptions {
            max_queued_events: 1,
            max_queued_bytes: 0,
            admission_observability: true,
        },
    );
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect("first backend event fits the budget");
    let err = queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect_err("budget of 1 must reject the second backend event");
    assert!(err.to_string().contains("full"), "budget rejection: {err}");

    queue
        .try_push(coroutine_resume_event())
        .expect("CoroutineResume must bypass the admission budget");

    let stats = queue.admission_stats();
    assert_eq!(
        stats.queued_events, 1,
        "lane events must not enter the admission budget accounting"
    );
    assert_eq!(
        stats.dropped_budget_events, 1,
        "only the backend event may be counted as a budget drop"
    );
}

#[test]
fn reserved_lane_succeeds_when_backend_channel_is_full() {
    // Bounded backend of capacity 1, no budget: backend full must not affect
    // the lane.
    let queue = EventQueue::new(1);
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect("fill the backend channel");
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect_err("backend channel of capacity 1 must be full");

    queue
        .try_push(coroutine_resume_event())
        .expect("CoroutineResume must not observe backend-channel fullness");
}

#[test]
fn reserved_lane_dequeues_ahead_of_backend_events() {
    let mut queue = EventQueue::new(8);
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect("backend event");
    queue
        .try_push(coroutine_resume_event())
        .expect("lane event");

    let first = queue.try_pop().expect("one of the two events");
    assert_eq!(
        first.kind(),
        EventKind::CoroutineResume,
        "the reserved lane drains ahead of backend classes"
    );
    let second = queue.try_pop().expect("the backend event remains");
    assert_eq!(second.kind(), EventKind::SubscriptionData);
    assert!(queue.try_pop().is_none());
}

#[test]
fn reserved_lane_events_preserve_fifo_order_and_run() {
    let mut queue = EventQueue::new(8);
    let seen = Arc::new(AtomicUsize::new(0));
    for i in 1..=3usize {
        let seen = Arc::clone(&seen);
        queue
            .try_push(Event::new(EventKind::CoroutineResume, move || {
                // Record the last-run marker; FIFO means we observe 1,2,3.
                seen.store(i, Ordering::SeqCst);
            }))
            .expect("lane push");
    }
    for expected in 1..=3usize {
        let event = queue.try_pop().expect("lane event in FIFO order");
        assert_eq!(event.kind(), EventKind::CoroutineResume);
        assert!(!event.run(), "callback must not panic");
        assert_eq!(seen.load(Ordering::SeqCst), expected, "FIFO order");
    }
}

#[test]
fn reserved_lane_counters_track_occupancy_and_high_water() {
    let mut queue = EventQueue::new(8);
    let stats = queue.reserved_lane_stats();
    assert_eq!(
        (stats.occupancy, stats.high_water, stats.pushed_total),
        (0, 0, 0)
    );

    queue.try_push(coroutine_resume_event()).expect("lane push");
    queue.try_push(coroutine_resume_event()).expect("lane push");
    let stats = queue.reserved_lane_stats();
    assert_eq!(stats.occupancy, 2);
    assert_eq!(stats.high_water, 2);
    assert_eq!(stats.pushed_total, 2);

    queue.try_pop().expect("lane pop");
    queue.try_pop().expect("lane pop");
    let stats = queue.reserved_lane_stats();
    assert_eq!(stats.occupancy, 0, "occupancy falls as the lane drains");
    assert_eq!(stats.high_water, 2, "high-water is sticky");
    assert_eq!(stats.pushed_total, 2);
}

#[test]
fn reserved_lane_rejects_push_after_close() {
    let queue = EventQueue::new(4);
    let sender = queue.sender();
    drop(queue);
    let err = sender
        .try_send(coroutine_resume_event())
        .expect_err("a closed queue is the one legal lane failure");
    assert!(
        matches!(err, tokio::sync::mpsc::error::TrySendError::Closed(_)),
        "closed must surface as Closed, never Full"
    );
}

#[test]
fn standalone_channel_sender_rejects_coroutine_resume_loudly() {
    // Senders built over a raw channel have no EventQueue to drain a lane:
    // a CoroutineResume through them must fail with Closed, never leak silently.
    let (tx, mut _rx) = tokio::sync::mpsc::channel(4);
    let sender = EventQueueSender::from_bounded_channel(tx);
    let err = sender
        .try_send(coroutine_resume_event())
        .expect_err("inert lane must reject CoroutineResume");
    assert!(matches!(
        err,
        tokio::sync::mpsc::error::TrySendError::Closed(_)
    ));
    // Backend-class events still flow normally through the raw channel.
    sender
        .try_send(Event::new(EventKind::SubscriptionData, || {}))
        .expect("backend events unaffected");
}

#[tokio::test]
async fn pop_wakes_on_reserved_lane_push() {
    // A parked pop() must wake for a lane-only push (no backend traffic).
    let mut queue = EventQueue::new(4);
    let sender = queue.sender();
    let pusher = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        sender
            .try_send(coroutine_resume_event())
            .expect("lane push");
    });
    let event = tokio::time::timeout(Duration::from_secs(2), queue.pop())
        .await
        .expect("pop must wake on the lane push, not hang")
        .expect("event");
    assert_eq!(event.kind(), EventKind::CoroutineResume);
    pusher.await.expect("pusher task");
}

#[tokio::test]
async fn async_send_routes_coroutine_resume_through_the_lane() {
    let mut queue = EventQueue::new(1);
    // Fill the bounded backend so a mis-routed async send would block.
    queue
        .try_push(Event::new(EventKind::SubscriptionData, || {}))
        .expect("fill backend");
    let sender = queue.sender();
    tokio::time::timeout(
        Duration::from_secs(1),
        sender.send(coroutine_resume_event()),
    )
    .await
    .expect("async lane send must not block on a full backend")
    .expect("lane send succeeds");
    let event = queue.try_pop().expect("lane event");
    assert_eq!(event.kind(), EventKind::CoroutineResume);
}

#[test]
fn direct_multi_ingress_queue_carries_coroutine_resume() {
    let mut queue = EventQueue::new_multi_ingress(4);
    queue.try_push(coroutine_resume_event()).expect("lane push");
    let event = queue.try_pop().expect("lane event");
    assert_eq!(event.kind(), EventKind::CoroutineResume);
}

#[test]
fn bridged_multi_ingress_sender_shares_the_main_queue_lane() {
    // try_new_multi_ingress wraps a main queue's sender; a CoroutineResume pushed
    // through the ingress sender must surface on the MAIN queue's lane (the
    // bridge hop is for backend classes only).
    let mut sink = EventQueue::new(8);
    let ingress = EventQueueSender::try_new_multi_ingress(
        sink.sender(),
        super::MultiIngressOptions::default(),
    )
    .expect("bridged multi-ingress sender");
    ingress
        .try_send(coroutine_resume_event())
        .expect("lane push");
    let event = sink.try_pop().expect("lane event on the main queue");
    assert_eq!(event.kind(), EventKind::CoroutineResume);
}

#[cfg(target_os = "linux")]
mod crossbeam {
    use super::*;

    #[test]
    fn crossbeam_queue_carries_coroutine_resume() {
        let mut queue = EventQueue::new_crossbeam(4);
        queue.try_push(coroutine_resume_event()).expect("lane push");
        let event = queue.try_pop().expect("lane event");
        assert_eq!(event.kind(), EventKind::CoroutineResume);
    }

    #[test]
    fn crossbeam_pop_timeout_sync_wakes_on_lane_push() {
        // The executor's crossbeam spin loop parks in pop_timeout_sync; a
        // lane push must break that wait via the eventfd waker so the next
        // try_pop observes the event (spec: recv_timeout returns None on an
        // external wake, the spin loop re-runs its top-of-loop checks).
        let queue = EventQueue::new_crossbeam(4);
        if !queue.is_crossbeam() {
            // eventfd unavailable in this environment; backend fell back.
            return;
        }
        let sender = queue.sender();
        let queue = std::sync::Mutex::new(queue);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(20));
                sender
                    .try_send(coroutine_resume_event())
                    .expect("lane push");
            });
            let start = std::time::Instant::now();
            let mut queue = queue.lock().unwrap();
            let event = loop {
                if let Some(event) = queue.pop_timeout_sync(Duration::from_secs(2)) {
                    break event;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(2),
                    "lane push must wake pop_timeout_sync promptly"
                );
            };
            assert_eq!(event.kind(), EventKind::CoroutineResume);
        });
    }
}
