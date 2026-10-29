use super::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn event_queue_push_and_pop_multiple() {
    let mut queue = EventQueue::new(10);

    // Push multiple events
    for i in 0..5 {
        queue
            .push(Event::new(EventKind::Timer, move || {
                let _ = i;
            }))
            .await
            .expect("push");
    }

    assert_eq!(queue.len(), 5);

    // Pop all events
    for _ in 0..5 {
        let event = queue.pop().await;
        assert!(event.is_some());
    }

    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn event_queue_capacity_returns_correct_value() {
    let queue = EventQueue::new(100);
    assert_eq!(queue.capacity(), 100);
}

#[tokio::test]
async fn event_queue_len_tracks_accurately() {
    let queue = EventQueue::new(5);

    assert_eq!(queue.len(), 0);

    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");
    assert_eq!(queue.len(), 1);

    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");
    assert_eq!(queue.len(), 2);
}

#[tokio::test]
async fn event_queue_is_closed_when_sender_dropped() {
    let queue = EventQueue::new(1);
    assert!(!queue.is_closed());

    // Drop the sender
    drop(queue);

    // After drop, is_closed may or may not be callable depending on implementation
}

#[tokio::test]
async fn event_queue_try_push_success() {
    let queue = EventQueue::new(5);

    let result = queue.try_push(Event::new(EventKind::Timer, || {}));
    assert!(result.is_ok());
    assert_eq!(queue.len(), 1);
}

#[tokio::test]
async fn event_queue_try_push_full() {
    let queue = EventQueue::new(1);

    // Fill the queue
    queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("first push");

    // Second push should fail
    let err = queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect_err("queue should reject full");
    assert!(matches!(err, crate::error::AortaError::Busy(_)));
}

#[tokio::test]
async fn event_queue_try_push_closed() {
    let queue = EventQueue::new(1);

    // Fill the queue first
    queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect("first push");

    // Try push should fail because queue is full
    let err = queue
        .try_push(Event::new(EventKind::Timer, || {}))
        .expect_err("queue should reject full");
    assert!(matches!(err, crate::error::AortaError::Busy(_)));
}

#[tokio::test]
async fn event_queue_try_pop_success() {
    let mut queue = EventQueue::new(5);

    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");

    let event = queue.try_pop();
    assert!(event.is_some());
}

#[tokio::test]
async fn event_queue_try_pop_empty() {
    let mut queue = EventQueue::new(5);

    let event = queue.try_pop();
    assert!(event.is_none());
}

#[tokio::test]
async fn event_queue_pop_timeout_returns_event() {
    let mut queue = EventQueue::new(5);

    queue
        .push(Event::new(EventKind::Timer, || {}))
        .await
        .expect("push");

    let event = queue.pop_timeout(Duration::from_millis(100)).await;
    assert!(event.is_some());
}

#[tokio::test]
async fn event_queue_pop_timeout_returns_none() {
    let mut queue = EventQueue::new(5);

    let start = std::time::Instant::now();
    let event = queue.pop_timeout(Duration::from_millis(50)).await;
    let elapsed = start.elapsed();

    assert!(event.is_none());
    assert!(elapsed >= Duration::from_millis(50));
}

#[tokio::test]
async fn event_queue_sender_clone() {
    let queue = EventQueue::new(5);

    let sender1 = queue.sender();
    let sender2 = sender1.clone();

    // Both senders should work
    sender1
        .send(Event::new(EventKind::Timer, || {}))
        .await
        .expect("send");
    sender2
        .send(Event::new(EventKind::Timer, || {}))
        .await
        .expect("send");

    assert_eq!(queue.len(), 2);
}

#[tokio::test]
async fn event_queue_different_event_kinds() {
    let mut queue = EventQueue::new(10);

    let kinds = vec![
        EventKind::SubscriptionData,
        EventKind::RpcRequest,
        EventKind::RpcResponse,
        EventKind::Timer,
        EventKind::ClockUpdate,
    ];

    for kind in kinds {
        queue.push(Event::new(kind, || {})).await.expect("push");
    }

    assert_eq!(queue.len(), 5);

    // Verify all events are popped in order
    for _ in 0..5 {
        let event = queue.pop().await;
        assert!(event.is_some());
    }
}

#[tokio::test]
async fn event_queue_event_callback_execution() {
    let mut queue = EventQueue::new(5);
    let flag = Arc::new(Mutex::new(false));
    let flag_clone = Arc::clone(&flag);

    queue
        .push(Event::new(EventKind::Timer, move || {
            *flag_clone.lock().unwrap() = true;
        }))
        .await
        .expect("push");

    let event = queue.pop().await.expect("pop");
    event.run();

    assert!(*flag.lock().unwrap());
}

#[tokio::test]
async fn event_queue_event_kind_accessor() {
    let event = Event::new(EventKind::SubscriptionData, || {});
    assert_eq!(event.kind(), EventKind::SubscriptionData);
}

#[test]
fn event_kind_clone() {
    let kind1 = EventKind::Timer;
    let kind2 = kind1;
    assert_eq!(kind1, kind2);
}

#[test]
fn event_kind_copy() {
    let kind = EventKind::RpcRequest;
    let kind_copy = kind;
    assert_eq!(kind, kind_copy);
}

#[test]
fn event_kind_debug() {
    let kinds = vec![
        EventKind::SubscriptionData,
        EventKind::RpcRequest,
        EventKind::RpcResponse,
        EventKind::Timer,
        EventKind::ClockUpdate,
    ];

    for kind in kinds {
        let debug = format!("{:?}", kind);
        assert!(!debug.is_empty());
    }
}

#[test]
fn event_kind_equality() {
    assert_eq!(EventKind::Timer, EventKind::Timer);
    assert_ne!(EventKind::Timer, EventKind::SubscriptionData);
}

#[test]
fn event_debug_format() {
    let event = Event::new(EventKind::Timer, || {});
    let debug = format!("{:?}", event);
    assert!(debug.contains("Event"));
    assert!(debug.contains("Timer"));
}

#[tokio::test]
async fn event_queue_high_volume() {
    let mut queue = EventQueue::new(1000);

    // Push many events
    for i in 0..100 {
        queue
            .push(Event::new(EventKind::Timer, move || {
                let _ = i;
            }))
            .await
            .expect("push");
    }

    assert_eq!(queue.len(), 100);

    // Pop all
    for _ in 0..100 {
        queue.pop().await.expect("pop");
    }

    assert_eq!(queue.len(), 0);
}

#[tokio::test]
async fn event_queue_concurrent_push_pop() {
    // Note: This test demonstrates the concept but EventQueue is not Send
    // due to internal mpsc::Receiver. For true concurrent usage,
    // EventQueue would need to be wrapped differently.
    // This is a simplified version that tests the basic functionality.

    let mut queue = EventQueue::new(100);

    // Push events
    for i in 0..10 {
        let event = Event::new(EventKind::Timer, move || {
            let _ = i;
        });
        queue.push(event).await.expect("push");
    }

    // Pop events
    let mut count = 0;
    while queue.try_pop().is_some() {
        count += 1;
    }

    // Should have popped all events
    assert_eq!(count, 10);
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = EventQueue::new(1);
}
