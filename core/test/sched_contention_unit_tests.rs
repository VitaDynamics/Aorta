//! Deterministic unit tests for the scheduler CONTENTION signals
//! (`Aorta_Scheduler_Tracing_Proposal` §4.2): the per-group waiter counter
//! (`sched.group_waiters:<group>`) and the priority-inversion instant
//! (`sched.priority_inversion:<group>`). Driving these through real timers is
//! non-deterministic (all timer events share the Realtime priority, so they can
//! never invert each other), so these exercise the `acquire_group_traced` helper
//! directly with a held lock and a constructed priority gap.

use super::{acquire_group_traced, group_trace_events, GroupContention};
use crate::event_queue::EventPriority;
use crate::trace::chunk::ChunkEventType;
use crate::trace::{
    intern_event, intern_scope, set_scope_enabled, set_trace_enabled, SCHED_SCOPE_NAME,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

/// A higher-priority dispatch that has to wait behind a lower-priority holder
/// must (a) emit `sched.priority_inversion:<group>` and (b) sample the
/// `sched.group_waiters:<group>` COUNTER while it is parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acquire_group_traced_flags_inversion_and_counts_waiters() {
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    set_scope_enabled(intern_scope(SCHED_SCOPE_NAME), true);

    let events = group_trace_events(Some("invtest"), 4242);
    let contention = GroupContention::new(&events);
    let lock = Arc::new(TokioMutex::new(()));

    // A NORMAL-priority holder occupies the lock (record its priority as the
    // helper would on a real acquire).
    let holder_guard = lock.lock().await;
    contention
        .holder_priority
        .store(EventPriority::Normal as u8, Ordering::Release);

    // A REALTIME waiter contends → it must wait behind the lower-priority holder.
    let lock_w = Arc::clone(&lock);
    let c_w = Arc::clone(&contention);
    let waiter = tokio::spawn(async move {
        let _g = acquire_group_traced(&lock_w, &c_w, EventPriority::Realtime).await;
    });
    // Let the waiter reach the contended branch and park on the lock.
    tokio::time::sleep(Duration::from_millis(60)).await;
    drop(holder_guard); // release → the waiter acquires and the counter decrements
    waiter.await.unwrap();

    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);

    let inversion = intern_event("sched.priority_inversion:invtest");
    let waiters = intern_event("sched.group_waiters:invtest");
    assert!(
        recs.iter().any(|r| r.event_id == inversion
            && ChunkEventType::from_u8(r.event_type) == ChunkEventType::Instant),
        "a higher-priority waiter behind a lower-priority holder must emit \
         sched.priority_inversion:<group>"
    );
    assert!(
        recs.iter().any(|r| r.event_id == waiters
            && ChunkEventType::from_u8(r.event_type) == ChunkEventType::Counter),
        "a parked waiter must sample the sched.group_waiters:<group> counter"
    );
}

/// The uncontended fast path takes the lock with NO waiter accounting and NO
/// inversion instant — contention signals are paid only under real contention.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_group_traced_uncontended_is_silent() {
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    set_scope_enabled(intern_scope(SCHED_SCOPE_NAME), true);

    let events = group_trace_events(Some("quiet"), 99);
    let contention = GroupContention::new(&events);
    let lock = Arc::new(TokioMutex::new(()));

    {
        let _g = acquire_group_traced(&lock, &contention, EventPriority::Normal).await;
    } // released immediately; no other waiter ever contended

    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);

    let inversion = intern_event("sched.priority_inversion:quiet");
    let waiters = intern_event("sched.group_waiters:quiet");
    assert!(
        !recs
            .iter()
            .any(|r| r.event_id == inversion || r.event_id == waiters),
        "an uncontended acquire must emit no contention signals"
    );
    assert_eq!(
        contention.waiters.load(Ordering::Acquire),
        0,
        "the waiter counter must be balanced back to zero"
    );
}
