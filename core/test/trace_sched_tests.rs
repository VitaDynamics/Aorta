//! Integration tests for SCHEDULER-path trace instrumentation
//! (`Aorta_Scheduler_Tracing_Proposal`). A timer on a MultiThreadedExecutor
//! drives the dispatch chokepoint (`spawn_event` → `dispatch_traced`), so one
//! run exercises queue_wait + group_wait (the timer's MutuallyExclusive group) +
//! the synchronous `SchedCallback`. Asserts the two-tier gating: histograms
//! follow the master switch; the per-event spans gate under `aorta.sched`.
//!
//! Run filtered single-threaded:
//! `cargo test -p aorta_core --lib trace_sched_tests -- --test-threads=1`.

use crate::error::AortaResult;
use crate::executor::{MultiThreadedExecutor, SingleThreadedExecutor};
use crate::node::Node;
use crate::trace::{
    intern_event, intern_scope, set_scope_enabled, set_trace_enabled, stage_snapshot, StageId,
    SCHED_SCOPE_NAME,
};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn unique(base: &str) -> String {
    static C: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}_{}_{}",
        base,
        std::process::id(),
        C.fetch_add(1, Ordering::Relaxed)
    )
}

/// Drive ~10 timer ticks through a MultiThreadedExecutor and return the drained
/// ring records + the before/after histogram counts for the three sched stages.
async fn run_timer_executor() -> AortaResult<(u32, Vec<crate::trace::RingRecord>)> {
    let mut exec = MultiThreadedExecutor::new(256);
    let node = Node::new(unique("sched_timer_node"), "default").await?;
    exec.add_node(node.clone());

    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_cb = Arc::clone(&ticks);
    let _timer = node
        .create_timer(Duration::from_millis(20), move || {
            ticks_cb.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(220)).await;
        stop.stop();
    });
    exec.spin().await;

    // Drain whatever the worker threads pushed.
    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    Ok((ticks.load(Ordering::Relaxed), recs))
}

/// With `aorta.sched` ON, a dispatched timer records the queue_wait + group_wait
/// + callback histograms AND emits their waterfall spans.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sched_dispatch_records_spans_when_enabled() -> AortaResult<()> {
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    let sched_scope = intern_scope(SCHED_SCOPE_NAME);
    set_scope_enabled(sched_scope, true);

    let ev_qw = intern_event("sched.queue_wait");
    let ev_cb = intern_event("sched.callback:timer");
    let qw_before = stage_snapshot(StageId::SchedQueueWait).count;
    let gw_before = stage_snapshot(StageId::SchedGroupWait).count;
    let cb_before = stage_snapshot(StageId::SchedCallback).count;

    let (ticks, recs) = run_timer_executor().await?;

    set_trace_enabled(false);
    assert!(ticks > 0, "the timer must have fired");

    // Histograms recorded (queue_wait + group_wait are master-gated; callback is
    // sched-gated and we enabled sched).
    assert!(
        stage_snapshot(StageId::SchedQueueWait).count > qw_before,
        "queue_wait histogram must record"
    );
    assert!(
        stage_snapshot(StageId::SchedGroupWait).count > gw_before,
        "group_wait histogram must record (timer uses a MutuallyExclusive group)"
    );
    assert!(
        stage_snapshot(StageId::SchedCallback).count > cb_before,
        "callback histogram must record when sched is enabled"
    );

    use crate::trace::chunk::ChunkEventType;
    let has_begin = |ev: u16| {
        recs.iter().any(|r| {
            r.event_id == ev && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
    };
    assert!(has_begin(ev_qw), "a sched.queue_wait span must be emitted");
    assert!(
        has_begin(ev_cb),
        "a sched.callback:timer span must be emitted"
    );
    // group_wait carries a per-group label suffix; just assert one exists.
    assert!(
        recs.iter().any(|r| {
            ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
                && crate::trace::scope_name(r.scope_id).as_deref() == Some(SCHED_SCOPE_NAME)
        }),
        "at least one aorta.sched span must be on the ring"
    );

    // The three phases of ONE dispatch must share a trace and CHAIN
    // (queue_wait root → group_wait → callback) so a consumer renders the
    // scheduler waterfall and attributes the callback to its waits (a
    // fresh `inbound_root_link` per phase produced disconnected
    // roots). Find a callback begin whose parent resolves to a group_wait begin
    // whose parent resolves to a root queue_wait, all in one trace.
    let begin = |r: &crate::trace::RingRecord| {
        ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
    };
    let chained = recs
        .iter()
        .filter(|r| begin(r) && r.event_id == ev_cb)
        .any(|cb| {
            // callback's parent is a same-trace group_wait...
            recs.iter().any(|gw| {
                begin(gw)
                && gw.trace_id == cb.trace_id
                && gw.span_id == cb.parent_span_id
                && crate::trace::scope_name(gw.scope_id).as_deref() == Some(SCHED_SCOPE_NAME)
                // ...whose parent is the same-trace queue_wait root.
                && recs.iter().any(|qw| {
                    begin(qw)
                        && qw.event_id == ev_qw
                        && qw.trace_id == cb.trace_id
                        && qw.span_id == gw.parent_span_id
                        && qw.parent_span_id == 0
                })
            })
        });
    assert!(
        chained,
        "one dispatch's queue_wait → group_wait → callback spans must share a \
         trace_id and chain via parent_span_id"
    );
    Ok(())
}

/// At the default tier (master ON, `aorta.sched` OFF) the dispatch records the
/// always-on histograms but emits NO sched spans — the default deployment pays
/// nothing on the scheduler timeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sched_dispatch_histograms_only_when_scope_off() -> AortaResult<()> {
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    let sched_scope = intern_scope(SCHED_SCOPE_NAME);
    set_scope_enabled(sched_scope, false); // explicitly off (default posture)

    let ev_qw = intern_event("sched.queue_wait");
    let qw_before = stage_snapshot(StageId::SchedQueueWait).count;

    let (ticks, recs) = run_timer_executor().await?;

    set_trace_enabled(false);
    assert!(ticks > 0, "the timer must have fired");
    assert!(
        stage_snapshot(StageId::SchedQueueWait).count > qw_before,
        "queue_wait histogram still records at the default tier (master-gated)"
    );
    assert!(
        !recs.iter().any(|r| r.event_id == ev_qw),
        "no sched.queue_wait span may be emitted when aorta.sched is off"
    );
    assert!(
        !recs
            .iter()
            .any(|r| crate::trace::scope_name(r.scope_id).as_deref() == Some(SCHED_SCOPE_NAME)),
        "no aorta.sched span may be emitted when the scope is off"
    );
    Ok(())
}

/// A NAMED callback group labels the scheduler spans by NAME, not the opaque
/// numeric handle: `sched.group_wait:<name>` (the wait) and `sched.callback:<name>`
/// (the timer's callback) — so a deployment with several groups is legible and a
/// callback is attributable to the group it waited on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sched_named_group_labels_spans_by_name() -> AortaResult<()> {
    use crate::event_queue::CallbackGroupType;
    use crate::timer::TimerOptions;
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    let sched_scope = intern_scope(SCHED_SCOPE_NAME);
    set_scope_enabled(sched_scope, true);

    let mut exec = MultiThreadedExecutor::new(256);
    let node = Node::new(unique("sched_named_node"), "default").await?;
    exec.add_node(node.clone());
    let vision =
        node.create_callback_group_named(CallbackGroupType::MutuallyExclusive, Some("vision"))?;
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_cb = Arc::clone(&ticks);
    let _timer = node
        .create_timer_with_options(
            Duration::from_millis(20),
            move || {
                ticks_cb.fetch_add(1, Ordering::Relaxed);
            },
            TimerOptions::default().with_callback_group_handle(vision),
        )
        .await?;
    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(220)).await;
        stop.stop();
    });
    exec.spin().await;
    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);
    assert!(
        ticks.load(Ordering::Relaxed) > 0,
        "the timer must have fired"
    );

    use crate::trace::chunk::ChunkEventType;
    // intern_event is idempotent by string, so this returns the id the dispatch
    // used (if it labeled by name).
    let has_begin = |name: &str| {
        let ev = intern_event(name);
        recs.iter().any(|r| {
            r.event_id == ev && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        })
    };
    assert!(
        has_begin("sched.group_wait:vision"),
        "group_wait must be labeled by the group NAME, not the numeric handle"
    );
    assert!(
        has_begin("sched.callback:timer:vision"),
        "the timer callback must be labeled timer + group NAME"
    );
    Ok(())
}

/// A NAMED timer labels its callback by TASK: `sched.callback:<timer-name>`
/// (the name is pre-interned once at registration → zero per-dispatch cost),
/// taking precedence over the group label. So a specific timer is identifiable
/// even when several share a group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sched_named_timer_labels_callback_by_task() -> AortaResult<()> {
    use crate::timer::TimerOptions;
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    let sched_scope = intern_scope(SCHED_SCOPE_NAME);
    set_scope_enabled(sched_scope, true);

    let mut exec = MultiThreadedExecutor::new(256);
    let node = Node::new(unique("sched_named_timer_node"), "default").await?;
    exec.add_node(node.clone());
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_cb = Arc::clone(&ticks);
    let _timer = node
        .create_timer_with_options(
            Duration::from_millis(20),
            move || {
                ticks_cb.fetch_add(1, Ordering::Relaxed);
            },
            TimerOptions::default().with_name("control_loop"),
        )
        .await?;
    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(220)).await;
        stop.stop();
    });
    exec.spin().await;
    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);
    assert!(
        ticks.load(Ordering::Relaxed) > 0,
        "the timer must have fired"
    );

    use crate::trace::chunk::ChunkEventType;
    let ev = intern_event("sched.callback:timer:control_loop");
    assert!(
        recs.iter().any(|r| {
            r.event_id == ev && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        }),
        "the timer's callback must be labeled timer + TASK (sched.callback:timer:control_loop)"
    );
    Ok(())
}

/// The SINGLE-threaded executor resolves the callback group for the trace too
/// (no lock — it serializes by construction): an unnamed timer on a NAMED group
/// labels `sched.callback:timer:<group>`, matching the multi-threaded path.
/// The registry read is gated on `trace_enabled`, so the
/// non-traced hot path stays free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sched_single_threaded_resolves_group_label() -> AortaResult<()> {
    use crate::event_queue::CallbackGroupType;
    use crate::timer::TimerOptions;
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    let sched_scope = intern_scope(SCHED_SCOPE_NAME);
    set_scope_enabled(sched_scope, true);

    let mut exec = SingleThreadedExecutor::new(256);
    let node = Node::new(unique("sched_st_node"), "default").await?;
    exec.add_node(node.clone());
    let nav =
        node.create_callback_group_named(CallbackGroupType::MutuallyExclusive, Some("nav"))?;
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_cb = Arc::clone(&ticks);
    // UNNAMED timer on the NAMED group → the callback must fall back to the
    // group label (the timer-name path is already covered elsewhere).
    let _timer = node
        .create_timer_with_options(
            Duration::from_millis(15),
            move || {
                ticks_cb.fetch_add(1, Ordering::Relaxed);
            },
            TimerOptions::default().with_callback_group_handle(nav),
        )
        .await?;
    for _ in 0..20 {
        let _ = exec.spin_once(Duration::from_millis(20)).await;
        if ticks.load(Ordering::Relaxed) >= 2 {
            break;
        }
    }
    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);
    assert!(
        ticks.load(Ordering::Relaxed) > 0,
        "the timer must have fired"
    );

    use crate::trace::chunk::ChunkEventType;
    let ev = intern_event("sched.callback:timer:nav");
    assert!(
        recs.iter().any(|r| {
            r.event_id == ev && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        }),
        "single-threaded callback must carry the group label (sched.callback:timer:nav)"
    );
    Ok(())
}

/// Two callbacks on the SAME MutuallyExclusive group, one holding the lock far
/// longer than the tick period, make the other park on `lock().await`. Those
/// parked waiters are backlog the `EventQueue`/`AdmissionStats` cannot see (they
/// have left the queue), so the executor must surface them as the
/// `sched.group_waiters:<group>` COUNTER (Scheduler Tracing Proposal §4.2).
/// End-to-end wiring check for the
/// `resolve_for_dispatch → dispatch_traced → acquire_group_traced` path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sched_group_waiters_counter_under_contention() -> AortaResult<()> {
    use crate::event_queue::CallbackGroupType;
    use crate::timer::TimerOptions;
    use crate::trace::chunk::ChunkEventType;
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    set_scope_enabled(intern_scope(SCHED_SCOPE_NAME), true);

    let mut exec = MultiThreadedExecutor::new(256);
    let node = Node::new(unique("sched_contention_node"), "default").await?;
    exec.add_node(node.clone());
    let group =
        node.create_callback_group_named(CallbackGroupType::MutuallyExclusive, Some("busy"))?;

    let ticks = Arc::new(AtomicU32::new(0));
    // SLOW callback: holds the group lock ~40ms each tick (≫ the 15ms period).
    let t_slow = Arc::clone(&ticks);
    let _slow = node
        .create_timer_with_options(
            Duration::from_millis(15),
            move || {
                t_slow.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(40));
            },
            TimerOptions::default().with_callback_group_handle(group),
        )
        .await?;
    // FAST callback on the SAME group: repeatedly parks behind the slow one.
    let t_fast = Arc::clone(&ticks);
    let _fast = node
        .create_timer_with_options(
            Duration::from_millis(15),
            move || {
                t_fast.fetch_add(1, Ordering::Relaxed);
            },
            TimerOptions::default().with_callback_group_handle(group),
        )
        .await?;

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.stop();
    });
    exec.spin().await;

    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);
    assert!(
        ticks.load(Ordering::Relaxed) > 0,
        "the timers must have fired"
    );

    let waiters = intern_event("sched.group_waiters:busy");
    assert!(
        recs.iter().any(|r| {
            r.event_id == waiters
                && ChunkEventType::from_u8(r.event_type) == ChunkEventType::Counter
        }),
        "a fast callback parking on a busy group lock must sample sched.group_waiters:busy"
    );
    Ok(())
}

/// An event leaving the subscriber overflow-RELAY queue records `SchedRelayWait`
/// (Scheduler Tracing Proposal §4.1): its pre-executor age, kept distinct from
/// `SchedQueueWait` so a slow callback backing the relay up is not mis-blamed on scheduler queue_wait. The
/// histogram is master-gated; the async span gates under `aorta.sched`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sched_relay_wait_records_when_enabled() -> AortaResult<()> {
    use crate::event_queue::{Event, EventKind};
    use crate::trace::chunk::ChunkEventType;
    let _guard = crate::trace_test_support::lock();
    set_trace_enabled(true);
    set_scope_enabled(intern_scope(SCHED_SCOPE_NAME), true);

    let before = stage_snapshot(StageId::SchedRelayWait).count;
    let ev = intern_event("sched.relay_wait");

    let event = Event::new(EventKind::SubscriptionData, || {});
    tokio::time::sleep(Duration::from_millis(5)).await; // let the event age
    crate::executor::record_relay_wait(&event);

    let mut recs = Vec::new();
    crate::trace::ring::drain_all(&mut recs);
    set_trace_enabled(false);

    assert!(
        stage_snapshot(StageId::SchedRelayWait).count > before,
        "relay_wait histogram must record (master-gated)"
    );
    assert!(
        recs.iter().any(|r| {
            r.event_id == ev && ChunkEventType::from_u8(r.event_type) == ChunkEventType::SliceBegin
        }),
        "a sched.relay_wait async span must be emitted under aorta.sched"
    );
    Ok(())
}
