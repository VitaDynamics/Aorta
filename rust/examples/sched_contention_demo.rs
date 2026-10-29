//! Scheduler-tracing demo: callback-group contention made visible.
//!
//! Two timer fleets do the SAME ~6 ms of CPU work per tick on a 4-worker
//! `MultiThreadedExecutor`, but on different callback groups:
//!
//!   * `SERIAL`  — 4 timers on ONE `MutuallyExclusive` group. The group lock
//!     serializes them, so even with 4 idle workers only one callback runs at a
//!     time and the others pile up: the scheduler trace shows a growing
//!     `sched.group_wait:<group>` span before each `sched.callback`.
//!   * `PARALLEL` — 4 timers on a `Reentrant` group. No group lock, so the
//!     callbacks run concurrently across the workers: the trace shows
//!     `sched.queue_wait` → `sched.callback` with NO `group_wait` at all.
//!
//! Run it with the scheduler scope on and convert the result:
//!
//! ```bash
//! AORTA_TRACE=standard AORTA_TRACE_DIR=/tmp/sched AORTA_TRACE_FILE_MAX_SECS=1 \
//!   bazel run //rust/examples:sched_contention_demo
//! aorta-trace2perfetto /tmp/sched -o /tmp/sched/trace.json   # open in ui.perfetto.dev
//! ```
//!
//! In Perfetto the SERIAL group's `group_wait` bars stack up (the smoking gun
//! for an over-shared `MutuallyExclusive` group) while the PARALLEL group's do
//! not — Aorta self-diagnosing a callback-group misuse without any user probes.
//!
//! It also runs a publisher → subscriber on `/demo/feed`, so the trace shows the
//! USER layer (`topic.publish` / `topic.callback`) ABOVE the scheduler layer
//! (`sched.queue_wait` / `sched.group_wait`) — the subscriber callback
//! dispatches through the same executor, so its scheduler waits sit beneath its
//! `topic.callback`.

use aorta_sdk::{
    AortaResult, CallbackGroupType, ExecutorOptions, MultiThreadedExecutor, Node, TimerOptions,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Burn ~`ms` of CPU (a tight LCG so the optimizer can't elide it) and fold the
/// result into `sink` — a CPU-bound callback, so wall time ≈ thread time except
/// for the group_wait spent blocked on the lock.
fn burn_cpu(ms: u64, sink: &AtomicU64) {
    let start = Instant::now();
    let mut x = 0x9e3779b97f4a7c15u64;
    while start.elapsed() < Duration::from_millis(ms) {
        for _ in 0..2048 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        }
    }
    sink.fetch_add(x | 1, Ordering::Relaxed);
}

#[tokio::main]
async fn main() -> AortaResult<()> {
    const WORKERS: usize = 4;
    const FLEET: usize = 4; // timers per group
    const PERIOD_MS: u64 = 20;
    const WORK_MS: u64 = 6;
    const RUN_SECS: u64 = 5;

    // A multi-worker executor: enough threads that callbacks COULD run in
    // parallel — so any serialization we see is the callback group, not thread
    // starvation.
    let mut exec = MultiThreadedExecutor::new_with_options(1024, WORKERS, ExecutorOptions::default());
    let node = Node::new("sched_contention_demo", "default").await?;
    // Attach the SDK node's core node BEFORE creating groups/timers, so the
    // timers reuse this multi-worker executor (not the SDK single-threaded
    // fallback). This is exactly what `ExecutorNodeAttachExt::attach_sdk_node` does.
    exec.add_node(node.inner().clone());

    // One shared MutuallyExclusive group (the serializing one) and one Reentrant
    // group (the concurrent one). Callback groups live on the core node.
    let serial = node
        .inner()
        .create_callback_group(CallbackGroupType::MutuallyExclusive)?;
    let parallel = node
        .inner()
        .create_callback_group(CallbackGroupType::Reentrant)?;

    let serial_ticks = Arc::new(AtomicU64::new(0));
    let parallel_ticks = Arc::new(AtomicU64::new(0));
    let sink = Arc::new(AtomicU64::new(0));

    // Keep the timers alive for the whole run.
    let mut timers = Vec::new();

    for _ in 0..FLEET {
        let ticks = Arc::clone(&serial_ticks);
        let sink = Arc::clone(&sink);
        let opts = TimerOptions::default().with_callback_group_handle(serial);
        timers.push(
            node.create_timer_with_options(
                Duration::from_millis(PERIOD_MS),
                move || {
                    burn_cpu(WORK_MS, &sink);
                    ticks.fetch_add(1, Ordering::Relaxed);
                },
                opts,
            )
            .await?,
        );
    }

    for _ in 0..FLEET {
        let ticks = Arc::clone(&parallel_ticks);
        let sink = Arc::clone(&sink);
        let opts = TimerOptions::default().with_callback_group_handle(parallel);
        timers.push(
            node.create_timer_with_options(
                Duration::from_millis(PERIOD_MS),
                move || {
                    burn_cpu(WORK_MS, &sink);
                    ticks.fetch_add(1, Ordering::Relaxed);
                },
                opts,
            )
            .await?,
        );
    }

    // --- topic layer: a publisher + subscriber so the trace ALSO shows the
    // USER-level topic.publish / topic.callback ABOVE the scheduler waits. The
    // subscriber callback dispatches through the SAME executor, so it gets its
    // own sched.queue_wait / sched.group_wait beneath the topic.callback — the
    // layered view (user callback on top, scheduler waits underneath). ---
    let sub_ticks = Arc::new(AtomicU64::new(0));
    let sub_cb_ticks = Arc::clone(&sub_ticks);
    let sub_sink = Arc::clone(&sink);
    let _sub = node
        .create_subscriber("/demo/feed", move |_payload, _ctx| {
            burn_cpu(2, &sub_sink); // a little user work so topic.callback is visible
            sub_cb_ticks.fetch_add(1, Ordering::Relaxed);
        })
        .await?;
    let publisher = node.create_publisher_raw("/demo/feed").await?;
    let pub_seq = Arc::new(AtomicU64::new(0));
    timers.push(
        node.create_timer(Duration::from_millis(25), move || {
            let n = pub_seq.fetch_add(1, Ordering::Relaxed);
            let _ = publisher.publish(n.to_le_bytes().to_vec());
        })
        .await?,
    );

    println!(
        "sched_contention_demo: {WORKERS} workers, {FLEET} timers x {PERIOD_MS}ms x {WORK_MS}ms work \
         on each of a MutuallyExclusive (SERIAL) and a Reentrant (PARALLEL) group, plus a 40Hz \
         publisher → subscriber on /demo/feed (the topic.callback layer); running {RUN_SECS}s..."
    );

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(RUN_SECS)).await;
        stop.stop();
    });
    exec.spin().await;

    let s = serial_ticks.load(Ordering::Relaxed);
    let p = parallel_ticks.load(Ordering::Relaxed);
    let subs = sub_ticks.load(Ordering::Relaxed);
    let ideal = (RUN_SECS * 1000 / PERIOD_MS) * FLEET as u64;
    println!(
        "done. SERIAL group ran {s} callbacks, PARALLEL group ran {p} (ideal ~{ideal} each); \
         subscriber received {subs} messages.\n\
         The SERIAL group is throttled by its one lock — see the stacked \
         sched.group_wait spans in the trace; the PARALLEL group has none. The \
         topic.callback slices show the user layer above the scheduler waits.\n\
         sink={} (anti-DCE)",
        sink.load(Ordering::Relaxed)
    );
    Ok(())
}
