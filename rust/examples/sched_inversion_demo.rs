//! Scheduler-tracing demo: callback-group PRIORITY INVERSION + waiter backlog
//! made visible (via the scheduler's contention signals).
//!
//! One `MutuallyExclusive` callback group `control` is SHARED by two subscribers
//! of very different urgency, on a 4-worker `MultiThreadedExecutor`:
//!
//!   * `/demo/bulk`   — a BACKGROUND-priority subscriber whose callback burns
//!     ~20 ms of CPU. It is the lock HOLDER: a heavy, low-priority job that hogs
//!     the group lock.
//!   * `/demo/urgent` — a REALTIME-priority subscriber on the SAME group with a
//!     ~1 ms callback. Whenever an urgent message arrives while the bulk job
//!     holds the lock, it must WAIT BEHIND A LOWER-PRIORITY HOLDER — the textbook
//!     priority inversion.
//!
//! With four idle workers available, any wait is the group lock, not thread
//! starvation. The scheduler trace then shows, on the `control` group:
//!
//!   * `sched.priority_inversion:control` INSTANTS — one each time the realtime
//!     waiter is stuck behind the background holder.
//!   * a `sched.group_waiters:control` COUNTER track that climbs as urgent
//!     messages pile up on the lock and falls as they drain (the backlog the
//!     EventQueue/AdmissionStats cannot see — those events have left the queue).
//!   * `sched.group_wait:control` spans — the actual blocked intervals — with the
//!     user `topic.callback` slices sitting above them.
//!
//! Run it with the scheduler scope on (the master `verbose` tier turns the
//! master switch on AND admits the Debug-tier `aorta.sched` scope), persist the
//! trace, and convert it:
//!
//! ```bash
//! AORTA_TRACE=verbose AORTA_TRACE_AUTO_INIT=1 AORTA_TRACE_DIR=/tmp/inv \
//!   cargo run -p aorta_sdk --example sched_inversion_demo --features examples
//! # or: bazel run //rust/examples:sched_inversion_demo  (with the same env)
//! cargo run -p aorta-trace2perfetto -- /tmp/inv -o /tmp/inv/trace.json
//! # open /tmp/inv/trace.json in https://ui.perfetto.dev
//! ```
//!
//! NOTE: `AORTA_TRACE_AUTO_INIT=1` is required here ONLY because this repo's
//! `.cargo/config.toml` sets `[env] AORTA_TRACE_AUTO_INIT = "0"` for all
//! `cargo run`/`cargo test` (so test/example processes don't spawn the trace
//! sink unless asked). The library default is the opposite — unset ⇒ auto-init
//! ON — so a normally-deployed binary needs nothing. The `=1` overrides the
//! repo config (cargo's non-forced `[env]` keeps a value already in your env).

use aorta_sdk::{
    AortaResult, CallbackGroupType, ExecutorOptions, MultiThreadedExecutor, Node, Priority,
    SubscriberOptions, TimerOptions,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Burn ~`ms` of CPU (a tight LCG the optimizer can't elide) and fold it into
/// `sink` — a CPU-bound callback, so its wall time inside the lock is real work.
fn burn_cpu(ms: u64, sink: &AtomicU64) {
    let start = Instant::now();
    let mut x = 0x9e3779b97f4a7c15u64;
    while start.elapsed() < Duration::from_millis(ms) {
        for _ in 0..2048 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
        }
    }
    sink.fetch_add(x | 1, Ordering::Relaxed);
}

#[tokio::main]
async fn main() -> AortaResult<()> {
    const WORKERS: usize = 4;
    const BULK_WORK_MS: u64 = 20; // the low-priority holder hogs the lock this long
    const BULK_PERIOD_MS: u64 = 33; // ~30 Hz bulk traffic
    const URGENT_WORK_MS: u64 = 1;
    const URGENT_PERIOD_MS: u64 = 20; // ~50 Hz urgent traffic
    const RUN_SECS: u64 = 6;

    // Enough workers that the urgent and bulk callbacks COULD run in parallel —
    // so any wait we see is the shared group lock, not thread starvation.
    let mut exec =
        MultiThreadedExecutor::new_with_options(1024, WORKERS, ExecutorOptions::default());
    let node = Node::new("sched_inversion_demo", "default").await?;
    exec.add_node(node.inner().clone());

    // One NAMED MutuallyExclusive group → the trace labels everything
    // `sched.*:control` instead of an opaque handle.
    let control = node
        .inner()
        .create_callback_group_named(CallbackGroupType::MutuallyExclusive, Some("control"))?;

    let sink = Arc::new(AtomicU64::new(0));
    let bulk_ticks = Arc::new(AtomicU64::new(0));
    let urgent_ticks = Arc::new(AtomicU64::new(0));

    // --- the HOLDER: a BACKGROUND-priority subscriber with a heavy callback. ---
    let bulk_sink = Arc::clone(&sink);
    let bulk_count = Arc::clone(&bulk_ticks);
    let _bulk_sub = node
        .create_subscriber_with_options(
            "/demo/bulk",
            move |_payload, _ctx| {
                burn_cpu(BULK_WORK_MS, &bulk_sink);
                bulk_count.fetch_add(1, Ordering::Relaxed);
            },
            SubscriberOptions::default()
                .with_callback_group_handle(control)
                .with_priority(Priority::Background),
        )
        .await?;

    // --- the WAITER: a REALTIME-priority subscriber on the SAME group. ---
    let urgent_sink = Arc::clone(&sink);
    let urgent_count = Arc::clone(&urgent_ticks);
    let _urgent_sub = node
        .create_subscriber_with_options(
            "/demo/urgent",
            move |_payload, _ctx| {
                burn_cpu(URGENT_WORK_MS, &urgent_sink);
                urgent_count.fetch_add(1, Ordering::Relaxed);
            },
            SubscriberOptions::default()
                .with_callback_group_handle(control)
                .with_priority(Priority::Realtime),
        )
        .await?;

    // Two publishers feed the two topics in-process (zenoh loopback), so the
    // trace shows the topic.publish / topic.callback user layer above the
    // scheduler waits. The publisher timers themselves run on the DEFAULT group
    // (not `control`), so they don't perturb the contention we're showing.
    let mut timers = Vec::new();
    let bulk_pub = node.create_publisher_raw("/demo/bulk").await?;
    let bulk_seq = Arc::new(AtomicU64::new(0));
    timers.push(
        node.create_timer_with_options(
            Duration::from_millis(BULK_PERIOD_MS),
            move || {
                let n = bulk_seq.fetch_add(1, Ordering::Relaxed);
                let _ = bulk_pub.publish(n.to_le_bytes().to_vec());
            },
            TimerOptions::default().with_name("bulk_pub"),
        )
        .await?,
    );
    let urgent_pub = node.create_publisher_raw("/demo/urgent").await?;
    let urgent_seq = Arc::new(AtomicU64::new(0));
    timers.push(
        node.create_timer_with_options(
            Duration::from_millis(URGENT_PERIOD_MS),
            move || {
                let n = urgent_seq.fetch_add(1, Ordering::Relaxed);
                let _ = urgent_pub.publish(n.to_le_bytes().to_vec());
            },
            TimerOptions::default().with_name("urgent_pub"),
        )
        .await?,
    );

    println!(
        "sched_inversion_demo: {WORKERS} workers; a BACKGROUND /demo/bulk subscriber \
         (~{BULK_WORK_MS}ms work @ ~{}Hz) and a REALTIME /demo/urgent subscriber \
         (~{URGENT_WORK_MS}ms work @ ~{}Hz) SHARE one MutuallyExclusive group 'control'. \
         Running {RUN_SECS}s...",
        1000 / BULK_PERIOD_MS,
        1000 / URGENT_PERIOD_MS,
    );

    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(RUN_SECS)).await;
        stop.stop();
    });
    exec.spin().await;

    println!(
        "done. bulk ran {} callbacks, urgent ran {}.\n\
         In Perfetto, on the 'control' group look for:\n\
           * sched.priority_inversion:control instants — realtime urgent stuck behind background bulk\n\
           * the sched.group_waiters:control counter climbing as urgent piles up on the lock\n\
           * sched.group_wait:control spans (the blocked intervals) under the topic.callback slices\n\
         sink={} (anti-DCE)",
        bulk_ticks.load(Ordering::Relaxed),
        urgent_ticks.load(Ordering::Relaxed),
        sink.load(Ordering::Relaxed),
    );
    Ok(())
}
