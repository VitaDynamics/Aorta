//! Comprehensive cross-process scheduler-tracing demo: 3 processes, several
//! callback groups, a mix of SERIAL and PARALLEL work, timers and subscribers.
//!
//! Start the three roles (subscribers first), all with the scheduler scope on
//! and the SAME trace dir, then merge the per-process chunks into one trace:
//!
//! ```bash
//! export AORTA_TRACE=standard AORTA_TRACE_DIR=/tmp/xproc AORTA_LOG=off
//! bazel run //rust/examples:sched_xproc_demo -- --role perception &
//! bazel run //rust/examples:sched_xproc_demo -- --role planner &
//! bazel run //rust/examples:sched_xproc_demo -- --role hub &       # publisher last
//! # ...let them run ~8s, then:
//! aorta-trace2perfetto /tmp/xproc -o /tmp/xproc/trace.json          # open in ui.perfetto.dev
//! ```
//!
//! The merged trace renders THREE Perfetto process groups:
//!
//!   * `sensor_hub` (hub): a publisher per topic (camera/{left,right,depth},
//!     imu/{accel,gyro}, cmd, scan) on timers — the `topic.publish:<t>` rows.
//!   * `perception`: two groups with OPPOSITE behavior on the same executor —
//!       - `vision` (MutuallyExclusive): the three camera subscribers each do
//!         ~6 ms, so they SERIALIZE on one lock → `sched.group_wait:<vision>`
//!         stacks up (the over-shared-group smoking gun);
//!       - `imu` (Reentrant): the two imu subscribers do ~1 ms and run
//!         CONCURRENTLY → no group_wait, just queue_wait.
//!   * `planner`: a mixed MutuallyExclusive group + a Reentrant one —
//!       - `control` (MutuallyExclusive): a 50 Hz control-loop TIMER (~4 ms) and
//!         the `cmd` subscriber (~3 ms) share one lock → they serialize against
//!         each other (timer-vs-subscriber contention);
//!       - `nav` (Reentrant): the `scan` subscriber (~5 ms) runs unblocked.
//!
//! So in one picture: multiple callback groups, serial vs parallel tasks, timers
//! vs subscribers, and cross-process `topic.publish:<t>` → `topic.callback:<t>`
//! flow arrows from the hub into both consumers. Each callback also carries its
//! queue_wait/group_wait breakdown in args (click it).

use aorta_sdk::{
    AortaResult, CallbackGroupHandle, CallbackGroupType, ExecutorOptions, MultiThreadedExecutor,
    Node, SubscriberOptions,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const RUN_SECS: u64 = 8;

/// Burn ~`ms` of CPU (a tight LCG the optimizer can't elide) and fold into
/// `sink` — a CPU-bound callback, so wall ≈ thread time except the group_wait
/// spent blocked on a shared lock.
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

fn role() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--role" {
            return args.next().unwrap_or_default();
        }
    }
    String::new()
}

/// Run `exec` for `RUN_SECS` then stop. Shared tail for every role.
async fn spin_for(mut exec: MultiThreadedExecutor) {
    let stop = exec.stop_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(RUN_SECS)).await;
        stop.stop();
    });
    exec.spin().await;
}

/// SENSOR HUB: one timer per topic publishes a small payload. The timers are the
/// only (anonymous) sched.callbacks here; the named work lives in the consumers.
async fn run_hub() -> AortaResult<()> {
    let mut exec = MultiThreadedExecutor::new_with_options(1024, 2, ExecutorOptions::default());
    let node = Node::new("sensor_hub", "default").await?;
    exec.add_node(node.inner().clone());
    // Named Reentrant group → the hub's timers don't serialize and their
    // callbacks read `sched.callback:publish`.
    let publish = node
        .inner()
        .create_callback_group_named(CallbackGroupType::Reentrant, Some("publish"))?;

    // (topic, period_ms)
    let streams = [
        ("camera/left", 60u64),
        ("camera/right", 60),
        ("camera/depth", 60),
        ("imu/accel", 15),
        ("imu/gyro", 15),
        ("cmd", 40),
        ("scan", 100),
    ];
    let mut timers = Vec::new();
    for (topic, period) in streams {
        let publisher = node.create_publisher_raw(topic).await?;
        let seq = Arc::new(AtomicU64::new(0));
        // Publish on the named Reentrant group so the hub's own timers don't
        // serialize against each other — the hub just emits; the contention
        // story lives in the consumers. Name each timer by its topic so its
        // callback reads `sched.callback:publish:<topic>`.
        let opts = aorta_sdk::TimerOptions::default()
            .with_callback_group_handle(publish)
            .with_name(format!("publish:{topic}"));
        timers.push(
            node.create_timer_with_options(
                Duration::from_millis(period),
                move || {
                    let n = seq.fetch_add(1, Ordering::Relaxed);
                    let _ = publisher.publish(n.to_le_bytes().to_vec());
                },
                opts,
            )
            .await?,
        );
    }
    println!("sensor_hub: publishing {} streams for {RUN_SECS}s...", streams.len());
    spin_for(exec).await;
    println!("sensor_hub: done.");
    Ok(())
}

/// Subscribe `topic` on `group`, burning `work_ms` per message into `sink` and
/// counting into `received`.
async fn sub_on(
    node: &Node,
    topic: &str,
    group: CallbackGroupHandle,
    work_ms: u64,
    sink: &Arc<AtomicU64>,
    received: &Arc<AtomicU64>,
) -> AortaResult<aorta_sdk::Subscriber> {
    let sink = Arc::clone(sink);
    let received = Arc::clone(received);
    let opts = SubscriberOptions::default().with_callback_group_handle(group);
    node.create_subscriber_with_options(
        topic,
        move |_payload, _ctx| {
            burn_cpu(work_ms, &sink);
            received.fetch_add(1, Ordering::Relaxed);
        },
        opts,
    )
    .await
}

/// PERCEPTION: a MutuallyExclusive `vision` group (3 cameras SERIALIZE) and a
/// Reentrant `imu` group (2 imu subs run CONCURRENTLY).
async fn run_perception() -> AortaResult<()> {
    let mut exec = MultiThreadedExecutor::new_with_options(1024, 4, ExecutorOptions::default());
    let node = Node::new("perception", "default").await?;
    exec.add_node(node.inner().clone());
    let vision = node
        .inner()
        .create_callback_group_named(CallbackGroupType::MutuallyExclusive, Some("vision"))?;
    let imu = node
        .inner()
        .create_callback_group_named(CallbackGroupType::Reentrant, Some("imu"))?;

    let sink = Arc::new(AtomicU64::new(0));
    let recv = Arc::new(AtomicU64::new(0));
    let mut subs = Vec::new();
    for cam in ["camera/left", "camera/right", "camera/depth"] {
        subs.push(sub_on(&node, cam, vision, 6, &sink, &recv).await?); // serialize
    }
    for axis in ["imu/accel", "imu/gyro"] {
        subs.push(sub_on(&node, axis, imu, 1, &sink, &recv).await?); // concurrent
    }
    println!("perception: vision(MutEx, 3 cams ~6ms) + imu(Reentrant, 2 ~1ms), {RUN_SECS}s...");
    spin_for(exec).await;
    println!(
        "perception: done, {} messages (sink={}).",
        recv.load(Ordering::Relaxed),
        sink.load(Ordering::Relaxed)
    );
    Ok(())
}

/// PLANNER: a MutuallyExclusive `control` group where a 50 Hz control-loop TIMER
/// and the `cmd` subscriber contend on one lock, plus a Reentrant `nav` group
/// for the `scan` subscriber.
async fn run_planner() -> AortaResult<()> {
    let mut exec = MultiThreadedExecutor::new_with_options(1024, 4, ExecutorOptions::default());
    let node = Node::new("planner", "default").await?;
    exec.add_node(node.inner().clone());
    let control = node
        .inner()
        .create_callback_group_named(CallbackGroupType::MutuallyExclusive, Some("control"))?;
    let nav = node
        .inner()
        .create_callback_group_named(CallbackGroupType::Reentrant, Some("nav"))?;

    let sink = Arc::new(AtomicU64::new(0));
    let recv = Arc::new(AtomicU64::new(0));
    let mut subs = Vec::new();
    subs.push(sub_on(&node, "cmd", control, 3, &sink, &recv).await?); // contends with the timer
    subs.push(sub_on(&node, "scan", nav, 5, &sink, &recv).await?); // concurrent

    // The control-loop timer shares the `control` MutuallyExclusive group with
    // the cmd subscriber, so they serialize against each other.
    let ticks = Arc::new(AtomicU64::new(0));
    let ticks_cb = Arc::clone(&ticks);
    let sink_cb = Arc::clone(&sink);
    let opts = aorta_sdk::TimerOptions::default()
        .with_callback_group_handle(control)
        .with_name("control_loop");
    let _timer = node
        .create_timer_with_options(
            Duration::from_millis(20),
            move || {
                burn_cpu(4, &sink_cb);
                ticks_cb.fetch_add(1, Ordering::Relaxed);
            },
            opts,
        )
        .await?;

    println!("planner: control(MutEx, 50Hz timer ~4ms + cmd ~3ms) + nav(Reentrant, scan ~5ms), {RUN_SECS}s...");
    spin_for(exec).await;
    println!(
        "planner: done, {} ticks, {} messages (sink={}).",
        ticks.load(Ordering::Relaxed),
        recv.load(Ordering::Relaxed),
        sink.load(Ordering::Relaxed)
    );
    Ok(())
}

#[tokio::main]
async fn main() -> AortaResult<()> {
    match role().as_str() {
        "hub" => run_hub().await,
        "perception" => run_perception().await,
        "planner" => run_planner().await,
        other => {
            eprintln!("usage: sched_xproc_demo --role hub|perception|planner (got {other:?})");
            Ok(())
        }
    }
}
