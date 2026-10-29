use aorta_sdk::log_prelude::*;

/// Late Joiner Example — pub/sub is a VOLATILE live stream.
///
/// The QoS & buffering redesign removed `TransientLocal`/`durability`/
/// `history_depth` (they were silent no-ops). Aorta pub/sub does NOT replay
/// earlier messages to a late-joining subscriber: a subscriber receives only
/// messages published after it joins, regardless of QoS.
///
/// To give a late joiner the CURRENT value on (re)connect — the thing
/// `TransientLocal` pretended to do — publish state through **ContextStore**
/// (a queryable last-value cache with an initial snapshot to new watchers). See
/// `rust/examples/` for a ContextStore example and the SDK README §ContextStore.
///
/// Usage:
/// 1. Terminal 1: bazel run //rust/examples:late_joiner
/// 2. Wait for a few messages to be published
/// 3. Terminal 2: bazel run //rust/examples:late_joiner -- --subscriber
/// 4. Observe that the late subscriber sees only NEW messages (no replay) — the
///    motivation for using ContextStore when you need the last value.
use aorta_sdk::{
    unix_time_ns, with_contiguous_bytes, AortaResult, MessageContext, Node, PublisherOptions, QoS,
    Reliability,
};
use examples_perf_msg::{aorta, examples, EXAMPLES_PERF_BFBS, EXAMPLES_PERF_FIXED_SIZE};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod fb_schema;

use examples::perf as perf_fb;
use fb_schema::{FbSchema, FbSchemaSpec};

struct RobotStatusSpec;

impl FbSchemaSpec for RobotStatusSpec {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_PERF_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_PERF_FIXED_SIZE
    }

    fn root_type() -> &'static str {
        "examples.perf.RobotStatus"
    }
}

type RobotStatusSchema = FbSchema<RobotStatusSpec>;

#[derive(Debug, Clone)]
struct RobotStatus {
    seq: u64,
    timestamp_ns: u64,
    battery_level: f32,
    mode: String,
    position: [f32; 3],
    error_count: u32,
}

impl RobotStatus {
    fn from_fb_data(data: &[u8]) -> Option<Self> {
        let msg = flatbuffers::root::<perf_fb::RobotStatus<'_>>(data).ok()?;
        let header = msg.aorta_header()?;
        Some(RobotStatus {
            seq: header.sequence(),
            timestamp_ns: header.publish_stamp_ns(),
            battery_level: msg.battery_level(),
            mode: msg.mode().unwrap_or("").to_string(),
            position: [msg.position_x(), msg.position_y(), msg.position_z()],
            error_count: msg.error_count(),
        })
    }
}

// Example binary marker to distinguish publisher vs subscriber
// This is a simple demo file - in real usage, these would be separate binaries

#[tokio::main]
async fn main() -> AortaResult<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 1 && args[1] == "--subscriber" {
        run_subscriber().await
    } else {
        run_publisher().await
    }
}

async fn run_publisher() -> AortaResult<()> {
    aorta_info!("=== Late Joiner Publisher (volatile pub/sub) ===\n");
    aorta_info!("Aorta pub/sub is a volatile live stream: late joiners do NOT get");
    aorta_info!("earlier messages. For last-value on (re)connect, use ContextStore.\n");

    let node = Node::new("late_joiner_publisher", "demo").await?;

    // Both topics are ordinary volatile publishers — `TransientLocal`/durability
    // were removed (silent no-ops). They are published on two keys only so the
    // subscriber can show that BOTH behave the same: no replay. Late-value /
    // latched snapshots are ContextStore's job now.
    aorta_info!("Creating two volatile publishers (no replay on either)...");
    let qos_a = QoS::default().with_reliability(Reliability::Reliable);
    let opts_a = PublisherOptions::new().with_qos(qos_a);
    let pub_transient = node
        .create_publisher_with_options::<RobotStatusSchema>("/robot/status/transient", opts_a)
        .await?;

    let qos_b = QoS::default().with_reliability(Reliability::Reliable);
    let opts_b = PublisherOptions::new().with_qos(qos_b);
    let pub_volatile = node
        .create_publisher_with_options::<RobotStatusSchema>("/robot/status/volatile", opts_b)
        .await?;

    aorta_info!("\nPublishing status updates every 2 seconds...");
    aorta_info!("Topics (both volatile — neither replays history):");
    aorta_info!("  - /robot/status/transient");
    aorta_info!("  - /robot/status/volatile");
    aorta_info!("*** A late subscriber will see only NEW messages on both ***\n");

    let seq = Arc::new(AtomicU64::new(0));
    let seq_clone = Arc::clone(&seq);

    let modes = [
        "IDLE",
        "NAVIGATING",
        "CHARGING",
        "TASK_EXECUTING",
        "STANDBY",
    ];

    let _timer = node
        .create_timer(Duration::from_secs(2), move || {
            let s = seq_clone.fetch_add(1, Ordering::Relaxed);
            let t = s as f32;

            let battery_level = 100.0 - (s as f32 * 2.5).min(80.0); // Drain from 100% to 20%
            let mode = modes[(s as usize) % modes.len()].to_string();
            let position_x = (t * 0.5).sin() * 10.0;
            let position_y = (t * 0.3).cos() * 10.0;
            let position_z = 0.0;
            let error_count = (s % 10) as u32; // Some varying error count

            aorta_info!(
                "[PUB] Status #{}: Battery={:.1}%, Mode={}, Pos=[{:.2}, {:.2}, {:.2}]",
                s,
                battery_level,
                mode,
                position_x,
                position_y,
                position_z
            );

            let mode_transient = mode.clone();
            let mode_volatile = mode;
            let uptime_seconds = s.saturating_mul(2);

            let _ = pub_transient.publish(|builder, header| {
                let mode = builder.create_string(mode_transient.as_str());
                let root = perf_fb::RobotStatus::create(
                    builder,
                    &perf_fb::RobotStatusArgs {
                        aorta_header: Some(header),
                        battery_level,
                        mode: Some(mode),
                        position_x,
                        position_y,
                        position_z,
                        error_count,
                        uptime_seconds,
                        last_error: None,
                    },
                );
                builder.finish(root, None);
            });
            let _ = pub_volatile.publish(|builder, header| {
                let mode = builder.create_string(mode_volatile.as_str());
                let root = perf_fb::RobotStatus::create(
                    builder,
                    &perf_fb::RobotStatusArgs {
                        aorta_header: Some(header),
                        battery_level,
                        mode: Some(mode),
                        position_x,
                        position_y,
                        position_z,
                        error_count,
                        uptime_seconds,
                        last_error: None,
                    },
                );
                builder.finish(root, None);
            });
        })
        .await?;

    aorta_info!("Publishing... Press Ctrl+C to stop\n");
    let _ = tokio::signal::ctrl_c().await;

    node.close().await?;
    Ok(())
}

async fn run_subscriber() -> AortaResult<()> {
    aorta_info!("=== Late Joiner Subscriber ===\n");
    aorta_info!("This subscriber joins AFTER the publisher has started.");
    aorta_info!("Because pub/sub is volatile, it will see only messages published");
    aorta_info!("from now on — no historical replay. (Use ContextStore for last-value.)\n");

    let node = Node::new("late_joiner_subscriber", "demo").await?;

    let transient_received = Arc::new(AtomicU64::new(0));
    let volatile_received = Arc::new(AtomicU64::new(0));

    // Subscribe to the first topic (a plain volatile subscriber).
    aorta_info!("Subscribing to /robot/status/transient...");
    let tr_count = Arc::clone(&transient_received);
    let _sub_transient = node
        .create_subscriber(
            "/robot/status/transient",
            move |data, _ctx: &MessageContext| {
                let count = tr_count.fetch_add(1, Ordering::Relaxed) + 1;
                with_contiguous_bytes(data, |bytes| {
                    if let Some(status) = RobotStatus::from_fb_data(bytes) {
                        aorta_info!(
                            "\n[TRANSIENT] Message #{} (received count: {})",
                            count, count
                        );
                        aorta_info!(
                            "  Status #{}: Battery={:.1}%, Mode={}, Errors={}",
                            status.seq, status.battery_level, status.mode, status.error_count
                        );
                        aorta_info!(
                            "  Position: [{:.2}, {:.2}, {:.2}]",
                            status.position[0], status.position[1], status.position[2]
                        );

                        if count == 1 {
                            aorta_info!("  *** FIRST message seen — it was published AFTER we subscribed (no replay) ***");
                        }

                        let now_ns = unix_time_ns();
                        let age_ms = now_ns.saturating_sub(status.timestamp_ns) / 1_000_000;
                        aorta_info!("  Message age: {} ms", age_ms);
                    }
                });
            },
        )
        .await?;

    // Subscribe to the second topic (also a plain volatile subscriber).
    aorta_info!("Subscribing to /robot/status/volatile...");
    let vo_count = Arc::clone(&volatile_received);
    let _sub_volatile = node
        .create_subscriber(
            "/robot/status/volatile",
            move |data, _ctx: &MessageContext| {
                let count = vo_count.fetch_add(1, Ordering::Relaxed) + 1;
                with_contiguous_bytes(data, |bytes| {
                    if let Some(status) = RobotStatus::from_fb_data(bytes) {
                        aorta_info!(
                            "\n[VOLATILE] Message #{} (received count: {})",
                            count,
                            count
                        );
                        aorta_info!(
                            "  Status #{}: Battery={:.1}%, Mode={}",
                            status.seq,
                            status.battery_level,
                            status.mode
                        );

                        if count == 1 {
                            aorta_info!(
                                "  *** FIRST message — published after subscription (no replay) ***"
                            );
                        }
                    }
                });
            },
        )
        .await?;

    aorta_info!("\nSubscriber ready. Waiting for messages...");
    aorta_info!("Expected behavior (both topics are volatile):");
    aorta_info!("  - Neither replays earlier messages; you see only NEW messages.");
    aorta_info!("  - Need the last value on connect? Publish it via ContextStore.\n");

    // Run for 30 seconds
    tokio::time::sleep(Duration::from_secs(30)).await;

    aorta_info!("\n=== Subscriber Statistics ===");
    aorta_info!(
        "'/transient' messages received: {}",
        transient_received.load(Ordering::Relaxed)
    );
    aorta_info!(
        "'/volatile' messages received: {}",
        volatile_received.load(Ordering::Relaxed)
    );
    aorta_info!(
        "\nNote: both counts reflect only messages published after subscribing (no replay)."
    );

    node.close().await?;
    aorta_info!("\n=== Subscriber Stopped ===");
    Ok(())
}
