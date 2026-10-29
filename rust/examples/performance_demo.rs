use aorta_sdk::log_prelude::*;

/// Performance Demo - Live QoS Comparison
///
/// Demonstrates and compares performance characteristics of different QoS settings:
/// - Latency comparison (Realtime vs Normal vs Background QoS)
/// - Reliability comparison (BestEffort vs Reliable)
/// - Throughput under load
/// - Message loss under high frequency
///
/// This example provides real-time performance metrics to help users choose
/// appropriate QoS settings for their use cases.
///
/// Run with: bazel run //rust/examples:performance_demo
use aorta_sdk::{
    unix_time_ns, with_contiguous_bytes, AortaResult, MessageContext, Node, PublisherOptions, QoS,
    Reliability, SubscriberOptions,
};
use examples_perf_msg::{aorta, examples, EXAMPLES_PERF_BFBS, EXAMPLES_PERF_FIXED_SIZE};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod fb_schema;

use examples::perf as perf_fb;
use fb_schema::{FbSchema, FbSchemaSpec};

struct TestMessageSpec;

impl FbSchemaSpec for TestMessageSpec {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_PERF_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_PERF_FIXED_SIZE
    }

    fn root_type() -> &'static str {
        "examples.perf.TestMessage"
    }
}

type TestMessageSchema = FbSchema<TestMessageSpec>;

#[derive(Debug, Clone)]
struct PerformanceMetrics {
    message_count: u64,
    min_latency_us: u64,
    max_latency_us: u64,
    avg_latency_us: u64,
    messages_lost: u64,
    last_seq: u64,
}

#[derive(Clone)]
struct LatencyTracker {
    samples: Arc<Mutex<VecDeque<u64>>>,
    max_samples: usize,
}

impl LatencyTracker {
    fn new(max_samples: usize) -> Self {
        Self {
            samples: Arc::new(Mutex::new(VecDeque::with_capacity(max_samples))),
            max_samples,
        }
    }

    fn add_sample(&self, latency_us: u64) {
        let mut samples = self.samples.lock().unwrap();
        samples.push_back(latency_us);
        if samples.len() > self.max_samples {
            samples.pop_front();
        }
    }

    fn get_metrics(&self) -> (u64, u64, u64) {
        // Returns (min, max, avg)
        let samples = self.samples.lock().unwrap();
        if samples.is_empty() {
            return (0, 0, 0);
        }

        let min = *samples.iter().min().unwrap();
        let max = *samples.iter().max().unwrap();
        let sum: u64 = samples.iter().sum();
        let avg = sum / samples.len() as u64;

        (min, max, avg)
    }
}

fn parse_test_message(data: &[u8]) -> Option<(u64, u64)> {
    let msg = flatbuffers::root::<perf_fb::TestMessage<'_>>(data).ok()?;
    let header = msg.aorta_header()?;
    Some((header.sequence(), msg.send_timestamp_ns()))
}

async fn run_publisher_test(
    node: &Node,
    topic: &str,
    qos: QoS,
    rate_hz: u64,
    duration_secs: u64,
    label: &str,
) -> AortaResult<u64> {
    aorta_info!("\n=== {} ===", label);
    aorta_info!("  Topic: {}", topic);
    aorta_info!("  Rate: {} Hz", rate_hz);
    aorta_info!(
        "  QoS: Priority={}, Reliability={:?}",
        qos.priority,
        qos.reliability
    );

    let options = PublisherOptions::new().with_qos(qos);
    let publisher = node
        .create_publisher_with_options::<TestMessageSchema>(topic, options)
        .await?;

    let seq = Arc::new(AtomicU64::new(0));
    let seq_clone = Arc::clone(&seq);

    let interval_us = 1_000_000 / rate_hz;
    let _timer = node
        .create_timer(Duration::from_micros(interval_us), move || {
            let _ = seq_clone.fetch_add(1, Ordering::Relaxed);
            let _ = publisher.publish(|builder, header| {
                let root = perf_fb::TestMessage::create(
                    builder,
                    &perf_fb::TestMessageArgs {
                        aorta_header: Some(header),
                        send_timestamp_ns: unix_time_ns(),
                        payload_size: 0,
                        payload: None,
                        test_id: None,
                    },
                );
                builder.finish(root, None);
            });
        })
        .await?;

    aorta_info!("  Publishing for {} seconds...", duration_secs);
    tokio::time::sleep(Duration::from_secs(duration_secs)).await;

    let total_sent = seq.load(Ordering::Relaxed);
    aorta_info!("  Messages sent: {}", total_sent);

    Ok(total_sent)
}

async fn run_subscriber_test(
    node: &Node,
    topic: &str,
    qos: QoS,
    duration_secs: u64,
    label: &str,
) -> AortaResult<PerformanceMetrics> {
    aorta_info!("\n=== {} (Subscriber) ===", label);
    aorta_info!(
        "  QoS: Priority={}, Reliability={:?}",
        qos.priority,
        qos.reliability
    );

    let tracker = LatencyTracker::new(1000);
    let tracker_clone = tracker.clone();

    let msg_count = Arc::new(AtomicU64::new(0));
    let msg_count_clone = Arc::clone(&msg_count);

    let last_seq = Arc::new(AtomicU64::new(0));
    let last_seq_clone = Arc::clone(&last_seq);

    let lost_count = Arc::new(AtomicU64::new(0));
    let lost_count_clone = Arc::clone(&lost_count);

    let options = SubscriberOptions::new().with_history_depth(10);

    let _subscriber = node
        .create_subscriber_with_options(
            topic,
            move |data, _ctx: &MessageContext| {
                with_contiguous_bytes(data, |bytes| {
                    if let Some((seq, send_time_ns)) = parse_test_message(bytes) {
                        let recv_time_ns = unix_time_ns();
                        let latency_ns = recv_time_ns.saturating_sub(send_time_ns);
                        let latency_us = latency_ns / 1000;

                        tracker_clone.add_sample(latency_us);
                        msg_count_clone.fetch_add(1, Ordering::Relaxed);

                        // Check for lost messages
                        let prev_seq = last_seq_clone.load(Ordering::Relaxed);
                        if prev_seq > 0 && seq > prev_seq + 1 {
                            let lost = seq - prev_seq - 1;
                            lost_count_clone.fetch_add(lost, Ordering::Relaxed);
                        }
                        last_seq_clone.store(seq, Ordering::Relaxed);
                    }
                });
            },
            options,
        )
        .await?;

    aorta_info!("  Receiving for {} seconds...", duration_secs);
    tokio::time::sleep(Duration::from_secs(duration_secs)).await;

    let (min, max, avg) = tracker.get_metrics();
    let metrics = PerformanceMetrics {
        message_count: msg_count.load(Ordering::Relaxed),
        min_latency_us: min,
        max_latency_us: max,
        avg_latency_us: avg,
        messages_lost: lost_count.load(Ordering::Relaxed),
        last_seq: last_seq.load(Ordering::Relaxed),
    };

    aorta_info!("\n  Results:");
    aorta_info!("    Messages received: {}", metrics.message_count);
    aorta_info!("    Messages lost: {}", metrics.messages_lost);
    aorta_info!("    Last sequence: {}", metrics.last_seq);
    aorta_info!(
        "    Latency (us): min={}, max={}, avg={}",
        metrics.min_latency_us,
        metrics.max_latency_us,
        metrics.avg_latency_us
    );

    Ok(metrics)
}

async fn run_latency_comparison() -> AortaResult<()> {
    aorta_info!("\n");
    aorta_info!("╔════════════════════════════════════════════════════════════╗");
    aorta_info!("║         TEST 1: Latency Comparison (100 Hz)               ║");
    aorta_info!("╚════════════════════════════════════════════════════════════╝");

    let pub_node = Node::new("perf_pub_latency", "perf_test").await?;
    let sub_node = Node::new("perf_sub_latency", "perf_test").await?;

    let test_duration = 5;

    // Test 1: Realtime QoS (Reliable, High Priority)
    let qos_realtime = QoS::realtime_control();
    let tracker1 = LatencyTracker::new(1000);
    let t1_clone = tracker1.clone();
    let recv_count1 = Arc::new(AtomicU64::new(0));
    let rc1_clone = Arc::clone(&recv_count1);

    let _sub1 = sub_node
        .create_subscriber_with_options(
            "/perf/latency/realtime",
            move |data, _ctx: &MessageContext| {
                with_contiguous_bytes(data, |bytes| {
                    if let Some((_, send_time_ns)) = parse_test_message(bytes) {
                        let latency_us = (unix_time_ns() - send_time_ns) / 1000;
                        t1_clone.add_sample(latency_us);
                        rc1_clone.fetch_add(1, Ordering::Relaxed);
                    }
                });
            },
            SubscriberOptions::new().with_history_depth(5),
        )
        .await?;

    run_publisher_test(
        &pub_node,
        "/perf/latency/realtime",
        qos_realtime,
        100,
        test_duration,
        "Realtime QoS Test",
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test 2: Normal QoS (Reliable, Normal Priority)
    let qos_normal = QoS::default().with_reliability(Reliability::Reliable);
    let tracker2 = LatencyTracker::new(1000);
    let t2_clone = tracker2.clone();
    let recv_count2 = Arc::new(AtomicU64::new(0));
    let rc2_clone = Arc::clone(&recv_count2);

    let _sub2 = sub_node
        .create_subscriber_with_options(
            "/perf/latency/normal",
            move |data, _ctx: &MessageContext| {
                with_contiguous_bytes(data, |bytes| {
                    if let Some((_, send_time_ns)) = parse_test_message(bytes) {
                        let latency_us = (unix_time_ns() - send_time_ns) / 1000;
                        t2_clone.add_sample(latency_us);
                        rc2_clone.fetch_add(1, Ordering::Relaxed);
                    }
                });
            },
            SubscriberOptions::new().with_history_depth(10),
        )
        .await?;

    run_publisher_test(
        &pub_node,
        "/perf/latency/normal",
        qos_normal,
        100,
        test_duration,
        "Normal QoS Test",
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Test 3: Background QoS (Reliable, Low Priority)
    let qos_background = QoS::bulk_transfer();
    let tracker3 = LatencyTracker::new(1000);
    let t3_clone = tracker3.clone();
    let recv_count3 = Arc::new(AtomicU64::new(0));
    let rc3_clone = Arc::clone(&recv_count3);

    let _sub3 = sub_node
        .create_subscriber_with_options(
            "/perf/latency/background",
            move |data, _ctx: &MessageContext| {
                with_contiguous_bytes(data, |bytes| {
                    if let Some((_, send_time_ns)) = parse_test_message(bytes) {
                        let latency_us = (unix_time_ns() - send_time_ns) / 1000;
                        t3_clone.add_sample(latency_us);
                        rc3_clone.fetch_add(1, Ordering::Relaxed);
                    }
                });
            },
            SubscriberOptions::new().with_history_depth(10),
        )
        .await?;

    run_publisher_test(
        &pub_node,
        "/perf/latency/background",
        qos_background,
        100,
        test_duration,
        "Background QoS Test",
    )
    .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Print comparison
    aorta_info!("\n");
    aorta_info!("╔════════════════════════════════════════════════════════════╗");
    aorta_info!("║              Latency Comparison Results                   ║");
    aorta_info!("╚════════════════════════════════════════════════════════════╝");

    let (min1, max1, avg1) = tracker1.get_metrics();
    let (min2, max2, avg2) = tracker2.get_metrics();
    let (min3, max3, avg3) = tracker3.get_metrics();

    aorta_info!(
        "\n{:<20} {:<15} {:<15} {:<15} {:<10}",
        "QoS Type",
        "Min (µs)",
        "Max (µs)",
        "Avg (µs)",
        "Messages"
    );
    aorta_info!("{:-<75}", "");
    aorta_info!(
        "{:<20} {:<15} {:<15} {:<15} {:<10}",
        "Realtime",
        min1,
        max1,
        avg1,
        recv_count1.load(Ordering::Relaxed)
    );
    aorta_info!(
        "{:<20} {:<15} {:<15} {:<15} {:<10}",
        "Normal",
        min2,
        max2,
        avg2,
        recv_count2.load(Ordering::Relaxed)
    );
    aorta_info!(
        "{:<20} {:<15} {:<15} {:<15} {:<10}",
        "Background",
        min3,
        max3,
        avg3,
        recv_count3.load(Ordering::Relaxed)
    );

    pub_node.close().await?;
    sub_node.close().await?;

    Ok(())
}

async fn run_reliability_comparison() -> AortaResult<()> {
    aorta_info!("\n");
    aorta_info!("╔════════════════════════════════════════════════════════════╗");
    aorta_info!("║    TEST 2: Reliability Comparison (High Rate: 200 Hz)     ║");
    aorta_info!("╚════════════════════════════════════════════════════════════╝");

    let pub_node = Node::new("perf_pub_reliability", "perf_test").await?;
    let sub_node = Node::new("perf_sub_reliability", "perf_test").await?;

    let test_duration = 5;
    let rate_hz = 200; // High rate to potentially induce drops

    // Test BestEffort
    let metrics1 = run_subscriber_test(
        &sub_node,
        "/perf/reliability/best_effort",
        QoS::default().with_reliability(Reliability::BestEffort),
        test_duration + 1,
        "BestEffort Reliability",
    );

    let sent1 = run_publisher_test(
        &pub_node,
        "/perf/reliability/best_effort",
        QoS::default().with_reliability(Reliability::BestEffort),
        rate_hz,
        test_duration,
        "BestEffort Publisher",
    );

    let (sent1, metrics1) = tokio::join!(sent1, metrics1);
    let sent1 = sent1?;
    let metrics1 = metrics1?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test Reliable
    let metrics2 = run_subscriber_test(
        &sub_node,
        "/perf/reliability/reliable",
        QoS::default().with_reliability(Reliability::Reliable),
        test_duration + 1,
        "Reliable Reliability",
    );

    let sent2 = run_publisher_test(
        &pub_node,
        "/perf/reliability/reliable",
        QoS::default().with_reliability(Reliability::Reliable),
        rate_hz,
        test_duration,
        "Reliable Publisher",
    );

    let (sent2, metrics2) = tokio::join!(sent2, metrics2);
    let sent2 = sent2?;
    let metrics2 = metrics2?;

    // Print comparison
    aorta_info!("\n");
    aorta_info!("╔════════════════════════════════════════════════════════════╗");
    aorta_info!("║           Reliability Comparison Results                  ║");
    aorta_info!("╚════════════════════════════════════════════════════════════╝");

    aorta_info!(
        "\n{:<20} {:<15} {:<15} {:<15}",
        "Reliability",
        "Sent",
        "Received",
        "Loss Rate"
    );
    aorta_info!("{:-<65}", "");

    let loss_rate1 = if sent1 > 0 {
        ((sent1 - metrics1.message_count) as f64 / sent1 as f64) * 100.0
    } else {
        0.0
    };

    let loss_rate2 = if sent2 > 0 {
        ((sent2 - metrics2.message_count) as f64 / sent2 as f64) * 100.0
    } else {
        0.0
    };

    aorta_info!(
        "{:<20} {:<15} {:<15} {:<14.2}%",
        "BestEffort",
        sent1,
        metrics1.message_count,
        loss_rate1
    );
    aorta_info!(
        "{:<20} {:<15} {:<15} {:<14.2}%",
        "Reliable",
        sent2,
        metrics2.message_count,
        loss_rate2
    );

    pub_node.close().await?;
    sub_node.close().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> AortaResult<()> {
    aorta_info!("\n");
    aorta_info!("╔════════════════════════════════════════════════════════════╗");
    aorta_info!("║         Aorta Middleware Performance Demo                 ║");
    aorta_info!("║                                                            ║");
    aorta_info!("║  This demo compares QoS settings with real measurements   ║");
    aorta_info!("╚════════════════════════════════════════════════════════════╝");

    let start = Instant::now();

    // Run latency comparison
    run_latency_comparison().await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Run reliability comparison
    run_reliability_comparison().await?;

    let elapsed = start.elapsed();

    aorta_info!("\n");
    aorta_info!("╔════════════════════════════════════════════════════════════╗");
    aorta_info!("║                  Performance Demo Complete                ║");
    aorta_info!("╚════════════════════════════════════════════════════════════╝");
    aorta_info!("\nTotal execution time: {:.2}s", elapsed.as_secs_f64());

    aorta_info!("\nKey Takeaways:");
    aorta_info!("  1. Realtime QoS offers lowest latency (BestEffort + High Priority)");
    aorta_info!("  2. Reliable QoS ensures delivery but may add latency");
    aorta_info!("  3. Background QoS has lower priority but similar reliability");
    aorta_info!("  4. Choose QoS based on your application's latency/reliability needs");

    Ok(())
}
