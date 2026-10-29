use aorta_sdk::log_prelude::*;

/// Sensor Subscriber Example
///
/// Companion to sensor_publisher - subscribes to sensor data with appropriate QoS:
/// - IMU data with Realtime QoS
/// - Camera frames with Normal QoS
/// - Health status (no last-value replay on startup — latched state for late
///   joiners is ContextStore's job, see the `late_joiner` example)
///
/// Run with: bazel run //rust/examples:sensor_subscriber
/// (Start sensor_publisher first in another terminal)
use aorta_sdk::{
    with_contiguous_bytes, AortaResult, MessageContext, Node, QoS, Reliability, SubscriberOptions,
};
use examples_sensor_msg::examples::sensor as sensor_fb;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn parse_imu_data(data: &[u8]) -> Option<String> {
    let msg = sensor_fb::root_as_imu_data(data).ok()?;
    let header = msg.aorta_header()?;
    let accel = msg.acceleration()?;
    let gyro = msg.gyroscope()?;
    Some(format!(
        "seq={}, accel=[{:.3}, {:.3}, {:.3}], gyro=[{:.3}, {:.3}, {:.3}]",
        header.sequence(),
        accel.x(),
        accel.y(),
        accel.z(),
        gyro.x(),
        gyro.y(),
        gyro.z()
    ))
}

fn parse_camera_frame(data: &[u8]) -> Option<String> {
    let msg = flatbuffers::root::<sensor_fb::CameraFrame<'_>>(data).ok()?;
    let header = msg.aorta_header()?;
    Some(format!(
        "seq={}, resolution={}x{}, size={} format={}",
        header.sequence(),
        msg.width(),
        msg.height(),
        msg.data_size(),
        msg.format().unwrap_or("")
    ))
}

fn parse_health_status(data: &[u8]) -> Option<String> {
    let msg = flatbuffers::root::<sensor_fb::HealthStatus<'_>>(data).ok()?;
    let header = msg.aorta_header()?;
    Some(format!(
        "seq={}, CPU={:.1}%, MEM={:.1}%, Status={}",
        header.sequence(),
        msg.cpu_usage(),
        msg.memory_usage(),
        msg.status().unwrap_or("")
    ))
}

#[tokio::main]
async fn main() -> AortaResult<()> {
    aorta_info!("=== Aorta Sensor Subscriber Example ===\n");

    let node = Node::new("sensor_subscriber", "robot").await?;
    aorta_info!("Created node: {}\n", node.name());

    // === 1. IMU Subscriber - Realtime QoS ===
    aorta_info!("1. Subscribing to /sensors/imu (Realtime QoS, Reliable)...");

    let imu_qos = QoS::realtime_control();
    aorta_info!("   QoS: {}", imu_qos);
    let imu_options = SubscriberOptions::new()
        .with_qos(imu_qos)
        .with_history_depth(5) // Small buffer for realtime data
        .with_receive_timestamp(true);

    let imu_count = Arc::new(AtomicU64::new(0));
    let imu_count_clone = Arc::clone(&imu_count);

    let _imu_sub = node
        .create_subscriber_with_options(
            "/sensors/imu",
            move |data, ctx: &MessageContext| {
                let count = imu_count_clone.fetch_add(1, Ordering::Relaxed) + 1;

                // Print every 50th message to avoid spam
                if count % 50 == 0 {
                    with_contiguous_bytes(data, |bytes| {
                        if let Some(parsed) = parse_imu_data(bytes) {
                            aorta_info!("  [IMU] Received #{}: {}", count, parsed);
                            aorta_info!("       Receive timestamp: {} ns", ctx.receive_stamp_ns);
                        } else {
                            aorta_warn!(
                                "  [IMU] Received invalid FlatBuffers payload ({} bytes)",
                                bytes.len()
                            );
                        }
                    });
                }
            },
            imu_options,
        )
        .await?;

    // === 2. Camera Subscriber - Normal QoS ===
    aorta_info!("\n2. Subscribing to /sensors/camera (Normal QoS, Reliable)...");

    let camera_qos = QoS::default().with_reliability(Reliability::Reliable);
    aorta_info!("   QoS: {}", camera_qos);
    let camera_options = SubscriberOptions::new()
        .with_qos(camera_qos)
        .with_history_depth(10)
        .with_topic_in_context(true)
        .with_priority_in_context(true);

    let camera_count = Arc::new(AtomicU64::new(0));
    let camera_count_clone = Arc::clone(&camera_count);

    let _camera_sub = node
        .create_subscriber_with_options(
            "/sensors/camera",
            move |data, ctx: &MessageContext| {
                let count = camera_count_clone.fetch_add(1, Ordering::Relaxed) + 1;
                with_contiguous_bytes(data, |bytes| {
                    if let Some(parsed) = parse_camera_frame(bytes) {
                        aorta_info!("  [CAMERA] Received #{}: {}", count, parsed);
                        aorta_info!(
                            "           Topic: {}, Priority: {}",
                            ctx.topic,
                            ctx.priority
                        );
                    } else {
                        aorta_warn!(
                            "  [CAMERA] Received invalid FlatBuffers payload ({} bytes)",
                            bytes.len()
                        );
                    }
                });
            },
            camera_options,
        )
        .await?;

    // === 3. Health Subscriber - Reliable QoS ===
    aorta_info!("\n3. Subscribing to /robot/health (Reliable QoS)...");
    aorta_info!("   Note: last-value snapshots for late joiners are now a ContextStore concern\n");

    let health_qos = QoS::default().with_reliability(Reliability::Reliable);
    aorta_info!("   QoS: {}", health_qos);
    let health_options = SubscriberOptions::new()
        .with_qos(health_qos)
        .with_history_depth(1)
        .with_receive_timestamp(true);

    let health_count = Arc::new(AtomicU64::new(0));
    let health_count_clone = Arc::clone(&health_count);
    let first_health = Arc::new(AtomicU64::new(0));
    let first_health_clone = Arc::clone(&first_health);

    let _health_sub = node
        .create_subscriber_with_options(
            "/robot/health",
            move |data, ctx: &MessageContext| {
                let count = health_count_clone.fetch_add(1, Ordering::Relaxed) + 1;

                if first_health_clone.load(Ordering::Relaxed) == 0 {
                    first_health_clone.store(1, Ordering::Relaxed);
                    aorta_info!("  [HEALTH] *** First message received (may be historical) ***");
                }

                with_contiguous_bytes(data, |bytes| {
                    if let Some(parsed) = parse_health_status(bytes) {
                        aorta_info!("  [HEALTH] Update #{}: {}", count, parsed);
                        let ts_ms = ctx.receive_stamp_ns / 1_000_000;
                        aorta_info!("           Receive time: {} ms", ts_ms);
                    } else {
                        aorta_warn!(
                            "  [HEALTH] Received invalid FlatBuffers payload ({} bytes)",
                            bytes.len()
                        );
                    }
                });
            },
            health_options,
        )
        .await?;

    aorta_info!("=== Receiving sensor data... (Press Ctrl+C to stop) ===\n");
    aorta_info!("Subscribed topics:");
    aorta_info!("  - /sensors/imu (printing every 50th message)");
    aorta_info!("  - /sensors/camera (all messages)");
    aorta_info!("  - /robot/health (all messages)\n");

    // Run for 30 seconds or until Ctrl+C
    tokio::time::sleep(Duration::from_secs(30)).await;

    aorta_info!("\n=== Statistics ===");
    aorta_info!(
        "IMU messages received: {}",
        imu_count.load(Ordering::Relaxed)
    );
    aorta_info!(
        "Camera frames received: {}",
        camera_count.load(Ordering::Relaxed)
    );
    aorta_info!(
        "Health updates received: {}",
        health_count.load(Ordering::Relaxed)
    );

    node.close().await?;
    aorta_info!("\n=== Sensor Subscriber Stopped ===");
    Ok(())
}
