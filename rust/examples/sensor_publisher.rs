use aorta_sdk::log_prelude::*;

/// Sensor Publisher Example
///
/// Demonstrates publishing different types of sensor data with appropriate QoS settings:
/// - IMU data at 100Hz with Realtime QoS (low latency, best effort)
/// - Camera frames at 30Hz with Normal QoS (reliable delivery)
/// - Health status at 1Hz (no last-value replay — latched state for late
///   joiners is ContextStore's job, see the `late_joiner` example)
///
/// Run with: bazel run //rust/examples:sensor_publisher
use aorta_sdk::{
    unix_time_ns, AortaResult, HeaderInjector, HeaderValues, Node, PublisherOptions, QoS,
    Reliability, SchemaMeta,
};
use examples_sensor_msg::{aorta, examples, EXAMPLES_SENSOR_BFBS, EXAMPLES_SENSOR_FIXED_SIZE};
use flatbuffers::FlatBufferBuilder;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Module alias for convenience
use examples::sensor as sensor_fb;

// Schema metadata implementations
struct ImuDataSchema;
struct CameraFrameSchema;
struct HealthStatusSchema;

impl SchemaMeta for ImuDataSchema {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_SENSOR_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_SENSOR_FIXED_SIZE
    }
}

impl HeaderInjector for ImuDataSchema {
    type Header<'a> = aorta::sys::AortaHeader<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {
        let node = builder.create_string(values.publisher_node);
        aorta::sys::AortaHeader::create(
            builder,
            &aorta::sys::AortaHeaderArgs {
                publish_stamp_ns: values.publish_stamp_ns,
                publisher_node: Some(node),
                sequence: values.sequence,
            },
        )
    }
}

impl SchemaMeta for CameraFrameSchema {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_SENSOR_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_SENSOR_FIXED_SIZE
    }
}

impl HeaderInjector for CameraFrameSchema {
    type Header<'a> = aorta::sys::AortaHeader<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {
        let node = builder.create_string(values.publisher_node);
        aorta::sys::AortaHeader::create(
            builder,
            &aorta::sys::AortaHeaderArgs {
                publish_stamp_ns: values.publish_stamp_ns,
                publisher_node: Some(node),
                sequence: values.sequence,
            },
        )
    }
}

impl SchemaMeta for HealthStatusSchema {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_SENSOR_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_SENSOR_FIXED_SIZE
    }
}

impl HeaderInjector for HealthStatusSchema {
    type Header<'a> = aorta::sys::AortaHeader<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> flatbuffers::WIPOffset<Self::Header<'b>> {
        let node = builder.create_string(values.publisher_node);
        aorta::sys::AortaHeader::create(
            builder,
            &aorta::sys::AortaHeaderArgs {
                publish_stamp_ns: values.publish_stamp_ns,
                publisher_node: Some(node),
                sequence: values.sequence,
            },
        )
    }
}

fn simulate_imu_reading(seq: u64) -> (sensor_fb::Vector3, sensor_fb::Vector3) {
    let t = seq as f32 * 0.01; // 100Hz
    let accel = sensor_fb::Vector3::new(
        (t * 2.0).sin() * 9.81,
        (t * 3.0).cos() * 9.81,
        9.81 + (t * 1.5).sin() * 0.5,
    );
    let gyro = sensor_fb::Vector3::new(
        (t * 1.5).sin() * 0.1,
        (t * 2.5).cos() * 0.1,
        (t * 0.8).sin() * 0.05,
    );
    (accel, gyro)
}

#[tokio::main]
async fn main() -> AortaResult<()> {
    aorta_info!("=== Aorta Sensor Publisher Example ===\n");

    let node = Node::new("sensor_publisher", "robot").await?;
    aorta_info!("Created node: {}\n", node.name());

    // === 1. IMU Publisher - Realtime QoS (100Hz) ===
    aorta_info!("1. Creating IMU publisher (Realtime QoS, 100Hz)...");
    aorta_info!("   - Reliable delivery (realtime priority)");
    aorta_info!("   - No history for late joiners (latched state = ContextStore)");

    let mut imu_options = PublisherOptions::default();
    imu_options.qos = QoS::realtime_control(); // High priority, Reliable
    let imu_pub = node
        .create_publisher_with_options::<ImuDataSchema>("/sensors/imu", imu_options)
        .await?;

    let imu_seq = Arc::new(AtomicU64::new(0));
    let imu_seq_clone = Arc::clone(&imu_seq);

    let imu_timer = node
        .create_timer(Duration::from_millis(10), move || {
            // 100Hz
            let seq = imu_seq_clone.fetch_add(1, Ordering::Relaxed);
            let (accel, gyro) = simulate_imu_reading(seq);

            if seq % 50 == 0 {
                // Print every 50th message (every 0.5s)
                aorta_info!(
                    "  [IMU] Published frame #{}: accel=[{:.2}, {:.2}, {:.2}]",
                    seq,
                    accel.x(),
                    accel.y(),
                    accel.z()
                );
            }

            let _ = imu_pub.publish(|builder, header| {
                let mag = sensor_fb::Vector3::new(0.0, 0.0, 0.0);
                let data = sensor_fb::ImuData::create(
                    builder,
                    &sensor_fb::ImuDataArgs {
                        aorta_header: Some(header),
                        acceleration: Some(&accel),
                        gyroscope: Some(&gyro),
                        magnetometer: Some(&mag),
                        sensor_timestamp_ns: unix_time_ns(),
                        temperature: 25.0,
                    },
                );
                builder.finish(data, None);
            });
        })
        .await?;

    // === 2. Camera Publisher - Normal QoS (30Hz) ===
    aorta_info!("\n2. Creating Camera publisher (Normal QoS, 30Hz)...");
    aorta_info!("   - Reliable delivery (ensures message delivery)");
    aorta_info!("   - Volatile durability");

    let mut camera_options = PublisherOptions::default();
    camera_options.qos = QoS::default().with_reliability(Reliability::Reliable);
    let camera_pub = node
        .create_publisher_with_options::<CameraFrameSchema>("/sensors/camera", camera_options)
        .await?;

    let camera_seq = Arc::new(AtomicU64::new(0));
    let camera_seq_clone = Arc::clone(&camera_seq);

    let camera_timer = node
        .create_timer(Duration::from_millis(33), move || {
            // ~30Hz
            let seq = camera_seq_clone.fetch_add(1, Ordering::Relaxed);

            aorta_info!("  [CAMERA] Published frame #{}: 1920x1080 (RGB)", seq);

            let _ = camera_pub.publish(|builder, header| {
                let format = builder.create_string("RGB");
                let frame = sensor_fb::CameraFrame::create(
                    builder,
                    &sensor_fb::CameraFrameArgs {
                        aorta_header: Some(header),
                        width: 1920,
                        height: 1080,
                        format: Some(format),
                        data_size: 1920 * 1080 * 3,
                        capture_timestamp_ns: unix_time_ns(),
                        exposure_us: 8000,
                        iso: 400,
                    },
                );
                builder.finish(frame, None);
            });
        })
        .await?;

    // === 3. Health Publisher - Reliable QoS (1Hz) ===
    aorta_info!("\n3. Creating Health publisher (Reliable QoS, 1Hz)...");
    aorta_info!("   - Reliable delivery");
    // NOTE: late-joiner "last value" is no longer transport durability after the
    // QoS & buffering redesign; use ContextStore for latched/last-value state.

    let mut health_options = PublisherOptions::default();
    health_options.qos = QoS::default().with_reliability(Reliability::Reliable);
    let health_pub = node
        .create_publisher_with_options::<HealthStatusSchema>("/robot/health", health_options)
        .await?;

    let health_seq = Arc::new(AtomicU64::new(0));
    let health_seq_clone = Arc::clone(&health_seq);

    let health_timer = node
        .create_timer(Duration::from_secs(1), move || {
            // 1Hz
            let seq = health_seq_clone.fetch_add(1, Ordering::Relaxed);
            let cpu = 45.0 + (seq as f32 * 0.5).sin() * 15.0;
            let mem = 60.0 + (seq as f32 * 0.3).cos() * 10.0;
            let status_str = if cpu > 70.0 { "WARNING" } else { "OK" };

            aorta_info!(
                "  [HEALTH] Status #{}: CPU={:.1}%, MEM={:.1}%, Status={}",
                seq,
                cpu,
                mem,
                status_str
            );

            let _ = health_pub.publish(|builder, header| {
                let status = builder.create_string(status_str);
                let pos = sensor_fb::Vector3::new(
                    (seq as f32 * 0.1).sin() * 5.0,
                    (seq as f32 * 0.1).cos() * 5.0,
                    0.0,
                );
                let health = sensor_fb::HealthStatus::create(
                    builder,
                    &sensor_fb::HealthStatusArgs {
                        aorta_header: Some(header),
                        cpu_usage: cpu,
                        memory_usage: mem,
                        battery_level: 100.0 - (seq as f32 * 0.5).min(80.0),
                        status: Some(status),
                        position: Some(&pos),
                        error_count: 0,
                        uptime_seconds: seq,
                        error_message: None,
                    },
                );
                builder.finish(health, None);
            });
        })
        .await?;

    aorta_info!("\n=== Publishing sensor data... (Press Ctrl+C to stop) ===\n");
    aorta_info!("Tip: Run sensor_subscriber in another terminal to receive data");
    aorta_info!("Topics:");
    aorta_info!("  - /sensors/imu (100Hz, Realtime QoS)");
    aorta_info!("  - /sensors/camera (30Hz, Normal QoS)");
    aorta_info!("  - /robot/health (1Hz; no last-value replay — see ContextStore)\n");

    // Run for 30 seconds or until Ctrl+C
    tokio::time::sleep(Duration::from_secs(30)).await;

    aorta_info!("\n=== Statistics ===");
    aorta_info!(
        "IMU messages published: {}",
        imu_seq.load(Ordering::Relaxed)
    );
    aorta_info!(
        "Camera frames published: {}",
        camera_seq.load(Ordering::Relaxed)
    );
    aorta_info!(
        "Health updates published: {}",
        health_seq.load(Ordering::Relaxed)
    );

    // Cleanup
    imu_timer.pause();
    camera_timer.pause();
    health_timer.pause();
    node.close().await?;

    aorta_info!("\n=== Sensor Publisher Stopped ===");
    Ok(())
}
