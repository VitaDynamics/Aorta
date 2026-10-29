use aorta_sdk::log_prelude::*;

/// Control Loop Example
///
/// Demonstrates a multi-node robotic control system with:
/// - Sensor Node: Publishes position/velocity feedback (100Hz, Realtime QoS)
/// - Controller Node: Subscribes to feedback, publishes commands (100Hz, Realtime QoS)
/// - State Estimator Node: Fuses sensor data, publishes state (50Hz, Reliable QoS)
/// - Monitor Node: Logs system state (10Hz, Normal QoS)
///
/// This simulates a typical robot control architecture with multiple interacting nodes.
///
/// Run with: bazel run //rust/examples:control_loop
use aorta_sdk::{
    unix_time_ns, with_contiguous_bytes, AortaResult, MessageContext, Node, PublisherOptions, QoS,
    Reliability, SubscriberOptions,
};
use examples_control_msg::{aorta, examples, EXAMPLES_CONTROL_BFBS, EXAMPLES_CONTROL_FIXED_SIZE};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod fb_schema;

use examples::control as control_fb;
use fb_schema::{FbSchema, FbSchemaSpec};

struct SensorFeedbackSpec;
impl FbSchemaSpec for SensorFeedbackSpec {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_CONTROL_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_CONTROL_FIXED_SIZE
    }

    fn root_type() -> &'static str {
        "examples.control.SensorFeedback"
    }
}

struct ControlCommandSpec;
impl FbSchemaSpec for ControlCommandSpec {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_CONTROL_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_CONTROL_FIXED_SIZE
    }

    fn root_type() -> &'static str {
        "examples.control.ControlCommand"
    }
}

struct StateEstimateSpec;
impl FbSchemaSpec for StateEstimateSpec {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_CONTROL_BFBS
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_CONTROL_FIXED_SIZE
    }

    fn root_type() -> &'static str {
        "examples.control.StateEstimate"
    }
}

type SensorFeedbackSchema = FbSchema<SensorFeedbackSpec>;
type ControlCommandSchema = FbSchema<ControlCommandSpec>;
type StateEstimateSchema = FbSchema<StateEstimateSpec>;

#[derive(Debug, Clone)]
struct SensorFeedback {
    position: f32,
    velocity: f32,
}

#[derive(Debug, Clone)]
struct StateEstimate {
    position: f32,
    velocity: f32,
    acceleration: f32,
    confidence: f32,
}

impl SensorFeedback {
    fn from_fb_data(data: &[u8]) -> Option<Self> {
        let msg = flatbuffers::root::<control_fb::SensorFeedback<'_>>(data).ok()?;
        Some(Self {
            position: msg.position(),
            velocity: msg.velocity(),
        })
    }
}

impl StateEstimate {
    fn from_fb_data(data: &[u8]) -> Option<Self> {
        let msg = flatbuffers::root::<control_fb::StateEstimate<'_>>(data).ok()?;
        Some(Self {
            position: msg.position(),
            velocity: msg.velocity(),
            acceleration: msg.acceleration(),
            confidence: msg.confidence(),
        })
    }
}

// ============================================================================
// Sensor Node - Simulates hardware sensors
// ============================================================================

async fn run_sensor_node(running: Arc<AtomicBool>) -> AortaResult<()> {
    aorta_info!("[SENSOR] Starting sensor node...");
    let node = Node::new("sensor_node", "control_system").await?;

    let qos = QoS::realtime_control(); // High priority, Reliable for control traffic
    let options = PublisherOptions::new().with_qos(qos);
    let pub_feedback = node
        .create_publisher_with_options::<SensorFeedbackSchema>("/control/feedback", options)
        .await?;

    let seq = Arc::new(AtomicU64::new(0));
    let seq_clone = Arc::clone(&seq);

    // Simulate simple harmonic motion
    let running_timer = Arc::clone(&running);
    let _timer = node
        .create_timer(Duration::from_millis(10), move || {
            // 100Hz
            if !running_timer.load(Ordering::Relaxed) {
                return;
            }

            let s = seq_clone.fetch_add(1, Ordering::Relaxed);
            let t = s as f32 * 0.01; // Time in seconds

            // Simulate position following a sine wave with some noise
            let position = (t * 2.0).sin() + (t * 15.0).sin() * 0.05;
            let velocity = (t * 2.0).cos() * 2.0;
            let sensor_timestamp_ns = unix_time_ns();

            if s % 100 == 0 {
                // Log every 1 second
                aorta_info!(
                    "[SENSOR] Publishing feedback #{}: pos={:.3}, vel={:.3}",
                    s,
                    position,
                    velocity
                );
            }

            let _ = pub_feedback.publish(|builder, header| {
                let root = control_fb::SensorFeedback::create(
                    builder,
                    &control_fb::SensorFeedbackArgs {
                        aorta_header: Some(header),
                        position,
                        velocity,
                        acceleration: 0.0,
                        effort: 0.0,
                        sensor_timestamp_ns,
                    },
                );
                builder.finish(root, None);
            });
        })
        .await?;

    // Keep running until stopped
    while running.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    node.close().await?;
    aorta_info!("[SENSOR] Sensor node stopped");
    Ok(())
}

// ============================================================================
// Controller Node - PID controller
// ============================================================================

async fn run_controller_node(running: Arc<AtomicBool>) -> AortaResult<()> {
    aorta_info!("[CONTROLLER] Starting controller node...");
    let node = Node::new("controller_node", "control_system").await?;

    // Publisher for control commands
    let qos = QoS::realtime_control();
    let pub_options = PublisherOptions::new().with_qos(qos);
    let pub_command = node
        .create_publisher_with_options::<ControlCommandSchema>("/control/command", pub_options)
        .await?;

    // Subscriber for sensor feedback
    let sub_options = SubscriberOptions::new().with_history_depth(5);

    let last_feedback = Arc::new(Mutex::new(None::<SensorFeedback>));
    let last_feedback_clone = Arc::clone(&last_feedback);

    let _sub_feedback = node
        .create_subscriber_with_options(
            "/control/feedback",
            move |data, _ctx: &MessageContext| {
                with_contiguous_bytes(data, |bytes| {
                    if let Some(fb) = SensorFeedback::from_fb_data(bytes) {
                        *last_feedback_clone.lock().unwrap() = Some(fb);
                    }
                });
            },
            sub_options,
        )
        .await?;

    // Control loop at 100Hz
    let cmd_seq = Arc::new(AtomicU64::new(0));
    let cmd_seq_clone = Arc::clone(&cmd_seq);
    let last_feedback_timer = Arc::clone(&last_feedback);

    let running_timer = Arc::clone(&running);
    let _timer = node
        .create_timer(Duration::from_millis(10), move || {
            // 100Hz
            if !running_timer.load(Ordering::Relaxed) {
                return;
            }

            let s = cmd_seq_clone.fetch_add(1, Ordering::Relaxed);
            let target_position = 0.0; // Setpoint: maintain position at 0
            let target_velocity = 0.0;

            // Simple P controller
            let control_effort = if let Some(fb) = last_feedback_timer.lock().unwrap().clone() {
                let error = target_position - fb.position;
                let kp = 10.0; // Proportional gain
                kp * error
            } else {
                0.0
            };

            if s % 100 == 0 {
                aorta_info!(
                    "[CONTROLLER] Publishing command #{}: effort={:.3}",
                    s,
                    control_effort
                );
            }

            let _ = pub_command.publish(|builder, header| {
                let root = control_fb::ControlCommand::create(
                    builder,
                    &control_fb::ControlCommandArgs {
                        aorta_header: Some(header),
                        target_position,
                        target_velocity,
                        control_effort,
                        feedforward_accel: 0.0,
                        control_mode: 0,
                    },
                );
                builder.finish(root, None);
            });
        })
        .await?;

    while running.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    node.close().await?;
    aorta_info!("[CONTROLLER] Controller node stopped");
    Ok(())
}

// ============================================================================
// State Estimator Node - Kalman filter (simplified)
// ============================================================================

async fn run_state_estimator_node(running: Arc<AtomicBool>) -> AortaResult<()> {
    aorta_info!("[ESTIMATOR] Starting state estimator node...");
    let node = Node::new("estimator_node", "control_system").await?;

    // Publisher for state estimates
    let qos = QoS::default().with_reliability(Reliability::Reliable);
    let pub_options = PublisherOptions::new().with_qos(qos);
    let pub_state = node
        .create_publisher_with_options::<StateEstimateSchema>("/control/state", pub_options)
        .await?;

    // Subscriber for feedback
    let sub_options = SubscriberOptions::new().with_history_depth(10);

    let feedback_buffer = Arc::new(Mutex::new(Vec::<SensorFeedback>::new()));
    let feedback_buffer_clone = Arc::clone(&feedback_buffer);

    let _sub_feedback = node
        .create_subscriber_with_options(
            "/control/feedback",
            move |data, _ctx: &MessageContext| {
                with_contiguous_bytes(data, |bytes| {
                    if let Some(fb) = SensorFeedback::from_fb_data(bytes) {
                        let mut buffer = feedback_buffer_clone.lock().unwrap();
                        buffer.push(fb);
                        if buffer.len() > 10 {
                            buffer.remove(0);
                        }
                    }
                });
            },
            sub_options,
        )
        .await?;

    // Estimate state at 50Hz
    let state_seq = Arc::new(AtomicU64::new(0));
    let state_seq_clone = Arc::clone(&state_seq);

    let running_timer = Arc::clone(&running);
    let _timer = node
        .create_timer(Duration::from_millis(20), move || {
            // 50Hz
            if !running_timer.load(Ordering::Relaxed) {
                return;
            }

            let s = state_seq_clone.fetch_add(1, Ordering::Relaxed);
            let buffer = feedback_buffer.lock().unwrap();

            if buffer.is_empty() {
                return;
            }

            // Simple averaging filter
            let avg_pos: f32 = buffer.iter().map(|f| f.position).sum::<f32>() / buffer.len() as f32;
            let avg_vel: f32 = buffer.iter().map(|f| f.velocity).sum::<f32>() / buffer.len() as f32;

            // Estimate acceleration from velocity change
            let accel = if buffer.len() >= 2 {
                let dv = buffer.last().unwrap().velocity - buffer[buffer.len() - 2].velocity;
                dv / 0.01 // dt = 10ms
            } else {
                0.0
            };

            let confidence = (buffer.len() as f32 / 10.0).min(1.0);
            let sample_count = buffer.len() as u32;

            if s % 50 == 0 {
                // Log every 1 second
                aorta_info!(
                    "[ESTIMATOR] State estimate #{}: pos={:.3}, vel={:.3}, acc={:.3}, conf={:.2}",
                    s,
                    avg_pos,
                    avg_vel,
                    accel,
                    confidence
                );
            }

            let _ = pub_state.publish(|builder, header| {
                let root = control_fb::StateEstimate::create(
                    builder,
                    &control_fb::StateEstimateArgs {
                        aorta_header: Some(header),
                        position: avg_pos,
                        velocity: avg_vel,
                        acceleration: accel,
                        confidence,
                        sample_count,
                        innovation: 0.0,
                    },
                );
                builder.finish(root, None);
            });
        })
        .await?;

    while running.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    node.close().await?;
    aorta_info!("[ESTIMATOR] State estimator node stopped");
    Ok(())
}

// ============================================================================
// Monitor Node - System health monitoring
// ============================================================================

async fn run_monitor_node(running: Arc<AtomicBool>) -> AortaResult<()> {
    aorta_info!("[MONITOR] Starting monitor node...");
    let node = Node::new("monitor_node", "control_system").await?;

    let feedback_count = Arc::new(AtomicU64::new(0));
    let command_count = Arc::new(AtomicU64::new(0));
    let state_count = Arc::new(AtomicU64::new(0));

    // Subscribe to all topics
    let fc_clone = Arc::clone(&feedback_count);
    let _sub_feedback = node
        .create_subscriber("/control/feedback", move |_data, _ctx: &MessageContext| {
            fc_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    let cc_clone = Arc::clone(&command_count);
    let _sub_command = node
        .create_subscriber("/control/command", move |_data, _ctx: &MessageContext| {
            cc_clone.fetch_add(1, Ordering::Relaxed);
        })
        .await?;

    let sc_clone = Arc::clone(&state_count);
    let last_state = Arc::new(Mutex::new(None::<StateEstimate>));
    let last_state_clone = Arc::clone(&last_state);
    let _sub_state = node
        .create_subscriber("/control/state", move |data, _ctx: &MessageContext| {
            sc_clone.fetch_add(1, Ordering::Relaxed);
            with_contiguous_bytes(data, |bytes| {
                if let Some(state) = StateEstimate::from_fb_data(bytes) {
                    *last_state_clone.lock().unwrap() = Some(state);
                }
            });
        })
        .await?;

    // Print statistics every second
    let running_timer = Arc::clone(&running);
    let _timer = node
        .create_timer(Duration::from_secs(1), move || {
            if !running_timer.load(Ordering::Relaxed) {
                return;
            }

            let fb = feedback_count.load(Ordering::Relaxed);
            let cmd = command_count.load(Ordering::Relaxed);
            let state = state_count.load(Ordering::Relaxed);

            aorta_info!("\n[MONITOR] === System Status ===");
            aorta_info!("  Feedback messages: {} (~{}Hz)", fb, fb);
            aorta_info!("  Control commands: {} (~{}Hz)", cmd, cmd);
            aorta_info!("  State estimates: {} (~{}Hz)", state, state);

            if let Some(s) = last_state.lock().unwrap().clone() {
                aorta_info!(
                    "  Latest State: pos={:.3}, vel={:.3}, acc={:.3}, conf={:.2}",
                    s.position,
                    s.velocity,
                    s.acceleration,
                    s.confidence
                );
            }
            aorta_info!("========================\n");

            // Reset counters
            feedback_count.store(0, Ordering::Relaxed);
            command_count.store(0, Ordering::Relaxed);
            state_count.store(0, Ordering::Relaxed);
        })
        .await?;

    while running.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    node.close().await?;
    aorta_info!("[MONITOR] Monitor node stopped");
    Ok(())
}

// ============================================================================
// Main - Launch all nodes
// ============================================================================

#[tokio::main]
async fn main() -> AortaResult<()> {
    aorta_info!("=== Aorta Control Loop Example ===\n");
    aorta_info!("Launching multi-node control system:");
    aorta_info!("  1. Sensor Node (100Hz feedback)");
    aorta_info!("  2. Controller Node (100Hz PID control)");
    aorta_info!("  3. State Estimator Node (50Hz state fusion)");
    aorta_info!("  4. Monitor Node (1Hz system statistics)\n");

    let running = Arc::new(AtomicBool::new(true));

    // Launch all nodes concurrently
    let r1 = Arc::clone(&running);
    let r2 = Arc::clone(&running);
    let r3 = Arc::clone(&running);
    let r4 = Arc::clone(&running);

    let handle_sensor = tokio::spawn(async move { run_sensor_node(r1).await });
    tokio::time::sleep(Duration::from_millis(100)).await; // Stagger startup

    let handle_controller = tokio::spawn(async move { run_controller_node(r2).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let handle_estimator = tokio::spawn(async move { run_state_estimator_node(r3).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let handle_monitor = tokio::spawn(async move { run_monitor_node(r4).await });

    aorta_info!("\n=== All nodes running (Press Ctrl+C to stop) ===\n");

    // Run for 30 seconds
    tokio::time::sleep(Duration::from_secs(30)).await;

    aorta_info!("\n\n=== Shutting down control system ===");
    running.store(false, Ordering::Relaxed);

    // Wait for all nodes to complete
    let _ = tokio::join!(
        handle_sensor,
        handle_controller,
        handle_estimator,
        handle_monitor
    );

    aorta_info!("\n=== Control Loop Example Complete ===");
    Ok(())
}
