use super::*;

#[test]
fn test_priority_default() {
    assert_eq!(Priority::default(), Priority::Normal);
}

#[test]
fn test_priority_to_zenoh() {
    assert_eq!(
        zenoh::qos::Priority::from(Priority::Realtime),
        zenoh::qos::Priority::RealTime
    );
    assert_eq!(
        zenoh::qos::Priority::from(Priority::High),
        zenoh::qos::Priority::InteractiveLow
    );
    assert_eq!(
        zenoh::qos::Priority::from(Priority::Normal),
        zenoh::qos::Priority::DataLow
    );
    assert_eq!(
        zenoh::qos::Priority::from(Priority::Low),
        zenoh::qos::Priority::Background
    );
    assert_eq!(
        zenoh::qos::Priority::from(Priority::Background),
        zenoh::qos::Priority::Background
    );
}

#[test]
fn test_reliability_default() {
    assert_eq!(Reliability::default(), Reliability::Reliable);
}

#[test]
fn test_reliability_to_zenoh() {
    assert_eq!(
        zenoh::qos::Reliability::from(Reliability::BestEffort),
        zenoh::qos::Reliability::BestEffort
    );
    assert_eq!(
        zenoh::qos::Reliability::from(Reliability::Reliable),
        zenoh::qos::Reliability::Reliable
    );
}

#[test]
fn test_congestion_control_default() {
    assert_eq!(CongestionControl::default(), CongestionControl::Drop);
}

#[test]
fn test_qos_default() {
    // QoS & buffering redesign: 4 wired fields, no durability/history.
    let qos = QoS::default();
    assert_eq!(qos.priority, Priority::Normal);
    assert_eq!(qos.reliability, Reliability::Reliable);
    assert_eq!(qos.congestion_control, CongestionControl::Drop);
    assert!(!qos.express);
}

#[test]
fn test_qos_new_defaults() {
    let qos = QoS::new(Priority::High, Reliability::BestEffort);
    assert_eq!(qos.priority, Priority::High);
    assert_eq!(qos.reliability, Reliability::BestEffort);
    assert_eq!(qos.congestion_control, CongestionControl::Drop);
    assert!(!qos.express);
}

#[test]
fn test_qos_builder() {
    let qos = QoS::default()
        .with_priority(Priority::High)
        .with_reliability(Reliability::BestEffort)
        .with_congestion_control(CongestionControl::Block)
        .with_express(true);

    assert_eq!(qos.priority, Priority::High);
    assert_eq!(qos.reliability, Reliability::BestEffort);
    assert_eq!(qos.congestion_control, CongestionControl::Block);
    assert!(qos.express);
}

#[test]
fn test_qos_presets() {
    // Presets — every field maps to a wired Zenoh primitive.
    let rt = QoS::realtime_control();
    assert_eq!(rt.priority, Priority::Realtime);
    assert_eq!(rt.reliability, Reliability::Reliable);
    // No preset blocks by default (pre-redesign every publisher effectively
    // ran Drop; Block is an explicit opt-in via with_congestion_control).
    assert_eq!(rt.congestion_control, CongestionControl::Drop);
    assert!(rt.express);

    // Only realtime_control expresses by default; every other preset is
    // lowest-latency Drop + non-express.
    let sensor = QoS::sensor_data();
    assert_eq!(sensor.priority, Priority::Normal);
    assert_eq!(sensor.reliability, Reliability::BestEffort);
    assert_eq!(sensor.congestion_control, CongestionControl::Drop);
    assert!(!sensor.express);

    let state = QoS::state_update();
    assert_eq!(state.priority, Priority::High);
    assert_eq!(state.reliability, Reliability::Reliable);
    assert_eq!(state.congestion_control, CongestionControl::Drop);
    assert!(!state.express);

    let bulk = QoS::bulk_transfer();
    assert_eq!(bulk.priority, Priority::Background);
    assert_eq!(bulk.congestion_control, CongestionControl::Drop);
    assert!(!bulk.express);
}

#[test]
fn test_display() {
    assert_eq!(Priority::Realtime.to_string(), "Realtime");
    assert_eq!(Reliability::BestEffort.to_string(), "BestEffort");
    assert_eq!(CongestionControl::Block.to_string(), "Block");

    let qos = QoS::default();
    assert!(qos.to_string().contains("priority=Normal"));
    assert!(qos.to_string().contains("reliability=Reliable"));
    assert!(qos.to_string().contains("congestion_control=Drop"));
}
