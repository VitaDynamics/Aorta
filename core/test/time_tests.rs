use super::{
    now, now_ns, set_simulation_time_ns, set_time_source, simulation_time_ns, time_source,
    unix_time_ns, Clock, ClockMode, Time, TimeSource,
};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

fn with_time_source_guard<T>(f: impl FnOnce() -> T) -> T {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("time source test lock poisoned");
    let prev_source = time_source();
    let prev_sim_ns = simulation_time_ns();
    let result = f();
    set_simulation_time_ns(prev_sim_ns);
    set_time_source(prev_source);
    drop(guard);
    result
}

#[test]
fn time_conversions_roundtrip() {
    let t = Time::from_nanos(1_500_000_000);
    assert_eq!(t.as_nanos(), 1_500_000_000);
    assert!((t.as_secs_f64() - 1.5).abs() < f64::EPSILON);
}

#[test]
fn clock_mode_accessors() {
    let clock = Clock::system();
    assert_eq!(clock.mode(), ClockMode::System);
    let clock = Clock::steady();
    assert_eq!(clock.mode(), ClockMode::Steady);
}

#[test]
fn steady_clock_is_monotonic() {
    let clock = Clock::steady();
    let first = clock.now().as_nanos();
    std::thread::sleep(Duration::from_millis(1));
    let second = clock.now().as_nanos();
    assert!(second >= first);
}

#[test]
fn global_simulation_source_controls_now_interfaces() {
    with_time_source_guard(|| {
        let expected_ns = 1_234_567_890_i64;
        set_simulation_time_ns(expected_ns);
        set_time_source(TimeSource::Simulation);

        assert_eq!(now().as_nanos(), expected_ns);
        assert_eq!(now_ns(), expected_ns);
        assert_eq!(unix_time_ns(), expected_ns as u64);
    });
}

#[test]
fn unix_time_ns_clamps_negative_simulation_time() {
    with_time_source_guard(|| {
        set_simulation_time_ns(-42);
        set_time_source(TimeSource::Simulation);
        assert_eq!(unix_time_ns(), 0);
    });
}

#[test]
fn switching_back_to_system_source_restores_live_clock() {
    with_time_source_guard(|| {
        set_simulation_time_ns(7);
        set_time_source(TimeSource::Simulation);
        assert_eq!(now_ns(), 7);

        set_time_source(TimeSource::System);
        let system_ns = now_ns();
        assert!(system_ns > 0);
    });
}

#[test]
fn monotonic_ns_is_monotonic_nondecreasing() {
    let a = super::monotonic_ns();
    std::thread::sleep(Duration::from_millis(1));
    let b = super::monotonic_ns();
    assert!(b >= a, "monotonic_ns went backwards: {a} -> {b}");
    assert!(b > a, "expected progress after a 1ms sleep: {a} -> {b}");
}

#[test]
fn monotonic_ns_is_independent_of_time_source() {
    with_time_source_guard(|| {
        // Freeze the wall clock via the simulation time source.
        set_simulation_time_ns(1_000);
        set_time_source(TimeSource::Simulation);
        // Wall-clock helpers are frozen at the simulation value...
        assert_eq!(now_ns(), 1_000);

        // ...but monotonic_ns is independent of the configured time source and
        // keeps advancing across a real sleep. This is the property the tracer
        // relies on: latency timing must NOT come from the steerable wall clock.
        let a = super::monotonic_ns();
        std::thread::sleep(Duration::from_millis(1));
        let b = super::monotonic_ns();
        assert!(
            b > a,
            "monotonic_ns must ignore the simulation time source and advance: {a} -> {b}"
        );
        assert_eq!(now_ns(), 1_000, "wall clock must still be frozen");
    });
}
