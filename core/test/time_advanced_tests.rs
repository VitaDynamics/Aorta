use super::*;
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
fn time_from_nanos_positive() {
    let time = Time::from_nanos(1_500_000_000);
    assert_eq!(time.as_nanos(), 1_500_000_000);
}

#[test]
fn time_from_nanos_zero() {
    let time = Time::from_nanos(0);
    assert_eq!(time.as_nanos(), 0);
}

#[test]
fn time_from_nanos_negative() {
    let time = Time::from_nanos(-1_000_000_000);
    assert_eq!(time.as_nanos(), -1_000_000_000);
}

#[test]
fn time_from_nanos_large() {
    let large = i64::MAX;
    let time = Time::from_nanos(large);
    assert_eq!(time.as_nanos(), large);
}

#[test]
fn time_as_secs_f64_positive() {
    let time = Time::from_nanos(1_500_000_000);
    let secs = time.as_secs_f64();
    assert!((secs - 1.5).abs() < f64::EPSILON);
}

#[test]
fn time_as_secs_f64_zero() {
    let time = Time::from_nanos(0);
    assert_eq!(time.as_secs_f64(), 0.0);
}

#[test]
fn time_as_secs_f64_negative() {
    let time = Time::from_nanos(-1_000_000_000);
    assert_eq!(time.as_secs_f64(), -1.0);
}

#[test]
fn time_as_secs_f64_fractional() {
    let time = Time::from_nanos(500_000_000);
    assert_eq!(time.as_secs_f64(), 0.5);
}

#[test]
fn time_clone() {
    let time = Time::from_nanos(1_000_000_000);
    let cloned = time;
    assert_eq!(time.as_nanos(), cloned.as_nanos());
}

#[test]
fn time_copy() {
    let time = Time::from_nanos(1_000_000_000);
    let copied = time;
    assert_eq!(time.as_nanos(), copied.as_nanos());
}

#[test]
fn time_equality() {
    let t1 = Time::from_nanos(1_000_000_000);
    let t2 = Time::from_nanos(1_000_000_000);
    let t3 = Time::from_nanos(2_000_000_000);

    assert_eq!(t1, t2);
    assert_ne!(t1, t3);
}

#[test]
fn time_debug() {
    let time = Time::from_nanos(1_000_000_000);
    let debug = format!("{:?}", time);
    assert!(debug.contains("Time"));
}

#[test]
fn clock_new_system() {
    let clock = Clock::new(ClockMode::System);
    assert_eq!(clock.mode(), ClockMode::System);
}

#[test]
fn clock_new_steady() {
    let clock = Clock::new(ClockMode::Steady);
    assert_eq!(clock.mode(), ClockMode::Steady);
}

#[test]
fn clock_new_simulation() {
    let clock = Clock::new(ClockMode::Simulation);
    assert_eq!(clock.mode(), ClockMode::Simulation);
}

#[test]
fn clock_system_constructor() {
    let clock = Clock::system();
    assert_eq!(clock.mode(), ClockMode::System);
}

#[test]
fn clock_steady_constructor() {
    let clock = Clock::steady();
    assert_eq!(clock.mode(), ClockMode::Steady);
}

#[test]
fn clock_clone() {
    let clock = Clock::system();
    let cloned = clock.clone();
    assert_eq!(clock.mode(), cloned.mode());
}

#[test]
fn clock_debug() {
    let clock = Clock::system();
    let debug = format!("{:?}", clock);
    assert!(debug.contains("Clock"));
}

#[test]
fn clock_mode_equality() {
    assert_eq!(ClockMode::System, ClockMode::System);
    assert_eq!(ClockMode::Steady, ClockMode::Steady);
    assert_eq!(ClockMode::Simulation, ClockMode::Simulation);

    assert_ne!(ClockMode::System, ClockMode::Steady);
    assert_ne!(ClockMode::Steady, ClockMode::Simulation);
}

#[test]
fn clock_mode_clone() {
    let mode = ClockMode::System;
    let cloned = mode;
    assert_eq!(mode, cloned);
}

#[test]
fn clock_mode_copy() {
    let mode = ClockMode::System;
    let copied = mode;
    assert_eq!(mode, copied);
}

#[test]
fn clock_mode_debug() {
    let modes = vec![ClockMode::System, ClockMode::Steady, ClockMode::Simulation];

    for mode in modes {
        let debug = format!("{:?}", mode);
        assert!(!debug.is_empty());
    }
}

#[test]
fn clock_now_system_advances() {
    let clock = Clock::system();
    let t1 = clock.now();
    std::thread::sleep(Duration::from_millis(10));
    let t2 = clock.now();

    assert!(t2.as_nanos() > t1.as_nanos());
}

#[test]
fn clock_now_steady_advances() {
    let clock = Clock::steady();
    let t1 = clock.now();
    std::thread::sleep(Duration::from_millis(10));
    let t2 = clock.now();

    assert!(t2.as_nanos() > t1.as_nanos());
}

#[test]
fn clock_now_steady_monotonic() {
    let clock = Clock::steady();

    // Take multiple samples
    let times: Vec<i64> = (0..10)
        .map(|_| {
            let t = clock.now().as_nanos();
            std::thread::sleep(Duration::from_millis(1));
            t
        })
        .collect();

    // Verify monotonic increase
    for i in 1..times.len() {
        assert!(
            times[i] >= times[i - 1],
            "Time went backwards: {} -> {}",
            times[i - 1],
            times[i]
        );
    }
}

#[test]
fn clock_now_simulation_follows_global_time_source() {
    with_time_source_guard(|| {
        let clock = Clock::new(ClockMode::Simulation);
        set_simulation_time_ns(42);
        set_time_source(TimeSource::Simulation);
        assert_eq!(clock.now().as_nanos(), 42);

        set_time_source(TimeSource::System);
        assert!(clock.now().as_nanos() > 0);
    });
}

#[test]
fn time_roundtrip_conversions() {
    let original_nanos = 1_234_567_890i64;
    let time = Time::from_nanos(original_nanos);
    let secs = time.as_secs_f64();
    let back_to_nanos = (secs * 1_000_000_000.0) as i64;

    // Allow for small floating point error
    assert!((back_to_nanos - original_nanos).abs() <= 1);
}

#[test]
fn time_extreme_values() {
    // Maximum positive
    let max_time = Time::from_nanos(i64::MAX);
    assert_eq!(max_time.as_nanos(), i64::MAX);

    // Maximum negative
    let min_time = Time::from_nanos(i64::MIN);
    assert_eq!(min_time.as_nanos(), i64::MIN);
}

#[test]
fn clock_steady_starts_from_zero() {
    let clock = Clock::steady();
    // First now() should be close to 0 (elapsed time since creation)
    let t1 = clock.now().as_nanos();
    assert!(t1 >= 0);
    assert!(t1 < 1_000_000_000); // Less than 1 second
}

#[test]
fn clock_multiple_instances_independent() {
    let clock1 = Clock::system();
    std::thread::sleep(Duration::from_millis(10));
    let clock2 = Clock::system();

    // Both should return valid times
    let t1 = clock1.now();
    let t2 = clock2.now();

    assert!(t1.as_nanos() > 0);
    assert!(t2.as_nanos() > 0);

    // System times should be close (within 100ms)
    let diff = (t1.as_nanos() - t2.as_nanos()).abs();
    assert!(diff < 100_000_000);
}

#[test]
fn clock_steady_independent_instances() {
    let clock1 = Clock::steady();
    std::thread::sleep(Duration::from_millis(50));
    let clock2 = Clock::steady();

    let t1 = clock1.now().as_nanos();
    let t2 = clock2.now().as_nanos();

    // clock1 should have ~50ms head start
    assert!(t1 > t2);
    assert!(t1 - t2 >= 40_000_000); // At least 40ms difference
}

#[test]
fn time_as_secs_f64_precision() {
    // Test nanosecond precision is preserved
    let nanos = 1_234_567_890i64;
    let time = Time::from_nanos(nanos);
    let secs = time.as_secs_f64();

    // 1.234567890 seconds
    assert!((secs - 1.234567890).abs() < 1e-9);
}

#[test]
fn time_as_secs_f64_very_small() {
    let time = Time::from_nanos(1);
    let secs = time.as_secs_f64();
    assert_eq!(secs, 1e-9);
}

#[test]
fn time_as_secs_f64_very_large() {
    // 1_000_000_000 nanoseconds = 1 second
    let time = Time::from_nanos(1_000_000_000i64);
    let secs = time.as_secs_f64();
    assert_eq!(secs, 1.0);
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = Time::from_nanos(0);
    let _ = Clock::new(ClockMode::System);
}
