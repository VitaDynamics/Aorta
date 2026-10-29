use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time(i64);

impl Time {
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    pub fn as_nanos(self) -> i64 {
        self.0
    }

    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1_000_000_000.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockMode {
    System,
    Steady,
    Simulation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSource {
    System,
    Simulation,
}

const TIME_SOURCE_SYSTEM: u8 = 0;
const TIME_SOURCE_SIMULATION: u8 = 1;

static GLOBAL_TIME_SOURCE: AtomicU8 = AtomicU8::new(TIME_SOURCE_SYSTEM);
static SIMULATION_TIME_NS: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, Clone)]
pub struct Clock {
    mode: ClockMode,
    steady_start: Instant,
}

impl Clock {
    pub fn new(mode: ClockMode) -> Self {
        Self {
            mode,
            steady_start: Instant::now(),
        }
    }

    pub fn system() -> Self {
        Self::new(ClockMode::System)
    }

    pub fn steady() -> Self {
        Self::new(ClockMode::Steady)
    }

    pub fn mode(&self) -> ClockMode {
        self.mode
    }

    pub fn now(&self) -> Time {
        match self.mode {
            ClockMode::System => Time::from_nanos(system_time_ns()),
            ClockMode::Steady => Time::from_nanos(duration_to_i64(self.steady_start.elapsed())),
            ClockMode::Simulation => Time::from_nanos(source_time_ns()),
        }
    }
}

/// Set process-wide time source used by `now()`, `now_ns()` and `unix_time_ns()`.
pub fn set_time_source(source: TimeSource) {
    let raw = match source {
        TimeSource::System => TIME_SOURCE_SYSTEM,
        TimeSource::Simulation => TIME_SOURCE_SIMULATION,
    };
    GLOBAL_TIME_SOURCE.store(raw, Ordering::Relaxed);
}

/// Get process-wide time source used by `now()`, `now_ns()` and `unix_time_ns()`.
pub fn time_source() -> TimeSource {
    match GLOBAL_TIME_SOURCE.load(Ordering::Relaxed) {
        TIME_SOURCE_SIMULATION => TimeSource::Simulation,
        _ => TimeSource::System,
    }
}

/// Set process-wide simulation time in nanoseconds.
pub fn set_simulation_time_ns(nanos: i64) {
    SIMULATION_TIME_NS.store(nanos, Ordering::Relaxed);
}

/// Get process-wide simulation time in nanoseconds.
pub fn simulation_time_ns() -> i64 {
    SIMULATION_TIME_NS.load(Ordering::Relaxed)
}

/// Current time as `Time` from process-wide time source.
pub fn now() -> Time {
    Time::from_nanos(source_time_ns())
}

/// Current time in nanoseconds from process-wide time source.
pub fn now_ns() -> i64 {
    source_time_ns()
}

/// Current UNIX time in nanoseconds from process-wide time source.
///
/// Returns `0` when current source returns negative time.
pub fn unix_time_ns() -> u64 {
    let nanos = source_time_ns();
    if nanos > 0 {
        nanos as u64
    } else {
        0
    }
}

/// Process-global anchor for [`monotonic_ns`]. Initialized on first use.
static MONOTONIC_ANCHOR: OnceLock<Instant> = OnceLock::new();

/// Process-global MONOTONIC nanoseconds, backed by `std::time::Instant`.
///
/// Unlike [`now_ns`] / [`unix_time_ns`] (wall-clock, and steerable via
/// [`set_time_source`]), this is a strictly non-decreasing per-process timeline
/// that is independent of the wall clock, of NTP steps, and of the configured
/// time source. It is the authoritative ordering / latency source for tracing
/// (spans, stages, RPC/action stage timing); never use the wall-clock helpers
/// for latency deltas (they can absorb NTP/DST steps and produce negative or
/// skewed durations).
///
/// The value is nanoseconds since the first call in the process (an arbitrary
/// but fixed per-process epoch); only differences are meaningful. The range is
/// ~584 years before saturating at `u64::MAX`.
#[inline]
pub fn monotonic_ns() -> u64 {
    let anchor = MONOTONIC_ANCHOR.get_or_init(Instant::now);
    let nanos = anchor.elapsed().as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}

fn source_time_ns() -> i64 {
    match time_source() {
        TimeSource::System => system_time_ns(),
        TimeSource::Simulation => simulation_time_ns(),
    }
}

fn system_time_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_to_i64(duration),
        Err(_) => 0,
    }
}

fn duration_to_i64(duration: Duration) -> i64 {
    let nanos = duration.as_nanos();
    if nanos > i64::MAX as u128 {
        i64::MAX
    } else {
        nanos as i64
    }
}

#[cfg(test)]
#[path = "../test/time_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/time_advanced_tests.rs"]
mod advanced_tests;
