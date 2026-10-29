//! Effective timeline and scheduler pure functions (MCAP Player Proposal §6.5).
//!
//! Two independent pieces live here, both deliberately free of I/O and of
//! `tokio`:
//!
//! - [`TimelineAccumulator`] — the streaming `previous_max` accumulator that
//!   turns the canonical record stream's `log_time`s into the *effective
//!   timeline* (`effective(m) = max(m.log_time, effective(prev))`, O(1)
//!   memory, clamp events counted). Pass 1 folds every message of every
//!   segment through it *before* windowing and *before* topic filtering, so
//!   pacing of the remainder is filter-independent (§6.5 ordering contract).
//! - [`Scheduler`] — absolute-target pacing over an injected [`Clock`].
//!   `target(m) = wall0 + (effective(m) − t0) / rate`, checked; a message
//!   published later than `target + late_threshold` counts as *late*. The real
//!   `tokio::time::sleep_until` driver is a later phase; the decision function
//!   here is what it will call, and what the mock-clock tests drive.

use crate::validate;
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Publish-lateness threshold (MCAP Player Proposal §6.5: 10 ms default).
pub const DEFAULT_LATE_THRESHOLD: Duration = Duration::from_millis(10);

// ---------------------------------------------------------------------------
// Effective timeline (MCAP Player Proposal §6.5 step 1)
// ---------------------------------------------------------------------------

/// Streaming `previous_max` accumulator over the canonical record stream.
///
/// `log_time` is only approximately monotonic within a recorder file (MCAP
/// Player Proposal §2), so a message that goes backwards is *clamped* to the
/// running maximum rather than reordered: file order stays authoritative and
/// the clamped message is replayed back-to-back with its predecessor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimelineAccumulator {
    previous_max: Option<u64>,
    messages: u64,
    clamped: u64,
    t_first: Option<u64>,
    t_last: Option<u64>,
}

impl TimelineAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one `log_time` into the timeline, returning its effective time.
    pub fn push(&mut self, log_time: u64) -> u64 {
        let effective = match self.previous_max {
            Some(previous) if previous > log_time => {
                self.clamped += 1;
                previous
            }
            _ => log_time,
        };
        self.previous_max = Some(effective);
        self.messages += 1;
        if self.t_first.is_none() {
            self.t_first = Some(effective);
        }
        self.t_last = Some(effective);
        effective
    }

    /// Effective time of the first message of the ordered segment set.
    pub fn t_first(&self) -> Option<u64> {
        self.t_first
    }

    /// Effective time of the last message folded so far.
    pub fn t_last(&self) -> Option<u64> {
        self.t_last
    }

    pub fn messages(&self) -> u64 {
        self.messages
    }

    /// Messages whose `log_time` went backwards and was clamped (MCAP Player
    /// Proposal §5).
    pub fn clamped(&self) -> u64 {
        self.clamped
    }
}

/// Convenience: the effective timeline of a whole `log_time` sequence.
pub fn effective_times(log_times: &[u64]) -> Vec<u64> {
    let mut accumulator = TimelineAccumulator::new();
    log_times.iter().map(|t| accumulator.push(*t)).collect()
}

// ---------------------------------------------------------------------------
// Window (MCAP Player Proposal §6.5 step 2)
// ---------------------------------------------------------------------------

/// The inclusive effective-time window `[t_first + S, t_first + S + D]`.
/// `end == None` means "no `--duration`", i.e. an open-ended window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub start: u64,
    pub end: Option<u64>,
}

impl Window {
    /// Resolve the window from the first effective time (MCAP Player Proposal
    /// §6.12: checked `t_first + start (+ duration)`; overflow is a CLI-style
    /// error, never a saturating clamp).
    pub fn resolve(t_first: u64, start: Duration, duration: Option<Duration>) -> Result<Self> {
        let start = validate::checked_window_start(t_first, start)?;
        let end = validate::checked_window_end(start, duration)?;
        Ok(Self { start, end })
    }

    pub fn contains(&self, effective_time: u64) -> bool {
        effective_time >= self.start && self.end.is_none_or(|end| effective_time <= end)
    }
}

// ---------------------------------------------------------------------------
// Clock (MCAP Player Proposal §6.5: "pure functions over an injected clock")
// ---------------------------------------------------------------------------

/// Monotonic time source. The pass-2 driver injects [`SystemClock`]; unit tests
/// inject [`ManualClock`] and never sleep.
pub trait Clock {
    fn now(&self) -> Instant;
}

/// The real monotonic clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A clock advanced by hand, so scheduling can be asserted without sleeping.
#[derive(Debug)]
pub struct ManualClock {
    origin: Instant,
    offset_nanos: AtomicU64,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            offset_nanos: AtomicU64::new(0),
        }
    }

    /// Move the clock forward. Saturating: a test that advances past `u64` ns
    /// (585 years) has bigger problems than a wrapped counter.
    pub fn advance(&self, by: Duration) {
        let nanos = u64::try_from(by.as_nanos()).unwrap_or(u64::MAX);
        self.offset_nanos.fetch_add(nanos, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.origin + Duration::from_nanos(self.offset_nanos.load(Ordering::SeqCst))
    }
}

// ---------------------------------------------------------------------------
// Scheduler (MCAP Player Proposal §6.5, absolute targets)
// ---------------------------------------------------------------------------

/// `target(m) − wall0 = (effective(m) − t0) / rate`, checked (MCAP Player
/// Proposal §6.12).
///
/// The effective timeline is non-decreasing by construction, so
/// `effective_time < t0` means the caller mixed up its anchors — that is a
/// hard error, not a saturating zero.
pub fn target_for(effective_time_ns: u64, t0_ns: u64, rate: f64) -> Result<Duration> {
    let delta = effective_time_ns.checked_sub(t0_ns).ok_or_else(|| {
        anyhow::anyhow!(
            "effective time {} ns is before the replay anchor t0 = {} ns \
                 (the effective timeline is non-decreasing by construction)",
            effective_time_ns,
            t0_ns
        )
    })?;
    validate::checked_scaled_duration(delta, rate)
}

/// What the pass-2 driver should do with the message it is holding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Not due yet: sleep this long (`tokio::time::sleep_until(target)`), then
    /// ask again. Never zero.
    Sleep(Duration),
    /// Due (or overdue): publish now. `late` is `lateness > late_threshold`,
    /// in which case the message counts towards the summary's late tally.
    Publish { lateness: Duration, late: bool },
}

/// Absolute-target pacer over an injected clock.
#[derive(Debug, Clone)]
pub struct Scheduler {
    t0_ns: u64,
    rate: f64,
    wall0: Instant,
    late_threshold: Duration,
    late_messages: u64,
    max_lateness: Duration,
}

impl Scheduler {
    /// Anchor the scheduler: `t0` is the effective time of the first *selected*
    /// message and `wall0` is now (MCAP Player Proposal §6.5 — read after the
    /// matching barrier).
    pub fn start(t0_ns: u64, rate: f64, late_threshold: Duration, clock: &dyn Clock) -> Self {
        Self {
            t0_ns,
            rate,
            wall0: clock.now(),
            late_threshold,
            late_messages: 0,
            max_lateness: Duration::ZERO,
        }
    }

    /// Re-derive `wall0` for the next `--loop` iteration (MCAP Player Proposal
    /// §6.5). Lateness tallies accumulate across iterations, matching the
    /// single end-of-run summary.
    pub fn restart(&mut self, clock: &dyn Clock) {
        self.wall0 = clock.now();
    }

    pub fn wall0(&self) -> Instant {
        self.wall0
    }

    /// The pacing anchor: effective time of the first selected message.
    pub fn t0_ns(&self) -> u64 {
        self.t0_ns
    }

    /// Absolute target instant of a message, checked end to end.
    pub fn target_instant(&self, effective_time_ns: u64) -> Result<Instant> {
        let offset = target_for(effective_time_ns, self.t0_ns, self.rate)?;
        self.wall0.checked_add(offset).ok_or_else(|| {
            anyhow::anyhow!(
                "replay target {:?} past the replay start overflows the monotonic clock",
                offset
            )
        })
    }

    /// Decide what to do with the message at `effective_time_ns`.
    ///
    /// A `Sleep` answer does not mutate the scheduler, so the driver may call
    /// this again after sleeping (or after observing cancellation) without
    /// double-counting anything.
    pub fn step(&mut self, effective_time_ns: u64, clock: &dyn Clock) -> Result<Step> {
        let target = self.target_instant(effective_time_ns)?;
        let now = clock.now();
        if now < target {
            return Ok(Step::Sleep(target - now));
        }
        let lateness = now - target;
        let late = lateness > self.late_threshold;
        if late {
            self.late_messages += 1;
            if lateness > self.max_lateness {
                self.max_lateness = lateness;
            }
        }
        Ok(Step::Publish { lateness, late })
    }

    pub fn late_messages(&self) -> u64 {
        self.late_messages
    }

    pub fn max_lateness(&self) -> Duration {
        self.max_lateness
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- effective timeline (MCAP Player Proposal §6.5) --------------------

    #[test]
    fn the_required_worked_example_clamps_to_a_back_to_back_replay() {
        // MCAP Player Proposal §6.5: file-order log_time 100, 90, 95 →
        // effective 100, 100, 100.
        assert_eq!(effective_times(&[100, 90, 95]), vec![100, 100, 100]);
    }

    #[test]
    fn the_accumulator_counts_clamp_events_and_tracks_first_and_last() {
        let mut accumulator = TimelineAccumulator::new();
        for log_time in [100u64, 90, 95, 300] {
            accumulator.push(log_time);
        }
        assert_eq!(accumulator.clamped(), 2);
        assert_eq!(accumulator.messages(), 4);
        assert_eq!(accumulator.t_first(), Some(100));
        assert_eq!(accumulator.t_last(), Some(300));
    }

    #[test]
    fn a_monotone_stream_is_never_clamped() {
        let mut accumulator = TimelineAccumulator::new();
        for log_time in [1u64, 2, 2, 3] {
            accumulator.push(log_time);
        }
        assert_eq!(accumulator.clamped(), 0, "equal timestamps are not clamps");
        assert_eq!(accumulator.t_last(), Some(3));
    }

    #[test]
    fn an_empty_timeline_has_no_anchor() {
        let accumulator = TimelineAccumulator::new();
        assert_eq!(accumulator.t_first(), None);
        assert_eq!(accumulator.t_last(), None);
        assert_eq!(accumulator.messages(), 0);
    }

    // -- window (MCAP Player Proposal §6.5 step 2) -------------------------

    #[test]
    fn a_window_is_inclusive_on_both_ends_and_open_without_a_duration() {
        let window = Window::resolve(
            1_000,
            Duration::from_nanos(10),
            Duration::from_nanos(5).into(),
        )
        .expect("representable");
        assert_eq!(window.start, 1_010);
        assert_eq!(window.end, Some(1_015));
        assert!(window.contains(1_010) && window.contains(1_015));
        assert!(!window.contains(1_009) && !window.contains(1_016));

        let open = Window::resolve(1_000, Duration::ZERO, None).expect("representable");
        assert_eq!(open.end, None);
        assert!(open.contains(u64::MAX));
        assert!(!open.contains(999));
    }

    #[test]
    fn a_window_past_the_end_of_the_nanosecond_timeline_is_rejected() {
        let err = Window::resolve(u64::MAX, Duration::from_nanos(1), None).unwrap_err();
        assert!(err.to_string().contains("--start"), "got: {err}");
        let err = Window::resolve(u64::MAX - 1, Duration::ZERO, Some(Duration::from_nanos(2)))
            .unwrap_err();
        assert!(err.to_string().contains("--duration"), "got: {err}");
    }

    // -- scheduler targets (MCAP Player Proposal §6.5) ---------------------

    #[test]
    fn targets_scale_with_the_rate() {
        let one_second = 1_000_000_000u64;
        assert_eq!(
            target_for(3 * one_second, one_second, 1.0).unwrap(),
            Duration::from_secs(2)
        );
        assert_eq!(
            target_for(3 * one_second, one_second, 2.0).unwrap(),
            Duration::from_secs(1)
        );
        assert_eq!(
            target_for(3 * one_second, one_second, 0.5).unwrap(),
            Duration::from_secs(4)
        );
        assert_eq!(
            target_for(one_second, one_second, 0.5).unwrap(),
            Duration::ZERO
        );
    }

    #[test]
    fn target_arithmetic_is_checked_not_saturating() {
        assert!(target_for(10, 20, 1.0).is_err(), "t0 must anchor the run");
        assert!(target_for(u64::MAX, 0, 1e-9).is_err(), "unrepresentable");
        assert!(target_for(10, 0, 0.0).is_err(), "rate 0 is rejected");
        assert!(target_for(10, 0, f64::NAN).is_err());
    }

    #[test]
    fn the_scheduler_sleeps_until_the_target_then_publishes() {
        let clock = ManualClock::new();
        let mut scheduler = Scheduler::start(1_000_000_000, 1.0, DEFAULT_LATE_THRESHOLD, &clock);

        // 500 ms of recorded time ahead of t0, at rate 1.0.
        let effective = 1_500_000_000u64;
        assert_eq!(
            scheduler.step(effective, &clock).unwrap(),
            Step::Sleep(Duration::from_millis(500))
        );
        clock.advance(Duration::from_millis(200));
        assert_eq!(
            scheduler.step(effective, &clock).unwrap(),
            Step::Sleep(Duration::from_millis(300))
        );
        clock.advance(Duration::from_millis(300));
        assert_eq!(
            scheduler.step(effective, &clock).unwrap(),
            Step::Publish {
                lateness: Duration::ZERO,
                late: false
            }
        );
        assert_eq!(scheduler.late_messages(), 0);
    }

    #[test]
    fn the_scheduler_halves_and_doubles_the_wait_with_the_rate() {
        for (rate, expected) in [(0.5f64, 1_000u64), (1.0, 500), (2.0, 250)] {
            let clock = ManualClock::new();
            let mut scheduler = Scheduler::start(0, rate, DEFAULT_LATE_THRESHOLD, &clock);
            assert_eq!(
                scheduler.step(500_000_000, &clock).unwrap(),
                Step::Sleep(Duration::from_millis(expected)),
                "rate {rate}"
            );
        }
    }

    #[test]
    fn a_message_past_the_late_threshold_is_counted_late() {
        let clock = ManualClock::new();
        let mut scheduler = Scheduler::start(0, 1.0, DEFAULT_LATE_THRESHOLD, &clock);

        // Exactly at the threshold is *not* late (MCAP Player Proposal §6.5:
        // `now > target + T`).
        clock.advance(Duration::from_millis(10));
        assert_eq!(
            scheduler.step(0, &clock).unwrap(),
            Step::Publish {
                lateness: Duration::from_millis(10),
                late: false
            }
        );
        assert_eq!(scheduler.late_messages(), 0);

        clock.advance(Duration::from_millis(15));
        let step = scheduler.step(0, &clock).unwrap();
        assert_eq!(
            step,
            Step::Publish {
                lateness: Duration::from_millis(25),
                late: true
            }
        );
        assert_eq!(scheduler.late_messages(), 1);
        assert_eq!(scheduler.max_lateness(), Duration::from_millis(25));
    }

    #[test]
    fn an_already_due_run_never_sleeps_and_never_skips() {
        // A run of clamped, same-effective-time messages: every one is due, and
        // the scheduler asks for no sleep at all (MCAP Player Proposal §6.5: no
        // catch-up skipping, and the driver must still check its token every
        // iteration).
        let clock = ManualClock::new();
        let mut scheduler = Scheduler::start(100, 1.0, DEFAULT_LATE_THRESHOLD, &clock);
        clock.advance(Duration::from_millis(50));
        for _ in 0..1_000 {
            match scheduler.step(100, &clock).unwrap() {
                Step::Publish { late, .. } => assert!(late),
                Step::Sleep(_) => panic!("an already-due message must not sleep"),
            }
        }
        assert_eq!(scheduler.late_messages(), 1_000);
    }

    #[test]
    fn restart_re_derives_wall0_for_the_next_loop_iteration() {
        let clock = ManualClock::new();
        let mut scheduler = Scheduler::start(0, 1.0, DEFAULT_LATE_THRESHOLD, &clock);
        clock.advance(Duration::from_secs(5));
        // Without a restart the whole second pass would be hopelessly late.
        assert!(matches!(
            scheduler.step(1_000_000_000, &clock).unwrap(),
            Step::Publish { late: true, .. }
        ));
        scheduler.restart(&clock);
        assert_eq!(
            scheduler.step(1_000_000_000, &clock).unwrap(),
            Step::Sleep(Duration::from_secs(1))
        );
    }
}
