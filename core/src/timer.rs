#[cfg(target_os = "linux")]
use std::cmp::Ordering as CmpOrdering;
#[cfg(target_os = "linux")]
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(target_os = "linux")]
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle as StdJoinHandle;
use std::time::{Duration, Instant as StdInstant};

#[cfg(target_os = "linux")]
use libc::{
    clock_gettime, clock_nanosleep, eventfd, itimerspec, poll, pollfd, read, timerfd_create,
    timerfd_settime, timespec, write, CLOCK_MONOTONIC, EAGAIN, EFD_CLOEXEC, EFD_NONBLOCK, EINTR,
    POLLIN, TFD_CLOEXEC, TFD_NONBLOCK, TFD_TIMER_ABSTIME, TIMER_ABSTIME,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle as TokioJoinHandle;
use tokio::time::Instant;

use crate::error::{AortaError, AortaResult};
use crate::event_queue::{
    CallbackGroupHandle, CallbackGroupType, Event, EventKind, EventQueueSender, EventQueueWaker,
};

// Bound inline coalesce dispatch so one hot timer cannot monopolize the executor indefinitely.
// Reduced from 8 to 4: at ≤1kHz, 8 inline dispatches can consume up to 8ms before yielding,
// adding jitter that causes p99 losses vs ROS2. 4 limits worst-case inline latency while
// still avoiding unnecessary thread wakes at high rates.
const COALESCE_INLINE_BURST_MAX: usize = 4;
const HIGH_PRECISION_SLEEP_MARGIN_MIN: Duration = Duration::from_micros(20);
const HIGH_PRECISION_SLEEP_MARGIN_MAX: Duration = Duration::from_micros(200);
const HIGH_PRECISION_YIELD_MARGIN_MIN: Duration = Duration::from_micros(5);
const HIGH_PRECISION_YIELD_MARGIN_MAX: Duration = Duration::from_micros(25);
const HIGH_PRECISION_ABS_SLEEP_WINDOW_MAX: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerOverflowPolicy {
    CoalesceLatest,
    Block,
}

/// Configuration options for Timer creation.
///
/// `#[non_exhaustive]`: construct via [`TimerOptions::new`] / [`Default`] + the
/// `with_*` builders, never a struct literal — so adding fields (e.g. `name`)
/// stays source-compatible for downstream SDK users.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TimerOptions {
    /// Whether the timer starts immediately upon creation
    pub auto_start: bool,

    /// Whether to invoke callback on a missed tick if system is busy
    /// - true: invoke callback for every period, even if delayed
    /// - false: skip missed ticks and wait for next period
    pub invoke_on_missed: bool,

    /// Callback group execution mode.
    pub callback_group: CallbackGroupType,

    /// Executor-scoped callback group handle.
    /// When set, this takes precedence over `callback_group`.
    pub callback_group_handle: Option<CallbackGroupHandle>,

    /// Timer overflow behavior when executor queue is saturated.
    pub overflow_policy: TimerOverflowPolicy,

    /// Timeout used when `overflow_policy == TimerOverflowPolicy::Block`.
    ///
    /// Note: the runtime enforces a lower bound of `max(period * 5, 2ms)`
    /// to avoid bimodal p99 cascades at high tick rates. A value smaller
    /// than this floor is silently raised; pass the floor explicitly if
    /// you want predictable failure detection. The floor was sized to give
    /// the executor enough headroom to drain one event at 5kHz (period=200µs
    /// → 1ms timeout) without spurious timeout cascades; tighten it only
    /// for low-rate timers where strict timeout semantics are required.
    pub overflow_block_timeout: Duration,

    /// Optional task label. When set, the scheduler tracer names the timer's
    /// callback `sched.callback:timer:<name>` (so an otherwise anonymous timer is
    /// identifiable). The string is interned ONCE at timer registration, so it
    /// costs nothing per dispatch.
    pub name: Option<String>,
}

impl Default for TimerOptions {
    fn default() -> Self {
        Self {
            auto_start: true,
            invoke_on_missed: false,
            callback_group: CallbackGroupType::MutuallyExclusive,
            callback_group_handle: None,
            overflow_policy: TimerOverflowPolicy::CoalesceLatest,
            overflow_block_timeout: Duration::from_millis(0),
            name: None,
        }
    }
}

impl TimerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_auto_start(mut self, auto_start: bool) -> Self {
        self.auto_start = auto_start;
        self
    }

    pub fn with_invoke_on_missed(mut self, invoke_on_missed: bool) -> Self {
        self.invoke_on_missed = invoke_on_missed;
        self
    }

    pub fn with_callback_group(mut self, callback_group: CallbackGroupType) -> Self {
        self.callback_group = callback_group;
        self
    }

    pub fn with_callback_group_handle(
        mut self,
        callback_group_handle: CallbackGroupHandle,
    ) -> Self {
        self.callback_group_handle = Some(callback_group_handle);
        self
    }

    pub fn with_overflow_policy(mut self, overflow_policy: TimerOverflowPolicy) -> Self {
        self.overflow_policy = overflow_policy;
        self
    }

    /// Name the timer so its scheduler callback reads `sched.callback:timer:<name>`.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the timeout used when `overflow_policy == TimerOverflowPolicy::Block`.
    ///
    /// See [`TimerOptions::overflow_block_timeout`] for the runtime floor of
    /// `max(period * 5, 2ms)`. Values below this floor are silently raised.
    pub fn with_overflow_block_timeout(mut self, timeout: Duration) -> Self {
        self.overflow_block_timeout = timeout;
        self
    }
}

#[derive(Clone)]
pub(crate) struct ExecutorTimerScheduler {
    inner: Arc<Mutex<ExecutorTimerSchedulerInner>>,
    notify: Arc<Notify>,
    queue_waker: Option<EventQueueWaker>,
}

impl fmt::Debug for ExecutorTimerScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timer_count = self
            .inner
            .lock()
            .map(|inner| inner.timers.len())
            .unwrap_or_default();
        f.debug_struct("ExecutorTimerScheduler")
            .field("timer_count", &timer_count)
            .finish()
    }
}

#[derive(Default)]
struct ExecutorTimerSchedulerInner {
    next_id: u64,
    timers: HashMap<u64, ExecutorOwnedTimerEntry>,
}

struct ExecutorOwnedTimerEntry {
    period: Duration,
    next_deadline: Instant,
    invoke_on_missed: bool,
    callback: Arc<dyn Fn() + Send + Sync + 'static>,
    callback_group: CallbackGroupType,
    callback_group_handle: Option<CallbackGroupHandle>,
    /// Pre-interned `sched.callback:timer:<name>` id for a named timer (interned once
    /// here; zero per-dispatch cost). `None` → unnamed.
    callback_trace_event: Option<u16>,
    running: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    coalesced_tick_count: Arc<AtomicU64>,
    deadline_slip_total_ns: Arc<AtomicU64>,
    deadline_slip_max_ns: Arc<AtomicU64>,
    due_firings_total: Arc<AtomicU64>,
    due_firings_max: Arc<AtomicU64>,
    due_sample_count: Arc<AtomicU64>,
    coalesce_requeue_count: Arc<AtomicU64>,
    coalesce_inline_dispatch_total: Arc<AtomicU64>,
    coalesce_inline_dispatch_max: Arc<AtomicU64>,
    coalesce_event_wait_total_ns: Arc<AtomicU64>,
    coalesce_event_wait_max_ns: Arc<AtomicU64>,
    coalesce_event_wait_sample_count: Arc<AtomicU64>,
    pending_event: bool,
    rearm_requested: bool,
    // Set when a firing was coalesced while a prior event was still
    // in flight. On the next collect it dispatches one catch-up event for the
    // latest pending firing (preserving the CoalesceLatest "one runnable
    // event" contract) WITHOUT rebasing `next_deadline`, so the period-aligned
    // cadence is preserved and does not drift to callback-completion time.
    immediate_rearm: bool,
}

impl ExecutorOwnedTimerEntry {
    fn due_sample(&mut self, now: Instant) -> DueFiringsSample {
        compute_due_sample(
            now,
            &mut self.next_deadline,
            self.period,
            self.invoke_on_missed,
        )
    }

    fn record_due_sample(&self, sample: DueFiringsSample) {
        record_due_sample(
            sample,
            &self.deadline_slip_total_ns,
            &self.deadline_slip_max_ns,
            &self.due_firings_total,
            &self.due_firings_max,
            &self.due_sample_count,
        );
    }

    /// Build a dispatchable event spec from this entry's current snapshot.
    /// Single source for both the regular due-firing path and the coalesced
    /// catch-up (immediate-rearm) path so the two never diverge.
    fn build_event_spec(&self, timer_id: u64) -> ExecutorOwnedEventSpec {
        ExecutorOwnedEventSpec {
            timer_id,
            generation_snapshot: self.generation.load(Ordering::Relaxed),
            callback: Arc::clone(&self.callback),
            callback_group: self.callback_group,
            callback_group_handle: self.callback_group_handle,
            callback_trace_event: self.callback_trace_event,
            running: Arc::clone(&self.running),
            stopped: Arc::clone(&self.stopped),
            generation: Arc::clone(&self.generation),
            coalesce_event_wait_total_ns: Arc::clone(&self.coalesce_event_wait_total_ns),
            coalesce_event_wait_max_ns: Arc::clone(&self.coalesce_event_wait_max_ns),
            coalesce_event_wait_sample_count: Arc::clone(&self.coalesce_event_wait_sample_count),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DueFiringsSample {
    due_firings: u64,
    deadline_slip_ns: u64,
}

/// O(1) catch-up arithmetic shared by the tokio- and std-`Instant` due-sample
/// helpers.
///
/// Given `elapsed` (time `now` is past the current, already-due
/// `next_deadline`), returns how many firings are due and how far to advance
/// the deadline so it lands strictly after `now`. The previous implementation
/// looped once per missed period while holding the scheduler's global lock, so
/// a long stall (e.g. a paused timer or a clock jump relative to a small
/// period) turned into an O(N) stall storm under the lock. This computes the
/// same result with a single division.
fn due_firings_and_advance(
    elapsed: Duration,
    period: Duration,
    invoke_on_missed: bool,
) -> (u64, Duration) {
    // `period` is guaranteed non-zero by register_timer; `.max(1)` is belt-and-
    // suspenders so this can never divide by zero.
    let period_ns = period.as_nanos().max(1);
    // Periods fully elapsed beyond the first due firing.
    let extra = elapsed.as_nanos() / period_ns;
    // Advance by (extra + 1) periods so the new deadline is strictly after
    // `now` (the loop terminated on exactly this condition).
    let advance_periods = extra.saturating_add(1);
    let advance_ns = advance_periods
        .saturating_mul(period_ns)
        .min(u64::MAX as u128) as u64;
    let due_firings = if invoke_on_missed {
        advance_periods.min(u64::MAX as u128) as u64
    } else {
        1
    };
    (due_firings, Duration::from_nanos(advance_ns))
}

fn compute_due_sample(
    now: Instant,
    next_deadline: &mut Instant,
    period: Duration,
    invoke_on_missed: bool,
) -> DueFiringsSample {
    if now < *next_deadline {
        return DueFiringsSample::default();
    }

    let elapsed = now.saturating_duration_since(*next_deadline);
    let deadline_slip_ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
    let (due_firings, advance) = due_firings_and_advance(elapsed, period, invoke_on_missed);
    *next_deadline += advance;
    DueFiringsSample {
        due_firings,
        deadline_slip_ns,
    }
}

fn record_due_sample(
    sample: DueFiringsSample,
    deadline_slip_total_ns: &AtomicU64,
    deadline_slip_max_ns: &AtomicU64,
    due_firings_total: &AtomicU64,
    due_firings_max: &AtomicU64,
    due_sample_count: &AtomicU64,
) {
    if sample.due_firings == 0 {
        return;
    }

    deadline_slip_total_ns.fetch_add(sample.deadline_slip_ns, Ordering::Relaxed);
    fetch_max_relaxed(deadline_slip_max_ns, sample.deadline_slip_ns);
    due_firings_total.fetch_add(sample.due_firings, Ordering::Relaxed);
    fetch_max_relaxed(due_firings_max, sample.due_firings);
    due_sample_count.fetch_add(1, Ordering::Relaxed);
}

fn block_overflow_missed_tick_behavior(invoke_on_missed: bool) -> tokio::time::MissedTickBehavior {
    if invoke_on_missed {
        tokio::time::MissedTickBehavior::Delay
    } else {
        tokio::time::MissedTickBehavior::Skip
    }
}

fn compute_due_sample_std(
    now: StdInstant,
    next_deadline: &mut StdInstant,
    period: Duration,
    invoke_on_missed: bool,
) -> DueFiringsSample {
    if now < *next_deadline {
        return DueFiringsSample::default();
    }

    let elapsed = now.saturating_duration_since(*next_deadline);
    let deadline_slip_ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
    let (due_firings, advance) = due_firings_and_advance(elapsed, period, invoke_on_missed);
    *next_deadline += advance;
    DueFiringsSample {
        due_firings,
        deadline_slip_ns,
    }
}

fn fetch_max_relaxed(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn duration_from_nanos_saturating(ns: u128) -> Duration {
    Duration::from_nanos(ns.min(u64::MAX as u128) as u64)
}

fn adaptive_margin(period: Duration, divisor: u32, min: Duration, max: Duration) -> Duration {
    let period_ns = period.as_nanos();
    if period_ns <= 1 {
        return Duration::from_nanos(1);
    }

    let candidate_ns = (period_ns / divisor as u128)
        .max(min.as_nanos())
        .min(max.as_nanos())
        .min(period_ns - 1);
    duration_from_nanos_saturating(candidate_ns)
}

fn high_precision_wait_margins(period: Duration) -> (Duration, Duration) {
    let sleep_margin = adaptive_margin(
        period,
        8,
        HIGH_PRECISION_SLEEP_MARGIN_MIN,
        HIGH_PRECISION_SLEEP_MARGIN_MAX,
    );
    let yield_ceiling = if sleep_margin > Duration::from_nanos(1) {
        duration_from_nanos_saturating((sleep_margin.as_nanos() / 2).max(1))
    } else {
        sleep_margin
    };
    let yield_margin = adaptive_margin(
        period,
        32,
        HIGH_PRECISION_YIELD_MARGIN_MIN,
        HIGH_PRECISION_YIELD_MARGIN_MAX.min(yield_ceiling),
    );
    (sleep_margin, yield_margin)
}

fn high_precision_abs_sleep_window(period: Duration, yield_margin: Duration) -> Duration {
    let upper_bound = period.min(HIGH_PRECISION_ABS_SLEEP_WINDOW_MAX);
    if upper_bound <= yield_margin {
        duration_from_nanos_saturating(upper_bound.as_nanos().saturating_sub(1))
    } else {
        upper_bound
    }
}

/// What the non-Linux high-precision wait loop should do for a given `remaining`
/// time to the deadline. Non-Linux targets lack `clock_nanosleep(TIMER_ABSTIME)`,
/// so the gap between the coarse sleep margin and the final precision spin must be
/// bridged by a real `thread::sleep` rather than by spin-yielding — otherwise the
/// lane pins a full core for the whole `[yield_margin, sleep_margin]` window of
/// every firing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(target_os = "linux", allow(dead_code))]
enum NonLinuxWaitAction {
    /// `remaining > sleep_margin`: block on the control condvar so a
    /// pause/stop/new-timer notification wakes the lane promptly.
    Condvar,
    /// `yield_margin < remaining <= sleep_margin`: sleep this long (down to the
    /// final yield margin), then re-check, instead of burning the core.
    Sleep(Duration),
    /// `remaining <= yield_margin`: fall through to the final precision spin.
    PrecisionSpin,
}

/// Pure decision for [`NonLinuxWaitAction`]; compiled on every target so the
/// boundary math is unit-tested under Linux CI even though the runtime path it
/// drives only executes on non-Linux targets.
#[cfg_attr(target_os = "linux", allow(dead_code))]
fn non_linux_wait_action(
    remaining: Duration,
    yield_margin: Duration,
    sleep_margin: Duration,
) -> NonLinuxWaitAction {
    if remaining <= yield_margin {
        NonLinuxWaitAction::PrecisionSpin
    } else if remaining > sleep_margin {
        NonLinuxWaitAction::Condvar
    } else {
        NonLinuxWaitAction::Sleep(remaining - yield_margin)
    }
}

struct ExecutorOwnedEventSpec {
    timer_id: u64,
    generation_snapshot: u64,
    callback: Arc<dyn Fn() + Send + Sync + 'static>,
    callback_group: CallbackGroupType,
    callback_group_handle: Option<CallbackGroupHandle>,
    callback_trace_event: Option<u16>,
    running: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    coalesce_event_wait_total_ns: Arc<AtomicU64>,
    coalesce_event_wait_max_ns: Arc<AtomicU64>,
    coalesce_event_wait_sample_count: Arc<AtomicU64>,
}

impl ExecutorTimerScheduler {
    pub(crate) fn new(notify: Arc<Notify>, queue_waker: Option<EventQueueWaker>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ExecutorTimerSchedulerInner {
                next_id: 1,
                timers: HashMap::new(),
            })),
            notify,
            queue_waker,
        }
    }

    fn notify_executor(&self) {
        self.notify.notify_waiters();
        if let Some(waker) = &self.queue_waker {
            waker.wake();
        }
    }

    pub(crate) fn register_timer<F>(
        &self,
        period: Duration,
        callback: F,
        options: &TimerOptions,
        running: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        generation: Arc<AtomicU64>,
        coalesced_tick_count: Arc<AtomicU64>,
        deadline_slip_total_ns: Arc<AtomicU64>,
        deadline_slip_max_ns: Arc<AtomicU64>,
        due_firings_total: Arc<AtomicU64>,
        due_firings_max: Arc<AtomicU64>,
        due_sample_count: Arc<AtomicU64>,
        coalesce_requeue_count: Arc<AtomicU64>,
        coalesce_inline_dispatch_total: Arc<AtomicU64>,
        coalesce_inline_dispatch_max: Arc<AtomicU64>,
        coalesce_event_wait_total_ns: Arc<AtomicU64>,
        coalesce_event_wait_max_ns: Arc<AtomicU64>,
        coalesce_event_wait_sample_count: Arc<AtomicU64>,
    ) -> AortaResult<u64>
    where
        F: Fn() + Send + Sync + 'static,
    {
        if period.is_zero() {
            return Err(AortaError::invalid_argument(
                "timer period must be non-zero",
            ));
        }

        let callback: Arc<dyn Fn() + Send + Sync + 'static> = Arc::new(callback);
        // Intern `sched.callback:timer:<name>` ONCE here (timer registration is rare),
        // so dispatch just carries the u16 — no per-tick alloc/intern.
        let callback_trace_event = options
            .name
            .as_ref()
            .map(|name| crate::trace::intern_event(&format!("sched.callback:timer:{name}")));
        let timer_id = {
            let mut inner = self
                .inner
                .lock()
                .expect("executor timer scheduler mutex poisoned");
            let timer_id = inner.next_id;
            inner.next_id = inner.next_id.saturating_add(1);
            inner.timers.insert(
                timer_id,
                ExecutorOwnedTimerEntry {
                    period,
                    next_deadline: Instant::now() + period,
                    invoke_on_missed: options.invoke_on_missed,
                    callback,
                    callback_group: options.callback_group,
                    callback_group_handle: options.callback_group_handle,
                    callback_trace_event,
                    running,
                    stopped,
                    generation,
                    coalesced_tick_count,
                    deadline_slip_total_ns,
                    deadline_slip_max_ns,
                    due_firings_total,
                    due_firings_max,
                    due_sample_count,
                    coalesce_requeue_count,
                    coalesce_inline_dispatch_total,
                    coalesce_inline_dispatch_max,
                    coalesce_event_wait_total_ns,
                    coalesce_event_wait_max_ns,
                    coalesce_event_wait_sample_count,
                    pending_event: false,
                    rearm_requested: false,
                    immediate_rearm: false,
                },
            );
            timer_id
        };

        self.notify_executor();
        Ok(timer_id)
    }

    pub(crate) fn collect_due_events(&self) -> Vec<Event> {
        let (specs, _) = self.collect_due_events_and_next_deadline_inner();
        specs
            .into_iter()
            .map(|spec| self.make_event(spec))
            .collect::<Vec<_>>()
    }

    /// Combined method that collects due events and returns the next deadline
    /// in a single mutex acquisition, avoiding the overhead of locking twice
    /// per executor spin cycle.
    pub(crate) fn collect_due_events_and_next_deadline(&self) -> (Vec<Event>, Option<Instant>) {
        let (specs, next_deadline) = self.collect_due_events_and_next_deadline_inner();
        let events = specs
            .into_iter()
            .map(|spec| self.make_event(spec))
            .collect::<Vec<_>>();
        (events, next_deadline)
    }

    fn collect_due_events_and_next_deadline_inner(
        &self,
    ) -> (Vec<ExecutorOwnedEventSpec>, Option<Instant>) {
        let now = Instant::now();
        let mut specs = Vec::new();
        let mut next_deadline = None;
        let mut inner = self
            .inner
            .lock()
            .expect("executor timer scheduler mutex poisoned");
        for (timer_id, entry) in inner.timers.iter_mut() {
            if entry.stopped.load(Ordering::Relaxed) || !entry.running.load(Ordering::Relaxed) {
                continue;
            }

            // Dispatch a coalesced catch-up firing promptly. A firing
            // came due while the previous event was in flight and was coalesced;
            // `on_callback_complete` set `immediate_rearm` without touching the
            // period-aligned `next_deadline`. Emit one event for it now (latest
            // pending firing) and keep the aligned cadence for everything after.
            if entry.immediate_rearm && !entry.pending_event {
                entry.immediate_rearm = false;
                entry.pending_event = true;
                match next_deadline {
                    None => next_deadline = Some(entry.next_deadline),
                    Some(current) if entry.next_deadline < current => {
                        next_deadline = Some(entry.next_deadline);
                    }
                    _ => {}
                }
                specs.push(entry.build_event_spec(*timer_id));
                continue;
            }

            let due_sample = entry.due_sample(now);
            if due_sample.due_firings == 0 {
                // Timer not due — track its (unchanged) next_deadline.
                match next_deadline {
                    None => next_deadline = Some(entry.next_deadline),
                    Some(current) if entry.next_deadline < current => {
                        next_deadline = Some(entry.next_deadline);
                    }
                    _ => {}
                }
                continue;
            }
            entry.record_due_sample(due_sample);
            let due_firings = due_sample.due_firings;

            // Track next_deadline AFTER due_sample/record_due_sample have
            // advanced entry.next_deadline.  Capturing before advancement
            // would return a stale (past) deadline, causing the executor
            // to spin at 100% CPU on pop_timeout_sync(Duration::ZERO).
            match next_deadline {
                None => next_deadline = Some(entry.next_deadline),
                Some(current) if entry.next_deadline < current => {
                    next_deadline = Some(entry.next_deadline);
                }
                _ => {}
            }

            if entry.pending_event {
                entry.rearm_requested = true;
                entry
                    .coalesced_tick_count
                    .fetch_add(due_firings, Ordering::Relaxed);
                continue;
            }

            if due_firings > 1 {
                entry
                    .coalesced_tick_count
                    .fetch_add(due_firings - 1, Ordering::Relaxed);
            }

            entry.pending_event = true;
            entry.rearm_requested = false;
            specs.push(entry.build_event_spec(*timer_id));
        }
        (specs, next_deadline)
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.inner.lock().ok().and_then(|inner| {
            inner
                .timers
                .values()
                .filter(|entry| {
                    !entry.stopped.load(Ordering::Relaxed) && entry.running.load(Ordering::Relaxed)
                })
                .map(|entry| entry.next_deadline)
                .min()
        })
    }

    pub(crate) fn pause_timer(&self, timer_id: u64) {
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(entry) = inner.timers.get_mut(&timer_id) {
                entry.running.store(false, Ordering::Relaxed);
                entry.pending_event = false;
                entry.rearm_requested = false;
                entry.immediate_rearm = false;
                entry.generation.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.notify_executor();
    }

    pub(crate) fn resume_timer(&self, timer_id: u64) {
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(entry) = inner.timers.get_mut(&timer_id) {
                if entry.stopped.load(Ordering::Relaxed) {
                    return;
                }
                entry.running.store(true, Ordering::Relaxed);
                entry.pending_event = false;
                entry.rearm_requested = false;
                entry.immediate_rearm = false;
                entry.next_deadline = Instant::now() + entry.period;
                entry.generation.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.notify_executor();
    }

    pub(crate) fn remove_timer(&self, timer_id: u64) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.timers.remove(&timer_id);
        }
        self.notify_executor();
    }

    fn make_event(&self, spec: ExecutorOwnedEventSpec) -> Event {
        let scheduler = self.clone();
        let enqueue_started = StdInstant::now();
        Event::new(EventKind::Timer, move || {
            let wait_ns = enqueue_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            spec.coalesce_event_wait_total_ns
                .fetch_add(wait_ns, Ordering::Relaxed);
            fetch_max_relaxed(&spec.coalesce_event_wait_max_ns, wait_ns);
            spec.coalesce_event_wait_sample_count
                .fetch_add(1, Ordering::Relaxed);

            // Guarantee the timer is re-armed even if the user
            // callback panics. Event::run isolates the panic so it can't kill
            // the executor, but the unwind would still skip the
            // `on_callback_complete` call below, leaving `pending_event = true`
            // forever and permanently wedging this timer. A drop guard runs the
            // completion bookkeeping on both the normal and the unwinding path.
            struct CompleteOnDrop {
                scheduler: ExecutorTimerScheduler,
                timer_id: u64,
                generation_snapshot: u64,
            }
            impl Drop for CompleteOnDrop {
                fn drop(&mut self) {
                    self.scheduler
                        .on_callback_complete(self.timer_id, self.generation_snapshot);
                }
            }
            let _complete = CompleteOnDrop {
                scheduler,
                timer_id: spec.timer_id,
                generation_snapshot: spec.generation_snapshot,
            };

            if !spec.stopped.load(Ordering::Relaxed)
                && spec.running.load(Ordering::Relaxed)
                && spec.generation.load(Ordering::Relaxed) == spec.generation_snapshot
            {
                (spec.callback)();
            }
            // `_complete` drops here (or during unwind) -> on_callback_complete.
        })
        .with_callback_group(spec.callback_group, spec.callback_group_handle)
        .with_callback_trace_event(spec.callback_trace_event)
    }

    fn on_callback_complete(&self, timer_id: u64, generation_snapshot: u64) {
        let should_notify = {
            let mut inner = match self.inner.lock() {
                Ok(inner) => inner,
                Err(_) => return,
            };
            let Some(entry) = inner.timers.get_mut(&timer_id) else {
                return;
            };
            if entry.generation.load(Ordering::Relaxed) != generation_snapshot {
                return;
            }

            entry.pending_event = false;
            if entry.rearm_requested {
                entry.rearm_requested = false;
                // A firing was coalesced while this event was in
                // flight. Make the latest pending firing runnable promptly via
                // `immediate_rearm` so the CoalesceLatest contract is honored,
                // but do NOT rebase `next_deadline` to `Instant::now()`:
                // `collect_due_events` already advanced it by whole periods, so
                // it holds the period-aligned deadline. Rebasing would anchor
                // the cadence to callback-completion time and drift; the
                // immediate-rearm path dispatches the catch-up without touching
                // it, preserving fixed-rate cadence for subsequent firings.
                entry.immediate_rearm = true;
                true
            } else {
                false
            }
        };

        if should_notify {
            self.notify_executor();
        }
    }
}

/// Periodic timer that invokes a callback at regular intervals
///
/// Timer sends events to the event queue, ensuring callbacks are executed
/// on the executor thread. Under `CoalesceLatest`, coalesced firings may be
/// serviced inline on that executor thread to reduce queue-hop amplification
/// while keeping runnable backlog bounded. On Linux, coalesced timers are
/// driven by a shared high-precision timing lane so missed executor wake-ups
/// do not directly inflate due-firing batches before admission. The timer can
/// be paused, resumed, and stopped.
///
/// # Example
/// ```no_run
/// use aorta_core::node::Node;
/// use std::time::Duration;
///
/// # async fn example() -> aorta_core::error::AortaResult<()> {
/// let node = Node::new("timer_node", "default").await?;
/// let timer = node.create_timer(Duration::from_millis(100), || {
///     println!("Timer tick!");
/// }).await?;
///
/// // Timer runs until dropped
/// # Ok(())
/// # }
/// ```
#[must_use = "dropping a Timer cancels it; bind it (`let _timer = ...`) for the timer lifetime"]
pub struct Timer {
    period: Duration,
    running: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    coalesced_tick_count: Arc<AtomicU64>,
    overflow_timeout_count: Arc<AtomicU64>,
    deadline_slip_total_ns: Arc<AtomicU64>,
    deadline_slip_max_ns: Arc<AtomicU64>,
    due_firings_total: Arc<AtomicU64>,
    due_firings_max: Arc<AtomicU64>,
    due_sample_count: Arc<AtomicU64>,
    coalesce_requeue_count: Arc<AtomicU64>,
    coalesce_inline_dispatch_total: Arc<AtomicU64>,
    coalesce_inline_dispatch_max: Arc<AtomicU64>,
    coalesce_event_wait_total_ns: Arc<AtomicU64>,
    coalesce_event_wait_max_ns: Arc<AtomicU64>,
    coalesce_event_wait_sample_count: Arc<AtomicU64>,
    backend: Option<TimerBackend>,
    /// Keeps this timer counted among the node's live snapshot producers: it
    /// captured `event_sender` at construction and cannot follow the node to
    /// another executor, so its existence must block a handoff.
    snapshot_producer_guard: Option<crate::node::SnapshotProducerGuard>,
}

enum TimerBackend {
    Task(TokioJoinHandle<()>),
    ExecutorOwned(ExecutorOwnedTimerBackend),
    #[cfg(target_os = "linux")]
    LinuxTimerfd(LinuxTimerfdTimerBackend),
    #[cfg(not(target_os = "linux"))]
    HighPrecision(HighPrecisionTimerBackend),
}

struct ExecutorOwnedTimerBackend {
    scheduler: ExecutorTimerScheduler,
    timer_id: u64,
}

struct HighPrecisionTimerBackend {
    control: Arc<HighPrecisionTimerControl>,
    thread: Option<StdJoinHandle<()>>,
}

struct HighPrecisionTimerControl {
    state: Mutex<HighPrecisionTimerControlState>,
    cv: Condvar,
}

#[derive(Default)]
struct HighPrecisionTimerControlState {
    notify_seq: u64,
}

#[cfg(target_os = "linux")]
struct LinuxTimerfdTimerBackend {
    scheduler: LinuxTimerfdScheduler,
    timer_id: u64,
}

/// Upper bound on the lane's per-failure backoff sleep. Keeps a deterministic
/// syscall failure from busy-spinning the core while still observing new-timer
/// notifications within a bounded delay.
#[cfg(target_os = "linux")]
const LANE_BACKOFF_MAX: Duration = Duration::from_millis(50);

/// Minimum interval between lane respawns, so a persistently-dying lane cannot
/// spawn a thread storm.
#[cfg(target_os = "linux")]
const LANE_RESPAWN_MIN_INTERVAL: Duration = Duration::from_secs(1);

/// The timerfd deadline heap uses lazy deletion: pause/resume/remove leave stale
/// entries that are only reclaimed once they bubble to the top. A timer with a
/// long period that is repeatedly paused/resumed can therefore accumulate far-
/// future stale entries faster than the top reclaims them, growing the heap
/// without bound. When the heap exceeds
/// `live_timers * HEAP_COMPACTION_FACTOR + HEAP_COMPACTION_SLACK`, it is rebuilt
/// to exactly one valid entry per live timer, bounding heap size to O(timers).
#[cfg(target_os = "linux")]
const HEAP_COMPACTION_FACTOR: usize = 2;
#[cfg(target_os = "linux")]
const HEAP_COMPACTION_SLACK: usize = 64;

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct LinuxTimerfdScheduler {
    inner: Arc<LinuxTimerfdSchedulerInner>,
}

#[cfg(target_os = "linux")]
struct LinuxTimerfdSchedulerInner {
    state: Mutex<LinuxTimerfdSchedulerState>,
    notify_fd: OwnedFd,
    timer_fd: OwnedFd,
    // True while the lane poll thread is running. Set false when `run` exits
    // (a panic — the loop itself no longer breaks on syscall errors), so
    // `register_timer` can lazily respawn the lane instead of leaving every
    // periodic timer in the process silently dead.
    alive: AtomicBool,
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct LinuxTimerfdSchedulerState {
    next_id: u64,
    timers: HashMap<u64, LinuxTimerfdTimerEntry>,
    heap: BinaryHeap<LinuxTimerfdHeapEntry>,
    // Last time the lane was (re)spawned, used to gate respawns to at most one
    // per `LANE_RESPAWN_MIN_INTERVAL` so a persistently-dying lane cannot spawn
    // a thread storm.
    last_respawn_at: Option<StdInstant>,
}

#[cfg(target_os = "linux")]
struct LinuxTimerfdTimerEntry {
    period: Duration,
    next_deadline: StdInstant,
    invoke_on_missed: bool,
    callback: Arc<dyn Fn() + Send + Sync + 'static>,
    event_sender: EventQueueSender,
    callback_group: CallbackGroupType,
    callback_group_handle: Option<CallbackGroupHandle>,
    callback_trace_event: Option<u16>,
    running: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    coalesced_tick_count: Arc<AtomicU64>,
    deadline_slip_total_ns: Arc<AtomicU64>,
    deadline_slip_max_ns: Arc<AtomicU64>,
    due_firings_total: Arc<AtomicU64>,
    due_firings_max: Arc<AtomicU64>,
    due_sample_count: Arc<AtomicU64>,
    coalesce_requeue_count: Arc<AtomicU64>,
    coalesce_inline_dispatch_total: Arc<AtomicU64>,
    coalesce_inline_dispatch_max: Arc<AtomicU64>,
    coalesce_event_wait_total_ns: Arc<AtomicU64>,
    coalesce_event_wait_max_ns: Arc<AtomicU64>,
    coalesce_event_wait_sample_count: Arc<AtomicU64>,
    coalesce_state: Arc<CoalesceDispatchState>,
    schedule_version: u64,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct LinuxTimerfdEnqueueSpec {
    timer_id: u64,
    callback: Arc<dyn Fn() + Send + Sync + 'static>,
    event_sender: EventQueueSender,
    running: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    coalesce_state: Arc<CoalesceDispatchState>,
    coalesced_tick_count: Arc<AtomicU64>,
    coalesce_requeue_count: Arc<AtomicU64>,
    coalesce_inline_dispatch_total: Arc<AtomicU64>,
    coalesce_inline_dispatch_max: Arc<AtomicU64>,
    coalesce_event_wait_total_ns: Arc<AtomicU64>,
    coalesce_event_wait_max_ns: Arc<AtomicU64>,
    coalesce_event_wait_sample_count: Arc<AtomicU64>,
    callback_group: CallbackGroupType,
    callback_group_handle: Option<CallbackGroupHandle>,
    callback_trace_event: Option<u16>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LinuxTimerfdHeapEntry {
    deadline: StdInstant,
    timer_id: u64,
    schedule_version: u64,
}

#[cfg(target_os = "linux")]
impl Ord for LinuxTimerfdHeapEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.timer_id.cmp(&self.timer_id))
            .then_with(|| other.schedule_version.cmp(&self.schedule_version))
    }
}

#[cfg(target_os = "linux")]
impl PartialOrd for LinuxTimerfdHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// Lock-free coalesce dispatch state, packed into an AtomicU8.
///
/// Bit 0 (`COALESCE_PENDING`): an event is currently queued/executing in the executor.
/// Bit 1 (`COALESCE_REARM`):   the timer thread fired again while an event was pending.
///
/// State transitions (CAS-based):
/// - Timer fires, no pending event:  0b00 → 0b01  (set PENDING)
/// - Timer fires, event pending:     0b01 → 0b11  (set REARM)
/// - Callback done, no rearm:        0b01 → 0b00  (clear PENDING → Complete)
/// - Callback done, rearm requested: 0b11 → 0b01  (clear REARM → ContinueInline/Requeue)
/// - Reset guard (panic cleanup):    *    → 0b00   (clear all)
struct CoalesceDispatchState {
    bits: AtomicU8,
}

const COALESCE_PENDING: u8 = 0b01;
const COALESCE_REARM: u8 = 0b10;

impl CoalesceDispatchState {
    fn new() -> Self {
        Self {
            bits: AtomicU8::new(0),
        }
    }

    /// Called by the timer thread when a tick is due.
    /// Returns `true` if this tick should enqueue a new event (transitions 0→PENDING).
    /// Returns `false` if an event is already pending (sets REARM flag via CAS).
    #[inline]
    fn try_mark_pending(&self) -> bool {
        let mut current = self.bits.load(Ordering::Acquire);
        loop {
            if current & COALESCE_PENDING != 0 {
                // Already pending — try to set REARM.
                match self.bits.compare_exchange_weak(
                    current,
                    current | COALESCE_REARM,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return false,
                    Err(observed) => {
                        current = observed;
                        continue;
                    }
                }
            } else {
                // Not pending — try to claim PENDING.
                match self.bits.compare_exchange_weak(
                    current,
                    COALESCE_PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(observed) => {
                        current = observed;
                        continue;
                    }
                }
            }
        }
    }

    /// Called by the executor callback after invoking the user callback.
    /// Determines the next action based on the REARM flag.
    #[inline]
    fn post_callback_action(&self, inline_dispatches: usize) -> CoalesceDispatchAction {
        let mut current = self.bits.load(Ordering::Acquire);
        loop {
            if current & COALESCE_REARM != 0 {
                // Rearm was requested — clear REARM, keep PENDING.
                let desired = current & !COALESCE_REARM;
                match self.bits.compare_exchange_weak(
                    current,
                    desired,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        if inline_dispatches < COALESCE_INLINE_BURST_MAX {
                            return CoalesceDispatchAction::ContinueInline;
                        } else {
                            return CoalesceDispatchAction::Requeue;
                        }
                    }
                    Err(observed) => {
                        current = observed;
                        continue;
                    }
                }
            } else {
                // No rearm — clear PENDING, we're done.
                match self.bits.compare_exchange_weak(
                    current,
                    0,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return CoalesceDispatchAction::Complete,
                    Err(observed) => {
                        current = observed;
                        continue;
                    }
                }
            }
        }
    }

    /// Unconditionally clear PENDING (used on error/panic paths).
    #[inline]
    fn clear_pending(&self) {
        self.bits.fetch_and(!COALESCE_PENDING, Ordering::Release);
    }

    #[inline]
    fn clear_all(&self) {
        self.bits.store(0, Ordering::Release);
    }
}

impl fmt::Debug for CoalesceDispatchState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bits = self.bits.load(Ordering::Relaxed);
        f.debug_struct("CoalesceDispatchState")
            .field("pending", &(bits & COALESCE_PENDING != 0))
            .field("rearm", &(bits & COALESCE_REARM != 0))
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoalesceDispatchAction {
    ContinueInline,
    Requeue,
    Complete,
}

struct PendingResetGuard {
    state: Arc<CoalesceDispatchState>,
    armed: bool,
}

impl PendingResetGuard {
    fn new(state: Arc<CoalesceDispatchState>) -> Self {
        Self { state, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingResetGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        self.state.clear_all();
    }
}

impl HighPrecisionTimerControl {
    fn new() -> Self {
        Self {
            state: Mutex::new(HighPrecisionTimerControlState::default()),
            cv: Condvar::new(),
        }
    }

    fn notify(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.notify_seq = state.notify_seq.wrapping_add(1);
            self.cv.notify_all();
        }
    }
}

#[cfg(target_os = "linux")]
impl LinuxTimerfdScheduler {
    fn global() -> AortaResult<Self> {
        static SCHEDULER: OnceLock<Result<LinuxTimerfdScheduler, String>> = OnceLock::new();
        match SCHEDULER.get_or_init(|| Self::new().map_err(|err| err.to_string())) {
            Ok(scheduler) => Ok(scheduler.clone()),
            Err(err) => Err(AortaError::internal(format!(
                "failed to initialize linux timerfd scheduler: {err}"
            ))),
        }
    }

    fn new() -> AortaResult<Self> {
        let timer_fd = Self::create_timer_fd()?;
        let notify_fd = Self::create_event_fd()?;
        let inner = Arc::new(LinuxTimerfdSchedulerInner {
            state: Mutex::new(LinuxTimerfdSchedulerState {
                next_id: 1,
                ..LinuxTimerfdSchedulerState::default()
            }),
            notify_fd,
            timer_fd,
            alive: AtomicBool::new(false),
        });
        Self::spawn_lane(&inner)?;
        Ok(Self { inner })
    }

    /// Spawn the lane poll thread for `inner`, marking the lane alive. Single
    /// source for both initial creation and `register_timer`'s lazy respawn.
    fn spawn_lane(inner: &Arc<LinuxTimerfdSchedulerInner>) -> AortaResult<()> {
        inner.alive.store(true, Ordering::Release);
        let thread_inner = Arc::clone(inner);
        std::thread::Builder::new()
            .name("aorta-timerfd-lane".to_string())
            .spawn(move || Self::run(thread_inner))
            .map_err(|err| {
                // Spawn failed — the lane is not actually running.
                inner.alive.store(false, Ordering::Release);
                AortaError::internal(format!("failed to spawn linux timerfd scheduler: {err}"))
            })?;
        Ok(())
    }

    /// If the lane has died, lazily respawn it. Gated to one respawn per
    /// `LANE_RESPAWN_MIN_INTERVAL` so a persistently-failing lane can't spawn a
    /// thread storm. Called from `register_timer` so a newly-registered timer
    /// revives a dead subsystem.
    fn ensure_lane_alive(&self) {
        if self.inner.alive.load(Ordering::Acquire) {
            return;
        }
        {
            let mut state = match self.inner.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            let now = StdInstant::now();
            if let Some(last) = state.last_respawn_at {
                if now.duration_since(last) < LANE_RESPAWN_MIN_INTERVAL {
                    return;
                }
            }
            state.last_respawn_at = Some(now);
        }
        // Re-check after taking the gate; the old lane's AliveGuard has already
        // run (that's how we observed `alive == false`), so no clobber race.
        if self.inner.alive.load(Ordering::Acquire) {
            return;
        }
        match Self::spawn_lane(&self.inner) {
            Ok(()) => {
                crate::aorta_core_warn!("aorta timerfd lane respawned after it had exited")
            }
            Err(err) => crate::aorta_core_error!(
                "aorta timerfd lane respawn failed: {err}; periodic timers remain stalled"
            ),
        }
    }

    fn create_timer_fd() -> AortaResult<OwnedFd> {
        // SAFETY: timerfd_create is called with valid Linux constants.
        let fd = unsafe { timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK) };
        if fd < 0 {
            return Err(AortaError::internal(format!(
                "timerfd_create failed: {}",
                std::io::Error::last_os_error()
            )));
        }
        // SAFETY: timerfd_create returns a new owned file descriptor on success.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn create_event_fd() -> AortaResult<OwnedFd> {
        // SAFETY: eventfd is called with valid Linux constants.
        let fd = unsafe { eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK) };
        if fd < 0 {
            return Err(AortaError::internal(format!(
                "eventfd failed: {}",
                std::io::Error::last_os_error()
            )));
        }
        // SAFETY: eventfd returns a new owned file descriptor on success.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn register_timer(
        &self,
        period: Duration,
        callback: Arc<dyn Fn() + Send + Sync + 'static>,
        event_sender: EventQueueSender,
        running: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        generation: Arc<AtomicU64>,
        coalesced_tick_count: Arc<AtomicU64>,
        deadline_slip_total_ns: Arc<AtomicU64>,
        deadline_slip_max_ns: Arc<AtomicU64>,
        due_firings_total: Arc<AtomicU64>,
        due_firings_max: Arc<AtomicU64>,
        due_sample_count: Arc<AtomicU64>,
        coalesce_requeue_count: Arc<AtomicU64>,
        coalesce_inline_dispatch_total: Arc<AtomicU64>,
        coalesce_inline_dispatch_max: Arc<AtomicU64>,
        coalesce_event_wait_total_ns: Arc<AtomicU64>,
        coalesce_event_wait_max_ns: Arc<AtomicU64>,
        coalesce_event_wait_sample_count: Arc<AtomicU64>,
        invoke_on_missed: bool,
        callback_group: CallbackGroupType,
        callback_group_handle: Option<CallbackGroupHandle>,
        callback_trace_event: Option<u16>,
    ) -> AortaResult<u64> {
        if period.is_zero() {
            return Err(AortaError::invalid_argument(
                "timer period must be non-zero",
            ));
        }

        let mut state = self
            .inner
            .state
            .lock()
            .expect("linux timerfd scheduler mutex poisoned");
        let timer_id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);

        let entry = LinuxTimerfdTimerEntry {
            period,
            next_deadline: StdInstant::now() + period,
            invoke_on_missed,
            callback,
            event_sender,
            callback_group,
            callback_group_handle,
            callback_trace_event,
            running,
            stopped,
            generation,
            coalesced_tick_count,
            deadline_slip_total_ns,
            deadline_slip_max_ns,
            due_firings_total,
            due_firings_max,
            due_sample_count,
            coalesce_requeue_count,
            coalesce_inline_dispatch_total,
            coalesce_inline_dispatch_max,
            coalesce_event_wait_total_ns,
            coalesce_event_wait_max_ns,
            coalesce_event_wait_sample_count,
            coalesce_state: Arc::new(CoalesceDispatchState::new()),
            schedule_version: 1,
        };
        if entry.running.load(Ordering::Relaxed) && !entry.stopped.load(Ordering::Relaxed) {
            state.heap.push(LinuxTimerfdHeapEntry {
                deadline: entry.next_deadline,
                timer_id,
                schedule_version: entry.schedule_version,
            });
        }
        state.timers.insert(timer_id, entry);
        drop(state);
        // Revive the lane if it had died, so a newly-registered timer is not
        // silently dropped on the floor.
        self.ensure_lane_alive();
        self.notify();
        Ok(timer_id)
    }

    fn pause_timer(&self, timer_id: u64) {
        if let Ok(mut state) = self.inner.state.lock() {
            if let Some(entry) = state.timers.get_mut(&timer_id) {
                entry.running.store(false, Ordering::Relaxed);
                entry.schedule_version = entry.schedule_version.wrapping_add(1);
            }
            // The bumped schedule_version stranded this timer's old heap entry;
            // reclaim accumulated stale entries if the heap has bloated.
            Self::compact_heap_if_bloated(&mut state);
        }
        self.notify();
    }

    fn resume_timer(&self, timer_id: u64) {
        if let Ok(mut state) = self.inner.state.lock() {
            let mut heap_entry = None;
            if let Some(entry) = state.timers.get_mut(&timer_id) {
                if entry.stopped.load(Ordering::Relaxed) {
                    return;
                }
                entry.running.store(true, Ordering::Relaxed);
                entry.next_deadline = StdInstant::now() + entry.period;
                entry.schedule_version = entry.schedule_version.wrapping_add(1);
                heap_entry = Some(LinuxTimerfdHeapEntry {
                    deadline: entry.next_deadline,
                    timer_id,
                    schedule_version: entry.schedule_version,
                });
            }
            if let Some(heap_entry) = heap_entry {
                state.heap.push(heap_entry);
            }
            // Each resume pushes a fresh entry and strands the previous one;
            // repeated pause/resume of a long-period timer would grow the heap
            // unbounded without periodic compaction.
            Self::compact_heap_if_bloated(&mut state);
        }
        self.notify();
    }

    fn remove_timer(&self, timer_id: u64) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.timers.remove(&timer_id);
            // Removing the timer leaves its heap entries orphaned and lowers the
            // live-timer count; compact so a churn of add/remove can't bloat the
            // heap with entries that never reach the top.
            Self::compact_heap_if_bloated(&mut state);
        }
        self.notify();
    }

    fn notify(&self) {
        let value: u64 = 1;
        loop {
            // SAFETY: notify_fd is a valid eventfd opened by create_event_fd.
            let rc = unsafe {
                write(
                    self.inner.notify_fd.as_raw_fd(),
                    &value as *const u64 as *const _,
                    std::mem::size_of::<u64>(),
                )
            };
            if rc as usize == std::mem::size_of::<u64>() {
                return;
            }
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                Some(EINTR) => continue,
                Some(EAGAIN) => return,
                _ => return,
            }
        }
    }

    fn run(inner: Arc<LinuxTimerfdSchedulerInner>) {
        // Mark the lane dead on ANY exit from `run` (only a panic can get here
        // now — the loop no longer breaks on syscall errors). This lets
        // `register_timer` lazily respawn the lane.
        struct AliveGuard<'a>(&'a AtomicBool);
        impl Drop for AliveGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
                crate::aorta_core_error!(
                    "aorta timerfd lane thread exited unexpectedly; periodic timers are stalled \
                     until the next timer registration respawns the lane"
                );
            }
        }
        let _alive_guard = AliveGuard(&inner.alive);

        // Track the last deadline we armed the timerfd with to skip redundant
        // timerfd_settime syscalls when the next deadline hasn't changed.
        let mut last_armed_deadline: Option<StdInstant> = None;
        // Consecutive transient syscall failures, for bounded backoff. Reset on
        // any successful poll. Previously the loop `break`-ed on these errors,
        // silently killing every periodic timer in the process.
        let mut consecutive_failures: u32 = 0;
        loop {
            // Only re-arm the timerfd when the next deadline actually changed.
            let next_deadline = {
                let mut state = inner
                    .state
                    .lock()
                    .expect("linux timerfd scheduler mutex poisoned");
                Self::next_active_deadline_locked(&mut state)
            };
            if next_deadline != last_armed_deadline {
                if let Err(err) =
                    Self::set_timer_fd_deadline(inner.timer_fd.as_raw_fd(), next_deadline)
                {
                    // Do NOT update last_armed_deadline — retry arming next loop.
                    let msg = err.to_string();
                    Self::lane_backoff_after_error("arm timerfd", &msg, &mut consecutive_failures);
                    continue;
                }
                last_armed_deadline = next_deadline;
            }

            let mut poll_fds = [
                pollfd {
                    fd: inner.timer_fd.as_raw_fd(),
                    events: POLLIN,
                    revents: 0,
                },
                pollfd {
                    fd: inner.notify_fd.as_raw_fd(),
                    events: POLLIN,
                    revents: 0,
                },
            ];

            // SAFETY: poll_fds points to valid pollfd entries and timeout -1 blocks indefinitely.
            let rc = unsafe { poll(poll_fds.as_mut_ptr(), poll_fds.len() as _, -1) };
            if rc < 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(EINTR) {
                    continue;
                }
                let msg = err.to_string();
                Self::lane_backoff_after_error("poll", &msg, &mut consecutive_failures);
                continue;
            }
            // Successful poll — clear the failure streak.
            consecutive_failures = 0;

            if poll_fds[1].revents & POLLIN != 0 {
                Self::drain_counter_fd(inner.notify_fd.as_raw_fd());
                // A new timer was registered or rescheduled — invalidate cache.
                last_armed_deadline = None;
            }

            if poll_fds[0].revents & POLLIN != 0 {
                Self::drain_counter_fd(inner.timer_fd.as_raw_fd());
                let specs = Self::collect_due_enqueues(&inner);
                Self::dispatch_due_enqueues(&inner, specs);
                // Timer events were dispatched — deadline has changed.
                last_armed_deadline = None;
            }
        }
    }

    /// Log (throttled) and sleep with bounded exponential backoff after a
    /// recoverable lane syscall error, then let the caller `continue`. Replaces
    /// the old `break` that silently killed the lane; the bounded backoff stops
    /// a deterministic failure from busy-spinning a core at 100%.
    fn lane_backoff_after_error(op: &str, err: &str, consecutive_failures: &mut u32) {
        *consecutive_failures = consecutive_failures.saturating_add(1);
        // 1ms, 2, 4, ... capped at LANE_BACKOFF_MAX so new-timer notifications
        // are still observed within a bounded delay.
        let shift = (*consecutive_failures - 1).min(6);
        let backoff = Duration::from_millis(1u64 << shift).min(LANE_BACKOFF_MAX);
        // Throttle: first few failures, then sparsely, so a persistent failure
        // is visible without flooding the log.
        if *consecutive_failures <= 3 || (*consecutive_failures).is_multiple_of(256) {
            crate::aorta_core_warn!(
                "aorta timerfd lane: {op} failed ({err}); retrying after {}ms \
                 (consecutive failure #{})",
                backoff.as_millis(),
                *consecutive_failures
            );
        }
        std::thread::sleep(backoff);
    }

    fn rearm_timer_fd(inner: &Arc<LinuxTimerfdSchedulerInner>) -> AortaResult<()> {
        let next_deadline = {
            let mut state = inner
                .state
                .lock()
                .expect("linux timerfd scheduler mutex poisoned");
            Self::next_active_deadline_locked(&mut state)
        };
        Self::set_timer_fd_deadline(inner.timer_fd.as_raw_fd(), next_deadline)
    }

    /// Returns the number of heap entries that are valid for their timer's
    /// current schedule (one per live, running timer). Stale entries left behind
    /// by pause/resume/remove are not counted.
    fn live_heap_entry_count(state: &LinuxTimerfdSchedulerState) -> usize {
        state
            .timers
            .values()
            .filter(|entry| {
                entry.running.load(Ordering::Relaxed) && !entry.stopped.load(Ordering::Relaxed)
            })
            .count()
    }

    /// Rebuilds the deadline heap to hold exactly one entry per live timer when
    /// lazy-deletion debris has bloated it past
    /// `live * HEAP_COMPACTION_FACTOR + HEAP_COMPACTION_SLACK`. Cheap and a no-op
    /// in the common case; bounds heap size to O(live timers) under pause/resume
    /// churn. Must be called while holding the state lock.
    fn compact_heap_if_bloated(state: &mut LinuxTimerfdSchedulerState) {
        let live = Self::live_heap_entry_count(state);
        let threshold = live
            .saturating_mul(HEAP_COMPACTION_FACTOR)
            .saturating_add(HEAP_COMPACTION_SLACK);
        if state.heap.len() <= threshold {
            return;
        }
        let mut rebuilt = BinaryHeap::with_capacity(live);
        for (timer_id, entry) in state.timers.iter() {
            if entry.running.load(Ordering::Relaxed) && !entry.stopped.load(Ordering::Relaxed) {
                rebuilt.push(LinuxTimerfdHeapEntry {
                    deadline: entry.next_deadline,
                    timer_id: *timer_id,
                    schedule_version: entry.schedule_version,
                });
            }
        }
        state.heap = rebuilt;
    }

    fn next_active_deadline_locked(state: &mut LinuxTimerfdSchedulerState) -> Option<StdInstant> {
        loop {
            let heap_entry = *state.heap.peek()?;
            let Some(entry) = state.timers.get(&heap_entry.timer_id) else {
                state.heap.pop();
                continue;
            };
            if entry.schedule_version != heap_entry.schedule_version
                || entry.next_deadline != heap_entry.deadline
                || entry.stopped.load(Ordering::Relaxed)
                || !entry.running.load(Ordering::Relaxed)
            {
                state.heap.pop();
                continue;
            }
            return Some(heap_entry.deadline);
        }
    }

    fn set_timer_fd_deadline(deadline_fd: i32, deadline: Option<StdInstant>) -> AortaResult<()> {
        let spec = match deadline {
            Some(deadline) => {
                let now = StdInstant::now();
                let remaining = deadline.saturating_duration_since(now);
                let Some(now_ts) = Timer::clock_monotonic_now() else {
                    return Err(AortaError::internal(
                        "clock_gettime(CLOCK_MONOTONIC) failed while arming timerfd",
                    ));
                };
                let target_ts = Timer::timespec_add_duration(now_ts, remaining);
                itimerspec {
                    it_interval: timespec {
                        tv_sec: 0,
                        tv_nsec: 0,
                    },
                    it_value: target_ts,
                }
            }
            None => itimerspec {
                it_interval: timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                it_value: timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
            },
        };

        // SAFETY: deadline_fd is a valid timerfd and spec points to initialized memory.
        let rc = unsafe {
            timerfd_settime(
                deadline_fd,
                TFD_TIMER_ABSTIME,
                &spec as *const itimerspec,
                std::ptr::null_mut(),
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(AortaError::internal(format!(
                "timerfd_settime failed: {}",
                std::io::Error::last_os_error()
            )))
        }
    }

    fn drain_counter_fd(fd: i32) {
        let mut value = 0u64;
        loop {
            // SAFETY: fd refers to either timerfd or eventfd, both readable into a u64 buffer.
            let rc = unsafe {
                read(
                    fd,
                    &mut value as *mut u64 as *mut _,
                    std::mem::size_of::<u64>(),
                )
            };
            if rc as usize == std::mem::size_of::<u64>() {
                continue;
            }
            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                Some(EINTR) => continue,
                Some(EAGAIN) => break,
                _ => break,
            }
        }
    }

    fn collect_due_enqueues(
        inner: &Arc<LinuxTimerfdSchedulerInner>,
    ) -> Vec<LinuxTimerfdEnqueueSpec> {
        let now = StdInstant::now();
        let mut specs = Vec::new();
        let mut state = inner
            .state
            .lock()
            .expect("linux timerfd scheduler mutex poisoned");

        loop {
            let Some(next_deadline) = Self::next_active_deadline_locked(&mut state) else {
                break;
            };
            if next_deadline > now {
                break;
            }

            let heap_entry = state
                .heap
                .pop()
                .expect("heap entry disappeared after deadline check");
            let Some(entry) = state.timers.get_mut(&heap_entry.timer_id) else {
                continue;
            };
            let mut requeue_entry = None;
            let due_sample = compute_due_sample_std(
                now,
                &mut entry.next_deadline,
                entry.period,
                entry.invoke_on_missed,
            );
            if due_sample.due_firings > 0 {
                record_due_sample(
                    due_sample,
                    &entry.deadline_slip_total_ns,
                    &entry.deadline_slip_max_ns,
                    &entry.due_firings_total,
                    &entry.due_firings_max,
                    &entry.due_sample_count,
                );

                let due_firings = due_sample.due_firings;
                let should_enqueue = entry.coalesce_state.try_mark_pending();

                if !should_enqueue {
                    entry
                        .coalesced_tick_count
                        .fetch_add(due_firings, Ordering::Relaxed);
                } else {
                    if due_firings > 1 {
                        entry
                            .coalesced_tick_count
                            .fetch_add(due_firings - 1, Ordering::Relaxed);
                    }

                    specs.push(LinuxTimerfdEnqueueSpec {
                        timer_id: heap_entry.timer_id,
                        callback: Arc::clone(&entry.callback),
                        event_sender: entry.event_sender.clone(),
                        running: Arc::clone(&entry.running),
                        stopped: Arc::clone(&entry.stopped),
                        generation: Arc::clone(&entry.generation),
                        coalesce_state: Arc::clone(&entry.coalesce_state),
                        coalesced_tick_count: Arc::clone(&entry.coalesced_tick_count),
                        coalesce_requeue_count: Arc::clone(&entry.coalesce_requeue_count),
                        coalesce_inline_dispatch_total: Arc::clone(
                            &entry.coalesce_inline_dispatch_total,
                        ),
                        coalesce_inline_dispatch_max: Arc::clone(
                            &entry.coalesce_inline_dispatch_max,
                        ),
                        coalesce_event_wait_total_ns: Arc::clone(
                            &entry.coalesce_event_wait_total_ns,
                        ),
                        coalesce_event_wait_max_ns: Arc::clone(&entry.coalesce_event_wait_max_ns),
                        coalesce_event_wait_sample_count: Arc::clone(
                            &entry.coalesce_event_wait_sample_count,
                        ),
                        callback_group: entry.callback_group,
                        callback_group_handle: entry.callback_group_handle,
                        callback_trace_event: entry.callback_trace_event,
                    });
                }
            }

            if !entry.stopped.load(Ordering::Relaxed) && entry.running.load(Ordering::Relaxed) {
                requeue_entry = Some(LinuxTimerfdHeapEntry {
                    deadline: entry.next_deadline,
                    timer_id: heap_entry.timer_id,
                    schedule_version: entry.schedule_version,
                });
            }
            if let Some(requeue_entry) = requeue_entry {
                state.heap.push(requeue_entry);
            }
        }

        specs
    }

    fn dispatch_due_enqueues(
        inner: &Arc<LinuxTimerfdSchedulerInner>,
        specs: Vec<LinuxTimerfdEnqueueSpec>,
    ) {
        let mut remove_timer_ids = Vec::new();
        for spec in specs {
            // Keep references for error-path cleanup before consuming the spec fields.
            let err_coalesce = Arc::clone(&spec.coalesce_state);
            let err_ticks = Arc::clone(&spec.coalesced_tick_count);
            let err_stopped = Arc::clone(&spec.stopped);
            let timer_id = spec.timer_id;
            match Timer::try_enqueue_coalesced_event(
                spec.event_sender,
                spec.callback,
                spec.running,
                spec.stopped,
                spec.generation,
                spec.coalesce_state,
                spec.coalesced_tick_count,
                spec.coalesce_requeue_count,
                spec.coalesce_inline_dispatch_total,
                spec.coalesce_inline_dispatch_max,
                spec.coalesce_event_wait_total_ns,
                spec.coalesce_event_wait_max_ns,
                spec.coalesce_event_wait_sample_count,
                spec.callback_group,
                spec.callback_group_handle,
                spec.callback_trace_event,
            ) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_event)) => {
                    err_coalesce.clear_pending();
                    err_ticks.fetch_add(1, Ordering::Relaxed);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_event)) => {
                    err_coalesce.clear_all();
                    err_stopped.store(true, Ordering::Relaxed);
                    remove_timer_ids.push(timer_id);
                }
            }
        }

        if !remove_timer_ids.is_empty() {
            if let Ok(mut state) = inner.state.lock() {
                for timer_id in remove_timer_ids {
                    state.timers.remove(&timer_id);
                }
            }
        }
    }
}

impl Timer {
    #[cfg(target_os = "linux")]
    fn clock_monotonic_now() -> Option<timespec> {
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: ts points to valid writable memory and CLOCK_MONOTONIC is a valid clock id.
        let rc = unsafe { clock_gettime(CLOCK_MONOTONIC, &mut ts as *mut timespec) };
        if rc == 0 {
            Some(ts)
        } else {
            None
        }
    }

    #[cfg(target_os = "linux")]
    fn timespec_add_duration(mut ts: timespec, duration: Duration) -> timespec {
        let total_ns = ts.tv_nsec as i128 + duration.subsec_nanos() as i128;
        ts.tv_sec += duration.as_secs() as i64 + (total_ns / 1_000_000_000) as i64;
        ts.tv_nsec = (total_ns % 1_000_000_000) as i64;
        if ts.tv_nsec < 0 {
            ts.tv_sec -= 1;
            ts.tv_nsec += 1_000_000_000;
        }
        ts
    }

    #[cfg(target_os = "linux")]
    fn clock_nanosleep_until_remaining(remaining: Duration) {
        if remaining.is_zero() {
            return;
        }
        let Some(now_ts) = Self::clock_monotonic_now() else {
            std::thread::sleep(remaining);
            return;
        };
        let target_ts = Self::timespec_add_duration(now_ts, remaining);
        loop {
            // SAFETY: target_ts references valid memory, CLOCK_MONOTONIC/TIMER_ABSTIME are valid.
            let rc = unsafe {
                clock_nanosleep(
                    CLOCK_MONOTONIC,
                    TIMER_ABSTIME,
                    &target_ts as *const timespec,
                    std::ptr::null_mut(),
                )
            };
            if rc == 0 {
                break;
            }
            if rc != EINTR {
                break;
            }
        }
    }

    fn wait_high_precision_until(
        control: &HighPrecisionTimerControl,
        deadline: StdInstant,
        period: Duration,
        running: &AtomicBool,
        stopped: &AtomicBool,
    ) -> bool {
        let (_sleep_margin, yield_margin) = high_precision_wait_margins(period);
        #[cfg(target_os = "linux")]
        let abs_sleep_window = high_precision_abs_sleep_window(period, yield_margin);
        let mut observed_seq = control
            .state
            .lock()
            .expect("high precision timer control mutex poisoned")
            .notify_seq;

        loop {
            if stopped.load(Ordering::Relaxed) {
                return false;
            }
            if !running.load(Ordering::Relaxed) {
                return false;
            }

            let now = StdInstant::now();
            if now >= deadline {
                return true;
            }

            let remaining = deadline.saturating_duration_since(now);
            if remaining <= yield_margin {
                break;
            }

            #[cfg(target_os = "linux")]
            if remaining > abs_sleep_window {
                let wait = remaining - abs_sleep_window;
                let mut state = control
                    .state
                    .lock()
                    .expect("high precision timer control mutex poisoned");
                while state.notify_seq == observed_seq
                    && running.load(Ordering::Relaxed)
                    && !stopped.load(Ordering::Relaxed)
                {
                    let (next_state, timeout_result) = control
                        .cv
                        .wait_timeout(state, wait)
                        .expect("high precision timer condvar poisoned");
                    state = next_state;
                    if timeout_result.timed_out() {
                        break;
                    }
                }
                observed_seq = state.notify_seq;
                continue;
            }

            #[cfg(target_os = "linux")]
            if remaining > yield_margin {
                Self::clock_nanosleep_until_remaining(remaining - yield_margin);
                continue;
            }

            #[cfg(not(target_os = "linux"))]
            match non_linux_wait_action(remaining, yield_margin, _sleep_margin) {
                NonLinuxWaitAction::Condvar => {
                    let wait = remaining - _sleep_margin;
                    let mut state = control
                        .state
                        .lock()
                        .expect("high precision timer control mutex poisoned");
                    while state.notify_seq == observed_seq
                        && running.load(Ordering::Relaxed)
                        && !stopped.load(Ordering::Relaxed)
                    {
                        let (next_state, timeout_result) = control
                            .cv
                            .wait_timeout(state, wait)
                            .expect("high precision timer condvar poisoned");
                        state = next_state;
                        if timeout_result.timed_out() {
                            break;
                        }
                    }
                    observed_seq = state.notify_seq;
                    continue;
                }
                NonLinuxWaitAction::Sleep(nap) => {
                    // Real sleep down to the final yield margin instead of
                    // spin-yielding the whole window and pinning a core.
                    if !nap.is_zero() {
                        std::thread::sleep(nap);
                    }
                    continue;
                }
                NonLinuxWaitAction::PrecisionSpin => break,
            }

            #[cfg(target_os = "linux")]
            std::thread::yield_now();
        }

        while StdInstant::now() < deadline {
            if stopped.load(Ordering::Relaxed) || !running.load(Ordering::Relaxed) {
                return false;
            }
            let remaining = deadline.saturating_duration_since(StdInstant::now());
            if remaining > yield_margin {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
        true
    }

    fn wait_high_precision_resume(
        control: &HighPrecisionTimerControl,
        running: &AtomicBool,
        stopped: &AtomicBool,
    ) -> bool {
        let mut state = control
            .state
            .lock()
            .expect("high precision timer control mutex poisoned");
        let observed_seq = state.notify_seq;
        while !running.load(Ordering::Relaxed) && !stopped.load(Ordering::Relaxed) {
            state = control
                .cv
                .wait(state)
                .expect("high precision timer condvar poisoned");
            if state.notify_seq != observed_seq {
                // Continue re-checking atomics after any external signal.
            }
        }
        !stopped.load(Ordering::Relaxed)
    }

    /// Creates a new timer with the given period and callback
    ///
    /// # Arguments
    /// * `period` - Duration between timer ticks
    /// * `callback` - Function to invoke on each tick
    /// * `event_sender` - Channel to send timer events
    /// * `options` - Timer configuration options
    pub fn new<F>(
        period: Duration,
        callback: F,
        event_sender: EventQueueSender,
        options: TimerOptions,
    ) -> AortaResult<Self>
    where
        F: Fn() + Send + Sync + 'static,
    {
        if period.is_zero() {
            return Err(AortaError::invalid_argument(
                "timer period must be non-zero",
            ));
        }

        let running = Arc::new(AtomicBool::new(options.auto_start));
        let stopped = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(AtomicU64::new(1));
        let coalesced_tick_count = Arc::new(AtomicU64::new(0));
        let overflow_timeout_count = Arc::new(AtomicU64::new(0));
        let deadline_slip_total_ns = Arc::new(AtomicU64::new(0));
        let deadline_slip_max_ns = Arc::new(AtomicU64::new(0));
        let due_firings_total = Arc::new(AtomicU64::new(0));
        let due_firings_max = Arc::new(AtomicU64::new(0));
        let due_sample_count = Arc::new(AtomicU64::new(0));
        let coalesce_requeue_count = Arc::new(AtomicU64::new(0));
        let coalesce_inline_dispatch_total = Arc::new(AtomicU64::new(0));
        let coalesce_inline_dispatch_max = Arc::new(AtomicU64::new(0));
        let coalesce_event_wait_total_ns = Arc::new(AtomicU64::new(0));
        let coalesce_event_wait_max_ns = Arc::new(AtomicU64::new(0));
        let coalesce_event_wait_sample_count = Arc::new(AtomicU64::new(0));
        // Pre-intern `sched.callback:timer:<name>` ONCE so dispatch carries only a u16
        // (zero per-tick alloc/intern). `None` for an unnamed timer.
        let callback_trace_event = options
            .name
            .as_ref()
            .map(|name| crate::trace::intern_event(&format!("sched.callback:timer:{name}")));
        let backend = Self::spawn_timer_task(
            period,
            callback,
            event_sender,
            Arc::clone(&running),
            Arc::clone(&stopped),
            Arc::clone(&generation),
            Arc::clone(&coalesced_tick_count),
            Arc::clone(&overflow_timeout_count),
            Arc::clone(&deadline_slip_total_ns),
            Arc::clone(&deadline_slip_max_ns),
            Arc::clone(&due_firings_total),
            Arc::clone(&due_firings_max),
            Arc::clone(&due_sample_count),
            Arc::clone(&coalesce_requeue_count),
            Arc::clone(&coalesce_inline_dispatch_total),
            Arc::clone(&coalesce_inline_dispatch_max),
            Arc::clone(&coalesce_event_wait_total_ns),
            Arc::clone(&coalesce_event_wait_max_ns),
            Arc::clone(&coalesce_event_wait_sample_count),
            options.invoke_on_missed,
            options.callback_group,
            options.callback_group_handle,
            callback_trace_event,
            options.overflow_policy,
            options.overflow_block_timeout,
        )?;

        Ok(Self {
            period,
            running,
            stopped,
            generation,
            coalesced_tick_count,
            overflow_timeout_count,
            deadline_slip_total_ns,
            deadline_slip_max_ns,
            due_firings_total,
            due_firings_max,
            due_sample_count,
            coalesce_requeue_count,
            coalesce_inline_dispatch_total,
            coalesce_inline_dispatch_max,
            coalesce_event_wait_total_ns,
            coalesce_event_wait_max_ns,
            coalesce_event_wait_sample_count,
            backend: Some(backend),
            snapshot_producer_guard: None,
        })
    }

    pub(crate) fn new_executor_owned<F>(
        period: Duration,
        callback: F,
        scheduler: ExecutorTimerScheduler,
        options: TimerOptions,
    ) -> AortaResult<Self>
    where
        F: Fn() + Send + Sync + 'static,
    {
        if period.is_zero() {
            return Err(AortaError::invalid_argument(
                "timer period must be non-zero",
            ));
        }

        let running = Arc::new(AtomicBool::new(options.auto_start));
        let stopped = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(AtomicU64::new(1));
        let coalesced_tick_count = Arc::new(AtomicU64::new(0));
        let overflow_timeout_count = Arc::new(AtomicU64::new(0));
        let deadline_slip_total_ns = Arc::new(AtomicU64::new(0));
        let deadline_slip_max_ns = Arc::new(AtomicU64::new(0));
        let due_firings_total = Arc::new(AtomicU64::new(0));
        let due_firings_max = Arc::new(AtomicU64::new(0));
        let due_sample_count = Arc::new(AtomicU64::new(0));
        let coalesce_requeue_count = Arc::new(AtomicU64::new(0));
        let coalesce_inline_dispatch_total = Arc::new(AtomicU64::new(0));
        let coalesce_inline_dispatch_max = Arc::new(AtomicU64::new(0));
        let coalesce_event_wait_total_ns = Arc::new(AtomicU64::new(0));
        let coalesce_event_wait_max_ns = Arc::new(AtomicU64::new(0));
        let coalesce_event_wait_sample_count = Arc::new(AtomicU64::new(0));
        let timer_id = scheduler.register_timer(
            period,
            callback,
            &options,
            Arc::clone(&running),
            Arc::clone(&stopped),
            Arc::clone(&generation),
            Arc::clone(&coalesced_tick_count),
            Arc::clone(&deadline_slip_total_ns),
            Arc::clone(&deadline_slip_max_ns),
            Arc::clone(&due_firings_total),
            Arc::clone(&due_firings_max),
            Arc::clone(&due_sample_count),
            Arc::clone(&coalesce_requeue_count),
            Arc::clone(&coalesce_inline_dispatch_total),
            Arc::clone(&coalesce_inline_dispatch_max),
            Arc::clone(&coalesce_event_wait_total_ns),
            Arc::clone(&coalesce_event_wait_max_ns),
            Arc::clone(&coalesce_event_wait_sample_count),
        )?;

        Ok(Self {
            period,
            running,
            stopped,
            generation,
            coalesced_tick_count,
            overflow_timeout_count,
            deadline_slip_total_ns,
            deadline_slip_max_ns,
            due_firings_total,
            due_firings_max,
            due_sample_count,
            coalesce_requeue_count,
            coalesce_inline_dispatch_total,
            coalesce_inline_dispatch_max,
            coalesce_event_wait_total_ns,
            coalesce_event_wait_max_ns,
            coalesce_event_wait_sample_count,
            snapshot_producer_guard: None,
            backend: Some(TimerBackend::ExecutorOwned(ExecutorOwnedTimerBackend {
                scheduler,
                timer_id,
            })),
        })
    }

    /// Spawns the background task that generates timer events
    fn spawn_timer_task<F>(
        period: Duration,
        callback: F,
        event_sender: EventQueueSender,
        running: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        generation: Arc<AtomicU64>,
        coalesced_tick_count: Arc<AtomicU64>,
        overflow_timeout_count: Arc<AtomicU64>,
        deadline_slip_total_ns: Arc<AtomicU64>,
        deadline_slip_max_ns: Arc<AtomicU64>,
        due_firings_total: Arc<AtomicU64>,
        due_firings_max: Arc<AtomicU64>,
        due_sample_count: Arc<AtomicU64>,
        coalesce_requeue_count: Arc<AtomicU64>,
        coalesce_inline_dispatch_total: Arc<AtomicU64>,
        coalesce_inline_dispatch_max: Arc<AtomicU64>,
        coalesce_event_wait_total_ns: Arc<AtomicU64>,
        coalesce_event_wait_max_ns: Arc<AtomicU64>,
        coalesce_event_wait_sample_count: Arc<AtomicU64>,
        invoke_on_missed: bool,
        callback_group: CallbackGroupType,
        callback_group_handle: Option<CallbackGroupHandle>,
        callback_trace_event: Option<u16>,
        overflow_policy: TimerOverflowPolicy,
        overflow_block_timeout: Duration,
    ) -> AortaResult<TimerBackend>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let callback: Arc<dyn Fn() + Send + Sync + 'static> = Arc::new(callback);
        match overflow_policy {
            TimerOverflowPolicy::CoalesceLatest => {
                #[cfg(target_os = "linux")]
                {
                    let scheduler = LinuxTimerfdScheduler::global()?;
                    let timer_id = scheduler.register_timer(
                        period,
                        Arc::clone(&callback),
                        event_sender,
                        running,
                        stopped,
                        generation,
                        coalesced_tick_count,
                        deadline_slip_total_ns,
                        deadline_slip_max_ns,
                        due_firings_total,
                        due_firings_max,
                        due_sample_count,
                        coalesce_requeue_count,
                        coalesce_inline_dispatch_total,
                        coalesce_inline_dispatch_max,
                        coalesce_event_wait_total_ns,
                        coalesce_event_wait_max_ns,
                        coalesce_event_wait_sample_count,
                        invoke_on_missed,
                        callback_group,
                        callback_group_handle,
                        callback_trace_event,
                    )?;
                    Ok(TimerBackend::LinuxTimerfd(LinuxTimerfdTimerBackend {
                        scheduler,
                        timer_id,
                    }))
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let control = Arc::new(HighPrecisionTimerControl::new());
                    let coalesce_state = Arc::new(CoalesceDispatchState::new());
                    let thread_control = Arc::clone(&control);
                    let thread_name = "aorta_timer_keep_latest".to_string();
                    let thread = std::thread::Builder::new()
                        .name(thread_name)
                        .spawn(move || {
                            let mut next_deadline = StdInstant::now() + period;

                            loop {
                                if stopped.load(Ordering::Relaxed) {
                                    break;
                                }

                                if !running.load(Ordering::Relaxed) {
                                    if !Self::wait_high_precision_resume(
                                        &thread_control,
                                        &running,
                                        &stopped,
                                    ) {
                                        break;
                                    }
                                    next_deadline = StdInstant::now() + period;
                                    continue;
                                }

                                if !Self::wait_high_precision_until(
                                    &thread_control,
                                    next_deadline,
                                    period,
                                    &running,
                                    &stopped,
                                ) {
                                    if stopped.load(Ordering::Relaxed) {
                                        break;
                                    }
                                    next_deadline = StdInstant::now() + period;
                                    continue;
                                }

                                let now = StdInstant::now();
                                let due_sample = compute_due_sample_std(
                                    now,
                                    &mut next_deadline,
                                    period,
                                    invoke_on_missed,
                                );
                                if due_sample.due_firings == 0 {
                                    continue;
                                }

                                record_due_sample(
                                    due_sample,
                                    &deadline_slip_total_ns,
                                    &deadline_slip_max_ns,
                                    &due_firings_total,
                                    &due_firings_max,
                                    &due_sample_count,
                                );

                                let due_firings = due_sample.due_firings;
                                let should_enqueue = coalesce_state.try_mark_pending();

                                if !should_enqueue {
                                    coalesced_tick_count.fetch_add(due_firings, Ordering::Relaxed);
                                    continue;
                                }

                                if due_firings > 1 {
                                    coalesced_tick_count
                                        .fetch_add(due_firings - 1, Ordering::Relaxed);
                                }

                                match Self::try_enqueue_coalesced_event(
                                    event_sender.clone(),
                                    Arc::clone(&callback),
                                    Arc::clone(&running),
                                    Arc::clone(&stopped),
                                    Arc::clone(&generation),
                                    Arc::clone(&coalesce_state),
                                    Arc::clone(&coalesced_tick_count),
                                    Arc::clone(&coalesce_requeue_count),
                                    Arc::clone(&coalesce_inline_dispatch_total),
                                    Arc::clone(&coalesce_inline_dispatch_max),
                                    Arc::clone(&coalesce_event_wait_total_ns),
                                    Arc::clone(&coalesce_event_wait_max_ns),
                                    Arc::clone(&coalesce_event_wait_sample_count),
                                    callback_group,
                                    callback_group_handle,
                                    callback_trace_event,
                                ) {
                                    Ok(_) => {}
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_event)) => {
                                        coalesce_state.clear_pending();
                                        coalesced_tick_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_event)) => {
                                        coalesce_state.clear_all();
                                        break;
                                    }
                                }
                            }
                        })
                        .map_err(|err| {
                            AortaError::internal(format!(
                                "failed to spawn keep-latest timer thread: {err}"
                            ))
                        })?;
                    Ok(TimerBackend::HighPrecision(HighPrecisionTimerBackend {
                        control,
                        thread: Some(thread),
                    }))
                }
            }
            TimerOverflowPolicy::Block => Ok(TimerBackend::Task(tokio::spawn(async move {
                let mut interval = tokio::time::interval(period);
                interval.set_missed_tick_behavior(block_overflow_missed_tick_behavior(
                    invoke_on_missed,
                ));

                // Ensure the effective block timeout is at least 5x the period (min 2ms).
                // At 5kHz (period=200µs) this gives 1ms — enough for the executor to
                // drain one event without triggering false timeouts. The 2x multiplier
                // used previously caused timeout cascades at high rates, producing
                // bimodal p99 distributions (~2300µs ceiling).
                let effective_block_timeout = overflow_block_timeout
                    .max(period.checked_mul(5).unwrap_or(Duration::MAX))
                    .max(Duration::from_millis(2));

                interval.tick().await;

                loop {
                    interval.tick().await;

                    if stopped.load(Ordering::Relaxed) {
                        break;
                    }

                    if !running.load(Ordering::Relaxed) {
                        continue;
                    }

                    let cb = Arc::clone(&callback);
                    let running_for_event = Arc::clone(&running);
                    let stopped_for_event = Arc::clone(&stopped);
                    let generation_for_event = Arc::clone(&generation);
                    let generation_snapshot = generation_for_event.load(Ordering::Relaxed);
                    let event = Event::new(EventKind::Timer, move || {
                        if !stopped_for_event.load(Ordering::Relaxed)
                            && running_for_event.load(Ordering::Relaxed)
                            && generation_for_event.load(Ordering::Relaxed) == generation_snapshot
                        {
                            cb();
                        }
                    })
                    .with_callback_group(callback_group, callback_group_handle)
                    .with_callback_trace_event(callback_trace_event);

                    match tokio::time::timeout(effective_block_timeout, event_sender.send(event))
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => break,
                        Err(_) => {
                            overflow_timeout_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }))),
        }
    }

    fn try_enqueue_coalesced_event(
        event_sender: EventQueueSender,
        callback: Arc<dyn Fn() + Send + Sync + 'static>,
        running: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
        generation: Arc<AtomicU64>,
        coalesce_state: Arc<CoalesceDispatchState>,
        coalesced_tick_count: Arc<AtomicU64>,
        coalesce_requeue_count: Arc<AtomicU64>,
        coalesce_inline_dispatch_total: Arc<AtomicU64>,
        coalesce_inline_dispatch_max: Arc<AtomicU64>,
        coalesce_event_wait_total_ns: Arc<AtomicU64>,
        coalesce_event_wait_max_ns: Arc<AtomicU64>,
        coalesce_event_wait_sample_count: Arc<AtomicU64>,
        callback_group: CallbackGroupType,
        callback_group_handle: Option<CallbackGroupHandle>,
        callback_trace_event: Option<u16>,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<Event>> {
        let cb = Arc::clone(&callback);
        let running_for_event = Arc::clone(&running);
        let stopped_for_event = Arc::clone(&stopped);
        let generation_for_event = Arc::clone(&generation);
        let generation_snapshot = generation_for_event.load(Ordering::Relaxed);
        let event_sender_for_callback = event_sender.clone();
        let coalesce_state_for_callback = Arc::clone(&coalesce_state);
        let coalesced_tick_count_for_callback = Arc::clone(&coalesced_tick_count);
        let enqueue_started = StdInstant::now();
        let event = Event::new(EventKind::Timer, move || {
            let wait_ns = enqueue_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            coalesce_event_wait_total_ns.fetch_add(wait_ns, Ordering::Relaxed);
            fetch_max_relaxed(&coalesce_event_wait_max_ns, wait_ns);
            coalesce_event_wait_sample_count.fetch_add(1, Ordering::Relaxed);
            let mut pending_reset =
                PendingResetGuard::new(Arc::clone(&coalesce_state_for_callback));
            let mut inline_dispatches = 0usize;

            loop {
                if !stopped_for_event.load(Ordering::Relaxed)
                    && running_for_event.load(Ordering::Relaxed)
                    && generation_for_event.load(Ordering::Relaxed) == generation_snapshot
                {
                    cb();
                }
                inline_dispatches += 1;

                match coalesce_state_for_callback.post_callback_action(inline_dispatches) {
                    CoalesceDispatchAction::ContinueInline => {
                        coalesce_inline_dispatch_total.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    CoalesceDispatchAction::Complete => {
                        if inline_dispatches > 1 {
                            fetch_max_relaxed(
                                &coalesce_inline_dispatch_max,
                                (inline_dispatches - 1) as u64,
                            );
                        }
                        pending_reset.disarm();
                        return;
                    }
                    CoalesceDispatchAction::Requeue => {
                        if inline_dispatches > 1 {
                            fetch_max_relaxed(
                                &coalesce_inline_dispatch_max,
                                (inline_dispatches - 1) as u64,
                            );
                        }
                        coalesce_requeue_count.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }

            match Self::try_enqueue_coalesced_event(
                event_sender_for_callback.clone(),
                Arc::clone(&callback),
                Arc::clone(&running),
                Arc::clone(&stopped),
                Arc::clone(&generation),
                Arc::clone(&coalesce_state_for_callback),
                Arc::clone(&coalesced_tick_count_for_callback),
                Arc::clone(&coalesce_requeue_count),
                Arc::clone(&coalesce_inline_dispatch_total),
                Arc::clone(&coalesce_inline_dispatch_max),
                Arc::clone(&coalesce_event_wait_total_ns),
                Arc::clone(&coalesce_event_wait_max_ns),
                Arc::clone(&coalesce_event_wait_sample_count),
                callback_group,
                callback_group_handle,
                callback_trace_event,
            ) {
                Ok(()) => {
                    pending_reset.disarm();
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_event)) => {
                    coalesce_state_for_callback.clear_pending();
                    coalesced_tick_count_for_callback.fetch_add(1, Ordering::Relaxed);
                    pending_reset.disarm();
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_event)) => {
                    coalesce_state_for_callback.clear_pending();
                    pending_reset.disarm();
                }
            }
        })
        .with_callback_group(callback_group, callback_group_handle)
        .with_callback_trace_event(callback_trace_event);

        event_sender.try_send(event)
    }

    /// Attach the node's snapshot-producer guard. Kept as a
    /// post-construction setter so `Timer::new`'s public signature is
    /// unchanged; `Node::create_timer_with_options` is the caller.
    pub fn with_snapshot_producer_guard(
        mut self,
        guard: crate::node::SnapshotProducerGuard,
    ) -> Self {
        self.snapshot_producer_guard = Some(guard);
        self
    }

    /// Returns the timer period
    pub fn period(&self) -> Duration {
        self.period
    }

    /// Returns whether the timer is currently running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed) && !self.stopped.load(Ordering::Relaxed)
    }

    /// Returns how many timer firings were coalesced while one callback event was already pending.
    pub fn coalesced_tick_count(&self) -> u64 {
        self.coalesced_tick_count.load(Ordering::Relaxed)
    }

    /// Returns how many timer firings exceeded block timeout while queue was saturated.
    pub fn overflow_timeout_count(&self) -> u64 {
        self.overflow_timeout_count.load(Ordering::Relaxed)
    }

    /// Returns accumulated executor-owned timer deadline slip in nanoseconds.
    pub fn deadline_slip_total_ns(&self) -> u64 {
        self.deadline_slip_total_ns.load(Ordering::Relaxed)
    }

    /// Returns maximum executor-owned timer deadline slip in nanoseconds.
    pub fn deadline_slip_max_ns(&self) -> u64 {
        self.deadline_slip_max_ns.load(Ordering::Relaxed)
    }

    /// Returns accumulated number of due firings observed by the executor-owned scheduler.
    pub fn due_firings_total(&self) -> u64 {
        self.due_firings_total.load(Ordering::Relaxed)
    }

    /// Returns maximum number of due firings observed in a single scheduler sample.
    pub fn due_firings_max(&self) -> u64 {
        self.due_firings_max.load(Ordering::Relaxed)
    }

    /// Returns number of scheduler samples that observed one or more due firings.
    pub fn due_sample_count(&self) -> u64 {
        self.due_sample_count.load(Ordering::Relaxed)
    }

    /// Returns how many times a coalesced timer callback requeued itself after
    /// exhausting the inline dispatch budget.
    pub fn coalesce_requeue_count(&self) -> u64 {
        self.coalesce_requeue_count.load(Ordering::Relaxed)
    }

    /// Returns the total number of extra callback invocations that were
    /// dispatched inline under coalescing (beyond the first invocation in each
    /// event).
    pub fn coalesce_inline_dispatch_total(&self) -> u64 {
        self.coalesce_inline_dispatch_total.load(Ordering::Relaxed)
    }

    /// Returns the maximum number of extra inline dispatches observed in a
    /// single coalesced event.
    pub fn coalesce_inline_dispatch_max(&self) -> u64 {
        self.coalesce_inline_dispatch_max.load(Ordering::Relaxed)
    }

    pub fn coalesce_event_wait_total_ns(&self) -> u64 {
        self.coalesce_event_wait_total_ns.load(Ordering::Relaxed)
    }

    pub fn coalesce_event_wait_max_ns(&self) -> u64 {
        self.coalesce_event_wait_max_ns.load(Ordering::Relaxed)
    }

    pub fn coalesce_event_wait_sample_count(&self) -> u64 {
        self.coalesce_event_wait_sample_count
            .load(Ordering::Relaxed)
    }

    /// Pauses the timer (stops invoking callbacks until resumed)
    pub fn pause(&self) {
        if self.stopped.load(Ordering::Relaxed) {
            return;
        }

        self.running.store(false, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);
        match self.backend.as_ref() {
            Some(TimerBackend::ExecutorOwned(backend)) => {
                backend.scheduler.pause_timer(backend.timer_id);
            }
            #[cfg(target_os = "linux")]
            Some(TimerBackend::LinuxTimerfd(backend)) => {
                backend.scheduler.pause_timer(backend.timer_id);
            }
            #[cfg(not(target_os = "linux"))]
            Some(TimerBackend::HighPrecision(backend)) => {
                backend.control.notify();
            }
            _ => {}
        }
    }

    /// Resumes a paused timer
    pub fn resume(&self) {
        if self.stopped.load(Ordering::Relaxed) {
            return;
        }

        self.running.store(true, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);
        match self.backend.as_ref() {
            Some(TimerBackend::ExecutorOwned(backend)) => {
                backend.scheduler.resume_timer(backend.timer_id);
            }
            #[cfg(target_os = "linux")]
            Some(TimerBackend::LinuxTimerfd(backend)) => {
                backend.scheduler.resume_timer(backend.timer_id);
            }
            #[cfg(not(target_os = "linux"))]
            Some(TimerBackend::HighPrecision(backend)) => {
                backend.control.notify();
            }
            _ => {}
        }
    }

    /// Stops the timer and cancels the background task
    ///
    /// After calling stop(), the timer cannot be restarted.
    pub fn stop(&mut self) {
        if self.stopped.swap(true, Ordering::Relaxed) {
            return;
        }

        self.running.store(false, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);

        match self.backend.take() {
            Some(TimerBackend::Task(handle)) => {
                handle.abort();
            }
            Some(TimerBackend::ExecutorOwned(backend)) => {
                backend.scheduler.remove_timer(backend.timer_id);
            }
            #[cfg(target_os = "linux")]
            Some(TimerBackend::LinuxTimerfd(backend)) => {
                backend.scheduler.remove_timer(backend.timer_id);
            }
            #[cfg(not(target_os = "linux"))]
            Some(TimerBackend::HighPrecision(mut backend)) => {
                backend.control.notify();
                if let Some(thread) = backend.thread.take() {
                    let _ = thread.join();
                }
            }
            None => {}
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
#[path = "../test/timer_tests.rs"]
mod tests;
