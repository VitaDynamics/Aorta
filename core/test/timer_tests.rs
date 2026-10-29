use super::*;
use crate::event_queue::EventQueueSender;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

async fn wait_for_coalesced_ticks_above(timer: &Timer, baseline: u64, timeout: Duration) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let count = timer.coalesced_tick_count();
        if count > baseline {
            return count;
        }

        if tokio::time::Instant::now() >= deadline {
            return count;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn high_precision_wait_margins_scale_with_period() {
    let (sleep_5khz, yield_5khz) = high_precision_wait_margins(Duration::from_micros(200));
    let (sleep_1khz, yield_1khz) = high_precision_wait_margins(Duration::from_millis(1));

    assert_eq!(sleep_5khz, Duration::from_micros(25));
    assert!(yield_5khz <= Duration::from_micros(7));
    assert_eq!(sleep_1khz, Duration::from_micros(125));
    assert!(yield_1khz <= Duration::from_micros(25));
    assert!(sleep_5khz < sleep_1khz);
    assert!(yield_5khz < yield_1khz);
}

#[test]
fn high_precision_abs_sleep_window_caps_at_one_millisecond() {
    let short_period = Duration::from_micros(200);
    let long_period = Duration::from_millis(10);
    let (_, short_yield) = high_precision_wait_margins(short_period);
    let (_, long_yield) = high_precision_wait_margins(long_period);

    assert_eq!(
        high_precision_abs_sleep_window(short_period, short_yield),
        short_period
    );
    assert_eq!(
        high_precision_abs_sleep_window(long_period, long_yield),
        Duration::from_millis(1)
    );
}

#[test]
fn non_linux_wait_action_sleeps_through_the_mid_window_instead_of_spinning() {
    let yield_margin = Duration::from_micros(10);
    let sleep_margin = Duration::from_micros(100);

    // remaining <= yield_margin: fall through to the final precision spin.
    assert_eq!(
        non_linux_wait_action(Duration::from_micros(5), yield_margin, sleep_margin),
        NonLinuxWaitAction::PrecisionSpin
    );
    assert_eq!(
        non_linux_wait_action(yield_margin, yield_margin, sleep_margin),
        NonLinuxWaitAction::PrecisionSpin
    );

    // yield_margin < remaining <= sleep_margin: the sleep window. Must be a
    // real sleep down to the yield margin, NOT a busy spin.
    assert_eq!(
        non_linux_wait_action(Duration::from_micros(50), yield_margin, sleep_margin),
        NonLinuxWaitAction::Sleep(Duration::from_micros(40))
    );
    assert_eq!(
        non_linux_wait_action(sleep_margin, yield_margin, sleep_margin),
        NonLinuxWaitAction::Sleep(Duration::from_micros(90))
    );

    // remaining > sleep_margin: block on the condvar (wakeable by notify).
    assert_eq!(
        non_linux_wait_action(Duration::from_micros(200), yield_margin, sleep_margin),
        NonLinuxWaitAction::Condvar
    );
}

#[tokio::test]
async fn timer_invokes_callback_periodically() {
    let (tx, mut rx) = mpsc::channel(10);
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let _timer = Timer::new(
        Duration::from_millis(50),
        move || {
            *counter_clone.lock().unwrap() += 1;
        },
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    )
    .expect("create timer");

    // Wait for 3 timer events
    for _ in 0..3 {
        let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timeout")
            .expect("event");
        assert_eq!(event.kind(), EventKind::Timer);
        event.run();
    }

    let count = *counter.lock().unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn timer_pause_resume() {
    let (tx, mut rx) = mpsc::channel(10);
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let timer = Timer::new(
        Duration::from_millis(50),
        move || {
            *counter_clone.lock().unwrap() += 1;
        },
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    )
    .expect("create timer");

    // Wait for 1 tick
    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("timeout")
        .expect("event");
    event.run();
    assert_eq!(*counter.lock().unwrap(), 1);

    // Pause and wait
    timer.pause();
    assert!(!timer.is_running());
    tokio::time::sleep(Duration::from_millis(150)).await;

    // No new events should arrive
    assert_eq!(*counter.lock().unwrap(), 1);

    // Resume and wait for another tick
    timer.resume();
    assert!(timer.is_running());
    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("timeout")
        .expect("event");
    event.run();
    assert_eq!(*counter.lock().unwrap(), 2);
}

#[tokio::test]
async fn timer_stop_prevents_further_ticks() {
    let (tx, mut rx) = mpsc::channel(10);
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let mut timer = Timer::new(
        Duration::from_millis(50),
        move || {
            *counter_clone.lock().unwrap() += 1;
        },
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    )
    .expect("create timer");

    // Wait for 1 tick
    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("timeout")
        .expect("event");
    event.run();
    let observed_after_first = *counter.lock().unwrap();
    assert!(
        observed_after_first >= 1,
        "first dispatched timer event should execute at least one callback"
    );

    // Stop timer
    timer.stop();
    assert!(!timer.is_running());

    // Wait and verify no more events
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(rx.try_recv().is_err());
    assert_eq!(
        *counter.lock().unwrap(),
        observed_after_first,
        "stop should prevent additional callbacks after the first dispatched event completes"
    );
}

#[tokio::test]
async fn timer_auto_start_false() {
    let (tx, mut rx) = mpsc::channel(10);
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let options = TimerOptions::new().with_auto_start(false);
    let timer = Timer::new(
        Duration::from_millis(50),
        move || {
            *counter_clone.lock().unwrap() += 1;
        },
        EventQueueSender::from_bounded_channel(tx),
        options,
    )
    .expect("create timer");

    assert!(!timer.is_running());

    // Wait and verify no events
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(rx.try_recv().is_err());

    // Resume and verify it works
    timer.resume();
    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("timeout")
        .expect("event");
    event.run();
    assert!(
        *counter.lock().unwrap() >= 1,
        "timer should begin executing callbacks only after resume()"
    );
}

#[tokio::test]
async fn timer_rejects_zero_period() {
    let (tx, _rx) = mpsc::channel(10);
    let result = Timer::new(
        Duration::ZERO,
        || {},
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn timer_period_returns_correct_value() {
    let (tx, _rx) = mpsc::channel(10);
    let period = Duration::from_millis(123);
    let timer = Timer::new(
        period,
        || {},
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    )
    .expect("create timer");
    assert_eq!(timer.period(), period);
}

#[tokio::test]
async fn timer_coalesces_when_previous_tick_is_pending() {
    let (tx, mut rx) = mpsc::channel(1);
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let timer = Timer::new(
        Duration::from_millis(20),
        move || {
            *counter_clone.lock().unwrap() += 1;
        },
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    )
    .expect("create timer");

    let first = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("first timer timeout")
        .expect("first timer event");

    let baseline = timer.coalesced_tick_count();
    let coalesced = wait_for_coalesced_ticks_above(&timer, baseline, Duration::from_secs(1)).await;
    assert!(
        coalesced > baseline,
        "coalesced tick counter should increase when pending timer event is not drained"
    );

    first.run();
    assert!(
        *counter.lock().unwrap() >= 2,
        "running the pending timer event should service at least one coalesced firing inline"
    );
    assert!(
        rx.try_recv().is_err(),
        "coalesced timer should service the latest pending firing without growing queued backlog"
    );
}

#[tokio::test]
async fn timer_coalesce_continues_inline_after_callback_when_ticks_accumulated() {
    let (tx, mut rx) = mpsc::channel(1);
    let counter = Arc::new(Mutex::new(0));
    let counter_clone = Arc::clone(&counter);

    let timer = Timer::new(
        Duration::from_millis(40),
        move || {
            *counter_clone.lock().unwrap() += 1;
        },
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    )
    .expect("create timer");

    let first = tokio::time::timeout(Duration::from_millis(200), rx.recv())
        .await
        .expect("first timer timeout")
        .expect("first timer event");

    // Hold the first event without running it so later ticks accumulate behind it.
    let baseline = timer.coalesced_tick_count();
    let coalesced = wait_for_coalesced_ticks_above(&timer, baseline, Duration::from_secs(1)).await;
    assert!(
        coalesced > baseline,
        "coalesced tick counter should increase while callback execution is pending"
    );

    first.run();
    assert!(
        *counter.lock().unwrap() >= 2,
        "coalesced timer should run at least one additional callback inline before returning"
    );
    assert!(
        rx.try_recv().is_err(),
        "inline timer burst should not grow queued backlog when one extra firing is enough"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timer_coalesce_requeues_after_inline_burst_cap() {
    let (tx, mut rx) = mpsc::channel(1);
    let counter = Arc::new(Mutex::new(0usize));
    let counter_clone = Arc::clone(&counter);

    let _timer = Timer::new(
        Duration::from_millis(1),
        move || {
            *counter_clone.lock().unwrap() += 1;
            std::thread::sleep(Duration::from_millis(20));
        },
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::default(),
    )
    .expect("create timer");

    let first = tokio::time::timeout(Duration::from_millis(40), rx.recv())
        .await
        .expect("first timer timeout")
        .expect("first timer event");

    tokio::time::sleep(Duration::from_millis(10)).await;
    first.run();

    let observed_after_first = *counter.lock().unwrap();
    assert!(
        observed_after_first >= 2,
        "hot coalesced timer should execute more than one callback inline before returning"
    );
    assert!(
        observed_after_first <= COALESCE_INLINE_BURST_MAX,
        "inline burst should never exceed the configured cap before yielding back to the queue"
    );

    if observed_after_first == COALESCE_INLINE_BURST_MAX {
        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timer should requeue a replacement event after reaching inline burst cap")
            .expect("requeued timer event");
        second.run();
        assert!(
            *counter.lock().unwrap() > COALESCE_INLINE_BURST_MAX,
            "requeued event should continue servicing coalesced timer work"
        );
    }
}

#[test]
fn coalesce_post_callback_action_requeues_at_inline_burst_cap() {
    let state = CoalesceDispatchState::new();
    assert!(state.try_mark_pending());

    for inline_dispatches in 1..COALESCE_INLINE_BURST_MAX {
        assert!(!state.try_mark_pending());
        let action = state.post_callback_action(inline_dispatches);
        assert_eq!(
            action,
            CoalesceDispatchAction::ContinueInline,
            "coalesced dispatches below the burst cap should continue inline"
        );
    }

    assert!(!state.try_mark_pending());
    let action = state.post_callback_action(COALESCE_INLINE_BURST_MAX);
    assert_eq!(
        action,
        CoalesceDispatchAction::Requeue,
        "coalesced dispatches at the burst cap should yield by requeueing"
    );
    state.clear_pending();
}

#[test]
fn timer_block_overflow_preserves_invoke_on_missed_behavior() {
    assert_eq!(
        block_overflow_missed_tick_behavior(true),
        tokio::time::MissedTickBehavior::Delay
    );
    assert_eq!(
        block_overflow_missed_tick_behavior(false),
        tokio::time::MissedTickBehavior::Skip
    );
}

#[test]
fn compute_due_sample_preserves_invoke_on_missed_behavior() {
    let start = Instant::now();
    let period = Duration::from_millis(10);

    let mut next_deadline = start;
    let catch_up = compute_due_sample(
        start + Duration::from_millis(35),
        &mut next_deadline,
        period,
        true,
    );
    assert_eq!(catch_up.due_firings, 4);
    assert_eq!(next_deadline, start + Duration::from_millis(40));

    let mut next_deadline = start;
    let skip = compute_due_sample(
        start + Duration::from_millis(35),
        &mut next_deadline,
        period,
        false,
    );
    assert_eq!(skip.due_firings, 1);
    assert_eq!(next_deadline, start + Duration::from_millis(40));

    let start = StdInstant::now();
    let mut next_deadline = start;
    let catch_up = compute_due_sample_std(
        start + Duration::from_millis(35),
        &mut next_deadline,
        period,
        true,
    );
    assert_eq!(catch_up.due_firings, 4);
    assert_eq!(next_deadline, start + Duration::from_millis(40));

    let mut next_deadline = start;
    let skip = compute_due_sample_std(
        start + Duration::from_millis(35),
        &mut next_deadline,
        period,
        false,
    );
    assert_eq!(skip.due_firings, 1);
    assert_eq!(next_deadline, start + Duration::from_millis(40));
}

#[test]
fn compute_due_sample_boundary_cases() {
    let start = Instant::now();
    let period = Duration::from_millis(10);

    // Exactly due: one firing, deadline advances by one period.
    let mut nd = start;
    let s = compute_due_sample(start, &mut nd, period, true);
    assert_eq!(s.due_firings, 1);
    assert_eq!(nd, start + period);

    // Just before the second deadline: still only one firing.
    let mut nd = start;
    let s = compute_due_sample(
        start + period - Duration::from_nanos(1),
        &mut nd,
        period,
        true,
    );
    assert_eq!(s.due_firings, 1);
    assert!(nd > start + period - Duration::from_nanos(1));

    // Fractional overrun: 5.5 periods elapsed -> 6 firings due.
    let mut nd = start;
    let s = compute_due_sample(start + period * 5 + period / 2, &mut nd, period, true);
    assert_eq!(s.due_firings, 6);
    assert_eq!(nd, start + period * 6);

    // Post-condition: the advanced deadline is always strictly after `now`.
    let now = start + period * 7 + Duration::from_millis(3);
    let mut nd = start;
    compute_due_sample(now, &mut nd, period, false);
    assert!(nd > now, "next_deadline must land strictly after now");
}

#[test]
fn compute_due_sample_is_constant_time_for_huge_catch_up() {
    // A long stall relative to the period (here 2 billion periods)
    // must resolve in O(1). The previous loop ran once per missed period under
    // the global lock — this case would iterate ~2e9 times and effectively
    // hang. The arithmetic version returns immediately with the exact count.
    let start = Instant::now();
    let period = Duration::from_nanos(1);
    let missed: u64 = 2_000_000_000;
    let now = start + Duration::from_nanos(missed);

    let mut nd = start;
    let s = compute_due_sample(now, &mut nd, period, true);
    // Firings = missed periods + the one at `start` itself.
    assert_eq!(s.due_firings, missed + 1);
    assert!(nd > now);

    // invoke_on_missed = false coalesces to a single firing but still advances
    // past `now` in O(1).
    let mut nd = start;
    let s = compute_due_sample(now, &mut nd, period, false);
    assert_eq!(s.due_firings, 1);
    assert!(nd > now);
}

#[tokio::test]
async fn timer_block_overflow_counts_timeout_when_queue_full() {
    let (tx, _rx) = mpsc::channel(1);
    // Use 10ms period with 5ms explicit timeout. Because the effective timeout
    // is max(user_timeout, period*5, 2ms) = max(5ms, 50ms, 2ms) = 50ms, we
    // need to wait long enough for at least one timeout to occur.
    let timer = Timer::new(
        Duration::from_millis(10),
        || {},
        EventQueueSender::from_bounded_channel(tx),
        TimerOptions::new()
            .with_overflow_policy(TimerOverflowPolicy::Block)
            .with_overflow_block_timeout(Duration::from_millis(5)),
    )
    .expect("create timer");

    // Wait 300ms — enough for several ticks + at least one 50ms timeout.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(timer.coalesced_tick_count(), 0);
    assert!(
        timer.overflow_timeout_count() > 0,
        "block overflow should accumulate timeout count when queue stays full"
    );
}

// ---- The timerfd lane survives transient syscall
// errors (bounded backoff, no silent permanent death) and is respawned by a
// later registration if the thread ever exits. Linux-only: the timerfd lane
// does not exist on other targets.

#[cfg(target_os = "linux")]
#[test]
fn timer_lane_respawns_after_death() {
    use std::sync::atomic::Ordering;

    // A fresh scheduler spawns a live lane.
    let sched = LinuxTimerfdScheduler::new().expect("scheduler should start");
    assert!(sched.inner.alive.load(Ordering::Acquire));

    // Simulate the lane thread having exited (e.g. a panic); a subsequent
    // registration calls ensure_lane_alive, which must respawn it.
    sched.inner.alive.store(false, Ordering::Release);
    sched.ensure_lane_alive();
    assert!(
        sched.inner.alive.load(Ordering::Acquire),
        "ensure_lane_alive should respawn a dead lane"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn timer_lane_respawn_is_rate_gated() {
    use std::sync::atomic::Ordering;

    let sched = LinuxTimerfdScheduler::new().expect("scheduler should start");

    // First death + respawn records the respawn timestamp.
    sched.inner.alive.store(false, Ordering::Release);
    sched.ensure_lane_alive();
    assert!(sched.inner.alive.load(Ordering::Acquire));

    // A second death within LANE_RESPAWN_MIN_INTERVAL must be suppressed so a
    // persistently-dying lane cannot spawn a thread storm.
    sched.inner.alive.store(false, Ordering::Release);
    sched.ensure_lane_alive();
    assert!(
        !sched.inner.alive.load(Ordering::Acquire),
        "respawn within the gate interval should be suppressed"
    );
}

/// Registers a never-firing timer (very long period) directly on the timerfd
/// scheduler with zeroed metric counters, returning `(timer_id, running)`.
#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn register_timerfd_timer(
    sched: &LinuxTimerfdScheduler,
    period: Duration,
) -> (u64, Arc<AtomicBool>) {
    let (tx, _rx) = mpsc::channel(1);
    let running = Arc::new(AtomicBool::new(true));
    let id = sched
        .register_timer(
            period,
            Arc::new(|| {}),
            EventQueueSender::from_bounded_channel(tx),
            Arc::clone(&running),
            Arc::new(AtomicBool::new(false)), // stopped
            Arc::new(AtomicU64::new(0)),      // generation
            Arc::new(AtomicU64::new(0)),      // coalesced_tick_count
            Arc::new(AtomicU64::new(0)),      // deadline_slip_total_ns
            Arc::new(AtomicU64::new(0)),      // deadline_slip_max_ns
            Arc::new(AtomicU64::new(0)),      // due_firings_total
            Arc::new(AtomicU64::new(0)),      // due_firings_max
            Arc::new(AtomicU64::new(0)),      // due_sample_count
            Arc::new(AtomicU64::new(0)),      // coalesce_requeue_count
            Arc::new(AtomicU64::new(0)),      // coalesce_inline_dispatch_total
            Arc::new(AtomicU64::new(0)),      // coalesce_inline_dispatch_max
            Arc::new(AtomicU64::new(0)),      // coalesce_event_wait_total_ns
            Arc::new(AtomicU64::new(0)),      // coalesce_event_wait_max_ns
            Arc::new(AtomicU64::new(0)),      // coalesce_event_wait_sample_count
            false,                            // invoke_on_missed
            CallbackGroupType::MutuallyExclusive,
            None, // callback_group_handle
            None, // callback_trace_event
        )
        .expect("register timerfd timer");
    (id, running)
}

/// Pausing/resuming a long-period timer strands its old heap entry. As
/// long as another timer holds an earlier deadline at the top, those stale far-
/// future entries never bubble up to be lazily reclaimed, so the heap grows once
/// per resume. Periodic compaction must bound the heap to O(live timers).
///
/// Without `compact_heap_if_bloated`, 500 resume cycles leave ~500 stale entries
/// in the heap; with it, the heap stays bounded by the compaction threshold.
#[cfg(target_os = "linux")]
#[test]
fn timerfd_heap_stays_bounded_under_pause_resume_churn() {
    let sched = LinuxTimerfdScheduler::new().expect("scheduler should start");

    // Blocker B holds the earliest deadline so it always sits at the heap top;
    // next_active_deadline_locked returns it immediately and never pops the far-
    // future stale entries that target A leaves below it.
    let _blocker = register_timerfd_timer(&sched, Duration::from_secs(1_000));
    let (target, _running) = register_timerfd_timer(&sched, Duration::from_secs(3_600));

    for _ in 0..500 {
        sched.pause_timer(target);
        sched.resume_timer(target);
    }

    let heap_len = sched.inner.state.lock().expect("state lock").heap.len();
    let bound = 2 * HEAP_COMPACTION_FACTOR + HEAP_COMPACTION_SLACK;
    assert!(
        heap_len <= bound,
        "timerfd heap grew to {heap_len} entries (bound {bound}); stale pause/resume \
         debris is not being reclaimed"
    );
}

// ---- Executor-owned timer path: panic wedge / drift ----

fn fresh_executor_scheduler() -> ExecutorTimerScheduler {
    ExecutorTimerScheduler::new(Arc::new(Notify::new()), None)
}

/// Register a timer on `sched` with default options and zeroed metric
/// counters, mirroring the production registration surface. Returns the
/// timer id; the caller keeps the running/stopped/generation handles.
#[allow(clippy::too_many_arguments)]
fn register_executor_timer<F>(
    sched: &ExecutorTimerScheduler,
    period: Duration,
    running: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    callback: F,
) -> u64
where
    F: Fn() + Send + Sync + 'static,
{
    sched
        .register_timer(
            period,
            callback,
            &TimerOptions::default(),
            running,
            stopped,
            generation,
            Arc::new(AtomicU64::new(0)), // coalesced_tick_count
            Arc::new(AtomicU64::new(0)), // deadline_slip_total_ns
            Arc::new(AtomicU64::new(0)), // deadline_slip_max_ns
            Arc::new(AtomicU64::new(0)), // due_firings_total
            Arc::new(AtomicU64::new(0)), // due_firings_max
            Arc::new(AtomicU64::new(0)), // due_sample_count
            Arc::new(AtomicU64::new(0)), // coalesce_requeue_count
            Arc::new(AtomicU64::new(0)), // coalesce_inline_dispatch_total
            Arc::new(AtomicU64::new(0)), // coalesce_inline_dispatch_max
            Arc::new(AtomicU64::new(0)), // coalesce_event_wait_total_ns
            Arc::new(AtomicU64::new(0)), // coalesce_event_wait_max_ns
            Arc::new(AtomicU64::new(0)), // coalesce_event_wait_sample_count
        )
        .expect("register executor timer")
}

#[tokio::test]
async fn executor_owned_timer_survives_panicking_callback() {
    // A panicking callback must not permanently wedge the timer. The
    // event's drop guard runs `on_callback_complete` even on unwind, so
    // `pending_event` is reset and the timer fires again next period. Without
    // the guard the unwind skips completion, `pending_event` stays true, and
    // the second collect coalesces forever (emits nothing).
    let sched = fresh_executor_scheduler();
    let running = Arc::new(AtomicBool::new(true));
    let stopped = Arc::new(AtomicBool::new(false));
    let generation = Arc::new(AtomicU64::new(0));
    let _id = register_executor_timer(
        &sched,
        Duration::from_millis(20),
        running,
        stopped,
        generation,
        || panic!("boom from executor-owned timer callback"),
    );

    tokio::time::sleep(Duration::from_millis(35)).await;
    let events = sched.collect_due_events();
    assert_eq!(events.len(), 1, "timer should be due exactly once");
    for ev in events {
        ev.run(); // panics inside; isolated by Event::run, guard resets state
    }

    tokio::time::sleep(Duration::from_millis(35)).await;
    let events2 = sched.collect_due_events();
    assert_eq!(
        events2.len(),
        1,
        "a panicking callback must not wedge the timer (pending_event must reset)"
    );
}

#[tokio::test]
async fn executor_owned_rearm_preserves_period_aligned_deadline() {
    // When a tick coalesces (becomes due again while an event is
    // still pending), completing the callback must NOT rebase `next_deadline`
    // to `Instant::now()` — that anchors the cadence to execution time and
    // drifts. `collect_due_events` already advanced it to the period-aligned
    // value; the rearm must preserve it exactly.
    let sched = fresh_executor_scheduler();
    let running = Arc::new(AtomicBool::new(true));
    let stopped = Arc::new(AtomicBool::new(false));
    let generation = Arc::new(AtomicU64::new(0));
    let _id = register_executor_timer(
        &sched,
        Duration::from_millis(50),
        running,
        stopped,
        generation,
        || {},
    );

    // First collect: timer due -> pending, next_deadline advanced to aligned.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let events = sched.collect_due_events();
    assert_eq!(events.len(), 1);

    // Second collect after another period: due again while still pending ->
    // sets rearm_requested and advances next_deadline to the aligned value.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let coalesced = sched.collect_due_events();
    assert!(
        coalesced.is_empty(),
        "a coalesced tick while pending must not emit a second event"
    );

    // The aligned deadline captured here must be preserved across completion.
    let aligned = sched.next_deadline().expect("an active deadline");
    for ev in events {
        ev.run(); // empty callback -> on_callback_complete takes the rearm branch
    }
    let after = sched.next_deadline().expect("an active deadline");
    assert_eq!(
        after, aligned,
        "rearm must keep the period-aligned deadline, not rebase to completion time"
    );
}

#[tokio::test]
async fn executor_owned_coalesced_rearm_dispatches_promptly() {
    // CoalesceLatest contract: when a firing is coalesced because the
    // prior event is still in flight, completing that event must make the
    // latest pending firing runnable PROMPTLY — a collect right after
    // completion emits a catch-up event even though `next_deadline` is in the
    // future. Keeping the deadline aligned (no drift) must NOT swallow the
    // coalesced firing until the next period.
    let sched = fresh_executor_scheduler();
    let running = Arc::new(AtomicBool::new(true));
    let stopped = Arc::new(AtomicBool::new(false));
    let generation = Arc::new(AtomicU64::new(0));
    let _id = register_executor_timer(
        &sched,
        Duration::from_millis(50),
        running,
        stopped,
        generation,
        || {},
    );

    // First firing -> pending.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let first = sched.collect_due_events();
    assert_eq!(first.len(), 1);

    // Next period elapses while still pending -> coalesced rearm (no event).
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        sched.collect_due_events().is_empty(),
        "a coalesced tick while pending must not emit a second event"
    );

    // Complete the in-flight event. The deadline stays in the future...
    for ev in first {
        ev.run();
    }
    let deadline = sched.next_deadline().expect("aligned deadline");
    assert!(
        deadline > Instant::now(),
        "subsequent cadence must stay period-aligned (deadline in the future)"
    );

    // ...yet the coalesced firing is dispatched immediately on the next collect.
    let catch_up = sched.collect_due_events();
    assert_eq!(
        catch_up.len(),
        1,
        "the coalesced latest firing must be dispatched promptly after completion, \
not deferred to the next aligned deadline"
    );
}
