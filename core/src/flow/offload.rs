//! `offload()` — the one sanctioned blocking sink.
//!
//! Moves a CPU-heavy segment or a genuinely-blocking call (one with no
//! async form) off the executor/runtime workers onto a process-shared,
//! lazy, bounded thread pool, and resumes the caller when it completes.
//!
//! - **process-shared**: one pool serves all offloaded work.
//! - **lazy**: a process that never calls `offload()` spawns zero threads;
//!   workers retire after an idle timeout, so a burst does not pin `~ncpu`
//!   threads forever on an embedded SoC.
//! - **bounded**: at most `available_parallelism()` workers (env
//!   `AORTA_OFFLOAD_THREADS` overrides). The design deliberately trades away
//!   elastic blocking capacity — the JOB QUEUE is unbounded (a
//!   resumption of admitted work must not be droppable), and saturation is
//!   made observable instead: a warning fires when a job waited longer
//!   than [`STARVATION_WARN`] for a worker (diagnose-only).
//! - **distinct from the executor and from `spawn_blocking`**: no user
//!   blocking segment rides a raw `spawn_blocking`.
//!
//! Concurrency: every pool transition (enqueue, pickup, idle mark, retire)
//! happens under ONE mutex + condvar, so the retire decision atomically
//! re-checks the queue — a retiring worker can never strand a job that a
//! concurrent submitter counted on it to take.
//!
//! A panic inside the offloaded closure resumes as a panic in the awaiting
//! task (which fail-fast then handles like any child panic).

use std::collections::VecDeque;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Workers retire after this long idle (re-spawned lazily on demand;
/// the LAST worker never retires once the pool has been used).
#[cfg(not(test))]
const IDLE_RETIRE: Duration = Duration::from_secs(30);
#[cfg(test)]
const IDLE_RETIRE: Duration = Duration::from_millis(100);

/// A job older than this without a worker logs a saturation warning.
const STARVATION_WARN: Duration = Duration::from_secs(1);

/// Priority wiring: five per-class FIFO queues + the same
/// WRR pop discipline the executor's PriorityDispatchState proves out —
/// higher classes get more pops per round but every class progresses (no
/// starvation by construction). Weights mirror the executor defaults.
const OFFLOAD_CLASSES: usize = 5;
// ONE source of truth with the executor's WRR (the two
// tables had already been copy-drifted once).
const OFFLOAD_WEIGHTS: [u8; OFFLOAD_CLASSES] = crate::executor::DEFAULT_PRIORITY_WEIGHTS;

struct PoolState {
    queues: [VecDeque<(Instant, Job)>; OFFLOAD_CLASSES],
    /// WRR round state: remaining pops per class this round + cursor.
    remaining: [u8; OFFLOAD_CLASSES],
    cursor: usize,
    queued_total: usize,
    workers: usize,
    idle: usize,
    /// Sticky flag so the starvation warning logs once per saturation
    /// episode instead of per job.
    warned_saturated: bool,
}

impl PoolState {
    fn push(&mut self, class: usize, job: (Instant, Job)) {
        self.queues[class.min(OFFLOAD_CLASSES - 1)].push_back(job);
        self.queued_total += 1;
    }

    /// WRR pop: identical discipline to the executor's priority buckets.
    fn pop(&mut self) -> Option<(Instant, Job)> {
        if self.queued_total == 0 {
            return None;
        }
        for _ in 0..(OFFLOAD_CLASSES * 2) {
            let idx = self.cursor;
            if self.remaining[idx] == 0 || self.queues[idx].is_empty() {
                // Advance past exhausted-budget or empty classes without
                // burning the round budget (the old nested
                // form carried two identical arms — a dead branch).
                self.cursor = (self.cursor + 1) % OFFLOAD_CLASSES;
                continue;
            }
            self.remaining[idx] -= 1;
            self.queued_total -= 1;
            return self.queues[idx].pop_front();
        }
        // Round exhausted: refill weights and take the highest non-empty.
        self.remaining = OFFLOAD_WEIGHTS;
        for idx in 0..OFFLOAD_CLASSES {
            if !self.queues[idx].is_empty() {
                self.remaining[idx] = self.remaining[idx].saturating_sub(1);
                self.queued_total -= 1;
                self.cursor = (idx + 1) % OFFLOAD_CLASSES;
                return self.queues[idx].pop_front();
            }
        }
        None
    }

    fn queue_len(&self) -> usize {
        self.queued_total
    }
}

struct OffloadPool {
    state: Mutex<PoolState>,
    available: Condvar,
    cap: usize,
}

fn pool() -> &'static OffloadPool {
    static POOL: OnceLock<OffloadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let cap = crate::runtime_env::var("AORTA_OFFLOAD_THREADS")
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(std::num::NonZeroUsize::get)
                    .unwrap_or(4)
            });
        OffloadPool {
            state: Mutex::new(PoolState {
                queues: std::array::from_fn(|_| VecDeque::new()),
                remaining: OFFLOAD_WEIGHTS,
                cursor: 0,
                queued_total: 0,
                workers: 0,
                idle: 0,
                warned_saturated: false,
            }),
            available: Condvar::new(),
            cap,
        }
    })
}

fn submit(job: Job) {
    submit_prio(job, crate::event_queue::EventPriority::Normal)
}

#[cfg(test)]
thread_local! {
    /// Test seam: the last priority THIS THREAD submitted with (the
    /// wiring assertion for offload_in; ordering is covered by the WRR
    /// unit test — completion order across the global pool is fair, not
    /// strict, so an e2e ordering assert would be inherently racy).
    pub(crate) static LAST_SUBMITTED_PRIORITY:
        std::cell::Cell<Option<crate::event_queue::EventPriority>> =
        const { std::cell::Cell::new(None) };
}

fn submit_prio(job: Job, priority: crate::event_queue::EventPriority) {
    #[cfg(test)]
    LAST_SUBMITTED_PRIORITY.with(|c| c.set(Some(priority)));
    // The offload pool keeps the FULL 5-bucket index vocabulary (canonical
    // `priority_index`) — this is NOT the 5→4 lane fold.
    let class = usize::from(crate::event_queue::priority_index(priority));
    let pool = pool();
    let spawn_worker = {
        let mut state = pool.state.lock().unwrap_or_else(|p| p.into_inner());
        state.push(class, (Instant::now(), job));
        // Spawn when no idle worker will take this job and the bound allows.
        let spawn = state.idle == 0 && state.workers < pool.cap;
        if spawn {
            state.workers += 1;
        }
        spawn
    };
    pool.available.notify_one();
    if spawn_worker {
        if let Err(err) = std::thread::Builder::new()
            .name("aorta-offload".to_string())
            .spawn(worker_loop)
        {
            let (queued, workers) = {
                let mut state = pool.state.lock().unwrap_or_else(|p| p.into_inner());
                state.workers -= 1;
                (state.queue_len(), state.workers)
            };
            // WARN outside the lock (log I/O under the pool mutex
            // stalls concurrent submits).
            crate::aorta_core_warn!(
                "Aorta WARN: failed to spawn offload worker ({}); {} job(s) queued with {} worker(s)",
                err,
                queued,
                workers
            );
            // With ZERO live workers the admitted
            // jobs would strand FOREVER (the awaiting oneshot never
            // resolves; an action execute callback wedges its goal). The
            // module contract says undroppable — honor it by running the
            // stranded queue inline on this thread. Degraded latency in a
            // pthread-exhaustion failure mode beats a permanent hang; if a
            // worker appears mid-drain, hand back over.
            drain_inline_while_workerless(pool);
        }
    }
}

/// Fallback: run queued jobs on the CURRENT thread for as long as
/// the pool has no live workers. One job per lock acquisition; stops the
/// moment a worker exists (that worker owns the queue again).
fn drain_inline_while_workerless(pool: &OffloadPool) {
    loop {
        let job = {
            let mut state = pool.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.workers > 0 || state.queue_len() == 0 {
                return;
            }
            state.pop()
        };
        match job {
            Some((_at, job)) => job(),
            None => return,
        }
    }
}

fn worker_loop() {
    let pool = pool();
    let mut state = pool.state.lock().unwrap_or_else(|p| p.into_inner());
    loop {
        if let Some((enqueued_at, job)) = state.pop() {
            // Saturation WARN decision under the lock, log AFTER release
            // (log I/O under the pool mutex stalls every submit).
            let waited = enqueued_at.elapsed();
            let mut warn_saturated = None;
            if waited > STARVATION_WARN && !state.warned_saturated {
                state.warned_saturated = true;
                warn_saturated = Some(state.workers);
            } else if waited <= STARVATION_WARN {
                state.warned_saturated = false;
            }
            drop(state);
            if let Some(workers) = warn_saturated {
                crate::aorta_core_warn!(
                    "Aorta WARN: offload pool saturated — a blocking job waited {:?} \
                     for a worker (cap {}, live {}); long-blocking segments may need \
                     AORTA_OFFLOAD_THREADS raised",
                    waited,
                    pool.cap,
                    workers
                );
            }
            job();
            state = pool.state.lock().unwrap_or_else(|p| p.into_inner());
            continue;
        }
        // Queue empty: wait for work, retire on idle timeout. The retire
        // decision re-checks the queue UNDER THE SAME LOCK, so a submit that
        // observed `idle > 0` (and skipped spawning) either lands before the
        // re-check (we take the job) or after we decremented `idle` (it sees
        // idle == 0 and spawns a replacement) — no stranded job.
        state.idle += 1;
        let (next, timeout) = pool
            .available
            .wait_timeout(state, IDLE_RETIRE)
            .unwrap_or_else(|p| p.into_inner());
        state = next;
        state.idle -= 1;
        if timeout.timed_out() && state.queue_len() == 0 && state.workers > 1 {
            // Retire down to ONE resident worker,
            // never zero — a once-a-minute offload user otherwise pays a
            // cold thread spawn (on the submitting executor thread) every
            // single call after the 30s retire.
            state.workers -= 1;
            return;
        }
    }
}

/// [`offload`] gated on a scope (`offload` alone looked like a safe
/// unbounded escape hatch with no cancel path): refuses dispatch when the
/// scope is already cancelled/expired, returning the checkpoint's own
/// `Stopped::cancelled(reason)`. NOTE the pool never kills a RUNNING
/// closure — poll `scope.is_cancelled()` inside long jobs. Queued work is
/// still unbounded (accepted residual: revisit a cap if offload becomes contended).
pub async fn offload_in<R, F>(scope: &crate::flow::Scope, f: F) -> crate::flow::Outcome<R>
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    scope.checkpoint()?;
    // The job queues at the SCOPE's priority class (WRR-ordered).
    Ok(offload_with_priority(f, scope.priority()).await)
}

/// Run `f` on the offload pool; resume here with its result. A panic in
/// `f` resumes as a panic here.
///
/// Cancellation: `offload` itself has NO cancel path — the
/// pool never kills a queued or running closure, and a scope cancel does
/// not reach it. Use [`offload_in`] to at least refuse dispatch on a
/// cancelled scope, and poll `scope.is_cancelled()` inside long closures.
/// Requires an ambient tokio runtime to resume on.
pub async fn offload<R, F>(f: F) -> R
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    offload_with_priority(f, crate::event_queue::EventPriority::Normal).await
}

/// [`offload`] at an explicit priority class (priority wiring: `offload_in`
/// submits at the scope's priority; the pool orders classes by WRR).
pub async fn offload_with_priority<R, F>(f: F, priority: crate::event_queue::EventPriority) -> R
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    submit_prio(
        Box::new(move || {
            let outcome = catch_unwind(AssertUnwindSafe(f));
            // A dropped receiver (awaiting task aborted) makes delivery moot.
            let _ = tx.send(outcome);
        }),
        priority,
    );
    match rx.await {
        Ok(Ok(value)) => value,
        Ok(Err(panic)) => resume_unwind(panic),
        Err(_) => unreachable!("offload worker dropped the result channel without sending"),
    }
}

/// Run `f` on a freshly spawned OS thread named `name`; resume here with
/// its result. A panic in `f` resumes as a panic here.
///
/// For blocking work whose duration is set by an external protocol rather
/// than by CPU — a raw action `execute` handler may legally block until its
/// goal reaches a terminal state. Such work must not sit on the shared,
/// CPU-sized pool: one long job per worker starves every other offloaded
/// job behind it. The caller bounds concurrency (an action server admits
/// at most `max_concurrency` goals). If the thread cannot be spawned the
/// job falls back to the shared pool with a warning — degraded, never
/// stranded.
pub async fn offload_dedicated<R, F>(name: &str, f: F) -> R
where
    R: Send + 'static,
    F: FnOnce() -> R + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    let job: Job = Box::new(move || {
        let outcome = catch_unwind(AssertUnwindSafe(f));
        // A dropped receiver (awaiting task aborted) makes delivery moot.
        let _ = tx.send(outcome);
    });
    // The spawned closure only borrows the job through this slot, so a
    // failed spawn (which drops the closure) leaves it here for the
    // fallback instead of losing it.
    let slot = std::sync::Arc::new(Mutex::new(Some(job)));
    let thread_slot = std::sync::Arc::clone(&slot);
    let spawned = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let job = thread_slot.lock().unwrap_or_else(|p| p.into_inner()).take();
            if let Some(job) = job {
                job();
            }
        });
    if let Err(err) = spawned {
        let job = slot.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(job) = job {
            crate::aorta_core_warn!(
                "Aorta WARN: failed to spawn dedicated thread '{}' ({}); \
                 running the job on the shared offload pool",
                name,
                err
            );
            submit(job);
        }
    }
    match rx.await {
        Ok(Ok(value)) => value,
        Ok(Err(panic)) => resume_unwind(panic),
        Err(_) => {
            unreachable!("dedicated offload thread dropped the result channel without sending")
        }
    }
}

/// Upper bound on shared-pool workers (`AORTA_OFFLOAD_THREADS`, else the
/// available parallelism).
pub fn capacity() -> usize {
    pool().cap
}

/// Observability: current (live, idle) worker counts (tests + telemetry).
pub fn worker_counts() -> (usize, usize) {
    let state = pool().state.lock().unwrap_or_else(|p| p.into_inner());
    (state.workers, state.idle)
}

/// Test probe: live worker count.
#[cfg(test)]
pub(crate) fn debug_worker_count() -> usize {
    pool()
        .state
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .workers
}

/// Observability: current queued-job depth.
pub fn queue_depth() -> usize {
    pool()
        .state
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .queue_len()
}

#[cfg(test)]
mod wrr_tests {
    use super::*;

    #[test]
    fn wrr_orders_classes_and_never_starves() {
        // Higher classes pop more per round; every class
        // progresses (WRR, not strict priority).
        let mut state = PoolState {
            queues: std::array::from_fn(|_| VecDeque::new()),
            remaining: OFFLOAD_WEIGHTS,
            cursor: 0,
            queued_total: 0,
            workers: 0,
            idle: 0,
            warned_saturated: false,
        };
        // 4 jobs each in Realtime(0), Normal(2), Background(4).
        for class in [0usize, 2, 4] {
            for _ in 0..4 {
                state.push(class, (Instant::now(), Box::new(move || {})));
            }
        }
        // Drain and record class order by probing queue lengths.
        let mut order = Vec::new();
        while state.queue_len() > 0 {
            let before: Vec<usize> = state.queues.iter().map(|q| q.len()).collect();
            let (_at, job) = state.pop().expect("job");
            job(); // consume: closures are lazy
            let after: Vec<usize> = state.queues.iter().map(|q| q.len()).collect();
            let class = (0..OFFLOAD_CLASSES)
                .find(|&i| before[i] != after[i])
                .expect("class");
            order.push(class);
        }
        assert_eq!(order.len(), 12);
        // Realtime finishes its 4 within the first 6 pops (weight 8 vs 2/1),
        // and Background still gets service before Realtime's round budget
        // could exhaust twice — i.e., appears somewhere in the sequence.
        let rt_done_at = order.iter().rposition(|&c| c == 0).expect("rt");
        assert!(rt_done_at < 6, "realtime front-loaded: {order:?}");
        assert!(order.contains(&4), "background never starved: {order:?}");
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    #[test]
    fn workerless_inline_drain_runs_stranded_jobs() {
        // An admitted job with zero live workers must still run
        // (inline fallback), never strand the awaiting oneshot.
        let local = OffloadPool {
            state: Mutex::new(PoolState {
                queues: std::array::from_fn(|_| VecDeque::new()),
                remaining: OFFLOAD_WEIGHTS,
                cursor: 0,
                queued_total: 0,
                workers: 0,
                idle: 0,
                warned_saturated: false,
            }),
            available: Condvar::new(),
            cap: 4,
        };
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let mut state = local.state.lock().unwrap();
            for _ in 0..3 {
                let ran = std::sync::Arc::clone(&ran);
                state.push(
                    2,
                    (
                        Instant::now(),
                        Box::new(move || {
                            ran.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }),
                    ),
                );
            }
        }
        drain_inline_while_workerless(&local);
        assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(
            local.state.lock().unwrap().queue_len(),
            0,
            "queue fully drained"
        );
    }

    #[test]
    fn inline_drain_defers_to_a_live_worker() {
        // The fallback hands over the moment a worker exists.
        let local = OffloadPool {
            state: Mutex::new(PoolState {
                queues: std::array::from_fn(|_| VecDeque::new()),
                remaining: OFFLOAD_WEIGHTS,
                cursor: 0,
                queued_total: 0,
                workers: 1, // a live worker owns the queue
                idle: 0,
                warned_saturated: false,
            }),
            available: Condvar::new(),
            cap: 4,
        };
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        {
            let mut state = local.state.lock().unwrap();
            let probe = std::sync::Arc::clone(&ran);
            state.push(
                2,
                (
                    Instant::now(),
                    Box::new(move || {
                        probe.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }),
                ),
            );
        }
        drain_inline_while_workerless(&local);
        assert_eq!(
            ran.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "with a live worker the fallback must not steal the queue"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn last_worker_survives_idle_retire() {
        // After use + idle > IDLE_RETIRE(test: 100ms), exactly one
        // resident worker remains — periodic users stop paying cold spawns.
        offload(|| ()).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(debug_worker_count(), 1, "the last worker must never retire");
        // And it still serves the next job without a fresh spawn.
        let v = offload(|| 21 * 2).await;
        assert_eq!(v, 42);
    }
}

#[cfg(test)]
mod dedicated_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedicated_runs_on_a_named_thread() {
        let name = offload_dedicated("aorta-dedicated-test", || {
            std::thread::current().name().map(str::to_string)
        })
        .await;
        assert_eq!(name.as_deref(), Some("aorta-dedicated-test"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedicated_resumes_a_panic_in_the_caller() {
        let outcome = tokio::spawn(offload_dedicated("aorta-dedicated-test", || -> () {
            panic!("boom")
        }))
        .await;
        assert!(outcome.unwrap_err().is_panic());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dedicated_jobs_are_not_bounded_by_the_pool() {
        // More simultaneously blocked jobs than the pool has workers: every
        // one must be running at once.
        let jobs = capacity() + 1;
        let entered = Arc::new(AtomicUsize::new(0));
        let runs: Vec<_> = (0..jobs)
            .map(|_| {
                let entered = Arc::clone(&entered);
                tokio::spawn(offload_dedicated("aorta-dedicated-test", move || {
                    entered.fetch_add(1, Ordering::SeqCst);
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while entered.load(Ordering::SeqCst) < jobs && Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    entered.load(Ordering::SeqCst) >= jobs
                }))
            })
            .collect();
        for run in runs {
            assert!(run.await.unwrap(), "a dedicated job was queued");
        }
    }
}
