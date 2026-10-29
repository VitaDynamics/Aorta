use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::runtime::{Handle as TokioHandle, RuntimeFlavor};
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::event_queue::{
    lane_class_for_priority, AdmissionStats, AdmissionStatsReader, CallbackGroupHandle,
    CallbackGroupType, Event, EventKind, EventPriority, EventQueue, EventQueueOptions,
    EventQueueWaker,
};
use crate::log_prelude::*;
use crate::node::Node;
use crate::runtime_env;
use crate::service::RpcOverflowPolicy;
use crate::subscriber::SubscriberOverflowPolicy;
use crate::timer::{ExecutorTimerScheduler, TimerOverflowPolicy};

const PRIORITY_LEVELS: usize = 5;
const DEFAULT_MAX_DRAIN_PER_CYCLE: u32 = 32;
pub(crate) const DEFAULT_PRIORITY_WEIGHTS: [u8; PRIORITY_LEVELS] = [8, 4, 2, 1, 1];
const DEFAULT_MULTI_WORKERS: usize = 4;
const DEFAULT_GROUP_MUTUALLY_EXCLUSIVE_HANDLE: CallbackGroupHandle = 1;
const DEFAULT_GROUP_REENTRANT_HANDLE: CallbackGroupHandle = 2;
const ENV_MULTI_INGRESS_ENABLE: &str = "AORTA_EXECUTOR_MULTI_INGRESS";
const ENV_MULTI_INGRESS_CAPACITY: &str = "AORTA_EXECUTOR_MULTI_INGRESS_CAPACITY";
const ENV_MAX_QUEUED_EVENTS: &str = "AORTA_EXECUTOR_MAX_QUEUED_EVENTS";
const ENV_MAX_QUEUED_BYTES: &str = "AORTA_EXECUTOR_MAX_QUEUED_BYTES";
const ENV_ADMISSION_OBSERVABILITY: &str = "AORTA_EXECUTOR_ADMISSION_OBSERVABILITY";
#[cfg(target_os = "linux")]
const ENV_CROSSBEAM_DISABLE: &str = "AORTA_EXECUTOR_CROSSBEAM_DISABLE";
#[cfg(target_os = "linux")]
const ENV_CROSSBEAM_ENABLE: &str = "AORTA_EXECUTOR_CROSSBEAM_ENABLE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpinOutcome {
    Event,
    Timeout,
    Stopped,
    QueueClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerMode {
    Fifo,
    Priority,
}

#[derive(Debug, Clone, Copy)]
pub struct PrioritySchedulerOptions {
    pub max_drain_per_cycle: u32,
    pub weights: [u8; PRIORITY_LEVELS],
    pub map_subscription_qos: bool,
}

impl Default for PrioritySchedulerOptions {
    fn default() -> Self {
        Self {
            max_drain_per_cycle: DEFAULT_MAX_DRAIN_PER_CYCLE,
            weights: DEFAULT_PRIORITY_WEIGHTS,
            map_subscription_qos: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutorOptions {
    pub mode: SchedulerMode,
    pub priority: PrioritySchedulerOptions,
    pub admission: ExecutorAdmissionOptions,
    pub class_policy: ExecutorClassPolicyOptions,
    /// Force the tokio mpsc queue backend, skipping the crossbeam+eventfd path.
    /// Required when the executor runs inside a `current_thread` tokio runtime
    /// (e.g. the SDK auto-executor) because `block_in_place` is not available.
    pub force_tokio_queue: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExecutorAdmissionOptions {
    pub max_queued_events: usize,
    pub max_queued_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutorClassPolicyOptions {
    pub subscriber_overflow_policy: SubscriberOverflowPolicy,
    pub subscriber_overflow_relay_capacity: usize,
    pub rpc_overflow_policy: RpcOverflowPolicy,
    pub rpc_overflow_block_timeout: Duration,
    pub timer_overflow_policy: TimerOverflowPolicy,
    pub timer_overflow_block_timeout: Duration,
}

impl Default for ExecutorClassPolicyOptions {
    fn default() -> Self {
        Self {
            subscriber_overflow_policy: SubscriberOverflowPolicy::Drop,
            subscriber_overflow_relay_capacity: 1024,
            rpc_overflow_policy: RpcOverflowPolicy::FastFailBusy,
            rpc_overflow_block_timeout: Duration::from_millis(0),
            timer_overflow_policy: TimerOverflowPolicy::CoalesceLatest,
            timer_overflow_block_timeout: Duration::from_millis(0),
        }
    }
}

impl ExecutorClassPolicyOptions {
    /// Apply executor class-policy defaults to subscriber options when the
    /// subscriber remains at legacy SDK defaults.
    pub fn apply_subscriber_overflow_compat(
        &self,
        options: &mut crate::subscriber::SubscriberOptions,
    ) {
        if options.overflow_policy == SubscriberOverflowPolicy::Drop
            && options.overflow_relay_capacity == 1024
        {
            options.overflow_policy = self.subscriber_overflow_policy;
            if options.overflow_policy == SubscriberOverflowPolicy::Relay {
                options.overflow_relay_capacity = self.subscriber_overflow_relay_capacity.max(1);
            }
            return;
        }

        if options.overflow_policy == SubscriberOverflowPolicy::Relay {
            options.overflow_relay_capacity = options.overflow_relay_capacity.max(1);
        }
    }

    /// Resolve RPC overflow settings with compatibility mapping:
    /// - legacy "timeout only" (`FastFailBusy` + timeout>0) maps to `Block(timeout)`.
    /// - untouched defaults inherit executor class-policy defaults.
    pub fn resolve_rpc_overflow_compat(
        &self,
        policy: RpcOverflowPolicy,
        timeout: Duration,
    ) -> (RpcOverflowPolicy, Duration) {
        if policy == RpcOverflowPolicy::FastFailBusy {
            if !timeout.is_zero() {
                return (RpcOverflowPolicy::Block, timeout);
            }
            return (self.rpc_overflow_policy, self.rpc_overflow_block_timeout);
        }
        (policy, timeout)
    }

    /// Resolve timer overflow settings with compatibility mapping:
    /// - legacy "timeout only" (`CoalesceLatest` + timeout>0) maps to `Block(timeout)`.
    /// - untouched defaults inherit executor class-policy defaults.
    pub fn resolve_timer_overflow_compat(
        &self,
        policy: TimerOverflowPolicy,
        timeout: Duration,
    ) -> (TimerOverflowPolicy, Duration) {
        if policy == TimerOverflowPolicy::CoalesceLatest {
            if !timeout.is_zero() {
                return (TimerOverflowPolicy::Block, timeout);
            }
            return (
                self.timer_overflow_policy,
                self.timer_overflow_block_timeout,
            );
        }
        (policy, timeout)
    }
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            mode: SchedulerMode::Fifo,
            priority: PrioritySchedulerOptions::default(),
            admission: ExecutorAdmissionOptions::default(),
            class_policy: ExecutorClassPolicyOptions::default(),
            force_tokio_queue: false,
        }
    }
}

/// Registry of DELAYED resumptions (`post_resume_after`):
/// a sleeping post is not yet in the reserved lane, so executor stop must
/// find it here and resume it into cancellation PROMPTLY (never waiting
/// out the delay, never leaking the suspended frame). Map-removal is the
/// exactly-once arbiter between the wake path and the stop path.
pub struct DelayedResumes {
    next_id: AtomicU64,
    map: parking_lot::Mutex<DelayedMap>,
}

/// Map + closed flag under ONE mutex: `closed` is
/// set inside `drain_cancel`'s critical section, so an `insert` racing a
/// stop either lands before the drain (drained promptly) or observes
/// `closed` and hands the fire closure straight back for an immediate
/// inline-cancelled delivery — a post can never sleep out its full delay
/// during teardown. Mirrors the ReservedLane's lock-coupled close-vs-push.
struct DelayedMap {
    closed: bool,
    entries: std::collections::HashMap<u64, DelayedEntry>,
}

pub(crate) struct DelayedEntry {
    /// `fire(cancelled)`: on the wake path (`false`) posts into the lane
    /// (falling back to an inline cancelled resume if refused); on the
    /// stop path (`true`) resumes inline with the cancelled status.
    fire: Box<dyn FnOnce(bool) + Send>,
    abort: parking_lot::Mutex<Option<tokio::task::AbortHandle>>,
}

impl DelayedResumes {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicU64::new(1),
            map: parking_lot::Mutex::new(DelayedMap {
                closed: false,
                entries: std::collections::HashMap::new(),
            }),
        })
    }

    /// Register `fire` BEFORE spawning the sleeper (a zero-delay task must
    /// find itself in the map). `Err(fire)` when the registry is already
    /// closed (stop drained it): the caller must deliver the inline
    /// cancelled resume itself, immediately — never spawn the sleeper.
    pub(crate) fn insert(
        &self,
        fire: Box<dyn FnOnce(bool) + Send>,
    ) -> Result<u64, Box<dyn FnOnce(bool) + Send>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut map = self.map.lock();
        if map.closed {
            return Err(fire);
        }
        map.entries.insert(
            id,
            DelayedEntry {
                fire,
                abort: parking_lot::Mutex::new(None),
            },
        );
        Ok(id)
    }

    /// Returns false when the entry is already gone (a stop's
    /// drain_cancel raced the store) — the caller owns aborting its
    /// sleeper then, or it lingers for the full delay.
    #[must_use]
    pub(crate) fn set_abort(&self, id: u64, abort: tokio::task::AbortHandle) -> bool {
        if let Some(entry) = self.map.lock().entries.get(&id) {
            *entry.abort.lock() = Some(abort);
            true
        } else {
            false
        }
    }

    /// Wake path: `Some(fire)` exactly once; `None` = stop already owned it.
    pub(crate) fn take(&self, id: u64) -> Option<Box<dyn FnOnce(bool) + Send>> {
        self.map.lock().entries.remove(&id).map(|e| e.fire)
    }

    /// Stop path: close the registry (inside the same critical section, so
    /// no insert can land after the drain) and resume every sleeper into
    /// cancellation NOW.
    pub(crate) fn drain_cancel(&self) {
        let drained: Vec<DelayedEntry> = {
            let mut map = self.map.lock();
            map.closed = true;
            map.entries.drain().map(|(_, e)| e).collect()
        };
        for entry in drained {
            if let Some(abort) = entry.abort.lock().take() {
                abort.abort();
            }
            (entry.fire)(true);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.lock().entries.len()
    }
}

/// Flow max lane items drained per spin iteration — the
/// WRR-style fairness bound between the lane pool and the core queue
/// (one queue event per ≤N lane items under lane backlog).
const LANE_DRAIN_BATCH: u32 = 16;

/// Flow the attached lane pool's hooks. The C++
/// SDK registers these via `aorta_executor_attach_lane_pool`; the spin
/// loops invoke `drain_fn` each iteration (under the callback context marks)
/// and never park while it reports backlog; every stop path invokes
/// `on_stop` (trampoline-class: pool close + revoke sweep + wake).
#[derive(Clone, Copy)]
pub struct LaneHooks {
    pub drain_fn: unsafe extern "C" fn(*mut std::ffi::c_void, u32) -> u64,
    pub ctx: usize,
    pub on_stop: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
    /// forward an admitted C++ callback item to the lane
    /// pool ON THE PRODUCING THREAD. Returns the
    /// ForwardVerdict discriminant (1 accepted, 2 refused-full
    /// caller-owned, 0 refused-closed).
    pub forward_fn: Option<
        unsafe extern "C" fn(
            ctx: *mut std::ffi::c_void,
            run: unsafe extern "C" fn(*mut std::ffi::c_void),
            drop: unsafe extern "C" fn(*mut std::ffi::c_void),
            item: *mut std::ffi::c_void,
            lane_class: u8,
            group: u64,
            deadline_rel_ns: u64,
            cost_bytes: u32,
        ) -> u8,
    >,
    /// The pool's worker count for the attach pairing check.
    pub pool_workers: u32,
}

// SAFETY: raw pointers are used per the attach contract (the SDK keeps
// `ctx` alive until detach; detach's write lock is the quiesce barrier).
unsafe impl Send for LaneHooks {}
// SAFETY: same contract as Send above.
unsafe impl Sync for LaneHooks {}

/// The registration slot. Invocations COPY the hooks out (never holding
/// the lock across user code: a drained frame calling
/// stop/on_stop would recursively read-lock behind a queued writer and
/// deadlock); the quiesce barrier is the in-flight counter: `detach`
/// clears the slot then waits for zero.
#[derive(Default)]
pub struct LaneHookSlot {
    /// Relaxed fast-gate — every try_send on EVERY executor
    /// (detached included) checks attachment; the RwLock read + in-flight
    /// RMW must not tax the detached hot path. Set before attach, cleared
    /// first in detach.
    attached_fast: std::sync::atomic::AtomicBool,
    hooks: parking_lot::RwLock<Option<LaneHooks>>,
    in_flight: std::sync::atomic::AtomicU32,
    counters: std::sync::OnceLock<ForwardCounters>,
    /// Per-group DECLARED class (set at create_v2; the
    /// default group and class-less groups read Normal).
    group_classes: parking_lot::Mutex<std::collections::HashMap<u64, u8>>,
    /// Telemetry v6: the SDK-pushed per-class-lane
    /// counter snapshot [depth, high_water, enqueued, shed, expired]×4.
    /// None = never attached / no report yet.
    lane_telemetry: parking_lot::Mutex<Option<[[u64; 4]; 5]>>,
}

impl std::fmt::Debug for LaneHookSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaneHookSlot")
            .field("attached", &self.is_attached())
            .finish()
    }
}

#[derive(Clone)]
struct ForwardCounters {
    // Weak: the slot outlives the executor (StopHandle/sender clones);
    // it must not extend the counters' lives (the vital error-reader
    // accounting asserts exact strong counts at executor drop).
    error_count: std::sync::Weak<AtomicU64>,
    dropped_expired: std::sync::Weak<AtomicU64>,
}

/// Outcome of a forward attempt.
pub enum ForwardOutcome {
    Forwarded,
    /// Not attached / no forward hook / pool closing: run the queue path.
    NotAttached(crate::event_queue::Event),
    /// Lane budget full — caller owns the caller overflow policy.
    RefusedFull(crate::event_queue::Event),
}

// The EventPriority → lane class fold (emit/forward path) is the canonical
// `crate::event_queue::lane_class_for_priority`; C++ twin
// (resume-post path) in cpp/include/aorta/lane_runtime.h.

/// An Event deadline as RELATIVE remaining ns.
/// Relative, because the two sides anchor different monotonic epochs
/// (crate::time::monotonic_ns is process-anchored; the lane compares
/// against boot-epoch steady_clock) — an absolute value crosses epochs
/// and insta-expires everything. The SDK re-anchors in ITS domain at
/// the hook. 0 = no deadline; an already-expired event sends 1
/// (expire at the very next dequeue).
fn forward_deadline_rel_ns(event: &crate::event_queue::Event) -> u64 {
    event
        .deadline()
        .map(|at| {
            let now = std::time::Instant::now();
            if at <= now {
                1
            } else {
                ((at - now).as_nanos() as u64).max(1)
            }
        })
        .unwrap_or(0)
}

/// The core-owned shims: the SDK posts these as the lane Work item.
struct ForwardedEvent {
    event: Option<crate::event_queue::Event>,
    counters: Option<ForwardCounters>,
}

unsafe extern "C" fn forwarded_event_run(ctx: *mut std::ffi::c_void) {
    // SAFETY: ctx is the Box::into_raw'd ForwardedEvent; run/drop is
    // exactly-once by the lane contract.
    let mut boxed = unsafe { Box::from_raw(ctx as *mut ForwardedEvent) };
    if let Some(event) = boxed.event.take() {
        // trace mechanics the SAME machinery as queue
        // dispatch — `sched.queue_wait` measured from Event.enqueued_at
        // (its meaning on the lane path: forward→run latency), the
        // dispatch chain ROOT minted here so callback spans keep
        // parenting into the waterfall, and the callback runs timed.
        let root = dispatch_root_link();
        record_queue_wait(&event, root);
        let panicked = run_event_timed(event, root.map(|r| r.child()), None);
        if panicked {
            if let Some(c) = boxed
                .counters
                .as_ref()
                .and_then(|c| c.error_count.upgrade())
            {
                c.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// The RESUME drop shim (shutdown rule): run the closure under the stop-drain
/// mark — the C resume fn observes AORTA_ERR_CANCELLED and the frame
/// unwinds. Never destroys an accepted resume un-resumed.
unsafe extern "C" fn forwarded_resume_drop(ctx: *mut std::ffi::c_void) {
    // SAFETY: ctx is the Box::into_raw'd ForwardedEvent (exactly-once).
    let mut boxed = unsafe { Box::from_raw(ctx as *mut ForwardedEvent) };
    if let Some(event) = boxed.event.take() {
        let _guard = crate::flow::reentrancy::StopDrainGuard::enter();
        let _ = event.run();
    }
}

unsafe extern "C" fn forwarded_event_drop(ctx: *mut std::ffi::c_void) {
    // Lane expiry or pool close: release WITHOUT user code (the queue path's
    // verified drop-at-stop semantics) + expiry attribution.
    // SAFETY: as above.
    let mut boxed = unsafe { Box::from_raw(ctx as *mut ForwardedEvent) };
    if let Some(event) = boxed.event.take() {
        // Per-endpoint expiry attribution: the shim
        // distinguishes expiry from close-drain itself (an unexpired
        // event dropped at close is NOT counted as expired).
        if event.deadline().is_some() && event.is_expired_at(std::time::Instant::now()) {
            event.count_deadline_drop();
            if let Some(c) = boxed
                .counters
                .as_ref()
                .and_then(|c| c.dropped_expired.upgrade())
            {
                c.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl LaneHookSlot {
    fn fast_attached(&self) -> bool {
        self.attached_fast
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Install hooks; `false` if a pool is already attached (a
    /// second attach while a `forward()`/`drain()`
    /// invocation is still mid-flight on the OLD hooks returned OK with no
    /// signal — a caller freeing the old pool on that OK raced a live
    /// callback into a use-after-free). Check-and-set under the write
    /// lock, so two racing attaches can never both succeed.
    #[must_use]
    pub fn attach(&self, hooks: LaneHooks) -> bool {
        let mut slot = self.hooks.write();
        if slot.is_some() {
            return false;
        }
        self.attached_fast
            .store(true, std::sync::atomic::Ordering::Release);
        *slot = Some(hooks);
        true
    }

    /// Quiesce barrier: clears the slot, then waits until every in-flight
    /// invocation (which incremented BEFORE re-reading the slot) returns.
    ///
    /// MUST NOT be called from inside a `checkout()`-guarded hook frame on
    /// this same slot (e.g. a `drain_fn` callback reacting to a message):
    /// the spin below waits on a counter only the blocked outer frame can
    /// decrement — a silent 100%-CPU hang. The single caller
    /// (`aorta_executor_detach_lane_pool`) rejects that path with
    /// `AORTA_ERR_REENTRANT` before reaching here (the ST
    /// `LaneRuntime::SingleThread` default runs
    /// `drain_fn` inline on exactly this thread).
    pub fn detach(&self) {
        self.attached_fast
            .store(false, std::sync::atomic::Ordering::Release);
        *self.hooks.write() = None;
        // A detached executor must not publish frozen v6
        // vectors ("empty = not in facade mode" is the schema contract).
        *self.lane_telemetry.lock() = None;
        // Bounded spin, then yield: `drain()` runs
        // arbitrary-length user lane callbacks under the in-flight window,
        // so a detach racing a long callback would otherwise burn 100% of
        // a core for the callback's whole duration. The quiesce is
        // usually sub-µs — spin briefly for that case, then get off the
        // CPU (detach is a teardown-path call, never RT).
        let mut spins = 0u32;
        while self.in_flight.load(std::sync::atomic::Ordering::Acquire) > 0 {
            spins += 1;
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }

    pub fn is_attached(&self) -> bool {
        self.hooks.read().is_some()
    }

    /// Copy-out under the in-flight window: incr FIRST, then read — a
    /// detach that saw zero can only have run before our read (we see
    /// None) or waits for our decrement. Airtight either way.
    fn checkout(&self) -> Option<LaneHooks> {
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let hooks = *self.hooks.read();
        if hooks.is_none() {
            self.in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
        hooks
    }

    fn checkin(&self) {
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }

    /// Drain up to `max` lane items on THIS thread (the draining
    /// thread), under the callback context marks (reentrancy
    /// guards must keep firing inside lane-drained frames). Returns the
    /// hook's remaining-work report (0 = empty or no hook).
    pub fn drain(&self, max: u32) -> u64 {
        // Detached fast path: one Relaxed load, mirroring forward()/
        // forward_resume() — drain runs once per spin tick on EVERY
        // executor, so the RwLock read + in-flight RMW in checkout()
        // must not tax never-attached nodes.
        if !self.fast_attached() {
            return 0;
        }
        let Some(h) = self.checkout() else { return 0 };
        let _marks = crate::flow::reentrancy::CallbackContextGuard::enter();
        // SAFETY: attach contract — (drain_fn, ctx) valid until detach
        // returns; the in-flight window holds detach open.
        let remaining = unsafe { (h.drain_fn)(h.ctx as *mut std::ffi::c_void, max) };
        self.checkin();
        remaining
    }

    /// The ST FINAL stop-drain pass : after `on_stop`
    /// closed the pool, drain to empty on the spin (draining) thread —
    /// drop deliveries resume frames into cancellation.
    pub fn drain_to_empty(&self) {
        let _drain_guard = crate::flow::reentrancy::StopDrainGuard::enter();
        loop {
            if self.drain(64) != 0 {
                continue;
            }
            // Producer-complete quiesce: a timer/RPC
            // completion mid-flight on another thread may still be about
            // to land a refused resume in the pool residue — wait it out
            // and re-drain before concluding.
            if crate::ffi::c_api::arbiter::resolutions_in_flight() == 0 {
                if self.drain(64) == 0 {
                    return;
                }
                continue;
            }
            std::thread::yield_now();
        }
    }

    /// forward one admitted callback event to the lane
    /// pool on THIS (producing) thread. The shims are CORE-owned:
    /// run = the event's own isolated dispatch (panic contained +
    /// counted); drop = release without user code + expiry attribution.
    pub fn forward(&self, event: crate::event_queue::Event) -> ForwardOutcome {
        if !self.fast_attached() {
            return ForwardOutcome::NotAttached(event);
        }
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let hooks = *self.hooks.read();
        let Some(h) = hooks else {
            self.checkin();
            return ForwardOutcome::NotAttached(event);
        };
        let Some(forward_fn) = h.forward_fn else {
            self.checkin();
            return ForwardOutcome::NotAttached(event);
        };
        let class = lane_class_for_priority(event.priority());
        // The lane enforces drop-if-expired at dequeue
        // and its byte budgets — both need the event's numbers on the wire.
        let deadline_rel_ns = forward_deadline_rel_ns(&event);
        let cost_bytes = u32::try_from(event.estimated_bytes()).unwrap_or(u32::MAX);
        // Group descriptor: bits[0..2] = type (0 default-ME,
        // 1 explicit-ME, 2 reentrant), bits[2..10] = the group's DECLARED
        // class, bits[10..] = handle id. The SDK routes explicit-ME (and
        // MT default-ME) through wake-bit SerialGroups at the declared
        // class; ST ignores groups (the single drainer serializes).
        let group_desc: u64 = match (event.callback_group(), event.callback_group_handle()) {
            (crate::event_queue::CallbackGroupType::Reentrant, _) => 2,
            (_, None) => {
                // default-ME: type bits stay 0; declared class = Normal.
                u64::from(lane_class_for_priority(
                    crate::event_queue::EventPriority::Normal,
                )) << 2
            }
            (_, Some(handle)) => {
                let declared = self.group_classes.lock().get(&handle).copied().unwrap_or(2); // class-less group: Normal
                (handle << 10) | (u64::from(declared) << 2) | 1
            }
        };
        let counters = self.counters.get().cloned();
        let item = Box::into_raw(Box::new(ForwardedEvent {
            event: Some(event),
            counters,
        }));
        // SAFETY: attach contract; the in-flight window holds detach open.
        let verdict = unsafe {
            forward_fn(
                h.ctx as *mut std::ffi::c_void,
                forwarded_event_run,
                forwarded_event_drop,
                item as *mut std::ffi::c_void,
                class,
                group_desc,
                deadline_rel_ns,
                cost_bytes,
            )
        };
        self.checkin();
        match verdict {
            1 => ForwardOutcome::Forwarded,
            2 => {
                // Refused-full, caller-owned: reclaim the un-consumed item.
                // SAFETY: verdict 2 = the SDK did NOT take ownership.
                let boxed = unsafe { Box::from_raw(item) };
                ForwardOutcome::RefusedFull(boxed.event.expect("unconsumed"))
            }
            _ => {
                // Refused-closed (pool closing, executor maybe live):
                // reclaim and fall back to the queue path.
                // SAFETY: as above.
                let boxed = unsafe { Box::from_raw(item) };
                ForwardOutcome::NotAttached(boxed.event.expect("unconsumed"))
            }
        }
    }

    /// Forward an accepted RESUME to the lane pool
    /// (budget-exempt on the SDK side — group sentinel u64::MAX). The
    /// drop shim differs from callbacks: the shutdown rule requires an accepted
    /// resume to RESUME-into-cancellation, never be destroyed — the
    /// shim runs the closure under the StopDrainGuard so the resume fn
    /// reports AORTA_ERR_CANCELLED exactly like the reserved-lane
    /// drain.
    pub fn forward_resume(&self, event: crate::event_queue::Event) -> ForwardOutcome {
        if !self.fast_attached() {
            return ForwardOutcome::NotAttached(event);
        }
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let hooks = *self.hooks.read();
        let Some(h) = hooks else {
            self.checkin();
            return ForwardOutcome::NotAttached(event);
        };
        let Some(forward_fn) = h.forward_fn else {
            self.checkin();
            return ForwardOutcome::NotAttached(event);
        };
        let class = lane_class_for_priority(event.priority());
        let counters = self.counters.get().cloned();
        let item = Box::into_raw(Box::new(ForwardedEvent {
            event: Some(event),
            counters,
        }));
        // SAFETY: attach contract; in-flight window holds detach open.
        let verdict = unsafe {
            forward_fn(
                h.ctx as *mut std::ffi::c_void,
                forwarded_event_run,
                forwarded_resume_drop,
                item as *mut std::ffi::c_void,
                class,
                u64::MAX, // resume sentinel: SDK posts budget-exempt
                0,        // resumes never expire (must-deliver)
                0,
            )
        };
        self.checkin();
        match verdict {
            1 => ForwardOutcome::Forwarded,
            2 => {
                // SAFETY: verdict 2 = SDK did not take ownership.
                let boxed = unsafe { Box::from_raw(item) };
                ForwardOutcome::RefusedFull(boxed.event.expect("unconsumed"))
            }
            _ => {
                // SAFETY: as above.
                let boxed = unsafe { Box::from_raw(item) };
                ForwardOutcome::NotAttached(boxed.event.expect("unconsumed"))
            }
        }
    }

    /// Record a group's declared class (create_v2).
    pub fn set_group_class(&self, handle: u64, class: u8) {
        self.group_classes.lock().insert(handle, class.min(3));
    }

    /// the SDK reports pool-lane counters (telemetry v6).
    pub fn set_lane_telemetry(&self, snapshot: [[u64; 4]; 5]) {
        *self.lane_telemetry.lock() = Some(snapshot);
    }

    pub fn lane_telemetry(&self) -> Option<[[u64; 4]; 5]> {
        *self.lane_telemetry.lock()
    }

    /// Counter binding (error_count / dropped_expired parity).
    pub fn bind_counters(&self, error_count: &Arc<AtomicU64>, dropped_expired: &Arc<AtomicU64>) {
        let _ = self.counters.set(ForwardCounters {
            error_count: Arc::downgrade(error_count),
            dropped_expired: Arc::downgrade(dropped_expired),
        });
    }

    /// Trampoline-class stop bridge : pool close + revoke
    /// sweep + wake live in the SDK's on_stop; NO user code runs here and
    /// the stop call returns immediately (queue-path parity).
    pub fn on_stop(&self) {
        let Some(h) = self.checkout() else { return };
        if let Some(f) = h.on_stop {
            // SAFETY: attach contract; the in-flight window holds detach
            // open (no lock held — reentrant-safe from a drained frame).
            unsafe { f(h.ctx as *mut std::ffi::c_void) };
        }
        self.checkin();
    }
}

/// SerialDomain stop-flag-window seam (TEST-ONLY, `test-seams`
/// cargo feature). The stop paths publish the stop FLAG strictly before
/// invoking the lane facade's `on_stop` bridge; the C++ stop-window
/// test family must hold that window open deterministically, and a C++
/// `#ifdef` cannot reach this Rust code — so the pause point is this
/// feature-gated callback slot, driven through the equally feature-gated
/// `aorta_test_stop_window_seam` C symbol. Fully excluded from release
/// builds: only the dedicated `//core:aorta_core_ffi_test_seams` Bazel
/// target enables the feature.
#[cfg(feature = "test-seams")]
pub mod stop_window_seam {
    use std::ffi::c_void;

    pub type SeamFn = unsafe extern "C" fn(ctx: *mut c_void);

    struct Slot {
        f: Option<SeamFn>,
        ctx: usize,
    }
    static SLOT: parking_lot::Mutex<Slot> = parking_lot::Mutex::new(Slot { f: None, ctx: 0 });

    /// Install (or clear, with `None`) the seam. Test-only contract:
    /// single writer; `ctx` stays valid until cleared.
    pub fn set(f: Option<SeamFn>, ctx: *mut c_void) {
        let mut slot = SLOT.lock();
        slot.f = f;
        slot.ctx = ctx as usize;
    }

    /// The pause point — both stop paths call this between the flag
    /// store and `on_stop`. The slot is copied out before invoking so a
    /// reentrant `set(None)` from the callback cannot deadlock.
    pub(crate) fn pause() {
        let (f, ctx) = {
            let slot = SLOT.lock();
            (slot.f, slot.ctx)
        };
        if let Some(f) = f {
            // SAFETY: installer contract — callback + ctx outlive the
            // installed window (test-only).
            unsafe { f(ctx as *mut c_void) };
        }
    }
}

/// Test-only pause point inside `detach_nodes`, between the ownership
/// re-check and the slot clears. It widens a window that is
/// otherwise a few instructions wide, so the "detach racing a successor
/// attach" test is deterministic rather than a coin flip. Compiled out of
/// every non-test build.
#[cfg(test)]
pub(crate) mod detach_window_seam {
    use std::sync::atomic::{AtomicU64, Ordering};

    static PAUSE_MICROS: AtomicU64 = AtomicU64::new(0);

    /// Install (0 clears) the pause the seam applies. Test-only: single writer.
    pub(crate) fn set_pause_micros(micros: u64) {
        PAUSE_MICROS.store(micros, Ordering::SeqCst);
    }

    pub(crate) fn pause() {
        let micros = PAUSE_MICROS.load(Ordering::SeqCst);
        if micros > 0 {
            std::thread::sleep(std::time::Duration::from_micros(micros));
        }
    }
}

#[derive(Clone)]
pub struct StopHandle {
    stop: Arc<AtomicBool>,
    notify: Arc<Notify>,
    queue_waker: Option<EventQueueWaker>,
    reserved_lane: Arc<crate::event_queue::ReservedLane>,
    delayed: Arc<DelayedResumes>,
    lane_hooks: Arc<LaneHookSlot>,
}

impl StopHandle {
    /// Wake the spin loop WITHOUT stopping (Flow `aorta_executor_wake`:
    /// a lane post signals the combined ST wait through the executor's
    /// own Notify — the primitive the default backend parks on).
    pub fn wake(&self) {
        self.notify.notify_one();
        if let Some(waker) = &self.queue_waker {
            waker.wake();
        }
    }

    pub fn lane_hooks(&self) -> Arc<LaneHookSlot> {
        Arc::clone(&self.lane_hooks)
    }

    /// Resolve-once RT-safe pointer to the monotonic stop flag — the very
    /// atomic the spin thread observes to enter its final drain
    /// (`is_stopped`). Exposed through `aorta_executor_stop_flag_ptr` as
    /// the SDK stop-bridge's arming edge (SerialDomain).
    /// Valid while any clone of the owning `Arc` lives (the FFI registry
    /// entry holds one until `aorta_executor_free`); pointee is a 0→1
    /// monotonic byte, stored with release order, to be read with acquire.
    pub fn stop_flag_ptr(&self) -> *const u8 {
        self.stop.as_ptr() as *const u8
    }

    pub fn stop(&self) {
        // Release pairs with the acquire loads through `stop_flag_ptr`
        // (the SDK bridge reads this byte lock-free).
        self.stop.store(true, Ordering::Release);
        #[cfg(feature = "test-seams")]
        stop_window_seam::pause();
        // Flow stop bridge trampoline-class — the SDK's
        // on_stop closes the pool and sweeps arbiter registrations; the
        // DRAIN happens on the draining thread, never here.
        self.lane_hooks.on_stop();
        // Use notify_one so the wakeup is retained even if the executor loop
        // has not yet started waiting on `notified()`.
        self.notify.notify_one();
        if let Some(waker) = &self.queue_waker {
            waker.wake();
        }
        // Stop is one-way: nothing will dispatch the reserved lane anymore,
        // so close it now and RESUME the drained events into cancellation on
        // this thread rather than dropping accepted resumptions.
        // Producers racing past the close observe `Closed` and run inline
        // (e.g. the FFI result delivery on its watcher task).
        self.reserved_lane
            .close_and_resume("executor stop requested");
        // delayed (still-sleeping) resumptions are not in the lane —
        // abort their sleepers and resume them into cancellation PROMPTLY.
        self.delayed.drain_cancel();
    }

    /// The delayed-resume registry (pub(crate): the FFI post path inserts
    /// through the ExecutorEntry's held handle).
    pub(crate) fn delayed(&self) -> Arc<DelayedResumes> {
        Arc::clone(&self.delayed)
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
}

fn env_flag_enabled(name: &str) -> bool {
    runtime_env::var(name)
        .map(|value| {
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false)
}

fn multi_ingress_capacity(default_capacity: usize) -> usize {
    let base = default_capacity.max(1);
    let parsed = runtime_env::var(ENV_MULTI_INGRESS_CAPACITY)
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(base);
    parsed.max(1)
}

fn env_usize(name: &str) -> Option<usize> {
    runtime_env::var(name).and_then(|raw| raw.parse::<usize>().ok())
}

fn resolve_budget(override_value: usize, env_key: &str) -> usize {
    if override_value > 0 {
        override_value
    } else {
        env_usize(env_key).unwrap_or(0)
    }
}

fn env_flag_enabled_default_false(name: &str) -> bool {
    match runtime_env::var(name) {
        Some(value) => {
            value == "1"
                || value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("yes")
                || value.eq_ignore_ascii_case("on")
        }
        None => false,
    }
}

fn current_runtime_supports_block_in_place() -> bool {
    matches!(
        TokioHandle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(RuntimeFlavor::MultiThread)
    )
}

/// Run `f` under `tokio::task::block_in_place` when the current runtime
/// supports it, otherwise call `f` directly.
///
/// `block_in_place` panics on the current-thread runtime, so a defensive
/// re-check at the call site protects against the case where the executor
/// was constructed under a multi-thread runtime but is later driven from a
/// current-thread runtime (e.g. via `block_on` from a synchronous helper).
/// The crossbeam queue is only selected when `current_runtime_supports_block_in_place`
/// returned true at build time, so the fallback branch should be unreachable in
/// practice; it exists to convert a panic into a (slower) correct path.
fn maybe_block_in_place<T>(f: impl FnOnce() -> T) -> T {
    if current_runtime_supports_block_in_place() {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

fn build_executor_queue(
    capacity: usize,
    admission: ExecutorAdmissionOptions,
    _force_tokio: bool,
) -> EventQueue {
    let options = EventQueueOptions {
        max_queued_events: resolve_budget(admission.max_queued_events, ENV_MAX_QUEUED_EVENTS),
        max_queued_bytes: resolve_budget(admission.max_queued_bytes, ENV_MAX_QUEUED_BYTES),
        admission_observability: env_flag_enabled_default_false(ENV_ADMISSION_OBSERVABILITY),
    };

    // Crossbeam+eventfd remains available on Linux, but it is opt-in because it
    // bypasses Tokio's notify path and has a narrower correctness surface than
    // the default Tokio queue.
    #[cfg(target_os = "linux")]
    {
        let crossbeam_enabled = env_flag_enabled_default_false(ENV_CROSSBEAM_ENABLE);
        let crossbeam_disabled = !crossbeam_enabled
            || _force_tokio
            || !current_runtime_supports_block_in_place()
            || env_flag_enabled(ENV_CROSSBEAM_DISABLE);
        let multi_ingress = env_flag_enabled(ENV_MULTI_INGRESS_ENABLE);
        if !crossbeam_disabled && !multi_ingress {
            return EventQueue::new_crossbeam_with_options(capacity.max(1), options);
        }
    }

    if !env_flag_enabled(ENV_MULTI_INGRESS_ENABLE) {
        return EventQueue::new_with_options(capacity, options);
    }
    EventQueue::new_multi_ingress_with_options(multi_ingress_capacity(capacity), options)
}

#[derive(Clone, Debug)]
struct CallbackGroupEntry {
    group_type: CallbackGroupType,
    mutex: Option<Arc<TokioMutex<()>>>,
    /// The group's scheduler-trace event ids, INTERNED ONCE at group creation
    /// (`sched.group_wait:<label>` / `sched.callback:timer:<label>`, label = name
    /// if given else the handle) — so dispatch carries only `u16`s with zero
    /// per-tick format!/intern, exactly like a named timer.
    group_wait_event: u16,
    callback_event: u16,
    /// Per-group contention state (waiter counter + holder priority), shared
    /// across all dispatches of this group. Cloned into the `DispatchGroup`
    /// ONLY when `aorta.sched` is enabled, so the default path never refcounts it.
    contention: Arc<GroupContention>,
}

/// Per-group contention state shared (behind an `Arc`) by every dispatch of one
/// callback group. Touched only on the traced path: a waiter increments the
/// counter around `lock().await` (the backlog `AdmissionStats` misses — those
/// events have left the queue) and reads the holder's priority to detect
/// inversion. The event ids are pre-interned at group creation (zero per-tick).
#[derive(Debug)]
struct GroupContention {
    /// Live count of dispatches parked on this group's lock. Surfaced as the
    /// `sched.group_waiters:<label>` COUNTER. `i64` so a racing dec never wraps.
    waiters: AtomicI64,
    /// The current holder's `EventPriority as u8` (0 = Realtime/highest). A
    /// contending waiter compares against it to flag priority inversion.
    /// Initialised to 0 (highest) so nothing is flagged until a real holder
    /// records itself — a brief stale read only ever costs a missed/extra
    /// best-effort instant, never correctness.
    holder_priority: AtomicU8,
    /// Pre-interned `sched.group_waiters:<label>` COUNTER event id.
    waiters_event: u16,
    /// Pre-interned `sched.priority_inversion:<label>` INSTANT event id.
    inversion_event: u16,
}

impl GroupContention {
    fn new(ev: &GroupTraceEvents) -> Arc<Self> {
        Arc::new(Self {
            waiters: AtomicI64::new(0),
            holder_priority: AtomicU8::new(0),
            waiters_event: ev.waiters,
            inversion_event: ev.inversion,
        })
    }
}

/// The callback group resolved for one dispatch (see `resolve_for_dispatch`).
pub(crate) struct DispatchGroup {
    /// The MutuallyExclusive lock to hold across the callback (None = Reentrant).
    mutex: Option<Arc<TokioMutex<()>>>,
    /// The group the callback actually ran under (requested, or default).
    effective_handle: CallbackGroupHandle,
    /// Pre-interned trace event ids for this group (see `CallbackGroupEntry`).
    group_wait_event: u16,
    callback_event: u16,
    /// The shared contention state — `Some` ONLY when `aorta.sched` was enabled
    /// at resolve time, so the default path neither refcounts nor accounts.
    contention: Option<Arc<GroupContention>>,
    /// `Some(requested)` when a non-existent handle silently fell back to the
    /// default group — surfaced as a `sched.group_fallback` instant.
    fell_back_from: Option<CallbackGroupHandle>,
}

// ---- scheduler-path tracing (Aorta_Scheduler_Tracing_Proposal §4.1) --------
// The shared dispatch wrapper: every executor dispatch site runs the event
// through `dispatch_traced`, which times queue_wait (enqueue → dispatch) and,
// for a MutuallyExclusive group, group_wait (lock acquisition), records the
// `sched.group_fallback` instant on a silent handle fallback, and times the
// synchronous timer callback (`SchedCallback`, with thread_dur). The histograms
// follow the master switch; the spans/instants gate under `aorta.sched`.

fn sched_queue_wait_event() -> u16 {
    static E: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *E.get_or_init(|| crate::trace::intern_event("sched.queue_wait"))
}

fn sched_callback_event() -> u16 {
    static E: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *E.get_or_init(|| crate::trace::intern_event("sched.callback:timer"))
}

/// Mint the per-dispatch ROOT [`SpanLink`] when `aorta.sched` is enabled: every
/// phase span of ONE dispatch (queue_wait → group_wait → callback) must share a
/// trace and chain (`parent_span_id`) so a consumer renders the scheduler
/// waterfall and attributes the callback to its waits. `None` when the scope is
/// off (histograms still record; no spans). queue_wait is the chain root.
#[inline]
fn dispatch_root_link() -> Option<crate::trace::SpanLink> {
    crate::trace::sched_scope_enabled()
        .then(|| crate::trace::inbound_root_link(None))
        .flatten()
}

/// Record `SchedQueueWait` at dispatch start (now). The queue-wait DURATION is
/// an `Instant` delta (different epoch from the span clock), so the span's
/// monotonic start is reconstructed as `end - duration`. `link` is the dispatch
/// chain root (see [`dispatch_root_link`]); the histogram still records when it
/// is `None`.
#[inline]
fn record_queue_wait(event: &Event, link: Option<crate::trace::SpanLink>) {
    if !crate::trace::trace_enabled() {
        return;
    }
    let dur_ns = event.queue_wait_ns_at(std::time::Instant::now());
    let end = crate::time::monotonic_ns();
    let start = end.saturating_sub(dur_ns);
    crate::trace::record_sched_wait(
        crate::trace::StageId::SchedQueueWait,
        start,
        end,
        sched_queue_wait_event(),
        link,
    );
}

/// Run the event, timing a TIMER callback as `SchedCallback` (subscribers and
/// services already record their own callback span). Synchronous — `event.run`
/// does not `.await`, so thread-CPU around it is meaningful (wall-vs-cpu).
/// `link` is the callback's place in the dispatch chain (child of the last wait
/// phase); `None` (scope off or not a timer) runs the event untraced.
/// `group_callback_event` is the group's PRE-INTERNED `sched.callback:timer:<group>`
/// id (zero per-tick cost); a named timer's own pre-interned id wins over it,
/// and a bare `sched.callback:timer` is the last resort.
#[inline]
/// Parsed-once stall threshold; a dispatched callback holding
/// the thread past it gets a rate-limited WARN (blocking-detection 90%).
fn callback_stall_threshold_ns() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        runtime_env::var("AORTA_CALLBACK_STALL_MS")
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(500)
            .saturating_mul(1_000_000)
    })
}

fn warn_callback_stall(kind: EventKind, elapsed_ns: u64) {
    // Rate-limit: at most one WARN per 5s process-wide (cold path).
    static LAST_WARN_S: AtomicU64 = AtomicU64::new(0);
    let now_s = crate::time::monotonic_ns() / 1_000_000_000;
    let last = LAST_WARN_S.load(Ordering::Relaxed);
    if now_s.saturating_sub(last) >= 5
        && LAST_WARN_S
            .compare_exchange(last, now_s, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        crate::aorta_core_warn!(
            "Aorta WARN: a {:?} callback held the dispatch thread for {}ms \
             (threshold {}ms) — move blocking/CPU-heavy work to offload() \
             (AORTA_CALLBACK_STALL_MS tunes this)",
            kind,
            elapsed_ns / 1_000_000,
            callback_stall_threshold_ns() / 1_000_000
        );
    }
}

/// Deadline drops must never be fully silent — rate-limited (10s) WARN
/// alongside the counters.
fn warn_deadline_drop() {
    static LAST_WARN_S: AtomicU64 = AtomicU64::new(0);
    let now_s = crate::time::monotonic_ns() / 1_000_000_000;
    let last = LAST_WARN_S.load(Ordering::Relaxed);
    if now_s.saturating_sub(last) >= 10
        && LAST_WARN_S
            .compare_exchange(last, now_s, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        crate::aorta_core_warn!(
            "Aorta WARN: events are being dropped at dequeue past their \
             FlowAdmission deadline (see executor dropped_expired / the \
             subscriber's dropped_for_deadline counters) — deadline too \
             tight, or the executor is backed up"
        );
    }
}

fn run_event_timed(
    event: Event,
    link: Option<crate::trace::SpanLink>,
    group_callback_event: Option<u16>,
) -> bool {
    // Coroutine slices are timed like timer callbacks (they are the
    // C++-user hot path that would otherwise be invisible).
    let timed_kind = matches!(event.kind(), EventKind::Timer | EventKind::CoroutineResume);
    let Some(link) = link.filter(|_| timed_kind) else {
        // Stall detection covers EVERY dispatched callback
        // whenever we can afford the clock pair — tracing master on OR a
        // debug build. Release with tracing off stays one branch.
        if cfg!(debug_assertions) || crate::trace::trace_enabled() {
            let kind = event.kind();
            let start = crate::time::monotonic_ns();
            let panicked = event.run();
            let elapsed = crate::time::monotonic_ns().saturating_sub(start);
            if elapsed > callback_stall_threshold_ns() {
                warn_callback_stall(kind, elapsed);
            }
            return panicked;
        }
        return event.run();
    };
    // The timer's pre-interned `sched.callback:timer:<name>` id (read before
    // `run` consumes the event). Named timer (TASK) wins, else the group's
    // pre-interned id, else the bare callback — all `u16`s, no format!/intern.
    let kind = event.kind();
    let named_event = event.callback_trace_event();
    let cpu_start = crate::trace::thread_time_enabled().then(crate::trace::thread_cpu_ns);
    let start = crate::time::monotonic_ns();
    let panicked = event.run();
    let end = crate::time::monotonic_ns();
    let elapsed = end.saturating_sub(start);
    // Stall WARN whenever we measured (tracing on), any kind.
    if elapsed > callback_stall_threshold_ns() {
        warn_callback_stall(kind, elapsed);
    }
    let thread_delta = cpu_start
        .map(|c0| crate::trace::thread_cpu_ns().saturating_sub(c0))
        .unwrap_or(0);
    let event_id = if kind == EventKind::CoroutineResume {
        coroutine_callback_event()
    } else {
        named_event
            .or(group_callback_event)
            .unwrap_or_else(sched_callback_event)
    };
    crate::trace::record_sched_callback(start, end, thread_delta, event_id, link);
    panicked
}

fn coroutine_callback_event() -> u16 {
    static E: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *E.get_or_init(|| crate::trace::intern_event("sched.callback:coroutine"))
}

/// The traced dispatch body shared by every executor path. Returns whether the
/// callback panicked (preserving the existing error-count contract). The group
/// lock is held across the callback exactly as before. All phase spans of one
/// dispatch chain off a single root link (queue_wait → group_wait → callback).
async fn dispatch_traced(group: DispatchGroup, event: Event) -> bool {
    if let Some(requested) = group.fell_back_from {
        crate::trace::sched_instant(crate::trace::intern_event(&format!(
            "sched.group_fallback:{requested}"
        )));
    }
    // The group's PRE-INTERNED trace event ids (interned once at group creation)
    // — both phases use them directly, so dispatch never format!s/interns.
    let group_wait_event = group.group_wait_event;
    let group_callback_event = group.callback_event;
    // queue_wait is the chain root; later phases are its descendants.
    let root = dispatch_root_link();
    record_queue_wait(&event, root);

    match group.mutex {
        Some(lock) => {
            let g_start = crate::trace::trace_enabled().then(crate::time::monotonic_ns);
            // `contention` is `Some` only when `aorta.sched` is on (resolve time);
            // then acquisition maintains the waiter counter + flags inversion.
            let _guard = match &group.contention {
                Some(c) => acquire_group_traced(&lock, c, event.priority()).await,
                None => lock.lock().await,
            };
            // group_wait is a child of queue_wait; the callback then chains off
            // group_wait so the waterfall reads queue_wait → group_wait → cb.
            let mut callback_parent = root;
            if let Some(g_start) = g_start {
                let g_end = crate::time::monotonic_ns();
                let gw_link = root.map(|r| r.child());
                crate::trace::record_sched_wait(
                    crate::trace::StageId::SchedGroupWait,
                    g_start,
                    g_end,
                    group_wait_event,
                    gw_link,
                );
                if gw_link.is_some() {
                    callback_parent = gw_link;
                }
            }
            run_event_timed(
                event,
                callback_parent.map(|p| p.child()),
                Some(group_callback_event),
            )
        }
        None => run_event_timed(event, root.map(|r| r.child()), Some(group_callback_event)),
    }
}

/// The single-threaded executor's traced dispatch: it serializes by
/// construction (no callback-group LOCK), so only queue_wait + the timer
/// callback span apply (no group_wait). The callback chains directly off the
/// queue_wait root. When tracing, the caller still resolves the event's group
/// (passed as `group`, `Some` only when tracing is on — keeping the registry
/// read off the non-traced hot path) so the callback is labeled by group and a
/// stale handle still flags `sched.group_fallback`, matching the multi-threaded
/// path. Returns whether the callback panicked.
#[inline]
fn dispatch_traced_sync(group: Option<DispatchGroup>, event: Event) -> bool {
    if let Some(requested) = group.as_ref().and_then(|g| g.fell_back_from) {
        crate::trace::sched_instant(crate::trace::intern_event(&format!(
            "sched.group_fallback:{requested}"
        )));
    }
    let root = dispatch_root_link();
    record_queue_wait(&event, root);
    let group_callback_event = group.as_ref().map(|g| g.callback_event);
    run_event_timed(event, root.map(|r| r.child()), group_callback_event)
}

/// Acquire a MutuallyExclusive group's lock WITH contention accounting. Called
/// only on the traced path (`group.contention` is `Some` ⇒ `aorta.sched` on).
/// The uncontended acquire takes the fast `try_lock` path and only records the
/// new holder's priority. A CONTENDED acquire (a holder is in the guard right
/// now) additionally: (a) emits `sched.priority_inversion:<group>` when this
/// waiter outranks the holder, and (b) maintains the `sched.group_waiters:<group>`
/// COUNTER across the wait — the backlog `AdmissionStats` cannot see, because a
/// parked waiter has already left the EventQueue.
async fn acquire_group_traced<'a>(
    lock: &'a Arc<TokioMutex<()>>,
    contention: &GroupContention,
    waiter_priority: EventPriority,
) -> tokio::sync::MutexGuard<'a, ()> {
    let waiter = waiter_priority as u8;
    // Fast path: lock free → no wait, no inversion, just record the holder.
    if let Ok(guard) = lock.try_lock() {
        contention.holder_priority.store(waiter, Ordering::Release);
        return guard;
    }
    // Contended: a lower-discriminant priority is HIGHER (Realtime = 0). If this
    // waiter outranks the holder we must wait behind it → priority inversion.
    if waiter < contention.holder_priority.load(Ordering::Acquire) {
        crate::trace::sched_instant(contention.inversion_event);
    }
    let depth = contention.waiters.fetch_add(1, Ordering::AcqRel) + 1;
    crate::trace::sched_counter(contention.waiters_event, depth.max(0) as u64);
    let guard = lock.lock().await;
    let depth = contention.waiters.fetch_sub(1, Ordering::AcqRel) - 1;
    crate::trace::sched_counter(contention.waiters_event, depth.max(0) as u64);
    contention.holder_priority.store(waiter, Ordering::Release);
    guard
}

fn sched_relay_wait_event() -> u16 {
    static E: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *E.get_or_init(|| crate::trace::intern_event("sched.relay_wait"))
}

/// Record `SchedRelayWait` for an event leaving the subscriber overflow-relay
/// queue: its age so far (construction → now) is the time it spent BEFORE
/// reaching the executor queue, which `SchedQueueWait` (construction → dispatch)
/// would otherwise silently absorb. So a slow callback backing the relay up is
/// attributable rather than mis-blamed on scheduler queue_wait. Histogram is
/// master-gated; the async span is `aorta.sched`-gated. Called from the relay
/// task at dequeue; a no-op when tracing is off.
pub(crate) fn record_relay_wait(event: &Event) {
    if !crate::trace::trace_enabled() {
        return;
    }
    let dur_ns = event.queue_wait_ns_at(std::time::Instant::now());
    let end = crate::time::monotonic_ns();
    let start = end.saturating_sub(dur_ns);
    // The relay task is off the dispatch chain, so this marker is its own root.
    let link = crate::trace::sched_scope_enabled()
        .then(|| crate::trace::inbound_root_link(None))
        .flatten();
    crate::trace::record_sched_wait(
        crate::trace::StageId::SchedRelayWait,
        start,
        end,
        sched_relay_wait_event(),
        link,
    );
}

#[derive(Clone, Debug)]
pub(crate) struct CallbackGroupRegistry {
    inner: Arc<CallbackGroupRegistryInner>,
}

#[derive(Debug)]
struct CallbackGroupRegistryInner {
    next_id: AtomicU64,
    entries: RwLock<HashMap<CallbackGroupHandle, CallbackGroupEntry>>,
}

/// A group's scheduler-trace event ids, interned ONCE at group creation.
struct GroupTraceEvents {
    group_wait: u16,
    callback: u16,
    waiters: u16,
    inversion: u16,
}

/// Pre-intern a group's scheduler-trace event ids ONCE at group creation, so
/// dispatch carries only the `u16`s — no per-tick format!/intern. A NAMED group
/// labels everything by name; an UNNAMED group uses the numeric handle for the
/// wait/contention labels and keeps the callback BARE (`sched.callback:timer`,
/// the timer's own name still wins over it at dispatch).
fn group_trace_events(name: Option<&str>, handle: CallbackGroupHandle) -> GroupTraceEvents {
    // The wait / contention labels share one `<label>` (name if given, else the
    // handle); only the callback has the bare-unnamed special case.
    let label = match name {
        Some(n) => n.to_owned(),
        None => handle.to_string(),
    };
    let callback = match name {
        Some(n) => crate::trace::intern_event(&format!("sched.callback:timer:{n}")),
        None => sched_callback_event(),
    };
    GroupTraceEvents {
        group_wait: crate::trace::intern_event(&format!("sched.group_wait:{label}")),
        callback,
        waiters: crate::trace::intern_event(&format!("sched.group_waiters:{label}")),
        inversion: crate::trace::intern_event(&format!("sched.priority_inversion:{label}")),
    }
}

impl CallbackGroupRegistry {
    pub(crate) fn new() -> Self {
        let mut entries = HashMap::new();
        let mx = group_trace_events(None, DEFAULT_GROUP_MUTUALLY_EXCLUSIVE_HANDLE);
        entries.insert(
            DEFAULT_GROUP_MUTUALLY_EXCLUSIVE_HANDLE,
            CallbackGroupEntry {
                group_type: CallbackGroupType::MutuallyExclusive,
                mutex: Some(Arc::new(TokioMutex::new(()))),
                group_wait_event: mx.group_wait,
                callback_event: mx.callback,
                contention: GroupContention::new(&mx),
            },
        );
        let re = group_trace_events(None, DEFAULT_GROUP_REENTRANT_HANDLE);
        entries.insert(
            DEFAULT_GROUP_REENTRANT_HANDLE,
            CallbackGroupEntry {
                group_type: CallbackGroupType::Reentrant,
                mutex: None,
                group_wait_event: re.group_wait,
                callback_event: re.callback,
                contention: GroupContention::new(&re),
            },
        );

        Self {
            inner: Arc::new(CallbackGroupRegistryInner {
                next_id: AtomicU64::new(DEFAULT_GROUP_REENTRANT_HANDLE + 1),
                entries: RwLock::new(entries),
            }),
        }
    }

    pub(crate) fn create_group(&self, group_type: CallbackGroupType) -> CallbackGroupHandle {
        self.create_group_named(group_type, None)
    }

    /// Like [`Self::create_group`] but attaches a human label, which the
    /// scheduler tracer uses in `sched.group_wait:<name>` / `sched.callback:<name>`
    /// (and the per-callback `args.group`) instead of the opaque numeric handle —
    /// so a multi-group deployment is legible in Perfetto.
    pub(crate) fn create_group_named(
        &self,
        group_type: CallbackGroupType,
        name: Option<&str>,
    ) -> CallbackGroupHandle {
        let handle = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        // Intern the group's trace event ids ONCE here.
        let events = group_trace_events(name, handle);
        let entry = CallbackGroupEntry {
            group_type,
            mutex: match group_type {
                CallbackGroupType::MutuallyExclusive => Some(Arc::new(TokioMutex::new(()))),
                CallbackGroupType::Reentrant => None,
            },
            group_wait_event: events.group_wait,
            callback_event: events.callback,
            contention: GroupContention::new(&events),
        };
        if let Ok(mut entries) = self.inner.entries.write() {
            entries.insert(handle, entry);
        }
        handle
    }

    pub(crate) fn remove_group(&self, handle: CallbackGroupHandle) -> bool {
        if handle == DEFAULT_GROUP_MUTUALLY_EXCLUSIVE_HANDLE
            || handle == DEFAULT_GROUP_REENTRANT_HANDLE
        {
            return false;
        }

        self.inner
            .entries
            .write()
            .ok()
            .and_then(|mut entries| entries.remove(&handle))
            .is_some()
    }

    /// The MutuallyExclusive lock for `event`'s callback group (None for a
    /// Reentrant group). Thin wrapper over [`Self::resolve_for_dispatch`] for
    /// callers/tests that only need the lock.
    pub(crate) fn lock_for_event(&self, event: &Event) -> Option<Arc<TokioMutex<()>>> {
        self.resolve_for_dispatch(event).mutex
    }

    /// Resolve the callback group for dispatch: the lock to take (if any), the
    /// EFFECTIVE handle (the requested one, or the default it fell back to), and
    /// — when a non-default requested handle was missing — the requested handle
    /// (so the scheduler tracer can flag a silent fallback). Replaces the old
    /// `resolve_entry`: one registry read, plus the trace info.
    pub(crate) fn resolve_for_dispatch(&self, event: &Event) -> DispatchGroup {
        let fallback = match event.callback_group() {
            CallbackGroupType::MutuallyExclusive => DEFAULT_GROUP_MUTUALLY_EXCLUSIVE_HANDLE,
            CallbackGroupType::Reentrant => DEFAULT_GROUP_REENTRANT_HANDLE,
        };
        let requested = event.callback_group_handle();
        let handle = requested.unwrap_or(fallback);

        if let Some(entry) = self
            .inner
            .entries
            .read()
            .ok()
            .and_then(|entries| entries.get(&handle).cloned())
        {
            // Only carry the shared contention state when the scheduler scope is
            // on (one relaxed atomic load); the default path skips the Arc clone.
            let contention =
                crate::trace::sched_scope_enabled().then(|| Arc::clone(&entry.contention));
            return DispatchGroup {
                mutex: entry.mutex,
                effective_handle: handle,
                group_wait_event: entry.group_wait_event,
                callback_event: entry.callback_event,
                contention,
                fell_back_from: None,
            };
        }

        // The requested handle is unknown → silently serialized onto the
        // default group. Warn (as before) and report it to the tracer.
        if requested.is_some() {
            aorta_core_warn!(
                "Aorta WARN: callback_group_handle {} not found in executor; fallback to default {:?} group.",
                handle,
                event.callback_group()
            );
        }

        let entry = self
            .inner
            .entries
            .read()
            .ok()
            .and_then(|entries| entries.get(&fallback).cloned())
            .unwrap_or_else(|| {
                let events = group_trace_events(None, fallback);
                CallbackGroupEntry {
                    group_type: CallbackGroupType::MutuallyExclusive,
                    mutex: Some(Arc::new(TokioMutex::new(()))),
                    group_wait_event: events.group_wait,
                    callback_event: events.callback,
                    contention: GroupContention::new(&events),
                }
            });
        let contention = crate::trace::sched_scope_enabled().then(|| Arc::clone(&entry.contention));
        DispatchGroup {
            mutex: entry.mutex,
            effective_handle: fallback,
            group_wait_event: entry.group_wait_event,
            callback_event: entry.callback_event,
            contention,
            fell_back_from: requested,
        }
    }

    #[cfg(test)]
    fn group_type(&self, handle: CallbackGroupHandle) -> Option<CallbackGroupType> {
        self.inner
            .entries
            .read()
            .ok()
            .and_then(|entries| entries.get(&handle).map(|entry| entry.group_type))
    }
}

pub struct SingleThreadedExecutor {
    queue: EventQueue,
    nodes: Vec<AttachedNode>,
    /// See `next_executor_id`.
    executor_id: u64,
    stop: Arc<AtomicBool>,
    notify: Arc<Notify>,
    lane_hooks: Arc<LaneHookSlot>,
    options: ExecutorOptions,
    timer_scheduler: ExecutorTimerScheduler,
    fifo_state: VecDeque<Event>,
    priority_state: PriorityDispatchState,
    callback_groups: CallbackGroupRegistry,
    // Monotonic spin-loop counter, shared read-only with runtime telemetry.
    // Own Arc allocation, so it never shares a cache line with the AdmissionState
    // hot atomics (which live in the EventQueue), avoiding false sharing.
    spin_count: Arc<AtomicU64>,
    // Monotonic count of park ENTRIES on the spin loop — one increment
    // each time the loop is about to block, never per message and never per
    // spin iteration. Own Arc, same reasoning as spin_count.
    park_count: Arc<AtomicU64>,
    // Monotonic count of caught callback panics (isolated dispatch
    // errors), its own Arc like spin_count; shared read-only with the vital
    // health pulse (RuntimeHealth.error_count). Bumped only on the rare panic
    // path, never on the hot dispatch path.
    error_count: Arc<AtomicU64>,
    /// Flow: events dropped at dequeue because their deadline passed.
    dropped_expired: Arc<AtomicU64>,
    /// delayed resumptions (post_resume_after) — shared with every
    /// StopHandle so stop can cancel sleepers promptly.
    delayed: Arc<DelayedResumes>,
}

/// Spin-loop occupancy snapshot (RX wake-syscall accounting).
///
/// The three counters answer "how much of this executor's traffic cost a wake
/// syscall pair":
///
/// * `spins` — spin-loop iterations (executor liveness; the pre-existing counter).
/// * `parks` — park ENTRIES: one per block, not per message and not per
///   iteration. Each park is terminated by exactly one wake, so this is also
///   the count of wakes the consumer side actually paid for.
/// * `wake_signals` — events published to the backend channel, i.e. wakes the
///   producer side attempted. See [`EventQueue::wake_signals`].
///
/// `parks / wake_signals` is therefore the share of sends that cost a wake:
/// ≈ 1.0 at a paced rate (the consumer drains to empty between messages) and
/// ≈ 0 under saturation (it never drains). Nothing here changes behaviour; the
/// counters are Relaxed and read-only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutorSpinStats {
    pub spins: u64,
    pub parks: u64,
    pub wake_signals: u64,
}

/// Clonable read handle for [`ExecutorSpinStats`], mirroring
/// [`AdmissionStatsReader`]. Take it BEFORE moving the executor onto its own
/// thread (`spawn_executor_thread` consumes the executor) and the counters stay
/// observable for the process's lifetime.
#[derive(Clone, Debug)]
pub struct ExecutorSpinStatsReader {
    spin_count: Arc<AtomicU64>,
    park_count: Arc<AtomicU64>,
    admission: Arc<crate::event_queue::AdmissionState>,
}

impl ExecutorSpinStatsReader {
    pub fn snapshot(&self) -> ExecutorSpinStats {
        ExecutorSpinStats {
            spins: self.spin_count.load(Ordering::Relaxed),
            parks: self.park_count.load(Ordering::Relaxed),
            wake_signals: self.admission.wake_signal_count(),
        }
    }
}

/// Process-unique identity for an executor instance, so a node can record
/// WHICH executor last attached it. A monotonic counter rather
/// than a pointer: an executor's address can be recycled after it is dropped,
/// and a recycled id would let an unrelated executor inherit a predecessor's
/// reattach privilege.
fn next_executor_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// A node attached to an executor, together with the attachment generation
/// this executor installed.
///
/// Detach and drop only unwind attachments this executor still OWNS: once a
/// node has been released and handed to another executor, the old executor's
/// teardown must not clear the new executor's wiring out from under it.
#[derive(Debug)]
struct AttachedNode {
    node: Node,
    generation: u64,
}

impl AttachedNode {
    /// Whether this record still describes the node's CURRENT attachment.
    fn owns_current_attachment_of(&self, node: &Node) -> bool {
        self.node.is_same_instance(node) && node.executor_attachment_generation() == self.generation
    }

    /// Whether this executor is still the node's attached executor.
    fn still_attached(&self) -> bool {
        self.node.executor_attachment_generation() == self.generation
    }
}

/// The ST/MT executors implement these members VERBATIM-identically
/// (the hand-copied versions drifted once —
/// the MT spin_crossbeam drain gap — so the shared bodies now expand
/// from ONE macro into both impl blocks; the near-identical spin/dispatch
/// loops stay per-type because their JoinSet/dispatch shapes differ).
macro_rules! impl_executor_shared_members {
    () => {
        pub fn stop_handle(&self) -> StopHandle {
            StopHandle {
                stop: Arc::clone(&self.stop),
                notify: Arc::clone(&self.notify),
                queue_waker: self.queue.waker(),
                reserved_lane: self.queue.reserved_lane(),
                delayed: Arc::clone(&self.delayed),
                lane_hooks: Arc::clone(&self.lane_hooks),
            }
        }
        /// Whether this executor's queue is the tokio-mpsc backend, i.e.
        /// whether it is safe to spin on a `current_thread` runtime. The
        /// crossbeam+eventfd backend parks via `block_in_place`, which a
        /// `current_thread` runtime cannot serve.
        pub fn forces_tokio_queue(&self) -> bool {
            self.options.force_tokio_queue
        }
        pub fn request_stop(&self) {
            // Release for the same acquire pairing as StopHandle::stop.
            self.stop.store(true, Ordering::Release);
            #[cfg(feature = "test-seams")]
            crate::executor::stop_window_seam::pause();
            // Every stop origin bridges to the attached C++ lane pool:
            // StopHandle::stop() fires on_stop; a
            // direct request_stop() must too, or a Rust-plane stop leaves
            // the pool open (no close, no arbiter revoke sweep, no wake).
            self.lane_hooks.on_stop();
            self.notify.notify_one();
            if let Some(waker) = self.queue.waker() {
                waker.wake();
            }
            // See StopHandle::stop — a stopped executor must not strand lane
            // deliveries; close the lane so producers fail over.
            self.queue
                .reserved_lane()
                .close_and_resume("executor stop requested");
            // direct stops must cancel sleeping delayed
            // resumes promptly, exactly like StopHandle::stop().
            self.delayed.drain_cancel();
        }
        /// Attach `node` to this executor. A node that is already attached to
        /// a DIFFERENT live executor is REFUSED with an error log and left
        /// untouched — see [`Self::try_add_node`] for the rationale and the
        /// fallible form.
        pub fn add_node(&mut self, node: Node) {
            if let Err(err) = self.try_add_node(node) {
                crate::aorta_core_error!("Aorta ERROR: {}", err);
            }
        }

        /// Fallible [`Self::add_node`].
        ///
        /// The FIRST executor attached to a node OWNS it.
        /// Re-pointing the node's event-sender cell under a second executor
        /// used to succeed silently, but every subscriber/timer/service
        /// created before the re-point kept feeding the PREVIOUS executor:
        /// the application's own executor then spun forever without ever
        /// receiving an event, and its configured queue depth and callback
        /// groups were inert. Refusing keeps the node in one coherent state
        /// and surfaces the misuse instead of hiding it.
        ///
        /// Re-attaching the SAME executor is an idempotent refresh, and a node
        /// released with [`Self::detach_nodes`] may be attached to a new
        /// executor.
        pub fn try_add_node(&mut self, node: Node) -> crate::error::AortaResult<()> {
            // ONE transaction for the ownership check AND the whole wiring
            // sequence. A plain check-then-set let two threads
            // attaching the same cloned node both observe it as unowned and
            // both claim it, and let a predecessor's detach interleave with
            // this attach. Holding the node's attachment transaction across
            // the entire body makes the claim atomic and mutually exclusive
            // with `detach_nodes`.
            let _txn = node.attachment_transaction();

            let already_ours = self
                .nodes
                .iter()
                .any(|attached| attached.owns_current_attachment_of(&node));
            if node.has_event_sender() && !already_ours {
                return Err(crate::error::AortaError::invalid_argument(format!(
                    "node '{}' already has a live executor; attach it before \
                     creating the first callback-capable primitive, or detach \
                     it first (re-attaching under live subscribers, timers and \
                     services would split callbacks across two executors)",
                    node.name()
                )));
            }
            // A HANDOFF — attaching a node to a DIFFERENT executor than the one
            // that last owned it — must not strand producers that snapshotted
            // the previous executor's sender. Subscribers re-validate against
            // the attachment generation and follow the node, but timers and
            // services keep the sender they captured at construction, so they
            // would keep enqueueing into the executor the node just left.
            // Refuse rather than create that split.
            //
            // Reattaching to the executor the node just LEFT is not a handoff
            // and is always safe: `self.queue` is never
            // reassigned and `detach_nodes()` only clears the node's cell, so
            // those producers already hold exactly the queue this attach is
            // about to install. Keying on the recorded owner rather than on
            // "this executor still lists the node" matters — an executor keeps
            // its `self.nodes` entry forever, so a stale predecessor would
            // otherwise reclaim a node whose producers have since captured a
            // different executor's queue.
            let we_last_owned_it = node.last_attachment_owner() == self.executor_id;
            if !already_ours && !we_last_owned_it && node.executor_attachment_generation() != 0 {
                let live = node.live_snapshot_producer_count();
                if live > 0 {
                    return Err(crate::error::AortaError::invalid_argument(format!(
                        "node '{}' cannot be handed to another executor while {} \
                         timer/service callback producer(s) are still live: they \
                         captured the previous executor's queue at construction \
                         and cannot follow the node, which would split its \
                         callbacks across two executors. Drop the timers and \
                         services first, or keep the node on its original \
                         executor (subscribers do follow a handoff)",
                        node.name(),
                        live
                    )));
                }
            }
            let generation = node.set_event_sender(self.queue.sender(), self.executor_id);
            node.set_executor_admission_reader(self.queue.admission_reader());
            node.set_executor_spin_reader(Arc::clone(&self.spin_count));
            node.set_executor_flow_readers(crate::telemetry::runtime::FlowExecutorReaders {
                lane_hooks: Arc::clone(&self.lane_hooks),
                lane: self.queue.reserved_lane(),
                dropped_expired: Arc::clone(&self.dropped_expired),
            });
            node.set_executor_error_reader(Arc::clone(&self.error_count));
            node.set_timer_scheduler(self.timer_scheduler.clone());
            node.set_callback_group_registry(self.callback_groups.clone());
            node.set_executor_class_policy_defaults(self.options.class_policy);
            match self
                .nodes
                .iter_mut()
                .find(|attached| attached.node.is_same_instance(&node))
            {
                Some(attached) => attached.generation = generation,
                // Clone rather than move: `_txn` borrows `node` for the rest
                // of this transaction. `Node` is an Arc-backed handle.
                None => self.nodes.push(AttachedNode {
                    node: node.clone(),
                    generation,
                }),
            }
            Ok(())
        }
        pub fn create_callback_group(&self, group_type: CallbackGroupType) -> CallbackGroupHandle {
            self.callback_groups.create_group(group_type)
        }
        /// On an ATTACHED executor a fired TIMER
        /// callback is posted as a lane-class item from this (spin) thread —
        /// the core timer machinery stays bit-for-bit; only the
        /// callback's EXECUTION moves. Refusal (budget full / pool closing)
        /// falls back to inline queue dispatch — a timer tick is never shed.
        fn forward_timer_or_keep(&self, event: Event) -> Option<Event> {
            if event.kind() != EventKind::Timer || !self.lane_hooks.fast_attached() {
                // fast_attached: one Relaxed load per fired timer — is_attached()
                // takes the RwLock. A transient
                // false-negative during attach keeps the tick on the queue
                // path (the detached behavior), which is benign.
                return Some(event);
            }
            match self.lane_hooks.forward(event) {
                crate::executor::ForwardOutcome::Forwarded => None,
                crate::executor::ForwardOutcome::NotAttached(event)
                | crate::executor::ForwardOutcome::RefusedFull(event) => Some(event),
            }
        }
        fn pop_ready_event(&mut self) -> Option<Event> {
            loop {
                let event = match self.options.mode {
                    SchedulerMode::Fifo => self.fifo_state.pop_front(),
                    SchedulerMode::Priority => self.priority_state.pop(),
                }?;
                // Flow: drop-if-expired at dequeue — one monotonic compare,
                // and the clock is read only for deadline-carrying events.
                if event.deadline().is_some() && event.is_expired_at(std::time::Instant::now()) {
                    self.dropped_expired.fetch_add(1, Ordering::Relaxed);
                    event.count_deadline_drop(); // per-endpoint attribution
                    warn_deadline_drop();
                    continue;
                }
                return Some(event);
            }
        }
    };
}

impl SingleThreadedExecutor {
    impl_executor_shared_members!();

    /// Release every node this executor still owns, so each may be attached
    /// to another executor.
    ///
    /// Nodes that have since moved on are SKIPPED: clearing them
    /// would tear down the wiring their current executor installed.
    pub fn detach_nodes(&self) {
        for attached in &self.nodes {
            // The ownership re-check and every clear below happen INSIDE the
            // node's attachment transaction: otherwise a
            // successor's attach could land between this check and the last
            // clear, leaving the node advertising the successor's sender with
            // parts of its configuration already stripped.
            let _txn = attached.node.attachment_transaction();
            if !attached.still_attached() {
                continue;
            }
            let node = &attached.node;
            node.clear_event_sender();
            // Seam sits in the exact race window: between the
            // sender clear and the rest of the teardown.
            #[cfg(test)]
            crate::executor::detach_window_seam::pause();
            node.clear_timer_scheduler();
            node.clear_callback_group_registry();
            node.clear_executor_admission_reader();
            node.clear_executor_spin_reader();
            node.clear_executor_flow_readers();
            node.clear_executor_error_reader();
        }
    }

    pub fn new(capacity: usize) -> Self {
        Self::new_with_options(capacity, ExecutorOptions::default())
    }

    pub fn new_with_options(capacity: usize, options: ExecutorOptions) -> Self {
        let notify = Arc::new(Notify::new());
        let mut queue =
            build_executor_queue(capacity, options.admission, options.force_tokio_queue);
        // The queue path does NOT take this `Notify`: a channel send already
        // wakes the receiver the spin loop selects on, so a second wake would
        // be per-message cost for nothing (see `EventQueueSender::try_send_queue`).
        // It stays the wake primitive for the timer scheduler and `StopHandle`.
        // the sender carries the hook slot from birth so
        // every clone sees a later attach; counters bind for shim parity.
        let lane_hooks = Arc::new(LaneHookSlot::default());
        queue.set_lane_hooks(Arc::clone(&lane_hooks));
        let error_count = Arc::new(AtomicU64::new(0));
        let dropped_expired = Arc::new(AtomicU64::new(0));
        lane_hooks.bind_counters(&error_count, &dropped_expired);
        let queue_waker = queue.waker();
        Self {
            queue,
            nodes: Vec::new(),
            stop: Arc::new(AtomicBool::new(false)),
            notify: Arc::clone(&notify),
            lane_hooks,
            options,
            timer_scheduler: ExecutorTimerScheduler::new(notify, queue_waker),
            fifo_state: VecDeque::new(),
            priority_state: PriorityDispatchState::new(
                options.priority.weights,
                options.priority.map_subscription_qos,
            ),
            callback_groups: CallbackGroupRegistry::new(),
            executor_id: next_executor_id(),
            spin_count: Arc::new(AtomicU64::new(0)),
            park_count: Arc::new(AtomicU64::new(0)),
            error_count,
            dropped_expired,
            delayed: DelayedResumes::new(),
        }
    }

    /// Like [`Self::create_callback_group`] but names the group so the scheduler
    /// tracer labels `sched.group_wait:<name>` / `sched.callback:<name>` instead
    /// of the opaque numeric handle.
    pub fn create_callback_group_named(
        &self,
        group_type: CallbackGroupType,
        name: Option<&str>,
    ) -> CallbackGroupHandle {
        self.callback_groups.create_group_named(group_type, name)
    }

    pub fn remove_callback_group(&self, handle: CallbackGroupHandle) -> bool {
        self.callback_groups.remove_group(handle)
    }

    pub fn queue(&self) -> &EventQueue {
        &self.queue
    }

    pub fn admission_stats(&self) -> AdmissionStats {
        self.queue.admission_stats()
    }

    pub fn admission_reader(&self) -> AdmissionStatsReader {
        self.queue.admission_reader()
    }

    /// Flow observability: events dropped at dequeue on an expired
    /// deadline.
    pub fn dropped_expired(&self) -> u64 {
        self.dropped_expired.load(Ordering::Relaxed)
    }

    /// Current spin/park/wake-signal counters.
    pub fn spin_stats(&self) -> ExecutorSpinStats {
        self.spin_stats_reader().snapshot()
    }

    /// Clonable read handle for [`Self::spin_stats`], usable after the
    /// executor has been moved onto its own thread.
    pub fn spin_stats_reader(&self) -> ExecutorSpinStatsReader {
        ExecutorSpinStatsReader {
            spin_count: Arc::clone(&self.spin_count),
            park_count: Arc::clone(&self.park_count),
            admission: self.queue.wake_signal_reader(),
        }
    }

    pub async fn spin(&mut self) {
        if self.queue.is_crossbeam() {
            self.spin_crossbeam();
        } else {
            self.spin_tokio().await;
        }
        // Resume ready-state coroutine resumptions into cancellation
        // on this (draining) thread before the spin returns.
        self.resume_ready_coroutines_on_stop();
        // Flow: the ST FINAL stop-drain pass — on_stop
        // (fired by the stop bridge) closed the attached lane pool; drain
        // its residue to the drop path HERE, on the draining thread, so no
        // lane frame sits un-resumed after this spin returns.
        self.lane_hooks.drain_to_empty();
    }

    /// Spin loop for tokio mpsc / multi-ingress queues (original path).
    async fn spin_tokio(&mut self) {
        loop {
            // Count every spin-loop iteration (cheap Relaxed; no clock read).
            self.spin_count.fetch_add(1, Ordering::Relaxed);
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            // Flow drain the attached lane pool BEFORE the
            // queue pop — the combined ST wait's lane half.
            let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

            self.ingest_timer_events();
            if self.dispatch_ready_batch() {
                continue;
            }
            if let Some(event) = self.queue.try_pop() {
                self.ingest_batch(event);
                continue;
            }
            if lane_remaining > 0 {
                // Never park while the hook reports backlog: the
                // wake permit may already be consumed.
                continue;
            }

            let timer_wait = self.time_until_next_timer_deadline();

            // One increment per park ENTRY (the loop is about to
            // block); never per message, never per spin iteration.
            self.park_count.fetch_add(1, Ordering::Relaxed);
            let event = tokio::select! {
                _ = self.notify.notified() => {
                    continue;
                }
                event = async {
                    match timer_wait {
                        Some(wait) => self.queue.pop_timeout(wait).await,
                        None => self.queue.pop().await,
                    }
                } => event,
            };

            match event {
                Some(event) => self.ingest_batch(event),
                None => {
                    if self.queue.is_closed() {
                        break;
                    }
                }
            }
        }
    }

    /// Spin loop for crossbeam+eventfd queues.
    ///
    /// Bypasses `tokio::select!` entirely.  The crossbeam receiver's
    /// spin+eventfd blocking provides low-latency wake for queue events.
    /// Timer events and stop requests are checked on each iteration and
    /// the synchronous wait timeout is bounded by the next timer deadline.
    fn spin_crossbeam(&mut self) {
        // We use block_in_place so that the tokio runtime can schedule
        // other tasks on this worker thread while we block on eventfd.
        maybe_block_in_place(|| loop {
            self.spin_count.fetch_add(1, Ordering::Relaxed);
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            // Flow the lane half of the combined wait.
            let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

            let next_timer_deadline = self.ingest_timer_events_and_next_deadline();
            if self.dispatch_ready_batch() {
                continue;
            }
            if let Some(event) = self.queue.try_pop() {
                self.ingest_batch(event);
                continue;
            }
            if lane_remaining > 0 {
                continue; // no park with lane backlog
            }

            let wait = next_timer_deadline
                .map(|d| d.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_millis(100));

            // One increment per park ENTRY (the loop is about to
            // block); never per message, never per spin iteration.
            self.park_count.fetch_add(1, Ordering::Relaxed);
            if let Some(event) = self.queue.pop_timeout_sync(wait) {
                self.ingest_batch(event);
            } else if self.queue.is_closed() {
                break;
            }
        });
    }

    pub async fn spin_once(&mut self, timeout: Duration) -> SpinOutcome {
        if self.queue.is_crossbeam() {
            return self.spin_once_crossbeam(timeout);
        }
        self.spin_once_tokio(timeout).await
    }

    async fn spin_once_tokio(&mut self, timeout: Duration) -> SpinOutcome {
        let deadline = Instant::now() + timeout;

        loop {
            if self.stop.load(Ordering::Relaxed) {
                // Also on the spin_once driving pattern: resumptions
                // already ingested into ready state are resumed into
                // cancellation, never dropped.
                self.resume_ready_coroutines_on_stop();
                // Flow the final lane stop-drain pass.
                self.lane_hooks.drain_to_empty();
                return SpinOutcome::Stopped;
            }

            // Flow: the lane half of the
            // combined wait — spin_once drives BOTH planes.
            let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

            self.ingest_timer_events();
            if self.dispatch_ready_batch() {
                return SpinOutcome::Event;
            }
            if let Some(event) = self.queue.try_pop() {
                self.ingest_batch(event);
                continue;
            }
            if lane_remaining > 0 {
                return SpinOutcome::Event; // lane work = progress
            }

            let now = Instant::now();
            if now >= deadline {
                return SpinOutcome::Timeout;
            }

            let wait = self.wait_duration_until(deadline);

            // One increment per park ENTRY (the loop is about to
            // block); never per message, never per spin iteration.
            self.park_count.fetch_add(1, Ordering::Relaxed);

            let event = tokio::select! {
                _ = self.notify.notified() => {
                    continue;
                }
                event = self.queue.pop_timeout(wait) => event,
            };

            match event {
                Some(event) => {
                    self.ingest_batch(event);
                }
                None => {
                    if self.queue.is_closed() {
                        return SpinOutcome::QueueClosed;
                    }
                }
            }
        }
    }

    fn spin_once_crossbeam(&mut self, timeout: Duration) -> SpinOutcome {
        maybe_block_in_place(|| {
            let deadline = std::time::Instant::now() + timeout;

            loop {
                if self.stop.load(Ordering::Relaxed) {
                    self.resume_ready_coroutines_on_stop();
                    // Flow the final lane stop-drain pass.
                    self.lane_hooks.drain_to_empty();
                    return SpinOutcome::Stopped;
                }

                // Flow: the lane half.
                let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

                let next_timer_deadline = self.ingest_timer_events_and_next_deadline();
                if self.dispatch_ready_batch() {
                    return SpinOutcome::Event;
                }
                if let Some(event) = self.queue.try_pop() {
                    self.ingest_batch(event);
                    continue;
                }
                if lane_remaining > 0 {
                    return SpinOutcome::Event; // lane work = progress
                }

                let now = std::time::Instant::now();
                if now >= deadline {
                    return SpinOutcome::Timeout;
                }

                let remaining = deadline - now;
                let wait = match next_timer_deadline {
                    Some(d) => remaining.min(d.saturating_duration_since(now.into())),
                    None => remaining,
                };

                match self.queue.pop_timeout_sync(wait) {
                    Some(event) => {
                        self.ingest_batch(event);
                    }
                    None => {
                        if self.queue.is_closed() {
                            return SpinOutcome::QueueClosed;
                        }
                    }
                }
            }
        })
    }

    pub async fn push_event(&self, event: Event) -> crate::error::AortaResult<()> {
        self.queue.push(event).await
    }

    fn ingest_timer_events(&mut self) {
        for event in self.timer_scheduler.collect_due_events() {
            self.push_ready_event(event);
        }
    }

    /// Ingest timer events and return the next timer deadline, using a single
    /// mutex acquisition instead of two separate calls.
    fn ingest_timer_events_and_next_deadline(&mut self) -> Option<Instant> {
        let (events, next_deadline) = self.timer_scheduler.collect_due_events_and_next_deadline();
        for event in events {
            self.push_ready_event(event);
        }
        next_deadline
    }

    fn ingest_batch(&mut self, first: Event) {
        match self.options.mode {
            SchedulerMode::Fifo => {
                self.fifo_state.push_back(first);
                let max_drain = self.options.priority.max_drain_per_cycle.max(1) as usize;
                for _ in 1..max_drain {
                    let Some(event) = self.queue.try_pop() else {
                        break;
                    };
                    self.fifo_state.push_back(event);
                }
            }
            SchedulerMode::Priority => {
                self.ingest_priority_batch(first);
            }
        }
    }

    fn push_ready_event(&mut self, event: Event) {
        match self.options.mode {
            SchedulerMode::Fifo => self.fifo_state.push_back(event),
            SchedulerMode::Priority => self.priority_state.push(event),
        }
    }

    /// Spin-exit drain: resumptions already INGESTED into the
    /// ready state when stop landed are resumed into cancellation on this
    /// thread — never dropped (the lane close covers un-ingested ones).
    /// Other event classes keep the existing drop-on-stop semantics.
    fn resume_ready_coroutines_on_stop(&mut self) {
        let _drain_guard = crate::flow::reentrancy::StopDrainGuard::enter();
        while let Some(event) = self.pop_ready_event() {
            if event.kind() == EventKind::CoroutineResume {
                let _ = event.run();
            }
        }
        // Backend leftovers can't hold CoroutineResume (lane-only class),
        // but the lane may still hold events if stop() hasn't drained yet —
        // closing here is idempotent with StopHandle::stop's own drain.
        self.queue
            .reserved_lane()
            .close_and_resume("executor spin exit");
    }

    #[cfg(test)]
    pub(crate) fn error_count_for_test(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn dropped_expired_for_test(&self) -> u64 {
        self.dropped_expired.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn push_ready_event_for_test(&mut self, event: Event) {
        self.push_ready_event(event);
    }

    #[cfg(test)]
    pub(crate) fn dispatch_ready_batch_for_test(&mut self) -> bool {
        self.dispatch_ready_batch()
    }

    fn dispatch_ready_batch(&mut self) -> bool {
        let Some(event) = self.pop_ready_event() else {
            return false;
        };
        let Some(event) = self.forward_timer_or_keep(event) else {
            return true; // forwarded = progress
        };
        // Resolve the group ONLY when tracing — the single-threaded path needs no
        // lock, so a registry read per dispatch would be pure overhead otherwise.
        let group = crate::trace::trace_enabled()
            .then(|| self.callback_groups.resolve_for_dispatch(&event));
        if dispatch_traced_sync(group, event) {
            self.error_count.fetch_add(1, Ordering::Relaxed);
        }

        let max_drain = self.options.priority.max_drain_per_cycle.max(1);
        for _ in 1..max_drain {
            if self.stop.load(Ordering::Relaxed) {
                break;
            }
            let Some(next) = self.pop_ready_event() else {
                break;
            };
            let Some(next) = self.forward_timer_or_keep(next) else {
                continue;
            };
            let group = crate::trace::trace_enabled()
                .then(|| self.callback_groups.resolve_for_dispatch(&next));
            if dispatch_traced_sync(group, next) {
                self.error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        true
    }

    fn time_until_next_timer_deadline(&self) -> Option<Duration> {
        self.timer_scheduler
            .next_deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    fn wait_duration_until(&self, deadline: Instant) -> Duration {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match self.time_until_next_timer_deadline() {
            Some(timer_wait) => remaining.min(timer_wait),
            None => remaining,
        }
    }

    fn ingest_priority_batch(&mut self, first: Event) {
        self.priority_state.push(first);
        // Drain all ready events into the priority state so the scheduler
        // can see the full set and reorder by priority/weight.  The
        // max_drain_per_cycle limit is enforced later at *dispatch* time,
        // not at ingestion time — otherwise the scheduler cannot promote
        // higher-priority events that arrive after lower-priority ones.
        loop {
            let Some(event) = self.queue.try_pop() else {
                break;
            };
            self.priority_state.push(event);
        }
    }
}

pub struct MultiThreadedExecutor {
    queue: EventQueue,
    nodes: Vec<AttachedNode>,
    /// See `next_executor_id`.
    executor_id: u64,
    stop: Arc<AtomicBool>,
    notify: Arc<Notify>,
    lane_hooks: Arc<LaneHookSlot>,
    options: ExecutorOptions,
    timer_scheduler: ExecutorTimerScheduler,
    priority_state: PriorityDispatchState,
    fifo_state: VecDeque<Event>,
    callback_groups: CallbackGroupRegistry,
    worker_threads: usize,
    // Monotonic spin-loop counter (mirrors SingleThreadedExecutor).
    spin_count: Arc<AtomicU64>,
    // Monotonic park-entry counter (mirrors SingleThreadedExecutor).
    park_count: Arc<AtomicU64>,
    // Monotonic count of caught callback panics (isolated dispatch
    // errors); own Arc like spin_count, shared read-only with the vital health
    // pulse. Cloned into each spawned dispatch task so a panic on any worker
    // thread is counted.
    error_count: Arc<AtomicU64>,
    /// Flow: events dropped at dequeue because their deadline passed.
    dropped_expired: Arc<AtomicU64>,
    /// delayed resumptions (post_resume_after) — shared with every
    /// StopHandle so stop can cancel sleepers promptly.
    delayed: Arc<DelayedResumes>,
}

impl MultiThreadedExecutor {
    impl_executor_shared_members!();

    /// Release every node this executor still owns, so each may be attached
    /// to another executor.
    ///
    /// Nodes that have since moved on are SKIPPED: clearing them
    /// would tear down the wiring their current executor installed.
    pub fn detach_nodes(&self) {
        for attached in &self.nodes {
            // The ownership re-check and every clear below happen INSIDE the
            // node's attachment transaction: otherwise a
            // successor's attach could land between this check and the last
            // clear, leaving the node advertising the successor's sender with
            // parts of its configuration already stripped.
            let _txn = attached.node.attachment_transaction();
            if !attached.still_attached() {
                continue;
            }
            let node = &attached.node;
            node.clear_event_sender();
            // Seam sits in the exact race window: between the
            // sender clear and the rest of the teardown.
            #[cfg(test)]
            crate::executor::detach_window_seam::pause();
            node.clear_timer_scheduler();
            node.clear_callback_group_registry();
            node.clear_executor_admission_reader();
            node.clear_executor_spin_reader();
            node.clear_executor_flow_readers();
            node.clear_executor_error_reader();
        }
    }

    pub fn new(capacity: usize) -> Self {
        Self::new_with_options(capacity, DEFAULT_MULTI_WORKERS, ExecutorOptions::default())
    }

    pub fn new_with_options(
        capacity: usize,
        worker_threads: usize,
        options: ExecutorOptions,
    ) -> Self {
        let notify = Arc::new(Notify::new());
        let mut queue =
            build_executor_queue(capacity, options.admission, options.force_tokio_queue);
        // The queue path does NOT take this `Notify`: a channel send already
        // wakes the receiver the spin loop selects on, so a second wake would
        // be per-message cost for nothing (see `EventQueueSender::try_send_queue`).
        // It stays the wake primitive for the timer scheduler and `StopHandle`.
        // the sender carries the hook slot from birth so
        // every clone sees a later attach; counters bind for shim parity.
        let lane_hooks = Arc::new(LaneHookSlot::default());
        queue.set_lane_hooks(Arc::clone(&lane_hooks));
        let error_count = Arc::new(AtomicU64::new(0));
        let dropped_expired = Arc::new(AtomicU64::new(0));
        lane_hooks.bind_counters(&error_count, &dropped_expired);
        let queue_waker = queue.waker();
        Self {
            queue,
            nodes: Vec::new(),
            stop: Arc::new(AtomicBool::new(false)),
            notify: Arc::clone(&notify),
            lane_hooks,
            options,
            timer_scheduler: ExecutorTimerScheduler::new(notify, queue_waker),
            priority_state: PriorityDispatchState::new(
                options.priority.weights,
                options.priority.map_subscription_qos,
            ),
            fifo_state: VecDeque::new(),
            callback_groups: CallbackGroupRegistry::new(),
            worker_threads: worker_threads.max(1),
            executor_id: next_executor_id(),
            spin_count: Arc::new(AtomicU64::new(0)),
            park_count: Arc::new(AtomicU64::new(0)),
            error_count,
            dropped_expired,
            delayed: DelayedResumes::new(),
        }
    }

    /// Like [`Self::create_callback_group`] but names the group so the scheduler
    /// tracer labels `sched.group_wait:<name>` / `sched.callback:<name>` instead
    /// of the opaque numeric handle.
    pub fn create_callback_group_named(
        &self,
        group_type: CallbackGroupType,
        name: Option<&str>,
    ) -> CallbackGroupHandle {
        self.callback_groups.create_group_named(group_type, name)
    }

    pub fn remove_callback_group(&self, handle: CallbackGroupHandle) -> bool {
        self.callback_groups.remove_group(handle)
    }

    pub fn queue(&self) -> &EventQueue {
        &self.queue
    }

    pub fn admission_stats(&self) -> AdmissionStats {
        self.queue.admission_stats()
    }

    pub fn admission_reader(&self) -> AdmissionStatsReader {
        self.queue.admission_reader()
    }

    /// Flow observability: events dropped at dequeue on an expired
    /// deadline.
    pub fn dropped_expired(&self) -> u64 {
        self.dropped_expired.load(Ordering::Relaxed)
    }

    /// Current spin/park/wake-signal counters.
    pub fn spin_stats(&self) -> ExecutorSpinStats {
        self.spin_stats_reader().snapshot()
    }

    /// Clonable read handle for [`Self::spin_stats`], usable after the
    /// executor has been moved onto its own thread.
    pub fn spin_stats_reader(&self) -> ExecutorSpinStatsReader {
        ExecutorSpinStatsReader {
            spin_count: Arc::clone(&self.spin_count),
            park_count: Arc::clone(&self.park_count),
            admission: self.queue.wake_signal_reader(),
        }
    }

    pub async fn spin(&mut self) {
        let mut in_flight = JoinSet::new();

        if self.queue.is_crossbeam() {
            self.spin_crossbeam(&mut in_flight);
        } else {
            self.spin_tokio(&mut in_flight).await;
        }

        while in_flight.join_next().await.is_some() {}
        // Resume ready-state coroutine resumptions into cancellation
        // on this (draining) thread before the spin returns.
        self.resume_ready_coroutines_on_stop();
        // Flow drain the attached lane pool's residue on
        // this (draining) thread — MT lane workers pop→drop after close;
        // whatever they left (ST-mode lanes attached to an MT executor,
        // or residue) drains here.
        self.lane_hooks.drain_to_empty();
    }

    async fn spin_tokio(&mut self, in_flight: &mut JoinSet<()>) {
        loop {
            self.spin_count.fetch_add(1, Ordering::Relaxed);
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            // Flow the lane half of the combined wait.
            let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

            self.ingest_timer_events();
            self.dispatch_ready(in_flight);

            if self.stop.load(Ordering::Relaxed) {
                break;
            }
            if lane_remaining > 0 {
                continue; // no park with lane backlog
            }

            // Drain the queue non-blockingly BEFORE counting
            // a park — mirrors the ST loop's guard. Without this, a saturated
            // queue reaches the select once per event with an immediately-ready
            // future: the task never blocks, yet park-entry counting reports
            // parks/msg ≈ 1 for exactly the workload the metric must classify
            // as non-parking.
            if let Some(event) = self.queue.try_pop() {
                self.ingest_batch(event);
                continue;
            }
            // Same guard for the select's other ready-arm: a worker that
            // already finished makes `join_next` immediately ready, which is a
            // free worker slot, not a block. Reap it and loop so
            // `dispatch_ready` can refill; only a genuinely pending JoinSet
            // (or empty queue + no notify) falls through to the counted park.
            if in_flight.try_join_next().is_some() {
                continue;
            }

            let timer_wait = self.time_until_next_timer_deadline();

            // One increment per park ENTRY (the loop is about to
            // block); never per message, never per spin iteration.
            self.park_count.fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                _ = self.notify.notified() => {
                    continue;
                }
                joined = in_flight.join_next(), if !in_flight.is_empty() => {
                    let _ = joined;
                    continue;
                }
                event = async {
                    match timer_wait {
                        Some(wait) => self.queue.pop_timeout(wait).await,
                        None => self.queue.pop().await,
                    }
                } => {
                    match event {
                        Some(event) => self.ingest_batch(event),
                        None => {
                            if self.queue.is_closed() {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    fn spin_crossbeam(&mut self, in_flight: &mut JoinSet<()>) {
        maybe_block_in_place(|| loop {
            self.spin_count.fetch_add(1, Ordering::Relaxed);
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            // Flow the lane half of the combined wait (this was the one
            // spin variant of the eight that never drained the attached
            // pool — lane backlog sat until a queue event happened to wake
            // the loop).
            let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

            self.drain_completed(in_flight);

            let next_timer_deadline = self.ingest_timer_events_and_next_deadline();
            self.dispatch_ready(in_flight);

            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            if let Some(event) = self.queue.try_pop() {
                self.ingest_batch(event);
                continue;
            }
            if lane_remaining > 0 {
                continue; // no park with lane backlog
            }

            let wait = next_timer_deadline
                .map(|d| d.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_millis(100));

            // One increment per park ENTRY (the loop is about to
            // block); never per message, never per spin iteration.
            self.park_count.fetch_add(1, Ordering::Relaxed);
            if let Some(event) = self.queue.pop_timeout_sync(wait) {
                self.ingest_batch(event);
            } else if self.queue.is_closed() {
                break;
            }
        });
    }

    pub async fn spin_once(&mut self, timeout: Duration) -> SpinOutcome {
        if self.queue.is_crossbeam() {
            return self.spin_once_crossbeam(timeout).await;
        }
        self.spin_once_tokio(timeout).await
    }

    async fn spin_once_tokio(&mut self, timeout: Duration) -> SpinOutcome {
        let deadline = Instant::now() + timeout;

        loop {
            if self.stop.load(Ordering::Relaxed) {
                // Also on the spin_once driving pattern: resumptions
                // already ingested into ready state are resumed into
                // cancellation, never dropped.
                self.resume_ready_coroutines_on_stop();
                // Flow the final lane stop-drain pass.
                self.lane_hooks.drain_to_empty();
                return SpinOutcome::Stopped;
            }

            // Flow spin_once drains a lane slice too — the
            // facade preserves the spin_once driving pattern verbatim
            // while the lane pool participates in each call.
            let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

            self.ingest_timer_events();
            if let Some(event) = self.pop_ready_event() {
                self.run_event_once(event).await;
                return SpinOutcome::Event;
            }
            if let Some(event) = self.queue.try_pop() {
                self.ingest_batch(event);
                continue;
            }
            if lane_remaining > 0 {
                return SpinOutcome::Event; // lane work counted as progress
            }

            let now = Instant::now();
            if now >= deadline {
                return SpinOutcome::Timeout;
            }

            let wait = self.wait_duration_until(deadline);

            // One increment per park ENTRY (the loop is about to
            // block); never per message, never per spin iteration.
            self.park_count.fetch_add(1, Ordering::Relaxed);

            let event = tokio::select! {
                _ = self.notify.notified() => {
                    continue;
                }
                event = self.queue.pop_timeout(wait) => event,
            };

            match event {
                Some(event) => {
                    self.ingest_batch(event);
                }
                None => {
                    if self.queue.is_closed() {
                        return SpinOutcome::QueueClosed;
                    }
                }
            }
        }
    }

    async fn spin_once_crossbeam(&mut self, timeout: Duration) -> SpinOutcome {
        let deadline = std::time::Instant::now() + timeout;

        loop {
            if self.stop.load(Ordering::Relaxed) {
                // Also on the spin_once driving pattern: resumptions
                // already ingested into ready state are resumed into
                // cancellation, never dropped.
                self.resume_ready_coroutines_on_stop();
                // Flow the final lane stop-drain pass.
                self.lane_hooks.drain_to_empty();
                return SpinOutcome::Stopped;
            }

            // Flow: the lane half.
            let lane_remaining = self.lane_hooks.drain(LANE_DRAIN_BATCH);

            let next_timer_deadline = self.ingest_timer_events_and_next_deadline();
            if let Some(event) = self.pop_ready_event() {
                self.run_event_once(event).await;
                return SpinOutcome::Event;
            }
            if let Some(event) = self.queue.try_pop() {
                self.ingest_batch(event);
                continue;
            }
            if lane_remaining > 0 {
                return SpinOutcome::Event; // lane work = progress
            }

            let now = std::time::Instant::now();
            if now >= deadline {
                return SpinOutcome::Timeout;
            }

            let remaining = deadline - now;
            let wait = match next_timer_deadline {
                Some(d) => remaining.min(d.saturating_duration_since(now.into())),
                None => remaining,
            };

            match maybe_block_in_place(|| self.queue.pop_timeout_sync(wait)) {
                Some(event) => {
                    self.ingest_batch(event);
                }
                None => {
                    if self.queue.is_closed() {
                        return SpinOutcome::QueueClosed;
                    }
                }
            }
        }
    }

    pub async fn push_event(&self, event: Event) -> crate::error::AortaResult<()> {
        self.queue.push(event).await
    }

    fn ingest_timer_events(&mut self) {
        for event in self.timer_scheduler.collect_due_events() {
            match self.options.mode {
                SchedulerMode::Fifo => self.fifo_state.push_back(event),
                SchedulerMode::Priority => self.priority_state.push(event),
            }
        }
    }

    /// Combined ingest + next-deadline using a single mutex acquisition.
    fn ingest_timer_events_and_next_deadline(&mut self) -> Option<Instant> {
        let (events, next_deadline) = self.timer_scheduler.collect_due_events_and_next_deadline();
        for event in events {
            match self.options.mode {
                SchedulerMode::Fifo => self.fifo_state.push_back(event),
                SchedulerMode::Priority => self.priority_state.push(event),
            }
        }
        next_deadline
    }

    fn ingest_batch(&mut self, first: Event) {
        match self.options.mode {
            SchedulerMode::Fifo => {
                self.fifo_state.push_back(first);
                let max_drain = self.options.priority.max_drain_per_cycle.max(1) as usize;
                for _ in 1..max_drain {
                    let Some(event) = self.queue.try_pop() else {
                        break;
                    };
                    self.fifo_state.push_back(event);
                }
            }
            SchedulerMode::Priority => {
                self.priority_state.push(first);
                // Drain all ready events so the priority scheduler can
                // reorder the full set (see SingleThreadedExecutor comment).
                loop {
                    let Some(event) = self.queue.try_pop() else {
                        break;
                    };
                    self.priority_state.push(event);
                }
            }
        }
    }

    /// Spin-exit drain: resumptions already INGESTED into the
    /// ready state when stop landed are resumed into cancellation on this
    /// thread — never dropped (the lane close covers un-ingested ones).
    fn resume_ready_coroutines_on_stop(&mut self) {
        let _drain_guard = crate::flow::reentrancy::StopDrainGuard::enter();
        while let Some(event) = self.pop_ready_event() {
            if event.kind() == EventKind::CoroutineResume {
                let _ = event.run();
            }
        }
        self.queue
            .reserved_lane()
            .close_and_resume("executor spin exit");
    }

    fn drain_completed(&self, in_flight: &mut JoinSet<()>) {
        while let Some(joined) = in_flight.try_join_next() {
            let _ = joined;
        }
    }

    fn dispatch_ready(&mut self, in_flight: &mut JoinSet<()>) {
        self.drain_completed(in_flight);
        while in_flight.len() < self.worker_threads {
            let Some(event) = self.pop_ready_event() else {
                break;
            };
            let Some(event) = self.forward_timer_or_keep(event) else {
                continue; // forwarded at fire
            };
            self.spawn_event(in_flight, event);
        }
    }

    fn spawn_event(&self, in_flight: &mut JoinSet<()>, event: Event) {
        let group = self.callback_groups.resolve_for_dispatch(&event);
        let error_count = Arc::clone(&self.error_count);
        in_flight.spawn(async move {
            if dispatch_traced(group, event).await {
                error_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    async fn run_event_once(&self, event: Event) {
        let Some(event) = self.forward_timer_or_keep(event) else {
            return; // forwarded at fire
        };
        let group = self.callback_groups.resolve_for_dispatch(&event);
        if dispatch_traced(group, event).await {
            self.error_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn time_until_next_timer_deadline(&self) -> Option<Duration> {
        self.timer_scheduler
            .next_deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    fn wait_duration_until(&self, deadline: Instant) -> Duration {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match self.time_until_next_timer_deadline() {
            Some(timer_wait) => remaining.min(timer_wait),
            None => remaining,
        }
    }
}

struct PriorityDispatchState {
    buckets: [VecDeque<Event>; PRIORITY_LEVELS],
    weights: [u8; PRIORITY_LEVELS],
    remaining: [u8; PRIORITY_LEVELS],
    cursor: usize,
    non_empty_mask: u8,
    map_subscription_qos: bool,
    /// Per-bucket count of deadline-carrying events: the EDF
    /// tie-break scan only runs when non-zero — zero-cost when no endpoint
    /// sets deadlines.
    deadline_count: [usize; PRIORITY_LEVELS],
}

impl PriorityDispatchState {
    fn new(weights: [u8; PRIORITY_LEVELS], map_subscription_qos: bool) -> Self {
        let weights = sanitize_weights(weights);
        Self {
            buckets: std::array::from_fn(|_| VecDeque::new()),
            remaining: weights,
            weights,
            cursor: 0,
            non_empty_mask: 0,
            map_subscription_qos,
            deadline_count: [0; PRIORITY_LEVELS],
        }
    }

    fn push(&mut self, event: Event) {
        let idx = if self.map_subscription_qos || event.kind() != EventKind::SubscriptionData {
            priority_index(event.priority())
        } else {
            priority_index(EventPriority::Normal)
        };
        if event.deadline().is_some() {
            self.deadline_count[idx] += 1;
        }
        self.buckets[idx].push_back(event);
        self.non_empty_mask |= 1u8 << idx;
    }

    fn pop(&mut self) -> Option<Event> {
        if self.non_empty_mask == 0 {
            return None;
        }
        if self.non_empty_mask.is_power_of_two() {
            let idx = self.non_empty_mask.trailing_zeros() as usize;
            if self.remaining[idx] == 0 {
                self.remaining[idx] = self.weights[idx];
            }
            let event = self.pop_bucket_front(idx);
            self.remaining[idx] = self.remaining[idx].saturating_sub(1);
            self.cursor = (idx + 1) % PRIORITY_LEVELS;
            return Some(event);
        }

        for _ in 0..PRIORITY_LEVELS {
            let idx = self.cursor;
            if (self.non_empty_mask & (1u8 << idx)) == 0 || self.remaining[idx] == 0 {
                self.cursor = (self.cursor + 1) % PRIORITY_LEVELS;
                continue;
            }
            let event = self.pop_bucket_front(idx);
            self.remaining[idx] = self.remaining[idx].saturating_sub(1);
            if self.remaining[idx] == 0 || (self.non_empty_mask & (1u8 << idx)) == 0 {
                self.cursor = (idx + 1) % PRIORITY_LEVELS;
            }
            return Some(event);
        }

        self.remaining = self.weights;
        for _ in 0..PRIORITY_LEVELS {
            let idx = self.cursor;
            if (self.non_empty_mask & (1u8 << idx)) == 0 || self.remaining[idx] == 0 {
                self.cursor = (self.cursor + 1) % PRIORITY_LEVELS;
                continue;
            }
            let event = self.pop_bucket_front(idx);
            self.remaining[idx] = self.remaining[idx].saturating_sub(1);
            if self.remaining[idx] == 0 || (self.non_empty_mask & (1u8 << idx)) == 0 {
                self.cursor = (idx + 1) % PRIORITY_LEVELS;
            }
            return Some(event);
        }

        for idx in 0..PRIORITY_LEVELS {
            if (self.non_empty_mask & (1u8 << idx)) != 0 {
                let event = self.pop_bucket_front(idx);
                self.cursor = (idx + 1) % PRIORITY_LEVELS;
                return Some(event);
            }
        }
        None
    }

    fn pop_bucket_front(&mut self, idx: usize) -> Event {
        // Flow: a cheap WITHIN-BUCKET earliest-deadline tie-break —
        // dispatch order across buckets stays priority round-robin. The scan
        // runs only while the bucket holds deadline-carrying events
        // (zero-cost when no endpoint sets deadlines).
        let event = if self.deadline_count[idx] == 0 {
            self.buckets[idx].pop_front().expect("bucket not empty")
        } else {
            let bucket = &mut self.buckets[idx];
            let earliest = bucket
                .iter()
                .enumerate()
                .filter_map(|(i, e)| e.deadline().map(|d| (i, d)))
                .min_by_key(|(_, d)| *d)
                .map(|(i, _)| i)
                .expect("deadline_count > 0 implies a deadline item");
            self.deadline_count[idx] -= 1;
            bucket.remove(earliest).expect("index in range")
        };
        if self.buckets[idx].is_empty() {
            self.non_empty_mask &= !(1u8 << idx);
        }
        event
    }
}

fn sanitize_weights(mut weights: [u8; PRIORITY_LEVELS]) -> [u8; PRIORITY_LEVELS] {
    for weight in &mut weights {
        if *weight == 0 {
            *weight = 1;
        }
    }
    weights
}

/// WRR bucket id as usize (array index into the 5 `PRIORITY_LEVELS`
/// buckets) — thin cast over the canonical `event_queue::priority_index`
/// (the canonical priority table). The Rust plane keeps all 5 buckets; the 5→4 lane
/// fold is `lane_class_for_priority`, NOT this.
fn priority_index(priority: EventPriority) -> usize {
    usize::from(crate::event_queue::priority_index(priority))
}

impl Drop for SingleThreadedExecutor {
    fn drop(&mut self) {
        // A `Node` is a shared handle: the executor holds clones in `self.nodes`,
        // so the caller's handle can outlive an executor dropped via RAII (without
        // the FFI `detach_nodes()` / `Node::close()` paths). Clear ALL executor-owned
        // readers — admission, spin and error — so a node that keeps publishing
        // vital health after its executor is gone reports the honest no-reader/zero
        // state instead of the dead executor's frozen counters.
        // Only for nodes this executor still owns — a node handed to
        // another executor keeps ITS readers. Each node's check and clears run
        // inside its attachment transaction so a concurrent attach cannot be
        // half-undone.
        for attached in &self.nodes {
            let _txn = attached.node.attachment_transaction();
            if !attached.still_attached() {
                continue;
            }
            let node = &attached.node;
            node.clear_executor_admission_reader();
            node.clear_executor_spin_reader();
            node.clear_executor_flow_readers();
            node.clear_executor_error_reader();
        }
    }
}

impl Drop for MultiThreadedExecutor {
    fn drop(&mut self) {
        // See SingleThreadedExecutor::drop — clear every executor-owned reader so a
        // node outliving its executor does not report frozen spin/error counters.
        // Only for nodes this executor still owns — a node handed to
        // another executor keeps ITS readers. Each node's check and clears run
        // inside its attachment transaction so a concurrent attach cannot be
        // half-undone.
        for attached in &self.nodes {
            let _txn = attached.node.attachment_transaction();
            if !attached.still_attached() {
                continue;
            }
            let node = &attached.node;
            node.clear_executor_admission_reader();
            node.clear_executor_spin_reader();
            node.clear_executor_flow_readers();
            node.clear_executor_error_reader();
        }
    }
}

#[cfg(test)]
impl SingleThreadedExecutor {
    pub(crate) fn scheduler_mode(&self) -> SchedulerMode {
        self.options.mode
    }

    pub(crate) fn map_subscription_qos_enabled(&self) -> bool {
        self.options.priority.map_subscription_qos
    }

    pub(crate) fn priority_weights(&self) -> [u8; PRIORITY_LEVELS] {
        self.priority_state.weights
    }

    pub(crate) fn max_drain_per_cycle(&self) -> u32 {
        self.options.priority.max_drain_per_cycle.max(1)
    }
}

#[cfg(test)]
impl MultiThreadedExecutor {
    pub(crate) fn scheduler_mode(&self) -> SchedulerMode {
        self.options.mode
    }

    pub(crate) fn callback_group_type(
        &self,
        handle: CallbackGroupHandle,
    ) -> Option<CallbackGroupType> {
        self.callback_groups.group_type(handle)
    }
}

#[cfg(test)]
#[path = "../test/executor_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/executor_advanced_tests.rs"]
mod advanced_tests;

#[cfg(test)]
#[path = "../test/executor_park_stats_tests.rs"]
mod park_stats_tests;

#[cfg(test)]
#[path = "../test/sched_contention_unit_tests.rs"]
mod sched_contention_unit_tests;

#[cfg(test)]
mod delayed_resume_tests {
    use super::*;

    #[test]
    fn insert_after_drain_returns_the_fire_closure() {
        // A post racing stop must never sleep out its delay — a
        // closed registry hands the closure back for IMMEDIATE inline
        // cancelled delivery.
        let delayed = DelayedResumes::new();
        let hit = Arc::new(AtomicU64::new(0));
        let h1 = Arc::clone(&hit);
        assert!(delayed
            .insert(Box::new(move |cancelled| {
                assert!(cancelled);
                h1.fetch_add(1, Ordering::SeqCst);
            }))
            .is_ok());
        delayed.drain_cancel();
        assert_eq!(hit.load(Ordering::SeqCst), 1, "pre-drain entry fired");
        let h2 = Arc::clone(&hit);
        match delayed.insert(Box::new(move |cancelled| {
            assert!(cancelled);
            h2.fetch_add(10, Ordering::SeqCst);
        })) {
            Err(fire) => fire(true),
            Ok(_) => panic!("insert into a closed registry must be refused"),
        }
        assert_eq!(hit.load(Ordering::SeqCst), 11);
        assert_eq!(delayed.len(), 0);
    }
}
