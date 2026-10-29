use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use smallbox::{smallbox, SmallBox};
#[cfg(target_os = "linux")]
use tokio::runtime::{Handle as TokioHandle, RuntimeFlavor};
use tokio::sync::mpsc;
use tokio::sync::Notify;

use crate::error::{AortaError, AortaResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionDropReason {
    QueueFull,
    QueueClosed,
    BudgetEvents,
    BudgetBytes,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmissionStats {
    pub queued_events: u64,
    pub queued_bytes: u64,
    pub peak_queued_events: u64,
    pub peak_queued_bytes: u64,
    pub dropped_queue_full: u64,
    pub dropped_queue_closed: u64,
    pub dropped_budget_events: u64,
    pub dropped_budget_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct AdmissionStatsReader {
    admission: Arc<AdmissionState>,
}

impl AdmissionStatsReader {
    pub fn snapshot(&self) -> AdmissionStats {
        self.admission.snapshot()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EventQueueOptions {
    pub max_queued_events: usize,
    pub max_queued_bytes: usize,
    pub admission_observability: bool,
}

/// Cache-line stride used to keep the producer-written and consumer-written
/// admission counters apart (PERF: see [`AdmissionState`]).
///
/// 64 B is the line size on x86-64 and on the Cortex-A55/A78 cores Aorta ships
/// to. On a machine with a wider line the separation merely under-shoots — the
/// counters stay *correct*, they just may share a line again — so the small,
/// portable constant is preferred over per-arch `cfg`.
const ADMISSION_CACHE_LINE: usize = 64;

/// Aligns its payload to a cache line so neighbouring fields cannot land on the
/// same line. The alignment also pads `T` out to the line, so consecutive
/// `CacheLine` fields never share one.
#[repr(align(64))]
#[derive(Debug, Default)]
struct CacheLine<T>(T);

const _: () = assert!(std::mem::align_of::<CacheLine<u8>>() == ADMISSION_CACHE_LINE);

/// One side's monotonic totals. Written by exactly one role (producer or
/// consumer), read by both.
#[derive(Debug, Default)]
struct SideTotals {
    events: AtomicU64,
    bytes: AtomicU64,
}

impl SideTotals {
    #[inline]
    fn load(&self) -> (u64, u64) {
        (
            self.events.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

/// Immutable admission configuration — read on every `can_admit`, never written
/// after construction.
#[derive(Debug, Default)]
struct AdmissionConfig {
    max_queued_events: usize,
    max_queued_bytes: usize,
    admission_observability: bool,
}

/// Observability-only counters: touched exclusively when
/// `admission_observability` is on (off by default), so they are parked on
/// their own line away from the hot totals.
#[derive(Debug, Default)]
struct AdmissionObservability {
    peak_queued_events: AtomicU64,
    peak_queued_bytes: AtomicU64,
    dropped_queue_full: AtomicU64,
    dropped_queue_closed: AtomicU64,
    dropped_budget_events: AtomicU64,
    dropped_budget_bytes: AtomicU64,
}

/// Admission accounting for one event queue.
///
/// # Occupancy is a producer/consumer pair, not a shared counter (PERF)
///
/// Occupancy used to be a single `queued_events` / `queued_bytes` pair that the
/// producer `fetch_add`-ed on enqueue and the consumer ran a `fetch_update` CAS
/// **loop** over on dequeue. On the pub/sub hot path the producer (the zenoh RX
/// / acceptance thread) and the consumer (the executor thread) sit on different
/// cores, so that is a guaranteed per-message ping-pong: every message dragged
/// the same line through Modified state on both cores, and the CAS loop could
/// spin while it did.
///
/// The counters are now **split by writer**: [`Self::enqueued`] is written only
/// by producers, [`Self::dequeued`] only by consumers, and each sits on its own
/// cache line alongside the immutable [`AdmissionConfig`] and the cold
/// observability block. Occupancy is derived — `enqueued - dequeued`,
/// saturating — so:
///
/// * the underflow clamp that motivated the CAS loop is now structural (both
///   sides are monotonic and the subtraction saturates), and dequeue is a plain
///   `fetch_add`;
/// * in the default configuration (no budget, observability off) neither role
///   reads the other's line at all — the sharing is gone, not merely reduced.
///
/// Readers load the **consumer** total first so the derived occupancy can only
/// over-estimate, never under-estimate: a budget is never overshot because of a
/// skewed read.
///
/// # Accuracy of the two tiers
///
/// `queued_events` / `queued_bytes` — which feed `EventQueue::len` and the
/// admission budgets — are **exact**: every enqueue and every dequeue is
/// recorded on a monotonic counter, so the derived occupancy always returns to
/// zero on a drained queue. (The pre-split counter could not promise this: its
/// `saturating_sub` discarded a decrement whenever a dequeue observed zero,
/// leaving a small persistent over-count equal to the burst depth of that race.
/// `dequeue_winning_the_race_keeps_occupancy_exact_and_peak_at_least_one`
/// pins the new behaviour.)
///
/// `peak_queued_events` / `peak_queued_bytes` are an **observability-only**
/// high-water diagnostic, computed solely when `admission_observability` is on
/// (off by default). They may under-report — see the note in
/// [`Self::on_enqueued`] — because the enqueue is published to the channel
/// before it is counted. The skew is bounded, one-directional, and smallest
/// under backlog, which is when the mark is worth reading.
#[doc(hidden)]
#[derive(Debug)]
pub struct AdmissionState {
    config: CacheLine<AdmissionConfig>,
    /// Producer-owned monotonic totals (never decremented).
    enqueued: CacheLine<SideTotals>,
    /// Consumer-owned monotonic totals (never decremented).
    dequeued: CacheLine<SideTotals>,
    observability: CacheLine<AdmissionObservability>,
    /// Producer-owned count of events actually PUBLISHED to the backend
    /// channel — one per wake the sender attempts (tokio's mpsc fires the
    /// receiver's waker on every successful send; whether that costs a syscall
    /// depends on whether the consumer was parked, which the executor's park
    /// counter measures). Distinct from [`Self::enqueued`], which counts
    /// *reservations*: a reservation whose send is then refused is charged
    /// there and released, but never signals a wake.
    ///
    /// It lives here because `AdmissionState` is the one `Arc` every sender
    /// backend and the owning [`EventQueue`] already share — threading a
    /// second `Arc<AtomicU64>` through all seven sender constructors would
    /// duplicate that plumbing for a read-only diagnostic. It is on its own
    /// cache line for the same reason the totals are.
    wake_signals: CacheLine<AtomicU64>,
}

impl AdmissionState {
    fn new(options: EventQueueOptions) -> Self {
        Self {
            config: CacheLine(AdmissionConfig {
                max_queued_events: options.max_queued_events,
                max_queued_bytes: options.max_queued_bytes,
                admission_observability: options.admission_observability,
            }),
            enqueued: CacheLine(SideTotals::default()),
            dequeued: CacheLine(SideTotals::default()),
            observability: CacheLine(AdmissionObservability::default()),
            wake_signals: CacheLine(AtomicU64::new(0)),
        }
    }

    /// Record one wake signal — call on every send that reached the
    /// backend channel, never on a refused one. One Relaxed increment on a
    /// producer-owned line.
    #[inline]
    fn on_wake_signal(&self) {
        self.wake_signals.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Monotonic count of published sends. See [`Self::wake_signals`].
    #[inline]
    pub(crate) fn wake_signal_count(&self) -> u64 {
        self.wake_signals.0.load(Ordering::Relaxed)
    }

    /// Derived occupancy `(events, bytes)`. See the type docs for why the
    /// consumer total is loaded first.
    #[inline]
    fn queued(&self) -> (u64, u64) {
        let (dequeued_events, dequeued_bytes) = self.dequeued.0.load();
        let (enqueued_events, enqueued_bytes) = self.enqueued.0.load();
        (
            enqueued_events.saturating_sub(dequeued_events),
            enqueued_bytes.saturating_sub(dequeued_bytes),
        )
    }

    /// Reserve one event's worth of admission **before** the entry is
    /// published to the backend channel, returning `Err` if a configured
    /// budget is already full.
    ///
    /// # Why reservation must precede publication
    ///
    /// This used to be a `can_admit` query followed, *after* a successful
    /// `tx.try_send`, by an `on_enqueued` counter bump. That ordering has a
    /// window in which the budget can be exceeded:
    ///
    /// 1. producer A publishes A — it is now consumable — but is descheduled
    ///    before recording the enqueue;
    /// 2. the consumer pops A and records the dequeue, so `dequeued` counts an
    ///    event `enqueued` does not;
    /// 3. producer B reserves and publishes B; `enqueued == dequeued`, so the
    ///    derived occupancy reads **zero while B is genuinely queued**;
    /// 4. producer C is admitted against a budget of one.
    ///
    /// Counting the reservation first closes it: an event is charged to the
    /// budget from before the instant it becomes visible to a consumer until
    /// the consumer accounts for it, so occupancy is never an under-estimate.
    /// A send that then fails hands the slot back via
    /// [`release_reservation`](Self::release_reservation).
    ///
    /// # Cost
    ///
    /// The unbudgeted configuration — the default, and the only one any
    /// in-tree caller uses — keeps exactly the accounting it had before: two
    /// producer-local `fetch_add`s, no loop, no read of the consumer's line.
    /// The compare-exchange below runs **only** when a budget is configured,
    /// and it contends producers against producers on the producer-owned line;
    /// it does not reintroduce the producer↔consumer ping-pong that the split
    /// counters removed.
    fn try_reserve(&self, bytes: usize) -> Result<(), AdmissionDropReason> {
        let config = &self.config.0;
        let bytes = bytes as u64;

        if config.max_queued_events == 0 && config.max_queued_bytes == 0 {
            // Fast path: no budget to enforce, so nothing to check against.
            let events_total = self.enqueued.0.events.fetch_add(1, Ordering::Relaxed) + 1;
            let bytes_total = self.enqueued.0.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
            self.record_peak(events_total, bytes_total, bytes);
            return Ok(());
        }

        // Budgeted: the check and the reservation have to be ONE atomic step,
        // or two producers both observe a free slot and both take it.
        let max_events = config.max_queued_events as u64;
        let events_total = loop {
            // Load the consumer's total FIRST so the derived occupancy can only
            // over-estimate — the conservative direction for a budget.
            let dequeued_events = self.dequeued.0.events.load(Ordering::Relaxed);
            let current = self.enqueued.0.events.load(Ordering::Relaxed);
            if max_events > 0 && current.saturating_sub(dequeued_events) >= max_events {
                return Err(AdmissionDropReason::BudgetEvents);
            }
            match self.enqueued.0.events.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break current + 1,
                Err(_) => continue,
            }
        };

        let bytes_total = self.enqueued.0.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if config.max_queued_bytes > 0 {
            let dequeued_bytes = self.dequeued.0.bytes.load(Ordering::Relaxed);
            if bytes_total.saturating_sub(dequeued_bytes) > config.max_queued_bytes as u64 {
                // Roll the whole reservation back before refusing, so a byte
                // rejection cannot leak the event slot taken above.
                self.release_reservation(bytes as usize);
                return Err(AdmissionDropReason::BudgetBytes);
            }
        }

        self.record_peak(events_total, bytes_total, bytes);
        Ok(())
    }

    /// Hand back a reservation whose send did not make it into the channel.
    ///
    /// Symmetric with [`try_reserve`](Self::try_reserve) and expressed in the
    /// monotonic model: releasing is an increment of the consumer-side total,
    /// exactly as a real dequeue is, so occupancy returns to where it was with
    /// no CAS and no counter ever moving backwards.
    fn release_reservation(&self, bytes: usize) {
        self.dequeued.0.events.fetch_add(1, Ordering::Relaxed);
        if bytes > 0 {
            self.dequeued
                .0
                .bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    /// High-water bookkeeping, shared by both `try_reserve` arms.
    ///
    /// Note the mark is now taken at RESERVATION time, so a reservation that is
    /// later released by a failed send can leave the peak one event high. That
    /// is the correct trade for a diagnostic that is off by default: the
    /// alternative is deferring the mark until the send succeeds, which would
    /// reopen the under-reporting this floor exists to prevent.
    #[inline]
    fn record_peak(&self, enqueued_events: u64, enqueued_bytes: u64, bytes: u64) {
        if !self.config.0.admission_observability {
            return;
        }
        // Only the observability path pays for reading the consumer's line.
        let (dequeued_events, dequeued_bytes) = self.dequeued.0.load();
        let observability = &self.observability.0;
        // Floor the candidate at this event's OWN contribution: concurrent
        // dequeues of *other* events can still cancel it out, and `fetch_max`
        // never lowers an existing mark, so the floor can only restore a lost
        // observation.
        observability.peak_queued_events.fetch_max(
            enqueued_events.saturating_sub(dequeued_events).max(1),
            Ordering::Relaxed,
        );
        observability.peak_queued_bytes.fetch_max(
            enqueued_bytes.saturating_sub(dequeued_bytes).max(bytes),
            Ordering::Relaxed,
        );
    }

    fn on_dequeued(&self, bytes: usize) {
        // Monotonic: the clamp against a spurious dequeue lives in `queued()`'s
        // `saturating_sub`, so this side needs no CAS loop.
        self.dequeued.0.events.fetch_add(1, Ordering::Relaxed);
        if bytes > 0 {
            self.dequeued
                .0
                .bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    fn on_drop(&self, reason: AdmissionDropReason) {
        if !self.config.0.admission_observability {
            return;
        }
        let observability = &self.observability.0;
        match reason {
            AdmissionDropReason::QueueFull => {
                observability
                    .dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionDropReason::QueueClosed => {
                observability
                    .dropped_queue_closed
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionDropReason::BudgetEvents => {
                observability
                    .dropped_budget_events
                    .fetch_add(1, Ordering::Relaxed);
            }
            AdmissionDropReason::BudgetBytes => {
                observability
                    .dropped_budget_bytes
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> AdmissionStats {
        let (queued_events, queued_bytes) = self.queued();
        let observability = &self.observability.0;
        let (
            peak_queued_events,
            peak_queued_bytes,
            dropped_queue_full,
            dropped_queue_closed,
            dropped_budget_events,
            dropped_budget_bytes,
        ) = if self.config.0.admission_observability {
            (
                observability.peak_queued_events.load(Ordering::Relaxed),
                observability.peak_queued_bytes.load(Ordering::Relaxed),
                observability.dropped_queue_full.load(Ordering::Relaxed),
                observability.dropped_queue_closed.load(Ordering::Relaxed),
                observability.dropped_budget_events.load(Ordering::Relaxed),
                observability.dropped_budget_bytes.load(Ordering::Relaxed),
            )
        } else {
            (0, 0, 0, 0, 0, 0)
        };
        AdmissionStats {
            queued_events,
            queued_bytes,
            peak_queued_events,
            peak_queued_bytes,
            dropped_queue_full,
            dropped_queue_closed,
            dropped_budget_events,
            dropped_budget_bytes,
        }
    }

    fn queued_events(&self) -> usize {
        self.queued().0 as usize
    }
}

/// Inline storage budget for [`EventCallback`], in `usize` words.
///
/// # Why this number (PERF — do not shrink without re-measuring)
///
/// The pub/sub hot-path callback is the subscription closure built in
/// `core/src/subscriber.rs` (`Event::new_with_priority` at the RX callback).
/// Its capture set measures **184 bytes** on 64-bit targets:
///
/// | capture | bytes |
/// |---|---|
/// | `Arc<SubscriberSharedCtx>` | 8 |
/// | `payload: ZBytes` | 40 |
/// | `cb_link: Option<SpanLink>` | 40 |
/// | `source_identity: Option<String>` | 24 |
/// | `source_namespace: Option<String>` | 24 |
/// | `attachment_hash: Option<u64>` | 16 |
/// | `publisher_stamp_ns: Option<i64>` | 16 |
/// | `receive_stamp_ns` + `Priority` + 3 × `bool` | 16 |
///
/// The previous budget (`S16` = 16 words = 128 bytes) did not fit it, so every
/// received message heap-allocated the closure on the zenoh RX / acceptance
/// thread and freed it on the executor thread. A cross-thread free leaves the
/// producer's tcache cold and pushes both sides into the shared arena, where
/// glibc spins before it futexes — user CPU up, syscall count flat.
///
/// 24 words (192 bytes) is the smallest `usize`-word budget that fits the
/// closure; the cost is a wider [`Event`] to `memcpy` into the channel slot,
/// which is why the value is measured rather than rounded up to `S32`.
/// `event_callback_budget_fits_subscription_capture` (core/test) pins the
/// contract against a mirror of the capture set — update BOTH when the
/// subscription closure's captures change.
pub const EVENT_CALLBACK_INLINE_WORDS: usize = 24;

/// Inline space token for [`EventCallback`] — `smallbox` sizes and aligns the
/// inline buffer from this type. See [`EVENT_CALLBACK_INLINE_WORDS`].
#[doc(hidden)]
pub struct EventCallbackSpace([usize; EVENT_CALLBACK_INLINE_WORDS]);

/// Number of bytes an [`EventCallback`] can hold without touching the heap.
pub const EVENT_CALLBACK_INLINE_BYTES: usize = std::mem::size_of::<EventCallbackSpace>();

/// Event callback stored inline when it fits [`EVENT_CALLBACK_INLINE_BYTES`],
/// falling back to heap allocation for larger closures.
///
/// Uses `FnMut` instead of `FnOnce` because `SmallBox` cannot invoke `FnOnce`
/// trait objects on stable Rust (only `Box` has compiler special-casing for this).
/// Each callback is wrapped via `wrap_fn_once` to guarantee single invocation.
pub type EventCallback = SmallBox<dyn FnMut() + Send + 'static, EventCallbackSpace>;

/// Wrap a `FnOnce` closure into a `FnMut` closure suitable for `EventCallback`.
/// The inner `Option` ensures the original closure is called at most once.
#[inline]
fn wrap_fn_once<F: FnOnce() + Send + 'static>(f: F) -> EventCallback {
    let mut opt = Some(f);
    smallbox!(move || {
        if let Some(inner) = opt.take() {
            inner();
        }
    })
}

pub type CallbackGroupHandle = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallbackGroupType {
    MutuallyExclusive,
    Reentrant,
}

/// Sender for the executor's cross-thread ingress: an admission-governed
/// backend channel for the ordinary event classes, plus the reserved
/// [`CoroutineResume`](EventKind::CoroutineResume) lane shared with the queue.
///
/// Note: the standalone channel constructors ([`from_bounded_channel`]
/// (Self::from_bounded_channel) / [`from_unbounded_channel`]
/// (Self::from_unbounded_channel)) carry an inert (closed-from-birth) lane —
/// their raw receivers cannot drain one, so a `CoroutineResume` send through them
/// fails loudly with `Closed` instead of leaking silently.
#[derive(Clone, Debug)]
pub struct EventQueueSender {
    backend: SenderBackend,
    lane: Arc<ReservedLane>,
    /// the executor's lane-hook slot. Present from
    /// construction (interior decides attached-ness) so every cloned
    /// sender sees an attach that happens later.
    lane_hooks: Option<Arc<crate::executor::LaneHookSlot>>,
}

/// Backend channel abstraction (bounded / unbounded / multi-ingress /
/// crossbeam) behind [`EventQueueSender`].
#[derive(Clone, Debug)]
enum SenderBackend {
    Bounded {
        tx: mpsc::Sender<Event>,
        admission: Arc<AdmissionState>,
    },
    Unbounded {
        tx: mpsc::UnboundedSender<Event>,
        admission: Arc<AdmissionState>,
    },
    MultiIngress {
        tx: Arc<MultiIngressSender>,
        admission: Arc<AdmissionState>,
    },
    /// Lock-free crossbeam channel + Linux eventfd for low-latency timer dispatch.
    #[cfg(target_os = "linux")]
    Crossbeam {
        tx: crate::crossbeam_queue::CrossbeamEventfdSender,
        admission: Arc<AdmissionState>,
    },
}

#[derive(Clone)]
pub(crate) struct EventQueueWaker {
    #[cfg(target_os = "linux")]
    crossbeam: crate::crossbeam_queue::CrossbeamEventfdWaker,
}

impl EventQueueWaker {
    pub(crate) fn wake(&self) {
        #[cfg(target_os = "linux")]
        self.crossbeam.wake();
    }
}

/// Counters for the reserved `CoroutineResume` lane.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReservedLaneStats {
    /// Events currently queued on the lane.
    pub occupancy: u64,
    /// Sticky maximum occupancy observed since queue creation.
    pub high_water: u64,
    /// Total events ever pushed onto the lane.
    pub pushed_total: u64,
}

/// One-shot WARN threshold for reserved-lane
/// occupancy — far above any disciplined workload (one resume per live
/// frame), cheap insurance against a runaway raw-ABI post loop.
pub(crate) const RESERVED_LANE_HIGH_WATER_WARN: u64 = 65_536;

/// The reserved, admission-bypassing lane for `EventKind::CoroutineResume`.
///
/// A `CoroutineResume` event is the continuation of already-admitted in-flight
/// work, so it must never hit the admission drop that governs *new* work: a
/// push bypasses `AdmissionState` and the backend channels entirely; the only
/// legal failure is a closed queue. "Full" is structurally unrepresentable —
/// the queue is unbounded. **Memory bounded-ness is NOT a lane property**:
/// it is a per-source DISCIPLINE of the callers —
/// the internal Rust paths ride `aorta_sync::WakeSlot` (at most one wake
/// outstanding per live source) and the shipped C++ awaitables are
/// structurally one-outstanding-resume-per-frame, but the raw C-ABI post
/// surface (`aorta_executor_post_resume`/`aorta_scope_spawn`) can violate it.
/// Occupancy is observable through [`ReservedLaneStats`], and crossing
/// [`RESERVED_LANE_HIGH_WATER_WARN`] fires a one-shot WARN.
///
/// One lane instance is shared by an [`EventQueue`] and every
/// [`EventQueueSender`] cloned from it (including a bridged multi-ingress
/// sender wrapping the queue's sender), uniformly across all backend kinds.
/// Consumers drain it ahead of the backend classes; dispatch-order fairness
/// still comes from the executor's priority buckets, which ingest everything
/// before dispatching.
///
/// Wake paths: `notify` wakes the async `pop`/`pop_timeout` paths; on the
/// crossbeam backend the eventfd `waker` additionally breaks the executor's
/// synchronous `pop_timeout_sync` wait (an external wake with an empty
/// channel returns `None` there, and the spin loop's next `try_pop` observes
/// the lane).
pub struct ReservedLane {
    inner: Mutex<ReservedLaneInner>,
    notify: Notify,
    occupancy: AtomicUsize,
    high_water: AtomicUsize,
    pushed_total: AtomicU64,
    /// Sticky flag for the one-shot high-water WARN.
    growth_warned: std::sync::atomic::AtomicBool,
    #[cfg(target_os = "linux")]
    waker: std::sync::OnceLock<EventQueueWaker>,
}

/// Lane state guarded by one lock: keeping `closed` inside the same critical
/// section as the queue makes close-vs-push race-free — a push can never land
/// in a lane that will not be drained and still report success (the TOCTOU a
/// separate pre-push closed check would reintroduce).
struct ReservedLaneInner {
    queue: VecDeque<Event>,
    closed: bool,
}

impl ReservedLane {
    fn new() -> Arc<Self> {
        Self::with_closed(false)
    }

    /// A lane that is closed from birth — used by the standalone channel
    /// constructors, whose raw receivers cannot drain a lane: a `CoroutineResume`
    /// send through them fails loudly with `Closed` instead of leaking.
    fn new_inert() -> Arc<Self> {
        Self::with_closed(true)
    }

    fn with_closed(closed: bool) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(ReservedLaneInner {
                queue: VecDeque::new(),
                closed,
            }),
            notify: Notify::new(),
            occupancy: AtomicUsize::new(0),
            growth_warned: std::sync::atomic::AtomicBool::new(false),
            high_water: AtomicUsize::new(0),
            pushed_total: AtomicU64::new(0),
            #[cfg(target_os = "linux")]
            waker: std::sync::OnceLock::new(),
        })
    }

    /// Push onto the lane. `Err(event)` iff the lane is closed (the owning
    /// `EventQueue` was dropped) — the one legal failure; the closed check
    /// happens under the same lock as the push, so success means the event
    /// was still drainable at the instant it was enqueued.
    fn push(&self, event: Event) -> Result<(), Event> {
        let should_warn = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inner.closed {
                return Err(event);
            }
            inner.queue.push_back(event);
            // Increment inside the critical section so a non-zero occupancy
            // is visible whenever an item is committed to the queue.
            let occupancy = self.occupancy.fetch_add(1, Ordering::Release) + 1;
            self.high_water.fetch_max(occupancy, Ordering::Relaxed);
            // Sticky-flag CHECK only inside the lock; the log
            // write happens after release (log I/O under a hot mutex
            // stalls every producer).
            occupancy > RESERVED_LANE_HIGH_WATER_WARN as usize
                && !self.growth_warned.swap(true, Ordering::Relaxed)
        };
        if should_warn {
            // One-shot: unbounded growth here means a raw C-ABI caller is
            // posting without the one-outstanding-resume-per-frame
            // discipline (or the executor stopped draining).
            crate::aorta_core_warn!(
                "Aorta WARN: reserved CoroutineResume lane exceeded {} queued resumes — a C-ABI caller is posting without the one-outstanding-resume-per-frame discipline, or the executor is not draining",
                RESERVED_LANE_HIGH_WATER_WARN
            );
        }
        self.pushed_total.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
        #[cfg(target_os = "linux")]
        if let Some(waker) = self.waker.get() {
            waker.wake();
        }
        Ok(())
    }

    fn try_pop(&self) -> Option<Event> {
        // Fast path: the lane is empty almost always; one Acquire load keeps
        // the executor's per-iteration cost at a branch (zero-cost-when-off).
        if self.occupancy.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let event = inner.queue.pop_front();
        if event.is_some() {
            self.occupancy.fetch_sub(1, Ordering::Release);
        }
        event
    }

    /// [`close`](Self::close), then RUN every drained event on the calling
    /// thread — the shutdown rule: an accepted resumption is
    /// resumed into cancellation (its frame observes the cancelled state and
    /// unwinds; destructors run on the draining thread), NEVER silently
    /// dropped. This is what keeps `aorta_executor_post_resume`'s
    /// success-means-exactly-once contract true across a concurrent stop
    /// (drop-on-drain once leaked C++ frames and hung
    /// scope close). A drained event that re-posts observes the now-closed
    /// lane and continues inline — same rule, applied recursively.
    /// `what` names the closing context for the log line.
    pub(crate) fn close_and_resume(&self, what: &str) -> usize {
        let drained = self.close();
        let count = drained.len();
        if count > 0 {
            crate::aorta_core_warn!(
                "Aorta WARN: {}: resuming {} undelivered reserved-lane event(s)                  into cancellation on the closing thread",
                what,
                count
            );
            // Posted resumptions delivered under this flag report
            // AORTA_ERR_CANCELLED to their resume fn (teardown-visible).
            let _drain_guard = crate::flow::reentrancy::StopDrainGuard::enter();
            for event in drained {
                // Panic-isolated like any dispatch; the callback-context
                // guard applies (a blocking FFI re-entry classifies
                // reentrant, not deadlocking the close).
                let _ = event.run();
            }
        }
        count
    }

    /// Close the lane (owning queue teardown or executor stop): bars further
    /// pushes and hands back what was never dispatched — the caller MUST
    /// resume the drained events (see [`Self::close_and_resume`]); events
    /// leave the lock before running (their closures must not run under it).
    fn close(&self) -> VecDeque<Event> {
        let drained = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inner.closed {
                return VecDeque::new();
            }
            inner.closed = true;
            let drained = std::mem::take(&mut inner.queue);
            if !drained.is_empty() {
                self.occupancy.fetch_sub(drained.len(), Ordering::Release);
            }
            drained
        };
        drained
    }

    async fn notified(&self) {
        self.notify.notified().await;
    }

    pub(crate) fn stats(&self) -> ReservedLaneStats {
        ReservedLaneStats {
            occupancy: self.occupancy.load(Ordering::Relaxed) as u64,
            high_water: self.high_water.load(Ordering::Relaxed) as u64,
            pushed_total: self.pushed_total.load(Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for ReservedLane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReservedLane")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl EventQueueSender {
    pub fn from_bounded_channel(tx: mpsc::Sender<Event>) -> Self {
        Self {
            backend: SenderBackend::Bounded {
                tx,
                admission: Arc::new(AdmissionState::new(EventQueueOptions::default())),
            },
            lane: ReservedLane::new_inert(),
            lane_hooks: None,
        }
    }

    pub fn from_unbounded_channel(tx: mpsc::UnboundedSender<Event>) -> Self {
        Self {
            backend: SenderBackend::Unbounded {
                tx,
                admission: Arc::new(AdmissionState::new(EventQueueOptions::default())),
            },
            lane: ReservedLane::new_inert(),
            lane_hooks: None,
        }
    }

    pub fn try_new_multi_ingress(
        main_sender: EventQueueSender,
        options: MultiIngressOptions,
    ) -> AortaResult<Self> {
        match &main_sender.backend {
            SenderBackend::MultiIngress { .. } => Err(AortaError::invalid_argument(
                "main sender for multi-ingress cannot be another multi-ingress sender",
            )),
            #[cfg(target_os = "linux")]
            SenderBackend::Crossbeam { .. } => Err(AortaError::invalid_argument(
                "main sender for multi-ingress cannot be a crossbeam sender",
            )),
            SenderBackend::Bounded { .. } | SenderBackend::Unbounded { .. } => {
                // The ingress wrapper shares the main queue's reserved lane:
                // a CoroutineResume pushed through it must surface on the queue
                // the executor drains, never on a private lane and never
                // through the bridge hop.
                let lane = Arc::clone(&main_sender.lane);
                let ingress = MultiIngressSender::new(main_sender, options)?;
                let admission = ingress.admission();
                Ok(Self {
                    backend: SenderBackend::MultiIngress {
                        tx: Arc::new(ingress),
                        admission,
                    },
                    lane,
                    lane_hooks: None,
                })
            }
        }
    }

    /// Push a `CoroutineResume` event onto the reserved lane (admission bypass).
    /// The only legal failure is a closed lane (owning queue dropped, or a
    /// standalone-constructed sender's inert lane); the check-and-push is
    /// atomic under the lane lock, so `Ok` means the event was drainable when
    /// enqueued.
    ///
    /// Wake path: `ReservedLane::push` fires the lane's own `Notify` (which
    /// every parked `EventQueue::pop`/`pop_timeout` selects on) and, on the
    /// crossbeam backend, the eventfd waker. The executor's outer `Notify` is
    /// deliberately NOT fired — the executor only parks inside
    /// `pop`/`pop_timeout`/`pop_timeout_sync`, all lane-aware, so a second wake
    /// would be redundant cost on every lane push. The backend-channel paths
    /// follow the same rule (see [`Self::try_send_queue`]).
    fn push_reserved(&self, event: Event) -> Result<(), Event> {
        self.lane.push(event)
    }

    fn backend_is_closed(&self) -> bool {
        match &self.backend {
            SenderBackend::Bounded { tx, .. } => tx.is_closed(),
            SenderBackend::Unbounded { tx, .. } => tx.is_closed(),
            SenderBackend::MultiIngress { tx, .. } => tx.is_closed(),
            #[cfg(target_os = "linux")]
            SenderBackend::Crossbeam { tx, .. } => tx.is_closed(),
        }
    }

    /// Admission counters snapshot WITHOUT the executor mutex:
    /// every backend shares the queue's `AdmissionState`
    /// (admission is enforced sender-side), so this reads the same
    /// counters as [`EventQueue::admission_stats`]. LOAD-BEARING for the
    /// Flow C-ABI: `aorta_executor_get_admission_stats` must not lock
    /// `entry.exec` — `spin` holds that mutex for its whole run, so a
    /// monitor thread would block until stop and a dispatched callback
    /// would self-deadlock (parking_lot is non-reentrant).
    pub fn admission_stats(&self) -> AdmissionStats {
        match &self.backend {
            SenderBackend::Bounded { admission, .. }
            | SenderBackend::Unbounded { admission, .. }
            | SenderBackend::MultiIngress { admission, .. } => admission.snapshot(),
            #[cfg(target_os = "linux")]
            SenderBackend::Crossbeam { admission, .. } => admission.snapshot(),
        }
    }

    pub async fn send(&self, event: Event) -> Result<(), mpsc::error::SendError<Event>> {
        if event.kind() == EventKind::CoroutineResume {
            // The resume ladder applies to the async
            // path too (same shape as try_send).
            let event = if let Some(slot) = &self.lane_hooks {
                match slot.forward_resume(event) {
                    crate::executor::ForwardOutcome::Forwarded => return Ok(()),
                    crate::executor::ForwardOutcome::NotAttached(event)
                    | crate::executor::ForwardOutcome::RefusedFull(event) => event,
                }
            } else {
                event
            };
            return self.push_reserved(event).map_err(mpsc::error::SendError);
        }
        // send().await previously bypassed the facade —
        // the service Block overflow arm and the subscriber overflow-relay
        // worker re-sent INTO THE CORE QUEUE on attached executors,
        // violating the facade guarantee that nothing reaches the core
        // queue on an attached executor (Relay included). Blocking
        // semantics on the lane tier = bounded-interval retry until the
        // lane accepts (Block means block; the callers are relay/Block
        // tokio tasks, never RX threads).
        if let Some(slot) = &self.lane_hooks {
            let mut event = event;
            // Exhaustive twin of try_send's kind gate: resumes took the
            // ladder above; timers forward at fire; residual control
            // kinds stay queue-side. The kind is retry-invariant, so the
            // gate hoists out of the retry loop.
            match event.kind() {
                EventKind::SubscriptionData | EventKind::RpcRequest | EventKind::RpcResponse => {
                    loop {
                        match slot.forward(event) {
                            crate::executor::ForwardOutcome::Forwarded => return Ok(()),
                            crate::executor::ForwardOutcome::RefusedFull(e) => {
                                event = e;
                                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                            }
                            crate::executor::ForwardOutcome::NotAttached(e) => {
                                event = e;
                                break;
                            }
                        }
                    }
                }
                EventKind::CoroutineResume | EventKind::Timer | EventKind::ClockUpdate => {}
            }
            return self.send_queue(event).await;
        }
        self.send_queue(event).await
    }

    async fn send_queue(&self, event: Event) -> Result<(), mpsc::error::SendError<Event>> {
        let event_bytes = event.estimated_bytes();
        match &self.backend {
            SenderBackend::Bounded { tx, admission } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::SendError(event));
                }
                match tx.send(event).await {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(err) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        Err(err)
                    }
                }
            }
            SenderBackend::Unbounded { tx, admission } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::SendError(event));
                }
                match tx.send(event) {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(err) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        // Convert UnboundedSendError to SendError for consistency
                        Err(mpsc::error::SendError(err.0))
                    }
                }
            }
            SenderBackend::MultiIngress { tx, admission } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::SendError(event));
                }
                match tx.send(event).await {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(err) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        Err(err)
                    }
                }
            }
            #[cfg(target_os = "linux")]
            SenderBackend::Crossbeam {
                ref tx,
                ref admission,
            } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::SendError(event));
                }
                match tx.send(event).await {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(event) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        Err(mpsc::error::SendError(event))
                    }
                }
            }
        }
    }

    pub fn try_send(&self, event: Event) -> Result<(), mpsc::error::TrySendError<Event>> {
        if event.kind() == EventKind::CoroutineResume {
            // Attached executors post resumes as
            // budget-exempt lane items. The refusal LADDER: pool refused
            // while the executor is LIVE → the reserved lane (still
            // constructed — detach rollback must not brick post_resume);
            // reserved lane closed (executor stopped) → the queue path Closed
            // path (caller-inline resume-into-cancellation, bit-for-bit).
            let event = if let Some(slot) = &self.lane_hooks {
                match slot.forward_resume(event) {
                    crate::executor::ForwardOutcome::Forwarded => return Ok(()),
                    crate::executor::ForwardOutcome::NotAttached(event)
                    | crate::executor::ForwardOutcome::RefusedFull(event) => event,
                }
            } else {
                event
            };
            return self
                .push_reserved(event)
                .map_err(mpsc::error::TrySendError::Closed);
        }
        // Emit-side: on an ATTACHED executor,
        // an admitted C++ callback item never enters the core queue —
        // it forwards to the lane pool ON THIS (producing) thread.
        // Exhaustive kind gate: only the three callback kinds forward;
        // Timer forwards at fire; residual kinds stay queue-side.
        if let Some(slot) = &self.lane_hooks {
            let event = match event.kind() {
                EventKind::SubscriptionData | EventKind::RpcRequest | EventKind::RpcResponse => {
                    match slot.forward(event) {
                        crate::executor::ForwardOutcome::Forwarded => return Ok(()),
                        crate::executor::ForwardOutcome::RefusedFull(event) => {
                            // Lane budget full: surface the queue path's Full so the
                            // caller's overflow policy (Drop) runs
                            // unchanged (caller-owned refusal).
                            return Err(mpsc::error::TrySendError::Full(event));
                        }
                        crate::executor::ForwardOutcome::NotAttached(event) => event,
                    }
                }
                // Exhaustive (a future kind must be classified
                // HERE deliberately or it could split a callback group):
                // timers forward AT FIRE; resumes took the ladder
                // above; ClockUpdate and result-push wakes stay queue-side.
                EventKind::Timer | EventKind::ClockUpdate | EventKind::CoroutineResume => event,
            };
            return self.try_send_queue(event);
        }
        self.try_send_queue(event)
    }

    /// Enqueue onto the backend channel.
    ///
    /// # Wake path (PERF — one wake, not two)
    ///
    /// A successful `tx.try_send` already wakes the mpsc receiver's registered
    /// waker, and every executor park has that receiver in its `tokio::select!`
    /// (`spin_tokio` → `EventQueue::pop` → `pop_lane_aware` → `receiver.recv()`).
    /// This path used to *also* fire the executor's outer `Notify`, whose only
    /// effect on the queue path was to win the same select and `continue` into
    /// a `try_pop` that would have got the event anyway. `Notify` takes an
    /// internal waiter-list lock, contended against the executor thread that
    /// re-registers on it at every park — a per-message cost for a wake that
    /// was already delivered.
    ///
    /// The outer `Notify` is still owned by the executor and still fired by the
    /// TIMER scheduler and by `StopHandle::wake`/`stop`, which have no channel
    /// send to piggyback on. The reserved lane has always followed this rule
    /// (see [`Self::push_reserved`]).
    fn try_send_queue(&self, event: Event) -> Result<(), mpsc::error::TrySendError<Event>> {
        let event_bytes = event.estimated_bytes();
        match &self.backend {
            SenderBackend::Bounded { tx, admission } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::TrySendError::Full(event));
                }
                match tx.try_send(event) {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Full(event)) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueFull);
                        Err(mpsc::error::TrySendError::Full(event))
                    }
                    Err(mpsc::error::TrySendError::Closed(event)) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        Err(mpsc::error::TrySendError::Closed(event))
                    }
                }
            }
            SenderBackend::Unbounded { tx, admission } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::TrySendError::Full(event));
                }
                match tx.send(event) {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(err) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        // Unbounded send only fails if closed
                        Err(mpsc::error::TrySendError::Closed(err.0))
                    }
                }
            }
            SenderBackend::MultiIngress { tx, admission } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::TrySendError::Full(event));
                }
                match tx.try_send(event) {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Full(event)) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueFull);
                        Err(mpsc::error::TrySendError::Full(event))
                    }
                    Err(mpsc::error::TrySendError::Closed(event)) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        Err(mpsc::error::TrySendError::Closed(event))
                    }
                }
            }
            #[cfg(target_os = "linux")]
            SenderBackend::Crossbeam {
                ref tx,
                ref admission,
            } => {
                if let Err(reason) = admission.try_reserve(event_bytes) {
                    admission.on_drop(reason);
                    return Err(mpsc::error::TrySendError::Full(event));
                }
                match tx.try_send(event) {
                    Ok(_) => {
                        // The entry is now published, so the mpsc/
                        // crossbeam send has already fired the receiver's
                        // waker — count exactly one wake signal here and
                        // nowhere else (a refused send signals nothing).
                        admission.on_wake_signal();
                        Ok(())
                    }
                    Err(crossbeam_channel::TrySendError::Full(event)) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueFull);
                        Err(mpsc::error::TrySendError::Full(event))
                    }
                    Err(crossbeam_channel::TrySendError::Disconnected(event)) => {
                        admission.release_reservation(event_bytes);
                        admission.on_drop(AdmissionDropReason::QueueClosed);
                        Err(mpsc::error::TrySendError::Closed(event))
                    }
                }
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.backend_is_closed()
    }

    pub fn capacity(&self) -> usize {
        match &self.backend {
            SenderBackend::Bounded { tx, .. } => tx.capacity(),
            SenderBackend::Unbounded { .. } => usize::MAX,
            SenderBackend::MultiIngress { .. } => 0,
            #[cfg(target_os = "linux")]
            SenderBackend::Crossbeam { tx, .. } => tx.capacity(),
        }
    }

    pub(crate) fn waker(&self) -> Option<EventQueueWaker> {
        match &self.backend {
            #[cfg(target_os = "linux")]
            SenderBackend::Crossbeam { tx, .. } => Some(EventQueueWaker {
                crossbeam: tx.waker(),
            }),
            _ => None,
        }
    }

    /// Attach the executor's lane-hook slot so admitted callback items forward
    /// to the attached lane pool on the producing thread.
    pub fn set_lane_hooks(&mut self, slot: Arc<crate::executor::LaneHookSlot>) {
        self.lane_hooks = Some(slot);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MultiIngressOptions {
    pub sub_data_capacity: usize,
    pub rpc_request_capacity: usize,
    pub rpc_response_capacity: usize,
    pub timer_capacity: usize,
    pub control_capacity: usize,
}

impl MultiIngressOptions {
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            sub_data_capacity: capacity,
            rpc_request_capacity: capacity,
            rpc_response_capacity: capacity,
            timer_capacity: capacity,
            control_capacity: capacity,
        }
    }
}

impl Default for MultiIngressOptions {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

#[derive(Debug)]
pub struct MultiIngressSender {
    sub_data: mpsc::Sender<Event>,
    rpc_request: mpsc::Sender<Event>,
    rpc_response: mpsc::Sender<Event>,
    timer: mpsc::Sender<Event>,
    control: mpsc::Sender<Event>,
    admission: Arc<AdmissionState>,
}

impl MultiIngressSender {
    fn new(main_sender: EventQueueSender, options: MultiIngressOptions) -> AortaResult<Self> {
        let admission = Arc::new(AdmissionState::new(EventQueueOptions::default()));
        let (sender, mut receiver) = Self::channel_pair(options, Arc::clone(&admission));
        let bridge = async move {
            while let Some(event) = receiver.recv().await {
                if main_sender.send(event).await.is_err() {
                    break;
                }
            }
        };

        if let Ok(runtime_handle) = tokio::runtime::Handle::try_current() {
            runtime_handle.spawn(bridge);
        } else {
            let bridge_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| {
                    AortaError::internal(format!(
                        "failed to build multi-ingress bridge runtime: {}",
                        err
                    ))
                })?;
            thread::Builder::new()
                .name("aorta-multi-ingress-bridge".to_string())
                .spawn(move || {
                    bridge_runtime.block_on(bridge);
                })
                .map_err(|err| {
                    AortaError::internal(format!(
                        "failed to spawn multi-ingress bridge thread: {}",
                        err
                    ))
                })?;
        }

        Ok(sender)
    }

    fn admission(&self) -> Arc<AdmissionState> {
        Arc::clone(&self.admission)
    }

    fn channel_pair(
        options: MultiIngressOptions,
        admission: Arc<AdmissionState>,
    ) -> (Self, MultiIngressReceiver) {
        let (sub_data_tx, sub_data_rx) = mpsc::channel(options.sub_data_capacity.max(1));
        let (rpc_request_tx, rpc_request_rx) = mpsc::channel(options.rpc_request_capacity.max(1));
        let (rpc_response_tx, rpc_response_rx) =
            mpsc::channel(options.rpc_response_capacity.max(1));
        let (timer_tx, timer_rx) = mpsc::channel(options.timer_capacity.max(1));
        let (control_tx, control_rx) = mpsc::channel(options.control_capacity.max(1));

        (
            Self {
                sub_data: sub_data_tx,
                rpc_request: rpc_request_tx,
                rpc_response: rpc_response_tx,
                timer: timer_tx,
                control: control_tx,
                admission: Arc::clone(&admission),
            },
            MultiIngressReceiver {
                sub_data: sub_data_rx,
                rpc_request: rpc_request_rx,
                rpc_response: rpc_response_rx,
                timer: timer_rx,
                control: control_rx,
                admission,
                sub_data_closed: false,
                rpc_request_closed: false,
                rpc_response_closed: false,
                timer_closed: false,
                control_closed: false,
            },
        )
    }

    async fn send(&self, event: Event) -> Result<(), mpsc::error::SendError<Event>> {
        self.sender_for_kind(event.kind()).send(event).await
    }

    fn try_send(&self, event: Event) -> Result<(), mpsc::error::TrySendError<Event>> {
        self.sender_for_kind(event.kind()).try_send(event)
    }

    fn is_closed(&self) -> bool {
        self.sub_data.is_closed()
            && self.rpc_request.is_closed()
            && self.rpc_response.is_closed()
            && self.timer.is_closed()
            && self.control.is_closed()
    }

    fn sender_for_kind(&self, kind: EventKind) -> &mpsc::Sender<Event> {
        match kind {
            EventKind::SubscriptionData => &self.sub_data,
            EventKind::RpcRequest => &self.rpc_request,
            EventKind::RpcResponse => &self.rpc_response,
            EventKind::Timer => &self.timer,
            EventKind::ClockUpdate => &self.control,
            EventKind::CoroutineResume => {
                // Unreachable by construction: `EventQueueSender` intercepts
                // CoroutineResume onto the reserved lane before backend dispatch.
                debug_assert!(false, "CoroutineResume must ride the reserved lane");
                &self.control
            }
        }
    }
}

#[derive(Debug)]
struct MultiIngressReceiver {
    sub_data: mpsc::Receiver<Event>,
    rpc_request: mpsc::Receiver<Event>,
    rpc_response: mpsc::Receiver<Event>,
    timer: mpsc::Receiver<Event>,
    control: mpsc::Receiver<Event>,
    admission: Arc<AdmissionState>,
    sub_data_closed: bool,
    rpc_request_closed: bool,
    rpc_response_closed: bool,
    timer_closed: bool,
    control_closed: bool,
}

impl MultiIngressReceiver {
    async fn recv(&mut self) -> Option<Event> {
        loop {
            if self.all_closed() {
                return None;
            }

            tokio::select! {
                item = self.control.recv(), if !self.control_closed => {
                    match item {
                        Some(event) => {
                            self.admission.on_dequeued(event.estimated_bytes());
                            return Some(event);
                        }
                        None => self.control_closed = true,
                    }
                }
                item = self.rpc_response.recv(), if !self.rpc_response_closed => {
                    match item {
                        Some(event) => {
                            self.admission.on_dequeued(event.estimated_bytes());
                            return Some(event);
                        }
                        None => self.rpc_response_closed = true,
                    }
                }
                item = self.rpc_request.recv(), if !self.rpc_request_closed => {
                    match item {
                        Some(event) => {
                            self.admission.on_dequeued(event.estimated_bytes());
                            return Some(event);
                        }
                        None => self.rpc_request_closed = true,
                    }
                }
                item = self.timer.recv(), if !self.timer_closed => {
                    match item {
                        Some(event) => {
                            self.admission.on_dequeued(event.estimated_bytes());
                            return Some(event);
                        }
                        None => self.timer_closed = true,
                    }
                }
                item = self.sub_data.recv(), if !self.sub_data_closed => {
                    match item {
                        Some(event) => {
                            self.admission.on_dequeued(event.estimated_bytes());
                            return Some(event);
                        }
                        None => self.sub_data_closed = true,
                    }
                }
            }
        }
    }

    fn try_recv(&mut self) -> Result<Event, mpsc::error::TryRecvError> {
        if let Some(event) = self.try_recv_control() {
            return Ok(event);
        }
        if let Some(event) = self.try_recv_rpc_response() {
            return Ok(event);
        }
        if let Some(event) = self.try_recv_rpc_request() {
            return Ok(event);
        }
        if let Some(event) = self.try_recv_timer() {
            return Ok(event);
        }
        if let Some(event) = self.try_recv_sub_data() {
            return Ok(event);
        }

        if self.all_closed() {
            Err(mpsc::error::TryRecvError::Disconnected)
        } else {
            Err(mpsc::error::TryRecvError::Empty)
        }
    }

    fn all_closed(&self) -> bool {
        self.sub_data_closed
            && self.rpc_request_closed
            && self.rpc_response_closed
            && self.timer_closed
            && self.control_closed
    }

    fn try_recv_control(&mut self) -> Option<Event> {
        if self.control_closed {
            return None;
        }
        match self.control.try_recv() {
            Ok(event) => {
                self.admission.on_dequeued(event.estimated_bytes());
                Some(event)
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.control_closed = true;
                None
            }
        }
    }

    fn try_recv_rpc_response(&mut self) -> Option<Event> {
        if self.rpc_response_closed {
            return None;
        }
        match self.rpc_response.try_recv() {
            Ok(event) => {
                self.admission.on_dequeued(event.estimated_bytes());
                Some(event)
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.rpc_response_closed = true;
                None
            }
        }
    }

    fn try_recv_rpc_request(&mut self) -> Option<Event> {
        if self.rpc_request_closed {
            return None;
        }
        match self.rpc_request.try_recv() {
            Ok(event) => {
                self.admission.on_dequeued(event.estimated_bytes());
                Some(event)
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.rpc_request_closed = true;
                None
            }
        }
    }

    fn try_recv_timer(&mut self) -> Option<Event> {
        if self.timer_closed {
            return None;
        }
        match self.timer.try_recv() {
            Ok(event) => {
                self.admission.on_dequeued(event.estimated_bytes());
                Some(event)
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.timer_closed = true;
                None
            }
        }
    }

    fn try_recv_sub_data(&mut self) -> Option<Event> {
        if self.sub_data_closed {
            return None;
        }
        match self.sub_data.try_recv() {
            Ok(event) => {
                self.admission.on_dequeued(event.estimated_bytes());
                Some(event)
            }
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.sub_data_closed = true;
                None
            }
        }
    }
}

/// Receiver abstraction that can be either bounded or unbounded
enum EventQueueReceiver {
    Bounded {
        rx: mpsc::Receiver<Event>,
        admission: Arc<AdmissionState>,
    },
    Unbounded {
        rx: mpsc::UnboundedReceiver<Event>,
        admission: Arc<AdmissionState>,
    },
    MultiIngress(MultiIngressReceiver),
    /// Lock-free crossbeam channel + Linux eventfd receiver.
    #[cfg(target_os = "linux")]
    Crossbeam {
        rx: crate::crossbeam_queue::CrossbeamEventfdReceiver,
        admission: Arc<AdmissionState>,
    },
}

impl EventQueueReceiver {
    async fn recv(&mut self) -> Option<Event> {
        match self {
            Self::Bounded { rx, admission } => {
                let event = rx.recv().await;
                if let Some(ref event) = event {
                    admission.on_dequeued(event.estimated_bytes());
                }
                event
            }
            Self::Unbounded { rx, admission } => {
                let event = rx.recv().await;
                if let Some(ref event) = event {
                    admission.on_dequeued(event.estimated_bytes());
                }
                event
            }
            Self::MultiIngress(r) => r.recv().await,
            #[cfg(target_os = "linux")]
            Self::Crossbeam { rx, admission } => {
                let event = if current_runtime_supports_block_in_place() {
                    tokio::task::block_in_place(|| rx.recv())
                } else {
                    rx.recv()
                };
                if let Some(ref event) = event {
                    admission.on_dequeued(event.estimated_bytes());
                }
                event
            }
        }
    }

    fn try_recv(&mut self) -> Result<Event, mpsc::error::TryRecvError> {
        match self {
            Self::Bounded { rx, admission } => match rx.try_recv() {
                Ok(event) => {
                    admission.on_dequeued(event.estimated_bytes());
                    Ok(event)
                }
                Err(err) => Err(err),
            },
            Self::Unbounded { rx, admission } => match rx.try_recv() {
                Ok(event) => {
                    admission.on_dequeued(event.estimated_bytes());
                    Ok(event)
                }
                Err(err) => Err(err),
            },
            Self::MultiIngress(r) => r.try_recv(),
            #[cfg(target_os = "linux")]
            Self::Crossbeam { rx, admission } => match rx.try_recv() {
                Ok(event) => {
                    admission.on_dequeued(event.estimated_bytes());
                    Ok(event)
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    Err(mpsc::error::TryRecvError::Empty)
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    Err(mpsc::error::TryRecvError::Disconnected)
                }
            },
        }
    }

    /// Receive with timeout.  For the crossbeam variant this calls the
    /// synchronous `recv_timeout()` wrapped in `block_in_place` so that the
    /// tokio runtime can schedule other work during the wait.
    #[cfg(target_os = "linux")]
    fn recv_timeout_sync(&mut self, timeout: Duration) -> Option<Event> {
        match self {
            Self::Crossbeam { rx, admission } => {
                let event = rx.recv_timeout(timeout);
                if let Some(ref event) = event {
                    admission.on_dequeued(event.estimated_bytes());
                }
                event
            }
            _ => None, // caller should not use this for non-Crossbeam variants
        }
    }

    #[cfg(target_os = "linux")]
    fn is_crossbeam(&self) -> bool {
        matches!(self, Self::Crossbeam { .. })
    }

    #[cfg(not(target_os = "linux"))]
    fn is_crossbeam(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    SubscriptionData,
    RpcRequest,
    RpcResponse,
    Timer,
    ClockUpdate,
    /// Result-push wake: the continuation of
    /// already-admitted in-flight work (action terminal result / feedback /
    /// RPC reply). Rides the reserved lane — bypasses the admission budget
    /// and the backend channels entirely; capacity discipline comes from the
    /// per-source `WakeSlot` protocol (≤1 wake outstanding per live source).
    CoroutineResume,
}

/// EXECUTOR dispatch priority (which ready event runs first) — one of the
/// SDK's two priority vocabularies:
/// - `EventPriority` (this): local scheduling — `ScopeOpts::priority`,
///   `Subscriber::set_flow_priority`, the WRR bucket weights.
/// - [`qos::Priority`](crate::qos::Priority): the ZENOH WIRE priority
///   (network QoS on publish) — `QoS::priority`, publisher options.
///
/// A subscriber's wire priority maps into an EventPriority at RX
/// (`map_event_priority`) unless overridden by the Flow setter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventPriority {
    Realtime,
    High,
    Normal,
    Low,
    Background,
}

// ---------------------------------------------------------------------------
// Priority index / lane-class maps — the SINGLE Rust source of truth.
//
// These maps are the authoritative priority-mapping table. The C++ twin of the lane
// fold lives in cpp/include/aorta/lane_runtime.h (`lane_class_for` /
// `lane_class_for_u8`, resume-post path) and MUST agree with
// `lane_class_for_priority` below forever. Drift guards: the Rust pinning
// test `priority_index_and_lane_class_tables_pinned`
// (core/test/event_queue_tests.rs) and the C++
// `LaneRuntime.ClassMappingMatchesSpecPriorityTable` /
// `.ClassByteMappingMatchesSpecPriorityTable` /
// `.EventPriorityBytesMatchTheSpecTable` (cpp/test/lane_runtime_test.cc —
// the last pins the C++ enum discriminants to the C-ABI byte directly).
// ---------------------------------------------------------------------------

/// Canonical `EventPriority` → index map (0..=4): Realtime→0, High→1,
/// Normal→2, Low→3, Background→4.
///
/// This is the C-ABI priority byte (`aorta_subscriber_set_priority`,
/// `aorta_scope_priority`, resume posts), the executor's 5-bucket WRR index,
/// and the scope clamp ordering — one table, one home.
/// 255 (`AORTA_FLOW_PRIORITY_INHERIT` / `u8::MAX`) is a SENTINEL owned by
/// the call sites, never produced here.
pub fn priority_index(priority: EventPriority) -> u8 {
    match priority {
        EventPriority::Realtime => 0,
        EventPriority::High => 1,
        EventPriority::Normal => 2,
        EventPriority::Low => 3,
        EventPriority::Background => 4,
    }
}

/// Strict inverse of [`priority_index`]: decodes 0..=4, `None` for anything
/// else (including the 255 inherit sentinel — decoding sentinels is the
/// caller's job). C++ posts carry the same byte.
pub fn event_priority_from_index(raw: u8) -> Option<EventPriority> {
    match raw {
        0 => Some(EventPriority::Realtime),
        1 => Some(EventPriority::High),
        2 => Some(EventPriority::Normal),
        3 => Some(EventPriority::Low),
        4 => Some(EventPriority::Background),
        _ => None,
    }
}

/// Lossy inverse of [`priority_index`]: out-of-range clamps to `Normal`
/// (a clamp, not a demotion default — see the C-ABI decode in
/// `ffi/c_api/flow.rs` for the inherit-sentinel layering).
pub fn event_priority_from_index_lossy(raw: u8) -> EventPriority {
    event_priority_from_index(raw).unwrap_or(EventPriority::Normal)
}

/// The 5→4 lane-class fold: Realtime→0, High→1, Normal→2,
/// Low→3, **Background→3** (folded with Low at the lane plane; the Rust
/// core-queue plane keeps all 5 WRR buckets — an intentional asymmetry).
/// Cross-language twin: `lane_class_for` /
/// `lane_class_for_u8` in cpp/include/aorta/lane_runtime.h — the C++ side
/// clamps the index byte with `min(3)` exactly as done here.
pub fn lane_class_for_priority(priority: EventPriority) -> u8 {
    priority_index(priority).min(3)
}

/// RAII decrement-on-drop guard for a per-subscriber in-flight counter
/// (QoS & buffering redesign, Tier A — `SubscriberOptions::receive_depth`).
///
/// Created when an event is admitted past a subscriber's `receive_depth` gate
/// (the counter is incremented at the gate via a single `fetch_update` CAS),
/// then rides the [`Event`] until it is dropped — on `run()` completion, on a
/// Full/Closed rejection, or on a panic-isolated dispatch. `Drop` decrements
/// the counter exactly once, so the accounting is correct on every exit path
/// without any explicit bookkeeping at the dispatch sites.
///
/// It owns an `Arc<AtomicUsize>` (not a borrow like the C-ABI teardown gate at
/// `ffi/c_api/subscriber.rs`) so the counter outlives the subscriber: an event
/// already in flight when the subscriber is torn down keeps the `Arc` alive and
/// decrements cleanly when it finally runs.
///
/// Move-only (no `Clone`): exactly one guard per admitted event.
pub struct InflightGuard(Arc<AtomicUsize>);

impl InflightGuard {
    /// Wrap an already-incremented counter. The caller MUST have incremented
    /// `counter` (e.g. via the `receive_depth` gate's `fetch_update`) before
    /// constructing the guard; `Drop` performs the matching decrement.
    pub fn new(counter: Arc<AtomicUsize>) -> Self {
        Self(counter)
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Flow deadline rider: the absolute expiry plus the endpoint's shed
/// counter (per-subscriber attribution).
pub struct EventDeadline {
    pub(crate) at: Instant,
    pub(crate) dropped: Option<Arc<std::sync::atomic::AtomicU64>>,
}

pub struct Event {
    kind: EventKind,
    priority: EventPriority,
    estimated_bytes: usize,
    callback_group: CallbackGroupType,
    callback_group_handle: Option<CallbackGroupHandle>,
    /// Pre-interned `sched.callback:<name>` event id for a NAMED timer, so the
    /// scheduler tracer labels the callback by task with zero per-dispatch cost
    /// (the string is interned once at timer registration). `None` → unnamed.
    callback_trace_event: Option<u16>,
    enqueued_at: Instant,
    /// Optional absolute deadline. The scheduler drops
    /// an expired event at dequeue (one monotonic compare) and, in priority
    /// mode, breaks ties within a bucket by earliest deadline. `None` for
    /// every event whose endpoint sets no deadline — zero cost when absent
    /// (Box keeps the Some-case pointer-sized; deadline events are already
    /// the slow-ish path).
    deadline: Option<Box<EventDeadline>>,
    /// Per-subscriber in-flight guard for the `receive_depth` cap (Tier A).
    /// `None` for every event whose subscriber left `receive_depth == 0` (the
    /// default) and for all non-subscription events — a null-niche `Option`, so
    /// `None` adds no per-event allocation and the default hot path is unchanged.
    inflight_guard: Option<InflightGuard>,
    callback: EventCallback,
}

impl Event {
    pub fn new<F>(kind: EventKind, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            kind,
            priority: default_priority_for_kind(kind),
            estimated_bytes: 0,
            callback_group: CallbackGroupType::MutuallyExclusive,
            callback_group_handle: None,
            callback_trace_event: None,
            enqueued_at: Instant::now(),
            deadline: None,
            inflight_guard: None,
            callback: wrap_fn_once(callback),
        }
    }

    pub fn new_with_priority<F>(kind: EventKind, priority: EventPriority, callback: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Self {
            kind,
            priority,
            estimated_bytes: 0,
            callback_group: CallbackGroupType::MutuallyExclusive,
            callback_group_handle: None,
            callback_trace_event: None,
            enqueued_at: Instant::now(),
            deadline: None,
            inflight_guard: None,
            callback: wrap_fn_once(callback),
        }
    }

    pub fn with_callback_group(
        mut self,
        callback_group: CallbackGroupType,
        callback_group_handle: Option<CallbackGroupHandle>,
    ) -> Self {
        self.callback_group = callback_group;
        self.callback_group_handle = callback_group_handle;
        self
    }

    pub fn with_estimated_bytes(mut self, bytes: usize) -> Self {
        self.estimated_bytes = bytes;
        self
    }

    /// Attach a Flow deadline: the scheduler drops the event if it is
    /// still queued past this instant (admission-and-ordering, never
    /// preemption — dropping stale work only makes things faster). The
    /// optional counter is the ENDPOINT's shed counter (per-subscriber
    /// deadline drops must be attributable, not just executor-global).
    pub fn with_deadline(mut self, deadline: Option<Instant>) -> Self {
        self.deadline = deadline.map(|at| Box::new(EventDeadline { at, dropped: None }));
        self
    }

    /// `with_deadline` plus the endpoint's drop counter.
    pub fn with_deadline_counted(
        mut self,
        deadline: Option<Instant>,
        dropped: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        self.deadline = deadline.map(|at| {
            Box::new(EventDeadline {
                at,
                dropped: Some(dropped),
            })
        });
        self
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline.as_ref().map(|d| d.at)
    }

    /// Count this event's expiry into its endpoint's shed counter.
    pub(crate) fn count_deadline_drop(&self) {
        if let Some(d) = &self.deadline {
            if let Some(dropped) = &d.dropped {
                dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// One monotonic compare (the drop-if-expired check).
    #[inline]
    pub fn is_expired_at(&self, now: Instant) -> bool {
        matches!(&self.deadline, Some(d) if now >= d.at)
    }

    /// Attach the per-subscriber in-flight guard for the `receive_depth` cap
    /// (Tier A). The subscriber callback path increments the counter at its
    /// admission gate and then calls this so the matching decrement fires when
    /// the event is dropped (run/Full/Closed/panic). See [`InflightGuard`].
    pub fn with_inflight_guard(mut self, guard: Option<InflightGuard>) -> Self {
        self.inflight_guard = guard;
        self
    }

    /// Attach the pre-interned `sched.callback:<name>` event id for a named
    /// timer (see [`Self::callback_trace_event`]).
    pub fn with_callback_trace_event(mut self, event: Option<u16>) -> Self {
        self.callback_trace_event = event;
        self
    }

    pub fn callback_trace_event(&self) -> Option<u16> {
        self.callback_trace_event
    }

    pub fn kind(&self) -> EventKind {
        self.kind
    }

    pub fn priority(&self) -> EventPriority {
        self.priority
    }

    pub fn callback_group(&self) -> CallbackGroupType {
        self.callback_group
    }

    pub fn callback_group_handle(&self) -> Option<CallbackGroupHandle> {
        self.callback_group_handle
    }

    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub fn queue_wait_ns_at(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.enqueued_at).as_nanos();
        elapsed.min(u128::from(u64::MAX)) as u64
    }

    pub fn run_and_measure(mut self) -> u64 {
        let start = Instant::now();
        Self::dispatch_isolated(&mut self.callback, self.kind);
        let elapsed = start.elapsed().as_nanos();
        elapsed.min(u128::from(u64::MAX)) as u64
    }

    /// Returns `true` if the callback panicked and was isolated. The executor
    /// counts these as cumulative dispatch errors for the runtime-health pulse.
    pub fn run(mut self) -> bool {
        Self::dispatch_isolated(&mut self.callback, self.kind)
    }

    /// Run a callback with panic isolation.
    ///
    /// User callbacks (timer ticks, subscriber on-msg, service handlers, …)
    /// run on the executor's dispatch threads. Without isolation, a single
    /// panicking callback unwinds the dispatch loop and kills the whole node —
    /// every timer, subscription and service on that executor goes silent. We
    /// catch the unwind here, at the single dispatch point, so the panic is
    /// contained to the one callback and logged; the executor keeps running.
    ///
    /// `AssertUnwindSafe` is sound because on a caught unwind we drop the
    /// callback (and its captured state) without observing it again — no
    /// broken invariant escapes. No lock is held across this call, so a panic
    /// cannot poison executor state.
    ///
    /// Returns `true` if the callback panicked and was isolated (counted as a
    /// dispatch error), `false` on a clean return.
    fn dispatch_isolated(callback: &mut EventCallback, kind: EventKind) -> bool {
        // Explicit reentrancy detector: mark this thread
        // as "inside a dispatched callback" for the duration, so a blocking
        // FFI re-entry classifies reentrant without constructing a panic.
        let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
        let callback: &mut (dyn FnMut() + Send) = &mut **callback;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
        if let Err(panic) = result {
            let msg = crate::runtime::panic_payload_string(&panic);
            crate::aorta_core_error!(
                "Aorta ERROR: {:?} callback panicked; isolated to protect the executor: {}",
                kind,
                msg
            );
            true
        } else {
            false
        }
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event").field("kind", &self.kind).finish()
    }
}

pub struct EventQueue {
    // The queue self-holds one `EventQueueSender`. Consequence: on a plain
    // `EventQueue`, `receiver.recv()` NEVER returns None just because every
    // external clone of the sender was dropped — a consumption loop waiting
    // for channel close would park forever. Consumers that need the
    // sender-drop termination semantic must use [`EventQueue::into_parts`]
    // and consume via [`EventQueueConsumer`], which holds NO sender (its
    // `pop()` None branch is real).
    sender: EventQueueSender,
    receiver: EventQueueReceiver,
    capacity: usize,
    admission: Arc<AdmissionState>,
    /// Reserved `CoroutineResume` lane, shared with `sender` and all its clones.
    lane: Arc<ReservedLane>,
}

/// Consume-side half of a split [`EventQueue`] (see
/// [`EventQueue::into_parts`]). Holds the receiver and the reserved lane
/// but NOT a sender — so `pop()` returns `None` once
/// every handed-out `EventQueueSender` clone is dropped: the None branch is
/// a reachable, real semantic here (unlike on the self-holding
/// [`EventQueue`]). Note that sender-drop termination is timing-dependent
/// (it fires when the last external sender clone happens to drop), so
/// consumers that need deterministic shutdown must still pair it with an
/// explicit close signal.
///
/// Dropping the consumer closes the reserved lane — the same duty as
/// [`EventQueue`]'s `Drop` (which is suppressed on the split path).
pub struct EventQueueConsumer {
    receiver: EventQueueReceiver,
    lane: Arc<ReservedLane>,
}

impl Drop for EventQueueConsumer {
    fn drop(&mut self) {
        // Close the lane under its lock so a concurrent `push_reserved` either
        // lands before the close (resumed here) or fails with `Closed`
        // — never a silently-lost event reported as delivered.
        self.lane.close_and_resume("event queue consumer dropped");
    }
}

/// Shared lane-aware pop loop: drains the reserved lane first, then awaits
/// the backend receiver; on backend close delivers lane stragglers before
/// returning `None`. Cancel-safe (tokio mpsc).
async fn pop_lane_aware(receiver: &mut EventQueueReceiver, lane: &ReservedLane) -> Option<Event> {
    loop {
        if let Some(event) = lane.try_pop() {
            return Some(event);
        }
        tokio::select! {
            _ = lane.notified() => continue,
            event = receiver.recv() => {
                if event.is_some() {
                    return event;
                }
                // Backend closed: deliver lane stragglers before None.
                return lane.try_pop();
            }
        }
    }
}

/// Shared lane-aware synchronous pop: reserved lane first, then `try_recv`.
fn try_pop_lane_aware(receiver: &mut EventQueueReceiver, lane: &ReservedLane) -> Option<Event> {
    if let Some(event) = lane.try_pop() {
        return Some(event);
    }
    receiver.try_recv().ok()
}

#[cfg(target_os = "linux")]
fn current_runtime_supports_block_in_place() -> bool {
    matches!(
        TokioHandle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(RuntimeFlavor::MultiThread)
    )
}

impl EventQueue {
    /// Create a new bounded event queue with the specified capacity.
    ///
    /// # Panics
    /// Panics if capacity is 0. Use `new_unbounded()` instead.
    pub fn new(capacity: usize) -> Self {
        Self::new_with_options(capacity, EventQueueOptions::default())
    }

    pub fn new_with_options(capacity: usize, options: EventQueueOptions) -> Self {
        assert!(
            capacity > 0,
            "capacity must be > 0, use new_unbounded() for unbounded queue"
        );
        let (tx, rx) = mpsc::channel(capacity);
        let admission = Arc::new(AdmissionState::new(options));
        let lane = ReservedLane::new();
        Self {
            sender: EventQueueSender {
                backend: SenderBackend::Bounded {
                    tx,
                    admission: Arc::clone(&admission),
                },
                lane: Arc::clone(&lane),
                lane_hooks: None,
            },
            receiver: EventQueueReceiver::Bounded {
                rx,
                admission: Arc::clone(&admission),
            },
            capacity,
            admission,
            lane,
        }
    }

    /// Create a direct multi-ingress queue with bounded per-class channels.
    ///
    /// This avoids the bridge hop (`class ingress -> main queue`) and lets
    /// executor ingestion consume class channels directly.
    pub fn new_multi_ingress(capacity: usize) -> Self {
        Self::new_multi_ingress_with_options(capacity, EventQueueOptions::default())
    }

    pub fn new_multi_ingress_with_options(capacity: usize, options: EventQueueOptions) -> Self {
        assert!(
            capacity > 0,
            "capacity must be > 0, use new_unbounded() for unbounded queue"
        );
        let admission = Arc::new(AdmissionState::new(options));
        let ingress_options = MultiIngressOptions::with_capacity(capacity);
        let (sender, receiver) =
            MultiIngressSender::channel_pair(ingress_options, Arc::clone(&admission));
        let lane = ReservedLane::new();
        Self {
            sender: EventQueueSender {
                backend: SenderBackend::MultiIngress {
                    tx: Arc::new(sender),
                    admission: Arc::clone(&admission),
                },
                lane: Arc::clone(&lane),
                lane_hooks: None,
            },
            receiver: EventQueueReceiver::MultiIngress(receiver),
            capacity,
            admission,
            lane,
        }
    }

    /// Create a new unbounded event queue.
    pub fn new_unbounded() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let admission = Arc::new(AdmissionState::new(EventQueueOptions::default()));
        let lane = ReservedLane::new();
        Self {
            sender: EventQueueSender {
                backend: SenderBackend::Unbounded {
                    tx,
                    admission: Arc::clone(&admission),
                },
                lane: Arc::clone(&lane),
                lane_hooks: None,
            },
            receiver: EventQueueReceiver::Unbounded {
                rx,
                admission: Arc::clone(&admission),
            },
            capacity: 0, // 0 indicates unbounded
            admission,
            lane,
        }
    }

    /// Create a bounded crossbeam+eventfd queue (Linux only).
    ///
    /// Uses a lock-free crossbeam channel with Linux eventfd for signaling,
    /// eliminating tokio mpsc overhead (~20-25 µs) on the timer dispatch path.
    /// Async `pop()`/`pop_timeout()` should be used from a multi-thread Tokio
    /// runtime; executor construction handles that backend selection.
    #[cfg(target_os = "linux")]
    pub fn new_crossbeam(capacity: usize) -> Self {
        Self::new_crossbeam_with_options(capacity, EventQueueOptions::default())
    }

    #[cfg(target_os = "linux")]
    pub fn new_crossbeam_with_options(capacity: usize, options: EventQueueOptions) -> Self {
        assert!(capacity > 0, "capacity must be > 0 for crossbeam queue");
        let (tx, rx) = match crate::crossbeam_queue::channel(capacity) {
            Ok(pair) => pair,
            Err(err) => {
                crate::aorta_core_warn!(
                    "Aorta WARN: crossbeam+eventfd queue unavailable; falling back to tokio queue: {}",
                    err
                );
                return Self::new_with_options(capacity, options);
            }
        };
        let admission = Arc::new(AdmissionState::new(options));
        let lane = ReservedLane::new();
        let sender = EventQueueSender {
            backend: SenderBackend::Crossbeam {
                tx,
                admission: Arc::clone(&admission),
            },
            lane: Arc::clone(&lane),
            lane_hooks: None,
        };
        // A lane push must break the executor's synchronous eventfd wait
        // (`pop_timeout_sync`) the same way a backend push does.
        if let Some(waker) = sender.waker() {
            let _ = lane.waker.set(waker);
        }
        Self {
            sender,
            receiver: EventQueueReceiver::Crossbeam {
                rx,
                admission: Arc::clone(&admission),
            },
            capacity,
            admission,
            lane,
        }
    }

    pub fn sender(&self) -> EventQueueSender {
        self.sender.clone()
    }

    /// Monotonic count of events published to this queue's backend
    /// channel — one per wake the sender attempts. Pair it with the executor's
    /// park count to get the share of sends that actually cost a wake syscall
    /// pair (`parks / wake_signals`). Reserved-lane pushes are NOT counted:
    /// the lane keeps its own total in [`ReservedLaneStats::pushed_total`].
    pub fn wake_signals(&self) -> u64 {
        self.admission.wake_signal_count()
    }

    /// Clonable read handle for [`Self::wake_signals`], so a caller can keep
    /// observing after the queue has been moved into an executor (and the
    /// executor onto its own thread).
    pub(crate) fn wake_signal_reader(&self) -> Arc<AdmissionState> {
        Arc::clone(&self.admission)
    }

    /// Attach the executor's lane-hook slot to the internal sender so that all
    /// cloned senders produced by [`sender()`](Self::sender) carry it.
    pub fn set_lane_hooks(&mut self, slot: Arc<crate::executor::LaneHookSlot>) {
        self.sender.set_lane_hooks(slot);
    }

    pub(crate) fn waker(&self) -> Option<EventQueueWaker> {
        self.sender.waker()
    }

    pub fn is_unbounded(&self) -> bool {
        self.capacity == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub fn len(&self) -> usize {
        self.admission.queued_events()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn push(&self, event: Event) -> AortaResult<()> {
        self.sender
            .send(event)
            .await
            .map_err(|_| AortaError::internal("event queue closed"))
    }

    pub fn try_push(&self, event: Event) -> AortaResult<()> {
        self.sender.try_send(event).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => AortaError::busy("event queue full"),
            mpsc::error::TrySendError::Closed(_) => AortaError::internal("event queue closed"),
        })
    }

    /// Cost note: under load the executor consumes via
    /// `try_pop` (one lane atomic load); this async path — and its
    /// `lane.notified()` select registration (two uncontended lock ops) — is
    /// reached only on the idle park/unpark transition, so the no-lane-traffic
    /// steady state pays nothing beyond the `try_pop` branch.
    pub async fn pop(&mut self) -> Option<Event> {
        #[cfg(target_os = "linux")]
        if self.receiver.is_crossbeam() {
            return self.pop_crossbeam_lane_aware().await;
        }
        pop_lane_aware(&mut self.receiver, &self.lane).await
    }

    /// Crossbeam variant of the lane-aware pop. The backend's blocking
    /// `recv()` cannot be raced against the lane `Notify` (it parks the whole
    /// thread), so slice the wait: a lane push fires the eventfd waker, which
    /// makes `recv_timeout` return early with `None` (the documented
    /// external-wake contract), and the next iteration drains the lane.
    #[cfg(target_os = "linux")]
    async fn pop_crossbeam_lane_aware(&mut self) -> Option<Event> {
        const SLICE: Duration = Duration::from_millis(10);
        loop {
            if let Some(event) = self.lane.try_pop() {
                return Some(event);
            }
            let event = if current_runtime_supports_block_in_place() {
                tokio::task::block_in_place(|| self.receiver.recv_timeout_sync(SLICE))
            } else {
                self.receiver.recv_timeout_sync(SLICE)
            };
            if event.is_some() {
                return event;
            }
            if self.sender.is_closed() {
                return self.lane.try_pop();
            }
        }
    }

    pub fn try_pop(&mut self) -> Option<Event> {
        try_pop_lane_aware(&mut self.receiver, &self.lane)
    }

    pub async fn pop_timeout(&mut self, timeout: Duration) -> Option<Event> {
        tokio::time::timeout(timeout, self.pop())
            .await
            .ok()
            .flatten()
    }

    /// Synchronous pop with timeout for crossbeam variant.
    /// Uses the crossbeam receiver's spin+eventfd blocking directly,
    /// bypassing tokio entirely.  Returns `None` on timeout or closed.
    ///
    /// # Panics
    /// Debug-asserts if called on a non-crossbeam queue.
    pub fn pop_timeout_sync(&mut self, timeout: Duration) -> Option<Event> {
        debug_assert!(
            self.is_crossbeam(),
            "pop_timeout_sync called on non-crossbeam queue"
        );
        if let Some(event) = self.lane.try_pop() {
            return Some(event);
        }
        #[cfg(target_os = "linux")]
        {
            self.receiver.recv_timeout_sync(timeout)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = timeout;
            debug_assert!(false, "pop_timeout_sync called on non-crossbeam queue");
            None
        }
    }

    /// Split the queue into its sender half and consume half. The
    /// consumer holds receiver + lane but NO sender, so its `pop()`
    /// returns `None` once every handed-out sender clone is dropped —
    /// the sender-drop termination semantic is restored at the type
    /// level (on a plain [`EventQueue`] the self-held sender makes that
    /// None branch unreachable). The lane close-on-drop duty moves to
    /// [`EventQueueConsumer`]'s `Drop`.
    ///
    /// Consumers needing DETERMINISTIC termination must still pair this
    /// with an explicit close signal: sender-drop fires only when the
    /// last external sender clone happens to drop.
    ///
    /// # Precondition
    /// The queue must NOT use the crossbeam backend: the consumer's
    /// generic pop path races the reserved lane's `Notify` against a
    /// blocking `recv()` and cannot work there (that is why
    /// [`EventQueue`] keeps a dedicated 10ms-slice crossbeam pop
    /// variant). Crossbeam queues (executor-owned) must be consumed via
    /// [`EventQueue`] itself. Enforced with a debug assertion.
    pub fn into_parts(self) -> (EventQueueSender, EventQueueConsumer) {
        // `EventQueue` implements Drop (lane close), so the fields cannot
        // be destructured directly (E0509): read them out under
        // ManuallyDrop and let `EventQueueConsumer::drop` take over the
        // lane-close duty.
        // Plain `assert!` (not debug_assert): release builds default to
        // debug-assertions = false, and silently emitting a consumer that
        // can hang forever on a crossbeam queue is the worst kind of
        // failure. into_parts runs once per subscriber — one enum
        // discriminant compare is a trivial cost.
        assert!(
            !self.receiver.is_crossbeam(),
            "into_parts is unsupported on a crossbeam queue: the consumer's \
             generic pop cannot race the reserved lane's Notify against the \
             blocking crossbeam recv (see EventQueue::pop_crossbeam_lane_aware)"
        );
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: `EventQueue`'s Drop never runs (ManuallyDrop), so every
        // field must be handled here exactly once, by destination:
        //   - sender   -> moved into the returned sender half;
        //   - receiver -> moved into the returned consumer;
        //   - lane     -> moved into the returned consumer (its Drop closes
        //                 the lane, the duty formerly in EventQueue::drop);
        //   - admission-> explicitly dropped: the consumer does not use it
        //                 (each receiver variant holds its own clone), but
        //                 the queue's own Arc must still be released;
        //   - capacity -> Copy, no action needed.
        let sender = unsafe { std::ptr::read(&this.sender) };
        // SAFETY: see the field-by-field note above.
        let receiver = unsafe { std::ptr::read(&this.receiver) };
        // SAFETY: see the field-by-field note above.
        let lane = unsafe { std::ptr::read(&this.lane) };
        // SAFETY: see the field-by-field note above; the Arc is released
        // here so its allocation is not leaked by the suppressed Drop.
        drop(unsafe { std::ptr::read(&this.admission) });
        (sender, EventQueueConsumer { receiver, lane })
    }

    /// Returns `true` if this queue uses the crossbeam+eventfd backend.
    pub fn is_crossbeam(&self) -> bool {
        self.receiver.is_crossbeam()
    }

    pub fn admission_stats(&self) -> AdmissionStats {
        self.admission.snapshot()
    }

    /// Counters for the reserved `CoroutineResume` lane.
    /// Lane events never enter the admission budget accounting.
    pub fn reserved_lane_stats(&self) -> ReservedLaneStats {
        self.lane.stats()
    }

    /// The shared lane handle, for the executor's stop path (a stopped-but-
    /// not-yet-freed executor must close the lane so deliveries fail over
    /// instead of stalling silently until the eventual free).
    pub(crate) fn reserved_lane(&self) -> Arc<ReservedLane> {
        Arc::clone(&self.lane)
    }

    pub fn admission_reader(&self) -> AdmissionStatsReader {
        AdmissionStatsReader {
            admission: Arc::clone(&self.admission),
        }
    }
}

impl Drop for EventQueue {
    fn drop(&mut self) {
        // Close the lane under its lock so a concurrent `push_reserved` either
        // lands before the close (resumed here) or fails with `Closed`
        // — never a silently-lost event reported as delivered.
        self.lane.close_and_resume("event queue dropped");
    }
}

impl EventQueueConsumer {
    /// Consume the next event, or `None` once the backend is closed (all
    /// sender clones dropped) and the lane is empty. See
    /// [`EventQueue::into_parts`] for why this None branch is real here.
    pub async fn pop(&mut self) -> Option<Event> {
        pop_lane_aware(&mut self.receiver, &self.lane).await
    }

    /// Synchronous lane-aware pop (see [`EventQueue::try_pop`]).
    pub fn try_pop(&mut self) -> Option<Event> {
        try_pop_lane_aware(&mut self.receiver, &self.lane)
    }
}

fn default_priority_for_kind(kind: EventKind) -> EventPriority {
    match kind {
        EventKind::Timer => EventPriority::Realtime,
        // CoroutineResume: the reserved class defaults to its
        // own high bucket (a resumption of admitted work outranks new data).
        EventKind::RpcRequest
        | EventKind::RpcResponse
        | EventKind::ClockUpdate
        | EventKind::CoroutineResume => EventPriority::High,
        EventKind::SubscriptionData => EventPriority::Normal,
    }
}

#[cfg(test)]
#[path = "../test/event_queue_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/event_queue_advanced_tests.rs"]
mod advanced_tests;

#[cfg(test)]
#[path = "../test/event_queue_reserved_lane_tests.rs"]
mod reserved_lane_tests;
