//! Readiness — the trigger facet.
//!
//! A task is dispatchable when its await-set is satisfied: an AND of
//! [`Condition`]s joined by the loom-verified O(1) bitmask gate
//! (`aorta_sync::ReadinessGate` — each satisfied condition `fetch_or`s its
//! bit; the completing arrival fires the wave and resets the mask). The
//! boundary is precise: **time / count / existence readiness is mechanism
//! (here); value-predicate correlation is
//! policy (application)**.
//!
//! [`WindowSync`] is the approximate-time multi-sensor alignment
//! (camera+lidar+imu) every robot otherwise rebuilds — the universal
//! `message_filters`-style mechanism. This module ships the
//! latest-per-input form; the per-input time-pruned history buffer is a
//! calibration follow-up.
//!
//! `Condition` maps each trigger onto its carrier:
//! - `MessageAvailable` — wire a subscriber/[`Watch`](crate::watch::Watch)
//!   arrival to [`ReadinessSet::satisfy`].
//! - `Periodic` — wire a timer tick the same way.
//! - `Window` — [`WindowSync`] feeds the gate when an aligned set exists.
//! - `SubtaskComplete` — existence-only by construction: join a
//!   [`TaskHandle`](crate::flow::TaskHandle) and satisfy.
//! - `ContextPresent` — a ContextStore watch satisfies on key existence.
//! - `FdReadable` — [`Node::poll_fd`](crate::node::Node) (unix): an
//!   executor-side reader drains a pollable fd into a depth-1 watch. **Not a
//!   hard-RT path** (a 1 kHz control loop must not use it).

use std::sync::Arc;
use std::time::Duration;

use aorta_sync::ReadinessGate;
use tokio::sync::Notify;

use crate::ZBytes;

/// The readiness condition kinds (documentary: the uniform runtime carrier is a
/// gate index — see the module docs for each kind's wiring).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Condition {
    /// An input's watch/queue is non-empty.
    MessageAvailable,
    /// A (phase-aligned) timer fired.
    Periodic,
    /// An approximate-time window over a trigger set aligned (`WindowSync`).
    Window,
    /// A child task reached terminal (join signal — not a result predicate).
    SubtaskComplete,
    /// A ContextStore key exists (presence — not a value predicate).
    ContextPresent,
    /// A pollable fd is readable (`Node::poll_fd`; unix, non-RT plane).
    FdReadable,
}

/// An awaitable AND-join over up to 64 conditions: sources call
/// [`satisfy`](Self::satisfy); consumers [`wait_wave`](Self::wait_wave).
/// This is what replaces the hand-rolled application poll loop.
pub struct ReadinessSet {
    gate: ReadinessGate,
    waves: std::sync::atomic::AtomicU64,
    notify: Notify,
    /// The declared condition kinds, index-aligned with the gate bits
    /// (empty when constructed by count alone).
    conditions: Vec<Condition>,
    /// C-plane wave hook (trampoline-class, single-awaiter — the same
    /// contract as `PullRecvHandle`); fired on wave completion.
    c_hook: crate::subscriber::NotifyHookSlot,
}

impl ReadinessSet {
    /// A set over `count` anonymous inputs (1..=64). Prefer
    /// [`with_conditions`](Self::with_conditions) — named kinds make
    /// `pending_mask` diagnostics readable.
    pub fn new(count: usize) -> crate::error::AortaResult<Arc<Self>> {
        let gate = ReadinessGate::new(count).ok_or_else(|| {
            crate::error::AortaError::invalid_argument(format!(
                "ReadinessSet supports 1..=64 conditions, got {count}"
            ))
        })?;
        Ok(Arc::new(Self {
            gate,
            waves: std::sync::atomic::AtomicU64::new(0),
            notify: Notify::new(),
            conditions: Vec::new(),
            c_hook: crate::subscriber::NotifyHookSlot::default(),
        }))
    }

    /// A set whose bit `i` is `conditions[i]` (`Condition` is consumed
    /// by a constructor, not just documentation). Wire each source to
    /// [`satisfy`](Self::satisfy)`(i)` per the module docs.
    pub fn with_conditions(conditions: &[Condition]) -> crate::error::AortaResult<Arc<Self>> {
        let gate = ReadinessGate::new(conditions.len()).ok_or_else(|| {
            crate::error::AortaError::invalid_argument(format!(
                "ReadinessSet supports 1..=64 conditions, got {}",
                conditions.len()
            ))
        })?;
        Ok(Arc::new(Self {
            gate,
            waves: std::sync::atomic::AtomicU64::new(0),
            notify: Notify::new(),
            conditions: conditions.to_vec(),
            c_hook: crate::subscriber::NotifyHookSlot::default(),
        }))
    }

    /// The declared kind at bit `index` (None for by-count sets).
    pub fn condition(&self, index: usize) -> Option<Condition> {
        self.conditions.get(index).copied()
    }

    /// The number of conditions this set joins (bind-time validation).
    pub fn count(&self) -> usize {
        self.gate.full_mask().count_ones() as usize
    }

    /// A validated closure that satisfies condition `index` — the wiring
    /// sugar for non-subscriber sources: timers
    /// (`node.create_timer(period, set.satisfier(1)?)`), join completions,
    /// ContextStore watches. Out-of-range indices are refused HERE, at
    /// build time, not silently at fire time.
    pub fn satisfier(
        self: &Arc<Self>,
        index: usize,
    ) -> crate::error::AortaResult<impl Fn() + Send + Sync + Clone + 'static> {
        if index >= self.count() {
            return Err(crate::error::AortaError::invalid_argument(format!(
                "ReadinessSet::satisfier condition index {index} out of range (set has {} conditions)",
                self.count()
            )));
        }
        let set = Arc::clone(self);
        Ok(move || set.satisfy(index))
    }

    /// Mark condition `index` satisfied (any thread; one RMW). The arrival
    /// that completes the wave publishes it and wakes waiters.
    pub fn satisfy(&self, index: usize) {
        if self.gate.satisfy(index) {
            self.waves
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            self.notify.notify_waiters();
            self.fire_hook();
        }
    }

    /// C-plane bridge: set/clear the wave hook. SINGLE-AWAITER contract
    /// (same as the pull subscriber): a different non-null hook over an
    /// existing one is refused (returns false).
    pub(crate) fn set_notify_hook(&self, hook: Option<crate::subscriber::PullNotifyHook>) -> bool {
        self.c_hook.set(hook)
    }

    /// Fire the C hook (F4 copy-out semantics live in NotifyHookSlot).
    pub(crate) fn fire_hook(&self) {
        self.c_hook.fire();
    }

    /// Await a wave numbered above `seen` (snapshot [`Self::waves`] BEFORE
    /// arming your trigger — the explicit baseline is what makes the
    /// trigger-then-wait pattern race-free). Returns the wave number.
    /// (Register-then-recheck: a wave completing concurrently is not lost.)
    pub async fn wait_wave_after(&self, seen: u64) -> u64 {
        loop {
            let notified = self.notify.notified();
            let now = self.waves.load(std::sync::atomic::Ordering::Acquire);
            if now != seen {
                return now;
            }
            notified.await;
        }
    }

    /// Convenience: await a wave completing after THIS call (a wave that
    /// fired before the call does not count — snapshot with
    /// [`Self::waves`] + [`Self::wait_wave_after`] when the trigger may
    /// race the wait).
    pub async fn wait_wave(&self) -> u64 {
        let seen = self.waves.load(std::sync::atomic::Ordering::Acquire);
        self.wait_wave_after(seen).await
    }

    /// Completed-wave count (observability).
    pub fn waves(&self) -> u64 {
        self.waves.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Observability: the bitmask of inputs still missing.
    pub fn pending_mask(&self) -> u64 {
        self.gate.pending_mask()
    }
}

/// Approximate-time alignment over K inputs within `slop`: feed each
/// source; await aligned sets. The whole camera/lidar/imu fusion pattern:
///
/// ```ignore
/// let window = WindowSync::new(3, Duration::from_millis(5));
/// let fusion = scope.child(ScopeOpts::named("fusion"));
/// for (i, watch) in [camera, lidar, imu].into_iter().enumerate() {
///     let window = Arc::clone(&window);
///     let _feed = fusion.spawn_with(move |scope| async move {
///         let mut seen = 0;
///         while !scope.is_cancelled() {
///             seen = watch.changed(seen).await;
///             let snap = watch.snapshot().unwrap();
///             window.feed(i, ts_of(&snap.payload), Arc::clone(&snap.payload));
///         }
///     });
/// }
/// let aligned = window.aligned().await; // zero polling
/// ```
///
/// input's samples with their timestamps; when every input holds a sample
/// and the set's timestamp spread is ≤ `slop`, the set is emitted (and the
/// inputs cleared for the next wave). Latest-per-input: a newer sample
/// replaces an older unaligned one (the older one can never join a future
/// window that the newer one couldn't).
pub struct WindowSync {
    slop_ns: u64,
    /// Input count, denormalized from `inner.latest.len()` so bind-time
    /// validation never takes the sample-path lock.
    inputs: usize,
    /// Emitted-set queue depth cap (drop-OLDEST on overflow — every
    /// other Flow surface is latest-wins/bounded; this queue must be too).
    depth: usize,
    inner: parking_lot::Mutex<WindowInner>,
    notify: Notify,
    /// C-plane emission hook (trampoline-class, single-awaiter —
    /// contract); fired after each aligned-set emission.
    c_hook: crate::subscriber::NotifyHookSlot,
}

/// Observability probe for [`WindowSync::stats`].
#[derive(Clone, Debug, Default)]
pub struct WindowStats {
    /// Samples fed per input.
    pub fed: Vec<u64>,
    /// Aligned sets emitted.
    pub emitted: u64,
    /// Aligned sets dropped because the consumer lagged past the depth cap.
    pub dropped_sets: u64,
    /// Latest timestamp seen per input (0 = none yet).
    pub last_ts: Vec<u64>,
    /// The one-time never-aligns WARN fired (8 consecutive gross misses —
    /// exposed so the detector is machine-assertable, not
    /// log-eyeball-only).
    pub never_aligns_warned: bool,
}

struct WindowInner {
    latest: Vec<Option<(u64, Arc<ZBytes>)>>,
    emitted: std::collections::VecDeque<Vec<(u64, Arc<ZBytes>)>>,
    fed: Vec<u64>,
    /// Last timestamp fed per input — tracked independently of the
    /// pending slots (which are cleared on every alignment).
    last_ts: Vec<u64>,
    emitted_total: u64,
    dropped_sets: u64,
    warned_dropped: bool,
    /// Never-aligns detection: consecutive full-set rejections whose
    /// spread grossly exceeds slop.
    gross_misses: u32,
    warned_never_aligns: bool,
}

impl WindowSync {
    /// `inputs` must be > 0 (debug-asserted). Subscribers wire themselves
    /// declaratively (`SubscriberOptions::with_window_feed` at build time,
    /// `Subscriber::bind_window` post-declare); non-subscriber sources call
    /// [`feed`](Self::feed) with their index directly.
    pub fn new(inputs: usize, slop: Duration) -> Arc<Self> {
        Self::with_capacity(inputs, slop, 8)
    }

    /// [`new`](Self::new) with an explicit emitted-queue depth: a
    /// consumer slower than the alignment rate drops the OLDEST aligned
    /// set (counted in [`stats`](Self::stats); one-time WARN on first drop)
    /// instead of growing memory without bound.
    pub fn with_capacity(inputs: usize, slop: Duration, depth: usize) -> Arc<Self> {
        debug_assert!(inputs > 0, "WindowSync needs at least one input");
        Arc::new(Self {
            slop_ns: u64::try_from(slop.as_nanos()).unwrap_or(u64::MAX),
            inputs,
            depth: depth.max(1),
            inner: parking_lot::Mutex::new(WindowInner {
                latest: vec![None; inputs],
                emitted: std::collections::VecDeque::new(),
                fed: vec![0; inputs],
                last_ts: vec![0; inputs],
                emitted_total: 0,
                dropped_sets: 0,
                warned_dropped: false,
                gross_misses: 0,
                warned_never_aligns: false,
            }),
            notify: Notify::new(),
            c_hook: crate::subscriber::NotifyHookSlot::default(),
        })
    }

    /// The number of inputs this window aligns (bind-time validation).
    pub fn inputs(&self) -> usize {
        self.inputs
    }

    /// Observability probe: fed/emitted/dropped/last-ts per input.
    pub fn stats(&self) -> WindowStats {
        let inner = self.inner.lock();
        WindowStats {
            fed: inner.fed.clone(),
            emitted: inner.emitted_total,
            dropped_sets: inner.dropped_sets,
            last_ts: inner.last_ts.clone(),
            never_aligns_warned: inner.warned_never_aligns,
        }
    }

    /// Feed input `index` with a sample stamped `ts_ns` (any thread).
    /// Out-of-range `index` is a caller wiring bug: debug builds assert;
    /// release builds drop the sample (never corrupt neighbouring inputs).
    pub fn feed(&self, index: usize, ts_ns: u64, payload: Arc<ZBytes>) {
        let mut inner = self.inner.lock();
        debug_assert!(
            index < inner.latest.len(),
            "WindowSync::feed index {index} out of range"
        );
        if index >= inner.latest.len() {
            return;
        }
        inner.latest[index] = Some((ts_ns, payload));
        inner.fed[index] += 1;
        inner.last_ts[index] = ts_ns;
        let (mut min_ts, mut max_ts) = (u64::MAX, 0u64);
        for slot in &inner.latest {
            match slot {
                Some((ts, _)) => {
                    min_ts = min_ts.min(*ts);
                    max_ts = max_ts.max(*ts);
                }
                None => return, // an input is still empty
            }
        }
        let spread = max_ts - min_ts;
        if spread <= self.slop_ns {
            inner.gross_misses = 0;
            let set: Vec<(u64, Arc<ZBytes>)> = inner
                .latest
                .iter_mut()
                .map(|slot| slot.take().expect("all inputs present"))
                .collect();
            inner.emitted.push_back(set);
            inner.emitted_total += 1;
            // Bounded queue — a lagging consumer drops the OLDEST set.
            if inner.emitted.len() > self.depth {
                inner.emitted.pop_front();
                inner.dropped_sets += 1;
                if !inner.warned_dropped {
                    inner.warned_dropped = true;
                    crate::aorta_core_warn!(
                        "Aorta WARN: WindowSync consumer lagging — aligned sets are \
                         being dropped (oldest-first; depth {}). Consume aligned() \
                         faster or raise with_capacity()",
                        self.depth
                    );
                }
            }
            drop(inner);
            self.notify.notify_waiters();
            self.fire_hook();
        } else {
            // Never-aligns detection — full sets whose spread grossly
            // exceeds slop, CONSECUTIVELY (an ordinary near-miss resets the
            // streak), usually mean mixed clock domains.
            if spread <= self.slop_ns.saturating_mul(64) {
                inner.gross_misses = 0;
            }
            if spread > self.slop_ns.saturating_mul(64) {
                inner.gross_misses = inner.gross_misses.saturating_add(1);
                if inner.gross_misses >= 8 && !inner.warned_never_aligns {
                    inner.warned_never_aligns = true;
                    crate::aorta_core_warn!(
                        "Aorta WARN: WindowSync inputs never align — spread {}ns vs \
                         slop {}ns over 8 consecutive full sets. Mixed clock domains \
                         (unix vs monotonic) or a stalled input? (see stats())",
                        spread,
                        self.slop_ns
                    );
                }
            }
        }
    }

    /// NON-POPPING queue probe (C++ awaiter recheck — never steal a
    /// set the caller cannot deliver).
    pub fn has_aligned(&self) -> bool {
        !self.inner.lock().emitted.is_empty()
    }

    /// Await the next aligned set (FIFO across waves).
    pub async fn aligned(&self) -> Vec<(u64, Arc<ZBytes>)> {
        loop {
            let notified = self.notify.notified();
            if let Some(set) = self.inner.lock().emitted.pop_front() {
                return set;
            }
            notified.await;
        }
    }

    /// Non-blocking probe.
    pub fn try_aligned(&self) -> Option<Vec<(u64, Arc<ZBytes>)>> {
        self.inner.lock().emitted.pop_front()
    }

    /// C-plane bridge: set/clear the emission hook. SINGLE-AWAITER
    /// contract — see [`ReadinessSet::set_notify_hook`].
    pub(crate) fn set_notify_hook(&self, hook: Option<crate::subscriber::PullNotifyHook>) -> bool {
        self.c_hook.set(hook)
    }

    /// Fire the C hook (F4 copy-out semantics live in NotifyHookSlot).
    pub(crate) fn fire_hook(&self) {
        self.c_hook.fire();
    }
}

/// Timestamp source for a builder-level [`WindowSync`] feed.
///
/// This is the TRANSPORT-level choice only. Sensor-capture stamps live in
/// the payload (application schema) — feeding those is policy: decode in
/// your consumer and call [`WindowSync::feed`] yourself.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WindowStamp {
    /// Local receive time (node clock, unix ns) — always mutually
    /// comparable across this process's inputs. The default.
    #[default]
    Receive,
    /// The transport publisher timestamp (Zenoh HLC → unix ns); falls back
    /// to receive time when the sample carries none (HLC timestamping is
    /// config-dependent). Requires publisher clocks in one domain — the
    /// never-aligns WARN catches mixes.
    Publisher,
}

/// One declarative subscriber→readiness binding: built by
/// `SubscriberOptions::with_window_feed` / `with_readiness_satisfy` or
/// bound post-declare via `Subscriber::bind_window` / `bind_readiness`.
/// The wire OBSERVES the stream — delivery (callback/pull) is unaffected.
#[derive(Clone)]
pub enum ReadinessWire {
    /// Feed every arriving sample into `window` input `input`.
    Window {
        window: Arc<WindowSync>,
        input: usize,
        stamp: WindowStamp,
    },
    /// Satisfy `set` condition `condition` on every arriving sample.
    Satisfy {
        set: Arc<ReadinessSet>,
        condition: usize,
    },
}

impl std::fmt::Debug for ReadinessWire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Window { input, stamp, .. } => f
                .debug_struct("ReadinessWire::Window")
                .field("input", input)
                .field("stamp", stamp)
                .finish_non_exhaustive(),
            Self::Satisfy { condition, .. } => f
                .debug_struct("ReadinessWire::Satisfy")
                .field("condition", condition)
                .finish_non_exhaustive(),
        }
    }
}

impl ReadinessWire {
    /// Bind-time index validation: an out-of-range wire is a wiring bug —
    /// rejected loudly at declare/bind, never silently dropped per sample.
    pub(crate) fn validate(&self) -> crate::error::AortaResult<()> {
        match self {
            Self::Window { window, input, .. } if *input >= window.inputs() => {
                Err(crate::error::AortaError::invalid_argument(format!(
                    "readiness wire: window input index {input} out of range \
                     (window has {} inputs)",
                    window.inputs()
                )))
            }
            Self::Satisfy { set, condition, .. } if *condition >= set.count() => {
                Err(crate::error::AortaError::invalid_argument(format!(
                    "readiness wire: condition index {condition} out of range \
                     (set has {} conditions)",
                    set.count()
                )))
            }
            _ => Ok(()),
        }
    }

    fn dispatch(&self, payload: &ZBytes, receive_ns: u64, publisher_ns: Option<u64>) {
        match self {
            Self::Window {
                window,
                input,
                stamp,
            } => {
                let ts = match stamp {
                    WindowStamp::Receive => receive_ns,
                    WindowStamp::Publisher => publisher_ns.unwrap_or(receive_ns),
                };
                window.feed(*input, ts, Arc::new(payload.clone()));
            }
            Self::Satisfy { set, condition } => set.satisfy(*condition),
        }
    }
}

/// HLC publisher-timestamp capture for the RX glue (this
/// expression was duplicated between the executor-mode and pull-mode
/// subscriber closures; the dispatch ORDERING difference between them —
/// before the executor hop vs after the watch store — is deliberate and
/// stays at the call sites).
pub(crate) fn publisher_ns_of(sample: &zenoh::sample::Sample) -> Option<u64> {
    sample.timestamp().map(|ts| hlc_to_unix_ns(*ts.get_time()))
}

/// The ONE HLC NTP64 → unix-ns conversion:
/// `get_time().as_u64()` is the raw fixed-point bit pattern
/// (`seconds << 32 | frac`), ~4.295x (2^32/1e9) off from nanoseconds —
/// three RX paths shipped that bug before this was factored out and
/// unit-pinned.
pub(crate) fn hlc_to_unix_ns(time: zenoh::time::NTP64) -> u64 {
    u64::try_from(time.to_duration().as_nanos()).unwrap_or(u64::MAX)
}

/// The subscriber-side wiring slot: RX threads take one lock-free load per
/// sample (`None` — the universal default — costs a load + branch);
/// binds are rare RCU appends. Mirrors the `FlowCaps` dynamic-capability
/// pattern so declare-time options and post-declare binds share ONE
/// mechanism.
#[derive(Default)]
pub(crate) struct ReadinessWiring {
    wires: arc_swap::ArcSwapOption<Vec<ReadinessWire>>,
}

impl ReadinessWiring {
    pub(crate) fn new(initial: Vec<ReadinessWire>) -> Self {
        Self {
            wires: arc_swap::ArcSwapOption::from(if initial.is_empty() {
                None
            } else {
                Some(Arc::new(initial))
            }),
        }
    }

    /// Validate + append one wire (post-declare bind).
    pub(crate) fn bind(&self, wire: ReadinessWire) -> crate::error::AortaResult<()> {
        wire.validate()?;
        self.wires.rcu(|current| {
            let mut next = current.as_deref().cloned().unwrap_or_default();
            next.push(wire.clone());
            Some(Arc::new(next))
        });
        Ok(())
    }

    /// RX hot path. `receive_ns` is the node clock captured by the caller
    /// (only when wires exist); `publisher_ns` is the transport HLC stamp
    /// when the sample carried one.
    pub(crate) fn dispatch(&self, payload: &ZBytes, receive_ns: u64, publisher_ns: Option<u64>) {
        if let Some(wires) = self.wires.load().as_deref() {
            for wire in wires.iter() {
                wire.dispatch(payload, receive_ns, publisher_ns);
            }
        }
    }

    /// Remove every wire `pred` matches (post-declare unbind —
    /// bind had no inverse, so a subscriber's Arc clones kept
    /// feeding a window/set forever once the owner was done with it).
    /// Returns how many wires were removed.
    pub(crate) fn unbind_matching(&self, pred: impl Fn(&ReadinessWire) -> bool) -> usize {
        let mut removed = 0;
        self.wires.rcu(|current| {
            let Some(current) = current.as_deref() else {
                removed = 0;
                return None;
            };
            let next: Vec<ReadinessWire> = current.iter().filter(|w| !pred(w)).cloned().collect();
            removed = current.len() - next.len();
            if next.is_empty() {
                None
            } else {
                Some(Arc::new(next))
            }
        });
        removed
    }

    /// Fast emptiness probe for the RX path (skip clock reads when unwired).
    pub(crate) fn is_empty(&self) -> bool {
        self.wires.load().is_none()
    }
}

/// `Condition::FdReadable`: drain a pollable fd into a
/// depth-1 [`Watch`](crate::watch::Watch) from an async reader task. Core
/// does NOT define a frame type — framing stays in the application's
/// consumer. The reader exits when the watch is dropped or the
/// fd reaches EOF/error. NOT for hard-RT loops.
#[cfg(unix)]
pub(crate) async fn poll_fd_into_watch(
    fd: std::os::unix::io::OwnedFd,
    scope: &crate::flow::Scope,
) -> crate::error::AortaResult<Arc<crate::watch::Watch>> {
    use tokio::io::unix::AsyncFd;
    use tokio::io::Interest;

    // The reader task joins the structured-
    // concurrency tree — it is accounted by `scope.close()` (ExternalChild)
    // and exits PROMPTLY on scope cancellation (select arm below) instead
    // of waiting out the 500ms watch-drop tick past node teardown.
    let Some(external) = scope.begin_external_child() else {
        return Err(crate::error::AortaError::cancelled(
            "poll_fd: scope is closing",
        ));
    };
    let cancel_scope = scope.clone();

    let watch = crate::watch::Watch::detached("fd");
    let weak = Arc::downgrade(&watch);
    // Enforce the non-blocking precondition here instead of trusting the
    // caller: the drain loop reads until WouldBlock, and
    // a BLOCKING fd would hang the task on the read after the last byte.
    {
        use std::os::unix::io::AsRawFd;
        let raw = fd.as_raw_fd();
        // SAFETY: fcntl(2) on a valid fd we own; no pointers involved.
        let ok = unsafe {
            let flags = libc::fcntl(raw, libc::F_GETFL);
            flags >= 0 && libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) >= 0
        };
        if !ok {
            return Err(crate::error::AortaError::internal(format!(
                "poll_fd: setting O_NONBLOCK failed: {}",
                std::io::Error::last_os_error()
            )));
        }
    }
    let async_fd = AsyncFd::with_interest(fd, Interest::READABLE)
        .map_err(|e| crate::error::AortaError::internal(format!("poll_fd: {e}")))?;
    tokio::spawn(async move {
        let _external = external; // release on EVERY exit path (drop)
        let mut buf = vec![0u8; 4096];
        // Idle-fd liveness: `readable()` may pend forever on a quiet fd, so
        // the watch-drop check below is raced against a coarse interval —
        // otherwise the task (and the fd) would leak until process exit
        // (idle-prone fds are the TYPICAL case here).
        let mut liveness = tokio::time::interval(Duration::from_millis(500));
        liveness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let mut guard = tokio::select! {
                readable = async_fd.readable() => match readable {
                    Ok(guard) => guard,
                    Err(_) => return,
                },
                _ = cancel_scope.cancelled() => {
                    return; // prompt exit on scope cancellation
                }
                _ = liveness.tick() => {
                    if weak.upgrade().is_none() {
                        return; // consumer dropped the watch while the fd idled
                    }
                    continue;
                }
            };
            let Some(watch) = weak.upgrade() else {
                return; // consumer dropped the watch: stop reading
            };
            // Drain until WouldBlock BEFORE clearing readiness: tokio's
            // AsyncFd is edge-triggered — clearing after a partial read
            // would strand kernel-buffered bytes with no further edge
            // (tokio clear_ready contract). The drain is CAPPED
            // per wake: a continuously-fed fd must not
            // monopolize the worker or outrun the watch-drop check — on cap,
            // break WITHOUT clear_ready so readiness stays set and the
            // outer loop re-enters after yielding.
            const MAX_CHUNKS_PER_WAKE: usize = 64; // ~256 KiB per wake
            let mut chunks = 0usize;
            loop {
                let n = {
                    use std::os::unix::io::AsRawFd;
                    let raw = async_fd.get_ref().as_raw_fd();
                    // SAFETY: plain non-blocking read(2) into a live local
                    // buffer; the fd is owned by `async_fd`.
                    unsafe { libc::read(raw, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) }
                };
                match n {
                    0 => return, // EOF
                    n if n > 0 => {
                        watch.store(ZBytes::from(buf[..n as usize].to_vec()));
                        chunks += 1;
                        if chunks >= MAX_CHUNKS_PER_WAKE {
                            break; // readiness left set: re-enter after a yield
                        }
                        // Loop: more may be buffered behind this chunk.
                    }
                    _ => {
                        let err = std::io::Error::last_os_error();
                        if err.kind() == std::io::ErrorKind::WouldBlock {
                            guard.clear_ready(); // truly drained: re-arm
                            break;
                        }
                        crate::aorta_core_warn!(
                            "Aorta WARN: poll_fd reader exiting on read error: {err}"
                        );
                        return;
                    }
                }
            }
        }
    });
    Ok(watch)
}

#[cfg(test)]
#[path = "../../test/flow_readiness_tests.rs"]
mod tests;
