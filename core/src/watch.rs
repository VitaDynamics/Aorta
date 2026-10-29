//! Depth-1 latest-value streaming — `Watch::load()`.
//!
//! The streaming path bypasses the executor entirely: the Zenoh RX callback
//! `store`s the refcounted payload handle into an atomically-swapped shared
//! pointer (`arc_swap::ArcSwapOption` — a proven dependency, NOT a
//! hand-rolled lock-free protocol, so it carries no loom burden of its own);
//! a soft-RT consumer `load`s the latest value as an owned
//! `Arc` (one atomic Arc-clone, zero payload copy) or via
//! [`Watch::load_guard`] (no clone; defers the old value's reclaim while
//! held). No Event, no alloc on the consumer side, no wake, no dispatch —
//! streaming nets NEGATIVE cost versus the callback path.
//!
//! Contracts:
//! - **Writer** (Zenoh RX or a foreign-producer thread via [`Watch::store`]):
//!   moves the payload handle in — no byte copy; one control-block
//!   allocation per publish (the RX is soft-RT; the callback path already
//!   allocates an Event per sample, so this is not a regression).
//! - **Reader**: lock-free and near-wait-free; safe from any thread. This is
//!   a **soft-RT** latest-value read — NOT a hard-RT hot-loop primitive
//!   (a hard-RT loop reads app-staged setpoints off a dedicated local
//!   path, not an Aorta topic).
//! - `recv()`/`try_recv()` (the pull subscriber) and the admission
//!   gate are byte-for-byte unchanged by this file (invariant 9).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;

use crate::error::AortaResult;
use crate::node::Node;
use crate::ZBytes;

/// Guard returned by [`Watch::load_guard`]: dereferences to the latest value
/// without cloning the `Arc`; holding it defers reclamation of a value
/// displaced by a concurrent `store` (hot-reader optimization).
pub type WatchGuard = arc_swap::Guard<Option<Arc<SeqPayload>>>;

/// One atomic snapshot of the cell: the sequence number RIDES the payload
/// (a separate counter read could pair a stale seq with
/// a fresh payload, breaking consumer dedup-by-seq). The payload keeps its
/// own `Arc<ZBytes>`, so `load()`'s zero-copy identity guarantee
/// (`Arc::ptr_eq` across loads of one snapshot) is preserved.
#[derive(Debug)]
pub struct SeqPayload {
    /// 1-based store sequence (0 never appears; monotonic per watch).
    pub seq: u64,
    /// The SOURCE timestamp (Zenoh sample timestamp, ns) when the publisher
    /// stamped the sample; `None` for unstamped publishes and foreign
    /// `store()`s. This is what `WindowSync::feed` wants — never
    /// substitute arrival time (wrong clock domain).
    pub stamp_ns: Option<i64>,
    pub payload: Arc<ZBytes>,
}

/// A depth-1 latest-value watch cell over one topic.
///
/// Obtained from [`Node::watch`] (Zenoh-fed) or [`Watch::detached`] (a pure
/// in-process cell for foreign producers). Dropping the watch undeclares its
/// subscriber.
pub struct Watch {
    cell: ArcSwapOption<SeqPayload>,
    stored_total: AtomicU64,
    /// Lazily-armed change notification: stays empty until the first
    /// `changed()` caller, so an unwatched watch's `store()` pays one
    /// `OnceLock::get` (an Acquire load + branch) and nothing else — the
    /// honest cost claim, benchmarked in `watch_tests` and re-checked by
    /// the on-target benchmark gate (fallback design if that gate ever trips: an
    /// `AtomicBool` fast-gate in front).
    watchers: std::sync::OnceLock<tokio::sync::Notify>,
    key: String,
    /// Keeps the Zenoh subscription alive; `None` for a detached watch.
    /// Undeclared implicitly on drop.
    _subscriber: parking_lot::Mutex<Option<zenoh::pubsub::Subscriber<()>>>,
}

impl std::fmt::Debug for Watch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watch")
            .field("key", &self.key)
            .field("stored_total", &self.stored_total.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Watch {
    fn new(key: String) -> Arc<Self> {
        Arc::new(Self {
            cell: ArcSwapOption::empty(),
            stored_total: AtomicU64::new(0),
            watchers: std::sync::OnceLock::new(),
            key,
            _subscriber: parking_lot::Mutex::new(None),
        })
    }

    /// A watch with no Zenoh subscription — the external-bus ingress shape
    /// (a device thread `store`s each tick; consumers `load`).
    pub fn detached(name: impl Into<String>) -> Arc<Self> {
        Self::new(name.into())
    }

    /// The latest value as an owned handle: one atomic Arc-clone, zero
    /// payload copy. `None` until the first publish/store.
    #[inline]
    pub fn load(&self) -> Option<Arc<ZBytes>> {
        self.cell.load().as_ref().map(|s| Arc::clone(&s.payload))
    }

    /// The latest snapshot as one atomic structured view (the single
    /// reader for anything beyond the bare payload). Seq and
    /// payload can never be observed mismatched; seq is 1-based and
    /// monotonic (change detection / dedup). `stamp_ns` is the SOURCE
    /// timestamp channel (the fusion workflow's input — see
    /// `WindowSync::feed`; `None` for unstamped publishes and foreign
    /// `store()`s). One Arc-clone, zero payload copy — the `load()`
    /// identity guarantee (`Arc::ptr_eq` across reads of one store) holds.
    #[inline]
    pub fn snapshot(&self) -> Option<Arc<SeqPayload>> {
        self.cell.load_full()
    }

    /// The latest snapshot's sequence (0 = nothing stored yet).
    #[inline]
    pub fn seq(&self) -> u64 {
        self.cell.load().as_ref().map(|s| s.seq).unwrap_or(0)
    }

    /// Hot-reader variant: no Arc-clone; the guard defers the displaced
    /// value's reclaim while held — hold it briefly.
    #[inline]
    pub fn load_guard(&self) -> WatchGuard {
        self.cell.load()
    }

    /// Foreign-producer store (thread-safe from any thread): moves the
    /// refcounted payload handle in; the previous value is reclaimed once
    /// the last reader drops it.
    /// Cost note: a store allocates TWO small control blocks —
    /// `Arc<SeqPayload>` (the snapshot) wrapping `Arc<ZBytes>` (the payload
    /// handle `load()` hands out zero-copy). The pair is what keeps
    /// seq+payload atomic AND keeps load() allocation-free; re-checked by
    /// the on-target benchmark gate.
    pub fn store(&self, payload: ZBytes) {
        self.store_stamped(payload, None);
    }

    /// [`store`](Self::store) carrying a source timestamp (ns) — what the
    /// Zenoh RX uses when the publisher stamped the sample.
    pub fn store_stamped(&self, payload: ZBytes, stamp_ns: Option<i64>) {
        self.stored_total.fetch_add(1, Ordering::Relaxed);
        let payload = Arc::new(payload);
        // seq derives from the DISPLACED snapshot inside one atomic
        // exchange (`rcu` = load + CAS, retried under writer contention) —
        // a separate counter + store let two racing writers publish out of
        // order, silently losing the newer payload and making `seq()`
        // observably non-monotonic (tokio's
        // `watch` locks around the same pair for the same reason). The
        // uncontended path — the only one the RX callback ever sees — is
        // one load + one CAS.
        self.cell.rcu(|current| {
            Some(Arc::new(SeqPayload {
                seq: current.as_ref().map(|s| s.seq).unwrap_or(0) + 1,
                stamp_ns,
                payload: Arc::clone(&payload),
            }))
        });
        // One Acquire load + branch when nobody ever awaited `changed()`.
        if let Some(notify) = self.watchers.get() {
            notify.notify_waiters();
        } else {
            // Startup-race pin: a FIRST `changed()`
            // racing this store is the store-buffer shape — we publish
            // the cell then read `watchers`; it initializes `watchers`
            // then reads the cell. Without SeqCst on both sides the
            // model lets BOTH reads see stale values (a missed wakeup
            // until the next store). The paired fences (here and in
            // `changed()`) forbid that; the cost lands only on the
            // no-watcher path, keeping the RX hot path at load+CAS.
            std::sync::atomic::fence(Ordering::SeqCst);
            if let Some(notify) = self.watchers.get() {
                notify.notify_waiters();
            }
        }
    }

    /// Await a store with a sequence ABOVE `seen`; returns the new seq.
    /// Snapshot the baseline with [`Watch::seq`] (or the seq from
    /// [`Watch::snapshot`]) BEFORE deciding to wait — the explicit baseline
    /// is what makes check-then-wait race-free. Latest-wins: intermediate
    /// values may be skipped; `load()` after return reads the newest.
    ///
    /// Lost-wakeup-free by the register-then-recheck pattern (the
    /// `Notified`-created-before-`notify_waiters` guarantee — same
    /// algorithm as `ReadinessSet::wait_wave_after`).
    pub async fn changed(&self, seen: u64) -> u64 {
        let notify = self.watchers.get_or_init(tokio::sync::Notify::new);
        // The other half of store_stamped's startup-race pin:
        // order the `watchers` publication above before the
        // first `seq()` read below, so a concurrent first store either
        // sees the initialized `watchers` (and notifies) or its cell
        // write is visible to our recheck. No-op once initialized.
        std::sync::atomic::fence(Ordering::SeqCst);
        loop {
            let notified = notify.notified();
            let now = self.seq();
            if now > seen {
                return now;
            }
            notified.await;
        }
    }

    /// Total values ever stored (RX + foreign), for observability/tests.
    pub fn stored_total(&self) -> u64 {
        self.stored_total.load(Ordering::Relaxed)
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Node {
    /// Declare a depth-1 latest-value watch cell over `topic`:
    /// the returned [`Watch`]'s `load()` is the streaming consumer path —
    /// no executor, no callback, no queue.
    pub async fn watch(&self, topic: &str) -> AortaResult<Arc<Watch>> {
        let key = self.key_builder().topic_key_sub(topic)?;
        let key_str: String = key.clone().into();
        let watch = Watch::new(key_str);
        let rx_watch = Arc::clone(&watch);
        let subscriber = self
            .session()
            .inner()
            .declare_subscriber(key)
            .callback(move |sample| {
                // RX path: move the payload handle into the cell — no byte
                // copy, no Event, no executor (the "negative cost" path).
                // `publisher_ns_of` converts the HLC NTP64 to REAL ns —
                // `get_time().as_u64()` is the raw fixed-point bit pattern
                // (~4.295x off), not nanoseconds.
                let stamp_ns = crate::flow::readiness::publisher_ns_of(&sample).map(|ns| ns as i64);
                let fields: zenoh::sample::SampleFields = sample.into();
                rx_watch.store_stamped(fields.payload, stamp_ns);
            })
            .await
            .map_err(|e| crate::error::AortaError::internal(e.to_string()))?;
        *watch._subscriber.lock() = Some(subscriber);
        Ok(watch)
    }
}

#[cfg(test)]
#[path = "../test/watch_tests.rs"]
mod tests;
