//! Per-thread lock-free SPSC ring buffer for trace records (recording hot path).
//!
//! Each tracing thread owns one [`ThreadRing`]; the single drain thread
//! ([`super::sink`]) is the only consumer. The hot path is one bounded-queue
//! append (a memcpy of a [`RingRecord`] + two relaxed/release atomics); when the
//! ring is full it **drops the newest** record and bumps a counter (surfaced in
//! the chunk header) — lossy by design, never blocking the producer.
//!
//! # Concurrency contract (the load-bearing `unsafe`)
//!
//! [`ThreadRing`] is a textbook single-producer/single-consumer queue:
//! - **Exactly one producer** — the owning thread — ever calls [`ThreadRing::push`].
//! - **Exactly one consumer** — the drain thread — ever calls
//!   [`ThreadRing::drain_into`].
//!
//! `head` is written only by the producer (Release) and read by the consumer
//! (Acquire); `tail` is written only by the consumer (Release) and read by the
//! producer (Acquire). A slot is written by the producer strictly before the
//! `head` store that publishes it, and is not overwritten until the producer has
//! observed (Acquire) a `tail` that has advanced past it — so producer writes and
//! consumer reads of any given slot never overlap. After the owning thread exits
//! it sets `orphaned`; no further `push` occurs, so the drain can finish draining
//! and the registry can reclaim the ring.

use super::chunk::RingRecord;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

/// Per-thread ring capacity in records (power of two). 8192 × 72 B ≈ 576 KB per
/// traced thread. Sized so the ~few-ms drain interval rarely overflows under
/// sparse T1 load; verbose/high-rate sessions may drop (counted, not silent).
pub const RING_CAPACITY: usize = 8192;

/// A bounded SPSC ring of [`RingRecord`]. See the module contract for the safety
/// invariants behind the `UnsafeCell` slots.
pub struct ThreadRing {
    slots: Box<[UnsafeCell<RingRecord>]>,
    mask: usize,
    /// Producer cursor (monotonic; index = `head & mask`).
    head: AtomicUsize,
    /// Consumer cursor (monotonic; index = `tail & mask`).
    tail: AtomicUsize,
    /// Records dropped because the ring was full since the last drain.
    dropped: AtomicU64,
    /// Set by the owning thread on exit; the drain reclaims once fully drained.
    orphaned: AtomicBool,
}

// SAFETY: access is the disciplined SPSC documented at the module level — the
// `UnsafeCell` slots are only touched by one producer and one consumer whose
// accesses are ordered by the head/tail acquire/release handshake, so they never
// alias concurrently. `RingRecord` is `Copy` POD.
unsafe impl Sync for ThreadRing {}
// SAFETY: ownership of the `Arc<ThreadRing>` moves to the registry/drain; the
// type contains only atomics + POD slots, safe to send.
unsafe impl Send for ThreadRing {}

impl ThreadRing {
    fn new() -> Self {
        assert!(RING_CAPACITY.is_power_of_two());
        let mut v = Vec::with_capacity(RING_CAPACITY);
        v.resize_with(RING_CAPACITY, || UnsafeCell::new(RingRecord::default()));
        ThreadRing {
            slots: v.into_boxed_slice(),
            mask: RING_CAPACITY - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            orphaned: AtomicBool::new(false),
        }
    }

    /// Append one record (producer side / hot path). Drops the newest record and
    /// bumps the dropped counter when full. Single-producer only.
    #[inline]
    pub fn push(&self, rec: &RingRecord) {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= RING_CAPACITY {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let idx = head & self.mask;
        // SAFETY: SPSC contract — this slot is free (head has not lapped tail)
        // and only this producer thread writes it; the Release store below
        // publishes the write to the consumer.
        unsafe {
            *self.slots[idx].get() = *rec;
        }
        self.head.store(head.wrapping_add(1), Ordering::Release);
    }

    /// Drain all currently-available records into `out` (consumer / drain side).
    /// Returns the number of records dropped since the previous drain (and resets
    /// that counter). Single-consumer only.
    pub fn drain_into(&self, out: &mut Vec<RingRecord>) -> u64 {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        while tail != head {
            let idx = tail & self.mask;
            // SAFETY: SPSC contract — indices in `tail..head` were fully written
            // by the producer before its Release store of `head` that this
            // Acquire load observed; the producer will not overwrite them until
            // it observes our Release store of the advanced `tail` below.
            let rec = unsafe { *self.slots[idx].get() };
            out.push(rec);
            tail = tail.wrapping_add(1);
        }
        self.tail.store(tail, Ordering::Release);
        self.dropped.swap(0, Ordering::Relaxed)
    }

    /// Is the producing thread gone?
    fn is_orphaned(&self) -> bool {
        self.orphaned.load(Ordering::Acquire)
    }

    /// Mark the producing thread as exited (called from the thread-local Drop).
    pub fn mark_orphaned(&self) {
        self.orphaned.store(true, Ordering::Release);
    }

    /// Are there no unread records?
    fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Relaxed)
    }
}

/// Process-global registry of live rings (one per traced thread). The drain
/// snapshots this under the lock, then drains each ring lock-free.
static RINGS: OnceLock<Mutex<Vec<Arc<ThreadRing>>>> = OnceLock::new();

fn rings() -> &'static Mutex<Vec<Arc<ThreadRing>>> {
    RINGS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Create a fresh ring for the calling thread and register it. Cold: once per
/// traced thread. The returned `Arc` is held thread-locally; the registry holds
/// a second `Arc` so the drain can keep reading after the thread exits.
pub fn register_thread_ring() -> Arc<ThreadRing> {
    let ring = Arc::new(ThreadRing::new());
    rings().lock().push(Arc::clone(&ring));
    ring
}

/// Drain every registered ring into `out`, returning the total dropped count.
/// Reclaims rings whose owning thread has exited and which are fully drained.
/// Called only by the single drain thread.
pub fn drain_all(out: &mut Vec<RingRecord>) -> u64 {
    // Snapshot the Arcs under the lock, then drain without holding it (a producer
    // registering a new thread must not block on the drain).
    let snapshot: Vec<Arc<ThreadRing>> = rings().lock().iter().cloned().collect();
    let mut dropped = 0u64;
    for ring in &snapshot {
        dropped += ring.drain_into(out);
    }
    // Reclaim orphaned + empty rings (drain ran first, so anything orphaned and
    // still empty has been fully consumed).
    rings()
        .lock()
        .retain(|r| !(r.is_orphaned() && r.is_empty()));
    dropped
}

/// Number of registered rings (for tests / introspection).
pub fn ring_count() -> usize {
    rings().lock().len()
}

#[cfg(test)]
#[path = "../../test/trace_ring_tests.rs"]
mod tests;
