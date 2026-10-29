//! A model-checked wake-coalescing slot (the "scheduled flag" protocol).
//!
//! [`WakeSlot`] is the per-source guard in front of Aorta's reserved
//! `CoroutineResume` event lane: a producer that has published a payload to
//! its source queue calls
//! [`arm`](WakeSlot::arm), and only the caller that wins the disarmed→armed
//! transition enqueues the (single) wake event onto the lane; the consumer
//! [`disarm`](WakeSlot::disarm)s **before** draining the source queue
//! (clear-before-poll), so a producer racing the drain re-arms and enqueues a
//! follow-up wake. Together these give the lane its capacity discipline:
//! **at most one wake outstanding per live source**, which is what makes the
//! lane's push infallible-by-construction ("full" is unrepresentable) with
//! memory bounded by the number of live sources.
//!
//! Correctness obligations, **exhaustively verified by the `#[cfg(loom)]`
//! tests below** across every thread interleaving:
//!
//!   1. **No lost wake.** Every published payload is observed by a drain:
//!      either the publisher wins `arm()` and enqueues a fresh wake, or a wake
//!      is already outstanding whose dispatch (disarm-then-drain) must observe
//!      the payload. The disarm-then-drain *order* is load-bearing — the
//!      negative-control test proves loom catches the drain-then-disarm bug.
//!   2. **At most one wake outstanding.** Between a winning `arm()` and the
//!      dispatching `disarm()`, every further `arm()` loses; wake events on
//!      the lane can never exceed the number of live sources.
//!   3. **Stale wakes are safe.** A dispatch that finds the source queue
//!      empty (payload already drained by a prior dispatch) is a no-op.
//!
//! Both operations are single `swap` RMWs — non-blocking, alloc-free, safe
//! from any thread (including Zenoh RX threads). The atomics route through
//! [`crate::loom_sync`] so loom can intercept them; in production they compile
//! to plain `std` atomics.

use crate::loom_sync::{AtomicBool, Ordering};

/// Wake-coalescing flag for one lane source.
///
/// See the [module docs](self) for the protocol and the correctness contract
/// loom verifies.
#[derive(Debug)]
pub struct WakeSlot {
    /// `true` while a wake event is outstanding on the lane (armed).
    armed: AtomicBool,
}

impl Default for WakeSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl WakeSlot {
    /// A fresh, disarmed slot (no wake outstanding).
    pub fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
        }
    }

    /// Producer side. Call **after** publishing the payload to the source
    /// queue. Returns `true` iff this caller won the disarmed→armed
    /// transition and MUST enqueue exactly one wake event; on `false` a wake
    /// is already outstanding and its dispatch will observe the payload.
    ///
    /// `AcqRel`: the swap is a release of the payload publish that precedes
    /// it and an acquire of the consumer's prior `disarm` — RMWs on the flag
    /// form a total order, which is what makes "losing the race" safe: a
    /// losing `arm()` is ordered before the dispatching `disarm()`, so the
    /// subsequent drain observes the payload.
    ///
    /// (`Release` alone would suffice while every payload rides its own
    /// synchronized queue, as all current sources do; `AcqRel` is kept
    /// deliberately so future payload paths that lean on the flag itself
    /// — e.g. a task-harness `scheduled` flag — stay correct
    /// without re-verification.)
    #[inline]
    pub fn arm(&self) -> bool {
        !self.armed.swap(true, Ordering::AcqRel)
    }

    /// Consumer side. Call **before** draining the source queue
    /// (clear-before-poll). After this returns, a producer publishing a new
    /// payload wins `arm()` again and enqueues a follow-up wake, so nothing
    /// published after the drain's snapshot is stranded.
    ///
    /// Returns `true` if the slot was armed (the normal dispatch case);
    /// `false` for a spurious/stale dispatch, which is a safe no-op.
    #[inline]
    pub fn disarm(&self) -> bool {
        self.armed.swap(false, Ordering::AcqRel)
    }

    /// Point-in-time observation (observability / tests only — the protocol
    /// never branches on a separate load).
    #[inline]
    pub fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }
}

// Single-threaded behavioural tests for the normal (std) build — these run
// under `cargo test` / `bazel test`. The exhaustive concurrent proofs live in
// the `#[cfg(loom)]` module below and run only under `--cfg loom`.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::WakeSlot;

    #[test]
    fn new_starts_disarmed() {
        let slot = WakeSlot::new();
        assert!(!slot.is_armed());
        assert!(!WakeSlot::default().is_armed());
    }

    #[test]
    fn first_arm_wins_repeat_arms_lose() {
        let slot = WakeSlot::new();
        assert!(slot.arm(), "first arm on a disarmed slot must win");
        assert!(slot.is_armed());
        assert!(!slot.arm(), "arm on an armed slot must lose (coalesce)");
        assert!(!slot.arm(), "every further arm keeps losing");
    }

    #[test]
    fn disarm_reports_armed_state_and_rearms_cleanly() {
        let slot = WakeSlot::new();
        assert!(!slot.disarm(), "disarm on a disarmed slot is a stale no-op");
        assert!(slot.arm());
        assert!(slot.disarm(), "disarm on an armed slot reports true");
        assert!(!slot.is_armed());
        assert!(slot.arm(), "slot is reusable after a full arm/disarm cycle");
    }
}

// Exhaustive concurrency proofs. Run with:
//   RUSTFLAGS="--cfg loom" cargo test -p aorta_sync wake_slot::loom_tests
// Gated on `test` too (not just `loom`) so the module is excluded from the
// non-test lib build under `--cfg loom`, where the `#[test]` fns would be dead
// code and trip unused-import warnings.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::WakeSlot;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::{Arc, Mutex};

    /// Drain everything currently in the source queue, returning the count.
    fn drain_all(source: &Mutex<Vec<u32>>) -> usize {
        let mut queue = source.lock().unwrap();
        let drained = queue.len();
        queue.clear();
        drained
    }

    /// Dispatch one wake: disarm FIRST (clear-before-poll), then drain.
    fn dispatch(slot: &WakeSlot, source: &Mutex<Vec<u32>>) -> usize {
        slot.disarm();
        drain_all(source)
    }

    /// THE no-lost-wake proof. Initial state:
    /// one payload published and its wake outstanding (armed). A producer
    /// publishes a second payload and arms; concurrently a dispatcher runs the
    /// outstanding wake (disarm-then-drain). Across every interleaving, after
    /// dispatching whatever wakes remain outstanding, BOTH payloads must have
    /// been drained — no payload is stranded and no wake is lost — and wakes
    /// outstanding never exceeded one.
    #[test]
    fn no_publish_is_stranded_across_arm_disarm_race() {
        loom::model(|| {
            let slot = Arc::new(WakeSlot::new());
            let source = Arc::new(Mutex::new(Vec::new()));
            let outstanding = Arc::new(AtomicUsize::new(0));
            let drained = Arc::new(AtomicUsize::new(0));

            // Seed: payload 0 published, its wake enqueued (slot armed).
            source.lock().unwrap().push(0);
            assert!(slot.arm());
            outstanding.fetch_add(1, Ordering::Relaxed);

            let producer = {
                let slot = Arc::clone(&slot);
                let source = Arc::clone(&source);
                let outstanding = Arc::clone(&outstanding);
                loom::thread::spawn(move || {
                    source.lock().unwrap().push(1);
                    if slot.arm() {
                        let prev = outstanding.fetch_add(1, Ordering::Relaxed);
                        assert_eq!(prev, 0, "a winning arm implies no wake outstanding");
                    }
                })
            };
            let dispatcher = {
                let slot = Arc::clone(&slot);
                let source = Arc::clone(&source);
                let outstanding = Arc::clone(&outstanding);
                let drained = Arc::clone(&drained);
                loom::thread::spawn(move || {
                    // Dispatch the seeded wake.
                    outstanding.fetch_sub(1, Ordering::Relaxed);
                    drained.fetch_add(dispatch(&slot, &source), Ordering::Relaxed);
                })
            };
            producer.join().unwrap();
            dispatcher.join().unwrap();

            // Executor quiescence: dispatch any wake still outstanding.
            while outstanding.load(Ordering::Relaxed) > 0 {
                outstanding.fetch_sub(1, Ordering::Relaxed);
                drained.fetch_add(dispatch(&slot, &source), Ordering::Relaxed);
            }

            assert_eq!(
                drained.load(Ordering::Relaxed),
                2,
                "every published payload must be observed by a drain"
            );
            assert!(
                source.lock().unwrap().is_empty(),
                "no payload may be stranded in the source queue"
            );
        });
    }

    /// Capacity discipline: two producers racing to arm a disarmed slot —
    /// exactly ONE wins and enqueues a wake, the other coalesces. This is the
    /// ≤1-outstanding-wake-per-source bound the reserved lane's memory
    /// discipline rests on.
    #[test]
    fn concurrent_arms_enqueue_exactly_one_wake() {
        loom::model(|| {
            let slot = Arc::new(WakeSlot::new());
            let wakes = Arc::new(AtomicUsize::new(0));
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let slot = Arc::clone(&slot);
                    let wakes = Arc::clone(&wakes);
                    loom::thread::spawn(move || {
                        if slot.arm() {
                            wakes.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(
                wakes.load(Ordering::Relaxed),
                1,
                "exactly one racing producer may enqueue the wake"
            );
        });
    }

    /// Negative control — proof the harness has teeth. The BUGGY dispatch
    /// order (drain THEN disarm) strands a payload: the producer publishes
    /// between the drain and the disarm, loses `arm()` against the
    /// still-armed slot, and no follow-up wake is ever enqueued. We assert
    /// loom FINDS that interleaving (the inner model panics, which we catch),
    /// so this test passing is durable evidence loom would catch a regression
    /// of the clear-before-poll order in [`WakeSlot`]'s consumers.
    #[test]
    fn loom_detects_the_drain_before_disarm_bug() {
        let detected = std::panic::catch_unwind(|| {
            loom::model(|| {
                let slot = Arc::new(WakeSlot::new());
                let source = Arc::new(Mutex::new(Vec::new()));
                let outstanding = Arc::new(AtomicUsize::new(0));
                let drained = Arc::new(AtomicUsize::new(0));

                source.lock().unwrap().push(0);
                assert!(slot.arm());
                outstanding.fetch_add(1, Ordering::Relaxed);

                let producer = {
                    let slot = Arc::clone(&slot);
                    let source = Arc::clone(&source);
                    let outstanding = Arc::clone(&outstanding);
                    loom::thread::spawn(move || {
                        source.lock().unwrap().push(1);
                        if slot.arm() {
                            outstanding.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                };
                let dispatcher = {
                    let slot = Arc::clone(&slot);
                    let source = Arc::clone(&source);
                    let outstanding = Arc::clone(&outstanding);
                    let drained = Arc::clone(&drained);
                    loom::thread::spawn(move || {
                        outstanding.fetch_sub(1, Ordering::Relaxed);
                        // BUGGY: drain first, disarm after — the window
                        // between them strands a concurrent publish.
                        drained.fetch_add(drain_all(&source), Ordering::Relaxed);
                        slot.disarm();
                    })
                };
                producer.join().unwrap();
                dispatcher.join().unwrap();

                while outstanding.load(Ordering::Relaxed) > 0 {
                    outstanding.fetch_sub(1, Ordering::Relaxed);
                    drained.fetch_add(
                        {
                            slot.disarm();
                            drain_all(&source)
                        },
                        Ordering::Relaxed,
                    );
                }

                // The buggy order CAN strand payload 1; asserting full
                // delivery makes loom's exhaustive search panic on the bad
                // interleaving.
                assert_eq!(drained.load(Ordering::Relaxed), 2);
            });
        })
        .is_err();

        assert!(
            detected,
            "loom should have found an interleaving where drain-before-disarm strands a payload"
        );
    }
}
