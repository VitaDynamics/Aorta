//! A model-checked in-flight request counter.
//!
//! [`InFlightCounter`] is the shared "how many requests are currently
//! executing" counter behind a service's drain-on-shutdown handshake (in
//! `aorta_core`'s `service` module): producers [`incr`](InFlightCounter::incr)
//! on dispatch, consumers [`decr`](InFlightCounter::decr) on completion, and
//! the *one* decrement that drives the count `1 → 0` returns `true` so the
//! caller can wake an idle waiter (`Notify::notify_waiters`). A drainer
//! (`wait_for_idle`) registers its waiter, re-checks the count, then sleeps —
//! the classic register-then-recheck lost-wakeup idiom.
//!
//! Its correctness obligations, both **exhaustively verified by the
//! `#[cfg(loom)]` tests below** across every thread interleaving:
//!
//!   1. **No underflow.** A stray `decr` at zero is a no-op, never a wrap to
//!      `u64::MAX` — otherwise `wait_for_idle`'s `count == 0` check would never
//!      again be true and shutdown would hang forever.
//!   2. **Exactly-once drain signal.** The `1 → 0` edge returns `true` exactly
//!      once: never zero times (a lost wakeup → hung shutdown) and never twice
//!      (a spurious wake). This is the Rust-side analogue of the C++ callback
//!      in-flight drain (`cpp/src/*.cc` `CallbackScopeGuard`) — the same bug
//!      class ThreadSanitizer can only sample; here loom proves it.
//!
//! The atomics route through [`crate::loom_sync`] so loom can intercept them;
//! in production they compile to plain `std` atomics with no overhead.

use crate::loom_sync::{AtomicU64, Ordering};

/// Shared count of in-flight items behind a drain-on-idle handshake.
///
/// See the [module docs](self) for the correctness contract loom verifies.
#[derive(Debug)]
pub struct InFlightCounter {
    count: AtomicU64,
}

impl Default for InFlightCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl InFlightCounter {
    /// A fresh counter at zero in-flight.
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
        }
    }

    /// Register a newly dispatched in-flight item.
    #[inline]
    pub fn incr(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Retire one in-flight item.
    ///
    /// Returns `true` iff this call drove the count from `1` to `0` — i.e. the
    /// caller now owns the "became idle" edge and should wake any drainer. A
    /// `decr` on an already-zero counter is a no-op returning `false`; that is
    /// precisely what keeps the counter from underflow-wrapping.
    pub fn decr(&self) -> bool {
        let mut current = self.count.load(Ordering::Relaxed);
        while current > 0 {
            // The pre-decrement value `current` is the CAS expectation, so
            // EXACTLY ONE racing decrementer can win the `1 → 0` transition
            // (the one whose `current == 1` swap succeeds) — that is what makes
            // the "became idle" signal fire exactly once. `compare_exchange_weak`
            // may fail spuriously; the loop retries with the observed value.
            match self.count.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return current == 1,
                Err(observed) => current = observed,
            }
        }
        false
    }

    /// Current in-flight count — a Relaxed point-in-time observation (the
    /// `Notify` handshake, not this load, carries the drain happens-before).
    #[inline]
    pub fn get(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

// Single-threaded behavioural tests for the normal (std) build — these run
// under `cargo test` / `bazel test`. The exhaustive concurrent proofs live in
// the `#[cfg(loom)]` module below and run only under `--cfg loom`.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::InFlightCounter;

    #[test]
    fn new_starts_idle() {
        let c = InFlightCounter::new();
        assert_eq!(c.get(), 0);
        assert_eq!(InFlightCounter::default().get(), 0);
    }

    #[test]
    fn decr_signals_only_on_the_one_to_zero_edge() {
        let c = InFlightCounter::new();
        c.incr();
        c.incr();
        assert_eq!(c.get(), 2);
        assert!(!c.decr(), "2 -> 1 is not the idle edge");
        assert!(c.decr(), "1 -> 0 is the idle edge");
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn decr_at_zero_is_a_no_op_not_an_underflow() {
        let c = InFlightCounter::new();
        assert!(!c.decr(), "decr at zero returns false");
        assert_eq!(c.get(), 0, "must not wrap to u64::MAX");
    }
}

// Exhaustive concurrency proofs. Run with:
//   RUSTFLAGS="--cfg loom" cargo test -p aorta_sync in_flight::loom_tests
// Gated on `test` too (not just `loom`) so the module is excluded from the
// non-test lib build under `--cfg loom`, where the `#[test]` fns would be dead
// code and trip unused-import warnings.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::InFlightCounter;
    use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use loom::sync::Arc;

    /// Two requests in flight, two replies racing to retire them: across every
    /// interleaving the counter must drain to exactly `0` (no underflow) AND
    /// the "became idle" edge must fire EXACTLY ONCE — the property
    /// `wait_for_idle` relies on to neither hang (zero signals) nor be
    /// spuriously woken (two signals).
    #[test]
    fn concurrent_drain_fires_idle_signal_exactly_once() {
        loom::model(|| {
            let counter = Arc::new(InFlightCounter::new());
            counter.incr();
            counter.incr();

            let idle_signals = Arc::new(AtomicUsize::new(0));
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let counter = Arc::clone(&counter);
                    let idle_signals = Arc::clone(&idle_signals);
                    loom::thread::spawn(move || {
                        if counter.decr() {
                            idle_signals.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }

            assert_eq!(
                counter.get(),
                0,
                "counter must fully drain without underflow"
            );
            assert_eq!(
                idle_signals.load(Ordering::Relaxed),
                1,
                "the 1->0 drain signal must fire exactly once across all interleavings"
            );
        });
    }

    /// One in-flight item, two retirements racing for it: one wins the `1 → 0`
    /// edge, the other observes zero and is a no-op. The count must never
    /// underflow past zero (which would wrap to `u64::MAX` and strand a
    /// drainer forever).
    #[test]
    fn over_decrement_never_underflows() {
        loom::model(|| {
            let counter = Arc::new(InFlightCounter::new());
            counter.incr();

            let signals = Arc::new(AtomicUsize::new(0));
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let counter = Arc::clone(&counter);
                    let signals = Arc::clone(&signals);
                    loom::thread::spawn(move || {
                        if counter.decr() {
                            signals.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }

            assert_eq!(counter.get(), 0, "count must not underflow past zero");
            assert_eq!(
                signals.load(Ordering::Relaxed),
                1,
                "exactly one retirement may claim the single in-flight item"
            );
        });
    }

    /// Negative control — proof the harness has teeth. The NAIVE decrement
    /// (`fetch_sub` then a *separate* `load == 0`, instead of an atomic RMW
    /// decision) lets two racing retirements both observe zero and double-fire
    /// the idle signal. We assert loom FINDS that interleaving (the inner model
    /// panics, which we catch), so this test passing is durable evidence loom
    /// would catch a regression of [`InFlightCounter::decr`] back to the naive
    /// form. Run it directly to watch loom report the bad schedule.
    #[test]
    fn loom_detects_the_naive_non_atomic_decrement() {
        let detected = std::panic::catch_unwind(|| {
            loom::model(|| {
                let count = Arc::new(AtomicU64::new(2));
                let signals = Arc::new(AtomicUsize::new(0));
                let handles: Vec<_> = (0..2)
                    .map(|_| {
                        let count = Arc::clone(&count);
                        let signals = Arc::clone(&signals);
                        loom::thread::spawn(move || {
                            // BUGGY: the decrement and the "am I zero?" decision
                            // are two separate atomic ops, not one RMW.
                            count.fetch_sub(1, Ordering::Relaxed);
                            if count.load(Ordering::Relaxed) == 0 {
                                signals.fetch_add(1, Ordering::Relaxed);
                            }
                        })
                    })
                    .collect();
                for handle in handles {
                    handle.join().unwrap();
                }
                // The naive form CAN reach signals == 2; asserting == 1 makes
                // loom's exhaustive search panic on the bad interleaving.
                assert_eq!(signals.load(Ordering::Relaxed), 1);
            });
        })
        .is_err();

        assert!(
            detected,
            "loom should have found an interleaving where the naive decrement double-signals"
        );
    }
}
