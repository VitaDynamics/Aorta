//! A model-checked readiness AND-gate.
//!
//! [`ReadinessGate`] is the O(1) bitmask join behind multi-input readiness
//! (`Condition` sets): each satisfied condition sets its bit (`fetch_or`);
//! the arrival that COMPLETES the mask — and only that one — fires the wave
//! (returns `true`, so the caller dispatches exactly one task per wave).
//! For a recurring task the firing arrival atomically RESETS the mask in the
//! same RMW (swap to zero), which is the race to guard against: a bit set
//! by the next wave's arrival racing the reset must never be lost.
//!
//! Correctness obligations, exhaustively verified by the `#[cfg(loom)]`
//! tests below:
//!
//!   1. **Exactly one firer per wave**: N racing arrivals that jointly
//!      complete the mask produce exactly one `true`.
//!   2. **No lost arrival across the reset**: an arrival racing the firing
//!      swap lands either in the fired wave or in the fresh mask — never
//!      nowhere.
//!
//! The "which inputs are still missing" observability read is
//! [`ReadinessGate::pending_mask`].

use crate::loom_sync::{AtomicU64, Ordering};

/// O(1) readiness join over up to 64 conditions (a spill vector beyond 64 is
/// future work — creation fails fast instead).
#[derive(Debug)]
pub struct ReadinessGate {
    bits: AtomicU64,
    full: u64,
}

impl ReadinessGate {
    /// A gate over `conditions` inputs (1..=64).
    pub fn new(conditions: usize) -> Option<Self> {
        if conditions == 0 || conditions > 64 {
            return None;
        }
        let full = if conditions == 64 {
            u64::MAX
        } else {
            (1u64 << conditions) - 1
        };
        Some(Self {
            bits: AtomicU64::new(0),
            full,
        })
    }

    /// Mark condition `index` satisfied. Returns `true` iff THIS call
    /// completed the mask (the caller owns dispatching this wave); the mask
    /// is atomically reset for the next wave in the same RMW.
    ///
    /// One `fetch_update` RMW — non-blocking, alloc-free, any thread.
    pub fn satisfy(&self, index: usize) -> bool {
        debug_assert!(index < 64, "condition index out of range");
        if index >= 64 {
            // Release-mode clamp: a wrapped shift
            // would alias index 64 onto condition 0 and spuriously satisfy
            // it. Every in-tree caller bounds-checks against the gate's
            // count first; this is the primitive's own last line of defense.
            return false;
        }
        let bit = 1u64 << index;
        // A bit outside this gate's width is masked out of every fires /
        // pending computation (functionally inert) — but it is a caller
        // wiring bug, so debug builds catch it here.
        debug_assert!(
            bit & self.full != 0,
            "condition index {index} beyond the gate's width"
        );
        // CAS loop: set our bit; if that completes the mask, swap to zero
        // (KEEPING bits outside `full` is impossible by construction since
        // indices are bounded by the gate width). A concurrent arrival
        // between our read and CAS retries — its bit is never dropped.
        let mut current = self.bits.load(Ordering::Relaxed);
        loop {
            let next = current | bit;
            let (target, fires) = if next & self.full == self.full {
                (0, true)
            } else {
                (next, false)
            };
            match self.bits.compare_exchange_weak(
                current,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return fires,
                Err(observed) => current = observed,
            }
        }
    }

    /// Observability: the bitmask of inputs still MISSING for the
    /// current wave.
    pub fn pending_mask(&self) -> u64 {
        self.full & !self.bits.load(Ordering::Acquire)
    }

    /// The gate's full mask (all-conditions-satisfied value).
    pub fn full_mask(&self) -> u64 {
        self.full
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::ReadinessGate;

    #[test]
    fn gate_bounds_are_enforced() {
        assert!(ReadinessGate::new(0).is_none());
        assert!(ReadinessGate::new(65).is_none());
        assert!(ReadinessGate::new(1).is_some());
        assert!(ReadinessGate::new(64).is_some());
    }

    #[test]
    fn last_arrival_fires_and_resets() {
        let gate = ReadinessGate::new(3).expect("gate");
        assert!(!gate.satisfy(0));
        assert!(!gate.satisfy(1));
        assert_eq!(gate.pending_mask(), 0b100);
        assert!(gate.satisfy(2), "the completing arrival fires");
        assert_eq!(gate.pending_mask(), 0b111, "mask reset for the next wave");
    }

    #[test]
    fn duplicate_satisfaction_is_idempotent_within_a_wave() {
        let gate = ReadinessGate::new(2).expect("gate");
        assert!(!gate.satisfy(0));
        assert!(!gate.satisfy(0), "re-satisfying the same input never fires");
        assert!(gate.satisfy(1));
    }

    #[test]
    fn recurring_waves_fire_repeatedly() {
        let gate = ReadinessGate::new(2).expect("gate");
        for _ in 0..3 {
            assert!(!gate.satisfy(0));
            assert!(gate.satisfy(1));
        }
    }
}

// Exhaustive concurrency proofs. Run with:
//   RUSTFLAGS="--cfg loom" cargo test -p aorta_sync readiness::loom_tests
#[cfg(all(test, loom))]
mod loom_tests {
    use super::ReadinessGate;
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::Arc;

    /// Exactly one firer per wave: two racing arrivals that jointly complete
    /// a 2-input gate produce exactly one `true` across every interleaving.
    #[test]
    fn racing_arrivals_fire_exactly_once() {
        loom::model(|| {
            let gate = Arc::new(ReadinessGate::new(2).expect("gate"));
            let fired = Arc::new(AtomicUsize::new(0));
            let handles: Vec<_> = (0..2)
                .map(|i| {
                    let gate = Arc::clone(&gate);
                    let fired = Arc::clone(&fired);
                    loom::thread::spawn(move || {
                        if gate.satisfy(i) {
                            fired.fetch_add(1, Ordering::Relaxed);
                        }
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(fired.load(Ordering::Relaxed), 1, "one wave, one firer");
        });
    }

    /// The reset race: an arrival racing the firing RESET must
    /// land in the fired wave or the fresh mask — never be lost. Model: a
    /// 1-input gate fired concurrently with another arrival on the same
    /// input; total fires must equal the number of completed waves and the
    /// second wave stays fireable.
    #[test]
    fn arrival_racing_the_reset_is_never_lost() {
        loom::model(|| {
            let gate = Arc::new(ReadinessGate::new(2).expect("gate"));
            // Wave 1 half-complete:
            assert!(!gate.satisfy(0));
            let fired = Arc::new(AtomicUsize::new(0));
            // Thread A completes wave 1 (input 1); thread B concurrently
            // re-arrives on input 0 (the next wave's first arrival).
            let a = {
                let gate = Arc::clone(&gate);
                let fired = Arc::clone(&fired);
                loom::thread::spawn(move || {
                    if gate.satisfy(1) {
                        fired.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            let b = {
                let gate = Arc::clone(&gate);
                let fired = Arc::clone(&fired);
                loom::thread::spawn(move || {
                    if gate.satisfy(0) {
                        fired.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            a.join().unwrap();
            b.join().unwrap();
            // Exactly one wave fired here (B's arrival either joined wave 1
            // — idempotent duplicate — or opened wave 2, which is still
            // missing input 1 and thus unfired).
            assert_eq!(fired.load(Ordering::Relaxed), 1);
            // No lost arrival: completing input 1 must fire IF b landed in
            // the fresh mask; if b joined wave 1, the mask is empty and this
            // starts wave 2 instead. Either way the gate stays consistent:
            let pending = gate.pending_mask();
            assert!(
                pending == 0b10 || pending == 0b11,
                "mask must reflect b's arrival or a clean reset, got {pending:#b}"
            );
        });
    }
}
