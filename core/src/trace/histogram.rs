//! Lock-free fixed-bucket latency histogram for tracing (Step 3).
//!
//! Power-of-two nanosecond buckets give coarse (bucket-bounded) but cheap and
//! allocation-free latency distributions. [`FixedHistogram::record`] is a
//! branch-free bucket index (via `leading_zeros`) plus relaxed `fetch_add`s and
//! a small CAS loop for the running max — no locks, no heap. This is the
//! tracing-OWNED histogram; it does not live in or import `crate::telemetry`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of buckets. Bucket `k` covers `[2^(k-1), 2^k)` ns for `k >= 1`, bucket
/// `0` is exactly `0` ns. Index range is `0..=64` (a `u64` has at most 64 bits),
/// hence 65 slots. ~520 bytes per histogram.
pub const BUCKET_COUNT: usize = 65;

/// Branch-free bucket index for a duration in nanoseconds: the number of
/// significant bits of `ns` (`0` for `0`, else `floor(log2(ns)) + 1`).
#[inline(always)]
pub fn bucket_index(ns: u64) -> usize {
    (u64::BITS - ns.leading_zeros()) as usize
}

/// Inclusive upper bound (largest value) of bucket `k`, used for approximate
/// percentile reporting. Bucket `0` is `0`; bucket `k` upper bound is `2^k - 1`.
#[inline]
pub fn bucket_upper_bound(k: usize) -> u64 {
    if k == 0 {
        0
    } else if k >= 64 {
        u64::MAX
    } else {
        (1u64 << k) - 1
    }
}

/// A lock-free latency histogram. Construct once (`const fn new`), then `record`
/// concurrently from any thread; snapshot for export/analysis.
#[derive(Debug)]
pub struct FixedHistogram {
    buckets: [AtomicU64; BUCKET_COUNT],
    count: AtomicU64,
    sum_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Default for FixedHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl FixedHistogram {
    /// A new empty histogram. `const` so it can back a `static`/`OnceLock`.
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BUCKET_COUNT],
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
        }
    }

    /// Record one observation. Lock-free, allocation-free; safe to call from any
    /// thread concurrently. All counters use `Relaxed` ordering (histogram
    /// counts have no cross-variable ordering requirements).
    #[inline]
    pub fn record(&self, ns: u64) {
        let idx = bucket_index(ns);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        // Wraps after ~2^64 ns (~584 years of accumulated latency); the derived
        // `mean_ns` is advisory, and the bucket distribution + max are unaffected.
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        // Running max via a CAS loop (only spins under genuine contention with a
        // larger concurrent sample).
        let mut cur = self.max_ns.load(Ordering::Relaxed);
        while ns > cur {
            match self
                .max_ns
                .compare_exchange_weak(cur, ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Total number of recorded observations.
    #[inline]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Take a consistent-enough snapshot for export. Reads are relaxed; if taken
    /// while `record` runs concurrently the totals may be momentarily skewed, so
    /// snapshot after writers quiesce when exact totals matter.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut buckets = [0u64; BUCKET_COUNT];
        for (dst, src) in buckets.iter_mut().zip(self.buckets.iter()) {
            *dst = src.load(Ordering::Relaxed);
        }
        HistogramSnapshot {
            buckets,
            count: self.count.load(Ordering::Relaxed),
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
        }
    }
}

/// An immutable point-in-time view of a [`FixedHistogram`], with derived stats.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    pub buckets: [u64; BUCKET_COUNT],
    pub count: u64,
    pub sum_ns: u64,
    pub max_ns: u64,
}

impl HistogramSnapshot {
    /// Arithmetic mean in ns (`0` when empty).
    pub fn mean_ns(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.sum_ns / self.count
        }
    }

    /// Approximate percentile in ns for `q` in `0.0..=1.0`, reported as the
    /// inclusive upper bound of the bucket that contains the `q`-th observation.
    /// Bucket-bounded (coarse to a factor of 2); `0` when empty.
    pub fn percentile_ns(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let q = q.clamp(0.0, 1.0);
        // Rank of the target observation (1-based), rounded up.
        let target = (((self.count as f64) * q).ceil() as u64).clamp(1, self.count);
        let mut cumulative = 0u64;
        for (k, &c) in self.buckets.iter().enumerate() {
            cumulative += c;
            if cumulative >= target {
                return bucket_upper_bound(k);
            }
        }
        self.max_ns
    }

    pub fn p50_ns(&self) -> u64 {
        self.percentile_ns(0.50)
    }
    pub fn p95_ns(&self) -> u64 {
        self.percentile_ns(0.95)
    }
    pub fn p99_ns(&self) -> u64 {
        self.percentile_ns(0.99)
    }
}
