//! Tests for the lock-free FixedHistogram (Step 3).

use crate::trace::histogram::{bucket_index, bucket_upper_bound, BUCKET_COUNT};
use crate::trace::FixedHistogram;
use std::sync::Arc;

#[test]
fn bucket_index_classifies_known_values() {
    assert_eq!(bucket_index(0), 0);
    assert_eq!(bucket_index(1), 1);
    assert_eq!(bucket_index(2), 2);
    assert_eq!(bucket_index(3), 2); // [2,4)
    assert_eq!(bucket_index(4), 3); // [4,8)
    assert_eq!(bucket_index(1000), 10); // [512,1024)
    assert_eq!(bucket_index(1023), 10);
    assert_eq!(bucket_index(1024), 11); // [1024,2048)
    assert_eq!(bucket_index(u64::MAX), 64);
}

#[test]
fn bucket_upper_bounds_are_powers_of_two_minus_one() {
    assert_eq!(bucket_upper_bound(0), 0);
    assert_eq!(bucket_upper_bound(1), 1);
    assert_eq!(bucket_upper_bound(10), 1023);
    assert_eq!(bucket_upper_bound(11), 2047);
}

#[test]
fn histogram_records_into_correct_bucket() {
    let h = FixedHistogram::new();
    h.record(1000);
    h.record(1023);
    h.record(1024);
    let snap = h.snapshot();
    assert_eq!(snap.buckets[10], 2, "1000 and 1023 fall in bucket 10");
    assert_eq!(snap.buckets[11], 1, "1024 falls in bucket 11");
    assert_eq!(snap.count, 3);
}

#[test]
fn histogram_tracks_count_sum_max() {
    let h = FixedHistogram::new();
    for &v in &[10u64, 200, 3000, 40000] {
        h.record(v);
    }
    let snap = h.snapshot();
    assert_eq!(snap.count, 4);
    assert_eq!(snap.sum_ns, 10 + 200 + 3000 + 40000);
    assert_eq!(snap.max_ns, 40000);
    assert_eq!(snap.mean_ns(), (10 + 200 + 3000 + 40000) / 4);
}

#[test]
fn percentiles_empty_histogram_are_zero() {
    let snap = FixedHistogram::new().snapshot();
    assert_eq!(snap.p50_ns(), 0);
    assert_eq!(snap.p99_ns(), 0);
    assert_eq!(snap.mean_ns(), 0);
}

#[test]
fn percentiles_on_uniform_load_land_in_expected_bucket() {
    // 100 identical 1000ns samples: every percentile is in bucket 10 → upper
    // bound 1023.
    let h = FixedHistogram::new();
    for _ in 0..100 {
        h.record(1000);
    }
    let snap = h.snapshot();
    assert_eq!(snap.p50_ns(), 1023);
    assert_eq!(snap.p95_ns(), 1023);
    assert_eq!(snap.p99_ns(), 1023);
}

#[test]
fn percentiles_are_monotonic_and_ordered_across_a_spread() {
    // A spread skewed low: 90 small, 9 medium, 1 large. p50 < p95 <= p99.
    let h = FixedHistogram::new();
    for _ in 0..90 {
        h.record(500); // bucket 9 → ub 511
    }
    for _ in 0..9 {
        h.record(50_000); // bucket 16 → ub 65535
    }
    h.record(5_000_000); // bucket 23 → ub 8_388_607
    let snap = h.snapshot();
    assert_eq!(snap.count, 100);
    assert!(snap.p50_ns() <= snap.p95_ns());
    assert!(snap.p95_ns() <= snap.p99_ns());
    assert_eq!(snap.p50_ns(), bucket_upper_bound(9)); // 90% are at 500ns
    assert!(snap.p99_ns() >= bucket_upper_bound(16));
}

#[test]
fn record_is_lock_free_threadsafe() {
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 10_000;
    let h = Arc::new(FixedHistogram::new());
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let h = Arc::clone(&h);
        handles.push(std::thread::spawn(move || {
            for i in 0..PER_THREAD {
                // vary values so multiple buckets are exercised concurrently
                h.record((t + 1) * (i % 2048 + 1));
            }
        }));
    }
    for handle in handles {
        handle.join().expect("record thread panicked");
    }
    let snap = h.snapshot();
    assert_eq!(snap.count, THREADS * PER_THREAD);
    let bucket_total: u64 = snap.buckets.iter().sum();
    assert_eq!(
        bucket_total, snap.count,
        "every observation must land in exactly one bucket"
    );
    assert_eq!(snap.buckets.len(), BUCKET_COUNT);
}
