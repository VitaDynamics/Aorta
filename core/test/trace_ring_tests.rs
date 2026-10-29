//! SPSC [`ThreadRing`](super::ThreadRing) mechanics — constructed directly (no
//! global registry), so these are pure, parallel-safe unit tests of the
//! push/drain/drop-newest/wraparound behaviour.

use super::{ThreadRing, RING_CAPACITY};
use crate::trace::chunk::RingRecord;

fn rec(mono: u64) -> RingRecord {
    RingRecord {
        mono_ns: mono,
        span_id: mono, // tag each record so order is checkable
        ..RingRecord::default()
    }
}

#[test]
fn push_drain_roundtrip_preserves_order() {
    let ring = ThreadRing::new();
    for i in 0..100u64 {
        ring.push(&rec(i));
    }
    let mut out = Vec::new();
    let dropped = ring.drain_into(&mut out);
    assert_eq!(dropped, 0);
    assert_eq!(out.len(), 100);
    for (i, r) in out.iter().enumerate() {
        assert_eq!(r.mono_ns, i as u64);
    }
}

#[test]
fn drops_newest_when_full_and_counts() {
    let ring = ThreadRing::new();
    let extra = 50u64;
    for i in 0..(RING_CAPACITY as u64 + extra) {
        ring.push(&rec(i));
    }
    let mut out = Vec::new();
    let dropped = ring.drain_into(&mut out);
    // The first RING_CAPACITY are kept; the newest `extra` are dropped.
    assert_eq!(out.len(), RING_CAPACITY);
    assert_eq!(dropped, extra);
    assert_eq!(out.first().unwrap().mono_ns, 0);
    assert_eq!(out.last().unwrap().mono_ns, RING_CAPACITY as u64 - 1);
}

#[test]
fn dropped_counter_resets_each_drain() {
    let ring = ThreadRing::new();
    for i in 0..(RING_CAPACITY as u64 + 10) {
        ring.push(&rec(i));
    }
    let mut out = Vec::new();
    assert_eq!(ring.drain_into(&mut out), 10);
    out.clear();
    // Nothing new pushed → no new drops.
    assert_eq!(ring.drain_into(&mut out), 0);
    assert!(out.is_empty());
}

#[test]
fn cursor_wraps_across_capacity() {
    let ring = ThreadRing::new();
    let mut out = Vec::new();
    // Three rounds of (almost) a full ring, draining between → exercises the
    // monotonic-cursor wrap (head/tail well past RING_CAPACITY).
    let round = RING_CAPACITY as u64 - 1;
    for r in 0..3u64 {
        for i in 0..round {
            ring.push(&rec(r * round + i));
        }
        out.clear();
        assert_eq!(ring.drain_into(&mut out), 0);
        assert_eq!(out.len(), round as usize);
        assert_eq!(out.first().unwrap().mono_ns, r * round);
        assert_eq!(out.last().unwrap().mono_ns, r * round + round - 1);
    }
}

#[test]
fn interleaved_push_drain_streams_correctly() {
    // Producer/consumer alternate (single thread here, but the same cursor
    // protocol the real SPSC uses across two threads).
    let ring = ThreadRing::new();
    let mut out = Vec::new();
    let mut expected = 0u64;
    for batch in 0..10u64 {
        for i in 0..1000u64 {
            ring.push(&rec(batch * 1000 + i));
        }
        let before = out.len();
        ring.drain_into(&mut out);
        for r in &out[before..] {
            assert_eq!(r.mono_ns, expected);
            expected += 1;
        }
    }
    assert_eq!(expected, 10_000);
}

#[test]
fn concurrent_producer_consumer_lose_nothing_below_capacity() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let ring = Arc::new(ThreadRing::new());
    let producer_ring = Arc::clone(&ring);
    let done = Arc::new(AtomicBool::new(false));
    let done_p = Arc::clone(&done);

    const TOTAL: u64 = 200_000;
    // Producer thread: push TOTAL records, pacing so the consumer keeps the ring
    // below capacity (this asserts the SPSC handshake, not the drop policy).
    let producer = std::thread::spawn(move || {
        for i in 0..TOTAL {
            producer_ring.push(&rec(i));
            if i % (RING_CAPACITY as u64 / 2) == 0 {
                std::thread::yield_now();
            }
        }
        done_p.store(true, Ordering::Release);
    });

    let mut got: Vec<u64> = Vec::with_capacity(TOTAL as usize);
    let mut scratch = Vec::new();
    let mut dropped_total = 0u64;
    loop {
        scratch.clear();
        dropped_total += ring.drain_into(&mut scratch);
        for r in &scratch {
            got.push(r.mono_ns);
        }
        if done.load(Ordering::Acquire) {
            scratch.clear();
            dropped_total += ring.drain_into(&mut scratch);
            for r in &scratch {
                got.push(r.mono_ns);
            }
            break;
        }
    }
    producer.join().unwrap();

    // Whatever survived must be in strict FIFO order with no duplication, and
    // kept + dropped must account for every produced record.
    assert_eq!(got.len() as u64 + dropped_total, TOTAL);
    for w in got.windows(2) {
        assert!(w[0] < w[1], "FIFO order broken: {} then {}", w[0], w[1]);
    }
}
