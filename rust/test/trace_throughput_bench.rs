//! Comprehensive on-target throughput/overhead bench for the Rust SDK trace path.
//!
//! Complements `core/test/trace_overhead_bench.rs` (the disabled near-zero gate).
//! Covers: disabled-path steady-state cost, enabled single-thread per-event cost
//! (cached `aorta_trace_record!` and the RAII `aorta_trace_span!`), multi-thread
//! scaling (same metric vs sharded) at 1/2/4 threads, and memory boundedness.
//! Prints `RESULT <category> <variant> t=<n> <ns/ev> <Mev/s>` lines.
//!
//! `#[ignore]`d (a bench, not a CI gate). Run explicitly:
//!   cargo test -p aorta_sdk --release trace_throughput -- --nocapture --ignored --test-threads=1

use crate::trace::{
    intern_scope, register_user_metric, set_scope_enabled, set_trace_enabled, EventType, LogLevel,
};
use crate::{aorta_trace_record, aorta_trace_span};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const N1: u64 = 50_000_000; // single-thread
const NN: u64 = 20_000_000; // per-thread (multi)
const THREADS: [u64; 3] = [1, 2, 4];

fn enable() {
    set_trace_enabled(true);
    set_scope_enabled(intern_scope("bench.tput"), true);
}

fn emit(cat: &str, variant: &str, threads: u64, total: u64, secs: f64) {
    eprintln!(
        "RESULT {:<9} {:<14} t={}  {:8.2} ns/ev  {:8.1} M ev/s",
        cat,
        variant,
        threads,
        secs * 1e9 / total as f64,
        total as f64 / 1e6 / secs
    );
}

fn rss_kb() -> i64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

#[test]
#[ignore]
fn rust_sdk_disabled_overhead() {
    // Master OFF: steady-state cost of an instrumented call-site that is compiled
    // in but disabled (the #1 no-regression gate, from the SDK macro).
    set_trace_enabled(false);
    for _ in 0..200_000 {
        aorta_trace_record!(LogLevel::Info, "bench.tput", "off", 1);
    }
    let t = Instant::now();
    for _ in 0..N1 {
        aorta_trace_record!(LogLevel::Info, "bench.tput", "off", 1);
    }
    emit("disabled", "record!", 1, N1, t.elapsed().as_secs_f64());
}

#[test]
#[ignore]
fn rust_sdk_single_thread() {
    enable();
    for i in 0..200_000u64 {
        aorta_trace_record!(LogLevel::Info, "bench.tput", "single", i & 0x3FF);
    }
    let t = Instant::now();
    for i in 0..N1 {
        aorta_trace_record!(LogLevel::Info, "bench.tput", "single", i & 0x3FF);
    }
    emit("enabled", "record!", 1, N1, t.elapsed().as_secs_f64());

    // RAII span: gate + 2 clock reads + record on drop.
    let t = Instant::now();
    for _ in 0..N1 {
        let _s = aorta_trace_span!(LogLevel::Info, "bench.tput", "span");
    }
    emit("enabled", "span!", 1, N1, t.elapsed().as_secs_f64());
    set_trace_enabled(false);
}

#[test]
#[ignore]
fn rust_sdk_multi_same_metric() {
    enable();
    for &threads in THREADS.iter() {
        let barrier = Arc::new(Barrier::new(threads as usize + 1));
        let mut handles = Vec::new();
        for _ in 0..threads {
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                for i in 0..50_000u64 {
                    aorta_trace_record!(LogLevel::Info, "bench.tput", "mt_shared", i & 0x3FF);
                }
                barrier.wait();
                for i in 0..NN {
                    aorta_trace_record!(LogLevel::Info, "bench.tput", "mt_shared", i & 0x3FF);
                }
            }));
        }
        barrier.wait();
        let t = Instant::now();
        for h in handles {
            h.join().unwrap();
        }
        emit(
            "mt-same",
            "record!",
            threads,
            threads * NN,
            t.elapsed().as_secs_f64(),
        );
    }
    set_trace_enabled(false);
}

#[test]
#[ignore]
fn rust_sdk_multi_sharded() {
    enable();
    let scope = intern_scope("bench.tput");
    for &threads in THREADS.iter() {
        let barrier = Arc::new(Barrier::new(threads as usize + 1));
        let mut handles = Vec::new();
        for tid in 0..threads {
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                // Each thread records into its OWN metric -> distinct histogram /
                // cache line: no shared-atomic contention.
                let hist = register_user_metric(scope, &format!("mt_shard_{tid}"), EventType::Span);
                for i in 0..50_000u64 {
                    hist.record(i & 0x3FF);
                }
                barrier.wait();
                for i in 0..NN {
                    hist.record(i & 0x3FF);
                }
            }));
        }
        barrier.wait();
        let t = Instant::now();
        for h in handles {
            h.join().unwrap();
        }
        emit(
            "mt-shard",
            "by-handle",
            threads,
            threads * NN,
            t.elapsed().as_secs_f64(),
        );
    }
    set_trace_enabled(false);
}

#[test]
#[ignore]
fn rust_sdk_memory_bounded() {
    enable();
    let scope = intern_scope("bench.tput");
    let hist = register_user_metric(scope, "mem_one", EventType::Span);
    let before = rss_kb();
    for i in 0..100_000_000u64 {
        hist.record(i & 0x3FF);
    }
    eprintln!(
        "RESULT memory    one-metric-100M    rss_delta={} KB (expect ~0)",
        rss_kb() - before
    );

    let b2 = rss_kb();
    for k in 0..10_000u64 {
        register_user_metric(scope, &format!("mem_many_{k}"), EventType::Span).record(1);
    }
    eprintln!(
        "RESULT memory    10k-distinct       rss_delta={} KB (~520B*10k + map)",
        rss_kb() - b2
    );
    set_trace_enabled(false);
}
