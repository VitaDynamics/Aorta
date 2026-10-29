//! Disabled-path overhead microbenchmark (Step 12) — the "no regression"
//! acceptance gate. The disabled trace gate must cost only a relaxed atomic load
//! plus a branch. This prints absolute ns/op (run with `--nocapture`) and asserts a
//! GENEROUS absolute ceiling so it catches gross regressions without flaking on
//! a loaded CI box. The stronger correctness proof (disabled = zero argument
//! work) is the gate-before-eval property test in trace_user_tests.
//!
//! NOTE: dev-machine numbers are indicative; the binding on-target validation is
//! the A/B affordability run on the aarch64 SoCs.

use crate::trace::{
    emit_completed_span, gate_allows, intern_event, intern_scope, register_user_metric, scope_gate,
    set_scope_enabled, set_trace_enabled, span_begin, span_end, EventType, LogLevel,
};
use std::hint::black_box;
use std::time::Instant;

fn guard<T>(f: impl FnOnce() -> T) -> T {
    crate::trace_test_support::guarded(f)
}

fn ns_per_op(iters: u64, mut f: impl FnMut()) -> f64 {
    // warm up
    for _ in 0..(iters / 10).max(1) {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed().as_nanos() as f64 / iters as f64
}

/// Best-of-`batches` per-op time (the minimum `ns_per_op` across repeated runs).
///
/// The disabled trace gate is a fixed atomic load + branch, so its TRUE cost is
/// the best case; a single mean over millions of iterations is easily inflated by
/// scheduler preemption / CPU contention on a shared CI runner (the disabled-path
/// assertions flaked at ~100+ ns/op on GitHub-hosted runners while staying near
/// zero on self-hosted + dev boxes). Taking the minimum rejects those transient
/// spikes without weakening the gate: a real regression — an accidental
/// lock/alloc/FFI on the disabled path — adds cost to EVERY iteration, so it
/// still shows up in the best-case batch.
fn min_ns_per_op(iters: u64, batches: u32, mut f: impl FnMut()) -> f64 {
    let mut best = f64::INFINITY;
    for _ in 0..batches.max(1) {
        best = best.min(ns_per_op(iters, &mut f));
    }
    best
}

#[test]
fn trace_disabled_path_is_near_zero() {
    guard(|| {
        let iters = 5_000_000u64;
        let scope = intern_scope("bench.scope");
        let gate = scope_gate(scope);

        // (1) DISABLED gate check — exactly what an instrumented call site does
        // before any work when tracing is off.
        set_trace_enabled(false);
        // best-of-N: the disabled gate cost is fixed; take the minimum so a noisy
        // shared CI runner's scheduling spikes don't flake the near-zero gate.
        let disabled = min_ns_per_op(iters, 5, || {
            black_box(gate_allows(black_box(gate), black_box(LogLevel::Info)));
        });

        // (2) ENABLED gate check (passes) — the on-but-evaluated cost.
        set_trace_enabled(true);
        set_scope_enabled(scope, true);
        let enabled_gate = ns_per_op(iters, || {
            black_box(gate_allows(black_box(gate), black_box(LogLevel::Info)));
        });

        // (3) ENABLED record on a cached histogram — the bare aggregate update
        // (one bucket bump; NO clock read, NO per-event object). This is the
        // cheapest piece, and is NOT a span.
        let hist = register_user_metric(scope, "m", EventType::Span);
        let enabled_record = ns_per_op(iters, || {
            hist.record(black_box(1234));
        });

        // (4) the monotonic clock read — the DOMINANT cost of a real timed span.
        let clock = ns_per_op(iters, || {
            black_box(crate::time::monotonic_ns());
        });

        // (5) a FULL enabled span: gate + TWO clock reads (start + end) + the
        // record on drop. THIS — not (3) — is what a real timed user span costs.
        let full_span = ns_per_op(iters, || {
            let _t = crate::aorta_trace_span!(LogLevel::Info, "bench.scope", "span");
        });

        eprintln!("=== trace overhead (release; absolute ns/op on the host it runs on) ===");
        eprintln!("  disabled gate check : {disabled:.2} ns/op");
        eprintln!("  enabled  gate check : {enabled_gate:.2} ns/op");
        eprintln!(
            "  enabled  record only: {enabled_record:.2} ns/op  (bare histogram bump, no clock)"
        );
        eprintln!("  monotonic_ns read   : {clock:.2} ns/op");
        eprintln!(
            "  enabled FULL span   : {full_span:.2} ns/op  (gate + 2x clock + record on drop)"
        );

        // Generous ceiling (real cost is a few ns): catches gross regressions
        // (e.g. an accidental lock/alloc/FFI on the disabled path) without
        // flaking on a loaded box.
        assert!(
            disabled < 100.0,
            "disabled trace gate must be near-zero, got {disabled:.2} ns/op"
        );
    });
}

/// Read current RSS in KB from `/proc/self/status` (Linux); `0` elsewhere.
fn rss_kb() -> i64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmRSS:"))
                .and_then(|v| v.split_whitespace().next())
                .and_then(|n| n.parse::<i64>().ok())
        })
        .unwrap_or(0)
}

/// Multi-thread span throughput + memory boundedness — completes the
/// validation axis (sharded vs shared scaling, memory under volume).
/// Each thread owns its ring (thread-local), so the only cross-thread contention
/// on the span path is the global `SPAN_COUNTER` (and, for "shared", one common
/// aggregate histogram). Prints aggregate M span/s and RSS deltas.
#[test]
fn trace_phase2_scaling() {
    use std::sync::{Arc, Barrier};
    guard(|| {
        crate::trace::set_trace_enabled(true);
        eprintln!("=== multi-thread span throughput (aggregate M span/s) ===");
        for &threads in &[1usize, 2, 4] {
            // SHARDED: each thread its own (scope, event) -> own ring + own metric.
            let per = 1_000_000u64;
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            for t in 0..threads {
                let b = Arc::clone(&barrier);
                handles.push(std::thread::spawn(move || {
                    let scope = intern_scope(&format!("bench.scale.shard.{t}"));
                    set_scope_enabled(scope, true);
                    let ev = intern_event("span");
                    b.wait();
                    for _ in 0..per {
                        let tk = span_begin(scope, ev, 2, 0);
                        span_end(tk);
                    }
                    // Keep the ring from blocking: drop is fine, drain not needed.
                }));
            }
            barrier.wait();
            let start = Instant::now();
            for h in handles {
                h.join().unwrap();
            }
            let secs = start.elapsed().as_secs_f64();
            let mevs = (threads as u64 * per) as f64 / 1e6 / secs;
            eprintln!("  sharded  t={threads}: {mevs:.1} M span/s");
        }
        for &threads in &[1usize, 2, 4] {
            // SHARED: all threads into ONE (scope, event) -> distinct rings still
            // (thread-local) but ONE shared aggregate histogram (contended).
            let per = 1_000_000u64;
            let scope = intern_scope("bench.scale.shared");
            set_scope_enabled(scope, true);
            let _ev = intern_event("span");
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            for _ in 0..threads {
                let b = Arc::clone(&barrier);
                handles.push(std::thread::spawn(move || {
                    let s = intern_scope("bench.scale.shared");
                    let ev = intern_event("span");
                    b.wait();
                    for _ in 0..per {
                        let tk = span_begin(s, ev, 2, 0);
                        span_end(tk);
                    }
                }));
            }
            barrier.wait();
            let start = Instant::now();
            for h in handles {
                h.join().unwrap();
            }
            let secs = start.elapsed().as_secs_f64();
            let mevs = (threads as u64 * per) as f64 / 1e6 / secs;
            eprintln!("  shared   t={threads}: {mevs:.1} M span/s");
        }

        // MEMORY: spans go to the FIXED per-thread ring (drop-newest when full),
        // so volume must not grow RSS; only DISTINCT (scope,name) metrics do.
        eprintln!("=== memory ===");
        let scope = intern_scope("bench.mem");
        set_scope_enabled(scope, true);
        let ev = intern_event("one");
        for _ in 0..200_000 {
            let tk = span_begin(scope, ev, 2, 0);
            span_end(tk);
        }
        let before = rss_kb();
        for _ in 0..5_000_000 {
            let tk = span_begin(scope, ev, 2, 0);
            span_end(tk);
        }
        eprintln!(
            "  5M spans into ONE metric : RSS delta {} KB (expect ~0; fixed ring)",
            rss_kb() - before
        );
        let b2 = rss_kb();
        for k in 0..10_000u64 {
            let e = intern_event(&format!("bench.mem.many.{k}"));
            let tk = span_begin(scope, e, 2, 0);
            span_end(tk);
        }
        eprintln!(
            "  10k DISTINCT event names : RSS delta {} KB (interner + per-metric)",
            rss_kb() - b2
        );
    });
}

/// Per-event cost: the ring (waterfall) path — span begin/end, completed
/// spans (stage path), instants — plus the drain-side chunk assembly throughput.
/// Prints absolute ns/op; the binding numbers are the on-target aarch64 runs.
#[test]
fn trace_phase2_overhead() {
    use crate::trace::chunk::{build_chunk, ChunkEventType, ChunkProvenance, RingRecord};
    guard(|| {
        let iters = 2_000_000u64;
        let scope = intern_scope("bench.p2");
        let ev = intern_event("span");
        let gate = scope_gate(scope);

        // (0) DISABLED unified span macro — must stay near the bare gate cost.
        // best-of-N (see min_ns_per_op): rejects shared-CI scheduling spikes that
        // would otherwise inflate this near-zero measurement and flake the gate.
        set_trace_enabled(false);
        let disabled_span = min_ns_per_op(iters, 5, || {
            let _t = crate::aorta_trace_span!(LogLevel::Info, "bench.p2", "span");
        });

        set_trace_enabled(true);
        set_scope_enabled(scope, true);

        // (1) raw span_begin/end pair — the ring waterfall cost WITHOUT the
        // aggregate histogram (2 clock reads + thread-local stack + 2 ring pushes).
        let begin_end = ns_per_op(iters, || {
            let t = span_begin(black_box(scope), black_box(ev), 2, 0);
            span_end(black_box(t));
        });

        // (2) emit_completed_span — the service/action STAGE path (no stack; 1
        // clock read here since the caller supplies the interval; 2 ring pushes).
        let completed = ns_per_op(iters, || {
            emit_completed_span(black_box(scope), black_box(ev), 2, 1000, 2000, 0);
        });

        // (3) unified span macro (ring + aggregate histogram) — the full user span.
        let unified = ns_per_op(iters, || {
            let _t = crate::aorta_trace_span!(LogLevel::Info, "bench.p2", "span");
        });

        // (4) instant via the dual-emit macro (histogram + ring point record).
        let instant = ns_per_op(iters, || {
            crate::aorta_trace_instant!(LogLevel::Info, "bench.p2", "blip");
        });

        // Drain the rings we just filled so the chunk-build bench starts clean.
        let mut buf: Vec<RingRecord> = Vec::new();
        crate::trace::ring::drain_all(&mut buf);

        // (5) drain-side chunk assembly throughput: build a FlatBuffers CHUNK +
        // (6) zstd-compress it, per record. Use a realistic batch of spans.
        let n = 50_000usize;
        let mut recs: Vec<RingRecord> = Vec::with_capacity(n);
        for i in 0..n as u64 {
            recs.push(RingRecord {
                trace_id: [0xCD; 16],
                span_id: i + 1,
                mono_ns: i * 1000,
                scope_id: scope,
                event_id: ev,
                event_type: ChunkEventType::SliceBegin as u8,
                ..RingRecord::default()
            });
        }
        let prov = ChunkProvenance {
            node_id: "bench".into(),
            run_id: "r".into(),
            pid: 1,
            process_name: "bench".into(),
            host_clock_domain_id: 0,
            seq: 0,
            dropped: 0,
        };
        let build_iters = 200u64;
        let mut last_bytes = 0usize;
        let build_ns = ns_per_op(build_iters, || {
            let b = build_chunk(
                &prov,
                0,
                0,
                &recs,
                |_| "bench.p2".to_string(),
                |_| "span".to_string(),
                |_| (1, "t".to_string()),
            );
            last_bytes = b.len();
            black_box(&b);
        });
        let raw = last_bytes;
        let compressed = zstd::stream::encode_all(
            &build_chunk(
                &prov,
                0,
                0,
                &recs,
                |_| "bench.p2".to_string(),
                |_| "span".to_string(),
                |_| (1, "t".to_string()),
            )[..],
            1,
        )
        .unwrap()
        .len();
        let zstd_ns = ns_per_op(build_iters, || {
            let b = build_chunk(
                &prov,
                0,
                0,
                &recs,
                |_| "bench.p2".to_string(),
                |_| "span".to_string(),
                |_| (1, "t".to_string()),
            );
            let c = zstd::stream::encode_all(&b[..], 1).unwrap();
            black_box(c.len());
        });

        eprintln!("=== trace overhead (release; absolute ns/op) ===");
        eprintln!(
            "  DISABLED unified span : {disabled_span:.2} ns/op  (gate off; no-regression gate)"
        );
        eprintln!(
            "  span_begin+end (ring) : {begin_end:.2} ns/op  (2 clock + stack + 2 ring push)"
        );
        eprintln!(
            "  emit_completed_span   : {completed:.2} ns/op  (stage path; 1 clock + 2 ring push)"
        );
        eprintln!("  unified span (r+hist) : {unified:.2} ns/op  (ring waterfall + aggregate)");
        eprintln!("  dual-emit instant     : {instant:.2} ns/op");
        eprintln!(
            "  build_chunk           : {:.1} ns/record  ({n} recs -> {raw} B raw)",
            build_ns / n as f64
        );
        eprintln!(
            "  build_chunk + zstd-1  : {:.1} ns/record  ({raw} B -> {compressed} B, {:.2}x)",
            zstd_ns / n as f64,
            raw as f64 / compressed as f64
        );

        assert!(
            disabled_span < 100.0,
            "disabled unified span must be near-zero, got {disabled_span:.2} ns/op"
        );
        let _ = gate;
    });
}

/// Scheduler-path dispatch overhead (`Aorta_Scheduler_Tracing_Proposal` — the
/// "overhead gate"). Mirrors `executor::dispatch_traced` exactly: every dispatch
/// records queue_wait + group_wait + (for a timer) the callback. Reports the
/// per-dispatch cost at the three tiers a deployment actually runs.
///
/// OFF: the master switch is off — the residual gate cost. DEFAULT (the shipped
/// posture): master on, `aorta.sched` off — what EVERY deployment always pays
/// (queue_wait + group_wait histograms only; the callback span is fully
/// sched-gated, so it is skipped at this tier). SCHED ON: `aorta.sched` enabled —
/// the active-tracing cost: one root link + a 3-span chain (= 6 ring pushes) +
/// the per-dispatch group label intern + 2x thread-CPU syscalls (wall-vs-cpu).
///
/// Also breaks out the two dominant SCHED-ON components (the thread-CPU syscall
/// and the per-dispatch group-label intern) so the number is attributable.
/// Dev-machine indicative; the binding gate is the on-target aarch64 A/B.
#[test]
fn trace_sched_dispatch_overhead() {
    use crate::trace::chunk::RingRecord;
    use crate::trace::{
        inbound_root_link, record_sched_callback, record_sched_wait, sched_scope_enabled,
        set_thread_time_enabled, thread_cpu_ns, StageId, SCHED_SCOPE_NAME,
    };

    // Time `total` iterations of `f` in BATCH-sized chunks, draining the ring
    // (UNTIMED — that is the consumer thread's job, not the dispatching thread's)
    // between chunks so the SPSC ring never fills and `push` keeps doing real
    // work. 1000 dispatches x <=6 records < the 8192-record ring => no mid-batch
    // drops, so each timed chunk measures the true steady-state push cost.
    fn ns_per_dispatch(total: u64, mut f: impl FnMut()) -> f64 {
        const BATCH: u64 = 1000;
        let mut sink: Vec<RingRecord> = Vec::new();
        for _ in 0..BATCH {
            f(); // warmup
        }
        crate::trace::ring::drain_all(&mut sink);
        sink.clear();
        let mut elapsed = std::time::Duration::ZERO;
        let mut done = 0u64;
        while done < total {
            let n = BATCH.min(total - done);
            let t = Instant::now();
            for _ in 0..n {
                f();
            }
            elapsed += t.elapsed();
            crate::trace::ring::drain_all(&mut sink);
            sink.clear();
            done += n;
        }
        elapsed.as_nanos() as f64 / done as f64
    }

    guard(|| {
        let iters = 1_000_000u64;
        let qw = intern_event("sched.queue_wait");
        let cb = intern_event("sched.callback:timer");
        // The group's group_wait id is PRE-INTERNED once at group creation in the
        // real code (executor::group_trace_events), so dispatch carries only this
        // u16 — no per-tick format!/intern.
        let gw_group = intern_event("sched.group_wait:7");

        // ONE closure mirroring dispatch_traced + run_event_timed, including the
        // SAME gating ORDER as the executor so each tier pays exactly what the
        // real path does: clocks are read only AFTER the trace_enabled gate (as
        // record_queue_wait does), the group label is interned only when the
        // scope is on, and the callback bracket runs only at the sched-on tier.
        let dispatch = || {
            // dispatch_root_link: always runs (a couple of atomic loads).
            let root = sched_scope_enabled()
                .then(|| inbound_root_link(None))
                .flatten();
            // record_queue_wait gates on trace_enabled BEFORE any clock read.
            if !crate::trace::trace_enabled() {
                return;
            }
            // queue_wait — chain root. (`_enq` mirrors queue_wait_ns_at's now.)
            let _enq = Instant::now();
            let qend = crate::time::monotonic_ns();
            let qstart = qend.saturating_sub(1500);
            record_sched_wait(StageId::SchedQueueWait, qstart, qend, qw, root);
            // group_wait — child of queue_wait (MutuallyExclusive group). The
            // real path brackets the lock with two monotonic reads (g_start /
            // g_end) and uses the group's PRE-INTERNED id (no per-tick intern).
            let g_start = crate::time::monotonic_ns();
            let g_end = crate::time::monotonic_ns();
            let gw_link = root.map(|r| r.child());
            let gw_ev = if gw_link.is_some() { gw_group } else { 0 };
            record_sched_wait(StageId::SchedGroupWait, g_start, g_end, gw_ev, gw_link);
            // SchedCallback — child of group_wait, timer-only: run_event_timed
            // brackets event.run() with two monotonic reads + two thread-CPU
            // syscalls. None at the default tier (link is None), so it is skipped
            // there exactly as run_event_timed early-returns event.run().
            if let Some(cb_link) = gw_link.map(|r| r.child()) {
                let c0 = thread_cpu_ns();
                let cb_start = crate::time::monotonic_ns();
                let cb_end = crate::time::monotonic_ns();
                let delta = thread_cpu_ns().saturating_sub(c0);
                record_sched_callback(cb_start, cb_end, delta, cb, cb_link);
            }
        };

        set_thread_time_enabled(true);

        // (1) OFF — master switch off.
        set_trace_enabled(false);
        let off = ns_per_dispatch(iters, dispatch);

        // (2) DEFAULT — master on, aorta.sched off (the shipped default posture).
        set_trace_enabled(true);
        let sched_scope = intern_scope(SCHED_SCOPE_NAME);
        set_scope_enabled(sched_scope, false);
        let default_tier = ns_per_dispatch(iters, dispatch);

        // (3) SCHED ON — aorta.sched enabled.
        set_scope_enabled(sched_scope, true);
        let sched_on = ns_per_dispatch(iters, dispatch);

        // Component breakdown for the SCHED-ON tier.
        let one_syscall = ns_per_op(iters, || {
            black_box(thread_cpu_ns());
        });
        let label_intern = ns_per_op(iters, || {
            black_box(intern_event(black_box(&format!(
                "sched.group_wait:{}",
                7u64
            ))));
        });

        set_trace_enabled(false);

        eprintln!("=== scheduler dispatch overhead (release; ns per DISPATCH) ===");
        eprintln!("  OFF (master off)       : {off:.1} ns/dispatch  (residual gate)");
        eprintln!(
            "  DEFAULT (sched off)    : {default_tier:.1} ns/dispatch  \
             (queue+group histograms — ALWAYS paid)"
        );
        eprintln!(
            "  SCHED ON (aorta.sched) : {sched_on:.1} ns/dispatch  \
             (3-span chain/6 ring push + 2x thread-CPU; labels pre-interned)"
        );
        eprintln!(
            "    of which 2x thread-CPU: {:.1} ns/dispatch  (syscall bracket)",
            2.0 * one_syscall
        );
        eprintln!(
            "    label intern ELIMINATED: {label_intern:.1} ns/dispatch would be the \
             per-tick format!+intern — now pre-interned ONCE at group/timer creation"
        );
        eprintln!(
            "  ADDED by default tier  : {:.1} ns/dispatch",
            default_tier - off
        );
        eprintln!(
            "  ADDED by sched on      : {:.1} ns/dispatch",
            sched_on - off
        );
        for hz in [1_000.0f64, 10_000.0] {
            eprintln!(
                "  @ {hz:>6.0} dispatch/s    : default {:.4}% of a core, sched-on {:.3}% of a core",
                (default_tier - off) * hz / 1e9 * 100.0,
                (sched_on - off) * hz / 1e9 * 100.0
            );
        }

        // No-regression gate: OFF must be near-free and the ALWAYS-paid default
        // tier must not explode (a lock/alloc/syscall accidentally on it). The
        // binding affordability gate is the on-target aarch64 A/B.
        assert!(
            off < 200.0,
            "scheduler dispatch with the master switch off must be near-zero, got {off:.1} ns"
        );
        assert!(
            default_tier < 1500.0,
            "default-tier scheduler overhead must stay modest, got {default_tier:.1} ns/dispatch"
        );
    });
}
