//! Flow TCK emitter — Rust plane (`test/flow_tck/SCHEMA.md`).
//!
//! Writes ONLY newline-delimited JSON event records to stdout, one per
//! observed event, in observation order (logs go to stderr) — the TCK
//! driver (`test/flow_tck/driver.py`) parses this stream, normalizes it
//! per SCHEMA.md, and diffs it against
//! `test/flow_tck/expected/<scenario>*.json`.
//!
//! Scenario name is argv[1]. Setup mirrors `rust/examples/flow_demo.rs`
//! (tokio multi-thread runtime, `aorta_sdk` re-exports of
//! `aorta_core::flow`) but skips the `Node`/Zenoh session entirely —
//! every scenario here is pure `Scope`/`TaskHandle` plumbing, no pub/sub
//! or RPC surface needed.
//!
//! Run: `cargo run -p aorta_sdk --features examples --example flow_tck_emitter -- <scenario>`
//!
//! Emission is centralized through [`emit_line`]: SCHEMA.md's "Emission
//! rules (v1)" require ONE mutex around "assign seq + write line" — a
//! plain global counter races once task bodies emit from runtime
//! threads (several wave-1 scenarios below emit from a spawned child's
//! own task, concurrently with the main task).

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aorta_sdk::{when_all, when_any, CancelReason, Scope, ScopeOpts, StopReason, Supervision};

/// Wave-3 direct `aorta_core::*` imports (the layer rule lets `rust/`
/// depend directly on the high-level `aorta_core::*` API, excluding
/// `aorta_core::ffi::*`). `Event`/`EventKind` ARE crate-root exports of
/// `aorta_sdk` since the surface-diet closure, but `Session` and the raw
/// core `Client`/`Service` these wave-3 scenarios need to DRIVE the
/// native admission/deadline machinery are not — and this emitter sits
/// on the sanctioned in-tree core path anyway; `aorta_core` is already a
/// direct `rust/Cargo.toml` dependency (`rust/examples/
/// unified_publish_demo.rs` is the existing precedent for this exact
/// import shape).
use aorta_core::client::Client as CoreClient;
use aorta_core::event_queue::{Event, EventKind};
use aorta_core::executor::SingleThreadedExecutor;
use aorta_core::service::Service as CoreService;
use aorta_core::session::Session;

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// The single seq-assignment-plus-print mutex (SCHEMA.md "Emission
/// rules"). One process runs exactly one scenario, so a process-global
/// counter (rather than a per-scenario-fn-scoped one) is correct.
static SEQ: Mutex<u64> = Mutex::new(0);

/// Assign the next `seq` and print the line as ONE atomic step — the
/// closure builds the JSON text using the seq value handed to it, and
/// the mutex is held across both the build and the `println!` so two
/// concurrent emitters (main task + a spawned child's task) can never
/// interleave a seq assignment with someone else's write.
fn emit_line(build: impl FnOnce(u64) -> String) {
    let mut seq = SEQ.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let line = build(*seq);
    println!("{line}");
    *seq += 1;
}

fn emit_lifecycle(scenario: &str, op: &str, phase: &str) {
    emit_line(|seq| {
        format!(
            "{{\"scenario\":\"{}\",\"seq\":{},\"op\":\"{}\",\"event\":\"lifecycle\",\"phase\":\"{}\"}}",
            json_escape(scenario),
            seq,
            json_escape(op),
            json_escape(phase)
        )
    });
}

fn emit_stopped_cancelled(scenario: &str, op: &str, reason: &str, raw_reason: u8) {
    emit_line(|seq| {
        format!(
            "{{\"scenario\":\"{}\",\"seq\":{},\"op\":\"{}\",\"event\":\"stopped\",\"kind\":\"cancelled\",\"reason\":\"{}\",\"raw_reason\":{}}}",
            json_escape(scenario),
            seq,
            json_escape(op),
            reason,
            raw_reason
        )
    });
}

fn emit_stopped_fault(scenario: &str, op: &str, code: i32, message: &str) {
    emit_line(|seq| {
        format!(
            "{{\"scenario\":\"{}\",\"seq\":{},\"op\":\"{}\",\"event\":\"stopped\",\"kind\":\"fault\",\"code\":{},\"message\":\"{}\"}}",
            json_escape(scenario),
            seq,
            json_escape(op),
            code,
            json_escape(message)
        )
    });
}

fn emit_value(scenario: &str, op: &str, value: &str) {
    emit_line(|seq| {
        format!(
            "{{\"scenario\":\"{}\",\"seq\":{},\"op\":\"{}\",\"event\":\"value\",\"value\":\"{}\"}}",
            json_escape(scenario),
            seq,
            json_escape(op),
            json_escape(value)
        )
    });
}

fn emit_counter(scenario: &str, op: &str, name: &str, value: i64) {
    emit_line(|seq| {
        format!(
            "{{\"scenario\":\"{}\",\"seq\":{},\"op\":\"{}\",\"event\":\"counter\",\"name\":\"{}\",\"value\":{}}}",
            json_escape(scenario),
            seq,
            json_escape(op),
            json_escape(name),
            value
        )
    });
}

/// Cheap dependency-free jitter (no `rand` dependency — this binary owns
/// only itself, see `close_spawn_race`'s doc comment): a tiny RANDOM (not
/// hardcoded) bounded spin so two racing execution contexts' relative
/// arrival time at their own atomic op varies across iterations instead
/// of one side systematically winning every time.
fn jitter_spin(seed: u64, extra: u64) {
    let mut x = seed ^ 0x2545_F491_4F6C_DD1D;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    let spins = (x % 4000) + extra;
    for _ in 0..spins {
        std::hint::spin_loop();
    }
}

/// Seed source for [`jitter_spin`]: sub-second wall-clock nanos (real
/// hardware timing jitter, not a fixed value) mixed with the iteration
/// index and a per-call-site salt so the two racing sides never draw the
/// same spin count from the same instant.
fn race_jitter_seed(iteration: u64, salt: u64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos ^ iteration.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ salt.wrapping_mul(0xBF58_476D_1CE4_E5B9)
}

fn unexpected_ok(scenario: &str, ctx: &str) -> ! {
    eprintln!("flow_tck_emitter[{scenario}]: {ctx}: unexpected Ok outcome");
    std::process::exit(2);
}

fn unexpected_stop(scenario: &str, ctx: &str, other: &StopReason) -> ! {
    eprintln!("flow_tck_emitter[{scenario}]: {ctx}: unexpected stop reason: {other:?}");
    std::process::exit(2);
}

fn format_i32_vec(values: &[i32]) -> String {
    let mut s = String::from("[");
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&v.to_string());
    }
    s.push(']');
    s
}

/// `CancelReason` is `#[non_exhaustive]` (sync/src/cancel.rs) — the
/// wildcard arm is required, and also happens to be the SCHEMA.md
/// `"unknown"` fallback for any future reason value this emitter doesn't
/// know about yet.
fn cancel_reason_name(reason: CancelReason) -> &'static str {
    match reason {
        CancelReason::Explicit => "explicit",
        CancelReason::DeadlineExceeded => "deadline",
        CancelReason::Shutdown => "shutdown",
        CancelReason::ChildFailed => "child_failed",
        _ => "unknown",
    }
}

/// `cancel_reason_observation` (SCHEMA.md "Phase 0 baseline-divergence
/// scenario"): a parent scope cancels a parked child with `Shutdown`;
/// the child's `TaskHandle` join observes the stop and this emits the
/// stopped record.
///
/// The child parks on the scope's own cancellation — the idiom used
/// throughout `core/test/flow_scope_tests.rs`
/// (`scope.spawn_with(|scope| async move { scope.cancelled().await; })`).
/// In practice the DispatchBoundary that wraps every `Scope::spawn`/
/// `spawn_with` races the child future against the scope's own cancel
/// token (`core/src/flow/scope.rs`), so the join resolves the instant
/// the parent cancels regardless of whether the child's own await or the
/// boundary's own race observes the cancel first — Rust preserves the
/// full `CancelReason` end to end either way. This is the full-fidelity
/// plane: the C++ plane's `Outcome<T>` cannot carry a reason yet (see
/// `cpp/test/flow_tck_emitter.cc`).
async fn cancel_reason_observation(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-cancel-reason-root"));

    let handle = root.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    root.cancel(CancelReason::Shutdown);
    emit_lifecycle(scenario, "scope.cancel", "parent_cancelled");

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// Same shape as `cancel_reason_observation`, but the parent cancels
/// with `Explicit` instead of `Shutdown` — the other named reason a
/// user-facing `cancel()` call commonly carries.
async fn cancel_reason_explicit(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-cancel-explicit-root"));

    let handle = root.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    root.cancel(CancelReason::Explicit);
    emit_lifecycle(scenario, "scope.cancel", "parent_cancelled");

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// A plain successful child: the value crosses the join unmodified.
async fn value_delivery(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-value-delivery-root"));

    let handle = root.spawn(async { 7i32 });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    match handle.join().await {
        Ok(v) => emit_value(scenario, "task.await", &v.to_string()),
        Err(stopped) => unexpected_stop(scenario, "task.await", stopped.reason()),
    }

    root.close().await;
}

/// A move-only (no `Clone`) value delivered through the join — proves
/// the outcome channel doesn't require `Clone`/`Copy`.
struct MotorFrame(String);

async fn move_only_value(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-move-only-root"));

    let handle = root.spawn(async { MotorFrame("motor-frame".to_string()) });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    match handle.join().await {
        Ok(frame) => emit_value(scenario, "task.await", &format!("moved:{}", frame.0)),
        Err(stopped) => unexpected_stop(scenario, "task.await", stopped.reason()),
    }

    root.close().await;
}

/// Spawning on an ALREADY-expired budget fires the token at entry
/// (`Scope::spawn` doc: "Spawning on an expired budget fires the
/// token first; the child is then born into a cancelled scope"). The
/// child never runs at all — `spawn()`'s own `checkpoint()` call cancels
/// the token before the `DispatchBoundary` is even built, and that
/// boundary's own hard-expiry check resolves the join on its very first
/// poll, deterministically (no child body code ever executes).
async fn budget_expiry_entry(scenario: &str) {
    let root =
        Scope::new_root(ScopeOpts::named("tck-budget-entry-root").budget(Duration::from_millis(1)));
    tokio::time::sleep(Duration::from_millis(20)).await;

    let handle = root.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_on_expired");

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// A child parked on `cancelled()` with nothing to observe itself — the
/// scope's OWN budget expires naturally via the `DispatchBoundary`'s
/// dedicated expiry timer (armed at spawn time) and resolves the join
/// with `Stopped(DeadlineExceeded)`. No child-body code needs to run
/// (same non-racy shape as `cancel_reason_observation`, using budget
/// expiry instead of an explicit `cancel()` call as the trigger).
async fn budget_expiry_parked(scenario: &str) {
    let root = Scope::new_root(
        ScopeOpts::named("tck-budget-parked-root").budget(Duration::from_millis(80)),
    );

    let handle = root.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// The child observes budget expiry ITSELF via a `checkpoint()` polling
/// loop, emits a lifecycle marker, and only THEN does the join resolve.
///
/// IMPLEMENTATION NOTE (see the FINDINGS section of the delivering
/// report): a single shared budget cannot make this ordering
/// deterministic — `Scope::spawn`'s `DispatchBoundary` (core/src/flow/
/// scope.rs) checks its OWN hard budget-expiry bound BEFORE ever polling
/// the child future on every single poll, and that bound is armed from
/// the exact same deadline the child's own `checkpoint()` would observe,
/// so the boundary always resolves the join first once both timers are
/// due — the child's own detection code never gets to run. This uses a
/// deliberately NARROWER child scope (`task_scope`, budget 30ms) purely
/// for the checkpoint loop, spawned on `root` (budget 150ms, the outer
/// safety net that actually backs the `DispatchBoundary`) — the child
/// reliably observes ITS OWN earlier deadline and emits well before
/// root's boundary ever fires, then parks so the join still resolves via
/// the boundary's normal mechanism (both report `DeadlineExceeded`, same
/// SCHEMA fields either way).
async fn budget_expiry_checkpoint(scenario: &str) {
    // Two budgets, two roles: the CHILD's 30ms budget is what the body's
    // checkpoint() observes; the ROOT's budget is only the eventual
    // hard-stop for the parked body afterwards (the stopped record's
    // DeadlineExceeded). The root budget must comfortably out-wait task
    // FIRST-SCHEDULING latency, or the boundary abort wins before the
    // body ever runs its loop (GH-hosted 2-vCPU runner, 2026-07-15: a
    // 150ms root budget lost that race — the checkpoint record went
    // missing and the driver flagged divergence). 1.5s keeps the same
    // expected sequence with a >1s scheduling margin.
    let root = Scope::new_root(
        ScopeOpts::named("tck-budget-checkpoint-root").budget(Duration::from_millis(1500)),
    );
    let task_scope = root
        .child(ScopeOpts::named("tck-budget-checkpoint-child").budget(Duration::from_millis(30)));

    let observed = Arc::new(AtomicBool::new(false));
    let observed_child = Arc::clone(&observed);
    let scenario_owned = scenario.to_string();

    let handle = root.spawn(async move {
        loop {
            if task_scope.checkpoint().is_err() {
                emit_lifecycle(
                    &scenario_owned,
                    "budget.checkpoint",
                    "checkpoint_observed_deadline",
                );
                observed_child.store(true, Ordering::Release);
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    // SCHEMA.md "Emission rules": the CHILD's lifecycle record must
    // precede the parent's stopped record in observation order — since
    // that emit runs on a different task, synchronize on the flag rather
    // than relying on `join()`'s timing alone.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !observed.load(Ordering::Acquire) && tokio::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    if !observed.load(Ordering::Acquire) {
        eprintln!(
            "flow_tck_emitter[{scenario}]: budget_expiry_checkpoint: child never observed its own checkpoint deadline within 5s"
        );
    }

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// The child panics with an EXACT, byte-for-byte message; the join's
/// `Fault` carries it verbatim (`crate::runtime::panic_payload_string`
/// downcasts a `&'static str`/`String` panic payload with no added
/// decoration — see core/src/runtime.rs).
async fn fault_message_exact(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-fault-exact-root"));

    let handle = root.spawn(async {
        panic!("tck: exact fault text");
    });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Fault { code, message } => {
                emit_stopped_fault(scenario, "task.await", *code, message);
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// The child observes the parent's cancel and emits BEFORE the parent's
/// join sees the stopped outcome.
///
/// IMPLEMENTATION NOTE (see FINDINGS): `DispatchBoundary` checks its own
/// cancellation bound BEFORE ever polling the child future on EVERY
/// poll (core/src/flow/scope.rs) — a child parked on
/// `scope.cancelled()`, or busy-polling `is_cancelled()` across
/// `tokio::task::yield_now()` points, can never reliably run code after
/// observing the cancellation: by the time any SUBSEQUENT poll happens
/// (wake-triggered, from `root.cancel()`'s own wake of the boundary's
/// waiter), the boundary's own check already sees the state set and
/// short-circuits without ever reaching the child future again
/// (confirmed empirically: 0/30 runs with a yield-based busy loop ever
/// emitted the child's own record — see the delivering report).
///
/// This uses a genuine (non-yielding) busy-spin instead: a `Future`
/// `poll()` call runs to completion synchronously on one thread — the
/// executor cannot preempt an in-flight `this.fut.poll()` call, so once
/// the child's FIRST poll actually starts spinning, it observes
/// `root.cancel()`'s write DIRECTLY (a plain concurrent atomic store,
/// on the OTHER worker thread) entirely within that SAME poll call,
/// strictly before the boundary's own (separate, LATER) check ever runs
/// again. The `started` handshake ensures the spin has actually begun
/// (i.e. the child's first poll has already passed the boundary's
/// ENTRY check) before the parent calls `cancel()` — otherwise the
/// token could already be cancelled before the child's first poll ever
/// happens, and the entry check alone would win instead.
async fn parent_child_cancel_order(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-parent-child-cancel-root"));

    let started = Arc::new(AtomicBool::new(false));
    let started_child = Arc::clone(&started);
    let observed = Arc::new(AtomicBool::new(false));
    let observed_child = Arc::clone(&observed);
    let scenario_owned = scenario.to_string();

    let handle = root.spawn_with(move |scope| async move {
        started_child.store(true, Ordering::Release);
        // Bounded busy-spin (no `.await` inside — a genuinely
        // uninterruptible poll — capped so a logic error elsewhere
        // can't hang the process forever).
        let mut spins: u64 = 0;
        loop {
            if scope.is_cancelled() {
                emit_lifecycle(&scenario_owned, "task.body", "child_observed_cancel");
                observed_child.store(true, Ordering::Release);
                break;
            }
            spins += 1;
            if spins > 200_000_000 {
                eprintln!(
                    "flow_tck_emitter[{scenario_owned}]: parent_child_cancel_order: child spin gave up without observing cancel"
                );
                break;
            }
            std::hint::spin_loop();
        }
        std::future::pending::<()>().await;
    });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    // Wait for the child's spin to actually START before cancelling —
    // otherwise `cancel()` could land before the child's first poll,
    // and the boundary's own ENTRY check would win instead (see above).
    while !started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    emit_lifecycle(scenario, "scope.cancel", "parent_cancelling");
    root.cancel(CancelReason::Shutdown);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !observed.load(Ordering::Acquire) && tokio::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    if !observed.load(Ordering::Acquire) {
        eprintln!(
            "flow_tck_emitter[{scenario}]: parent_child_cancel_order: child never observed its own cancellation within 5s"
        );
    }

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// Default `FailFast` supervision: sibling A panics, which cancels the
/// whole scope with `ChildFailed`; sibling B (parked, nothing to emit
/// itself — the join's own outcome is correct regardless of which path,
/// boundary or child await, observes the cancel first, same reasoning as
/// `cancel_reason_observation`) observes that same cancellation.
async fn sibling_fail_fast(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-sibling-root"));

    let handle_a = root.spawn(async {
        panic!("tck-sibling-fault");
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_a");

    let handle_b = root.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_b");

    match handle_a.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Fault { code, message } => {
                emit_stopped_fault(scenario, "task.await", *code, message);
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    match handle_b.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    let raw = root.cancel_reason().map(|r| r.as_u8() as i64).unwrap_or(-1);
    emit_counter(scenario, "scope.stats", "scope_cancel_reason_raw", raw);

    root.close().await;
}

/// `Supervision::Isolate`: sibling A's panic resolves ONLY its own
/// handle; sibling B keeps running and completes normally; the scope
/// itself is never cancelled.
async fn isolated_child_failure(scenario: &str) {
    let root =
        Scope::new_root(ScopeOpts::named("tck-isolated-root").supervision(Supervision::Isolate));

    let handle_a = root.spawn(async {
        panic!("tck-isolated-fault");
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_a");

    let handle_b = root.spawn(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        42i32
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_b");

    match handle_a.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Fault { code, message } => {
                emit_stopped_fault(scenario, "task.await", *code, message);
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    match handle_b.join().await {
        Ok(v) => emit_value(scenario, "task.await", &v.to_string()),
        Err(stopped) => unexpected_stop(scenario, "task.await", stopped.reason()),
    }

    emit_counter(
        scenario,
        "scope.stats",
        "scope_cancelled",
        root.is_cancelled() as i64,
    );

    root.close().await;
}

/// `when_all` over an empty vec: vacuously `Ok(vec![])`.
async fn when_all_empty(scenario: &str) {
    let empty: Vec<aorta_sdk::TaskHandle<i32>> = Vec::new();
    match when_all(empty).await {
        Ok(values) => emit_value(scenario, "when_all", &format_i32_vec(&values)),
        Err(stopped) => unexpected_stop(scenario, "when_all", stopped.reason()),
    }
}

/// `when_all` over three children whose COMPLETION order differs from
/// their INPUT order (staggered sleeps) — the result vec must still
/// come back in input order.
async fn when_all_multi(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-when-all-multi-root"));

    let h1 = root.spawn(async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        1i32
    });
    let h2 = root.spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        2i32
    });
    let h3 = root.spawn(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        3i32
    });

    match when_all(vec![h1, h2, h3]).await {
        Ok(values) => emit_value(scenario, "when_all", &format_i32_vec(&values)),
        Err(stopped) => unexpected_stop(scenario, "when_all", stopped.reason()),
    }

    root.close().await;
}

/// `when_all` fail-fast: one child parks (aborted), the other panics —
/// the combinator resolves the fault.
async fn when_all_fault(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-when-all-fault-root"));

    let h_park = root.spawn_with(|scope| async move {
        scope.cancelled().await;
    });
    let h_fault = root.spawn(async {
        panic!("tck-when-fault");
    });

    match when_all(vec![h_park, h_fault]).await {
        Err(stopped) => match stopped.reason() {
            StopReason::Fault { code, message } => {
                emit_stopped_fault(scenario, "when_all", *code, message);
            }
            other => unexpected_stop(scenario, "when_all", other),
        },
        Ok(_) => unexpected_ok(scenario, "when_all"),
    }

    root.close().await;
}

/// `when_any` first-completion-wins: the first child resolves
/// immediately; the losers are aborted.
async fn when_any_first_wins(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-when-any-first-root"));

    let h0 = root.spawn(async { 9i32 });
    let h1 = root.spawn_with(|scope| async move {
        scope.cancelled().await;
        0i32
    });
    let h2 = root.spawn_with(|scope| async move {
        scope.cancelled().await;
        0i32
    });

    match when_any(vec![h0, h1, h2]).await {
        // "index:value" — the WINNER IDENTITY is part of the contract
        // (a value-only comparison would stay green if
        // a wrong same-valued branch won).
        Ok((idx, value)) => emit_value(scenario, "when_any", &format!("{idx}:{value}")),
        Err(stopped) => unexpected_stop(scenario, "when_any", stopped.reason()),
    }

    root.close().await;
}

/// `when_any` over an empty vec: no winner and no failure to report —
/// `Err(Stopped::cancelled(Explicit))` (core/src/flow/combinators.rs).
async fn when_any_empty(scenario: &str) {
    let empty: Vec<aorta_sdk::TaskHandle<i32>> = Vec::new();
    match when_any(empty).await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "when_any",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "when_any", other),
        },
        Ok(_) => unexpected_ok(scenario, "when_any"),
    }
}

/// `close()` on an empty scope, then a `spawn()` racing PAST that
/// close: `Scope::spawn`'s own closing-bar check (core/src/flow/
/// scope.rs) deterministically refuses it with a synthesized
/// `Stopped::cancelled(Shutdown)` — no timing race, the bar is already
/// set by the time `spawn()` runs.
async fn close_vs_racing_spawn(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-close-race-root"));

    root.close().await;
    emit_lifecycle(scenario, "scope.close", "closed");

    let handle = root.spawn(async {});
    emit_lifecycle(scenario, "task.spawn", "spawn_refused");

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "task.await",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    root.close().await;
}

/// `close_spawn_race` (`test/flow_tck/expected/close_spawn_race.json`,
/// scenarios.json manifest note): the GENUINELY CONCURRENT half of the
/// "close versus racing spawn" family — `close_vs_racing_spawn`
/// above is the deterministic SEQUENTIAL half (`close()` always returns
/// strictly BEFORE the racing `spawn()` is even attempted, per its own
/// doc comment: "no timing race, the bar is already set"); this scenario
/// instead races `Scope::close()`'s closing-bar WRITE
/// (`ScopeInner::mark_closing_subtree`, `close()`'s very first line)
/// against a CONCURRENT `Scope::spawn_with()` call's closing-bar READ
/// (`Scope::spawn`'s two `self.inner.closing.load()` checks) from a
/// SEPARATE tokio task, on a FRESH child scope each of 50 iterations.
///
/// Start-line synchronization: the racer task busy-spins (no `.await` —
/// an uninterruptible poll, the same technique `parent_child_cancel_order`
/// above uses and documents) on a per-iteration `go` flag once it has
/// signalled `started`; this (main) task waits cooperatively
/// (`yield_now`) for `started`, flips `go`, then each side independently
/// spends a RANDOM [`jitter_spin`] before performing its half of the race
/// (`close()` here; `spawn_with()` on the racer) — without that jitter
/// one side has a small but SYSTEMATIC latency edge (a freshly-scheduled
/// tokio task reacting to an atomic vs. code already mid-poll on the
/// current task) and the race stops actually racing (see the delivering
/// report's SANITY section for the observed per-run admitted/refused
/// split this produces).
///
/// Per-iteration classification does NOT trust `join()`'s timing alone —
/// unlike the C++ plane, `Scope::spawn_with` has no synchronous "was this
/// admitted" signal (`spawn()` always returns a `TaskHandle`; refusal is
/// baked into a pre-resolved `Outcome`), so the trivial child sets `ran`
/// as the FIRST statement in its body: an OBSERVED signal, independent of
/// whether `join()` itself resolves within the 5s bound, of whether the
/// scope admitted the spawn at all (a refused handle's body never runs,
/// so `ran` stays false forever).
async fn close_spawn_race(scenario: &str) {
    // Adaptive handicap escalation (mirrors the C++ leg; 3rd recurrence
    // of the GH-hosted degenerate run, 2026-07-14: even the fixed
    // 6000-spin parity handicap lost all 50 races on the small throttled
    // runner). A fixed constant cannot cover every runner speed class:
    // close() on an as-yet-empty child scope is intrinsically CHEAPER
    // than spawn_with() (task allocation + scope admission gate), and
    // HOW MUCH cheaper varies with host parallelism and contention. So a
    // fully one-sided 50-iteration round escalates the handicap 4x and
    // re-races — the parity alternation hands the growing handicap to the
    // persistent winner on half the iterations, so SOME magnitude flips
    // it on every host. Emitted records come from the final
    // (non-degenerate) round ONLY; a still-degenerate last escalation
    // fails closed exactly like before.
    const MAX_ROUNDS: u32 = 5;
    let mut handicap: u64 = 6000;
    let mut round = 0u32;
    let r = loop {
        let r = close_spawn_race_round(scenario, handicap).await;
        if r.observed_admitted > 0 && r.observed_refused > 0 {
            break r;
        }
        round += 1;
        if round >= MAX_ROUNDS {
            eprintln!(
                "flow_tck_emitter[{scenario}]: close_spawn_race: degenerate run — admitted={}, refused={} after {MAX_ROUNDS} handicap escalations to {handicap} (both must be > 0 to prove a genuine race occurred)",
                r.observed_admitted, r.observed_refused
            );
            std::process::exit(2);
        }
        eprintln!(
            "flow_tck_emitter[{scenario}]: close_spawn_race: round {round} one-sided (admitted={}, refused={}) — escalating handicap {handicap} -> {} for this runner's speed class",
            r.observed_admitted, r.observed_refused,
            handicap * 4
        );
        handicap *= 4;
    };

    emit_counter(scenario, "scope.close", "iterations", ITERATIONS as i64);
    emit_counter(
        scenario,
        "scope.close",
        "admitted_lost",
        r.admitted_lost as i64,
    );
    emit_counter(
        scenario,
        "scope.close",
        "refused_unresolved",
        r.refused_unresolved as i64,
    );
    emit_counter(
        scenario,
        "scope.close",
        "close_incomplete",
        r.close_incomplete as i64,
    );

    // DRIVER-DROPPED (SCHEMA.md "Normalization": op prefix `debug.`) —
    // humans-only visibility into the timing-dependent admitted/refused
    // split the final round actually produced.
    emit_counter(
        scenario,
        "debug.close_spawn_race",
        "admitted",
        r.observed_admitted as i64,
    );
    emit_counter(
        scenario,
        "debug.close_spawn_race",
        "refused",
        r.observed_refused as i64,
    );
}

const ITERATIONS: u64 = 50;

struct RaceRound {
    admitted_lost: u64,
    refused_unresolved: u64,
    close_incomplete: u64,
    observed_admitted: u64,
    observed_refused: u64,
}

/// One 50-iteration racing round at a given handicap magnitude; the
/// caller (`close_spawn_race`) owns escalation and the fail-closed
/// postcondition. Start-line failures still exit(2) here: those are
/// harness-health conditions no escalation can repair.
async fn close_spawn_race_round(scenario: &str, handicap: u64) -> RaceRound {
    let root = Scope::new_root(ScopeOpts::named("tck-close-spawn-race-root"));

    let mut admitted_lost: u64 = 0;
    let mut refused_unresolved: u64 = 0;
    let mut close_incomplete: u64 = 0;
    let mut observed_admitted: u64 = 0;
    let mut observed_refused: u64 = 0;

    for i in 0..ITERATIONS {
        let child_scope = root.child(ScopeOpts::named("tck-close-spawn-race-child"));

        let closer_extra: u64 = if i % 2 == 0 { 0 } else { handicap };
        let racer_extra: u64 = if i % 2 == 0 { handicap } else { 0 };

        let ran = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let go = Arc::new(AtomicBool::new(false));

        let racer_scope = child_scope.clone();
        let racer_ran = Arc::clone(&ran);
        let racer_started = Arc::clone(&started);
        let racer_go = Arc::clone(&go);
        let racer = tokio::spawn(
            async move {
                // The task's OWN return value is deliberately a `TaskHandle`
                // (itself `Future`-shaped — clippy's `async_yields_async`
                // fires on that shape by default): the whole point is to
                // hand the UNRESOLVED handle back to the caller so admission
                // vs. resolution can be observed as two separate steps below.
                #![allow(clippy::async_yields_async)]
                racer_started.store(true, Ordering::Release);
                // Uninterruptible busy-spin (no yield point): reacts to `go`
                // with the minimum possible latency instead of paying a
                // wake+reschedule round trip (see the doc comment above).
                let mut spins: u64 = 0;
                while !racer_go.load(Ordering::Acquire) {
                    spins += 1;
                    if spins > 200_000_000 {
                        break; // safety valve; main's own 5s deadline covers the rest
                    }
                    std::hint::spin_loop();
                }
                jitter_spin(race_jitter_seed(i, 1), racer_extra);
                racer_scope.spawn_with(move |_task_scope| {
                    let ran = Arc::clone(&racer_ran);
                    async move {
                        ran.store(true, Ordering::Release);
                    }
                })
            },
        );

        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !started.load(Ordering::Acquire) && tokio::time::Instant::now() < ready_deadline {
            tokio::task::yield_now().await;
        }
        if !started.load(Ordering::Acquire) {
            // FAIL CLOSED: the old code
            // logged and continued, degenerating this iteration into a
            // non-concurrent close-then-spawn — the scenario would still
            // report all-zero invariants and PASS without ever having
            // raced anything. If the harness cannot even prove the racer
            // reached its start line, it cannot claim to have exercised
            // concurrent admission at all.
            eprintln!(
                "flow_tck_emitter[{scenario}]: close_spawn_race: iteration {i}: racer never reached its start line within 5s"
            );
            std::process::exit(2);
        }

        go.store(true, Ordering::Release);
        jitter_spin(race_jitter_seed(i, 0), closer_extra);

        let close_ok = tokio::time::timeout(Duration::from_secs(5), child_scope.close())
            .await
            .is_ok();
        if !close_ok {
            close_incomplete += 1;
        }

        let handle = match racer.await {
            Ok(h) => h,
            Err(e) => {
                eprintln!(
                    "flow_tck_emitter[{scenario}]: close_spawn_race: iteration {i}: racer task panicked/was aborted: {e}"
                );
                std::process::exit(2);
            }
        };

        match tokio::time::timeout(Duration::from_secs(5), handle.join()).await {
            Ok(_outcome) => {
                // value OR cancelled both fine — only the ADMISSION
                // classification (via `ran`, observed independently of
                // the outcome) matters for this scenario's counters.
                if ran.load(Ordering::Acquire) {
                    observed_admitted += 1;
                } else {
                    observed_refused += 1;
                }
            }
            Err(_) => {
                if ran.load(Ordering::Acquire) {
                    observed_admitted += 1;
                    admitted_lost += 1;
                } else {
                    observed_refused += 1;
                    refused_unresolved += 1;
                }
            }
        }
    }

    // The ENFORCED both-outcomes postcondition lives in the caller: it
    // applies to the FINAL round after handicap escalation, not to every probe round.
    root.close().await;
    RaceRound {
        admitted_lost,
        refused_unresolved,
        close_incomplete,
        observed_admitted,
        observed_refused,
    }
}

/// `Supervision::Restart`: the first attempt faults, the second
/// succeeds — the handle resolves the SUCCESSFUL value, and the
/// factory-invocation count is observed via the closure's own counter
/// (not an internal implementation detail).
async fn restart_success(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-restart-success-root").supervision(
        Supervision::Restart {
            max_restarts: 3,
            window: Duration::from_secs(10),
            backoff: Duration::from_millis(5),
        },
    ));

    let invocations = Arc::new(AtomicU32::new(0));
    let inv = Arc::clone(&invocations);
    let handle = root.spawn_supervised(move || {
        let n = inv.fetch_add(1, Ordering::SeqCst);
        async move {
            if n == 0 {
                panic!("tck-restart-fault");
            }
            "recovered".to_string()
        }
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_supervised");

    match handle.join().await {
        Ok(v) => emit_value(scenario, "task.await", &v),
        Err(stopped) => unexpected_stop(scenario, "task.await", stopped.reason()),
    }

    emit_counter(
        scenario,
        "scope.stats",
        "factory_invocations",
        invocations.load(Ordering::SeqCst) as i64,
    );

    root.close().await;
}

/// `Supervision::Restart` with `max_restarts: 1`: the factory always
/// faults, so the SECOND fault exhausts restarts and escalates to
/// fail-fast — the handle resolves the FINAL fault verbatim, the scope
/// is cancelled `ChildFailed`, and exactly 2 factory invocations ran.
async fn restart_exhaustion(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-restart-exhaustion-root").supervision(
        Supervision::Restart {
            max_restarts: 1,
            window: Duration::from_secs(10),
            backoff: Duration::from_millis(5),
        },
    ));

    let invocations = Arc::new(AtomicU32::new(0));
    let inv = Arc::clone(&invocations);
    let handle = root.spawn_supervised(move || {
        inv.fetch_add(1, Ordering::SeqCst);
        async move {
            panic!("tck-restart-fault");
        }
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_supervised");

    match handle.join().await {
        Err(stopped) => match stopped.reason() {
            StopReason::Fault { code, message } => {
                emit_stopped_fault(scenario, "task.await", *code, message);
            }
            other => unexpected_stop(scenario, "task.await", other),
        },
        Ok(()) => unexpected_ok(scenario, "task.await"),
    }

    let raw = root.cancel_reason().map(|r| r.as_u8() as i64).unwrap_or(-1);
    emit_counter(scenario, "scope.stats", "scope_cancel_reason_raw", raw);
    emit_counter(
        scenario,
        "scope.stats",
        "factory_invocations",
        invocations.load(Ordering::SeqCst) as i64,
    );

    root.close().await;
}

// --- wave-2 additions below (SCHEMA.md v1, `scope.shutdown_grace` op) ------

/// A quick child completes well inside a generous (2s) grace window —
/// `shutdown_with_grace`'s own bounded `close()` (`core/src/flow/scope.rs`:
/// `if tokio::time::timeout(grace, self.close()).await.is_ok() { return 0;
/// }`) drains the subtree normally, so the OBSERVED detached count is 0 —
/// the clean-drain path. Matches `expected/shutdown_clean.json`.
async fn shutdown_clean(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-shutdown-clean-root"));

    let handle = root.spawn(async { 7i32 });
    emit_lifecycle(scenario, "task.spawn", "spawned");

    match handle.join().await {
        Ok(v) => emit_value(scenario, "task.await", &v.to_string()),
        Err(stopped) => unexpected_stop(scenario, "task.await", stopped.reason()),
    }

    // `shutdown_with_grace` performs its own cancel()+close() pair — a
    // separate `root.close()` call would just be a redundant no-op on an
    // already-drained scope.
    let detached = root
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_secs(2))
        .await;
    emit_counter(
        scenario,
        "scope.shutdown_grace",
        "detached",
        detached as i64,
    );
}

/// Straggler recipe proven in `grace_shutdown_detaches_stragglers_and_settles`
/// (`core/test/flow_scope_tests.rs`): a child that BLOCKS its own worker
/// thread with `std::thread::sleep` (never `tokio::time::sleep`, which
/// would yield at a cooperative await point) reliably outlives a short
/// grace window — Rust cannot forcibly preempt a running task, only a
/// checkpoint/await can observe cancellation, and this child never reaches
/// one until `release` flips. `shutdown_with_grace` detaches it (returns 1)
/// rather than blocking `close()` forever. Matches
/// `expected/shutdown_grace_expired.json`.
async fn shutdown_grace_expired(scenario: &str) {
    let root = Scope::new_root(ScopeOpts::named("tck-shutdown-grace-expired-root"));

    let release = Arc::new(AtomicBool::new(false));
    let release_child = Arc::clone(&release);
    let _handle = root.spawn(async move {
        loop {
            std::thread::sleep(Duration::from_millis(15));
            if release_child.load(Ordering::SeqCst) {
                return;
            }
        }
    });
    emit_lifecycle(scenario, "task.spawn", "spawned_straggler");

    // Let the straggler actually start blocking before racing the grace
    // window against it (mirrors the core test's own settle sleep before
    // calling shutdown_with_grace).
    tokio::time::sleep(Duration::from_millis(20)).await;

    let detached = root
        .shutdown_with_grace(CancelReason::Shutdown, Duration::from_millis(50))
        .await;
    emit_counter(
        scenario,
        "scope.shutdown_grace",
        "detached",
        detached as i64,
    );

    // Release the straggler so it exits promptly instead of forcing this
    // process's own tokio runtime to block on it at teardown.
    release.store(true, Ordering::SeqCst);
}

// --- wave-3 additions below (SCHEMA.md v1) ---------------------------------

/// `admission_counters` (`test/flow_tck/expected/admission_counters.json`):
/// drives the Rust plane's NATIVE queue-admission tier to produce EXACTLY 3
/// full-queue admission refusals and 2 deadline drops, then emits the
/// OBSERVED counter reads under the SCHEMA.md unified logical names
/// (`admission_refused`, `deadline_dropped`).
///
/// Two SEPARATE `SingleThreadedExecutor`s isolate the two mechanisms so
/// neither counter's post/dequeue timing can perturb the other:
///
/// - `admission_refused`: `core/src/event_queue.rs`'s `AdmissionState` (the
///   post-time admission tier) — a capacity-1 bounded queue, filled once,
///   then 3 more `try_send`s each observe `TrySendError::Full` and bump the
///   native `dropped_queue_full` counter (`event_queue.rs:144`, via
///   `EventQueueSender::try_send_queue`, `core/src/event_queue.rs:814`).
///   `dropped_queue_full` only populates with `admission_observability` on
///   (`AORTA_EXECUTOR_ADMISSION_OBSERVABILITY`, read once at construction —
///   `core/src/executor.rs::build_executor_queue`), so this process sets
///   that env var before either executor below is built — safe because
///   this binary runs exactly one scenario per process and nothing else
///   reads/writes it concurrently.
/// - `deadline_dropped`: the per-event deadline mechanism
///   (`Event::with_deadline`/`is_expired_at`) — 2 events posted with an
///   ALREADY-expired deadline, then `spin_once()` dequeues each: the
///   scheduler's drop-if-expired check (`SingleThreadedExecutor::
///   pop_ready_event`, `core/src/executor.rs:1625`) fires and bumps the
///   native `dropped_expired` counter — UNCONDITIONAL, no observability
///   gate unlike the admission tier above (proven by the existing core
///   test `expired_events_are_dropped_at_dequeue`,
///   `core/test/executor_tests.rs:664`).
async fn admission_counters(scenario: &str) {
    // Gates `dropped_queue_full` (and the other `AdmissionStats` drop
    // counters) — env read once at `EventQueue` construction
    // (`core/src/executor.rs` `ENV_ADMISSION_OBSERVABILITY`), so it must be
    // set before the executor below is built.
    std::env::set_var("AORTA_EXECUTOR_ADMISSION_OBSERVABILITY", "1");

    // --- admission_refused: capacity-1 queue, fill it, then 3 refusals.
    let refused_exec = SingleThreadedExecutor::new(1);
    refused_exec
        .queue()
        .sender()
        .try_send(Event::new(EventKind::SubscriptionData, || {}))
        .expect("filler event admitted (queue starts empty, capacity=1)");
    let mut observed_refusals = 0u64;
    for _ in 0..3 {
        match refused_exec
            .queue()
            .sender()
            .try_send(Event::new(EventKind::SubscriptionData, || {}))
        {
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => observed_refusals += 1,
            other => {
                eprintln!(
                    "flow_tck_emitter[{scenario}]: admission_counters: expected TrySendError::Full on a saturated queue, got {other:?}"
                );
                std::process::exit(2);
            }
        }
    }
    let admission_refused = refused_exec.admission_stats().dropped_queue_full;
    if observed_refusals != 3 || admission_refused != 3 {
        eprintln!(
            "flow_tck_emitter[{scenario}]: admission_counters: expected 3 observed refusals and dropped_queue_full==3, got observed={observed_refusals} dropped_queue_full={admission_refused}"
        );
        std::process::exit(2);
    }
    emit_counter(
        scenario,
        "queue.stats",
        "admission_refused",
        admission_refused as i64,
    );

    // --- deadline_dropped: 2 already-expired events, dequeued via spin_once().
    let mut expiry_exec = SingleThreadedExecutor::new(4);
    for _ in 0..2 {
        let event = Event::new(EventKind::SubscriptionData, || {})
            .with_deadline(Some(std::time::Instant::now()));
        expiry_exec
            .queue()
            .sender()
            .try_send(event)
            .expect("expired-deadline event admitted (queue has headroom)");
    }
    // Let a nonzero amount of monotonic time pass so `is_expired_at`'s
    // `now >= deadline` compare is unambiguously true at dequeue (the
    // deadline was captured at `Instant::now()` above; the clock only ever
    // advances, so there is no underflow risk either way).
    tokio::time::sleep(Duration::from_millis(5)).await;
    let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while expiry_exec.dropped_expired() < 2 && tokio::time::Instant::now() < poll_deadline {
        let _ = expiry_exec.spin_once(Duration::from_millis(50)).await;
    }
    let deadline_dropped = expiry_exec.dropped_expired();
    if deadline_dropped != 2 {
        eprintln!(
            "flow_tck_emitter[{scenario}]: admission_counters: expected dropped_expired==2 within 5s, got {deadline_dropped}"
        );
        std::process::exit(2);
    }
    emit_counter(
        scenario,
        "queue.stats",
        "deadline_dropped",
        deadline_dropped as i64,
    );
}

/// `budget_expiry_rpc_call` (`test/flow_tck/expected/budget_expiry_rpc_call.json`):
/// a flow client call issued under an ALREADY-EXPIRED scope budget stops
/// with `DeadlineExceeded` BEFORE the wire — `Client::call_in`'s own
/// `scope.checkpoint()?` (`core/src/client.rs:441`, the very first
/// statement) observes the expired budget and returns immediately; no
/// query is ever issued. Mirrors the exact harness in
/// `call_in_reports_deadline_on_an_expired_budget`
/// (`core/test/flow_scope_tests.rs:326`): a `Session::open_default()`
/// against the in-process local test router. This binary is NOT
/// `#[cfg(test)]`, so `AORTA_TEST_LOCAL_ROUTER` is what flips
/// `should_use_local_test_router()` on (`core/src/session.rs:84-95`) — the
/// SAME env var `rust/tests/cross_namespace_sdk.rs` sets from ordinary
/// (non-`cfg(test)`) Rust test code, so no `run_tck.sh` env export is
/// needed (verified empirically — see the VALIDATE section).
async fn budget_expiry_rpc_call(scenario: &str) {
    std::env::set_var("AORTA_TEST_LOCAL_ROUTER", "1");
    let session = Session::open_default()
        .await
        .expect("local-router session open");
    let client = CoreClient::builder(session, "tck-budget-rpc-call-svc").build();

    // budget=ZERO is already-expired the instant it is read (`Budget::
    // is_expired`: `dur.saturating_sub(elapsed) == ZERO` for `dur == ZERO`
    // regardless of `elapsed`) — same recipe as
    // `call_in_reports_deadline_on_an_expired_budget`, no sleep needed.
    let scope =
        Scope::new_root(ScopeOpts::named("tck-budget-rpc-call-root").budget(Duration::ZERO));

    emit_lifecycle(scenario, "call", "call_on_expired_budget");
    match client.call_in(&scope, vec![1u8]).await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "call",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "call", other),
        },
        Ok(_) => unexpected_ok(scenario, "call"),
    }

    scope.close().await;
}

/// `budget_expiry_queued_server`
/// (`test/flow_tck/expected/budget_expiry_queued_server.json`): a request
/// whose wire-stamped budget expires WHILE QUEUED behind a blocked handler
/// is dropped by the server's pre-handler deadline gate (RPC deadline
/// enforcement, `core/src/service.rs`
/// `wrap_handler_with_deadline_gate`/`deadline_gate_drops`) — a DEDICATED
/// `request_dropped_deadline_count` counter (`core/src/service.rs:62,81`),
/// a SUBSET of the generic `request_dropped_count`.
///
/// Harness: a `SingleThreadedExecutor`-backed `Service` (so requests
/// genuinely QUEUE behind a blocked in-flight dispatch — one dispatch
/// thread) whose handler blocks the FIRST ("gate") request on an atomic
/// flag; the SECOND (budgeted) request is sent via `call_in` with a short
/// scope budget, queues behind the gate, and its OWN client-side timeout
/// resolves `Stopped::cancelled(DeadlineExceeded)` — independent of the
/// server (no reply is sent: the caller's own guard has already fired by
/// definition). Only THEN does this emitter release the
/// gate, letting the executor dequeue the budgeted request's event and the
/// deadline gate drop it — the emitter POLLS `RunningService::stats()`
/// (bounded, OBSERVED reads) until the drop counter reads 1.
async fn budget_expiry_queued_server(scenario: &str) {
    std::env::set_var("AORTA_TEST_LOCAL_ROUTER", "1");
    let session = Session::open_default()
        .await
        .expect("local-router session open");
    let service_name = format!("tck-budget-queued-svc-{}", std::process::id());

    let mut exec = SingleThreadedExecutor::new(8);
    let stop_handle = exec.stop_handle();
    let sender = exec.queue().sender();
    let spin_task = tokio::spawn(async move {
        exec.spin().await;
    });

    let released = Arc::new(AtomicBool::new(false));
    let released_handler = Arc::clone(&released);
    let unexpected_run = Arc::new(AtomicBool::new(false));
    let unexpected_run_handler = Arc::clone(&unexpected_run);
    let gate_entered = Arc::new(AtomicBool::new(false));
    let gate_entered_handler = Arc::clone(&gate_entered);
    let service = CoreService::builder(session.clone(), &service_name)
        .event_sender(sender)
        .handler(move |request| {
            let payload = request.payload().to_bytes().into_owned();
            if payload == b"gate".to_vec() {
                // OBSERVABLE occupancy barrier (GH-hosted CI, 2026-07-15:
                // the fixed 500ms/100ms sleeps raced routing under load —
                // the budgeted request then never reached the queue and
                // the drop count stayed 0). The emitter waits on this
                // flag, which simultaneously proves the route works AND
                // that this single dispatch thread is now blocked.
                gate_entered_handler.store(true, Ordering::SeqCst);
                // Block THIS single dispatch thread until the emitter
                // releases the gate — every request behind this one sits
                // queued on the executor for the duration.
                while !released_handler.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
                tokio::spawn(async move {
                    let _ = request.reply(b"released".to_vec()).await;
                });
            } else {
                // The deadline gate should have dropped this request in
                // queue — reaching the real handler at all is a harness
                // finding, not expected behavior.
                unexpected_run_handler.store(true, Ordering::SeqCst);
                tokio::spawn(async move {
                    let _ = request.reply(b"unexpected".to_vec()).await;
                });
            }
        })
        .build();
    let running = service.start().await.expect("service start");

    // Route + occupancy barrier: retry the gate call until the handler
    // proves (via gate_entered) that it is running and blocking the
    // single dispatch thread. Each attempt is its own call — a routing
    // race can drop an attempt wholesale ("sending on a closed channel"),
    // observed on the GH-hosted runner. Bounded and fail-closed.
    let mut attempts = 0u32;
    while !gate_entered.load(Ordering::SeqCst) {
        attempts += 1;
        if attempts > 10 {
            eprintln!(
                "flow_tck_emitter[{scenario}]: budget_expiry_queued_server: gate request never reached the handler after {attempts} attempts"
            );
            std::process::exit(2);
        }
        let gate_client = CoreClient::builder(session.clone(), &service_name).build();
        tokio::spawn(async move {
            let _ = gate_client
                .call_with_timeout(b"gate".to_vec(), 30_000)
                .await;
        });
        let attempt_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !gate_entered.load(Ordering::SeqCst) && tokio::time::Instant::now() < attempt_deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let client = CoreClient::builder(session.clone(), &service_name).build();
    let scope = Scope::new_root(
        ScopeOpts::named("tck-budget-queued-root").budget(Duration::from_millis(200)),
    );

    emit_lifecycle(scenario, "call", "request_sent");
    match client.call_in(&scope, b"payload".to_vec()).await {
        Err(stopped) => match stopped.reason() {
            StopReason::Cancelled(reason) => {
                emit_stopped_cancelled(
                    scenario,
                    "call",
                    cancel_reason_name(*reason),
                    reason.as_u8(),
                );
            }
            other => unexpected_stop(scenario, "call", other),
        },
        Ok(_) => unexpected_ok(scenario, "call"),
    }

    // Only now release the blocked gate handler — the deadline gate must
    // observe the ALREADY-expired budget when it finally dequeues the
    // budgeted request.
    released.store(true, Ordering::SeqCst);

    let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut dropped = 0u64;
    while tokio::time::Instant::now() < poll_deadline {
        dropped = running.stats().request_dropped_deadline_count;
        if dropped >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if dropped != 1 {
        eprintln!(
            "flow_tck_emitter[{scenario}]: budget_expiry_queued_server: expected request_dropped_deadline_count==1 within 5s, got {dropped}"
        );
        std::process::exit(2);
    }
    if unexpected_run.load(Ordering::SeqCst) {
        eprintln!(
            "flow_tck_emitter[{scenario}]: budget_expiry_queued_server: the budgeted request's real handler ran — the deadline gate did not drop it"
        );
        std::process::exit(2);
    }
    emit_counter(
        scenario,
        "service.stats",
        "request_dropped_deadline",
        dropped as i64,
    );

    stop_handle.stop();
    let _ = spin_task.await;
    drop(running);
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let scenario = match std::env::args().nth(1) {
        Some(s) => s,
        None => {
            eprintln!("usage: flow_tck_emitter <scenario>");
            std::process::exit(2);
        }
    };
    match scenario.as_str() {
        "cancel_reason_observation" => cancel_reason_observation(&scenario).await,
        "cancel_reason_explicit" => cancel_reason_explicit(&scenario).await,
        "value_delivery" => value_delivery(&scenario).await,
        "move_only_value" => move_only_value(&scenario).await,
        "budget_expiry_entry" => budget_expiry_entry(&scenario).await,
        "budget_expiry_parked" => budget_expiry_parked(&scenario).await,
        "budget_expiry_checkpoint" => budget_expiry_checkpoint(&scenario).await,
        "fault_message_exact" => fault_message_exact(&scenario).await,
        "parent_child_cancel_order" => parent_child_cancel_order(&scenario).await,
        "sibling_fail_fast" => sibling_fail_fast(&scenario).await,
        "isolated_child_failure" => isolated_child_failure(&scenario).await,
        "when_all_empty" => when_all_empty(&scenario).await,
        "when_all_multi" => when_all_multi(&scenario).await,
        "when_all_fault" => when_all_fault(&scenario).await,
        "when_any_first_wins" => when_any_first_wins(&scenario).await,
        "when_any_empty" => when_any_empty(&scenario).await,
        "close_vs_racing_spawn" => close_vs_racing_spawn(&scenario).await,
        "close_spawn_race" => close_spawn_race(&scenario).await,
        "restart_success" => restart_success(&scenario).await,
        "restart_exhaustion" => restart_exhaustion(&scenario).await,
        "shutdown_clean" => shutdown_clean(&scenario).await,
        "shutdown_grace_expired" => shutdown_grace_expired(&scenario).await,
        "admission_counters" => admission_counters(&scenario).await,
        "budget_expiry_rpc_call" => budget_expiry_rpc_call(&scenario).await,
        "budget_expiry_queued_server" => budget_expiry_queued_server(&scenario).await,
        other => {
            eprintln!("flow_tck_emitter: unknown scenario {other:?}");
            std::process::exit(2);
        }
    }
}
