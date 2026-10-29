# Aorta Timer and Periodic Path Design

## Status

- Date: March 13, 2026
- Scope: Rust SDK, C++ SDK, Core timer semantics, generic timer-driven publish path
- Goal: keep the existing generic SDK surface, optimize it aggressively, and avoid adding a dedicated periodic-publisher API unless benchmark evidence later proves it is necessary

## Current Direction

This design intentionally does **not** introduce a publisher-owned periodic API such as
`start_periodic_*()` or `StartPeriodic*()`.

Current product direction:

1. Keep the public SDK model centered on the existing generic interfaces:
   - create a timer
   - execute user callback
   - publish inside that callback
2. Treat dedicated periodic publishing as an internal optimization problem first, not as a new public abstraction.
3. Optimize the generic path until benchmark evidence shows that the remaining gap cannot be closed without a new API.

## Benchmark-Grounded Findings

Recent benchmark work established the following:

### 1. Timer wake-up quality was a real problem

For Linux `KeepLatest`/coalescing behavior, the previous best-effort wake-up backend caused
severe firing loss at high rates.

That issue is now largely addressed on real Linux ARM64 by the shared high-precision timer lane:

- shared `timerfd`
- shared `eventfd`
- min-heap of next deadlines

This means current timer precision is no longer the primary bottleneck for high-rate
`KeepLatest` on Linux.

**Backend robustness notes:**

- **Non-Linux high-precision wait.** Linux bridges the gap between the coarse
  sleep margin and the final sub-`yield_margin` precision spin with
  `clock_nanosleep(TIMER_ABSTIME)`. Non-Linux targets lack that call; the lane now bridges
  the same `[yield_margin, sleep_margin]` window with a real `thread::sleep` down to the
  yield margin instead of spin-yielding it, so a high-precision timer no longer pins a full
  core per firing on non-Linux hosts. The decision (`non_linux_wait_action`) is a pure
  function unit-tested on every target.
- **Deadline-heap compaction.** The `timerfd` min-heap uses lazy deletion:
  pause/resume/remove leave stale entries reclaimed only when they reach the top. A long-
  period timer that is repeatedly paused/resumed while another timer holds an earlier
  deadline could therefore accumulate far-future stale entries without bound. The scheduler
  now compacts the heap to one entry per live timer whenever it bloats past
  `live * 2 + 64`, bounding heap size to O(live timers).

### 2. Typed glue is not the dominant bottleneck

Both direct publisher benchmark (`bench_pub`) and timer-driven benchmark (`timer_compare`) were
run with `manual|typed` variants.

Observed result:

- `manual ~= typed` for Rust
- `manual ~= typed` for C++

Therefore the following are not the main optimization target right now:

- `publish_typed_msg(...)`
- `PublishTypedMessage(...)`
- typed wrapper glue
- typed codegen dispatch overhead

### 3. C++ is slower than Rust in pure publish path

In direct publisher benchmark at `1MB@5000`, C++ already trails Rust in pure publish throughput.

This means C++ has an intrinsic publish/build-body cost gap even before timer callback overhead
is added.

### 4. C++ loses much more additional throughput under generic timer-driven publishing

When moving from direct publish benchmark to `timer_compare`:

- Rust remains close to its direct-publish ceiling
- C++ drops much more sharply

This indicates that C++ has an additional cost in the generic
`timer callback -> publish(...)` path beyond its already-heavier direct publish path.

## Public API Position

### Short-Term Position

No dedicated periodic publisher API is planned in the current phase.

The preferred user model remains:

1. `create_timer(...)`
2. perform user work inside the callback
3. call `publish(...)` from that callback

### Timer Semantics

The system still benefits from intent-based periodic semantics, but they are treated as
runtime behavior first, not as a reason to add a dedicated publishing API.

Useful semantic terms:

- `KeepLatest`
- `CatchUp`

These describe periodic behavior, but current engineering work does **not** require introducing a
separate publisher-owned API surface to express them.

### Advanced Options

Existing low-level controls remain valid as Core/advanced configuration:

- `invoke_on_missed`
- `overflow_policy`
- `overflow_block_timeout_ms`

They are still runtime tuning knobs, not the primary optimization target.

## Design Principles

1. Preserve the current generic public SDK surface.
2. Prefer internal optimization over new public APIs.
3. Keep Rust and C++ semantics aligned.
4. Keep `ExecutionGroup` as the only callback concurrency abstraction.
5. Optimize the common path first, not a specialized side path.
6. Avoid introducing a new abstraction until the generic path is proven insufficient.

## What Is Explicitly Out of Scope Right Now

The following are intentionally out of scope for the current phase:

- publisher-owned periodic APIs such as `start_periodic_*()` / `StartPeriodic*()`
- public SHM / loan / frame publishing APIs
- new public timing-class knobs
- a redesign of user-visible callback concurrency semantics
- a full executor topology rewrite

## Optimization Plan

### Phase 1: Improve C++ generic publish path

Primary target:

- reduce C++ build/body overhead in the normal `publish()` path

Reasoning:

- C++ already trails Rust in direct publish benchmark
- this gap exists before timer callback overhead is involved

Expected focus areas:

- builder lifecycle and reuse strategy
- buffer lifetime and publish handoff preparation
- avoid unnecessary per-publish setup work in the common path

This phase does **not** change the public API.

### Phase 2: Reduce generic timer-callback glue overhead

Primary target:

- reduce the fixed cost of `timer callback -> publish(...)`

Reasoning:

- the largest remaining C++ loss appears only when publish is driven from the generic timer path
- `manual ~= typed`, so the extra loss is not typed glue; it is the generic timer-driven publish
  composition path

Expected focus areas:

- timer event wrapper cost
- callback dispatch glue
- publish invocation path from generic timer callbacks
- unnecessary metadata or wrapper movement between timer and publish

This phase also does **not** change the public API.

### Phase 3: Optimize shared handoff/core/transport path

Primary target:

- reduce the shared publish handoff/core cost that affects both Rust and C++

Reasoning:

- benchmark timing shows a stable handoff/core cost in both languages
- this is a real shared cost, but it is not the first-order explanation for the current C++
  deficit

Expected focus areas:

- shared publish handoff path
- core publish cost
- transport publish lock/wait cost

## Why a Dedicated Periodic API Is Deferred

A dedicated periodic-publisher API remains a possible future option, but it is intentionally
deferred because current evidence does not justify making it the first move.

Benchmark evidence currently shows:

1. timer precision on Linux can be improved behind the existing generic model
2. typed glue is not the main bottleneck
3. the largest remaining gap is still inside the generic path

Therefore the pragmatic move is:

- optimize the generic path first
- re-evaluate later

Only if the generic path still leaves a meaningful structural gap after those optimizations should
the project consider introducing a dedicated periodic-publisher API.

## Verification Plan

Every optimization phase should be validated with the benchmark suite:

1. `timer_noop_compare`
   - verify wake-up quality
   - verify `deadline_slip` and `due_firings`
2. direct publisher benchmark (`bench_pub`)
   - verify direct publish ceiling
3. timer-driven benchmark (`timer_compare`)
   - verify end-to-end periodic publish behavior
   - compare Rust vs C++
   - compare Aorta vs ROS2

For the current plan, the most important validation cases are:

- `1MB@1000`
- `1MB@5000`
- `64K@5000`

## Decision Summary

- Keep the public SDK surface centered on generic timer + publish.
- Do not introduce a dedicated periodic publisher API in the current phase.
- Treat Linux high-precision timer work as an internal backend improvement.
- Prioritize C++ generic publish-path optimization first.
- Then optimize generic timer-callback glue.
- Then optimize shared handoff/core/transport cost.
