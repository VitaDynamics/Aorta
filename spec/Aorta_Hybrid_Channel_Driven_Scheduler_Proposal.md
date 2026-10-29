# Aorta Hybrid Channel-Driven Scheduler Proposal

Last updated: 2026-02-11

> Historical design note: describes the design at the time it was written; it is not a v0.1.0 instruction.

## 1. Purpose
This document proposes a **Hybrid channel-driven scheduler** for Aorta `MultiThreadedExecutor`, and defines how it aligns with:
- Existing Core/FFI contracts.
- Future `AsyncExecutor` evolution.
- Rust/C++/Python SDK APIs.

This proposal is designed for the executor work at the time of writing and future async expansion.

Related docs:
- `Aorta_Executor_Queue_Design_And_Rust_Queue_Survey.md`

---

## 2. Background and Problem
Current status:
- `SingleThreadedExecutor` + explicit priority mode is available.
- `AORTA_EXECUTOR_MULTI` is not implemented yet.
- `callback_group` is exposed in FFI/C++ options, but runtime semantics are not fully enforced in Core multi-thread scheduling.

Constraints:
- Aorta uses Zenoh callbacks and Rust/Tokio runtime.
- Callback hot path must stay lightweight.
- Existing FIFO users must not be penalized.
- Break change is acceptable in this project stage, but cross-language consistency must be preserved.

Key design question:
- How to get low tail latency and high throughput while keeping runtime behavior debuggable and SDK-friendly?

---

## 3. Candidate Comparison

### 3.1 Schemes in Scope
1. ROS2-style callback group scheduling (group mutex + simple ready queue).
2. Resource-tag conflict graph scheduling.
3. Priority + quota + anti-starvation policy scheduler.
4. Actor/shared-nothing model.
5. **Hybrid channel-driven scheduler** (this proposal).

### 3.2 Summary Comparison

| Dimension | ROS2-style Group | Resource Conflict Graph | Priority+Quota | Actor Model | Hybrid Channel-Driven |
| :--- | :--- | :--- | :--- | :--- | :--- |
| p50 latency | Good | Good | Good | Medium | Good |
| p99 latency control | Medium | Medium | Excellent | Good | Excellent |
| Peak throughput | Medium | Excellent | Good | Medium | Good-Excellent |
| Starvation control | Medium | Medium | Excellent | Good | Excellent |
| Runtime complexity | Low | High | Medium-High | High | Medium |
| Debuggability | Excellent | Medium-Low | Medium | Medium | Good |
| Migration risk | Low | High | Medium | Very High | Medium-Low |
| Zenoh + Tokio fit | Good | Medium | Good | Medium | Excellent |

### 3.3 Why Hybrid is Preferred
Hybrid keeps the strongest practical properties for current Aorta phase:
- Channel-driven ingress fits Zenoh callback model.
- Central admission point enables deterministic priority + fairness policy.
- Callback group semantics remain explicit and user understandable.
- Backend can evolve from thread-pool to async execution without redesigning scheduling semantics.

---

## 4. Design Goals and Non-Goals

### 4.1 Goals
1. Implement `MultiThreadedExecutor` with deterministic scheduling behavior.
2. Enforce callback-group concurrency semantics (`MutuallyExclusive` / `Reentrant`).
3. Maintain low overhead for non-priority workloads.
4. Keep async evolution path open (`AORTA_EXECUTOR_ASYNC`).
5. Provide consistent behavior across Core/FFI/Rust/C++ and future Python.

### 4.2 Non-Goals (This Phase)
1. Full resource-conflict graph scheduling as default runtime.
2. Hard realtime guarantee at OS level.
3. Full deadline enforcement (deadline remains soft/observability-first).
4. Replacing current ingress channel substrate (`tokio::sync::mpsc`) in this phase.

---

## 5. Proposed Architecture

### 5.1 High-Level Pipeline

1. **Ingress (channel-driven)**  
   Zenoh callback wraps payload into `EventEnvelope` and pushes into bounded ingress channel.

2. **Admission + Scheduling (centralized)**  
   Scheduler dispatcher:
   - Drains ingress in batches (`max_drain_per_cycle`).
   - Classifies into priority lanes.
   - Applies callback-group gating.
   - Applies fairness/anti-starvation policy.
   - Dispatches runnable tasks to execution backend.

3. **Execution backend (pluggable)**  
   - Phase 3: fixed worker thread-pool backend.
   - Phase 4+: async backend on Tokio (`AsyncExecutor`) reusing same admission semantics.

### 5.2 Scheduler Core Types (Rust Core)

```rust
enum SchedulerMode {
    Fifo,
    Priority,
}

enum CallbackGroupKind {
    MutuallyExclusive,
    Reentrant,
}

struct EventEnvelope {
    priority: EventPriority,
    callback_group_id: CallbackGroupId,
    task: Event,
}
```

Important rule:
- Scheduling and execution are separated.  
  Scheduler decides *when/if* a task runs, backend decides *where/how* task runs.

### 5.3 Callback Group Semantics (Executor-Scoped Protection Domain)

Design decision:
- Callback groups are enforced **inside the same executor scheduling domain**.
- Default behavior is safe: entities without explicit group binding go to a default `MutuallyExclusive` group.

Effect:
- Same default group: serialized execution, stronger race protection.
- Different groups or `Reentrant`: allows concurrency for throughput.

This is consistent with ROS2 mental model but keeps Aorta scheduler policy explicit.

### 5.4 Priority and Fairness

Priority lanes:
- Realtime / High / Normal / Low / Background.

Dispatch policy:
1. Strict preference for higher priority runnable tasks.
2. Weighted round-robin + per-cycle quota to prevent starvation.
3. Admission-level starvation tracking and fallback dispatch for long-wait lower priorities.

Compatibility:
- FIFO mode remains default for backward behavior and minimal overhead.

### 5.5 Backpressure and Overload Behavior

1. Bounded ingress queue (required).
2. Configurable drop/degrade strategy for low-priority overload.
3. High-priority lanes should be protected from low-priority queue explosion.

Planned overload signals:
- queue_full_count
- dropped_low_priority_count
- scheduler_overrun_count

---

## 6. API and Contract Evolution

### 6.1 Core API (Rust `aorta_core`)

Phase 3 target:
1. Add `MultiThreadedExecutor`.
2. Add callback-group binding on subscriber/service/timer creation paths.
3. Keep `SingleThreadedExecutor` semantics unchanged.

Proposed additions:
- `MultiThreadedExecutor::create_callback_group(kind) -> CallbackGroupHandle`
- `SubscriberOptions.callback_group_handle: Option<CallbackGroupHandle>`
- `ServiceBuilder.callback_group_handle(...)`
- `TimerOptions.callback_group_handle: Option<CallbackGroupHandle>` (optional in this phase, can be staged)

### 6.2 FFI (`c_api.rs` / `aorta.h`)

Phase 3 target:
1. `aorta_executor_new_with_options(AORTA_EXECUTOR_MULTI, ...)` supported.
2. Existing `callback_group` fields become runtime-effective.

Planned extension (break change accepted):
- Add callback-group handle type and APIs:

```c
typedef uint64_t aorta_callback_group_handle_t;

aorta_result_t aorta_executor_create_callback_group(
    aorta_exec_t exec,
    aorta_callback_group_t kind,
    aorta_callback_group_handle_t* out);

aorta_result_t aorta_executor_free_callback_group(
    aorta_exec_t exec,
    aorta_callback_group_handle_t group);
```

- Extend `aorta_sub_options_t` and `aorta_service_options_t`:
  - keep existing `callback_group` for compatibility fallback.
  - add `callback_group_handle` (0 means default group).

### 6.3 C++ SDK

Goals:
1. Keep `aorta/aorta.h` single include UX.
2. Preserve current `CallbackGroup` enum and option ergonomics.
3. Add explicit group object for shared protection domain when needed.

Proposed API:
- `Executor::CreateCallbackGroup(CallbackGroup kind)`
- `SubscriberOptions::callback_group_handle` / `ServiceOptions::callback_group_handle`
- `TimerOptions::callback_group_handle`
- If handle not provided, use executor default group.

Default guidance:
- Shared mutable state: bind to the same `MutuallyExclusive` group.
- Stateless hot path: use `Reentrant` group.

### 6.4 Rust SDK

Goals:
1. Re-export callback-group handle API from Core.
2. Keep typed API ergonomic.
3. Prepare for async executor migration.

Proposed additions:
- `MultiThreadedExecutor::create_callback_group(kind) -> CallbackGroupHandle`
- `ServiceOptions::callback_group_handle`
- `SubscriberOptions` and `TimerOptions` use callback-group handle fields from Core.

### 6.5 Python3 SDK (Future Phase Requirement)

Python constraints:
- GIL impacts callback concurrency semantics.
- Async/await integration is first-class requirement.

Recommended Python design:
1. Python API mirrors callback-group concepts (`MutuallyExclusive` / `Reentrant`).
2. Provide both sync callback mode and asyncio mode.
3. For sync callbacks:
   - execute native scheduling in Rust side.
   - enter Python callback with explicit GIL scope.
4. For asyncio mode:
   - scheduler submits events into loop-safe queue.
   - Python `async` handlers run in event loop domain.

This ensures scheduler semantics remain cross-language consistent while respecting Python runtime constraints.

---

## 7. Async Scheduler Compatibility

Key decision:
- Hybrid channel-driven scheduler is the shared semantic core for both:
  - `MultiThreadedExecutor` (thread-pool backend)
  - `AsyncExecutor` (tokio async backend)

Do not couple semantics to backend details.

Backend interface concept:
```rust
trait ExecutionBackend {
    fn try_submit(&self, task: ScheduledTask) -> SubmitResult;
    fn available_slots(&self) -> usize;
    fn request_stop(&self);
}
```

Benefits:
1. Same scheduling policy and observability across executors.
2. Async executor can be added without reworking admission/fairness logic.
3. Easier cross-SDK contract stability.

---

## 8. Performance Model and Practical Latency Guarantees

Absolute "globally optimal latency" cannot be guaranteed due to OS scheduling, CPU contention, and transport variability.

What can be guaranteed by design:
1. Bounded ingress under configured queue capacity.
2. Deterministic priority preference at scheduler admission layer.
3. Bounded starvation with quota/anti-starvation policy.
4. No Zenoh IO-thread user callback execution.

Latency formula (internal scheduling component):

`T_internal = T_ingress_wait + T_admission_wait + T_group_gate_wait + T_worker_wait`

Control levers:
- queue capacity
- per-priority weights/quota
- worker count
- callback-group topology
- overload policy

Recommended deployment tuning (not mandatory):
1. CPU affinity for scheduler/worker threads.
2. Controlled oversubscription.
3. Prefer low-latency kernel/runtime configuration on target robots.

---

## 9. Observability Requirements

Metrics (Core + FFI export):
1. ingress_queue_len
2. per_priority_queue_len
3. dispatch_count_by_priority
4. callback_group_block_count
5. callback_group_wait_ns
6. worker_utilization
7. dropped_count_by_priority
8. scheduler_cycle_ns (p50/p95/p99)
9. event_wait_ns (enqueue -> start-run, p50/p95/p99)

These metrics are required for:
- fairness tuning
- starvation diagnosis
- cross-language behavior equivalence checks

---

## 10. Testing and Verification Plan

### 10.1 Core
1. Multi-thread lifecycle (`spin/stop/spin_once`) no deadlock.
2. Callback group correctness:
   - same-group `MutuallyExclusive` no overlap
   - same-group `Reentrant` allows overlap
3. Priority + fairness:
   - high-priority preference
   - bounded progress for lower priorities

### 10.2 FFI
1. `AORTA_EXECUTOR_MULTI` creation/use/free.
2. callback_group and callback_group_handle behavior.
3. Error mapping stability.

### 10.3 Rust SDK
1. API coverage for group creation/binding.
2. Multi-thread behavior parity with Core.
3. Compatibility with typed pub/sub/service APIs.

### 10.4 C++ SDK
1. `ExecutorType::kMultiThreaded` integration.
2. `CallbackGroup` binding behavior.
3. Update examples to cover shared-group race protection and reentrant throughput path.

### 10.5 Python3 SDK (future)
1. Sync callback mode with GIL safety.
2. Async callback mode with event loop integration.
3. Consistency tests against Rust/C++ semantics.

Note: Bazel verification is developer-run per repository policy.

---

## 11. Delivery Plan Mapping

Phase 3 (now):
1. Implement `MultiThreadedExecutor` backend with hybrid admission.
2. Enforce callback-group semantics in runtime.
3. FFI support for `AORTA_EXECUTOR_MULTI`.
4. Complete the multi-threaded executor validation checklist.

Phase 3.5:
1. Add callback-group handle APIs across Core/FFI/Rust/C++.
2. Add starvation/queue metrics export.

Phase 4+:
1. Introduce `AsyncExecutor` backend using same admission semantics.
2. Expose equivalent concepts in Python SDK.
3. Optional resource-tag conflict extension as non-default advanced mode.

---

## 12. Open Decisions for Review

1. Default-group scope:
   - Option A: one default group per node
   - Option B: one default group per executor
   Decision: **Option B (per-executor)**.

2. Timer callback-group binding:
   - Include in first implementation or stage after sub/service stabilization.
   Decision: **include in first implementation**.

3. Resource-tag scheduling:
   - Keep as future optional mode, not default.
   Recommendation: yes (after hybrid baseline is stable).

### 12.1 Confirmed Usage Pattern

1. Callback groups are created from an executor and are valid only in that executor scope.
2. If `callback_group_handle` is not specified:
   - `MutuallyExclusive` entities use executor default mutually-exclusive group.
   - `Reentrant` entities use executor default reentrant group.
3. `Subscriber`, `Service`, and `Timer` all support callback-group binding in this model.
4. Shared mutable state should bind to the same `MutuallyExclusive` group.
5. Stateless high-throughput paths should use `Reentrant` groups.

---

## 13. Final Recommendation
Adopt **Hybrid channel-driven scheduler** as the mainline for Aorta multi-thread scheduling:
1. Best balance between latency control, throughput, complexity, and migration risk.
2. Strongest alignment with Zenoh callback ingress and Tokio runtime model.
3. Clean path to future `AsyncExecutor` and Python SDK without semantic rewrite.
