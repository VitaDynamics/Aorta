# Aorta Unified Channel-Driven Executor Admission Spec

Last updated: 2026-02-11
Status: Draft (implementation-oriented)
Owners: Core/FFI/SDK maintainers

> Historical design note: describes the design at the time it was written; it is not a v0.1.0 instruction.

## 1. Purpose

Define one scheduling and admission model that is shared by:

- `SingleThreadedExecutor`
- current channel-driven `MultiThreadedExecutor`
- future `AsyncExecutor`

The design must satisfy:

- low end-to-end latency under mixed load
- bounded memory growth
- explicit per-event-type backpressure behavior
- stable callback-group semantics across Core/FFI/Rust/C++/Python

## 2. Problem Statement

Current implementation uses a single executor ingress queue per executor instance.
This keeps the design simple, but under heavy subscriber traffic it can create head-of-line blocking for RPC and timer events.

Recent subscriber overflow fixes also show that event-type-specific behavior is required:

- subscriber data may accept loss with explicit policy
- RPC requests should not be silently dropped
- timer events should prefer coalescing over unbounded backlog

## 3. Scope and Non-Goals

In scope:

- L1 Core executor admission/scheduling model
- FFI and SDK option surface for admission/backpressure
- observability counters and acceptance criteria

Out of scope for this phase:

- replacing Tokio `mpsc` substrate with a custom lock-free queue
- SHM/loan data path changes
- transport-level QoS changes in Zenoh

## 4. Design Decisions

## 4.1 Logical single executor, physical multi-ingress

Keep one logical executor per instance, but split ingress into bounded channels by event class:

- `sub_data` (maps to `SubscriptionData`)
- `rpc_request` (maps to `RpcRequest`)
- `rpc_response` (maps to `RpcResponse`)
- `timer` (maps to `Timer`)
- `control` (maps to `ClockUpdate`, executor lifecycle/control notifications, internal wakeup/control-plane events)

This remains channel-driven, but removes a single shared admission bottleneck.
Implementation should use direct admission (`producer -> class ingress -> executor scheduler`) and avoid extra per-event bridge hops through a secondary queue.

## 4.2 All ingress queues are bounded

No unbounded ingress channels are allowed in scheduler hot paths.
Overflow behavior is always explicit and class-specific.

## 4.3 Event-type-specific overflow policy defaults

| Event class | Default overflow policy | Rationale |
| :--- | :--- | :--- |
| `sub_data` | `Drop` | Data stream can trade completeness for latency stability |
| `rpc_request` | `FastFailBusy` | Preserve RPC semantics, avoid silent loss |
| `rpc_response` | `Block(timeout)` | Responses should be delivered whenever possible; avoid request/response semantic drift |
| `timer` | `CoalesceLatest` | Avoid timer backlog amplification |
| `control` | `Block` (bounded) | Control-plane safety over throughput |

## 4.4 Callback-group semantics remain unchanged

`MutuallyExclusive` and `Reentrant` behavior remains per-executor and is enforced at execution stage.
Admission changes must not bypass callback-group locking.

## 4.5 Priority contract compatibility (P0)

To stay compatible with the executor priority contract:

- **Scheduling main order remains priority-first** (`Realtime > High > Normal > Low > Background`).
- Event class is used primarily for **admission isolation/backpressure policy**, not as a hard scheduler precedence that can invert priority ordering.
- Class-level fairness is applied only when picking candidates within the same priority lane (or as bounded deficit compensation), and must not allow persistent inversion where lower-priority class traffic starves higher-priority traffic.

This keeps the model aligned with current one-dimensional priority semantics while adding admission isolation.

## 4.6 Overflow semantic definitions

### 4.6.1 `CoalesceLatest` for timer

- Coalescing is performed **per timer instance** (`timer_id`), never across different timers.
- If multiple pending firings exist for the same timer, keep one runnable event representing the latest firing.
- Coalesced firings increment `timer_coalesce_count`.
- Timestamp semantics:
  - callback execution timestamp remains runtime `now()`;
  - optional metadata may carry `latest_scheduled_deadline_ns` for diagnostics.

### 4.6.2 `FastFailBusy` for RPC request

- On admission reject, service-side handling does not execute user callback.
- The server sends an explicit error reply when protocol path supports it (`Busy` class error), instead of silent drop.
- If explicit error reply cannot be emitted due to transport/context constraints, behavior degrades to no-reply and client observes timeout; this fallback must be counted as `rpc_busy_no_reply_count`.
- Client-observable outcome priority:
  1. explicit Busy error reply
  2. timeout fallback (counted and logged rate-limited)

## 5. Admission Model

## 5.1 Event envelope

Each ingressed event carries:

- event class
- priority
- callback-group metadata
- optional estimated memory cost (`estimated_bytes`, phased)
- enqueue timestamp

Hot-path rule:

- Ingress path should perform one bounded enqueue into class ingress.
- Executor should drain class ingress directly.
- Avoid per-event enqueue/dequeue relay between ingress and scheduler.

## 5.2 Memory budget

Each executor has:

- `max_queued_events` (count bound, mandatory in Phase A)
- `max_queued_bytes` (byte bound, optional in Phase B+)

Current Core implementation (2026-02-11):

- `AORTA_EXECUTOR_MAX_QUEUED_EVENTS` enables global queued-event guard (`0` = disabled)
- `AORTA_EXECUTOR_MAX_QUEUED_BYTES` enables global queued-byte guard (`0` = disabled)
- admission counters track `queued/peak/drop_reason` in `EventQueue` stats

Phased behavior:

- Phase A: enforce count bound only (lower overhead, deterministic)
- Phase B+: optionally enforce byte budget for selected classes (`sub_data` first)

When active limits are exceeded, overflow policy is applied.

## 5.3 Subscriber relay mode

For `sub_data`, optional `Relay` mode is supported:

- bounded overflow relay queue
- single relay task per subscriber stream
- no per-event spawned relay tasks

This preserves callback-group path consistency while preventing runtime task explosion.

## 5.4 Per-subscription QoS layering (P0)

Class-level admission does not replace per-subscription controls.
Admission stack is:

1. class-level ingress guard (bounded queue + class overflow policy)
2. per-subscription quota/policy (existing `priority`, `queue_depth`, overflow behaviors)
3. optional global executor budget (`max_queued_events`, `max_queued_bytes`)

For `sub_data`, per-subscription settings remain authoritative for local stream behavior; class-level policy only prevents cross-class interference.

## 6. Scheduler Model

Scheduler remains priority-lane based and compatible with existing architecture:

1. select highest non-empty priority lane (`Realtime` to `Background`)
2. inside that lane, select event by class-aware fairness (bounded WDRR/deficit)
3. dispatch to execution backend with callback-group lock enforcement

This means class fairness cannot dominate priority main order.

Recommended lane-internal class weights (tunable defaults):

- `control = 8`
- `rpc_request = 6`
- `rpc_response = 6`
- `timer = 4`
- `sub_data = 2`

## 7. Executor-Specific Execution Backends

## 7.1 SingleThreadedExecutor

- same admission + scheduling pipeline
- one execution lane
- callback-group lock still applied for semantic consistency

## 7.2 MultiThreadedExecutor

- same admission + scheduling pipeline
- fixed worker concurrency (`worker_threads`)
- bounded in-flight task count
- callback-group lock acquired in worker execution stage

## 7.3 CurrentThreadExecutor

- reuses the same admission and priority-lane scheduler contract as single-thread mode
- executes callbacks on the caller thread
- preserves identical overflow and callback-group semantics

## 7.4 Future AsyncExecutor

- reuse the same admission + scheduler core
- execution backend dispatches async callbacks/futures
- bounded in-flight futures via semaphore
- no change to ingress/backpressure semantics

## 8. Zenoh Integration Rules

Zenoh callback handlers are treated as strict hot path:

- must not block on `await send()`
- should avoid heavy parsing and network operations
- should only build envelope + `try_send` to ingress

Slow-path work happens after scheduler admission.

## 9. API Surface Alignment

## 9.1 Core options (target shape)

- `ExecutorAdmissionOptions` for global bounds and class capacities
- class-level overflow policy configuration
- existing subscriber overflow options map to `sub_data` class policy

## 9.2 FFI options (target shape)

Add executor-level admission options struct and class policy enum.
Preserve existing subscriber overflow fields for backward mapping during migration.

Proposed C-ABI extension pattern (versioned, additive):

```c
typedef enum {
  AORTA_INGRESS_CLASS_SUB_DATA = 0,
  AORTA_INGRESS_CLASS_RPC_REQUEST = 1,
  AORTA_INGRESS_CLASS_RPC_RESPONSE = 2,
  AORTA_INGRESS_CLASS_TIMER = 3,
  AORTA_INGRESS_CLASS_CONTROL = 4,
} aorta_ingress_class_t;

typedef enum {
  AORTA_OVERFLOW_DROP = 0,
  AORTA_OVERFLOW_RELAY = 1,
  AORTA_OVERFLOW_FAST_FAIL_BUSY = 2,
  AORTA_OVERFLOW_BLOCK = 3,
  AORTA_OVERFLOW_COALESCE_LATEST = 4,
} aorta_overflow_policy_t;

typedef struct {
  uint32_t capacity;
  aorta_overflow_policy_t overflow_policy;
  uint32_t block_timeout_ms;   // only for BLOCK
} aorta_ingress_class_options_t;

typedef struct {
  uint32_t max_queued_events;
  uint64_t max_queued_bytes;   // 0 = disabled in count-only phase
  aorta_ingress_class_options_t class_opts[5];
} aorta_executor_admission_options_t;

typedef struct {
  aorta_scheduler_mode_t scheduler_mode;
  const aorta_priority_scheduler_options_t* priority;
  const aorta_executor_admission_options_t* admission; // optional v2 extension
} aorta_executor_options_v2_t;
```

Versioning rule:

- keep `aorta_executor_options_t` source-compatible
- add `_v2` constructor path first
- allow SDK wrappers to map old fields into new defaults

## 9.3 Rust/C++ SDK options (target shape)

- expose executor admission config directly
- keep per-entity knobs when useful
- document precedence: entity override > executor class default

## 9.4 Current implementation status (2026-02-17)

Delivered surfaces:

- Core:
  - `EventQueue::admission_stats()` snapshot counters
  - global budget guards via `AORTA_EXECUTOR_MAX_QUEUED_EVENTS` and `AORTA_EXECUTOR_MAX_QUEUED_BYTES`
- FFI:
  - `aorta_executor_get_admission_stats(...)`
  - `aorta_executor_admission_stats_t` exposed in generated header
- Rust SDK:
  - `SingleThreadedExecutor::admission_stats()`
  - `MultiThreadedExecutor::admission_stats()`
- C++ SDK:
  - `Executor::GetAdmissionStats() -> StatusOr<ExecutorAdmissionStats>`

Implemented class-specific overflow controls:

- Subscriber: `Drop` / `Relay`
- RPC service ingress: `FastFailBusy` / `Block(timeout)`
- Timer ingress: `CoalesceLatest` / `Block(timeout)`

Scope note:

- Cross-language admission counters and memory guardrails are available.
- Cross-language explicit executor admission overrides for global budgets are available
  via `ExecutorOptions.admission` / `aorta_executor_options_t.admission`.
- Cross-language executor class-policy defaults are available via
  `ExecutorOptions.class_policy` / `aorta_executor_options_t.class_policy`
  for `sub/rpc/timer` overflow defaults.
- Compatibility mapping helpers are unified in Core and reused by FFI/Rust paths:
  - untouched entity defaults inherit executor class policy;
  - legacy timeout-only config maps to `Block(timeout)` for RPC/Timer.
- FFI `AORTA_EXECUTOR_ASYNC` now uses an async-compatible backend that reuses
  the same multi-thread admission/scheduler core with bounded in-flight tasks
  (`AORTA_ASYNC_EXECUTOR_MAX_IN_FLIGHT`, default `32`).
- Full class-policy unification (`_v2` shape in this spec) remains tracked as Phase C (§12).

## 10. Observability Requirements

Per executor:

- `ingress_queue_len{class}`
- `ingress_drop_count{class,reason}`
- `relay_queue_len{subscriber}`
- `fast_fail_busy_count{rpc}`
- `timer_coalesce_count`
- `admission_wait_ns` (p50/p95/p99)
- `callback_group_wait_ns` (p50/p95/p99)
- `dispatch_latency_ns` (p50/p95/p99)
- `queued_bytes`

All warnings for drops/busy/coalescing must be rate-limited.

Current Core baseline counters:

- `queued_events`, `queued_bytes`
- `peak_queued_events`, `peak_queued_bytes`
- `dropped_queue_full`, `dropped_queue_closed`
- `dropped_budget_events`, `dropped_budget_bytes`

## 11. Correctness and Resource Invariants

The implementation must satisfy:

- no unbounded queue growth in executor paths
- no per-event task spawning on overflow paths
- callback-group lock is never bypassed by fallback path
- RPC requests are never silently dropped under default policy
- timer overload does not create unbounded queued timer backlog
- class admission does not violate priority-first scheduling contract

Priority-inversion note:

- callback-group lock contention may still cause priority inversion
- multi-ingress admission must not worsen this by forcing low-priority class events to bypass higher-priority queued events
- PI-mutex/priority-aware locking remains an orthogonal executor lock policy topic and should be evaluated with the same benchmark suite

## 12. Rollout Plan

Phase A:

- introduce multi-ingress internals behind feature flag
- keep existing external defaults

Phase B:

- enable class-specific overflow policy for RPC/timer
- add observability counters and rate-limited diagnostics

Phase C:

- unify single-thread/multi-thread backends on same admission core
- add async backend using same scheduler contract

Phase D:

- remove deprecated single-ingress-only code paths
- document final stable API across Rust/C++/FFI

## 13. Validation and Benchmarks

Mandatory validation:

- mixed-load stress: high-rate subscriber + RPC latency + timer jitter
- sustained overload: memory usage plateau with bounded queues
- callback-group correctness under overload
- cross-language parity (Core/FFI/Rust/C++)

Acceptance targets (initial):

- no monotonic memory growth in 30-minute overload run
- RPC timeout/error semantics stable under subscriber flood
- subscriber drop behavior matches configured policy
- timer coalescing visible and bounded

## 14. Compatibility Notes

This plan allows controlled breaking changes because Aorta is pre-production.
However, migration notes must be provided when:

- default overflow policy for RPC/timer changes
- executor options are extended with admission/class policy fields
- old single-ingress-specific options are removed

## 15. Implementation Work Breakdown

## 15.1 Milestone M1: Core multi-ingress admission skeleton

Deliverables:

- introduce ingress classes (`sub_data/rpc_request/rpc_response/timer/control`) as bounded channels
- introduce `ExecutorAdmissionOptions` with:
  - class capacities
  - `max_queued_events`
  - `max_queued_bytes`
- route existing producer paths to class ingress without external API break
- executor consumes class ingress directly (no bridge queue between class ingress and scheduler)
- treat existing subscriber `Drop|Relay` overflow implementation as migration baseline:
  - migrate from subscriber-local fallback wiring to executor-scoped `sub_data` ingress admission
  - do not reintroduce per-event spawn relay behavior

Acceptance:

- existing FIFO/priority behavior remains functionally equivalent under default config
- no unbounded queue in executor hot paths

## 15.2 Milestone M2: Class-specific overflow policies

Deliverables:

- `sub_data`: `Drop | Relay` (bounded relay + single relay task)
- `rpc_request`: `FastFailBusy | Block(timeout)`
- `rpc_response`: `Block(timeout)` with bounded fallback handling
- `timer`: `CoalesceLatest | Block(timeout)`
- rate-limited diagnostics for all non-success admission outcomes

Acceptance:

- overload on one class does not force equivalent loss mode on other classes
- callback-group semantics stay unchanged under overload

## 15.3 Milestone M3: Unified scheduler admission arbitration

Deliverables:

- priority-lane main scheduler (architecture-compatible)
- lane-internal class-aware fairness arbitration
- tunable class weights and drain budget

Acceptance:

- no starvation in mixed load long-run test
- expected latency ordering by priority remains dominant and observable in metrics

## 15.4 Milestone M4: API propagation (FFI + Rust + C++)

Deliverables:

- C-ABI structs/enums for admission options and class policy
- Rust SDK and C++ SDK wrappers for executor admission config
- compatibility mapping from legacy per-entity options

Acceptance:

- same semantic behavior reachable from Core/FFI/Rust/C++
- no semantic drift in option precedence

## 15.5 Milestone M5: Async executor compatibility layer

Deliverables:

- share admission + scheduling core with async execution backend
- bounded in-flight async futures
- async-specific telemetry (`inflight_futures`, `poll_budget_exhausted`)

Acceptance:

- async backend uses identical ingress and overflow semantics
- no unbounded task/future growth under stress

## 16. Test and Benchmark Matrix

## 16.1 Core correctness tests

- mixed ingress ordering and fairness (class + priority)
- callback-group mutual exclusion under multi-thread overload
- overflow policy correctness per class
- admission memory budget hard-limit behavior

## 16.2 Cross-language contract tests

- FFI options roundtrip (C <-> Core)
- Rust SDK and C++ SDK parity for admission config
- matrix tests with strict RPC + high-rate subscriber coexistence

## 16.3 Performance and stability tests

- subscriber flood + RPC latency (p50/p95/p99)
- timer jitter under subscriber overload
- 30-minute soak with bounded RSS and queue depth plateau
- relay mode stress (no task explosion)

## 17. Recommended Execution Order for Current Branch

1. Finish M1 internals under feature gate.
2. Complete M2 for RPC and timer (subscriber already partially landed).
3. Land M3 arbitration and counters.
4. Expose M4 in FFI/Rust/C++.
5. Enable M1+M2 by default after benchmark gate passes.
