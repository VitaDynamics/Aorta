# Aorta Executor Queue Design and Rust Queue Survey

Last updated: 2026-02-11

> Historical design note: describes the design at the time it was written; it is not a v0.1.0 instruction.

## 1. Purpose
This document captures:
- The queue design decision for Aorta executor hot path.
- A Rust queue/channel survey (including lock-free options).
- The recommended implementation path balancing throughput, latency, and engineering risk.

## 2. Workload Profile (Aorta)
Current executor path characteristics:
- Multiple producers: subscriber callbacks, timer callbacks, service handlers.
- Single consumer per `SingleThreadedExecutor` instance.
- Async runtime integration is required (Tokio-based scheduling/wakeup).
- Bounded queue + backpressure is preferred to avoid unbounded memory growth.
- Priority semantics are needed for scheduling, but should not penalize FIFO-only users.

## 3. Candidate Survey

### 3.1 `tokio::sync::mpsc`
- Type: async MPSC.
- Bounded + unbounded variants.
- Bounded channel provides backpressure.
- Internals: block-based storage (32 msgs/block on 64-bit; 16 on 32-bit), optimized for async wakeup.
- Strengths:
  - Native Tokio integration.
  - Stable semantics and mature ecosystem usage.
  - Good baseline for mixed producer/consumer async workflows.
- Risks:
  - Not a "pure lock-free queue" API abstraction.
  - Raw throughput may be outperformed by specialized lock-free channels in some synthetic benches.

### 3.2 `crossbeam-channel`
- Type: sync MPMC channel (can be used in async with wrappers, but not async-native).
- Bounded + unbounded + rendezvous.
- Docs position: alternative to `std::sync::mpsc` with better performance.
- Strengths:
  - Mature and widely used.
  - Strong synchronous channel semantics.
- Risks:
  - Async integration adds adaptation overhead (blocking/bridging/wakeup handoff).
  - Not a drop-in async hot path replacement for Tokio task scheduling.

### 3.3 `flume`
- Type: MPMC channel with sync + async APIs.
- Docs claim: faster than `std::sync::mpsc`, sometimes faster than `crossbeam-channel`.
- Strengths:
  - API ergonomics and flexible usage modes.
  - No `unsafe` in crate codebase.
- Risks:
  - Performance claims are crate self-reported and workload dependent.
  - Replacing existing queue substrate adds migration and compatibility risk.

### 3.4 `async-channel` (+ `concurrent-queue`)
- Type: async MPMC channel.
- Bounded + unbounded.
- Built on `concurrent-queue` primitives.
- Strengths:
  - Async-ready MPMC model.
  - Clean close semantics and cloneable sender/receiver.
- Risks:
  - Switching queue core in executor path still requires end-to-end validation and migration cost.

### 3.5 `crossbeam-queue` (`ArrayQueue` / `SegQueue`)
- Type: concurrent queue primitives (not full async channels).
- `ArrayQueue`: bounded MPMC, fixed-capacity allocation.
- `SegQueue`: unbounded MPMC, segment allocation; docs state it is generally slower than `ArrayQueue`.
- Strengths:
  - Good low-level building blocks.
  - Useful for custom scheduler internals.
- Risks:
  - Requires explicit wakeup/notification design in async runtime.
  - Higher implementation complexity for production-grade channel semantics.

### 3.6 `thingbuf` (lock-free ring + bounded MPSC channel)
- Type: lock-free array-based ring buffer + async/blocking bounded MPSC channels.
- Strengths:
  - Lock-free bounded MPSC design.
  - Allocation on channel creation (no per-message grow path for bounded usage).
- Risks:
  - Capacity-proportional upfront allocation can be expensive for very high bounds.
  - Adoption adds dependency and integration/test migration overhead.

## 4. Performance Evidence and Interpretation
Key observations from primary sources:
- Tokio bounded mpsc backpressure behavior and memory model are explicitly documented.
- Crossbeam/flume docs advertise strong performance vs std channels.
- Thingbuf positions itself as a high-throughput lock-free bounded MPSC option.

Important caveat:
- Cross-channel benchmark results are highly workload- and executor-dependent.
- Async benchmark authors explicitly warn about high variability and scheduler sensitivity.
- Therefore, there is no globally "best" queue without Aorta-specific benchmark profiles.

## 5. Decision for Aorta (Phase 3)
Recommended approach:

1. Keep `tokio::sync::mpsc` as ingress queue substrate for executor event intake.
2. Move priority scheduling logic to executor-local in-memory scheduler (bucketed priority queues).
3. Ensure FIFO path remains the default low-overhead mode for non-priority users.
4. Enable priority mode explicitly for scenarios that need scheduling differentiation.

Why this is the best fit now:
- Preserves async runtime integration and backpressure semantics.
- Avoids high-risk queue substrate migration during ongoing Phase 3 delivery.
- Delivers major scheduling gain where needed (priority dispatch) with controlled complexity.
- Keeps room for later A/B channel evaluation if benchmark evidence justifies migration.

## 6. High-Performance Implementation Sketch
For `SingleThreadedExecutor`:
- Add `SchedulerMode`:
  - `Fifo` (default): minimal overhead.
  - `Priority`: use 5 buckets (`Realtime/High/Normal/Low/Background`) + non-empty bitmask.
- Event priority assignment:
  - `Timer`: Realtime.
  - `RpcRequest/RpcResponse`: High.
  - `SubscriptionData`: mapped from transport/sample priority when available; fallback Normal.
- Fairness:
  - Weighted round-robin or quota-based dispatch to avoid starvation.
- Bounded intake:
  - Per-cycle `max_drain` limit to avoid long monopolization and latency spikes.

## 7. Validation Plan
- Functional:
  - Priority dominance (high before normal).
  - Same-priority FIFO.
  - Starvation prevention under sustained high-priority load.
- Performance:
  - Compare `Fifo` vs `Priority` mode at low/mid/high load.
  - Measure p50/p99 dispatch latency, throughput, and queue depth.
- Compatibility:
  - No regression for existing FIFO-only use cases.
  - Cross-language behavior remains contract-consistent.

## 8. Open Follow-ups
- Whether to introduce optional channel A/B backend benchmarking (`tokio::mpsc` vs `thingbuf`) in a non-default experiment target.
- Whether to expose scheduler mode as user-facing configuration in FFI/SDK or keep internal first.

## 9. Explicit Priority Mode API Proposal

### 9.1 Design Decision
- Priority scheduling is **explicit opt-in**.
- Default executor behavior remains `FIFO` to protect non-priority workloads.
- When priority mode is disabled, QoS priority mapping is bypassed in dispatch path.

### 9.2 Core API (Rust, `aorta_core`)

```rust
pub enum SchedulerMode {
    Fifo = 0,
    Priority = 1,
}

pub struct PrioritySchedulerOptions {
    pub max_drain_per_cycle: u32,         // default: 32
    pub weights: [u8; 5],                 // [rt, high, normal, low, bg], default: [8,4,2,1,1]
    pub map_subscription_qos: bool,       // default: true
}

pub struct ExecutorOptions {
    pub mode: SchedulerMode,              // default: Fifo
    pub priority: PrioritySchedulerOptions,
}
```

`SingleThreadedExecutor::new(capacity)` remains and is equivalent to `ExecutorOptions::default()` for compatibility.

### 9.3 FFI API (`c_api.rs` / generated header)

```c
typedef enum {
  AORTA_SCHEDULER_FIFO = 0,
  AORTA_SCHEDULER_PRIORITY = 1,
} aorta_scheduler_mode_t;

typedef struct {
  uint32_t max_drain_per_cycle;
  uint8_t weights[5];  // [rt, high, normal, low, bg]
  uint8_t map_subscription_qos;
} aorta_priority_scheduler_options_t;

typedef struct {
  aorta_scheduler_mode_t scheduler_mode;
  const aorta_priority_scheduler_options_t* priority;
} aorta_executor_options_t;

aorta_result_t aorta_executor_new(
    aorta_executor_type_t type,
    aorta_exec_t* out);

aorta_result_t aorta_executor_new_with_options(
    aorta_executor_type_t type,
    const aorta_executor_options_t* options,
    aorta_exec_t* out);
```

Behavior:
- `aorta_executor_new(type, out)` is a compatibility wrapper.
- `aorta_executor_new_with_options(..., NULL, ...)` => default FIFO.
- Invalid `map_subscription_qos` (not `0/1`) returns `AORTA_ERR_INVALID_ARGUMENT`.

### 9.4 C++ SDK API

```cpp
enum class SchedulerMode {
  kFifo,
  kPriority,
};

struct PrioritySchedulerOptions {
  uint32_t max_drain_per_cycle;
  std::array<uint8_t, 5> weights{{8, 4, 2, 1, 1}};
  bool map_subscription_qos = true;
};

struct ExecutorOptions {
  ExecutorType type = ExecutorType::kSingleThreaded;
  SchedulerMode scheduler_mode = SchedulerMode::kFifo;
  PrioritySchedulerOptions priority{};
};
```

New API:
- `Executor::Create(const ExecutorOptions& options)`
- Keep existing `Executor::Create(ExecutorType type)` for compatibility.

### 9.5 Rust SDK API
Rust SDK re-exports Core executor configuration types:
- `SchedulerMode`
- `PrioritySchedulerOptions`
- `ExecutorOptions`

Current behavior:
- SDK internal auto-executor for timer remains `FIFO` by default.
- No `Node`-level auto-executor option override API yet.

### 9.6 QoS Integration Rules
- **Yes, integrate with QoS**, but only in `Priority` mode.
- Mapping rule (subscriber ingress):
  - `QoS Priority::Realtime` -> `EventPriority::Realtime`
  - `High` -> `High`
  - `Normal` -> `Normal`
  - `Low` -> `Low`
  - `Background` -> `Background`
- Service/Timer default event priorities:
  - Timer -> `Realtime`
  - RPC request/reply -> `High`
- In `FIFO` mode:
  - Queue dispatch does not reorder by priority.
  - QoS-derived event priority may be recorded for observability but must not affect dispatch order.

### 9.7 Validation Expectations
- FIFO mode regression must show no measurable overhead increase beyond noise.
- Priority mode must pass the priority-mode checks:
  - deterministic preference
  - bounded starvation
  - fairness within same priority
## 10. Sources
- Tokio mpsc docs: https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html
- Tokio channels tutorial (backpressure): https://tokio.rs/tokio/tutorial/channels
- Tokio unbounded channel docs: https://docs.rs/tokio/latest/tokio/sync/mpsc/fn.unbounded_channel.html
- Crossbeam queue docs: https://docs.rs/crossbeam-queue
- Crossbeam channel docs: https://docs.rs/crossbeam-channel
- Async-channel docs: https://docs.rs/async-channel
- Concurrent-queue docs: https://docs.rs/concurrent-queue
- Flume docs: https://docs.rs/flume
- Thingbuf docs: https://docs.rs/thingbuf
- Tachyobench (async channel benchmark suite and caveats): https://github.com/asynchronics/tachyobench
- rust-channel-benchmarks (community benchmark corpus): https://github.com/fereidani/rust-channel-benchmarks
