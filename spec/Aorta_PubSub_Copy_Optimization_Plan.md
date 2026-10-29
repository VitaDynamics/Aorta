# Aorta Pub/Sub Copy Optimization Plan

Last updated: 2026-02-27

## 1. Scope and Goal

This document defines an end-to-end optimization plan for Aorta pub/sub data paths with one primary objective:

- Minimize avoidable payload copies in C++ and Rust SDK pub/sub chains.

Assumptions for this plan:

- We do **not** preserve backward compatibility in API/ABI shape if a cleaner fast path is available.
- We optimize for default production path performance first.

## 2. Current Data Path and Copy Inventory

## 2.1 C++ Typed Publish (current)

Path:

1. User fills typed wrapper message (`*_msg.h`).
2. SDK serializes into `flatbuffers::FlatBufferBuilder`.
3. SDK publishes via `aorta_publisher_publish(handle, ptr, len)`.
4. FFI converts borrowed bytes to owned payload using `to_vec()`.
5. Core enqueues `ZBytes` and Zenoh sends.

Avoidable copy:

- Step 4 performs one full payload copy in FFI (`to_vec()`).

## 2.2 Rust Typed Publish (current)

Path:

1. User fills typed wrapper message from Rust wrapper crate (`*_msg`, backed by Bazel target `*_msg_rs`).
2. SDK serializes into `FlatBufferBuilder`.
3. SDK uses builder collapse + owner-backed bytes path (`Bytes::from_owner` -> `ZBytes`).
4. Core publishes `ZBytes`.

Copy status:

- No obvious extra full-payload copy after serialization.
- Path is already ownership-based and close to optimal.

## 2.3 C++ Subscribe (current)

Path:

1. Core receives sample payload as `ZBytes`.
2. FFI exports borrowed slices into `aorta_bytes_view_t`.
3. C++ SDK exposes `MessageView` (borrowed, possibly multi-slice).
4. User callback consumes `MessageView`.

Copy status:

- No payload copy by default.
- Copy happens only when user calls `MessageView::ToVector()` (explicit materialization).

## 2.4 Rust Subscribe (current)

Path:

1. Core provides borrowed `&ZBytes` to callback.
2. User may decode from payload directly or call `to_bytes()`.

Copy status:

- No payload copy by default.
- Potential copy when callback uses `to_bytes()` on fragmented payload.

## 3. Target Architecture

## 3.1 Publish Path Principle

- Use **ownership-based payload handoff** from SDK -> FFI -> Core as the default fast path.
- Keep serialization as the main unavoidable memory write; remove additional bridge copies.

## 3.2 Subscribe Path Principle

- Keep callback payload borrowed by default.
- Avoid forced payload materialization in framework-level logic.
- Materialize only on explicit user demand or unavoidable fragmented parse path.

## 4. Layer-by-Layer Optimization Plan

## 4.1 Core + FFI (P0)

1. Make owned publish path the primary FFI contract:
   - `aorta_publisher_publish_owned(...)` becomes the canonical path.
2. Remove full-copy conversion in hot publish path:
   - No `to_vec()` for normal C++ typed publish.
3. Keep zero-copy ownership transfer semantics with release callback.

Implementation note:

- Borrowed publish API can be removed or demoted to a compatibility wrapper, but no longer used by SDK fast path.

## 4.2 C++ SDK Publish (P0)

1. Change typed publish internals to owned handoff:
   - Build FlatBuffer.
   - Detach builder storage (`Release()`/equivalent ownership extraction).
   - Send via `PublishOwned(...)` with release callback.
2. Keep external typed API style simple:
   - `PublishTyped<T>(fill_fn)`
   - `PublishTypedMessage<T>(const Msg&)`
3. Ensure no extra payload memcpy between builder finish and FFI call.

## 4.3 Rust SDK Publish (P1 hardening)

1. Keep current ownership path as baseline (already strong).
2. Audit generated typed wrappers for avoidable temporary allocations (especially vectors/enum conversions).
3. Add micro-optimizations only when benchmark shows measurable gains.

## 4.4 Subscriber Hot Path (P1)

1. Avoid framework-level forced `to_bytes()` where possible.
2. Keep header-context strategy parsing borrowed-first.
3. Add helper APIs for typed decode that prefer borrowed contiguous data and only materialize when required.

## 4.5 Message Context String Overhead (P2)

1. Replace per-message C-string construction where possible with pointer+length views in FFI context.
2. Let SDK convert to owning strings only when user code needs ownership.

This is secondary to payload-copy elimination and should be done after P0/P1.

## 5. TDD Plan

Each behavior change must follow red -> green -> refactor.

## 5.1 New/Updated Tests

1. C++ publisher tests:
   - Verify typed publish uses owned FFI path (`aorta_publisher_publish_owned`) by default.
   - Verify release callback is invoked exactly once.
2. FFI tests:
   - Verify owned publish with release callbacks has no fallback memcpy path.
3. Rust SDK tests:
   - Guard existing zero-copy ownership behavior.
4. Integration tests:
   - C++ typed publisher -> Rust listener.
   - Rust typed publisher -> C++ listener.
   - Mixed payload sizes (small/medium/large).

## 5.2 Regression Focus

1. Header injection correctness (`aorta_header` fields).
2. Schema attach behavior.
3. Queue length / error counters behavior consistency.
4. Service/client path unaffected by pub/sub changes.

## 6. Performance Validation Plan

Measure before/after on identical scenarios:

1. Throughput (msgs/s).
2. CPU utilization.
3. Tail latency (p95/p99 end-to-end).
4. Allocation/memory churn.

Suggested scenarios:

1. 1 KB payload, high frequency.
2. 32 KB payload, medium frequency.
3. 256 KB payload, lower frequency.
4. Mixed-size workload.

Expected near-term result:

- C++ typed publish should remove one full payload copy per message (FFI `to_vec()` removal).

## 7. Delivery Phases

## Phase A (Immediate, P0)

1. C++ typed publish -> owned FFI fast path.
2. FFI publish hot path copy removal.
3. Unit/integration tests green.

## Phase B (P1)

1. Subscriber decode/materialization optimization.
2. Rust wrapper micro-optimization pass.
3. Perf benchmark update.
4. Add dual-track builder APIs in C++ SDK:
   - keep `PublishWithBuilder(...)` as borrowed/copy path;
   - add `PublishWithBuilderOwned(...)` as ownership fast path.
   This item is explicitly deferred from current P0 delivery.

## Phase C (P2)

1. FFI context string-view refactor.
2. C++/Rust SDK context ownership policy alignment.

## 8. Risks and Mitigations

Risk:

- Ownership/release callback bugs can cause leaks or double-free.

Mitigation:

1. Add strict release-count tests.
2. Add stress tests for repeated publish/free cycles.
3. Run ASAN/TSAN compatible checks where available.

Risk:

- Fragmented payload edge cases in subscriber decode helpers.

Mitigation:

1. Add tests for single-slice and multi-slice payloads.
2. Keep explicit fallback materialization path.

## 9. Immediate Next Step

Start Phase A implementation:

1. Rewire C++ typed publish internals to ownership publish path.
2. Ensure FFI hot path no longer performs payload `to_vec()` for typed C++ publishing.
3. Submit with test evidence and benchmark delta summary.
