# Aorta Proposal: Remove Core Hot-Path Header Parsing and Move Context Strategy to SDK

Last updated: 2026-02-10  
Status: Implemented (Core/FFI/C++ strategy options landed; cross-language matrix validated; standalone FFI parser API deferred)

## 1. Background

Today, Core subscriber callback path parses `aorta_header` directly from FlatBuffers payload to populate `MessageContext.publisher_node` and `MessageContext.sequence`.

Current parsing behavior:
- Runs on every received message in Core callback hot path.
- Uses manual low-level FlatBuffers table parsing with schema-field index assumptions.
- Returns `None` on parse failure and keeps message delivery working.

This design has two issues:
- **Performance cost in hot path**: extra parse work and string allocation on every message.
- **Maintainability risk**: parser depends on schema field order/layout assumptions.

Given current product stage (no production deployment yet), breaking API changes are acceptable to improve long-term architecture and performance.

## 2. Problem Statement

Core currently mixes two concerns:
- Transport/runtime message delivery.
- Application-level metadata extraction (`publisher_node`, `sequence`) from payload schema.

For most real subscribers, schema is known at compile time and metadata extraction is an application/SDK concern, not a transport-core concern.

## 3. Goals

1. Remove header parsing from Core message callback hot path.
2. Keep parsing logic in one source of truth (no Rust/C++ duplicated parsers).
3. Enable SDK-level optional strategies:
- header context extraction
- de-duplication
- reorder detection
- source-node identification
4. Preserve default high-performance behavior for common cases.

## 4. Non-Goals

- No wire-format/envelope migration in this proposal.
- No change to publisher-side header injection behavior.
- No mandatory dynamic schema lookup at runtime callback path.

## 5. Decision Summary

### 5.1 Core Layer

Core subscriber runtime path will stop parsing `aorta_header` from payload by default.

Core `MessageContext` should be treated as transport-level metadata container:
- keep: `topic`, `receive_stamp_ns`, `priority`, `publisher_stamp_ns`
- header-derived fields (`publisher_node`, `sequence`) are no longer auto-populated by Core callback path

Breaking-change options:
1. **Preferred**: remove header-derived fields from Core context API.
2. Transitional: keep fields but always empty/`None` unless explicitly filled by SDK strategy.

### 5.2 Single Parsing Source of Truth

Header parsing implementation remains centralized in Core (library function), but is no longer called automatically by subscriber hot path.

SDKs must consume this parser through a shared boundary:
- Rust SDK: configure Core strategy options directly.
- C++ SDK: configure Core strategy options via FFI.

This keeps one parsing implementation and avoids cross-language drift.

### 5.3 SDK Strategy Layer

Add opt-in strategy pipeline at SDK layer (default off):
- `enable_header_context`
- `enable_dedup`
- `enable_reorder_check`

The strategy pipeline runs after message reception and before/around user callback.

## 6. Proposed Architecture

## 6.1 Core Parser Utility (Not Hot Path)

Introduce a Core utility module (example naming):
- `core/src/header_extract.rs`

API shape (illustrative):

```rust
pub struct ParsedHeaderContext {
    pub publisher_node: Option<String>,
    pub sequence: Option<u64>,
}

pub fn parse_header_context(payload: &[u8]) -> ParsedHeaderContext;
```

Requirements:
- Never panic on malformed bytes.
- Bounded checks for all offsets/lengths.
- Return empty context on parse failure.

## 6.2 FFI Parser API

Expose parser for C++ SDK reuse (illustrative):

Current status:
- Deferred. Current implementation centralizes parsing in Core subscriber strategy path and
  exposes strategy toggles through `aorta_sub_options_t`.
- A standalone FFI parser API remains optional future work if external C callers need parser-only access.

```c
typedef struct {
  const char* publisher_node;  // nullable
  uint64_t sequence;
  uint8_t has_sequence;
} aorta_header_context_t;

aorta_result_t aorta_parse_header_context(
    const aorta_bytes_view_t* payload,
    aorta_header_context_t* out);
```

Notes:
- Memory ownership of returned strings must be explicit and test-covered.
- API returns success with empty fields when header missing or parse fails.

## 6.3 SDK Strategy APIs (Illustrative)

Rust:
- `SubscriberOptions::with_header_context(bool)`
- `SubscriberOptions::with_dedup(bool)`
- `SubscriberOptions::with_dedup_window(usize)`
- `SubscriberOptions::with_reorder_check(bool)`
- `SubscriberOptions::with_drop_out_of_order(bool)`

C++:
- `SubscriberOptions::enable_header_context`
- `SubscriberOptions::enable_dedup`
- `SubscriberOptions::dedup_window`
- `SubscriberOptions::enable_reorder_check`
- `SubscriberOptions::drop_out_of_order`

Behavior:
- Default is disabled for all three to keep maximal throughput.
- When enabled, SDK uses centralized parser utility (Core/FFI), not local parser copies.

## 7. De-dup and Reorder Strategy Design

Key:
- `source_key = (publisher_node, topic)` when available.
- fallback key path configurable when `publisher_node` missing.

De-dup:
- Track recent sequences per source key with bounded LRU/window.
- Drop duplicate sequence values within window.

Reorder detection:
- Track last seen sequence per source key.
- Mark event/reason when `seq <= last_seq` (policy can be warn or callback flag).

All state must be bounded and configurable to avoid unbounded memory growth.

## 8. Performance Expectations

After removing Core hot-path parse:
- Lower per-message CPU cost.
- Fewer transient allocations in callback path.
- Better tail latency under high-frequency pub/sub workloads.

SDK opt-in strategy cost becomes explicit and user-controlled.

## 9. Compatibility and Migration

Breaking change scope:
- `MessageContext` semantics change across Core/Rust/C++/FFI.
- Existing code that reads `publisher_node/sequence` from default callback context must migrate.

Migration path:
1. Update code to new context contract.
2. Enable SDK header strategy where needed.
3. Enable de-dup/reorder only for streams that require it.

## 10. Test Plan

Core:
- Unit tests for parser utility with valid/missing/corrupted payloads.
- No parser invocation in default subscriber callback hot path.

FFI:
- `aorta_sub_options_t` strategy field mapping tests.
- Null/invalid options safety tests.

Rust/C++ SDK:
- Strategy off: no header parse, no dedup/reorder.
- Strategy on: header extraction works through shared parser.
- Dedup/reorder behavior tests with deterministic sequence streams.

Integration:
- Cross-language pub/sub matrix with strategy on/off combinations (`test_subscriber_strategy_matrix.sh`).

## 11. Risks and Mitigations

Risk:
- Users rely on old implicit context fields.

Mitigation:
- Clear release note and migration section in SDK guides.
- Keep temporary compatibility mode if needed (short window only).

Risk:
- Parser still fragile if schema layout changes.

Mitigation:
- Keep parser as single source and add schema-contract checks in CI.
- If schema contract evolves, update parser once at Core boundary.

## 12. Implementation Phases (Suggested)

1. Introduce/centralize parser utility in Core strategy path.
2. Remove Core hot-path parser call.
3. Add SDK strategy options and wrappers (Rust/C++).
4. Update tests/examples/docs in the same change set.
5. Add and run cross-language strategy matrix validation.

## 13. Acceptance Criteria

- Core callback hot path no longer parses payload header by default.
- Rust/C++ SDK header strategy works and reuses single parser source.
- Dedup/reorder strategies are opt-in and bounded.
- Documentation and examples updated to new behavior.
