# Aorta Core Header Layout Reflection Plan

Last updated: 2026-02-26

## 1. Background

Current Core header-context parsing assumes fixed FlatBuffers field positions:

- Root table `aorta_header` is at field id/index `0`.
- `AortaHeader.publisher_node` is at field id/index `1`.
- `AortaHeader.sequence` is at field id/index `2`.

This is fragile when schemas use explicit FlatBuffers `(id: N)` assignments (for example, third-party schemas such as Foxglove-derived messages). In those schemas, `aorta_header` may not be at id `0`.

## 2. Goal

Make Core header parsing robust to schema field id layout changes by:

1. Resolving header field ids from BFBS reflection metadata.
2. Caching resolved layout off the hot path.
3. Keeping message callback hot path reflection-free.
4. Preserving backward compatibility for legacy schemas.

## 3. Scope

In scope:

- Core subscriber header-context parsing path (`enable_header_context`, dedup/reorder strategy path).
- BFBS reflection-based header layout inference.
- Layout caching keyed by topic key and schema hash.
- Tests and docs.

Out of scope:

- Changing wire format.
- Changing typed publish contract (`aorta_header` remains required for typed auto injection).
- Bazel/build graph redesign.

## 4. Design

### 4.1 Header Layout Model

Introduce a Core model:

```rust
AortaHeaderLayout {
  root_aorta_header_field_id: usize,
  header_publisher_node_field_id: usize,
  header_sequence_field_id: usize,
}
```

This model stores FlatBuffers field ids (not declaration-order indices).

### 4.2 BFBS Reflection Inference

Add schema helper:

- `infer_aorta_header_layout_from_bfbs(bfbs: &[u8]) -> Option<AortaHeaderLayout>`

Inference strategy:

1. Parse BFBS schema via `flatbuffers_reflection::reflection`.
2. Find root table.
3. Find root field named `aorta_header`.
4. Resolve its referenced object type (`AortaHeader` object) from field type metadata.
5. Find fields `publisher_node` and `sequence` in that object.
6. Return their field ids.

### 4.3 Subscriber Parsing Path

Add parser variant that accepts resolved layout:

- `parse_aorta_header_fields_with_layout(payload, layout)`

The existing parser API remains and uses a legacy fallback layout (0/1/2) for compatibility.

### 4.4 Non-Hot-Path Cache Warming

When subscriber needs header parsing (header_context or dedup/reorder enabled):

1. Start schema prefetch background task at subscriber initialization.
2. Fetch schema-by-key value from registry (existing path).
3. Infer header layout from fetched BFBS once.
4. Cache layout:
   - by topic key (subscriber key expression)
   - by schema hash (for attachment hash fast path)

### 4.5 Hot Path Behavior

Per message:

1. Optional parse of attachment hash (if present).
2. Resolve layout from in-memory cache:
   - by hash first
   - then by topic key
   - else fallback layout
3. Parse payload fields using resolved ids.

No BFBS reflection on hot path.

## 5. Performance Expectations

- Reflection cost moves to initialization/background prefetch.
- Hot path adds only lightweight cache lookup + branch.
- Existing fast path with header parsing disabled remains unchanged.

## 6. Backward Compatibility

- Legacy schemas with `aorta_header` at id 0 continue to work.
- If layout cache is absent/unavailable, parser falls back to legacy layout.
- Invalid payload or missing fields still returns empty parsed context.

## 7. Test Plan

1. Schema-layer tests:
   - infer layout from BFBS with non-zero root `aorta_header` id.
   - infer fails gracefully when required fields are missing.

2. Subscriber parser tests:
   - payload with `aorta_header` at non-zero field id parses correctly with inferred/explicit layout.
   - legacy fallback still parses old payload.
   - invalid payload remains safe and returns `None` fields.

3. Regression tests:
   - dedup/reorder logic semantics unchanged except improved header extraction robustness.

## 8. Rollout Notes

- This is an internal Core behavior improvement; external API remains stable.
- No caller changes required.
- Follow-up optional optimization: topic-layout cache invalidation policy if schema hash changes frequently per topic.
