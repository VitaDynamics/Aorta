# Aorta Publisher Builder Management Design

**Document Version**: 1.2  
**Date**: 2026-01-29  
**Status**: Draft (V1 scope)  
**Authors**: Development Team

---

## 1. Problem Statement

The current publish API exposes multiple tiers and puts builder lifecycle on the user. This causes:

- Cognitive overhead (multiple APIs with unclear guidance)
- Verbose boilerplate in timer callbacks (`Arc<Mutex<FlatBufferBuilder>>`)
- Easy misuse of builder capacity and header injection
- Inconsistent usage across languages

V1 should provide a **single default publish API** with **auto-managed builder**, while still keeping an expert API for users who want to manage their own builder.

---

## 2. Design Goals

1. **Single sync publish API** with fire-and-forget semantics
2. **Auto-managed builder** by default (per Publisher)
3. **Capacity provided at Publisher creation** (`max_message_size`, optional but recommended)
4. **Predictable oversize behavior** (auto-grow, warn, and keep)
5. **Auto header injection** on builder-based API, no extra copy
6. **No API bloat**: observability via stats, not publish return values
7. **Cross-language consistency** (Rust/C++/Python/FFI)

---

## 3. API Surface

### 3.1 Core Publisher (bytes path)

```rust
impl Publisher {
    /// Sync fire-and-forget publish.
    /// No header injection, raw bytes are sent as-is.
    pub fn publish(&self, payload: impl Into<ZBytes>) -> Result<()>;
}
```

### 3.2 SDK Typed Publisher (builder path + bytes path)

```rust
impl<M: SchemaMeta + HeaderInjector> Publisher<M> {
    /// Build and publish with auto-managed builder.
    /// Header is auto-injected by generated code.
    pub fn publish<F>(&self, build: F) -> Result<()>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>);

    /// Publish raw bytes (no header injection, no schema checks).
    /// Vec avoids extra copy; &[u8] will be copied into a new Vec.
    pub fn publish_bytes(&self, payload: impl Into<Vec<u8>>) -> Result<()>;

    /// Advanced: user-managed builder (expert only).
    /// Header is auto-injected by generated code, builder lifecycle is user-managed.
    pub fn publish_with_builder<F>(
        &self,
        builder: &mut FlatBufferBuilder<'_>,
        build: F,
    ) -> Result<()>
    where
        for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>);
}
```

### 3.3 Cross-Language Consistency

- C++ and Python expose the same default `publish()` behavior.
- `publish_bytes()` is available in all SDKs.
- `publish_with_builder()` is optional and explicitly marked as expert usage.

---

## 4. Builder Management (V1)

### 4.1 Capacity Source

**Priority order**:

1. `PublisherOptions.max_message_size` (provided at Publisher creation)
2. Codegen fixed-size (only scalars/structs)
3. Fallback default: **4 KB** (variable-size or unknown)

Fixed-size messages use their compile-time size. For variable-size types, `max_message_size`
is **recommended**. If missing, use the default size and log a warning at Publisher creation.

### 4.2 PublisherOptions (V1)

```rust
pub struct PublisherOptions {
    /// Max expected payload size in bytes.
    /// Used as initial builder capacity and for diagnostics.
    pub max_message_size: Option<usize>,

    /// Builder growth policy when payload exceeds capacity.
    /// V1 supports GrowAndWarn only.
    pub builder_growth_policy: BuilderGrowthPolicy,

    /// Builder reuse policy for auto-managed builder.
    pub builder_reuse_policy: BuilderReusePolicy,
}

pub enum BuilderGrowthPolicy {
    /// Grow builder, keep new capacity, and log once per interval (default).
    GrowAndWarn,
}

pub enum BuilderReusePolicy {
    /// Per-thread cached builder (default).
    ThreadLocal,
    /// Single builder protected by mutex.
    Mutex,
    /// No reuse, allocate per publish.
    None,
}
```

### 4.3 Oversize Behavior (GrowAndWarn)

When payload size exceeds current builder capacity:

1. Publish succeeds by auto-growing the builder.
2. A **rate-limited warning** is logged (`oversize` warning, default interval **1s**).
3. `oversize_count` is incremented.
4. Builder **keeps the new capacity** for subsequent publishes.

### 4.4 Thread-Local Reuse and Re-entrancy

Thread-local reuse avoids locks but is **not re-entrant**. If the same thread calls `publish()` again before the previous build finishes, the same builder is reused and will cause borrow conflicts or deadlock.

**V1 handling**:

- Detect re-entrant publish on the same thread
- Use a **temporary builder** for the nested call
- Increment `reentrant_count` and emit a rate-limited warning (default interval **1s**)

### 4.5 TLS Memory Amplification Mitigation

Thread-local reuse can amplify memory when a large payload causes the builder
capacity to grow. To avoid N-threads * large-capacity usage, V1 applies a
fallback rule:

- If a thread-local builder grows beyond `TLS_LARGE_CAPACITY_THRESHOLD`
  (default **256 KB**; override via env, e.g. `AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB`),
  the Publisher switches to a **shared mutex builder**
  for subsequent publishes.
- The shared builder holds the large capacity once, avoiding per-thread
  duplication.
- Once switched to mutex mode, the Publisher does **not** fall back to TLS.

This keeps the fast path for small/medium messages and limits memory blow-up
for occasional large payloads.

This avoids deadlocks/panics while keeping the default path fast.

---

## 5. Header Injection

- **Builder-based publish** auto-injects `AortaHeader` during build.
- **publish_bytes** does **no header injection** and does not parse payload.
- This keeps raw publish fast and predictable.
  - Rust `publish_bytes()` accepts payloads convertible to `ZBytes`;
    use `Vec<u8>` (move) or `bytes::Bytes` (shared) to avoid extra copy.

If a user requires header injection, they must use the builder-based API.

**Schema attachment policy**:
- Attachment (schema hash) is decided **at publisher creation** via `PublisherOptions.schema_attach_policy`
  and registered schema metadata.
- Publish calls do **not** accept per-call schema hash overrides (no `publish_bytes_with_schema`).

---

## 6. Observability (Stats)

### 6.1 PublisherStats (V1)

```rust
pub struct PublisherStats {
    pub publish_count: u64,
    pub oversize_count: u64,
    pub reentrant_count: u64,
    pub last_oversize_size: usize,
    pub current_capacity: usize,
    pub max_capacity_seen: usize,
    pub send_error_count: u64,
    pub queue_len: u32,
}
```

### 6.2 Accessors

- Rust: `publisher.stats()` (queue_len = current internal send queue size; best-effort)
- C++: `publisher.stats()`
- Python: `publisher.stats()`
- FFI: `aorta_publisher_get_stats(pub, &mut out)`

Stats are optional and can be disabled via build flag or `PublisherOptions`.

---

## 7. Performance Notes

| Reuse Policy | Locking | Memory | Notes |
| --- | --- | --- | --- |
| ThreadLocal (default) | None | Higher | Best throughput; one builder per thread per Publisher |
| Mutex | Lock | Lowest | Simple, predictable; risk of lock contention |
| None | None | Lowest | Worst CPU; allocates each publish |

Oversize growth with `GrowAndWarn` keeps the larger capacity to avoid repeated reallocations.

---

## 8. Migration Notes

- Default `publish()` remains, but now uses auto-managed builder.
- Existing `publish_with_builder()` can remain as expert API.
- Oversize behavior is explicit: **grow + warn + keep**.

---

## 9. Open Questions

None.
