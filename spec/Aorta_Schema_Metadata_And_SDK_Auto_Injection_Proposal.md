# Aorta Schema Metadata & SDK Auto-Injection Proposal

Last updated: 2026-01-21

## 1. Summary
This proposal defines a **compile-time embedding + schema metadata** approach to guarantee **automatic BFBS registration** and **SDK-only AortaHeader injection**. The core runtime remains schema-agnostic: it **registers schemas and logs warnings**, but **never modifies payloads**. All schema hashes are **full SHA-256** (32 bytes raw; 64-byte lowercase hex as text). **V1 attachment uses 64-byte hex**; FB Attachment is planned for later.

## 2. Goals
- **Automatic BFBS registration** without user manual input (required by CLI/record/play).
- **Deterministic schema identification**: SHA-256 digest of BFBS, with **hex (64 bytes)** for keys/logs.
- **SDK injects AortaHeader**; core does **not** depend on message FBS.
- **No core-side reflection** or per-schema code inclusion.
- **Publisher publish path remains non-blocking**; only warn on missing/invalid schema metadata.

## 3. Non-goals
- Backward compatibility with **short/partial hashes**.
- Core-level payload rewriting or schema reflection.
- Runtime fetching of BFBS from external sources (network, file) in core.

## 4. Schema Hash & Attachment Contract
**SchemaHash**
- Raw bytes: 32 bytes (SHA-256 digest of BFBS).
- Hex representation: **64 bytes ASCII, lowercase**.

**Attachment**
- **V1**: content is **exactly 64 bytes of lowercase hex**, no header/prefix.
- **Future**: AORT + version + FlatBuffers payload (`AortaAttachment`) carrying raw 32-byte hash.
- Attached based on `SchemaAttachPolicy` (default follows Node policy).
- Core does not enforce, only logs at publisher registration when hash/metadata is missing.

## 5. Compile-Time Metadata Model
Each message schema exports a **SchemaMeta** package generated at build time.

**Required fields**:
- `schema_kind` (Topic / Req / Resp).
- `hash_hex` (64-byte lowercase hex string; used for keys/logs).
- `bfbs` (embedded bytes, exact output of `flatc --binary --schema`).
- `has_aorta_header` (bool; computed during generation or via a small build-time probe).

**Optional fields**:
- `schema_name` or `type_name` for diagnostics.
- `bfbs_sha256_bytes` (32 bytes) for faster comparisons.

## 6. Build Integration (Bazel-first)
Introduce/extend `aorta_fbs_library` to produce:
1. Language bindings (Rust/C++/Python).
2. `.bfbs` binary schema.
3. Generated metadata artifacts:
   - Rust: `*_schema_meta.rs` + `include_bytes!("*.bfbs")`.
   - C++: `*_schema_meta.h` with `constexpr std::array<uint8_t, N>`.
   - Python: `*_schema_meta.py` or package data with bytes + constants.

**Pipeline**:
- `flatc --binary --schema` → `.bfbs`
- Compute SHA-256 → lowercase hex
- Generate metadata → embedded into SDK builds

## 7. Runtime Registration Flow
**Publisher creation (SDK)**:
1. SDK retrieves `SchemaMeta` from generated module/trait.
2. SDK calls core API: `register_schema_for_topic(meta)`.
3. Core registers `schema_by_key` KV entry and caches locally.
4. If bfbs is missing or `has_aorta_header == false`, core **logs warning**.
5. Publisher creation succeeds; publish is not blocked.

**Why log-only?**
- Enforces user policy without breaking runtime data flow.
- Supports incremental adoption and debugging.

## 8. AortaHeader Injection (SDK-only)
- SDK builds payload with `AortaHeader` embedded into message root.
- Header fields (timestamp, seq, node id, frame id, etc.) are injected **before publish**.
- For SHM, SDK writes header directly into SHM payload.
- Core **does not inspect or mutate payloads**, keeping it independent of schema definitions.

## 9. SDK API Sketch
**Rust SDK**
- `trait SchemaMeta { const HASH_HEX: &'static str; const BFBS: &'static [u8]; const HAS_AORTA_HEADER: bool; }`
- `Publisher::new<T: SchemaMeta>(...)` → auto-register + attach per policy + auto-inject.
- `publish_raw(...)` allowed but logs “missing header/hash”.

**C++ SDK**
- `template<typename T> struct SchemaMeta;` generated per message.
- `Publisher<T>` auto-registers and injects header.
- `PublisherRaw` for bytes-only with log warnings.

**Python SDK**
- Generated module exports `BFBS` bytes and `SCHEMA_HASH_HEX`.
- Publisher auto-registers and injects header from Python builder helpers.

## 10. Tools & CLI Expectations
- Recorder/Player/CLI use `schema_by_key` to decode.
- Missing bfbs → tool logs and stores raw payload.
- Full SHA-256 hex is treated as authoritative identifier.

## 11. Compatibility & Migration
- **Short hash** inputs are rejected by SDK/core parsing.
- Existing topics must be rebuilt with new metadata to publish compliant frames.
- Conflicting `schema_by_key` registrations are logged; publishing continues.

## 12. Testing Strategy
- Unit tests: hash computation, hex parse/format, attachment encoding.
- Integration tests: talker/listener + schema_by_key fetch + tool decode.
- Negative tests: missing bfbs, header absent in bfbs, invalid hex.

## 13. Risks & Mitigations
- **Binary size** increase (bfbs embedded): mitigated by per-target stripping or optional build flags.
- **Duplication across languages**: mitigated by centralized generator in Bazel rule.
- **Build time cost**: mitigated by caching and incremental builds.

## 14. Implementation Plan (High Level)
1. **Rust SDK**: finalize `SchemaMeta` derive + auto registration + header injection.
2. **C++ SDK**: introduce metadata macros + auto registration.
3. **Python SDK**: package BFBS bytes + metadata constants.
4. **Tooling**: enforce schema_by_key usage in recorder/CLI.

---
**Decision**: SDKs are responsible for AortaHeader injection and attachment per policy. Core only registers schemas and logs warnings, maintaining schema-agnostic design.
