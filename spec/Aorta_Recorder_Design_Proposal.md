# Aorta Recorder Design Proposal

Last updated: 2026-02-18

## 1. Summary

This document defines the implementation design for Aorta MCAP recording.

Primary goals:

- Single implementation reused by both `aorta-record` and `aorta record`.
- Low-overhead recording path (no payload decoding on the hot path).
- Topic selection via Zenoh KeyExpr include rules plus blacklist.
- Deterministic schema lookup and raw fallback without blocking recording.
- Forward-compatible configuration for compression/rotation features.

## 2. Confirmed Decisions

1. Provide standalone binary `aorta-record`.
2. Provide `aorta record ...` in `aorta-cli`, reusing the same recorder library code.
3. `rpc_record` is disabled by default in MVP.
4. If schema is unavailable, recorder still writes payload as raw channel.
5. Topic matching is suffix-oriented in MVP.
6. Do not implement a regex-mode subscription model.
7. Logging configuration only exposes log level.

## 3. Scope

### 3.1 In Scope (Recorder MVP)

- Record topic data stream to MCAP.
- Optional RPC record stream capture (off by default).
- Schema resolution with explicit by-hash/by-key fallback order.
- KeyExpr include + blacklist filtering.
- Batched writer and bounded queue.
- Config file support (YAML/JSON/TOML).
- Single-group recording per process.

### 3.2 Out of Scope (MVP)

- MCAP player/replay.
- Runtime payload decode to JSON.
- Multi-group recording in one process.
- Full implementation of compression/rotation policies (fields reserved for forward compatibility).

## 4. Architecture

### 4.1 Single Implementation, Dual Entry

- Main crate: `tools/aorta-record` (library + `aorta-record` binary).
- `aorta-cli` adds `record` subcommand and calls `aorta_record::run(config)`.

All recording behavior is implemented once in the recorder library.

### 4.2 Data Path

1. Ingress receives Zenoh `Sample`.
2. Fast-path extraction only:
   - key expression,
   - timestamp,
   - payload bytes,
   - optional schema hash from attachment.
3. Schema resolver performs cache lookup and network query only when needed.
4. Event is pushed to a bounded queue.
5. Dedicated writer task batches events and writes MCAP records.

No FlatBuffers payload decoding or payload re-serialization occurs on the hot path.

## 5. Topic Selection Model

### 5.1 Why no regex mode

Regex-first filtering tends to require broad wildcard subscription and local post-filtering, increasing unnecessary traffic and CPU cost.

Recorder uses Zenoh KeyExpr selection directly, then applies blacklist as guard.

### 5.2 Include + Blacklist Rules

- Include rules are suffix-oriented KeyExpr patterns over topic suffix (for example `demo/**`).
- Include rules are converted to full subscription keys: `aorta/{group}/pub/{pattern}`.
- Blacklist checks are applied before enqueueing events.

### 5.3 Effective Subscription Strategy

1. If include list is empty, subscribe `aorta/{group}/pub/**`.
2. If include list is provided, subscribe only derived include key expressions.
3. If include list contains only exact suffix topics, subscribe one exact key per topic for minimum fan-in.
4. Apply blacklist filters before queue push.

### 5.4 Group Model

MVP supports one group per process.  
For multi-group recording, run one recorder process per group.

## 6. Schema Resolution Strategy

Schema hash format is full SHA-256 lowercase hex (64 characters).

Resolution order:

1. If sample carries attachment schema hash:
   - query by hash: `aorta/sys/schema/{hash_hex}`.
2. If by-hash is unavailable or no attachment hash:
   - query by key: `aorta/{group}/sys/schema_by_key/{key_suffix}`.
3. If both fail:
   - write payload to raw channel,
   - emit rate-limited warning.

Cache is maintained with two indexes:

- `by_hash(hash_hex) -> bfbs`
- `by_key(key_suffix) -> {hash_hex, bfbs}`

## 7. MCAP Writing and File Integrity

### 7.1 Channel Mapping

- Typed channel key: `(aorta_key, schema_hash_hex64)`.
- Raw channel key: `(aorta_key, raw)`.
- For typed channels, MCAP schema name should be the FlatBuffers root type name from BFBS
  (for example `demo.Chat`) for reader compatibility.

If schema is discovered later, subsequent messages switch to typed channel; existing raw records are preserved.

### 7.2 Batch, Flush, Backpressure

Writer uses:

- `batch_max_messages`
- `batch_max_bytes`
- `flush_interval_ms`

Queue backpressure policy:

- `block` (default): producer waits.
- `drop_newest`: drop new event and increment drop counter.

### 7.3 MCAP Conventions

Recorder writes:

- Schema/channel records for typed streams.
- Message records for all accepted events.
- Metadata record including recorder/runtime information.

Recommended metadata keys:

- `aorta.profile = aorta`
- `aorta.group`
- `aorta.recorder_version`
- `aorta.started_at_ns`
- `aorta.ended_at_ns`
- `aorta.rpc_record_enabled`

Channel conventions:

- `message_encoding = flatbuffer` for typed channels.
- `schema.encoding = flatbuffer`.
- Raw channel uses `message_encoding = aorta_raw_bytes` (only for genuinely
  schema-less streams that fail to resolve / parse).
- Context (`ctx/**`) channels are typed `flatbuffer`: every context value is the
  single static `aorta.ctx.ContextValue` schema, so the recorder embeds its
  committed BFBS directly (no runtime resolution — ctx puts carry no
  schema-hash attachment). This is required so MCAP readers such as Foxglove
  parse context values instead of rejecting an `aorta_raw_bytes` channel with
  "Unsupported encoding". The t0 wildcard-GET snapshot and later live puts share
  one typed channel (byte-identical stream). A ctx payload that does not parse
  as a `ContextValue` falls back to the raw channel rather than being mislabeled.

### 7.4 Graceful Shutdown

On `SIGINT`/`SIGTERM`:

1. Stop accepting new ingress events.
2. Drain bounded queue until empty or shutdown timeout.
3. Execute final flush.
4. Close MCAP writer cleanly so footer/summary remain readable.
5. Print final counters summary.

If shutdown timeout is reached, recorder logs forced-stop warning and exits with non-zero status.

## 8. RPC Record Data Model

When `rpc_record` is enabled, recorder subscribes to:

- `aorta/{group}/sys/record/srv/**`

MVP behavior:

- Record RPC record messages as-is (same no-decode rule).
- Use distinct MCAP channels under service record key path.
- Preserve request/reply payload and timestamps exactly as received.

Request/reply correlation details remain owned by the upstream RPC record payload format; recorder does not reconstruct call graph in MVP.

## 9. Configuration Model

### 9.1 File Formats

- YAML (`.yaml`, `.yml`)
- JSON (`.json`)
- TOML (`.toml`)

### 9.2 Precedence

`CLI args > environment variables > config file > defaults`

### 9.3 Config Schema (v1)

```yaml
version: 1
group: default

zenoh:
  config_file: null       # optional; equivalent to --zenoh-config or env-based config
                          #   without it, resolution matches aorta-cli/aorta-tui
                          #   (env config, else the local default)
  client_mode_override: true   # force mode=client on the resolved config, the
                               #   same observer-tool default as aorta-cli/tui
                               #   (CLI: --no-client-override)

record:
  output: /tmp/aorta.mcap
  rpc_record: false
  duration_sec: null
  max_messages: null             # hard cap on total recorded messages; the t0
                                 #   ctx snapshot counts toward it, so the run
                                 #   stops at t0 if the snapshot already hits it
  ctx_snapshot: true             # capture current ctx/** values at t0 via one
                                 #   wildcard GET (also CLI --no-ctx-snapshot)

topics:
  include_suffix_keyexpr: []     # empty => subscribe aorta/{group}/pub/**
  blacklist_suffix_keyexpr: []
  blacklist_full_keyexpr: []

schema:
  by_hash_timeout_ms: 2000
  by_key_timeout_ms: 2000
  attachment_fallback: true
  cache_max_entries: 4096
  negative_cache_ttl_ms: 30000   # remember a not-found for this long so an
                                 #   unregistered topic costs one bounded query
                                 #   per window, not one per message;
                                 #   0 disables; a late registration is picked
                                 #   up within one window

writer:
  queue_capacity: 8192           # bounds BOTH queues: subscriber→drain ingress
                                 #   and drain→writer-thread commands
  batch_max_messages: 1024
  batch_max_bytes: 4194304
  flush_interval_ms: 200
  shutdown_timeout_ms: 5000
  drop_policy: block             # block | drop_newest
  stats_interval_ms: 10000       # periodic live stats log line (accepted/
                                 #   dropped/schema_miss/queue depths); 0 off

storage:
  compression: zstd              # none | zstd | lz4  (default: zstd)
  compression_level: fast        # fast | default | best  or numeric
                                 #   zstd: 1..=22 (aliases fast=1, default=3, best=19)
                                 #   lz4:  0..=12 (aliases fast=0, default=0, best=12)
  chunk_size: 786432             # MCAP chunk target size in bytes (768 KiB)
  chunking: true                 # false disables chunking (diagnostics)
  summary: true                  # false skips summary/index section (diagnostics)
  rotation:
    enabled: false               # reserved for later phase
    max_file_size_mb: 1024
    max_duration_sec: 3600
    max_files: 10                # future behavior: roll file and delete oldest when exceeding cap

logging:
  level: info                    # trace | debug | info | warn | error
```

Notes:

- `storage.compression` defaults to `zstd` at `fast` level. Set to `none`
  to restore the pre-Phase-C uncompressed behavior.
- `rotation` remains a forward-compatible reserved field.
- Log rate limiting is built-in and not user-configurable in MVP.

## 10. Observability

While recording, a periodic stats line (default every 10 s,
`writer.stats_interval_ms`, 0 disables) surfaces the live health the
end-of-run summary can't (drops and queue pressure were previously invisible
until shutdown):

```
Recorder stats: accepted=N dropped=N schema_miss=N ingress_queue=n/cap writer_queue=n/cap schema_queries={by_hash:N,by_key:N}
```

MCAP writing runs on a dedicated writer thread (bounded command queue): a
stalled disk shows up as `writer_queue` depth and ingress backpressure instead
of silently stalling ingestion and schema resolution.

Recorder should export final run summary counters (stdout/log):

- accepted messages
- dropped messages
- accepted bytes
- schema miss count
- by-hash lookup failures
- by-key lookup failures
- flush count
- final file size bytes

These counters are required for operation diagnostics and test assertions.

## 11. Performance Principles

1. No payload decode on hot path.
2. Reuse buffers and reduce allocations where possible.
3. Keep schema caches indexed by both hash and key.
4. Prefer targeted subscriptions from include rules.
5. Isolate writer from ingress via queue and batch writes.

## 12. Phased Delivery Plan

### 12.1 Phase A (MVP)

- `tools/aorta-record` crate and `aorta-record` binary.
- Shared `run(config)` API reused by `aorta record`.
- Topic recording path + schema resolution + raw fallback.
- Batched MCAP writer + graceful shutdown.
- Unit/integration tests for core behavior.

### 12.2 Phase B (Performance hardening)

- More aggressive include-to-subscription specialization.
- Backpressure/drop behavior hardening with stress tests.
- Throughput/CPU benchmark baseline.

### 12.3 Phase C (Advanced storage)

- Compression runtime behavior: **done**. `storage.compression` defaults
  to `zstd` at `fast` level with 768 KiB chunks; `lz4` and `none` are
  selectable via config/CLI.
- Rotation runtime behavior: still reserved.
- Re-evaluate default enablement of `rpc_record`.

## 13. Validation Checklist

- `aorta-record` and `aorta record` both work with shared implementation.
- No payload decode dependency for baseline recording.
- Schema miss does not block recording.
- Include/blacklist behavior is correct.
- Graceful shutdown keeps MCAP file readable.
- Output file is readable by standard tooling (for example Foxglove).
- Drop counters and accepted counters are reported and internally consistent.
