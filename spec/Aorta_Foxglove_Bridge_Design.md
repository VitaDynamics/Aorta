# Aorta Foxglove Bridge — Design Proposal

Last updated: 2026-07-18 (rebased onto current `main`; post-review hardening
round: single-channel-per-topic registry, drop policy, shared core
`SchemaResolver` with negative caching, ctx/telemetry channel typing)

## 1. Purpose

Aorta currently has no equivalent of the `foxglove_bridge` package shipped by
ROS 2, which means engineers cannot point a Foxglove Studio (desktop or web)
client at a running Aorta deployment and watch live topics. The recorder
already produces MCAP files that Foxglove can open offline, but online
debugging — live point clouds, images, transforms, plots — requires a
WebSocket bridge.

This document specifies a new tool, `aorta-foxglove-bridge`, that exposes
all Aorta `pub/**` traffic in the same group as Foxglove WebSocket
channels. It is designed to reuse the existing Aorta discovery and schema
resolution machinery so the bridge stays consistent with the recorder and
the CLI.

Reference for the Foxglove side of the contract:
[Foxglove getting-started — Rust](https://docs.foxglove.dev/docs/getting-started/rust)
and [`docs.rs/foxglove`](https://docs.rs/foxglove/latest/foxglove/).

## 2. Acceptance Tiers

Calling this tool "the Aorta equivalent of `ros/src/foxglove_bridge`"
sets an expectation that the ROS 2 bridge cleared with years of work
and multiple capability flags (`clientPublish`, `parameters`,
`services`, `connectionGraph`, `assets`, TLS, sim-time broadcast, …).
This spec splits acceptance into two explicit tiers so the rollout
plan is unambiguous and so review can match implementation to scope.

### 2.1 P0 — Live Topic Viewer (current target)

P0 ships a one-way *live topic viewer*: open Foxglove Studio, point
it at the bridge, see every Aorta publisher's data flow with the
correct FlatBuffers schema attached. Concretely:

- Live topic forwarding for `aorta/{group}/pub/**`,
  `aorta/{group}/ctx/**`, and `aorta/{group}/sys/telemetry/**` (the
  recorder's default subscription set).
- FlatBuffers schema attachment via the canonical
  `aorta_core::schema_resolve::SchemaResolver`
  (`schema_encoding = "flatbuffer"`), with `aorta_raw_bytes` fallback
  for streams whose BFBS cannot be resolved. `ctx/**` values are typed
  with the static `aorta.ctx.ContextValue` schema (same fix as the
  recorder — Foxglove rejects the raw encoding).
- Admin-space pre-advertise: idle-but-declared topics appear in the
  channel list even before the first sample.
- Dynamic discovery: topics that appear after startup are picked up
  without restart; a schema-hash change triggers `close()` +
  re-advertise. A failed resolve never downgrades a typed channel
  (see §4.4).
- Configurable include / blacklist patterns and listen address.
- Graceful shutdown, bounded queue with drop policy
  (`drop_newest` by default — a live viewer prefers fresh frames;
  `block` opts into recorder-style backpressure).
- Smoke test asserts the `foxglove.sdk.v1` WebSocket handshake +
  `serverInfo` frame round-trip.

P0 is **not** a drop-in replacement for `ros/src/foxglove_bridge`. It
is a focused subset whose review surface is small and whose user value
("can I see my topic in Studio?") is large per line of code.

### 2.2 P1 — Full `foxglove_bridge` Parity (separate milestone)

P1 covers everything the ROS 2 bridge advertises in its default
capability set. Each item below is its own design + implementation
slice and should land in its own PR. The bridge announces a Foxglove
capability flag only after the corresponding slice is complete; until
then the SDK reports an empty `capabilities` array (matching today's
`serverInfo` payload), so clients see honest feature-gating instead
of silent half-implementations.

| Capability | What it adds | Aorta side that must exist first | Foxglove SDK hook |
|:---|:---|:---|:---|
| `services` | Studio can call Aorta `srv/**` services from the panel | RPC introspection + JSON↔FB conversion (already in `aorta-cli`'s C++ FFI) | `WebSocketServerHandle::add_services` |
| `parameters` | Studio can read/edit Aorta params; `parametersSubscribe` pushes updates | `aorta/{group}/prm/**` discovery + write path | `WebSocketServerHandle::publish_parameter_values` |
| `clientPublish` | Studio can publish back into Aorta (reverse direction) | Per-topic ACL story + schema-bound encode | `ServerListener::on_message_data` |
| `connectionGraph` | Studio's node graph view: who publishes / subscribes / serves what | Periodic graph snapshot from `IntrospectClient::list_nodes` + edges | `WebSocketServerHandle::publish_connection_graph` |
| `assets` | Studio can fetch URDFs, meshes, configs over the bridge | Aorta-side asset registry (none today) | `ServerListener::on_fetch_asset` |
| Time broadcast / `use_sim_time` | Studio timeline can be driven by Aorta sim clock | Aorta sim-time topic convention | `WebSocketServerHandle::broadcast_time` |
| TLS / WSS | Encrypted transport; required for any non-localhost deployment | Cert/key on disk or referenced from a secret store | `foxglove::WebSocketServer::tls(...)` (feature `tls`) |
| Status / sysinfo / `client_count` | Operator telemetry: connected clients, queue depth, schema cache hits | Internal counters already wired in `RunSummary` | `WebSocketServerHandle::publish_status`, `client_count()` |
| Remote-access gateway | Foxglove Layout Cloud reachability | New Aorta-side gateway story | `foxglove::remote_access` feature |
| QoS / latched / `tf_static` | Late-subscriber gets the last value on `RELIABLE` + `TRANSIENT_LOCAL`-equivalent streams | Aorta QoS spec (not landed) | Per-channel replay buffer in the bridge |

### 2.3 Out-of-Scope, Both Tiers

- ROS 1 / ROS 2 `.msg` / `.idl` schema translation on the wire. Aorta
  has standardized on FlatBuffers; if a downstream tool needs ROS-IDL
  schemas it should run a separate translation layer.
- Cross-group bridging in a single process. Operators that need to
  surface multiple groups run multiple bridges.

### 2.4 Naming and Rollout

- The binary name (`aorta-foxglove-bridge`) is stable across tiers.
- The README's status block declares which tier the build implements
  ("P0 — Live Topic Viewer" today), so users do not silently assume
  parity.
- Anything in §2.2 that is partially implemented must be guarded
  behind a feature flag and **must not** be announced in the
  `capabilities` array of `serverInfo` until end-to-end test coverage
  exists for that capability.

## 3. Design Principles

### 3.1 Reuse, do not fork (DRY)

The bridge must use the same discovery, key normalization, and schema
resolution path as the recorder and the CLI. Concretely:

- `aorta_core::KeyExprBuilder` for key parsing and suffix extraction.
- `aorta_core::schema_resolve::SchemaResolver` for BFBS lookup — the
  same canonical core resolver the recorder migrated to (core
  layering), preferring the per-message attachment hash and falling
  back to `schema_by_key`, with identical config knobs
  (`by_hash_timeout_ms`, `by_key_timeout_ms`, `cache_max_entries`,
  `negative_cache_ttl_ms`) so the operator's mental model carries over.
- The recorder's filter semantics verbatim: `blacklist_suffix_keyexpr`
  patterns are rooted under `pub/`, `blacklist_full_keyexpr` matches
  the full wire key (covers `ctx/**` and `sys/telemetry/**`). The same
  YAML stanza means the same thing in both tools.
- `should_resolve_schema` / ctx `ContextValue` typing policy mirrored
  from `aorta-record` (follow-up: hoist both into `aorta_core` so the
  two copies cannot drift).
- Zenoh router admin space for publisher snapshotting, following the
  pattern landed for `aorta topic list`.

Anything new (channel bookkeeping, Foxglove SDK glue) lives only in this
crate.

### 3.2 Lightweight session, not a Node

Like the CLI and the recorder, the bridge opens a raw `zenoh::Session`
and never instantiates an Aorta `Node`. It does **not** declare a
liveliness token, does **not** register schema queryables, and does not
appear in `aorta nodes list`. This keeps the bridge passive and avoids
polluting the schema-discovery space.

### 3.3 Foxglove SDK only at the edge

All Foxglove-specific types (`WebSocketServer`, `RawChannel`, `Schema`,
`PartialMetadata`) are confined to one module (`server.rs`). The rest of
the crate works with plain Aorta concepts (key suffix, BFBS, attachment
hash). Swapping the SDK or upgrading versions stays a local edit.

### 3.4 Schema encoding matches the recorder

The recorder writes MCAP with `schema_encoding = "flatbuffer"` and
embeds the raw BFBS reflection blob; the bridge uses the same string
and the same bytes. This guarantees that whatever Foxglove can render
from an Aorta MCAP recording it can also render live.

## 4. Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                    aorta-foxglove-bridge                           │
│                                                                    │
│  ┌─────────────────┐    ┌─────────────────┐    ┌────────────────┐  │
│  │ Discovery       │ →  │ Subscription    │ →  │ Foxglove       │  │
│  │ Loop            │    │ Manager         │    │ WebSocket      │  │
│  │ (admin space)   │    │ (zenoh::Subs)   │    │ Server         │  │
│  └─────────────────┘    └─────────────────┘    └────────────────┘  │
│                                ↑                       ↑           │
│                                │                       │           │
│                         ┌──────────────┐         ┌──────────────┐  │
│                         │ Channel      │ ◀────── │ Schema       │  │
│                         │ Registry     │         │ Resolver     │  │
│                         │ (key→Raw     │         │ (introspect) │  │
│                         │  Channel)    │         │              │  │
│                         └──────────────┘         └──────────────┘  │
└────────────────────────────────────────────────────────────────────┘
       ▲                            ▲                       ▲
       │ admin/router/publisher/**  │ pub/**                │ ws://
       │                            │                       │
   ┌───────┐                    ┌───────┐               ┌─────────┐
   │ Zenoh │                    │ Aorta │               │Foxglove │
   │Router │                    │Nodes  │               │ Studio  │
   └───────┘                    └───────┘               └─────────┘
```

### 4.1 Discovery

Two discovery channels run in parallel:

**Data plane — wildcard subscribers.** One Zenoh subscriber per
`--include` pattern (defaulting to `aorta/{group}/pub/**`), plus
`aorta/{group}/ctx/**` and `aorta/{group}/sys/telemetry/**`
unconditionally — matching `aorta-record::build_topic_subscriptions`,
where include patterns narrow only the `pub/**` set. The subscriber
stream feeds the central forwarder and is what actually moves bytes
from Aorta to Foxglove. Queue-full behavior follows the configured
`drop_policy` (`drop_newest` default; drops are counted and reported
in the run summary).

**Control plane — admin-space probe.** A background task polls
`aorta_introspect::IntrospectClient::list_topics` every
`discovery_interval_ms` (default `2000`). For each declared topic the
loop resolves the BFBS schema and calls
`Server::ensure_channel(full_key, key_suffix, schema)`, which advertises
the channel to connected Foxglove clients without sending a payload.
This matches the behavior users expect from `ros2 launch
foxglove_bridge`: a topic that has been declared but is idle still
shows up in Studio's channel list. The wildcard subscriber then fills
in actual frames when data starts flowing.

`include` / `blacklist` patterns are applied on both paths so the two
sets stay consistent. The data plane uses the patterns to choose
selectors at startup; the control plane filters after `list_topics`
returns (the include gate lives in `TopicFilter` so a `--include`
bridge never pre-advertises channels no subscription will feed).

**Ctx snapshot refresh.** On its own interval
(`ctx_snapshot_interval_ms`, default 5 s, 0 disables) the discovery
task issues one wildcard GET over `aorta/{group}/ctx/**` — the same
query shape as the recorder's t0 snapshot — and forwards each current
value onto its typed `ContextValue` channel with the snapshot
wall-clock as `log_time`. This is the live-viewer equivalent of the
recorder's t0 snapshot: a Foxglove client that connects (or
subscribes) late sees full context state within one interval instead
of waiting for the next put, and idle ctx keys get their channels
advertised without any put at all.

### 4.2 Subscription Manager

For each accepted full key the manager:

1. Declares a `zenoh::Subscriber` on the full key.
2. Spawns a forwarding task that reads samples and pushes them onto an
   in-process channel destined for the bridge's central forwarder
   (the same single-consumer pattern the recorder uses, so backpressure
   semantics are familiar).

The manager owns the `JoinHandle`s and closes the subscribers cleanly
on shutdown or on a discovery-removed event.

### 4.3 Schema Resolver

One `Arc<aorta_core::schema_resolve::SchemaResolver>` (interior
mutability, `&self` API) is shared by the forwarder and the discovery
loop, so positive AND negative caches are common: a resolve that
failed on the data path is not re-paid by the next discovery tick, and
vice versa. It is configured identically to the recorder
(`by_hash_timeout_ms`, `by_key_timeout_ms`, `cache_max_entries`,
`negative_cache_ttl_ms`). The non-zero negative TTL (default 30 s,
matching the recorder) is load-bearing here:
without it, one unresolvable topic would re-pay the full query timeout
per message inside the single-consumer forwarder and stall every other
channel.

Only `pub/**` and the attachment-carrying telemetry families are
resolved at all (`should_resolve_schema`, mirrored from the recorder);
`ctx/**` is typed statically (§2.1) and everything else is forwarded
raw without paying a query timeout.

### 4.4 Channel Registry

The bridge keeps ONE channel per Aorta key
(`HashMap<full_key, { hash_hex: Option<String>, channel }>`). This
deliberately does NOT mirror the recorder's per-`(key, hash)`
`ChannelKey`: MCAP channels are append-only records, so the recorder
can open a second channel on a schema flap at zero cost — but for a
live bridge every `RawChannel::close()` is a client-visible
`unadvertise` that tears down panel subscriptions. Transitions are
therefore asymmetric:

| existing → wanted | action |
|:---|:---|
| none → anything | create |
| typed(h) → typed(h) | reuse |
| raw → raw | reuse |
| raw → typed | replace (schema became known) |
| typed(h) → typed(h′) | replace (schema evolution: close + re-advertise) |
| typed → raw | **reuse — never downgrade** |

The last row is the hard rule: a transient schema-resolve failure
(query timeout, publisher restart churn) must not close a healthy
typed channel. Without it, a failed resolve on the 2 s discovery tick
would flap unadvertise/advertise against every connected client. The
schema-less sample rides the existing channel instead — an Aorta
payload is the topic's FlatBuffer regardless of whether this
particular resolve round-tripped.

For each new channel the bridge constructs:

```rust
let schema = foxglove::Schema::new(
    bfbs_root_type_name(&bfbs).unwrap_or_else(|| hash_hex.clone()),
    "flatbuffer",
    bfbs.clone(),
);

let channel = foxglove::ChannelBuilder::new(/* topic: */ &full_key)
    .message_encoding("flatbuffer")
    .schema(schema)
    .add_metadata("aorta_key", &full_key)
    .add_metadata("key_suffix", &key_suffix)
    .add_metadata("schema_hash", &hash_hex)
    .build_raw()?;
```

The full Aorta key is used as the Foxglove topic name so clients see the
same routable identifier they would see in CLI output and in MCAP
recordings — there is no second naming convention to learn.

### 4.5 Forwarding

Each sample arriving on the central channel becomes:

```rust
channel.log_with_meta(
    &payload,
    foxglove::PartialMetadata::default()
        .with_log_time(publish_time_ns),
);
```

`publish_time_ns` is sourced from the Zenoh sample timestamp when
present (`sample.timestamp().get_time().as_nanos()`); otherwise the
ingestion wall-clock is used. This matches `resolve_publish_time_ns` in
`aorta-record` line-for-line.

### 4.6 Foxglove WebSocket Server

The server is created once at startup with
`foxglove::WebSocketServer::new()`:

```rust
let _handle = foxglove::WebSocketServer::new()
    .name(&config.server_name)
    .bind(&config.host, config.port)
    .start()
    .await?;
```

The returned `WebSocketServerHandle` is held until shutdown, at which
point `handle.stop().await` is called. Channel advertise / unadvertise
happens implicitly through `ChannelBuilder::build_raw` and
`RawChannel::close`; no explicit "advertise" call is needed because the
Foxglove SDK tracks the global context.

## 5. Configuration

The bridge accepts the same three configuration surfaces as the
recorder: a YAML/TOML/JSON file via `--config`, CLI flags, and
environment variables (`AORTA_GROUP` for the group fallback).

### 5.1 CLI Flags

| Flag | Default | Purpose |
|:---|:---|:---|
| `--config <path>` | none | YAML/TOML/JSON config file |
| `--group <name>` | env `AORTA_GROUP` or `default` | Aorta group |
| `--zenoh-config <path>` | env | Zenoh session config file |
| `--host <addr>` | `127.0.0.1` | WebSocket bind host |
| `--port <port>` | `8765` | WebSocket bind port (Foxglove default) |
| `--server-name <str>` | `aorta-foxglove-bridge` | Advertised server name |
| `--include <pattern>` (repeatable) | none → `pub/**` | Suffix patterns to forward |
| `--blacklist <pattern>` (repeatable) | none | Suffix patterns to exclude |
| `--blacklist-full <pattern>` (repeatable) | none | Full-key patterns to exclude |
| `--by-hash-timeout-ms <ms>` | `2000` | Schema-by-hash query timeout |
| `--by-key-timeout-ms <ms>` | `2000` | Schema-by-key query timeout |
| `--cache-max-entries <n>` | `256` | Resolver cache cap |
| `--negative-cache-ttl-ms <ms>` | `30000` | Negative schema-cache TTL (0 disables) |
| `--discovery-interval-ms <ms>` | `2000` | Admin-space poll period |
| `--ctx-snapshot-interval-ms <ms>` | `5000` | Periodic `ctx/**` snapshot refresh (0 disables) |
| `--queue-capacity <n>` | `8192` | Forwarder queue capacity |
| `--drop-policy <p>` | `drop_newest` | Queue-full policy: `drop_newest` / `block` |
| `--log-level <lvl>` | `info` | trace/debug/info/warn/error |

### 5.2 File Sections

```yaml
group: default
zenoh:
  config_file: /etc/zenoh/router.json5
server:
  host: 0.0.0.0
  port: 8765
  name: lab-bridge
topics:
  include_suffix_keyexpr: [ "demo/**" ]
  blacklist_suffix_keyexpr: [ "demo/private/**" ]
  blacklist_full_keyexpr: []
schema:
  by_hash_timeout_ms: 2000
  by_key_timeout_ms: 2000
  cache_max_entries: 256
  negative_cache_ttl_ms: 30000
discovery:
  interval_ms: 2000
  ctx_snapshot_interval_ms: 5000
forwarder:
  queue_capacity: 8192
  drop_policy: drop_newest
logging:
  level: info
```

The same precedence applies as in the recorder: file → CLI overrides → env.

## 6. Crate Layout

```
tools/aorta-foxglove-bridge/
├── Cargo.toml
├── BUILD.bazel
├── README.md
├── src/
│   ├── main.rs          # clap parsing, calls aorta_foxglove_bridge::run
│   ├── lib.rs           # entry point: run(config) -> Result<RunSummary>
│   ├── config.rs        # FileConfig + ConfigOverrides + build_config
│   ├── filter.rs        # suffix/full keyexpr filter (matches recorder semantics)
│   ├── discovery.rs     # admin-space publisher snapshot loop
│   ├── server.rs        # foxglove::WebSocketServer wrapper, channel registry
│   └── forwarder.rs     # Zenoh sample → RawChannel::log_with_meta
└── tests/
    └── bridge_smoke.sh  # talker + bridge + foxglove-ws client smoke test
```

## 7. Dependencies

| Crate | Version | Purpose |
|:---|:---|:---|
| `aorta_core` | path | KeyExprBuilder, `SchemaResolver`, ContextValue schema, bfbs root name helper |
| `aorta-introspect` | path | `IntrospectClient::list_topics`, `extract_attachment_schema_hash` |
| `zenoh` | `workspace = true` (single git pin in the root `Cargo.toml`) | Session, subscriber, admin query |
| `foxglove` | 0.23 (default features include `websocket`) | WebSocketServer, ChannelBuilder, RawChannel, Schema |
| `tokio` | 1.x | async runtime, signal, time |
| `clap` | 4.5 | CLI parsing |
| `anyhow` | 1 | error wiring |
| `serde`, `serde_json`, `serde_yaml`, `toml` | latest | config files |

The `foxglove` crate is pinned to the same version the official
Foxglove SDK getting-started page recommends; bumping it is a routine
PR. No vendored fork.

For Bazel: `foxglove` is declared via `crate.spec(...)` in
`MODULE.bazel` and consumed as `@aorta_crates//:foxglove` from
`tools/aorta-foxglove-bridge/BUILD.bazel`. Default features (`derive`,
`websocket`, `lz4`, `zstd`) are sufficient — the `websocket` feature
gates the `WebSocketServer` type the bridge depends on. Live
visualization is also covered by the `live_visualization` feature in
0.23, but it overlaps with `websocket`, so we stay on defaults.

## 8. Implementation Stages

### 8.1 P0 — Live Topic Viewer (current target)

| Stage | Description | Status |
|:---|:---|:---|
| **S0** | Crate scaffold + Cargo / Bazel build wired up | ✅ done |
| **S1** | Config layer mirroring `aorta-record::config` | ✅ done |
| **S2** | Filter module (extracted minimal copy of recorder filter) | ✅ done |
| **S3** | Discovery loop against admin space (`IntrospectClient::list_topics`) | ✅ done |
| **S4** | Channel registry + Foxglove server bring-up | ✅ done |
| **S5** | End-to-end forwarder (subscriber → resolver → channel) with `publish_time_ns` | ✅ done |
| **S6** | Docs + `foxglove.sdk.v1` handshake smoke + Bazel test wiring | ✅ done |
| **S7** | Rebase onto the updated core + review hardening: single-channel-per-topic registry (no typed→raw downgrade), drop policy + drop counter, shared core `SchemaResolver` with negative TTL, ctx `ContextValue` typing, `sys/telemetry/**` coverage, recorder-aligned filter semantics, forwarder-death shutdown | ✅ done (2026-07-18) |
| **S8** | Functional-gap round: discovery honors `--include` (no dead pre-advertised channels), periodic `ctx/**` snapshot refresh for late-connecting clients, one-shot WARN + recovery INFO when the admin space is unreachable | ✅ done (2026-07-18) |

P0 effort, retrospectively: ~5 engineer-days, since discovery and
schema-resolution logic was reused from existing crates and the only
genuinely new code is the Foxglove SDK glue and the channel registry.

### 8.2 P1 — Capability Parity (separate milestone)

Each row maps to a self-contained PR. None of these are wired today.

| Stage | Capability | Deliverable |
|:---|:---|:---|
| **P1-A** | TLS / WSS | `foxglove::WebSocketServer::tls(cert, key)` wiring + `--tls-cert` / `--tls-key` CLI; `bridge_smoke.sh` extended to assert WSS handshake with a self-signed pair |
| **P1-B** | Status / `client_count` / sysinfo | `WebSocketServerHandle::client_count()` + counters from `RunSummary` published as Foxglove `Status` frames every N seconds |
| **P1-C** | Time broadcast / `use_sim_time` | New `sim_time` topic convention + periodic `broadcast_time(ts)` on the handle |
| **P1-D** | `connectionGraph` | Periodic snapshot from `IntrospectClient::list_nodes` + edges, published via `publish_connection_graph`. Capability flag enabled iff snapshot fresh. |
| **P1-E** | `services` | C++ FFI JSON↔FB reused from `aorta-cli`; `WebSocketServerHandle::add_services` registers each discovered Aorta service. Capability flag toggles in `serverInfo`. |
| **P1-F** | `parameters` / `parametersSubscribe` | Discover `prm/**`, expose read; write path requires Aorta parameter QoS spec (blocked on upstream design) |
| **P1-G** | `clientPublish` | `ServerListener::on_message_data` validates payload against channel schema, then writes back to Aorta via `aorta_core::Publisher` |
| **P1-H** | `assets` | Aorta-side asset registry (out-of-scope of this spec); bridge exposes it via `ServerListener::on_fetch_asset` |
| **P1-I** | Real ws-protocol e2e | Rust `tokio-tungstenite` client lands in `test_scripts/`; matrix: advertise → subscribe → binary frame → schema decode → topic remove → schema flap → service call → param set → client publish |
| **P1-J** | Late-subscriber replay (`tf_static`-style) | Per-channel last-value cache, replayed on subscribe — gated on Aorta QoS spec |
| **P1-K** | Lazy Zenoh subscription | Drive `declare_subscriber` from `ServerListener::on_subscribe` / `on_unsubscribe` so an idle bridge stops pulling full camera/pointcloud streams over the robot's WiFi uplink when no Foxglove client is watching. Today the bridge subscribes its wildcard set unconditionally; `--include` is the interim mitigation. High value on constrained links — schedule early in P1. |

Each P1 stage explicitly turns on its Foxglove `capabilities` flag
only after its end-to-end test exists. Until then `serverInfo`
advertises an empty capability set, matching today's behavior.

## 9. Risks & Mitigations

| Risk | Impact | Mitigation |
|:---|:---|:---|
| `foxglove` crate API drift between releases | Bridge stops compiling on `cargo update` | Pin minor version in `Cargo.toml`; bump intentionally with a PR that runs `bazel build`. |
| Admin space disabled / ACL'd on production routers | Discovery silently empty | First failed admin probe logs an operator-visible WARN with remediation hints (and an INFO on recovery); the bridge keeps running because the data plane is independent — router-less peer setups are legitimate, topics then appear on first sample instead of pre-advertising. |
| Schema-hash flap on the same Aorta key | Foxglove client sees brief gap as old channel closes and new one is advertised | Acceptable for a REAL schema change; same behavior as Foxglove's own ROS 2 bridge. A failed resolve is NOT a schema change and never closes a typed channel (§4.4). |
| FlatBuffers BFBS not present | Foxglove cannot decode | `ctx/**` is typed statically (`aorta.ctx.ContextValue`); remaining schema-less streams fall back to `message_encoding = "aorta_raw_bytes"` with no schema — Foxglove still shows the channel with timestamps and payload size, matching recorder. |
| Unresolvable topic stalls the single-consumer forwarder | One bad topic re-pays query timeouts per message, starving every channel | Non-zero `negative_cache_ttl_ms` (default 30 s) + `should_resolve_schema` gating + `drop_newest` queue policy. |
| Memory growth from many short-lived topics | Long-running bridges accumulate `RawChannel`s | One channel per topic (replaced channels are closed); resolver cache capped by `cache_max_entries`. Channels for topics that disappear are NOT yet closed (no discovery-removed detection) — a stale idle channel costs one advertise entry; revisit with P1-D connection graph. |
| Bind on `0.0.0.0` exposes the bridge to the LAN | Unauthenticated read of all topics | Default bind is `127.0.0.1`; README documents the security tradeoff for `0.0.0.0`. |

## 10. Verification

### 10.1 P0 — Live Topic Viewer

Build + unit tests (developer-run; the bridge tier follows the repo
rule that agent-driven Bazel runs require explicit operator
authorization per task):

```bash
bazel build //tools/aorta-foxglove-bridge:aorta-foxglove-bridge
bazel test  //tools/aorta-foxglove-bridge:aorta_foxglove_bridge_test
```

Smoke (`tests/bridge_smoke.sh`, wired as
`//tools/aorta-foxglove-bridge:aorta_foxglove_bridge_smoke_test`):

1. Spawn `aorta-foxglove-bridge` on a free port.
2. Send a `Sec-WebSocket-Protocol: foxglove.sdk.v1` upgrade request.
3. Assert `HTTP/1.1 101 Switching Protocols` and a `serverInfo` JSON
   frame containing the bridge's name and the SDK library version.

P0 smoke deliberately stops short of asserting binary message frames;
that lives in P1-I (real ws-protocol matrix).

Manual end-to-end against the existing example:

```bash
bazel run //tools/aorta-foxglove-bridge:aorta-foxglove-bridge -- \
    --group default --port 8765 &
bazel run //rust/examples:cli_talker -- --topic /demo/chatter

# Open Foxglove Studio and connect to ws://localhost:8765
#   Expected: /aorta/default/pub/demo/chatter visible with the
#   FlatBuffers schema attached and live samples flowing.
```

### 10.2 P1 — Capability Matrix

Each P1 stage adds its own test row before its capability flag is
allowed in `serverInfo`:

| Stage | Test |
|:---|:---|
| P1-A TLS | `bridge_smoke.sh --tls` with a self-signed pair, expect WSS handshake |
| P1-B status | Foxglove client subscribes to status; assert `client_count` increments and decrements with connections |
| P1-C time | Move sim clock; assert Studio timeline follows |
| P1-D graph | Spawn 2 talkers + 1 listener; assert nodes/edges show up |
| P1-E services | Call an Aorta service from a Studio panel script; assert JSON↔FB round-trip |
| P1-F parameters | Set a param via Studio; assert Aorta-side observer sees the new value |
| P1-G clientPublish | Publish from Studio; assert an Aorta subscriber receives the message with the right schema hash |
| P1-H assets | Fetch a URDF via the bridge; assert byte-identical to the asset on disk |
| P1-I e2e matrix | Full advertise / subscribe / binary frame / schema decode / topic remove / schema flap / service call / param set / client publish matrix |
| P1-J latched | Late subscriber to a `RELIABLE+TRANSIENT_LOCAL`-style topic receives the last value |

## 11. Open Questions

These cluster under P1; flagging here so the rollout plan does not
silently inherit them.

1. **`tf_static` parity** — ROS 2's `foxglove_bridge` replays the last
   value on subscribe for `TRANSIENT_LOCAL` topics. Aorta has no QoS
   layer today; P1-J needs an upstream decision on whether the bridge
   maintains its own per-channel last-value cache or whether Aorta
   grows a real durability profile first.
2. **Parameter write semantics** — Foxglove parameter clients expect
   atomic read-modify-write. Aorta's `prm/**` design has not pinned
   down conflict semantics, so P1-F is blocked on the parameter spec.
3. **Asset registry** — P1-H assumes an Aorta-side asset namespace
   (URDFs, meshes, configs) that does not exist. Either we punt to
   filesystem-only assets, or we land a small `assets/**` namespace
   convention as a prerequisite.
4. **Bridge-as-Node** — recurring ask to make the bridge a
   discoverable Aorta Node so operators can `nodes list` it. Couples
   the bridge into the liveliness model; recommend keeping the bridge
   passive in P0/P1 and revisiting after Phase 3 schema discovery
   closes.
5. **Capability-flag policy** — every P1 stage advertises its
   Foxglove capability only after its e2e test exists. Need a written
   policy (probably in this spec, §2.4) that the `serverInfo` payload
   is the contract; we should not enable a flag for partial
   functionality, ever. This matters because Studio gates UI on the
   flag — half-on means broken UI.

## 12. Recommendation

Ship P0 as the first milestone under the explicit name "live topic
viewer," and treat each row of §2.2 as its own scoped follow-up. P0
gives the operator value (FlatBuffers-typed live data in Studio) at
low maintenance cost, while P1 unlocks the full `foxglove_bridge`
parity story without inflating the P0 surface or making honest review
impossible.

The bridge is intentionally a thin adapter on top of
`aorta-introspect` and the Foxglove Rust SDK, so each P1 slice stays
self-contained and the maintenance cost grows linearly with the
capability set rather than as a coupled monolith.
