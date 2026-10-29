# aorta-foxglove-bridge

A live WebSocket bridge between an Aorta deployment and
[Foxglove Studio](https://foxglove.dev). For every topic published under
`aorta/{group}/pub/**` the bridge advertises a Foxglove channel with the
topic's FlatBuffers schema attached, then forwards each Zenoh sample as
a Foxglove message frame. `ctx/**` values are typed with the static
`aorta.ctx.ContextValue` schema and `sys/telemetry/**` streams are
covered too, matching what `aorta-record` writes into MCAP: whatever
Foxglove can render from a recording it can also render live.

> **Scope today: P0 — Live Topic Viewer.** This is *not* a drop-in
> replacement for `ros/src/foxglove_bridge`. Services, parameters,
> client-publish, `connectionGraph`, `assets`, TLS, time broadcast,
> remote-access, lazy per-client subscription, and late-subscriber
> replay are all P1 work tracked in
> `spec/Aorta_Foxglove_Bridge_Design.md` §2.2 / §8.2. The bridge's
> `serverInfo.capabilities` array stays empty until each P1 capability
> has end-to-end test coverage.

Design doc: [`spec/Aorta_Foxglove_Bridge_Design.md`](../../spec/Aorta_Foxglove_Bridge_Design.md).

## Quick start

```bash
# Run the bridge against the default group, listening on 127.0.0.1:8765
bazel run //tools/aorta-foxglove-bridge:aorta-foxglove-bridge -- --group default

# In Foxglove Studio: Open Connection → Foxglove WebSocket → ws://localhost:8765
```

What you can expect in Studio: Raw Messages, Plot, and State Transitions
panels decode any `aorta.*` FlatBuffers schema. Panels that key off
well-known schema names (Image, 3D) need `foxglove.*`-convention
schemas — the same constraint as when opening an Aorta MCAP recording.

Note the bridge subscribes to its wildcard set even while no Foxglove
client is connected; on constrained links (robot WiFi uplink), narrow
the set with `--include` until lazy subscription (P1-K) lands.

## CLI flags

| Flag | Default | Notes |
|:---|:---|:---|
| `--config <path>` | none | YAML/TOML/JSON config file. CLI flags override file values. |
| `--group <name>` | env `AORTA_GROUP` or `default` | Aorta group. |
| `--zenoh-config <path>` | env `ZENOH_SESSION_CONFIG_URI` | Zenoh session config file. |
| `--host <addr>` | `127.0.0.1` | WebSocket bind host. |
| `--port <port>` | `8765` | WebSocket bind port (Foxglove default). |
| `--server-name <str>` | `aorta-foxglove-bridge` | Advertised server name. |
| `--include <pattern>` (repeatable) | `pub/**` | Suffix keyexpr to forward; narrows the `pub/**` set only (`ctx/**` + `sys/telemetry/**` stay subscribed). Patterns may overlap (e.g. `**` plus `demo/chatter`); a sample matched by several patterns is forwarded by one subscriber only (at most once under `drop_newest`). |
| `--blacklist <pattern>` (repeatable) | none | Suffix keyexpr to drop, rooted under `pub/` (recorder semantics: `demo/private/**` blocks `pub/demo/private/**`). |
| `--blacklist-full <pattern>` (repeatable) | none | Full-key keyexpr to drop (use this for `ctx/**` / `sys/telemetry/**`). |
| `--by-hash-timeout-ms <ms>` | `2000` | Schema-by-hash query timeout. |
| `--by-key-timeout-ms <ms>` | `2000` | Schema-by-key query timeout. |
| `--cache-max-entries <n>` | `256` | Schema resolver cache cap. |
| `--negative-cache-ttl-ms <ms>` | `30000` | Negative schema-cache TTL; 0 disables. Bounds re-query cost for unregistered topics. |
| `--discovery-interval-ms <ms>` | `2000` | Admin-space poll period. |
| `--ctx-snapshot-interval-ms <ms>` | `5000` | Periodic `ctx/**` snapshot refresh so late-connecting clients see context state; 0 disables. |
| `--queue-capacity <n>` | `8192` | Forwarder queue capacity. |
| `--drop-policy <p>` | `drop_newest` | Queue-full policy: `drop_newest` (live viewer default; drops counted in the summary) or `block` (recorder-style backpressure). |
| `--log-level <lvl>` | `info` | trace / debug / info / warn / error. |

## Signals

`SIGINT` and `SIGTERM` both take the same graceful path: stop the
subscribers, drain the forwarder (5 s budget), close the WebSocket
server, print the run summary, exit 0. `SIGTERM` matters for supervised
deployments — it is systemd's default `KillSignal`, so a unit needs no
`KillSignal=` override to stop the bridge cleanly.

## Config file

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

## Status

### P0 — Live Topic Viewer (today)

- `WebSocketServer` boots and accepts Foxglove Studio clients on
  `--host`/`--port`; smoke test asserts the `foxglove.sdk.v1`
  handshake + `serverInfo` round-trip.
- Wildcard subscribers cover `aorta/{group}/pub/**`,
  `aorta/{group}/ctx/**`, and `aorta/{group}/sys/telemetry/**`
  (matching `aorta-record` defaults).
- Admin-space discovery loop (`IntrospectClient::list_topics`) runs
  every `--discovery-interval-ms` and pre-advertises channels for
  declared-but-idle publishers, honoring the `--include` set; the
  first unreachable-admin-space probe logs an operator-visible WARN
  (the bridge keeps running — data-plane channels appear on first
  sample).
- Periodic `ctx/**` snapshot (`--ctx-snapshot-interval-ms`, default
  5 s): current context values are re-logged on their typed channels
  so a client connecting late sees robot state within one interval —
  the live equivalent of the recorder's t0 snapshot.
- Schema reuse: the canonical `aorta_core::schema_resolve::SchemaResolver`
  (attachment hash preferred, by-key fallback, positive + negative
  caches) shared by the live forwarder and the discovery loop;
  `ctx/**` typed via the static `aorta.ctx.ContextValue` schema.
- Foxglove `log_time` uses the publisher's Zenoh sample timestamp when
  available, falling back to the bridge's ingest time.
- One channel per Aorta key: raw upgrades to typed when the schema
  resolves, a real schema-hash change closes and re-advertises, and a
  failed resolve never downgrades a typed channel (no advertise flap).
- Queue-full behavior is configurable (`drop_newest` default); drops
  are counted and reported in the run summary.

### P1 — `foxglove_bridge` Parity (not started)

Each row is its own follow-up PR; capability flags in `serverInfo`
turn on **only** after the matching test row lands.

| Capability | Status |
|:---|:---|
| TLS / WSS | not started |
| Status / `client_count` / sysinfo | not started |
| Time broadcast / `use_sim_time` | not started |
| `connectionGraph` (node graph) | not started |
| `services` | not started |
| `parameters` / `parametersSubscribe` | not started |
| `clientPublish` (Studio → Aorta) | not started |
| `assets` (URDF / mesh / config fetch) | not started |
| Lazy per-client Zenoh subscription (P1-K) | not started — bridge currently subscribes unconditionally |
| Real ws-protocol e2e matrix (advertise / subscribe / binary frame / schema decode / topic remove / schema flap / service call / param set / client publish) | not started — P0 smoke is handshake-only |
| Late-subscriber replay (`tf_static`-style) | not started — blocked on Aorta QoS spec |

Until P1 work lands the bridge announces an empty `capabilities`
array — clients see honest feature-gating instead of a half-wired
flag.
