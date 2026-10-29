# Aorta SDK — Common Concepts (Shared Across Rust / C++ / Python)

> **Audience:** users of any Aorta SDK. This document is the single source of
> truth for the **language-agnostic** concepts that every SDK exposes — the
> layer model, key-expression format, schema/header rules, QoS, time/clock
> model, and the error code table. The per-language READMEs
> ([`rust/README.md`](../rust/README.md), [`cpp/README.md`](../cpp/README.md),
> [`python/README.md`](../python/README.md)) cover install, the language API
> surface, idioms, and code examples — and link back here for these shared
> concepts so the semantics are defined in exactly one place.
>
> **Environment-variable configuration** lives in its own shared reference:
> [`spec/Aorta_Environment_Variables_Reference.md`](./Aorta_Environment_Variables_Reference.md).

---

## 1. Layer model

Aorta is layered; an SDK is the top of this stack:

```
L0  Zenoh transport            (pub/sub, query/queryable, liveliness)
L1  Rust Core (aorta_core)     (primitives, schema, QoS, executor, telemetry, trace)
L2  C-ABI (cbindgen)           (stable libaorta_core surface)
L3  SDKs  rust/ · cpp/ · python/
```

Consequences that matter to SDK users:

- **All SDKs share one runtime.** A behavior defined in L1 Core (e.g. how a key
  is built, how a schema hash is computed, what an env var does) is identical
  in every SDK. That is why this doc and the env-var reference are shared.
- **The Rust SDK is a thin layer over Core; the C++ and Python SDKs sit on the
  C-ABI** (Python via `ctypes`, no compiled extension). The conceptual model
  below is the same regardless.

---

## 2. Primitives

| Primitive | Pattern | Purpose |
|---|---|---|
| **Topic** (Publisher / Subscriber) | pub/sub | Fire-and-forget streaming; per-message `AortaHeader` injected automatically on the typed path. |
| **Service** (Service / Client) | query/queryable | Request–response RPC; single logical provider expected. |
| **Action** (ActionServer / ActionClient) | composite | Long-running, cancellable goals with feedback + terminal result. |
| **Timer** | local | Periodic callback driven by an Executor (no independent thread per timer). |
| **Context** (ContextStore) | declarative state | Key/value state with watches and snapshots. |

Concept notes shared by all SDKs:

- **Publish is fire-and-forget** and synchronous at the call site. The typed
  path injects an `AortaHeader` (monotonic per-publisher sequence, publisher
  node name, publish timestamp from the Core clock).
- **Subscribers** support per-message context (topic, sequence, publisher node,
  receive stamp, priority), a **deduplication** window, and **reorder
  detection**; overflow handling is configurable.
- **Service/Client** ride Zenoh query/queryable. Automatic RPC **retries are
  OFF by default** (a non-idempotent service must not be silently re-invoked);
  see `AORTA_RPC_ALLOW_RETRY` in the env-var reference.
- **Actions** guarantee a single terminal state (succeed / abort / cancel) and
  support cooperative cancellation and feedback; a goal can be resumed from a
  persisted `(goal_id, goal_secret, server_epoch)` after a restart.

---

## 3. Key-expression format

Every primitive resolves to a Zenoh key with a fixed shape:

```
aorta/{group}/{primitive}/{category}/{name}
```

- `{group}` — the node group, a namespace/isolation boundary. The **effective**
  group is resolved from the `AORTA_GROUP` env var, falling back to `default`
  (see env-var reference). **⚠️ The `group` argument to the node constructor
  (`Node::new(name, group)` / `Node::Create(name, group)`) is currently ignored**
  — Core always re-resolves the group from `AORTA_GROUP`/`default`
  (`core/src/node.rs:246`). To isolate nodes into different groups, set distinct
  `AORTA_GROUP` values (per process); passing different constructor arguments has
  no effect. `node.group()` returns the resolved effective value.
- `{primitive}` — the literal wire segment for the primitive family: `pub`
  (topic), `srv` (service), `act` (action), `prm` (param). System/telemetry
  keys use `sys`.
- `{category}` — the per-primitive surface segment. It is **not** present for
  every primitive: basic topic and service keys have no separate `{category}`
  segment, so they collapse to `aorta/{group}/pub/{name}` and
  `aorta/{group}/srv/{name}`. `{category}` only appears for composite primitives
  (actions: `aorta/{group}/act/{surface}/{name}`) and system keys
  (`aorta/{group}/sys/{subsystem}/...`).
- `{name}` — the user-supplied name (topic name, service name, action name).

Actions expand to several keys under `aorta/{group}/act/{surface}/{name}`,
where `{surface}` is one of `send_goal`, `cancel_goal`, `feedback`, `status`,
`get_result`, `info`; see [`rust/README.md` §14](../rust/README.md#14-actions-long-running-tasks). Params use
`aorta/{group}/prm/{node}/{param}`.

---

## 4. Schema model (FlatBuffers + AortaHeader)

Aorta is schema-first and zero-copy on the read path.

- **Messages are FlatBuffers tables.** Every Aorta message embeds an
  `aorta_header` field; the typed publish/serve/call paths fill it for you.
- **Schema hash** — each schema has a **SHA-256** identity, rendered as **64
  lowercase hex characters**. It is the cross-language contract identity.
- **Schema attach policy** — whether the schema hash travels on each message:

  | Policy | Meaning |
  |---|---|
  | `inherit` | Use the node/primitive default. |
  | `always` | Always attach the hash. |
  | `never` | Never attach (default). |

  Configurable in code per primitive and via `AORTA_SCHEMA_ATTACH` (default
  `never`).
- **Schema discovery & validation** — a publisher/service can expose its schema
  via a Zenoh queryable; peers can prefetch and validate the SHA-256 hash before
  exchanging data. RPC can additionally pin a `service_schema_hash` for safety.
- **Conflict action** — on a schema-hash mismatch, the registry either warns or
  rejects; configurable via `AORTA_SCHEMA_CONFLICT_ACTION` (default `warn`).

For authoring/evolving schemas see `message/` and the `coding-aorta-schema`
guidance; the registry-driven codegen produces the per-language wrappers.

---

## 5. QoS model

`QoS` is **4 fields** — every one wired to a verified Zenoh primitive, with **no
silent no-ops** (QoS & buffering redesign). The values are defined in Core
(`core/src/qos.rs`) and are identical across SDKs.

| Field | Values | Meaning |
|---|---|---|
| **Priority** | `Realtime` · `High` · `Normal` · `Low` · `Background` | Scheduling importance; maps to executor priority (below). |
| **Reliability** | `Reliable` · `BestEffort` | Reliable link-class preference vs lowest-latency best-effort. |
| **CongestionControl** | `Drop` (default) · `Block` | What to do when the Zenoh egress queue is full (see below). |
| **express** | bool | Low-latency express path (no batching). |

> **Removed fields.** `durability` (with the `Durability` / `TransientLocal` /
> `Volatile` enum) and the QoS `history_depth` are **gone**. They were forwarded
> to Zenoh nowhere — `TransientLocal` was an unimplemented no-op — so deleting
> them changes no running behavior.
>
> - **Last-value / latched / late-joiner snapshots are not a QoS concern.** If a
>   late joiner or a reconnecting reader needs "the current value", publish that
>   state through **`ContextStore`** (Aorta's fifth primitive, §2), which owns
>   last-value / watch semantics.
> - **Per-topic receive depth / drop-isolation** is now
>   `SubscriberOptions.receive_depth` (subscriber side), not a QoS dimension —
>   see [§5 · Per-topic subscriber buffering](#per-topic-subscriber-buffering).

### Congestion control

`CongestionControl` is now a **QoS field** (formerly publisher-only). It is the
honest "don't silently drop" lever and governs what the publisher does when the
Zenoh egress queue is full:

| Value | Behavior |
|---|---|
| `Drop` (default) | Drop messages on a full egress queue — lowest latency, **may lose data even under `Reliability::Reliable`** (the loss happens locally before transmission). |
| `Block` | Backpressure the sender (the publish call stalls) until the queue drains — no local queue drops (end-to-end delivery is still not guaranteed), but a slow/absent route can stall the caller. |

Per-SDK spelling: Rust `QoS::with_congestion_control(CongestionControl::Block)`;
C++ `qos.congestion_control = CongestionControl::kBlock`; Python
`QoS(congestion_control=CongestionControl.BLOCK)`. (The C++/Python
`PublisherOptions` retain an optional per-publisher congestion-control *override*
of the QoS value.)

### Built-in profiles

| Profile | Priority | Reliability | CongestionControl | express |
|---|---|---|---|---|
| `default` | Normal | Reliable | Drop | false |
| `realtime_control` | Realtime | Reliable | Drop | **true** |
| `sensor_data` | Normal | BestEffort | Drop | false |
| `state_update` | High | Reliable | Drop | false |
| `bulk_transfer` | Background | Reliable | Drop | false |

**No profile blocks by default** — every preset is lowest-latency `Drop`
(pre-redesign every publisher effectively ran `Drop`; a preset silently
flipping to `Block` would let a congested transport stall the publishing
thread with zero code change). Only `realtime_control` sets `express`. If a
stream must never drop at the local egress queue, set `with_congestion_control(Block)`
explicitly — an informed opt-in that accepts a potentially stalled
`publish()`.

`state_update` no longer implies any latched/last-value replay (the old
`TransientLocal` was a no-op); for "late joiner gets the current value", use
`ContextStore`.

### Per-topic subscriber buffering

Orthogonal to QoS, the subscriber side gained per-topic backpressure:

- **Tier A — `SubscriberOptions.receive_depth`.** A per-topic in-flight cap.
  `0` (default) = today's behavior (unbounded, shared executor queue); `>0`
  drops the **newest** event once the cap is reached (drop-newest), isolating
  one slow topic's congestion. The shed count is observable —
  `Subscriber::dropped_for_depth()` (Rust) / `Subscriber::DroppedCount()` (C++)
  / `Subscriber.dropped_count()` (Python).
- **Tier B-pull — pull-mode subscriber.** A Zenoh-native keep-last ring with no
  callback; the application drains it itself (the latest-wins / `wait_for_message`
  idiom). Created via `create_subscriber_pull(topic, depth)` (Rust core /
  Python) / `CreatePullSubscriber(topic, depth)` (C++); drained via
  `recv` / `try_recv` / `recv_timeout` returning a `PullSample { payload,
  context }`. Pull is opt-in; the inline-push variant is not offered in Python.

### QoS → Executor priority mapping

A message's `Priority` maps onto the executor's priority lanes when the Priority
scheduler is enabled: `Realtime → High → Normal → Low → Background`. With the
default FIFO scheduler, priority does not reorder callback dispatch.

---

## 6. Time / clock model

All SDKs read time from one unified Core clock; timestamps are **nanoseconds**.

| Mode | Behavior | Typical use |
|---|---|---|
| **System** (default) | Wall-clock (`CLOCK_REALTIME`-class), auto-advancing. | Normal operation, log/trace correlation. |
| **Steady** | Monotonic, auto-advancing. | Measuring durations without wall-clock jumps. |
| **Simulation** | Manually advanced by the test/harness. | Deterministic tests, replay. |

- `AortaHeader.publish_stamp_ns` is filled from this clock on the typed publish
  path, so subscriber-side latency math is consistent fleet-wide.
- The **mode is process-global** (selected by Core / tests). The runtime-
  selectable source is **System / Simulation**; `Steady` is a Core `ClockMode`
  not exposed as a runtime switch. No SDK exposes a clock-mode switching API.
- Each SDK exposes a node-scoped “now”. **Rust and C++** additionally expose a
  global now / now-ns accessor; the **Python** SDK exposes `Node.now()` only.
  See the per-language README.

---

## 7. Error model

The C-ABI result codes are the shared error vocabulary; each SDK maps them to
its idiomatic form (Rust `AortaError`/`AortaResult`, C++ `Status`/`StatusOr`,
Python typed exceptions). Values are defined in `core/src/ffi/c_api/mod.rs`.

| Code | Value | Meaning |
|---|---|---|
| `AORTA_OK` | `0` | Success. |
| `AORTA_ERR_INVALID_ARGUMENT` | `-1` | Bad/empty argument. |
| `AORTA_ERR_NOT_FOUND` | `-2` | Target not found (e.g. goal, key). |
| `AORTA_ERR_TIMEOUT` | `-3` | Operation timed out. |
| `AORTA_ERR_BUSY` | `-4` | Resource busy / admission rejected. |
| `AORTA_ERR_INTERNAL` | `-5` | Internal error. |
| `AORTA_ERR_SCHEMA_CONFLICT` | `-7` | Schema-hash mismatch (reject mode). |
| `AORTA_ERR_CONCURRENT_CONFLICT` | `-8` | Concurrent state conflict. |
| `AORTA_ERR_ALREADY_REPLIED` | `-10` | Service request already replied/released. |
| `AORTA_ERR_PREFIX_NOT_DECLARED` | `-11` | Context key prefix not declared. |
| `AORTA_ERR_DECLARATION_NOT_READY` | `-12` | Declaration not yet ready. |
| `AORTA_ERR_KIND_MISMATCH` | `-13` | Primitive/kind mismatch. |
| `AORTA_ERR_INSTANCE_ID_COLLISION` | `-14` | Instance ID collision. |
| `AORTA_ERR_REENTRANT` | `-15` | Illegal reentrant call. |

> Values `-6` and `-9` are intentionally unused (reserved gaps).

---

## 8. Configuration via environment variables

All runtime configuration env vars (logging, session/Zenoh config, schema, QoS,
RPC, executor admission, trace, telemetry, identity) are documented once in:

→ **[`spec/Aorta_Environment_Variables_Reference.md`](./Aorta_Environment_Variables_Reference.md)**

Highlights every SDK shares:

- **Session config:** `ZENOH_SESSION_CONFIG_URI` (file path or `file://` URI;
  L1-Core-consumed, so honored by all SDKs; takes priority over `ZENOH_CONFIG`).
- **Group:** `AORTA_GROUP`.
- **Logging:** `AORTA_LOG` / `RUST_LOG`, `AORTA_LOG_FORMAT`, `AORTA_LOG_FILE`.
- **Schema:** `AORTA_SCHEMA_ATTACH`, `AORTA_SCHEMA_CONFLICT_ACTION`.
- **Tracing:** `AORTA_TRACE`, `AORTA_TRACE_DIR`, … — trace *configuration* is
  env-driven and shared by all SDKs. Beyond that, the **C++ and Rust SDKs do
  expose a per-SDK tracing API**: C++ via the `aorta::trace` namespace and the
  `AORTA_TRACE_*` macros (`cpp/include/aorta/trace.h`), Rust via the trace
  surface re-exported as `aorta_sdk::trace` (from `aorta_core::trace`). Only the
  **Python SDK has no per-request tracing API** — it is env-driven only.

---

## 9. See also

- Env-var reference: [`spec/Aorta_Environment_Variables_Reference.md`](./Aorta_Environment_Variables_Reference.md)
- Rust SDK: [`rust/README.md`](../rust/README.md)
- C++ SDK: [`cpp/README.md`](../cpp/README.md)
- Python SDK: [`python/README.md`](../python/README.md)
