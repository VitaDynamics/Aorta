# Aorta Cloud Cross-Namespace Addressing Proposal (Option B)

> Historical design note: describes the design at the time it was written; it is not a v0.1.0 instruction.

- **Status**: v2 — revised after design review (change summary in §0); **P0 implemented** (see §12)
- **Date**: 2026-06-16
- **Author**: (TBD)
- **Code baseline**: file/line references are against the code at the time of writing and may have drifted since
- **Cloud SDK language**: **Rust** (drives the P0 scope, see §12)
- **Related docs**: `spec/Aorta_Router_ACL_Minimal_Samples.md`

---

## 0. Change summary (review MUST FIX items)

| Review MUST FIX | Resolution | Section |
|---|---|---|
| namespace must be normalized per-segment; `WildcardMode` must actually reach the splice point; a multi-segment ns can't be wildcard-checked as one string | Rewrote validation: `with_namespace` validates per-segment; an independent `NamespaceMode` is checked at the splice point | §4.1 |
| wildcard namespace is too permissive for service/action (`QueryTarget::All` + first-reply-wins) | wildcard ns is **observe-only** (topic/feedback/status subscribe); **rejected** on request/reply (service client, action goal·cancel·get_result) | §4.2, §5.1 |
| the reverse parse is also used by subscriber init schema-prefetch / header-layout refresh, not just the callback topic | single-point change to `key_suffix_from_full_key` (returns suffix + source_namespace), covering all call sites | §4.3 |
| schema-by-hash path hardcodes `aorta/sys/schema/**` and `strip("aorta/sys/schema/")` | keep the producer side static; make the consumer-side parse namespace-aware | §4.4 |
| `SchemaRegistry` holds the Node's own key_builder, so a per-call namespaced client does not change its queries | P0: cross-ns subscribers skip init prefetch / can pass a layout hint; P1: registry can use a namespaced key_builder + cache keyed by namespace | §4.5, §6 |
| `Target::group("*")` must not be silently ignored if unimplemented in v1 | v1 `Target` only exposes `namespace`; no `.group()` setter | §5.5 |
| unknown attachment-hash degradation behavior was unspecified | spelled out: typed decode continues; header-context degradation rules | §6 |
| C-ABI/C++/Python and `source_namespace` must not be deferred to P2 (contradicts "cloud uses the SDK") | cloud = Rust → P0 includes the Rust SDK; C-ABI/C++/Python deferred with explicit dependencies | §9, §12 |
| ACL doc only covers schema keys; lifecycle semantics missing | add a per-primitive ACL surface + lifecycle/cleanup semantics | §8 |
| C++ `BuildActionStatusTopic` hand-writes the key, bypassing the Core keyhole | listed as a C++-phase MUST FIX; corrected the "single keyhole" claim | §3, §9 |

---

## 1. Background & goals

### 1.1 Problem

Each robot deploys with **its SN as the Zenoh session namespace**. For a service, the robot declares its queryable on the relative key `aorta/{group}/srv/{service}`, which the Zenoh namespace layer turns into the following on the wire:

```
{SN}/aorta/{group}/srv/{service}
```

A cloud service wants to use the **Aorta SDK (Rust)** to subscribe to / call topics / services / actions on **any robot**, with:

- **one session** covering the whole fleet;
- dynamic addressing **by SN**, **without reopening / reconfiguring the session per robot**;
- reuse of the SDK's typed (de)serialization, schema, and header handling (not dropping down to hand-rolled raw Zenoh keys + payloads).

### 1.2 Non-goals

- Do not change the robot-side deployment model (still "one Zenoh namespace = SN per robot").
- Do not introduce cloud-side **publishing** of owner/liveliness/telemetry (the cloud is a pure consumer / caller).
- Do not change any wire bytes for the default (no-namespace) case (strict backward compat, see §10).

---

## 2. Constraint: the Zenoh namespace mechanism (facts)

From the zenoh fork (`rev 1cdf9f8`):

- Config field `namespace: Option<OwnedNonWildKeyExpr>` (`commons/zenoh-config/src/lib.rs:920`).
- Semantics (`zenoh/src/net/routing/namespace.rs`): a `Primitives` interceptor layer — **outgoing** keys are auto-prefixed with `{ns}/`, **incoming** keys have that prefix stripped.
- **Three hard constraints**:
  1. **Session-scoped** — bound at session open;
  2. **Immutable at runtime** — changing it means reopening the session (exactly what we must avoid);
  3. **Must be non-wildcard** (`OwnedNonWildKeyExpr`) — you **cannot** set a session namespace of `*` to "cover all SNs with one session".

**Corollary**: making one session "switch SN dynamically / cover the whole fleet" **cannot** rely on the Zenoh session namespace. The cloud must open a **namespace-free session** and put the SN as an explicit leading key segment (this is just a plain key expr, which may contain `*` for fan-out).

---

## 3. Design overview

> **Core idea**: introduce an **application-level namespace prefix** in Aorta (decoupled from the Zenoh session namespace). The cloud uses a **single namespace-free session** as its base and builds keys as `{SN}/aorta/{group}/...` per target.

```
Robot side (unchanged):
  local relative key   aorta/{group}/srv/{svc}
  + zenoh ns={SN}  ──►  wire {SN}/aorta/{group}/srv/{svc}

Cloud (this proposal):
  zenoh ns = none
  Aorta app-level ns = {SN}  ──►  builds {SN}/aorta/{group}/srv/{svc}
                             ──►  lines up exactly with the robot's wire key
```

The main "keyhole" is `KeyExprBuilder`: nearly all keys flow through `build_grouped` / `build_grouped_string`, which produce `aorta/{group}/{suffix}` (`core/src/key_expr.rs:349-356`). Prepending a namespace there covers topic / service / action / etc.

> **But it is not the only keyhole** (the review corrected the over-broad claim in proposal v1):
> - The static method `KeyExprBuilder::schema_key` (`schema_registry.rs:762/918`) and the parser `schema_hash_from_full_key` (`:967`) do **not** go through `build_grouped`; handled separately (§4.4).
> - The C++ SDK's `BuildActionStatusTopic` **hand-writes** `"aorta/"+group+"/act/status/..."` (`cpp/include/aorta/action.h:1725-1732`) and then calls `CreateSubscriber` (`:1114`), **bypassing** the Core keyhole — a C++-phase MUST FIX (§9).

---

## 4. Core change: add a namespace to `KeyExprBuilder`

### 4.1 Data structure & per-segment validation

```rust
pub struct KeyExprBuilder {
    group: String,
    namespace: Option<String>,   // new: app-level namespace (e.g. SN). None = old behavior
}

impl KeyExprBuilder {
    /// Derive a builder bound to an app-level namespace prefix (e.g. an SN).
    /// The namespace is normalized per-segment with the same rules as a path
    /// (NFC, per-segment charset, whole-segment `*`/`**` allowed), with
    /// multi-segment namespaces (e.g. "region/site/robot_a") validated segment by
    /// segment. Whether a wildcard is actually permitted is decided by the
    /// invoking primitive (§4.2).
    pub fn with_namespace(self, namespace: impl AsRef<str>) -> AortaResult<Self>;
    pub fn namespace(&self) -> Option<&str>;
}
```

**Validation-placement fix (review MUST FIX)**: today `build_grouped` / `build_grouped_string` do **not** take a `WildcardMode`; the mode is only used upstream in `normalize_path_inner`. This proposal:

1. `with_namespace` performs the **syntactic** validation of the namespace (per-segment normalize).
2. The splice point takes a `NamespaceMode` (`Allow` / `Reject`) **independent of** the path `WildcardMode`, supplied by the concrete primitive; if the namespace contains a wildcard segment while `NamespaceMode::Reject`, it errors. No more "check the whole string" via a naive `segment_is_wildcard`.

```rust
fn build_grouped_string(&self, suffix: &str, ns_mode: NamespaceMode) -> AortaResult<String> {
    let base = format!("{ROOT_PREFIX}/{}/{suffix}", self.group);
    match &self.namespace {
        None => Ok(base),
        Some(ns) => {
            if ns_mode == NamespaceMode::Reject && ns_has_wildcard_segment(ns) {
                return Err(AortaError::invalid_argument(
                    "wildcard namespace not allowed for this operation (request/reply / publish)",
                ));
            }
            Ok(format!("{ns}/{base}"))
        }
    }
}
```

### 4.2 Wildcard semantics (key fix)

The app-level namespace is not the Zenoh session namespace, so it **may** contain a wildcard — but **only on "pure observe" primitives**. Reason: the service client uses `QueryTarget::All` with first-reply-wins (`core/src/client.rs`); `namespace="*"` would fan the request out to **all robots**, which is dangerous for a non-idempotent service / action.

| Primitive | `NamespaceMode` | Note |
|---|---|---|
| topic subscribe `create_subscriber_for` | **Allow** | `*/aorta/{group}/pub/{topic}` fan-out |
| action feedback / status subscribe | **Allow** | observe multi-robot progress |
| **service client** | **Reject** | must hit exactly one robot |
| **action send_goal / cancel_goal / get_result** | **Reject** | same |
| all publish/owner surfaces (cloud rarely uses these) | **Reject** | cannot publish to / claim ownership of a glob |

A multi-segment, non-wildcard namespace (`region/site/robot_a`) is allowed on any primitive; client-supplied wildcard namespaces are observe-only. If a "broadcast RPC" is ever truly needed, it should be an **explicit new API with multi-reply semantics**, not folded into the default `service_client_for`.

#### 4.2.1 Server-side all-namespaces services

A service server may explicitly opt in to serving every caller namespace. The
server declares one queryable at `**/aorta/{group}/srv/{service}`. This is a
**service scope**, not client fan-out: clients still target one concrete
namespace, while one server queryable accepts those independently addressed
requests. Zenoh's zero-segment `**` match also lets the same queryable serve a
namespace-free caller at `aorta/{group}/srv/{service}`.

The opt-in is mutually exclusive with both application-level and Zenoh session
namespaces on the server. An application-level namespace would conflict with
the leading `**`; a Zenoh session namespace would physically prefix declarations
and filter inbound traffic, preventing a fleet-wide queryable from receiving
other namespaces. Service startup therefore fails with `invalid_argument` when
the session config contains a non-empty `namespace`.

`ServiceRequest::caller_namespace()` reverse-parses the arriving query key and
returns the transport-level routing namespace, including multi-segment values,
or `None` for a namespace-free caller. This value is not an authenticated
identity: it is trustworthy only when router ACLs pin the permitted SN prefix
per link.

The single-provider owner token remains unchanged. Cloud service instances are
namespace-free, so they claim the existing literal
`aorta/{group}/sys/svc_owner/{service}/{node}` key. Reserving a service prefix
(for example `srv/remote/**`) for cloud-only channels is a deployment
convention; no wildcard owner token is needed.

### 4.3 Reverse parse (touch-point #1, mandatory companion change)

`key_suffix_from_full_key` (`core/src/key_expr.rs:297`) currently strips only `aorta/{group}/`. Re-verification showed that it is used in **multiple** places, not just to extract the callback topic:

- callback topic extraction (`subscriber.rs:1175/1256`);
- subscriber init schema-prefetch (`subscriber.rs:1119-1127`);
- header-layout refresh after a miss (`subscriber.rs:1200-1206`).

With a `{ns}/` prefix these would all fail to match. A **single-point change** covers them all:

```rust
/// Returns (source_namespace, suffix). Anchors on ROOT_PREFIX, so it naturally
/// supports any namespace depth and the concrete segment of a received sample
/// (the `*` is already concrete on receipt).
pub fn split_full_key(full_key: &str) -> Option<(Option<String>, String)> {
    // Shape {ns...}/aorta/{group}/{suffix}: locate the first segment equal to
    // "aorta", everything before = namespace (may be empty), the next segment =
    // group, the rest = suffix.
}
```

- Constraint: no namespace segment may equal `aorta` (a robot SN never uses this value; `with_namespace` rejects it), otherwise the anchor is ambiguous.
- `source_namespace` is surfaced to the callback via `MessageContext.source_namespace: Option<String>` (§5.4). On a fan-out (`*`) subscription the callback uses it to tell robots apart.

### 4.4 schema-by-hash path (touch-point #2, P1)

- **Producer side (robot) unchanged**: `start_schema_queryables` still declares `aorta/sys/schema/**` (`schema_registry.rs:315`); the robot's Zenoh namespace auto-wraps it to `{SN}/aorta/sys/schema/**`. **Never** namespace the producer side or you break existing deployments.
- **Consumer side (cloud) becomes namespace-aware**: `KeyExprBuilder::schema_key` changes from static to namespace-capable; `schema_hash_from_full_key` (`:967`, `strip_prefix("aorta/sys/schema/")`) strips a leading `{ns}/` first. Only needed once P1 enables "resolve schema by attachment hash".

### 4.5 `SchemaRegistry` vs per-call namespace (review MUST FIX)

Key constraint: `SchemaRegistry` holds the **Node's own** key_builder at Node construction time (`node.rs:191` → `SchemaRegistry::new(session, key_builder)`). A per-call namespaced `Client` / subscriber **does not** change the registry's query key — so "a namespaced key_builder automatically makes schema queries carry `{SN}/`" is **false**.

- **P0 (pre-shared schema)**: cross-ns subscribers must **disable `prefetch_schema_on_init`** (the default is `true`, `subscriber.rs:316`) or pass a `header_layout_hint`; otherwise the init prefetch queries the **wrong (un-namespaced) key**, and on a miss the hot path falls back to the default header layout (§6 degradation). The `_for` subscriber methods skip init prefetch for cross-ns subs by default.
- **P1 (runtime resolution)**: let schema resolution use a **namespaced key_builder** (passed per-call, or registry supports per-namespace resolution), and key the schema cache by `(namespace, hash/key_suffix)` — today the cache key has no namespace (`schema_registry.rs:440-465`), so cross-ns would collide.

---

## 5. Public API shape

> **Shape = per-call target parameter** (no bound handle). Vocabulary is uniformly **namespace** (matching Zenoh), not `sn`.

### 5.1 Core: one Node + per-call namespace argument

```rust
let node = Node::with_zenoh_config("cloud-fleet-svc", "default", cloud_cfg).await?;

// subscribe (you hold / drop the Subscriber)
let sub  = node.create_subscriber_for("robot_a", "demo/chatter", cb).await?;

// service: short-lived client; a wildcard namespace is rejected
let resp = node.service_client_for("robot_a", "demo/echo")?.call(req).await?;

// action: held for the goal's lifetime
let ac   = node.action_client_for("robot_a", "demo/fibonacci")?;
let goal = ac.send_goal(g).await?;

// fleet fan-out: only the subscribe side allows "*"
let all  = node.create_subscriber_for("*", "demo/chatter", cb).await?;
// node.service_client_for("*", ...)   // -> Err: request/reply may not use a wildcard namespace
```

Each `_for` internally does `self.key_builder.clone().with_namespace(target)?` then the **existing** creation path (the service client uses the existing `Client::builder().key_builder(kb)` injection point `client.rs:629`; the subscriber / action client thread the namespaced key_builder into their existing creators).

**Namespaced consumer surface (P0 covers the subset the cloud needs)**:

| Primitive | New methods | namespace wildcard |
|---|---|---|
| subscribe | `create_subscriber_for`, `create_subscriber_typed_view_for::<M>`, `create_subscriber_typed_retained_for::<M>` (+ `_with_options` as needed) | Allow |
| service client | `create_client_for`, `create_client_typed_for::<Req,Res>` (+ `_with_options`) | Reject |
| action client | `ActionClient::<T>::new_for` (the generic action client is inherently typed; feedback/status subscribe internally Allow, goal/cancel/result Reject) | see left |

> Producer surfaces (`create_publisher` / `create_service` / telemetry / liveliness / owner) get **no** namespace variant.

### 5.2 `RemoteNode` — optional future sugar (not in P0)

If a "many calls to one robot in a burst" use case appears, add an ephemeral view `node.namespace(target)` exposing the same methods without the `_for` suffix. Use-and-discard, thin view, no separate session/lifecycle.

### 5.3 Fleet fan-out subscribe & "source namespace"

`MessageContext` has no source field today (`subscriber.rs:64+`: topic / receive_stamp_ns / priority / publisher_node / sequence …). Add `source_namespace: Option<String>`, populated by the §4.3 reverse parse. In P0 this lands in Core + the Rust SDK `MessageContext`; the C-ABI / C++ / Python equivalent fields land in their phases (§9).

### 5.4 `Target` type (forward-compatible with group wildcards, but no silent ignore)

```rust
pub struct Target {
    namespace: String,        // "robot_a" / "*"; wildcard allowed on observe, rejected on request/reply
    // group axis reserved for the future; v1 has no public setter (see below)
}

impl Target {
    pub fn namespace(ns: impl Into<String>) -> Self;   // Target::namespace("robot_a")
}
impl From<&str>   for Target { /* whole string = namespace */ }
impl From<String> for Target { ... }
```

**Review MUST FIX**: v1 does **not** provide a `.group(...)` setter — group wildcards are unimplemented, so a setter that's silently ignored would be a footgun. When group wildcards land later, add `.group()`; the `impl Into<Target>` signature stays stable.

---

## 6. Cross-namespace schema resolution & degradation

Three paths, ordered by dependence on the ns prefix:

1. **Pre-shared / local schema** (P0 main path): the cloud knows the `.fbs` at compile time (same registry source as the robot) and decodes with the generated typed wrapper, **independent of** robot-side schema queries. Combined with the §4.5 "cross-ns subs skip prefetch / pass a layout hint" rule, this works.
2. **`schema_by_key`** (P1): needs the §4.5 namespaced registry + namespace-keyed cache.
3. **attachment-hash lookup** (P1): `{SN}/aorta/sys/schema/{hash}`, depends on §4.4.

**Unknown attachment-hash degradation (must be specified, review MUST FIX)**: when the cloud receives a message with a hash but has no matching local schema/layout —

- the **payload is still delivered to the callback** (typed decode continues with the pre-shared schema; with no pre-shared schema either, raw bytes);
- **header-derived context degrades**: `publisher_node` / `sequence` (which depend on the header layout) are parsed with the **default layout** when the layout is unknown, so they may be empty/inaccurate;
- **dedup / reorder** that depend on `sequence` should **degrade to no-dedup / no-reorder** rather than silently misjudge;
- P0 recommendation: cross-ns subs **disable dedup/reorder or pass an explicit layout hint** to avoid this uncertainty.

---

## 7. Fleet discovery

A namespace-free session discovers across SNs for free, via wildcards:

- liveliness `*/aorta/{group}/sys/alive/**`; or vital telemetry `*/aorta/{group}/sys/telemetry/vital/**`.
- extract `source_namespace` from the received full key via §4.3 `split_full_key`, maintain a set of "active namespaces", and subscribe/call them per-call on demand.
- a formal full-key→namespace parse for the discovery helper (incl. multi-segment / `**`) is P2.

---

## 8. ACL / security & lifecycle

### 8.1 ACL (needs an addition to `Aorta_Router_ACL_Minimal_Samples.md`, review MUST FIX)

The existing ACL samples only cover schema keys (§A/B/C of that doc). This proposal needs a new section "cloud single-session cross-namespace consumption" with a **per-primitive minimum authorization surface**, not a blanket `*/aorta/...`:

- list `cloud_link` egress/ingress separately;
- per-primitive allow surface: `*/aorta/{group}/pub/**` (topic subscribe), `{SN}/aorta/{group}/srv/**` (service), `.../act/**` (action goal/feedback/result/status), `.../sys/alive/**` and `.../sys/telemetry/vital/**` (discovery);
- scope by namespace range (SN allowlist / region prefix), default deny.

### 8.2 Lifecycle / cleanup semantics

- **Robot offline**: cross-ns subscriptions simply **stop receiving** once the namespace goes inactive, with no error; service/action calls return via the RPC timeout (dual-guard).
- **Fan-out churn**: `*` subscriptions automatically gain/lose matching sources as robots come and go (Zenoh routing); the cloud need not rebuild the subscription.
- **Resource release**: each `Subscriber` / `ActionClient` holds its own declarations; **drop = undeclare**. The cloud's "active namespace table" should proactively drop handles for robots it sees go offline, to avoid leaks.
- These semantics must be made explicit and tested in the implementation.

---

## 9. Multi-language SDK propagation (cloud = Rust)

- **P0 = Core(Rust) + Rust SDK**: §4 + §5 land in Core (single source of truth); the Rust SDK passes through the `_for` methods, `Target`, and `MessageContext.source_namespace`. The cloud is then usable end-to-end.
- **C-ABI (later phase)**: add `_for` variants taking a `namespace` (`const char*`) argument alongside the existing subscriber / service-client / action-client creators; add `source_namespace` to the C-ABI message context (today `ffi/c_api/mod.rs` context lacks it).
- **C++ SDK (later phase)**: pass through `_for` + `Target` + the context field; **and MUST FIX** the `BuildActionStatusTopic` hand-written-key bypass (§3) — route it through the Core key_builder / make it namespace-aware.
- **Python SDK (later phase)**: ctypes-over-C-ABI, available once the C-ABI lands; consistent with the parity principle in `spec/Aorta_Python_SDK_Design_Proposal.md`.

DRY: namespace validation/parse semantics live only in Core; FFI/SDKs only pass through.

---

## 10. Backward compatibility

- When `namespace = None`, `build_grouped*` (the new `ns_mode` arg takes the `None` branch and prepends nothing), `split_full_key` (`source_namespace = None`), and `schema_key` (static behavior preserved) produce output **identical to the current behavior, byte for byte**.
- No change to any robot-side code or wire format; the producer-side schema queryable stays `aorta/sys/schema/**`.
- Existing Node/Client/Subscriber constructor signatures unchanged; the additions are **incremental** API (`with_namespace` + the `_for` variants + `Target` + `MessageContext.source_namespace`).
- The "single keyhole" claim is corrected to "main keyhole + two exceptions" (§3).

---

## 11. Test plan (TDD: Red → Green → Refactor)

1. `key_expr` unit tests:
   - `with_namespace("robot_a").topic_key_sub("x")` → `robot_a/aorta/default/pub/x`;
   - service / action goal primitives + a wildcard namespace (`*` / `**`) → **error**; topic subscribe + `*` / `**` → ok;
   - multi-segment namespace `region/site/robot_a` validates per-segment; an illegal-char segment / a segment equal to `aorta` → error;
   - `split_full_key("robot_a/aorta/default/pub/x")` → `(Some("robot_a"), "pub/x")`; `split_full_key("region/site/robot_a/aorta/default/pub/x")` → `(Some("region/site/robot_a"), "pub/x")`; no-ns regression `(None, "pub/x")`;
   - `namespace=None` byte-identical to current across all paths.
2. Integration (local test router):
   - bring up a "robot" Node (zenoh ns=`robot_a`/`robot_b`) publishing a topic / serving a service / action;
   - a single cloud session does `create_subscriber_for("robot_a")` / `service_client_for` / `action_client_for`, asserting receipt / call success / feedback+result;
   - `create_subscriber_for("*")` fans out, asserting `source_namespace` distinguishes robot_a/robot_b;
   - `service_client_for("*")` returns an error (no broadcast RPC).
3. schema:
   - path 1 (pre-shared + prefetch off) must pass;
   - **unknown-hash degradation**: construct a hash with no local schema, assert the payload still reaches the callback, header context uses the default layout, and dedup/reorder degrade;
   - P1: `schema_by_key` / by-hash cross-ns resolution correct; the cache does **not** bleed across namespaces (same key_suffix + different ns must not hit each other).
4. `MessageContext.source_namespace` propagates correctly in Core + the Rust SDK.

---

## 12. Phased delivery

- **P0 (Rust end-to-end usable)** ✅ **implemented**:
  - Core: §4.1 (namespace + `NamespaceMode`), §4.2 (wildcard rules), §4.3 (`split_full_key` + `source_namespace`), the P0 part of §4.5 (namespaced subs skip init schema-prefetch), §5.4 `Target`, `MessageContext.source_namespace`;
  - Rust SDK: `Node::create_subscriber_for[/_typed_view/_typed_retained/_with_options]`, `create_client_for[/_typed/_with_options]` (fail-fast wildcard-namespace rejection), `ActionClient::new_for`, re-export `Target`;
  - Tests: 12 `key_expr` unit + 3 core integration (concrete / `*` fan-out / service) + 2 SDK surface (fail-fast) + a runnable two-robot action demo. fmt + clippy hard gate pass.
  - Note: the unknown-hash degradation (§6) test + cross-ns schema_by_key/by-hash fold into P1 (runtime schema resolution).
- **P1 (runtime cross-ns schema)**: §4.4 + the P1 part of §4.5 (namespaced registry + namespace-keyed cache) + paths 2/3 + the P1 §11.3 cases.
- **P2 (discovery + ACL + multi-language)**: §7 discovery helper, §8 ACL samples & lifecycle spec, §9 C-ABI/C++ (incl. the action-status keyhole fix)/Python propagation.

Each phase is independently verifiable and independently reviewable.

---

## 13. Decision log (all settled)

1. **API shape**: per-call `_for` argument is the core (§5.1); `RemoteNode` is only future sugar (§5.2).
2. **group axis**: v1 inherits the base group; `Target` exposes only `namespace` and provides **no** `.group()` (no silent ignore), added later (§5.4).
3. **schema strategy**: P0 only pre-shared schema (§6 path 1) + a specified unknown-hash degradation; runtime lookup (schema_by_key / by-hash) is P1, incl. a namespaced registry + namespace-keyed cache.
4. **namespace validation**: `with_namespace` normalizes per-segment; the splice point uses an independent `NamespaceMode` — Allow wildcards on observe, Reject on request/reply and publish/owner (§4.1–4.2).
5. **reverse parse**: single-point change `key_suffix_from_full_key` → `split_full_key` returning `(source_namespace, suffix)`, anchored on `aorta` (supports multi-segment / `**`), covering callback / prefetch / layout-refresh call sites (§4.3); `MessageContext.source_namespace` carries it back.
6. **naming (final)**: `Target::namespace("robot_a")`; `create_subscriber_for` / `service_client_for` (`create_client_for` in the SDK) / `action_client_for` (incl. typed); `KeyExprBuilder::with_namespace`; C-ABI argument `namespace` (`const char*`).
7. **cloud SDK = Rust**: P0 = Core + Rust SDK; C-ABI/C++/Python deferred (§9/§12).

---

## Appendix: why not the alternatives (for reference)

- **One namespaced session per SN (session pool)**: zero Aorta change, usable today, but one session/connection per live robot — resources scale linearly with fleet size; and since the Zenoh ns is non-wildcard, a single session cannot fan out across SNs for discovery. A viable interim before Option B lands.
- **Hand-rolled raw Zenoh keys + payloads**: loses typed/schema/header handling, defeating the "use the Aorta SDK" goal.
