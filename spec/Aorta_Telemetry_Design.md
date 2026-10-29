# Aorta Telemetry Design

Last updated: 2026-07-16

> Consolidated from earlier telemetry design drafts (2026-03). The
> identity / inventory / runtime families described here are **shipped**;
> the deferred families (§6) remain design-only. Key-expr layout and
> publication cadence were later restructured by the telemetry tiering work
> (`sys/telemetry/{tier}/...`); `core/src/key_expr.rs` is authoritative for
> the current keys.

## 1. Purpose

This document defines the telemetry semantics for Aorta: which telemetry
feature families exist, which layer owns each family, and the rules every
family must satisfy. It is the scope source of truth for telemetry semantics;
transport-level layout (tiers, keys, cadence) is owned by the tiering doc
referenced above.

## 2. Common Rules

### 2.1 Core-First Ownership

- `core` owns node identity telemetry;
- `core` owns resource inventory telemetry;
- `core` owns runtime metrics telemetry;
- `core` owns correlation event telemetry (deferred family);
- a host-side sidecar owns host/process telemetry only (deferred family).

Rationale: identity, resource ownership, and runtime state are middleware
semantics; duplicating telemetry emitters in SDKs would violate DRY. SDKs and
FFI remain metadata adapters rather than independent telemetry publishers.

### 2.2 Delivery Model

- authoritative in-memory state is owned by the node in `core`;
- `Queryable` returns the exact latest snapshot;
- `put/delete` propagates state transitions;
- latest-snapshot semantics only — telemetry is not a history plane;
- JSON-first V1 payloads for the core-owned families.

### 2.3 Exact-or-Unavailable Rule

Telemetry must not silently invent data. If a field cannot be sourced exactly
from an authoritative source, the family contract encodes it as unavailable,
empty, or omitted — never as a best-effort guess. Examples:

- `connected_router_zids` may be empty until an exact source exists;
- executor metrics may be unavailable when admission observability is disabled;
- runtime counters are never synthesized from tool-side inference.

### 2.4 Join Keys

Primary: `node_id`, `group`. Secondary when available: `host_id`,
`session_zid`, `resource_instance_id`, `schema_hash`.

### 2.5 Cost-Control Rule

Telemetry is a product surface and must be affordable. Unacceptable designs:

- telemetry-only per-message serialization, heap allocation, or locking;
- unbounded in-memory retention;
- duplicated counters when an authoritative core surface already exists;
- high-cardinality payload dimensions without a bounded storage plan.

## 3. Feature Matrix

| Family | Owner | Scope | Status |
| --- | --- | --- | --- |
| Node identity | `core` | Authoritative node/session/process identity | **Shipped** |
| Resource inventory | `core` | Authoritative node-local resource ownership | **Shipped** |
| Runtime metrics | `core` | Node-scoped runtime health snapshot | **Shipped** |
| Activity / edge-rate | `core` later | Traffic/activity summaries beyond minimal runtime | Deferred |
| Host / process sidecar | Sidecar | OS-derived host and process metrics | Deferred |
| Correlation events | `core` later | Causal event stream for traces/debugging | Deferred |

## 4. Shipped Family Contracts

### 4.1 Node Identity Telemetry

Purpose: authoritative node identity; exact `node_id <-> session_zid`
correlation; stable join anchor for later host/process and correlation views.

Contract:

- one node identity payload per node, published by every node in `core`;
- includes SDK/FFI metadata routed through `core` (never independently
  published by an SDK);
- `connected_router_zids` is exact-or-empty, never inferred;
- changes slowly; stays separate from inventory/runtime payloads.

Minimum fields: `telemetry_version`, `sequence`, `timestamp_ns`, `node_id`,
`node_name`, `group`, `host_id`, `host_name`, `process_start_time_ns`, `pid`,
`executable`, `language`, `sdk_name`, `sdk_version`, `session_zid`,
`connected_router_zids`; optional `container_id`, `build_id`, `labels`.

### 4.2 Resource Inventory Telemetry

Purpose: authoritative node-local ownership of publishers, subscribers,
services, and clients; exact topology input without endpoint/admin-space
inference.

Contract:

- node-scoped full inventory snapshot; full snapshot on semantic change;
- per-resource-instance lifecycle tracking; latest snapshot only;
- entity types: publishers, subscribers, services, clients;
- per-entry fields: `resource_instance_id`, `resource_name`, `full_key`,
  `schema_hash` when available, `declared_at_ns`, optional labels,
  service/client request/response root type metadata when available.

### 4.3 Runtime Metrics Telemetry

Purpose: exact node runtime health — publisher/service/client runtime
visibility, subscriber exception and pressure visibility, executor/admission
visibility.

Contract:

- node-scoped runtime snapshot; exact-or-unavailable semantics; bounded
  collection cadence (tier cadence per the tiering doc);
- sub-families: publisher, subscriber-exception, service, client,
  executor/admission runtime metrics;
- subscriber runtime metrics are **exception-first**: pressure/exception
  counters only (`duplicate_drop_count`, `out_of_order_drop_count`,
  `executor_drop_count`, `local_drop_count`, `overflow_relay_drop_count`;
  optionally `overflow_relay_enqueue_count`, `out_of_order_count`) — no
  normal-flow counters such as `sample_received_count`;
- runtime telemetry is enabled by default; disabled runtime telemetry must
  not impose continuous hot-path cost;
- minimal runtime telemetry is not activity/edge-rate telemetry; activity
  summaries remain a separate deferred family (§6.1).

## 5. Delivery Order Rationale

Identity → inventory → runtime shipped first, in that order, because exact
joins start with identity, exact topology starts with inventory, and
operational visibility starts with minimal runtime telemetry. Host/process
and causal views (below) depend on those stable join anchors; activity /
edge-rate comes only after the runtime cost model is proven.

## 6. Deferred Families (defined, not implemented)

### 6.1 Activity / Edge-Rate Telemetry

Answers "which topic/service edges are active right now, at what rate".
Boundary for later work: separate from minimal runtime metrics; must not
expand the current subscriber hot path; prefer bounded topic/service activity
summaries over high-cardinality per-edge history.

### 6.2 Host / Process Sidecar Telemetry

OS-derived CPU/memory/IO/process metrics belong in one host-side sidecar, not
in every middleware runtime. Contract: host identity payload; process
identity + metrics payload; sidecar owns OS-derived metrics only; prefer
`node_id` joins once the mapping is known.

### 6.3 Correlation Events

Causal debugging requires an event stream, not another runtime snapshot —
correlation identifiers and event semantics must be designed explicitly
rather than smuggled into the runtime payload. Must remain
exact-or-unavailable. (Tracing covers the intra-process story.)

## 7. Out of Scope

- long-term retention/history (recorder/MCAP territory);
- OTLP-first wire format;
- full distributed tracing backend integration.
