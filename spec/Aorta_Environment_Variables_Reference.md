# Aorta Environment Variables Reference

> Source of truth: this document was written by reading the source code.
> When code and this doc disagree, **code wins** — update this file alongside any
> behavior/API change.

## How to read this document

Aorta variables are classified by **the layer that consumes them**, not by the SDK
language. This is deliberate: a variable read in **L1 Rust Core** is honored by
*every* SDK (Rust, C++, Python) for free, because all SDKs open their session
through Core (`L0 Zenoh → L1 Core → L2 C-ABI → L3 SDK`). A variable only appearing
inside an example or a benchmark script is **not** language-specific to that
example — do not mistake "shows up in `cpp/examples/`" for "C++ only".

### Consumption model (important)

- Almost all `AORTA_*` variables are read in Rust via `core/src/runtime_env.rs`
  (`runtime_env::var` / `var_os`).
- In **hermetic test builds** (`cfg(test)`), env reads are **disabled** unless
  `AORTA_TEST_ALLOW_ENV` is set. This keeps the test suite from picking up the
  developer's ambient environment. Production (non-test) builds always read env.
- **Boolean parsing** is *mostly* uniform but not byte-identical across modules.
  The common convention:
  - **truthy**: `1`, `true`, `yes`, `on` (some sites also accept `enable`/`enabled`)
  - **falsy**: `0`, `false`, `no` (some sites also accept `off`)
  - The per-variable "Default" column below is authoritative for the
    unset/unparsable case (most switches **fail open** = default on).

---

## 1. Trace (tracing subsystem) — `core/src/trace/`

Trace has two independent config surfaces: **gating** (which spans get recorded)
and **sink/drain** (how they are persisted). The grammar is the tracing
subsystem's *own* — it is **independent of** `AORTA_LOG` and `AORTA_TELEMETRY_*`.

### 1.1 Gating / master switch

| Variable | Purpose | Default | Source |
|---|---|---|---|
| `AORTA_TRACE` | Trace gate spec, comma-separated (see grammar below). | unset/empty ⇒ **`always`** (master on, T1=Info floor) | `trace/runtime.rs:110`, `trace/scope.rs:306` |
| `AORTA_TRACE_AUTO_INIT` | Whether `Session::open` auto-runs `trace::init()` (spawns drain thread + sink). | **on** (fail-open); set `0`/`false` to keep manual control (call `aorta_trace_init` yourself) | `trace/runtime.rs:69` |
| `AORTA_TRACE_THREAD_TIME` | Capture per-thread CPU time on spans. | **off**; truthy to enable | `trace/runtime.rs:89` |

**`AORTA_TRACE` grammar** (`trace/scope.rs:266` `apply_spec`):

- `off` — turn the master switch off.
- `info` / `debug` / `trace` (bare level) — set the floor for **all** scopes and turn the master on.
- `scope=level` — set one scope's threshold and turn the master on.
- `scope=off` — silence **just** that scope; does **not** touch the master switch.
- Tier aliases: `always` = Info (T1), `standard` = Debug (T2), `verbose` = Trace (T3).
  `error`/`warn` sit above `always`, so they always pass at any tier.

### 1.2 Sink — on-disk container files (`*.atrc`) — `SinkConfig::from_env` @ `trace/sink.rs:62`

| Variable | Purpose | Default |
|---|---|---|
| `AORTA_TRACE_DIR` | Output directory for trace files. | `$TMPDIR/aorta-trace` |
| `AORTA_TRACE_MAX_BYTES` | **Total** byte budget; oldest files are FIFO-evicted past it. | **2 GiB** |
| `AORTA_TRACE_FILE_MAX_BYTES` | Per-container-file rotation size (LTTng `--tracefile-size` analog). | **8 MiB** |
| `AORTA_TRACE_FILE_MAX_SECS` | Per-container-file rotation age (seconds; clamped in `FileSink::new`). | **300 s** |
| `AORTA_TRACE_RUN_ID` | Run identifier stamped into chunk provenance. | random UUID |
| `AORTA_NODE_ID` | Node identifier; also derives `host_clock_domain_id`. Shared with telemetry. | auto-derived (see §7) |

### 1.3 Drain — ring → chunk flush cadence — `DrainConfig::from_env` @ `trace/sink.rs:705`

| Variable | Purpose | Default |
|---|---|---|
| `AORTA_TRACE_SWEEP_MS` | Ring sweep cadence. | **5 ms** |
| `AORTA_TRACE_CHUNK_RECORDS` | Records-per-chunk flush threshold. | **16384** |
| `AORTA_TRACE_CHUNK_AGE_MS` | Chunk flush age = **in-process crash-loss window**. | **1000 ms** |

> If the sink cannot open its directory, the process **does not** fail to start:
> it degrades to gate-only (histograms still record; spans accumulate in rings but
> are not persisted) and prints one warning. A one-shot process flushes its tail
> chunk via an `atexit` hook.

---

## 2. Logging — `core/src/logging.rs` (+ C++ `cpp/src/internal/common.cc`)

| Variable | Purpose | Default |
|---|---|---|
| `AORTA_LOG` / `RUST_LOG` | Log filter directive (`AORTA_LOG` takes priority). | unset ⇒ built-in |
| `AORTA_LOG_FORMAT` | Log output format. | built-in |
| `AORTA_LOG_STDERR` | Emit logs to stderr. | built-in |
| `AORTA_LOG_AUTO_INIT` | Auto-initialize logging on first use. | **on** (fail-open) |
| `AORTA_LOG_FILE` | Log file path. | unset |
| `AORTA_LOG_FILE_DIR` | Log directory. | unset |
| `AORTA_LOG_QUEUE_CAPACITY` | Async log queue capacity. | built-in |
| `AORTA_LOG_NODE_CONTEXT_POLICY` | Node-context injection policy. | built-in |
| `AORTA_LOG_ORIGIN_FIELDS` | Include origin fields in log records. | built-in |
| `AORTA_LOG_TOPIC_ENABLED` | Forward logs as a `/log` topic. | built-in |
| `AORTA_LOG_TOPIC` | Topic name for log forwarding. | built-in |
| `AORTA_LOG_TOPIC_QUEUE_SIZE` | Log-topic forwarding queue size. | built-in |
| `AORTA_LOG_RATE_LIMIT_MS` | (**C++**, `common.cc:69`) Log rate-limit window. | built-in |

---

## 3. Telemetry — `core/src/telemetry/` (on by default)

| Variable | Purpose | Default |
|---|---|---|
| `AORTA_TELEMETRY_NODE_IDENTITY` | Node-identity telemetry switch. | **on** |
| `AORTA_TELEMETRY_RESOURCE_INVENTORY` | Resource-inventory telemetry switch. | **on** |
| `AORTA_TELEMETRY_RUNTIME` | Runtime telemetry switch. | **on** |
| `AORTA_TELEMETRY_RUNTIME_HEALTH` | Runtime-health telemetry switch. | **on** |
| `AORTA_TELEMETRY_HEARTBEAT_MS` | Heartbeat interval for node-identity + resource-inventory snapshots. | **30000** (30 s) |
| `AORTA_TELEMETRY_RUNTIME_HEARTBEAT_MS` | Runtime-**metrics** telemetry heartbeat interval (publish cadence for the `AORTA_TELEMETRY_RUNTIME` runtime-metrics publisher, `RuntimeMetricsTelemetryPublisher` @ `telemetry/runtime.rs:290`). **Not** the runtime-health cadence — runtime-health (`AORTA_TELEMETRY_RUNTIME_HEALTH`) beats at `AORTA_TELEMETRY_HEARTBEAT_MS` (30 s) instead. | **5000** (5 s) |
| `AORTA_TELEMETRY_INVENTORY_HEARTBEAT_MS` | **Legacy / no-op** — referenced only in test cleanup; inventory now uses `AORTA_TELEMETRY_HEARTBEAT_MS`. | — |

> **Heartbeat floor**: any operator-supplied heartbeat below `MIN_HEARTBEAT_INTERVAL_MS`
> = **100 ms** is clamped (with a warning) to prevent a fat-fingered config
> (`=1` instead of `1000`) from triggering fleet-wide telemetry self-DoS.
> Unset/invalid values use the trusted default verbatim (never clamped).

---

## 4. Executor / Flow runtime tuning — `core/src/executor.rs`, `core/src/flow/scope.rs`, `core/src/flow/offload.rs`, `core/src/ffi/c_api/mod.rs`, `cpp/include/aorta/lane_runtime.h`

| Variable | Purpose | Default |
|---|---|---|
| `AORTA_EXECUTOR_MULTI_INGRESS` | Enable multi-ingress mode. | **off** (single ingress) |
| `AORTA_EXECUTOR_MULTI_INGRESS_CAPACITY` | Per-ingress queue capacity. | code default (`.max(1)`) |
| `AORTA_EXECUTOR_MAX_QUEUED_EVENTS` | Admission cap on queued events. | **0** = disabled/unbounded unless set |
| `AORTA_EXECUTOR_MAX_QUEUED_BYTES` | Admission cap on queued bytes. | **0** = disabled/unbounded unless set |
| `AORTA_EXECUTOR_ADMISSION_OBSERVABILITY` | Emit admission/backpressure observability. | **off** |
| `AORTA_EXECUTOR_CROSSBEAM_ENABLE` | Force-enable crossbeam queue path. | code default |
| `AORTA_EXECUTOR_CROSSBEAM_DISABLE` | Force-disable crossbeam queue path. | code default |
| `AORTA_ASYNC_EXECUTOR_MAX_IN_FLIGHT` | Max in-flight ops for the C-ABI async executor. | **32** (`.max(1)`) |
| `AORTA_CALLBACK_STALL_MS` | Callback stall-watchdog WARN threshold (`executor.rs:1044` `callback_stall_threshold_ns`): a dispatched callback that holds the dispatch thread past this many ms gets a rate-limited WARN (at most one per 5 s process-wide) suggesting `offload()`. Read via `runtime_env::var` and parsed once per process (`OnceLock`) — changing it after first use has no effect. Non-numeric values fall back to the default. | **500 ms** |
| `AORTA_OFFLOAD_THREADS` | Cap on the lazily-spawned offload pool's worker count (`flow/offload.rs:118`). Raw action `execute` handlers do not use this pool (each goal gets a dedicated thread, bounded by the server's `max_concurrency`), so it need not be raised for long-running goals. Read via `runtime_env::var` once per process, at first offload use (`OnceLock` pool init) — changing it later has no effect. Values `<= 0` or non-numeric are ignored (default applies). | `std::thread::available_parallelism()` (fallback **4**) |
| `AORTA_CPP_LANES` | (**C++**, `lane_runtime.h` `rollback_via_env`) Process-level rollback of the whole C++ lane plane: set to **exactly `0`** and every lane-enabling constructor (`LaneRuntime::SingleThread` / `MultiThread` / `FromEnv`) returns `Disabled()` — every lane-backed C++ surface transparently falls back to the core queue dispatch path. Read via `std::getenv` at each constructor call (not `runtime_env`). Any value other than the exact string `0` leaves lanes enabled. | lanes **enabled** |
| `AORTA_FLOW_STALL_WARN_MS` | Flow stall-WARN threshold (`flow/scope.rs:1315` `stall_warn_threshold_ms`): after a scope is cancelled, subtree children that still haven't departed past this many ms get a one-shot WARN naming the scope (mid-poll stalls or long shielded cleanup). Read via `std::env::var` and cached once per process (`OnceLock`) — changing it after first use has no effect. | **200 ms** |

---

## 5. RPC — `core/src/client.rs`, `core/src/service.rs`

| Variable | Purpose | Default |
|---|---|---|
| `AORTA_RPC_ALLOW_RETRY` | Opt-in to automatic RPC retries. **Off by default** because a non-idempotent service (step counters, ID allocation, gripper actions) would be invoked 2+ times for one logical `call()`. Set truthy only when the target is provably idempotent. | **off** |
| `AORTA_RPC_DEADLINE` | Deadline-over-RPC kill-switch. Falsy (`0` / `false` / `off`) disables **both halves independently, per process**: on the client, wire stamping of the remaining budget (`client.rs:1043` `rpc_deadline_stamping_enabled`); on the server, the expired-in-queue drop gate **and** the action goal-budget seeding (`service.rs:1592` `rpc_deadline_drop_enabled`). Any other value (including unset) = enabled. Read via `std::env::var` and cached once per process (`OnceLock`). A fleet can roll the behavior back on either side without touching the other. | **on** |
| `AORTA_RPC_ENFORCE_SINGLE_PROVIDER` | Enforce a single provider per service. | **off** |
| `AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS` | Window for detecting multiple replies (multiple providers answering). | **5 ms** |

---

## 6. Schema — `core/src/schema.rs`, `core/src/schema_registry.rs`

| Variable | Purpose | Default |
|---|---|---|
| `AORTA_SCHEMA_ATTACH` | Schema-hash attach policy: `always` / `never` (unknown ⇒ `never`). Resolves against the node default when `Inherit`. | **never** |
| `AORTA_SCHEMA_CONFLICT_ACTION` | On schema-hash conflict: `warn` / `reject` (case-insensitive; invalid ⇒ `warn` + warning). | **warn** |
| `AORTA_SCHEMA_QUERYABLE` | Expose the schema queryable. | **true** |
| `AORTA_SCHEMA_QUERY_TIMEOUT_MS` | Schema query timeout (must be `> 0`). | **200 ms** |
| `AORTA_SERVICE_SCHEMA_READY_TIMEOUT_MS` | Service-schema readiness wait timeout. | **500 ms** |
| `AORTA_SERVICE_SCHEMA_READY_POLL_MS` | Service-schema readiness poll interval. | **50 ms** |

---

## 7. Session, addressing & identity — `core/src/{session,node,key_expr,telemetry/identity}.rs`

These are **L1 Core** variables → honored by **all SDKs** (Rust / C++ / Python).

| Variable | Purpose | Default |
|---|---|---|
| `ZENOH_SESSION_CONFIG_URI` | **Aorta-specific** Zenoh session config: a file path or `file://` (incl. `file://localhost/`) URI. Read first; **takes priority over** `ZENOH_CONFIG`. Empty/whitespace ⇒ treated as unset. (`Session::SESSION_CONFIG_ENV` @ `session.rs:220`, loaded in `config_from_env` @ `session.rs:253`.) | unset ⇒ fall through |
| `ZENOH_CONFIG` | Native Zenoh config path (`zenoh::Config::DEFAULT_CONFIG_PATH_ENV`); fallback when `ZENOH_SESSION_CONFIG_URI` is unset. | unset ⇒ local peer default |
| `AORTA_GROUP` | The `{group}` segment of the key format `aorta/{group}/...`. Read in Core (`key_expr.rs:734`; Core re-resolves it, ignoring the constructor-supplied group, at `node.rs:168`/`:177`) **and independently** by the C++ SDK (`cpp/src/internal/common.cc:90` → `cpp/src/node.cc:102`) and every tool (CLI / record / sysmon). Not Core-only. | `default` (`DEFAULT_GROUP`) |
| `AORTA_LIVELINESS` | Publish liveliness tokens. | **enabled** |
| `AORTA_NODE_ID` | Explicit node-ID override (also consumed by the trace sink, §1.2). | auto-derived |
| `AORTA_HOST_NAME` | Host name override for Core telemetry/identity; else falls back to `HOSTNAME` → `COMPUTERNAME` (`telemetry/identity.rs:14-21`). Note `HOSTNAME` has a **second, independent** read site in aorta-cli `ctx set` (with a `HOST` fallback, not this chain) — see §9. | auto |
| `AORTA_HOST_ID` | Host ID override. | auto |
| `AORTA_KEY_NORMALIZE_CASE` | **Deprecated / no-op.** Key-case normalization was relaxed; no non-test read site remains in `core/src`. Listed for historical awareness only. | n/a |

---

## 8. Publisher header / payload — per-SDK publisher layer

> **Not** L1-Core-consumed: there is no read site in `core/src`. These are read
> at each SDK's own publisher layer (parallel per-SDK reads, not inheritance
> through Core) — and the two variables do **not** have the same reach:

| Variable | Read by | Purpose | Default |
|---|---|---|---|
| `AORTA_HEADER_INCLUDE_PUBLISHER_NODE` | **All SDKs** — Rust `rust/src/publisher.rs:315`/`:343`, C++ `cpp/src/publisher.cc:258`, Python `python/aorta/publisher.py:22`/`:29` | Include the publisher node name in the message header. Falsy = `0`/`false`/`no`/`off`. | **true** (include) |
| `AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB` | **Rust SDK only** — read solely at `rust/src/publisher.rs:327`; no read site in C++/Python. The C++ builder-reuse path is governed by a recycle pool capped at 4× `base_capacity` instead, and ignores this var. | Thread-local builder "large capacity" upgrade threshold, in KB (`> 0`). Above it the publisher switches off the TLS fast path. | **2 MiB** (2048 KB) |

---

## 9. Tools — `tools/`

| Variable | Tool | Purpose |
|---|---|---|
| `AORTA_SYSMON_NODE_NAME` | aorta-sysmon | Node name. |
| `AORTA_SYSMON_HOST_NAME` | aorta-sysmon | Host name. |
| `AORTA_SYSMON_HOST_ID` | aorta-sysmon | Host ID. |
| `AORTA_SYSMON_TOPIC_SUFFIX` | aorta-sysmon | Metrics topic suffix. |
| `AORTA_SYSMON_ALERT_TOPIC_SUFFIX` | aorta-sysmon | Alert topic suffix. |
| `AORTA_SYSMON_INTERVAL_MS` | aorta-sysmon | Sampling interval. |
| `AORTA_TUI_STATE_DIR` | aorta-tui | State dir; else `XDG_STATE_HOME` → `HOME`. |
| `AORTA_FLATBUFFERS_ROOT` | aorta-introspect (build) | FlatBuffers schema root for reflection. |
| `HOSTNAME` | aorta-cli | Host name for `source_node = "cli@{host}"` on `ctx set` context writes (`tools/aorta-cli/src/cmd/ctx.rs:488`). **Independent** of the Core telemetry/identity chain (§7) — this read site does **not** consult `AORTA_HOST_NAME`; falls back to `HOST`, then `"unknown"`. |
| `HOST` | aorta-cli | Fallback host name for `source_node = "cli@{host}"` (tried **after** `HOSTNAME`, **before** `"unknown"`) on `ctx set` context writes (`tools/aorta-cli/src/cmd/ctx.rs:488-490`). Distinct from the Core telemetry identity chain (`telemetry/identity.rs:14-21`), which does **not** read `HOST`. |

> **`HOSTNAME` has two independent read sites.** The Core telemetry/identity
> fallback (§7, `AORTA_HOST_NAME` → `HOSTNAME` → `COMPUTERNAME`) and the aorta-cli
> `ctx set` resolver above (`HOSTNAME` → `HOST` → `"unknown"`) are separate: the
> aorta-cli path does not honor `AORTA_HOST_NAME`/`COMPUTERNAME`, and the Core
> path does not honor `HOST`.

---

## 10. Python / FFI loader

| Variable | Purpose |
|---|---|
| `AORTA_CORE_FFI_LIB` | Explicit path to the Aorta C-ABI shared library (`python/aorta/_ffi/loader.py`, `python/tracing.py`). Overrides auto-discovery. |
| `AORTA_HEADER_INCLUDE_PUBLISHER_NODE` | Same semantics as §8; Python reads it independently at `python/aorta/publisher.py:22`/`:29`. |

---

## 11. Build-time (not runtime)

| Variable | Purpose |
|---|---|
| `FLATC` | Path to the `flatc` compiler (`core/build.rs`, `rust/build.rs`, `aorta-introspect`, `regen_telemetry_fb.py`). |
| `CARGO_MANIFEST_DIR` | Cargo build context. |
| `OUT_DIR` | Cargo codegen output dir. |
| `USER` | Read by aorta-introspect's build script (`tools/aorta-introspect/build.rs:70`) as a **fallback** to derive the Bazel output base (`/private/var/tmp/_bazel_{user}`, `/tmp/_bazel_{user}`) when auto-discovering the FlatBuffers checkout. Only consulted when `AORTA_FLATBUFFERS_ROOT` (§9) is unset/invalid; set `AORTA_FLATBUFFERS_ROOT` to bypass entirely. |

> Note: `AORTA_TYPED_WRAPPER` is **not** an environment variable — despite the
> name, it is never read from the environment. It is only the C++ include-guard
> prefix emitted by the wrapper codegen (`message/tools/gen_message_wrapper.py:600`).

---

## 12. Test-only gates (never set in production)

| Variable | Purpose |
|---|---|
| `AORTA_TEST_ALLOW_ENV` | **The** test env gate (`core/src/runtime_env.rs:1`/`:8`): allow `cfg(test)` builds to read real env. Without it, env reads return `None`. |
| `AORTA_TEST_LOCAL_ROUTER` | Force the local-router test path. |
| `AORTA_TEST_TLM2_HEARTBEAT_MS` | Test-only heartbeat override consumed inside `#[cfg(test)]` telemetry tests (`core/src/telemetry/mod.rs:83`/`:89`/`:97`). |
| `TEST_TMPDIR` | Bazel-provided test temp dir (also flags the test path). |
| `AORTA_PERF_LONG` | Run the long-form performance tests. |

> Note: `AORTA_TEST_ALLOW_ZENOH_ENV` / `AORTA_TEST_ALLOW_SCHEMA_ENV` are **not**
> real gates — they have no read site in code; they are only `set` by a test
> helper (`core/test/test_support.rs:94`/`:98`). The single effective gate is
> `AORTA_TEST_ALLOW_ENV`.

---

## 13. Script-level wrappers (benchmark / test_scripts only)

These are **shell-script variables**, not consumed by Aorta code. Each is just
re-exported into the canonical `ZENOH_SESSION_CONFIG_URI` for the spawned
processes — listed so readers don't confuse them with library variables.

| Variable | Re-exported as |
|---|---|
| `AORTA_BENCH_ZENOH_SESSION_CONFIG_URI` | `ZENOH_SESSION_CONFIG_URI` |
| `AORTA_SAMPLY_ZENOH_SESSION_CONFIG_URI` | `ZENOH_SESSION_CONFIG_URI` |
| `AORTA_RPC_ZENOH_SESSION_CONFIG_URI` | `ZENOH_SESSION_CONFIG_URI` |

---

## Cross-references

- Tracing scheduler-layer extension: `spec/Aorta_Scheduler_Tracing_Proposal.md`
- Telemetry design: `spec/Aorta_Telemetry_Design.md`
