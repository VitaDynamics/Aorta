# Aorta Python SDK — Implementation Status & Parity Gaps (living)

This is the **single source of truth for what the Python SDK implements and where
it diverges from the C++ / Rust SDKs.** Parity basis (per the design proposal
§9): the union of the C++ SDK (`cpp/include/aorta/*`) and Rust SDK (`rust/src/*`)
*public API*, with the **C++ SDK as the primary target**. Updated at each phase.

A "gap" here is intentional and explained — either a capability the C-ABI does
not bridge (Python binds the L2 C-ABI, so it cannot exceed it without a new
`extern "C"` fn), or a feature deferred to a later phase. Gaps are never silent.

Legend: ✅ delivered · ➕ Python superset (has it; the C++ SDK does not) ·
🕳 gap (not in Python; reason given) · ⏳ deferred to a later phase.

---

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 0 | Build foundation (cdylib, loader, ABI guard, errors) | ✅ done |
| 1 | Pub/sub + Python schema-meta codegen | ✅ done |
| 2 | **RPC (service + client)** | ✅ done — gaps below documented; RPC interop wired into CI |
| 3 | **Timer + ExecutionGroup + ContextStore** | ✅ done — auto-executor + Timer + ContextStore + ExecutionGroup + subscriber runtime_stats; 4 concurrency/lifecycle fixes applied after review |
| 4 | **Action (client + server, 8-fn codec)** | ✅ done — live roundtrips, 100% cov; review findings fixed |
| 5 | **Hardening / packaging / coverage / docs** | ✅ done — `SubscriberOptions`, explicit `Executor`, action tails (`from_persisted`, info-only server), `py.typed` + per-arch `py_wheel`, consolidated parity summary; 354 tests, 100% line+branch |

ABI: `AORTA_ABI_VERSION = 4` (1→2 `aorta_node_get_group` P1; 2→3
`aorta_service_request_reply_error` P2; 3→4 the five Phase-3 bridge fns
`aorta_timer_is_running`, `aorta_timer_get_period_ns`, `aorta_ctx_is_key_declared`,
`aorta_ctx_declared_prefixes`, `aorta_subscriber_runtime_stats`). Phase 5 added NO
new C-ABI symbol — `SubscriberOptions`, `Executor.get_admission_stats`, and the
info-only server all bind functions/structs that already existed — so the ABI
stays at v4. Only the Python loader guards this version; the C++/Rust SDKs do not
read it, so each bump is additive and safe.

---

## Phase 2 — RPC: delivered surface

**Service (server)** — `Node.create_service` (raw), `create_service_typed_view`
(decoded request + `TypedServiceResponder`). `Service.name()` / `stats()` (11
real FFI counters) / `close()` + context manager.

**ServiceRequest** — `reply(bytes)` / `reply_error(str)` / `release()` /
`is_valid()`; the handle is consumed on every reply outcome; a `weakref.finalize`
safety net releases an unreplied request when its wrapper is GC'd (so the
deferred-reply pattern — retain the request, reply from another thread later —
works), matching the C++ `ServiceRequest` destructor.

**TypedServiceResponder** — `reply(fill)` (injects a byte-identical AortaHeader
with a per-service monotonic sequence) / `reply_bytes` / `reply_error` / `raw()`
/ `is_valid()`.

**Client (sync)** — `Node.create_client` / `create_client_with_timeout`;
`call(bytes) -> bytes` (blocking), `service_name()`, `stats()` (9 real FFI
counters), `close()` + context manager. `create_client_typed` →
`TypedClient.call(fill)` (injects request header) → `TypedClientResponse`
(`payload()` / `decode()`).

**Options** — `ServiceOptions` (schema_attach_policy=Never, callback_group,
rpc_overflow_policy=FastFailBusy + block_timeout, schema injection fields) and
`ClientOptions` (timeout_ms=5000, schema_attach=Inherit, tri-state
`enforce_single_provider` + `allow_retry`, multi_reply_check_window_ms,
service_schema_hash_hex). `ServiceStats` / `ClientStats` frozen dataclasses.

Crash-safety: the service callback runs on a Zenoh worker thread through ONE
process-global ctypes trampoline; a Python exception is logged and swallowed
(never crosses the `extern "C"`/nounwind boundary), and the request is released
so the caller fails fast. Coverage: 226 tests, 100% line+branch. Cross-process
Python↔Python RPC interop gate passes (`test_scripts/python_rpc_interop.sh`).

## Phase 2 — parity vs C++ / Rust

➕ **Python has, C++ lacks:**
- `ServiceRequest.reply_error(str)` / `TypedServiceResponder.reply_error(str)` —
  a real Zenoh *error* reply. The C++ raw `ServiceRequest` has no `ReplyError`
  (it sends an empty OK payload on typed decode failure). This is Rust-SDK parity
  (`rust/src/service.rs` `reply_error`), enabled by the new C-ABI fn
  `aorta_service_request_reply_error` added this phase.

🕳 **Rust has, Python does not (C-ABI does not bridge it — documented gap):**
- `Client::call_with_timeout(req, ms)` — per-call timeout override. The C-ABI
  `aorta_client_call` takes no timeout; the timeout is fixed at construction via
  `ClientOptions.timeout_ms`. **This matches the C++ `Client` exactly** (C++ also
  has no per-call timeout), so it is not a C++-parity gap — only a Rust-only
  convenience left unbridged.
- `ClientOptions.validate_schema` (bool) and `ClientOptions.request_root_type` /
  `response_root_type` — `aorta_client_options_t` has no such fields. Not exposed
  by the C++ `ClientOptions` either; Rust-only. (Service-side root types ARE
  supported via `ServiceOptions`.)

🕳 **C++ has, Python defers (⏳ to a later phase or covered another way):**
- **Typed RPC schema auto-injection.** C++ `CreateServiceTypedView` /
  `CreateClientTyped` and Rust `create_service_typed_view` /
  `create_client_typed` auto-inject the request/response schema BFBS + hash +
  root types (derived from the template/generic schema types) into the service/
  client options, so cross-language schema discovery (the Zenoh `schema_by_key`
  queryable) sees them. Python's typed factories take a runtime `root_decoder`
  function (no template types to derive from), so they do **not** auto-inject.
  **Workaround available today:** the capability is fully reachable — pass schema
  explicitly via `ServiceOptions(service_schema_bfbs=…, request_root_type=…,
  response_root_type=…)` and `ClientOptions(service_schema_hash_hex=…)`. Only the
  auto-derivation convenience is deferred (revisit in Phase 5 alongside the
  generated-schema examples). *(Phase-2 review, High.)*
- `ServiceOptions.callback_group_handle` — C++/Rust let a service bind to an
  explicit callback group handle; Python currently hardcodes 0. The handle is
  **minted by an `ExecutionGroup`, which is Phase 3** — this option will be wired
  to the Python `ExecutionGroup` when that lands. *(Phase-2 review, Medium.)*
- `TypedServiceResponder.reply_message` / `TypedClient.call_message` — C++/Rust
  accept a generated message *builder object*. Python's typed layer is
  fill-callback + raw-bytes based (flatbuffers-python has no equivalent mutable
  Msg object), so **`reply_bytes(data)` / `Client.call(bytes)` are the Python
  equivalents** for the "I already built/serialized the message" path.
  *(Phase-2 review, Medium.)*
- `CreateServiceTypedRetained` / `TypedRetainedServiceRequest` — the retained
  variant. Covered functionally today by `create_service_typed_view` + the
  deferred-reply pattern (retain the responder/request, reply later). An explicit
  `create_service_typed_retained` factory is **deferred** (tracked, low value
  given the deferred-reply path).
- C++ `TypedClientResponse.View()` / `Raw()` / `Message()` / `ToVector()` — richer
  zero-copy accessors. Python exposes `payload() -> bytes` + `decode()`, the
  functional equivalent (a copy, consistent with the Phase-2 copy model).
- `Service::stop()` (Rust `&mut self`) — Python uses idempotent `close()` + the
  context-manager protocol (the C++ model), which is equivalent.

**Behavioral note (documented):** a `reply_error` message string is NOT preserved
verbatim at the client — Core wraps it in a `ReplyError` payload, so the client
receives a mapped error whose message is the wrapped form, not the literal text.
`deadline_ns` (ServiceOptions) is RESERVED/no-op: the field exists for signature
parity but Core's FFI builder does not consume it.

---

## Phase 3 — Timer / ExecutionGroup / ContextStore: delivered surface

**Auto-executor** — the C-ABI does not auto-create an executor (that convenience
lives in the Rust/C++ SDK wrappers), so `Node.ensure_executor()` lazily creates a
single-threaded executor, `add_node`s, and spins it on a daemon thread (ctypes
releases the GIL during `aorta_executor_spin` so worker callbacks run). Timers
auto-ensure it; `Node.close()` stops + joins the spin thread.

**Timer** — `Node.create_timer(period, callback, options)` (period = seconds or
`timedelta`); `pause`/`resume` (Rust `Timer::pause`/`resume`), `stop`/`close` +
context manager, `is_running()`, `period()`/`period_ns()`, `stats()` → the 13
diagnostic counters (`TimerStats`). `TimerOptions` (callback_group, auto_start,
invoke_on_missed, overflow_policy + block_timeout, callback_group_handle). Tick
trampoline = process-global, token registry, GIL, exception-swallow.

**ContextStore** — `Node.create_context_store()`; `declare_owned_prefix`
(keyexpr *pattern* — use a wildcard like `foo/**` to cover children);
`is_key_declared` / `declared_prefixes` (Rust-parity, new bridge fns);
`set_{bool,i64,u64,f64,string}(key, value, expires_at_ns=0)`;
`get_{...}(key) -> ContextRead | None` (None on missing key; copies the owned
`ctx_read` strings out and frees the FFI read in a `finally`); `watch` /
`watch_with_initial_snapshot` → `WatchHandle`; `shutdown` / `close`. Types:
`ContextRead`, `ContextChange` (+ `is_snapshot_done()`), `ContextKind`,
`ContextChangeKind`, `SnapshotDoneReason`, `WatchSnapshotOptions`.

**ExecutionGroup** — `Node.create_execution_group(policy)` (SERIALIZED →
MutuallyExclusive, CONCURRENT → Reentrant); `handle()`, `policy()`,
`timer_options()` / `service_options()` stamp the group's `callback_group_handle`
onto a copy of the options. This **closes the Phase-2 `callback_group_handle`
gap** (ServiceOptions gained the field).

**Subscriber** — `Subscriber.runtime_stats()` → `SubscriberRuntimeStats` (7
counters; Rust-parity, new bridge fn). 285 tests, 100% line+branch coverage.

## Phase 3 — parity vs C++ / Rust

➕ **Python has, C++ lacks:** `Timer.pause`/`resume`/`stop`/`is_running`/`period`
(C++ `Timer` exposes only the 13 stat getters — these are Rust-SDK parity,
bridged via two new C-ABI fns); `ContextStore.is_key_declared` /
`declared_prefixes` (Rust diagnostic accessors; C++ lacks them);
`Subscriber.runtime_stats` (Rust parity).

✅ **Closed in Phase 5 (were Phase-3 gaps):**
- **Explicit user-managed `Executor`** — ✅ **delivered Phase 5** (`aorta.Executor`):
  `create`/`add_node`/`spin`/`spin_once`/`stop`/`get_stop_handle`/
  `create_callback_group`/`free_callback_group`/`get_admission_stats` + the
  detachable `StopHandle` + `ExecutorAdmissionStats`. The auto-executor (common
  path) now *reuses* this wrapper internally. One-model-per-node rule documented
  (don't drive a node with both an explicit Executor and the auto-executor
  triggers). See Phase 5 below.
- **`ExecutionGroup.subscriber_options` stamping** + `SubscriberOptions` — ✅
  **delivered Phase 5**. See Phase 5 below.

🕳 **C++ has, Python defers (documented gaps):**
- **`ContextStore.get_*_with_deadline`** (Rust per-call deadline) — Python uses
  Core's default 2 s deadline, exactly like the C++ surface (C++ exposes no
  deadline either), so this is a Rust-only convenience, not a C++-parity gap.
- C++ `WatchHandle::valid()` / `ContextStore::Shutdown` return semantics — Python
  uses idempotent `shutdown()`/`close()` + `_closed`, equivalent.

---

## Phase 4 — Action: delivered surface

**ActionClient** — `Node.create_action_client(action_name, codec, options)`;
`send_goal(goal)` (auto UUIDv4) / `send_goal_with_id(goal_id, goal)`,
`query_status(goal_id) -> PublicStatus|None`, `cancel`, `get_result`, `info() ->
ActionInfo|None`, `subscribe_feedback(callback, goal_id=None) ->
ActionFeedbackSubscription`. **GoalHandle** — `goal_id`/`goal_secret`/
`server_epoch`, `query_status`/`cancel`/`get_result`, `wait_result(timeout)`
(polls to terminal), `subscribe_feedback`.

**ActionServer** — `Node.create_action_server(action_name, schema_bfbs, codec,
handler, options, goal_decoder)`. The handler `handler(goal, ctx)` runs on a
worker thread; a handler exception auto-aborts with `HANDLER_ERROR`, and not
calling a terminal op auto-aborts `HandlerCompletedWithoutResult` (Core).
**ActionExecutionContext** — `goal_id`, `is_cancel_requested` (poll),
`publish_feedback`, `succeed`, `abort`, `cancel`. **RetainedActionGoal** —
`goal_id`/`payload`/`decode`.

**Runtime action name (already aligned).** The Rust/C++ SDKs made the action
keyexpr name a runtime parameter (decoupling it from the
schema-baked `const ACTION_NAME` / `ActionName()`). The Python SDK was never
coupled: `create_action_client` / `create_action_server` /
`create_action_info_server` already take `action_name` as a mandatory positional
arg (mirroring `topic` / `service_name`) and pass it straight into the C-ABI
slot (`aorta_action_{client,server}_new_with_options`) that has always accepted
it. No Python change is needed. Two corollaries of that change also do not apply to
Python: (a) the additive `aorta_normalize_action_name` helper exists only for
SDKs that build client-side action keys **themselves** (C++ `GoalHandle::
WaitResult` subscribes to a client-constructed status key) — Python's
`wait_result` **polls** via the C-ABI (`query_status`/`get_result`), and every
key-dependent op delegates to the handle, so Python inherits the core's
`normalize_path` for free; (b) the `_for`/`Target` type-safety tightening is a
Rust-only `impl Into<Target>` footgun fix with no C-ABI/wire change. **Getter
parity (closed).** After that change, Rust/C++ `action_name()` returns the *normalized*
name; Python now matches — `ActionClient` / `ActionServer` / `ActionInfoServer`
normalize the name once at construction via the additive C-ABI
`aorta_normalize_action_name` and store that, so `action_name()` / `name()`
return the same canonical form (e.g. `"/foo/"` → `"foo"`). A malformed name now
raises at construction (parity with Rust `try_new` / C++ `NormalizeActionName`).

**Codec** — an `ActionCodec` adapter (8 server codec methods + client build/parse)
encodes/decodes the per-action wrapper FlatBuffers tables. The SDK wires it into
the C-ABI: the 8 codec callbacks are per-server ctypes closures (the C-ABI codec
fns take no user_data, like C++ template statics); **encode out-params use a
release callback** (Core owns + frees the buffer — a per-call keepalive dangles
because Core interleaves codec calls); decode out-params use thread-local scratch.
`ActionStatus` is `c_int8`, `ActionReplyCode` is `c_int16` (signed, enforced).

325 tests, 100% line+branch coverage; live roundtrips cover succeed+feedback,
handler-exception auto-abort, explicit abort, cancel, query_status, wait_result
timeout, BUSY rejection. Rejection codes (NotFound/Unauthorized/Busy/…) raise the
mapped exception (`get_result` uses a narrow set since its `code` doubles as the
goal outcome). The execute callback is a process-global trampoline + token
registry so a late dispatch after `close()` no-ops (the C pointer is never freed);
`aorta_action_server_free` drains the queryable/status tasks (quiescing the codec
callbacks) but does NOT join the spawned execute task — the token guard, not the
drain, is what makes a late execute dispatch safe.

## Phase 4 — parity vs C++ / Rust

🕳 **Documented gaps:**
- **Codec generation.** C++ generates the 8 codec callbacks from the action
  schema via templates; Rust via the `ActionWire` trait. Python's SDK provides
  the generic machinery + the `ActionCodec` ABC; the user supplies the codec
  (hand-written, or generated). A `flatc --python --gen-object-api` codegen rule
  that emits an `ActionCodec` from an action `.fbs` is a **Phase-5 packaging
  item** (the demo codec proves the contract). This is the main "not yet at C++
  parity" item — the *machinery* is complete; the *codegen ergonomics* are not.
- **`GoalHandle.wait_result`** polls `get_result` to terminal; C++ subscribes to
  the public-status topic + a condvar. Same outcome; the Python form is simpler
  and avoids a subscriber, documented.
- **Await-on-cancel.** There is no `aorta_action_context_wait_cancelled` C-ABI fn
  (Core's async `cancelled()` is unbridged); Python polls `is_cancel_requested`,
  exactly as the C++ SDK does (C++ also has no await form). Not a C++-parity gap.
- **`GoalHandle.from_persisted`** (C++ + Rust) — ✅ **delivered Phase 5**.
  `GoalHandle.from_persisted(client, goal_id, goal_secret, server_epoch="")`
  reconstructs a fully-functional handle (control plane keys off id+secret).
- **`ActionServer.serve_info_only`** (C++ `ServeInfoOnly` / Rust `serve_info_only`)
  — ✅ **delivered Phase 5** as `Node.create_action_info_server(action_name,
  schema_bfbs)` → `ActionInfoServer` (over
  `aorta_action_server_info_new_with_options`).
- **TraceContext attachment** on `send_goal` / `ActionExecutionContext`
  (C++ `send_goal_traced`, inbound trace on ctx) — the Python SDK does not surface
  per-goal trace propagation (the Phase-1/2/3 SDKs likewise left explicit tracing
  to the env-driven path). Deferred / out of scope, consistent with the rest of
  the Python SDK (tracing is env-driven, no Python API).
- **`RetainedActionGoal.request()` / `message()` / `valid()`** (C++) — Python
  exposes `goal_id()` / `payload()` / `decode()`; `decode()` returns the codec's
  decoded goal (equivalent to `message()`), and `payload()` is the raw request
  bytes (a superset of `request()`). No separate `valid()` — a failed decode
  raises. Functionally equivalent.
- **Rust-only (not in C++; deferred):** typed `ActionFeedbackStream`
  (recv/dropped_count/close) — Python mirrors C++'s callback-based feedback
  (`subscribe_feedback`); `feedback_capacity`/`feedback_overflow_policy` client
  options; `send_goal_traced`; `ActionServer.stop` / `stop_and_join` (Python uses
  `close()` + context manager; `aorta_action_server_free` drains the queryable/
  status tasks synchronously on the non-runtime path — the spawned execute task is
  not joined, so the global execute-trampoline token guard, not the drain, is what
  makes a late dispatch safe).
- **Cross-language action interop** test deferred — needs a Rust/C++ action
  server/client built against the demo schema; the in-process Python↔Python
  roundtrips (and the proven byte-format identity from Phase 1) cover correctness.

---

## Phase 5 — Hardening / packaging / coverage / docs: delivered surface

**`SubscriberOptions`** — `Node.create_subscriber[_typed](topic, handler, *,
options=SubscriberOptions(...))` now exposes the full `aorta_sub_options_t`
surface (callback-group binding, `enable_header_context` + the five MessageContext
flags, `enable_dedup`/`dedup_window`, `enable_reorder_check`/`drop_out_of_order`,
`overflow_policy` (`SubscriberOverflowPolicy.DROP`/`RELAY`) + `overflow_relay_capacity`,
per-subscriber `schema_bfbs`). Defaults mirror Core `SubscriberOptions::default()`.
`ExecutionGroup.subscriber_options()` stamps the group's callback-group handle (the
last of the three stampers, alongside timer/service). This closes the Phase-3
`ExecutionGroup.subscriber_options` gap.

**Explicit `Executor`** — `aorta.Executor` (1:1 with C++ `aorta::Executor`):
`create`/ctor (`ExecutorType.SINGLE_THREADED`/`MULTI_THREADED`/`ASYNC`),
`add_node`, `spin` (blocking, releases the GIL), `spin_once(timeout) -> bool`
(`True` on event/stopped, `False` on timeout), `stop`, `get_stop_handle` →
`StopHandle` (detachable; `stop()`/`close()` + context manager),
`create_callback_group`/`free_callback_group`, `get_admission_stats` →
`ExecutorAdmissionStats` (8 queue counters). The Node **auto-executor** (the common
path) now constructs through this same wrapper (one executor-lifecycle source of
truth), keeping its bespoke daemon-spin + leak-on-timeout teardown. **One model
per node:** do not drive a node with an explicit `Executor` *and* the
auto-executor triggers (`create_timer`/`create_context_store().watch`/
`create_execution_group`) — the two event loops would race.

**Action parity tails** — `GoalHandle.from_persisted(client, goal_id, goal_secret,
server_epoch="")` reconstructs a fully-functional handle from a persisted goal
(another process / post-restart); the control plane keys off id+secret.
`Node.create_action_info_server(action_name, schema_bfbs, *, options=None)` →
`ActionInfoServer` is an **info-only** server (no codec/handler) over
`aorta_action_server_info_new_with_options` — it answers discovery so a client's
`info()` resolves without accepting goals.

**Packaging** — `py.typed` (PEP 561 inline-types marker) ships in the package.
`//python:aorta_sdk_wheel` (`py_wheel`) builds a **per-arch platform wheel**:
distribution `aorta_sdk`, import package `aorta`, tag `py3-none-<platform>`
(`linux_{x86_64,aarch64}` for the robot/CI via `--config=robot`; macOS host tag is
a dev-build smoke). The L2 cdylib is bundled **package-relative**
(`aorta/libaorta_core.{so,dylib}` — exactly where `loader._package_relative_candidates`
looks), so `import aorta` works with no `AORTA_CORE_FFI_LIB` (verified: clean-venv
install → `load_library()` → ABI guard → live `Node`). `flatbuffers` is a
`Requires-Dist`, not bundled. manylinux/auditwheel repair is a separate later gate.
Both wheels are finished by `python_wheel_licenses`
(`message/tools/python_distribution.bzl`) from payload-only stages in
`//python/stage`: `LICENSE`, `NOTICE` and `THIRD_PARTY_NOTICES.md` (for
`aorta_sdk`, the cargo-about notices restricted to the crates linked into the
bundled `libaorta_core`; for `aorta_msgs`, the foxglove-derived module list plus
`THIRD_PARTY_LICENSES/foxglove-sdk.LICENSE`) land under `.dist-info/licenses/`
with PEP 639 `License-File` metadata, which the ownership verifier treats as
distribution-owned rather than payload.

**Coverage** — 354 tests, **100% line + branch** on `python/aorta/` (the gate is
enforced in CI, `--cov-fail-under=100`). The tri-language ABI lock (Python ctypes
mirror ↔ `core/src/ffi/abi_assert.rs` sizes ↔ `cpp/test/abi_layout_test.cc`
offsetof) covers every new struct (`aorta_sub_options_t`,
`aorta_executor_admission_stats_t`).

## Phase 5 — parity vs C++ / Rust

➕ **Python has, C++ lacks:** `Executor.spin_once` returns a `bool`
(C++ returns `Status`); `ExecutorAdmissionStats`/`StopHandle` are first-class
(parity). `Executor.ASYNC` backend exposed (Rust/Core parity).

✅ **Both post-Phase-5 ergonomic gaps are now CLOSED:** typed-RPC schema
**auto-injection** (pass a `*_schema_meta`) and the **action codec**
(`aorta.FlatbuffersActionCodec` — one generic codec; no hand-written or generated
codec needed). Everything else in the summary is either a Rust-only convenience
the C++ SDK also lacks (so not a C++-parity gap) or an equivalent spelling.

---

## C++ Parity Summary (consolidated)

Parity basis: the C++ SDK public API (`cpp/include/aorta/*`) is the primary
target. **Every C++ SDK capability is now reachable from Python.** The table is
the single at-a-glance map; the per-phase sections above carry the detail.

| C++ capability | Python | Notes |
|---|---|---|
| Node + raw/typed Publisher/Subscriber | ✅ | `create_subscriber[_typed]` (typed = **view**; retained-sample variant not exposed — low value) |
| **QoS** (`QoS` struct + per-pub/sub `priority`/`reliability`/`congestion_control`/`express` + 4 presets) | ✅ | **Closed (2026-06-22; updated by QoS & buffering redesign).** `aorta.QoS(...)` + `Priority`/`Reliability`/`CongestionControl` enums + `QoS.{default,realtime_control,sensor_data,state_update,bulk_transfer}()` (read from the C-ABI `aorta_qos_preset`, which returns the full struct — the single source of preset truth). `aorta_qos_t` now mirrors the struct 1:1 (`congestion_control`+`express` folded in; `durability`/`history_depth` removed). Passed via `create_publisher`/`create_subscriber[_typed]`(`qos=`) → `aorta_{pub,sub}_new` (express rides the qos struct). No-QoS path unchanged. Live roundtrip + all 5 presets tested. |
| `Publisher.GetStats()` | ✅ | **Closed (2026-06-22).** `Publisher.stats() -> PublisherStats` via `aorta_publisher_get_stats`. NB: the core currently **stubs** these counters (`publisher_stats_from_core` returns default), so values are 0 on **both** the C++ and Python C-ABI paths — identical behavior; returns real data once the core fills it. (The Rust SDK tracks richer stats in its own wrapper, not via the C-ABI.) |
| `ServiceRequest.TraceContext()` / `GoalHandle.TraceContext()` / `ActionExecutionContext` trace | 🕳 | Same family as the tracing-API row below — no per-request/goal trace-context accessor in Python. |
| `SubscriberOptions` (ctx flags, dedup, reorder, overflow, schema) | ✅ | Phase 5 |
| Service (raw + typed-view) + `ServiceRequest` + deferred reply | ✅ | Phase 2; `reply_error` is a ➕ superset |
| Client (raw + typed) sync `call` | ✅ | Phase 2 |
| `ServiceOptions`/`ClientOptions`/stats | ✅ | Phase 2 |
| Timer (+ pause/resume/is_running/period — ➕ Rust parity) | ✅ | Phase 3 |
| ContextStore (declare/set/get/watch + snapshot) | ✅ | Phase 3 |
| ExecutionGroup (stamps timer/service/**subscriber** options) | ✅ | Phase 3 + 5 |
| **Explicit Executor** (spin/spin_once/stop/stop-handle/callback-group/admission-stats) | ✅ | Phase 5 |
| Action client/server (full 8-fn codec, feedback, cancel) | ✅ | Phase 4 |
| `GoalHandle.from_persisted` | ✅ | Phase 5 |
| Action **info-only server** (`ServeInfoOnly`) | ✅ | Phase 5 |
| `py.typed` + platform wheel | ✅ | Phase 5 |
| Typed-RPC/Pub schema **auto-injection** | ✅ | `create_service_typed_view` / `create_client_typed` take `request_schema_meta` / `response_schema_meta` (a generated `*_schema_meta` module) and auto-inject the schema BFBS/hash + root types (parity with Rust `typed_service`; conflict-checks explicit options). Publishers already inject via `create_publisher_typed(schema_meta, …)`. |
| Action codec (no hand-written codec) | ✅ | `aorta.FlatbuffersActionCodec(goal=…T, result=…T, feedback=…T)` — one generic codec for any action, parameterized only by the three generated **object-API** domain classes. The fixed wrapper layout matches `gen_action_wrapper`; domain payloads are spliced as real **nested tables** via the object API (`--gen-object-api`, on by default), so it is wire-compatible with the Rust/C++ generated codecs. (The `ActionCodec` ABC is still there for fully custom encodings.) |
| Tracing **API** (`send_goal_traced`, per-callback TraceContext) | 🕳 | Out of scope across all phases — Python tracing is env-driven, no Python API (consistent SDK-wide). |

**Rust-only conveniences the C++ SDK also lacks** (so *not* C++-parity gaps, left
unbridged): `Client::call_with_timeout`, `ContextStore.get_*_with_deadline`, typed
`ActionFeedbackStream`, `feedback_capacity`/`feedback_overflow_policy`. Each is
documented in its phase section with the equivalent Python spelling.

**Verdict:** the Python SDK covers the **core pub/sub + RPC + action + executor +
contextstore surface** at functional parity. A full interface audit (2026-06-22,
vs `rust/src/*` + `cpp/include/aorta/*`) surfaced three real C++-parity gaps the
earlier "every capability reachable" claim had missed; **two are now closed** —
(1) **QoS configuration** and (2) **`Publisher.stats()`** (both landed
2026-06-22, tabled above). The **one remaining** C++-parity gap is **per-request/
goal `TraceContext`**, deliberately left with the rest of the tracing API as
out-of-scope (Python tracing is env-driven, no Python API — consistent SDK-wide).
Everything else is either present or a Rust-only convenience the C++ SDK also
lacks (`call_with_timeout`, `get_*_with_deadline`, `query_info`, typed
`ActionFeedbackStream`, `*_for` cross-namespace, module-level time helpers).

---

## Message definitions — where they come from & how to import

The SDK runtime (the `aorta` package / `aorta_sdk` wheel) ships **no** message
classes. Message definitions are generated **per `.fbs`** and consumed
separately — exactly like Rust (a generated msgs crate) and C++ (generated
headers). `aorta_define_schema_package` emits, for any schema with
`python = True` / `schema_meta_py = True` in `message/schemas/registry.bzl`:

| Target | One import gives you |
|---|---|
| `//…:<stem>_schema_meta_py_lib` **(one-stop — prefer this)** | `import <stem>_schema_meta as m` → `m.<Root>` (class), `m.<Root>Start`/… (builders), **and** `m.SCHEMA_BFBS` / `m.SCHEMA_HASH_HEX` / `m.ROOT_TYPE_NAME` / `m.ROOT_TYPE` (meta) |
| `//…:<stem>_py_lib` (classes only) | `import demo.Chat`, `import aorta.sys.AortaHeader` |

`<stem>_schema_meta_py_lib` **deps on** `<stem>_py_lib` and the generated
`<stem>_schema_meta` module **re-exports** the message class + builders
(`from <ns>.<Root> import *`, guarded so the metadata still loads standalone), so
**one dep + one import** is all a typed consumer needs — no separate message-class
dep. (`SCHEMA_BFBS` is byte-identical to the cpp/rust meta → `SCHEMA_HASH_HEX`
matches across languages.) Use `<stem>_py_lib` directly only when you want the raw
classes without metadata.

### In-repo (Bazel) — the primary, robust path

One dep, one import:

```python
py_binary(
    name = "my_app",
    deps = [
        "//python:aorta_py",                                             # the SDK
        "//message/schemas/topic/example:demo_chat_schema_meta_py_lib",  # msg + meta
    ],
)
```
```python
import demo_chat_schema_meta as chat          # ONE import
node.create_publisher_typed(chat, topic)      # chat.SCHEMA_BFBS rides along
node.create_subscriber_typed(chat.Chat.GetRootAs, topic, handler)  # chat.Chat = class
```

Per scenario (runnable examples under `python/examples/`, gated by
`test_scripts/python_typed_examples.sh`):

- **pub/sub** — `pubsub_typed.py`: `create_publisher_typed(chat, topic)` +
  `publish_typed(fill)` (builds with `chat.ChatStart`/… + the SDK-injected header);
  `create_subscriber_typed(chat.Chat.GetRootAs, topic, handler)`.
- **service** — `service_typed.py`: `create_service_typed_view(decoder, name, handler)`
  + `create_client_typed(name, decoder)`, request/response built & decoded with
  `chat`.
- **action** — `action_typed.py`: `aorta.FlatbuffersActionCodec(goal=chat.ChatT,
  result=chat.ChatT, feedback=chat.ChatT)` — the generic codec, parameterized only
  by the generated object-API domain classes; no hand-written codec. Serialize a
  domain message with `FlatbuffersActionCodec.serialize(<T>)`.

### External (pip) — the `aorta_msgs` wheel

`//python:aorta_msgs_wheel` builds a **separate, pure-Python `py3-none-any`**
distribution (`aorta_msgs`) of the generated message modules + schema-meta —
**not** bundled into `aorta_sdk` (SDK and message defs stay decoupled, per the
Q-choice). It bundles **every** Python-enabled schema in the registry: the wheel
collects the per-package `all_python` message trees (`SCHEMA_PYTHON_GENERATED_TARGETS`)
and `all_schema_meta_py` modules (`SCHEMA_META_PY_TARGETS`), so it can never drift
out of sync with the schema set — a new schema with `python = True` (plus `bfbs`
for its schema-meta, on by default via `schema_meta_py_enabled`) is picked up
automatically, no per-schema curation.

### Namespace coexistence (`aorta.*` generated ↔ SDK `aorta` package)

Most schemas carry an `aorta_header`, so their generated tree includes
`aorta.sys.AortaHeader` — i.e. an `aorta/` dir alongside the SDK's `aorta`
package. The SDK `aorta/__init__.py` calls `pkgutil.extend_path`, so both are
importable **as long as the SDK package resolves first on `sys.path`** (verified
by `python/tests/test_namespace.py`; the examples put the SDK dir first).

- **In-repo / dev** (multiple `aorta/` dirs on the path): `extend_path` folds them
  together — robust.
- **pip co-install** (one `site-packages/aorta/`): the generated tree's *empty*
  `aorta/__init__.py` must **not** overwrite the SDK's real one. **Resolved:** the
  `aorta_msgs` wheel is assembled by `python_msgs_tree`
  (`message/tools/flatbuffers_rules.bzl`), which merges the generated modules +
  schema-meta into one tree and **drops the top-level `aorta/__init__.py`** (keeps
  `aorta/sys/`). So `aorta_sdk` always owns `aorta/__init__.py` (and `aorta` is a
  clean PEP 420 namespace when `aorta_msgs` installs alone). Verified: extracting
  both wheels into one tree in either order keeps the SDK's `aorta/__init__.py`
  and `import aorta` / `demo.Chat` / `aorta.sys.AortaHeader` all resolve.
