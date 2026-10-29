# Aorta Python SDK

> Pure-Python (`ctypes`) bindings over the Aorta L2 C-ABI (`libaorta_core`).
> No compiled extension — the SDK loads the shared library at runtime.
>
> **Shared concepts** (layer model, key-expression format, schema/header rules,
> QoS, time model, error codes) are documented once in
> [`spec/Aorta_SDK_Common_Concepts.md`](../spec/Aorta_SDK_Common_Concepts.md).
> **Environment-variable configuration** is in
> [`spec/Aorta_Environment_Variables_Reference.md`](../spec/Aorta_Environment_Variables_Reference.md).
> This README covers install/loading, the Python API surface, idioms, and
> examples. For the equivalent Rust/C++ docs see
> [`rust/README.md`](../rust/README.md) and [`cpp/README.md`](../cpp/README.md).

## Contents

1. [Install & Loading](#1-install--loading)
2. [Quick Start](#2-quick-start)
3. [Node](#3-node)
4. [Publisher & Subscriber](#4-publisher--subscriber)
5. [RPC: Service & Client](#5-rpc-service--client)
6. [Timer](#6-timer)
7. [Executor & ExecutionGroup](#7-executor--executiongroup)
8. [ContextStore: Declarative State](#8-contextstore-declarative-state)
9. [Actions: Long-Running Tasks](#9-actions-long-running-tasks)
10. [QoS / Schema / Time (Shared Concepts)](#10-qos--schema--time-shared-concepts)
11. [Error Handling](#11-error-handling)
12. [Logging & Tracing](#12-logging--tracing)
13. [Capability Comparison with the Rust / C++ SDKs](#13-capability-comparison-with-the-rust--c-sdks)

---

## 1. Install & Loading

The SDK is a pure-Python `ctypes` binding — there is no C extension to compile.

**Requirements:** Python ≥ 3.10, and the [`flatbuffers`](https://pypi.org/project/flatbuffers/)
runtime package (declared as a dependency; not bundled).

**Library discovery order** (`aorta/_ffi/loader.py`) — the loader finds
`libaorta_core` via, in order:

1. **`AORTA_CORE_FFI_LIB`** environment variable (absolute path override).
2. **Bundled** `aorta/libaorta_core.{so,dylib}` (shipped in the wheel).
3. **System** library via `ctypes.util.find_library` (`aorta_core`, `aorta_core_ffi`, `aorta`).

On first load the SDK runs an **ABI guard**: it checks the C-ABI version
(`AORTA_ABI_VERSION`) and required symbols/struct layout, raising
`AbiGuardError` on mismatch to prevent undefined behavior. `ctypes` releases
the GIL across blocking C calls (e.g. `Executor.spin`).

```python
import aorta

# Optional explicit load (otherwise auto-loads on first use, e.g. Node(...)).
aorta.load_library()
```

**License files:** every published wheel ships `LICENSE` (Apache-2.0),
`NOTICE` and `THIRD_PARTY_NOTICES.md` under its `.dist-info/licenses/`
directory (PEP 639 `License-File` metadata); `aorta_msgs` also carries the
foxglove-sdk license its derived schemas require. `pip show -f aorta-sdk`
lists them. The `//python:aorta_sdk_wheel` and `//python:aorta_msgs_wheel`
targets generate them at build time (`tools/release/gen_third_party_notices.py`
and `message/tools/python_distribution.py`).

**Build and install from source.** This release does not publish prebuilt
wheels. Build both wheels on the Linux x86_64 or aarch64 host you will run on;
the SDK wheel bundles `libaorta_core` for that architecture. The SDK wheel's
`THIRD_PARTY_NOTICES.md` is generated from the Cargo dependency graph, so the
build needs `cargo` and `cargo-about` 0.9.2 on `PATH`:

```bash
cargo install --locked cargo-about --version 0.9.2 --features cli

VERSION=0.1.0  # the version of this source release
bazel build //python:aorta_sdk_wheel //python:aorta_msgs_wheel \
  --stamp --embed_label="$VERSION"

# The Bazel outputs keep a literal {BUILD_EMBED_LABEL} in their file names;
# copy them under the real version so pip accepts them.
mkdir -p dist
for w in bazel-bin/python/*.whl; do
  cp "$w" "dist/$(basename "$w" | sed "s/{BUILD_EMBED_LABEL}/$VERSION/")"
done

python3 -m venv .venv
. .venv/bin/activate
pip install dist/*.whl
python -c "import aorta; aorta.load_library(); print('aorta loaded')"
```

`--embed_label` sets the version recorded inside the wheels. `pip` installs
the `flatbuffers` runtime from PyPI as a dependency.

**In-repo (Bazel):** depend on the Python SDK target and point
`AORTA_CORE_FFI_LIB` at the built `libaorta_core`:

```python
py_binary(name = "my_app", deps = ["//python:aorta_py"])
```

---

## 2. Quick Start

A minimal publisher + subscriber (raw bytes). It works without creating an
executor. **Where callbacks run** depends on the node's executor attachment
(the auto-executor, or one you added with `Executor.add_node`), and subscribers
and services look at it at different times:

- **Subscribers follow the node's current attachment.** Each message goes to
  the executor the node is attached to when the message arrives and runs on the
  thread that spins it, under its callback-group rules. With no executor
  attached, each subscriber has its own bounded local queue that the core
  runtime drains on a background worker, one callback at a time per subscriber;
  callbacks for different subscribers can then run concurrently.
- **A service captures its dispatch target when it is created.** A service
  created while the node is attached to an executor queues every request to
  that executor. A service created before any executor is attached handles
  every request on its own background task, and keeps doing so after an
  executor is attached later. If service handlers must run on an executor (for
  example, to serialize them with timers or a callback group), create or attach
  the executor first: call `node.ensure_executor()` or `Executor.add_node(node)`
  before `create_service`.

Callbacks never run inside the Zenoh receive callback itself. The auto-executor
is created lazily, on the first `create_timer`, `create_execution_group` or
explicit `ensure_executor()`; creating subscribers or services does not create
it (see §7).

```python
import aorta
import time

with aorta.Node("quickstart", group="demo") as node:
    received = []
    node.create_subscriber("/demo/hello", lambda payload, ctx: received.append(payload))
    time.sleep(0.3)  # let the subscriber declaration propagate

    pub = node.create_publisher("/demo/hello")
    for i in range(3):
        pub.publish_bytes(f"msg-{i}".encode())

    time.sleep(0.2)
    print(received)  # [b'msg-0', b'msg-1', b'msg-2']
```

> `time.sleep` here is only to let pub/sub matching settle in a tiny script.
> For deterministic startup, use `Publisher.wait_for_matching(timeout_ms)`.

---

## 3. Node

`Node` is the entry point and the factory for every primitive. It owns the
Zenoh session.

```python
node = aorta.Node(name: str, group: str = "default")
```

- `name() -> str` — configured node name.
- `id() -> str` — unique node UUID.
- `group() -> str` — **effective** group (resolved from `AORTA_GROUP` / default).
- `now() -> int` — current Core time in nanoseconds.
- `close() -> None` — close all children (draining callbacks), stop the
  auto-executor, free the node. Idempotent; also runs on `__exit__`.

Factory methods (detailed in later sections): `create_publisher[_typed]`,
`create_subscriber[_typed]`, `create_service[_typed_view]`,
`create_client[_with_timeout|_typed]`, `create_timer`, `create_execution_group`,
`create_context_store`, `create_action_client`,
`create_action_server` / `create_action_info_server`, and `ensure_executor`.

> **⚠️ The `group` constructor argument is currently ignored.** Core re-resolves
> the effective group from `AORTA_GROUP` (falling back to `default`,
> `core/src/node.rs:246`), regardless of what you pass to `Node(...)`. Set
> `AORTA_GROUP` (per process) to isolate nodes into different groups; `group()`
> returns the resolved value.

> **One event loop per node.** Either rely on the auto-executor *or* drive the
> node with an explicit `Executor` — never both, or the two loops race.

---

## 4. Publisher & Subscriber

### 4.1 Publisher

```python
pub = node.create_publisher(topic: str, *, qos: QoS = None)
pub = node.create_publisher_typed(schema_meta, topic, *, qos=None,
                                  schema_attach_policy=SchemaAttachPolicy.INHERIT)
```

- `publish_bytes(data)` — publish raw bytes (fire-and-forget).
- `publish_typed(fill, *, initial_size=256)` — build a FlatBuffers message via a
  `fill(builder, ...)` callback and publish it with an injected `AortaHeader`
  (per-publisher monotonic sequence + node name + Core timestamp).
- `matching_status() -> bool` — is there a matching subscriber right now?
- `wait_for_matching(timeout_ms) -> bool` — wait for a match; `False` = timeout
  (not an error).
- `stats() -> PublisherStats` — FFI counters (publish/oversize/send-error counts, …).
- `topic() -> str`, `close()`, context manager.

```python
pub = node.create_publisher("/sensors/imu")
pub.publish_bytes(payload)
if not pub.wait_for_matching(1000):
    print("no subscriber yet")
```

### 4.2 Subscriber

```python
sub = node.create_subscriber(topic, handler, *, qos=None, options=None)
sub = node.create_subscriber_typed(root_decoder, topic, handler, *, qos=None, options=None)
```

- `handler(payload: bytes, ctx: MessageContext)` — invoked on the attached
  executor's thread, or on the subscriber's local-queue worker when no
  executor is attached (see §2). The typed variant decodes via `root_decoder(bytes) -> Any` and
  delivers the decoded view; decode failures are logged and skipped.
- `MessageContext` (frozen): `topic`, `sequence`, `publisher_node`,
  `receive_stamp_ns`, `priority`. Populated according to `SubscriberOptions`.
- `runtime_stats() -> SubscriberRuntimeStats` — dedup/reorder/overflow counters.
- `dropped_count() -> int` — events shed by the Tier A `receive_depth` gate
  (drop-newest); `0` when `receive_depth == 0` (see §4.3).
- `topic() -> str`, `close()`, context manager.
- `SubscriberOptions` configures QoS, dedup window, reorder detection, callback
  group, overflow policy (`SubscriberOverflowPolicy`), and the per-topic
  `receive_depth` (see §4.3).

```python
def on_msg(payload, ctx):
    print(ctx.topic, ctx.sequence, len(payload))

node.create_subscriber("/sensors/imu", on_msg)
```

### 4.3 Per-Topic Buffering & Pull Subscriptions (receive_depth / pull)

The QoS & buffering redesign adds **per-topic** subscriber backpressure,
orthogonal to QoS.

**Tier A — `SubscriberOptions.receive_depth` (per-topic in-flight cap + drop
isolation).** A per-topic in-flight cap: `0` (default) keeps today's behavior
(unbounded, shared executor queue); when `>0`, once the cap is reached the
**newest event is dropped** (drop-newest), isolating one slow topic's
congestion. The shed count is observable via `Subscriber.dropped_count()`:

```python
opts = aorta.SubscriberOptions(receive_depth=16)
sub = node.create_subscriber("/sensors/lidar", on_msg, options=opts)
# ... later
print("shed by depth cap =", sub.dropped_count())
```

**Tier B-pull — pull-mode subscriber (`create_subscriber_pull`).** A
Zenoh-native keep-last ring with **no callback** — you drain it yourself. The
latest-wins sensor idiom (like rclpy `wait_for_message`), with structural
per-topic isolation and no executor hop. `node.create_subscriber_pull(topic,
depth)` returns a `PullSubscriber`; drain it with `recv(timeout=None)` (blocking
/ timeout in seconds) or `try_recv()` (non-blocking), each returning a
`PullSample(payload: bytes, context)` or `None` on timeout / empty ring:

```python
sub = node.create_subscriber_pull("/sensors/lidar", 8)  # ring capacity = 8
while True:
    sample = sub.recv()            # blocks until a sample arrives
    if sample is None:
        continue
    process(sample.payload, sample.context)
# or: sample = sub.recv(timeout=0.05)   # None on timeout
#     sample = sub.try_recv()           # None when the ring is empty
```

> Pull is opt-in. Unlike Rust/C++, the Python SDK does **not** offer an inline-push
> (callback-on-the-Zenoh-RX-thread) mode — a slow GIL callback there would be a
> node-wide footgun.

---

## 5. RPC: Service & Client

Request–response over Zenoh query/queryable. **Automatic retries are OFF by
default** (a non-idempotent service must not be silently re-invoked).

### 5.1 Service (server)

```python
svc = node.create_service(service_name, handler, *, options=None)
svc = node.create_service_typed_view(root_decoder, service_name, handler, *,
                                     request_schema_meta=None,
                                     response_schema_meta=None, options=None)
```

The handler receives a request and a reply handle:

```python
def handler(request_bytes: bytes, req: aorta.ServiceRequest):
    answer = int.from_bytes(request_bytes, "little") * 2
    req.reply(answer.to_bytes(8, "little"))

node.create_service("/math/double", handler)
```

- `ServiceRequest`: `reply(data=b"")`, `reply_error(message)`, `release()`,
  `is_valid()`. The handle is single-use (replying/releasing consumes it; it
  auto-releases on GC). `reply_error` sends a real Zenoh error (a Python/Rust
  feature; the C++ SDK has no equivalent).
- Typed: `TypedServiceResponder.reply(fill, *, initial_size=256)` /
  `reply_bytes(data)` / `reply_error(message)`.
- `Service.name() -> str`, `Service.stats() -> ServiceStats`, `close()`, context manager.
- The service fixes where its handler runs when it is created: on the node's
  executor if one is attached then, otherwise on the service's own background
  task for its whole lifetime (see §2).

### 5.2 Client

```python
client = node.create_client(service_name, *, options=None)
client = node.create_client_with_timeout(service_name, timeout_ms)
typed  = node.create_client_typed(service_name, response_decoder, *,
                                  request_schema_meta=None, options=None)
```

- `call(request: bytes) -> bytes` — blocking; returns the response or raises
  `TimeoutError` / a mapped error. The request must be non-empty.
- `service_name() -> str`, `matching_status()` / `wait_for_matching(timeout_ms)`.
- `stats() -> ClientStats`, `close()`, context manager.
- Typed: `TypedClient.call(fill, *, initial_size=256) -> TypedClientResponse`,
  whose `.payload() -> bytes` and `.decode() -> Any` extract the result.
  `TypedClient` also exposes `service_name() -> str`.

```python
client = node.create_client("/math/double", options=aorta.ClientOptions(timeout_ms=5000))
resp = client.call((21).to_bytes(8, "little"))
assert int.from_bytes(resp, "little") == 42
```

> The timeout is fixed at construction (`ClientOptions.timeout_ms`); there is no
> per-call `call_with_timeout` (matches the C++ SDK).

---

## 6. Timer

Periodic callback dispatched by the executor (no dedicated thread per timer).

```python
timer = node.create_timer(period, callback, *, options=None)
```

- `period` accepts seconds (`float`/`int`) or a `datetime.timedelta`.
- `options: TimerOptions` — `auto_start` (default `True`), missed-tick policy,
  overflow policy (`TimerOverflowPolicy`).
- Control: `pause()`, `resume()`, `is_running()`, `stop()` (= `close()`).
- Introspection: `period() -> float` (seconds), `period_ns() -> int`,
  `stats() -> TimerStats`.

```python
ticks = []
timer = node.create_timer(0.05, lambda: ticks.append(1))
time.sleep(0.2)
timer.pause()
# ... later ...
timer.resume()
```

---

## 7. Executor & ExecutionGroup

A node creates a single-threaded auto-executor on a daemon thread the first time
it needs one (`create_timer`, `create_execution_group`); until then subscriber
callbacks use the local queues described in §2, and services created in that
window keep handling requests on their own background task. Create an explicit
executor for control over threading and shutdown.

```python
ex = aorta.Executor(aorta.ExecutorType.SINGLE_THREADED)   # or MULTI_THREADED / ASYNC
ex.add_node(node)
threading.Thread(target=ex.spin, daemon=True).start()
# ...
ex.get_stop_handle().stop()    # stop from another thread / callback
ex.close()
```

- `spin()` — block, draining the event queue until stopped.
- `spin_once(timeout=0.0) -> bool` — process at most one event; `False` on timeout.
- `stop()` / `get_stop_handle() -> StopHandle` — the handle is detachable and
  callback-safe.
- `create_callback_group(group=CallbackGroup.MUTUALLY_EXCLUSIVE) -> int` /
  `free_callback_group(handle)`.
- `get_admission_stats() -> ExecutorAdmissionStats`.

**ExecutionGroup** is a convenience for binding several callbacks to one shared
group (serialized or concurrent):

```python
grp = node.create_execution_group(aorta.ExecutionPolicy.SERIALIZED)  # or CONCURRENT
node.create_timer(0.1, cb, options=grp.timer_options())
node.create_subscriber("/t", on_msg, options=grp.subscriber_options())
```

`ExecutionGroup` stamps `timer_options` / `service_options` /
`subscriber_options` with its callback-group handle.

---

## 8. ContextStore: Declarative State

Declarative key/value state with typed setters/getters and watches.

```python
store = node.create_context_store()
store.declare_owned_prefix("robot/**")
store.set_string("robot/state", "idle")
store.set_i64("robot/battery", 87)

reading = store.get_string("robot/state")   # ContextRead | None
print(reading.value if reading else None)

def on_change(change: aorta.ContextChange):
    print(change)

handle = store.watch("robot/**", on_change)  # WatchHandle
```

- Setters: `set_bool/set_i64/set_u64/set_f64/set_string(key, value, *, expires_at_ns=0)`.
- Getters: `get_bool/get_i64/get_u64/get_f64/get_string(key) -> ContextRead | None`.
- `watch(prefix, callback)` and
  `watch_with_initial_snapshot(prefix, callback, *, options=None)` — callbacks
  fire on a worker thread; a `SnapshotDone` event signals the initial query drained.
- `is_key_declared`, `declared_prefixes`, `shutdown`, `close`.

---

## 9. Actions: Long-Running Tasks

Long-running, cancellable goals with feedback and a terminal result. Most users
need no hand-written codec: **`FlatbuffersActionCodec`** wraps three generated
object-API classes (goal / result / feedback).

```python
codec = aorta.FlatbuffersActionCodec(goal=ChatT, result=ChatT, feedback=ChatT)

# Server
def handler(goal, ctx: aorta.ActionExecutionContext):
    ctx.publish_feedback(aorta.FlatbuffersActionCodec.serialize(fb_obj), "halfway")
    if ctx.is_cancel_requested():
        ctx.cancel(aorta.ActionReplyCode.CANCELED); return
    ctx.succeed(aorta.FlatbuffersActionCodec.serialize(result_obj))

node.create_action_server("/actions/chat", schema_bfbs, codec, handler,
                          goal_decoder=lambda b: codec.decode_goal(b))

# Client
client = node.create_action_client("/actions/chat", codec)
goal = client.send_goal(my_goal_obj)              # -> GoalHandle
result = goal.wait_result(timeout=5.0)
assert result.status == aorta.ActionStatus.SUCCEEDED
```

- **ActionClient:** `send_goal` / `send_goal_with_id`, `query_status`, `cancel`,
  `get_result`, `info`, `subscribe_feedback`.
- **GoalHandle:** `goal_id`, `goal_secret`, `server_epoch`, `query_status`,
  `cancel`, `get_result`, `wait_result`, `subscribe_feedback`, and
  `GoalHandle.from_persisted(client, goal_id, goal_secret, server_epoch="")` to
  resume control after a restart / in another process.
- **ActionExecutionContext (server):** `goal_id`, `is_cancel_requested`,
  `publish_feedback`, and the terminal ops, all of which take required args:
  `succeed(result_payload: bytes)`, `abort(code: ActionReplyCode, message="")`,
  `cancel(code: ActionReplyCode, message="")`.
- **Info-only server:** `node.create_action_info_server(name, schema_bfbs)`
  answers discovery without running goals.

---

## 10. QoS / Schema / Time (Shared Concepts)

These are language-agnostic — see
[`spec/Aorta_SDK_Common_Concepts.md`](../spec/Aorta_SDK_Common_Concepts.md) for
the full model. Python bindings:

- **QoS:** the 4 wired fields —
  `aorta.QoS(priority=…, reliability=…, congestion_control=…, express=False)`,
  plus presets read from the C-ABI: `QoS.default()`, `QoS.realtime_control()`,
  `QoS.sensor_data()`, `QoS.state_update()`, `QoS.bulk_transfer()`. Enums:
  `Priority`, `Reliability`, `CongestionControl` (`DROP` default / `BLOCK`).
  `BLOCK` is the honest "don't silently drop locally" lever (backpressure on
  the local egress queue; end-to-end delivery is still not guaranteed);
  `DROP` is lowest-latency lossy.
  The former `durability` (and `Durability`/`TransientLocal`/`Volatile`) and the
  QoS `history_depth` were silent no-ops and are **gone**. For "give a late
  joiner / reconnecting reader the last value", use
  [`ContextStore`](#8-contextstore-declarative-state) (latched / last-value is its job,
  not QoS). For a per-topic receive cap, use `SubscriberOptions.receive_depth`
  (see [§4.3](#43-per-topic-buffering--pull-subscriptions-receive_depth--pull)).
- **Schema:** `SchemaMeta` + `SchemaAttachPolicy` (`INHERIT` / `ALWAYS` /
  `NEVER`). The typed factory methods auto-inject the request/response schema
  when you pass `*_schema_meta`. Schema hash = SHA-256, 64 hex chars.
- **Time:** `node.now() -> int` (nanoseconds). The Python SDK exposes time
  **reads** only — there is no clock-mode switching API; the process clock
  source (System / Simulation) is selected elsewhere (Core / tests). See
  [SDK Common Concepts](../spec/Aorta_SDK_Common_Concepts.md) §6.

---

## 11. Error Handling

C-ABI result codes map to typed Python exceptions, all subclasses of
`aorta.AortaError` (with a `ResultCode`):

`InvalidArgumentError`, `NotFoundError`, `TimeoutError`, `BusyError`,
`InternalError`, `SchemaConflictError`, `ConcurrentConflictError`,
`AlreadyRepliedError`, `PrefixNotDeclaredError`, `DeclarationNotReadyError`,
`KindMismatchError`, `InstanceIdCollisionError`, `ReentrantError`.

```python
try:
    resp = client.call(request)
except aorta.TimeoutError:
    ...
except aorta.AortaError as e:
    print(e.code, e)
```

### Callback-safe operations

Callbacks (subscriber `on_msg`, service handler, timer tick,
ContextStore watch, action feedback) run on Aorta's dispatch substrate,
where blocking FFI calls are refused and raise `ReentrantError`:

- **Legal inside a callback:** `publish` / `publish_owned`
  (fire-and-forget hot path), zero-timeout probes such as
  `sub.try_recv()` / `recv(timeout=0)`, the spawn-and-detach `_free`s,
  and `stop_handle.stop()` (signaling only).
- **Rejected with `ReentrantError`:** blocking calls — resource
  construction, `client.call(...)`, blocking `recv`/`wait_result`,
  `executor.spin`.

Action `execute` handlers are the exception: each goal's handler runs on
its own dedicated thread, where `aorta_action_context_*` calls succeed. The full
substrate × entry matrix and the `aorta_reentrant_context()` detector
are documented in the REENTRANCY CONTRACT block of
`cpp/include/aorta/aorta.h`.

The numeric code table is in
[`spec/Aorta_SDK_Common_Concepts.md`](../spec/Aorta_SDK_Common_Concepts.md#7-error-model).

---

## 12. Logging & Tracing

- **Logging** is configured via environment variables shared with all SDKs
  (`AORTA_LOG` / `RUST_LOG`, `AORTA_LOG_FORMAT`, `AORTA_LOG_FILE`, …) — see §2 of
  the [env-var reference](../spec/Aorta_Environment_Variables_Reference.md).
  The `aorta.logging` module (alias `aorta.log`) exposes the helpers.
- **Tracing** is **env-driven** in Python (`AORTA_TRACE`, `AORTA_TRACE_DIR`, …):
  the Python SDK has no per-request tracing API by design — see §1 of the
  [env-var reference](../spec/Aorta_Environment_Variables_Reference.md). Note this
  is Python-specific: the C++ SDK does expose a per-SDK tracing API (the
  `aorta::trace` namespace + `AORTA_TRACE_*` macros in `aorta/trace.h`) and Rust
  exposes its own trace surface. The env-var *configuration* (`AORTA_TRACE`, …) is
  shared by all SDKs; only the per-request *API* differs.

---

## 13. Capability Comparison with the Rust / C++ SDKs

The Python SDK targets **full parity with the C++ SDK** (see
`spec/Aorta_Python_SDK_Parity_Status.md`). Notes worth knowing:

- **Typed path is view/callback-based.** Python builds messages via a
  `fill(builder, …)` callback and reads via a `root_decoder`; there is no mutable
  message-object builder (flatbuffers-python has no equivalent). `reply_bytes` /
  `Client.call(bytes)` are the raw equivalents.
- **Python supersets:** `ServiceRequest.reply_error(str)` (real Zenoh error
  reply; also in Rust, not C++); `Executor.spin_once` returns `bool`. (The
  `ASYNC` executor backend is **not** Python-only — C++ exposes
  `ExecutorType::kAsync` too, so it is shared Rust/Core parity, not a superset.)
- **Rust-only conveniences left unbridged** (also absent in C++, so not parity
  gaps): `Client::call_with_timeout`, per-call context `get_*_with_deadline`,
  typed feedback streams.
- **Out of scope:** per-request `TraceContext` (the Python SDK has no per-request
  tracing API by design); `send_goal_traced` — note this **is** exposed by C++
  (`ActionClient::SendGoalPayloadTraced`) and Rust, so it is unbridged in Python
  only, not absent from C++.
- `Publisher.stats()` binds `aorta_publisher_get_stats`, but Core currently stubs
  the counters (`publisher_stats_from_core` returns an all-zero
  `aorta_publisher_stats_t::default()`), so it returns 0 today on both the Python
  and C++ C-ABI paths; it will return real values once Core fills them.

---

## See also

- [`spec/Aorta_SDK_Common_Concepts.md`](../spec/Aorta_SDK_Common_Concepts.md) — shared concepts
- [`spec/Aorta_Environment_Variables_Reference.md`](../spec/Aorta_Environment_Variables_Reference.md) — all env vars
- [`spec/Aorta_Python_SDK_Design_Proposal.md`](../spec/Aorta_Python_SDK_Design_Proposal.md) — design rationale
- [`spec/Aorta_Python_SDK_Parity_Status.md`](../spec/Aorta_Python_SDK_Parity_Status.md) — parity matrix
- [`rust/README.md`](../rust/README.md) · [`cpp/README.md`](../cpp/README.md)
