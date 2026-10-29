# Aorta Rust SDK Guide

> Canonical location: `rust/README.md`
>
> Supported platforms: Linux x86_64 and aarch64. macOS paths that appear in
> this guide are development conveniences, not a supported platform.

> **Shared docs** (language-agnostic, single source of truth — this guide links
> back to them instead of duplicating):
> - Concepts (layer model, key-expr format, schema/header, QoS, time, error codes):
>   [`spec/Aorta_SDK_Common_Concepts.md`](../spec/Aorta_SDK_Common_Concepts.md)
> - Environment variables: [`spec/Aorta_Environment_Variables_Reference.md`](../spec/Aorta_Environment_Variables_Reference.md)

---

## Contents

1. [Quick Start](#1-quick-start)
2. [Node](#2-node)
3. [Publisher & Subscriber](#3-publisher--subscriber)
4. [RPC: Service & Client](#4-rpc-service--client)
5. [Timer](#5-timer)
6. [Executor](#6-executor)
7. [QoS](#7-qos)
8. [Schema](#8-schema)
9. [Logging](#9-logging)
10. [Time: the Unified Time Source](#10-time-the-unified-time-source)
11. [Advanced Features](#11-advanced-features)
12. [Worked Examples](#12-worked-examples)
13. [Best Practices](#13-best-practices)
14. [Actions: Long-Running Tasks](#14-actions-long-running-tasks)
15. [Aorta Flow: Structured Concurrency](#15-aorta-flow-structured-concurrency)

---

## 1. Quick Start

### 1.1 Adding the Crate

The Rust SDK ships all of its functionality through a single crate, `aorta_sdk`. Use the `prelude` module to pull in every commonly used type at once:

```rust
use aorta_sdk::prelude::*;
```

The `prelude` module (`rust/src/lib.rs`) exports the following core types:

| Category | Exported types |
|------|---------|
| Node | `Node`, `NodeOptions`, `TelemetryNodeMetadata` |
| Pub/sub | `Publisher`, `PublisherOptions`, `BuilderGrowthPolicy`, `BuilderReusePolicy`, `Subscriber`, `SubscriberOptions`, `SubscriberOverflowPolicy`, `SubscriberRuntimeStats`, `MessageContext`, `PullNotifyHook`, `FlowAdmission` |
| RPC | `Client`, `ClientOptions`, `ClientStats`, `TypedClient`, `TypedClientResponse`, `Service`, `ServiceOptions`, `ServiceRequest`, `TypedServiceResponder`, `TypedRetainedServiceRequest`, `TypedRpcRequestMessage`, `TypedRpcResponseMessage`, `ServiceSchemaInfo`, `ServiceStats`, `RpcOverflowPolicy` |
| Action | `ActionClient`, `ActionClientOptions`, `ActionServer`, `ActionServerOptions`, `GoalHandle`, `GoalGuard`, `RetainedActionGoal`, `ActionExecutionContext`, `ActionFeedbackStream`, `ActionFeedbackSample`, `FeedbackOverflowPolicy`, `ActionInfo`, `ActionStatus`, `ActionPublicStatus`, `ActionReplyCode`, `ActionSchema`, `ActionWire`, `ActionRootType`, `ActionRootTypeNames`, `ActionSendGoalResponse`, `ActionGetResultResponse`, `ActionCancelGoalResponse`, `ResultObserver`, `goal_id_to_key`, `goal_key_to_id`, `resolve_action_root_types_from_bfbs`, `ACTION_INFO_ROOT_TYPE`, `DEFAULT_ACTION_FEEDBACK_CAPACITY`, `DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS` |
| Executor | `ExecutionGroup`, `ExecutionPolicy`, `SingleThreadedExecutor`, `MultiThreadedExecutor`, `ExecutorOptions`, `ExecutorAdmissionOptions`, `ExecutorClassPolicyOptions`, `ExecutorNodeAttachExt`, `CallbackGroupHandle`, `CallbackGroupType`, `PrioritySchedulerOptions`, `EventPriority`, `AdmissionStats`, `AdmissionStatsReader`, `AdmissionDropReason`, `StopHandle`, `SpinOutcome`, `SchedulerMode` |
| QoS | `QoS`, `Priority`, `Reliability`, `CongestionControl` |
| Schema | `SchemaMeta`, `SchemaAttachPolicy`, `SchemaHash`, `HeaderInjector`, `HeaderValues`, `AortaHeaderLayout`, `TypedPublishMessage`, `TypedSubscribeMessage`, `TypedRetainedSample` |
| Timer | `Timer`, `TimerOptions`, `TimerOverflowPolicy` |
| Flow (§15) | `Scope`, `ScopeOpts`, `ActionScope`, `Supervision`, `ExternalChild`, `Budget`, `CancelToken`, `CancelReason`, `RawCancelReason`, `StopReason`, `Cancelled`, `Stopped`, `Shield`, `Outcome`, `TaskHandle`, `Either`, `offload`, `offload_in`, `when_all`, `when_any`, `Watch`, `WatchGuard`, `SeqPayload`, `WindowSync`, `WindowStamp`, `WindowStats`, `ReadinessSet`, `ReadinessWire`, `Condition` |
| Logging | `LogConfig`, `LogFormat`, `LogLevel`, `LogTopicConfig`, `init_from_env_once`, `configure_logging`, `flush_logging`, `set_default_context`, `is_log_enabled`, `is_logging_initialized` |
| Time | `Time`, `TimeSource`, `now`, `now_ns`, `unix_time_ns`, `set_time_source`, `time_source`, `set_simulation_time_ns`, `simulation_time_ns` |
| Errors | `AortaError`, `AortaResult`, `TransportError` |
| Utilities | `ZBytes`, `with_contiguous_bytes`, `FlatBufferBuilder`, `new_uuid_v7` |
| Logging macros | `aorta_info!`, `aorta_warn!`, `aorta_error!`, `aorta_debug!`, `aorta_trace!`, `aorta_log!`, `aorta_log_if!`, `aorta_log_every_n!` |

If you only need the logging macros, use `log_prelude` on its own:

```rust
use aorta_sdk::log_prelude::*;
```

#### 1.1.1 Downstream Bazel module consumption (Bzlmod)

`aorta_sdk`'s public API relies on three third-party crates: **tokio** (the
`#[tokio::main]` entry point), **flatbuffers** (`FlatBufferBuilder`
re-exported by the prelude), and **bytes** (the zero-copy
`publish_bytes(bytes::Bytes::from(...))` path documented in §3.3.4 — the
`Into<ZBytes>` impl is bound to the exact `bytes` crate identity that
Aorta's zenoh links, so a consumer-built hub of a different version
cannot satisfy it).

`@aorta_crates` is Aorta's **private** crate-universe repo name (generated by
`crate.from_cargo` in `MODULE.bazel`); downstream modules have no such repo
mapping, so writing `@aorta_crates//:tokio` directly fails by construction
(a module's crate-universe repository is not visible to the modules that
depend on it). Aorta
therefore exposes public aliases in the `//rust` package that downstream
users reference instead:

| Alias label | Actual target | Use |
|-------------|---------------|-----|
| `@aorta//rust:tokio` | `@aorta_crates//:tokio` | `#[tokio::main]` entry point (required) |
| `@aorta//rust:flatbuffers` | `@aorta_crates//:flatbuffers` | Naming flatbuffers types explicitly |
| `@aorta//rust:bytes` | `@aorta_crates//:bytes` | `bytes::Bytes` payloads for the zero-copy `publish_bytes` path |

Downstream `BUILD.bazel` usage (the consumer declares its own `rules_rust`
dependency and registers a Rust toolchain):

```python
load("@rules_rust//rust:defs.bzl", "rust_binary")

rust_binary(
    name = "my_node",
    srcs = ["main.rs"],
    edition = "2021",
    deps = [
        "@aorta//rust:aorta_sdk",
        "@aorta//rust:tokio",  # runtime required by #[tokio::main]
    ],
)
```

```rust
use aorta_sdk::prelude::*;

#[tokio::main]
async fn main() {
    // ...
}
```

`@aorta//rust:tokio` is the **same Bazel target** the in-tree examples use,
so version and features align by construction (the SDK manifest explicitly
declares the `macros` + `rt-multi-thread` features the `#[tokio::main]`
expansion needs, rather than inheriting them from other crates' feature
unions). The maintainers' release flow compiles this path in a separate
downstream consumer workspace (not part of this tree) that also constructs a
`FlatBufferBuilder` and converts a `bytes::Bytes` into `ZBytes`, to prove the
flatbuffers/bytes aliases work downstream.

### 1.2 Minimal Publisher Example

The following code shows the simplest possible typed publisher program (see `rust/examples/talker.rs`):

```rust
use aorta_sdk::prelude::*;
use demo_chat_msg::prelude::*;  // code-generated message wrappers

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create the Node (asynchronous)
    let node = Node::new("talker", "default").await?;
    aorta_info!("sdk talker started");

    // 2. Create a typed Publisher (asynchronous; the schema BFBS is injected automatically)
    let publisher = node.create_publisher::<ChatSchema>("/demo/chatter").await?;

    // 3. Publish messages (synchronous fire-and-forget — no .await!)
    let mut counter: u64 = 0;
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(500));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                publisher.publish_typed(|msg: &mut ChatMsg| {
                    msg.text = Some(format!("hello {} from {}", counter, node.name()));
                })?;  // Note: no .await
                counter = counter.wrapping_add(1);
            }
        }
    }

    // 4. Graceful shutdown
    node.close().await?;
    Ok(())
}
```

**Key points:**

- `Node::new().await?` is an **asynchronous** operation: it connects to the Zenoh runtime and registers a liveliness token
- `create_publisher::<T>().await?` is an **asynchronous** operation; the generic parameter `T` must implement `SchemaMeta + HeaderInjector`
- `publish_typed()` is **synchronous fire-and-forget** — no `.await` needed, no waiting for subscriber acknowledgement. Under the default `CongestionControl::Drop` it never blocks; under `CongestionControl::Block` it can block the calling thread while the transport queue is backpressured
- The AortaHeader (timestamp, sequence number, publishing node name) is **injected automatically** by the SDK
- All asynchronous operations run on the **Tokio** runtime

### 1.3 Minimal Subscriber Example

The following code shows the simplest possible subscriber program (see `rust/examples/listener.rs`):

```rust
use aorta_sdk::log_prelude::*;
use aorta_sdk::{with_contiguous_bytes, Node, SubscriberOptions};
use demo_chat_msg::demo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Node::new("listener", "default").await?;

    // Create the subscriber; the callback receives &ZBytes + &MessageContext
    let options = SubscriberOptions::new()
        .with_header_context(true)
        .with_receive_timestamp(true);

    let _subscriber = node
        .create_subscriber_with_options(
            "/demo/chatter",
            |payload, context| {
                with_contiguous_bytes(payload, |bytes| {
                    if let Ok(chat) = demo::root_as_chat(bytes) {
                        let text = chat.text().unwrap_or("");
                        if let Some(header) = chat.aorta_header() {
                            aorta_info!(
                                "seq={} node={} text={}",
                                header.sequence(),
                                header.publisher_node().unwrap_or(""),
                                text
                            );
                        }
                    }
                });
            },
            options,
        )
        .await?;

    // Wait for Ctrl+C (the Node manages its internal Executor automatically)
    tokio::signal::ctrl_c().await?;
    node.close().await?;
    Ok(())
}
```

**Key differences from the C++ SDK:**

| Behavior | C++ SDK | Rust SDK |
|------|---------|---------|
| What drives subscriber / service / timer callbacks | An explicit Executor may be attached; without one, the Node creates an internal fallback Executor | An explicit Executor may be attached (`ExecutorNodeAttachExt`); without one, the Node creates an internal fallback Executor |
| Callback signature | `(MessageView&, MessageContext&)` | `(&ZBytes, &MessageContext)` |
| Data access | `msg.data` pointer | The `with_contiguous_bytes()` helper |

---

## 2. Node

### 2.1 Overview

`Node` is the central object of the Aorta Rust SDK (`rust/src/node.rs`); every communication primitive is created through a Node. Each Node has:

- **name**: the node name (user-specified)
- **id**: a stable 128-bit hex identity derived from `(host, group, name)` — the same configuration yields the same id across restarts — or an explicit `AORTA_NODE_ID` override. It is not a random UUID and is not guaranteed globally unique
- **group**: the group name (used for the `{group}` segment of key expressions)

`Node` implements `Clone` and is internally `Arc`-refcounted, so it can be shared across threads safely.

### 2.2 Creating a Node

```rust
// Signature
pub async fn new(
    name: impl Into<String>,
    group: impl AsRef<str>,
) -> AortaResult<Self>
```

```rust
// Example
let node = Node::new("my_node", "default").await?;

// Accessing the properties (group() returns the resolved effective group)
aorta_info!("name={} id={} group={}", node.name(), node.id(), node.group());
```

**Design notes:**

- `Node::new()` is an **asynchronous** operation: it connects to the Zenoh runtime and registers a liveliness token
- The group participates in key expression construction: `aorta/{group}/pub/{topic}`
- **⚠️ The constructor's `group` argument is currently ignored**: the Core always resolves the effective group from `AORTA_GROUP` (falling back to `default`) (`core/src/node.rs:246,256`). To isolate a node into a different group, set `AORTA_GROUP` (per process); passing a different constructor argument has no effect. `node.group()` returns the resolved effective value
- `Node` implements `Clone` (backed by `Arc`), so it can be handed to multiple tasks / closures safely

### 2.3 Node Lifecycle Management

```rust
// Graceful shutdown
pub async fn close(&self) -> AortaResult<()>
```

`close()` does the following:

1. Stops the automatically created internal Executor (if any)
2. Closes the underlying Zenoh session

```rust
// Recommended lifecycle pattern
let node = Node::new("demo", "default").await?;

// ... use the node to create primitives ...

// Shut down gracefully before exiting
node.close().await?;
```

### 2.4 The AortaResult Error-Handling Pattern

The Rust SDK uses the standard `Result` pattern:

```rust
// AortaResult<T> = Result<T, AortaError>
pub type AortaResult<T> = Result<T, AortaError>;

// The AortaError type
pub struct AortaError {
    // error kind + message
}
```

The `?` operator is recommended:

```rust
let node = Node::new("demo", "default").await?;
let publisher = node.create_publisher::<ChatSchema>("/topic").await?;
publisher.publish_typed(|msg| { /* ... */ })?;
```

Or handle it manually with `match`:

```rust
match node.create_publisher::<ChatSchema>("/topic").await {
    Ok(publisher) => { /* use the publisher */ },
    Err(err) => aorta_error!("failed to create publisher: {}", err),
}
```

### 2.5 Node Creation Primitives at a Glance

| Method | Async | Return type |
|------|------|---------|
| `create_publisher::<M>(topic)` | `.await` | `AortaResult<Publisher<M>>` |
| `create_publisher_with_options::<M>(topic, opts)` | `.await` | `AortaResult<Publisher<M>>` |
| `create_publisher_raw(topic)` | `.await` | `AortaResult<CorePublisher>` |
| `create_subscriber(topic, callback)` | `.await` | `AortaResult<Subscriber>` |
| `create_subscriber_typed_view::<M, _>(topic, callback)` | `.await` | `AortaResult<Subscriber>` |
| `create_subscriber_typed_retained::<M, _>(topic, callback)` | `.await` | `AortaResult<Subscriber>` |
| `create_subscriber_with_options(topic, callback, opts)` | `.await` | `AortaResult<Subscriber>` |
| `create_subscriber_with_qos(topic, callback, qos)` | `.await` | `AortaResult<Subscriber>` |
| `create_service(name, handler)` | `.await` | `AortaResult<Service>` |
| `create_service_typed::<Req, Resp, _>(name, handler)` | `.await` | `AortaResult<Service>` |
| `create_service_typed_view::<Req, Resp, _>(name, handler)` | `.await` | `AortaResult<Service>` |
| `create_service_typed_retained::<Req, Resp, _>(name, handler)` | `.await` | `AortaResult<Service>` |
| `create_client(name)` | **sync** | `Client` |
| `create_client_with_timeout(name, ms)` | **sync** | `Client` |
| `create_client_with_options(name, opts)` | **sync** | `Client` |
| `create_client_typed::<Req, Resp>(name)` | **sync** | `AortaResult<TypedClient<Req, Resp>>` |
| `create_execution_group(policy)` | **sync** | `AortaResult<ExecutionGroup>` |
| `create_context_store()` | **sync** | `ContextStore` |
| `create_timer(period, callback)` | `.await` | `AortaResult<Timer>` |
| `create_timer_with_options(period, callback, opts)` | `.await` | `AortaResult<Timer>` |

> The table lists only the common entry points. Every typed `create_*`
> (subscriber/service/client/publisher/timer) has a matching **`_with_options`**
> overload for passing a `*Options` (for example
> `create_service_typed_view_with_options`, `create_subscriber_typed_retained_with_options`,
> `create_client_typed_with_options`); cross-namespace addressing additionally has
> **`_for`** / **`_for_with_options`** variants. `rust/src/node.rs` is authoritative
> for the full signatures.

**Note:** `create_client*` is **synchronous** (no `.await` needed) — the key difference from every other `create_*` method.

> **ContextStore**: `create_context_store()` synchronously returns a `ContextStore` (Aorta's fifth
> core primitive, a peer of Topic/Service/Action/Timer). It is imported through the path
> `aorta_sdk::contextstore::ContextStore` (**not in the prelude**). Every call mints a fresh
> `source_instance_id`, and the return value is `Clone` (backed by `Arc`), so share the handle
> instead of calling it repeatedly. See `rust/src/contextstore.rs` and `rust/src/node.rs:264`.
>
> **ensure_executor()**: the only executor-related Node entry points are `create_execution_group(policy)`
> (listed above) and `ensure_executor()` (idempotently lazy-starts the internal SDK executor; it is
> called automatically by each `create_*`, see `rust/src/node.rs:310`).

---

## 3. Publisher & Subscriber

### 3.1 Publisher Overview

`Publisher<M>` (`rust/src/publisher.rs`) is the generic message publisher, constrained to `M: SchemaMeta + HeaderInjector`. Core design:

- **Synchronous fire-and-forget**: `publish()` / `publish_typed()` / `publish_bytes()` are all synchronous calls — **no `.await` needed**
- **Zero-copy publishing**: `RecyclePool` (a bounded, mutex-guarded LIFO stack — see §11.1) plus `Bytes::from_owner()` gives zero memcpy in steady state
- **Automatic AortaHeader injection**: timestamp, sequence number and publishing node name are filled in for you
- **A converged stable API surface**: the stable `PublisherOptions` exposes QoS, schema attach, BFBS, `max_message_size`, publish timestamp, `congestion_control` and `express`; builder/shm policies stay SDK/Core internals
- **Builder reuse strategy**: ThreadLocal / mutex / fresh-builder are SDK implementation details
- **Statistical observability**: `stats()` returns publish counts, oversize-message statistics, reentrancy counts and more

### 3.2 Creating a Publisher

#### 3.2.1 Typed Publisher (Recommended)

```rust
// Signature
pub async fn create_publisher<M: SchemaMeta + HeaderInjector>(
    &self, topic: &str,
) -> AortaResult<Publisher<M>>
```

```rust
// Example (from rust/examples/talker.rs)
let publisher = node.create_publisher::<ChatSchema>("/demo/chatter").await?;

// Inspect the schema hash
if let Some(hash) = publisher.schema_hash() {
    aorta_info!("schema hash={}", hash.to_hex());
}
```

#### 3.2.2 Creating with Options

```rust
// Signature
pub async fn create_publisher_with_options<M: SchemaMeta + HeaderInjector>(
    &self, topic: &str, options: PublisherOptions,
) -> AortaResult<Publisher<M>>
```

```rust
// Example (from rust/examples/sensor_publisher.rs)
let mut options = PublisherOptions::default();
options.qos = QoS::realtime_control();
let imu_pub = node
    .create_publisher_with_options::<ImuDataSchema>("/sensors/imu", options)
    .await?;
```

#### 3.2.3 PublisherOptions

```rust
#[non_exhaustive]
pub struct PublisherOptions {
    pub schema_attach_policy: SchemaAttachPolicy,
    pub schema_bfbs: Option<Vec<u8>>,
    pub schema_root_type: Option<String>,
    pub qos: QoS,
    pub max_message_size: Option<usize>,
    pub include_publish_timestamp: bool,
    pub congestion_control: Option<CongestionControl>,
    pub express: bool,
}
```

`PublisherOptions` supports the builder pattern:

```rust
let options = PublisherOptions::new()
    .with_qos(QoS::realtime_control())
    .with_max_message_size(64 * 1024)
    .with_publish_timestamp(true)
    .with_congestion_control(CongestionControl::Block)
    .with_express(true);
```

| Field | Description |
|------|------|
| `qos` | QoS configuration (priority, reliability, congestion control, express) |
| `schema_attach_policy` | Controls whether the schema hash is attached (`Inherit` / `Always` / `Never`) |
| `schema_bfbs` | Schema binary reflection data (filled in automatically for a typed publisher) |
| `schema_root_type` | Fully-qualified root table name matching `schema_bfbs`, so decoders can select a non-declared root table |
| `max_message_size` | Builder initial capacity (bytes) |
| `include_publish_timestamp` | Whether to fill `publish_stamp_ns` in the header (default `true`) |
| `congestion_control` | Per-publisher **override** of `qos.congestion_control`; `None` (the default) inherits the QoS value, `Some(_)` wins. `Drop` (the QoS default) drops when the egress queue is full — lowest latency; `Block` backpressures the sender instead of dropping locally on congestion (not an end-to-end no-loss guarantee). Set via `.with_congestion_control(...)` |
| `express` | Whether to bypass zenoh batching and send immediately (default `false`; ORed with `qos.express`); `.with_express(...)` |

### 3.3 Publishing Messages

#### 3.3.1 publish_typed (Recommended)

The most concise way to publish: fill in the generated message struct through a lambda:

```rust
// Signature
pub fn publish_typed<'d, F>(&self, fill: F) -> AortaResult<()>
where
    M: TypedPublishMessage,
    F: FnOnce(&mut M::Msg<'d>),
```

```rust
// Example (from rust/examples/talker.rs)
publisher.publish_typed(|msg: &mut ChatMsg| {
    msg.text = Some(format!("hello {}", counter));
})?;  // synchronous, no .await
```

**Workflow:**

1. Construct an `M::Msg::default()` message struct
2. Call the user lambda to fill the fields
3. Call `M::build_typed_root()` to serialize into a FlatBuffer
4. Automatically inject the AortaHeader (publish_stamp_ns + publisher_node + sequence)
5. Zero-copy send: `builder.collapse()` → `RecyclableBuffer` → `Bytes::from_owner()` → Zenoh

**About scalar vector fields:** in the code-generated `Msg` struct, vector fields of scalar type (such as `Vec<u8>` / `Vec<i32>` / `Vec<f64>`) use a borrowed slice `&'a [T]` rather than an owned `Vec<T>`. This lets the callback borrow existing data directly and avoid an extra copy:

```rust
// data is an already-existing Vec or slice
let data = vec![1u8, 2, 3, 4];
publisher.publish_typed(|msg: &mut MyMsg| {
    // borrow, not copy
    msg.data = Some(&data);
})?;
```

Make sure the borrowed data stays valid until the `publish_typed` call returns.

#### 3.3.2 publish_typed_msg

Pass an already-filled message struct directly:

```rust
pub fn publish_typed_msg(&self, msg: &M::Msg<'_>) -> AortaResult<()>
where M: TypedPublishMessage,
```

```rust
let mut msg = ChatMsg::default();
msg.text = Some("hello".to_string());
publisher.publish_typed_msg(&msg)?;
```

#### 3.3.3 publish (Builder Callback)

Build the message by hand with a FlatBufferBuilder — for cases that are not `TypedPublishMessage`:

```rust
// Signature
pub fn publish<F>(&self, build: F) -> AortaResult<()>
where
    for<'b> F: FnOnce(&mut FlatBufferBuilder<'b>, WIPOffset<M::Header<'b>>),
```

```rust
// Example (from rust/examples/sensor_publisher.rs)
imu_pub.publish(|builder, header| {
    let accel = sensor_fb::Vector3::new(1.0, 2.0, 9.81);
    let gyro = sensor_fb::Vector3::new(0.1, 0.05, 0.0);
    let data = sensor_fb::ImuData::create(
        builder,
        &sensor_fb::ImuDataArgs {
            aorta_header: Some(header),  // the header supplied by the SDK
            acceleration: Some(&accel),
            gyroscope: Some(&gyro),
            magnetometer: None,
            sensor_timestamp_ns: unix_time_ns(),
            temperature: 25.0,
        },
    );
    builder.finish(data, None);
})?;
```

**Note:** the `header` argument is built automatically by the SDK (timestamp, sequence number, node name); you only need to embed it in the message's `aorta_header` field.

#### 3.3.4 publish_bytes (Raw Bytes)

Send raw bytes directly, without adding an AortaHeader and without going through FlatBuffers:

```rust
pub fn publish_bytes<IntoZBytes>(&self, payload: IntoZBytes) -> AortaResult<()>
where IntoZBytes: Into<ZBytes>,
```

```rust
// Several types are accepted
publisher.publish_bytes(vec![1u8, 2, 3])?;          // Vec<u8>
publisher.publish_bytes(b"raw bytes")?;              // &[u8]
publisher.publish_bytes(bytes::Bytes::from("data"))?; // Bytes (zero-copy)
publisher.publish_bytes("string payload")?;          // &str / String
```

#### 3.3.5 undeclare

Release the Publisher asynchronously:

```rust
publisher.undeclare().await?;
```

#### 3.3.6 Readiness Gating

To avoid the "publish-before-route-known" race (messages published before the route is established are silently dropped),
`Publisher<M>` offers two readiness-gating methods (see `rust/src/publisher.rs:636,645`):

```rust
// One-shot check: does this publisher's key currently have a matching subscriber?
// Reads local routing tables, no network round-trip.
pub async fn matching_status(&self) -> AortaResult<bool>;

// Wait, bounded by timeout, for a matching subscriber to appear.
// Ok(true) = matched; Ok(false) = timed out (a timeout is not an error).
pub async fn wait_for_matching(&self, timeout: std::time::Duration) -> AortaResult<bool>;
```

| Method | Description |
|------|------|
| `matching_status()` | One-shot local check, no network round-trip; `Ok(true)` means a matching subscriber exists right now |
| `wait_for_matching(timeout)` | Polls until a matching subscriber appears; `Ok(true)` = matched, `Ok(false)` = timed out (**a false return on timeout is not an error**). Use it to gate the first publish on a freshly created publisher |

```rust
// Gate the first publish on a new publisher so no message is lost while the route is unknown
if publisher.wait_for_matching(std::time::Duration::from_secs(1)).await? {
    publisher.publish_typed(|msg: &mut ChatMsg| {
        msg.text = Some("first".to_string());
    })?;
}
```

> You can also read this publisher's key (topic) via `publisher.topic()`. `inner()` is an internal detail and not part of the stable surface.

### 3.4 Publisher Statistics

```rust
#[derive(Clone, Debug, Default)]
pub struct PublisherStats {
    pub publish_count: u64,                 // total publishes
    pub oversize_count: u64,                // times the initial capacity was exceeded
    pub reentrant_count: u64,               // reentrancy detections
    pub tls_builder_publish_count: u64,     // publishes that went through the ThreadLocal builder
    pub mutex_builder_publish_count: u64,   // publishes that went through the mutex builder
    pub fresh_builder_publish_count: u64,   // publishes that went through a fresh builder
    pub recycle_pool_hit_count: u64,        // RecyclePool hits
    pub recycle_pool_miss_count: u64,       // RecyclePool misses
    pub mutex_mode_switch_count: u64,       // times the strategy switched to mutex mode
    pub last_oversize_size: usize,          // byte size of the most recent oversize message
    pub current_capacity: usize,            // current builder capacity
    pub max_capacity_seen: usize,           // largest capacity observed
    pub last_publish_ns: u64,               // timestamp of the last publish
    pub send_error_count: u64,              // send failures
    pub build_total_ns: u64,                // cumulative build time
    pub build_max_ns: u64,                  // longest build time
    pub build_sample_count: u64,            // number of build samples
    pub transport_total_ns: u64,            // cumulative transport time
    pub transport_max_ns: u64,              // longest transport time
    pub transport_sample_count: u64,        // number of transport samples
}

let stats = publisher.stats();
aorta_info!("Published {} messages", stats.publish_count);
```

### 3.5 Subscriber Overview

A Subscriber receives messages through a callback. Key characteristics:

- **Callback signature**: `Fn(&ZBytes, &MessageContext)` — it receives Zenoh's native `ZBytes`
- **Zero-copy access**: the `with_contiguous_bytes()` helper hands you a contiguous byte slice
- **Header context**: optionally parses sequence / publisher_node out of the AortaHeader
- **Deduplication (dedup)**: sliding-window deduplication keyed on `(topic, publisher_node, sequence)`; when no stable publisher identity is present in the header, the transport source identity is used as a fallback, and if neither is available the message skips dedup/reorder checks entirely
- **Out-of-order detection (reorder)**: detects and optionally drops out-of-order messages
- **Typed Subscriber**: automatically injects a header layout hint to speed up parsing

### 3.6 Creating a Subscriber

#### 3.6.1 Basic Creation

```rust
// Signature
pub async fn create_subscriber<F>(
    &self, topic: &str, callback: F,
) -> AortaResult<Subscriber>
where F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
```

```rust
// Example
let _sub = node
    .create_subscriber("/demo/chatter", |payload, ctx| {
        with_contiguous_bytes(payload, |bytes| {
            aorta_info!("Received {} bytes", bytes.len());
        });
    })
    .await?;
```

#### 3.6.2 Typed Subscriber: View vs Retained

Rust typed subscribe is explicitly split into two interfaces today:

- `create_subscriber_typed_view::<M, _>()`: a borrowed view after decoding, valid only inside the callback
- `create_subscriber_typed_retained::<M, _>()`: use this when you need to keep the payload beyond the callback

**View API**

```rust
pub async fn create_subscriber_typed_view<M, F>(
    &self, topic: &str, callback: F,
) -> AortaResult<Subscriber>
where
    M: TypedSubscribeMessage,
    F: for<'a> Fn(M::Root<'a>, &MessageContext) + Send + Sync + 'static,
```

```rust
let _sub = node
    .create_subscriber_typed_view::<ChatSchema, _>("/demo/chatter", |msg, ctx| {
        let _ = ctx;
        aorta_info!("text={}", msg.text().unwrap_or(""));
    })
    .await?;
```

**Retained API**

```rust
pub async fn create_subscriber_typed_retained<M, F>(
    &self, topic: &str, callback: F,
) -> AortaResult<Subscriber>
where
    M: TypedSubscribeMessage,
    F: Fn(TypedRetainedSample<M>) + Send + Sync + 'static,
```

```rust
let _sub = node
    .create_subscriber_typed_retained::<ChatSchema, _>("/demo/chatter", |sample| {
        if let Ok(msg) = sample.decode() {
            aorta_info!("retained text={}", msg.text().unwrap_or(""));
        }
    })
    .await?;
```

When `header_context` / `dedup` / `reorder_check` is enabled, both kinds of typed subscriber automatically infer the `header_layout_hint` from `M::schema_bfbs()`, letting the Core parse the AortaHeader directly by fixed field id.

#### 3.6.3 Creating with Options

```rust
// Example (from rust/examples/listener.rs)
let options = SubscriberOptions::new()
    .with_header_context(true)
    .with_topic_in_context(true)
    .with_receive_timestamp(true)
    .with_priority_in_context(true)
    .with_publisher_timestamp(true)
    .with_dedup(true)
    .with_dedup_window(1024)
    .with_reorder_check(true)
    .with_drop_out_of_order(true);

let _sub = node
    .create_subscriber_with_options("/demo/chatter", callback, options)
    .await?;
```

#### 3.6.4 Creating with QoS

```rust
let _sub = node
    .create_subscriber_with_qos("/sensor/imu", callback, QoS::realtime_control())
    .await?;
```

#### 3.6.5 Subscriber Handle Methods

Every `create_subscriber*` returns a `Subscriber` handle. It is more than a callback registration — it also
provides the following methods (see `core/src/subscriber.rs:2192,2345,2356`):

| Method | Signature | Description |
|------|------|------|
| `topic()` | `fn topic(&self) -> &str` | This subscriber's key (topic) |
| `runtime_stats()` | `fn runtime_stats(&self) -> SubscriberRuntimeStats` | Runtime statistics snapshot (dedup / out-of-order / overflow, see the table below) |
| `undeclare()` | `async fn undeclare(self) -> AortaResult<()>` | Asynchronously undeclare and release (consumes `self`) |

```rust
let stats = subscriber.runtime_stats();
aorta_info!("dropped(dup)={}", stats.duplicate_drop_count);

subscriber.undeclare().await?;
```

##### Subscriber Runtime Statistics / SubscriberRuntimeStats

`runtime_stats()` returns a `SubscriberRuntimeStats` (`#[derive(Clone, Copy, Default)]`, see
`core/src/subscriber.rs:349`). The fields cross-reference §11.4 (dedup / out-of-order) and §11.5 (overflow):

| Field | Description |
|------|------|
| `duplicate_drop_count` | Messages dropped because the dedup window judged them duplicates (see §11.4) |
| `out_of_order_count` | Out-of-order messages detected |
| `out_of_order_drop_count` | Messages dropped as out-of-order when `drop_out_of_order` is enabled (see §11.4) |
| `executor_drop_count` | Callbacks dropped because enqueueing on the executor failed or was rejected |
| `local_drop_count` | Local queue drops |
| `overflow_relay_enqueue_count` | Overflow relay queue enqueues (see §11.5) |
| `overflow_relay_drop_count` | Overflow relay queue drops (see §11.5) |
| `receive_depth_drop_count` | Events shed by the Tier A `receive_depth` gate (drop-newest); always `0` when `receive_depth == 0` and for pull subscribers (see §3.10) |
| `dropped_for_deadline` | Events dropped at dequeue past their `FlowAdmission` deadline |

### 3.7 The with_contiguous_bytes Helper

`with_contiguous_bytes()` (`rust/src/zbytes.rs`) gives safe ZBytes → `&[u8]` access:

```rust
pub fn with_contiguous_bytes<R, F>(payload: &ZBytes, f: F) -> R
where F: FnOnce(&[u8]) -> R,
```

**How it works:**

- **Fast path**: if the ZBytes holds a single slice (the overwhelming majority of cases), it **borrows directly with zero copy**
- **Fallback path**: if the ZBytes is made of several slices, they are merged into a contiguous buffer first, then passed in

```rust
// Recommended pattern (from rust/examples/listener.rs)
with_contiguous_bytes(payload, |bytes| {
    if let Ok(chat) = demo::root_as_chat(bytes) {
        let text = chat.text().unwrap_or("");
        aorta_info!("Received: {}", text);
    }
});
```

### 3.8 MessageContext

```rust
pub struct MessageContext {
    pub topic: Arc<str>,             // the publish key expression
    pub receive_stamp_ns: i64,       // receive timestamp
    pub priority: Priority,          // priority (enum)
    pub publisher_node: Option<String>,  // publishing node name
    pub publisher_stamp_ns: Option<i64>, // publisher timestamp
    pub sequence: Option<u64>,       // sequence number
    pub source_namespace: Option<String>, // source application-level namespace (e.g. a per-device identifier); populated only for *_for cross-namespace subscribers, None for ordinary ones
}
```

> `source_namespace` is populated only when the subscriber enables namespace capture
> (`SubscriberOptions::with_source_namespace_capture(true)`, set automatically by the `*_for`
> cross-namespace creation methods); ordinary subscribers get `None` and stay on the fast path.
> See `core/src/subscriber.rs:95,532`.

Context fields have to be enabled through `SubscriberOptions`:

| Option method | Context fields enabled |
|---------|-------------------|
| `.with_header_context(true)` | `publisher_node`, `sequence` (parsed from the AortaHeader) |
| `.with_topic_in_context(true)` | `topic` |
| `.with_receive_timestamp(true)` | `receive_stamp_ns` |
| `.with_priority_in_context(true)` | `priority` |
| `.with_publisher_timestamp(true)` | `publisher_stamp_ns` |

### 3.9 SubscriberOptions in Detail

`SubscriberOptions` supports the full builder pattern:

```rust
let options = SubscriberOptions::new()
    // QoS (the 4-field model)
    .with_qos(QoS::sensor_data())
    // Buffering: local executor queue depth (unrelated to QoS); for the per-topic cap see .with_receive_depth(...) (§3.10)
    .with_history_depth(5)
    // Header and context
    .with_header_context(true)
    .with_topic_in_context(true)
    .with_receive_timestamp(true)
    .with_priority_in_context(true)
    .with_publisher_timestamp(true)
    // Deduplication and out-of-order handling
    .with_dedup(true)
    .with_dedup_window(1024)
    .with_reorder_check(true)
    .with_drop_out_of_order(true);
```

### 3.10 Per-Topic Subscription Buffering (receive_depth / Pull Subscriptions)

The QoS & buffering redesign introduced **per-topic** subscribe-side backpressure, orthogonal to QoS.

**Tier A — `SubscriberOptions::receive_depth` (in-flight cap + drop isolation)**

`receive_depth: usize` is the in-flight event cap for a single topic. `0` (the default) disables the
per-topic gate — events flow straight into the shared executor queue, which is itself bounded (1024 events
for the Node's auto-created executor; for an explicit executor the capacity is the constructor argument you
pass). With `>0`, once the cap is reached **the newest event is dropped**
(drop-newest), which confines one slow topic's congestion to itself instead of dragging down the shared
executor. The dropped count is observable via `Subscriber::dropped_for_depth()`:

```rust
let options = SubscriberOptions::new().with_receive_depth(16);
let sub = node
    .create_subscriber_with_options("/sensors/lidar", callback, options)
    .await?;
// ... after running for a while
aorta_info!("shed by depth cap = {}", sub.dropped_for_depth());
```

**Tier B-pull — pull-mode subscribers (`create_subscriber_pull`)**

A pull subscriber owns a Zenoh-native keep-last ring; there is **no callback** — you drain it yourself. It is
the "latest value wins" sensor idiom (similar to rclpy's `wait_for_message`), with structural per-topic
isolation and no executor hop (it does not start the SDK's auto-executor either). The SDK `Node` provides
`create_subscriber_pull` / `create_subscriber_pull_with_options` directly; the returned `Subscriber`
offers `recv()` / `try_recv()` / `recv_timeout(d)`, yielding `PullSample { payload, context }`
(`DeliveryMode`, `ReceiveOverflow` and `PullSample` are all exported directly from `aorta_sdk`):

```rust
let sub = node
    .create_subscriber_pull("/sensors/lidar", 8)  // ring capacity = 8
    .await?;

// Drain it in your own loop, latest value wins
loop {
    let sample = sub.recv()?;  // blocks until a sample arrives
    process(&sample.payload, &sample.context);
}
// Or non-blocking / with a timeout:
// let maybe = sub.try_recv()?;
// let maybe = sub.recv_timeout(Duration::from_millis(50))?;
```

---

## 4. RPC: Service & Client

### 4.1 Overview

The Aorta RPC framework implements request/response on top of Zenoh Query/Queryable:

- **Service**: the server side; registers a Queryable to handle requests, handler signature `Fn(ServiceRequest)`
- **Client**: the client side; sends a Query and **awaits** the response asynchronously
- **Typed RPC**: automatically injects the request/response schema BFBS
- **Key difference**: the Rust SDK's `create_client()` is **synchronous** (no `.await` needed), but `call()` is **asynchronous**

### 4.2 Creating a Service

#### 4.2.1 Basic Service

```rust
// Signature
pub async fn create_service<F>(
    &self, service_name: &str, handler: F,
) -> AortaResult<Service>
where F: Fn(ServiceRequest) + Send + Sync + 'static,
```

```rust
// Generic create_service_with_options snippet (single-argument Fn(ServiceRequest) handler)
// Note: echo_server.rs itself uses the typed view path, create_service_typed_view_with_options
let mut service = node
    .create_service_with_options(
        "/echo",
        |request| {
            let response = request.payload().clone();
            // reply() is async, so tokio::spawn is needed
            tokio::spawn(async move {
                match request.reply(response).await {
                    Ok(_) => aorta_info!("Replied echo"),
                    Err(err) => aorta_error!("Reply failed: {}", err),
                }
            });
        },
        ServiceOptions::new(),
    )
    .await?;
```

**Important:** the service handler signature is `Fn(ServiceRequest)` — not `async` — but `ServiceRequest::reply()` is asynchronous. The recommended pattern is to run the async reply via `tokio::spawn()` inside the handler.

#### 4.2.2 Schema-Typed Service Compatibility API

```rust
// Signature
pub async fn create_service_typed<RequestSchema, ResponseSchema, F>(
    &self, service_name: &str, handler: F,
) -> AortaResult<Service>
where
    RequestSchema: SchemaMeta,
    ResponseSchema: SchemaMeta,
    F: Fn(ServiceRequest) + Send + Sync + 'static,
```

```rust
// Example (from rust/examples/navigation_service_server.rs)
let mut get_plan_service = node
    .create_service_typed_with_options::<GetPlanRequestSchema, GetPlanResponseSchema, _>(
        "/get_plan",
        move |request| {
            let payload_view = request.payload().clone();
            with_contiguous_bytes(&payload_view, |payload| {
                let req = match nav_fb::root_as_get_plan_request(payload) {
                    Ok(req) => req,
                    Err(err) => {
                        tokio::spawn(async move {
                            let _ = request.reply_error(
                                &format!("invalid payload: {}", err)
                            ).await;
                        });
                        return;
                    }
                };
                let response = build_get_plan_response(req);
                tokio::spawn(async move {
                    let _ = request.reply(response).await;
                });
            });
        },
        ServiceOptions::new(),
    )
    .await?;
```

This API is a **schema-injection helper**:

- it injects service BFBS + root type metadata automatically
- it does **not** decode the request into a typed view
- it does **not** build typed responses for you

Use it when you want schema registration convenience while still keeping a raw `ServiceRequest` handler.

**typed schema constraints:**

- `RequestSchema` and `ResponseSchema` must have the same `schema_hash()` (one FlatBuffers schema file defines both request and response)
- Runtime check: if the hashes do not match, `AortaError::invalid_argument` is returned
- The SDK derives `request_root_type` and `response_root_type` from `SchemaMeta::root_type()` automatically

#### 4.2.3 End-to-End Typed Service APIs

```rust
pub async fn create_service_typed_view<Request, Response, F>(
    &self, service_name: &str, handler: F,
) -> AortaResult<Service>
where
    Request: TypedRpcRequestMessage,
    Response: TypedRpcResponseMessage + Send + Sync + 'static,
    F: for<'a> Fn(Request::Root<'a>, TypedServiceResponder<Response>) + Send + Sync + 'static;

pub async fn create_service_typed_retained<Request, Response, F>(
    &self, service_name: &str, handler: F,
) -> AortaResult<Service>
where
    Request: TypedRpcRequestMessage + Send + Sync + 'static,
    Response: TypedRpcResponseMessage + Send + Sync + 'static,
    F: Fn(TypedRetainedServiceRequest<Request, Response>) + Send + Sync + 'static;
```

Recommended usage:

- `create_service_typed_view()`
  - zero-copy fast path
  - request view is valid only within the callback
  - best for synchronous request handling
- `create_service_typed_retained()`
  - retains payload as owned bytes
  - safe to move into `tokio::spawn()` or other deferred work
  - best for async reply workflows

`TypedServiceResponder<Response>` provides:

- `reply(|msg| { ... }).await`
- `reply_message(&msg).await`
- `reply_bytes(bytes).await`
- `reply_error(msg).await`

### 4.3 The ServiceRequest Object

```rust
pub struct ServiceRequest {
    // ...
}

impl ServiceRequest {
    pub fn payload(&self) -> &ZBytes;                          // get the request payload
    pub async fn reply<T: Into<ZBytes>>(self, response: T)     // send a success response
        -> AortaResult<()>;
    pub async fn reply_error(self, error_msg: &str)            // send an error response
        -> AortaResult<()>;
}
```

**Important rules:**

- `reply()` / `reply_error()` consume `self` (move semantics), so each request can be replied to exactly once
- `reply()` is an **asynchronous** operation and needs `.await`
- `payload()` returns `&ZBytes`; use `with_contiguous_bytes()` to obtain a byte slice

### 4.4 ServiceOptions

`ServiceOptions` supports the builder pattern:

```rust
pub struct ServiceOptions {
    pub serve_all_namespaces: bool,
    pub schema_attach_policy: SchemaAttachPolicy,
    pub callback_group: CallbackGroupType,
    pub callback_group_handle: Option<CallbackGroupHandle>,
    pub rpc_overflow_policy: RpcOverflowPolicy,
    pub rpc_overflow_block_timeout: Duration,
    pub schema: Option<ServiceSchemaInfo>,
}
```

```rust
let options = ServiceOptions::new()
    .with_schema_attach_policy(SchemaAttachPolicy::Never)
    .with_rpc_overflow_policy(RpcOverflowPolicy::FastFailBusy)
    .with_callback_group(CallbackGroupType::MutuallyExclusive);
```

| Field | Description |
|------|------|
| `rpc_overflow_policy` | Concurrency overflow policy: `FastFailBusy` (fail fast) or `Block` (block and wait) |
| `callback_group` | Callback group kind: `MutuallyExclusive` or `Reentrant` |
| `schema` | `ServiceSchemaInfo` (filled in automatically in typed mode) |

### 4.5 Service Lifecycle

```rust
// Stop the service (synchronous; requires &mut self)
service.stop();

// Get statistics
let stats = service.stats();
```

### 4.6 Creating a Client

**Key point:** the `create_client*` methods are **synchronous** — no `.await`:

```rust
// Three ways to create one
let client = node.create_client("/echo");                            // default options
let client = node.create_client_with_timeout("/echo", 2000);        // explicit timeout
let client = node.create_client_with_options("/echo", options);      // full options
```

### 4.7 ClientOptions

`ClientOptions` supports the builder pattern:

```rust
// Example (from rust/examples/echo_client.rs)
let options = ClientOptions::new()
    .with_timeout_ms(2000)                                     // 2 second timeout
    .with_schema_attach_policy(SchemaAttachPolicy::Never)
    .with_enforce_single_provider(true)                        // enforce a single provider
    .with_multi_reply_check_window_ms(100);                    // multi-reply detection window

let client = node.create_client_with_options("/echo", options);
```

#### 4.6.1 Typed Client

```rust
pub fn create_client_typed<Request, Response>(
    &self,
    service_name: &str,
) -> AortaResult<TypedClient<Request, Response>>
where
    Request: TypedRpcRequestMessage,
    Response: TypedRpcResponseMessage;
```

`create_client_typed()` validates the request/response schema pair and auto-injects:

- `service_schema`
- `request_root_type`
- `response_root_type`

It returns `AortaResult<_>` because typed schema mismatch is validated at client creation time.

Full options:

```rust
pub struct ClientOptions {
    pub timeout: Duration,                                // default 5 seconds
    pub schema_attach_policy: SchemaAttachPolicy,
    pub service_schema: Option<SchemaHash>,               // service schema hash
    pub request_root_type: Option<String>,                // request root type
    pub response_root_type: Option<String>,               // response root type
    pub validate_schema: bool,                            // whether to validate the schema
    pub enforce_single_provider: Option<bool>,            // single-provider mode
    pub multi_reply_check_window: Option<Duration>,       // multi-reply detection
    pub allow_retry: Option<bool>,                        // None = inherit the core default (off)
}
```

| Builder method | Description |
|-------------|------|
| `.with_timeout_ms(ms)` | Set the timeout (milliseconds) |
| `.with_timeout(duration)` | Set the timeout (`Duration`) |
| `.with_service_schema(hash)` | Declare the service schema hash |
| `.with_service_root_types(req, resp)` | Declare the request and response root types in one call |
| `.with_validate_schema(true)` | Enable schema validation (warn-only: a mismatch is logged, never rejected) |
| `.with_enforce_single_provider(true)` | Enforce a single provider |
| `.with_multi_reply_check_window_ms(ms)` | Multi-reply detection window |
| `.with_allow_retry(bool)` | Per-client RPC retry switch; **off by default** (`None` inherits the core default, OFF). Retries silently re-invoke a non-idempotent provider, so set `true` only for services that are provably idempotent |

### 4.8 Making an RPC Call

```rust
// Signature
pub async fn call<IntoZBytes>(&self, request: IntoZBytes) -> AortaResult<ZBytes>
where IntoZBytes: Into<ZBytes>,
```

`call()` is an **async blocking** operation that waits for the service reply or the timeout:

```rust
// Example (from rust/examples/echo_client.rs)
let payload = format!("echo request #{}", counter);
let request = payload.as_bytes().to_vec();

match client.call(&request).await {
    Ok(response) => {
        with_contiguous_bytes(&response, |bytes| {
            let text = String::from_utf8_lossy(bytes);
            aorta_info!("Response: {}", text);
        });
    }
    Err(err) => aorta_error!("Call failed: {}", err),
}
```

You can also specify a per-call timeout (overriding the default in `ClientOptions`):

```rust
// call_with_timeout(request, timeout_ms) — the second argument is milliseconds
let response = client.call_with_timeout(request, 3000).await?;
```

For end-to-end typed RPC, use `TypedClient<Request, Response>`:

```rust
let client = node
    .create_client_typed::<GetPlanRequestSchema, GetPlanResponseSchema>("/get_plan")?;

let response = client
    .call(|msg| {
        // fill typed request message
    })
    .await?;

let decoded = response.decode()?;
```

`TypedClientResponse<Response>` stores the raw response payload and exposes:

- `payload() -> &[u8]`
- `decode() -> AortaResult<Response::Root<'_>>`
- `into_payload() -> Vec<u8>`

**`call()` vs `call_with_timeout()`:**

| Method | Timeout source | When to use |
|------|---------|---------|
| `call(request).await` | `ClientOptions::timeout` (default 5s) | Most cases |
| `call_with_timeout(request, ms).await` | Explicit (milliseconds) | A single call needs a different timeout |

### 4.9 Client Statistics

```rust
pub struct ClientStats {
    pub call_count: u64,
    pub success_count: u64,
    pub timeout_count: u64,
    pub concurrent_conflict_count: u64,
    pub error_count: u64,
    pub retry_count: u64,
    pub last_call_started_ns: u64,
    pub last_call_finished_ns: u64,
    pub last_call_latency_ns: u64,
}

let stats = client.stats();
aorta_info!("Calls: {} success, {} timeouts", stats.success_count, stats.timeout_count);
```

### 4.9.1 Client Accessors and Readiness Gating

Besides `call` / `call_with_timeout` / `stats`, `Client` also provides the following methods
(see `rust/src/client.rs:190,241,248`), aligned with the readiness gating of the Publisher and the Python SDK:

| Method | Signature | Description |
|------|------|------|
| `service_name()` | `fn service_name(&self) -> &str` | The service key this client is bound to |
| `matching_status()` | `async fn matching_status(&self) -> AortaResult<bool>` | One-shot check: does this service key currently have a matching provider? Reads local routing tables, **no network round-trip** |
| `wait_for_matching(timeout)` | `async fn wait_for_matching(&self, timeout: Duration) -> AortaResult<bool>` | Wait up to `timeout` for a provider to appear; `Ok(true)` = matched, `Ok(false)` = timed out (**a timeout is not an error**). Use it to gate the first `call()` on a fresh client |

```rust
// Gate the first call so no request goes out before the provider route is established
if client.wait_for_matching(std::time::Duration::from_secs(2)).await? {
    let response = client.call(request).await?;
}
```

### 4.10 Full RPC Example

**Navigation Service Client (from `rust/examples/navigation_service_client.rs`):**

```rust
let node = Node::new("nav_client", "default").await?;

// Compute the schema hash from the BFBS
let service_schema = SchemaHash::from_bfbs_bytes(NAVIGATION_BFBS);

// Create a client with schema validation (synchronous — no .await!)
let client = node.create_client_with_options(
    "/get_plan",
    ClientOptions::new()
        .with_timeout_ms(5000)
        .with_service_schema(service_schema)
        .with_service_root_types(
            "aorta.services.navigation.GetPlanRequest",
            "aorta.services.navigation.GetPlanResponse",
        )
        .with_validate_schema(true),
);

// Build the FlatBuffer request
let request = build_get_plan_request(/* ... */);

// Async call
match client.call(request).await {
    Ok(response) => {
        with_contiguous_bytes(&response, |bytes| {
            let msg = flatbuffers::root::<nav_fb::GetPlanResponse>(bytes)?;
            aorta_info!("Plan: {} waypoints, length={:.3}",
                msg.waypoint_count(), msg.path_length());
            Ok::<(), Box<dyn std::error::Error>>(())
        })?;
    }
    Err(err) => aorta_error!("GetPlan failed: {}", err),
}
```

---

## 5. Timer

### 5.1 Overview

A timer provides periodic callback execution. In the Rust SDK the timer is driven by an executor that the node manages internally — **you do not need to create an executor by hand**.

```
create_timer() → Node.ensure_executor()
                    ↓
                 [automatically creates SingleThreadedExecutor + dedicated thread + current-thread Tokio runtime]
                    ↓
                 the timer callback fires periodically on the executor thread
```

### 5.2 Creating a Timer

```rust
// Basic creation
pub async fn create_timer<F>(
    &self, period: Duration, callback: F,
) -> AortaResult<Timer>
where F: Fn() + Send + Sync + 'static,

// Creation with options
pub async fn create_timer_with_options<F>(
    &self, period: Duration, callback: F, options: TimerOptions,
) -> AortaResult<Timer>
where F: Fn() + Send + Sync + 'static,
```

```rust
// Example (from rust/examples/timer_demo.rs)
let counter = Arc::new(AtomicU64::new(0));
let counter_clone = Arc::clone(&counter);

let _timer = node
    .create_timer(Duration::from_millis(100), move || {
        let count = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;
        aorta_info!("Timer tick: count = {}", count);
    })
    .await?;
```

**Note:** `create_timer()` internally calls `Node::ensure_executor()`. If you have already attached the `Node` to an explicit executor, the timer reuses that external executor; only when no executor is attached yet does the SDK fall back to the internal `SingleThreadedExecutor` (queue size 1024, driving `spin()` on a dedicated thread).

### 5.3 TimerOptions

```rust
let options = TimerOptions::new()
    .with_auto_start(false)        // do not start automatically after creation
    .with_invoke_on_missed(true)   // compensate for missed tick periods
    .with_overflow_policy(TimerOverflowPolicy::Block)  // block on overflow
    .with_overflow_block_timeout(Duration::from_millis(100));  // block timeout
```

| Method | Default | Description |
|------|--------|------|
| `.with_auto_start(bool)` | `true` | Whether the timer starts firing right after creation |
| `.with_invoke_on_missed(bool)` | `false` | Whether to compensate for missed tick periods |
| `.with_overflow_policy(policy)` | `CoalesceLatest` | Queue overflow policy |
| `.with_overflow_block_timeout(dur)` | `0` (the runtime raises it to a floor of `max(period × 5, 2 ms)`) | Timeout for the `Block` policy |

#### TimerOverflowPolicy

Behavior when the executor queue is full and the timer event cannot be enqueued:

| Policy | Behavior |
|------|------|
| `CoalesceLatest` (default) | Coalesce into the latest single tick; the skipped count is recorded in `coalesced_tick_count()` |
| `Block` | Block waiting for queue space; timeouts are recorded in `overflow_timeout_count()` |

### 5.4 Timer Control

The timer offers a runtime control API; all methods are **synchronous** (no `.await`):

```rust
// Pause (from rust/examples/timer_demo.rs)
timer.pause();

// Resume
timer.resume();

// Stop (irreversible, consumes &mut self)
timer.stop();
```

#### Commonly used control and statistics methods

| Method | Signature | Description |
|------|------|------|
| `pause()` | `&self` | Pause firing (resumable) |
| `resume()` | `&self` | Resume firing |
| `stop()` | `&mut self` | Stop permanently (irreversible) |
| `is_running()` | `&self → bool` | Whether it is running (not paused / not stopped) |
| `period()` | `&self → Duration` | Get the period |
| `coalesced_tick_count()` | `&self → u64` | Number of ticks coalesced away on overflow |
| `overflow_timeout_count()` | `&self → u64` | Number of overflow block timeouts |

Beyond these, `Timer` exposes further deadline/coalescing statistics accessors —
`deadline_slip_total_ns()` / `deadline_slip_max_ns()`, `due_firings_total()` /
`due_firings_max()` / `due_sample_count()`, and the
`coalesce_requeue_count()` / `coalesce_inline_dispatch_*()` /
`coalesce_event_wait_*()` family — see `core/src/timer.rs`.

```rust
// State queries and statistics
if timer.is_running() {
    aorta_info!("Timer period: {:?}", timer.period());
}

let coalesced = timer.coalesced_tick_count();
if coalesced > 0 {
    aorta_warn!("Timer coalesced {} ticks (executor overloaded?)", coalesced);
}
```

> Source reference: `Timer` is a direct re-export from Core (`aorta_core::timer::Timer`), defined in `core/src/timer.rs` (`pub struct Timer` around line 837, `impl Timer` around line 1875)

```rust
// Full example: auto_start=false + manual resume
let options = TimerOptions::new().with_auto_start(false);
let timer = node
    .create_timer_with_options(
        Duration::from_millis(100),
        || aorta_info!("tick"),
        options,
    )
    .await?;

// The timer does not fire yet
tokio::time::sleep(Duration::from_millis(500)).await;

// Start it manually
timer.resume();
```

### 5.5 Timer + Publisher

You can call `publish()` directly inside a timer callback, because publishing is a synchronous operation:

```rust
// Example (from rust/examples/simple_timer_publish.rs)
let publisher = node.create_publisher::<MySchema>("/counter").await?;

let counter = Arc::new(AtomicU64::new(0));
let counter_clone = Arc::clone(&counter);

let _timer = node
    .create_timer(Duration::from_millis(100), move || {
        let count = counter_clone.fetch_add(1, Ordering::Relaxed);

        // publish() is synchronous — call it directly in the timer callback
        let _ = publisher.publish(|builder, header| {
            let msg = create_counter_msg(builder, header, count, 0.0);
            builder.finish(msg, None);
        });
    })
    .await?;
```

---

## 6. Executor

### 6.1 Overview

In the Rust SDK, a node automatically makes sure an internal executor exists the first time you create a subscriber, service, or timer, so most applications **do not need to create an executor by hand**:

- **Subscriber**: creation makes sure the node is wired to some executor-backed event sender
- **Service**: creation likewise requires an executor-backed event sender
- **Timer**: `create_timer()` calls `ensure_executor()` automatically

If you need fine-grained control (multi-threaded executor, priority scheduling), you can use the executor types re-exported from Core directly.

### 6.2 Executor Types

The Rust SDK re-exports Core's executor types through `prelude`:

```rust
use aorta_sdk::prelude::*;

// Available types
// SingleThreadedExecutor — single-threaded executor
// MultiThreadedExecutor  — multi-threaded executor
// ExecutorOptions        — executor options
// SchedulerMode          — FIFO / Priority
// StopHandle             — stop handle
// SpinOutcome            — spin result
```

### 6.3 ExecutionGroup

If all you need is to control whether a set of callbacks runs serially or concurrently, you do not have to hand-roll a callback group handle; the Rust SDK offers the more direct `ExecutionGroup`:

```rust
let group = node.create_execution_group(ExecutionPolicy::Serialized)?;

let sub_options = group.subscriber_options(SubscriberOptions::new());
let service_options = group.service_options(ServiceOptions::new());
let timer_options = group.timer_options(TimerOptions::new());
```

`ExecutionGroup` releases the underlying callback group handle automatically on drop, which makes it a good fit for binding subscribers / services / timers to the same concurrency domain.

### 6.4 Creating an Executor Manually

**Three hard rules**:

1. **A node with live timers/services cannot be handed over to another executor.**
   Subscribers migrate along with `detach_nodes()` + re-attach; timers and services
   do not (they snapshot the sender at construction time), so such a handoff is
   rejected with an explanation. Drop the timers/services first, or simply do not
   move the node.
2. **Attach first, then create any callback primitive.** A node belongs to the *first*
   executor that attaches it; attaching it to a second executor afterwards is
   **rejected** (`try_attach_sdk_node` returns `InvalidArgument`; `attach_sdk_node` /
   the underlying `add_node` log an ERROR). Use `NodeOptions::with_auto_executor(false)`
   to turn this ordering requirement into a **hard constraint** instead of something
   you have to remember: creating a subscriber / service / timer / execution group
   before the attach then fails outright instead of quietly starting an SDK-internal
   executor.
3. **Do not `spin()` on the application's shared tokio runtime.** Use
   `spawn_executor_thread()` to put the spin on a **dedicated OS thread with its own
   `current_thread` runtime** (exactly the shape of the SDK's internal auto-executor).
   `tokio::spawn`-ing `exec.spin()` onto the application's multi-threaded runtime makes
   the dispatch loop compete with zenoh's RX tasks for the same workers: at 64 B @ 1 kHz
   the measured cost is about **+96 µs** p50 receive latency and **5.4×** subscriber-thread
   CPU.

```rust
use aorta_sdk::prelude::*;
use aorta_sdk::{spawn_executor_thread, ExecutorNodeAttachExt, ExecutorOptions,
                SingleThreadedExecutor};

// 1) Declare "the application manages the executor" — wrong ordering now errors
//    instead of silently degrading
let node = Node::with_options(
    "app",
    "default",
    NodeOptions::default().with_auto_executor(false),
).await?;

// 2) Attach first
let mut exec = SingleThreadedExecutor::new_with_options(
    1024,                                   // queue size is a constructor argument, not in ExecutorOptions
    ExecutorOptions {
        force_tokio_queue: true,            // prerequisite for spawn_executor_thread, see 6.5
        ..ExecutorOptions::default()
    },
);
exec.try_attach_sdk_node(&node)?;

// 3) Then create callback primitives
let _sub = node.create_subscriber("demo/topic", |_p, _c| {}).await?;

// 4) Spin on a dedicated thread (auto stop + join on drop)
let mut spin = spawn_executor_thread("app-exec", exec)?;
// ... when you need to stop
spin.stop();
```

Treat this path as the **normal production usage of the Rust SDK**: the application creates the executor explicitly, attaches the `Node` to it, and then controls the `spin()` lifecycle itself.

### 6.5 ExecutorOptions

```rust
pub struct ExecutorOptions {
    pub mode: SchedulerMode,
    pub priority: PrioritySchedulerOptions,
    pub admission: ExecutorAdmissionOptions,
    pub class_policy: ExecutorClassPolicyOptions,
    pub force_tokio_queue: bool,
}
```

> Queue capacity is **not** in `ExecutorOptions` — it is a constructor argument, e.g.
> `SingleThreadedExecutor::new(1024)` or `new_with_options(1024, options)` (see
> `core/src/executor.rs:1942-1946`). `force_tokio_queue` forces the tokio mpsc queue
> backend (skipping the crossbeam+eventfd path) and must be enabled when the executor
> runs inside a `current_thread` tokio runtime (which is what the SDK's internal
> auto-executor does).

### 6.6 SchedulerMode

```rust
pub enum SchedulerMode {
    Fifo,      // first in, first out (default)
    Priority,  // priority scheduling
}
```

### 6.7 Admission Control

```rust
pub struct ExecutorAdmissionOptions {
    pub max_queued_events: usize,  // max queued events (0 = unlimited)
    pub max_queued_bytes: usize,   // max queued bytes (0 = unlimited)
}
```

### 6.8 The Node's Internal Executor

How `Node::ensure_executor()` works:

```rust
pub fn ensure_executor(&self) -> AortaResult<()> {
    // 1. Idempotent + honors the with_auto_executor(false) opt-out
    if !self.auto_executor || self.inner.has_event_sender() {
        return Ok(());
    }
    // 2. Take the lock; create a SingleThreadedExecutor with force_tokio_queue
    //    (the internal executor runs on a current_thread runtime, which does not
    //     support block_in_place, so the tokio mpsc queue backend is mandatory)
    let options = ExecutorOptions {
        force_tokio_queue: true,
        ..ExecutorOptions::default()
    };
    let mut exec = SingleThreadedExecutor::new_with_options(DEFAULT_EXECUTOR_QUEUE, options); // 1024
    exec.try_add_node(self.inner.clone())?;   // attach first, and surface a refusal
    // 3. Dedicated OS thread + current_thread runtime — the same implementation
    //    the application gets from spawn_executor_thread() (DRY); thread name
    //    aorta-sdk-exec-<node>
    let spin = spawn_executor_thread(&format!("aorta-sdk-exec-{}", self.inner.name()), exec)?;
    // 4. Store the handle; close() / the last clone being dropped stops + joins it
}
```

This mechanism is a **convenience fallback**, not the primary execution model. The recommended semantics are:

- Normal applications: create the executor explicitly, then `spin()` it yourself via
  `spawn_executor_thread()` after `try_attach_sdk_node(&node)` (for the ordering and runtime ownership, see the two hard rules in §6.4)
- Special cases: if no external executor was attached first, the SDK falls back to the internal `SingleThreadedExecutor` when you create the first subscriber / service / timer / execution_group
- This fallback suits demos, small tools, or scenarios where you must coexist with another framework and cannot conveniently host the Aorta executor explicitly
- To disable the fallback entirely (so an ordering mistake fails immediately): `NodeOptions::with_auto_executor(false)`
- `Node::has_internal_executor()` lets you assert at startup that "the executor actually dispatching is the one I configured"
- `Node::close()` stops the executor automatically

---

## 7. QoS

> The QoS dimensions, the built-in profiles (`realtime_control` / `sensor_data` / `state_update` /
> `bulk_transfer`) and their mapping to executor priority are shared cross-SDK semantics; the
> specification is
> [`spec/Aorta_SDK_Common_Concepts.md` §5](../spec/Aorta_SDK_Common_Concepts.md#5-qos-model).
> This section covers Rust's `QoS` struct and its usage.

### 7.1 Overview

After the QoS & buffering redesign, `QoS` has converged to **4 fields that are genuinely wired to Zenoh** — no silent no-ops:

| Field | Values | Description |
|------|--------|------|
| **Priority** | `Realtime` / `High` / `Normal` / `Low` / `Background` | Transmission priority |
| **Reliability** | `BestEffort` / `Reliable` | Transmission reliability |
| **CongestionControl** | `Drop` (default) / `Block` | Behavior when the local transport egress queue is congested. `Block` applies backpressure to the publishing thread instead of dropping locally on congestion; it does **not** promise end-to-end lossless or exactly-once delivery. `Drop` is the lowest-latency lossy mode |
| **express** | `bool` (default `false`) | Bypass Zenoh's internal batching and send immediately, lowering latency (especially p99) at the cost of throughput — each message triggers its own syscall. Enable it explicitly for latency-sensitive publishers |

> Removed fields: `durability` (together with the `Durability`/`TransientLocal`/`Volatile` enums) and QoS-level `history_depth` — neither was ever forwarded to Zenoh (`TransientLocal` was an unimplemented no-op), so removing them changes no runtime behavior.
>
> - **Need "get the last value after reconnect / a late-joiner snapshot"?** That is no longer QoS's job; use [`ContextStore` (§2.5)](#25-node-creation-primitives-at-a-glance) (latched / last-value / late-joiner semantics all belong to ContextStore).
> - **Need per-topic receive depth / isolation?** Use `SubscriberOptions::receive_depth` (see [§3.10](#310-per-topic-subscription-buffering-receive_depth--pull-subscriptions)).

### 7.2 The QoS Struct

```rust
pub struct QoS {
    pub priority: Priority,
    pub reliability: Reliability,
    pub congestion_control: CongestionControl,  // default Drop
    pub express: bool,                          // default false
}
```

`QoS` supports the builder pattern:

```rust
let qos = QoS::default()
    .with_priority(Priority::High)
    .with_reliability(Reliability::Reliable)
    .with_congestion_control(CongestionControl::Block)
    .with_express(true);
```

### 7.3 Preset QoS Profiles

The SDK ships 5 scenario-oriented presets:

```rust
// Default: normal priority, reliable, Drop, no express
QoS::default()
// = {Normal, Reliable, Drop, express=false}

// Realtime control: realtime (highest) priority, reliable, Drop, express (no preset
// defaults to Block; when you must not drop, set with_congestion_control(Block)
// explicitly and knowingly accept that publish() may block)
QoS::realtime_control()
// = {Realtime, Reliable, Drop, express=true}

// Sensor data: normal priority, best effort, Drop (latest value wins), no express
QoS::sensor_data()
// = {Normal, BestEffort, Drop, express=false}

// State updates: high priority, reliable, Drop, no express
// Note: this no longer implies any latched/last-value replay — use ContextStore for
// late-joiner snapshots
// If a state stream must never drop at the local egress queue, set .with_congestion_control(CongestionControl::Block)
QoS::state_update()
// = {High, Reliable, Drop, express=false}

// Bulk transfer: background priority, reliable, Drop, no express
// To avoid local drops under congestion, set .with_congestion_control(CongestionControl::Block)
QoS::bulk_transfer()
// = {Background, Reliable, Drop, express=false}
```

### 7.4 Usage Examples

```rust
// Specify QoS when creating a publisher (from rust/examples/sensor_publisher.rs)
let mut options = PublisherOptions::default();
options.qos = QoS::realtime_control();
let publisher = node
    .create_publisher_with_options::<ImuDataSchema>("/sensors/imu", options)
    .await?;

// Specify QoS when creating a subscriber (from rust/examples/sensor_subscriber.rs)
let imu_options = SubscriberOptions::new()
    .with_qos(QoS::realtime_control());
let _sub = node
    .create_subscriber_with_options("/sensors/imu", callback, imu_options)
    .await?;

// Command stream that must not drop locally: Block congestion control — backpressure rather than dropping
let cmd_qos = QoS::default()
    .with_reliability(Reliability::Reliable)
    .with_congestion_control(CongestionControl::Block);
let opts = PublisherOptions::new().with_qos(cmd_qos);
let publisher = node
    .create_publisher_with_options::<MySchema>("/robot/status", opts)
    .await?;
```

> Want late joiners to get the last value of `/robot/status`? Publish the state through [`ContextStore` (§2.5)](#25-node-creation-primitives-at-a-glance) instead of relying on QoS.

---

## 8. Schema

> The schema model (FlatBuffers + `AortaHeader`, SHA-256 schema hash,
> `SchemaAttachPolicy`, discovery/validation, conflict handling) is shared
> semantics across SDKs; the specification is
> [`spec/Aorta_SDK_Common_Concepts.md` §4](../spec/Aorta_SDK_Common_Concepts.md#4-schema-model-flatbuffers--aortaheader).
> This section gives the Rust trait bindings: `SchemaMeta`,
> `TypedPublishMessage`, and friends.

### 8.1 Overview

Aorta uses **FlatBuffers** as its message serialization framework, combined with
codegen and the trait system to provide:

- Zero-copy deserialization (the receiver reads memory directly)
- Schema-first development (define the `.fbs` first, then generate code)
- Schema discovery and validation (SHA-256 hash check)
- Automatic AortaHeader injection

### 8.2 The SchemaMeta Trait

Every message type must implement the `SchemaMeta` trait:

```rust
pub trait SchemaMeta {
    fn schema_bfbs() -> &'static [u8];           // binary schema reflection data
    fn fixed_size() -> Option<usize> { None }     // byte size for fixed-size messages
    fn root_type() -> Option<&'static str> { None }  // root type name
    fn schema_hash() -> SchemaHash {              // SHA-256 hash (computed automatically)
        SchemaHash::from_bfbs_bytes(Self::schema_bfbs())
    }
}
```

### 8.3 The HeaderInjector Trait

Publishers require the message type to implement `HeaderInjector` as well:

```rust
pub trait HeaderInjector {
    type Header<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> WIPOffset<Self::Header<'b>>;
}

pub struct HeaderValues<'a> {
    pub publish_stamp_ns: u64,
    pub publisher_node: &'a str,
    pub sequence: u64,
}
```

### 8.4 Implementing SchemaMeta + HeaderInjector by Hand

When you are not using code generation, implement them by hand (see
`rust/examples/sensor_publisher.rs`):

```rust
struct ImuDataSchema;

impl SchemaMeta for ImuDataSchema {
    fn schema_bfbs() -> &'static [u8] {
        EXAMPLES_SENSOR_BFBS  // contents of the embedded .bfbs file
    }

    fn fixed_size() -> Option<usize> {
        EXAMPLES_SENSOR_FIXED_SIZE
    }
}

impl HeaderInjector for ImuDataSchema {
    type Header<'a> = aorta::sys::AortaHeader<'a>;

    fn build_header<'b>(
        builder: &mut FlatBufferBuilder<'b>,
        values: &HeaderValues<'_>,
    ) -> WIPOffset<Self::Header<'b>> {
        let node = builder.create_string(values.publisher_node);
        aorta::sys::AortaHeader::create(
            builder,
            &aorta::sys::AortaHeaderArgs {
                publish_stamp_ns: values.publish_stamp_ns,
                publisher_node: Some(node),
                sequence: values.sequence,
            },
        )
    }
}
```

### 8.5 The TypedPublishMessage Trait

Using `publish_typed()` requires an additional `TypedPublishMessage`
implementation (normally emitted by the code generator):

```rust
pub trait TypedPublishMessage: SchemaMeta + HeaderInjector {
    type Msg<'a>: Default;
    type Root<'a>;

    fn build_typed_root<'a>(
        builder: &mut FlatBufferBuilder<'a>,
        header: WIPOffset<Self::Header<'a>>,
        msg: &Self::Msg<'_>,
    ) -> WIPOffset<Self::Root<'a>>;
}
```

> **Note**: in generated `Msg` structs, scalar vector fields (such as `Vec<u8>`)
> use a borrowed slice `&'a [T]` rather than an owned `Vec<T>`. This is the key
> to the zero-copy optimization — inside a `publish_typed` callback you can
> assign borrowed data directly.

### 8.6 The FbSchema Generic Pattern

For cases that do not use code generation but need several schemas, use the
generic wrapper pattern (see `rust/examples/fb_schema.rs`):

```rust
pub trait FbSchemaSpec {
    fn schema_bfbs() -> &'static [u8];
    fn fixed_size() -> Option<usize>;
    fn root_type() -> &'static str;
}

pub struct FbSchema<S: FbSchemaSpec>(std::marker::PhantomData<S>);

impl<S: FbSchemaSpec> SchemaMeta for FbSchema<S> {
    fn schema_bfbs() -> &'static [u8] { S::schema_bfbs() }
    fn fixed_size() -> Option<usize> { S::fixed_size() }
    fn root_type() -> Option<&'static str> { Some(S::root_type()) }
}

// HeaderInjector is implemented the same way...
```

Then use it:

```rust
struct SensorFeedbackSpec;
impl FbSchemaSpec for SensorFeedbackSpec {
    fn schema_bfbs() -> &'static [u8] { EXAMPLES_CONTROL_BFBS }
    fn fixed_size() -> Option<usize> { EXAMPLES_CONTROL_FIXED_SIZE }
    fn root_type() -> &'static str { "examples.control.SensorFeedback" }
}

type SensorFeedbackSchema = FbSchema<SensorFeedbackSpec>;

// Usage
let publisher = node.create_publisher::<SensorFeedbackSchema>("/feedback").await?;
```

### 8.7 SchemaAttachPolicy

Controls whether the schema hash is attached to the transport metadata of every
message:

```rust
pub enum SchemaAttachPolicy {
    Inherit,  // inherit the global default policy
    Always,   // attach the schema hash to every message
    Never,    // never attach
}
```

### 8.8 SchemaHash

```rust
pub struct SchemaHash { /* 32 bytes */ }

impl SchemaHash {
    pub fn from_bfbs_bytes(bfbs: &[u8]) -> Self;   // compute SHA-256 from BFBS
    pub fn from_hex_str(hex: &str) -> Option<Self>; // parse from a 64-character hex string
    pub fn to_hex(self) -> String;                   // render as a 64-character hex string (by-value receiver; SchemaHash is Copy)
}
```

---

## 9. Logging

### 9.1 Overview

Aorta provides a unified logging system built on the Rust `tracing` ecosystem:

- 5 log levels: Trace / Debug / Info / Warn / Error
- Text and JSON output formats
- File output plus stderr, on two channels
- Environment-variable configuration
- A macro-style interface with conditional logging and rate limiting

### 9.2 Initialization

```rust
// Initialize from environment variables (recommended; idempotent)
init_from_env_once()?;

// Programmatic configuration
configure_logging(LogConfig {
    filter: "aorta=debug,zenoh=warn".to_string(),  // a plain String, not an Option
    file_path: Some("/tmp/aorta.log".into()),       // Option<PathBuf>, use .into()
    format: LogFormat::Text,
    stderr_enabled: true,
    ..Default::default()                            // topic: LogTopicConfig is required; take the default here
})?;
```

> `LogConfig` fields: `filter: String` (not an `Option`), `format: LogFormat`,
> `file_path: Option<PathBuf>` (convert a string with `.into()`),
> `stderr_enabled: bool`, `topic: LogTopicConfig`. `LogConfig` implements
> `Default`, and the `topic` field not listed explicitly must still be given a
> value (supplied here via `..Default::default()`). See
> `core/src/logging.rs:103-122`.

Environment variables:

| Variable | Meaning | Example |
|------|------|------|
| `AORTA_LOG` | Filter level | `debug`, `info`, `aorta_sdk=trace` |
| `AORTA_LOG_FILE` | Log file path | `/tmp/aorta.log` |
| `AORTA_LOG_FORMAT` | Output format | `text`, `json` |

### 9.3 Setting the Default Log Context

```rust
set_default_context(node.name(), node.group())?;
```

### 9.4 Logging Macros

#### Basic logging

```rust
aorta_info!("Node started: {}", node.name());
aorta_warn!("Publish failed: {}", err);
aorta_error!("Fatal error: {}", msg);
aorta_debug!("Debug info: count={}", count);
aorta_trace!("Trace detail");
```

#### Generic macro (explicit level)

```rust
aorta_log!(LogLevel::Info, "Custom level log: {}", data);
```

#### Conditional logging

```rust
aorta_log_if!(LogLevel::Warn, count > 100, "Count exceeds threshold: {}", count);
```

#### Rate limiting (every N calls)

```rust
aorta_log_every_n!(LogLevel::Info, 100, "Status update (every 100th call)");
```

### 9.5 Level Checks

```rust
if is_log_enabled(LogLevel::Debug, "aorta_sdk") {
    // build the expensive debug string
    aorta_debug!("{}", expensive_debug_string());
}
```

### 9.6 Flush

```rust
flush_logging();  // force a flush of all buffered log output
```

### 9.7 Checking Initialization State

```rust
if !is_logging_initialized() {
    init_from_env_once()?;
}
```

---

## 10. Time: the Unified Time Source

> The clock modes (System / Steady / Simulation) and the nanosecond semantics
> are shared across SDKs; the specification is
> [`spec/Aorta_SDK_Common_Concepts.md` §6](../spec/Aorta_SDK_Common_Concepts.md#6-time--clock-model).
> This section gives the Rust time API bindings.

### 10.1 Overview

Aorta provides a process-wide unified time source with three clock modes:

| Mode | Core type | Meaning |
|------|----------|------|
| `System` (default) | `ClockMode::System` | System wall clock (`CLOCK_REALTIME`) |
| `Steady` | `ClockMode::Steady` | Monotonic clock (`CLOCK_MONOTONIC`), unaffected by system time adjustments |
| `Simulation` | `ClockMode::Simulation` | Simulation clock, advanced externally |

- All time values are nanoseconds
- The SDK-level `TimeSource` enum has only two variants: `System` and
  `Simulation` (`Steady` is defined on the core-level `ClockMode`)
- `System` and `Steady` advance on their own; `Simulation` must be set manually
  through `set_simulation_time_ns()`

### 10.2 Getting Time from a Node

```rust
let time: Time = node.now();
// The Time struct carries nanosecond-resolution time
```

### 10.3 Global Time API

Use these when you do not have a Node instance:

```rust
use aorta_sdk::prelude::*;

// Current time (a Time struct)
let t = now();

// Nanosecond timestamp
let ns = now_ns();

// Unix nanosecond timestamp
let unix_ns = unix_time_ns();
```

### 10.4 Switching Time Sources

```rust
// Inspect the current time source
let source = time_source();  // TimeSource::System or TimeSource::Simulation

// Switch to simulation time
set_time_source(TimeSource::Simulation);

// Set the simulation time
set_simulation_time_ns(1_000_000_000);  // set to 1 second

// Read the simulation time
let sim_ns = simulation_time_ns();
```

### 10.5 Time and AortaHeader

On publish, the publisher automatically calls `node.now()` for a timestamp and
writes it into the AortaHeader `publish_stamp_ns` field. A subscriber can read
the following from `MessageContext`:

- `receive_stamp_ns`: timestamp of the receive instant (requires
  `.with_receive_timestamp(true)`)
- `publisher_stamp_ns`: timestamp injected by the publisher (requires
  `.with_publisher_timestamp(true)`)

---

## 11. Advanced Features

### 11.1 RecyclePool: Zero-Copy Buffer Pool

Internally the publisher uses a bounded LIFO buffer pool (`RecyclePool`) to
reach zero allocation in steady state:

```
publish() → obtain_builder() [take a Vec<u8> from the RecyclePool]
            ↓
         FlatBufferBuilder::from_vec(recycled_vec)
            ↓
         build message + inject header
            ↓
         builder.collapse() → (Vec<u8>, offset)
            ↓
         RecyclableBuffer { vec, offset, pool }
            ↓
         Bytes::from_owner(recyclable_buffer) → ZBytes
            ↓
         publish to Zenoh (zero-copy!)
            ↓
         [after Zenoh is done with it]
            ↓
         RecyclableBuffer::drop() → pool.push(vec)  [returned to the pool]
```

**Performance characteristics:**

- Zero malloc/free in steady state — Vecs are cycled through the RecyclePool
- Zero memcpy — `Bytes::from_owner()` transfers ownership straight to Zenoh
- Bounded and MPMC-safe — a mutex-guarded LIFO stack over a preallocated Vec.
  The critical section only moves a `Vec` (it never allocates), so an
  uncontended lock/unlock costs the same two atomic operations as the CAS loop
  it replaced, and LIFO order keeps the warmest buffer on top. (An earlier
  intrusive lock-free Treiber stack was removed: under the real MPMC usage an
  untagged Treiber pop is ABA-unsound.)

### 11.2 Builder Reuse (Internal)

The Rust SDK still implements the three builder-reuse policies `ThreadLocal` /
`Mutex` / `None`, but they are an SDK-internal implementation detail and are not
part of the current stable `PublisherOptions` configuration surface. The default
path prefers a thread-local builder; once capacity exceeds
`AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB` (default `2048`, i.e. 2 MiB) it switches
automatically to the mutex large-buffer path, so that no thread holds on to a
megabyte-scale buffer indefinitely.

### 11.3 Publisher Oversize-Message Detection

When a message exceeds `max_message_size`, the SDK records statistics and emits
a rate-limited warning log:

```rust
let stats = publisher.stats();
if stats.oversize_count > 0 {
    aorta_warn!(
        "Oversize: {} times, max_seen={} bytes",
        stats.oversize_count,
        stats.max_capacity_seen
    );
}
```

### 11.4 Subscriber Deduplication and Out-of-Order Detection

#### Deduplication (Dedup)

A sliding-window deduplication keyed on `{publisher_id, sequence}`:

```rust
let options = SubscriberOptions::new()
    .with_dedup(true)
    .with_dedup_window(2048);  // window size
```

#### Out-of-order detection (Reorder Check)

```rust
let options = SubscriberOptions::new()
    .with_reorder_check(true)
    .with_drop_out_of_order(true);   // drop out-of-order messages
    // or
    // .with_drop_out_of_order(false) // warn only, do not drop
```

### 11.5 SubscriberOverflowPolicy

The overflow policy applied when the executor cannot keep up with a subscriber's
messages (re-exported from `aorta_core::subscriber::SubscriberOverflowPolicy`):

```rust
pub enum SubscriberOverflowPolicy {
    Drop,   // drop the newest message (default) — counted in SubscriberRuntimeStats::executor_drop_count
    Relay,  // buffer into a relay queue and process later
}
```

```rust
let options = SubscriberOptions::new()
    .with_overflow_policy(SubscriberOverflowPolicy::Relay)
    .with_overflow_relay_capacity(4096);  // relay queue capacity
```

| Policy | Behavior | Use case |
|------|------|---------|
| `Drop` (default) | Drop the newest arriving message, rate-limited warning | Real-time systems that can tolerate dropped frames |
| `Relay` | Buffer into a separate relay queue | Message streams that must not be lost |

> Source reference: `core/src/subscriber.rs` lines 209-219 (the
> `overflow_policy` / `overflow_relay_capacity` fields) and lines 259-263 (the
> `SubscriberOverflowPolicy` enum); the `Drop` default is at line 468.

### 11.6 Late Joiners Get the Last Value (ContextStore)

The QoS & buffering redesign removed `TransientLocal` durability (it was never
wired up — a no-op). "A late-joining subscriber receives the last published
value / gets the current state after reconnecting" is now the job of
[`ContextStore` (§2.5)](#25-node-creation-primitives-at-a-glance), not of QoS.
Publish changing state into the ContextStore and a late joiner reads the latest
snapshot:

```rust
// Writer side: declare the prefix you own, then put state into the
// ContextStore (last-value semantics).
ctx_store.declare_owned_prefix("robot/**").await?;
ctx_store.set_string("robot/status", &payload).await?;

// Reader side (joining at any time): the last written value comes back immediately.
let read = ctx_store.get_string("robot/status").await?;
process(&read.value);
```

> For the concrete API see [ContextStore (§2.5)](#25-node-creation-primitives-at-a-glance)
> / `aorta_sdk::contextstore`. Typed accessors are `set_bool` / `set_i64` /
> `set_u64` / `set_f64` / `set_string` and the matching `get_*`, which return
> `ContextRead<T>` (`value`, `is_fresh`, `stamp_ns`, `source_node`, …); a
> `declare_owned_prefix` must resolve before any `set_*` on a key under that
> prefix. When you need per-topic receive buffering / isolation, use
> `SubscriberOptions::receive_depth` (see
> [§3.10](#310-per-topic-subscription-buffering-receive_depth--pull-subscriptions)) —
> that is orthogonal to last-value semantics.

### 11.7 Schema Hash as RPC Metadata

A client can declare `service_schema` to record the service schema hash it
expects. The hash travels on the wire only when the effective
`schema_attach_policy` resolves to `Always`; independently of attachment,
`validate_schema` enables a best-effort comparison against the provider's
locally registered schema:

```rust
// Compute the hash from BFBS bytes
let schema = SchemaHash::from_bfbs_bytes(MY_SERVICE_BFBS);

let client = node.create_client_with_options(
    "/my_service",
    ClientOptions::new()
        .with_service_schema(schema)
        .with_validate_schema(true),
);
```

This is a **diagnostic** facility, not version negotiation:
`validate_schema` is **warn-only** — a mismatch is logged and the response is
still delivered; no request is rejected and no call fails because of it. Do not
rely on it to guarantee the client is talking to the right service version.

### 11.8 Single-Provider Mode

```rust
let client = node.create_client_with_options(
    "/service",
    ClientOptions::new()
        .with_enforce_single_provider(true)         // allow exactly one provider
        .with_multi_reply_check_window_ms(100),     // multi-reply detection window
);
```

When `enforce_single_provider = true`:
- The request is still sent to **all** matching providers; enforcement is
  **bounded post-hoc multiple-reply detection**, not a routing restriction
- After the first reply arrives, the client listens for the check window; if a
  second provider's reply lands inside it, the call fails with a conflict
- This cannot guarantee exactly-once execution: multiple providers may each
  have executed the request's side effects, and a reply arriving after the
  window escapes detection. For non-idempotent services, guarantee a single
  provider through deployment topology — do not rely on this option for it

### 11.9 Reentrancy Detection

The publisher automatically detects reentrant publishes on the same thread (for
example, publishing from inside a subscription callback) and degrades to a
temporary builder to avoid a data race:

```rust
let stats = publisher.stats();
if stats.reentrant_count > 0 {
    aorta_warn!("Reentrant publishes detected: {}", stats.reentrant_count);
}
```

### 11.10 Tokio Integration Patterns

The Rust SDK is natively built on Tokio; use `tokio::select!` for graceful
shutdown:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Node::new("demo", "default").await?;
    let publisher = node.create_publisher::<MySchema>("/topic").await?;

    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                publisher.publish_typed(|msg| { /* ... */ })?;
            }
        }
    }

    node.close().await?;
    Ok(())
}
```

---

## 12. Worked Examples

### 12.1 Talker (Timer-Driven Publisher)

**File:** `rust/examples/talker.rs`

This example shows the most typical Rust Aorta publisher pattern: `tokio::select!` + `interval` + `publish_typed`:

```rust
use std::time::Duration;

use aorta_sdk::prelude::*;
use demo_chat_msg::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // (1) Create the Node (asynchronous)
    let node = Node::new("talker", "default").await?;
    aorta_info!("sdk talker topic=/demo/chatter");

    // (2) Create a typed Publisher (asynchronous, injects the schema BFBS automatically)
    let publisher = node.create_publisher::<ChatSchema>("/demo/chatter").await?;
    if let Some(hash) = publisher.schema_hash() {
        aorta_info!("schema registered hash={}", hash.to_hex());
    }

    // (3) tokio::select! event loop
    let mut counter: u64 = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                // (4) publish_typed: synchronous fire-and-forget
                publisher.publish_typed(|msg: &mut ChatMsg| {
                    msg.text = Some(format!("hello {}", counter));
                })?;
                counter = counter.wrapping_add(1);
            }
        }
    }

    // (5) Graceful shutdown
    node.close().await?;
    Ok(())
}
```

**Key pattern:**

```
#[tokio::main] → Node::new().await → create_publisher::<T>().await
    → tokio::select! {
          Ctrl+C  → break,
          tick    → publish_typed() [synchronous, no .await]
      }
    → node.close().await
```

### 12.2 Listener (Subscriber)

**File:** `rust/examples/listener.rs`

This example shows the Subscriber + `SubscriberOptions` builder + `with_contiguous_bytes()` pattern:

```rust
let options = SubscriberOptions::new()
    .with_header_context(true)
    .with_topic_in_context(true)
    .with_receive_timestamp(true)
    .with_dedup(true)
    .with_reorder_check(true)
    .with_drop_out_of_order(true);

let _subscriber = node
    .create_subscriber_with_options(
        "/demo/chatter",
        |payload, context| {
            // (1) Read metadata from the context
            aorta_info!(
                "[{}] priority={:?} pub_ts={:?}",
                context.topic,
                context.priority,
                context.publisher_stamp_ns
            );
            // (2) Read the sequence number from the header context
            if let Some(seq) = context.sequence {
                aorta_info!("  seq={} node={}", seq,
                    context.publisher_node.as_deref().unwrap_or(""));
            }
            // (3) Zero-copy payload access via with_contiguous_bytes
            with_contiguous_bytes(payload, |bytes| {
                if let Ok(chat) = demo::root_as_chat(bytes) {
                    let text = chat.text().unwrap_or("");
                    aorta_info!("  text={}", text);
                }
            });
        },
        options,
    )
    .await?;

// (4) Wait for the exit signal (the Node manages its internal executor for you)
tokio::signal::ctrl_c().await?;
node.close().await?;
```

### 12.3 Echo Server/Client (RPC Example)

**Server file:** `rust/examples/echo_server.rs`

```rust
let server_name = node.name().to_string();
let mut service = node
    .create_service_typed_view_with_options::<EchoRequestSchema, EchoResponseSchema, _>(
        "/echo",
        move |request, reply| {
            let echoed_message = format!(
                "echo from {}: {}",
                server_name,
                request.message().unwrap_or_default()
            );
            tokio::spawn(async move {
                match reply.reply(|msg| {
                    msg.echoed_message = Some(echoed_message.as_str());
                }).await {
                    Ok(_) => aorta_info!("Replied typed echo request"),
                    Err(err) => aorta_error!("Reply failed: {}", err),
                }
            });
        },
        ServiceOptions::new(),
    )
    .await?;

// (3) Keep running (the Node manages its internal executor for you)
tokio::signal::ctrl_c().await?;
service.stop();  // synchronous stop
node.close().await?;
```

**Client file:** `rust/examples/echo_client.rs`

```rust
// (1) create_client_typed_with_options is synchronous (no .await!)
//     echo_client.rs uses the with-options variant; use create_client_typed(...)
//     when you need no options.
let client = node.create_client_typed_with_options::<EchoRequestSchema, EchoResponseSchema>(
    "/echo",
    ClientOptions::new(),
)?;

// (2) call() builds the typed request; decode() returns the typed response
match client.call(|msg| {
    msg.message = Some("hello");
}).await {
    Ok(response) => {
        let decoded = response.decode()?;
        aorta_info!("Echo: {}", decoded.echoed_message().unwrap_or_default());
    }
    Err(err) => aorta_error!("Call failed: {}", err),
}
```

### 12.4 Sensor Publisher/Subscriber (Multi-QoS Example)

**Files:** `rust/examples/sensor_publisher.rs` + `rust/examples/sensor_subscriber.rs`

This example creates several publishers/subscribers on one node, each with a different QoS profile:

```rust
// === Publisher side ===
let node = Node::new("sensor_publisher", "robot").await?;

// IMU: Realtime QoS (100Hz)
let mut imu_opts = PublisherOptions::default();
imu_opts.qos = QoS::realtime_control();
let imu_pub = node
    .create_publisher_with_options::<ImuDataSchema>("/sensors/imu", imu_opts)
    .await?;

// Camera: Normal QoS (30Hz)
let mut cam_opts = PublisherOptions::default();
cam_opts.qos = QoS::default().with_reliability(Reliability::Reliable);
let cam_pub = node
    .create_publisher_with_options::<CameraFrameSchema>("/sensors/camera", cam_opts)
    .await?;

// Health: reliable + Block (critical state must not be dropped silently) (1Hz)
// Want late joiners to get the last health sample? Publish the state to the
// ContextStore (see §11.6) instead of relying on QoS.
let mut health_opts = PublisherOptions::default();
health_opts.qos = QoS::default()
    .with_reliability(Reliability::Reliable)
    .with_congestion_control(CongestionControl::Block);
let health_pub = node
    .create_publisher_with_options::<HealthStatusSchema>("/robot/health", health_opts)
    .await?;

// Timer-driven publishing (call publish directly in the timer callback — synchronous!)
let _imu_timer = node.create_timer(Duration::from_millis(10), move || {
    let _ = imu_pub.publish(|builder, header| { /* build IMU data */ });
}).await?;

let _cam_timer = node.create_timer(Duration::from_millis(33), move || {
    let _ = cam_pub.publish(|builder, header| { /* build Camera frame */ });
}).await?;

// === Subscriber side ===
let node = Node::new("sensor_subscriber", "robot").await?;

// IMU Subscriber
let imu_opts = SubscriberOptions::new()
    .with_qos(QoS::realtime_control())
    .with_history_depth(5);
let _imu_sub = node.create_subscriber_with_options(
    "/sensors/imu", imu_callback, imu_opts,
).await?;

// Health Subscriber (reliable; for a late-joiner snapshot use the ContextStore, see §11.6)
let health_opts = SubscriberOptions::new()
    .with_qos(QoS::default()
        .with_reliability(Reliability::Reliable))
    .with_history_depth(1);  // local executor queue depth (unrelated to QoS)
let _health_sub = node.create_subscriber_with_options(
    "/robot/health", health_callback, health_opts,
).await?;
```

### 12.5 Control Loop (Multi-Node System)

**File:** `rust/examples/control_loop.rs`

This example shows a complete 4-node robot control system running concurrently with `tokio::spawn`:

```rust
#[tokio::main]
async fn main() -> AortaResult<()> {
    let running = Arc::new(AtomicBool::new(true));

    // Start 4 nodes concurrently
    let h1 = tokio::spawn(run_sensor_node(Arc::clone(&running)));      // 100Hz
    let h2 = tokio::spawn(run_controller_node(Arc::clone(&running)));  // 100Hz PID
    let h3 = tokio::spawn(run_state_estimator_node(Arc::clone(&running))); // 50Hz
    let h4 = tokio::spawn(run_monitor_node(Arc::clone(&running)));     // 1Hz

    tokio::time::sleep(Duration::from_secs(30)).await;

    // Global stop
    running.store(false, Ordering::Relaxed);
    let _ = tokio::join!(h1, h2, h3, h4);
    Ok(())
}
```

**Data flow:**

```
[Sensor Node 100Hz] → /control/feedback → [Controller Node 100Hz]
                                        → [State Estimator 50Hz]
[Controller Node]   → /control/command  → [Monitor Node 1Hz]
[State Estimator]   → /control/state    → [Monitor Node 1Hz]
```

Every node follows the same internal pattern:
1. `Node::new().await`
2. `create_publisher_with_options::<Schema>(topic, opts).await`
3. `create_subscriber_with_options(topic, callback, opts).await`
4. `create_timer(period, callback).await` — timer-driven publish
5. `node.close().await`

### 12.6 Navigation Service (Full Typed RPC Example)

**Server file:** `rust/examples/navigation_service_server.rs`

Registering several typed services on one node:

```rust
let node = Node::new("navigation_server", "default").await?;

// Three services registered on the same node
let mut svc1 = node
    .create_service_typed_with_options::<GetPlanRequestSchema, GetPlanResponseSchema, _>(
        "/get_plan", get_plan_handler, ServiceOptions::new(),
    ).await?;

let mut svc2 = node
    .create_service_typed_with_options::<CancelPlanRequestSchema, CancelPlanResponseSchema, _>(
        "/cancel_plan", cancel_handler, ServiceOptions::new(),
    ).await?;

let mut svc3 = node
    .create_service_typed_with_options::<GetCurrentPoseRequestSchema, GetCurrentPoseResponseSchema, _>(
        "/get_current_pose", pose_handler, ServiceOptions::new(),
    ).await?;

tokio::signal::ctrl_c().await?;
svc1.stop();
svc2.stop();
svc3.stop();
node.close().await?;
```

**Client file:** `rust/examples/navigation_service_client.rs`

```rust
let schema = SchemaHash::from_bfbs_bytes(NAVIGATION_BFBS);
let client = node.create_client_with_options(
    "/get_plan",
    ClientOptions::new()
        .with_timeout_ms(5000)
        .with_service_schema(schema)
        .with_service_root_types(
            "aorta.services.navigation.GetPlanRequest",
            "aorta.services.navigation.GetPlanResponse",
        )
        .with_validate_schema(true),
);

let request = build_flatbuffer_request(/* ... */);
let response = client.call(request).await?;
```

---

## 13. Best Practices

### 13.1 Tokio Runtime Management

- **Always use `#[tokio::main]`** as the entry point; the Rust SDK requires a Tokio runtime
- **`close().await` on the Node** must be called before shutdown so the internal executor stops and resources are released
- **Do not `.await` inside a timer callback**: timer callbacks are synchronous `Fn()` and do not support async operations; use `tokio::spawn()` inside the callback to start an async task

### 13.2 Lifetimes and Ownership

- **Create the Node first, close it last**: publishers / subscribers / timers / services must not outlive the Node
- **A typed `Publisher<T>` is not `Clone`**: `move` it into the single task/closure that publishes, or wrap it in an `Arc<Publisher<T>>` yourself to share it across tasks
- **`Service::stop()` needs `&mut self`**: make sure no other reference is outstanding before stopping
- **`ServiceRequest::reply()` consumes `self`**: each request can be replied to exactly once, enforced at compile time

### 13.3 Performance Tuning

- **Set `max_message_size`**: set the expected message size in `PublisherOptions` to avoid repeated buffer growth
- **Watch the capacity threshold on large-message publishers**: MiB-class publishers depend on `max_message_size` and `AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB` for their steady-state memory footprint
- **Monitor `coalesced_tick_count()`**: a non-zero value means the executor is overloaded; consider more threads or a lower timer frequency
- **Enable admission control**: in production, set `AORTA_EXECUTOR_MAX_QUEUED_EVENTS` / `MAX_QUEUED_BYTES`

### 13.4 Error Handling

- **Propagate `AortaResult` with `?`**: every error in the Rust SDK is unified into `AortaResult<T>`
- **Common `AortaError` variants**: `InvalidArgument` / `NotFound` / `Busy` / `Timeout` / `Internal` / `Transport` / `SchemaConflict` / `ConcurrentConflict` / `AlreadyReplied` / `ShmUnavailable` / `CapacityExceeded` / `QuotaExceeded` / `NotSupported`
- **Design RPCs to be idempotent**: for non-idempotent services, enable `enforce_single_provider` + `multi_reply_check_window_ms`

### 13.5 Schema and Type Safety

- **Prefer the typed API**: `create_publisher::<M>()` / `create_service_typed()` inject the schema automatically
- **Schema conflicts**: in production, set `AORTA_SCHEMA_CONFLICT_ACTION=reject`
- **Implementing `SchemaMeta` by hand**: when you are not using codegen, see `rust/examples/sensor_publisher.rs`

---

## 14. Actions: Long-Running Tasks

### 14.1 Overview

The Rust SDK ships a typed Action API for long-running, cancellable tasks with feedback.

Core types:

- `ActionServer<TAction>`
- `ActionClient<TAction>`
- `GoalHandle<TAction>`
- `ActionExecutionContext<TAction>`

Current semantics and implementation state:

- Only one active server is allowed per `{group, action}`
- One server can serve several clients at once
- Whether multiple goals may execute in parallel is controlled by `ActionServerOptions::max_concurrency`
- Beyond `max_concurrency`, a new goal is rejected with `Busy`; there is no built-in queue today
- Dedup / result retention live in memory only and are not restored after a server restart

### 14.2 Creating an ActionServer

The Rust server-side entry point (`MyAction` stands for your own
codegen-generated action schema — here with a goal field `target`, a feedback
field `progress_pct`, and a result field `duration_ms`). Naming follows the
generator's rule: for tables `MyGoal` / `MyResult` / `MyFeedbackData`, the
marker type is `MyAction` (the `*Goal` prefix + `Action`) and the message
structs are `MyGoalMsg` / `MyResultMsg` / `MyFeedbackDataMsg`:

```rust
use aorta_sdk::prelude::*;
use my_action_msg::prelude::*;  // your generated action wrapper crate

let node = Node::new("my_action_server", "default").await?;
let server = ActionServer::<MyAction>::serve(
    &node,
    MyAction::ACTION_NAME,
    ActionServerOptions::default(),
    |goal, ctx: ActionExecutionContext<MyAction>| async move {
        let goal_view = goal.decode()?;
        let target = goal_view.target().unwrap_or_default();

        let feedback = MyFeedbackDataMsg {
            progress_pct: Some(25),
        };
        ctx.publish_feedback(&feedback).await?;

        // Cooperative cancellation: poll for / await the cancel request inside the long-running loop
        if ctx.is_cancel_requested() {
            return ctx.cancel(ActionReplyCode::Canceled, "canceled by client").await;
        }

        let result = MyResultMsg {
            duration_ms: Some(120),
        };
        ctx.succeed(&result).await
    },
).await?;
```

> **action_name is a required positional argument**: like topics and services, `serve`'s
> second positional argument `action_name: &str` is required (the signature is
> `serve(&node, action_name, options, handler)`, see `rust/src/action.rs:85-97`).
> By convention, pass the schema-generated `TAction::ACTION_NAME` constant.

Notes:

- `goal.decode()` decodes the typed goal view
- `ctx.publish_feedback(...)` may be called any number of times
- The handler must end in a terminal state via one of `ctx.succeed(...)` / `ctx.abort(...)` / `ctx.cancel(...)`
- The most important `ActionServerOptions` fields are:
  - `max_concurrency`
  - `result_ttl`
  - `dedup_ttl`

#### The ActionExecutionContext method surface / handler context surface

The `ActionExecutionContext<TAction>` handed to the handler exposes the following (see
`core/src/action_server.rs:139-300`):

| Method | Signature | Description |
|---|---|---|
| `goal_id()` | `fn goal_id(&self) -> &str` | The current goal's ID |
| `publish_feedback(data)` | `async fn publish_feedback(&self, data: &TAction::FeedbackDataMsg<'_>) -> AortaResult<()>` | Push one typed feedback sample; may be called repeatedly |
| `publish_feedback_with_message(data, msg)` | `async fn publish_feedback_with_message(&self, data, message: Option<&str>) -> AortaResult<()>` | Feedback with a text message |
| `is_cancel_requested()` | `fn is_cancel_requested(&self) -> bool` | Non-blocking read of the cancel flag (the cooperative-cancellation poll point) |
| `cancelled()` | `async fn cancelled(&self)` | Await until the client requests cancellation (usable in `select!`) |
| `succeed(result)` | `async fn succeed(&self, result: &TAction::ResultMsg<'_>) -> AortaResult<()>` | Terminal success; takes a `&ResultMsg` reference |
| `abort(code, message)` | `async fn abort(&self, code: ActionReplyCode, message: &str) -> AortaResult<()>` | Terminal failure; both `code` and the text message are required |
| `cancel(code, message)` | `async fn cancel(&self, code: ActionReplyCode, message: &str) -> AortaResult<()>` | Terminal cancellation; both `code` and the text message are required |

> **Cooperative cancellation**: cancellation is cooperative — the server never force-aborts the
> handler. A long-running task should either poll
> `if ctx.is_cancel_requested() { return ctx.cancel(...).await; }` inside its loop, or await
> `ctx.cancelled()` in a `tokio::select!`, then finish with `cancel(code, message)`.

#### ActionServer lifecycle and introspection / lifecycle & introspection

Besides `serve`, `ActionServer<TAction>` provides the following entry points (see
`rust/src/action.rs:41-151`):

| Method | Signature | Description |
|---|---|---|
| `serve_info_only(node, action_name, options)` | `async fn -> AortaResult<Self>` | Declare the action info / liveliness only, without binding an execution handler (probe/placeholder) |
| `query_info(node, action_name)` | `async fn -> AortaResult<Option<ActionInfo>>` | Static query: whether an active server exists, returning its `ActionInfo` |
| `query_info_with_timeout(node, action_name, timeout)` | `async fn -> AortaResult<Option<ActionInfo>>` | Same, with a custom timeout (default in `DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS`) |
| `action_name()` | `fn -> &str` | The action name this server is bound to |
| `info()` | `fn -> &ActionInfo` | This server's `ActionInfo` |
| `stop(&mut self)` | `fn` | Request that the background task stop (non-blocking) |
| `stop_and_join(self, timeout)` | `async fn` | Stop and wait for the background task to exit, up to `timeout` |

A full runnable example:

- `rust/examples/cli_execute_task_server.rs`

### 14.3 Creating an ActionClient

On the Rust side the client is not created through a `Node` factory; you construct it directly:

```rust
use aorta_sdk::prelude::*;
use my_action_msg::prelude::*;

let node = Node::new("my_action_client", "default").await?;
let client = ActionClient::<MyAction>::new(
    &node,
    MyAction::ACTION_NAME,
    ActionClientOptions::default(),
);

let goal = MyGoalMsg {
    target: Some("dock_a".to_string()),
};

let handle = client.send_goal(&goal).await?;
```

> **action_name is a required positional argument**: `new`'s signature is
> `new(&node, action_name: &str, options)` (see `rust/src/action.rs:185`). By convention,
> pass the schema-generated `TAction::ACTION_NAME` constant.

Once the goal is sent, the `GoalHandle` carries:

- `goal_id()`
- `goal_secret()`
- `server_epoch()`

Commonly used capabilities:

```rust
let status = handle.query_status().await?;
let mut stream = handle.feedback_stream().await?;
let result = handle.wait_result(Some(std::time::Duration::from_secs(10))).await?;
```

#### ActionClient client-level / out-of-band method surface / client-level & out-of-band API

Beyond the `send_goal` + `GoalHandle` path, `ActionClient<TAction>` also offers a set of
out-of-band methods keyed by `goal_id` / `goal_secret` (see `rust/src/action.rs:178-335`). They
complement — rather than replace — the `GoalHandle` path, and suit cross-process/recovery
scenarios where you no longer hold the original `GoalHandle`:

| Method | Signature | Description |
|---|---|---|
| `new_for(node, namespace, action_name, options)` | `fn -> AortaResult<Self>` | Cross-namespace addressing: `namespace` identifies the target device (a per-device identifier), and every key is built as `{namespace}/aorta/{group}/act/...`; a wildcard namespace is rejected (an action must target a single device) |
| `action_name()` | `fn -> &str` | The action name this client is bound to |
| `send_goal_with_id(goal_id, goal)` | `async fn -> AortaResult<GoalHandle<TAction>>` | Send a goal with a caller-supplied `goal_id` (idempotent/replayable) |
| `info()` | `async fn -> AortaResult<Option<ActionInfo>>` | Query whether an active server exists and return its `ActionInfo` |
| `query_status(goal_id)` | `async fn -> AortaResult<Option<ActionPublicStatus>>` | Query the public status by `goal_id` |
| `cancel(goal_id, goal_secret)` | `async fn -> AortaResult<ActionCancelGoalResponse>` | Request cancellation by `goal_id` + `goal_secret` |
| `get_result(goal_id, goal_secret)` | `async fn -> AortaResult<ActionGetResultResponse<..>>` | Fetch the final result by `goal_id` + `goal_secret` |
| `generate_goal_id()` | `fn -> String` | Generate a canonical goal_id accepted by `send_goal_with_id` (hyphenated UUID v4, identical to what `send_goal` does internally; aligned with C++ `GenerateGoalId`) |
| `feedback_stream_for(goal_id)` | `async fn -> AortaResult<ActionFeedbackStream<TAction>>` | Open a goal-level feedback stream for a pre-generated `goal_id` **before sending** (paired with `send_goal_with_id`), eliminating the early-sample loss window of subscribing after the send; it closes itself once this client observes the goal's terminal state (see below) |
| `feedback_stream_all()` | `async fn -> AortaResult<ActionFeedbackStream<TAction>>` | Subscribe to feedback from **all** goals on the action (not restricted to a single goal); **never** closes itself |

#### Goal-level feedback stream semantics / per-goal feedback stream semantics

A goal-level stream (`GoalHandle::feedback_stream()` or `ActionClient::feedback_stream_for(goal_id)`)
has two semantics of its own:

1. **Subscribe before sending (zero early-feedback loss)**: topic subscriptions do not replay
   history, so calling `feedback_stream()` after `send_goal` returns leaves a race window — if the
   server publishes as the first step of its handler (an application-level `ACCEPTED` frame, say),
   that sample is lost. Use this instead:

   ```rust
   let goal_id = ActionClient::<MyAction>::generate_goal_id();
   let mut stream = client.feedback_stream_for(&goal_id).await?;   // subscribe first
   let handle = client.send_goal_with_id(&goal_id, &goal).await?;  // then send
   ```

2. **Automatic close on terminal state (end-of-stream)**: as soon as this client observes the
   goal's terminal state through `get_result` / `wait_result` (or `NotFound` / `GoalExpired`), the
   goal-level stream closes itself — `recv()` first drains the already-buffered samples and then
   returns `None`, so a `while let Some(..)` loop terminates naturally and the consumer needs no
   timer or grace period. Note that this is **triggered by local observation** (not by an
   on-the-wire termination frame): the terminal response and feedback travel different paths, and
   under extreme timing the last few in-flight frames are not guaranteed to arrive — the
   authoritative terminal data (including the last feedback snapshot) is the return value of
   `get_result` / `wait_result`. Opening a goal-level stream **after** the terminal state has
   already been observed yields a born-closed stream (`recv()` returns `None` immediately and never
   hangs forever); that memory is bounded (the 128 most recent terminal goals — a race-window
   guard, not a permanent archive). `feedback_stream_all()` is unaffected and stays open.

To restore a persisted goal handle:

```rust
let restored = GoalHandle::<MyAction>::from_persisted(
    &client,
    goal_id,
    goal_secret,
)?;
```

### 14.4 End-to-End Example

The repository ships a pair of runnable Rust Action examples:

- `rust/examples/cli_execute_task_server.rs`
- `rust/examples/cli_execute_task_client.rs`

Recommended local verification order:

```bash
bazel run //rust/examples:cli_execute_task_server
```

In another terminal:

```bash
bazel run //rust/examples:cli_execute_task_client -- --source quick
```

Cancellation path:

```bash
bazel run //rust/examples:cli_execute_task_client -- \
  --source cancel \
  --cancel-after-ms 600
```

---

## Appendix A: Key Differences from the C++ SDK

| Feature | C++ SDK | Rust SDK |
|------|---------|---------|
| Runtime | None (Zenoh threads inside Core) | **Tokio** async runtime |
| Node creation | `Node::Create()`, synchronous | `Node::new().await`, **asynchronous** |
| Publisher creation | `CreatePublisher<T>()`, synchronous | `create_publisher::<M>().await`, **asynchronous** |
| Publish | `Publish()` / `PublishTyped()`, synchronous | `publish()` / `publish_typed()`, **synchronous** (no .await) |
| Driving subscribers / services / timers | An explicit executor may be attached; otherwise the Node creates an internal fallback executor | An explicit executor may be attached; otherwise the Node creates an internal fallback executor |
| Client creation | `CreateClient()`, synchronous | `create_client()`, **synchronous** (no .await) |
| Client call | `Call()`, **synchronous blocking** | `call().await`, **asynchronous** |
| Service reply | `req.Reply()`, synchronous | `request.reply().await`, **asynchronous** |
| Callback signature | `(MessageView&, MessageContext&)` | `(&ZBytes, &MessageContext)` or `(ServiceRequest)` |
| Data access | `msg.data` / `msg.ToVector()` | `with_contiguous_bytes()` |
| Error type | `StatusOr<T>` / `Status` | `AortaResult<T>` = `Result<T, AortaError>` |
| Smart pointers | `shared_ptr<T>` | `Arc<T>` (inside Node), value semantics (Publisher etc.) |
| Zero-copy | `PublishOwned()` with a release callback | `RecyclePool` + `Bytes::from_owner()`, **automatic** |
| Builder reuse | Staged allocator + `RecyclePool` under a shared mutex; the pool is capped at 4x `base_capacity` and oversized buffers are dropped | Thread-local by default; large buffers switch to the mutex path automatically |

## Appendix B: Common Patterns Cheat Sheet

### B.1 Tokio main Entry Point

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Node::new("demo", "default").await?;
    // ...
    node.close().await?;
    Ok(())
}
```

### B.2 Graceful Ctrl+C Shutdown

```rust
let shutdown = tokio::signal::ctrl_c();
tokio::pin!(shutdown);
loop {
    tokio::select! {
        _ = &mut shutdown => break,
        _ = ticker.tick() => { /* work */ }
    }
}
```

### B.3 Replying Asynchronously from a Service Handler

```rust
|request: ServiceRequest| {
    let response = request.payload().clone();
    tokio::spawn(async move {
        let _ = request.reply(response).await;
    });
}
```

### B.4 Shared-State Pattern

```rust
let counter = Arc::new(AtomicU64::new(0));
let counter_clone = Arc::clone(&counter);

let _timer = node.create_timer(period, move || {
    let count = counter_clone.fetch_add(1, Ordering::Relaxed);
    // ...
}).await?;
```

### B.5 Multiple Publishers + Timer Pattern

```rust
let pub1 = node.create_publisher::<Schema1>("/topic1").await?;
let pub2 = node.create_publisher::<Schema2>("/topic2").await?;

let _timer1 = node.create_timer(Duration::from_millis(10), move || {
    let _ = pub1.publish(|b, h| { /* ... */ });
}).await?;

let _timer2 = node.create_timer(Duration::from_millis(100), move || {
    let _ = pub2.publish(|b, h| { /* ... */ });
}).await?;
```

## Appendix C: Example File Index

37 `*.rs` files under `rust/examples/` (including 3 scheduling-trace demos, 3 Flow demos/TCK harnesses and 2 ContextStore fixtures):

| Example | File | Core feature |
|------|------|---------|
| Basic publisher | `talker.rs` | `publish_typed`, tokio::select!, Ctrl+C |
| Basic subscriber | `listener.rs` | `SubscriberOptions` builder, `with_contiguous_bytes` |
| Echo server | `echo_server.rs` | `create_service_typed_view`, typed reply |
| Echo client | `echo_client.rs` | `create_client_typed_with_options`, typed decode |
| Sensor publisher | `sensor_publisher.rs` | Multiple publishers + timers + different QoS |
| Sensor subscriber | `sensor_subscriber.rs` | Multiple subscribers + different QoS |
| Timer demo | `timer_demo.rs` | pause/resume, `TimerOptions`, overflow |
| Timer + publish | `simple_timer_publish.rs` | Synchronous publish from a timer callback |
| Late joiner | `late_joiner.rs` | Late joiners getting the last value (now provided by the ContextStore, see §11.6) |
| Control loop | `control_loop.rs` | 4-node system, `tokio::spawn` concurrency |
| Navigation server | `navigation_service_server.rs` | Typed service, multiple service registrations |
| Navigation client | `navigation_service_client.rs` | Schema validation, root type specification |
| All-types publisher | `all_types_talker.rs` | Complex FlatBuffers types |
| Unified publish | `unified_publish_demo.rs` | Raw publisher demo |
| Performance comparison | `performance_demo.rs` | QoS latency tracing |
| Schema attach | `schema_attach.rs` | SchemaAttachPolicy demo |
| FlatBuffer schema | `fb_schema.rs` | BFBS embedding and SchemaMeta implementation |
| CLI publisher | `cli_talker.rs` | Command-line-driven publishing |
| CLI echo server | `cli_echo_server.rs` | Command-line-driven RPC service |
| CLI action server | `cli_execute_task_server.rs` | Typed action server, feedback/cancel |
| CLI action client | `cli_execute_task_client.rs` | Typed action client, wait/cancel/result |
| CLI arguments | `cli_args.rs` | Shared command-line argument parsing |
| Foxglove video | `foxglove_compressed_video_publisher.rs` | Foxglove format integration |
| Performance arguments | `perf_args.rs` | Performance-test argument parsing |
| Performance publisher | `perf_talker.rs` | High-rate publish benchmark |
| Performance subscriber | `perf_listener.rs` | High-rate subscribe benchmark |
| Zenoh baseline publisher | `zenoh_perf_talker.rs` | Raw Zenoh API comparison baseline |
| Zenoh baseline subscriber | `zenoh_perf_listener.rs` | Raw Zenoh API comparison baseline |
| Zenoh common | `zenoh_perf_common.rs` | Shared module for the Zenoh performance tests |
| Scheduling contention demo | `sched_contention_demo.rs` | Scheduling-contention tracing demo |
| Scheduling inversion demo | `sched_inversion_demo.rs` | Priority-inversion tracing demo |
| Cross-process scheduling demo | `sched_xproc_demo.rs` | Cross-process scheduling tracing demo |
| Flow demo | `flow_demo.rs` | Scope/spawn/`when_all`/`offload`, cancellation and budgets, `watch`, `WindowSync` |
| Flow pull demo | `flow_recv_demo.rs` | `recv_in` returning `Err(Stopped)` on scope cancel |
| Flow TCK emitter | `flow_tck_emitter.rs` | NDJSON event emitter for the Flow TCK (`test/flow_tck/`) |
| ContextStore owner fixture | `cli_ctx_owner.rs` | ContextStore test fixture (not a user-facing demo) |
| ContextStore aggregator mirror | `ctx_aggregator_mirror.rs` | The two sanctioned watch-callback variants plus the mandatory heal pattern |

## Appendix D: Common Environment Variables

Environment-variable configuration is shared by all SDKs (Rust / C++ / Python) and is collected in
a single document with the complete variable list, defaults and consumption layers (treat it as
authoritative):

→ **[`spec/Aorta_Environment_Variables_Reference.md`](../spec/Aorta_Environment_Variables_Reference.md)**

The ones Rust users reach for most (defaults and semantics per the spec above; not duplicated inline here):
`AORTA_LOG` / `AORTA_LOG_FORMAT` / `AORTA_LOG_FILE` (logging),
`ZENOH_SESSION_CONFIG_URI` (Zenoh session config, takes precedence over `ZENOH_CONFIG`),
`AORTA_GROUP` (node group),
`AORTA_SCHEMA_ATTACH` / `AORTA_SCHEMA_CONFLICT_ACTION` (schema),
`AORTA_RPC_ENFORCE_SINGLE_PROVIDER` (RPC),
`AORTA_EXECUTOR_MAX_QUEUED_EVENTS` / `AORTA_EXECUTOR_MAX_QUEUED_BYTES` (executor admission),
`AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB` (Rust SDK publisher TLS upgrade threshold, see §11.2).

---

## 15. Aorta Flow: Structured Concurrency

> **New here? Read this first**: [`spec/Aorta_Flow_Concepts.md`](../spec/Aorta_Flow_Concepts.md)
> takes about 15 minutes to go from "what is a coroutine" to "why work has to form a
> tree". This chapter assumes you have read it and only covers how to write it in Rust;
> the rules stated in this chapter are the contract for the Rust SDK.
> A runnable full example: `cargo run -p aorta_sdk --features examples --example flow_demo`.

Flow is **incrementally adoptable**: the callback API stays exactly as it is, and you
migrate one function at a time. All the types live in `aorta_sdk::prelude::*`.

### 15.1 Six-Line Introduction

```rust
use aorta_sdk::prelude::*;

// A cancellable worker: the whole pattern is these six lines — no manual clone,
// no polling, no orphans.
let scope = node.root_scope().child(ScopeOpts::named("worker"));
let handle = scope.spawn_with(|scope| async move {
    while let Some(msg) = scope.run_until_cancelled(rx.recv()).await {
        process(msg).await;
    }
});
// ... and when you need to stop it (cancel + wait for everything to exit, one call):
scope.shutdown(CancelReason::Shutdown).await;
```

Key points:
- `spawn_with` clones the scope's ownership **into the task body**, so it is right
  there inside the loop;
- `run_until_cancelled(fut)` races the "slow wait" against "being cancelled", returns
  `None` on cancellation, and the loop exits naturally — this is the
  **recommended loop shape**;
- `shutdown(reason)` = `cancel(reason)` + `close().await`, and it is part of the normal
  path, not something reserved for error handling.

### 15.2 Scope: Creation and Lifetime

```rust
// Roots: tests and short-lived tools use a standalone root; long-lived scopes should
// hang under the node root (cascade-cancelled when the node closes).
let root  = node.root_scope();                       // the Node's Flow root
let s     = Scope::new_root(ScopeOpts::named("t"));  // standalone root (for tests)

// Child scopes: chained builder construction (ScopeOpts is #[non_exhaustive], so do not
// use a full-field literal)
let req = root.child(
    ScopeOpts::named("nav-request")
        .budget(Duration::from_millis(500)) // budget: only ever narrows, never widens
        .priority(EventPriority::High)      // omitted = inherit the parent; an explicit value is clamped, never raised
        // .supervised()                    // supervised: a failure hurts only itself, siblings are not dragged down
);
```

The three lifecycle verbs (**memorize this table**):

| Verb | Semantics | Waits? |
|---|---|---|
| `cancel(reason)` | Ask all descendants to stop | No, returns immediately |
| `close().await` | Refuse new tasks, **wait** for all children to finish | Yes |
| `shutdown(reason).await` | cancel + close | Yes |

⚠️ **Dropping the last `Scope` handle ≠ cancelling/waiting**. The child tasks become
orphans and keep running (a WARN naming the scope is logged). Ending a scope is always
an explicit `shutdown`/`close`.

⚠️ `spawn`/`close`/`offload` require an **ambient tokio runtime** (they panic otherwise,
exactly like `tokio::spawn`); a foreign thread must either `Handle::enter` or go through
the C-ABI.

### 15.3 Cancellation and Budgets: Four Observation Points

Cancellation is **cooperative**: there is no preemption, and a task observes it at points
of its own choosing and exits gracefully (destructors always run). The four observation
points:

```rust
// 1) In a hot loop: one atomic read, RT-safe (usable in a 1 kHz control loop)
if scope.is_cancelled() { return; }

// 2) At step boundaries: checkpoint observes both "cancelled" and "budget expired"
scope.checkpoint()?;   // on Err(Stopped), `?` exits right here (the reason is inside Stopped)

// 3) Awaiting cancellation itself (often paired with select!/racing; Cancelled is a standard Future)
scope.cancelled().await;

// 4) The recommended loop shape: wrap every slow await in run_until_cancelled
while let Some(x) = scope.run_until_cancelled(rx.recv()).await { /* ... */ }
```

A budget is a deadline attached to a scope that **only narrows as you go down the tree**:

```rust
let req = root.child(ScopeOpts::named("req").budget(Duration::from_millis(500)));
let sub = req.child(ScopeOpts::named("sub").budget(Duration::from_secs(2)));
// sub actually gets min(parent remaining, 2s) — "the whole request is 500ms" automatically
// constrains every sub-step.
if let Some(left) = req.remaining() {
    if left < Duration::from_millis(50) { /* switch to a cheaper algorithm branch */ }
}
```

Expiry is not an interrupt: the next `checkpoint()` (and `call_in`, which checkpoints at
its entry) fails with `DeadlineExceeded`. Note the exact semantics of `spawn`: spawning on
an already-expired scope does **observe** the expiry (the scope enters the cancelled
state), but the task is still created — only a scope that is **closing** rejects `spawn`
outright; a new task stops itself at its own first `checkpoint()`. There is no safety net
that stops code which never checkpoints.

### 15.4 Tasks and Handles

```rust
let h: TaskHandle<i32> = scope.spawn(async { 42 });

// TaskHandle is itself a Future (the tokio JoinHandle convention):
let v = h.await?;                 // == h.join().await?
// When the task body itself returns a Result, join_flat flattens the nesting:
let h = scope.spawn_with(|s| async move { s.checkpoint()?; Ok::<_, Stopped>(1) });
let v = h.join_flat().await?;
```

- `TaskHandle` is marked `#[must_use]`: **dropping the handle does not stop the task**
  (the scope is what owns the task's lifetime), but it does **swallow the result** —
  including a supervised child task's panic. If you really want fire-and-forget, write
  `let _detached = scope.spawn(...)` and let `close()` be the join point, so the intent
  is spelled out.
- Combinators: `when_all(vec![h1, h2]).await` (all complete, fail-fast) and
  `when_any(vec![h1, h2]).await` (first one wins, the losers are **aborted** — a hard
  stop at their next await point, not cooperative cancellation; give a loser its own
  child scope if it needs to clean up gracefully).
  ⚠️ "Winning" in `when_any` means **first to complete**: if the task body returns a
  `Result`, an `Err` that completes first still wins — racing a primary against a backup
  RPC can hand you the primary's error and miss the backup that would have succeeded
  (a `when_any_ok` variant is planned).
- The type tail of a `checkpoint()?` loop: `?` makes the async block return a `Result`,
  so after the `loop` you need an unreachable typed tail, otherwise E0277 reports
  something misleading:

  ```rust
  let h = scope.spawn_with(|s| async move {
      loop {
          s.checkpoint()?;           // exits here on Err(Cancelled)
          step().await;
      }
      #[allow(unreachable_code)]
      Ok::<(), Stopped>(())       // the tail the compiler needs
  });
  ```
- Failure semantics: fail-fast by default (one panic → all siblings cancelled with
  `ChildFailed`); a `supervised()` child scope's failure hurts only itself, and the error
  is still visible on the handle. This is a **typed, cross-plane** `Supervision` policy
  (Rust `Supervision` and C++ `flow::Supervision` are the same enum, passed across the
  C-ABI as validated bytes — unknown values are always rejected): `FailFast` (the
  default) and `Isolate` (what `.supervised()` means) exist on both planes. The third
  policy, **`Restart`, is Rust-plane-only in this release** — an explicitly documented
  divergence (D2); C++ has no equivalent spelling, and the C-ABI has no wire
  representation for a `Restart` argument either:

  ```rust
  let s = root.child(ScopeOpts::named("probe")
      .supervision(Supervision::Restart {
          max_restarts: 3,
          window: Duration::from_secs(10),
          backoff: Duration::from_millis(200),
      }));
  // Every fault reconstructs the task body from the factory closure; exceeding
  // max_restarts within the window escalates to FailFast (cancelling the whole scope).
  let h = s.spawn_supervised(|| async { flaky_probe().await });
  ```

  Under a non-`Restart` policy, `spawn_supervised` degrades to a plain `spawn` (the
  factory runs only once).

### 15.5 RPC: `call_in`

```rust
let client = node.create_client("plan_path");
let reply = client.call_in(&req_scope, request_bytes).await?;
```

- Suspends instead of blocking a thread; inherits the scope budget (min'd against the
  client's own timeout);
- The scope being cancelled mid-flight → returns `Cancelled` immediately; an
  already-dead scope → rejected right at the entry;
- The honest boundary: cancellation stops **your wait**; whether the remote service
  stops depends on the protocol (actions have a cancel channel, plain RPC currently
  does not).

### 15.6 offload: Moving Heavy Compute to a Pool

Executor threads should only run light work. Hand CPU-heavy work and genuinely blocking
calls to the offload pool:

```rust
let sum = offload(move || heavy_compute(data)).await;          // unconditional
let sum = offload_in(&scope, move || heavy_compute(data)).await?; // checks cancellation at the entry
```

Rule: the pool **will not** kill a closure that has already started, and scope
cancellation cannot reach inside one either — a long closure has to poll
`scope.is_cancelled()` itself; the queue is currently unbounded, so exercise restraint
(the residual risk is on record).

### 15.7 Watch: latest-wins Streaming Consumption

For data where you only care about the **latest value** (pose, battery, the most recent
frame), use a Watch — do not simulate it with a subscriber:

```rust
let watch = node.watch("robot_pose").await?;      // subscribe + keep only the latest
// RT side:
if let Some(v) = watch.load() { /* zero-copy shared handle */ }
// Structured read: seq / source timestamp / value all come from the same atomic snapshot, never mismatched
if let Some(s) = watch.snapshot() { /* s.seq / s.stamp_ns / s.payload */ }
// Await a new value asynchronously — this line deletes the sleep+poll loop:
let mut seen = watch.seq();
loop {
    seen = watch.changed(seen).await;              // lost-wakeup-free; latest-wins may skip intermediate values
    let v = watch.load().unwrap();
    consume(&v);
}
// Injection from a foreign bus (a CAN thread etc., not going through Zenoh):
let bus = Watch::detached("battery-bus");
bus.store(ZBytes::from(frame_bytes));
// unix: wire a readable fd into a Watch (serial port / socket; not a hard-RT path)
let watch = node.poll_fd(owned_fd).await?;
```

Data where every message must be handled → a plain Subscriber. Watch skipping
intermediate values is a **contract**, not a bug.

### 15.8 Endpoint Capabilities: Priority and Admission

```rust
// Dynamically override this subscriber's "executor dispatch priority" (None = restore the QoS mapping)
subscriber.set_flow_priority(Some(EventPriority::High))?;

// Counting gate + per-event queueing deadline (expired events are dropped at dequeue and counted, never silently)
subscriber.set_flow_admission(FlowAdmission::gated(
    4,                                   // at most 4 in flight (0 = ungated; the settable form of receive_depth)
    Some(Duration::from_millis(20)),     // an event that has sat in the queue for 20ms is better left unprocessed
))?;
```

Note the **two separate priority vocabularies**: `EventPriority` is local executor
scheduling (this section); `qos::Priority` is the Zenoh network wire-level priority for
publishing. A subscriber's wire-level priority maps to an event priority on the receive
side, unless overridden by the setter above.
(Pull-mode subscribers have no executor dispatch, so both setters reject explicitly.)

### 15.9 Readiness: Multi-Input Ready Triggers

"Run the fusion only once camera and lidar and imu all have new data" — do not poll each
input:

```rust
// AND gate: the position is the condition index; the satisfy that completes the whole set fires one wave and resets
let set = ReadinessSet::with_conditions(&[
    Condition::MessageAvailable,  // 0: camera
    Condition::MessageAvailable,  // 1: lidar
    Condition::Periodic,          // 2: tick
])?;
let seen = set.waves();           // take the baseline first, then arm the trigger sources — the key to being race-free
/* in each source's callback: set.satisfy(i); */
let wave = set.wait_wave_after(seen).await;

// Approximate-time alignment (message_filters style):
let window = WindowSync::new(3, Duration::from_millis(5));
/* each source: window.feed(i, stamp_ns, payload_arc); */
let aligned = window.aligned().await;   // emitted only when all three land inside a 5ms window
```

Wiring subscribers up is **declarative** — no hand-written glue needed: at declare time
use `SubscriberOptions::new().with_window_feed(&window, i)` /
`.with_readiness_satisfy(&set, i)`, or after the fact on an already-declared subscription
`sub.bind_window(&window, i, stamp)` / `bind_readiness(&set, i)`
(unbind with `unbind_window` / `unbind_readiness`). Only non-subscriber sources (timers,
foreign threads) call `set.satisfy(i)` / `window.feed(...)` by hand.

### 15.10 Actions: Goal Lifecycle Verbs and GoalGuard

The action verbs are async to begin with (§14): `client.send_goal(&goal)` →
`GoalHandle`, and the handle carries `wait_result` / `get_result` / `cancel` /
`feedback_stream` / `observe_result` (a cheap, lazy terminal-state view you can move into
another task or a `select!` arm). What Flow adds is the **RAII cancellation policy**:
`GoalHandle::into_guard()` wraps the handle in a `GoalGuard` (cancel-on-drop,
**opt-in** — dropping a bare `GoalHandle` never cancels the goal):

```rust
let handle = client.send_goal(&goal).await?;
let guard  = handle.into_guard();       // from here on: falling out of scope = best-effort cancel

// Compose as usual while guarded: handle() is a cheap Arc-clone view
let result = guard
    .handle()
    .wait_result(Some(Duration::from_secs(10)))
    .await;

match result {
    Ok(r) => {
        let _handle = guard.disarm();   // normal completion: release without cancelling
        use_result(r);
    }
    Err(_) => drop(guard),  // an armed drop = detached best-effort cancel;
                            // for a "cancel with an acknowledgement" use guard.cancel().await?
}
```

- **Layered semantics**: a scope cancel stops the **local wait** (a suspended
  `wait_result`); the server-side goal does not stop by itself — only two things reach
  the server as a cancel: `guard.cancel().await` (awaitable, acknowledgement checkable,
  consumes the guard) or an armed guard being dropped (spawns one detached best-effort
  cancel on the ambient tokio runtime; outside a runtime it is WARN-and-skip).
- **Budgets on the wire**: a `send_goal` issued inside a `scope.timeout(..)` ambient
  budget window stamps the remaining window onto the wire, and the server uses it to seed
  the goal scope (`AORTA_RPC_DEADLINE=0` turns the whole thing off).
- The C++ counterparts are `flow::send_goal` / `wait_result` / `cancel_goal` /
  `GoalGuard` (see `cpp/README.md` §17.10).

### 15.11 Best-Practices Checklist

1. **Every scope has a destination**: `shutdown`/`close` are the normal path; do not
   count on drop.
2. **Every loop is cancellable**: slow awaits → `run_until_cancelled`; compute stretches
   → `checkpoint()?`. If you cannot point at the line that observes cancellation, it will
   not stop.
3. **Do not drop results**: `#[must_use]` is not decoration; fire-and-forget has to be
   spelled out.
4. **Keep executor threads light**: heavy work goes to `offload_in`, and long closures
   check `is_cancelled` themselves.
5. **Put the deadline at the top of the request** (a scope budget) and let the narrowing
   mechanism constrain the sub-steps; per-call timeouts are there to guard against
   external services, not to express intent.
6. **Watch for latest-wins, Subscriber for per-message consumption** — do not simulate
   one with the other.
7. **Hang long-lived scopes under `node.root_scope()`**: node close automatically
   cascades **cancellation**, but only gives the tree a bounded 500 ms drain window —
   deterministic draining is still your own `shutdown`/`close`.
8. **Give scopes static names**: orphan warnings, cancellation logs and future tracing
   all depend on them.

### 15.12 Full Example

`rust/examples/flow_demo.rs` strings together everything in this chapter (scope tree /
supervised isolation / `call_in` cancellation / Watch `changed` / WindowSync fusion), and
runs in a single process with no router:

```bash
cargo run -p aorta_sdk --features examples --example flow_demo
```

### 15.13 flow_introspect: Runtime Introspection of the Scope Tree

When `close()` hangs, the question is "which scopes are actually still alive, and what
are they waiting on" — the SDK re-exports core's debug introspection surface:
`aorta_sdk::flow_introspect` (i.e. `aorta_core::flow::introspect`), whose API is
`enable()` / `enabled()` / `dump()` plus `ScopeInfo`:

```rust
use aorta_sdk::flow_introspect;

flow_introspect::enable();  // idempotent; only registers scopes created after enabling — call it early
/* ... create scopes, run tasks ... */
for s in flow_introspect::dump() {  // snapshot every live scope (pruning dead entries on the way)
    println!(
        "{}: children={} detached={} cancelled={:?} budget={:?} closing={} handles={}",
        s.name, s.live_children, s.detached, s.cancelled,
        s.budget_remaining, s.closing, s.user_handles
    );
}
```

Aimed at debugging and tooling (the `close()` watchdog names only one scope; this lists
them all). Lazily activated: until `enable()` is called, each Scope creation pays exactly
one `OnceLock::get()` atomic load and registers nothing; once registered, each scope
occupies one `Weak` in the global table, pruned incidentally by `dump()`. The current
output is a flat list (no parent edges — recorded as a follow-up).
