# Aorta C++ SDK Guide

> Canonical location: `cpp/README.md`
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
15. [ContextStore](#15-contextstore)
16. [Tracing](#16-tracing)
17. [Aorta Flow: C++20 Coroutines](#17-aorta-flow-c20-coroutines)

---

## 1. Quick Start

### 1.1 Including Headers

A single umbrella header gives you the baseline (core) Aorta C++ SDK:

```cpp
#include "aorta/aorta.h"
```

That header (`cpp/include/aorta/aorta.h`) is an umbrella header for the core
SDK surface. Optional modules are **not** part of it and need their own
includes: `ContextStore` (`aorta/contextstore.h`, §15), tracing
(`aorta/trace.h`, §16), and the C++20 Flow coroutine layer (`aorta/flow.hpp`, a
separate build target, §17). The umbrella re-exports the following submodules:

| Sub-header | Purpose |
|---------|------|
| `node.h` | Node creation and lifecycle |
| `publisher.h` | Publisher |
| `subscriber.h` | Subscriber |
| `pull_subscriber.h` | `PullSubscriber` / `PullSample` (pull-mode subscription) |
| `service.h` | RPC service side |
| `client.h` | RPC client side |
| `action.h` | Action client / server |
| `capture.h` | `CaptureSubscriber` / `SchemaResolver` (raw capture) |
| `convenience.h` | `Must()` / `CheckOk()` / `RunOrFatal()` / `FatalError` helpers |
| `executor.h` | Executor |
| `timer.h` | Timer |
| `qos.h` | QoS configuration |
| `options.h` | Per-component options |
| `status.h` | `Status` / `StatusOr` error handling |
| `types.h` | `MessageView` / `MessageContext` |
| `header.h` | `AortaHeader` construction |
| `schema_meta.h` | The `SchemaMeta` template |
| `typed_publish.h` | `TypedPublishTraits` |
| `typed_subscribe.h` | `TypedSubscribeTraits` / `TypedRetainedSample` |
| `typed_service.h` | Typed RPC service / client helpers |
| `logging.h` | Logging macros and configuration |
| `time.h` | Global time source |
| `uuid.h` | `NewUuidV7()` |
| `flatbuffers.h` | FlatBuffers re-export |

### 1.2 Minimal Publisher Example

The code below is the simplest possible publisher program (see
`cpp/examples/talker_simple.cc`):

```cpp
#include "aorta/aorta.h"
#include "demo_chat_msg.h"  // Generated message wrapper header

int main() {
  // 1. Initialize logging (configuration read from the environment)
  (void)aorta::InitLoggingFromEnv();

  // 2. Create the Node
  auto node = aorta::Must(aorta::Node::Create("my_talker", "default"),
                          "Create talker node");

  // 3. Create a typed Publisher (schema BFBS injected automatically)
  auto publisher = aorta::Must(
      node->CreatePublisher<demo::Chat>("/demo/chatter"),
      "Create chatter publisher");

  // 4. Publish a message (synchronous fire-and-forget)
  auto status = publisher->PublishTyped<demo::Chat>(
      [](demo::ChatMsg& msg) {
        msg.text = "hello from Aorta!";
      });

  aorta::CheckOk(status, "Publish chatter sample");

  return 0;
}
```

**Key points:**

- `Node::Create()` returns `StatusOr<shared_ptr<Node>>`; you must check `.ok()`
- The template parameter `T` of `CreatePublisher<T>()` is the FlatBuffers-generated message root type; the SDK injects the schema BFBS automatically through `SchemaMeta<T>`
- `PublishTyped<T>()` is a **synchronous fire-and-forget** operation: it does not wait for subscriber acknowledgement. Under the default `CongestionControl::kDrop` it never blocks; under `kBlock` it can block the calling thread while the transport queue is backpressured
- The AortaHeader (timestamp, sequence number, publishing node name) is **injected automatically** by the SDK; you never build it by hand
- `aorta::Must(...)` / `aorta::CheckOk(...)` suit application code that wants to fail fast; low-level library code can still handle `StatusOr` explicitly

### 1.3 Minimal Subscriber Example

The code below is the simplest possible subscriber program (see
`cpp/examples/listener.cc`):

```cpp
#include "aorta/aorta.h"
#include "demo_chat_msg.h"

int main() {
  (void)aorta::InitLoggingFromEnv();

  auto node = aorta::Must(
      aorta::Node::Create("my_listener", "default"), "Create node");

  // Attach the executor BEFORE creating any callback-capable primitive: the
  // first executor attached to a node owns it, so a subscriber created first
  // would bind to the SDK auto-executor and this one would spin without ever
  // receiving an event.
  auto executor = aorta::Must(aorta::Executor::Create(), "Create executor");
  aorta::CheckOk(executor->AddNode(node), "Executor::AddNode");

  // Create the subscriber; the callback receives MessageView + MessageContextView
  auto sub = aorta::Must(node->CreateSubscriber(
      "/demo/chatter",
      [](const aorta::MessageView& msg, const aorta::MessageContextView& ctx) {
        // msg.data can be null for multi-slice payloads; coalesce first.
        std::vector<uint8_t> owned;
        const uint8_t* payload = msg.data;
        if (!payload && msg.size > 0) {
          owned = msg.ToVector();
          payload = owned.empty() ? nullptr : owned.data();
        }
        auto chat = payload ? demo::GetChat(payload) : nullptr;
        if (chat && chat->text()) {
          AORTA_LOG(INFO) << "Received: " << chat->text()->c_str();
        }
      }),
      "Create subscriber");
  (void)sub;

  // Drive the callbacks
  aorta::CheckOk(executor->Spin(), "Executor::Spin");  // Blocks until Stop()

  return 0;
}
```

---

## 2. Node

### 2.1 Overview

`Node` is the central object of the Aorta SDK: every communication primitive
(Publisher / Subscriber / Service / Client / Timer) is created through a Node.
Every Node has:

- **name**: the node name (user-supplied)
- **id**: a stable node id derived by Core from (host id, group, node name) via SHA-256 and truncated to 32 hex characters; `AORTA_NODE_ID` overrides it
- **group**: the group name (used for the `{group}` segment of the key expression)

### 2.2 Creating a Node

```cpp
// Signature
static StatusOr<std::shared_ptr<Node>> Create(
    const std::string& name,
    const std::string& group = "default");
```

```cpp
// Example
auto node = aorta::Must(
    aorta::Node::Create("my_node", "default"), "Create node");

// Accessing the attributes (group() returns the resolved effective group)
AORTA_LOG(INFO) << "name=" << node->name()
                << " id=" << node->id()
                << " group=" << node->group();
```

**Design notes:**

- `Node::Create()` connects to the Zenoh runtime and registers a liveliness token internally, so it is an operation that **can fail**; it reports through `StatusOr`
- The group participates in key expression construction: `aorta/{group}/pub/{topic}`
- **⚠️ The `group` argument to the constructor is currently ignored**: Core always resolves the effective group from `AORTA_GROUP` (falling back to `default`) (`core/src/node.rs:246`). To isolate nodes into different groups, set `AORTA_GROUP` (per process); passing different constructor arguments has no effect. `node->group()` returns the resolved effective value
- A Node is a `std::shared_ptr`; every primitive created through it holds a `shared_ptr` to the Node, which keeps the lifetime safe

### 2.3 Thread Safety

Per the doc comments in `node.h`:

- `Create()` must be called from a **single thread**
- `CreatePublisher/Subscriber/Service/Client/Timer` and `Now()` are safe to call from **multiple threads** once the Node is constructed
- The destructor stops the internal executor and joins its thread; it must not race with any of the `Create*` methods

### 2.4 The StatusOr Error-Handling Pattern

Every factory method in the Aorta SDK returns `StatusOr<T>` and follows one
uniform error-handling pattern:

```cpp
// StatusOr<T> API
template <typename T>
class StatusOr {
 public:
  bool ok() const;               // Success?
  explicit operator bool() const;
  const Status& status() const;  // The Status (code + message)
  T& value();                    // The value (fail-fast on error, not UB)
};

// Status API
class Status {
 public:
  bool ok() const;
  aorta_result_t code() const;
  const std::string& message() const;
  static Status Ok();
};
```

The recommended error-handling pattern:

```cpp
auto publisher = aorta::Must(
    node->CreatePublisher<demo::Chat>("/topic"), "Create publisher");
```

For application-level code, if you prefer the terser ROS2-style spelling, you
can also use `aorta/convenience.h` (already re-exported by `aorta/aorta.h`):

```cpp
auto node = aorta::Must(aorta::Node::Create("demo_node"), "Create node");
auto publisher = aorta::Must(
    node->CreatePublisher<demo::Chat>("/topic"),
    "Create publisher");
aorta::CheckOk(
    publisher->PublishTyped<demo::Chat>(
        [](demo::ChatMsg& msg) { msg.text = "hello"; }),
    "Publish chat sample");
```

On failure, `Must()` / `CheckOk()` **no longer** `std::abort()`; they throw
`aorta::FatalError` (carrying the original `Status`). This lets stack RAII
destructors unwind, so live `Node` / `Publisher` /
`Subscriber` / `Service` handles all get a clean `aorta_*_free` call — peers see
a graceful undeclare instead of a TCP RST. (Under `-fno-exceptions` the
diagnostic is still printed and the process aborts.)

#### RunOrFatal() / FatalError

Wrapping the body of `main()` in `aorta::RunOrFatal()` is recommended: it
catches `FatalError` (and any `std::exception`), lets the unwind run every RAII
destructor, and only then returns `EXIT_FAILURE`, so handles get a graceful
`aorta_*_free` and undeclare. `RunOrFatal()` is itself marked `noexcept`, so no
exception escapes past `main`:

```cpp
// (see cpp/examples/listener.cc / talker.cc / echo_server.cc)
int main() {
  return aorta::RunOrFatal([&]() -> int {
    auto node = aorta::Must(aorta::Node::Create("listener"));
    // … any Must()/CheckOk() call that may throw FatalError …
    return 0;  // Return 0 normally; a caught exception yields EXIT_FAILURE
  });
}
```

`FatalError` derives from `std::exception` and exposes the underlying `Status`
through `status()`, so a custom try/catch can read the error code without losing
information.

---

## 3. Publisher & Subscriber

### 3.1 Publisher Overview

The Publisher is Aorta's message publisher. Its core design points:

- **Synchronous fire-and-forget**: `Publish()` / `PublishTyped()` / `PublishBytes()` are all synchronous calls; they do not block waiting for an acknowledgement
- **Automatic AortaHeader injection**: the SDK fills in the timestamp, sequence number, and publishing node name
- **A converged stable API surface**: the current stable C++ SDK exposes `Publish()` / `PublishTyped()` / `PublishBytes()` / `PublishOwned()` / `MatchingStatus()` / `WaitForMatching()`; explicit `loan()` / `publish_shm()` are not implemented yet
- **Builder reuse policy**: TLS / mutex / fresh-builder are SDK-internal implementation details, not exposed as stable `PublisherOptions` fields
- **Readiness gating**: use `MatchingStatus()` / `WaitForMatching()` to wait for a subscriber to appear before the first publish, avoiding publish-before-route-known drops (see the readiness-gating subsection after §3.4)
- **Statistics**: `GetStats()` gives you the publish count, oversize-message statistics, reentrancy statistics, and more

### 3.2 Creating a Publisher

#### 3.2.1 Typed Publisher (recommended)

```cpp
// Signature (template version; injects SchemaMeta automatically)
template <typename T>
StatusOr<std::shared_ptr<Publisher>> CreatePublisher(
    const std::string& topic,
    const QoS& qos = QoS(),
    PublisherOptions options = PublisherOptions());
```

```cpp
// Example: create a typed publisher
auto pub = aorta::Must(
    node->CreatePublisher<demo::Chat>("/demo/chatter"), "Create publisher");
```

The template parameter `T` must have a matching `SchemaMeta<T>` specialization
(generated automatically by the codegen tooling). The SDK then automatically:

1. Fetches the schema binary from `SchemaMeta<T>::SchemaBfbs()`
2. Prefers `SchemaMeta<T>::FixedSize()` as `max_message_size`
3. Falls back to `SchemaMeta<T>::ShmCapacityHint()` if `FixedSize()` is absent
4. Returns an `AORTA_ERR_INVALID_ARGUMENT` error if the schema BFBS is empty

#### 3.2.2 Untyped Publisher

```cpp
// Signature (non-template version)
StatusOr<std::shared_ptr<Publisher>> CreatePublisher(
    const std::string& topic,
    const QoS& qos = QoS(),
    const PublisherOptions& options = PublisherOptions());
```

```cpp
// Example: supply the schema BFBS by hand
aorta::PublisherOptions opts;
opts.schema_bfbs = GetMySchemaBytes();  // Supplied manually
auto pub = aorta::Must(
    node->CreatePublisher("/raw/topic", aorta::QoS(), opts),
    "Create raw publisher");
```

#### 3.2.3 PublisherOptions

```cpp
struct PublisherOptions {
  SchemaAttachPolicy schema_attach_policy = SchemaAttachPolicy::kInherit;
  std::vector<uint8_t> schema_bfbs;            // Schema binary
  std::string schema_root_type;                // Schema root table name
  std::optional<std::size_t> max_message_size; // Initial builder capacity
  std::optional<CongestionControl> congestion_control; // Overrides qos.congestion_control
};
```

| Field | Description |
|------|------|
| `schema_attach_policy` | Controls whether the schema hash is attached to published data (`kInherit` / `kAlways` / `kNever`) |
| `schema_bfbs` | The schema binary reflection data (filled in automatically for a typed publisher) |
| `schema_root_type` | The schema root table name (filled in automatically for a typed publisher from `SchemaMeta<T>::RootTypeName()`) |
| `max_message_size` | The builder's initial capacity (bytes); affects memory pre-allocation |
| `congestion_control` | An optional **per-publisher override** of `qos.congestion_control`. `std::nullopt` (the default) inherits the QoS value; an explicit value wins. For the semantics see [§7 QoS](#7-qos) — `kDrop` gives the lowest latency (it still drops even under `kReliable`); `kBlock` avoids local drops by backpressuring the sender (not an end-to-end no-loss guarantee) |

The current stable C++ SDK does not expose builder growth / reuse or explicit
SHM/loan options; those details are managed internally by the SDK/Core so the
stable surface does not bind to experimental capabilities prematurely.

### 3.3 Publishing Messages

#### 3.3.1 PublishTyped (recommended)

The most concise way to publish: fill in the generated message struct through a
lambda.

```cpp
// Signature
template <typename T, typename FillFn>
Status PublishTyped(FillFn&& fill) const;
```

```cpp
// Example (from cpp/examples/talker.cc)
auto status = publisher->PublishTyped<demo::Chat>(
    [&](demo::ChatMsg& msg) {
      msg.text = std::string("hello ") + std::to_string(counter);
    });
```

**How it works:**

1. The SDK creates a default-constructed `demo::ChatMsg` struct
2. It calls your lambda to fill in the fields
3. The SDK calls `TypedPublishTraits<T>::Build()` to serialize the struct into a FlatBuffer
4. The SDK injects the AortaHeader automatically (publish_stamp_ns + publisher_node + sequence)
5. The message goes out over the Zenoh transport

**Preconditions (checked at compile time):**

- A `TypedPublishTraits<T>` specialization must exist (provided by the generated `*_msg.h` header)
- The schema root table must have an `aorta_header` field

#### 3.3.2 PublishTypedMessage

Pass an already-populated message struct directly:

```cpp
// Signature
template <typename T>
Status PublishTypedMessage(const typename TypedPublishTraits<T>::Msg& msg) const;
```

```cpp
// Example
demo::ChatMsg msg;
msg.text = "hello";
publisher->PublishTypedMessage<demo::Chat>(msg);
```

#### 3.3.3 Publish (builder callback)

Build the message by hand with a FlatBufferBuilder — for the complex cases:

```cpp
// Signature
Status Publish(BuildCallback build) const;
// BuildCallback = std::function<void(FlatBufferBuilder&, Offset<AortaHeader>)>
```

```cpp
// Example
publisher->Publish([](flatbuffers::FlatBufferBuilder& builder,
                      flatbuffers::Offset<aorta::sys::AortaHeader> header) {
  auto text = builder.CreateString("manual build");
  auto chat = demo::CreateChat(builder, header, text);
  builder.Finish(chat);
});
```

#### 3.3.4 PublishBytes (raw bytes)

Send raw bytes directly, with no AortaHeader added:

```cpp
Status PublishBytes(const uint8_t* data, std::size_t len) const;
Status PublishBytes(const std::vector<uint8_t>& data) const;
```

#### 3.3.5 PublishOwned (zero-copy)

Transfer ownership of the data to the transport layer to avoid a copy:

```cpp
using ReleaseFunc = void (*)(void* ctx, const uint8_t* data, std::size_t len);
Status PublishOwned(const uint8_t* data, std::size_t len,
                    ReleaseFunc release_fn, void* release_ctx) const;
```

Once the send completes (or errors out), the transport layer calls `release_fn`
to hand the memory back.

#### 3.3.6 PublishWithBuilder (external builder)

Use a caller-supplied FlatBufferBuilder — for the self-managed-builder case:

```cpp
Status PublishWithBuilder(flatbuffers::FlatBufferBuilder& builder,
                          BuildCallback build) const;
```

### 3.4 Publisher Statistics

```cpp
struct Stats {
  uint64_t publish_count = 0;       // Total publishes
  uint64_t oversize_count = 0;      // Times the initial capacity was exceeded
  std::size_t last_oversize_size = 0;
  std::size_t current_capacity = 0;
  std::size_t max_capacity_seen = 0;
  uint64_t last_publish_ns = 0;     // Timestamp of the last publish
  uint64_t send_error_count = 0;    // Send failures
  // Builder-reuse / staging fast-path counters
  uint64_t detached_wrapper_reuse_hit_count = 0;
  uint64_t detached_wrapper_reuse_miss_count = 0;
  uint64_t stage_hit_count = 0;
  uint64_t stage_miss_count = 0;
  // Per-phase timing (ns; *_sample_count is the divisor for an average)
  uint64_t build_total_ns = 0;
  uint64_t build_max_ns = 0;
  uint64_t build_sample_count = 0;
  uint64_t transport_total_ns = 0;
  uint64_t transport_max_ns = 0;
  uint64_t transport_sample_count = 0;
};

auto stats = publisher->GetStats();
AORTA_LOG(INFO) << "Published " << stats.publish_count << " messages";
```

#### Publisher readiness gating

The Publisher exposes two readiness queries you can use to confirm a matching
subscriber exists before the first publish, avoiding the
publish-before-route-known drop:

```cpp
// One-shot read of the local routing tables; true means at least one matching
// subscriber currently exists. No network round-trip.
StatusOr<bool> MatchingStatus() const;

// Wait up to timeout_ms milliseconds for a matching subscriber to appear;
// returns the final matching state. Polls the local routing tables; a timeout
// returns false (not an error — the caller may "fire anyway").
StatusOr<bool> WaitForMatching(uint64_t timeout_ms) const;
```

```cpp
// Gate the first publish
if (!aorta::Must(pub->WaitForMatching(2000))) {
  AORTA_LOG(WARN) << "no subscriber yet; publishing anyway";
}
aorta::CheckOk(pub->PublishTyped<demo::Chat>(
    [](demo::ChatMsg& msg) { msg.text = "hello"; }));
```

> `MatchingStatus()` is a one-shot snapshot (local routing tables only, no
> network round-trip); `WaitForMatching()` polls until the timeout. A `false`
> on timeout does not mean failure — it only means no subscriber matched within
> the allotted time.

### 3.5 Subscriber Overview

A Subscriber receives messages through a callback. Its core features:

- **Header context**: optionally parses the sequence / publisher_node out of the AortaHeader
- **Deduplication (dedup)**: sliding-window deduplication keyed on `(topic, publisher_node, sequence)`; when no stable publisher identity is present in the header, the transport source identity is used as a fallback, and if neither is available the message skips dedup/reorder checks entirely
- **Out-of-order detection (reorder)**: detects and optionally drops out-of-order messages
- **Overflow policy**: Drop or Relay (buffered relay)
- **Schema prefetch**: actively queries the remote schema at initialization

### 3.6 Creating a Subscriber

#### 3.6.1 Basic creation

```cpp
// Signature
StatusOr<std::shared_ptr<Subscriber>> CreateSubscriber(
    const std::string& topic,
    Subscriber::Callback callback,
    const SubscriberOptions& options = SubscriberOptions());
```

```cpp
// Callback signature
using Callback = std::function<void(const MessageView&, const MessageContextView&)>;
```

```cpp
// Example (from cpp/examples/listener.cc)
auto sub = aorta::Must(node->CreateSubscriber(
    "/demo/chatter",
    [](const aorta::MessageView& msg, const aorta::MessageContextView& ctx) {
      // msg.data / msg.size: the raw payload
      // msg.slices / msg.slice_count: the sliced view
      // ctx.topic / ctx.sequence / ctx.publisher_node: the context
      // msg.data is null when the payload arrives as multiple slices —
      // coalesce with ToVector() before handing it to FlatBuffers.
      std::vector<uint8_t> owned;
      const uint8_t* payload = msg.data;
      if (!payload && msg.size > 0) {
        owned = msg.ToVector();
        payload = owned.empty() ? nullptr : owned.data();
      }
      auto chat = payload ? demo::GetChat(payload) : nullptr;
      if (chat && chat->text()) {
        AORTA_LOG(INFO) << chat->text()->c_str();
      }
    }),
    "Create subscriber");
```

> Prefer the typed subscriber (§3.6.2) when you can — it handles payload
> coalescing and FlatBuffers verification for you.

#### 3.6.2 Typed Subscriber: View vs Retained

The current C++ typed-subscribe surface is split explicitly into two interfaces:

- `CreateSubscriberTypedView<T>()`: the default high-performance borrowed view, valid only inside the callback
- `CreateSubscriberTypedRetained<T>()`: for when you need to retain the payload past the callback or across threads

**Borrowed-view API**

```cpp
template <typename T>
StatusOr<std::shared_ptr<Subscriber>> CreateSubscriberTypedView(
    const std::string& topic,
    std::function<void(const T&, const MessageContextView&)> callback,
    SubscriberOptions options = SubscriberOptions());
```

```cpp
auto sub = aorta::Must(node->CreateSubscriberTypedView<demo::Chat>(
    "/demo/chatter",
    [](const demo::Chat& msg, const aorta::MessageContextView& ctx) {
      (void)ctx;
      if (msg.text()) {
        AORTA_LOG(INFO) << msg.text()->c_str();
      }
    }),
    "Create typed-view subscriber");
```

**Retained API**

```cpp
template <typename T>
StatusOr<std::shared_ptr<Subscriber>> CreateSubscriberTypedRetained(
    const std::string& topic,
    std::function<void(TypedRetainedSamplePtr<T>)> callback,
    SubscriberOptions options = SubscriberOptions());
```

```cpp
auto sub = aorta::Must(node->CreateSubscriberTypedRetained<demo::Chat>(
    "/demo/chatter",
    [](aorta::TypedRetainedSamplePtr<demo::Chat> sample) {
      if (const demo::Chat* msg = sample->Message(); msg && msg->text()) {
        AORTA_LOG(INFO) << msg->text()->c_str();
      }
    }),
    "Create typed-retained subscriber");
```

Both interfaces inject the BFBS from `SchemaMeta<T>` automatically.
`TypedRetainedSamplePtr<T>` holds an owned copy of the payload before entering
the callback, so the caller can safely access the message content even after
retaining the sample past the callback.

The access surface of `TypedRetainedSample<T>` (reached through `sample->`):

| Method | Returns | Description |
|------|------|------|
| `Message()` | `const T*` | The parsed message root table (nullptr when the payload is empty) |
| `Context()` | `const MessageContext&` | The message context (topic / sequence / publisher_node, etc.) |
| `Data()` | `const uint8_t*` | Pointer to the raw payload |
| `Size()` | `std::size_t` | Payload size in bytes |
| `IsOwned()` | `bool` | Whether an owned copy is held (always true on the retained path) |

> Unlike the borrowed-view callback, the retained callback does **not** receive a
> separate `MessageContextView` — the only way to read the context is
> `sample->Context()` (which returns an owned `MessageContext`). It shares the
> sample's lifetime and is safe to use past the callback and across threads.

#### 3.6.3 Creation with QoS

```cpp
auto sub = node->CreateSubscriberTypedView<demo::Chat>(
    "/demo/chatter", callback,
    aorta::QoS::SensorData(),    // QoS profile
    sub_options);
```

### 3.7 MessageView and MessageContext

#### MessageView

```cpp
struct MessageView {
  const uint8_t* data = nullptr;   // Contiguous data pointer (may be nullptr)
  std::size_t size = 0;            // Total size
  const MessageSlice* slices = nullptr;  // Slice array
  std::size_t slice_count = 0;

  bool IsContiguous() const;       // Contiguous? (slice_count <= 1)
  std::vector<uint8_t> ToVector() const;  // Copy into a vector
};
```

**Important:** the pointers inside a `MessageView` are valid only while the
callback runs. To retain the data, call `ToVector()` to copy it. The type is
`[[nodiscard]]` and its copy constructor / copy assignment are deleted, so the
common "capture the view into a background thread" dangling pattern fails to
compile.

The recommended pattern for handling sliced data:

```cpp
[](const aorta::MessageView& msg, const aorta::MessageContextView&) {
  const uint8_t* payload = msg.data;
  std::vector<uint8_t> owned;
  if (!payload && msg.size > 0) {
    owned = msg.ToVector();  // Non-contiguous data: copy and coalesce
    payload = owned.data();
  }
  auto chat = demo::GetChat(payload);
  // ...
}
```

#### MessageContext

```cpp
struct MessageContext {
  std::string topic;              // The publish key expression
  int64_t receive_stamp_ns = 0;   // Receive timestamp
  int32_t priority = 0;           // Priority
  std::string publisher_node;     // Publishing node name
  uint64_t sequence = 0;          // Sequence number
};
```

The context fields have to be enabled through `SubscriberOptions`:

| Option | Context fields it enables |
|------|-------------------|
| `enable_header_context` | `publisher_node`, `sequence` (parsed from the AortaHeader) |
| `include_topic_in_context` | `topic` |
| `include_receive_timestamp` | `receive_stamp_ns` |
| `include_priority_in_context` | `priority` |
| `include_publisher_timestamp` | (none today — see note below) |

> **`include_publisher_timestamp` is currently not surfaced in C++**: the option
> exists on `SubscriberOptions`, but the C ABI context struct and the C++
> `MessageContext` / `MessageContextView` have no publisher-timestamp field, so
> a C++ observer cannot read that timestamp from the context today. To get the
> publish timestamp in C++, parse `publish_stamp_ns` out of the message's
> `AortaHeader`. (The Rust SDK does expose `publisher_stamp_ns` on its
> `MessageContext`.)

### 3.8 SubscriberOptions in Detail

```cpp
struct SubscriberOptions {
  QoS qos = QoS();
  CallbackGroup callback_group = CallbackGroup::kMutuallyExclusive;
  CallbackGroupHandle callback_group_handle = 0;
  int64_t deadline_ns = 0;

  // Schema
  bool prefetch_schema_on_init = true;
  std::vector<uint8_t> schema_bfbs;

  // Header and context
  bool enable_header_context = false;
  bool include_topic_in_context = false;
  bool include_receive_timestamp = false;
  bool include_priority_in_context = false;
  bool include_publisher_timestamp = false;

  // Deduplication and out-of-order handling
  bool enable_dedup = false;
  std::size_t dedup_window = 1024;
  bool enable_reorder_check = false;
  bool drop_out_of_order = false;

  // Overflow policy
  SubscriberOverflowPolicy overflow_policy = SubscriberOverflowPolicy::kDrop;
  std::size_t overflow_relay_capacity = 1024;

  // Per-topic buffering (see §3.9)
  std::size_t receive_depth = 0;            // Per-topic in-flight cap (0 = disabled, today's behavior)
  DeliveryMode delivery = DeliveryMode::kExecutor;          // Callback vs native pull ring
  ReceiveOverflow receive_overflow = ReceiveOverflow::kDropNewest;  // Drop direction for receive_depth
};
```

**Configuration example (from `cpp/examples/listener.cc`):**

```cpp
aorta::SubscriberOptions sub_options;
sub_options.enable_header_context = true;   // Parse the AortaHeader
sub_options.enable_dedup = true;            // Enable deduplication
sub_options.dedup_window = 1024;            // Dedup window
sub_options.enable_reorder_check = true;    // Out-of-order detection
sub_options.drop_out_of_order = true;       // Drop out-of-order messages
```

### 3.9 Per-Topic Subscription Buffering (receive_depth / Pull Subscriptions)

The QoS & buffering redesign introduces **per-topic** subscribe-side
backpressure, orthogonal to QoS.

**Tier A — `SubscriberOptions::receive_depth` (in-flight cap + drop isolation)**

`receive_depth` is the in-flight event cap for a single topic. `0` (the default)
disables the per-topic gate — events flow straight into the shared executor
queue, which is itself bounded (the executor's queue capacity); with `>0`, once
the cap is reached **the newest event is dropped** (drop-newest), isolating one
slow topic's congestion inside that topic. The drop count is observable through
`Subscriber::DroppedCount()`:

```cpp
aorta::SubscriberOptions opts;
opts.receive_depth = 16;
auto sub = aorta::Must(
    node->CreateSubscriber("/sensors/lidar", callback, opts),
    "Create lidar subscriber");
// ... after running for a while
AORTA_LOG(INFO) << "shed by depth cap = " << sub->DroppedCount();
```

**Tier B-pull — pull-mode subscribers (`CreatePullSubscriber`)**

A pull subscriber owns a Zenoh-native keep-last ring; there is **no callback**,
and you drain it yourself. It is the "latest value wins" sensor idiom (similar
to rclpy's `wait_for_message`), with structural per-topic isolation and no
executor hop. `node->CreatePullSubscriber(topic, depth)` returns a
`PullSubscriber` that you drain with `Recv()` / `Recv(timeout)` / `TryRecv()`,
yielding an RAII `PullSample` (`.ToVector()` for a payload copy, `.Context()`
for the context):

```cpp
auto sub = aorta::Must(
    node->CreatePullSubscriber("/sensors/lidar", 8),  // Ring capacity = 8
    "Create pull subscriber");

// Drain on your own thread, latest value wins
for (;;) {
  auto sample = aorta::Must(sub.Recv(), "recv");  // Blocks until a sample arrives
  std::vector<uint8_t> bytes = sample.ToVector();
  process(bytes, sample.Context());
}
// Or with a timeout / non-blocking:
// auto maybe = sub.Recv(std::chrono::milliseconds(50));  // std::nullopt = timed out
// auto maybe = sub.TryRecv();                            // std::nullopt = ring empty
```

> `pull_subscriber.h` **is** re-exported by the `aorta/aorta.h` umbrella, so
> `#include "aorta/aorta.h"` is enough; include `"aorta/pull_subscriber.h"`
> explicitly only if you bypass the umbrella.

---

## 4. RPC: Service & Client

### 4.1 Overview

The Aorta RPC framework implements request/response on top of Zenoh Query/Queryable:

- **Service**: the server side; registers a Queryable to handle requests
- **Client**: the client side; sends a Query and waits for the response
- **Typed RPC**: automatically injects the request/response schema BFBS
- **Single-provider mode**: deployments must ensure each `{namespace,group,service}` has exactly one live provider

### 4.2 Creating a Service

#### 4.2.1 Typed Service (recommended)

```cpp
// Signature
template <typename RequestSchema, typename ResponseSchema>
StatusOr<std::shared_ptr<Service>> CreateService(
    const std::string& service_name,
    Service::RequestCallback callback,
    ServiceOptions options = ServiceOptions());
```

```cpp
// RequestCallback signature
using RequestCallback =
    std::function<void(const MessageView& request, ServiceRequest&& req)>;
```

```cpp
// Example (raw service callback)
auto service = aorta::Must(
    node->CreateService<demo::EchoRequest, demo::EchoResponse>(
    "/echo",
    [](const aorta::MessageView& request, aorta::ServiceRequest&& req) {
      auto response = request.ToVector();
      aorta::CheckOk(req.Reply(std::move(response)), "Reply raw response");
    }),
    "Create raw service");
```

**Typed template constraints:**

- `SchemaMeta<T>::SchemaBfbs()` for `RequestSchema` and `ResponseSchema` must point at the same `.bfbs` file (one FlatBuffers schema file defining both request and response)
- Runtime validation: `CreateService()` `std::memcmp`s the two BFBS blobs at init; if the contents differ it returns a `StatusOr` error carrying `AORTA_ERR_INVALID_ARGUMENT` (**not** a `static_assert` / compile-time check), so you must check it at the call site
- The SDK derives `request_root_type` and `response_root_type` from `SchemaMeta<T>::RootTypeName()`

#### 4.2.2 End-to-End Typed Service

```cpp
template <typename Request, typename Response>
StatusOr<std::shared_ptr<Service>> CreateServiceTypedView(
    const std::string& service_name,
    std::function<void(const Request&, TypedServiceResponder<Response>)> callback,
    ServiceOptions options = ServiceOptions());

template <typename Request, typename Response>
StatusOr<std::shared_ptr<Service>> CreateServiceTypedRetained(
    const std::string& service_name,
    std::function<void(TypedRetainedServiceRequest<Request, Response>)> callback,
    ServiceOptions options = ServiceOptions());
```

```cpp
auto service = aorta::Must(
    node->CreateServiceTypedView<demo::EchoRequest, demo::EchoResponse>(
    "/echo",
    [](const demo::EchoRequest& request,
       aorta::TypedServiceResponder<demo::EchoResponse> reply) {
      aorta::CheckOk(reply.Reply([&](demo::EchoResponseMsg& msg) {
        msg.echoed_message = request.message()->str();
      }), "Reply typed response");
    }),
    "Create typed service");
```

Typed RPC contract in the current C++ SDK:

- `Request` and `Response` must be local FlatBuffers tables from the same service schema
- both tables must include `aorta_header`
- `CreateService<Req, Resp>()` remains the schema-injection compatibility helper
- end-to-end typed RPC should use `CreateServiceTypedView()` or `CreateServiceTypedRetained()`

**The method surface of `TypedServiceResponder<Response>`** (the reply handle passed to a view callback):

| Method | Description |
|------|------|
| `Reply(fill)` | Fill a `FillFn(Response::Msg&)` and send the reply |
| `ReplyMessage(msg)` | Reply directly with an already-built `Response::Msg` |
| `ReplyBytes(bytes)` | Reply with raw bytes |
| `ReplyError(msg)` | Send a Zenoh error reply (the client's `Call` fails and carries `msg`) |
| `IsValid()` | Whether the underlying `ServiceRequest` is still valid |
| `Raw()` | Get the underlying `ServiceRequest&` (escape hatch) |

**The method surface of `TypedRetainedServiceRequest<Request, Response>`** (the retained path, holdable across callbacks and threads):

| Method | Description |
|------|------|
| `Message()` | The decoded `const Request*` |
| `Data()` / `Size()` | Pointer to and size of the raw request payload |
| `Reply(fill)` / `ReplyMessage(msg)` / `ReplyBytes(bytes)` / `ReplyError(msg)` | The same four reply forms as the responder |
| `RawResponder()` | Get the underlying `TypedServiceResponder<Response>&` |

Retained usage example (hold the request past the callback and reply later):

```cpp
auto service = aorta::Must(
    node->CreateServiceTypedRetained<demo::EchoRequest, demo::EchoResponse>(
    "/echo",
    [](aorta::TypedRetainedServiceRequest<demo::EchoRequest, demo::EchoResponse> req) {
      // req owns the payload; it can be moved to a worker thread and replied to later
      std::string echoed = req.Message()->message()->str();
      aorta::CheckOk(req.Reply([&](demo::EchoResponseMsg& msg) {
        msg.echoed_message = echoed;
      }), "Reply retained response");
    }),
    "Create typed-retained service");
```

#### 4.2.3 Untyped Service

```cpp
auto service = aorta::Must(node->CreateService(
    "/my_service",
    [](const aorta::MessageView& request, aorta::ServiceRequest&& req) {
      // Handle manually
    }),
    "Create service");
```

### 4.3 The ServiceRequest Object

`ServiceRequest` is a **move-only** request handle:

```cpp
class ServiceRequest {
 public:
  Status Reply(std::vector<uint8_t>&& response);    // Send a response
  Status ReplyError(const std::string& error_msg);  // Send an error reply (Zenoh reply_err)
  void Release();                                   // Drop the request without replying
  trace::Context TraceContext() const;              // The trace context of this request
  bool IsValid() const;                             // Whether the handle is valid
};
```

**Important rules:**

- Call `Reply()` / `ReplyError()` or `Release()` explicitly on every `ServiceRequest`. If a live handle reaches the destructor without either, the destructor logs a warning and releases it automatically — usually a sign of a callback bug, but not a leak
- `Reply()` and `ReplyError()` consume the handle on every outcome; do not call `Release()` afterwards
- When a typed service receives a request that fails FlatBuffers verification, the SDK replies with `ReplyError` automatically (the client sees a call error carrying the decode failure, rather than an empty OK payload)
- After a move the original object becomes invalid (`IsValid()` returns `false`)
- `TraceContext()` returns the trace context identifying this request (valid only if tracing was enabled when the request arrived) and does not consume the handle. In the deferred-reply pattern: capture `TraceContext()` in the callback, move it with the work to a worker thread, and install it there with `aorta::trace::ContextGuard` so that spans recorded by `AORTA_TRACE_SPAN` on that thread stay attached to this request's trace (see the Tracing section)

### 4.4 ServiceOptions

```cpp
struct ServiceOptions {
  SchemaAttachPolicy schema_attach_policy = SchemaAttachPolicy::kNever;
  CallbackGroup callback_group = CallbackGroup::kMutuallyExclusive;
  CallbackGroupHandle callback_group_handle = 0;
  int64_t deadline_ns = 0;
  RpcOverflowPolicy rpc_overflow_policy = RpcOverflowPolicy::kFastFailBusy;
  uint64_t rpc_overflow_block_timeout_ms = 0;
  std::string service_schema_hash_hex;
  std::vector<uint8_t> service_schema_bfbs;
  std::string request_root_type;
  std::string response_root_type;
};
```

| Field | Description |
|------|------|
| `rpc_overflow_policy` | Concurrency overflow policy: `kFastFailBusy` (fail fast) or `kBlock` (block and wait) |
| `service_schema_hash_hex` | Manually specify the schema hash (computed automatically in typed mode) |
| `request_root_type` | FlatBuffers root type name (filled in automatically in typed mode) |

### 4.5 Service Statistics

```cpp
struct Stats {
  uint64_t request_received_count = 0;
  uint64_t request_dispatched_count = 0;
  uint64_t reply_ok_count = 0;
  uint64_t reply_error_count = 0;
  uint64_t reply_send_failure_count = 0;
  uint64_t request_dropped_count = 0;
  uint64_t query_receive_failure_count = 0;
  uint64_t task_exit_count = 0;
  uint64_t in_flight_requests = 0;
  uint64_t last_request_ns = 0;
  uint64_t last_reply_ns = 0;
  // Deadline-expired queue drops — a SUBSET of request_dropped_count.
  uint64_t request_dropped_deadline_count = 0;
};

auto stats = service->GetStats();
```

### 4.6 Creating a Client

```cpp
// Signature
StatusOr<std::shared_ptr<Client>> CreateClient(
    const std::string& service_name,
    const ClientOptions& options = ClientOptions());

// Convenience overload (timeout only)
StatusOr<std::shared_ptr<Client>> CreateClient(
    const std::string& service_name, uint64_t timeout_ms);
```

```cpp
// Example (from cpp/examples/echo_client.cc)
aorta::ClientOptions options;
options.timeout_ms = 2000;                     // 2-second timeout
options.schema_attach_policy = aorta::SchemaAttachPolicy::kNever;
options.enforce_single_provider = true;        // Enforce single provider
options.multi_reply_check_window_ms = 100;     // Multi-reply detection window

auto client = aorta::Must(node->CreateClient("/echo", options), "Create client");
```

### 4.7 ClientOptions

```cpp
struct ClientOptions {
  uint64_t timeout_ms = 5000;  // Default: 5 seconds
  SchemaAttachPolicy schema_attach_policy = SchemaAttachPolicy::kInherit;
  std::optional<bool> enforce_single_provider;          // Single-provider mode
  std::optional<uint64_t> multi_reply_check_window_ms;  // Multi-reply detection
  std::optional<bool> allow_retry;  // Per-client RPC retry opt-in; unset inherits the core default (OFF: retries silently re-invoke non-idempotent providers, so set true only for provably idempotent services)
  std::string service_schema_hash_hex;
};
```

### 4.8 Making an RPC Call

```cpp
StatusOr<ClientResponse> Call(std::vector<uint8_t>&& request) const;
```

`Call()` is a **synchronous, blocking** operation that waits for the service reply or a timeout:

```cpp
// Example
std::string payload = "echo request #" + std::to_string(counter);
std::vector<uint8_t> request(payload.begin(), payload.end());

auto result = client->Call(std::move(request));
if (!result.ok()) {
  AORTA_LOG(WARN) << "Call failed: " << result.status().message();
} else {
  auto response = std::move(result).value().ToVector();
  std::string text(response.begin(), response.end());
  AORTA_LOG(INFO) << "Response: " << text;
}
```

#### 4.8.1 End-to-End Typed Client

```cpp
template <typename Request, typename Response>
StatusOr<std::shared_ptr<TypedClient<Request, Response>>> CreateClientTyped(
    const std::string& service_name,
    ClientOptions options = ClientOptions());
```

```cpp
auto client = node->CreateClientTyped<demo::EchoRequest, demo::EchoResponse>(
    "/echo");
auto typed_client = aorta::Must(std::move(client), "Create typed client");

auto response = aorta::Must(typed_client->Call([](demo::EchoRequestMsg& msg) {
  msg.message = std::string("hello typed rpc");
}), "Call typed service");

std::cout << response.Message()->echoed_message()->str() << std::endl;
```

Current typed client behavior:

- auto-injects `service_schema_hash_hex` from `SchemaMeta<T>::SchemaHashHex()`
- validates that request/response schema hash matches
- verifies and decodes the response using `TypedSubscribeTraits<Response>`
- does not introduce a response callback; completion remains synchronous `Call()`

### 4.9 ClientResponse

```cpp
class ClientResponse {
 public:
  bool IsValid() const;
  MessageView View() const;
  bool IsContiguous() const;
  std::vector<uint8_t> ToVector() const;
};
```

`ClientResponse` is a move-only object that internally holds the data obtained from the transport. Call `View()` for a zero-copy view, or `ToVector()` to copy into a `vector<uint8_t>`.

### 4.10 Client Statistics

```cpp
struct Stats {
  uint64_t call_count = 0;
  uint64_t success_count = 0;
  uint64_t timeout_count = 0;
  uint64_t concurrent_conflict_count = 0;
  uint64_t error_count = 0;
  uint64_t retry_count = 0;
  uint64_t last_call_started_ns = 0;
  uint64_t last_call_finished_ns = 0;
  uint64_t last_call_latency_ns = 0;
};
```

#### Client readiness gating

Symmetrically with Publisher, Client exposes two readiness queries so you can confirm a matching service provider exists before the first `Call()`:

```cpp
// One-shot read of the local routing tables; true means at least one matching
// provider currently exists. Does not perform a network round-trip.
StatusOr<bool> MatchingStatus() const;

// Wait up to timeout_ms milliseconds for a provider to appear; returns the
// final state. Returns false on timeout (this is not an error). Use it to gate
// the first Call() on a fresh client.
StatusOr<bool> WaitForMatching(uint64_t timeout_ms) const;
```

```cpp
auto client = aorta::Must(node->CreateClient("/echo"), "Create client");
if (!aorta::Must(client->WaitForMatching(2000))) {
  AORTA_LOG(WARN) << "no service provider matched within 2s";
}
```

> `MatchingStatus()` is a one-shot snapshot (local routing tables only, no network round-trip); `WaitForMatching()` polls until the timeout. Returning `false` on timeout means no provider matched within the window — it is not an error.

### 4.11 Full RPC Example

**Navigation service server (from `cpp/examples/navigation_service_server.cc`):**

Registering several typed services on one node:

```cpp
auto node = aorta::Must(
    aorta::Node::Create("navigation_server", "default"), "Create node");

// 1. GetPlan service
auto get_plan = aorta::Must(
    node->CreateService<nav::GetPlanRequest, nav::GetPlanResponse>(
    "/get_plan",
    [](const aorta::MessageView& request, aorta::ServiceRequest&& req) {
      auto payload = request.ToVector();
      flatbuffers::Verifier verifier(payload.data(), payload.size());
      if (!verifier.VerifyBuffer<nav::GetPlanRequest>(nullptr)) {
        // Return an error response
        auto error_response = BuildErrorResponse(...);
        req.Reply(std::move(error_response));
        return;
      }
      const auto* msg = flatbuffers::GetRoot<nav::GetPlanRequest>(payload.data());
      auto response = BuildGetPlanResponse(msg);
      aorta::CheckOk(req.Reply(std::move(response)), "Reply GetPlan response");
    }),
    "Create get_plan service");

// 2. CancelPlan service
auto cancel_plan = aorta::Must(
    node->CreateService<nav::CancelPlanRequest, nav::CancelPlanResponse>(
        "/cancel_plan", cancel_handler),
    "Create cancel_plan service");

// 3. GetCurrentPose service
auto current_pose = aorta::Must(
    node->CreateService<nav::GetCurrentPoseRequest, nav::GetCurrentPoseResponse>(
        "/get_current_pose", pose_handler),
    "Create get_current_pose service");
```

---

## 5. Timer

### 5.1 Overview

Timer provides periodic callback execution. Timer callbacks are driven by the executor, not by a dedicated thread.

### 5.2 Creating a Timer

```cpp
// Signature
StatusOr<std::shared_ptr<Timer>> CreateTimer(
    std::chrono::nanoseconds period,
    Timer::Callback callback);

StatusOr<std::shared_ptr<Timer>> CreateTimer(
    std::chrono::nanoseconds period,
    Timer::Callback callback,
    const TimerOptions& options);
```

```cpp
// Timer::Callback signature
using Callback = std::function<void()>;
```

```cpp
// Example (from cpp/examples/talker.cc)
auto counter = std::make_shared<uint64_t>(0);
auto timer = aorta::Must(node->CreateTimer(
    std::chrono::milliseconds(500),  // 500ms period
    [publisher, counter]() {
      aorta::CheckOk(publisher->PublishTyped<demo::Chat>(
          [&](demo::ChatMsg& msg) {
            msg.text = "hello " + std::to_string((*counter)++);
          }),
          "Publish typed chat");
    }),
    "Create timer");
```

### 5.3 TimerOptions

```cpp
struct TimerOptions {
  CallbackGroup callback_group = CallbackGroup::kMutuallyExclusive;
  CallbackGroupHandle callback_group_handle = 0;
  bool auto_start = true;           // Start ticking automatically after creation
  bool invoke_on_missed = false;    // Whether to make up for missed ticks
  TimerOverflowPolicy overflow_policy = TimerOverflowPolicy::kCoalesceLatest;
  uint64_t overflow_block_timeout_ms = 0;
};
```

| Field | Description |
|------|------|
| `auto_start` | Defaults to `true`; the timer starts ticking immediately after creation |
| `invoke_on_missed` | When `true`, makes up for missed tick periods |
| `overflow_policy` | `kCoalesceLatest` (coalesce to the latest tick) or `kBlock` (block) |

### 5.4 Timer Lifecycle

Timer control in the C++ SDK differs from the Rust SDK:

- **Creation**: `Node::CreateTimer()` creates the timer and adds it to the executor's event queue
- **Stopping**: the timer's lifetime is managed by RAII destruction — the C++ public API exposes no `Stop()`; releasing the `shared_ptr` stops the timer
- **Observability**: besides destruction, Timer exposes a set of read-only statistics accessors (see "5.4.1 Timer Statistics" below)
- **Pause/resume**: `Timer::Pause()` and `Timer::Resume()` pause and resume ticking; a paused timer keeps its handle and its statistics. They map onto the FFI's `aorta_timer_cancel()` / `aorta_timer_reset()` (historical misnomers — the timer is not destroyed). `Resume()` is also how you first start a timer created with `auto_start = false`
- **Overflow policy**: queue overflow behavior is controlled by `TimerOptions::overflow_policy` (`kCoalesceLatest` coalesces, `kBlock` blocks)

```cpp
// How to stop a timer: release the shared_ptr
{
  auto timer = aorta::Must(
      node->CreateTimer(std::chrono::milliseconds(100), callback),
      "Create timer");
  // timer is alive within this scope
}  // shared_ptr destroyed -> timer stops automatically

// Or clear it explicitly
timer.reset();  // Stop the timer

// Temporary suspension without destroying the timer
aorta::CheckOk(timer->Pause(), "Timer::Pause");
aorta::CheckOk(timer->Resume(), "Timer::Resume");
```

> **Compared with the Rust SDK**: the Rust SDK's Timer exposes the full `stop()` / `pause()` / `resume()` / `is_running()` / `period()` surface; the C++ SDK exposes `Pause()` / `Resume()` but no `Stop()` — stopping is RAII destruction — plus the statistics accessors below for observability.

#### 5.4.1 Timer Statistics

Timer exposes 13 read-only statistics accessors, all returning `StatusOr<uint64_t>`, for diagnosing coalesce / overflow / deadline-slip behavior:

| Accessor | Meaning |
|--------|------|
| `CoalescedTickCount()` | Number of ticks that were coalesced away |
| `OverflowTimeoutCount()` | Number of wait timeouts under the `kBlock` overflow policy |
| `DeadlineSlipTotalNs()` / `DeadlineSlipMaxNs()` | Cumulative / peak deadline slip (nanoseconds) |
| `DueFiringsTotal()` / `DueFiringsMax()` / `DueSampleCount()` | Cumulative / peak / sample count of firings due per pass |
| `CoalesceRequeueCount()` | Number of coalesce requeues |
| `CoalesceInlineDispatchTotal()` / `CoalesceInlineDispatchMax()` | Cumulative / peak inline dispatches |
| `CoalesceEventWaitTotalNs()` / `CoalesceEventWaitMaxNs()` / `CoalesceEventWaitSampleCount()` | Cumulative / peak (nanoseconds) / sample count of coalesce event waits |

```cpp
auto coalesced = aorta::Must(timer->CoalescedTickCount());
auto slip_max = aorta::Must(timer->DeadlineSlipMaxNs());
AORTA_LOG(INFO) << "coalesced=" << coalesced << " slip_max_ns=" << slip_max;
```

### 5.5 Timer + Callback Group

Bind a timer to a specific callback group to control its concurrency behavior:

```cpp
// Create a callback group
auto cb_group = aorta::Must(
    executor->CreateCallbackGroup(aorta::CallbackGroup::kMutuallyExclusive),
    "Create callback group");

// Bind the timer to the callback group
aorta::TimerOptions timer_opts;
timer_opts.callback_group_handle = cb_group;

auto timer = aorta::Must(
    node->CreateTimer(std::chrono::milliseconds(100), my_callback, timer_opts),
    "Create timer");
```

---

## 6. Executor

### 6.1 Overview

The executor is Aorta's **callback scheduling engine**; it drives the execution of Subscriber, Service and Timer callbacks. Key features:

- **Execution models**: SingleThreaded / MultiThreaded (**only these two are separate implementations**; `kAsync` reuses the multi-threaded backend and `kCurrentThread` is unimplemented, see §6.3)
- **Two scheduling policies**: FIFO / Priority
- **Callback group**: controls the concurrency relationship between callbacks
- **Admission control**: queue length / byte limits that prevent resource overflow

### 6.2 Creating an Executor

```cpp
// Simple creation (defaults to SingleThreaded + FIFO)
auto executor = aorta::Must(aorta::Executor::Create(), "Create executor");

// Creation with a type
auto executor = aorta::Must(
    aorta::Executor::Create(aorta::ExecutorType::kMultiThreaded),
    "Create executor");

// Full options
aorta::ExecutorOptions opts;
opts.type = aorta::ExecutorType::kMultiThreaded;
opts.scheduler_mode = aorta::SchedulerMode::kPriority;
auto executor = aorta::Must(aorta::Executor::Create(opts), "Create executor");
```

### 6.3 ExecutorType

```cpp
enum class ExecutorType {
  kSingleThreaded,   // Single-threaded (default); all callbacks run in order
  kMultiThreaded,    // Multi-threaded; non-mutually-exclusive callbacks run in parallel
  kCurrentThread,    // ⚠️ Unimplemented: Executor::Create() returns AORTA_ERR_INVALID_ARGUMENT
  kAsync,            // ⚠️ Not a separate event loop — it reuses the multi-threaded executor,
                     //    with in-flight callbacks capped by AORTA_ASYNC_EXECUTOR_MAX_IN_FLIGHT (default 32)
};
```

> **⚠️ Implementation status**: only `kSingleThreaded` and `kMultiThreaded` are separately
> implemented execution backends. Choosing `kCurrentThread` fails at construction (the
> C-ABI returns `AORTA_ERR_INVALID_ARGUMENT` uniformly for unsupported types); `kAsync` is
> a compatibility backend — semantically equivalent to `kMultiThreaded` plus a bounded
> in-flight cap, not a separate async/event-loop model.
> The Python SDK therefore deliberately does not expose `CURRENT` (see `python/aorta/executor.py`).

### 6.4 Adding a Node

An executor is associated with a node via `AddNode()`, and then takes over callback scheduling for every primitive under that node:

```cpp
auto status = executor->AddNode(node);
if (!status.ok()) {
  AORTA_LOG(ERROR) << "Failed to add node: " << status.message();
}
```

If you do not explicitly add the node to an executor before `CreateSubscriber()` / `CreateService()` / `CreateTimer()`, the `Node` automatically creates an internal `SingleThreadedExecutor` as a convenience fallback. Production code that needs an explicit threading model, scheduling policy or admission observability should create the executor first and attach the node to it.

### 6.5 Spin / SpinOnce / Stop

```cpp
// Blocks until Stop() is called
Status Spin();

// Executes callbacks once, returning after the timeout
Status SpinOnce(std::chrono::milliseconds timeout);

// Stop the executor
Status Stop();
```

To stop the executor from an ordinary thread context such as a timer callback, use `GetStopHandle()`:

```cpp
auto stop_handle = aorta::Must(executor->GetStopHandle(), "Get stop handle");
stop_handle.Stop();
```

> **`StopHandle::Stop()` is neither lock-free nor async-signal-safe**. On the Rust side it first takes a brief registry mutex (for the lookup) and then triggers the underlying atomic notification (a deliberate design change that eliminated a double-free UAF on the earlier raw-pointer handle). Therefore:
> - it may be called from any ordinary thread context, including a timer callback running inside `Spin()`;
> - it **must never** be called from a signal handler — write to a self-pipe / signalfd in the handler and call `Stop()` from an ordinary thread (e.g. a `sigwait` thread).
>
> (Older versions documented `Stop()` as lock-free and signal-safe; that was wrong and has been corrected.)

**Spin pattern (from `cpp/examples/talker.cc`):**

```cpp
// Call Stop() from the signal-handling thread
std::thread signal_thread([executor, signal_set]() mutable {
  int sig = 0;
  sigwait(&signal_set, &sig);
  aorta::CheckOk(executor->Stop(), "Executor::Stop");
});

// The main thread blocks in Spin
aorta::CheckOk(executor->Spin(), "Executor::Spin");
signal_thread.join();
```

**SpinOnce pattern (from `cpp/examples/listener.cc`):**

```cpp
while (g_running) {
  aorta::CheckOk(executor->SpinOnce(std::chrono::milliseconds(100)),
                 "Executor::SpinOnce");
}
```

### 6.6 Callback Group / ExecutionGroup

Controls concurrency constraints between callbacks:

```cpp
enum class CallbackGroup {
  kMutuallyExclusive,  // Mutually exclusive group: callbacks in the group never run concurrently
  kReentrant,          // Reentrant group: callbacks in the group may run concurrently
};
```

```cpp
// Create a callback group
auto group_handle = executor->CreateCallbackGroup(
    aorta::CallbackGroup::kMutuallyExclusive);
auto callback_group = aorta::Must(std::move(group_handle), "Create callback group");

// Free the callback group
executor->FreeCallbackGroup(callback_group);
```

Binding a subscriber or timer to a callback group:

```cpp
aorta::SubscriberOptions sub_opts;
sub_opts.callback_group = aorta::CallbackGroup::kMutuallyExclusive;
sub_opts.callback_group_handle = callback_group;

auto sub = node->CreateSubscriber("/topic", callback, sub_opts);
```

The preferred SDK-level entry point is `Node::CreateExecutionGroup()`, which provides RAII-style lifetime management:

```cpp
auto exec_group = aorta::Must(
    node->CreateExecutionGroup(aorta::ExecutionPolicy::kSerialized),
    "Create execution group");

aorta::TimerOptions timer_opts;
timer_opts.callback_group_handle = exec_group.handle();
auto timer = node->CreateTimer(std::chrono::milliseconds(10), callback, timer_opts);
```

`ExecutionGroup` also provides convenience `Bind()` overloads (one each for `SubscriberOptions` / `ServiceOptions` / `TimerOptions`) that set **both** `callback_group` (derived from the group's `ExecutionPolicy`) and `callback_group_handle`; assigning `handle()` manually as above sets only the handle field:

```cpp
// Recommended: Bind() sets both the group and the handle in one step
auto timer = node->CreateTimer(
    std::chrono::milliseconds(10), callback,
    exec_group.Bind(aorta::TimerOptions()));

auto sub = node->CreateSubscriber(
    "/topic", callback, exec_group.Bind(aorta::SubscriberOptions()));
```

`ExecutionGroup` accessors and release:

| Member | Description |
|------|------|
| `valid()` | Whether it holds a valid group (handle is non-zero) |
| `policy()` | The group's `ExecutionPolicy` |
| `handle()` | The underlying `CallbackGroupHandle` |
| `Status Release()` | Explicit release (idempotent with the destructor; may be called repeatedly) |

### 6.7 SchedulerMode

```cpp
enum class SchedulerMode {
  kFifo,      // First in, first out (default)
  kPriority,  // Priority scheduling
};
```

Priority scheduling options:

```cpp
struct PrioritySchedulerOptions {
  uint32_t max_drain_per_cycle = 32;          // Max items processed per cycle
  std::array<uint8_t, 5> weights{{8, 4, 2, 1, 1}};  // Weights for the 5 priority levels
  bool map_subscription_qos = true;           // Map subscription QoS to priority
};
```

The weight array corresponds to the 5 priority levels: Realtime(8) > High(4) > Normal(2) > Low(1) > Background(1).

### 6.8 Admission Control

Prevents unbounded growth of the callback queue:

```cpp
struct ExecutorAdmissionOptions {
  std::size_t max_queued_events = 0;  // Max queued events (0 = unlimited)
  std::size_t max_queued_bytes = 0;   // Max queued bytes (0 = unlimited)
};
```

```cpp
aorta::ExecutorOptions opts;
opts.admission.max_queued_events = 10000;
opts.admission.max_queued_bytes = 64 * 1024 * 1024;  // 64MB
auto executor = aorta::Must(aorta::Executor::Create(opts), "Create executor");
```

Querying admission statistics:

```cpp
struct ExecutorAdmissionStats {
  uint64_t queued_events = 0;
  uint64_t queued_bytes = 0;
  uint64_t peak_queued_events = 0;
  uint64_t peak_queued_bytes = 0;
  uint64_t dropped_queue_full = 0;
  uint64_t dropped_queue_closed = 0;
  uint64_t dropped_budget_events = 0;
  uint64_t dropped_budget_bytes = 0;
};

auto stats = aorta::Must(executor->GetAdmissionStats(), "Get admission stats");
```

### 6.9 ExecutorClassPolicyOptions

Configure overflow policy per primitive class:

```cpp
struct ExecutorClassPolicyOptions {
  // Subscriber overflow
  SubscriberOverflowPolicy sub_overflow_policy = SubscriberOverflowPolicy::kDrop;
  std::size_t sub_overflow_relay_capacity = 1024;

  // RPC overflow
  RpcOverflowPolicy rpc_overflow_policy = RpcOverflowPolicy::kFastFailBusy;
  uint64_t rpc_overflow_block_timeout_ms = 0;

  // Timer overflow
  TimerOverflowPolicy timer_overflow_policy = TimerOverflowPolicy::kCoalesceLatest;
  uint64_t timer_overflow_block_timeout_ms = 0;
};
```

---

## 7. QoS

> The QoS dimensions, the built-in profiles (`realtime_control` / `sensor_data` /
> `state_update` / `bulk_transfer`) and their → Executor priority mapping are
> semantics shared across SDKs; the specification is
> [`spec/Aorta_SDK_Common_Concepts.md` §5](../spec/Aorta_SDK_Common_Concepts.md#5-qos-model).
> This section gives the C++ `aorta::QoS` struct and its usage.

### 7.1 Overview

After the QoS & buffering redesign, `aorta::QoS` has converged to **4 fields that are genuinely wired to Zenoh** — there are no silent no-ops left:

| Field | Values | Description |
|------|--------|------|
| **Priority** | Realtime / High / Normal / Low / Background | Transport priority |
| **Reliability** | BestEffort / Reliable | Transport reliability |
| **CongestionControl** | kDrop (default) / kBlock | Behavior when the local transport egress queue is congested. `kBlock` applies backpressure to the publishing thread instead of dropping locally on congestion; it does **not** promise end-to-end lossless or exactly-once delivery. `kDrop` is the lowest-latency lossy mode |
| **Express** | true / false (default false) | When true, bypasses Zenoh's internal batching (express mode), trading throughput for lower latency |

> Removed fields: `durability` (together with the `Durability`/`kTransientLocal`/`kVolatile` enums) and `history_depth` on QoS — they were never forwarded to Zenoh (`kTransientLocal` was an unimplemented no-op), and removing them changes no runtime behavior.
>
> - **Need "get the last value after reconnecting / a late-joiner snapshot"** → that is no longer QoS's job; use [`ContextStore`](#15-contextstore) (latched / last-value / late-joiner semantics all belong to ContextStore).
> - **Need per-topic receive depth / isolation** → use `SubscriberOptions::receive_depth` (see [§3.9](#39-per-topic-subscription-buffering-receive_depth--pull-subscriptions)).

### 7.2 The QoS Struct

```cpp
struct QoS {
  Priority priority = Priority::kNormal;
  Reliability reliability = Reliability::kReliable;
  CongestionControl congestion_control = CongestionControl::kDrop;
  bool express = false;  // true bypasses Zenoh batching: lower latency, less throughput
};
```

### 7.3 Preset QoS Profiles

The SDK provides 5 scenario-oriented presets:

```cpp
// Default: Normal priority, Reliable, Drop, non-express
QoS()
// = {kNormal, kReliable, kDrop, express=false}

// Realtime control: Realtime (highest) priority, Reliable, Drop, express (no preset
// defaults to Block; to avoid local drops under congestion, set congestion_control =
// kBlock explicitly and knowingly accept that publish() may block)
QoS::RealtimeControl()
// = {kRealtime, kReliable, kDrop, express=true}

// Sensor data: Normal priority, best-effort delivery, Drop (latest value wins), non-express
QoS::SensorData()
// = {kNormal, kBestEffort, kDrop, express=false}

// State updates: High priority, Reliable, Drop, non-express
// Note: this no longer implies any latched/last-value replay — use ContextStore for
// late-joiner snapshots
// If a state stream must never drop at the local egress queue, set congestion_control = kBlock explicitly
QoS::StateUpdate()
// = {kHigh, kReliable, kDrop, express=false}

// Bulk transfer: Background priority, Reliable, Drop, non-express (throughput optimised)
// To avoid local drops under congestion, set congestion_control = kBlock explicitly
QoS::BulkTransfer()
// = {kBackground, kReliable, kDrop, express=false}
```

### 7.4 Usage Examples

```cpp
// Specify QoS when creating a Publisher
auto pub = node->CreatePublisher<demo::Chat>(
    "/control/cmd", aorta::QoS::RealtimeControl());
auto control_pub = aorta::Must(std::move(pub), "Create control publisher");

// Specify QoS when creating a Subscriber
auto sub = node->CreateSubscriber(
    "/sensor/lidar", callback, aorta::QoS::SensorData());
auto lidar_sub = aorta::Must(std::move(sub), "Create lidar subscriber");

// Custom QoS: a command stream that must not drop locally (Block congestion
// control — backpressure rather than dropping)
aorta::QoS custom;
custom.priority = aorta::Priority::kHigh;
custom.reliability = aorta::Reliability::kReliable;
custom.congestion_control = aorta::CongestionControl::kBlock;
auto custom_pub = aorta::Must(
    node->CreatePublisher<MyMsg>("/topic", custom), "Create custom publisher");
```

> Want late joiners to get the last value of some state topic? Publish the state through [`ContextStore`](#15-contextstore) instead of relying on QoS.

### 7.5 Mapping QoS to Executor Priority

When the Executor uses `SchedulerMode::kPriority` and `map_subscription_qos = true`, a Subscriber's QoS Priority is automatically mapped onto the Executor's scheduling priority:

| QoS Priority | Executor Priority | Default weight |
|-------------|-------------------|---------|
| Realtime | Level 0 (highest) | 8 |
| High | Level 1 | 4 |
| Normal | Level 2 | 2 |
| Low | Level 3 | 1 |
| Background | Level 4 | 1 |

---

## 8. Schema

> The schema model (FlatBuffers + `AortaHeader`, the SHA-256 schema hash,
> `SchemaAttachPolicy`, discovery/validation, conflict handling) is semantics
> shared across SDKs; the specification is
> [`spec/Aorta_SDK_Common_Concepts.md` §4](../spec/Aorta_SDK_Common_Concepts.md#4-schema-model-flatbuffers--aortaheader).
> This section gives the C++ bindings: `SchemaMeta` / `TypedPublishTraits` and friends.

### 8.1 Overview

Aorta uses **FlatBuffers** as its message serialization framework, combined with **Registry-Driven Codegen**, to provide:

- Zero-copy deserialization (the receiver reads memory directly)
- Schema-first development (define the `.fbs` first, then generate code)
- Schema discovery and validation (SHA-256 hash check)
- Automatic AortaHeader injection

### 8.2 Defining a FlatBuffers Schema

Every message schema must include an `aorta_header` field in its root table:

```flatbuffers
// demo_chat.fbs
include "schemas/system/aorta_base.fbs";

namespace demo;

table Chat {
  aorta_header: aorta.sys.AortaHeader;  // required field
  text: string;
}

root_type Chat;
```

`aorta_base.fbs` defines the shared AortaHeader:

```flatbuffers
namespace aorta.sys;

table AortaHeader {
  publish_stamp_ns: ulong;  // Publish timestamp in nanoseconds.
  publisher_node: string;   // Publisher node name.
  sequence: ulong;          // Per-topic sequence number.
}
```

### 8.3 The Code-Generation Pipeline

Aorta's code generation consists of two tools:

1. **`gen_message_wrapper.py`**: generates the `*_msg.h` message wrapper header
   - Defines the mutable `XXXMsg` struct (used by the `PublishTyped<T>` fill lambda)
   - Defines the `TypedPublishTraits<T>` specialization (the Build method)
   - Includes the FlatBuffers-generated `*_generated.h`

2. **`gen_schema_meta.py`**: generates the `SchemaMeta<T>` specialization
   - `SchemaBfbs()`: the embedded schema binary reflection data
   - `SchemaBfbsSize()`: the binary size
   - `FixedSize()`: byte count for fixed-size messages (optional)
   - `ShmCapacityHint()`: publisher preallocation capacity hint (what is generated today defaults to the same value as `FixedSize()`, reserved for a looser hint later)
   - `RootTypeName()`: the root type name string
   - `SchemaHashHex()`: the SHA-256 hash (64 hex characters)

### 8.4 The SchemaMeta Template

```cpp
// schema_meta.h - base template (returns empty values)
template <typename T>
struct SchemaMeta {
  static const uint8_t* SchemaBfbs() { return nullptr; }
  static std::size_t SchemaBfbsSize() { return 0; }
  static std::optional<std::size_t> FixedSize() { return std::nullopt; }
  static std::optional<std::size_t> ShmCapacityHint() { return std::nullopt; }
  static const char* SchemaHashHex() { return nullptr; }
  static const char* RootTypeName() { return nullptr; }
};

// Example of a code-generated specialization:
template <>
struct SchemaMeta<demo::Chat> {
  static const uint8_t* SchemaBfbs() { return kChatBfbs; }
  static std::size_t SchemaBfbsSize() { return sizeof(kChatBfbs); }
  static std::optional<std::size_t> FixedSize() { return std::nullopt; }
  static std::optional<std::size_t> ShmCapacityHint() { return std::nullopt; }
  static const char* RootTypeName() { return "demo.Chat"; }
  static const char* SchemaHashHex() { return "abcd1234..."; }
};
```

### 8.5 The TypedPublishTraits Template

```cpp
// typed_publish.h - base template
template <typename T>
struct TypedPublishTraits;

// Example of a code-generated specialization:
template <>
struct TypedPublishTraits<demo::Chat> {
  static constexpr bool kHasAortaHeader = true;
  using Msg = demo::ChatMsg;

  static flatbuffers::Offset<demo::Chat> Build(
      flatbuffers::FlatBufferBuilder& builder,
      flatbuffers::Offset<aorta::sys::AortaHeader> header,
      const Msg& msg) {
    // Serialize ChatMsg into the builder
    auto text = builder.CreateString(msg.text);
    return demo::CreateChat(builder, header, text);
  }
};
```

### 8.6 Schema Discovery and Validation

When a Publisher/Subscriber is created, the SDK automatically takes part in schema discovery:

- **Publisher**: registers a Queryable that answers `schema_by_key` queries
- **Subscriber**: if `prefetch_schema_on_init = true` (the default), actively queries the remote schema at initialization
- **Hash validation**: compares the 64-character hex string of the SHA-256; on a conflict it can warn or reject

### 8.7 SchemaAttachPolicy

Controls whether the schema hash is attached to each message's transport metadata:

```cpp
enum class SchemaAttachPolicy {
  kInherit,  // inherit the global default policy
  kAlways,   // attach the schema hash to every message
  kNever,    // never attach
};
```

Usage example (from `cpp/examples/echo_server.cc`):

```cpp
aorta::ServiceOptions options;
options.schema_attach_policy = aorta::SchemaAttachPolicy::kNever;
```

---

## 9. Logging

### 9.1 Overview

Aorta provides a unified logging system built on the Rust `tracing` ecosystem:

- 5 log levels: Trace / Debug / Info / Warn / Error
- 2 output formats: Text / JSON
- Supports file output plus stderr as two channels
- Configured via environment variables
- A macro-style interface with conditional logging and rate limiting

### 9.2 Initialization

Log-topic forwarding is process-wide but owned by the Node that acquired the
forwarder slot. Closing that Node cancels and joins its forwarding worker before
closing telemetry and the Zenoh session; closing an unrelated Node does not stop
the forwarder. A later Node initialization can acquire the released slot. This
does not add automatic failover to already-existing Nodes or guarantee delivery
of queued best-effort logs during shutdown. The log topic remains enabled by
default; disabling it is not a lifecycle workaround.

The internal forwarding Publisher is excluded from the owner's user-facing
inventory and runtime-metrics telemetry. User-created Publishers, including
those named `/log`, remain counted normally. Log publication and schema
registration are unaffected.

The service-owner advisory query is also owned by its Service. Stopping a
Service withdraws its claim and cancels the query; deterministic C-ABI service
shutdown joins both the advisory query and the serving task within the existing
shared shutdown budget, before the owning Node can close its session.

```cpp
// Initialize from environment variables (recommended, simple)
Status InitLoggingFromEnv();

// Programmatic configuration
Status ConfigureLogging(const LogConfig& config);
```

```cpp
struct LogConfig {
  std::string filter;      // filter expression (e.g. "aorta=debug,zenoh=warn")
  std::string file_path;   // log file path
  LogFormat format = LogFormat::kText;  // Text or JSON
  bool stderr_enabled = true;
};
```

Environment variables:

| Variable | Description | Example |
|------|------|------|
| `AORTA_LOG` | Filter level | `debug`, `info`, `aorta_sdk=trace` |
| `AORTA_LOG_FILE` | Log file path | `/tmp/aorta.log` |
| `AORTA_LOG_FORMAT` | Output format | `text`, `json` |

### 9.3 Setting the Default Log Context

```cpp
Status SetDefaultLogContext(const std::string& node_name,
                            const std::string& group);
```

```cpp
// Typical usage: set it right after creating the Node
(void)aorta::SetDefaultLogContext(node->name(), node->group());
```

### 9.4 Logging Macros

#### Basic Logging

```cpp
AORTA_LOG(INFO) << "Node started: " << node->name();
AORTA_LOG(WARN) << "Publish failed: " << status.message();
AORTA_LOG(ERROR) << "Fatal error";
AORTA_LOG(DEBUG) << "Debug info: count=" << count;
AORTA_LOG(TRACE) << "Trace detail";
```

#### Custom Scope

```cpp
AORTA_LOG_SCOPE(INFO, "my_module") << "Module-specific log";
```

#### Conditional Logging

```cpp
AORTA_LOG_IF(WARN, count > 100) << "Count exceeds threshold";
AORTA_LOG_SCOPE_IF(DEBUG, "perf", latency > 10) << "High latency";
```

#### Rate Limiting (Every N)

```cpp
AORTA_LOG_EVERY_N(INFO, 100) << "Status update (every 100th)";
AORTA_LOG_SCOPE_EVERY_N(DEBUG, "stats", 1000) << "Periodic stats";
```

### 9.5 Level Checks

```cpp
bool IsLogEnabled(LogLevel level, const std::string& scope = "aorta_sdk");

if (aorta::IsLogEnabled(aorta::LogLevel::kDebug)) {
  // Build the expensive debug information
  AORTA_LOG(DEBUG) << expensive_debug_string();
}
```

### 9.6 Flush

```cpp
Status FlushLogs();  // force-flush all buffered logs
```

---

## 10. Time: the Unified Time Source

> The clock modes (System / Steady / Simulation) and the nanosecond semantics are
> shared across SDKs; the specification is
> [`spec/Aorta_SDK_Common_Concepts.md` §6](../spec/Aorta_SDK_Common_Concepts.md#6-time--clock-model).
> This section gives the C++ `Node::Now()` / `NowNs()` bindings.

### 10.1 Overview

Aorta's time source is unified **per process**; for the meaning of the underlying
clock modes (System / Steady / Simulation) see [shared concepts §6](../spec/Aorta_SDK_Common_Concepts.md#6-time--clock-model).

On time, the C++ SDK provides **read** interfaces only — `aorta::NowNs()` (global) and
`Node::Now()` (node-scoped), both returning `StatusOr<int64_t>` valued in nanoseconds.
**The C++ SDK exposes no API for switching the clock mode or advancing Simulation time**;
the process clock mode is set by Core / the test side.

### 10.2 Getting Time from a Node

```cpp
StatusOr<int64_t> Node::Now() const;
```

```cpp
auto now_result = node->Now();
if (now_result.ok()) {
  int64_t now_ns = now_result.value();
  AORTA_LOG(INFO) << "Current time: " << now_ns << " ns";
}
```

### 10.3 Global Time API

Use this when no Node instance is available:

```cpp
StatusOr<int64_t> NowNs();
```

```cpp
// Example (from cpp/examples/navigation_service_server.cc)
auto now_or = aorta::NowNs();
uint64_t now_ns = 0;
if (now_or.ok() && now_or.value() > 0) {
  now_ns = static_cast<uint64_t>(now_or.value());
}
```

### 10.4 Time and AortaHeader

On publish, the Publisher automatically calls `Node::Now()` to obtain a timestamp and writes it into the AortaHeader's `publish_stamp_ns` field. A Subscriber can read the publish timestamp by parsing the AortaHeader.

---

## 11. Advanced Features

### 11.1 Publisher Builder Reuse (Internal)

The C++ SDK's current builder-reuse strategy is an internal implementation detail and is not exposed through the stable `PublisherOptions`. The implementation prefers the thread-local / staged fast path by default, with the buffers managed by a **recycle pool**; that pool is capped at **4×** `base_capacity` (which comes from `max_message_size`, defaulting to 4 KB when unset), and buffers above the cap are discarded rather than growing without bound, so no thread holds on to an oversized builder buffer.

> Note: the environment variable `AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB` applies only to
> the **Rust SDK** publisher (`rust/src/publisher.rs`); the C++ SDK does not read it (see
> [`spec/Aorta_Environment_Variables_Reference.md` §8](../spec/Aorta_Environment_Variables_Reference.md#8-publisher-header--payload--per-sdk-publisher-layer)).

The stable, user-tunable options are:

- `PublisherOptions::max_message_size`
- `PublisherOptions::schema_attach_policy`
- `PublisherOptions::schema_bfbs`
- `PublisherOptions::schema_root_type`
- `PublisherOptions::congestion_control` (per-publisher override of `qos.congestion_control`; unset inherits the QoS value)

### 11.2 Publisher Oversize Detection

When a message exceeds `max_message_size`, the SDK automatically records oversize statistics and emits a warning log:

```cpp
// Set the initial capacity through PublisherOptions
aorta::PublisherOptions opts;
opts.max_message_size = 4096;  // 4KB expected
auto pub = aorta::Must(
    node->CreatePublisher<MyMsg>("/topic", aorta::QoS(), opts),
    "Create publisher");

// Runtime check
auto stats = pub->GetStats();
if (stats.oversize_count > 0) {
  AORTA_LOG(WARN) << "Oversize messages: " << stats.oversize_count
                  << " max_seen=" << stats.max_capacity_seen;
}
```

### 11.3 Subscriber Deduplication and Out-of-Order Detection

#### Deduplication (Dedup)

A sliding-window deduplication keyed on `{publisher_id, sequence}`, preventing duplicate messages under multi-path transport or retry scenarios:

```cpp
aorta::SubscriberOptions opts;
opts.enable_dedup = true;
opts.dedup_window = 2048;  // window size
```

#### Out-of-Order Detection (Reorder Check)

Detects whether the sequence increases monotonically:

```cpp
opts.enable_reorder_check = true;
opts.drop_out_of_order = true;  // drop out-of-order messages
// or
opts.drop_out_of_order = false; // warn only, do not drop
```

### 11.4 Subscriber Overflow Policy

The overflow policy for when the Executor cannot keep up with incoming messages:

```cpp
enum class SubscriberOverflowPolicy {
  kDrop,   // drop the newly arrived message (default)
  kRelay,  // buffer into a relay queue
};
```

```cpp
aorta::SubscriberOptions opts;
opts.overflow_policy = aorta::SubscriberOverflowPolicy::kRelay;
opts.overflow_relay_capacity = 4096;  // relay queue capacity
```

### 11.5 RPC Overflow Policy

```cpp
enum class RpcOverflowPolicy {
  kFastFailBusy,  // fail fast with a Busy error (default)
  kBlock,         // block and wait (with a timeout)
};
```

### 11.6 Timer Overflow Policy

```cpp
enum class TimerOverflowPolicy {
  kCoalesceLatest,  // coalesce into a single latest firing (default)
  kBlock,           // block and wait
};
```

### 11.7 Multi-Node Executor Pattern

One Executor can drive multiple Nodes:

```cpp
auto node_a = aorta::Must(aorta::Node::Create("node_a"), "Create node_a");
auto node_b = aorta::Must(aorta::Node::Create("node_b"), "Create node_b");

auto executor = aorta::Must(aorta::Executor::Create(), "Create executor");
aorta::CheckOk(executor->AddNode(node_a), "Executor::AddNode(node_a)");
aorta::CheckOk(executor->AddNode(node_b), "Executor::AddNode(node_b)");
aorta::CheckOk(executor->Spin(), "Executor::Spin");
```

### 11.8 Schema Hash as RPC Metadata

A Client can declare `service_schema_hash_hex` to record the service schema
hash it expects; the hash travels on the wire only when the effective schema
attach policy resolves to `Always`:

```cpp
// Get the hash from SchemaMeta
const char* hash = aorta::SchemaMeta<demo::EchoRequest>::SchemaHashHex();

aorta::ClientOptions opts;
opts.service_schema_hash_hex = hash;
auto client = aorta::Must(node->CreateClient("/echo", opts), "Create client");
```

This is a **diagnostic** facility, not version negotiation: a schema mismatch
is at most logged — no request is rejected and no call fails because of it
(there is no enforced-validation switch on the C++ `ClientOptions`). Do not
rely on it to guarantee the client is talking to the right service version.

### 11.9 Single-Provider Mode

```cpp
aorta::ClientOptions opts;
opts.enforce_single_provider = true;         // allow exactly one service provider
opts.multi_reply_check_window_ms = 100;      // multi-reply detection window
auto client = aorta::Must(node->CreateClient("/service", opts), "Create client");
```

When `enforce_single_provider = true`:

- The request is still sent to **all** matching providers; enforcement is
  **bounded post-hoc multiple-reply detection**, not a routing restriction
- After the first reply arrives, the Client listens for the
  `multi_reply_check_window_ms` window; a second provider's reply landing
  inside it fails the call with a conflict
- This cannot guarantee exactly-once execution: multiple providers may each
  have executed the request's side effects, and a reply arriving after the
  window escapes detection. For non-idempotent services, guarantee a single
  provider through deployment topology — do not rely on this option for it

---

## 12. Worked Examples

### 12.1 Talker (Timer-Driven Publisher)

**File:** `cpp/examples/talker.cc`

This example demonstrates the full Executor + Timer + Publisher interplay:

```cpp
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <memory>
#include <pthread.h>
#include <string>
#include <thread>

#include "aorta/aorta.h"
#include "demo_chat_msg.h"

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
    // (1) Logging initialization
    (void)aorta::InitLoggingFromEnv();

    // (2) Block SIGINT/SIGTERM before any thread exists (sigwait pattern)
    sigset_t signal_set;
    sigemptyset(&signal_set);
    sigaddset(&signal_set, SIGINT);
    sigaddset(&signal_set, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signal_set, nullptr) != 0) {
      AORTA_LOG(ERROR) << "Failed to block signal mask";
      return 1;
    }

    // (--name/--topic command-line parsing elided; see the file)
    std::string node_name = "cpp_talker";
    std::string topic = "/demo/chatter";
    (void)aorta::SetDefaultLogContext(node_name, "default");

    // (3) Create the Node
    auto node = aorta::Must(aorta::Node::Create(node_name, "default"),
                            "Create node");

    // (4) Create a typed Publisher
    auto publisher = aorta::Must(node->CreatePublisher<demo::Chat>(topic),
                                 "Create publisher");

    // (5) Create the Executor + Callback Group
    auto executor = aorta::Must(aorta::Executor::Create(), "Create executor");
    aorta::CheckOk(executor->AddNode(node), "Executor::AddNode");
    auto cb_group = aorta::Must(
        executor->CreateCallbackGroup(aorta::CallbackGroup::kMutuallyExclusive),
        "Create callback group");

    // (6) Create the Timer (500 ms period)
    aorta::TimerOptions timer_options;
    timer_options.callback_group_handle = cb_group;

    auto counter = std::make_shared<uint64_t>(0);
    auto timer = aorta::Must(node->CreateTimer(
        std::chrono::milliseconds(500),
        [publisher, counter, node_name = node->name()]() {
          const uint64_t current = (*counter)++;
          char payload[256];
          std::snprintf(payload, sizeof(payload), "hello %" PRIu64 " from %s",
                        current, node_name.c_str());
          aorta::CheckOk(
              publisher->PublishTyped<demo::Chat>(
                  [&](demo::ChatMsg& msg) { msg.text = std::string(payload); }),
              "Publish typed chat");
        },
        timer_options),
        "Create timer");
    (void)timer;

    // (7) Signal thread: stop the Executor on SIGINT/SIGTERM
    std::thread signal_thread([executor, signal_set]() mutable {
      int signal_number = 0;
      if (sigwait(&signal_set, &signal_number) == 0) {
        aorta::CheckOk(executor->Stop(), "Executor::Stop");
      }
    });

    // (8) Main thread blocks in Spin
    auto spin_status = executor->Spin();
    if (!spin_status.ok()) {
      AORTA_LOG(ERROR) << "Executor spin failed: " << spin_status.message();
    }
    if (signal_thread.joinable()) {
      signal_thread.join();
    }
    return spin_status.ok() ? 0 : 1;
  });
}
```

**Architecture flow:**

```
main() → RunOrFatal → Node::Create() → CreatePublisher<Chat>()
       → Executor::Create() → AddNode() → CreateCallbackGroup()
       → CreateTimer(500ms, callback)
       → signal_thread: sigwait → executor->Stop()
       → executor->Spin() [blocking]
           ↓
       Timer fires every 500ms → PublishTyped<Chat>()
           ↓
       [FlatBuffer Build + AortaHeader inject + Zenoh put]
```

### 12.2 Listener (Subscriber)

**File:** `cpp/examples/listener.cc`

This example demonstrates the Subscriber + SpinOnce loop pattern:

```cpp
#include <atomic>
#include <chrono>
#include <csignal>
#include <string>
#include <vector>

#include "aorta/aorta.h"
#include "demo_chat_msg.h"

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    (void)aorta::InitLoggingFromEnv();

    auto node = aorta::Must(
        aorta::Node::Create("cpp_listener", "default"), "Create node");

    // Attach BEFORE creating any callback-capable primitive: the first
    // executor attached to a node owns it, so a subscriber created first
    // would bind to the SDK auto-executor and this one would spin without
    // ever receiving an event.
    auto executor = aorta::Must(aorta::Executor::Create(), "Create executor");
    aorta::CheckOk(executor->AddNode(node), "Executor::AddNode");

    // Configure advanced Subscriber options
    aorta::SubscriberOptions sub_options;
    sub_options.enable_header_context = true;   // parse the header
    sub_options.enable_dedup = true;            // deduplication
    sub_options.enable_reorder_check = true;    // out-of-order detection
    sub_options.drop_out_of_order = true;       // drop out-of-order messages

    auto subscriber = aorta::Must(node->CreateSubscriber(
        "/demo/chatter",
        [](const aorta::MessageView& msg,
           const aorta::MessageContextView& ctx) {
          // (1) Handle non-contiguous data
          std::vector<uint8_t> owned;
          const uint8_t* payload = msg.data;
          if (!payload && msg.size > 0) {
            owned = msg.ToVector();
            payload = owned.empty() ? nullptr : owned.data();
          }

          // (2) Deserialize the FlatBuffer
          auto chat = payload ? demo::GetChat(payload) : nullptr;
          if (!chat) {
            AORTA_LOG(WARN) << "Failed to parse Chat message";
            return;
          }

          // (3) Read the AortaHeader
          const char* text = chat->text() ? chat->text()->c_str() : "(null)";
          auto header = chat->aorta_header();
          if (header) {
            AORTA_LOG(INFO) << "[seq="
                            << static_cast<uint64_t>(header->sequence())
                            << " stamp="
                            << static_cast<uint64_t>(header->publish_stamp_ns())
                            << "] " << text;
          } else {
            AORTA_LOG(INFO) << "[no header] " << text;
          }
        },
        aorta::QoS(), sub_options),
        "Create subscriber");
    (void)subscriber;

    // SpinOnce loop. SpinOnce returns TIMEOUT when no event arrives within
    // the window — a normal idle tick, not an error.
    while (g_running) {
      aorta::Status status =
          executor->SpinOnce(std::chrono::milliseconds(100));
      if (!status.ok() && status.code() != AORTA_ERR_TIMEOUT) {
        aorta::CheckOk(status, "Executor::SpinOnce");
      }
    }
    return 0;
  });
}
```

### 12.3 Echo Server/Client (RPC Example)

**Server file:** `cpp/examples/echo_server.cc`
**Client file:** `cpp/examples/echo_client.cc`

**Key server-side code:**

```cpp
auto service = aorta::Must(
    node->CreateServiceTypedView<demo::EchoRequest, demo::EchoResponse>(
    "/echo",
    [](const demo::EchoRequest& request,
       aorta::TypedServiceResponder<demo::EchoResponse> reply) {
      aorta::CheckOk(reply.Reply([&](demo::EchoResponseMsg& msg) {
        msg.echoed_message = request.message()->str();
      }), "Reply typed echo response");
    }),
    "Create typed service");

// Keep the process alive; if AddNode() is never called explicitly, the Node
// creates an internal executor automatically.
while (g_running) {
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}
```

**Key client-side code:**

```cpp
aorta::ClientOptions options;
options.timeout_ms = 2000;
options.enforce_single_provider = true;

auto client =
    aorta::Must(node->CreateClientTyped<demo::EchoRequest, demo::EchoResponse>(
                    "/echo", options),
                "Create typed client");

while (g_running) {
  auto result = client->Call([](demo::EchoRequestMsg& msg) {
    msg.message = std::string("hello typed rpc");
  });
  if (result.ok()) {
    auto response = std::move(result).value();
    auto text = response.Message()->echoed_message()->str();
    // Handle the typed response
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));
}
```

**Note:** `CreateService()`, like `CreateSubscriber()` / `CreateTimer()`, needs an executor-backed event sender. If the Node has not been attached to an explicit Executor first, the SDK falls back to a single-threaded executor (`ExecutorType::kSingleThreaded`) that the Node creates internally.

### 12.4 AllTypes Talker (All-Types Example)

**File:** `cpp/examples/all_types_talker.cc`

Shows `PublishTyped<T>()` support for complex message types:

```cpp
publisher->PublishTyped<demo::AllTypes>([&](demo::AllTypesMsg& msg) {
  // Scalars
  msg.flag = true;
  msg.val_int32 = -32;
  msg.val_float64 = 2.5;

  // String
  msg.text = "all-types-" + std::to_string(sequence);

  // Enums
  msg.color = demo::Color_RED;
  msg.mode = demo::Mode_RUN;

  // Struct
  msg.pose = demo::Vec3(1.0f, 2.0f, 3.0f);

  // Nested table
  auto child = std::make_shared<demo::ChildMsg>();
  child->id = 100;
  child->name = "child-0";
  msg.child = child;

  // Union
  msg.payload = demo::PayloadUnionMsg{child};

  // Optional table
  auto meta = std::make_shared<demo::MetaInfoMsg>();
  meta->tag = "meta-0";
  msg.meta = meta;

  // Vectors
  msg.bytes = {1, 2, 3, 4};
  msg.ints = {10, 20, 30};
  msg.names = {"alice", "bob"};
  msg.poses = {demo::Vec3(0.0f, 1.0f, 2.0f)};
  msg.children = {child};
  msg.colors = {demo::Color_GREEN, demo::Color_BLUE};
});
```

### 12.5 Foxglove CompressedVideo Publisher

**File:** `cpp/examples/foxglove_compressed_video_publisher.cc`

Shows integration with Foxglove formats:

```cpp
auto publisher = node->CreatePublisher<foxglove::CompressedVideo>(
    "/foxglove/compressed_video");
auto foxglove_pub =
    aorta::Must(std::move(publisher), "Create foxglove publisher");

while (g_running) {
  auto now_ns = aorta::Must(aorta::NowNs(), "Read current time");
  aorta::CheckOk(foxglove_pub->PublishTyped<foxglove::CompressedVideo>(
      [&](foxglove::CompressedVideoMsg& msg) {
        msg.timestamp = foxglove::Time(
            static_cast<uint32_t>(now_ns / 1'000'000'000ULL),
            static_cast<uint32_t>(now_ns % 1'000'000'000ULL));
        msg.frame_id = "camera/front";
        msg.format = "h264";
        msg.data = {0x00, 0x00, 0x00, 0x01, 0x09, 0x10};
      }),
      "Publish foxglove compressed video");
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
}
```

---

## 13. Best Practices

### 13.1 Lifetime Management

- **Create the Node before every primitive, and destroy it last**. The lifetime of Publisher / Subscriber / Timer / Service must be shorter than the Node's
- **Prefer creating the Executor first and calling `AddNode()`**, then creating Subscribers / Services / Timers, so that the threading model and scheduling policy stay under your control
- Without an explicit Executor, the Node automatically creates an internal single-threaded executor (`ExecutorType::kSingleThreaded`) the first time you create a Subscriber / Service / Timer
- Service / Timer / Subscriber callbacks all depend on an executor-backed event sender

### 13.2 Performance Tuning

- **Set `max_message_size`**: set `PublisherOptions::max_message_size` according to the actual message size to avoid repeated reallocation
- **Enable admission control**: in production, set `AORTA_EXECUTOR_MAX_QUEUED_EVENTS` / `AORTA_EXECUTOR_MAX_QUEUED_BYTES` to prevent unbounded memory growth
- **Use the priority scheduler**: when mixing low-frequency and high-frequency callbacks, use Priority mode so that critical callbacks are handled first

### 13.3 Observability

- **Log context**: call `SetDefaultLogContext(node->name(), node->group())` immediately after creating the Node
- **Statistics**: different metrics live on different structs/accessors — keep them apart:
  - `oversize_count` — via `publisher->GetStats()` (`Publisher::Stats`)
  - `timeout_count` — via `client->GetStats()` (`Client::Stats`)
  - `dropped_queue_full` (plus `dropped_queue_closed` / `dropped_budget_events` / `dropped_budget_bytes`) — via `executor->GetAdmissionStats()` (`ExecutorAdmissionStats`, requires `AORTA_EXECUTOR_ADMISSION_OBSERVABILITY=1`)
- **Admission observability**: set `AORTA_EXECUTOR_ADMISSION_OBSERVABILITY=1` to enable admission statistics

### 13.4 Error Handling

- **Prefer `Must`/`CheckOk` in application code**: fail fast on initialization and on paths that are expected to succeed; check `StatusOr` explicitly only on paths that can fail at runtime
- **Distinguish Status error codes**: consult the error-code table in Appendix B and handle `AORTA_ERR_TIMEOUT` / `AORTA_ERR_BUSY` specifically
- **Idempotent RPC design**: if a Service is non-idempotent, enable `enforce_single_provider` so a second provider's reply is detected (bounded, post-hoc — see §11.9); the single-provider deployment itself still has to be guaranteed by topology

### 13.5 Schema and Type Safety

- **Always use the typed API**: `CreatePublisher<T>()` / `CreateService<Req, Resp>()` beats passing bytes by hand
- **Schema conflict policy**: consider setting `AORTA_SCHEMA_CONFLICT_ACTION=reject` in production
- **Schema prefetch**: Subscribers default to `prefetch_schema_on_init = true` — a best-effort startup warmup (a detached task retries the schema fetch a bounded number of times at init, then gives up silently); it can fail or miss, so do not treat it as a guarantee that the receiver knows the peer's schema

---

## Appendices

### A. Header Cheat Sheet

| Functionality needed | Header to include |
|---------|-----------|
| Everything | `#include "aorta/aorta.h"` |
| Node | `#include "aorta/node.h"` |
| Publisher | `#include "aorta/publisher.h"` |
| Subscriber | `#include "aorta/subscriber.h"` |
| Service/Client | `#include "aorta/service.h"` / `#include "aorta/client.h"` |
| Executor | `#include "aorta/executor.h"` |
| Timer | `#include "aorta/timer.h"` |
| QoS | `#include "aorta/qos.h"` |
| Options | `#include "aorta/options.h"` |
| Status/StatusOr | `#include "aorta/status.h"` |
| Logging | `#include "aorta/logging.h"` |
| Time | `#include "aorta/time.h"` |
| SchemaMeta | `#include "aorta/schema_meta.h"` |
| TypedPublishTraits | `#include "aorta/typed_publish.h"` |
| MessageView/Context | `#include "aorta/types.h"` |
| AortaHeader construction | `#include "aorta/header.h"` |
| Message wrappers | `#include "xxx_msg.h"` (generated) |

### B. Error Code Cheat Sheet

> The complete result-code table (including `-11`…`-15`) is shared across SDKs;
> the specification is
> [`spec/Aorta_SDK_Common_Concepts.md` §7](../spec/Aorta_SDK_Common_Concepts.md#7-error-model).
> The table below lists the ones most commonly seen on the C++ side.

| Constant | Value | Meaning |
|------|----|------|
| `AORTA_OK` | `0` | Success |
| `AORTA_ERR_INVALID_ARGUMENT` | `-1` | Bad argument |
| `AORTA_ERR_NOT_FOUND` | `-2` | Not found |
| `AORTA_ERR_TIMEOUT` | `-3` | Timed out |
| `AORTA_ERR_BUSY` | `-4` | Resource busy (RPC overflow FastFailBusy) |
| `AORTA_ERR_INTERNAL` | `-5` | Internal error |
| `AORTA_ERR_SCHEMA_CONFLICT` | `-7` | Schema-hash conflict |
| `AORTA_ERR_CONCURRENT_CONFLICT` | `-8` | Concurrent conflict (multi-provider detection) |
| `AORTA_ERR_ALREADY_REPLIED` | `-10` | Service request already replied |

> Note: values `-6` and `-9` are unused reserved slots. Source reference: `core/src/ffi/c_api/mod.rs:120-147`

### C. Environment Variable Cheat Sheet

Environment-variable configuration is shared across all SDKs (Rust / C++ / Python)
and is consolidated into one document, with the full variable list, defaults and
consumption layers (treat it as authoritative):

→ **[`spec/Aorta_Environment_Variables_Reference.md`](../spec/Aorta_Environment_Variables_Reference.md)**

The ones C++ users reach for most: `AORTA_LOG` / `AORTA_LOG_FORMAT` / `AORTA_LOG_FILE` (logging),
`ZENOH_SESSION_CONFIG_URI` (Zenoh session config, takes precedence over `ZENOH_CONFIG`),
`AORTA_GROUP` (node group), `AORTA_SCHEMA_ATTACH` / `AORTA_SCHEMA_CONFLICT_ACTION` (schema),
`AORTA_RPC_ENFORCE_SINGLE_PROVIDER` (RPC),
`AORTA_EXECUTOR_MAX_QUEUED_EVENTS` / `AORTA_EXECUTOR_MAX_QUEUED_BYTES` (executor admission).

> `AORTA_TLS_LARGE_CAPACITY_THRESHOLD_KB` applies to the Rust SDK only; C++ does not read it (see §11.1).

### D. Typical Application Patterns

| Pattern | Example file | Key components |
|------|---------|---------|
| Timer-driven publishing | `talker.cc` | Node + Publisher + Timer + Executor |
| Simple publishing | `talker_simple.cc` | Node + Publisher + sleep loop |
| Subscribe and listen | `listener.cc` | Node + Subscriber + Executor |
| RPC server | `echo_server.cc` | Node + Service (no caller-created Executor; the Node starts a fallback executor automatically) |
| RPC client | `echo_client.cc` | Node + Client + sleep loop |
| Action server | `cli_execute_task_server.cc` | Node + ActionServer + feedback/cancel |
| Action client | `cli_execute_task_client.cc` | Node + ActionClient + feedback/result |
| Multi-service aggregation | `navigation_service_server.cc` | Node + multiple Services |
| Complex messages | `all_types_talker.cc` | All-types FlatBuffer message |
| Foxglove integration | `foxglove_compressed_video_publisher.cc` | Third-party schema integration |

## 14. Actions: Long-Running Tasks

### 14.1 Overview

The Aorta C++ SDK provides a typed Action API for long-running, cancellable tasks that report feedback.

Core objects:

- `ActionServer<Action>`: the server side — accepts goals, publishes feedback, returns the final result
- `ActionClient<Action>`: the client side — sends goals, queries status, cancels, waits for results
- `GoalHandle<Action>`: a durable handle for a single goal, holding `goal_id` / `goal_secret` / `server_epoch`
- `ActionExecutionContext<Action>`: the execution context inside a server handler

The typed Action headers as of today:

```cpp
#include "aorta/aorta.h"
#include "my_action_msg.h"  // your codegen-generated action wrapper header
```

### 14.2 Creating an ActionServer

#### 14.2.1 Publishing action info only

If you only want to expose the action metadata first (schema, TTL, concurrency), use:

The action name is a runtime parameter (same as topics/services, decoupled from the schema); when you do not need a custom name, pass the codegen-generated default name `MyAction::ActionName()`:

```cpp
auto server = aorta::Must(
    aorta::ActionServer<MyAction>::ServeInfoOnly(node, MyAction::ActionName()),
    "Create action info-only server");
```

This registers:

- the action info queryable

but does not actually execute goals.

#### 14.2.2 A full ActionServer

A full server uses `Serve(...)`:

```cpp
auto server = aorta::Must(
    aorta::ActionServer<MyAction>::Serve(
        node, MyAction::ActionName(), aorta::ActionServerOptions(),
        [](aorta::RetainedActionGoal<MyAction> goal,
           aorta::ActionExecutionContext<MyAction> ctx) {
          // decode goal
          // publish feedback
          // succeed / abort / cancel
        }),
    "Create action server");
```

Handler parameters:

- `RetainedActionGoal<Action>`
  - `goal.goal_id()`
  - `goal.request()`
  - `goal.message()`
  - `goal.payload()`
- `ActionExecutionContext<Action>`
  - `ctx.goal_id()`
  - `ctx.IsCancelRequested()`
  - `ctx.PublishFeedback(...)`
  - `ctx.Succeed(...)`
  - `ctx.Abort(...)`
  - `ctx.Cancel(...)`

Each goal's handler runs on its own dedicated thread (at most
`max_concurrency` goals are active at a time), so a handler may block
until its goal is terminal without delaying other goals or other Aorta
work. Return from the handler once the goal is finished: its thread lives
until then.

To react to cancellation without polling `ctx.IsCancelRequested()`, pass
a fifth argument, `on_cancel_requested(const std::string& goal_id)`. It is
called at most once per goal, when a cancel request moves the goal to
CANCELING, on an Aorta worker thread concurrently with the goal's
handler; keep it short (signal the handler and return). It can fire
before the handler has seen the goal or after the goal has finished, so a
handler that registers its goal for the callback to find should check
`ctx.IsCancelRequested()` once after registering. It requires
`supports_cancel` (otherwise `Serve` returns `AORTA_ERR_INVALID_ARGUMENT`).

```cpp
auto server = aorta::Must(
    aorta::ActionServer<MyAction>::Serve(
        node, MyAction::ActionName(), aorta::ActionServerOptions(),
        [&](aorta::RetainedActionGoal<MyAction> goal,
            aorta::ActionExecutionContext<MyAction> ctx) {
          tasks.Register(ctx.goal_id());
          if (ctx.IsCancelRequested()) tasks.Stop(ctx.goal_id());
          // ... run until done, then Succeed / Abort / Cancel
        },
        [&](const std::string& goal_id) { tasks.Stop(goal_id); }),
    "Create action server");
```

#### ActionServerOptions / ActionClientOptions

Both option structs are default-constructible (`ActionServerOptions()` / `ActionClientOptions()`); the field defaults are:

```cpp
struct ActionServerOptions {
  bool supports_cancel = true;                      // whether cancel is supported
  std::size_t max_concurrency = 1;                  // max concurrently executing goals
  std::chrono::milliseconds result_ttl{60000};      // result cache TTL (60s)
  std::chrono::milliseconds dedup_ttl{300000};      // dedup cache TTL (300s)
};

struct ActionClientOptions {
  std::chrono::milliseconds control_timeout{10000}; // control plane (send/cancel) timeout (10s)
  std::chrono::milliseconds info_timeout{5000};     // info query timeout (5s)
};
```

| Struct | Field | Default |
|------|------|--------|
| `ActionServerOptions` | `supports_cancel` | `true` |
| | `max_concurrency` | `1` |
| | `result_ttl` | `60000ms` |
| | `dedup_ttl` | `300000ms` |
| `ActionClientOptions` | `control_timeout` | `10000ms` |
| | `info_timeout` | `5000ms` |

#### 14.2.3 Server handler conventions

- On entering the handler, check `goal.valid()` first, then access `goal.request()` / `goal.message()`
- `PublishFeedback(...)` may be called many times, but the three terminal-state calls `Succeed(...)` / `Abort(...)` / `Cancel(...)` **must be hit exactly once**
- Once you reach a terminal state, return immediately; do not keep publishing feedback for the same goal
- Cancellation is cooperative: a long-running task should poll `ctx.IsCancelRequested()` periodically
- If you need idempotency or resume-after-restart, persist the `goal_id` on the application side

### 14.3 Creating an ActionClient

```cpp
auto client = aorta::Must(
    node->CreateActionClient<MyAction>(MyAction::ActionName()),
    "Create action client");
```

Common methods:

- `client->Info()`
- `client->SendGoal(goal)`
- `client->SendGoalWithId(goal_id, goal)`
- `client->QueryStatus(goal_id)`
- `client->SetFeedbackCallbackAll(...)`

Sending a goal yields a `GoalHandle<Action>`:

```cpp
auto handle = aorta::Must(client->SendGoal(goal), "Send action goal");
```

Common `GoalHandle<Action>` methods:

- `handle.goal_id()`
- `handle.goal_secret()`
- `handle.server_epoch()`
- `handle.QueryStatus()`
- `handle.Cancel()`
- `handle.GetResult()`
- `handle.WaitResult(timeout)`
- `handle.SetFeedbackCallback(...)`

If you still hold the `goal_id` and `goal_secret` after a process restart, you can restore the handle:

```cpp
auto handle = aorta::Must(
    aorta::GoalHandle<MyAction>::FromPersisted(client, goal_id, goal_secret),
    "Restore persisted goal handle");
```

A restored handle can still:

- `handle.QueryStatus()`
- `handle.GetResult()`
- `handle.WaitResult(...)`
- `handle.Cancel()`

At minimum, persist these fields:

- `goal_id`
- `goal_secret`

`goal_secret` acts as the capability token for private results and for cancellation, and should not be exposed to unrelated callers.

### 14.4 Minimal Server Example

Reference file:

- `cpp/examples/cli_execute_task_server.cc`

When neither `ZENOH_SESSION_CONFIG_URI` nor `ZENOH_CONFIG` is set, these two C++ action examples write a temporary Zenoh config automatically: the server listens on a fixed `tcp/127.0.0.1:17447`, the client connects to that address explicitly, and multicast / gossip discovery and SHM are all disabled — this avoids falling back to the stock Zenoh defaults, which are unstable on some machines. If you already have your own Zenoh config, the examples do not override it.

The example demonstrates:

- `ActionServer<...>::Serve(...)` with a concrete typed action
- reading the typed goal
- calling `PublishFeedback(...)` periodically
- cooperative cancel via `IsCancelRequested()`
- finally calling `Succeed(...)`

Run:

```bash
bazel run //cpp/examples:cli_execute_task_server
```

### 14.5 Minimal Client Example

Reference file:

- `cpp/examples/cli_execute_task_client.cc`

The example demonstrates:

- `CreateActionClient<...>(...::ActionName())` with the same typed action
- `SendGoal(...)` / `SendGoalWithId(...)`
- binding a per-goal feedback callback
- `Cancel()`
- `WaitResult(...)`

Run a success path:

```bash
bazel run //cpp/examples:cli_execute_task_client -- \
  --source quick \
  --preset demo/quick
```

Run a cancel path:

```bash
bazel run //cpp/examples:cli_execute_task_client -- \
  --source cancel \
  --cancel-after-ms 600 \
  --preset demo/cancel
```

If you want to drive this C++ action server with `aorta-cli` instead of the C++ client example, you must give the CLI a client-side Zenoh config of its own (`connect=tcp/127.0.0.1:17447`). The reason is that the server example runs in an isolated loopback-only peer config, and a separately launched CLI process does not inherit it. For the exact commands see the *Using CLI against the C++ local example server* section of `tools/aorta-cli/README.md`.

### 14.6 Per-Goal Feedback Stream (Pull-Based)

Besides the inline `SetFeedbackCallback`, there is a pull-based `FeedbackStream`
(bounded buffer, drained by the consumer at its own pace via `Recv()`/`TryRecv()`,
so a slow consumer never stalls the zenoh RX path):

- `GoalHandle::OpenFeedbackStream(options)`: filtered to the handle's goal;
- `ActionClient::OpenFeedbackStreamFor(goal_id, options)`: opens the stream
  **before sending**, keyed on a pre-minted goal_id — pair it with
  `GenerateGoalId()` + `SendGoalWithId(...)` to eliminate the "subscribe after
  send" window in which early feedback is lost (the application ACCEPTED frame a
  server handler publishes as its very first step will not be dropped).

Both are goal-scoped streams and share one semantics: once this client observes
the goal's terminal state through `GetResult()` / `WaitResult()` /
`SetResultCallback` (or `NotFound`/`GoalExpired`), the stream ends itself —
`Recv()` first drains the already-buffered samples, then returns `nullopt`, so the
consume loop terminates naturally with no timer and no grace period. If you open a
goal-scoped stream after the terminal state has already been observed, the stream
is born closed (`Recv()` returns `nullopt` immediately instead of blocking
forever; the memory is bounded to the 128 most recent terminal goals). Note that
this is driven by a local observation (not an on-the-wire termination frame): the
authoritative terminal data (including the last feedback snapshot) is whatever
`GetResult`/`WaitResult` returns.

```cpp
const std::string goal_id = aorta::ActionClient<MyAction>::GenerateGoalId();
auto stream = client->OpenFeedbackStreamFor(goal_id).value();  // subscribe first
auto handle = client->SendGoalWithId(goal_id, goal).value();   // then send
while (auto fb = stream.Recv()) {   // ends automatically after the terminal state
  // ... fb->message()
}
```

### 14.7 Usage Conventions

- `goal_id` is generated by the client; if you do not specify one, the SDK generates a UUID automatically (`GenerateGoalId()` lets you pre-mint one explicitly)
- `goal_secret` is returned by the server once the goal is accepted; the client must persist it
- Feedback is an action-wide shared stream; a client normally filters by `goal_id` with `GoalHandle::SetFeedbackCallback(...)`, or uses the goal-scoped `FeedbackStream` from 14.6
- `WaitResult(...)` blocks on the same event-driven result watcher as `SetResultCallback`: it fires exactly once on the terminal result (or on a timeout/transport error), with no polling loop and no node-executor coupling. For finer control, combine `QueryStatus()` + `GetResult()` yourself, or use `SetResultCallback(...)` for non-blocking delivery
- `Cancel()` is a cooperative cancel; actually stopping the work depends on the server handler polling `IsCancelRequested()`

---

## 15. ContextStore

> ContextStore is Aorta's fifth core primitive (alongside Topic / Service / Action / Timer).
> It provides a lightweight, keyed shared context (robot state, current task id, and so on),
> written by the owner node via `Set*` and read by other nodes via `Get*`/`Watch`, carrying freshness and owner metadata.

### 15.1 Including Headers

The ContextStore header is **not** part of the `aorta/aorta.h` umbrella; include it explicitly:

```cpp
#include "aorta/contextstore.h"
```

### 15.2 Creating a ContextStore

```cpp
// Via Node (the convenience entry point, declared in node.h, implemented in contextstore.cc)
StatusOr<std::shared_ptr<ContextStore>> Node::CreateContextStore();

// Or the explicit factory
static StatusOr<std::shared_ptr<ContextStore>> ContextStore::Create(
    std::shared_ptr<Node> node);
```

```cpp
auto store = aorta::Must(node->CreateContextStore(), "Create context store");
```

`ContextStore` is internally synchronised by the Rust core's `Arc<Inner>`, so all methods are thread-safe. Destruction (or an explicit `Shutdown()`) awaits Zenoh's undeclare of every Queryable / Subscriber and tears down every watch the store owns.

### 15.3 Bootstrap Contract: DeclareOwnedPrefix Before Set*

As the owner of a prefix you **must call `DeclareOwnedPrefix(prefix)` before any `Set*` on a key under that prefix**:

```cpp
aorta::CheckOk(store->DeclareOwnedPrefix("task/**"), "Declare owned prefix");

aorta::CheckOk(store->SetU64("task/state", static_cast<uint64_t>(0)),
               "Set task/state");
aorta::CheckOk(store->SetString("task/current_request_id", ""),
               "Set request id");
```

### 15.4 Typed Set* / Get*

Every type has a matching `Set*` / `Get*`. `Set*` takes an optional `expires_at_ns` (0 means no expiry):

```cpp
Status SetBool(std::string_view key, bool value, uint64_t expires_at_ns = 0);
Status SetI64(std::string_view key, int64_t value, uint64_t expires_at_ns = 0);
Status SetU64(std::string_view key, uint64_t value, uint64_t expires_at_ns = 0);
Status SetF64(std::string_view key, double value, uint64_t expires_at_ns = 0);
Status SetString(std::string_view key, std::string_view value, uint64_t expires_at_ns = 0);

StatusOr<ContextRead<bool>>        GetBool(std::string_view key);
StatusOr<ContextRead<int64_t>>     GetI64(std::string_view key);
StatusOr<ContextRead<uint64_t>>    GetU64(std::string_view key);
StatusOr<ContextRead<double>>      GetF64(std::string_view key);
StatusOr<ContextRead<std::string>> GetString(std::string_view key);
```

`Get*` returns a `ContextRead<T>` carrying the value plus metadata:

```cpp
template <typename T>
struct ContextRead {
  T value;
  bool is_fresh = false;        // within the freshness window and not expired
  uint64_t stamp_ns = 0;        // write timestamp
  uint64_t expires_at_ns = 0;   // expiry time (0 means no expiry)
  std::string source_node;      // writer's node name
  std::string source_instance_id;
  uint64_t sequence = 0;
};
```

```cpp
auto state = aorta::Must(store->GetU64("task/state"), "Get task/state");
if (state.is_fresh) {
  uint64_t current = state.value;  // only use it when is_fresh
}
```

### 15.5 Watch / WatchWithInitialSnapshot

`Watch` subscribes to live updates only; `WatchWithInitialSnapshot` first delivers a snapshot of the current owner state, then switches to live:

```cpp
using ChangeCallback = std::function<void(const ContextChange&)>;

StatusOr<WatchHandle> Watch(std::string_view prefix, ChangeCallback callback);

StatusOr<WatchHandle> WatchWithInitialSnapshot(
    std::string_view prefix, const WatchSnapshotOptions& options,
    ChangeCallback callback);
```

`WatchWithInitialSnapshot` delivers a `kSnapshot` event for every snapshot reply, then **one** `kSnapshotDone` marker (carrying `snapshot_done_reason`: `kQueryDrained` or `kDeadlineElapsed`), after which only `kLive` events are delivered. `WatchSnapshotOptions::snapshot_deadline` defaults to 1s.

```cpp
// Live updates only
auto watch = aorta::Must(store->Watch("task/**",
    [](const aorta::ContextChange& change) {
      // change.kind == kLive; read change.key / change.value_kind / *_value etc.
    }), "Watch task/**");

// With an initial snapshot
aorta::WatchSnapshotOptions opts;  // snapshot_deadline defaults to 1s
auto watch2 = aorta::Must(store->WatchWithInitialSnapshot("task/**", opts,
    [](const aorta::ContextChange& change) {
      if (change.kind == aorta::ContextChangeKind::kSnapshotDone) {
        // the initial snapshot has been fully delivered; only kLive follows
      }
    }), "Watch with snapshot");
```

Every `ContextChange` carries `kind` / `key` / `value_kind` (a `ContextKind`) and the matching `bool_value` / `i64_value` / `u64_value` / `f64_value` / `string_value`, plus `stamp_ns` / `expires_at_ns` / `source_node` / `source_instance_id` / `sequence`. When `kind == kSnapshotDone`, only `kind` and `snapshot_done_reason` are meaningful; all other fields hold their defaults.

### 15.6 WatchHandle RAII and Non-Reentrant Teardown

`WatchHandle` is the RAII handle for a watch (movable, not copyable). Destruction awaits Zenoh's undeclare so that — **when it reports `AORTA_OK`** — no further callback fires after the destructor returns; you can also call `Status Reset()` explicitly (equivalent to destruction, but it returns the FFI status, and later calls are idempotent no-ops returning OK).

> **Teardown is fallible, but safe from every context** — it is never undefined behavior. From a runtime callback context (for example tearing down a watch inside its own callback), the FFI reports `AORTA_ERR_REENTRANT` and re-inserts the watch: an explicit `Reset()` returns that error and must be retried from a safe thread, while the **destructor** hands ownership to a detached reaper thread that retries the free from a non-runtime context, keeping the callback state alive until the free succeeds — so a registered callback can never point at freed memory. The "no callback after teardown" guarantee therefore holds only on `AORTA_OK`: on any other non-OK result the drain is unconfirmed, the wrapper retains the callback state (a bounded leak), and you must assume the callback may still run.
>
> The right pattern: from inside the callback **only signal** (e.g. `std::promise::set_value()`), and tear the watch down **outside** the callback:

```cpp
std::promise<void> done;
auto watch = aorta::Must(store->WatchWithInitialSnapshot(prefix, opts,
    [&](const aorta::ContextChange& c) {
      if (c.kind == aorta::ContextChangeKind::kSnapshotDone) {
        done.set_value();
      }
    }), "Watch with snapshot");
done.get_future().wait();
// watch is destroyed at the end of this scope; the destructor does the teardown
```

---

## 16. Tracing

> Tracing is independent of Logging (§9) and Telemetry, and provides low-overhead stage/span
> timing plus cross-process trace propagation. Trace *configuration* (the `AORTA_TRACE`,
> `AORTA_TRACE_DIR`, … environment variables) is shared across SDKs; on top of that,
> **the C++ and Rust SDKs each expose their own per-SDK tracing API** (for C++, the
> `aorta::trace` namespace and the `AORTA_TRACE_*` macros covered in this section; for Rust,
> `aorta_sdk::trace`). Only the **Python SDK has no per-request tracing API** (it can only be
> configured through environment variables).

### 16.1 Including Headers

The tracing header is **not** part of the `aorta/aorta.h` umbrella; include it explicitly:

```cpp
#include "aorta/trace.h"
```

`trace.h` is a header-only thin wrapper over the trace C-ABI. The DISABLED path is an inlined relaxed read of a cached gate byte — **zero FFI calls**; timing uses `std::chrono::steady_clock` (monotonic).

### 16.2 Initialization and Activation

Bring up / tear down the tracer (drain thread + file sink) from the environment (`AORTA_TRACE`, `AORTA_TRACE_DIR`, …). Both are idempotent:

```cpp
namespace aorta::trace {
  inline void init();      // = aorta_trace_init()
  inline void shutdown();  // = aorta_trace_shutdown()
  inline void set_enabled(bool on);
  inline bool is_enabled();
}
```

```cpp
aorta::trace::init();  // typically called early in main()
// … run …
aorta::trace::shutdown();
```

### 16.3 Level and StageId

```cpp
enum class Level : uint8_t { Trace=0, Debug=1, Info=2, Warn=3, Error=4 };
```

`StageId` enumerates the built-in pipeline stages (numbered identically to the Rust core's `trace::StageId`) and is used with `record_stage_at` / `record_stage_under` — for example `kServiceClientCall`, `kTopicPublish`, `kActionAcceptedToFinished`, `kSchedQueueWait`.

### 16.4 Recommended Instrumentation Macros

The macros below are the recommended C++ instrumentation surface, mirroring Rust's `aorta_trace_*!` macros: write the metric name as a literal, and the handle is resolved **once** into a per-call-site `static` and cached, after which every event takes the lock-free by-handle path (no FFI string, no global lock, no allocation). The DISABLED cost is one inlined gate read, and argument expressions are evaluated only when enabled (gate-before-eval).

| Macro | Purpose |
|----|----|
| `AORTA_TRACE_SCOPE(scope, level, name)` | RAII timing of the enclosing block (histogram only) |
| `AORTA_TRACE_SPAN(scope, level, name)` | RAII waterfall span + aggregate histogram (the recommended span surface) |
| `AORTA_TRACE_RECORD(scope, level, name, value)` | Record a pre-computed latency (ns) |
| `AORTA_TRACE_INSTANT(scope, level, name)` | Record a point event (occurrence) |
| `AORTA_TRACE_COUNTER(scope, level, name, value)` | Record a counter value |

```cpp
static aorta::trace::Scope s("app.planner");

void Replan() {
  AORTA_TRACE_SCOPE(s, aorta::trace::Level::Info, "replan");  // time this block
  // …
  AORTA_TRACE_COUNTER(s, aorta::trace::Level::Debug, "waypoints", path.size());
}
```

### 16.5 Handle Caching for Scopes and Metrics

`Scope` interns the scope name once and caches the master + per-scope gate pointers, so a hot-path `enabled()` is just a few inlined relaxed byte reads (no FFI). Construct it once (e.g. as a `static` local) and reuse it:

```cpp
class Scope {
 public:
  explicit Scope(const char* name);
  uint16_t handle() const;
  void set_enabled(bool on) const;
  bool enabled(Level level) const;                              // inlined gate
  void record(const char* name, Level level, uint64_t ns) const; // BY-NAME (resolved every call)
  void instant(const char* name, Level level) const;
  void counter(const char* name, Level level, uint64_t value) const;
  uint64_t recorded_count(const char* name) const;              // read-back (tests/diagnostics)
  Metric metric(const char* name, Level level, uint8_t event_type = 0) const;
};
```

> `Scope::record/instant/counter(name, ...)` are the BY-NAME path: when enabled, every call converts the name into a Rust String (allocation), takes a global lock, and does a map lookup. Use them **only** for dynamic (runtime-computed) names or low-rate calls; on hot paths use the `AORTA_TRACE_*` macros above (which cache the handle per call-site), or pre-resolve a `Metric` with `Scope::metric(...)` and reuse it (`metric.record(value)` is an inlined gate plus one by-handle FFI call).

`Metric` resolves `(scope, name)` into a stable histogram handle once (cold) and caches it alongside the gate byte, after which every `record()` is a lock-free by-handle write.

### 16.6 Trace Propagation Across Threads and Processes

`Context` carries a thread-transferable trace identity (a 16-byte trace id plus the span to parent under):

```cpp
struct Context {
  uint8_t trace_id[16] = {};
  uint64_t span_id = 0;
  bool valid = false;
};

inline Context current_context();  // read the calling thread's active outbound context
```

`ContextGuard` is an RAII inbound-context scope for **deferred work**: capture a `Context` from the request/goal inside the callback, move the work to a worker thread, and wrap it there in a `ContextGuard` so that the spans recorded by `AORTA_TRACE_SPAN` inside it stay attached to the original request's trace. An invalid `Context` makes the whole guard a no-op:

```cpp
// Capture this request's trace context inside a service / action callback
aorta::trace::Context ctx = req.TraceContext();

// Install it after handing the work to a worker thread
std::thread([ctx]() {
  aorta::trace::ContextGuard guard(ctx);
  static aorta::trace::Scope s("aorta.service");
  AORTA_TRACE_SPAN(s, aorta::trace::Level::Info, "deferred_work");
  // … spans here hang under the original request's trace …
}).detach();
```

For cross-process (RPC / action) propagation, `set_context(trace_id, parent_span_id)` / `clear_context()` install / clear the inbound trace context on each inbound message (off the hot path).

---

## 17. Aorta Flow: C++20 Coroutines

> **Start here if you are new**: [`spec/Aorta_Flow_Concepts.md`](../spec/Aorta_Flow_Concepts.md)
> — it goes from "what is a coroutine" to "why work has to form a tree"; this
> chapter only covers how to write it in C++, plus the four C++-specific rules.
> The rules stated in this chapter, including the ambient-context rules in §17.5,
> are the contract for the C++ SDK.
> Runnable full example: `bazel run //cpp/examples:flow_demo` (single process, no
> router).

### 17.1 Enabling It: a Separate C++20 Target

The baseline SDK stays **C++17, unchanged**; the Flow coroutine layer is an
**optional** separate target sitting on the C ABI, so mixing the two targets in
one binary is ABI-safe:

```python
# BUILD.bazel
cc_binary(
    name = "my_flow_node",
    srcs = ["my_flow_node.cc"],
    copts = ["-std=c++20"],                # only your coroutine code needs C++20
    deps = [
        "//cpp:aorta_cpp",                 # regular SDK (C++17)
        "//cpp:aorta_cpp20_flow",          # Flow coroutine layer (header-only)
    ],
)
```

```cpp
#include <aorta/aorta.h>
#include <aorta/flow.hpp>
namespace flow = aorta::flow;
```

Toolchain requirement: GCC >= 11 / Clang >= 13 (coroutines are still an
experimental feature in GCC 10).

**Runtime setup: one object.** `flow::Runtime` folds the four things you
used to assemble by hand in a specific order — the core executor, the
`LaneRuntime` lane pool, the facade attachment, the spin thread — into a single
object; its destructor *is* the teardown order (stop -> final drain -> detach ->
destroy the runtime -> release the executor), so the stop-then-destroy **order**
is now structurally impossible to get wrong. The only thing left for
you to guarantee is the teardown **thread**: destroy the Runtime on an outside
thread — a task/callback must not hold the last Runtime reference (destroying it
on its own dispatch thread = self-join, and it is refused loudly):

```cpp
#include <aorta/flow_runtime.hpp>

auto rt = flow::Runtime::FromEnv();     // AORTA_CPP_LANES=0 = queue-path rollback
if (!rt.ok()) return 1;
flow::Scope scope = rt->root_scope();
auto h = rt->spawn(scope, work());      // TaskHandle<T> (§17.6)
// Order for awaiting a result from a non-coroutine context (main / tests):
//   h.wait_for(10s) -> h.take_outcome() -> scope.close()
// (closing first blocks the branch spawns of combinators inside the task body —
//  the closing fence is subtree-scoped)
// teardown: write nothing — the Runtime destructor does it in order
```

When you need node callbacks (service handlers, push subscriptions, timers) to
run on the same executor, pass the node handles to the factory:
`flow::Runtime::FromEnv({node->handle()})` (`Executor::AddNode` parity, applied
**before** the spin thread starts). There is deliberately no post-creation
`add_node` method: `aorta_executor_spin` holds the dispatch mutex for the whole
spin, so a post-creation add was a startup race against the spin thread (a hang
if it lost), and the C ABI now refuses it with `AORTA_ERR_BUSY`. Variants:
`Runtime::SingleThread()` (the default choice) / `Runtime::MultiThread(workers)`
(explicit performance opt-in) take the same node-handle vector. Both demos
(`flow_demo.cc` / `flow_lanes_demo.cc`) have this shape.

<details>
<summary><b>Expert tier: manual assembly (cross-executor, multiple runtimes,
detach-rollback)</b></summary>

The original four pieces are still public, for topologies that are not the
default:

```cpp
uint64_t exec = 0;
aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec);
auto rt = flow::LaneRuntime::FromEnv();    // AORTA_CPP_LANES=0 = queue-path rollback
flow::FacadeAttachment facade(*rt, exec);  // the spin thread drives both planes
std::thread spin([exec] { (void)aorta_executor_spin(exec); });
// ... spawn_on(*rt, scope, task); inside the coroutine body just
//     co_await flow::join(a(), b()) ...
// teardown order: scope cancel+close -> aorta_executor_stop(exec)
//                 -> spin.join() -> facade.detach() -> only then destroy LaneRuntime
```

⚠️ **LaneRuntime destruction precondition** (hard contract): you
must stop the executor **and wait for the stop-drain to finish** first (the
`aorta_executor_stop` + `spin.join()` in the order above), and **only then**
destroy the `LaneRuntime`. Destroying the runtime while the executor is still
alive is a contract violation: the revoke resumptions emitted by the destructor
sweep get queued onto the still-live fallback executor, and when those frames
resume later they touch the already-destroyed runtime (UAF). After the
stop-drain, the sweep's revokes are resolved on the destroying thread (the
Disabled leg resumes inline; the enabled leg's residue is delivered by the
destroying thread), and teardown finishes before the destructor
returns. `flow::Runtime`'s destructor is the structured form of exactly this
order — when you assemble by hand, copy the order above.

</details>

### 17.2 The Core Model in One Minute

`flow::Task<T>` is a **lazy** coroutine: not a single line of the body runs until
it is driven by `spawn_on` (or `co_await`ed). A coroutine **suspends** at marked
points via `co_await`, handing the thread back to the executor; when the event is
ready the **executor** resumes it — a suspended coroutine costs zero CPU.

When `spawn_on` starts a root coroutine it **stamps** `{execution plane, scope}`
into the promise, and every `co_await` chain then copies it parent -> child
(§17.5) — which is why verbs inside a coroutine body do not take executor /
runtime / scope arguments. The two most basic awaitables:

- `co_await flow::resume()` — suspend and requeue onto **this frame's own plane**;
- `co_await flow::call(client, payload)` — send an RPC, suspend, and resume on
  the context executor when the reply arrives.

### 17.3 Safety Model: One Rule Plus Three SDK Backstops

Earlier versions had "four iron rules" here; after the hardening work
(FLOW_CPP_HARDENING_PLAN) three of them are structurally backstopped by the SDK,
leaving one you have to remember:

**The one rule — lambda coroutines must go through `co_invoke`/`spawn_with`.**
A bare capturing lambda coroutine is still dangling UB (a language-level defect
the SDK cannot detect after the fact); but once wrapped by `co_invoke` the
closure is moved into the wrapper frame, outlives every suspension point, and the
captures are safe:

```cpp
// ✓ Capturing lambda, safe: co_invoke's wrapper frame holds the closure by value
auto t = flow::co_invoke([snapshot]() -> flow::Task<void> {
  co_await flow::resume();  // plane comes from the promise context (§17.5)
  use(snapshot);            // the capture is still alive
});

// ✓ More common: spawn_with goes through co_invoke AND injects the body's own
//   **real child scope**, in one step. The body's `s` is scope.child()
//   (Rust spawn_with parity): s.cancel() inside the body cancels only this
//   task's subtree, not the parent scope or sibling tasks; the task itself is
//   still spawned under the parent scope (structural ownership is unchanged,
//   close()/shutdown wait for it at the parent).
flow::spawn_with(*exec, scope,
    [](flow::Scope s, int x) -> flow::Task<void> { ... }, 42);
```

**Three that the SDK now backstops (good to know, no longer your obligation):**

1. **Scope lifetime**: `Scope` is copyable (shared ownership), and the SDK's
   `call`/`spawn_on`/`spawn_with` all hold it by value — the coroutine frame
   co-owns the scope, so a caller dropping its own handle early cannot dangle.
   In your own coroutines just take it by value (a reference parameter is only
   safe when "the caller guarantees it outlives close()").
2. **Teardown cannot be ignored**: `co_await flow::resume()` / `co_await
   flow::call(...)` (and their explicit forms) **unwind the frame automatically**
   on a teardown delivery (an internal `cancelled_unwind`, caught by the
   promise, never crossing the C ABI) — the code after the await simply does not
   run, all locals are destroyed, and the accounting balances in `final_suspend`.
   Forgot to check a status? There is no status to forget. Experts who need an
   explicit branch use `.nothrow()` (a manual branch exit available only in
   **exception-enabled builds**, returning the raw status — this is *not* the
   `-fno-exceptions` route; see D1 below).
   **Contract (not advice)**: a `catch (...)` inside a Task body **must**
   rethrow `cancelled_unwind` (or simply do not use a bare `catch (...)` —
   `cancelled_unwind` deliberately does not derive from `std::exception`, so
   `catch (std::exception&)` is naturally safe). Swallowing it eats the unwind
   signal, but it cannot eat the cancellation itself — it only delays it
   **boundedly**: the task body keeps running synchronous code until the next
   ambient `co_await`, whose **entry cancellation gate** (§17.6) sees the scope
   is still cancelled and unwinds again; one swallow costs at most one
   synchronous window and can never wedge the task. And it is **detected**
   either way: a thread-local canary is set on every unwind throw and cleared
   when the promise catches it, `final_suspend` checks that canary, and a swallow
   WARNs ("a cancelled_unwind was SWALLOWED…") — loud, never silent.
3. **An exception = task failure, no longer process death**: an exception
   escaping a Task body is caught by the promise and settled at the root frame as
   `aorta_task_finish_failed` — a non-supervised scope fails fast via
   `ChildFailed` cascade, a supervised one isolates it, **exactly matching Rust
   panic semantics**; `std::terminate` has been retired from the promise.
   Exceptions across the C ABI are still forbidden (the SDK internals have been
   fully noexcept-audited, including the `call::on_done` callback).

**D2: supervision is a typed, cross-plane policy.** Rust `Supervision` and C++
`flow::Supervision` (`Scope::Options::supervision`) are the same enum, passed
across the C ABI as validated bytes — unknown values are always rejected
(`INVALID_ARGUMENT`), never silently folded into the default. `FailFast` (the
default) and `Isolate` (the "supervised" case above) exist on both planes:

```cpp
auto probe = root.child({.supervision = flow::Supervision::kIsolate,
                          .name = "probe"});
```

The third policy, `Restart` (`spawn_supervised`: bounded restarts with
exponential backoff inside a rolling window, escalating to `FailFast` past the
limit), is a capability **exclusive to the Rust plane in this version** (D2) —
that is an explicitly recorded plane difference, not an omission; C++ has no
corresponding spelling, and the C ABI has no wire representation for a `Restart`
parameter either (only the two zero-argument policies cross the ABI).

**D1 (normative): C++ Flow requires exception support.** Cancellation and
deadline delivery go through unwinding (`cancelled_unwind`); `flow::Task`
coroutines have no parallel status-return channel. **Instantiating** (defining or
awaiting) any `flow::Task` coroutine in a `-fno-exceptions` translation unit is a
**compile-time**, didactic rejection (a `static_assert` at the promise type), not
a silent fallback to some other delivery mode — merely `#include`ing the Flow
headers is still legal. The C++17 baseline SDK (callbacks/executors/pull
subscribers — the capabilities that predate `flow::Task`) is the supported
`-fno-exceptions` surface; `.nothrow()` is ergonomic sugar for wanting a
status-return exit from a single await in an **exception-enabled build**, not a
`-fno-exceptions` story — code that needs end-to-end `-fno-exceptions` simply
cannot carry a `flow::Task`.

**ODR prohibition (one exception mode per whole program).** D1's compile-time
gate takes effect **per translation unit** (`__cpp_exceptions` is a preprocessor
condition), which means several inline functions/templates in the aorta headers
have bodies that differ depending on whether the including translation unit has
exceptions enabled (the `#if defined(__cpp_exceptions)` / `#else` pairs you see
throughout `flow.hpp`, `flow_combinators.hpp` and `convenience.h`). Under the
One Definition Rule (ODR), the same inline entity must have an **identical**
definition in every translation unit that instantiates it — therefore mixing
`-fno-exceptions` translation units and exception-enabled ones in the same
program, when both include these headers, is **unsupported**: the linker may pick
either translation unit's definition as the final version of that inline symbol,
so the actual behavior (unwind vs. didactic rejection, or the reverse) can end up
silently inconsistent with a given translation unit's own compile flags. This is
a **whole-program** build decision, not a per-file one: every translation unit
that will include an aorta Flow header (or a header covered by `convenience.h`'s
conditional guards) must settle on the **same** exception mode, and it must stay
consistent across the entire binary/shared-library boundary.

### 17.4 Scope: Creation, Lifetime, Budget

```cpp
flow::Scope root = flow::Scope::root({.name = "my-node-root"});
flow::Scope req  = root.child({
    .priority = aorta::EventPriority::kHigh,     // omitted (nullopt) = inherit the parent; explicit values clamp, never raise
    .budget   = std::chrono::milliseconds(500),  // any chrono duration; 0 = unlimited
    .name     = "nav-request",                   // use a static string (dynamic names intern and leak)
});
```

| Verb | Semantics | Waits? |
|---|---|---|
| `cancel(reason)` | Ask all descendants to stop | No |
| `close()` | Refuse new tasks, **wait** for all children to finish (blocking; must not be called inside an aorta callback) | Yes |

⚠️ **Destruction != cancel/wait**: destroying the last `Scope` copy only releases
the handle, and child tasks keep running. Ending a scope is always the explicit
`cancel()` + `close()` pair (the demos' teardown is the pattern). `close()` is
idempotent, and calling it on any copy is equivalent.

Observing cancellation (cooperative, no preemption):

```cpp
if (scope.is_cancelled()) { ... }        // RT-safe: one acquire atomic load
if (!scope.checkpoint().ok()) ...        // observes cancellation AND budget expiry (typed Status)
auto left = scope.remaining();           // std::optional<std::chrono::nanoseconds>
```

Inside a coroutine body you do not have to thread the Scope through function
signatures: `co_await flow::this_coro::scope` returns an **owning copy** of this
frame's context scope (§17.5), and
`(co_await flow::this_coro::scope).is_cancelled()` is the loop cancellation
observation point used in the demos.

`child()` on an invalid/moved-from scope returns an **invalid Scope** (`valid()`
is false) and never silently becomes a new root — roots only come from
`Scope::root()`.

### 17.5 Ambient Context: the {plane, scope} Carried by the Promise, and this_coro

The ambient-context design makes every flow coroutine frame's promise carry a **context snapshot**
`{LaneRuntime* rt, uint64_t exec, Scope scope}` (plus the pre-existing priority
byte):

- **Stamping**: `spawn_on(exec, scope, task)` stamps `{rt = nullptr, exec, scope}`
  (executor-only plane); `spawn_on(rt, scope, task)` stamps
  `{&rt, rt.fallback_executor(), scope}` — **a Disabled runtime also stamps
  `&rt`** (the fallback plane is a valid plane, not "no plane"; this is exactly
  why the ambient arbiter verbs keep working on the rollback leg).
- **Inheritance**: every `co_await` chain copies the whole context parent ->
  child; what a child frame sees is an **immutable snapshot** — "changing the
  context" means calling an explicit overload (§17.11) or spawning under a
  different scope, never rewriting the promise.
- **Reading**: the `this_coro` readers (zero suspension, return by value):

```cpp
auto ctx      = co_await flow::this_coro::context;   // the whole payload (rt/exec/scope/priority)
flow::Scope s = co_await flow::this_coro::scope;     // owning copy
auto* rt      = co_await flow::this_coro::runtime;   // LaneRuntime* (may be null)
uint64_t e    = co_await flow::this_coro::executor;  // executor handle
```

**Two rules you must know:**

1. **`{rt, exec}` is one coupled "plane" element, never split.** `exec` is always
   that plane's fallback/queue executor. Passing a `LaneRuntime&` to an explicit
   overload overrides the **whole plane** (`rt` **and** `exec := rt.fallback_executor()`
   together); passing an explicit executor to a hop/call/action verb selects the
   executor-only plane for that call. No API accepts a "ambient rt + explicit
   exec" mixture.
2. **`offload` leaves you on a lane worker; ambient `resume()` only returns to
   the context plane.** After `co_await flow::offload(...)`, the frame continues
   on a compute lane worker at that lane's priority until the next hop (there is
   no automatic hop back). `co_await flow::resume()` is
   "requeue onto my own plane at the frame's priority": on an enabled MT runtime
   that is a lane post, which may land on a lane worker again — it is a
   yield/requeue, **not** a "leave the worker set" primitive. If you need to be
   certain you are back on the executor's **dispatch thread**, use the explicit
   `resume_on{exec}` (§17.11).

**null-rt refusal: ambient arbiter verbs on an executor-only frame.** A frame
started by `spawn_on(exec, ...)` has `rt == nullptr` in its context. The ambient
`resume()`, `sleep(d)` (queue leg), `call` and the action verbs keep working; but
the arbiter family of ambient verbs — `join` / `race` / `timeout` / `offload` /
`recv` / `aligned` / `wait_wave` (including the `*_for` forms) — **refuse loudly**
at runtime: an ERROR log naming the fix ("Spawn this task on a LaneRuntime, or
use the explicit overload"), followed by the standard `cancelled_unwind` unwind.
This matches the explicit forms (which have no signature without a
`LaneRuntime&`); null is a runtime value and cannot be made a compile error.

**What happens in a non-flow coroutine (an outside coroutine).** `co_await`ing a
flow `Task`, any ambient awaiter (`resume()`, ambient `sleep`, the ambient gates
of `join`/`race`/`timeout`/`offload`) or `this_coro::*` from a non-flow coroutine
(one whose promise does not carry flow context) is a **compile error**, and the
static_assert's didactic text points the way ("use the explicit-argument overload
/ spawn via flow::spawn_on"; the ambient endpoint verbs `recv`/`aligned`/`wait_wave`
return an `EndpointTask` and are rejected through the same concept gate as Task).
**An honestly recorded exception**: the ambient
`call(client, payload)` and the three action verbs
`send_goal{client, goal}` / `wait_result{goal[, ms]}` / `cancel_goal{goal}` share
**the same class constructors** between their ambient and explicit forms, so
"is this the ambient form" is a runtime property the compiler cannot see — these
four verbs instead refuse loudly **at runtime** when awaited from an outside
coroutine: an ERROR log at the refusal point plus the raw
`AORTA_ERR_INVALID_ARGUMENT` returned through the RPC layer (`call`'s `Outcome` /
`.nothrow()`'s `call_result`; the action verbs' `StatusOr`), never silently
losing the context. The explicit forms remain the proper entry point for
non-flow coroutine contexts (§17.11).

### 17.6 spawn, TaskHandle, and Combinators: the Ambient Golden Path

Zero `{rt, exec, scope}` threading inside coroutine bodies — this is the golden
path of `flow_lanes_demo.cc`:

```cpp
flow::Task<int> read_imu() {
  co_await flow::sleep(5ms);   // arbiter timer — the mechanism is chosen by the context (see below)
  co_return 10;
}
flow::Task<int> read_lidar() {
  co_await flow::sleep(8ms);
  co_return 32;
}

flow::Task<int> golden_path() {
  // join: all complete, one await; {rt, scope} come from this frame's promise context.
  // join is N-ary: more than two branches and mixed value types are fine,
  // and results land in the tuple in input order.
  auto [imu, pcl] = co_await flow::join(read_imu(), read_lidar());

  // race: first to finish wins; the losers drain structurally under the scope
  int winner = co_await flow::race(primary_source(), backup_source());

  // offload: move CPU-heavy work onto the background lane
  // (rule 2: the resumption happens on a lane worker, with no automatic hop back)
  int grid = co_await flow::offload(aorta::EventPriority::kBackground,
                                    [&] { return imu + pcl; });

  // timeout: a bounded wait; if the task settles first the timer is revoked and
  // it resumes immediately
  int loc = co_await flow::timeout(50ms, localize());
  co_return winner + grid + loc;
}

flow::Scope scope = rt->root_scope();
auto fused = rt->spawn(scope, golden_path());   // the context is stamped here
if (!fused.ok()) { /* scope is closing / executor already stopped */ }
```

**spawn returns TaskHandle<T> (Rust `TaskHandle` parity).**
`rt->spawn` / `spawn_on` (taking a `LaneRuntime&`, the SDK's
`const aorta::Executor&`, or a raw `uint64_t`) now accept any `Task<T>` and
return a `TaskHandle<T>`:

- `.ok()` / `.status` are still the **admission** verdict (scope is closing /
  executor has stopped), compatible with the old `Outcome<uint64_t>` spelling;
  awaiting a refused handle resolves **immediately** with the refusal code and
  never parks forever.
- **Dropping the handle = fire-and-forget** (the old behavior): the task's
  lifetime belongs to the scope, the handle only carries the result. `id()` is an
  opaque accounting id — never call `aorta_task_finish` on it.
- **Inside a flow coroutine**: `Outcome<T> o = co_await std::move(h);` — a child
  task's cancellation / failure is a **value** here, not an unwind; only the
  await itself being resolved by a teardown unwinds. `Outcome<T>` can only
  be constructed via `Outcome<T>::Value(v)` / `Outcome<T>::Stop(stopped)` (by design:
  no aggregate `{status, value}`, no default constructor, no `.status`/`.value`
  fields, no `ToStatus()`). How to read it: `o.ok()` -> `*o` / `o->` /
  `o.value()` (fatal in the stopped state); otherwise `o.stopped()` (fatal in the
  OK state) gives a `Stopped` carrying the **exact** stop reason —
  `.kind()` (`kCancelled`/`kDeadline`/`kFault`),
  `.is_cancel()`/`.is_deadline()`/`.is_fault()`, `.cancel_reason()`
  (`CancelReason`, valid in the cancelled state), `.raw_reason()` (the raw wire
  byte, including unnamed future values), and `.fault()` (`const aorta::Status&`:
  `.code()`/`.message()`, valid in the fault state). Both the cancel reason and
  the fault's code+message are propagated **verbatim** (a headline capability
  of the Outcome design, no longer folded into a bare status code). **One consumer per
  handle** (a second await hits the B2 refusal: ERROR log + unwind).
- **Non-coroutine contexts** (main / tests): `h.wait_for(budget)` (blocking,
  millisecond granularity) -> `h.take_outcome()` (one-shot) -> then
  `scope.close()`. **The order matters**: closing first blocks the branch spawns
  of combinators inside the task body (the closing fence is subtree-scoped and
  one-way). `is_finished()` is an RT-safe single-atomic probe.
- **Handle combinators** (Rust `when_all`/`when_any` parity):

  ```cpp
  std::vector<flow::TaskHandle<int>> hs;
  hs.push_back(rt->spawn(scope, worker(0)));
  hs.push_back(rt->spawn(scope, worker(1)));
  auto all = co_await flow::when_all(std::move(hs));   // Outcome<vector<int>>, input order
  auto any = co_await flow::when_any(std::move(hs2));  // Outcome<pair<index, T>>
  ```

  Differences from Rust (an honest contract): a C++ frame **cannot be destroyed**
  while suspended, so there is no hard abort like Rust's — `when_all`
  waits for **all** handles to settle before reporting the error (fault outranks
  cancel: the fault is the cause that triggers the scope's fast failure
  propagation, sibling cancels are the effect); `when_any` resumes at the first
  **clean completion** (the losers' registrations are revoked immediately and the
  tasks themselves drain under the scope — race's loser contract; cooperative
  cancellation plus a grace period is a registered P3 follow-up). When everything
  stops, `when_any` reports the last stop code (Rust `last_error` parity). An
  empty `when_all` = immediately OK; an empty `when_any` = a B2 refusal.
- **`co_await flow::cancelled()` (Rust `scope.cancelled().await`
  parity)**: park on an arbiter branch waiting for **this frame's scope** to be
  cancelled — returning normally *means* "cancelled", so run your cleanup and
  then finish with `flow::stop_now()` (see below). The explicit form
  `cancelled(rt, watched_scope)` can watch a different scope; if the wait itself
  is resolved by a teardown (with the scope not cancelled) it unwinds. **A
  torn-down plane outranks the flag**: if the frame is resumed by a teardown
  delivery (pool closed, executor stopped, destructor sweep — both planes are
  covered) it always unwinds, even if the scope happens to be cancelled too
  (cleanup code must never run on the stop-drain / destructor / timer thread;
  cleanup that must survive teardown goes in a destructor, §17.3). The
  `race(work, a task wrapping cancelled)` composition still works as before.
- **`flow::stop_now()`**: `[[noreturn]]`, available only in
  exception-enabled builds (consistent with D1 — it does not exist under
  `-fno-exceptions`). The public spelling for finishing up — "I already observed
  the cancellation, the cleanup is done, end right here and record the result as
  CANCELLED" (a plain `co_return` would settle the handle as OK, which is not
  what you want). It unwinds via `cancelled_unwind`; the outcome's cancel reason
  is snapshotted verbatim from the frame's scope at the call site. The
  `cancelled()` cleanup idiom is therefore:

  ```cpp
  co_await flow::cancelled();  // cancellation observed
  release_resources();         // cleanup
  flow::stop_now();            // end this task here, marked CANCELLED
  ```

**The join family**:

- **join semantics**: all complete, fail fast; it spawns the branches under the
  (context) scope — semantically closest to Rust's `try_join`, not Rust's
  zero-spawn same-task `scope.join`. N-ary variadic (>= 2, mixed value types);
  for a dynamic count use `join_all(std::vector<Task<T>>)` -> `vector<T>` (an
  empty vector resolves immediately). To aggregate **handles** (rather than lazy
  tasks) use `when_all` above. **Capacity limit**: an arbiter group holds at most
  255 branches — the variadic join asserts at compile time, and `join_all` /
  `when_all` / `when_any` refuse loudly past the limit (B2 ERROR log + unwind),
  never truncating silently.
- **`TaskHandle<T>`'s `T` does not need to be default-constructible**:
  `Outcome<T>` is a discriminant (`std::variant`) of `T` or `Stopped`, and the
  stop path never constructs a `T` — move-only, non-default-constructible
  payloads work directly, with no extra `std::optional` wrapper
  (`cpp/test/flow_outcome_test.cc` has a dedicated test for this contract,
  including runtime-counted verification of "zero constructions on the stop
  path").
- The rule for combinators taking endpoint tasks (the return values of
  `recv()`/`aligned()`/`wait_wave()`) is unchanged: `join` accepts them, while
  `timeout()`/`race()` reject them at compile time and point at `*_for` (§17.9).

**The remaining invariants**:

- **Ambient `sleep(d)` is one verb with two mechanisms**, chosen by the context:
  `rt` non-null (enabled **or** Disabled) -> the arbiter timer (the resumption
  plane is then chosen by `enabled()`: the lane pool, or the fallback executor
  queue); `rt == nullptr` -> the queue-path timer on the context executor (i.e.
  the `sleep_on` mechanism, which on stop resumes with a cancellation
  **immediately** and never waits out the delay). Both legs have a zero-delay
  fast path; `.nothrow()` returns the raw status.
- **Per-call arguments stay explicit**: the deadline window (`timeout` / `*_for`),
  the sleep duration, `offload`'s lane priority and closure, payload/client/goal
  — only `{rt, exec, scope}` moved into the promise.
- **Deadline delivery differs from Rust (an honest contract)**: Rust's
  `scope.timeout(d, fut)` resolves that one wait with `DeadlineExceeded` as a
  **recoverable value** and the task keeps running; C++'s `flow::timeout` /
  `recv_for` and the other `*_for` forms deliver the deadline **as a whole-frame
  unwind** (there is no `.nothrow()` form) — the code after the await does not
  run. The "keep looping after a timeout" spelling is therefore Rust-only; the
  C++ way out is to spawn that bounded wait as its own child task (`spawn_on` +
  `TaskHandle`) and read the `Outcome` as a value on the handle — the unwind is
  then sealed inside the child task's frame.
- **An entry cancellation gate, not a race for the whole duration**:
  apart from `flow::cancelled()` itself (resolving immediately on an
  already-cancelled scope *is* its contract, so it needs no gate), every ambient
  verb does **one atomic read** of the frame's scope at `await_suspend` and
  refuses outright if it is already cancelled (no suspension, no machinery
  consumed). This is weaker than "racing the scope for the whole duration" —
  there are two classes:
  - **A scope branch for the whole duration (woken even while suspended)**:
    `recv`/`aligned`/`wait_wave` (and the `*_for` forms) hang the scope's
    cancellation token as a **second branch of the same wait group**, so a cancel
    arriving during the suspension wakes it immediately; the action verbs
    (`send_goal`/`wait_result`/`cancel_goal`) get the same "wakeable while
    suspended" behavior through the ctx bridge.
  - **Entry check only**: `sleep`, `join`/`join_all`/`race`/`timeout` and
    `offload` do not keep a live scope-cancel branch once suspended — a bare
    scope cancel during the suspension is only observed on the **next** entry
    into one of these verbs (the only thing that can wake a suspended one early
    is an executor/pool teardown — the resume-into-cancellation of §17.3, which
    is a separate mechanism from scope cancellation). This is the mechanical
    reason behind "swallowing a `catch (...)` only delays things by one window"
    in §17.3: the next ambient verb's entry gate sees the cancelled scope again.

### 17.7 call: Awaitable RPC

```cpp
flow::Task<void> do_request(std::shared_ptr<aorta::Client> client) {
  std::string payload = "ping";
  // The scope and executor come from this frame's promise context; the client
  // and payload stay explicit
  auto reply = co_await flow::call(
      *client,
      std::span<const uint8_t>(
          reinterpret_cast<const uint8_t*>(payload.data()), payload.size()));
  if (!reply.ok()) {
    // RPC-layer errors (scope cancelled / budget expired / timeout, etc.) —
    // explicit value errors; note that a teardown delivery never reaches
    // here: the default semantics unwind the frame directly
    // (to branch on both status layers explicitly use
    //  co_await flow::call(...).nothrow()).
    // reply.stopped() gives the exact reason: the fault state has code() +
    // message(), the cancelled state has cancel_reason()/raw_reason() — there
    // is no ToStatus()-style flattening adapter (that would throw the reason
    // vocabulary away again).
    const auto& stopped = reply.stopped();
    AORTA_LOG(WARN) << "call failed: "
                    << (stopped.is_fault() ? stopped.fault().message()
                                            : "cancelled");
    co_return;
  }
  use(*reply);  // std::vector<uint8_t>
}
```

The semantics match Rust's `call_in`, and are item-for-item identical to the
explicit form `call(*client, *exec, payload, scope)` (§17.11): the scope budget
is inherited, a scope cancellation returns `Cancelled` immediately, checkpoint
first at entry — the only thing that changed is who writes the plumbing. `call`
stores the `Scope` **by value inside the awaitable** (guaranteed by the C2
mechanism), so a temporary scope is safe too. In a non-flow coroutine, the
ambient `call` is a runtime refusal (§17.5) — use the explicit form there.

### 17.8 Watch: latest-wins Mailbox

`aorta/flow_watch.hpp` provides the RAII wrapper `flow::Watch` (header-only
C++17, shipped with the **baseline** SDK headers, no C++20 flow target required;
it is what `flow_demo.cc` uses), backed by the `aorta_watch_*` C ABI:

```cpp
#include <aorta/flow_watch.hpp>

auto watch = flow::Watch::create(node->handle(), "robot_pose");  // StatusOr
if (!watch.ok()) { /* ... */ }

// Copy out the latest value: an empty optional = no value yet; seq and payload
// come from the same atomic snapshot (safe for change detection / deduplication)
if (auto v = watch->load()) {
  use(v->seq, v->payload);          // WatchValue{seq, std::vector<uint8_t>}
}
// load() folds "empty / handle released / real error" into a single nullopt;
// when you need to tell them apart use the three-state load_checked()
// (WatchLoadResult{status, value})
watch->store(data, n);              // foreign-producer injection (any thread, no zenoh)
uint64_t total = watch->stored_total();  // observability: cumulative writes (RX + foreign)
// move-only; the destructor calls aorta_watch_free automatically
```

Use a Watch when you only need the latest value; use a subscriber when you need
every message.

### 17.9 Consumption Verbs: recv and aligned / wait_wave (Multi-Input Readiness)

`recv` / `recv_for` from `aorta/flow_subscriber.hpp`, plus `WindowSync`
(approximate time alignment, message_filters style) and `ReadinessSet` (an AND
gate over <= 64 conditions) from `aorta/flow_readiness.hpp`, with the matching
awaitable verbs `aligned` / `aligned_for` / `wait_wave` / `wait_wave_for`. The
ambient forms need a LaneRuntime plane in the frame context (started by
`spawn_on(*rt, ...)`, §17.1/§17.5). Subscriber wiring is declarative — no glue
callbacks:

```cpp
#include <aorta/flow_readiness.hpp>
#include <aorta/flow_subscriber.hpp>

// Message-by-message consumption: the frame parks on the pull subscriber; a
// cancel on the context scope unparks it immediately — the ambient form is
// **cancel-safe by default** (the scope branch always hangs on the same arbiter
// group)
flow::Task<void> consume(aorta::PullSubscriber& sub) {
  for (;;) {
    aorta::PullSample s = co_await flow::recv(sub);
    process(s);
  }  // a scope cancel / teardown unwinds the frame — no manual check in the loop
}

// 2 inputs, 5ms tolerance; once bound, the RX path feeds the window
// automatically (unbind with unbind_window)
auto ws = flow::WindowSync::create(2, std::chrono::milliseconds(5));
flow::bind_window(cam_sub, *ws, 0);
flow::bind_window(det_sub, *ws, 1);

flow::Task<void> fuse(flow::WindowSync& ws) {
  for (;;) {
    // Suspend until both inputs land inside the 5ms window; {rt, scope} come
    // from the frame context
    flow::AlignedSet set = co_await flow::aligned(ws);
    auto cam = set.at(0);   // Sample{ts_ns, data, len} view, valid while the set lives
    auto det = set.at(1);
    fuse_pair(cam, det);
  }
}

// AND gate: declarative subscriber wiring (bind_readiness), non-subscriber
// sources call satisfy(i) by hand; wait_wave snapshots the baseline internally
// at call time and waits for "the next wave completed after that"
auto set = flow::ReadinessSet::create(3);
flow::bind_readiness(imu_sub, *set, 2);
set->satisfy(0);
uint64_t wave = co_await flow::wait_wave(*set);
```

- For a deadline use `recv_for(sub, window)` / `aligned_for(ws, window)` /
  `wait_wave_for(set, window)` (the deadline window stays an explicit argument):
  the deadline is **the second branch of the same arbiter group** — **do not**
  wrap `recv()`/`aligned()`/`wait_wave()` in a bare `timeout()` combinator (the
  endpoint type tag makes that a compile error, and the diagnostic names `*_for`).
  Note that firing the deadline is delivered as a **whole-frame unwind** (no
  `.nothrow()`), not as a recoverable value like Rust's `scope.timeout` — see
  §17.6 for the difference entry and the C++ way out.
- All forms **always carry a scope branch** (the ambient form hangs the context
  scope; in the explicit form the scope is mandatory, aligned with `recv`): the
  scope-less explicit overloads that used to "stay parked until the stop-sweep"
  were removed. On an executor-only frame
  these verbs refuse at runtime (§17.5).
- Single-awaiter contract: only one `aligned()`/`wait_wave()` is allowed on
  the same window/set at a time; a second one resolves as Stopped. Destroying the
  window/set destroy-wakes any parked waiter. A **refused** await (a second
  concurrent waiter = `INVALID_ARGUMENT`, a stale handle = `NOT_FOUND`; `recv()`
  has the same contract) is first logged at **ERROR** level with the surface name
  and the raw error code (in the shape
  `flow::aligned: await REFUSED (status -1 / INVALID_ARGUMENT)`) before it
  unwinds — this is a programming error, not a cancellation; the unwind itself is
  still a `cancelled_unwind` (no new exception type), and the primitives with
  `.nothrow()` carry the raw status code in the return value for programmatic
  handling.
- Feeding the window by hand (when the sensor capture timestamp is in the
  payload): `ws->feed(i, ts_ns, data, len)`; non-blocking poll: `ws->try_aligned()`.
- Observability: `ws->stats()` (emitted / dropped_sets), `set->waves()`,
  `set->pending_mask()` (a bitmask of which conditions are still missing).

### 17.10 Action Verbs: send_goal / wait_result / cancel_goal and GoalGuard

`aorta/flow_action.hpp` turns the three action verbs into awaitables (through a
one-shot ctx bridge in the `flow::call` shape, occupying no thread). They only
need a scope plus an executor — both from the frame's promise context — and
**do not require** a LaneRuntime (they work on executor-only frames too); they
return `Outcome<T>` (the same vocabulary as the handle results in §17.6 — a
scope cancellation / budget expiry is `Stopped::Cancelled` carrying the **exact**
raw cancel byte; an action refusal or a transport-layer error is
`Stopped::Fault{raw code, message}`, no longer folded into a bare status code):

```cpp
#include <aorta/flow_action.hpp>

flow::Task<void> run_goal(
    std::shared_ptr<aorta::ActionClient<MyAction>> client,
    MyAction::GoalMsg goal) {
  // Sending: the context scope's remaining budget is stamped on the wire, and
  // the server uses it to seed the goal scope (Flow Concepts §6.2) — same
  // semantics as the explicit form
  auto accepted = co_await flow::send_goal{client, goal};
  if (!accepted.ok()) {
    // accepted.stopped(): an entry refusal / scope cancellation is is_cancel()
    // (the cancel reason is byte-exact via raw_reason()); a goal rejected by the
    // server or an RPC-layer error is is_fault() (fault().code() / .message()).
    co_return;
  }

  // RAII (opt-in): destroying an armed guard = fire-and-forget cancellation
  flow::GoalGuard<MyAction> guard(std::move(accepted).value());

  // Await the terminal state (push-based, the same status-subscription mechanism
  // as the blocking WaitResult); the timeout stays explicit
  auto result = co_await flow::wait_result{guard.goal(),
                                           std::chrono::milliseconds(5000)};
  if (result.ok()) {
    (void)guard.disarm();             // normal completion: release without cancelling
    use(result.value());
  }
  // Failure path: the guard's destructor sends one best-effort cancellation; for
  // an "acknowledged cancellation" use:
  //   co_await flow::cancel_goal{guard.disarm()};
}
```

A reminder (§17.5): these three verbs share one class between their ambient and
explicit forms — awaiting the ambient form from a non-flow coroutine is not a
compile error but a **loud runtime refusal** (ERROR log +
`stopped().is_fault() && stopped().fault().code() == INVALID_ARGUMENT` in the
`Outcome`); in a non-flow coroutine use the explicit form (§17.11).

The layered semantics match Rust: a scope cancellation stops the **local wait**;
the goal on the server side is not stopped automatically — the only cancellation
that reaches the server is an explicit `cancel_goal` (awaitable, with a checkable
acknowledgement) or the destructor of an armed `GoalGuard` (fire-and-forget). An
already-cancelled scope is refused at entry as `Stopped::Cancelled` (not a frame
unwind) — the exact cancel byte is propagated verbatim through the
completion record, not folded into a bare `CANCELLED` status code. The Rust
counterpart is `GoalHandle::into_guard()` -> `GoalGuard` (see `rust/README.md`
§15).

### 17.11 Explicit-Argument Forms: Cross-Scope / Cross-Runtime / Non-Flow Coroutines

The ambient verbs always read **this frame's** context snapshot; the
explicit-argument overloads are fully retained and are the proper entry point for
three situations — they are simultaneously an **override layer** (per call,
explicit wins, right-biased: whatever you pass is what is used, the promise
context neither participates in a merge nor gets written back) and a **test
seam** (unit tests can inject handles directly without going through a spawn
context):

1. **Cross-scope**: this one await should hang under a different scope (a
   different cancellation domain / budget);
2. **Cross-runtime / cross-executor**: this one hop / composition should land on
   a different plane — including `resume_on{exec}` when you want to be certain
   you are back on the executor's dispatch thread after an `offload`
   (§17.5 rule 2);
3. **Non-flow coroutines**: the frame carries no promise context, so the ambient
   forms are either a compile error or a runtime refusal (§17.5) — the explicit
   form is the only entry point. Across a C ABI / foreign-thread boundary the
   context never crosses implicitly either: handing the explicit handle over is
   the correct shape at the boundary.

**Argument-order rule (frozen): the main object, the execution plane, the
operation arguments, and the scope last.** Two recorded exceptions:
`join(rt, scope, tasks...)` has the scope right after the plane — C++ template
deduction cannot place a fixed parameter after a deduced parameter pack; and
`spawn_on`/`spawn_with` also have the scope right after the plane — there the
scope is the **structural owner** (the nursery parameter), not a participant in
the cancellation race, and `spawn_with`'s variadic tail pack forces that position
too.

| Ambient form | Explicit form | What the explicit form overrides |
|---|---|---|
| `flow::resume()` | `resume_on{exec}` / `resume_on_lane{&rt}` | The plane; `resume_on{exec}` is the only way to be **certain** you are back on the executor's dispatch thread |
| `flow::sleep(d)` | `sleep(rt, d)` (arbiter timer) / `sleep_on{exec, d}` (queue path) | The mechanism and the plane |
| `flow::call(client, payload)` | `call(client, exec, payload, scope)` (SDK object or raw handle) | Scope + executor |
| `flow::join(a, b)` | `join(rt, scope, a, b)` (recorded exception: the scope follows the plane) | Plane + scope |
| `flow::race(a, b)` | `race(rt, a, b, scope)` | Same as above |
| `flow::timeout(w, task)` | `timeout(rt, w, task, scope)` | Same as above |
| `flow::offload(prio, fn)` | `offload(rt, prio, fn)` | The plane (offload takes no scope) |
| `flow::recv(sub)` / `recv_for(sub, w)` | `recv(sub, rt, scope)` / `recv_for(sub, rt, w, scope)` (scope mandatory) | Plane + scope |
| `flow::aligned(ws)` / `aligned_for(ws, w)` | `aligned(ws, rt, scope)` / `aligned_for(ws, rt, w, scope)` (scope mandatory; the scope-less overloads were removed) | Same as above |
| `flow::wait_wave(set)` / `wait_wave_for(set, w)` | `wait_wave(set, rt, scope)` / `wait_wave_for(set, rt, w, scope)` (scope mandatory; the scope-less overloads were removed) | Same as above |
| `flow::send_goal{client, goal}` | `send_goal{client, exec, goal, scope}` | Scope + executor |
| `flow::wait_result{goal[, ms]}` | `wait_result{goal, exec[, ms], scope}` | Same as above |
| `flow::cancel_goal{goal}` | `cancel_goal{goal, exec, scope}` | Same as above |

Expert-tier note: `resume_on_lane{&rt}` is expert tier — use it only for
cross-runtime scenarios and as a test substrate; everyday code always uses the
ambient `flow::resume()`. `sleep(rt, d)` is likewise expert tier — outside
cross-runtime/expert uses and the test substrate, use the ambient
`flow::sleep(d)`.

Rule recap (§17.5): an explicit `LaneRuntime&` overrides the **whole plane**
(`{rt, exec}` together, with `exec` becoming that runtime's fallback executor);
an explicit executor argument on a hop/call/action verb selects the
executor-only plane for that call; `Scope` is an independent element where the
explicit value wins, per call. The branch wrapper frames spawned by a combinator
inherit **the combinator's plane and scope** (the explicit values if you passed
them, otherwise the ambient ones — which for an ambient form means the caller's).
Calling an explicit overload inside a flow coroutine is perfectly legal: the
arguments are used as given, and the promise context is untouched.

**Verbs with no ambient form**: `spawn_on` / `spawn_with`
(spawning is a control-flow capability and stays explicit; a root spawn has no
ambient context to begin with), `bind_window` / `bind_readiness` / `unbind_*`
(synchronous calls, no context), `Watch` (a C++17 non-coroutine header), and
`GoalGuard` (a RAII destructor path, by design emitting with scope=0).

### 17.12 Remaining C-ABI Touchpoints

| Need | Symbol |
|---|---|
| Hang the scope tree off the node's shutdown cascade | `aorta_node_root_scope(node, &out)` (a reference handle; freeing it does not harm the root) |
| Bridge a scope out of an existing cancellation token | `aorta_scope_child_of_token(token, opts, &out)` (one-way) |
| RT polling of the cancellation flag | Resolve `aorta_scope_cancelled_flag_ptr` once, then one acquire load |
| Subscriber priority / admission | `aorta_subscriber_set_priority(sub, prio /*255=inherit*/)`; admission: `aorta_admission_t a{.max_in_flight = 4, .deadline_ns = 20000000}; aorta_subscriber_set_admission(sub, &a);` |
| Cancel reasons | `flow::CancelReason::{kExplicit,kDeadlineExceeded,kShutdown,kChildFailed}` (the values are the C constants `AORTA_CANCEL_REASON_*`; any unknown non-zero value is treated as "cancelled"; the raw u8 overload is left for C interop) |

### 17.13 Best-Practices Checklist

1. Pair `cancel()`+`close()`, explicitly; destruction is not teardown.
2. Loops must have an observation point:
   `(co_await flow::this_coro::scope).is_cancelled()` / `scope.checkpoint()`
   (executor teardown already unwinds automatically, but scope-level
   cancellation still needs you to observe it; the ambient
   `recv`/`aligned`/`wait_wave` already carry a scope branch by default and need
   no extra observation point).
3. Lambda coroutines go through `co_invoke`/`spawn_with` (the one rule); a named
   function plus by-value parameters is still the simplest shape.
4. Ambient verbs are the golden path: do not thread `{rt, exec, scope}` through
   coroutine bodies; use the explicit forms only for cross-scope / cross-runtime
   / non-flow coroutines (§17.11).
5. Return business errors as `Outcome`/`StatusOr` values; exceptions no longer
   blow up the process (= ChildFailed), but treat them as a failure path, not as
   control flow.
6. A bare `catch (...)` inside a Task body **must** rethrow `cancelled_unwind`
   (a contract, not just advice) — failing to rethrow delays the cancellation
   by one synchronous window (the next ambient verb's entry cancellation gate
   unwinds again, §17.6) and is detected with a WARN, but still do not rely on
   that safety net: just never write a bare `catch (...)` that does not rethrow.
7. Use `flow::Runtime`: one line of setup, teardown delegated to the destructor
   (§17.1). Only manual assembly (expert tier) requires memorizing the order:
   first the scope `cancel`+`close`, then stop the executor, then `join` the spin
   thread, and **only then** destroy the `LaneRuntime`.
8. Give scopes **static** names; dynamic strings are interned per unique value
   (a leak).
9. Rust's `Shield` (the cleanup primitive under a cancellation shield) is
   **Rust-only**; C++ has no counterpart — in C++, cleanup that "must run after a
   cancellation" goes into a destructor (destructors are guaranteed to run during
   a teardown unwind, §17.3 backstop 2).
10. The order for awaiting a result in main / tests: `h.wait_for(...)` ->
    `h.take_outcome()` -> `scope.close()` — closing first blocks the branch
    spawns of combinators inside the task body (§17.6).
11. After `co_await flow::cancelled()`, run the cleanup and finish with
    `flow::stop_now()` (§17.6) — do not manufacture a redundant await just to
    get an unwind.
12. For `Supervision::Isolate` use `.supervision = flow::Supervision::kIsolate`
    (a typed field, not a bool); `Restart` exists only in Rust in this version
    (§17.3 D2).

### 17.14 Full Example

Both demos are acceptance artifacts for the golden path: `flow::Runtime` does
setup as a single object and teardown in its destructor, the
`TaskHandle` returned by `spawn` carries the result, and coroutine bodies
thread zero `{rt, exec, scope}`. `cpp/examples/flow_demo.cc` uses plain
SDK objects (`Node`/`Client`, with the node handle passed to
`Runtime::FromEnv`) to string together the basics: 3x ambient `flow::call` RPCs
inside a budgeted scope, a sibling task observing cancellation via
`this_coro::scope` + ambient `sleep`, refusing new work after a cancel, and Watch
snapshot sequence numbers; `cpp/examples/flow_lanes_demo.cc` strings together the
lane surface: ambient combinators (`join`/`race`/`offload`/`timeout`), `recv`,
`aligned`, and the main-side rendezvous with `wait_for`/`take_outcome`:

```bash
bazel run //cpp/examples:flow_demo
bazel run //cpp/examples:flow_lanes_demo
```
