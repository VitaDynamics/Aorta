use aorta_core::client::Client as CoreClient;
use aorta_core::error::{AortaError, AortaResult};
use aorta_core::executor::{ExecutorOptions, SingleThreadedExecutor};
use aorta_core::node::Node as CoreNode;
use aorta_core::publisher::{Publisher as CorePublisher, PublisherOptions};
use aorta_core::runtime::spawn_logged;
use aorta_core::schema::{infer_aorta_header_layout_from_bfbs, SchemaMeta};
use aorta_core::service::Service as CoreService;
use aorta_core::subscriber::{MessageContext, Subscriber as CoreSubscriber, SubscriberOptions};
use aorta_core::telemetry::{
    NodeOptions as CoreNodeOptions, TelemetryNodeMetadata as CoreTelemetryNodeMetadata,
};
use aorta_core::timer::{Timer, TimerOptions};
use aorta_core::Target;
use aorta_core::ZBytes;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::client::{Client, ClientOptions};
use crate::execution_group::{ExecutionGroup, ExecutionPolicy};
use crate::header::HeaderInjector;
use crate::publisher::{ensure_schema_bfbs, Publisher};
use crate::service::{Service, ServiceOptions, ServiceRequest, ServiceSchemaInfo};
use crate::typed_service::{
    validate_typed_client_options, TypedClient, TypedRetainedServiceRequest,
    TypedRpcRequestMessage, TypedRpcResponseMessage, TypedServiceResponder,
};
use crate::typed_subscribe::{TypedRetainedSample, TypedSubscribeMessage};
use crate::zbytes::with_contiguous_bytes;

const DEFAULT_EXECUTOR_QUEUE: usize = 1024;

/// Options for [`Node::with_options`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeOptions {
    /// Telemetry metadata attached to this node's liveliness/inventory
    /// entries. Unset fields are filled with SDK defaults (`language=rust`,
    /// `sdk_name=aorta_rust`, `sdk_version=<crate version>`).
    pub telemetry: CoreTelemetryNodeMetadata,
    /// Whether the node may start its own single-threaded executor on first
    /// use of a callback-capable primitive. Default `true`.
    ///
    /// Set it to `false` when the APPLICATION owns the executor: creating a
    /// subscriber/service/timer/execution group then fails with a clear error
    /// until an executor has been attached, instead of quietly starting a
    /// second one behind the application's back. See
    /// [`NodeOptions::with_auto_executor`].
    pub auto_executor: bool,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            telemetry: default_sdk_telemetry_metadata(),
            auto_executor: true,
        }
    }
}

impl NodeOptions {
    /// Opt out of (or back into) the SDK auto-executor — see
    /// [`NodeOptions::auto_executor`].
    ///
    /// The auto-executor is a convenience, not a contract: it runs the node's
    /// callbacks on a dedicated `aorta-sdk-exec-<node>` OS thread with its own
    /// `current_thread` tokio runtime. An application that wants to configure
    /// the executor itself (queue depth, callback groups, scheduler mode) must
    /// attach BEFORE the first callback-capable primitive is created, because
    /// the first executor attached to a node owns it. Turning the
    /// auto-executor off makes that requirement explicit and order-independent
    /// rather than something the caller has to remember.
    ///
    /// ```no_run
    /// use aorta_sdk::prelude::*;
    /// use aorta_sdk::{spawn_executor_thread, ExecutorNodeAttachExt, ExecutorOptions,
    ///                 SingleThreadedExecutor};
    ///
    /// # async fn demo() -> AortaResult<()> {
    /// let node = Node::with_options(
    ///     "owner",
    ///     "default",
    ///     NodeOptions::default().with_auto_executor(false),
    /// )
    /// .await?;
    ///
    /// let mut executor = SingleThreadedExecutor::new_with_options(
    ///     8192,
    ///     ExecutorOptions {
    ///         // Required to spin on a current_thread runtime.
    ///         force_tokio_queue: true,
    ///         ..ExecutorOptions::default()
    ///     },
    /// );
    /// executor.try_attach_sdk_node(&node)?;
    /// let _sub = node.create_subscriber("demo/topic", |_p, _c| {}).await?;
    ///
    /// // Same runtime placement as the auto-executor: a dedicated OS thread,
    /// // NOT the application's shared tokio worker pool.
    /// let spin = spawn_executor_thread("app-exec", executor)?;
    /// # let _ = spin;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_auto_executor(mut self, enabled: bool) -> Self {
        self.auto_executor = enabled;
        self
    }

    fn into_core(self) -> CoreNodeOptions {
        CoreNodeOptions {
            telemetry: merge_sdk_telemetry_metadata(self.telemetry),
        }
    }
}

/// The central object of the Aorta Rust SDK: most communication primitives
/// (publisher, subscriber, service, client, timer, execution group) are
/// created through a [`Node`]; the exception is the action API, whose
/// `ActionServer` / `ActionClient` are constructed directly in the
/// [`crate::action`] module and take a `&Node` for transport access.
///
/// A `Node` owns a Zenoh session plus a lazily auto-started single-threaded
/// internal executor (started on first callback-capable primitive, or via
/// [`Node::ensure_executor`]). Cloning is cheap (shared `Arc`) and safe to
/// move into tasks.
///
/// # Lifecycle
///
/// Call [`Node::close`] before shutdown: it stops the internal executor and
/// closes the Zenoh session gracefully. Dropping the last `Node` clone
/// without `close()` stops the auto-executor synchronously (with a warning)
/// but is **not** equivalent to `close()` — telemetry/schema/scope/session
/// cleanup only happens in the explicit `await` form. Prefer
/// `node.close().await`.
///
/// # Example
///
/// ```no_run
/// use aorta_sdk::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> AortaResult<()> {
///     let node = Node::new("demo", "default").await?;
///     let publisher = node.create_publisher_raw("/demo/raw").await?;
///     publisher.publish(b"hello".to_vec())?;
///     node.close().await?;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Node {
    inner: CoreNode,
    executor: Arc<Mutex<Option<InternalExecutorHandle>>>,
    /// `NodeOptions::auto_executor` — see [`Node::ensure_executor`].
    auto_executor: bool,
}

/// The SDK auto-executor's spin thread. Identical machinery to the one an
/// application gets from [`crate::spawn_executor_thread`] — one source of
/// truth for "executor on a dedicated OS thread with a current_thread
/// runtime".
type InternalExecutorHandle = crate::executor_ext::ExecutorThread;

impl Node {
    /// Create a node and connect to the Zenoh runtime (async).
    ///
    /// `name` is the user-facing node name; `group` is used for the
    /// `{group}` segment of key expressions. Note: the `group` argument is
    /// currently ignored by the core — the effective group is resolved from
    /// the `AORTA_GROUP` environment variable (fallback `default`).
    /// [`Node::group`] returns the resolved value.
    pub async fn new(name: impl Into<String>, group: impl AsRef<str>) -> AortaResult<Self> {
        Self::with_options(name, group, NodeOptions::default()).await
    }

    /// Create a node with explicit [`NodeOptions`] (telemetry metadata).
    pub async fn with_options(
        name: impl Into<String>,
        group: impl AsRef<str>,
        options: NodeOptions,
    ) -> AortaResult<Self> {
        let auto_executor = options.auto_executor;
        let inner = CoreNode::with_options(name, group, options.into_core()).await?;
        Ok(Self {
            inner,
            executor: Arc::new(Mutex::new(None)),
            auto_executor,
        })
    }

    /// The node name given at construction.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// The node identity: a stable 128-bit hex string derived from
    /// `(host, group, name)` — the same configuration yields the same id
    /// across restarts — or an explicit `AORTA_NODE_ID` override. Not a
    /// random UUID and not guaranteed globally unique.
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// The resolved group (`AORTA_GROUP` env var, or `default`).
    pub fn group(&self) -> &str {
        self.inner.group()
    }

    /// Current time from the unified Aorta time source (see [`Time`](aorta_core::time::Time)).
    pub fn now(&self) -> aorta_core::time::Time {
        self.inner.now()
    }

    /// The node's Flow root scope. `Node::close()` cancels
    /// it (Shutdown) and then gives the tree a BOUNDED drain window (500ms)
    /// before session teardown — pair with `scope.shutdown(..)` yourself
    /// when you need a full (unbounded) drain.
    pub fn root_scope(&self) -> aorta_core::flow::Scope {
        self.inner.root_scope()
    }

    /// Drain a pollable fd into a depth-1 latest-value watch cell
    /// (`Condition::FdReadable`). Not a hard-RT path.
    #[cfg(unix)]
    pub async fn poll_fd(
        &self,
        fd: std::os::unix::io::OwnedFd,
    ) -> AortaResult<std::sync::Arc<aorta_core::watch::Watch>> {
        self.inner.poll_fd(fd).await
    }

    /// [`poll_fd`](Self::poll_fd) under an EXPLICIT scope — the reader is
    /// accounted by (and cancelled with) that scope instead of the node
    /// root.
    #[cfg(unix)]
    pub async fn poll_fd_in(
        &self,
        fd: std::os::unix::io::OwnedFd,
        scope: &aorta_core::flow::Scope,
    ) -> AortaResult<std::sync::Arc<aorta_core::watch::Watch>> {
        self.inner.poll_fd_in(fd, scope).await
    }

    /// Declare a depth-1 latest-value watch cell: `load()` is
    /// the streaming consumer path — no executor, no callback, no queue.
    pub async fn watch(
        &self,
        topic: &str,
    ) -> AortaResult<std::sync::Arc<aorta_core::watch::Watch>> {
        self.inner.watch(topic).await
    }

    /// Access the underlying core node (advanced/FFI-adjacent use).
    pub fn inner(&self) -> &CoreNode {
        &self.inner
    }

    /// Create a `ContextStore` writer rooted at this node. Each call
    /// mints a fresh `source_instance_id`; share the returned handle
    /// (it's `Clone`, backed by `Arc`) instead of calling this twice on
    /// the same node.
    pub fn create_context_store(&self) -> crate::contextstore::ContextStore {
        crate::contextstore::ContextStore::new(&self.inner)
    }

    /// Lock the internal executor slot, recovering from poison.
    ///
    /// The critical sections only move the handle (no user code), so a
    /// poisoned lock means a panic elsewhere unwound through a guard — the
    /// slot contents are still structurally valid. `ensure_executor`
    /// previously panicked here while `close()` silently no-opped on the
    /// same condition; both now recover.
    fn lock_executor(&self) -> std::sync::MutexGuard<'_, Option<InternalExecutorHandle>> {
        self.executor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Idempotently ensure the internal single-threaded executor is running.
    ///
    /// Automatically called by every `create_*` for callback-capable
    /// primitives (subscriber, service, timer, execution group); call it
    /// explicitly when you need the executor for a raw core node. No-op if a
    /// live executor (internal or externally attached) already exists, and no-op
    /// when the node was built with
    /// [`NodeOptions::with_auto_executor(false)`](NodeOptions::with_auto_executor).
    ///
    /// # Runtime placement
    ///
    /// The auto-executor spins on a DEDICATED OS thread named
    /// `aorta-sdk-exec-<node>` running its own `current_thread` tokio runtime.
    /// That is deliberate: spinning on the application's shared multi-threaded
    /// runtime puts the dispatch loop on the same workers as Zenoh's RX tasks,
    /// which cost roughly +96 µs of p50 receive latency and 5× subscriber-thread
    /// CPU in a 64 B @ 1 kHz measurement. An
    /// application that supplies its own executor should reproduce this shape
    /// with [`spawn_executor_thread`](crate::spawn_executor_thread) rather than
    /// awaiting `spin()` on its `#[tokio::main]` runtime.
    ///
    /// # Errors
    ///
    /// Returns the attachment refusal when the auto-executor may not take the
    /// node — the reachable case being a node that was detached from an
    /// application executor while a timer or service it created is still alive,
    /// since those producers hold the previous executor's queue. Nothing is
    /// started or stored in that case, so a later call retries and succeeds
    /// once the blocking producer is dropped.
    pub fn ensure_executor(&self) -> AortaResult<()> {
        if !self.auto_executor || self.inner.has_event_sender() {
            return Ok(());
        }
        let mut guard = self.lock_executor();
        if guard.is_some() || self.inner.has_event_sender() {
            return Ok(());
        }

        // The auto-executor runs on a current_thread tokio runtime, which does
        // not support block_in_place(). Force the tokio mpsc queue to avoid
        // panicking when the crossbeam+eventfd path is otherwise enabled on Linux.
        let options = ExecutorOptions {
            force_tokio_queue: true,
            ..ExecutorOptions::default()
        };
        let mut exec = SingleThreadedExecutor::new_with_options(DEFAULT_EXECUTOR_QUEUE, options);

        // ATTACH FIRST, and surface a refusal. This used the
        // infallible `add_node`, which only logs: the executor was then spun up
        // and its handle stored even though it owned no node, so the caller got
        // `Ok`, the subscriber created next silently fell back to its local
        // queue — the very split the attach contract prevents — and the stored handle
        // made every later `ensure_executor` a no-op, so the node could not
        // recover even once the blocking producer was dropped.
        //
        // Attaching before the thread is spawned means a refusal costs nothing
        // and leaves `guard` as `None`, so a later call retries cleanly.
        exec.try_add_node(self.inner.clone())?;

        let spin = crate::executor_ext::spawn_executor_thread(
            &format!("aorta-sdk-exec-{}", self.inner.name()),
            exec,
        )?;
        *guard = Some(spin);
        Ok(())
    }

    /// Whether this node's SDK-managed auto-executor is currently running.
    ///
    /// `false` either because the application attached its own executor before
    /// the first callback-capable primitive, or because the node opted out via
    /// [`NodeOptions::with_auto_executor`]. Useful in tests and startup
    /// assertions to confirm that the executor the application configured is
    /// the one actually dispatching.
    pub fn has_internal_executor(&self) -> bool {
        self.lock_executor().is_some()
    }

    /// Ensure a callback-capable primitive has an executor to dispatch on.
    ///
    /// With the default `auto_executor` this starts the SDK auto-executor;
    /// with the opt-out it FAILS instead of silently starting a second
    /// executor behind the application's back.
    fn require_executor(&self, primitive: &str) -> AortaResult<()> {
        if self.inner.has_event_sender() {
            return Ok(());
        }
        if !self.auto_executor {
            return Err(AortaError::invalid_argument(format!(
                "node '{}' was created with NodeOptions::with_auto_executor(false), \
                 so no executor is started implicitly: attach one \
                 (ExecutorNodeAttachExt::try_attach_sdk_node) before creating a {}",
                self.inner.name(),
                primitive
            )));
        }
        self.ensure_executor()
    }

    /// Create an [`ExecutionGroup`] with the given [`ExecutionPolicy`],
    /// binding later-created subscribers/services/timers to one callback
    /// domain. The underlying callback group is released when the returned
    /// group is dropped.
    ///
    /// ```no_run
    /// use aorta_sdk::prelude::*;
    ///
    /// # async fn demo(node: &Node) -> AortaResult<()> {
    /// let group = node.create_execution_group(ExecutionPolicy::Serialized)?;
    /// let sub_options = group.subscriber_options(SubscriberOptions::new());
    /// let _sub = node
    ///     .create_subscriber_with_options("/demo/topic", |_payload, _ctx| {}, sub_options)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_execution_group(&self, policy: ExecutionPolicy) -> AortaResult<ExecutionGroup> {
        self.require_executor("execution group")?;
        let handle = self
            .inner
            .create_callback_group(policy.callback_group_type())?;
        Ok(ExecutionGroup::new(self.inner.clone(), policy, handle))
    }

    /// Gracefully shut the node down: stop the internal executor (if any)
    /// and close the underlying Zenoh session. Idempotent; prefer this over
    /// dropping the node.
    pub async fn close(&self) -> AortaResult<()> {
        let handle = self.lock_executor().take();
        if let Some(mut handle) = handle {
            handle.stop();
        }
        self.inner.close().await
    }

    /// Create a typed publisher for schema `M` on `topic`. The schema's
    /// BFBS is registered with the node's schema registry; whether a schema
    /// hash rides on the wire follows `SchemaAttachPolicy` (node default:
    /// `Never`). Every publish injects an Aorta header (publish timestamp,
    /// publisher node, sequence).
    ///
    /// ```ignore
    /// use aorta_sdk::prelude::*;
    ///
    /// # async fn demo(node: &Node) -> AortaResult<()> {
    /// let publisher = node.create_publisher::<ChatSchema>("/demo/chatter").await?;
    /// publisher.publish_typed(|msg: &mut ChatMsg| {
    ///     msg.text = Some("hello".to_string());
    /// })?; // synchronous fire-and-forget
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See `rust/examples/talker.rs` for a complete example (`ChatSchema` is
    /// generated from a FlatBuffers schema by the example crates).
    pub async fn create_publisher<M: SchemaMeta + HeaderInjector>(
        &self,
        topic: &str,
    ) -> AortaResult<Publisher<M>> {
        let mut options = PublisherOptions::default();
        ensure_schema_bfbs::<M>(&mut options)?;
        let config = Publisher::<M>::builder_config_from_options(&options, topic);
        let inner = CorePublisher::new_with_options(&self.inner, topic, options).await?;
        Ok(Publisher::new(self.inner.clone(), inner, config))
    }

    /// Create a typed publisher with full [`PublisherOptions`] control
    /// (QoS, `max_message_size`, congestion control, express mode, …).
    /// Consider setting `max_message_size` for predictable buffer sizing.
    pub async fn create_publisher_with_options<M: SchemaMeta + HeaderInjector>(
        &self,
        topic: &str,
        mut options: PublisherOptions,
    ) -> AortaResult<Publisher<M>> {
        ensure_schema_bfbs::<M>(&mut options)?;
        let config = Publisher::<M>::builder_config_from_options(&options, topic);
        let inner = CorePublisher::new_with_options(&self.inner, topic, options).await?;
        Ok(Publisher::new(self.inner.clone(), inner, config))
    }

    /// Create a raw (schema-free) publisher returning the core publisher
    /// type. Publish raw bytes only — no header injection, no FlatBuffers
    /// building. Useful for bridging/forwarding scenarios.
    pub async fn create_publisher_raw(&self, topic: &str) -> AortaResult<CorePublisher> {
        self.inner.create_publisher(topic).await
    }

    /// Create a raw (schema-free) subscriber: `callback` receives the raw
    /// payload and its [`MessageContext`]. Callbacks are dispatched by the
    /// node's executor (auto-started if needed).
    ///
    /// **Keep the returned subscriber bound** (`let _sub = …`) for the
    /// subscription lifetime: dropping it undeclares the subscription and
    /// stops delivery — the same as an explicit
    /// `subscriber.undeclare().await`. Callbacks are synchronous and cannot
    /// be preempted; keep them short.
    pub async fn create_subscriber<F>(
        &self,
        topic: &str,
        callback: F,
    ) -> AortaResult<CoreSubscriber>
    where
        F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
    {
        self.prepare_subscriber_creation(topic)?;
        self.inner.create_subscriber(topic, callback).await
    }

    /// Typed subscriber with a *borrowed* decoded view: `callback` receives
    /// `M::Root<'a>` which borrows the payload and is **only valid inside
    /// the callback**. Copy/clone anything you need out before returning.
    ///
    /// Decode failures are logged (rate-limited warn) and the sample is
    /// dropped, never delivered. When header context / dedup / reorder
    /// checks are enabled, the header layout hint is inferred from the
    /// schema automatically.
    ///
    /// ```ignore
    /// use aorta_sdk::prelude::*;
    ///
    /// # async fn demo(node: &Node) -> AortaResult<()> {
    /// let _sub = node
    ///     .create_subscriber_typed_view::<ChatSchema, _>("/demo/chatter", |msg, _ctx| {
    ///         let text = msg.text().unwrap_or("").to_string(); // copy out!
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// To keep the payload beyond the callback, use
    /// [`create_subscriber_typed_retained`](Self::create_subscriber_typed_retained)
    /// instead.
    pub async fn create_subscriber_typed_view<M, F>(
        &self,
        topic: &str,
        callback: F,
    ) -> AortaResult<CoreSubscriber>
    where
        M: TypedSubscribeMessage,
        F: for<'a> Fn(M::Root<'a>, &MessageContext) + Send + Sync + 'static,
    {
        self.create_subscriber_typed_view_with_options::<M, F>(
            topic,
            callback,
            SubscriberOptions::default(),
        )
        .await
    }

    /// [`create_subscriber_typed_view`](Self::create_subscriber_typed_view)
    /// with explicit [`SubscriberOptions`].
    pub async fn create_subscriber_typed_view_with_options<M, F>(
        &self,
        topic: &str,
        callback: F,
        mut options: SubscriberOptions,
    ) -> AortaResult<CoreSubscriber>
    where
        M: TypedSubscribeMessage,
        F: for<'a> Fn(M::Root<'a>, &MessageContext) + Send + Sync + 'static,
    {
        self.prepare_subscriber_creation(topic)?;
        inject_header_layout_hint_from_schema::<M>(&mut options);
        self.inner
            .create_subscriber_with_options(
                topic,
                move |payload: &ZBytes, ctx: &MessageContext| {
                    with_contiguous_bytes(payload, |bytes| match M::decode_typed_root(bytes) {
                        Ok(msg) => callback(msg, ctx),
                        Err(err) => {
                            crate::aorta_log_every_n!(
                                crate::logging::LogLevel::Warn,
                                1024,
                                "Aorta WARN: typed subscriber decode failed on topic '{}': {}",
                                ctx.topic,
                                err
                            );
                        }
                    });
                },
                options,
            )
            .await
    }

    /// Typed subscriber with an **owned** [`TypedRetainedSample`]: the
    /// payload is copied out of the transport buffer, so the sample (and
    /// anything decoded from it) stays valid after the callback returns —
    /// hand it to another task or store it.
    ///
    /// ```ignore
    /// use aorta_sdk::prelude::*;
    ///
    /// # async fn demo(node: &Node) -> AortaResult<()> {
    /// let _sub = node
    ///     .create_subscriber_typed_retained::<ChatSchema, _>("/demo/chatter", |sample| {
    ///         if let Ok(msg) = sample.decode() {
    ///             // msg borrows sample; sample itself can outlive the callback
    ///         }
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_subscriber_typed_retained<M, F>(
        &self,
        topic: &str,
        callback: F,
    ) -> AortaResult<CoreSubscriber>
    where
        M: TypedSubscribeMessage,
        F: Fn(TypedRetainedSample<M>) + Send + Sync + 'static,
    {
        self.create_subscriber_typed_retained_with_options::<M, F>(
            topic,
            callback,
            SubscriberOptions::default(),
        )
        .await
    }

    /// [`create_subscriber_typed_retained`](Self::create_subscriber_typed_retained)
    /// with explicit [`SubscriberOptions`].
    pub async fn create_subscriber_typed_retained_with_options<M, F>(
        &self,
        topic: &str,
        callback: F,
        mut options: SubscriberOptions,
    ) -> AortaResult<CoreSubscriber>
    where
        M: TypedSubscribeMessage,
        F: Fn(TypedRetainedSample<M>) + Send + Sync + 'static,
    {
        self.prepare_subscriber_creation(topic)?;
        inject_header_layout_hint_from_schema::<M>(&mut options);
        self.inner
            .create_subscriber_with_options(
                topic,
                move |payload: &ZBytes, ctx: &MessageContext| {
                    with_contiguous_bytes(payload, |bytes| {
                        let sample =
                            match TypedRetainedSample::<M>::try_new(bytes.to_vec(), ctx.clone()) {
                                Ok(sample) => sample,
                                Err(err) => {
                                    crate::aorta_log_every_n!(
                                        crate::logging::LogLevel::Warn,
                                        1024,
                                        "Aorta WARN: typed retained subscriber decode failed on topic '{}': {}",
                                        ctx.topic,
                                        err
                                    );
                                    return;
                                }
                            };
                        callback(sample);
                    });
                },
                options,
            )
            .await
    }

    /// Raw (schema-free) subscriber with full [`SubscriberOptions`] control
    /// (QoS, header context, dedup, reorder check, receive depth, …).
    pub async fn create_subscriber_with_options<F>(
        &self,
        topic: &str,
        callback: F,
        options: SubscriberOptions,
    ) -> AortaResult<CoreSubscriber>
    where
        F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
    {
        self.prepare_subscriber_creation(topic)?;
        self.inner
            .create_subscriber_with_options(topic, callback, options)
            .await
    }

    /// Raw subscriber with an explicit [`QoS`](aorta_core::qos::QoS) profile
    /// (e.g. [`QoS::realtime_control`](aorta_core::qos::QoS::realtime_control)
    /// for control traffic, [`QoS::sensor_data`](aorta_core::qos::QoS::sensor_data)
    /// for best-effort sensor streams).
    pub async fn create_subscriber_with_qos<F>(
        &self,
        topic: &str,
        callback: F,
        qos: aorta_core::qos::QoS,
    ) -> AortaResult<CoreSubscriber>
    where
        F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
    {
        self.prepare_subscriber_creation(topic)?;
        self.inner
            .create_subscriber_with_qos(topic, callback, qos)
            .await
    }

    /// Pull-mode (`DeliveryMode::InlinePull`) subscriber backed by a
    /// Zenoh-native keep-last-`depth` ring (Tier B-pull). No callback and no
    /// executor hop — drain via `Subscriber::recv` / `try_recv` /
    /// `recv_timeout` from a dedicated consumer thread.
    pub async fn create_subscriber_pull(
        &self,
        topic: &str,
        depth: usize,
    ) -> AortaResult<CoreSubscriber> {
        // Validate the topic key (subscribe-side, wildcards allowed) but do
        // NOT spin up the auto-executor: a pull ring is drained by the caller
        // and never dispatches onto an executor.
        self.inner.key_builder().topic_key_sub(topic)?;
        self.inner.create_subscriber_pull(topic, depth).await
    }

    /// Pull-mode subscriber with full options control (Tier B-pull).
    pub async fn create_subscriber_pull_with_options(
        &self,
        topic: &str,
        options: SubscriberOptions,
    ) -> AortaResult<CoreSubscriber> {
        self.inner.key_builder().topic_key_sub(topic)?;
        self.inner
            .create_subscriber_pull_with_options(topic, options)
            .await
    }

    /// Subscribe to a topic on a specific robot namespace (cross-namespace cloud
    /// addressing). `namespace` accepts a single robot (`"R1"`) or `"*"` for
    /// fleet fan-out; the originating namespace of each sample is surfaced via
    /// `MessageContext.source_namespace`. See the Cloud Cross-Namespace
    /// Addressing Proposal §5.
    pub async fn create_subscriber_for<F>(
        &self,
        namespace: Target,
        topic: &str,
        callback: F,
    ) -> AortaResult<CoreSubscriber>
    where
        F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
    {
        self.create_subscriber_for_with_options(
            namespace,
            topic,
            callback,
            SubscriberOptions::default(),
        )
        .await
    }

    /// [`create_subscriber_for`](Self::create_subscriber_for) with explicit
    /// options.
    pub async fn create_subscriber_for_with_options<F>(
        &self,
        namespace: Target,
        topic: &str,
        callback: F,
        options: SubscriberOptions,
    ) -> AortaResult<CoreSubscriber>
    where
        F: Fn(&ZBytes, &MessageContext) + Send + Sync + 'static,
    {
        let options = options.with_namespace(namespace.as_str());
        self.create_subscriber_with_options(topic, callback, options)
            .await
    }

    /// Typed-view subscribe on a specific robot namespace (cross-namespace).
    /// Typed-view subscribe on a specific robot namespace (cross-namespace).
    pub async fn create_subscriber_typed_view_for<M, F>(
        &self,
        namespace: Target,
        topic: &str,
        callback: F,
    ) -> AortaResult<CoreSubscriber>
    where
        M: TypedSubscribeMessage,
        F: for<'a> Fn(M::Root<'a>, &MessageContext) + Send + Sync + 'static,
    {
        self.create_subscriber_typed_view_with_options::<M, F>(
            topic,
            callback,
            SubscriberOptions::default().with_namespace(namespace.as_str()),
        )
        .await
    }

    /// Typed-retained subscribe on a specific robot namespace
    /// (cross-namespace).
    pub async fn create_subscriber_typed_retained_for<M, F>(
        &self,
        namespace: Target,
        topic: &str,
        callback: F,
    ) -> AortaResult<CoreSubscriber>
    where
        M: TypedSubscribeMessage,
        F: Fn(TypedRetainedSample<M>) + Send + Sync + 'static,
    {
        self.create_subscriber_typed_retained_with_options::<M, F>(
            topic,
            callback,
            SubscriberOptions::default().with_namespace(namespace.as_str()),
        )
        .await
    }

    /// Create a raw RPC service: `handler` receives each [`ServiceRequest`]
    /// and may reply at most once (see [`ServiceRequest::reply`] — dropping
    /// the request without replying leaves the caller to time out).
    /// Requires an executor (auto-started).
    pub async fn create_service<F>(&self, service_name: &str, handler: F) -> AortaResult<Service>
    where
        F: Fn(ServiceRequest) + Send + Sync + 'static,
    {
        self.create_service_with_options(service_name, handler, ServiceOptions::default())
            .await
    }

    /// Create a schema-typed service (schema-compat API): `RequestSchema` /
    /// `ResponseSchema` attach the schema BFBS and root types to the service
    /// metadata, but the handler still receives the raw [`ServiceRequest`].
    /// Both schemas must resolve to the **same service schema hash**.
    pub async fn create_service_typed<RequestSchema, ResponseSchema, F>(
        &self,
        service_name: &str,
        handler: F,
    ) -> AortaResult<Service>
    where
        RequestSchema: SchemaMeta,
        ResponseSchema: SchemaMeta,
        F: Fn(ServiceRequest) + Send + Sync + 'static,
    {
        self.create_service_typed_with_options::<RequestSchema, ResponseSchema, F>(
            service_name,
            handler,
            ServiceOptions::default(),
        )
        .await
    }

    /// [`create_service_typed`](Self::create_service_typed) with explicit
    /// [`ServiceOptions`]. Rejects mismatched request/response schema hashes
    /// or root types with `InvalidArgument`.
    pub async fn create_service_typed_with_options<RequestSchema, ResponseSchema, F>(
        &self,
        service_name: &str,
        handler: F,
        mut options: ServiceOptions,
    ) -> AortaResult<Service>
    where
        RequestSchema: SchemaMeta,
        ResponseSchema: SchemaMeta,
        F: Fn(ServiceRequest) + Send + Sync + 'static,
    {
        let typed_request_schema = RequestSchema::schema_hash();
        let typed_response_schema = ResponseSchema::schema_hash();
        if typed_request_schema != typed_response_schema {
            return Err(AortaError::invalid_argument(
                "typed service request/response must use the same service schema hash",
            ));
        }
        let typed_service_bfbs = RequestSchema::schema_bfbs().to_vec();
        let typed_request_root_type = RequestSchema::root_type()
            .map(str::to_string)
            .ok_or_else(|| {
                AortaError::invalid_argument(
                    "typed service request schema must declare root_type() for schema_by_key metadata",
                )
            })?;
        let typed_response_root_type = ResponseSchema::root_type()
            .map(str::to_string)
            .ok_or_else(|| {
                AortaError::invalid_argument(
                    "typed service response schema must declare root_type() for schema_by_key metadata",
                )
            })?;

        if let Some(schema) = options.schema.as_mut() {
            if schema.schema != typed_request_schema {
                return Err(AortaError::invalid_argument(
                    "typed service schema hash does not match request/response schema type",
                ));
            }
            if schema.schema_bfbs.is_empty() {
                schema.schema_bfbs = typed_service_bfbs;
            }
            match schema.request_root_type.as_deref() {
                Some(value) if value != typed_request_root_type.as_str() => {
                    return Err(AortaError::invalid_argument(
                        "typed service request root type does not match request schema type",
                    ));
                }
                None => {
                    schema.request_root_type = Some(typed_request_root_type.clone());
                }
                _ => {}
            }
            match schema.response_root_type.as_deref() {
                Some(value) if value != typed_response_root_type.as_str() => {
                    return Err(AortaError::invalid_argument(
                        "typed service response root type does not match response schema type",
                    ));
                }
                None => {
                    schema.response_root_type = Some(typed_response_root_type.clone());
                }
                _ => {}
            }
        } else {
            options.schema = Some(ServiceSchemaInfo {
                schema: typed_request_schema,
                schema_bfbs: typed_service_bfbs,
                request_root_type: Some(typed_request_root_type),
                response_root_type: Some(typed_response_root_type),
            });
        }

        self.create_service_with_options(service_name, handler, options)
            .await
    }

    /// End-to-end typed service: `handler` receives the **decoded request
    /// view** (`<Request as TypedSubscribeMessage>::Root<'a>`, valid only
    /// inside the handler) plus a [`TypedServiceResponder`] for typed
    /// replies. Decode failures are logged and answered with an error reply.
    ///
    /// ```ignore
    /// use aorta_sdk::prelude::*;
    ///
    /// # async fn demo(node: &Node) -> AortaResult<()> {
    /// let mut service = node
    ///     .create_service_typed_view::<EchoRequestSchema, EchoResponseSchema, _>(
    ///         "/echo",
    ///         |request, reply| {
    ///             let echoed = request.message().unwrap_or("").to_string();
    ///             tokio::spawn(async move {
    ///                 let _ = reply
    ///                     .reply(|msg| msg.echoed_message = Some(echoed))
    ///                     .await;
    ///             });
    ///         },
    ///     )
    ///     .await?;
    ///     service.stop();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See `rust/examples/echo_server.rs` for the complete example.
    pub async fn create_service_typed_view<Request, Response, F>(
        &self,
        service_name: &str,
        handler: F,
    ) -> AortaResult<Service>
    where
        Request: TypedRpcRequestMessage,
        Response: TypedRpcResponseMessage + Send + Sync + 'static,
        F: for<'a> Fn(
                <Request as TypedSubscribeMessage>::Root<'a>,
                TypedServiceResponder<Response>,
            ) + Send
            + Sync
            + 'static,
    {
        self.create_service_typed_view_with_options::<Request, Response, F>(
            service_name,
            handler,
            ServiceOptions::default(),
        )
        .await
    }

    /// [`create_service_typed_view`](Self::create_service_typed_view) with
    /// explicit [`ServiceOptions`].
    pub async fn create_service_typed_view_with_options<Request, Response, F>(
        &self,
        service_name: &str,
        handler: F,
        options: ServiceOptions,
    ) -> AortaResult<Service>
    where
        Request: TypedRpcRequestMessage,
        Response: TypedRpcResponseMessage + Send + Sync + 'static,
        F: for<'a> Fn(
                <Request as TypedSubscribeMessage>::Root<'a>,
                TypedServiceResponder<Response>,
            ) + Send
            + Sync
            + 'static,
    {
        let node_name = Arc::<str>::from(self.name().to_string());
        let sequence = Arc::new(AtomicU64::new(0));
        let service_name = service_name.to_string();
        let create_name = service_name.clone();
        self.create_service_typed_with_options::<Request, Response, _>(
            &create_name,
            move |request| {
                let payload = request.payload().clone();
                let responder = TypedServiceResponder::<Response>::new(
                    request,
                    Arc::clone(&node_name),
                    Arc::clone(&sequence),
                );
                with_contiguous_bytes(&payload, |bytes| match Request::decode_typed_root(bytes) {
                    Ok(decoded) => handler(decoded, responder),
                    Err(err) => {
                        crate::aorta_log_every_n!(
                            crate::logging::LogLevel::Warn,
                            1024,
                            "Aorta WARN: typed service request decode failed on '{}': {}",
                            service_name,
                            err
                        );
                        let service_name = service_name.clone();
                        spawn_logged("typed_service_decode_error_reply", async move {
                            let _ = responder
                                .reply_error(&format!(
                                    "invalid typed request payload for '{}': {}",
                                    service_name, err
                                ))
                                .await;
                        });
                    }
                });
            },
            options,
        )
        .await
    }

    /// Typed service with an **owned** [`TypedRetainedServiceRequest`]: the
    /// handler can keep the request (and decode it later, even from another
    /// task) before replying.
    pub async fn create_service_typed_retained<Request, Response, F>(
        &self,
        service_name: &str,
        handler: F,
    ) -> AortaResult<Service>
    where
        Request: TypedRpcRequestMessage + Send + Sync + 'static,
        Response: TypedRpcResponseMessage + Send + Sync + 'static,
        F: Fn(TypedRetainedServiceRequest<Request, Response>) + Send + Sync + 'static,
    {
        self.create_service_typed_retained_with_options::<Request, Response, F>(
            service_name,
            handler,
            ServiceOptions::default(),
        )
        .await
    }

    /// [`create_service_typed_retained`](Self::create_service_typed_retained)
    /// with explicit [`ServiceOptions`].
    pub async fn create_service_typed_retained_with_options<Request, Response, F>(
        &self,
        service_name: &str,
        handler: F,
        options: ServiceOptions,
    ) -> AortaResult<Service>
    where
        Request: TypedRpcRequestMessage + Send + Sync + 'static,
        Response: TypedRpcResponseMessage + Send + Sync + 'static,
        F: Fn(TypedRetainedServiceRequest<Request, Response>) + Send + Sync + 'static,
    {
        let node_name = Arc::<str>::from(self.name().to_string());
        let sequence = Arc::new(AtomicU64::new(0));
        let service_name = service_name.to_string();
        let create_name = service_name.clone();
        self.create_service_typed_with_options::<Request, Response, _>(
            &create_name,
            move |request| {
                let payload = request.payload().clone();
                with_contiguous_bytes(&payload, |bytes| {
                    if let Err(err) = Request::decode_typed_root(bytes) {
                        crate::aorta_log_every_n!(
                            crate::logging::LogLevel::Warn,
                            1024,
                            "Aorta WARN: typed retained service request decode failed on '{}': {}",
                            service_name,
                            err
                        );
                        let service_name = service_name.clone();
                        spawn_logged("typed_retained_service_decode_error_reply", async move {
                            let _ = request
                                .reply_error(&format!(
                                    "invalid typed request payload for '{}': {}",
                                    service_name, err
                                ))
                                .await;
                        });
                        return;
                    }
                    let typed_request = TypedRetainedServiceRequest::<Request, Response>::new(
                        bytes.to_vec(),
                        request,
                        Arc::clone(&node_name),
                        Arc::clone(&sequence),
                    );
                    handler(typed_request);
                });
            },
            options,
        )
        .await
    }

    /// Raw RPC service with full [`ServiceOptions`] control (schema attach
    /// policy, callback group, RPC overflow policy).
    pub async fn create_service_with_options<F>(
        &self,
        service_name: &str,
        handler: F,
        mut options: ServiceOptions,
    ) -> AortaResult<Service>
    where
        F: Fn(ServiceRequest) + Send + Sync + 'static,
    {
        self.prepare_service_creation(service_name, &mut options)?;

        // The serving task SNAPSHOTS this sender, so the service is registered
        // as a producer that blocks an executor handoff.
        let (service_event_sender, service_producer_guard) = self
            .inner
            .callback_event_sender_for_snapshot_producer()
            .ok_or_else(|| {
                AortaError::internal(
                    "service requires an executor-backed event sender; \
                     ensure Node is registered with an executor before creating services",
                )
            })?;

        let mut builder = CoreService::builder(self.inner.session().clone(), service_name)
            .key_builder(self.inner.key_builder().clone())
            .serve_all_namespaces(options.serve_all_namespaces)
            .node_id(self.inner.id())
            .default_schema_attach_policy(self.inner.schema_attach_policy())
            .schema_registry(self.inner.schema_registry().clone())
            .inventory_telemetry(self.inner.vital_telemetry())
            .runtime_telemetry(self.inner.runtime_metrics_telemetry())
            .event_sender(service_event_sender)
            .snapshot_producer_guard(service_producer_guard)
            .schema_attach_policy(options.schema_attach_policy)
            .rpc_overflow_policy(options.rpc_overflow_policy)
            .rpc_overflow_block_timeout(options.rpc_overflow_block_timeout)
            .callback_group(options.callback_group)
            .handler(move |request| {
                handler(ServiceRequest::from_core(request));
            });

        if let Some(callback_group_handle) = options.callback_group_handle {
            builder = builder.callback_group_handle(callback_group_handle);
        }

        if let Some(schema) = options.schema {
            builder = builder.schema(schema);
        }

        let running = builder.build().start().await?;
        Ok(Service::from_core(running))
    }

    pub(crate) fn prepare_service_creation(
        &self,
        service_name: &str,
        options: &mut ServiceOptions,
    ) -> AortaResult<()> {
        // Validate the service key before starting the internal executor so
        // invalid user input cannot leave behind executor state on an error path.
        // prepare_service_creation is the Service owner path → server-side validator.
        if options.serve_all_namespaces {
            self.inner
                .key_builder()
                .service_key_server_all_namespaces(service_name)?;
        } else {
            self.inner.key_builder().service_key_server(service_name)?;
        }
        self.require_executor("service")?;
        let defaults = self.inner.executor_class_policy_defaults();
        let (resolved_policy, resolved_timeout) = defaults.resolve_rpc_overflow_compat(
            options.rpc_overflow_policy,
            options.rpc_overflow_block_timeout,
        );
        options.rpc_overflow_policy = resolved_policy;
        options.rpc_overflow_block_timeout = resolved_timeout;
        Ok(())
    }

    pub(crate) fn prepare_subscriber_creation(&self, topic: &str) -> AortaResult<()> {
        // Validate the topic key before starting the internal executor so
        // invalid user input cannot leave behind executor state on an error path.
        // Subscriber creation: subscribe-side validator (wildcards allowed).
        self.inner.key_builder().topic_key_sub(topic)?;
        self.require_executor("subscriber")?;
        Ok(())
    }

    /// Create an RPC client for `service_name` (synchronous, no `.await`).
    /// Calls use the default timeout (5 s) and follow the service schema
    /// attach policy of the node.
    pub fn create_client(&self, service_name: &str) -> Client {
        self.create_client_with_options(service_name, ClientOptions::default())
    }

    /// Create an RPC client with an explicit call timeout in milliseconds.
    pub fn create_client_with_timeout(&self, service_name: &str, timeout_ms: u64) -> Client {
        self.create_client_with_options(
            service_name,
            ClientOptions::default().with_timeout_ms(timeout_ms),
        )
    }

    /// Create an RPC client with full [`ClientOptions`] control.
    pub fn create_client_with_options(&self, service_name: &str, options: ClientOptions) -> Client {
        self.build_client(service_name, options, self.inner.key_builder().clone())
    }

    /// Create a client targeting a specific robot namespace (cross-namespace
    /// cloud addressing). A wildcard namespace is rejected — a request/reply
    /// must target exactly one robot (`QueryTarget::All` + first-reply-wins
    /// would otherwise fan a call out across the whole fleet). See the Cloud
    /// Cross-Namespace Addressing Proposal §5.
    pub fn create_client_for(&self, namespace: Target, service_name: &str) -> AortaResult<Client> {
        self.create_client_for_with_options(namespace, service_name, ClientOptions::default())
    }

    /// [`create_client_for`](Self::create_client_for) with explicit options.
    pub fn create_client_for_with_options(
        &self,
        namespace: Target,
        service_name: &str,
        options: ClientOptions,
    ) -> AortaResult<Client> {
        let key_builder = self
            .inner
            .key_builder()
            .clone()
            .with_namespace(namespace.as_str())?;
        // Fail fast: reject a wildcard namespace for request/reply at creation
        // rather than on the first `.call()`.
        key_builder.service_key_client(service_name)?;
        Ok(self.build_client(service_name, options, key_builder))
    }

    fn build_client(
        &self,
        service_name: &str,
        options: ClientOptions,
        key_builder: aorta_core::KeyExprBuilder,
    ) -> Client {
        let mut builder = CoreClient::builder(self.inner.session().clone(), service_name)
            .key_builder(key_builder)
            .default_schema_attach_policy(self.inner.schema_attach_policy())
            .inventory_telemetry(self.inner.vital_telemetry())
            .runtime_telemetry(self.inner.runtime_metrics_telemetry())
            .timeout(options.timeout)
            .schema_attach_policy(options.schema_attach_policy)
            .validate_schema(options.validate_schema);

        if let Some(schema) = options.service_schema {
            builder = builder.service_schema(schema);
        }
        builder = builder.service_root_types(options.request_root_type, options.response_root_type);
        if let Some(enforce) = options.enforce_single_provider {
            builder = builder.enforce_single_provider(enforce);
        }
        if let Some(window) = options.multi_reply_check_window {
            builder = builder.multi_reply_check_window(window);
        }
        if let Some(allow_retry) = options.allow_retry {
            builder = builder.allow_retry(allow_retry);
        }
        if options.validate_schema {
            builder = builder.schema_registry(self.inner.schema_registry().clone());
        }

        Client::from_core(builder.build())
    }

    /// Create a **typed** RPC client (synchronous): `Request` / `Response`
    /// are validated against the options (same schema hash, matching root
    /// types) and their schema metadata is registered automatically
    /// (wire attachment follows `SchemaAttachPolicy`). Returns
    /// `InvalidArgument` on schema mismatch.
    pub fn create_client_typed<Request, Response>(
        &self,
        service_name: &str,
    ) -> AortaResult<TypedClient<Request, Response>>
    where
        Request: TypedRpcRequestMessage,
        Response: TypedRpcResponseMessage,
    {
        self.create_client_typed_with_options::<Request, Response>(
            service_name,
            ClientOptions::default(),
        )
    }

    /// [`create_client_typed`](Self::create_client_typed) with explicit
    /// [`ClientOptions`].
    pub fn create_client_typed_with_options<Request, Response>(
        &self,
        service_name: &str,
        mut options: ClientOptions,
    ) -> AortaResult<TypedClient<Request, Response>>
    where
        Request: TypedRpcRequestMessage,
        Response: TypedRpcResponseMessage,
    {
        validate_typed_client_options::<Request, Response>(&mut options)?;
        Ok(TypedClient::new(
            self.create_client_with_options(service_name, options),
            Arc::<str>::from(self.name().to_string()),
        ))
    }

    /// Typed service client targeting a specific robot namespace
    /// (cross-namespace cloud addressing) — the typed counterpart of
    /// [`Node::create_client_for`]. A wildcard namespace is rejected (request/
    /// reply must target one robot). See the Cloud Cross-Namespace
    /// Addressing Proposal §5.
    /// Typed client on a specific robot namespace — see
    /// [`Node::create_client_for`].
    pub fn create_client_typed_for<Request, Response>(
        &self,
        namespace: Target,
        service_name: &str,
    ) -> AortaResult<TypedClient<Request, Response>>
    where
        Request: TypedRpcRequestMessage,
        Response: TypedRpcResponseMessage,
    {
        self.create_client_typed_for_with_options::<Request, Response>(
            namespace,
            service_name,
            ClientOptions::default(),
        )
    }

    /// [`create_client_typed_for`](Self::create_client_typed_for) with
    /// explicit [`ClientOptions`].
    pub fn create_client_typed_for_with_options<Request, Response>(
        &self,
        namespace: Target,
        service_name: &str,
        mut options: ClientOptions,
    ) -> AortaResult<TypedClient<Request, Response>>
    where
        Request: TypedRpcRequestMessage,
        Response: TypedRpcResponseMessage,
    {
        validate_typed_client_options::<Request, Response>(&mut options)?;
        Ok(TypedClient::new(
            self.create_client_for_with_options(namespace, service_name, options)?,
            Arc::<str>::from(self.name().to_string()),
        ))
    }

    /// Create a periodic timer that schedules `callback` at `period`
    /// intervals.
    ///
    /// The callback is a **synchronous** `Fn()` dispatched by the node's
    /// executor — do not `.await` inside it; use `tokio::spawn` for async
    /// work (see `rust/examples/simple_timer_publish.rs`). By default
    /// (`invoke_on_missed=false`, `CoalesceLatest` overflow) missed ticks
    /// under load are **skipped/coalesced**, not back-filled — use
    /// [`TimerOptions`] to change that.
    pub async fn create_timer<F>(&self, period: Duration, callback: F) -> AortaResult<Timer>
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.require_executor("timer")?;
        self.inner.create_timer(period, callback).await
    }

    /// Create a periodic timer with custom [`TimerOptions`]
    /// (callback group, overflow policy, …).
    pub async fn create_timer_with_options<F>(
        &self,
        period: Duration,
        callback: F,
        options: TimerOptions,
    ) -> AortaResult<Timer>
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.require_executor("timer")?;
        self.inner
            .create_timer_with_options(period, callback, options)
            .await
    }
}

fn default_sdk_telemetry_metadata() -> CoreTelemetryNodeMetadata {
    CoreTelemetryNodeMetadata {
        executable: None,
        language: Some("rust".to_string()),
        sdk_name: Some("aorta_rust".to_string()),
        sdk_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        labels: Default::default(),
        build_id: None,
        container_id: None,
    }
}

fn merge_sdk_telemetry_metadata(
    mut telemetry: CoreTelemetryNodeMetadata,
) -> CoreTelemetryNodeMetadata {
    let defaults = default_sdk_telemetry_metadata();
    if telemetry.language.is_none() {
        telemetry.language = defaults.language;
    }
    if telemetry.sdk_name.is_none() {
        telemetry.sdk_name = defaults.sdk_name;
    }
    if telemetry.sdk_version.is_none() {
        telemetry.sdk_version = defaults.sdk_version;
    }
    telemetry
}

fn inject_header_layout_hint_from_schema<M: SchemaMeta>(options: &mut SubscriberOptions) {
    if options.header_layout_hint.is_some() {
        return;
    }
    let parse_header_for_strategy =
        options.enable_header_context || options.enable_dedup || options.enable_reorder_check;
    if !parse_header_for_strategy {
        return;
    }
    if let Some(layout) = infer_aorta_header_layout_from_bfbs(M::schema_bfbs()) {
        options.header_layout_hint = Some(layout);
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("name", &self.name())
            .field("group", &self.group())
            .finish()
    }
}

impl Drop for Node {
    /// Synchronous half of teardown for the last `Node` clone:
    /// without this, dropping a `Node` that was
    /// never `close().await`-ed leaked the auto-executor thread forever —
    /// and that thread holds a `CoreNode` clone, so the Zenoh session and
    /// every registered primitive leaked with it. C++'s `~Node()` already
    /// tears down automatically; this closes the cross-SDK gap.
    ///
    /// Prefer `node.close().await` — it also closes the session gracefully.
    fn drop(&mut self) {
        // Only the last clone acts: the executor slot Arc is private to the
        // Node clones, so a strong count of 1 means no other clone remains.
        if Arc::strong_count(&self.executor) != 1 {
            return;
        }
        let handle = self.lock_executor().take();
        if let Some(mut handle) = handle {
            crate::aorta_warn!(
                "Aorta WARN: node '{}' dropped without close(); stopping the \
                 auto-executor thread synchronously — prefer `node.close().await` \
                 for graceful teardown",
                self.inner.name()
            );
            // `stop()` skips the join when called from the auto-executor's own
            // thread — possible if the last clone is dropped inside a callback
            // it is running, where a self-join would deadlock.
            handle.stop();
        }
    }
}

#[cfg(test)]
mod node_lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use std::thread;

    fn unique_name(base: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{}_p{}_{}", base, std::process::id(), id)
    }

    // Regression: ensure_executor() panicked on a poisoned
    // executor lock while close() silently swallowed the same condition.
    // Both must recover.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_lock_poison_is_recovered_consistently() {
        let node = Node::new(unique_name("sdk_poison"), "default")
            .await
            .expect("node");
        let executor = Arc::clone(&node.executor);
        let _ = thread::spawn(move || {
            let _guard = executor.lock().expect("first lock");
            panic!("intentional poison");
        })
        .join();
        assert!(node.executor.is_poisoned(), "lock must be poisoned");

        node.ensure_executor()
            .expect("a fresh node's auto-executor must attach"); // must not panic
        node.close()
            .await
            .expect("close must still stop the executor");
    }

    // Regression: only the LAST clone's drop tears the
    // auto-executor down.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_a_non_last_clone_keeps_the_executor() {
        let node = Node::new(unique_name("sdk_clone_drop"), "default")
            .await
            .expect("node");
        node.ensure_executor().expect("auto-executor");
        let clone = node.clone();
        drop(node);
        assert!(
            clone.lock_executor().is_some(),
            "dropping a non-last clone must not stop the executor"
        );
        clone.close().await.expect("close");
    }
}
