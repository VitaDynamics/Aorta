//! # Aorta Rust SDK
//!
//! Aorta is a Zenoh-based pub/sub middleware for robotics and real-time
//! systems. This crate is the high-level Rust SDK: it layers typed
//! publishers/subscribers, RPC (services and clients), long-running
//! cancellable actions, timers, execution groups, logging, and a unified
//! time source on top of the [`aorta_core`] runtime.
//!
//! The SDK is node-centric: create a [`Node`], then ask it for most
//! primitives you need (the action API constructs its server/client
//! directly, taking a `&Node`). All communication keys follow the
//! `aorta/{group}/{primitive}/{category}/{name}` format, and the effective
//! `group` is resolved by the core from the `AORTA_GROUP` environment
//! variable (falling back to `default`).
//!
//! # Quick start
//!
//! The [`prelude`] module re-exports the golden path for application code:
//!
//! ```no_run
//! use aorta_sdk::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> AortaResult<()> {
//!     let node = Node::new("talker", "default").await?;
//!
//!     // Raw (schema-free) publish/subscribe; the typed variants build on
//!     // FlatBuffers schemas — see the crate-level examples in rust/examples/.
//!     // Bind the subscriber FIRST and keep it alive, then publish: messages
//!     // published before a matching subscriber exists are not replayed.
//!     let _subscriber = node
//!         .create_subscriber("/demo/raw", |payload, context| {
//!             let _ = (payload, context);
//!         })
//!         .await?;
//!
//!     let publisher = node.create_publisher_raw("/demo/raw").await?;
//!     publisher.wait_for_matching(std::time::Duration::from_secs(2)).await?;
//!     publisher.publish(b"hello".to_vec())?;
//!
//!     node.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Async model
//!
//! The SDK is built on tokio: `Node` creation, `close()`, service calls and
//! action operations are `async`, while `publish()` is synchronous
//! fire-and-forget — no `.await`, no acknowledgement (though under
//! `CongestionControl::Block` a publish may block while the transport is
//! backpressured). Callback-capable primitives (subscribers, services,
//! timers) are dispatched by an executor that the SDK auto-starts on first
//! use; application code can instead attach a custom
//! [`SingleThreadedExecutor`] / [`MultiThreadedExecutor`] via
//! [`ExecutorNodeAttachExt`] *before* creating any callback-capable
//! primitive — and must then drive it with `spin()` / `spin_once()`
//! (attach alone does not dispatch callbacks).
//!
//! # Module map
//!
//! | Module | Contents |
//! |---|---|
//! | [`node`] | [`Node`], the primitive factory |
//! | [`publisher`] | Typed FlatBuffers publisher with builder management |
//! | [`typed_subscribe`] | Retained typed samples for subscribers |
//! | [`service`] | Raw RPC server (`ServiceRequest` / `Service`) |
//! | [`typed_service`] | Schema-typed RPC client/server wrappers |
//! | [`client`] | Raw RPC client |
//! | [`action`] | Long-running cancellable tasks (action server/client) |
//! | [`execution_group`] | Serial/concurrent callback ordering |
//! | [`executor_ext`] | Attaching a node to an external executor |
//! | Flow (crate-root re-exports) | In-process structured concurrency, cancellation and budgets (`Scope`, `TaskHandle`, `when_all`, `offload`, …) — distinct from actions, which are a cross-node protocol; `ActionExecutionContext::scope()` links the two |
//! | [`logging`] | Logging init, macros, level checks |
//! | [`header`] | Aorta header injection types |
//! | [`contextstore`] | Shared state with late-joiner snapshot semantics |
//! | [`zbytes`] | Contiguous-byte-slice helper |
//!
//! The full usage guide (English/Chinese) lives in `rust/README.md` and the
//! architecture contracts in `spec/`.
#![allow(dead_code)]

/// Actions: long-running, cancellable tasks with progress feedback
/// (server/client, goal handles, feedback streams).
pub mod action;
/// Raw (schema-free) RPC client for service request/reply.
pub mod client;
pub mod contextstore;
/// Execution ordering for callback groups: [`ExecutionPolicy`] and
/// [`ExecutionGroup`] bind subscribers/services/timers to one serial or
/// concurrent callback domain.
pub mod execution_group;
/// Attach an SDK [`Node`] to an external single/multi-threaded executor.
pub mod executor_ext;
/// Aorta header types: [`HeaderInjector`], [`HeaderValues`] and
/// [`TypedPublishMessage`] (re-exported from core).
pub mod header;
/// Logging: initialization, level checks, the `aorta_log*` macro family.
pub mod logging;
/// The [`Node`] — central factory for most communication primitives.
pub mod node;
/// Typed FlatBuffers publisher with automatic Aorta header injection and
/// internal builder caching/recycling.
pub mod publisher;
/// Raw RPC server: [`Service`], [`ServiceRequest`], [`ServiceOptions`].
pub mod service;
/// Schema-typed RPC: [`TypedClient`], [`TypedServiceResponder`] and
/// retained/borrowed request handling.
pub mod typed_service;
/// Typed subscription support, including the retained-sample type.
pub mod typed_subscribe;
/// `ZBytes` helpers (contiguous byte access).
pub mod zbytes;

pub use action::{
    ActionClient, ActionClientOptions, ActionExecutionContext, ActionFeedbackSample,
    ActionFeedbackStream, ActionServer, ActionServerOptions, FeedbackOverflowPolicy, GoalGuard,
    GoalHandle, ResultObserver, RetainedActionGoal, DEFAULT_ACTION_FEEDBACK_CAPACITY,
};
pub use aorta_core::action::{
    goal_id_to_key, goal_key_to_id, resolve_action_root_types_from_bfbs, ActionCancelGoalResponse,
    ActionGetResultResponse, ActionInfo, ActionPublicStatus, ActionReplyCode, ActionRootType,
    ActionRootTypeNames, ActionSchema, ActionSendGoalResponse, ActionStatus, ActionWire,
    ACTION_INFO_ROOT_TYPE,
};
pub use aorta_core::action_server::DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS;
pub use aorta_core::error::{AortaError, AortaResult, TransportError};
// The event/executor PLUMBING types below (Event, EventQueue*, EventKind,
// InflightGuard, MultiIngressOptions, LaneHook*, ForwardOutcome) are
// crate-root exports ONLY, not prelude members: they are reachable through
// exported expert methods (`queue()`, `push_event`, `lane_hooks()`,
// `Timer::new`), so nameability requires the export — but the prelude stays
// the curated golden path and does not advertise them.
pub use aorta_core::event_queue::{
    AdmissionDropReason, AdmissionStats, AdmissionStatsReader, CallbackGroupHandle,
    CallbackGroupType, Event, EventKind, EventPriority, EventQueue, EventQueueOptions,
    EventQueueSender, InflightGuard, MultiIngressOptions, ReservedLaneStats,
};
pub use aorta_core::executor::{
    ExecutorAdmissionOptions, ExecutorClassPolicyOptions, ExecutorOptions, ExecutorSpinStats,
    ExecutorSpinStatsReader, ForwardOutcome, LaneHookSlot, LaneHooks, MultiThreadedExecutor,
    PrioritySchedulerOptions, SchedulerMode, SingleThreadedExecutor, SpinOutcome, StopHandle,
};
pub use aorta_core::flow::introspect as flow_introspect;
pub use aorta_core::flow::readiness::{
    Condition, ReadinessSet, ReadinessWire, WindowStamp, WindowStats, WindowSync,
};
pub use aorta_core::flow::{
    offload, offload_in, when_all, when_any, ActionScope, Budget, CancelReason, CancelToken,
    Cancelled, Either, ExternalChild, Outcome, RawCancelReason, Scope, ScopeOpts, Shield,
    StopReason, Stopped, Supervision, TaskHandle,
};
pub use aorta_core::new_uuid_v7;
/// Options for creating a (core-level) publisher, used by
/// [`Node::create_publisher_with_options`](crate::Node::create_publisher_with_options)
/// and [`Publisher`].
///
/// Key knobs: [`qos`](aorta_core::publisher::PublisherOptions::qos)
/// (priority / reliability / congestion control / express),
/// [`max_message_size`](aorta_core::publisher::PublisherOptions::max_message_size)
/// (expected payload size — avoids repeated builder growth; when `None`,
/// the initial builder capacity comes from the schema's `fixed_size()` or
/// defaults to 4096 bytes for variable-size messages),
/// [`schema_attach_policy`](aorta_core::publisher::PublisherOptions::schema_attach_policy),
/// [`include_publish_timestamp`](aorta_core::publisher::PublisherOptions::include_publish_timestamp),
/// and the per-publisher congestion-control / express overrides.
///
/// ```ignore
/// use aorta_sdk::prelude::*;
///
/// # async fn demo(node: &Node) -> AortaResult<()> {
/// let options = PublisherOptions::new()
///     .with_qos(QoS::realtime_control())
///     .with_max_message_size(4096);
/// let publisher = node
///     .create_publisher_with_options::<ChatSchema>("/demo/ctrl", options)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub use aorta_core::publisher::PublisherOptions;
pub use aorta_core::qos::{CongestionControl, Priority, QoS, Reliability};
pub use aorta_core::schema::{AortaHeaderLayout, SchemaAttachPolicy, SchemaHash, SchemaMeta};
pub use aorta_core::service::RpcOverflowPolicy;
pub use aorta_core::subscriber::{
    DeliveryMode, FlowAdmission, MessageContext, PullNotifyHook, PullRecvHandle, PullSample,
    ReceiveOverflow, Subscriber, SubscriberOptions, SubscriberOverflowPolicy,
    SubscriberRuntimeStats,
};
pub use aorta_core::telemetry::TelemetryNodeMetadata;
pub use aorta_core::time::Time;
pub use aorta_core::time::{
    monotonic_ns, now, now_ns, set_simulation_time_ns, set_time_source, simulation_time_ns,
    time_source, unix_time_ns, TimeSource,
};
pub use aorta_core::timer::{Timer, TimerOptions, TimerOverflowPolicy};
pub use aorta_core::watch::{SeqPayload, Watch, WatchGuard};
pub use aorta_core::Target;
// Tracing (distinct from telemetry/logging): the core tracing engine + the
// public user-instrumentation macros, exposed on the Rust SDK. `trace` is the
// single core engine used in-process (the Rust SDK does not go through the
// C-ABI). See `aorta_trace_span!` / `aorta_trace_record!`.
pub use aorta_core::trace;
pub use aorta_core::ZBytes;
pub use aorta_core::{
    aorta_trace_counter, aorta_trace_instant, aorta_trace_record, aorta_trace_span,
};
pub use client::{Client, ClientOptions, ClientStats};
pub use execution_group::{ExecutionGroup, ExecutionPolicy};
pub use executor_ext::{spawn_executor_thread, ExecutorNodeAttachExt, ExecutorThread};
pub use header::{HeaderInjector, HeaderValues, TypedPublishMessage};
pub use logging::{
    configure_logging, flush_logging, init_from_env_once, is_log_enabled, is_logging_initialized,
    log_at, set_default_context, LogConfig, LogFormat, LogLevel, LogTopicConfig,
};
pub use node::{Node, NodeOptions};
pub use publisher::Publisher;
pub use publisher::{BuilderGrowthPolicy, BuilderReusePolicy};
pub use service::{Service, ServiceOptions, ServiceRequest, ServiceSchemaInfo, ServiceStats};
pub use typed_service::{
    TypedClient, TypedClientResponse, TypedRetainedServiceRequest, TypedRpcRequestMessage,
    TypedRpcResponseMessage, TypedServiceResponder,
};
pub use typed_subscribe::{TypedRetainedSample, TypedSubscribeMessage};
pub use zbytes::with_contiguous_bytes;

/// Logging macros (`aorta_info!`, `aorta_warn!`, …) plus the level/scope
/// helpers they use. Import via `use aorta_sdk::log_prelude::*;` or the
/// full [`prelude`].
pub mod log_prelude {
    pub use crate::{
        aorta_debug, aorta_error, aorta_info, aorta_log, aorta_log_every_n, aorta_log_if,
        aorta_trace, aorta_warn,
    };
}

/// Common Rust SDK imports for application code.
///
/// Typical usage:
/// `use aorta_sdk::prelude::*;`
pub mod prelude {
    pub use crate::log_prelude::*;
    pub use crate::{
        configure_logging, flush_logging, goal_id_to_key, goal_key_to_id, init_from_env_once,
        is_log_enabled, is_logging_initialized, new_uuid_v7, now, now_ns,
        resolve_action_root_types_from_bfbs, set_default_context, set_simulation_time_ns,
        set_time_source, simulation_time_ns, time_source, unix_time_ns, with_contiguous_bytes,
        ActionCancelGoalResponse, ActionClient, ActionClientOptions, ActionExecutionContext,
        ActionFeedbackSample, ActionFeedbackStream, ActionGetResultResponse, ActionInfo,
        ActionPublicStatus, ActionReplyCode, ActionRootType, ActionRootTypeNames, ActionSchema,
        ActionScope, ActionSendGoalResponse, ActionServer, ActionServerOptions, ActionStatus,
        ActionWire, AdmissionDropReason, AdmissionStats, AdmissionStatsReader, AortaError,
        AortaHeaderLayout, AortaResult, Budget, BuilderGrowthPolicy, BuilderReusePolicy,
        CallbackGroupHandle, CallbackGroupType, CancelReason, CancelToken, Cancelled, Client,
        ClientOptions, ClientStats, Condition, CongestionControl, Either, EventPriority,
        ExecutionGroup, ExecutionPolicy, ExecutorAdmissionOptions, ExecutorClassPolicyOptions,
        ExecutorNodeAttachExt, ExecutorOptions, ExternalChild, FeedbackOverflowPolicy,
        FlowAdmission, GoalGuard, GoalHandle, HeaderInjector, HeaderValues, LogConfig, LogFormat,
        LogLevel, LogTopicConfig, MessageContext, MultiThreadedExecutor, Node, NodeOptions,
        Outcome, Priority, PrioritySchedulerOptions, Publisher, PublisherOptions, PullNotifyHook,
        QoS, RawCancelReason, ReadinessSet, ReadinessWire, Reliability, ResultObserver,
        RetainedActionGoal, RpcOverflowPolicy, SchedulerMode, SchemaAttachPolicy, SchemaHash,
        SchemaMeta, Scope, ScopeOpts, SeqPayload, Service, ServiceOptions, ServiceRequest,
        ServiceSchemaInfo, ServiceStats, Shield, SingleThreadedExecutor, SpinOutcome, StopHandle,
        StopReason, Stopped, Subscriber, SubscriberOptions, SubscriberOverflowPolicy,
        SubscriberRuntimeStats, Supervision, TaskHandle, TelemetryNodeMetadata, Time, TimeSource,
        Timer, TimerOptions, TimerOverflowPolicy, TransportError, TypedClient, TypedClientResponse,
        TypedPublishMessage, TypedRetainedSample, TypedRetainedServiceRequest,
        TypedRpcRequestMessage, TypedRpcResponseMessage, TypedServiceResponder,
        TypedSubscribeMessage, Watch, WatchGuard, WindowStamp, WindowStats, WindowSync, ZBytes,
        ACTION_INFO_ROOT_TYPE, DEFAULT_ACTION_FEEDBACK_CAPACITY,
        DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS,
    };
    pub use crate::{offload, offload_in, when_all, when_any};
    pub use flatbuffers::FlatBufferBuilder;
}

#[cfg(test)]
#[path = "../test/action_api_tests.rs"]
mod action_api_tests;
#[cfg(test)]
#[path = "../test/contextstore_sdk_tests.rs"]
mod contextstore_sdk_tests;
#[cfg(test)]
#[path = "../test/executor_api_tests.rs"]
mod executor_api_tests;
#[cfg(test)]
#[path = "../test/logging_tests.rs"]
mod logging_tests;
#[cfg(test)]
#[path = "../test/node_tests.rs"]
mod node_tests;
#[cfg(test)]
#[path = "../test/publisher_api_tests.rs"]
mod publisher_api_tests;
#[cfg(test)]
#[path = "../test/publisher_reuse_tests.rs"]
mod publisher_reuse_tests;
#[cfg(test)]
#[path = "../test/publisher_stats_tests.rs"]
mod publisher_stats_tests;
#[cfg(test)]
#[path = "../test/rpc_api_tests.rs"]
mod rpc_api_tests;
#[cfg(test)]
#[path = "../test/trace_sdk_tests.rs"]
mod trace_sdk_tests;
#[cfg(test)]
#[path = "../test/trace_throughput_bench.rs"]
mod trace_throughput_bench;
