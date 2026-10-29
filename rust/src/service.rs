use aorta_core::event_queue::{CallbackGroupHandle, CallbackGroupType};
use aorta_core::schema::SchemaAttachPolicy;
use aorta_core::service::{
    RpcOverflowPolicy, RunningService as CoreRunningService, ServiceRequest as CoreServiceRequest,
    ServiceStats as CoreServiceStats,
};
use aorta_core::ZBytes;
use std::time::Duration;

use crate::AortaResult;

/// Rust SDK service options for RPC server setup.
///
/// Build with [`ServiceOptions::new`] plus the `with_*` setters, or
/// construct the struct directly (all fields are public). Pass to
/// [`Node::create_service_with_options`](crate::Node::create_service_with_options)
/// or the typed variants.
#[derive(Clone, Debug)]
pub struct ServiceOptions {
    /// Serve requests arriving from **any** namespace by declaring the
    /// service on an all-namespaces key. Requires a namespace-free Zenoh
    /// session — service creation fails otherwise. Default: `false`
    /// (serve only this node's namespace).
    pub serve_all_namespaces: bool,
    /// When (and whether) the service's schema metadata is attached to its
    /// declarations. Default: [`SchemaAttachPolicy::Never`] — callers must
    /// know the schema out-of-band.
    pub schema_attach_policy: SchemaAttachPolicy,
    /// Callback group kind for the service handler. Default:
    /// [`CallbackGroupType::MutuallyExclusive`] (serialized handling).
    pub callback_group: CallbackGroupType,
    /// Explicit callback group handle (e.g. from an
    /// [`ExecutionGroup`](crate::ExecutionGroup)) to join, overriding
    /// `callback_group`. Default: `None` — requests route to the
    /// executor-wide default group for the `callback_group` kind.
    pub callback_group_handle: Option<CallbackGroupHandle>,
    /// Behavior when the executor's admission queue is full at request
    /// arrival (the queue is shared with other subscribers/services/timers
    /// on the same executor). Default: [`RpcOverflowPolicy::FastFailBusy`].
    pub rpc_overflow_policy: RpcOverflowPolicy,
    /// For blocking overflow policies: how long to wait for a free queue
    /// slot before giving up. Default: 0 ms.
    pub rpc_overflow_block_timeout: Duration,
    /// Service schema metadata (hash, BFBS, root types). The typed
    /// `create_service_typed*` factories fill this in automatically.
    pub schema: Option<ServiceSchemaInfo>,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            serve_all_namespaces: false,
            schema_attach_policy: SchemaAttachPolicy::Never,
            callback_group: CallbackGroupType::MutuallyExclusive,
            callback_group_handle: None,
            rpc_overflow_policy: RpcOverflowPolicy::FastFailBusy,
            rpc_overflow_block_timeout: Duration::from_millis(0),
            schema: None,
        }
    }
}

impl ServiceOptions {
    /// Create default service options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Serve requests from any namespace — see
    /// [`ServiceOptions::serve_all_namespaces`] for the session requirement.
    pub fn with_serve_all_namespaces(mut self, serve_all_namespaces: bool) -> Self {
        self.serve_all_namespaces = serve_all_namespaces;
        self
    }

    /// Set when the service schema metadata is attached to declarations.
    pub fn with_schema_attach_policy(mut self, policy: SchemaAttachPolicy) -> Self {
        self.schema_attach_policy = policy;
        self
    }

    /// Set the callback group kind for the service handler.
    pub fn with_callback_group(mut self, callback_group: CallbackGroupType) -> Self {
        self.callback_group = callback_group;
        self
    }

    /// Join an explicit callback group (e.g. one owned by an
    /// [`ExecutionGroup`](crate::ExecutionGroup)).
    pub fn with_callback_group_handle(
        mut self,
        callback_group_handle: CallbackGroupHandle,
    ) -> Self {
        self.callback_group_handle = Some(callback_group_handle);
        self
    }

    /// Set the RPC overflow policy (behavior when the executor admission
    /// queue is full at request arrival).
    pub fn with_rpc_overflow_policy(mut self, policy: RpcOverflowPolicy) -> Self {
        self.rpc_overflow_policy = policy;
        self
    }

    /// Set how long a blocking overflow policy waits for a free queue slot.
    pub fn with_rpc_overflow_block_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_overflow_block_timeout = timeout;
        self
    }

    /// Attach explicit service schema metadata (hash, BFBS, root types).
    pub fn with_schema(mut self, schema: ServiceSchemaInfo) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Rust SDK service request wrapper.
///
/// Delivered to the service handler. The `reply`-family methods **consume
/// `self`**, so a request can be replied to **at most once** — enforced at
/// compile time. Dropping the request without replying is allowed and
/// leaves the caller to time out.
///
/// Handlers are synchronous (`Fn(ServiceRequest)`); to reply from an async
/// context, move the request into a `tokio::spawn` (see
/// `rust/examples/echo_server.rs`).
pub struct ServiceRequest {
    inner: CoreServiceRequest,
}

impl ServiceRequest {
    pub(crate) fn from_core(inner: CoreServiceRequest) -> Self {
        Self { inner }
    }

    /// The raw request payload bytes.
    pub fn payload(&self) -> &ZBytes {
        self.inner.payload()
    }

    /// Returns the transport-level routing namespace parsed from the arriving
    /// key. This is not an authenticated identity; trust requires router ACLs
    /// that pin each link to its allowed namespace prefix.
    pub fn caller_namespace(&self) -> Option<&str> {
        self.inner.caller_namespace()
    }

    /// Reply to the request with a raw byte payload, consuming it.
    pub async fn reply<IntoZBytes>(self, response: IntoZBytes) -> AortaResult<()>
    where
        IntoZBytes: Into<ZBytes>,
    {
        self.inner.reply(response).await
    }

    /// Reply with a textual error message, consuming the request. The
    /// caller receives an error carrying `error_msg`.
    pub async fn reply_error(self, error_msg: &str) -> AortaResult<()> {
        self.inner.reply_error(error_msg).await
    }
}

/// Rust SDK running service wrapper.
///
/// Created by [`Node::create_service*`](crate::Node::create_service) — the
/// service starts serving immediately. Dropping it (or calling
/// [`stop`](Self::stop)) stops serving; keep it bound for the service
/// lifetime.
#[must_use = "dropping a Service stops serving; bind it for the service lifetime"]
pub struct Service {
    inner: CoreRunningService,
}

impl Service {
    pub(crate) fn from_core(inner: CoreRunningService) -> Self {
        Self { inner }
    }

    /// Access the underlying core running service (advanced use).
    pub fn inner(&self) -> &CoreRunningService {
        &self.inner
    }

    /// The service name (key) this service serves.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Stop serving (non-blocking). Requires `&mut self` — ensure no other
    /// reference is using the service.
    pub fn stop(&mut self) {
        self.inner.stop();
    }

    /// Snapshot of this service's runtime statistics (request count,
    /// latency, overflow drops, …).
    pub fn stats(&self) -> CoreServiceStats {
        self.inner.stats()
    }
}

pub use aorta_core::service::ServiceSchemaInfo;
pub use aorta_core::service::ServiceStats;
