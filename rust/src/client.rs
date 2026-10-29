use std::time::Duration;

use aorta_core::client::{Client as CoreClient, ClientStats as CoreClientStats};
use aorta_core::schema::{SchemaAttachPolicy, SchemaHash};
use aorta_core::ZBytes;

use crate::AortaResult;

const DEFAULT_TIMEOUT_MS: u64 = 5000;

/// Rust SDK client options for RPC calls.
///
/// Build with [`ClientOptions::new`] plus the `with_*` setters, or
/// construct the struct directly (all fields are public). Pass to
/// [`Node::create_client_with_options`](crate::Node::create_client_with_options)
/// or the typed client factories.
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Per-call timeout (default 5000 ms).
    pub timeout: Duration,
    /// When the request's schema metadata is attached. Default:
    /// [`SchemaAttachPolicy::Inherit`] (follow the node policy).
    pub schema_attach_policy: SchemaAttachPolicy,
    /// Service schema hash this client declares; used for schema
    /// verification. `None` = no explicit schema.
    pub service_schema: Option<SchemaHash>,
    /// FlatBuffers root type of the request message, when schema metadata
    /// is attached.
    pub request_root_type: Option<String>,
    /// FlatBuffers root type of the response message.
    pub response_root_type: Option<String>,
    /// Best-effort schema comparison of responses against the declared
    /// service schema. **Warn-only**: a mismatch is logged and the response
    /// is still delivered — it never rejects a reply. Default `false`.
    pub validate_schema: bool,
    /// Enforce a single active provider for this service. `None` inherits
    /// the core default; enable for non-idempotent services.
    pub enforce_single_provider: Option<bool>,
    /// Window in which a second (unexpected) provider reply is detected
    /// when `enforce_single_provider` is on.
    pub multi_reply_check_window: Option<Duration>,
    /// Per-client RPC retry opt-in. `None` inherits the core default (OFF —
    /// retries are disabled because they silently re-invoke non-idempotent
    /// providers). `Some(true)` re-enables the backoff retry loop for a
    /// provably idempotent service.
    pub allow_retry: Option<bool>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            schema_attach_policy: SchemaAttachPolicy::Inherit,
            service_schema: None,
            request_root_type: None,
            response_root_type: None,
            validate_schema: false,
            enforce_single_provider: None,
            multi_reply_check_window: None,
            allow_retry: None,
        }
    }
}

impl ClientOptions {
    /// Create default client options (5 s timeout, node-inherited schema
    /// attach policy, no retries).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the per-call timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the per-call timeout in milliseconds.
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout = Duration::from_millis(timeout_ms);
        self
    }

    /// Set when the request's schema metadata is attached.
    pub fn with_schema_attach_policy(mut self, policy: SchemaAttachPolicy) -> Self {
        self.schema_attach_policy = policy;
        self
    }

    /// Declare the service schema hash this client speaks.
    pub fn with_service_schema(mut self, schema: SchemaHash) -> Self {
        self.service_schema = Some(schema);
        self
    }

    /// Declare the request FlatBuffers root type.
    pub fn with_request_root_type(mut self, root_type: impl Into<String>) -> Self {
        self.request_root_type = Some(root_type.into());
        self
    }

    /// Declare the response FlatBuffers root type.
    pub fn with_response_root_type(mut self, root_type: impl Into<String>) -> Self {
        self.response_root_type = Some(root_type.into());
        self
    }

    /// Declare both request and response root types.
    pub fn with_service_root_types(
        mut self,
        request_root_type: impl Into<String>,
        response_root_type: impl Into<String>,
    ) -> Self {
        self.request_root_type = Some(request_root_type.into());
        self.response_root_type = Some(response_root_type.into());
        self
    }

    /// Enable best-effort response schema validation (warn-only: mismatches
    /// are logged, never rejected).
    pub fn with_validate_schema(mut self, validate: bool) -> Self {
        self.validate_schema = validate;
        self
    }

    /// Opt in to automatic RPC retries for this client. OFF by default because
    /// retries silently re-invoke the provider, corrupting state for
    /// non-idempotent services. Enable ONLY when the target service is
    /// provably idempotent-safe.
    pub fn with_allow_retry(mut self, allow: bool) -> Self {
        self.allow_retry = Some(allow);
        self
    }

    /// Enforce a single active provider for this service (fail fast if a
    /// second provider replies). Recommended for non-idempotent services.
    pub fn with_enforce_single_provider(mut self, enforce: bool) -> Self {
        self.enforce_single_provider = Some(enforce);
        self
    }

    /// Set the window in which a second (unexpected) provider reply is
    /// detected when single-provider enforcement is on.
    pub fn with_multi_reply_check_window(mut self, window: Duration) -> Self {
        self.multi_reply_check_window = Some(window);
        self
    }

    /// Set the multi-reply check window in milliseconds.
    pub fn with_multi_reply_check_window_ms(mut self, window_ms: u64) -> Self {
        self.multi_reply_check_window = Some(Duration::from_millis(window_ms));
        self
    }
}

/// Rust SDK RPC client wrapper, created via
/// [`Node::create_client*`](crate::Node::create_client).
///
/// Each `call` sends a request to the service key and awaits the reply
/// (bounded by the client's timeout). The client is `Clone`-free; share it
/// behind an `Arc` for concurrent callers.
///
/// # Example
///
/// ```no_run
/// use aorta_sdk::prelude::*;
///
/// # async fn demo(node: &Node) -> AortaResult<()> {
/// let client = node.create_client_with_timeout("/echo", 3000);
/// let response = client.call(b"ping".to_vec()).await?;
/// let _ = response;
/// # Ok(())
/// # }
/// ```
pub struct Client {
    inner: CoreClient,
}

impl Client {
    pub(crate) fn from_core(inner: CoreClient) -> Self {
        Self { inner }
    }

    /// Access the underlying core client (advanced use).
    pub fn inner(&self) -> &CoreClient {
        &self.inner
    }

    /// The service name (key) this client targets.
    pub fn service_name(&self) -> &str {
        self.inner.service_name()
    }

    /// Call inside a Flow [`Scope`](aorta_core::flow::Scope) (Flow Concepts
    /// §6.1): inherits the scope budget (narrow, never widen) and speaks
    /// the Flow completion vocabulary — scope cancel →
    /// `Stopped::cancelled(reason)` immediately, deadline expiry →
    /// `Stopped::cancelled(DeadlineExceeded)`, transport/service failure
    /// → `Stopped::fault(message)`.
    pub async fn call_in<IntoZBytes>(
        &self,
        scope: &aorta_core::flow::Scope,
        request: IntoZBytes,
    ) -> aorta_core::flow::Outcome<ZBytes>
    where
        IntoZBytes: Into<ZBytes>,
    {
        self.inner.call_in(scope, request).await
    }

    /// Perform an RPC call with the client's default timeout. Returns the
    /// raw response payload on success.
    pub async fn call<IntoZBytes>(&self, request: IntoZBytes) -> AortaResult<ZBytes>
    where
        IntoZBytes: Into<ZBytes>,
    {
        self.inner.call(request).await
    }

    /// Perform an RPC call with an explicit timeout override (ms).
    pub async fn call_with_timeout<IntoZBytes>(
        &self,
        request: IntoZBytes,
        timeout_ms: u64,
    ) -> AortaResult<ZBytes>
    where
        IntoZBytes: Into<ZBytes>,
    {
        self.inner.call_with_timeout(request, timeout_ms).await
    }

    /// Snapshot of this client's runtime statistics (call count, latency,
    /// error counters).
    pub fn stats(&self) -> CoreClientStats {
        self.inner.stats()
    }

    /// One-shot readiness check: is there at least one matching service
    /// provider for this client's service key right now? Reads local routing
    /// tables (no network round-trip); resolved via `.await`.
    pub async fn matching_status(&self) -> AortaResult<bool> {
        self.inner.matching_status().await
    }

    /// Wait (bounded by `timeout`) until a matching service provider appears.
    /// Returns `Ok(true)` once visible, `Ok(false)` on timeout. Use to gate the
    /// first `call()` on a fresh client.
    pub async fn wait_for_matching(&self, timeout: std::time::Duration) -> AortaResult<bool> {
        self.inner.wait_for_matching(timeout).await
    }
}

pub use aorta_core::client::ClientStats;
