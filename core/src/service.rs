//! Service (RPC server) implementation for Aorta middleware.
//!
//! A Service listens on a key expression and handles incoming requests,
//! returning responses to clients.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aorta_sync::InFlightCounter;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;
use zenoh::query::Query;

use crate::error::{AortaError, AortaResult};
use crate::event_queue::{
    CallbackGroupHandle, CallbackGroupType, Event, EventKind, EventQueue, EventQueueSender,
};
use crate::key_expr::{key_builder_from_group_env, KeyExprBuilder};
use crate::liveliness::LivelinessManager;
use crate::log_prelude::*;
use crate::schema::{SchemaAttachPolicy, SchemaHash as SchemaInfo};
use crate::schema_registry::SchemaRegistry;
use crate::session::Session;
use crate::telemetry::{
    RuntimeMetricsTelemetryPublisher, RuntimeTelemetryGuard, TelemetryResourceGuard,
    VitalSnapshotTelemetryPublisher,
};
use crate::time::unix_time_ns;
use crate::ZBytes;

/// Bound on the background single-provider owner-check liveliness query.
/// Off the hot path and off service-startup latency, so it can be
/// generous enough for tokens to be discovered across the cloud router.
const SERVICE_OWNER_CHECK_TIMEOUT: Duration = Duration::from_millis(1000);

/// Service schema metadata for RPC registration and attachment.
#[derive(Clone, Debug)]
pub struct ServiceSchemaInfo {
    pub schema: SchemaInfo,
    pub schema_bfbs: Vec<u8>,
    pub request_root_type: Option<String>,
    pub response_root_type: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcOverflowPolicy {
    FastFailBusy,
    Block,
}

/// Runtime service diagnostics counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ServiceStats {
    pub request_received_count: u64,
    pub request_dispatched_count: u64,
    pub reply_ok_count: u64,
    pub reply_error_count: u64,
    pub reply_send_failure_count: u64,
    pub request_dropped_count: u64,
    /// deadline-expired drops — a SUBSET of request_dropped_count.
    pub request_dropped_deadline_count: u64,
    pub query_receive_failure_count: u64,
    pub task_exit_count: u64,
    pub in_flight_requests: u64,
    pub last_request_ns: u64,
    pub last_reply_ns: u64,
}

#[derive(Debug, Default)]
pub(crate) struct ServiceStatsInner {
    request_received_count: AtomicU64,
    request_dispatched_count: AtomicU64,
    reply_ok_count: AtomicU64,
    reply_error_count: AtomicU64,
    reply_send_failure_count: AtomicU64,
    request_dropped_count: AtomicU64,
    /// Requests dropped because the caller's wire
    /// budget expired in queue — a SUBSET of request_dropped_count, so
    /// operators can tell deadline sheds from handler-forgot-to-reply.
    request_dropped_deadline_count: AtomicU64,
    query_receive_failure_count: AtomicU64,
    task_exit_count: AtomicU64,
    // Loom-verified drain counter (see crate::in_flight): the CAS-decrement +
    // exactly-once `1 → 0` signal that drives `wait_for_idle`/`idle_notify`.
    in_flight_requests: InFlightCounter,
    last_request_ns: AtomicU64,
    last_reply_ns: AtomicU64,
    idle_notify: Notify,
}

impl ServiceStatsInner {
    #[cfg(test)]
    pub(crate) fn deadline_dropped_for_test(&self) -> u64 {
        self.request_dropped_deadline_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn snapshot(&self) -> ServiceStats {
        ServiceStats {
            request_received_count: self.request_received_count.load(Ordering::Relaxed),
            request_dispatched_count: self.request_dispatched_count.load(Ordering::Relaxed),
            reply_ok_count: self.reply_ok_count.load(Ordering::Relaxed),
            reply_error_count: self.reply_error_count.load(Ordering::Relaxed),
            reply_send_failure_count: self.reply_send_failure_count.load(Ordering::Relaxed),
            request_dropped_count: self.request_dropped_count.load(Ordering::Relaxed),
            request_dropped_deadline_count: self
                .request_dropped_deadline_count
                .load(Ordering::Relaxed),
            query_receive_failure_count: self.query_receive_failure_count.load(Ordering::Relaxed),
            task_exit_count: self.task_exit_count.load(Ordering::Relaxed),
            in_flight_requests: self.in_flight_requests.get(),
            last_request_ns: self.last_request_ns.load(Ordering::Relaxed),
            last_reply_ns: self.last_reply_ns.load(Ordering::Relaxed),
        }
    }

    fn on_request_received(&self) {
        self.request_received_count.fetch_add(1, Ordering::Relaxed);
        self.in_flight_requests.incr();
        self.last_request_ns
            .store(unix_time_ns(), Ordering::Relaxed);
    }

    fn on_request_dispatched(&self) {
        self.request_dispatched_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn on_reply_ok(&self) {
        self.reply_ok_count.fetch_add(1, Ordering::Relaxed);
        self.last_reply_ns.store(unix_time_ns(), Ordering::Relaxed);
        self.decrement_in_flight();
    }

    fn on_reply_error(&self) {
        self.reply_error_count.fetch_add(1, Ordering::Relaxed);
        self.last_reply_ns.store(unix_time_ns(), Ordering::Relaxed);
        self.decrement_in_flight();
    }

    fn on_reply_send_failure(&self) {
        self.reply_send_failure_count
            .fetch_add(1, Ordering::Relaxed);
        self.last_reply_ns.store(unix_time_ns(), Ordering::Relaxed);
        self.decrement_in_flight();
    }

    fn on_request_dropped(&self) {
        self.request_dropped_count.fetch_add(1, Ordering::Relaxed);
        self.decrement_in_flight();
    }

    fn on_query_receive_failure(&self) {
        self.query_receive_failure_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn on_task_exit(&self) {
        self.task_exit_count.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_in_flight(&self) {
        // `decr` owns the CAS loop + the exactly-once `1 → 0` decision (loom-
        // proven in crate::in_flight); we only translate that edge into the
        // wakeup. `notify_waiters` after the drain pairs with `wait_for_idle`'s
        // register-then-recheck below to close the lost-wakeup window.
        if self.in_flight_requests.decr() {
            self.idle_notify.notify_waiters();
        }
    }
}

/// Pending request waiting to be processed
pub struct ServiceRequest {
    query: Query,
    payload: ZBytes,
    caller_namespace: Option<String>,
    /// Schema identity to echo on the reply attachment (per attach policy).
    /// Encoded at reply time: legacy bare-hex normally, composite when the
    /// request carried a traceparent (see [`crate::attachment`]).
    response_schema: Option<SchemaInfo>,
    stats: Option<Arc<ServiceStatsInner>>,
    completed: bool,
    /// Monotonic receive time (ns) for tracing's server-side stage latencies.
    /// `None` when tracing was disabled at receipt (so no clock was read).
    /// Independent of telemetry's wall-clock `last_request_ns`.
    received_mono_ns: Option<u64>,
    /// the caller's wire-stamped budget, RE-ANCHORED at receipt
    /// (`Budget::from_wire_relative_ns` — a cooperative narrowing hint;
    /// the sender's own guard stays authoritative). `None` = no stamp
    /// (old sender or kill-switch) → un-stamped behavior bit-for-bit.
    wire_budget: Option<crate::flow::Budget>,
    /// Interned per-service waterfall event
    /// (`service.received_to_reply:<service>`), minted once at service start.
    received_to_reply_event: u16,
    /// Interned `service.reply_send:<service>` event (reply transmission).
    reply_send_event: u16,
    /// Trace context propagated on the query attachment, when the caller
    /// injected one and tracing was enabled at receipt.
    inbound_trace: Option<crate::trace::TraceParent>,
    /// Per-request SERVER ROOT span identity: `received_to_reply` is emitted
    /// under exactly this id (parented to the caller's span when propagated),
    /// and every internal stage records as its child — one cross-process link
    /// per request, internal structure nested beneath it.
    root_link: Option<crate::trace::SpanLink>,
    /// Monotonic enqueue time, stamped after a successful executor dispatch;
    /// the handler entry records `queue_wait` from it.
    enqueued_mono_ns: Option<u64>,
}

impl ServiceRequest {
    /// Behavioral-test constructor — a real Query with a
    /// controlled wire budget, everything else defaulted.
    #[cfg(test)]
    pub(crate) fn for_deadline_test(
        query: Query,
        wire_budget: Option<crate::flow::Budget>,
        stats: Arc<ServiceStatsInner>,
    ) -> Self {
        Self {
            query,
            payload: ZBytes::default(),
            caller_namespace: None,
            response_schema: None,
            stats: Some(stats),
            completed: false,
            received_mono_ns: None,
            wire_budget,
            received_to_reply_event: 0,
            reply_send_event: 0,
            inbound_trace: None,
            root_link: None,
            enqueued_mono_ns: None,
        }
    }

    pub fn payload(&self) -> &ZBytes {
        &self.payload
    }

    /// Returns the caller's application-level namespace parsed from the
    /// arriving query key, or `None` for a namespace-free caller.
    ///
    /// This is a transport-level routing identity, not an authenticated
    /// identity. It is trustworthy only when router ACLs pin each link to its
    /// allowed namespace prefix.
    pub fn caller_namespace(&self) -> Option<&str> {
        self.caller_namespace.as_deref()
    }

    /// (deadline-over-RPC): the caller's remaining budget,
    /// re-anchored at receipt. `None` = no wire stamp (old sender or
    /// kill-switch) — treat as unlimited. A cooperative NARROWING hint:
    /// pass it to downstream calls (`call_in` narrows automatically via
    /// the ambient cap when the handler runs in a flow task).
    pub fn remaining_budget(&self) -> Option<std::time::Duration> {
        self.wire_budget.as_ref().and_then(|b| b.remaining())
    }

    /// the stamped budget expired while the request sat in the
    /// queue — running the handler would do doomed work (the caller's
    /// own guard has already fired).
    fn expired_in_queue(&self) -> bool {
        self.wire_budget.as_ref().is_some_and(|b| b.is_expired())
    }

    /// The traceparent the CALLER propagated on this request, if any.
    /// The built-in stage spans join it automatically, and the synchronous
    /// handler invocation runs under it; a handler that moves work to another
    /// task can re-adopt it there via [`crate::trace::adopt_remote_context`].
    pub fn inbound_trace(&self) -> Option<crate::trace::TraceParent> {
        self.inbound_trace
    }

    /// The trace context identifying THIS request, for parenting deferred
    /// work (a worker thread, a queued task) that outlives the synchronous
    /// handler: the trace id plus the span to parent under — the per-request
    /// SERVER ROOT when one was minted, else the caller's propagated span.
    /// `None` when tracing was disabled at receipt.
    ///
    /// Install it on the deferred thread via
    /// [`crate::trace::adopt_remote_context`] (or `aorta_trace_set_context`
    /// across the C ABI) so spans recorded there stay attached to this
    /// request's trace, nested under `received_to_reply`.
    pub fn trace_context(&self) -> Option<crate::trace::TraceParent> {
        if let Some(root) = self.root_link {
            return Some(crate::trace::TraceParent {
                trace_id: root.trace_id,
                span_id: root.span_id,
                flags: self
                    .inbound_trace
                    .map_or(crate::trace::TRACEPARENT_SAMPLED, |t| t.flags),
            });
        }
        self.inbound_trace
    }

    /// Reply to the request with a response payload
    pub async fn reply<IntoZBytes>(mut self, response: IntoZBytes) -> AortaResult<()>
    where
        IntoZBytes: Into<ZBytes>,
    {
        let key = self.query.key_expr().as_str().to_string();
        let response = response.into();
        let response_len = response.len();
        let builder = self.query.reply(&key, response);
        let attachment = self.reply_attachment();
        aorta_core_debug!(
            "sending service reply: key={}, response_len={}, has_attachment={}",
            key,
            response_len,
            attachment.is_some()
        );
        // Tracing: reply transmission (send start -> zenoh send completed),
        // child of the request root — splits "handler done" from "reply on
        // the wire". Gated; recorded on every send outcome.
        let send_start = self.root_link.is_some().then(crate::time::monotonic_ns);
        let reply_result = match attachment {
            Some(attachment) => builder.attachment(attachment).await,
            None => builder.await,
        };
        if let Some(start) = send_start {
            crate::trace::record_stage_elapsed_under(
                crate::trace::StageId::ServiceReplySend,
                start,
                self.reply_send_event,
                self.root_link.as_ref(),
            );
        }
        match reply_result {
            Ok(_) => {
                self.mark_reply_ok();
                if let Some(stats) = self.stats.as_ref() {
                    aorta_core_debug!(
                        "service reply completed: key={}, response_len={}, stats={:?}",
                        key,
                        response_len,
                        stats.snapshot()
                    );
                }
                Ok(())
            }
            Err(e) => {
                self.mark_reply_send_failure();
                if let Some(stats) = self.stats.as_ref() {
                    aorta_core_warn!(
                        "service reply failed after send attempt: key={}, response_len={}, error={}, stats={:?}",
                        key,
                        response_len,
                        e,
                        stats.snapshot()
                    );
                }
                Err(AortaError::internal(format!("service reply failed: {}", e)))
            }
        }
    }

    /// Reply with an error
    pub async fn reply_error(mut self, error_msg: &str) -> AortaResult<()> {
        aorta_core_debug!(
            "sending service error reply: key={}, error={}",
            self.query.key_expr().as_str(),
            error_msg
        );
        let send_start = self.root_link.is_some().then(crate::time::monotonic_ns);
        let send_result = self.query.reply_err(error_msg).await;
        if let Some(start) = send_start {
            crate::trace::record_stage_elapsed_under(
                crate::trace::StageId::ServiceReplySend,
                start,
                self.reply_send_event,
                self.root_link.as_ref(),
            );
        }
        match send_result {
            Ok(_) => {
                self.mark_reply_error();
                Ok(())
            }
            Err(e) => {
                self.mark_reply_send_failure();
                Err(AortaError::internal(format!(
                    "service reply_error failed: {}",
                    e
                )))
            }
        }
    }

    fn mark_reply_ok(&mut self) {
        if self.completed {
            return;
        }
        if let Some(stats) = self.stats.as_ref() {
            stats.on_reply_ok();
        }
        self.record_received_to_reply();
        self.completed = true;
    }

    fn mark_reply_error(&mut self) {
        if self.completed {
            return;
        }
        if let Some(stats) = self.stats.as_ref() {
            stats.on_reply_error();
        }
        self.record_received_to_reply();
        self.completed = true;
    }

    /// Build the reply attachment: the schema part per policy, promoted to the
    /// composite form carrying the server busy duration ONLY when the request
    /// itself carried a traceparent — so an old client always
    /// receives the legacy bare-hex form, byte-identical to the historical wire.
    fn reply_attachment(&self) -> Option<Vec<u8>> {
        let reply_busy_ns = match (self.inbound_trace, self.received_mono_ns) {
            (Some(_), Some(recv)) if crate::trace::trace_enabled() => {
                Some(crate::time::monotonic_ns().saturating_sub(recv))
            }
            _ => None,
        };
        crate::attachment::encode(&crate::attachment::AttachmentParts {
            schema_hash: self.response_schema,
            traceparent: None,
            reply_busy_ns,
            deadline_budget_ns: None,
        })
    }

    /// Record the server-side received->reply latency (server busy time) into the
    /// tracing-owned histogram. Monotonic and gated; a no-op with no clock read
    /// when tracing was disabled at receipt. Funnels all reply paths (every
    /// successful reply/reply_error passes through here). Independent of telemetry.
    /// The waterfall span JOINS the propagated trace when the request carried one.
    #[inline]
    fn record_received_to_reply(&self) {
        if let Some(recv) = self.received_mono_ns {
            match self.root_link {
                Some(root) => crate::trace::record_stage_elapsed_linked(
                    crate::trace::StageId::ServiceReceivedToReply,
                    recv,
                    self.received_to_reply_event,
                    root,
                ),
                None => crate::trace::record_stage_elapsed_as(
                    crate::trace::StageId::ServiceReceivedToReply,
                    recv,
                    self.received_to_reply_event,
                ),
            }
        }
    }

    fn mark_reply_send_failure(&mut self) {
        if self.completed {
            return;
        }
        if let Some(stats) = self.stats.as_ref() {
            stats.on_reply_send_failure();
        }
        self.completed = true;
    }
}

impl Drop for ServiceRequest {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(stats) = self.stats.as_ref() {
            stats.on_request_dropped();
            aorta_core_warn!(
                "service request dropped without reply: key={}, payload_len={}, stats={:?}",
                self.query.key_expr().as_str(),
                self.payload.len(),
                stats.snapshot()
            );
        }
        self.completed = true;
    }
}

/// Service handler callback type
pub type ServiceHandler = Box<dyn Fn(ServiceRequest) + Send + Sync + 'static>;

/// A Service that listens for incoming RPC requests
pub struct Service {
    session: Session,
    key_builder: KeyExprBuilder,
    service_name: String,
    schema_info: Option<ServiceSchemaInfo>,
    schema_registry: Option<SchemaRegistry>,
    event_queue: Option<Arc<EventQueue>>,
    event_sender: Option<EventQueueSender>,
    handler: Option<ServiceHandler>,
    schema_attach_policy: SchemaAttachPolicy,
    callback_group: CallbackGroupType,
    callback_group_handle: Option<CallbackGroupHandle>,
    rpc_overflow_policy: RpcOverflowPolicy,
    rpc_overflow_block_timeout: Duration,
    stats: Arc<ServiceStatsInner>,
    inventory_telemetry: Option<Arc<VitalSnapshotTelemetryPublisher>>,
    runtime_telemetry: Option<Arc<RuntimeMetricsTelemetryPublisher>>,
    node_id: Option<String>,
    serve_all_namespaces: bool,
    /// Keeps this service counted among the node's live snapshot producers:
    /// the serving task captures `event_sender` at start and cannot follow the
    /// node to another executor, so its existence must block a handoff.
    snapshot_producer_guard: Option<crate::node::SnapshotProducerGuard>,
}

impl Service {
    /// Create a new Service builder
    pub fn builder(session: Session, service_name: &str) -> ServiceBuilder {
        ServiceBuilder::new(session, service_name)
    }

    /// Get the service name
    pub fn name(&self) -> &str {
        &self.service_name
    }

    /// Get current runtime diagnostics counters.
    pub fn stats(&self) -> ServiceStats {
        self.stats.snapshot()
    }

    /// Start the service and begin listening for requests
    pub async fn start(self) -> AortaResult<RunningService> {
        if self.serve_all_namespaces {
            if let Some(namespace) = self.session.configured_namespace() {
                return Err(AortaError::invalid_argument(format!(
                    "all-namespaces service requires a namespace-free Zenoh session; \
                     configured Zenoh session namespace is '{}'",
                    namespace
                )));
            }
        }

        let response_schema = match self.schema_attach_policy {
            SchemaAttachPolicy::Always => {
                let schema_info = self.schema_info.as_ref().ok_or_else(|| {
                    AortaError::invalid_argument(
                        "service schema is required when rpc schema attach policy is always",
                    )
                })?;
                Some(schema_info.schema)
            }
            SchemaAttachPolicy::Never | SchemaAttachPolicy::Inherit => None,
        };

        let mut schema_info_for_register = self.schema_info.clone();
        let mut missing_registry_bfbs = false;

        if let (Some(schema_info), Some(schema_registry)) = (
            schema_info_for_register.as_mut(),
            self.schema_registry.as_ref(),
        ) {
            if schema_info.schema_bfbs.is_empty() {
                if let Some(bfbs) = schema_registry.fetch_schema(schema_info.schema).await? {
                    schema_info.schema_bfbs = bfbs;
                }
            }
            missing_registry_bfbs = schema_info.schema_bfbs.is_empty();
        }

        if missing_registry_bfbs {
            if matches!(self.schema_attach_policy, SchemaAttachPolicy::Always) {
                return Err(AortaError::invalid_argument(
                    "service schema bfbs is required when rpc schema attach policy is always",
                ));
            }
            if let Some(schema_info) = schema_info_for_register.as_ref() {
                aorta_core_warn!(
                    "Aorta WARN: service '{}' schema bfbs unavailable (hash={}); skip schema_by_key registration.",
                    self.service_name,
                    schema_info.schema.to_hex(),
                );
            }
            schema_info_for_register = None;
        }

        if let (Some(schema_info), Some(schema_registry)) = (
            schema_info_for_register.as_ref(),
            self.schema_registry.as_ref(),
        ) {
            aorta_core_debug!(
                "registering service schema_by_key before declaring service queryable: service={}, schema={}, bfbs_len={}",
                self.service_name,
                schema_info.schema.to_hex(),
                schema_info.schema_bfbs.len()
            );
            schema_registry
                .register_schema_by_key_service(
                    &self.service_name,
                    schema_info.schema,
                    schema_info.schema_bfbs.clone(),
                    schema_info.request_root_type.clone(),
                    schema_info.response_root_type.clone(),
                )
                .await?;
        }

        let key_expr = if self.serve_all_namespaces {
            self.key_builder
                .service_key_server_all_namespaces(&self.service_name)?
        } else {
            self.key_builder.service_key_server(&self.service_name)?
        };
        let key_expr: String = key_expr.into();

        aorta_core_info!(
            "starting service: service={}, key={}, has_schema={}, has_handler={}, has_event_sender={}, callback_group={:?}, overflow_policy={:?}, block_timeout_ms={}",
            self.service_name,
            key_expr,
            self.schema_info.is_some(),
            self.handler.is_some(),
            self.event_sender.is_some() || self.event_queue.is_some(),
            self.callback_group,
            self.rpc_overflow_policy,
            self.rpc_overflow_block_timeout.as_millis()
        );
        aorta_core_debug!(
            "declaring service queryable: service={}, key={}",
            self.service_name,
            key_expr
        );
        // FROZEN interop contract: the
        // queryable keeps zenoh's default `complete = false`, so clients —
        // including foreign native-zenoh peers per the interop profile — must
        // query with `QueryTarget::All`. Adding `.complete(true)` here changes
        // what foreign clients observe and requires amending the profile; the
        // pin lives in core/test/native_zenoh_interop_tests.rs.
        let queryable = self
            .session
            .inner()
            .declare_queryable(key_expr.clone())
            .await?;
        aorta_core_info!(
            "service queryable declared: service={}, key={}",
            self.service_name,
            key_expr
        );

        // Network-level single-provider ownership. Claim an
        // owner liveliness token for this (service, node) and, off the hot path,
        // check whether another node already owns the same service. Advisory
        // only — never rejects — because a crashed provider's token lingers until
        // session timeout and undeclare propagates asynchronously, so a hard
        // reject would block legitimate restart/failover. Scoped to the node's
        // zenoh namespace, so per-robot isolation makes this a within-robot check
        // by construction. Skipped when no node_id is wired (raw Service::builder).
        let owner_conflicts = Arc::new(AtomicU64::new(0));
        let owner_claim = match self.node_id.clone() {
            Some(node_id) => {
                self.claim_service_owner(&node_id, Arc::clone(&owner_conflicts))
                    .await
            }
            None => None,
        };

        // Optional forwarding to an executor-backed queue.
        // NOTE: EventQueue contains a Receiver and is not Sync; do not move Arc<EventQueue>
        // into tokio::spawn (which requires Send). Only move its Sender.
        let event_queue = self.event_queue;
        let event_sender = self
            .event_sender
            .or_else(|| event_queue.as_ref().map(|eq| eq.sender()));
        let callback_group = self.callback_group;
        let callback_group_handle = self.callback_group_handle;
        let rpc_overflow_policy = self.rpc_overflow_policy;
        let rpc_overflow_block_timeout = self.rpc_overflow_block_timeout;
        let callback_guard = self
            .handler
            .as_ref()
            .map(|_| Arc::new(AtomicBool::new(true)));
        let stats = Arc::clone(&self.stats);
        let handler = self.handler.map(|h| {
            if let Some(guard) = callback_guard.as_ref() {
                let guard = Arc::clone(guard);
                Box::new(move |request| {
                    if !guard.load(Ordering::Relaxed) {
                        return;
                    }
                    h(request);
                }) as ServiceHandler
            } else {
                h
            }
        });
        // (deadline-over-RPC): ONE gate wraps every dispatch path —
        // a request whose stamped budget expired in queue is dropped
        // without running the handler (the caller's own guard already
        // fired; the generic dropped-request accounting applies via Drop).
        let handler = handler.map(wrap_handler_with_deadline_gate);
        let handler = handler.map(Arc::new);

        // Fallback queue for callers that want to pull requests manually.
        let (tx, rx) = mpsc::channel::<ServiceRequest>(64);
        let service_name_for_logs = self.service_name.clone();
        // Per-service waterfall events (timeline attribution in multi-service
        // processes); interned once per service start, used per request.
        let ev_received_to_reply = crate::trace::stage_event_for(
            crate::trace::StageId::ServiceReceivedToReply,
            &self.service_name,
        );
        let ev_dispatched = crate::trace::stage_event_for(
            crate::trace::StageId::ServiceRequestReceivedToDispatched,
            &self.service_name,
        );
        let ev_queue_wait = crate::trace::stage_event_for(
            crate::trace::StageId::ServiceQueueWait,
            &self.service_name,
        );
        let ev_user_callback = crate::trace::stage_event_for(
            crate::trace::StageId::ServiceUserCallback,
            &self.service_name,
        );
        let ev_reply_send = crate::trace::stage_event_for(
            crate::trace::StageId::ServiceReplySend,
            &self.service_name,
        );
        let inventory_guard = self
            .inventory_telemetry
            .as_ref()
            .map(|inventory| {
                inventory.register_service(
                    &self.key_builder,
                    &self.service_name,
                    &key_expr,
                    self.schema_info.as_ref(),
                )
            })
            .transpose()
            .map_err(|err| {
                AortaError::internal(format!(
                    "failed to register service inventory telemetry '{}': {}",
                    self.service_name, err
                ))
            })
            .unwrap_or_else(|err| {
                aorta_core_warn!("Aorta WARN: {}", err);
                None
            });
        let runtime_guard = self
            .runtime_telemetry
            .as_ref()
            .map(|telemetry| {
                telemetry.register_service(
                    &self.service_name,
                    self.schema_info.as_ref().map(|info| info.schema),
                    Arc::new({
                        let stats = Arc::clone(&self.stats);
                        move || stats.snapshot()
                    }),
                )
            })
            .transpose()
            .map_err(|err| {
                aorta_core_warn!(
                    "Aorta WARN: service runtime telemetry disabled for '{}': {}",
                    self.service_name,
                    err
                );
                err
            })
            .ok()
            .flatten();

        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let key_expr_for_logs = key_expr.clone();
        let key_builder = self.key_builder.clone();
        let task_handle = tokio::spawn(async move {
            let queryable = queryable;
            aorta_core_info!(
                "service task started: service={}, key={}",
                service_name_for_logs,
                key_expr_for_logs
            );
            loop {
                let query = tokio::select! {
                    _ = &mut stop_rx => {
                        aorta_core_info!(
                            "service task received stop signal: service={}, key={}, stats={:?}",
                            service_name_for_logs,
                            key_expr_for_logs,
                            stats.snapshot()
                        );
                        break;
                    }
                    query = queryable.recv_async() => {
                        match query {
                            Ok(query) => query,
                            Err(err) => {
                                stats.on_query_receive_failure();
                                aorta_core_warn!(
                                    "service query receive failed: service={}, key={}, error={}, stats={:?}",
                                    service_name_for_logs,
                                    key_expr_for_logs,
                                    err,
                                    stats.snapshot()
                                );
                                break;
                            }
                        }
                    }
                };

                let payload = query.payload().cloned().unwrap_or_default();
                let payload_len = payload.len();
                stats.on_request_received();
                // Tracing: capture the monotonic receive time ONCE, only when
                // enabled (so a disabled build/runtime reads no clock here). Used
                // for the server stage latencies; independent of telemetry's
                // wall-clock `last_request_ns`.
                let received_mono_ns = if crate::trace::trace_enabled() {
                    Some(crate::time::monotonic_ns())
                } else {
                    None
                };
                // The caller's traceparent rides the query attachment.
                // Parsed only when tracing was enabled at receipt (gating the
                // stage records that would join it); absent/legacy/unknown
                // attachments all decode to `None`.
                let decoded_attachment = query
                    .attachment()
                    .map(|attachment| crate::attachment::decode(attachment.to_bytes().as_ref()));
                let inbound_trace = if received_mono_ns.is_some() {
                    decoded_attachment.as_ref().and_then(|p| p.traceparent)
                } else {
                    None
                };
                // re-anchor the caller's relative budget at receipt
                // (decoded UNCONDITIONALLY — the deadline is a delivery
                // semantic, not a tracing one).
                let wire_budget = decoded_attachment
                    .as_ref()
                    .and_then(|p| p.deadline_budget_ns)
                    .map(crate::flow::Budget::from_wire_relative_ns);
                // Per-request SERVER ROOT identity (trace hierarchy): one
                // cross-process link to the caller, internal stages nested.
                let root_link = crate::trace::inbound_root_link(inbound_trace.as_ref());
                aorta_core_debug!(
                    "service request received: service={}, payload_len={}, stats={:?}",
                    service_name_for_logs,
                    payload_len,
                    stats.snapshot()
                );

                let caller_namespace = key_builder
                    .split_full_key(query.key_expr().as_str())
                    .and_then(|(namespace, _)| namespace);
                let request = ServiceRequest {
                    query,
                    payload,
                    caller_namespace,
                    response_schema,
                    stats: Some(Arc::clone(&stats)),
                    completed: false,
                    received_mono_ns,
                    wire_budget,
                    received_to_reply_event: ev_received_to_reply,
                    reply_send_event: ev_reply_send,
                    inbound_trace,
                    root_link,
                    enqueued_mono_ns: None,
                };

                if let Some(sender) = event_sender.as_ref() {
                    if let Some(h) = handler.as_ref() {
                        let h = Arc::clone(h);
                        let request_slot = Arc::new(Mutex::new(Some(request)));
                        let request_slot_for_event = Arc::clone(&request_slot);
                        let request_bytes = request_slot
                            .lock()
                            .expect("service request slot lock poisoned")
                            .as_ref()
                            .map(|request| request.payload().len())
                            .unwrap_or(0);
                        let event = Event::new(EventKind::RpcRequest, move || {
                            let maybe_request = request_slot_for_event
                                .lock()
                                .expect("service request slot lock poisoned")
                                .take();
                            if let Some(request) = maybe_request {
                                let root = request.root_link;
                                // Queue-wait stage: executor queue wait
                                // (enqueued -> handler entry), child of root.
                                if let Some(enqueued) = request.enqueued_mono_ns {
                                    crate::trace::record_stage_elapsed_under(
                                        crate::trace::StageId::ServiceQueueWait,
                                        enqueued,
                                        ev_queue_wait,
                                        root.as_ref(),
                                    );
                                }
                                // Handler stage: user handler duration. The span
                                // id is pre-minted so spans the user opens
                                // under the adopted context nest beneath it.
                                let cb = root.map(|r| crate::trace::SpanLink {
                                    trace_id: r.trace_id,
                                    span_id: crate::trace::mint_span_id(),
                                    parent_span_id: r.span_id,
                                });
                                let cb_start = cb.is_some().then(crate::time::monotonic_ns);
                                {
                                    let _ctx = cb.as_ref().map(|c| {
                                        crate::trace::adopt_remote_context(
                                            &crate::trace::TraceParent {
                                                trace_id: c.trace_id,
                                                span_id: c.span_id,
                                                flags: crate::trace::TRACEPARENT_SAMPLED,
                                            },
                                        )
                                    });
                                    h(request);
                                }
                                if let (Some(c), Some(start)) = (cb, cb_start) {
                                    crate::trace::record_stage_elapsed_linked(
                                        crate::trace::StageId::ServiceUserCallback,
                                        start,
                                        ev_user_callback,
                                        c,
                                    );
                                }
                            }
                        })
                        .with_estimated_bytes(request_bytes)
                        .with_callback_group(callback_group, callback_group_handle);

                        match sender.try_send(event) {
                            Ok(_) => {
                                stats.on_request_dispatched();
                                // Tracing: received -> dispatched (executor queue
                                // wait). Gated; monotonic; independent of telemetry.
                                if let Some(recv) = received_mono_ns {
                                    crate::trace::record_stage_elapsed_under(
                                        crate::trace::StageId::ServiceRequestReceivedToDispatched,
                                        recv,
                                        ev_dispatched,
                                        root_link.as_ref(),
                                    );
                                }
                                // Stamp the enqueue time so the handler entry
                                // can record queue_wait. If the
                                // executor already took the request, the wait
                                // was ~zero and the stage is simply skipped.
                                if root_link.is_some() {
                                    if let Some(req) = request_slot
                                        .lock()
                                        .expect("service request slot lock poisoned")
                                        .as_mut()
                                    {
                                        req.enqueued_mono_ns = Some(crate::time::monotonic_ns());
                                    }
                                }
                                aorta_core_debug!("service request dispatched to executor: service={}, payload_len={}, stats={:?}", service_name_for_logs, request_bytes, stats.snapshot());
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
                                match rpc_overflow_policy {
                                    RpcOverflowPolicy::FastFailBusy => {
                                        aorta_core_warn!("service executor queue full; replying busy: service={}, payload_len={}, stats={:?}", service_name_for_logs, request_bytes, stats.snapshot());
                                        let busy_request = request_slot
                                            .lock()
                                            .expect("service request slot lock poisoned")
                                            .take();
                                        if let Some(request) = busy_request {
                                            if let Err(err) = request
                                                .reply_error("service busy: executor queue full")
                                                .await
                                            {
                                                aorta_core_warn!(
                                                    "Aorta WARN: service busy reply failed for '{}': {}",
                                                    service_name_for_logs, err
                                                );
                                            }
                                        }
                                    }
                                    RpcOverflowPolicy::Block => {
                                        match tokio::time::timeout(
                                            rpc_overflow_block_timeout,
                                            sender.send(event),
                                        )
                                        .await
                                        {
                                            Ok(Ok(())) => {
                                                stats.on_request_dispatched();
                                                if let Some(recv) = received_mono_ns {
                                                    crate::trace::record_stage_elapsed_under(
                                                        crate::trace::StageId::ServiceRequestReceivedToDispatched,
                                                        recv,
                                                        ev_dispatched,
                                                        root_link.as_ref(),
                                                    );
                                                }
                                                // Stamp the enqueue time so the handler entry
                                                // can record queue_wait. If the
                                                // executor already took the request, the wait
                                                // was ~zero and the stage is simply skipped.
                                                if root_link.is_some() {
                                                    if let Some(req) = request_slot
                                                        .lock()
                                                        .expect(
                                                            "service request slot lock poisoned",
                                                        )
                                                        .as_mut()
                                                    {
                                                        req.enqueued_mono_ns =
                                                            Some(crate::time::monotonic_ns());
                                                    }
                                                }
                                                aorta_core_debug!("service request dispatched after block-wait: service={}, payload_len={}, stats={:?}", service_name_for_logs, request_bytes, stats.snapshot());
                                            }
                                            Ok(Err(_)) => {
                                                aorta_core_warn!("service executor queue closed during block-wait: service={}, payload_len={}, stats={:?}", service_name_for_logs, request_bytes, stats.snapshot());
                                                let unavailable_request = request_slot
                                                    .lock()
                                                    .expect("service request slot lock poisoned")
                                                    .take();
                                                if let Some(request) = unavailable_request {
                                                    if let Err(err) = request
                                                        .reply_error(
                                                            "service unavailable: executor queue closed",
                                                        )
                                                        .await
                                                    {
                                                        aorta_core_warn!(
                                                            "Aorta WARN: service unavailable reply failed for '{}': {}",
                                                            service_name_for_logs, err
                                                        );
                                                    }
                                                }
                                            }
                                            Err(_) => {
                                                aorta_core_warn!("service executor queue block timed out: service={}, payload_len={}, timeout_ms={}, stats={:?}", service_name_for_logs, request_bytes, rpc_overflow_block_timeout.as_millis(), stats.snapshot());
                                                let busy_request = request_slot
                                                    .lock()
                                                    .expect("service request slot lock poisoned")
                                                    .take();
                                                if let Some(request) = busy_request {
                                                    if let Err(err) = request
                                                        .reply_error(
                                                            "service busy: executor queue block timeout",
                                                        )
                                                        .await
                                                    {
                                                        aorta_core_warn!(
                                                            "Aorta WARN: service busy reply failed for '{}': {}",
                                                            service_name_for_logs, err
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_event)) => {
                                aorta_core_warn!("service executor queue already closed: service={}, payload_len={}, stats={:?}", service_name_for_logs, request_bytes, stats.snapshot());
                                let busy_request = request_slot
                                    .lock()
                                    .expect("service request slot lock poisoned")
                                    .take();
                                if let Some(request) = busy_request {
                                    if let Err(err) = request
                                        .reply_error("service unavailable: executor queue closed")
                                        .await
                                    {
                                        aorta_core_warn!(
                                            "Aorta WARN: service unavailable reply failed for '{}': {}",
                                            service_name_for_logs,
                                            err
                                        );
                                    }
                                }
                            }
                        }
                        continue;
                    }
                }

                if let Some(h) = handler.as_ref() {
                    stats.on_request_dispatched();
                    if let Some(recv) = received_mono_ns {
                        crate::trace::record_stage_elapsed_under(
                            crate::trace::StageId::ServiceRequestReceivedToDispatched,
                            recv,
                            ev_dispatched,
                            root_link.as_ref(),
                        );
                    }
                    aorta_core_debug!("service request dispatched directly: service={}, payload_len={}, stats={:?}", service_name_for_logs, payload_len, stats.snapshot());
                    // Handler stage: user handler duration (direct dispatch —
                    // no queue, so no queue_wait stage). Matches the executor
                    // path: pre-minted id, adopted context for user spans.
                    let cb = root_link.map(|r| crate::trace::SpanLink {
                        trace_id: r.trace_id,
                        span_id: crate::trace::mint_span_id(),
                        parent_span_id: r.span_id,
                    });
                    let cb_start = cb.is_some().then(crate::time::monotonic_ns);
                    {
                        let _ctx = cb.as_ref().map(|c| {
                            crate::trace::adopt_remote_context(&crate::trace::TraceParent {
                                trace_id: c.trace_id,
                                span_id: c.span_id,
                                flags: crate::trace::TRACEPARENT_SAMPLED,
                            })
                        });
                        h(request);
                    }
                    if let (Some(c), Some(start)) = (cb, cb_start) {
                        crate::trace::record_stage_elapsed_linked(
                            crate::trace::StageId::ServiceUserCallback,
                            start,
                            ev_user_callback,
                            c,
                        );
                    }
                    continue;
                }

                // No handler: expose requests via the internal channel.
                // The deadline gate applies to this dispatch path too
                // — through the ONE shared
                // `deadline_gate_drops` (this
                // path hand-duplicated the check, so a rule added to the
                // shared gate would silently miss the pull surface).
                if deadline_gate_drops(&request) {
                    continue;
                }
                if tx.send(request).await.is_err() {
                    aorta_core_warn!("service internal request channel closed: service={}, payload_len={}, stats={:?}", service_name_for_logs, payload_len, stats.snapshot());
                    break;
                }
                stats.on_request_dispatched();
                if let Some(recv) = received_mono_ns {
                    crate::trace::record_stage_elapsed_under(
                        crate::trace::StageId::ServiceRequestReceivedToDispatched,
                        recv,
                        ev_dispatched,
                        root_link.as_ref(),
                    );
                }
                aorta_core_debug!("service request forwarded to internal receiver: service={}, payload_len={}, stats={:?}", service_name_for_logs, payload_len, stats.snapshot());
            }

            stats.on_task_exit();
            aorta_core_info!(
                "service task exiting: service={}, key={}, stats={:?}",
                service_name_for_logs,
                key_expr_for_logs,
                stats.snapshot()
            );
            aorta_core_info!(
                "service queryable undeclaring: service={}, key={}",
                service_name_for_logs,
                key_expr_for_logs
            );
            if let Err(err) = queryable.undeclare().await {
                aorta_core_warn!(
                    "service queryable undeclare failed: service={}, key={}, error={}",
                    service_name_for_logs,
                    key_expr_for_logs,
                    err
                );
            } else {
                aorta_core_info!(
                    "service queryable undeclared: service={}, key={}",
                    service_name_for_logs,
                    key_expr_for_logs
                );
            }
        });

        Ok(RunningService {
            service_name: self.service_name,
            _receiver: rx,
            _event_queue: event_queue,
            stop_tx: Some(stop_tx),
            task_handle: Some(task_handle),
            callback_guard,
            stats: Arc::clone(&self.stats),
            inventory_guard,
            runtime_guard,
            owner_claim,
            owner_conflicts,
        })
    }

    /// Declares this provider's owner liveliness token and spawns the advisory,
    /// off-hot-path single-provider check. Returns the token
    /// and its service-owned task, or `None` if the claim failed —
    /// a failed claim only disables the advisory check, never the service.
    async fn claim_service_owner(
        &self,
        node_id: &str,
        conflicts: Arc<AtomicU64>,
    ) -> Option<ServiceOwnerClaim> {
        let owner_key = match self
            .key_builder
            .service_owner_key(&self.service_name, node_id)
        {
            Ok(key) => key,
            Err(err) => {
                aorta_core_warn!(
                    "Aorta WARN: service '{}' owner key invalid: {}; single-provider check disabled",
                    self.service_name,
                    err
                );
                return None;
            }
        };
        match LivelinessManager::declare_key(self.session.inner(), &owner_key).await {
            Ok(token) => {
                let task = tokio::spawn(run_single_provider_owner_check(
                    self.session.clone(),
                    self.key_builder.clone(),
                    self.service_name.clone(),
                    node_id.to_string(),
                    conflicts,
                ));
                Some(ServiceOwnerClaim {
                    token: Some(token),
                    task: Some(task),
                })
            }
            Err(err) => {
                aorta_core_warn!(
                    "Aorta WARN: service '{}' owner token declaration failed: {}; \
                     single-provider check disabled",
                    self.service_name,
                    err
                );
                None
            }
        }
    }
}

/// Background advisory check: queries the live owner tokens for `service_name`
/// and warns (and counts) if any belong to a different node, i.e. another active
/// provider for the same service.
async fn run_single_provider_owner_check(
    session: Session,
    key_builder: KeyExprBuilder,
    service_name: String,
    node_id: String,
    conflicts: Arc<AtomicU64>,
) {
    let query = match key_builder.service_owner_query(&service_name) {
        Ok(query) => query,
        Err(err) => {
            aorta_core_warn!(
                "Aorta WARN: single-provider owner check skipped for service '{}': {}",
                service_name,
                err
            );
            return;
        }
    };
    let keys = match crate::liveliness::liveliness_get(
        session.inner(),
        &query,
        SERVICE_OWNER_CHECK_TIMEOUT,
    )
    .await
    {
        Ok(keys) => keys,
        Err(err) => {
            aorta_core_warn!(
                "Aorta WARN: single-provider owner check query failed for service '{}': {}",
                service_name,
                err
            );
            return;
        }
    };
    let foreign: Vec<&str> = keys
        .iter()
        .filter_map(|key| crate::key_expr::service_owner_key_node_id(key))
        .filter(|owner| *owner != node_id)
        .collect();
    if !foreign.is_empty() {
        conflicts.fetch_add(1, Ordering::Relaxed);
        aorta_core_warn!(
            "Aorta WARN: service '{}' is also claimed by other provider node(s) {:?}; \
             multiple active providers can return divergent answers to one RPC — ensure a \
             single active provider per service (advisory)",
            service_name,
            foreign
        );
    }
}

/// Builder for Service
pub struct ServiceBuilder {
    session: Session,
    key_builder: KeyExprBuilder,
    service_name: String,
    schema_info: Option<ServiceSchemaInfo>,
    schema_registry: Option<SchemaRegistry>,
    event_queue: Option<Arc<EventQueue>>,
    event_sender: Option<EventQueueSender>,
    handler: Option<ServiceHandler>,
    schema_attach_policy: SchemaAttachPolicy,
    default_schema_attach_policy: SchemaAttachPolicy,
    callback_group: CallbackGroupType,
    callback_group_handle: Option<CallbackGroupHandle>,
    rpc_overflow_policy: RpcOverflowPolicy,
    rpc_overflow_block_timeout: Duration,
    stats: Arc<ServiceStatsInner>,
    inventory_telemetry: Option<Arc<VitalSnapshotTelemetryPublisher>>,
    runtime_telemetry: Option<Arc<RuntimeMetricsTelemetryPublisher>>,
    node_id: Option<String>,
    serve_all_namespaces: bool,
    snapshot_producer_guard: Option<crate::node::SnapshotProducerGuard>,
}

impl ServiceBuilder {
    fn new(session: Session, service_name: &str) -> Self {
        let key_builder = key_builder_from_group_env();
        Self {
            session,
            key_builder,
            service_name: service_name.to_string(),
            node_id: None,
            schema_info: None,
            schema_registry: None,
            event_queue: None,
            event_sender: None,
            handler: None,
            schema_attach_policy: SchemaAttachPolicy::Inherit,
            default_schema_attach_policy: SchemaAttachPolicy::from_env(),
            callback_group: CallbackGroupType::MutuallyExclusive,
            callback_group_handle: None,
            rpc_overflow_policy: RpcOverflowPolicy::FastFailBusy,
            rpc_overflow_block_timeout: Duration::from_millis(0),
            stats: Arc::new(ServiceStatsInner::default()),
            inventory_telemetry: None,
            runtime_telemetry: None,
            serve_all_namespaces: false,
            snapshot_producer_guard: None,
        }
    }

    pub fn key_builder(mut self, key_builder: KeyExprBuilder) -> Self {
        self.key_builder = key_builder;
        self
    }

    pub fn serve_all_namespaces(mut self, serve_all_namespaces: bool) -> Self {
        self.serve_all_namespaces = serve_all_namespaces;
        self
    }

    pub fn default_schema_attach_policy(mut self, policy: SchemaAttachPolicy) -> Self {
        self.default_schema_attach_policy = policy;
        self
    }

    pub fn schema(mut self, info: ServiceSchemaInfo) -> Self {
        self.schema_info = Some(info);
        self
    }

    pub fn schema_registry(mut self, registry: SchemaRegistry) -> Self {
        self.schema_registry = Some(registry);
        self
    }

    /// Set the EventQueue for executor-based request handling
    pub fn event_queue(mut self, eq: Arc<EventQueue>) -> Self {
        self.event_sender = Some(eq.sender());
        self.event_queue = Some(eq);
        self
    }

    /// Set the EventQueue sender for executor-based request handling.
    ///
    /// This is the preferred integration point for SDKs that auto-attach
    /// nodes to an internal executor but do not expose the full EventQueue.
    pub fn event_sender(mut self, sender: EventQueueSender) -> Self {
        self.event_sender = Some(sender);
        self
    }

    /// Attach the node's snapshot-producer guard so this
    /// service's existence blocks an executor handoff for the node — the
    /// serving task captures the sender and cannot follow the node.
    pub fn snapshot_producer_guard(mut self, guard: crate::node::SnapshotProducerGuard) -> Self {
        self.snapshot_producer_guard = Some(guard);
        self
    }

    /// Set a direct handler callback (for non-executor usage)
    pub fn handler<F>(mut self, f: F) -> Self
    where
        F: Fn(ServiceRequest) + Send + Sync + 'static,
    {
        self.handler = Some(Box::new(f));
        self
    }

    pub fn schema_attach_policy(mut self, policy: SchemaAttachPolicy) -> Self {
        self.schema_attach_policy = policy;
        self
    }

    pub fn callback_group(mut self, callback_group: CallbackGroupType) -> Self {
        self.callback_group = callback_group;
        self
    }

    pub fn callback_group_handle(mut self, callback_group_handle: CallbackGroupHandle) -> Self {
        self.callback_group_handle = Some(callback_group_handle);
        self
    }

    pub fn rpc_overflow_policy(mut self, policy: RpcOverflowPolicy) -> Self {
        self.rpc_overflow_policy = policy;
        self
    }

    pub fn rpc_overflow_block_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_overflow_block_timeout = timeout;
        self
    }

    pub fn inventory_telemetry(mut self, telemetry: Arc<VitalSnapshotTelemetryPublisher>) -> Self {
        self.inventory_telemetry = Some(telemetry);
        self
    }

    pub fn runtime_telemetry(mut self, telemetry: Arc<RuntimeMetricsTelemetryPublisher>) -> Self {
        self.runtime_telemetry = Some(telemetry);
        self
    }

    /// Sets the owning node's stable id, enabling the network-level
    /// single-provider ownership check at service start.
    /// When unset the check is skipped (e.g. raw `Service::builder` usage).
    pub fn node_id(mut self, node_id: impl Into<String>) -> Self {
        self.node_id = Some(node_id.into());
        self
    }

    pub fn build(self) -> Service {
        let schema_attach_policy = self
            .schema_attach_policy
            .resolve(self.default_schema_attach_policy);
        Service {
            session: self.session,
            key_builder: self.key_builder,
            service_name: self.service_name,
            schema_info: self.schema_info,
            schema_registry: self.schema_registry,
            event_queue: self.event_queue,
            event_sender: self.event_sender,
            snapshot_producer_guard: self.snapshot_producer_guard,
            handler: self.handler,
            schema_attach_policy,
            callback_group: self.callback_group,
            callback_group_handle: self.callback_group_handle,
            rpc_overflow_policy: self.rpc_overflow_policy,
            rpc_overflow_block_timeout: self.rpc_overflow_block_timeout,
            stats: self.stats,
            inventory_telemetry: self.inventory_telemetry,
            runtime_telemetry: self.runtime_telemetry,
            node_id: self.node_id,
            serve_all_namespaces: self.serve_all_namespaces,
        }
    }
}

// The advisory query must not outlive its service and race Session teardown.
// Drop also covers a failed/cancelled service creation before RunningService exists.
struct ServiceOwnerClaim {
    token: Option<Arc<LivelinessManager>>,
    task: Option<JoinHandle<()>>,
}

impl ServiceOwnerClaim {
    fn request_stop(&mut self) {
        let _ = self.token.take();
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }

    async fn stop_and_join(mut self) {
        self.request_stop();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ServiceOwnerClaim {
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// A running service instance
#[must_use = "dropping a RunningService stops serving; bind it for the service lifetime"]
pub struct RunningService {
    service_name: String,
    _receiver: mpsc::Receiver<ServiceRequest>,
    _event_queue: Option<Arc<EventQueue>>,
    stop_tx: Option<oneshot::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
    callback_guard: Option<Arc<AtomicBool>>,
    stats: Arc<ServiceStatsInner>,
    inventory_guard: Option<TelemetryResourceGuard>,
    runtime_guard: Option<RuntimeTelemetryGuard>,
    // Released on stop (not just drop) so a stopped service stops claiming
    // ownership; dropping the last Arc undeclares the owner token.
    owner_claim: Option<ServiceOwnerClaim>,
    owner_conflicts: Arc<AtomicU64>,
}

impl RunningService {
    pub fn name(&self) -> &str {
        &self.service_name
    }

    /// Cumulative count of times the advisory single-provider check observed
    /// ANOTHER node already claiming this service. A non-zero
    /// value indicates a probable duplicate-provider misconfiguration.
    pub fn single_provider_conflict_count(&self) -> u64 {
        self.owner_conflicts.load(Ordering::Relaxed)
    }

    /// Gracefully stop the service and wait for its background task to exit.
    ///
    /// This is primarily useful for FFI callers that need deterministic shutdown
    /// before closing a Session.
    pub async fn stop_and_join(mut self, timeout: Duration) {
        self.request_stop();

        let task_handle = self.task_handle.take();
        let owner_claim = self.owner_claim.take();
        let join_tasks = async move {
            if let Some(owner_claim) = owner_claim {
                owner_claim.stop_and_join().await;
            }
            if let Some(task_handle) = task_handle {
                let _ = task_handle.await;
            }
        };
        // Preserve one shared shutdown budget, not a timeout per worker.
        if tokio::time::timeout(timeout, join_tasks).await.is_err() {
            aorta_core_warn!(
                "Aorta WARN: service '{}' stop timed out after {:?}; proceeding",
                self.service_name,
                timeout
            );
        }
    }

    /// Wait until all in-flight requests are resolved or timeout elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> AortaResult<()> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout;

        loop {
            if self.stats.in_flight_requests.get() == 0 {
                return Ok(());
            }

            // Register waiter first, then re-check to avoid missing a transition to idle.
            let notified = self.stats.idle_notify.notified();
            if self.stats.in_flight_requests.get() == 0 {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }

            let remaining = deadline - now;
            if tokio::time::timeout(remaining, notified).await.is_err() {
                break;
            }
        }

        let snapshot = self.stats.snapshot();
        Err(AortaError::timeout(format!(
            "service '{}' wait_for_idle timed out after {:?}: in_flight_requests={}",
            self.service_name,
            started.elapsed(),
            snapshot.in_flight_requests
        )))
    }

    pub fn stop(&mut self) {
        self.request_stop();
        let _ = self.task_handle.take();
    }

    pub(crate) fn request_stop(&mut self) {
        aorta_core_info!(
            "service stop requested: service={}, stats={:?}, has_callback_guard={}, has_inventory_guard={}, has_runtime_guard={}, has_stop_tx={}",
            self.service_name,
            self.stats.snapshot(),
            self.callback_guard.is_some(),
            self.inventory_guard.is_some(),
            self.runtime_guard.is_some(),
            self.stop_tx.is_some()
        );
        if let Some(guard) = self.callback_guard.as_ref() {
            guard.store(false, Ordering::Relaxed);
        }
        if let Some(inventory_guard) = self.inventory_guard.take() {
            inventory_guard.release();
        }
        if let Some(runtime_guard) = self.runtime_guard.take() {
            runtime_guard.release();
        }
        // Release the single-provider owner claim now, so a stopped (but not yet
        // dropped) service no longer appears to own the service to a replacement
        // provider — otherwise stop/restart would report a false conflict.
        if let Some(owner_claim) = self.owner_claim.as_mut() {
            owner_claim.request_stop();
        }
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
    }

    pub fn stats(&self) -> ServiceStats {
        self.stats.snapshot()
    }
}

impl Drop for RunningService {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
#[path = "../test/service_tests.rs"]
mod service_tests;

/// The ONE deadline-over-RPC decision + accounting, shared by the
/// wrapped-handler and manual-pull dispatch paths (the
/// pull path previously had no gate at all). Returns true when
/// the request expired in queue and must be dropped.
fn deadline_gate_drops(request: &ServiceRequest) -> bool {
    if rpc_deadline_drop_enabled() && request.expired_in_queue() {
        if let Some(stats) = request.stats.as_ref() {
            stats
                .request_dropped_deadline_count
                .fetch_add(1, Ordering::Relaxed);
        }
        aorta_core_warn!(
            "service request budget expired in queue; dropping without \
             dispatch (deadline-over-RPC)"
        );
        true
    } else {
        false
    }
}

/// Extracted for direct behavioral testing: the deadline
/// gate the registered-handler dispatch path runs through.
fn wrap_handler_with_deadline_gate(h: ServiceHandler) -> ServiceHandler {
    Box::new(move |request: ServiceRequest| {
        if deadline_gate_drops(&request) {
            drop(request);
        } else {
            h(request);
        }
    })
}

/// The SERVER half of the deadline kill-switch — with
/// `AORTA_RPC_DEADLINE=0` the drop gate disarms too (a fleet can roll
/// the behavior back on either side independently). Cached once.
/// pub(crate): the action server's goal-budget seed is the same server
/// half and honors the same switch.
pub(crate) fn rpc_deadline_drop_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        crate::client::rpc_deadline_env_enabled(std::env::var("AORTA_RPC_DEADLINE").ok().as_deref())
    })
}
