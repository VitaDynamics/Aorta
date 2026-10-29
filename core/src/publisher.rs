use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use zenoh::bytes::ZBytes;
use zenoh::Wait;

use crate::error::{AortaError, AortaResult};
use crate::log_prelude::*;
use crate::node::Node;
use crate::payload::{AortaPayload, OwnedPayload};
use crate::qos::{CongestionControl, QoS};
use crate::schema::{bfbs_contains_aorta_header, SchemaAttachPolicy, SchemaHash};
use crate::telemetry::{RuntimeTelemetryGuard, TelemetryResourceGuard};
use crate::transport::{TransportPublisher, ZenohTransportPublisher};

const POLICY_EVENT_LOG_INTERVAL: u64 = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum AllocationPolicy {
    #[default]
    HeapOnly,
    BestEffort,
    ShmRequired,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PublisherOptions {
    pub schema_attach_policy: SchemaAttachPolicy,
    pub schema_bfbs: Option<Vec<u8>>,
    /// Fully-qualified root table name matching `schema_bfbs`; carried into
    /// schema_by_key so decoders can select a non-declared root table.
    pub schema_root_type: Option<String>,
    pub qos: QoS,
    pub max_message_size: Option<usize>,
    pub(crate) allocation_policy: AllocationPolicy,
    pub(crate) max_loaned_samples: Option<u32>,
    /// Internal facilities must not appear in user-facing resource telemetry.
    pub(crate) register_telemetry: bool,
    /// When false, `publish_stamp_ns` in the header is set to 0 and the clock
    /// syscall is skipped entirely. Defaults to true.
    pub include_publish_timestamp: bool,
    /// Per-publisher congestion-control **override** of `qos.congestion_control`.
    /// `None` (the default) inherits the QoS
    /// value; `Some(_)` wins. Kept as a write-through override for one release
    /// after the canonical knob moved onto `QoS`.
    pub congestion_control: Option<CongestionControl>,
    /// When true, bypasses zenoh batching and sends immediately.
    /// Defaults to false (use zenoh batching). ORed with `qos.express`.
    pub express: bool,
}

impl Default for PublisherOptions {
    fn default() -> Self {
        Self {
            schema_attach_policy: SchemaAttachPolicy::Inherit,
            schema_bfbs: None,
            schema_root_type: None,
            qos: QoS::default(),
            max_message_size: None,
            allocation_policy: AllocationPolicy::HeapOnly,
            max_loaned_samples: None,
            register_telemetry: true,
            include_publish_timestamp: true,
            congestion_control: None,
            express: false,
        }
    }
}

impl PublisherOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    pub(crate) fn with_allocation_policy(mut self, policy: AllocationPolicy) -> Self {
        self.allocation_policy = policy;
        self
    }

    pub(crate) fn with_max_loaned_samples(mut self, max_loaned_samples: u32) -> Self {
        self.max_loaned_samples = Some(max_loaned_samples);
        self
    }

    pub fn with_publish_timestamp(mut self, enabled: bool) -> Self {
        self.include_publish_timestamp = enabled;
        self
    }

    pub fn with_congestion_control(mut self, congestion_control: CongestionControl) -> Self {
        self.congestion_control = Some(congestion_control);
        self
    }

    pub fn with_express(mut self, express: bool) -> Self {
        self.express = express;
        self
    }
}

pub struct Publisher {
    shared: Arc<PublisherShared>,
    /// Tracks only Publisher clones, not outstanding loans.
    clone_token: Arc<()>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublisherRuntimeStats {
    pub publish_shm_hit_total: u64,
    pub publish_shm_fallback_total: u64,
    pub publish_shm_required_fail_total: u64,
    pub loan_inflight_current: u64,
    pub loan_quota_exceeded_total: u64,
}

struct PublisherShared {
    transport: Arc<dyn TransportPublisher>,
    session: crate::session::Session,
    topic: String,
    schema_hash: Option<SchemaHash>,
    schema_attachment: Option<ZBytes>,
    /// Interned `topic.publish:<topic>` waterfall event (topic-path tracing), minted
    /// once at construction and reused per publish.
    topic_publish_event: u16,
    allocation_policy: AllocationPolicy,
    max_message_size: Option<usize>,
    loan_tracker: Arc<LoanTracker>,
    heap_allocator: Arc<HeapLoanAllocator>,
    runtime_stats: Arc<PublisherRuntimeStatsInner>,
    inventory_guard: TelemetryResourceGuard,
    runtime_guard: RuntimeTelemetryGuard,
}

struct LoanTracker {
    inflight: AtomicU32,
    max_inflight: Option<u32>,
    quota_exceeded_total: AtomicU64,
}

#[derive(Default)]
struct HeapLoanAllocator {
    free: Mutex<Vec<Vec<u8>>>,
}

pub(crate) struct LoanedBytes {
    publisher: Arc<PublisherShared>,
    buffer: Option<LoanBuffer>,
}

enum LoanBuffer {
    Heap(Vec<u8>),
    Shm(zenoh::shm::ZShmMut),
}

struct RecyclableHeapBuffer {
    data: Vec<u8>,
    allocator: Arc<HeapLoanAllocator>,
}

struct PublisherRuntimeStatsInner {
    publish_shm_hit_total: AtomicU64,
    publish_shm_fallback_total: AtomicU64,
    publish_shm_required_fail_total: AtomicU64,
}

pub(crate) fn should_log_policy_event(total: u64) -> bool {
    total == 1
        || (total >= POLICY_EVENT_LOG_INTERVAL && total.is_multiple_of(POLICY_EVENT_LOG_INTERVAL))
}

pub(crate) enum PreparedPublish {
    Payload(AortaPayload),
    Direct(ZBytes),
}

impl std::fmt::Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher")
            .field("topic", &self.shared.topic)
            .field("schema_hash", &self.shared.schema_hash)
            .field(
                "has_schema_attachment",
                &self.shared.schema_attachment.is_some(),
            )
            .finish()
    }
}

impl Clone for Publisher {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            clone_token: Arc::clone(&self.clone_token),
        }
    }
}

impl Publisher {
    pub async fn new(node: &Node, topic: &str) -> AortaResult<Self> {
        Self::new_with_options(node, topic, PublisherOptions::default()).await
    }

    pub async fn new_with_policy(
        node: &Node,
        topic: &str,
        policy: SchemaAttachPolicy,
    ) -> AortaResult<Self> {
        let options = PublisherOptions {
            schema_attach_policy: policy,
            schema_bfbs: None,
            qos: Default::default(),
            ..Default::default()
        };
        Self::new_with_options(node, topic, options).await
    }

    pub async fn new_with_options(
        node: &Node,
        topic: &str,
        options: PublisherOptions,
    ) -> AortaResult<Self> {
        if matches!(options.allocation_policy, AllocationPolicy::ShmRequired)
            && options.max_message_size.unwrap_or(0) == 0
        {
            return Err(AortaError::invalid_argument(
                "ShmRequired policy requires max_message_size > 0",
            ));
        }

        let header_probe = options
            .schema_bfbs
            .as_ref()
            .map(|bfbs| bfbs_contains_aorta_header(bfbs));
        let schema_attach_policy = options
            .schema_attach_policy
            .resolve(node.schema_attach_policy());

        if matches!(schema_attach_policy, SchemaAttachPolicy::Always)
            && options.schema_bfbs.is_none()
        {
            return Err(AortaError::invalid_argument(
                "schema required when schema attachment is always",
            ));
        }

        let schema_hash = if let Some(bfbs) = options.schema_bfbs {
            Some(
                node.register_schema_for_topic_bfbs(topic, bfbs, options.schema_root_type)
                    .await?,
            )
        } else {
            None
        };
        let schema_attachment = if matches!(schema_attach_policy, SchemaAttachPolicy::Always) {
            let hash = schema_hash.ok_or_else(|| {
                AortaError::invalid_argument("schema hash required for attachment")
            })?;
            Some(ZBytes::from(hash.to_hex_bytes().to_vec()))
        } else {
            None
        };

        if header_probe.is_none() {
            aorta_core_warn!(
                "Aorta WARN: publisher '{}' created without schema metadata; \
                 cannot validate aorta_header or auto-register bfbs.",
                topic
            );
        } else if header_probe == Some(false) {
            aorta_core_warn!(
                "Aorta WARN: schema for topic '{}' missing aorta_header; \
                 header auto-injection disabled for this publisher.",
                topic
            );
        }

        let key_expr = node.key_builder().topic_key_pub(topic)?;
        let key_expr: String = key_expr.into();

        // Apply QoS settings to Zenoh Publisher.
        //
        // Express has two opt-in surfaces post-merge: `QoS::express` (set via
        // QoS preset like `realtime_control` or `with_express(true)`) and the
        // newer `PublisherOptions::express` knob. OR them so either API enables
        // express; this matches the "any opt-in turns it on" intent and keeps
        // existing callers of both APIs working.
        let express = options.qos.express || options.express;
        // `congestion_control` now lives on `QoS`; `PublisherOptions` keeps an
        // optional write-through override for one release.
        let congestion_control = options
            .congestion_control
            .unwrap_or(options.qos.congestion_control);
        let pub_builder = node
            .session()
            .inner()
            .declare_publisher(key_expr)
            .priority(options.qos.priority.into())
            .reliability(options.qos.reliability.into())
            .congestion_control(congestion_control.into())
            .express(express);

        let publisher = pub_builder.await?;
        let inventory_guard = if options.register_telemetry {
            match node
                .vital_telemetry()
                .register_publisher(node.key_builder(), topic, schema_hash)
            {
                Ok(guard) => guard,
                Err(err) => {
                    aorta_core_warn!(
                        "Aorta WARN: publisher inventory telemetry disabled for '{}': {}",
                        topic,
                        err
                    );
                    TelemetryResourceGuard::disabled()
                }
            }
        } else {
            TelemetryResourceGuard::disabled()
        };
        let loan_tracker = Arc::new(LoanTracker::new(options.max_loaned_samples));
        let runtime_stats = Arc::new(PublisherRuntimeStatsInner::default());
        let runtime_guard = if options.register_telemetry {
            match node.runtime_metrics_telemetry().register_publisher(
                topic,
                schema_hash,
                Arc::new({
                    let loan_tracker = Arc::clone(&loan_tracker);
                    let runtime_stats = Arc::clone(&runtime_stats);
                    move || PublisherRuntimeStats {
                        publish_shm_hit_total: runtime_stats
                            .publish_shm_hit_total
                            .load(Ordering::Relaxed),
                        publish_shm_fallback_total: runtime_stats
                            .publish_shm_fallback_total
                            .load(Ordering::Relaxed),
                        publish_shm_required_fail_total: runtime_stats
                            .publish_shm_required_fail_total
                            .load(Ordering::Relaxed),
                        loan_inflight_current: loan_tracker.inflight.load(Ordering::Relaxed) as u64,
                        loan_quota_exceeded_total: loan_tracker
                            .quota_exceeded_total
                            .load(Ordering::Relaxed),
                    }
                }),
            ) {
                Ok(guard) => guard,
                Err(err) => {
                    aorta_core_warn!(
                        "Aorta WARN: publisher runtime telemetry disabled for '{}': {}",
                        topic,
                        err
                    );
                    RuntimeTelemetryGuard::disabled()
                }
            }
        } else {
            RuntimeTelemetryGuard::disabled()
        };

        Ok(Self {
            shared: Arc::new(PublisherShared {
                transport: Arc::new(ZenohTransportPublisher::new(publisher)),
                session: node.session().clone(),
                topic: topic.to_string(),
                schema_hash,
                schema_attachment,
                topic_publish_event: crate::trace::stage_event_for(
                    crate::trace::StageId::TopicPublish,
                    topic,
                ),
                allocation_policy: options.allocation_policy,
                max_message_size: options.max_message_size,
                loan_tracker,
                heap_allocator: Arc::new(HeapLoanAllocator::default()),
                runtime_stats,
                inventory_guard,
                runtime_guard,
            }),
            clone_token: Arc::new(()),
        })
    }

    /// One-shot readiness check: is there at least one matching subscriber for
    /// this publisher's key expression right now (Locality::Any)? Reads local
    /// routing tables (no network round-trip). `true` means a put issued now
    /// would be routed rather than dropped at the empty-route check — use it to
    /// gate the first publish on a fresh client. Resolved via `.await` (never
    /// `.wait()`), so it is safe on a `current_thread` runtime.
    pub async fn matching_status(&self) -> AortaResult<bool> {
        self.shared.transport.matching_status().await
    }

    /// Wait (bounded by `timeout`) until a matching subscriber appears. Returns
    /// `Ok(true)` once visible, or `Ok(false)` on timeout (NOT an error, so
    /// callers can "fire anyway"). Bounded async poll over
    /// [`matching_status`](Self::matching_status) — a one-time cold-start gate,
    /// not a hot path.
    pub async fn wait_for_matching(&self, timeout: Duration) -> AortaResult<bool> {
        const POLL_INTERVAL: Duration = Duration::from_millis(20);
        let start = std::time::Instant::now();
        loop {
            if self.matching_status().await? {
                return Ok(true);
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Ok(false);
            }
            tokio::time::sleep((timeout - elapsed).min(POLL_INTERVAL)).await;
        }
    }

    pub fn topic(&self) -> &str {
        &self.shared.topic
    }

    pub fn schema_hash(&self) -> Option<SchemaHash> {
        self.shared.schema_hash
    }

    pub fn runtime_stats(&self) -> PublisherRuntimeStats {
        self.shared.runtime_stats()
    }

    /// Synchronous fire-and-forget publish (unified API)
    ///
    /// Publishes the payload directly into zenoh's internal send buffer.
    /// The zenoh `put()` operation resolves immediately (returns `Ready<>`),
    /// so this call never blocks on I/O. Returns after the message enters
    /// the zenoh transport pipeline.
    ///
    /// # Arguments
    /// * `payload` - Message payload convertible to ZBytes
    ///
    /// # Returns
    /// * `Ok(())` if the message was published successfully
    /// * `Err` if the publisher has been undeclared or the mutex is poisoned
    ///
    /// # Usage
    /// Can be called from any context (async functions, sync functions, timer callbacks):
    /// ```no_run
    /// use aorta_core::node::Node;
    ///
    /// # async fn example() -> aorta_core::error::AortaResult<()> {
    /// let node = Node::new("publisher_node", "default").await?;
    /// let publisher = node.create_publisher("/example_topic").await?;
    /// let payload = vec![1u8, 2, 3];
    ///
    /// // In async context - no .await needed
    /// publisher.publish(payload.clone())?;
    ///
    /// // In sync context (timer callback)
    /// publisher.publish(payload)?;
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn publish<IntoZBytes>(&self, payload: IntoZBytes) -> AortaResult<()>
    where
        IntoZBytes: Into<ZBytes> + Send + 'static,
    {
        let payload: ZBytes = payload.into();
        if matches!(self.shared.allocation_policy, AllocationPolicy::HeapOnly) {
            return self.shared.publish_direct_zbytes(payload);
        }
        let contiguous = payload.to_bytes();
        let owned = OwnedPayload::from_bytes(bytes::Bytes::from(contiguous.into_owned()));
        self.publish_payload(AortaPayload::Owned(owned))
    }

    /// Synchronous fire-and-forget publish via transport-agnostic payload.
    pub(crate) fn publish_payload(&self, payload: AortaPayload) -> AortaResult<()> {
        self.shared.publish_payload(payload)
    }

    pub(crate) fn publish_prepared(&self, payload: PreparedPublish) -> AortaResult<()> {
        match payload {
            PreparedPublish::Payload(payload) => self.shared.publish_payload(payload),
            PreparedPublish::Direct(payload) => self.shared.publish_shm_zbytes(payload),
        }
    }

    /// Expert API: loan a writable byte buffer for zero-copy commit into the
    /// transport pipeline.
    pub(crate) fn loan_bytes(&self, size: usize) -> AortaResult<LoanedBytes> {
        self.shared.loan_bytes(size)
    }

    pub async fn undeclare(self) -> AortaResult<()> {
        if self.shared.has_outstanding_loans() {
            return Err(AortaError::busy(format!(
                "publisher '{}' has outstanding loaned buffers",
                self.shared.topic
            )));
        }

        // If other clones still hold references, skip undeclare.
        if Arc::strong_count(&self.clone_token) > 1 {
            aorta_core_warn!(
                "Aorta WARN: Cannot undeclare publisher '{}' - still has active references",
                self.shared.topic
            );
            return Ok(());
        }
        self.shared.transport.undeclare()
    }
}

impl PublisherShared {
    fn runtime_stats(&self) -> PublisherRuntimeStats {
        PublisherRuntimeStats {
            publish_shm_hit_total: self
                .runtime_stats
                .publish_shm_hit_total
                .load(Ordering::Relaxed),
            publish_shm_fallback_total: self
                .runtime_stats
                .publish_shm_fallback_total
                .load(Ordering::Relaxed),
            publish_shm_required_fail_total: self
                .runtime_stats
                .publish_shm_required_fail_total
                .load(Ordering::Relaxed),
            loan_inflight_current: self.loan_tracker.inflight.load(Ordering::Relaxed) as u64,
            loan_quota_exceeded_total: self
                .loan_tracker
                .quota_exceeded_total
                .load(Ordering::Relaxed),
        }
    }

    // Topic-path trace bracket. Every PUBLIC publish entry funnels
    // through here exactly once: it measures the publish interval for the
    // always-on `topic.publish` histogram, mints the publish span id + builds
    // the traceparent attachment ONLY when topic spans are enabled (else the
    // byte-identical legacy schema attachment), and emits the waterfall span on
    // return. The internal recursion uses the `*_inner` variants (which take the
    // resolved attachment) so a single message is never double-bracketed.
    #[inline]
    fn publish_traced<R>(&self, run: impl FnOnce(Option<ZBytes>) -> R) -> R {
        if !crate::trace::trace_enabled() {
            return run(self.schema_attachment.clone());
        }
        let start = crate::time::monotonic_ns();
        // Mint the publish span id ON THIS THREAD so a publish inside a traced
        // callback (e.g. a subscriber re-publishing) joins the enclosing trace
        // via `outbound_link`; otherwise it is a fresh root.
        let link = if crate::trace::topic_scope_enabled() {
            crate::trace::outbound_link()
        } else {
            None
        };
        let attachment = match link {
            Some(l) => self.traced_attachment(&l),
            None => self.schema_attachment.clone(),
        };
        let result = run(attachment);
        crate::trace::record_topic_span(
            crate::trace::StageId::TopicPublish,
            start,
            self.topic_publish_event,
            link,
        );
        result
    }

    /// Composite attachment carrying the publish traceparent (and the schema
    /// hash IFF this publisher's policy would have attached it — preserving the
    /// schema-attach contract). Built only on the topic-tracing-enabled path.
    fn traced_attachment(&self, link: &crate::trace::SpanLink) -> Option<ZBytes> {
        let schema_hash = if self.schema_attachment.is_some() {
            self.schema_hash
        } else {
            None
        };
        crate::attachment::encode(&crate::attachment::AttachmentParts {
            schema_hash,
            traceparent: Some(crate::trace::TraceParent {
                trace_id: link.trace_id,
                span_id: link.span_id,
                flags: crate::trace::TRACEPARENT_SAMPLED,
            }),
            reply_busy_ns: None,
            deadline_budget_ns: None,
        })
        .map(ZBytes::from)
    }

    #[inline]
    fn publish_direct_zbytes(&self, payload: ZBytes) -> AortaResult<()> {
        self.publish_traced(move |attachment| self.publish_direct_zbytes_inner(payload, attachment))
    }

    #[inline]
    fn publish_direct_zbytes_inner(
        &self,
        payload: ZBytes,
        attachment: Option<ZBytes>,
    ) -> AortaResult<()> {
        self.transport.publish_zbytes(payload, attachment)
    }

    fn publish_shm_zbytes(&self, payload: ZBytes) -> AortaResult<()> {
        self.publish_traced(move |attachment| self.publish_shm_zbytes_inner(payload, attachment))
    }

    fn publish_shm_zbytes_inner(
        &self,
        payload: ZBytes,
        attachment: Option<ZBytes>,
    ) -> AortaResult<()> {
        self.runtime_stats
            .publish_shm_hit_total
            .fetch_add(1, Ordering::Relaxed);
        self.publish_direct_zbytes_inner(payload, attachment)
    }

    #[inline]
    fn publish_payload(&self, payload: AortaPayload) -> AortaResult<()> {
        self.publish_traced(move |attachment| self.publish_payload_inner(payload, attachment))
    }

    #[inline]
    fn publish_payload_inner(
        &self,
        payload: AortaPayload,
        attachment: Option<ZBytes>,
    ) -> AortaResult<()> {
        match payload {
            AortaPayload::Owned(owned) => self.publish_owned_payload_inner(owned, attachment),
            AortaPayload::Shm(shm) => {
                self.runtime_stats
                    .publish_shm_hit_total
                    .fetch_add(1, Ordering::Relaxed);
                self.transport.publish(AortaPayload::Shm(shm), attachment)
            }
        }
    }

    #[inline]
    fn publish_owned_payload_inner(
        &self,
        owned: OwnedPayload,
        attachment: Option<ZBytes>,
    ) -> AortaResult<()> {
        if owned.is_empty() || matches!(self.allocation_policy, AllocationPolicy::HeapOnly) {
            return self
                .transport
                .publish(AortaPayload::Owned(owned), attachment);
        }

        match self.copy_owned_to_shm(owned.payload()) {
            Ok(payload) => self.publish_shm_zbytes_inner(payload, attachment),
            Err(err) if matches!(self.allocation_policy, AllocationPolicy::BestEffort) => {
                self.record_best_effort_fallback("publish", &err);
                self.transport
                    .publish(AortaPayload::Owned(owned), attachment)
            }
            Err(err) => {
                self.record_shm_required_failure("publish", &err);
                Err(err)
            }
        }
    }

    fn copy_owned_to_shm(&self, payload: &[u8]) -> AortaResult<ZBytes> {
        let mut shm = self.try_allocate_shm(payload.len())?;
        shm.as_mut()[..payload.len()].copy_from_slice(payload);
        Ok(ZBytes::from(shm))
    }

    fn loan_bytes(self: &Arc<Self>, size: usize) -> AortaResult<LoanedBytes> {
        self.validate_loan_size(size)?;

        if let Err(err) = self.loan_tracker.try_acquire(&self.topic) {
            self.maybe_log_loan_quota_exceeded(&err);
            return Err(err);
        }
        let buffer = match self.allocate_loan_buffer(size) {
            Ok(buffer) => buffer,
            Err(err) => {
                self.loan_tracker.release();
                return Err(err);
            }
        };

        Ok(LoanedBytes {
            publisher: Arc::clone(self),
            buffer: Some(buffer),
        })
    }

    fn validate_loan_size(&self, size: usize) -> AortaResult<()> {
        if let Some(max) = self.max_message_size {
            if size > max {
                return Err(AortaError::capacity_exceeded(format!(
                    "loan size {} exceeds max_message_size {} for publisher '{}'",
                    size, max, self.topic
                )));
            }
        }
        Ok(())
    }

    fn allocate_loan_buffer(&self, size: usize) -> AortaResult<LoanBuffer> {
        match self.allocation_policy {
            AllocationPolicy::HeapOnly => self.heap_allocator.allocate(size).map(LoanBuffer::Heap),
            AllocationPolicy::BestEffort => match self.try_allocate_shm(size) {
                Ok(buffer) => Ok(LoanBuffer::Shm(buffer)),
                Err(err) => {
                    self.record_best_effort_fallback("loan_bytes", &err);
                    self.heap_allocator.allocate(size).map(LoanBuffer::Heap)
                }
            },
            AllocationPolicy::ShmRequired => self
                .try_allocate_shm(size)
                .map(LoanBuffer::Shm)
                .inspect_err(|err| {
                    self.record_shm_required_failure("loan_bytes", err);
                }),
        }
    }

    fn record_best_effort_fallback(&self, operation: &str, err: &AortaError) {
        let total = self
            .runtime_stats
            .publish_shm_fallback_total
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if should_log_policy_event(total) {
            aorta_core_debug!(
                "Aorta DEBUG: publisher '{}' {} fell back to heap under BestEffort \
                 after SHM path failure (count={}): {}",
                self.topic,
                operation,
                total,
                err
            );
        }
    }

    fn record_shm_required_failure(&self, operation: &str, err: &AortaError) {
        let total = self
            .runtime_stats
            .publish_shm_required_fail_total
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if should_log_policy_event(total) {
            aorta_core_debug!(
                "Aorta DEBUG: publisher '{}' {} failed under ShmRequired \
                 because SHM submission was unavailable (count={}): {}",
                self.topic,
                operation,
                total,
                err
            );
        }
    }

    fn maybe_log_loan_quota_exceeded(&self, err: &AortaError) {
        if !matches!(err, AortaError::QuotaExceeded(_)) {
            return;
        }
        let total = self.loan_tracker.quota_exceeded_total();
        if should_log_policy_event(total) {
            aorta_core_debug!(
                "Aorta DEBUG: publisher '{}' loan_bytes hit max_loaned_samples \
                 and was rejected (count={}): {}",
                self.topic,
                total,
                err
            );
        }
    }

    fn try_allocate_shm(&self, size: usize) -> AortaResult<zenoh::shm::ZShmMut> {
        let provider = self
            .session
            .inner()
            .get_shm_provider()
            .into_option()
            .ok_or_else(|| {
                AortaError::shm_unavailable(format!(
                    "publisher '{}' SHM provider is unavailable",
                    self.topic
                ))
            })?;

        provider.alloc(size).wait().map_err(|err| {
            AortaError::shm_unavailable(format!(
                "publisher '{}' SHM allocation of {} bytes failed: {}",
                self.topic, size, err
            ))
        })
    }

    fn has_outstanding_loans(&self) -> bool {
        self.loan_tracker.has_outstanding()
    }
}

impl LoanTracker {
    fn new(max_inflight: Option<u32>) -> Self {
        Self {
            inflight: AtomicU32::new(0),
            max_inflight,
            quota_exceeded_total: AtomicU64::new(0),
        }
    }

    fn try_acquire(&self, topic: &str) -> AortaResult<()> {
        loop {
            let current = self.inflight.load(Ordering::Relaxed);
            if let Some(max) = self.max_inflight {
                if current >= max {
                    self.quota_exceeded_total.fetch_add(1, Ordering::Relaxed);
                    return Err(AortaError::quota_exceeded(format!(
                        "max_loaned_samples ({max}) reached for publisher '{topic}'"
                    )));
                }
            }
            if self
                .inflight
                .compare_exchange_weak(
                    current,
                    current.saturating_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn release(&self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }

    fn has_outstanding(&self) -> bool {
        self.inflight.load(Ordering::Acquire) > 0
    }

    fn quota_exceeded_total(&self) -> u64 {
        self.quota_exceeded_total.load(Ordering::Relaxed)
    }
}

impl Default for PublisherRuntimeStatsInner {
    fn default() -> Self {
        Self {
            publish_shm_hit_total: AtomicU64::new(0),
            publish_shm_fallback_total: AtomicU64::new(0),
            publish_shm_required_fail_total: AtomicU64::new(0),
        }
    }
}

impl HeapLoanAllocator {
    fn allocate(&self, size: usize) -> AortaResult<Vec<u8>> {
        let mut free = self
            .free
            .lock()
            .map_err(|_| AortaError::internal("heap loan allocator mutex poisoned"))?;

        if let Some(index) = free.iter().position(|buf| buf.capacity() >= size) {
            let mut buf = free.swap_remove(index);
            buf.resize(size, 0);
            return Ok(buf);
        }
        drop(free);

        Ok(vec![0u8; size])
    }

    fn recycle(&self, mut buf: Vec<u8>) {
        buf.clear();
        if let Ok(mut free) = self.free.lock() {
            free.push(buf);
        }
    }
}

impl LoanBuffer {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        match self {
            Self::Heap(buf) => buf.as_mut_slice(),
            Self::Shm(buf) => buf.as_mut(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Heap(buf) => buf.len(),
            Self::Shm(buf) => buf.len(),
        }
    }

    fn into_publish(
        self,
        allocator: Arc<HeapLoanAllocator>,
        written_len: usize,
    ) -> PreparedPublish {
        match self {
            Self::Heap(mut buf) => {
                buf.truncate(written_len);
                let bytes = Bytes::from_owner(RecyclableHeapBuffer {
                    data: buf,
                    allocator,
                });
                PreparedPublish::Payload(AortaPayload::Owned(OwnedPayload::from_bytes(bytes)))
            }
            Self::Shm(buf) => {
                if written_len == buf.len() {
                    PreparedPublish::Direct(ZBytes::from(buf))
                } else {
                    let bytes = Bytes::copy_from_slice(&buf[..written_len]);
                    PreparedPublish::Payload(AortaPayload::Owned(OwnedPayload::from_bytes(bytes)))
                }
            }
        }
    }

    fn recycle(self, allocator: &Arc<HeapLoanAllocator>) {
        match self {
            Self::Heap(buf) => allocator.recycle(buf),
            Self::Shm(_buf) => {}
        }
    }
}

impl RecyclableHeapBuffer {
    fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }
}

impl AsRef<[u8]> for RecyclableHeapBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for RecyclableHeapBuffer {
    fn drop(&mut self) {
        let buf = std::mem::take(&mut self.data);
        self.allocator.recycle(buf);
    }
}

impl LoanedBytes {
    pub fn capacity(&self) -> usize {
        self.buffer.as_ref().map(LoanBuffer::capacity).unwrap_or(0)
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer
            .as_mut()
            .expect("loaned buffer already finalized")
            .as_mut_slice()
    }

    pub fn commit(mut self, written_len: usize) -> AortaResult<()> {
        match self.prepare_commit(written_len)? {
            PreparedPublish::Payload(payload) => self.publisher.publish_payload(payload),
            PreparedPublish::Direct(payload) => self.publisher.publish_shm_zbytes(payload),
        }
    }

    pub fn abort(mut self) {
        self.abort_in_place();
    }

    pub(crate) fn prepare_commit(&mut self, written_len: usize) -> AortaResult<PreparedPublish> {
        let capacity = self.capacity();
        if written_len > capacity {
            return Err(AortaError::invalid_argument(format!(
                "written_len {} exceeds loan capacity {}",
                written_len, capacity
            )));
        }

        let buffer = self
            .buffer
            .take()
            .ok_or_else(|| AortaError::internal("loaned buffer already finalized"))?;
        self.publisher.loan_tracker.release();
        Ok(buffer.into_publish(Arc::clone(&self.publisher.heap_allocator), written_len))
    }

    pub(crate) fn writable_parts(&mut self) -> (*mut u8, usize) {
        let slice = self.as_mut_slice();
        let ptr = if slice.is_empty() {
            std::ptr::null_mut()
        } else {
            slice.as_mut_ptr()
        };
        (ptr, slice.len())
    }

    fn abort_in_place(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            buffer.recycle(&self.publisher.heap_allocator);
            self.publisher.loan_tracker.release();
        }
    }
}

impl Drop for LoanedBytes {
    fn drop(&mut self) {
        self.abort_in_place();
    }
}

#[cfg(test)]
#[path = "../test/publisher_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/publisher_advanced_tests.rs"]
mod advanced_tests;
