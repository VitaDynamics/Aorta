use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aorta_core::action::{
    goal_id_to_key, ActionInfo, ActionReplyCode, ActionRootType, ActionStatus,
};
use aorta_core::action_server::DEFAULT_ACTION_CONTROL_TIMEOUT_MS;
use aorta_core::contextstore_fb::{root_as_context_value, ContextKind};
use aorta_core::key_expr::normalize_path;
use aorta_core::schema::{bfbs_root_type_name, SchemaHash};
use aorta_core::schema_registry::ServiceSchemaByKeyValue;
use aorta_core::telemetry::schema::{ResourceInventoryTelemetry, RuntimeMetricsTelemetry};
use aorta_core::KeyExprBuilder;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use zenoh::query::{ConsolidationMode, QueryTarget};
use zenoh::session::{
    Link, LinkEvent, LinkEventsListener, Transport, TransportEvent, TransportEventsListener,
};

use crate::error::{IntrospectError, IntrospectErrorKind, IntrospectResult};
use crate::ids::{EndpointId, GroupName, NodeId, NodeName, ServiceSuffix, TopicSuffix};
use crate::reflect;
use crate::schema::{
    build_schema_model as build_schema_model_impl, ResolvedSchema, ResolvedServiceSchema,
    ResolvedTopicSchema, SchemaModel, SchemaRootSelector,
};
use crate::topic_schema::{
    decode_topic_schema_payload, extract_attachment_schema_hash, TopicSchemaResolver,
    TopicSchemaResolverConfig,
};

#[derive(Debug, Clone)]
pub struct IntrospectOptions {
    pub group: GroupName,
    pub admin_query_timeout: Duration,
    pub schema_query_timeout: Duration,
    pub service_call_timeout: Duration,
    pub rate_sample_duration: Duration,
    pub liveliness_timeout: Duration,
    pub telemetry_cache_ttl: Duration,
    pub topic_schema: TopicSchemaResolverConfig,
}

impl IntrospectOptions {
    pub fn new(group: GroupName) -> Self {
        Self {
            group,
            admin_query_timeout: Duration::from_secs(2),
            schema_query_timeout: Duration::from_secs(2),
            service_call_timeout: Duration::from_secs(2),
            rate_sample_duration: Duration::from_secs(1),
            liveliness_timeout: Duration::from_secs(2),
            telemetry_cache_ttl: Duration::from_millis(200),
            topic_schema: TopicSchemaResolverConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provenance {
    Exact,
    Declared,
    Observed,
    Resolved,
    Inferred,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithProvenance<T> {
    pub value: T,
    pub provenance: Provenance,
    pub note: Option<String>,
}

impl<T> WithProvenance<T> {
    fn exact(value: T) -> Self {
        Self {
            value,
            provenance: Provenance::Exact,
            note: None,
        }
    }

    fn declared(value: T) -> Self {
        Self {
            value,
            provenance: Provenance::Declared,
            note: None,
        }
    }

    fn observed(value: T) -> Self {
        Self {
            value,
            provenance: Provenance::Observed,
            note: None,
        }
    }

    fn resolved(value: T) -> Self {
        Self {
            value,
            provenance: Provenance::Resolved,
            note: None,
        }
    }

    fn inferred(value: T, note: impl Into<String>) -> Self {
        Self {
            value,
            provenance: Provenance::Inferred,
            note: Some(note.into()),
        }
    }

    fn unavailable(value: T, note: impl Into<String>) -> Self {
        Self {
            value,
            provenance: Provenance::Unavailable,
            note: Some(note.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateNodeRef {
    pub name: NodeName,
    pub id: NodeId,
    pub matched_endpoint_ids: Vec<EndpointId>,
}

#[derive(Debug, Clone)]
pub struct TopicListOptions {
    pub include_candidate_nodes: bool,
    pub node_filter: Option<NodeSelector>,
}

impl Default for TopicListOptions {
    fn default() -> Self {
        Self {
            include_candidate_nodes: true,
            node_filter: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicDetailOptions {
    pub include_candidate_nodes: bool,
    pub include_rate_sample: bool,
    pub rate_sample_duration: Duration,
}

impl Default for TopicDetailOptions {
    fn default() -> Self {
        Self {
            include_candidate_nodes: true,
            include_rate_sample: false,
            rate_sample_duration: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSnapshot {
    pub topic: TopicSuffix,
    pub present_in_admin: bool,
    /// True when a publisher of this topic reported a registered `schema_hash`
    /// in the telemetry inventory snapshot — i.e. the topic has a known schema.
    /// Drives the list "Schema" column (`confirmed` vs `?`). False in
    /// admin-only mode (no telemetry) or when no publisher declared a schema.
    pub schema_present: bool,
    pub provider_endpoints: WithProvenance<Vec<EndpointId>>,
    pub candidate_nodes: WithProvenance<Vec<CandidateNodeRef>>,
    pub publisher_node_count: WithProvenance<Option<usize>>,
    pub subscriber_node_count: WithProvenance<Option<usize>>,
    pub runtime: WithProvenance<Option<TopicRuntimeSummary>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicDetail {
    pub key_expr: String,
    pub snapshot: TopicSnapshot,
    pub schema: WithProvenance<Option<ResolvedTopicSchema>>,
    pub approx_rate: WithProvenance<Option<RateSnapshot>>,
    pub publisher_nodes: WithProvenance<Vec<CandidateNodeRef>>,
    pub subscriber_nodes: WithProvenance<Vec<CandidateNodeRef>>,
    pub runtime: WithProvenance<Option<TopicRuntimeSummary>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RateSnapshot {
    pub average_rate_hz: f64,
    pub min_interval_s: f64,
    pub max_interval_s: f64,
    pub stddev_interval_s: f64,
    pub window_samples: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRuntimeSummary {
    pub latest_timestamp_ns: u64,
    pub collection_period_ms: u32,
    pub publisher_metric_count: usize,
    pub subscriber_metric_count: usize,
    pub publish_shm_hit_total: u64,
    pub publish_shm_fallback_total: u64,
    pub publish_shm_required_fail_total: u64,
    pub loan_inflight_current: u64,
    pub loan_quota_exceeded_total: u64,
    pub subscriber_duplicate_drop_count: u64,
    pub subscriber_out_of_order_count: u64,
    pub subscriber_out_of_order_drop_count: u64,
    pub subscriber_executor_drop_count: u64,
    pub subscriber_local_drop_count: u64,
    pub subscriber_overflow_relay_enqueue_count: u64,
    pub subscriber_overflow_relay_drop_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicPreviewDecodeStatus {
    DecodedFromTopicSchema,
    DecodedFromAttachmentSchema,
    RawOnly,
    DecodeError(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPreviewFrame {
    pub topic: TopicSuffix,
    pub key_expr: String,
    pub payload_hex: String,
    pub decoded_json: Option<String>,
    pub schema_hash: Option<String>,
    pub decode_status: TopicPreviewDecodeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicPreviewEvent {
    Sample(TopicPreviewFrame),
    Error(String),
}

/// A single decoded entry from the Aorta system `/log` stream.
///
/// The `/log` topic carries `foxglove.Log` FlatBuffers published by the
/// in-process log forwarder (`aorta_core::logging::start_topic_forwarding`).
/// The forwarder registers a `schema_by_key` queryable for the topic, so
/// `watch_log_stream` resolves the schema and reflects each payload to JSON
/// exactly like the topic preview path, then lifts the well-known fields into
/// typed columns for the TUI. Unparseable fields fall back to defaults so a
/// partial or schema-less record still renders; the legacy
/// `aorta.sys.LogRecord` shape is still accepted (see
/// `log_record_frame_from_preview`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecordFrame {
    pub timestamp_ns: u64,
    pub level: String,
    pub target: String,
    pub message: String,
    pub node: String,
    /// Best-effort decoded JSON when the schema was resolvable; `None` when the
    /// payload could only be rendered raw (kept for diagnostics/export parity).
    pub decoded_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEvent {
    Record(LogRecordFrame),
    Error(String),
}

/// A single decoded `Feedback` entry for an in-flight action goal.
///
/// Mirrors the CLI `aorta action feedback` decode (reflect the `Feedback` root
/// type to JSON, lift `goal_id`/`seq`/`stamp_ns`/`status`/`message`/`data`).
/// `watch_action_feedback` filters the live feedback key down to a single goal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionFeedbackFrame {
    pub goal_id: String,
    pub seq: u64,
    pub stamp_ns: u64,
    pub status: String,
    pub message: String,
    /// The action-specific feedback payload, rendered as compact JSON.
    pub data_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionFeedbackEvent {
    Feedback(ActionFeedbackFrame),
    Error(String),
}

#[derive(Debug, Clone)]
pub struct ServiceListOptions {
    pub include_candidate_nodes: bool,
    pub node_filter: Option<NodeSelector>,
}

impl Default for ServiceListOptions {
    fn default() -> Self {
        Self {
            include_candidate_nodes: true,
            node_filter: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServiceDetailOptions {
    pub include_candidate_nodes: bool,
}

impl Default for ServiceDetailOptions {
    fn default() -> Self {
        Self {
            include_candidate_nodes: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSnapshot {
    pub service: ServiceSuffix,
    pub present_in_admin: bool,
    /// True when a provider of this service reported a registered `schema_hash`
    /// in the telemetry inventory snapshot — i.e. the service has a known
    /// schema. Drives the list "Schema" column. False in admin-only mode or
    /// when no provider declared a schema.
    pub schema_present: bool,
    pub provider_endpoints: WithProvenance<Vec<EndpointId>>,
    pub candidate_nodes: WithProvenance<Vec<CandidateNodeRef>>,
    pub provider_node_count: WithProvenance<Option<usize>>,
    pub client_node_count: WithProvenance<Option<usize>>,
    pub runtime: WithProvenance<Option<ServiceRuntimeSummary>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDetail {
    pub key_expr: String,
    pub snapshot: ServiceSnapshot,
    pub schema: WithProvenance<Option<ResolvedServiceSchema>>,
    pub provider_nodes: WithProvenance<Vec<CandidateNodeRef>>,
    pub client_nodes: WithProvenance<Vec<CandidateNodeRef>>,
    pub runtime: WithProvenance<Option<ServiceRuntimeSummary>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionGoalSnapshot {
    pub goal_id: String,
    pub status: ActionStatus,
    pub code: ActionReplyCode,
    pub accepted_stamp_ns: u64,
    pub started_stamp_ns: u64,
    pub finished_stamp_ns: u64,
    pub last_feedback_seq: u64,
    pub server_epoch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionSnapshot {
    pub action_name: String,
    pub key_expr: String,
    pub info: ActionInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionDetail {
    pub snapshot: ActionSnapshot,
    pub goals: Vec<ActionGoalSnapshot>,
    /// Fully-qualified FlatBuffers root type names for every action
    /// surface. Resolved from the compiled `.bfbs` schema in
    /// `get_action_detail` — previously was derived
    /// from `action_name` via path projection but that constrained
    /// action names to the FlatBuffers identifier set.
    /// `ActionSnapshot::root_types` was removed because the cheap
    /// `list_actions` path doesn't fetch bfbs; if you need root type
    /// names, call `get_action_detail`.
    pub root_types: ActionRootTypeNames,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRootTypeNames {
    pub send_goal_request: String,
    pub send_goal_response: String,
    pub cancel_goal_request: String,
    pub cancel_goal_response: String,
    pub feedback: String,
    pub public_status: String,
    pub get_result_request: String,
    pub get_result_response: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRuntimeSummary {
    pub latest_timestamp_ns: u64,
    pub collection_period_ms: u32,
    pub service_metric_count: usize,
    pub client_metric_count: usize,
    pub service_request_received_count: u64,
    pub service_request_dispatched_count: u64,
    pub service_reply_ok_count: u64,
    pub service_reply_error_count: u64,
    pub service_reply_send_failure_count: u64,
    pub service_request_dropped_count: u64,
    pub service_in_flight_requests: u64,
    pub client_call_count: u64,
    pub client_success_count: u64,
    pub client_timeout_count: u64,
    pub client_concurrent_conflict_count: u64,
    pub client_error_count: u64,
    pub client_retry_count: u64,
    /// Latency (a true elapsed duration) of the **most-recently-finished** client
    /// call for this service — the row with the greatest `last_call_finished_ns`,
    /// not the slowest. `0` when no call has completed.
    pub client_last_call_latency_ns: u64,
    /// Wall-clock `last_call_finished_ns` stamp of that most-recently-finished
    /// client call (`0` if no call has completed). Pairs with
    /// `client_last_call_latency_ns`; used as a de-duplication key when a tool
    /// polls this summary over time so the same unchanged last-value is not
    /// counted twice across successive polls. NOT a wire field — derived here.
    pub client_last_call_finished_ns: u64,
    /// Most recent `last_request_ns` wall-clock stamp across service rows (`0` if unset).
    pub service_last_request_ns: u64,
    /// Most recent `last_reply_ns` wall-clock stamp across service rows (`0` if unset).
    pub service_last_reply_ns: u64,
}

/// Client-observed latency distribution computed CLIENT-SIDE from samples a tool
/// has already observed over time (e.g. successive `last_call_latency_ns` values).
///
/// Percentiles are **approximate** — they are read from a bucket-bounded
/// [`aorta_core::trace::FixedHistogram`] (power-of-two ns buckets, factor-of-2
/// coarse) rather than from exact order statistics. They reflect what the TOOL
/// observed (network + tool overhead included), NOT the robot's internal
/// per-stage `record_stage` latency. Always label readouts "approx" and
/// "client-observed" to avoid implying exactness or on-robot stage timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencySnapshot {
    /// Number of samples fed into the histogram.
    pub count: u64,
    /// Approximate 50th-percentile latency in ns (bucket-bounded).
    pub p50_ns: u64,
    /// Approximate 95th-percentile latency in ns (bucket-bounded).
    pub p95_ns: u64,
    /// Approximate 99th-percentile latency in ns (bucket-bounded).
    pub p99_ns: u64,
    /// Exact maximum observed sample in ns (tracked precisely by the histogram).
    pub max_ns: u64,
    /// Arithmetic mean of all samples in ns.
    pub mean_ns: u64,
}

/// Feed `samples` (observed latencies in ns) into a tool-local
/// [`aorta_core::trace::FixedHistogram`] and read back the approximate
/// percentile distribution. Returns `None` when `samples` is empty.
///
/// Reuses the core lock-free histogram + `HistogramSnapshot` percentile engine —
/// percentiles are NOT re-implemented here. Pure and allocation-light; safe to
/// call on any sample slice with no Zenoh session.
pub fn compute_latency_snapshot(samples: &[u64]) -> Option<LatencySnapshot> {
    if samples.is_empty() {
        return None;
    }
    let histogram = aorta_core::trace::FixedHistogram::new();
    for &sample in samples {
        histogram.record(sample);
    }
    let snapshot = histogram.snapshot();
    Some(LatencySnapshot {
        count: snapshot.count,
        p50_ns: snapshot.p50_ns(),
        p95_ns: snapshot.p95_ns(),
        p99_ns: snapshot.p99_ns(),
        max_ns: snapshot.max_ns,
        mean_ns: snapshot.mean_ns(),
    })
}

/// A small bounded ring of observed latency samples keyed by a resource
/// identifier, with de-duplication on the `last_call_finished_ns` stamp.
///
/// A tool polls runtime telemetry (which carries only a single *last-value*
/// `last_call_latency_ns`) on a cadence; the same unchanged last-value would be
/// double-counted across polls without a dedup key. [`observe`] records a latency
/// only when the paired `finished_ns` stamp advances past the previously-seen
/// one for that resource, turning the single last-value into an observed-over-
/// time distribution without any robot/wire change. The ring is capped so history
/// never grows unbounded.
#[derive(Debug, Clone, Default)]
pub struct LatencyAccumulator {
    cap: usize,
    per_resource: BTreeMap<String, ResourceLatencyRing>,
}

#[derive(Debug, Clone, Default)]
struct ResourceLatencyRing {
    samples: VecDeque<u64>,
    last_finished_ns: u64,
}

impl LatencyAccumulator {
    /// A new accumulator whose per-resource rings retain at most `cap` samples
    /// (`cap` is floored to 1 so the ring is always usable).
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            per_resource: BTreeMap::new(),
        }
    }

    /// Record `latency_ns` for `resource` iff `finished_ns` is newer than the last
    /// observation for that resource (dedup on the finished stamp). A zero
    /// `finished_ns` (no call completed yet) is ignored. Returns `true` when the
    /// sample was a fresh observation that was recorded.
    pub fn observe(&mut self, resource: &str, latency_ns: u64, finished_ns: u64) -> bool {
        if finished_ns == 0 {
            return false;
        }
        let cap = self.cap;
        let ring = self.per_resource.entry(resource.to_string()).or_default();
        if finished_ns <= ring.last_finished_ns {
            return false;
        }
        ring.last_finished_ns = finished_ns;
        if ring.samples.len() >= cap {
            ring.samples.pop_front();
        }
        ring.samples.push_back(latency_ns);
        true
    }

    /// Approximate latency distribution for `resource` over its current ring, or
    /// `None` when no samples have been observed for it.
    pub fn snapshot_for(&self, resource: &str) -> Option<LatencySnapshot> {
        let ring = self.per_resource.get(resource)?;
        let samples: Vec<u64> = ring.samples.iter().copied().collect();
        compute_latency_snapshot(&samples)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSelector {
    ById(NodeId),
    ByName(NodeName),
}

#[derive(Debug, Clone)]
pub struct NodeListOptions {
    pub include_candidate_resources: bool,
}

impl Default for NodeListOptions {
    fn default() -> Self {
        Self {
            include_candidate_resources: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeDetailOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeDiscoverySource {
    TelemetrySnapshot,
    AdminSnapshot,
    LivelinessFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSnapshot {
    pub name: NodeName,
    pub id: NodeId,
    pub group: GroupName,
    pub owner_endpoints: WithProvenance<Vec<EndpointId>>,
    pub candidate_topic_count: WithProvenance<Option<usize>>,
    pub candidate_service_count: WithProvenance<Option<usize>>,
    pub runtime: WithProvenance<Option<NodeRuntimeSummary>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeListResult {
    pub source: NodeDiscoverySource,
    pub nodes: Vec<NodeSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDetail {
    pub node: NodeSnapshot,
    pub candidate_topics: WithProvenance<Vec<TopicSuffix>>,
    pub candidate_services: WithProvenance<Vec<ServiceSuffix>>,
    pub published_topics: WithProvenance<Vec<TopicSuffix>>,
    pub subscribed_topics: WithProvenance<Vec<TopicSuffix>>,
    pub provided_services: WithProvenance<Vec<ServiceSuffix>>,
    pub client_services: WithProvenance<Vec<ServiceSuffix>>,
    pub runtime: WithProvenance<Option<NodeRuntimeSummary>>,
    pub runtime_breakdown: WithProvenance<Option<NodeRuntimeBreakdown>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRuntimeSummary {
    pub timestamp_ns: u64,
    pub collection_period_ms: u32,
    pub publisher_metric_count: usize,
    pub subscriber_metric_count: usize,
    pub service_metric_count: usize,
    pub client_metric_count: usize,
    pub publish_shm_hit_total: u64,
    pub publish_shm_fallback_total: u64,
    pub publish_shm_required_fail_total: u64,
    pub loan_inflight_current: u64,
    pub loan_quota_exceeded_total: u64,
    pub subscriber_duplicate_drop_count: u64,
    pub subscriber_out_of_order_count: u64,
    pub subscriber_out_of_order_drop_count: u64,
    pub subscriber_executor_drop_count: u64,
    pub subscriber_local_drop_count: u64,
    pub subscriber_overflow_relay_enqueue_count: u64,
    pub subscriber_overflow_relay_drop_count: u64,
    pub service_request_received_count: u64,
    pub service_reply_ok_count: u64,
    pub service_reply_error_count: u64,
    pub service_reply_send_failure_count: u64,
    pub service_request_dropped_count: u64,
    pub service_in_flight_requests: u64,
    pub client_call_count: u64,
    pub client_success_count: u64,
    pub client_timeout_count: u64,
    pub client_concurrent_conflict_count: u64,
    pub client_error_count: u64,
    pub client_retry_count: u64,
    pub executor: Option<NodeExecutorRuntimeSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRuntimeBreakdown {
    pub publishers: Vec<NodePublisherRuntimeEntry>,
    pub subscribers: Vec<NodeSubscriberRuntimeEntry>,
    pub services: Vec<NodeServiceRuntimeEntry>,
    pub clients: Vec<NodeClientRuntimeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePublisherRuntimeEntry {
    pub resource_name: String,
    pub resource_instance_id: String,
    pub schema_hash: Option<String>,
    pub publish_shm_hit_total: u64,
    pub publish_shm_fallback_total: u64,
    pub publish_shm_required_fail_total: u64,
    pub loan_inflight_current: u64,
    pub loan_quota_exceeded_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSubscriberRuntimeEntry {
    pub resource_name: String,
    pub resource_instance_id: String,
    pub schema_hash: Option<String>,
    pub duplicate_drop_count: u64,
    pub out_of_order_count: u64,
    pub out_of_order_drop_count: u64,
    pub executor_drop_count: u64,
    pub local_drop_count: u64,
    pub overflow_relay_enqueue_count: u64,
    pub overflow_relay_drop_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeServiceRuntimeEntry {
    pub resource_name: String,
    pub resource_instance_id: String,
    pub schema_hash: Option<String>,
    pub request_received_count: u64,
    pub request_dispatched_count: u64,
    pub reply_ok_count: u64,
    pub reply_error_count: u64,
    pub reply_send_failure_count: u64,
    pub request_dropped_count: u64,
    pub in_flight_requests: u64,
    pub last_request_ns: u64,
    pub last_reply_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeClientRuntimeEntry {
    pub resource_name: String,
    pub resource_instance_id: String,
    pub schema_hash: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeExecutorRuntimeSummary {
    pub queued_events: u64,
    pub queued_bytes: u64,
    pub peak_queued_events: u64,
    pub peak_queued_bytes: u64,
    pub dropped_queue_full: u64,
    pub dropped_queue_closed: u64,
    pub dropped_budget_events: u64,
    pub dropped_budget_bytes: u64,
    /// X1: monotonic executor spin-loop counter (cloud derives spins/sec + stall).
    pub spin_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohAdminOverview {
    pub routers: Vec<ZenohRouterOverview>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohConnectivityOverview {
    pub session_zid: String,
    pub local_locators: Vec<String>,
    pub transports: Vec<ZenohTransportOverview>,
    pub links: Vec<ZenohLinkOverview>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohTransportOverview {
    pub remote_zid: String,
    pub remote_whatami: String,
    pub supports_qos: bool,
    pub is_multicast: bool,
    pub supports_shm: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohLinkOverview {
    pub transport_zid: String,
    pub src: String,
    pub dst: String,
    pub group: Option<String>,
    pub mtu: u16,
    pub is_streamed: bool,
    pub interfaces: Vec<String>,
    pub auth_identifier: Option<String>,
    pub priorities: Option<(u8, u8)>,
    pub reliability: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZenohConnectivityEventKind {
    TransportOpened,
    TransportClosed,
    LinkAdded,
    LinkRemoved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohConnectivityEvent {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub kind: ZenohConnectivityEventKind,
    pub transport: Option<ZenohTransportOverview>,
    pub link: Option<ZenohLinkOverview>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohRouterOverview {
    pub zid: String,
    pub whatami: String,
    pub version: Option<String>,
    pub locators: Vec<String>,
    pub session_count: Option<usize>,
    pub plugin_names: Vec<String>,
    /// Whether the router exposed a `@/<zid>/router/metrics` capability (a reply
    /// key was returned). Distinct from [`Self::metrics`], which is `Some` only
    /// when the metrics body could actually be parsed.
    pub metrics_available: bool,
    /// Parsed Zenoh router byte/message counters, when the metrics body was
    /// readable. `None` on routers without the `stats` feature, when ACL hides
    /// the endpoint, or when the body could not be decoded (e.g. still gzip).
    pub metrics: Option<ZenohRouterMetrics>,
    pub raw_payload_utf8: String,
}

/// Router byte/message counters parsed from the `@/<zid>/router/metrics`
/// OpenMetrics payload (zenoh `stats` feature). All counters are **cumulative
/// since router start** — a single read is a total, not a rate; a rate needs two
/// samples over a known interval.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZenohRouterMetrics {
    /// `zenoh_tx_bytes_total` — transport-message bytes sent (summed over labels).
    pub tx_bytes_total: u64,
    /// `zenoh_rx_bytes_total` — transport-message bytes received (summed over labels).
    pub rx_bytes_total: u64,
    /// `zenoh_tx_network_message_total` — network messages sent (summed over labels).
    pub tx_msgs_total: u64,
    /// `zenoh_rx_network_message_total` — network messages received (summed over labels).
    pub rx_msgs_total: u64,
    /// `zenoh_transports_opened` gauge, if present.
    pub transports_opened: Option<u64>,
    /// `zenoh_links_opened` gauge/counter, if present.
    pub links_opened: Option<u64>,
}

/// A single ContextStore owner reply decoded from a wildcard `aorta/{group}/ctx/**`
/// GET. Mirrors the read-only view surfaced by the CLI `aorta ctx list` command:
/// one entry per reply, so the same key can appear multiple times when more than
/// one writer (distinct `source_instance_id`) is publishing it — that multiplicity
/// is what callers use to flag a conflict (⚠).
#[derive(Debug, Clone, PartialEq)]
pub struct ContextEntry {
    /// Full keyexpr of the reply, e.g. `aorta/{group}/ctx/demo/state`.
    pub key: String,
    /// Human-readable rendering of the active value (kind-aware; long strings
    /// are truncated to 64 chars, matching the CLI `render_value`).
    pub value_text: String,
    /// The value discriminant name (`Bool`/`I64`/`U64`/`F64`/`String`/`None`).
    pub kind: &'static str,
    /// The writer's logical node id (`source_node`), empty when not set.
    pub source_node: String,
    /// The writer's instance id (`source_instance_id`), empty when not set.
    /// Distinct instance ids on the same key indicate competing writers.
    pub source_instance_id: String,
    /// Wall-clock expiry stamp in ns; `0` means "no expiry / always fresh".
    pub expires_at_ns: u64,
}

pub struct IntrospectClient {
    session: Arc<zenoh::Session>,
    options: IntrospectOptions,
    topic_schemas: Mutex<TopicSchemaResolver>,
    telemetry_cache: Mutex<TelemetrySnapshotCache>,
    connectivity_events: Arc<StdMutex<ConnectivityEventsBuffer>>,
    connectivity_event_listeners: Mutex<ConnectivityEventListenersState>,
}

#[derive(Debug, Clone)]
pub struct ServiceCallRequest {
    pub service: ServiceSuffix,
    pub timeout: Option<Duration>,
    pub request_json: String,
}

#[derive(Debug, Clone)]
pub struct ServiceCallResponse {
    pub response_json: Option<String>,
    pub response_utf8: Option<String>,
    pub response_hex: String,
    pub decoded_with_schema: bool,
    pub schema: ResolvedServiceSchema,
}

/// Decoded `SendGoalResponse` from a dynamic (runtime-action-name) action
/// send-goal RPC. Mirrors the fields the CLI `aorta action send` prints
/// (`tools/aorta-cli/src/cmd/action.rs::send`), built by reflecting the
/// action's compiled `.bfbs` over a raw Zenoh query — the core
/// `ActionClient<TAction>` is compile-time-typed and cannot serve an
/// arbitrary discovered action, so this is the same json_to_fb + raw
/// `session.get` path `call_service` uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionGoalSendResult {
    pub accepted: bool,
    pub goal_id: String,
    /// The per-goal secret the server returns on acceptance. The caller MUST
    /// retain it to cancel / fetch the result of this goal (it is the
    /// authorization token, exactly as the CLI treats it).
    pub goal_secret: String,
    pub status: ActionStatus,
    pub code: ActionReplyCode,
    pub message: String,
    pub server_epoch: String,
}

/// Decoded `CancelGoalResponse` from a dynamic action cancel RPC. Mirrors the
/// fields the CLI `aorta action cancel` prints
/// (`tools/aorta-cli/src/cmd/action.rs::cancel`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionCancelResult {
    pub accepted: bool,
    pub goal_id: String,
    pub status: ActionStatus,
    pub code: ActionReplyCode,
    pub message: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AdminEntityOwners {
    #[serde(default)]
    peers: Vec<String>,
    #[serde(default)]
    clients: Vec<String>,
    #[serde(default)]
    routers: Vec<String>,
}

#[derive(Debug, Clone)]
struct AdminNodeEntry {
    name: NodeName,
    id: NodeId,
    group: GroupName,
    owners: AdminEntityOwners,
}

#[derive(Debug, Clone)]
struct QueryReplySample {
    key_expr: String,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LivelinessNodeInfo {
    name: NodeName,
    id: NodeId,
    group: GroupName,
}

#[derive(Debug, Clone)]
struct TelemetryNodeEntry {
    name: NodeName,
    id: NodeId,
    group: GroupName,
    inventory: Option<ResourceInventoryTelemetry>,
    runtime: Option<RuntimeMetricsTelemetry>,
}

#[derive(Debug, Clone, Default)]
struct TelemetryDiscoverySnapshot {
    nodes: BTreeMap<String, TelemetryNodeEntry>,
    publisher_nodes: BTreeMap<String, BTreeSet<String>>,
    subscriber_nodes: BTreeMap<String, BTreeSet<String>>,
    service_nodes: BTreeMap<String, BTreeSet<String>>,
    client_nodes: BTreeMap<String, BTreeSet<String>>,
    /// Topic suffixes whose telemetry inventory publisher entry carried a
    /// non-empty `schema_hash` — i.e. a schema is registered. Drives the list
    /// "Schema" column without an extra `schema_by_key` probe.
    topics_with_schema: BTreeSet<String>,
    /// Service suffixes whose telemetry inventory provider entry carried a
    /// non-empty `schema_hash`.
    services_with_schema: BTreeSet<String>,
}

impl TelemetryDiscoverySnapshot {
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
            && self.publisher_nodes.is_empty()
            && self.subscriber_nodes.is_empty()
            && self.service_nodes.is_empty()
            && self.client_nodes.is_empty()
    }
}

#[derive(Debug, Clone)]
struct CachedTelemetrySnapshot {
    fetched_at: std::time::Instant,
    snapshot: TelemetryDiscoverySnapshot,
}

#[derive(Debug, Default)]
struct TelemetrySnapshotCache {
    entry: Option<CachedTelemetrySnapshot>,
}

const CONNECTIVITY_EVENT_RETENTION: usize = 128;

#[derive(Debug, Default)]
struct ConnectivityEventsBuffer {
    next_sequence: u64,
    events: VecDeque<ZenohConnectivityEvent>,
}

impl ConnectivityEventsBuffer {
    fn push(&mut self, mut event: ZenohConnectivityEvent) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        event.sequence = self.next_sequence;
        if self.events.len() >= CONNECTIVITY_EVENT_RETENTION {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }
}

#[derive(Default)]
struct ConnectivityEventListenersState {
    transport: Option<TransportEventsListener<()>>,
    link: Option<LinkEventsListener<()>>,
}

impl ConnectivityEventListenersState {
    fn initialized(&self) -> bool {
        self.transport.is_some() && self.link.is_some()
    }
}

impl TelemetrySnapshotCache {
    fn get_fresh(&self, ttl: Duration) -> Option<TelemetryDiscoverySnapshot> {
        let entry = self.entry.as_ref()?;
        if ttl.is_zero() || entry.fetched_at.elapsed() > ttl {
            return None;
        }
        Some(entry.snapshot.clone())
    }

    fn store(&mut self, snapshot: TelemetryDiscoverySnapshot) {
        self.entry = Some(CachedTelemetrySnapshot {
            fetched_at: std::time::Instant::now(),
            snapshot,
        });
    }
}

impl IntrospectClient {
    pub fn from_session(session: Arc<zenoh::Session>, options: IntrospectOptions) -> Self {
        Self {
            session,
            topic_schemas: Mutex::new(TopicSchemaResolver::new(options.topic_schema.clone())),
            telemetry_cache: Mutex::new(TelemetrySnapshotCache::default()),
            connectivity_events: Arc::new(StdMutex::new(ConnectivityEventsBuffer::default())),
            connectivity_event_listeners: Mutex::new(ConnectivityEventListenersState::default()),
            options,
        }
    }

    fn group(&self) -> &GroupName {
        &self.options.group
    }

    async fn telemetry_snapshot_cached(&self) -> IntrospectResult<TelemetryDiscoverySnapshot> {
        if let Some(snapshot) = self
            .telemetry_cache
            .lock()
            .await
            .get_fresh(self.options.telemetry_cache_ttl)
        {
            return Ok(snapshot);
        }

        let snapshot = discover_telemetry_snapshot(
            &self.session,
            self.group(),
            self.options.admin_query_timeout,
        )
        .await?;

        self.telemetry_cache.lock().await.store(snapshot.clone());
        Ok(snapshot)
    }

    async fn fetch_topic_schema(
        &self,
        topic: &TopicSuffix,
    ) -> IntrospectResult<ResolvedTopicSchema> {
        let key_suffix = format!("pub/{}", topic.as_str());
        let mut resolver = self.topic_schemas.lock().await;
        resolver
            .get_by_key(&self.session, self.group().as_str(), &key_suffix)
            .await
    }

    pub async fn fetch_service_schema(
        &self,
        service: &ServiceSuffix,
    ) -> IntrospectResult<ResolvedServiceSchema> {
        let key_suffix = format!("srv/{}", service.as_str());
        let resolved = self.resolve_schema_by_key_suffix(&key_suffix).await?;
        match resolved {
            ResolvedSchema::Service(schema) => Ok(schema),
            ResolvedSchema::Topic(_) => Err(IntrospectError::new(
                IntrospectErrorKind::Decode,
                format!("expected service schema at '{}'", key_suffix),
            )),
        }
    }

    pub async fn resolve_schema(&self, key_suffix: &str) -> IntrospectResult<ResolvedSchema> {
        self.resolve_schema_by_key_suffix(key_suffix.trim()).await
    }

    pub fn describe_schema(
        &self,
        schema: &ResolvedSchema,
        root: SchemaRootSelector,
    ) -> IntrospectResult<String> {
        let root_type = selected_root_type(schema, &root)?;
        let bfbs = match schema {
            ResolvedSchema::Topic(topic) => topic.bfbs.as_ref(),
            ResolvedSchema::Service(service) => service.bfbs.as_ref(),
        };
        reflect::bfbs_to_schema_text(bfbs, root_type.as_deref()).map_err(|err| {
            IntrospectError::decode(format!(
                "failed to describe schema via reflection bridge: {}",
                err
            ))
        })
    }

    pub fn build_schema_model(
        &self,
        schema: &ResolvedSchema,
        root: SchemaRootSelector,
    ) -> IntrospectResult<SchemaModel> {
        build_schema_model_impl(schema, root)
    }

    pub async fn call_service(
        &self,
        request: ServiceCallRequest,
    ) -> IntrospectResult<ServiceCallResponse> {
        let schema = self.fetch_service_schema(&request.service).await?;
        let timeout = request.timeout.unwrap_or(self.options.service_call_timeout);
        let service_key = build_service_key(self.group().as_str(), request.service.as_str())?;
        let request_fb = reflect::json_to_fb(
            &schema.bfbs,
            &request.request_json,
            schema.request_root_type.as_deref(),
        )
        .map_err(|err| {
            IntrospectError::decode(format!(
                "failed to encode request JSON using service schema: {}",
                err
            ))
        })?;

        let replies = self
            .session
            .get(&service_key)
            .target(QueryTarget::All)
            .payload(request_fb)
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to send service query '{}': {}", service_key, err),
                )
            })?;

        let reply = tokio::time::timeout(timeout, replies.recv_async())
            .await
            .map_err(|_| {
                IntrospectError::new(
                    IntrospectErrorKind::Timeout,
                    format!("service call timed out after {}s", timeout.as_secs()),
                )
            })?
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to receive service reply: {}", err),
                )
            })?;

        let sample = reply.into_result().map_err(|err| {
            IntrospectError::new(
                IntrospectErrorKind::Transport,
                format!(
                    "service returned error: {}",
                    aorta_core::reply_error_text(&err)
                ),
            )
        })?;
        let payload = sample.payload().to_bytes();

        let response_json = reflect::fb_to_json_display(
            &schema.bfbs,
            payload.as_ref(),
            schema.response_root_type.as_deref(),
        )
        .ok();
        let response_utf8 = std::str::from_utf8(payload.as_ref())
            .ok()
            .map(str::to_string);

        Ok(ServiceCallResponse {
            response_hex: to_hex(payload.as_ref()),
            decoded_with_schema: response_json.is_some(),
            response_json,
            response_utf8,
            schema,
        })
    }

    pub async fn list_topics(
        &self,
        options: TopicListOptions,
    ) -> IntrospectResult<Vec<TopicSnapshot>> {
        let telemetry = self
            .telemetry_snapshot_cached()
            .await
            .ok()
            .filter(|snapshot| !snapshot.is_empty());
        let admin_selector = format!("@/**/router/publisher/aorta/{}/pub/**", self.group());
        let admin_prefix = format!("aorta/{}/pub/", self.group());

        let admin_topics = discover_admin_entities(
            &self.session,
            &admin_selector,
            &admin_prefix,
            self.options.admin_query_timeout,
        )
        .await;
        let need_node_snapshot = options.include_candidate_nodes || options.node_filter.is_some();
        let candidate_nodes = if need_node_snapshot {
            Some(
                discover_nodes_via_admin(
                    &self.session,
                    self.group(),
                    self.options.admin_query_timeout,
                )
                .await,
            )
        } else {
            None
        };

        let admin_topics =
            admin_entities_with_telemetry_fallback(admin_topics, telemetry.as_ref())?;

        let mut all_topics = BTreeSet::new();
        all_topics.extend(admin_topics.keys().cloned());
        if let Some(telemetry) = telemetry.as_ref() {
            all_topics.extend(telemetry.publisher_nodes.keys().cloned());
            all_topics.extend(telemetry.subscriber_nodes.keys().cloned());
        }
        let selected_telemetry_node_id = if let (Some(selector), Some(telemetry)) =
            (options.node_filter.as_ref(), telemetry.as_ref())
        {
            Some(
                select_telemetry_node_by_selector(telemetry, selector, self.group())?
                    .id
                    .clone(),
            )
        } else {
            None
        };
        let selected_node_endpoints = if selected_telemetry_node_id.is_none() {
            options
                .node_filter
                .as_ref()
                .map(|selector| {
                    resolve_selected_node_endpoints(
                        &admin_topics,
                        candidate_nodes.as_ref().expect(
                            "node snapshot should be present when node filter is requested",
                        ),
                        selector,
                        self.group(),
                        "topic",
                    )
                })
                .transpose()?
        } else {
            None
        };

        let mut out = Vec::new();
        for (topic_suffix, topic) in partition_concrete_suffixes(
            all_topics,
            |suffix| TopicSuffix::new_subscribe_side(suffix),
            TopicSuffix::is_pattern,
        )? {
            let owners = admin_topics.get(&topic_suffix);
            let telemetry_publisher_node_ids = telemetry
                .as_ref()
                .and_then(|snapshot| snapshot.publisher_nodes.get(&topic_suffix));
            let telemetry_subscriber_node_ids = telemetry
                .as_ref()
                .and_then(|snapshot| snapshot.subscriber_nodes.get(&topic_suffix));
            let telemetry_node_ids =
                merge_node_id_sets(telemetry_publisher_node_ids, telemetry_subscriber_node_ids);
            if let Some(selected_node_id) = selected_telemetry_node_id.as_ref() {
                if !entity_matches_selected_telemetry_node(
                    telemetry_node_ids.as_ref(),
                    selected_node_id,
                ) {
                    continue;
                }
            } else if let Some(selected_node_endpoints) = selected_node_endpoints.as_ref() {
                if !entity_matches_selected_node(owners, selected_node_endpoints) {
                    continue;
                }
            }
            let candidate_nodes = if let Some(telemetry) = telemetry.as_ref() {
                exact_nodes_with_provenance(
                    telemetry_node_ids.as_ref(),
                    Some(telemetry),
                    options.include_candidate_nodes,
                )
            } else if options.include_candidate_nodes {
                candidate_nodes_with_provenance(
                    owners,
                    candidate_nodes.as_ref(),
                    "admin ownership is endpoint-level; candidate nodes are inferred from shared endpoint ids",
                )
            } else {
                WithProvenance::unavailable(Vec::new(), "nodes were not requested")
            };
            let inferred_publisher_count = if telemetry.is_none() && owners.is_some() {
                Some(candidate_nodes.value.len())
            } else {
                None
            };
            let runtime = if let Some(telemetry) = telemetry.as_ref() {
                summarize_topic_runtime_metrics(telemetry, &topic)
            } else {
                WithProvenance::unavailable(
                    None,
                    "topic runtime metrics unavailable outside telemetry snapshot mode",
                )
            };
            out.push(TopicSnapshot {
                topic,
                present_in_admin: owners.is_some(),
                schema_present: telemetry
                    .as_ref()
                    .is_some_and(|t| t.topics_with_schema.contains(&topic_suffix)),
                provider_endpoints: endpoints_with_provenance(owners),
                candidate_nodes,
                publisher_node_count: if let Some(telemetry) = telemetry.as_ref() {
                    node_count_with_provenance(telemetry.publisher_nodes.get(&topic_suffix), true)
                } else if owners.is_some() {
                    WithProvenance::inferred(
                        inferred_publisher_count,
                        "publisher nodes are inferred from admin endpoint ownership",
                    )
                } else {
                    WithProvenance::unavailable(
                        None,
                        "publisher nodes unavailable because topic was not discovered",
                    )
                },
                subscriber_node_count: if let Some(telemetry) = telemetry.as_ref() {
                    node_count_with_provenance(telemetry.subscriber_nodes.get(&topic_suffix), true)
                } else {
                    WithProvenance::unavailable(
                        None,
                        "subscriber nodes are unavailable outside telemetry snapshot mode",
                    )
                },
                runtime,
            });
        }
        Ok(out)
    }

    pub async fn get_topic_detail(
        &self,
        topic: &TopicSuffix,
        options: TopicDetailOptions,
    ) -> IntrospectResult<TopicDetail> {
        let telemetry = self
            .telemetry_snapshot_cached()
            .await
            .ok()
            .filter(|snapshot| !snapshot.is_empty());
        let snapshot = self
            .list_topics(TopicListOptions {
                include_candidate_nodes: options.include_candidate_nodes,
                node_filter: None,
            })
            .await?
            .into_iter()
            .find(|snapshot| snapshot.topic == *topic)
            .unwrap_or_else(|| TopicSnapshot {
                topic: topic.clone(),
                present_in_admin: false,
                schema_present: false,
                provider_endpoints: WithProvenance::unavailable(
                    Vec::new(),
                    "topic not present in admin publisher snapshot",
                ),
                candidate_nodes: WithProvenance::unavailable(
                    Vec::new(),
                    "candidate nodes unavailable because topic was not discovered in admin snapshot",
                ),
                publisher_node_count: WithProvenance::unavailable(
                    None,
                    "publisher nodes unavailable because topic was not discovered",
                ),
                subscriber_node_count: WithProvenance::unavailable(
                    None,
                    "subscriber nodes unavailable because topic was not discovered",
                ),
                runtime: WithProvenance::unavailable(
                    None,
                    "topic runtime metrics unavailable because topic was not discovered",
                ),
            });
        let runtime = snapshot.runtime.clone();

        let schema = match self.fetch_topic_schema(topic).await {
            Ok(schema) => WithProvenance::resolved(Some(schema)),
            Err(err) if err.kind == IntrospectErrorKind::NotFound => {
                WithProvenance::unavailable(None, "topic schema not found via schema_by_key")
            }
            Err(err) => return Err(err),
        };
        let approx_rate = if options.include_rate_sample {
            let rate = self
                .sample_topic_rate(topic, options.rate_sample_duration)
                .await?;
            WithProvenance::observed(rate)
        } else {
            WithProvenance::unavailable(None, "rate sample not requested")
        };
        let publisher_nodes = if let Some(telemetry) = telemetry.as_ref() {
            exact_nodes_with_provenance(
                telemetry.publisher_nodes.get(topic.as_str()),
                Some(telemetry),
                options.include_candidate_nodes,
            )
        } else if options.include_candidate_nodes {
            snapshot.candidate_nodes.clone()
        } else {
            WithProvenance::unavailable(Vec::new(), "nodes were not requested")
        };
        let subscriber_nodes = if let Some(telemetry) = telemetry.as_ref() {
            exact_nodes_with_provenance(
                telemetry.subscriber_nodes.get(topic.as_str()),
                Some(telemetry),
                options.include_candidate_nodes,
            )
        } else if options.include_candidate_nodes {
            WithProvenance::unavailable(
                Vec::new(),
                "subscriber ownership is unavailable in admin snapshot mode",
            )
        } else {
            WithProvenance::unavailable(Vec::new(), "nodes were not requested")
        };
        Ok(TopicDetail {
            key_expr: build_topic_key(self.group().as_str(), topic.as_str())?,
            snapshot,
            schema,
            approx_rate,
            publisher_nodes,
            subscriber_nodes,
            runtime,
        })
    }

    pub async fn list_services(
        &self,
        options: ServiceListOptions,
    ) -> IntrospectResult<Vec<ServiceSnapshot>> {
        let telemetry = self
            .telemetry_snapshot_cached()
            .await
            .ok()
            .filter(|snapshot| !snapshot.is_empty());
        let admin_selector = format!("@/**/router/queryable/aorta/{}/srv/**", self.group());
        let admin_prefix = format!("aorta/{}/srv/", self.group());

        let admin_services = discover_admin_entities(
            &self.session,
            &admin_selector,
            &admin_prefix,
            self.options.admin_query_timeout,
        )
        .await;
        let need_node_snapshot = options.include_candidate_nodes || options.node_filter.is_some();
        let candidate_nodes = if need_node_snapshot {
            Some(
                discover_nodes_via_admin(
                    &self.session,
                    self.group(),
                    self.options.admin_query_timeout,
                )
                .await,
            )
        } else {
            None
        };

        let admin_services =
            admin_entities_with_telemetry_fallback(admin_services, telemetry.as_ref())?;

        let mut all_services = BTreeSet::new();
        all_services.extend(admin_services.keys().cloned());
        if let Some(telemetry) = telemetry.as_ref() {
            all_services.extend(telemetry.service_nodes.keys().cloned());
            all_services.extend(telemetry.client_nodes.keys().cloned());
        }
        let selected_telemetry_node_id = if let (Some(selector), Some(telemetry)) =
            (options.node_filter.as_ref(), telemetry.as_ref())
        {
            Some(
                select_telemetry_node_by_selector(telemetry, selector, self.group())?
                    .id
                    .clone(),
            )
        } else {
            None
        };
        let selected_node_endpoints = if selected_telemetry_node_id.is_none() {
            options
                .node_filter
                .as_ref()
                .map(|selector| {
                    resolve_selected_node_endpoints(
                        &admin_services,
                        candidate_nodes.as_ref().expect(
                            "node snapshot should be present when node filter is requested",
                        ),
                        selector,
                        self.group(),
                        "service",
                    )
                })
                .transpose()?
        } else {
            None
        };

        let mut out = Vec::new();
        for (service_suffix, service) in partition_concrete_suffixes(
            all_services,
            |suffix| ServiceSuffix::new_subscribe_side(suffix),
            ServiceSuffix::is_pattern,
        )? {
            let owners = admin_services.get(&service_suffix);
            let telemetry_provider_node_ids = telemetry
                .as_ref()
                .and_then(|snapshot| snapshot.service_nodes.get(&service_suffix));
            let telemetry_client_node_ids = telemetry
                .as_ref()
                .and_then(|snapshot| snapshot.client_nodes.get(&service_suffix));
            let telemetry_node_ids =
                merge_node_id_sets(telemetry_provider_node_ids, telemetry_client_node_ids);
            if let Some(selected_node_id) = selected_telemetry_node_id.as_ref() {
                if !entity_matches_selected_telemetry_node(
                    telemetry_node_ids.as_ref(),
                    selected_node_id,
                ) {
                    continue;
                }
            } else if let Some(selected_node_endpoints) = selected_node_endpoints.as_ref() {
                if !entity_matches_selected_node(owners, selected_node_endpoints) {
                    continue;
                }
            }
            let candidate_nodes = if let Some(telemetry) = telemetry.as_ref() {
                exact_nodes_with_provenance(
                    telemetry_node_ids.as_ref(),
                    Some(telemetry),
                    options.include_candidate_nodes,
                )
            } else if options.include_candidate_nodes {
                candidate_nodes_with_provenance(
                    owners,
                    candidate_nodes.as_ref(),
                    "admin ownership is endpoint-level; candidate nodes are inferred from shared endpoint ids",
                )
            } else {
                WithProvenance::unavailable(Vec::new(), "nodes were not requested")
            };
            let inferred_provider_count = if telemetry.is_none() && owners.is_some() {
                Some(candidate_nodes.value.len())
            } else {
                None
            };
            let runtime = if let Some(telemetry) = telemetry.as_ref() {
                summarize_service_runtime_metrics(telemetry, &service)
            } else {
                WithProvenance::unavailable(
                    None,
                    "service runtime metrics unavailable outside telemetry snapshot mode",
                )
            };
            out.push(ServiceSnapshot {
                service,
                present_in_admin: owners.is_some(),
                schema_present: telemetry
                    .as_ref()
                    .is_some_and(|t| t.services_with_schema.contains(&service_suffix)),
                provider_endpoints: endpoints_with_provenance(owners),
                candidate_nodes,
                provider_node_count: if let Some(telemetry) = telemetry.as_ref() {
                    node_count_with_provenance(telemetry.service_nodes.get(&service_suffix), true)
                } else if owners.is_some() {
                    WithProvenance::inferred(
                        inferred_provider_count,
                        "provider nodes are inferred from admin endpoint ownership",
                    )
                } else {
                    WithProvenance::unavailable(
                        None,
                        "provider nodes unavailable because service was not discovered",
                    )
                },
                client_node_count: if let Some(telemetry) = telemetry.as_ref() {
                    node_count_with_provenance(telemetry.client_nodes.get(&service_suffix), true)
                } else {
                    WithProvenance::unavailable(
                        None,
                        "client nodes are unavailable outside telemetry snapshot mode",
                    )
                },
                runtime,
            });
        }
        Ok(out)
    }

    pub async fn get_service_detail(
        &self,
        service: &ServiceSuffix,
        options: ServiceDetailOptions,
    ) -> IntrospectResult<ServiceDetail> {
        let telemetry = self
            .telemetry_snapshot_cached()
            .await
            .ok()
            .filter(|snapshot| !snapshot.is_empty());
        let snapshot = self
            .list_services(ServiceListOptions {
                include_candidate_nodes: options.include_candidate_nodes,
                node_filter: None,
            })
            .await?
            .into_iter()
            .find(|snapshot| snapshot.service == *service)
            .unwrap_or_else(|| ServiceSnapshot {
                service: service.clone(),
                present_in_admin: false,
                schema_present: false,
                provider_endpoints: WithProvenance::unavailable(
                    Vec::new(),
                    "service not present in admin queryable snapshot",
                ),
                candidate_nodes: WithProvenance::unavailable(
                    Vec::new(),
                    "candidate nodes unavailable because service was not discovered in admin snapshot",
                ),
                provider_node_count: WithProvenance::unavailable(
                    None,
                    "provider nodes unavailable because service was not discovered",
                ),
                client_node_count: WithProvenance::unavailable(
                    None,
                    "client nodes unavailable because service was not discovered",
                ),
                runtime: WithProvenance::unavailable(
                    None,
                    "service runtime metrics unavailable because service was not discovered",
                ),
            });
        let runtime = snapshot.runtime.clone();
        let schema = match self.fetch_service_schema(service).await {
            Ok(schema) => WithProvenance::resolved(Some(schema)),
            Err(err) if err.kind == IntrospectErrorKind::NotFound => {
                WithProvenance::unavailable(None, "service schema not found via schema_by_key")
            }
            Err(err) => return Err(err),
        };
        let provider_nodes = if let Some(telemetry) = telemetry.as_ref() {
            exact_nodes_with_provenance(
                telemetry.service_nodes.get(service.as_str()),
                Some(telemetry),
                options.include_candidate_nodes,
            )
        } else if options.include_candidate_nodes {
            snapshot.candidate_nodes.clone()
        } else {
            WithProvenance::unavailable(Vec::new(), "nodes were not requested")
        };
        let client_nodes = if let Some(telemetry) = telemetry.as_ref() {
            exact_nodes_with_provenance(
                telemetry.client_nodes.get(service.as_str()),
                Some(telemetry),
                options.include_candidate_nodes,
            )
        } else if options.include_candidate_nodes {
            WithProvenance::unavailable(
                Vec::new(),
                "client ownership is unavailable in admin snapshot mode",
            )
        } else {
            WithProvenance::unavailable(Vec::new(), "nodes were not requested")
        };
        Ok(ServiceDetail {
            key_expr: build_service_key(self.group().as_str(), service.as_str())?,
            snapshot,
            schema,
            provider_nodes,
            client_nodes,
            runtime,
        })
    }

    pub async fn list_actions(&self) -> IntrospectResult<Vec<ActionSnapshot>> {
        let selector = action_info_selector(self.group().as_str())?;
        let mut actions =
            collect_get_replies(&self.session, &selector, self.options.admin_query_timeout)
                .await?
                .into_iter()
                .map(|sample| decode_action_snapshot(&sample))
                .collect::<IntrospectResult<Vec<_>>>()?;
        actions.sort_by(|left, right| left.action_name.cmp(&right.action_name));
        Ok(actions)
    }

    /// List ContextStore entries reachable under `prefix` (relative to
    /// `aorta/{group}/ctx/`), one [`ContextEntry`] per owner reply.
    ///
    /// This is the read-only data path behind the CLI `aorta ctx list` and the
    /// TUI Context screen: it issues a wildcard `GET` against
    /// `aorta/{group}/ctx/{prefix}` (or `aorta/{group}/ctx/**` when `prefix` is
    /// `None`) and decodes each reply with the core `ContextValue` decoder.
    /// Replies that fail to decode are skipped. The same key may appear in more
    /// than one entry when multiple instances are writing it; the caller groups
    /// by key to detect conflicts. Entries are sorted by `(key, source_instance_id)`
    /// for stable rendering.
    pub async fn list_context_entries(
        &self,
        prefix: Option<&str>,
    ) -> IntrospectResult<Vec<ContextEntry>> {
        let selector = ctx_root_for_group(self.group().as_str(), prefix);
        let replies =
            collect_get_replies(&self.session, &selector, self.options.admin_query_timeout).await?;
        let mut entries: Vec<ContextEntry> =
            replies.iter().filter_map(decode_context_entry).collect();
        entries.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.source_instance_id.cmp(&right.source_instance_id))
        });
        Ok(entries)
    }

    pub async fn get_action_detail(&self, action: &str) -> IntrospectResult<ActionDetail> {
        let snapshot = self.get_action_snapshot(action).await?;
        let bfbs =
            fetch_action_schema_bfbs(&self.session, self.group().as_str(), &snapshot.info).await?;
        // Root type names are resolved here (one bfbs fetch already
        // happened) rather than in `list_actions` / `get_action_snapshot`
        // — those cheap paths only need ActionInfo, not the schema. See
        // ActionDetail::root_types doc.
        let root_types = build_action_root_type_names_from_bfbs(&bfbs, &snapshot.action_name)?;
        let selector = format!(
            "{}/*",
            action_status_prefix(self.group().as_str(), &snapshot.action_name)?
        );
        let mut goals =
            collect_get_replies(&self.session, &selector, self.options.admin_query_timeout)
                .await?
                .into_iter()
                .map(|sample| {
                    decode_action_goal_snapshot(&snapshot.action_name, &bfbs, &sample.payload)
                })
                .collect::<IntrospectResult<Vec<_>>>()?;
        goals.sort_by(|left, right| {
            right
                .accepted_stamp_ns
                .cmp(&left.accepted_stamp_ns)
                .then_with(|| right.goal_id.cmp(&left.goal_id))
        });
        Ok(ActionDetail {
            snapshot,
            goals,
            root_types,
        })
    }

    /// Resolve the action's compiled `.bfbs` into a [`ResolvedSchema`] plus the
    /// fully-qualified `SendGoalRequest` root type name.
    ///
    /// This is the schema-discovery half of the dynamic send-goal flow: the
    /// caller feeds the returned `ResolvedSchema` to
    /// [`Client::build_schema_model`] with
    /// `SchemaRootSelector::NamedRoot(root_type)` to drive a JSON editor — the
    /// same way `fetch_service_schema` + `build_schema_model` power the Service
    /// Call editor. The bfbs is wrapped as a `ResolvedSchema::Topic` purely as a
    /// bfbs carrier (the topic/service distinction only matters for the
    /// Default/ServiceRequest selectors, which this path does not use).
    pub async fn fetch_action_send_goal_schema(
        &self,
        action: &str,
    ) -> IntrospectResult<(ResolvedSchema, String)> {
        let snapshot = self.get_action_snapshot(action).await?;
        let bfbs =
            fetch_action_schema_bfbs(&self.session, self.group().as_str(), &snapshot.info).await?;
        let root_types = build_action_root_type_names_from_bfbs(&bfbs, &snapshot.action_name)?;
        let resolved = ResolvedSchema::Topic(ResolvedTopicSchema::new(
            format!("act/send_goal/{}", snapshot.action_name),
            bfbs,
        ));
        Ok((resolved, root_types.send_goal_request))
    }

    pub async fn get_action_snapshot(&self, action: &str) -> IntrospectResult<ActionSnapshot> {
        let key = build_action_info_key(self.group().as_str(), action)?;
        let sample = query_single_reply(&self.session, &key, self.options.admin_query_timeout)
            .await?
            .ok_or_else(|| {
                IntrospectError::new(
                    IntrospectErrorKind::NotFound,
                    format!("action '{}' not found", action),
                )
            })?;
        decode_action_snapshot(&sample)
    }

    /// Send a goal to a dynamically discovered action.
    ///
    /// Ports the wire logic of the CLI `aorta action send`
    /// (`tools/aorta-cli/src/cmd/action.rs::send`): resolve the action's
    /// `ActionInfo` + compiled `.bfbs`, build a `SendGoalRequest`
    /// `{goal_id, goal}` JSON, encode it with `reflect::json_to_fb` against the
    /// `SendGoalRequest` root type, fire a raw Zenoh `get` against the
    /// send-goal keyexpr (same as `call_service`), then decode the
    /// `SendGoalResponse` reply. When `goal_id` is `None` a fresh v4 UUID is
    /// generated (mirroring the CLI's `resolve_goal_id`).
    ///
    /// Unlike the CLI, this does NOT block waiting for a terminal status — it
    /// returns as soon as the server accepts/rejects, returning the
    /// `goal_secret` the caller needs for a later cancel.
    pub async fn send_action_goal(
        &self,
        action: &str,
        request_json: &str,
        goal_id: Option<&str>,
    ) -> IntrospectResult<ActionGoalSendResult> {
        let snapshot = self.get_action_snapshot(action).await?;
        let bfbs =
            fetch_action_schema_bfbs(&self.session, self.group().as_str(), &snapshot.info).await?;
        let goal_id = resolve_action_goal_id(goal_id)?;
        let goal_value = parse_request_goal_json(request_json)?;
        let request_value = serde_json::json!({
            "goal_id": &goal_id,
            "goal": goal_value,
        });
        let key = build_action_send_goal_key(self.group().as_str(), &snapshot.action_name)?;
        let reply = self
            .query_action_json(
                &key,
                &bfbs,
                &snapshot.action_name,
                ActionRootType::SendGoalRequest,
                &request_value,
                ActionRootType::SendGoalResponse,
            )
            .await?;
        let result = parse_send_goal_result_json(&reply)?;
        if result.goal_id != goal_id {
            return Err(IntrospectError::decode(format!(
                "action '{}' send reply goal_id mismatch: expected '{}', got '{}'",
                snapshot.action_name, goal_id, result.goal_id
            )));
        }
        Ok(result)
    }

    /// Cancel an in-flight action goal.
    ///
    /// Ports the wire logic of the CLI `aorta action cancel`
    /// (`tools/aorta-cli/src/cmd/action.rs::cancel`): build a
    /// `CancelGoalRequest` `{goal_id, goal_secret}`, encode + query the
    /// cancel-goal keyexpr, decode the `CancelGoalResponse`. The `goal_secret`
    /// must be the one returned from the matching [`send_action_goal`].
    pub async fn cancel_action_goal(
        &self,
        action: &str,
        goal_id: &str,
        goal_secret: &str,
    ) -> IntrospectResult<ActionCancelResult> {
        // Validate the goal_id is a well-formed UUID up front (the CLI cancel
        // path enforces this via `goal_id_to_key`); reject a malformed id before
        // any network round-trip.
        uuid::Uuid::parse_str(goal_id.trim()).map_err(|err| {
            IntrospectError::invalid_input(format!("goal_id must be a valid UUID: {}", err))
        })?;
        if goal_secret.trim().is_empty() {
            return Err(IntrospectError::invalid_input(
                "goal_secret must not be empty",
            ));
        }
        let snapshot = self.get_action_snapshot(action).await?;
        let bfbs =
            fetch_action_schema_bfbs(&self.session, self.group().as_str(), &snapshot.info).await?;
        let request_value = serde_json::json!({
            "goal_id": goal_id,
            "goal_secret": goal_secret,
        });
        let key = build_action_cancel_goal_key(self.group().as_str(), &snapshot.action_name)?;
        let reply = self
            .query_action_json(
                &key,
                &bfbs,
                &snapshot.action_name,
                ActionRootType::CancelGoalRequest,
                &request_value,
                ActionRootType::CancelGoalResponse,
            )
            .await?;
        let result = parse_cancel_goal_result_json(&reply)?;
        if result.goal_id != goal_id {
            return Err(IntrospectError::decode(format!(
                "action '{}' cancel reply goal_id mismatch: expected '{}', got '{}'",
                snapshot.action_name, goal_id, result.goal_id
            )));
        }
        Ok(result)
    }

    /// Encode `request_value` against `request_root`, fire a raw payload query
    /// at `selector`, and decode the single reply against `response_root`.
    ///
    /// Mirrors `tools/aorta-cli/src/cmd/action.rs::query_action_json` but reuses
    /// the same raw `session.get(...).payload(...)` + reflect path
    /// `call_service` uses (the action control timeout governs the wait).
    async fn query_action_json(
        &self,
        selector: &str,
        bfbs: &[u8],
        action_name: &str,
        request_root: ActionRootType,
        request_value: &Value,
        response_root: ActionRootType,
    ) -> IntrospectResult<Value> {
        let request_fb = encode_action_json(bfbs, action_name, request_root, request_value)?;
        let timeout = Duration::from_millis(DEFAULT_ACTION_CONTROL_TIMEOUT_MS);
        let replies = self
            .session
            .get(selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .payload(request_fb)
            .timeout(timeout)
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to send action query '{}': {}", selector, err),
                )
            })?;

        // Collect the whole timeout window and require EXACTLY ONE authoritative
        // reply. A duplicate action provider must be an error, not a silent
        // first-wins (which could accept an arbitrary goal_secret). Mirrors the
        // CLI `query_single_reply` (cmd/action.rs).
        let started = std::time::Instant::now();
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        let mut first_error: Option<String> = None;
        loop {
            let elapsed = started.elapsed();
            if elapsed >= timeout {
                break;
            }
            let remaining = timeout.saturating_sub(elapsed);
            match tokio::time::timeout(remaining, replies.recv_async()).await {
                Ok(Ok(reply)) => match reply.into_result() {
                    Ok(sample) => payloads.push(sample.payload().to_bytes().into_owned()),
                    Err(err) => {
                        if first_error.is_none() {
                            first_error = Some(aorta_core::reply_error_text(&err));
                        }
                    }
                },
                Ok(Err(_)) | Err(_) => break,
            }
        }

        if payloads.is_empty() {
            return Err(match first_error {
                Some(err) => IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("action query '{}' returned only errors: {}", selector, err),
                ),
                None => IntrospectError::new(
                    IntrospectErrorKind::Timeout,
                    format!(
                        "action query '{}' timed out after {:?} with no reply",
                        selector, timeout
                    ),
                ),
            });
        }
        if payloads.len() > 1 {
            return Err(IntrospectError::new(
                IntrospectErrorKind::Decode,
                format!(
                    "action query '{}' returned {} replies; expected at most one (duplicate action provider?)",
                    selector,
                    payloads.len()
                ),
            ));
        }
        decode_action_json(bfbs, action_name, response_root, &payloads[0])
    }

    pub async fn list_nodes(&self, options: NodeListOptions) -> IntrospectResult<NodeListResult> {
        if let Ok(telemetry) = self.telemetry_snapshot_cached().await {
            if !telemetry.is_empty() {
                let mut snapshots: Vec<_> = telemetry
                    .nodes
                    .values()
                    .map(|node| NodeSnapshot {
                        name: node.name.clone(),
                        id: node.id.clone(),
                        group: node.group.clone(),
                        owner_endpoints: WithProvenance::unavailable(
                            Vec::new(),
                            "owner endpoints unavailable in telemetry snapshot mode",
                        ),
                        candidate_topic_count: if options.include_candidate_resources {
                            match node.inventory.as_ref() {
                                Some(inventory) => WithProvenance::exact(Some(
                                    telemetry_topic_names(inventory).len(),
                                )),
                                None => WithProvenance::unavailable(
                                    None,
                                    "resource inventory unavailable for this telemetry node",
                                ),
                            }
                        } else {
                            WithProvenance::unavailable(None, "resource counts were not requested")
                        },
                        candidate_service_count: if options.include_candidate_resources {
                            match node.inventory.as_ref() {
                                Some(inventory) => WithProvenance::exact(Some(
                                    telemetry_service_names(inventory).len(),
                                )),
                                None => WithProvenance::unavailable(
                                    None,
                                    "resource inventory unavailable for this telemetry node",
                                ),
                            }
                        } else {
                            WithProvenance::unavailable(None, "resource counts were not requested")
                        },
                        runtime: match node.runtime.as_ref() {
                            Some(runtime) => {
                                WithProvenance::exact(Some(summarize_runtime_metrics(runtime)))
                            }
                            None => WithProvenance::unavailable(
                                None,
                                "runtime metrics unavailable because node runtime telemetry is missing",
                            ),
                        },
                    })
                    .collect();
                snapshots.sort_by(|a, b| {
                    a.name
                        .as_str()
                        .cmp(b.name.as_str())
                        .then_with(|| a.id.as_str().cmp(b.id.as_str()))
                });
                return Ok(NodeListResult {
                    source: NodeDiscoverySource::TelemetrySnapshot,
                    nodes: snapshots,
                });
            }
        }
        match discover_nodes_via_admin(
            &self.session,
            self.group(),
            self.options.admin_query_timeout,
        )
        .await
        {
            Ok(nodes) if !nodes.is_empty() => {
                let resource_counts = if options.include_candidate_resources {
                    Some(
                        discover_resource_counts_for_nodes(
                            &self.session,
                            self.group(),
                            &nodes,
                            self.options.admin_query_timeout,
                        )
                        .await?,
                    )
                } else {
                    None
                };
                let snapshots = nodes
                    .into_iter()
                    .map(|node| {
                        let key = format!("{}/{}", node.name, node.id);
                        let (topic_count, service_count) = resource_counts
                            .as_ref()
                            .and_then(|counts| counts.get(&key))
                            .copied()
                            .unwrap_or((0, 0));
                        NodeSnapshot {
                            name: node.name,
                            id: node.id,
                            group: node.group,
                            owner_endpoints: WithProvenance::declared(node.owners.endpoint_ids()),
                            candidate_topic_count: if options.include_candidate_resources {
                                WithProvenance::inferred(
                                    Some(topic_count),
                                    "resource counts are inferred by matching node endpoint ownership with admin publisher/queryable ownership",
                                )
                            } else {
                                WithProvenance::unavailable(
                                    None,
                                    "candidate resource counts were not requested",
                                )
                            },
                            candidate_service_count: if options.include_candidate_resources {
                                WithProvenance::inferred(
                                    Some(service_count),
                                    "resource counts are inferred by matching node endpoint ownership with admin publisher/queryable ownership",
                                )
                            } else {
                                WithProvenance::unavailable(
                                    None,
                                    "candidate resource counts were not requested",
                                )
                            },
                            runtime: WithProvenance::unavailable(
                                None,
                                "runtime metrics unavailable outside telemetry snapshot mode",
                            ),
                        }
                    })
                    .collect();
                Ok(NodeListResult {
                    source: NodeDiscoverySource::AdminSnapshot,
                    nodes: snapshots,
                })
            }
            Ok(_) | Err(_) => {
                let nodes = discover_nodes_via_liveliness(
                    &self.session,
                    self.group(),
                    self.options.liveliness_timeout,
                )
                .await?;
                let snapshots = nodes
                    .into_iter()
                    .map(|node| NodeSnapshot {
                        name: node.name,
                        id: node.id,
                        group: node.group,
                        owner_endpoints: WithProvenance::unavailable(
                            Vec::new(),
                            "owner endpoints unavailable in liveliness fallback mode",
                        ),
                        candidate_topic_count: WithProvenance::unavailable(
                            None,
                            "candidate resource counts unavailable in liveliness fallback mode",
                        ),
                        candidate_service_count: WithProvenance::unavailable(
                            None,
                            "candidate resource counts unavailable in liveliness fallback mode",
                        ),
                        runtime: WithProvenance::unavailable(
                            None,
                            "runtime metrics unavailable in liveliness fallback mode",
                        ),
                    })
                    .collect();
                Ok(NodeListResult {
                    source: NodeDiscoverySource::LivelinessFallback,
                    nodes: snapshots,
                })
            }
        }
    }

    pub async fn get_node_detail(
        &self,
        selector: NodeSelector,
        _options: NodeDetailOptions,
    ) -> IntrospectResult<NodeDetail> {
        if let Ok(telemetry) = self.telemetry_snapshot_cached().await {
            if !telemetry.is_empty() {
                if let Some(detail) = telemetry_node_detail(&telemetry, &selector, self.group())? {
                    return Ok(detail);
                }
            }
        }
        match discover_nodes_via_admin(
            &self.session,
            self.group(),
            self.options.admin_query_timeout,
        )
        .await
        {
            Ok(nodes) if !nodes.is_empty() => {
                let selected = select_node_by_selector(&nodes, &selector, self.group())?;
                let node_endpoints: HashSet<_> = selected
                    .owners
                    .endpoint_ids()
                    .into_iter()
                    .map(|endpoint| endpoint.as_str().to_string())
                    .collect();
                let topics = discover_topics_for_node(
                    &self.session,
                    self.group(),
                    &node_endpoints,
                    self.options.admin_query_timeout,
                )
                .await?;
                let services = discover_services_for_node(
                    &self.session,
                    self.group(),
                    &node_endpoints,
                    self.options.admin_query_timeout,
                )
                .await?;
                let published_topics = topics.clone();
                let provided_services = services.clone();
                Ok(NodeDetail {
                    node: NodeSnapshot {
                        name: selected.name.clone(),
                        id: selected.id.clone(),
                        group: selected.group.clone(),
                        owner_endpoints: WithProvenance::declared(selected.owners.endpoint_ids()),
                        candidate_topic_count: WithProvenance::inferred(
                            Some(topics.len()),
                            "resource counts are inferred by matching node endpoint ownership with admin publisher/queryable ownership",
                        ),
                        candidate_service_count: WithProvenance::inferred(
                            Some(services.len()),
                            "resource counts are inferred by matching node endpoint ownership with admin publisher/queryable ownership",
                        ),
                        runtime: WithProvenance::unavailable(
                            None,
                            "runtime metrics unavailable in admin snapshot mode",
                        ),
                    },
                    candidate_topics: WithProvenance::inferred(
                        topics,
                        "node-resource mapping uses admin endpoint ownership; resources are candidate matches for the node endpoints",
                    ),
                    candidate_services: WithProvenance::inferred(
                        services,
                        "node-resource mapping uses admin endpoint ownership; resources are candidate matches for the node endpoints",
                    ),
                    published_topics: WithProvenance::inferred(
                        published_topics,
                        "published topics are inferred by matching node endpoint ownership with admin publisher ownership",
                    ),
                    subscribed_topics: WithProvenance::unavailable(
                        Vec::new(),
                        "subscriber ownership is unavailable in admin snapshot mode",
                    ),
                    provided_services: WithProvenance::inferred(
                        provided_services,
                        "provided services are inferred by matching node endpoint ownership with admin queryable ownership",
                    ),
                    client_services: WithProvenance::unavailable(
                        Vec::new(),
                        "client ownership is unavailable in admin snapshot mode",
                    ),
                    runtime: WithProvenance::unavailable(
                        None,
                        "runtime metrics unavailable in admin snapshot mode",
                    ),
                    runtime_breakdown: WithProvenance::unavailable(
                        None,
                        "runtime breakdown unavailable in admin snapshot mode",
                    ),
                })
            }
            Ok(_) | Err(_) => {
                let nodes = discover_nodes_via_liveliness(
                    &self.session,
                    self.group(),
                    self.options.liveliness_timeout,
                )
                .await?;
                let selected = select_liveliness_node_by_selector(&nodes, &selector, self.group())?;
                Ok(NodeDetail {
                    node: NodeSnapshot {
                        name: selected.name,
                        id: selected.id,
                        group: selected.group,
                        owner_endpoints: WithProvenance::unavailable(
                            Vec::new(),
                            "owner endpoints unavailable in liveliness fallback mode",
                        ),
                        candidate_topic_count: WithProvenance::unavailable(
                            None,
                            "candidate topics unavailable in liveliness fallback mode",
                        ),
                        candidate_service_count: WithProvenance::unavailable(
                            None,
                            "candidate services unavailable in liveliness fallback mode",
                        ),
                        runtime: WithProvenance::unavailable(
                            None,
                            "runtime metrics unavailable in liveliness fallback mode",
                        ),
                    },
                    candidate_topics: WithProvenance::unavailable(
                        Vec::new(),
                        "candidate topics unavailable in liveliness fallback mode",
                    ),
                    candidate_services: WithProvenance::unavailable(
                        Vec::new(),
                        "candidate services unavailable in liveliness fallback mode",
                    ),
                    published_topics: WithProvenance::unavailable(
                        Vec::new(),
                        "published topics unavailable in liveliness fallback mode",
                    ),
                    subscribed_topics: WithProvenance::unavailable(
                        Vec::new(),
                        "subscribed topics unavailable in liveliness fallback mode",
                    ),
                    provided_services: WithProvenance::unavailable(
                        Vec::new(),
                        "provided services unavailable in liveliness fallback mode",
                    ),
                    client_services: WithProvenance::unavailable(
                        Vec::new(),
                        "client services unavailable in liveliness fallback mode",
                    ),
                    runtime: WithProvenance::unavailable(
                        None,
                        "runtime metrics unavailable in liveliness fallback mode",
                    ),
                    runtime_breakdown: WithProvenance::unavailable(
                        None,
                        "runtime breakdown unavailable in liveliness fallback mode",
                    ),
                })
            }
        }
    }

    pub async fn fetch_zenoh_admin_overview(&self) -> IntrospectResult<ZenohAdminOverview> {
        let router_selector = "@/**/router";
        let router_replies = collect_get_replies(
            &self.session,
            router_selector,
            self.options.admin_query_timeout,
        )
        .await?;
        if router_replies.is_empty() {
            return Err(IntrospectError::new(
                IntrospectErrorKind::CapabilityMissing,
                "router admin root is unavailable; check admin-space ACL and router reachability",
            ));
        }

        // `?compression=false` keeps the OpenMetrics body as plain text — the
        // pinned zenoh fork gzip-compresses it by default, and `collect_get_replies`
        // discards the reply encoding, so without this the body would be gzip bytes.
        let metrics_selector = "@/**/router/metrics?compression=false";
        let metrics_replies = collect_get_replies(
            &self.session,
            metrics_selector,
            self.options.admin_query_timeout,
        )
        .await
        .unwrap_or_default();
        // `metrics_available` = a metrics reply key existed (capability exposed),
        // preserved from the key alone so the existing TUI badge is unchanged even
        // when the body cannot be parsed. `metrics_by_zid` carries the parsed counters.
        let metrics_zids: HashSet<_> = metrics_replies
            .iter()
            .filter_map(parse_router_metrics_zid)
            .collect();
        let mut metrics_by_zid = BTreeMap::<String, ZenohRouterMetrics>::new();
        for reply in &metrics_replies {
            if let Some((zid, parsed)) = parse_router_metrics(reply) {
                metrics_by_zid
                    .entry(zid)
                    .and_modify(|current| {
                        // Same zid replied twice — keep the higher total (most recent
                        // monotonic snapshot) so the readout never regresses.
                        // `saturating_add` guards the (extreme) case of counters near u64::MAX.
                        let parsed_total =
                            parsed.tx_bytes_total.saturating_add(parsed.rx_bytes_total);
                        let current_total = current
                            .tx_bytes_total
                            .saturating_add(current.rx_bytes_total);
                        if parsed_total > current_total {
                            *current = parsed.clone();
                        }
                    })
                    .or_insert(parsed);
            }
        }

        let mut routers = BTreeMap::<String, ZenohRouterOverview>::new();
        for reply in router_replies {
            let Some(mut overview) = parse_zenoh_router_overview(&reply) else {
                continue;
            };
            overview.metrics_available = metrics_zids.contains(overview.zid.as_str());
            overview.metrics = metrics_by_zid.get(overview.zid.as_str()).cloned();
            routers
                .entry(overview.zid.clone())
                .and_modify(|current| current.merge_from(&overview))
                .or_insert(overview);
        }

        if routers.is_empty() {
            return Err(IntrospectError::new(
                IntrospectErrorKind::Decode,
                "router admin root replies were returned, but no router root keys could be parsed",
            ));
        }

        Ok(ZenohAdminOverview {
            routers: routers.into_values().collect(),
        })
    }

    async fn ensure_connectivity_event_listeners(&self) -> IntrospectResult<()> {
        let mut state = self.connectivity_event_listeners.lock().await;
        if state.initialized() {
            return Ok(());
        }

        let transport_events = Arc::clone(&self.connectivity_events);
        let transport_listener = self
            .session
            .info()
            .transport_events_listener()
            .history(true)
            .callback(move |event| push_transport_connectivity_event(&transport_events, event))
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to start transport events listener: {}", err),
                )
            })?;

        let link_events = Arc::clone(&self.connectivity_events);
        let link_listener = self
            .session
            .info()
            .link_events_listener()
            .history(true)
            .callback(move |event| push_link_connectivity_event(&link_events, event))
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to start link events listener: {}", err),
                )
            })?;

        state.transport = Some(transport_listener);
        state.link = Some(link_listener);
        Ok(())
    }

    pub async fn shutdown(&self) -> IntrospectResult<()> {
        let (transport_listener, link_listener) = {
            let mut state = self.connectivity_event_listeners.lock().await;
            (state.transport.take(), state.link.take())
        };

        if let Some(listener) = link_listener {
            listener.undeclare().await.map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to stop link events listener: {}", err),
                )
            })?;
        }

        if let Some(listener) = transport_listener {
            listener.undeclare().await.map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to stop transport events listener: {}", err),
                )
            })?;
        }

        self.session.close().await.map_err(|err| {
            IntrospectError::new(
                IntrospectErrorKind::Transport,
                format!("failed to close zenoh session: {}", err),
            )
        })
    }

    pub async fn fetch_zenoh_connectivity_overview(
        &self,
    ) -> IntrospectResult<ZenohConnectivityOverview> {
        let session_info = self.session.info();
        let session_zid = session_info.zid().await.to_string();

        let mut local_locators: Vec<_> = session_info
            .locators()
            .await
            .into_iter()
            .map(|locator| locator.to_string())
            .collect();
        local_locators.sort();

        let mut transports: Vec<_> = session_info
            .transports()
            .await
            .map(|transport| transport_overview(transport))
            .collect();
        transports.sort_by(|left, right| {
            left.remote_zid
                .cmp(&right.remote_zid)
                .then_with(|| left.remote_whatami.cmp(&right.remote_whatami))
                .then_with(|| left.is_multicast.cmp(&right.is_multicast))
                .then_with(|| left.supports_qos.cmp(&right.supports_qos))
                .then_with(|| left.supports_shm.cmp(&right.supports_shm))
        });

        let mut links: Vec<_> = session_info
            .links()
            .await
            .map(|link| link_overview(link))
            .collect();
        links.sort_by(|left, right| {
            left.transport_zid
                .cmp(&right.transport_zid)
                .then_with(|| left.src.cmp(&right.src))
                .then_with(|| left.dst.cmp(&right.dst))
                .then_with(|| left.group.cmp(&right.group))
        });

        Ok(ZenohConnectivityOverview {
            session_zid,
            local_locators,
            transports,
            links,
        })
    }

    pub async fn fetch_recent_zenoh_connectivity_events(
        &self,
        limit: usize,
    ) -> IntrospectResult<Vec<ZenohConnectivityEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.ensure_connectivity_event_listeners().await?;

        let guard = self
            .connectivity_events
            .lock()
            .expect("connectivity events mutex poisoned");
        let skip = guard.events.len().saturating_sub(limit);
        Ok(guard.events.iter().skip(skip).cloned().collect())
    }

    pub async fn sample_topic_rate(
        &self,
        topic: &TopicSuffix,
        duration: Duration,
    ) -> IntrospectResult<Option<RateSnapshot>> {
        let topic_key = build_topic_key(self.group().as_str(), topic.as_str())?;
        let subscriber = self
            .session
            .declare_subscriber(&topic_key)
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to subscribe to '{}': {}", topic_key, err),
                )
            })?;
        let deadline = tokio::time::Instant::now() + duration;

        let mut intervals = VecDeque::new();
        let mut last_message_at: Option<tokio::time::Instant> = None;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, subscriber.recv_async()).await {
                Ok(Ok(_sample)) => {
                    let now = tokio::time::Instant::now();
                    if let Some(last) = last_message_at {
                        let interval = now.duration_since(last).as_secs_f64();
                        if interval > 0.0 {
                            intervals.push_back(interval);
                        }
                    }
                    last_message_at = Some(now);
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        compute_rate_snapshot(&intervals, duration)
    }

    async fn decode_topic_payload(
        &self,
        topic: &TopicSuffix,
        payload: &[u8],
        attachment_schema_hash: Option<&str>,
    ) -> TopicPreviewFrame {
        let key_expr = build_topic_key(self.group().as_str(), topic.as_str())
            .unwrap_or_else(|_| format!("aorta/{}/pub/{}", self.group(), topic));
        let payload_hex = to_hex(payload);

        match self.fetch_topic_schema(topic).await {
            Ok(schema) => match reflect::fb_to_json_display(
                &schema.bfbs,
                payload,
                schema.default_root_type.as_deref(),
            ) {
                Ok(decoded_json) => TopicPreviewFrame {
                    topic: topic.clone(),
                    key_expr,
                    payload_hex,
                    decoded_json: Some(decoded_json),
                    schema_hash: Some(schema.schema_hash),
                    decode_status: TopicPreviewDecodeStatus::DecodedFromTopicSchema,
                },
                Err(topic_err) => {
                    if let Some(schema_hash) = attachment_schema_hash {
                        if let Some(frame) = self
                            .decode_topic_payload_with_attachment(
                                topic,
                                &key_expr,
                                &payload_hex,
                                payload,
                                schema_hash,
                            )
                            .await
                        {
                            return frame;
                        }
                    }

                    TopicPreviewFrame {
                        topic: topic.clone(),
                        key_expr,
                        payload_hex,
                        decoded_json: None,
                        schema_hash: Some(schema.schema_hash),
                        decode_status: TopicPreviewDecodeStatus::DecodeError(format!(
                            "topic schema decode failed: {}",
                            topic_err
                        )),
                    }
                }
            },
            Err(err) if err.kind == IntrospectErrorKind::NotFound => {
                if let Some(schema_hash) = attachment_schema_hash {
                    if let Some(frame) = self
                        .decode_topic_payload_with_attachment(
                            topic,
                            &key_expr,
                            &payload_hex,
                            payload,
                            schema_hash,
                        )
                        .await
                    {
                        return frame;
                    }
                }

                TopicPreviewFrame {
                    topic: topic.clone(),
                    key_expr,
                    payload_hex,
                    decoded_json: None,
                    schema_hash: None,
                    decode_status: TopicPreviewDecodeStatus::RawOnly,
                }
            }
            Err(err) => TopicPreviewFrame {
                topic: topic.clone(),
                key_expr,
                payload_hex,
                decoded_json: None,
                schema_hash: None,
                decode_status: TopicPreviewDecodeStatus::DecodeError(format!(
                    "topic schema lookup failed: {}",
                    err
                )),
            },
        }
    }

    pub async fn watch_topic_preview(
        self: Arc<Self>,
        topic: TopicSuffix,
        sender: watch::Sender<Option<TopicPreviewEvent>>,
        mut stop: oneshot::Receiver<()>,
    ) -> IntrospectResult<()> {
        let topic_key = build_topic_key(self.group().as_str(), topic.as_str())?;
        let subscriber = self
            .session
            .declare_subscriber(&topic_key)
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to subscribe to '{}': {}", topic_key, err),
                )
            })?;

        loop {
            tokio::select! {
                _ = &mut stop => break,
                sample = subscriber.recv_async() => {
                    match sample {
                        Ok(sample) => {
                            let payload = sample.payload().to_bytes();
                            let attachment_schema_hash = extract_attachment_schema_hash(&sample);
                            let frame = self
                                .decode_topic_payload(
                                    &topic,
                                    payload.as_ref(),
                                    attachment_schema_hash.as_deref(),
                                )
                                .await;
                            if sender.send(Some(TopicPreviewEvent::Sample(frame))).is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = sender.send(Some(TopicPreviewEvent::Error(format!(
                                "failed to receive topic sample: {}",
                                err
                            ))));
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Subscribe to the Aorta system `/log` stream and stream decoded
    /// [`LogRecordFrame`]s into `sender` until `stop` fires.
    ///
    /// This is the literal `watch_topic_preview` template specialised to the
    /// `/log` topic: it declares a subscriber on the group-scoped `pub/log`
    /// keyexpr, and on each sample decodes the `LogRecord` payload via the same
    /// `schema_by_key` reflection path as topic preview (the log forwarder
    /// registers a `schema_by_key` queryable for the topic). Unlike preview this
    /// is group-scoped, not selection-scoped, so the caller starts it once at
    /// startup and never re-syncs on selection.
    pub async fn watch_log_stream(
        self: Arc<Self>,
        sender: watch::Sender<Option<LogEvent>>,
        mut stop: oneshot::Receiver<()>,
    ) -> IntrospectResult<()> {
        // The log forwarder publishes on the `/log` topic, which normalises to
        // the `log` suffix (leading slashes are stripped by the key builder).
        let topic = TopicSuffix::new("/log").map_err(|err| {
            IntrospectError::new(
                IntrospectErrorKind::InvalidInput,
                format!("failed to build /log topic suffix: {}", err),
            )
        })?;
        let topic_key = build_topic_key(self.group().as_str(), topic.as_str())?;
        let subscriber = self
            .session
            .declare_subscriber(&topic_key)
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to subscribe to '{}': {}", topic_key, err),
                )
            })?;

        loop {
            tokio::select! {
                _ = &mut stop => break,
                sample = subscriber.recv_async() => {
                    match sample {
                        Ok(sample) => {
                            let payload = sample.payload().to_bytes();
                            let attachment_schema_hash = extract_attachment_schema_hash(&sample);
                            let preview = self
                                .decode_topic_payload(
                                    &topic,
                                    payload.as_ref(),
                                    attachment_schema_hash.as_deref(),
                                )
                                .await;
                            let frame = log_record_frame_from_preview(&preview);
                            if sender.send(Some(LogEvent::Record(frame))).is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = sender.send(Some(LogEvent::Error(format!(
                                "failed to receive log sample: {}",
                                err
                            ))));
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Subscribe to an action's feedback keyexpr and stream the
    /// [`ActionFeedbackFrame`]s for a single `goal_id` into `sender` until
    /// `stop` fires.
    ///
    /// Ports the wire/decode logic of the CLI `aorta action feedback`
    /// (`tools/aorta-cli/src/cmd/action.rs::feedback`): resolve the action's
    /// `ActionInfo` + compiled `.bfbs`, declare a subscriber on the
    /// observe-side feedback keyexpr, and reflect each sample's `Feedback` root
    /// type to JSON. Samples whose `goal_id` differs from the requested goal are
    /// dropped, so the panel only shows feedback for the goal that was sent.
    pub async fn watch_action_feedback(
        self: Arc<Self>,
        action: String,
        goal_id: String,
        sender: mpsc::UnboundedSender<ActionFeedbackEvent>,
        mut stop: oneshot::Receiver<()>,
    ) -> IntrospectResult<()> {
        let snapshot = self.get_action_snapshot(&action).await?;
        let bfbs =
            fetch_action_schema_bfbs(&self.session, self.group().as_str(), &snapshot.info).await?;
        let feedback_root =
            build_action_root_type_names_from_bfbs(&bfbs, &snapshot.action_name)?.feedback;
        let feedback_key =
            build_action_feedback_key_sub(self.group().as_str(), &snapshot.action_name)?;
        let subscriber = self
            .session
            .declare_subscriber(&feedback_key)
            .await
            .map_err(|err| {
                IntrospectError::new(
                    IntrospectErrorKind::Transport,
                    format!("failed to subscribe to '{}': {}", feedback_key, err),
                )
            })?;

        loop {
            tokio::select! {
                _ = &mut stop => break,
                sample = subscriber.recv_async() => {
                    match sample {
                        Ok(sample) => {
                            let payload = sample.payload().to_bytes();
                            match decode_action_feedback_frame(&bfbs, &feedback_root, payload.as_ref()) {
                                Ok(frame) => {
                                    // Only surface feedback for the goal that was sent.
                                    if frame.goal_id != goal_id {
                                        continue;
                                    }
                                    if sender.send(ActionFeedbackEvent::Feedback(frame)).is_err() {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    if sender
                                        .send(ActionFeedbackEvent::Error(format!(
                                            "failed to decode action feedback: {}",
                                            err
                                        )))
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            let _ = sender.send(ActionFeedbackEvent::Error(format!(
                                "failed to receive feedback sample: {}",
                                err
                            )));
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn decode_topic_payload_with_attachment(
        &self,
        topic: &TopicSuffix,
        key_expr: &str,
        payload_hex: &str,
        payload: &[u8],
        schema_hash: &str,
    ) -> Option<TopicPreviewFrame> {
        let mut resolver = self.topic_schemas.lock().await;
        let attachment_bfbs = resolver
            .get_by_hash(&self.session, schema_hash)
            .await
            .ok()?;
        let root_type = bfbs_root_type_name(&attachment_bfbs);
        let decoded_json =
            reflect::fb_to_json_display(&attachment_bfbs, payload, root_type.as_deref()).ok()?;
        Some(TopicPreviewFrame {
            topic: topic.clone(),
            key_expr: key_expr.to_string(),
            payload_hex: payload_hex.to_string(),
            decoded_json: Some(decoded_json),
            schema_hash: Some(schema_hash.to_string()),
            decode_status: TopicPreviewDecodeStatus::DecodedFromAttachmentSchema,
        })
    }

    async fn resolve_schema_by_key_suffix(
        &self,
        key_suffix: &str,
    ) -> IntrospectResult<ResolvedSchema> {
        let query_key = format!(
            "aorta/{}/sys/schema_by_key/{}",
            self.group().as_str(),
            key_suffix
        );
        let started = std::time::Instant::now();
        let retry_interval = Duration::from_millis(100);
        let mut saw_decode_failure = false;

        loop {
            let elapsed = started.elapsed();
            if elapsed >= self.options.schema_query_timeout {
                break;
            }
            let remaining = self.options.schema_query_timeout.saturating_sub(elapsed);
            let query_timeout = retry_interval.min(remaining);
            let replies = collect_get_replies(&self.session, &query_key, query_timeout).await?;

            let mut service_candidates = Vec::new();
            let mut topic_candidates = Vec::new();
            for reply in replies {
                if let Ok(service_schema) =
                    decode_service_schema_payload(key_suffix, &reply.payload)
                {
                    service_candidates.push(service_schema);
                    continue;
                }
                if let Ok(topic_schema) = decode_topic_schema_payload(key_suffix, &reply.payload) {
                    topic_candidates.push(topic_schema);
                } else {
                    saw_decode_failure = true;
                }
            }

            if let Some(service_schema) =
                choose_best_service_schema_candidate(service_candidates.into_iter())
            {
                return Ok(ResolvedSchema::Service(service_schema));
            }
            if let Some(topic_schema) = topic_candidates.into_iter().next() {
                return Ok(ResolvedSchema::Topic(topic_schema));
            }

            let elapsed = started.elapsed();
            if elapsed >= self.options.schema_query_timeout {
                break;
            }
            tokio::time::sleep(
                retry_interval.min(self.options.schema_query_timeout.saturating_sub(elapsed)),
            )
            .await;
        }

        if saw_decode_failure {
            return Err(IntrospectError::decode(format!(
                "failed to decode schema_by_key payload for '{}'",
                query_key
            )));
        }

        Err(IntrospectError::new(
            IntrospectErrorKind::NotFound,
            format!("no schema found at '{}'", query_key),
        ))
    }

    /// Enumerate every registered schema across the fleet via a single wildcard GET
    /// over `aorta/{group}/sys/schema_by_key/**`, which fans out to the per-key
    /// queryables (verified by core test
    /// `wildcard_schema_by_key_get_enumerates_all_registered_schemas`) — so this
    /// needs no core aggregate queryable. Deduplicated by key suffix (first reply
    /// wins; multiple providers of the same suffix collapse), returned sorted.
    pub async fn list_schemas(&self) -> IntrospectResult<Vec<ResolvedSchema>> {
        let prefix = format!("aorta/{}/sys/schema_by_key/", self.group().as_str());
        let selector = format!("{}**", prefix);
        let replies =
            collect_get_replies(&self.session, &selector, self.options.admin_query_timeout).await?;
        Ok(schemas_from_replies(&prefix, &replies))
    }

    /// Fetch a schema's raw BFBS by its SHA-256 hash via the **global, group-agnostic**
    /// `aorta/sys/schema/{hash}` queryable. This deliberately mirrors the core design:
    /// schemas are *content-addressed* (a hash is globally unique), so core's
    /// `KeyExprBuilder::schema_key` / `SchemaRegistry::get_schema_from_network` use this
    /// same no-group key. The `group` argument here only selects the session/client, not
    /// the key. (Deployments that ACL-isolate per group must allow client GET on
    /// `aorta/sys/schema/**` for hash lookup to work — same as the existing hash path.)
    /// The returned bytes are re-hashed and verified against the request; a reply that
    /// does not match is skipped.
    pub async fn fetch_schema_by_hash(&self, hash_hex: &str) -> IntrospectResult<Vec<u8>> {
        let hash = SchemaHash::from_hex_str(hash_hex.trim()).ok_or_else(|| {
            IntrospectError::invalid_input(format!(
                "invalid schema hash '{}': expected 64 lowercase hex chars",
                hash_hex
            ))
        })?;
        let selector = format!("aorta/sys/schema/{}", hash.to_hex());
        let replies =
            collect_get_replies(&self.session, &selector, self.options.admin_query_timeout).await?;

        select_schema_by_hash(hash, &replies).ok_or_else(|| {
            IntrospectError::new(
                IntrospectErrorKind::NotFound,
                format!("no schema found for hash {}", hash.to_hex()),
            )
        })
    }
}

/// Decode + dedup schema_by_key replies into a sorted schema list. The `prefix`
/// is `aorta/{group}/sys/schema_by_key/`; each reply's suffix is the key tail.
/// First reply per suffix wins (collapses multiple providers). Service decode is
/// tried before topic (more specific envelope).
fn schemas_from_replies(prefix: &str, replies: &[QueryReplySample]) -> Vec<ResolvedSchema> {
    let mut by_suffix: BTreeMap<String, ResolvedSchema> = BTreeMap::new();
    for reply in replies {
        let Some(suffix) = reply.key_expr.strip_prefix(prefix) else {
            continue;
        };
        if by_suffix.contains_key(suffix) {
            continue;
        }
        if let Ok(service) = decode_service_schema_payload(suffix, &reply.payload) {
            by_suffix.insert(suffix.to_string(), ResolvedSchema::Service(service));
        } else if let Ok(topic) = decode_topic_schema_payload(suffix, &reply.payload) {
            by_suffix.insert(suffix.to_string(), ResolvedSchema::Topic(topic));
        }
    }
    by_suffix.into_values().collect()
}

/// Pick the first reply whose raw BFBS payload re-hashes to `hash` (a reply that
/// does not match the requested hash is corrupt/tampered and is skipped).
fn select_schema_by_hash(hash: SchemaHash, replies: &[QueryReplySample]) -> Option<Vec<u8>> {
    replies
        .iter()
        .find(|reply| {
            !reply.payload.is_empty() && SchemaHash::from_bfbs_bytes(&reply.payload) == hash
        })
        .map(|reply| reply.payload.clone())
}

fn endpoints_with_provenance(
    owners: Option<&AdminEntityOwners>,
) -> WithProvenance<Vec<EndpointId>> {
    match owners {
        Some(owners) => WithProvenance::declared(owners.endpoint_ids()),
        None => WithProvenance::unavailable(
            Vec::new(),
            "provider endpoints unavailable because admin snapshot did not report this resource",
        ),
    }
}

fn candidate_nodes_with_provenance(
    owners: Option<&AdminEntityOwners>,
    nodes: Option<&IntrospectResult<Vec<AdminNodeEntry>>>,
    note: &str,
) -> WithProvenance<Vec<CandidateNodeRef>> {
    match (owners, nodes) {
        (Some(owners), Some(Ok(nodes))) => {
            WithProvenance::inferred(resolve_nodes_for_entity(owners, nodes), note)
        }
        (Some(_), Some(Err(_))) => WithProvenance::unavailable(
            Vec::new(),
            "candidate nodes unavailable because admin node snapshot could not be resolved",
        ),
        (Some(_), None) => {
            WithProvenance::unavailable(Vec::new(), "candidate nodes were not requested")
        }
        (None, _) => WithProvenance::unavailable(
            Vec::new(),
            "candidate nodes unavailable because resource was not discovered in admin snapshot",
        ),
    }
}

fn telemetry_topic_names(inventory: &ResourceInventoryTelemetry) -> BTreeSet<&str> {
    inventory
        .publishers
        .iter()
        .map(|entry| entry.common.resource_name.as_str())
        .chain(
            inventory
                .subscribers
                .iter()
                .map(|entry| entry.common.resource_name.as_str()),
        )
        .collect()
}

fn telemetry_service_names(inventory: &ResourceInventoryTelemetry) -> BTreeSet<&str> {
    inventory
        .services
        .iter()
        .map(|entry| entry.common.resource_name.as_str())
        .chain(
            inventory
                .clients
                .iter()
                .map(|entry| entry.common.resource_name.as_str()),
        )
        .collect()
}

fn telemetry_topic_suffixes_from_names<'a>(
    names: impl Iterator<Item = &'a str>,
) -> IntrospectResult<Vec<TopicSuffix>> {
    names
        .collect::<BTreeSet<_>>()
        .into_iter()
        // Node-inventory names may be wildcard subscriber patterns.
        // They are genuine resources of the node, so keep them (a
        // pattern renders as e.g. `*/task_report` in `node info`); only the
        // aggregate topic listing drops them.
        .map(TopicSuffix::new_subscribe_side)
        .collect()
}

fn telemetry_service_suffixes_from_names<'a>(
    names: impl Iterator<Item = &'a str>,
) -> IntrospectResult<Vec<ServiceSuffix>> {
    names
        .collect::<BTreeSet<_>>()
        .into_iter()
        // Wildcard service *client* patterns are legal here too.
        .map(ServiceSuffix::new_subscribe_side)
        .collect()
}

/// Build the concrete rows of an aggregate topic/service listing from the
/// union of admin keys and telemetry resource names.
///
/// Wildcard subscriber/client patterns (e.g. `*/task_report`) are
/// *not* concrete topics/services: a single one used to abort the entire
/// listing when validated with the strict publish-side validator. They are
/// omitted here — the concrete topics they match still appear via their
/// publishers, and the pattern stays visible in `node info` for its owner.
/// Each surviving suffix is returned paired with its validated wrapper so
/// callers keep the raw string for map lookups.
fn partition_concrete_suffixes<T>(
    all_suffixes: BTreeSet<String>,
    make: impl Fn(&str) -> IntrospectResult<T>,
    is_pattern: impl Fn(&T) -> bool,
) -> IntrospectResult<Vec<(String, T)>> {
    let mut out = Vec::with_capacity(all_suffixes.len());
    for suffix in all_suffixes {
        let entity = make(&suffix)?;
        if is_pattern(&entity) {
            continue;
        }
        out.push((suffix, entity));
    }
    Ok(out)
}

fn merge_unique_topics(left: &[TopicSuffix], right: &[TopicSuffix]) -> Vec<TopicSuffix> {
    left.iter()
        .chain(right.iter())
        .map(|topic| topic.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        // Inputs may already be wildcard subscriber patterns;
        // re-validate on the subscribe side so the rebuild never panics.
        .map(TopicSuffix::new_subscribe_side)
        .collect::<IntrospectResult<Vec<_>>>()
        .expect("existing topic suffixes should remain valid")
}

fn merge_unique_services(left: &[ServiceSuffix], right: &[ServiceSuffix]) -> Vec<ServiceSuffix> {
    left.iter()
        .chain(right.iter())
        .map(|service| service.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(ServiceSuffix::new_subscribe_side)
        .collect::<IntrospectResult<Vec<_>>>()
        .expect("existing service suffixes should remain valid")
}

fn merge_node_id_sets(
    left: Option<&BTreeSet<String>>,
    right: Option<&BTreeSet<String>>,
) -> Option<BTreeSet<String>> {
    match (left, right) {
        (None, None) => None,
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (Some(left), Some(right)) => Some(left.union(right).cloned().collect()),
    }
}

fn node_count_with_provenance(
    node_ids: Option<&BTreeSet<String>>,
    requested: bool,
) -> WithProvenance<Option<usize>> {
    if !requested {
        return WithProvenance::unavailable(None, "nodes were not requested");
    }
    match node_ids {
        Some(node_ids) => WithProvenance::exact(Some(node_ids.len())),
        None => WithProvenance::exact(Some(0)),
    }
}

fn resolve_selected_node_endpoints(
    entities: &BTreeMap<String, AdminEntityOwners>,
    nodes: &IntrospectResult<Vec<AdminNodeEntry>>,
    selector: &NodeSelector,
    group: &GroupName,
    resource_kind: &str,
) -> IntrospectResult<HashSet<String>> {
    if !entities
        .values()
        .any(|owners| !owners.endpoint_ids().is_empty())
    {
        return Err(IntrospectError::new(
            IntrospectErrorKind::CapabilityMissing,
            format!(
                "node filter requires admin ownership for {} discovery, but no admin ownership data is available",
                resource_kind
            ),
        ));
    }

    let nodes = nodes.as_ref().map_err(|err| {
        IntrospectError::new(
            err.kind,
            format!("node filter requires admin node snapshot: {}", err),
        )
    })?;
    let selected = select_node_by_selector(nodes, selector, group)?;
    Ok(selected
        .owners
        .endpoint_ids()
        .into_iter()
        .map(|endpoint| endpoint.as_str().to_string())
        .collect())
}

fn entity_matches_selected_node(
    owners: Option<&AdminEntityOwners>,
    selected_node_endpoints: &HashSet<String>,
) -> bool {
    owners.is_some_and(|owners| {
        owners
            .endpoint_ids()
            .iter()
            .any(|id| selected_node_endpoints.contains(id.as_str()))
    })
}

fn decode_service_schema_payload(
    key_suffix: &str,
    payload: &[u8],
) -> IntrospectResult<ResolvedServiceSchema> {
    let schema = ServiceSchemaByKeyValue::decode(payload)
        .ok_or_else(|| IntrospectError::decode("failed to decode service schema_by_key payload"))?;
    let recomputed = SchemaHash::from_bfbs_bytes(&schema.service_bfbs);
    if recomputed != schema.service_hash {
        return Err(IntrospectError::new(
            IntrospectErrorKind::SchemaMismatch,
            format!(
                "service schema hash mismatch: payload has {}, recomputed {}",
                schema.service_hash.to_hex(),
                recomputed.to_hex()
            ),
        ));
    }
    Ok(ResolvedServiceSchema::new(
        key_suffix,
        schema.service_bfbs,
        schema.request_root_type,
        schema.response_root_type,
    ))
}

fn choose_best_service_schema_candidate(
    candidates: impl Iterator<Item = ResolvedServiceSchema>,
) -> Option<ResolvedServiceSchema> {
    let mut best: Option<(u8, ResolvedServiceSchema)> = None;
    for candidate in candidates {
        let mut score = 0u8;
        if candidate.request_root_type.is_some() {
            score = score.saturating_add(1);
        }
        if candidate.response_root_type.is_some() {
            score = score.saturating_add(1);
        }

        match best {
            Some((best_score, _)) if best_score >= score => {}
            _ => best = Some((score, candidate)),
        }
    }
    best.map(|(_, schema)| schema)
}

fn selected_root_type(
    schema: &ResolvedSchema,
    root: &SchemaRootSelector,
) -> IntrospectResult<Option<String>> {
    match schema {
        ResolvedSchema::Topic(topic) => match root {
            SchemaRootSelector::DefaultRoot => Ok(topic.default_root_type.clone()),
            SchemaRootSelector::NamedRoot(root_name) => Ok(Some(root_name.clone())),
            SchemaRootSelector::ServiceRequest | SchemaRootSelector::ServiceResponse => {
                Err(IntrospectError::invalid_input(
                    "service request/response selectors are invalid for topic schemas",
                ))
            }
        },
        ResolvedSchema::Service(service) => match root {
            SchemaRootSelector::DefaultRoot | SchemaRootSelector::ServiceRequest => {
                Ok(service.request_root_type.clone())
            }
            SchemaRootSelector::NamedRoot(root_name) => Ok(Some(root_name.clone())),
            SchemaRootSelector::ServiceResponse => Ok(service.response_root_type.clone()),
        },
    }
}

fn transport_overview(transport: Transport) -> ZenohTransportOverview {
    ZenohTransportOverview {
        remote_zid: transport.zid().to_string(),
        remote_whatami: format!("{:?}", transport.whatami()).to_ascii_lowercase(),
        supports_qos: transport.is_qos(),
        is_multicast: transport.is_multicast(),
        supports_shm: transport.is_shm(),
    }
}

fn link_overview(link: Link) -> ZenohLinkOverview {
    ZenohLinkOverview {
        transport_zid: link.zid().to_string(),
        src: link.src().to_string(),
        dst: link.dst().to_string(),
        group: link.group().map(ToString::to_string),
        mtu: link.mtu(),
        is_streamed: link.is_streamed(),
        interfaces: link.interfaces().to_vec(),
        auth_identifier: link.auth_identifier().map(ToString::to_string),
        priorities: link.priorities(),
        reliability: link.reliability().map(|value| format!("{value:?}")),
    }
}

fn push_transport_connectivity_event(
    events: &Arc<StdMutex<ConnectivityEventsBuffer>>,
    event: TransportEvent,
) {
    let kind = match event.kind() {
        zenoh::sample::SampleKind::Put => ZenohConnectivityEventKind::TransportOpened,
        zenoh::sample::SampleKind::Delete => ZenohConnectivityEventKind::TransportClosed,
    };

    let mut guard = events
        .lock()
        .expect("connectivity transport events mutex poisoned");
    guard.push(ZenohConnectivityEvent {
        sequence: 0,
        timestamp_ns: unix_timestamp_ns(),
        kind,
        transport: Some(transport_overview(event.transport().clone())),
        link: None,
    });
}

fn push_link_connectivity_event(
    events: &Arc<StdMutex<ConnectivityEventsBuffer>>,
    event: LinkEvent,
) {
    let kind = match event.kind() {
        zenoh::sample::SampleKind::Put => ZenohConnectivityEventKind::LinkAdded,
        zenoh::sample::SampleKind::Delete => ZenohConnectivityEventKind::LinkRemoved,
    };

    let mut guard = events
        .lock()
        .expect("connectivity link events mutex poisoned");
    guard.push(ZenohConnectivityEvent {
        sequence: 0,
        timestamp_ns: unix_timestamp_ns(),
        kind,
        transport: None,
        link: Some(link_overview(event.link().clone())),
    });
}

fn unix_timestamp_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn build_service_key(group: &str, service: &str) -> IntrospectResult<String> {
    let builder = KeyExprBuilder::new(group).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid group '{}': {}", group, err))
    })?;
    let key = builder.service_key_client(service).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid service '{}': {}", service, err))
    })?;
    Ok(key.to_string())
}

fn build_topic_key(group: &str, topic: &str) -> IntrospectResult<String> {
    let builder = KeyExprBuilder::new(group).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid group '{}': {}", group, err))
    })?;
    let key = builder.topic_key_sub(topic).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid topic '{}': {}", topic, err))
    })?;
    Ok(key.to_string())
}

async fn discover_telemetry_snapshot(
    session: &zenoh::Session,
    group: &GroupName,
    timeout: Duration,
) -> IntrospectResult<TelemetryDiscoverySnapshot> {
    // Tiered telemetry: the vital tier carries the merged VitalSnapshot
    // (node identity + resource inventory + health) on one key per node; the
    // bulk tier carries the full runtime metrics. introspect reads both.
    let vital_selector = format!("aorta/{}/sys/telemetry/vital/snapshot/**", group);
    let runtime_selector = format!("aorta/{}/sys/telemetry/bulk/runtime/**", group);
    let vital_replies = collect_get_replies(session, &vital_selector, timeout).await?;
    let runtime_replies = collect_get_replies(session, &runtime_selector, timeout).await?;

    let nodes = build_telemetry_node_index(group, vital_replies, runtime_replies);

    let mut snapshot = TelemetryDiscoverySnapshot {
        nodes,
        ..TelemetryDiscoverySnapshot::default()
    };
    for node in snapshot.nodes.values() {
        let Some(inventory) = node.inventory.as_ref() else {
            continue;
        };
        let node_id = node.id.as_str().to_string();
        collect_inventory_node_owners(
            &mut snapshot.publisher_nodes,
            &node_id,
            inventory
                .publishers
                .iter()
                .map(|entry| entry.common.resource_name.as_str()),
        );
        collect_inventory_node_owners(
            &mut snapshot.subscriber_nodes,
            &node_id,
            inventory
                .subscribers
                .iter()
                .map(|entry| entry.common.resource_name.as_str()),
        );
        collect_inventory_node_owners(
            &mut snapshot.service_nodes,
            &node_id,
            inventory
                .services
                .iter()
                .map(|entry| entry.common.resource_name.as_str()),
        );
        collect_inventory_node_owners(
            &mut snapshot.client_nodes,
            &node_id,
            inventory
                .clients
                .iter()
                .map(|entry| entry.common.resource_name.as_str()),
        );
        // Record which topics/services have a registered schema, so the list
        // "Schema" column can show `confirmed` instead of `?` without a
        // separate `schema_by_key` probe (the inventory already carries it).
        for entry in &inventory.publishers {
            if schema_hash_present(entry.common.schema_hash.as_deref()) {
                snapshot
                    .topics_with_schema
                    .insert(entry.common.resource_name.clone());
            }
        }
        for entry in &inventory.services {
            if schema_hash_present(entry.common.schema_hash.as_deref()) {
                snapshot
                    .services_with_schema
                    .insert(entry.common.resource_name.clone());
            }
        }
    }
    Ok(snapshot)
}

/// True when a telemetry inventory entry carries a non-empty `schema_hash`
/// (a schema is registered for the resource). Whitespace-only is treated as
/// absent.
fn schema_hash_present(schema_hash: Option<&str>) -> bool {
    schema_hash.is_some_and(|hash| !hash.trim().is_empty())
}

fn build_telemetry_node_index(
    group: &GroupName,
    vital_replies: Vec<QueryReplySample>,
    runtime_replies: Vec<QueryReplySample>,
) -> BTreeMap<String, TelemetryNodeEntry> {
    let mut nodes = BTreeMap::<String, TelemetryNodeEntry>::new();
    for reply in vital_replies {
        // The vital tier carries the merged VitalSnapshot. The
        // `identity` section drives the node entry and `inventory` attaches
        // inline; `health` is cloud-facing and intentionally ignored here (the
        // executor spin_count introspect surfaces comes from the bulk stream).
        let Ok(parts) = aorta_core::telemetry::fb::vital_snapshot_from_fb(&reply.payload) else {
            continue;
        };
        match parts.identity {
            Some(identity) => {
                // Identity present → full entry; inventory attaches inline.
                let Ok(node_id) = NodeId::new(&identity.node_id) else {
                    continue;
                };
                let Ok(node_name) = NodeName::new(&identity.node_name) else {
                    continue;
                };
                let Ok(node_group) = GroupName::new(&identity.group) else {
                    continue;
                };
                if node_group != *group {
                    continue;
                }
                nodes.insert(
                    node_id.as_str().to_string(),
                    TelemetryNodeEntry {
                        name: node_name,
                        id: node_id,
                        group: node_group,
                        inventory: parts.inventory,
                        runtime: None,
                    },
                );
            }
            None => {
                // Identity section gated off: still surface the inventory under
                // its node_id with the fallback name (resilience — mirrors how a
                // runtime-only node is surfaced below).
                let Some(inventory) = parts.inventory else {
                    continue;
                };
                let Ok(node_id) = NodeId::new(&inventory.node_id) else {
                    continue;
                };
                if inventory.group != group.as_str() {
                    continue;
                }
                let entry = ensure_telemetry_node_entry(&mut nodes, node_id, group);
                entry.inventory = Some(inventory);
            }
        }
    }

    for reply in runtime_replies {
        // runtime_metrics telemetry is FlatBuffers on the wire (telemetry V2).
        let Ok(runtime) = aorta_core::telemetry::fb::runtime_metrics_from_fb(&reply.payload) else {
            continue;
        };
        let Ok(node_id) = NodeId::new(&runtime.node_id) else {
            continue;
        };
        if runtime.group != group.as_str() {
            continue;
        }
        let entry = ensure_telemetry_node_entry(&mut nodes, node_id, group);
        entry.runtime = Some(runtime);
    }

    nodes
}

fn summarize_runtime_metrics(runtime: &RuntimeMetricsTelemetry) -> NodeRuntimeSummary {
    NodeRuntimeSummary {
        timestamp_ns: runtime.timestamp_ns,
        collection_period_ms: runtime.collection_period_ms,
        publisher_metric_count: runtime.publishers.len(),
        subscriber_metric_count: runtime.subscribers.len(),
        service_metric_count: runtime.services.len(),
        client_metric_count: runtime.clients.len(),
        publish_shm_hit_total: runtime
            .publishers
            .iter()
            .map(|entry| entry.publish_shm_hit_total)
            .sum(),
        publish_shm_fallback_total: runtime
            .publishers
            .iter()
            .map(|entry| entry.publish_shm_fallback_total)
            .sum(),
        publish_shm_required_fail_total: runtime
            .publishers
            .iter()
            .map(|entry| entry.publish_shm_required_fail_total)
            .sum(),
        loan_inflight_current: runtime
            .publishers
            .iter()
            .map(|entry| entry.loan_inflight_current)
            .sum(),
        loan_quota_exceeded_total: runtime
            .publishers
            .iter()
            .map(|entry| entry.loan_quota_exceeded_total)
            .sum(),
        subscriber_duplicate_drop_count: runtime
            .subscribers
            .iter()
            .map(|entry| entry.duplicate_drop_count)
            .sum(),
        subscriber_out_of_order_count: runtime
            .subscribers
            .iter()
            .map(|entry| entry.out_of_order_count)
            .sum(),
        subscriber_out_of_order_drop_count: runtime
            .subscribers
            .iter()
            .map(|entry| entry.out_of_order_drop_count)
            .sum(),
        subscriber_executor_drop_count: runtime
            .subscribers
            .iter()
            .map(|entry| entry.executor_drop_count)
            .sum(),
        subscriber_local_drop_count: runtime
            .subscribers
            .iter()
            .map(|entry| entry.local_drop_count)
            .sum(),
        subscriber_overflow_relay_enqueue_count: runtime
            .subscribers
            .iter()
            .map(|entry| entry.overflow_relay_enqueue_count)
            .sum(),
        subscriber_overflow_relay_drop_count: runtime
            .subscribers
            .iter()
            .map(|entry| entry.overflow_relay_drop_count)
            .sum(),
        service_request_received_count: runtime
            .services
            .iter()
            .map(|entry| entry.request_received_count)
            .sum(),
        service_reply_ok_count: runtime
            .services
            .iter()
            .map(|entry| entry.reply_ok_count)
            .sum(),
        service_reply_error_count: runtime
            .services
            .iter()
            .map(|entry| entry.reply_error_count)
            .sum(),
        service_reply_send_failure_count: runtime
            .services
            .iter()
            .map(|entry| entry.reply_send_failure_count)
            .sum(),
        service_request_dropped_count: runtime
            .services
            .iter()
            .map(|entry| entry.request_dropped_count)
            .sum(),
        service_in_flight_requests: runtime
            .services
            .iter()
            .map(|entry| entry.in_flight_requests)
            .sum(),
        client_call_count: runtime.clients.iter().map(|entry| entry.call_count).sum(),
        client_success_count: runtime
            .clients
            .iter()
            .map(|entry| entry.success_count)
            .sum(),
        client_timeout_count: runtime
            .clients
            .iter()
            .map(|entry| entry.timeout_count)
            .sum(),
        client_concurrent_conflict_count: runtime
            .clients
            .iter()
            .map(|entry| entry.concurrent_conflict_count)
            .sum(),
        client_error_count: runtime.clients.iter().map(|entry| entry.error_count).sum(),
        client_retry_count: runtime.clients.iter().map(|entry| entry.retry_count).sum(),
        executor: runtime
            .executor
            .as_ref()
            .map(|executor| NodeExecutorRuntimeSummary {
                queued_events: executor.queued_events,
                queued_bytes: executor.queued_bytes,
                peak_queued_events: executor.peak_queued_events,
                peak_queued_bytes: executor.peak_queued_bytes,
                dropped_queue_full: executor.dropped_queue_full,
                dropped_queue_closed: executor.dropped_queue_closed,
                dropped_budget_events: executor.dropped_budget_events,
                dropped_budget_bytes: executor.dropped_budget_bytes,
                spin_count: executor.spin_count,
            }),
    }
}

fn build_node_runtime_breakdown(runtime: &RuntimeMetricsTelemetry) -> NodeRuntimeBreakdown {
    NodeRuntimeBreakdown {
        publishers: runtime
            .publishers
            .iter()
            .map(|entry| NodePublisherRuntimeEntry {
                resource_name: entry.resource_name.clone(),
                resource_instance_id: entry.resource_instance_id.clone(),
                schema_hash: entry.schema_hash.clone(),
                publish_shm_hit_total: entry.publish_shm_hit_total,
                publish_shm_fallback_total: entry.publish_shm_fallback_total,
                publish_shm_required_fail_total: entry.publish_shm_required_fail_total,
                loan_inflight_current: entry.loan_inflight_current,
                loan_quota_exceeded_total: entry.loan_quota_exceeded_total,
            })
            .collect(),
        subscribers: runtime
            .subscribers
            .iter()
            .map(|entry| NodeSubscriberRuntimeEntry {
                resource_name: entry.resource_name.clone(),
                resource_instance_id: entry.resource_instance_id.clone(),
                schema_hash: entry.schema_hash.clone(),
                duplicate_drop_count: entry.duplicate_drop_count,
                out_of_order_count: entry.out_of_order_count,
                out_of_order_drop_count: entry.out_of_order_drop_count,
                executor_drop_count: entry.executor_drop_count,
                local_drop_count: entry.local_drop_count,
                overflow_relay_enqueue_count: entry.overflow_relay_enqueue_count,
                overflow_relay_drop_count: entry.overflow_relay_drop_count,
            })
            .collect(),
        services: runtime
            .services
            .iter()
            .map(|entry| NodeServiceRuntimeEntry {
                resource_name: entry.resource_name.clone(),
                resource_instance_id: entry.resource_instance_id.clone(),
                schema_hash: entry.schema_hash.clone(),
                request_received_count: entry.request_received_count,
                request_dispatched_count: entry.request_dispatched_count,
                reply_ok_count: entry.reply_ok_count,
                reply_error_count: entry.reply_error_count,
                reply_send_failure_count: entry.reply_send_failure_count,
                request_dropped_count: entry.request_dropped_count,
                in_flight_requests: entry.in_flight_requests,
                last_request_ns: entry.last_request_ns,
                last_reply_ns: entry.last_reply_ns,
            })
            .collect(),
        clients: runtime
            .clients
            .iter()
            .map(|entry| NodeClientRuntimeEntry {
                resource_name: entry.resource_name.clone(),
                resource_instance_id: entry.resource_instance_id.clone(),
                schema_hash: entry.schema_hash.clone(),
                call_count: entry.call_count,
                success_count: entry.success_count,
                timeout_count: entry.timeout_count,
                concurrent_conflict_count: entry.concurrent_conflict_count,
                error_count: entry.error_count,
                retry_count: entry.retry_count,
                last_call_started_ns: entry.last_call_started_ns,
                last_call_finished_ns: entry.last_call_finished_ns,
                last_call_latency_ns: entry.last_call_latency_ns,
            })
            .collect(),
    }
}

fn summarize_topic_runtime_metrics(
    telemetry: &TelemetryDiscoverySnapshot,
    topic: &TopicSuffix,
) -> WithProvenance<Option<TopicRuntimeSummary>> {
    let topic_name = topic.as_str();
    let mut summary = TopicRuntimeSummary {
        latest_timestamp_ns: 0,
        collection_period_ms: 0,
        publisher_metric_count: 0,
        subscriber_metric_count: 0,
        publish_shm_hit_total: 0,
        publish_shm_fallback_total: 0,
        publish_shm_required_fail_total: 0,
        loan_inflight_current: 0,
        loan_quota_exceeded_total: 0,
        subscriber_duplicate_drop_count: 0,
        subscriber_out_of_order_count: 0,
        subscriber_out_of_order_drop_count: 0,
        subscriber_executor_drop_count: 0,
        subscriber_local_drop_count: 0,
        subscriber_overflow_relay_enqueue_count: 0,
        subscriber_overflow_relay_drop_count: 0,
    };
    let mut matched = false;

    for node in telemetry.nodes.values() {
        let Some(runtime) = node.runtime.as_ref() else {
            continue;
        };

        let mut node_matched = false;
        for entry in runtime
            .publishers
            .iter()
            .filter(|entry| entry.resource_name == topic_name)
        {
            node_matched = true;
            matched = true;
            summary.publisher_metric_count += 1;
            summary.publish_shm_hit_total += entry.publish_shm_hit_total;
            summary.publish_shm_fallback_total += entry.publish_shm_fallback_total;
            summary.publish_shm_required_fail_total += entry.publish_shm_required_fail_total;
            summary.loan_inflight_current += entry.loan_inflight_current;
            summary.loan_quota_exceeded_total += entry.loan_quota_exceeded_total;
        }
        for entry in runtime
            .subscribers
            .iter()
            .filter(|entry| entry.resource_name == topic_name)
        {
            node_matched = true;
            matched = true;
            summary.subscriber_metric_count += 1;
            summary.subscriber_duplicate_drop_count += entry.duplicate_drop_count;
            summary.subscriber_out_of_order_count += entry.out_of_order_count;
            summary.subscriber_out_of_order_drop_count += entry.out_of_order_drop_count;
            summary.subscriber_executor_drop_count += entry.executor_drop_count;
            summary.subscriber_local_drop_count += entry.local_drop_count;
            summary.subscriber_overflow_relay_enqueue_count += entry.overflow_relay_enqueue_count;
            summary.subscriber_overflow_relay_drop_count += entry.overflow_relay_drop_count;
        }
        if node_matched {
            summary.latest_timestamp_ns = summary.latest_timestamp_ns.max(runtime.timestamp_ns);
            summary.collection_period_ms = summary
                .collection_period_ms
                .max(runtime.collection_period_ms);
        }
    }

    if matched {
        WithProvenance::exact(Some(summary))
    } else {
        WithProvenance::unavailable(
            None,
            "topic runtime metrics unavailable because telemetry rows are absent",
        )
    }
}

fn summarize_service_runtime_metrics(
    telemetry: &TelemetryDiscoverySnapshot,
    service: &ServiceSuffix,
) -> WithProvenance<Option<ServiceRuntimeSummary>> {
    let service_name = service.as_str();
    let mut summary = ServiceRuntimeSummary {
        latest_timestamp_ns: 0,
        collection_period_ms: 0,
        service_metric_count: 0,
        client_metric_count: 0,
        service_request_received_count: 0,
        service_request_dispatched_count: 0,
        service_reply_ok_count: 0,
        service_reply_error_count: 0,
        service_reply_send_failure_count: 0,
        service_request_dropped_count: 0,
        service_in_flight_requests: 0,
        client_call_count: 0,
        client_success_count: 0,
        client_timeout_count: 0,
        client_concurrent_conflict_count: 0,
        client_error_count: 0,
        client_retry_count: 0,
        client_last_call_latency_ns: 0,
        client_last_call_finished_ns: 0,
        service_last_request_ns: 0,
        service_last_reply_ns: 0,
    };
    let mut matched = false;
    // `client_last_call_latency_ns` is the latency of the *most recent* call across
    // client rows — picked by the greatest `last_call_finished_ns`, NOT max latency
    // (max would report the slowest call, not the last one).
    let mut latest_client_finish_ns = 0u64;

    for node in telemetry.nodes.values() {
        let Some(runtime) = node.runtime.as_ref() else {
            continue;
        };

        let mut node_matched = false;
        for entry in runtime
            .services
            .iter()
            .filter(|entry| entry.resource_name == service_name)
        {
            node_matched = true;
            matched = true;
            summary.service_metric_count += 1;
            summary.service_request_received_count += entry.request_received_count;
            summary.service_request_dispatched_count += entry.request_dispatched_count;
            summary.service_reply_ok_count += entry.reply_ok_count;
            summary.service_reply_error_count += entry.reply_error_count;
            summary.service_reply_send_failure_count += entry.reply_send_failure_count;
            summary.service_request_dropped_count += entry.request_dropped_count;
            summary.service_in_flight_requests += entry.in_flight_requests;
            summary.service_last_request_ns =
                summary.service_last_request_ns.max(entry.last_request_ns);
            summary.service_last_reply_ns = summary.service_last_reply_ns.max(entry.last_reply_ns);
        }
        for entry in runtime
            .clients
            .iter()
            .filter(|entry| entry.resource_name == service_name)
        {
            node_matched = true;
            matched = true;
            summary.client_metric_count += 1;
            summary.client_call_count += entry.call_count;
            summary.client_success_count += entry.success_count;
            summary.client_timeout_count += entry.timeout_count;
            summary.client_concurrent_conflict_count += entry.concurrent_conflict_count;
            summary.client_error_count += entry.error_count;
            summary.client_retry_count += entry.retry_count;
            // Keep the latency of the call that finished most recently.
            if entry.last_call_finished_ns > latest_client_finish_ns {
                latest_client_finish_ns = entry.last_call_finished_ns;
                summary.client_last_call_latency_ns = entry.last_call_latency_ns;
                summary.client_last_call_finished_ns = entry.last_call_finished_ns;
            }
        }
        if node_matched {
            summary.latest_timestamp_ns = summary.latest_timestamp_ns.max(runtime.timestamp_ns);
            summary.collection_period_ms = summary
                .collection_period_ms
                .max(runtime.collection_period_ms);
        }
    }

    if matched {
        WithProvenance::exact(Some(summary))
    } else {
        WithProvenance::unavailable(
            None,
            "service runtime metrics unavailable because telemetry rows are absent",
        )
    }
}

fn collect_inventory_node_owners<'a>(
    out: &mut BTreeMap<String, BTreeSet<String>>,
    node_id: &str,
    resources: impl Iterator<Item = &'a str>,
) {
    for resource in resources {
        out.entry(resource.to_string())
            .or_default()
            .insert(node_id.to_string());
    }
}

fn exact_nodes_with_provenance(
    node_ids: Option<&BTreeSet<String>>,
    telemetry: Option<&TelemetryDiscoverySnapshot>,
    requested: bool,
) -> WithProvenance<Vec<CandidateNodeRef>> {
    match (node_ids, telemetry, requested) {
        (_, _, false) => WithProvenance::unavailable(Vec::new(), "nodes were not requested"),
        (Some(node_ids), Some(telemetry), true) => {
            WithProvenance::exact(resolve_telemetry_node_refs(node_ids, telemetry))
        }
        (None, Some(_), true) => WithProvenance::unavailable(
            Vec::new(),
            "nodes unavailable because telemetry inventory did not report this resource",
        ),
        (_, None, true) => WithProvenance::unavailable(
            Vec::new(),
            "nodes unavailable because telemetry snapshots could not be resolved",
        ),
    }
}

fn resolve_telemetry_node_refs(
    node_ids: &BTreeSet<String>,
    telemetry: &TelemetryDiscoverySnapshot,
) -> Vec<CandidateNodeRef> {
    let mut refs = Vec::new();
    for node_id in node_ids {
        if let Some(node) = telemetry.nodes.get(node_id) {
            refs.push(CandidateNodeRef {
                name: node.name.clone(),
                id: node.id.clone(),
                matched_endpoint_ids: Vec::new(),
            });
        }
    }
    refs.sort_by(|a, b| {
        a.name
            .as_str()
            .cmp(b.name.as_str())
            .then_with(|| a.id.as_str().cmp(b.id.as_str()))
    });
    refs
}

fn telemetry_node_detail(
    telemetry: &TelemetryDiscoverySnapshot,
    selector: &NodeSelector,
    group: &GroupName,
) -> IntrospectResult<Option<NodeDetail>> {
    let selected = match select_telemetry_node_by_selector(telemetry, selector, group) {
        Ok(selected) => selected,
        Err(err) if err.kind == IntrospectErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    Ok(Some(build_telemetry_node_detail(selected)?))
}

fn build_telemetry_node_detail(selected: &TelemetryNodeEntry) -> IntrospectResult<NodeDetail> {
    let published_topics = selected
        .inventory
        .as_ref()
        .map(|inventory| {
            telemetry_topic_suffixes_from_names(
                inventory
                    .publishers
                    .iter()
                    .map(|entry| entry.common.resource_name.as_str()),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let subscribed_topics = selected
        .inventory
        .as_ref()
        .map(|inventory| {
            telemetry_topic_suffixes_from_names(
                inventory
                    .subscribers
                    .iter()
                    .map(|entry| entry.common.resource_name.as_str()),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let provided_services = selected
        .inventory
        .as_ref()
        .map(|inventory| {
            telemetry_service_suffixes_from_names(
                inventory
                    .services
                    .iter()
                    .map(|entry| entry.common.resource_name.as_str()),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let client_services = selected
        .inventory
        .as_ref()
        .map(|inventory| {
            telemetry_service_suffixes_from_names(
                inventory
                    .clients
                    .iter()
                    .map(|entry| entry.common.resource_name.as_str()),
            )
        })
        .transpose()?
        .unwrap_or_default();
    let exact_topics = merge_unique_topics(&published_topics, &subscribed_topics);
    let exact_services = merge_unique_services(&provided_services, &client_services);

    Ok(NodeDetail {
        node: NodeSnapshot {
            name: selected.name.clone(),
            id: selected.id.clone(),
            group: selected.group.clone(),
            owner_endpoints: WithProvenance::unavailable(
                Vec::new(),
                "owner endpoints unavailable in telemetry snapshot mode",
            ),
            candidate_topic_count: match selected.inventory.as_ref() {
                Some(_) => WithProvenance::exact(Some(exact_topics.len())),
                None => WithProvenance::unavailable(
                    None,
                    "resource inventory unavailable for this telemetry node",
                ),
            },
            candidate_service_count: match selected.inventory.as_ref() {
                Some(_) => WithProvenance::exact(Some(exact_services.len())),
                None => WithProvenance::unavailable(
                    None,
                    "resource inventory unavailable for this telemetry node",
                ),
            },
            runtime: match selected.runtime.as_ref() {
                Some(runtime) => WithProvenance::exact(Some(summarize_runtime_metrics(runtime))),
                None => WithProvenance::unavailable(
                    None,
                    "runtime metrics unavailable because node runtime telemetry is missing",
                ),
            },
        },
        candidate_topics: match selected.inventory.as_ref() {
            Some(_) => WithProvenance::exact(exact_topics),
            None => WithProvenance::unavailable(
                Vec::new(),
                "topics unavailable because telemetry resource inventory is missing",
            ),
        },
        candidate_services: match selected.inventory.as_ref() {
            Some(_) => WithProvenance::exact(exact_services),
            None => WithProvenance::unavailable(
                Vec::new(),
                "services unavailable because telemetry resource inventory is missing",
            ),
        },
        published_topics: match selected.inventory.as_ref() {
            Some(_) => WithProvenance::exact(published_topics),
            None => WithProvenance::unavailable(
                Vec::new(),
                "published topics unavailable because telemetry resource inventory is missing",
            ),
        },
        subscribed_topics: match selected.inventory.as_ref() {
            Some(_) => WithProvenance::exact(subscribed_topics),
            None => WithProvenance::unavailable(
                Vec::new(),
                "subscribed topics unavailable because telemetry resource inventory is missing",
            ),
        },
        provided_services: match selected.inventory.as_ref() {
            Some(_) => WithProvenance::exact(provided_services),
            None => WithProvenance::unavailable(
                Vec::new(),
                "provided services unavailable because telemetry resource inventory is missing",
            ),
        },
        client_services: match selected.inventory.as_ref() {
            Some(_) => WithProvenance::exact(client_services),
            None => WithProvenance::unavailable(
                Vec::new(),
                "client services unavailable because telemetry resource inventory is missing",
            ),
        },
        runtime: match selected.runtime.as_ref() {
            Some(runtime) => WithProvenance::exact(Some(summarize_runtime_metrics(runtime))),
            None => WithProvenance::unavailable(
                None,
                "runtime metrics unavailable because node runtime telemetry is missing",
            ),
        },
        runtime_breakdown: match selected.runtime.as_ref() {
            Some(runtime) => WithProvenance::exact(Some(build_node_runtime_breakdown(runtime))),
            None => WithProvenance::unavailable(
                None,
                "runtime breakdown unavailable because node runtime telemetry is missing",
            ),
        },
    })
}

fn ensure_telemetry_node_entry<'a>(
    nodes: &'a mut BTreeMap<String, TelemetryNodeEntry>,
    node_id: NodeId,
    group: &GroupName,
) -> &'a mut TelemetryNodeEntry {
    nodes
        .entry(node_id.as_str().to_string())
        .or_insert_with(|| TelemetryNodeEntry {
            name: synthesized_telemetry_node_name(&node_id),
            id: node_id,
            group: group.clone(),
            inventory: None,
            runtime: None,
        })
}

fn synthesized_telemetry_node_name(node_id: &NodeId) -> NodeName {
    NodeName::new(format!("telemetry/fallback/id{}", node_id.as_str()))
        .expect("synthetic telemetry node name must be valid")
}

fn select_telemetry_node_by_selector<'a>(
    telemetry: &'a TelemetryDiscoverySnapshot,
    selector: &NodeSelector,
    group: &GroupName,
) -> IntrospectResult<&'a TelemetryNodeEntry> {
    match selector {
        NodeSelector::ById(node_id) => telemetry.nodes.get(node_id.as_str()).ok_or_else(|| {
            IntrospectError::new(
                IntrospectErrorKind::NotFound,
                format!("node '{}' not found in group '{}'", node_id, group),
            )
        }),
        NodeSelector::ByName(node_name) => {
            let matches: Vec<_> = telemetry
                .nodes
                .values()
                .filter(|node| node.name == *node_name)
                .collect();
            if matches.is_empty() {
                return Err(IntrospectError::new(
                    IntrospectErrorKind::NotFound,
                    format!("node '{}' not found in group '{}'", node_name, group),
                ));
            }
            if matches.len() > 1 {
                let candidates = matches
                    .iter()
                    .map(|node| format!("{} ({})", node.name, node.id))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(IntrospectError::new(
                    IntrospectErrorKind::Ambiguous,
                    format!("node '{}' is ambiguous, matched: {}", node_name, candidates),
                ));
            }
            Ok(matches[0])
        }
    }
}

fn entity_matches_selected_telemetry_node(
    node_ids: Option<&BTreeSet<String>>,
    selected_node_id: &NodeId,
) -> bool {
    node_ids.is_some_and(|node_ids| node_ids.contains(selected_node_id.as_str()))
}

fn action_info_selector(group: &str) -> IntrospectResult<String> {
    Ok(format!("aorta/{}/act/info/**", GroupName::new(group)?))
}

fn build_action_info_key(group: &str, action: &str) -> IntrospectResult<String> {
    let builder = KeyExprBuilder::new(group).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid group '{}': {}", group, err))
    })?;
    let key = builder.action_info_key(action).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid action '{}': {}", action, err))
    })?;
    Ok(key.to_string())
}

fn action_status_prefix(group: &str, action: &str) -> IntrospectResult<String> {
    let builder = KeyExprBuilder::new(group).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid group '{}': {}", group, err))
    })?;
    let key = builder.action_status_prefix(action).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid action '{}': {}", action, err))
    })?;
    Ok(key.to_string())
}

async fn fetch_action_schema_bfbs(
    session: &zenoh::Session,
    group: &str,
    info: &ActionInfo,
) -> IntrospectResult<Vec<u8>> {
    let key_builder = KeyExprBuilder::new(group)
        .map_err(|err| IntrospectError::invalid_input(err.to_string()))?;
    aorta_core::action_server::fetch_action_schema_bfbs(
        session,
        &key_builder,
        info,
        Duration::from_secs(2),
    )
    .await
    .map_err(|err| IntrospectError::new(IntrospectErrorKind::Transport, err.to_string()))
}

fn decode_action_snapshot(sample: &QueryReplySample) -> IntrospectResult<ActionSnapshot> {
    let info = ActionInfo::decode(&sample.payload).map_err(|err| {
        IntrospectError::decode(format!(
            "failed to decode ActionInfo for '{}': {}",
            sample.key_expr, err
        ))
    })?;
    Ok(ActionSnapshot {
        key_expr: sample.key_expr.clone(),
        action_name: info.action_name.clone(),
        info,
    })
}

/// Resolve every action surface to its fully-qualified FlatBuffers
/// root type name from the compiled `.bfbs`. Used by
/// [`Client::get_action_detail`] to populate [`ActionDetail::root_types`].
///
/// The aliased import keeps the name distinct from this module's own
/// `ActionRootTypeNames` (a flat 8-string DTO kept stable for
/// downstream consumers like aorta-tui).
fn build_action_root_type_names_from_bfbs(
    bfbs: &[u8],
    action_name: &str,
) -> IntrospectResult<ActionRootTypeNames> {
    use aorta_core::action::resolve_action_root_types_from_bfbs;
    let resolved = resolve_action_root_types_from_bfbs(bfbs).map_err(|err| {
        IntrospectError::decode(format!(
            "failed to resolve action root types for '{}': {}",
            action_name, err
        ))
    })?;
    Ok(ActionRootTypeNames {
        send_goal_request: resolved.for_surface(ActionRootType::SendGoalRequest),
        send_goal_response: resolved.for_surface(ActionRootType::SendGoalResponse),
        cancel_goal_request: resolved.for_surface(ActionRootType::CancelGoalRequest),
        cancel_goal_response: resolved.for_surface(ActionRootType::CancelGoalResponse),
        feedback: resolved.for_surface(ActionRootType::Feedback),
        public_status: resolved.for_surface(ActionRootType::PublicStatus),
        get_result_request: resolved.for_surface(ActionRootType::GetResultRequest),
        get_result_response: resolved.for_surface(ActionRootType::GetResultResponse),
    })
}

fn build_action_send_goal_key(group: &str, action: &str) -> IntrospectResult<String> {
    let builder = KeyExprBuilder::new(group).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid group '{}': {}", group, err))
    })?;
    let key = builder.action_send_goal_key(action).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid action '{}': {}", action, err))
    })?;
    Ok(key.to_string())
}

fn build_action_cancel_goal_key(group: &str, action: &str) -> IntrospectResult<String> {
    let builder = KeyExprBuilder::new(group).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid group '{}': {}", group, err))
    })?;
    let key = builder.action_cancel_goal_key(action).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid action '{}': {}", action, err))
    })?;
    Ok(key.to_string())
}

/// Observe-side feedback subscribe key for an action (wildcard namespace OK).
/// Mirrors the CLI feedback path (`action_feedback_key_sub`).
fn build_action_feedback_key_sub(group: &str, action: &str) -> IntrospectResult<String> {
    let builder = KeyExprBuilder::new(group).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid group '{}': {}", group, err))
    })?;
    let key = builder.action_feedback_key_sub(action).map_err(|err| {
        IntrospectError::invalid_input(format!("invalid action '{}': {}", action, err))
    })?;
    Ok(key.to_string())
}

/// Decode a single `Feedback` payload to an [`ActionFeedbackFrame`].
///
/// Reflects the payload against the action's resolved `Feedback` root type and
/// lifts the well-known fields, mirroring the CLI's `decode_feedback` +
/// `parse_feedback_json` (`tools/aorta-cli/src/cmd/action.rs`). The `data`
/// sub-object (the action-specific feedback body) is re-serialised to compact
/// JSON for the panel.
fn decode_action_feedback_frame(
    bfbs: &[u8],
    feedback_root_type: &str,
    payload: &[u8],
) -> IntrospectResult<ActionFeedbackFrame> {
    let json = reflect::fb_to_json(bfbs, payload, Some(feedback_root_type)).map_err(|err| {
        IntrospectError::decode(format!(
            "failed to decode feedback with root type '{}': {}",
            feedback_root_type, err
        ))
    })?;
    let value: Value = serde_json::from_str(&json).map_err(|err| {
        IntrospectError::decode(format!("failed to parse decoded feedback json: {}", err))
    })?;
    parse_action_feedback_json(&value)
}

/// Parse a reflected `Feedback` JSON object into an [`ActionFeedbackFrame`].
/// Split out from [`decode_action_feedback_frame`] so the field-lifting logic is
/// unit-testable without a `.bfbs` fixture (mirrors the CLI's `parse_feedback_json`).
/// Wire-format (uppercase) label for an `ActionStatus`, matching the strings the
/// CLI parses/prints, rather than the Rust Debug casing.
fn action_status_label(status: ActionStatus) -> &'static str {
    match status {
        ActionStatus::Unknown => "UNKNOWN",
        ActionStatus::Accepted => "ACCEPTED",
        ActionStatus::Executing => "EXECUTING",
        ActionStatus::Canceling => "CANCELING",
        ActionStatus::Succeeded => "SUCCEEDED",
        ActionStatus::Aborted => "ABORTED",
        ActionStatus::Canceled => "CANCELED",
    }
}

fn parse_action_feedback_json(value: &Value) -> IntrospectResult<ActionFeedbackFrame> {
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    Ok(ActionFeedbackFrame {
        goal_id: required_json_str(value, "goal_id")?.to_string(),
        seq: optional_json_u64(value, "seq"),
        stamp_ns: optional_json_u64(value, "stamp_ns"),
        // Wire-format label (e.g. "EXECUTING"), matching CLI output, not Debug casing.
        status: action_status_label(parse_action_status(value.get("status"))?).to_string(),
        message: value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        data_json: serde_json::to_string(&data).unwrap_or_else(|_| "null".to_string()),
    })
}

/// Lift a decoded topic-preview frame for the `/log` topic into a typed
/// [`LogRecordFrame`]. The preview decodes the log FlatBuffer to JSON via the
/// topic's `schema_by_key` schema; this reads the well-known fields out of that
/// JSON, defaulting any that are missing so a schema-less or partial record
/// still renders.
///
/// Two wire shapes are accepted:
///
/// - `foxglove.Log` (current): `timestamp {sec,nsec}`, `name`, plus the Aorta
///   extension fields `aorta_level` and `target`.
/// - `aorta.sys.LogRecord` (legacy): `timestamp_ns`, `node`, `level`, `target`.
///
/// A node built before the `foxglove.Log` switch is still on the bus during a
/// rolling upgrade, and old MCAP captures replay the legacy shape forever, so
/// both keep decoding. `timestamp_ns` only exists in the legacy table, which
/// makes it the discriminator — the level enums do NOT share a numbering, so
/// guessing wrong would silently mislabel every record.
fn log_record_frame_from_preview(preview: &TopicPreviewFrame) -> LogRecordFrame {
    let value: Option<Value> = preview
        .decoded_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok());
    let read_str = |field: &str| -> String {
        value
            .as_ref()
            .and_then(|v| v.get(field))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let legacy_timestamp_ns = value
        .as_ref()
        .and_then(|v| v.get("timestamp_ns"))
        .and_then(Value::as_u64);
    let is_legacy = legacy_timestamp_ns.is_some();

    let timestamp_ns = match legacy_timestamp_ns {
        Some(ns) => ns,
        None => value
            .as_ref()
            .and_then(|v| v.get("timestamp"))
            .map(|stamp| {
                let sec = stamp.get("sec").and_then(Value::as_u64).unwrap_or(0);
                let nsec = stamp.get("nsec").and_then(Value::as_u64).unwrap_or(0);
                sec.saturating_mul(1_000_000_000).saturating_add(nsec)
            })
            .unwrap_or(0),
    };

    let level = if is_legacy {
        value
            .as_ref()
            .and_then(|v| v.get("level"))
            .map(|v| enum_label(v, LEGACY_LOG_LEVELS))
            .unwrap_or_else(|| "?".to_string())
    } else {
        // Prefer `aorta_level`: it is the only field that still distinguishes
        // TRACE, which `foxglove.Log.level` has to collapse onto DEBUG.
        value
            .as_ref()
            .and_then(|v| v.get("aorta_level"))
            .map(|v| enum_label(v, AORTA_LOG_LEVELS))
            .or_else(|| {
                value
                    .as_ref()
                    .and_then(|v| v.get("level"))
                    .map(|v| enum_label(v, FOXGLOVE_LOG_LEVELS))
            })
            .unwrap_or_else(|| "?".to_string())
    };

    LogRecordFrame {
        timestamp_ns,
        level,
        target: read_str("target"),
        message: read_str("message"),
        node: if is_legacy {
            read_str("node")
        } else {
            read_str("name")
        },
        decoded_json: preview.decoded_json.clone(),
    }
}

/// `aorta.log.AortaLogLevel` variants, indexed by wire value.
const AORTA_LOG_LEVELS: &[&str] = &[
    "UNKNOWN", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL",
];
/// `foxglove.LogLevel` variants, indexed by wire value.
const FOXGLOVE_LOG_LEVELS: &[&str] = &["UNKNOWN", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL"];
/// Legacy `aorta.sys.LogLevel` variants, indexed by wire value. Note 0 is
/// TRACE here but UNKNOWN in both enums above — the reason the caller must
/// pick the table from the record's shape rather than sharing one.
const LEGACY_LOG_LEVELS: &[&str] = &["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];

/// Map a reflected log-level enum to a short label. FlatBuffers reflection
/// renders enums as their variant name, but tolerate a numeric fallback by
/// indexing into `variants`.
fn enum_label(value: &Value, variants: &[&str]) -> String {
    if let Some(name) = value.as_str() {
        return name.to_string();
    }
    value
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .and_then(|idx| variants.get(idx))
        .copied()
        .unwrap_or("?")
        .to_string()
}

/// Resolve the goal_id for a send: validate a caller-supplied one (must be a
/// UUID, matching the server's `goal_id_to_key`), or mint a fresh v4 UUID.
/// Mirrors the CLI's `resolve_goal_id` (`cmd/action.rs`).
fn resolve_action_goal_id(goal_id: Option<&str>) -> IntrospectResult<String> {
    match goal_id {
        Some(goal_id) => {
            goal_id_to_key(goal_id).map_err(|err| {
                IntrospectError::invalid_input(format!("invalid goal_id '{}': {}", goal_id, err))
            })?;
            Ok(goal_id.to_string())
        }
        None => Ok(uuid::Uuid::new_v4().hyphenated().to_string()),
    }
}

/// Parse the user's send-goal request body into the `goal` sub-value. The TUI
/// editor holds the *goal payload* JSON directly (the `goal_id` wrapper is
/// added by [`Client::send_action_goal`]); an empty body is treated as `{}`.
fn parse_request_goal_json(request_json: &str) -> IntrospectResult<Value> {
    let trimmed = request_json.trim();
    if trimmed.is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    serde_json::from_str(trimmed)
        .map_err(|err| IntrospectError::invalid_input(format!("invalid goal JSON: {}", err)))
}

/// Encode an action surface JSON value to FlatBuffers via the reflection
/// bridge. Mirrors `cmd/action.rs::encode_action_json`.
fn encode_action_json(
    bfbs: &[u8],
    action_name: &str,
    root_type: ActionRootType,
    value: &Value,
) -> IntrospectResult<Vec<u8>> {
    let root_type_name = {
        use aorta_core::action::resolve_action_root_types_from_bfbs;
        resolve_action_root_types_from_bfbs(bfbs)
            .map(|names| names.for_surface(root_type))
            .map_err(|err| {
                IntrospectError::decode(format!(
                    "failed to resolve action root type {:?} for '{}': {}",
                    root_type, action_name, err
                ))
            })?
    };
    let json = serde_json::to_string(value).map_err(|err| {
        IntrospectError::decode(format!("failed to serialize action json value: {}", err))
    })?;
    reflect::json_to_fb(bfbs, &json, Some(&root_type_name)).map_err(|err| {
        IntrospectError::decode(format!(
            "failed to encode action '{}' payload with root type '{}': {}",
            action_name, root_type_name, err
        ))
    })
}

fn parse_send_goal_result_json(value: &Value) -> IntrospectResult<ActionGoalSendResult> {
    Ok(ActionGoalSendResult {
        accepted: required_json_bool(value, "accepted")?,
        goal_id: required_json_str(value, "goal_id")?.to_string(),
        goal_secret: required_json_str(value, "goal_secret")?.to_string(),
        status: parse_action_status(value.get("status"))?,
        code: parse_action_reply_code(value.get("code"))?,
        // Required (reflection emits every fbs field, so absence = malformed reply),
        // matching the CLI rather than silently defaulting to empty.
        message: required_json_str(value, "message")?.to_string(),
        server_epoch: required_json_str(value, "server_epoch")?.to_string(),
    })
}

fn parse_cancel_goal_result_json(value: &Value) -> IntrospectResult<ActionCancelResult> {
    Ok(ActionCancelResult {
        accepted: required_json_bool(value, "accepted")?,
        goal_id: required_json_str(value, "goal_id")?.to_string(),
        status: parse_action_status(value.get("status"))?,
        code: parse_action_reply_code(value.get("code"))?,
        message: required_json_str(value, "message")?.to_string(),
    })
}

fn required_json_str<'a>(value: &'a Value, field: &str) -> IntrospectResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| IntrospectError::decode(format!("missing string field '{}'", field)))
}

fn required_json_bool(value: &Value, field: &str) -> IntrospectResult<bool> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| IntrospectError::decode(format!("missing bool field '{}'", field)))
}

fn decode_action_goal_snapshot(
    action_name: &str,
    bfbs: &[u8],
    payload: &[u8],
) -> IntrospectResult<ActionGoalSnapshot> {
    let json = decode_action_json(bfbs, action_name, ActionRootType::PublicStatus, payload)?;
    parse_action_goal_snapshot_json(&json)
}

fn decode_action_json(
    bfbs: &[u8],
    action_name: &str,
    root_type: ActionRootType,
    payload: &[u8],
) -> IntrospectResult<Value> {
    let root_type_name = {
        use aorta_core::action::resolve_action_root_types_from_bfbs;
        resolve_action_root_types_from_bfbs(bfbs)
            .map(|names| names.for_surface(root_type))
            .map_err(|err| {
                IntrospectError::decode(format!(
                    "failed to resolve action root type {:?} for '{}': {}",
                    root_type, action_name, err
                ))
            })?
    };
    let json = reflect::fb_to_json(bfbs, payload, Some(&root_type_name)).map_err(|err| {
        IntrospectError::decode(format!(
            "failed to decode action '{}' payload with root type '{}': {}",
            action_name, root_type_name, err
        ))
    })?;
    serde_json::from_str(&json).map_err(|err| {
        IntrospectError::decode(format!(
            "failed to parse decoded action json '{}': {}",
            json, err
        ))
    })
}

fn parse_action_goal_snapshot_json(value: &Value) -> IntrospectResult<ActionGoalSnapshot> {
    Ok(ActionGoalSnapshot {
        goal_id: value
            .get("goal_id")
            .and_then(Value::as_str)
            .ok_or_else(|| IntrospectError::decode("missing goal_id in action public status"))?
            .to_string(),
        status: parse_action_status(value.get("status"))?,
        code: parse_action_reply_code(value.get("code"))?,
        accepted_stamp_ns: optional_json_u64(value, "accepted_stamp_ns"),
        started_stamp_ns: optional_json_u64(value, "started_stamp_ns"),
        finished_stamp_ns: optional_json_u64(value, "finished_stamp_ns"),
        last_feedback_seq: optional_json_u64(value, "last_feedback_seq"),
        server_epoch: value
            .get("server_epoch")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    })
}

fn optional_json_u64(value: &Value, field: &str) -> u64 {
    value
        .get(field)
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
        })
        .unwrap_or(0)
}

fn parse_action_status(value: Option<&Value>) -> IntrospectResult<ActionStatus> {
    let value =
        value.ok_or_else(|| IntrospectError::decode("missing status in action public status"))?;
    if let Some(text) = value.as_str() {
        return match text {
            "UNKNOWN" => Ok(ActionStatus::Unknown),
            "ACCEPTED" => Ok(ActionStatus::Accepted),
            "EXECUTING" => Ok(ActionStatus::Executing),
            "CANCELING" => Ok(ActionStatus::Canceling),
            "SUCCEEDED" => Ok(ActionStatus::Succeeded),
            "ABORTED" => Ok(ActionStatus::Aborted),
            "CANCELED" => Ok(ActionStatus::Canceled),
            other => Err(IntrospectError::decode(format!(
                "unknown action status '{}'",
                other
            ))),
        };
    }
    let number = value
        .as_i64()
        .ok_or_else(|| IntrospectError::decode("action status must be string or integer"))?;
    // Checked (not truncating) conversion, matching the CLI: an out-of-range
    // numeric reply must error rather than silently fold into a valid enum.
    let narrowed = i8::try_from(number)
        .map_err(|err| IntrospectError::decode(format!("action status out of range: {}", err)))?;
    ActionStatus::try_from(narrowed).map_err(|err| {
        IntrospectError::decode(format!("invalid action status value {}: {}", number, err))
    })
}

fn parse_action_reply_code(value: Option<&Value>) -> IntrospectResult<ActionReplyCode> {
    let value =
        value.ok_or_else(|| IntrospectError::decode("missing code in action public status"))?;
    if let Some(text) = value.as_str() {
        return match text {
            "NONE" => Ok(ActionReplyCode::None),
            "OK" => Ok(ActionReplyCode::Ok),
            "DUPLICATE_ACCEPTED" => Ok(ActionReplyCode::DuplicateAccepted),
            "SUCCEEDED" => Ok(ActionReplyCode::Succeeded),
            "CANCELED" => Ok(ActionReplyCode::Canceled),
            "BUSY" => Ok(ActionReplyCode::Busy),
            "PAYLOAD_CONFLICT" => Ok(ActionReplyCode::PayloadConflict),
            "UNAUTHORIZED" => Ok(ActionReplyCode::Unauthorized),
            "NOT_FOUND" => Ok(ActionReplyCode::NotFound),
            "ALREADY_TERMINAL" => Ok(ActionReplyCode::AlreadyTerminal),
            "GOAL_EXPIRED" => Ok(ActionReplyCode::GoalExpired),
            "HANDLER_ERROR" => Ok(ActionReplyCode::HandlerError),
            "HANDLER_PANIC" => Ok(ActionReplyCode::HandlerPanic),
            "HANDLER_COMPLETED_WITHOUT_RESULT" => {
                Ok(ActionReplyCode::HandlerCompletedWithoutResult)
            }
            "CONTEXT_DROPPED" => Ok(ActionReplyCode::ContextDropped),
            "SERVER_CRASH" => Ok(ActionReplyCode::ServerCrash),
            "CONFLICT" => Ok(ActionReplyCode::Conflict),
            other => Err(IntrospectError::decode(format!(
                "unknown action reply code '{}'",
                other
            ))),
        };
    }
    let number = value
        .as_i64()
        .ok_or_else(|| IntrospectError::decode("action reply code must be string or integer"))?;
    // Checked (not truncating) conversion, matching the CLI.
    let narrowed = i16::try_from(number).map_err(|err| {
        IntrospectError::decode(format!("action reply code out of range: {}", err))
    })?;
    ActionReplyCode::try_from(narrowed).map_err(|err| {
        IntrospectError::decode(format!(
            "invalid action reply code value {}: {}",
            number, err
        ))
    })
}

async fn query_single_reply(
    session: &zenoh::Session,
    selector: &str,
    timeout: Duration,
) -> IntrospectResult<Option<QueryReplySample>> {
    Ok(collect_get_replies(session, selector, timeout)
        .await?
        .into_iter()
        .next())
}

/// Build the wildcard ContextStore selector for a group + optional prefix.
///
/// Kept byte-for-byte equivalent to the CLI's `ctx_root_for_group`
/// (`tools/aorta-cli/src/cmd/ctx.rs`) so both surfaces query the exact same
/// keyexpr: `aorta/{group}/ctx/{prefix}` (slashes trimmed) or
/// `aorta/{group}/ctx/**` when no prefix is given.
fn ctx_root_for_group(group: &str, prefix: Option<&str>) -> String {
    match prefix {
        Some(p) => format!("aorta/{}/ctx/{}", group, p.trim_matches('/')),
        None => format!("aorta/{}/ctx/**", group),
    }
}

/// Render the active value of a decoded `ContextValue` as a human-readable
/// string. Mirrors the CLI's `render_value`: kind-aware, with String values
/// truncated to 64 characters.
fn render_context_value(value: &aorta_core::contextstore_fb::ContextValue<'_>) -> String {
    match value.kind() {
        ContextKind::None => "<none>".to_string(),
        ContextKind::Bool => format!("{}", value.bool_value()),
        ContextKind::I64 => format!("{}", value.i64_value()),
        ContextKind::U64 => format!("{}", value.u64_value()),
        ContextKind::F64 => format!("{}", value.f64_value()),
        ContextKind::String => value
            .string_value()
            .unwrap_or_default()
            .chars()
            .take(64)
            .collect(),
    }
}

/// Decode a single ContextStore owner reply into a [`ContextEntry`].
///
/// Returns `None` when the payload is not a valid `ContextValue` FlatBuffer,
/// matching the CLI's `parse_reply_payload` skip-on-malformed behavior.
fn decode_context_entry(sample: &QueryReplySample) -> Option<ContextEntry> {
    let value = root_as_context_value(&sample.payload)?;
    Some(ContextEntry {
        key: sample.key_expr.clone(),
        value_text: render_context_value(&value),
        kind: value.kind().name(),
        source_node: value.source_node().unwrap_or("").to_string(),
        source_instance_id: value.source_instance_id().unwrap_or("").to_string(),
        expires_at_ns: value.expires_at_ns(),
    })
}

async fn collect_get_replies(
    session: &zenoh::Session,
    selector: &str,
    timeout: Duration,
) -> IntrospectResult<Vec<QueryReplySample>> {
    let replies = tokio::time::timeout(
        timeout,
        session
            .get(selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(timeout),
    )
    .await
    .map_err(|_| {
        IntrospectError::new(
            IntrospectErrorKind::Timeout,
            format!("query '{}' timed out after {:?}", selector, timeout),
        )
    })?
    .map_err(|err| {
        IntrospectError::new(
            IntrospectErrorKind::Transport,
            format!("failed to query '{}': {}", selector, err),
        )
    })?;

    let started = std::time::Instant::now();
    let mut out = Vec::new();
    loop {
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            break;
        }
        let remaining = timeout.saturating_sub(elapsed);
        match tokio::time::timeout(remaining, replies.recv_async()).await {
            Ok(Ok(reply)) => {
                if let Ok(sample) = reply.into_result() {
                    out.push(QueryReplySample {
                        key_expr: sample.key_expr().as_str().to_string(),
                        payload: sample.payload().to_bytes().to_vec(),
                    });
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }
    Ok(out)
}

async fn discover_key_suffixes(
    session: &zenoh::Session,
    selector: &str,
    prefix: &str,
    timeout: Duration,
) -> IntrospectResult<BTreeSet<String>> {
    let replies = collect_get_replies(session, selector, timeout).await?;
    let mut out = BTreeSet::new();
    for reply in replies {
        if let Some(suffix) = extract_normalized_suffix(&reply, prefix) {
            out.insert(suffix);
        }
    }
    Ok(out)
}

async fn discover_admin_entities(
    session: &zenoh::Session,
    selector: &str,
    prefix: &str,
    timeout: Duration,
) -> IntrospectResult<BTreeMap<String, AdminEntityOwners>> {
    let replies = collect_get_replies(session, selector, timeout).await?;
    let mut entities = BTreeMap::<String, AdminEntityOwners>::new();
    for reply in replies {
        if let Some(suffix) = extract_normalized_suffix(&reply, prefix) {
            let owners = parse_admin_entity_owners(&reply);
            entities
                .entry(suffix)
                .and_modify(|acc| acc.merge_from(&owners))
                .or_insert(owners);
        }
    }
    Ok(entities)
}

fn admin_entities_with_telemetry_fallback(
    admin_entities: IntrospectResult<BTreeMap<String, AdminEntityOwners>>,
    telemetry: Option<&TelemetryDiscoverySnapshot>,
) -> IntrospectResult<BTreeMap<String, AdminEntityOwners>> {
    match admin_entities {
        Ok(entities) => Ok(entities),
        Err(_) if telemetry.is_some() => Ok(BTreeMap::new()),
        Err(err) => Err(err),
    }
}

async fn discover_nodes_via_admin(
    session: &zenoh::Session,
    group: &GroupName,
    timeout: Duration,
) -> IntrospectResult<Vec<AdminNodeEntry>> {
    let selector = format!("@/**/router/token/aorta/{}/sys/alive/**", group);
    let prefix = format!("aorta/{}/sys/alive/", group);
    let replies = collect_get_replies(session, &selector, timeout).await?;
    let mut nodes = HashMap::<String, AdminNodeEntry>::new();

    for reply in replies {
        if let Some((name, id)) = parse_node_from_admin_reply(&reply, &prefix) {
            let owners = parse_admin_entity_owners(&reply);
            let key = format!("{}/{}", name, id);
            nodes
                .entry(key)
                .and_modify(|entry| entry.owners.merge_from(&owners))
                .or_insert(AdminNodeEntry {
                    name,
                    id,
                    group: group.clone(),
                    owners,
                });
        }
    }

    let mut out: Vec<_> = nodes.into_values().collect();
    out.sort_by(|a, b| {
        a.name
            .as_str()
            .cmp(b.name.as_str())
            .then_with(|| a.id.as_str().cmp(b.id.as_str()))
    });
    Ok(out)
}

async fn discover_nodes_via_liveliness(
    session: &zenoh::Session,
    group: &GroupName,
    timeout: Duration,
) -> IntrospectResult<Vec<LivelinessNodeInfo>> {
    let pattern = format!("aorta/{}/sys/alive/**", group);
    let replies = session
        .liveliness()
        .get(&pattern)
        .timeout(timeout)
        .await
        .map_err(|err| {
            IntrospectError::new(
                IntrospectErrorKind::Transport,
                format!("failed to query liveliness '{}': {}", pattern, err),
            )
        })?;

    let deadline = tokio::time::Instant::now() + timeout;
    let mut nodes = HashMap::<String, LivelinessNodeInfo>::new();
    while let Ok(reply) = tokio::time::timeout_at(deadline, replies.recv_async()).await {
        let Ok(reply) = reply else {
            break;
        };
        if let Ok(sample) = reply.into_result() {
            if let Some(node) = parse_liveliness_key(sample.key_expr().as_str(), group) {
                let key = format!("{}/{}", node.name, node.id);
                nodes.insert(key, node);
            }
        }
    }

    let mut out: Vec<_> = nodes.into_values().collect();
    out.sort_by(|a, b| {
        a.name
            .as_str()
            .cmp(b.name.as_str())
            .then_with(|| a.id.as_str().cmp(b.id.as_str()))
    });
    Ok(out)
}

async fn discover_topics_for_node(
    session: &zenoh::Session,
    group: &GroupName,
    node_endpoints: &HashSet<String>,
    timeout: Duration,
) -> IntrospectResult<Vec<TopicSuffix>> {
    let selector = format!("@/**/router/publisher/aorta/{}/pub/**", group);
    let prefix = format!("aorta/{}/pub/", group);
    let topics = discover_admin_entities(session, &selector, &prefix, timeout).await?;
    filter_entities_by_endpoints(&topics, node_endpoints, |suffix| TopicSuffix::new(suffix))
}

async fn discover_services_for_node(
    session: &zenoh::Session,
    group: &GroupName,
    node_endpoints: &HashSet<String>,
    timeout: Duration,
) -> IntrospectResult<Vec<ServiceSuffix>> {
    let selector = format!("@/**/router/queryable/aorta/{}/srv/**", group);
    let prefix = format!("aorta/{}/srv/", group);
    let services = discover_admin_entities(session, &selector, &prefix, timeout).await?;
    filter_entities_by_endpoints(&services, node_endpoints, |suffix| {
        ServiceSuffix::new(suffix)
    })
}

async fn discover_resource_counts_for_nodes(
    session: &zenoh::Session,
    group: &GroupName,
    nodes: &[AdminNodeEntry],
    timeout: Duration,
) -> IntrospectResult<HashMap<String, (usize, usize)>> {
    let topic_selector = format!("@/**/router/publisher/aorta/{}/pub/**", group);
    let topic_prefix = format!("aorta/{}/pub/", group);
    let topics = discover_admin_entities(session, &topic_selector, &topic_prefix, timeout).await?;

    let service_selector = format!("@/**/router/queryable/aorta/{}/srv/**", group);
    let service_prefix = format!("aorta/{}/srv/", group);
    let services =
        discover_admin_entities(session, &service_selector, &service_prefix, timeout).await?;

    let mut counts = HashMap::<String, (usize, usize)>::new();
    for node in nodes {
        let endpoint_ids: HashSet<_> = node
            .owners
            .endpoint_ids()
            .into_iter()
            .map(|endpoint| endpoint.as_str().to_string())
            .collect();
        let topic_count = count_entities_by_endpoints(&topics, &endpoint_ids);
        let service_count = count_entities_by_endpoints(&services, &endpoint_ids);
        counts.insert(
            format!("{}/{}", node.name, node.id),
            (topic_count, service_count),
        );
    }
    Ok(counts)
}

fn count_entities_by_endpoints(
    entities: &BTreeMap<String, AdminEntityOwners>,
    endpoint_ids: &HashSet<String>,
) -> usize {
    entities
        .values()
        .filter(|owners| {
            owners
                .endpoint_ids()
                .iter()
                .any(|id| endpoint_ids.contains(id.as_str()))
        })
        .count()
}

fn filter_entities_by_endpoints<T, F>(
    entities: &BTreeMap<String, AdminEntityOwners>,
    node_endpoints: &HashSet<String>,
    build: F,
) -> IntrospectResult<Vec<T>>
where
    F: Fn(&str) -> IntrospectResult<T>,
{
    let mut matched = Vec::new();
    for (suffix, owners) in entities {
        let owner_ids = owners.endpoint_ids();
        if owner_ids
            .iter()
            .any(|id| node_endpoints.contains(id.as_str()))
        {
            matched.push(build(suffix)?);
        }
    }
    Ok(matched)
}

fn resolve_nodes_for_entity(
    entity_owners: &AdminEntityOwners,
    nodes: &[AdminNodeEntry],
) -> Vec<CandidateNodeRef> {
    let entity_ids: HashSet<String> = entity_owners
        .endpoint_ids()
        .into_iter()
        .map(|id| id.as_str().to_string())
        .collect();
    let mut out = Vec::new();
    for node in nodes {
        let matched_endpoint_ids: Vec<_> = node
            .owners
            .endpoint_ids()
            .into_iter()
            .filter(|id| entity_ids.contains(id.as_str()))
            .collect();
        if !matched_endpoint_ids.is_empty() {
            out.push(CandidateNodeRef {
                name: node.name.clone(),
                id: node.id.clone(),
                matched_endpoint_ids,
            });
        }
    }
    out.sort_by(|a, b| {
        a.name
            .as_str()
            .cmp(b.name.as_str())
            .then_with(|| a.id.as_str().cmp(b.id.as_str()))
    });
    out
}

fn select_node_by_selector<'a>(
    nodes: &'a [AdminNodeEntry],
    selector: &NodeSelector,
    group: &GroupName,
) -> IntrospectResult<&'a AdminNodeEntry> {
    match selector {
        NodeSelector::ById(node_id) => {
            nodes
                .iter()
                .find(|node| node.id == *node_id)
                .ok_or_else(|| {
                    IntrospectError::new(
                        IntrospectErrorKind::NotFound,
                        format!("node '{}' not found in group '{}'", node_id, group),
                    )
                })
        }
        NodeSelector::ByName(node_name) => {
            let matches: Vec<_> = nodes
                .iter()
                .filter(|node| node.name == *node_name)
                .collect();
            if matches.is_empty() {
                return Err(IntrospectError::new(
                    IntrospectErrorKind::NotFound,
                    format!("node '{}' not found in group '{}'", node_name, group),
                ));
            }
            if matches.len() > 1 {
                let candidates = matches
                    .iter()
                    .map(|node| format!("{} ({})", node.name, node.id))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(IntrospectError::new(
                    IntrospectErrorKind::Ambiguous,
                    format!("node '{}' is ambiguous, matched: {}", node_name, candidates),
                ));
            }
            Ok(matches[0])
        }
    }
}

fn select_liveliness_node_by_selector(
    nodes: &[LivelinessNodeInfo],
    selector: &NodeSelector,
    group: &GroupName,
) -> IntrospectResult<LivelinessNodeInfo> {
    match selector {
        NodeSelector::ById(node_id) => nodes
            .iter()
            .find(|node| node.id == *node_id)
            .cloned()
            .ok_or_else(|| {
                IntrospectError::new(
                    IntrospectErrorKind::NotFound,
                    format!("node '{}' not found in group '{}'", node_id, group),
                )
            }),
        NodeSelector::ByName(node_name) => {
            let matches: Vec<_> = nodes
                .iter()
                .filter(|node| node.name == *node_name)
                .cloned()
                .collect();
            if matches.is_empty() {
                return Err(IntrospectError::new(
                    IntrospectErrorKind::NotFound,
                    format!("node '{}' not found in group '{}'", node_name, group),
                ));
            }
            if matches.len() > 1 {
                let candidates = matches
                    .iter()
                    .map(|node| format!("{} ({})", node.name, node.id))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(IntrospectError::new(
                    IntrospectErrorKind::Ambiguous,
                    format!("node '{}' is ambiguous, matched: {}", node_name, candidates),
                ));
            }
            Ok(matches[0].clone())
        }
    }
}

fn parse_admin_entity_owners(reply: &QueryReplySample) -> AdminEntityOwners {
    serde_json::from_slice(&reply.payload).unwrap_or_default()
}

fn parse_zenoh_router_overview(reply: &QueryReplySample) -> Option<ZenohRouterOverview> {
    let (zid, whatami) = parse_router_root_key(&reply.key_expr)?;
    let raw_payload_utf8 = String::from_utf8_lossy(&reply.payload).to_string();
    let parsed = serde_json::from_slice::<Value>(&reply.payload).ok();
    Some(ZenohRouterOverview {
        zid,
        whatami,
        version: parsed
            .as_ref()
            .and_then(|value| value.get("version"))
            .and_then(Value::as_str)
            .map(str::to_string),
        locators: parsed
            .as_ref()
            .and_then(|value| value.get("locators"))
            .map(json_string_list)
            .unwrap_or_default(),
        session_count: parsed
            .as_ref()
            .and_then(|value| value.get("sessions"))
            .and_then(json_collection_count),
        plugin_names: parsed
            .as_ref()
            .and_then(|value| value.get("plugins"))
            .map(json_plugin_names)
            .unwrap_or_default(),
        metrics_available: false,
        metrics: None,
        raw_payload_utf8,
    })
}

/// Parse a `@/<zid>/router/metrics` reply into summed router counters. Returns
/// the router zid (from the key) plus the parsed metrics, or `None` if:
/// - the key is not a router-metrics key,
/// - the body is still gzip (the router ignored `?compression=false`), or
/// - the body exposes **no** stats families at all — a router built *without*
///   the `stats` feature still answers this endpoint with only a `zenoh_build_info`
///   line, and that must read as "metrics unavailable", not all-zero counters.
fn parse_router_metrics(reply: &QueryReplySample) -> Option<(String, ZenohRouterMetrics)> {
    let zid = parse_router_metrics_zid(reply)?;
    // A gzip body (router did not honor `?compression=false`) begins with the
    // gzip magic 0x1f 0x8b — never feed that to the text scanner; report the
    // metrics as unreadable instead of emitting garbage counters.
    if reply.payload.starts_with(&[0x1f, 0x8b]) {
        return None;
    }
    let body = std::str::from_utf8(&reply.payload).ok()?;
    if !body_exposes_router_stats(body) {
        return None;
    }
    Some((zid, parse_openmetrics_router_counters(body)))
}

/// Whether an OpenMetrics body exposes the `stats`-feature router families at all.
///
/// A router compiled *without* the `stats` feature still replies to
/// `@/<zid>/router/metrics`, but only with a `zenoh_build_info` line — there are
/// no traffic counters. A router *with* `stats` always emits the tx/rx family
/// descriptors (`# TYPE zenoh_tx_bytes counter`, …) even at zero traffic. So the
/// presence of any `zenoh_*` metric name other than `zenoh_build*` — on a sample
/// line **or** a `# TYPE/HELP/UNIT` descriptor — marks stats as exposed.
fn body_exposes_router_stats(body: &str) -> bool {
    body.lines().any(|line| {
        let line = line.trim();
        if line.is_empty() {
            return false;
        }
        // Metric name token: on a descriptor it is the 2nd word (`# TYPE <name> …`);
        // on a sample line it is the text before the `{labels}` or first space.
        let name = if let Some(rest) = line.strip_prefix('#') {
            rest.split_whitespace().nth(1)
        } else {
            line.split(['{', ' ']).next()
        };
        matches!(name, Some(name) if name.starts_with("zenoh_") && !name.starts_with("zenoh_build"))
    })
}

/// Sum the relevant OpenMetrics sample lines into [`ZenohRouterMetrics`].
/// Tolerant of `# HELP/TYPE/UNIT/EOF` comment lines and of a metric being split
/// across several label sets (e.g. per-protocol `zenoh_tx_bytes_total`), which
/// are summed. `_created` and histogram (`_bucket`/`_sum`/`_count`) lines do not
/// match the counter names and are ignored.
fn parse_openmetrics_router_counters(body: &str) -> ZenohRouterMetrics {
    let mut metrics = ZenohRouterMetrics::default();
    let mut transports_opened: Option<u64> = None;
    let mut links_opened: Option<u64> = None;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = parse_openmetrics_sample_line(line) else {
            continue;
        };
        match name {
            "zenoh_tx_bytes_total" => {
                metrics.tx_bytes_total = metrics.tx_bytes_total.saturating_add(value);
            }
            "zenoh_rx_bytes_total" => {
                metrics.rx_bytes_total = metrics.rx_bytes_total.saturating_add(value);
            }
            "zenoh_tx_network_message_total" => {
                metrics.tx_msgs_total = metrics.tx_msgs_total.saturating_add(value);
            }
            "zenoh_rx_network_message_total" => {
                metrics.rx_msgs_total = metrics.rx_msgs_total.saturating_add(value);
            }
            "zenoh_transports_opened" => {
                transports_opened = Some(transports_opened.unwrap_or(0).saturating_add(value));
            }
            "zenoh_links_opened" | "zenoh_links_opened_total" => {
                links_opened = Some(links_opened.unwrap_or(0).saturating_add(value));
            }
            _ => {}
        }
    }
    metrics.transports_opened = transports_opened;
    metrics.links_opened = links_opened;
    metrics
}

/// Split one OpenMetrics sample line into `(metric_name, value)`. The name is the
/// token before the `{labels}` block (or before the first space when unlabeled);
/// the value is the first whitespace token after the labels. Float values are
/// truncated to `u64` (zenoh counters are integers, but be tolerant).
fn parse_openmetrics_sample_line(line: &str) -> Option<(&str, u64)> {
    let (name, rest) = match line.split_once('{') {
        Some((name, after)) => {
            let close = after.find('}')?;
            (name.trim(), &after[close + 1..])
        }
        None => {
            let (name, rest) = line.split_once(char::is_whitespace)?;
            (name, rest)
        }
    };
    let value_token = rest.split_whitespace().next()?;
    let value = value_token
        .parse::<u64>()
        .ok()
        .or_else(|| value_token.parse::<f64>().ok().map(|v| v.max(0.0) as u64))?;
    Some((name, value))
}

fn parse_router_root_key(key_expr: &str) -> Option<(String, String)> {
    let mut parts = key_expr
        .strip_prefix('@')?
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty());
    let zid = parts.next()?.to_string();
    let whatami = parts.next()?.to_string();
    if parts.next().is_some() {
        return None;
    }
    Some((zid, whatami))
}

fn parse_router_metrics_zid(reply: &QueryReplySample) -> Option<String> {
    let mut parts = reply
        .key_expr
        .strip_prefix('@')?
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty());
    let zid = parts.next()?.to_string();
    let whatami = parts.next()?;
    let metrics = parts.next()?;
    if whatami != "router" || metrics != "metrics" {
        return None;
    }
    Some(zid)
}

fn json_collection_count(value: &Value) -> Option<usize> {
    match value {
        Value::Array(entries) => Some(entries.len()),
        Value::Object(entries) => Some(entries.len()),
        Value::Number(number) => number.as_u64().map(|count| count as usize),
        _ => None,
    }
}

fn json_string_list(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => vec![text.to_string()],
        Value::Array(entries) => entries
            .iter()
            .flat_map(json_string_list)
            .collect::<Vec<_>>(),
        Value::Object(entries) => entries.keys().cloned().collect(),
        _ => Vec::new(),
    }
}

fn json_plugin_names(value: &Value) -> Vec<String> {
    match value {
        Value::Object(entries) => entries.keys().cloned().collect(),
        Value::Array(entries) => {
            let mut names = Vec::new();
            for entry in entries {
                match entry {
                    Value::String(text) => names.push(text.clone()),
                    Value::Object(object) => {
                        if let Some(name) = object.get("name").and_then(Value::as_str) {
                            names.push(name.to_string());
                        }
                    }
                    _ => {}
                }
            }
            names
        }
        Value::String(text) => vec![text.to_string()],
        _ => Vec::new(),
    }
}

fn extract_normalized_suffix(reply: &QueryReplySample, prefix: &str) -> Option<String> {
    extract_normalized_suffix_from_text(&reply.key_expr, prefix).or_else(|| {
        std::str::from_utf8(&reply.payload)
            .ok()
            .and_then(|text| extract_normalized_suffix_from_text(text, prefix))
    })
}

fn extract_normalized_suffix_from_text(text: &str, prefix: &str) -> Option<String> {
    let remainder = find_after_prefix(text, prefix)?;
    let mut accepted: Vec<&str> = Vec::new();

    for seg in remainder.split('/') {
        if seg.is_empty() {
            continue;
        }
        accepted.push(seg);
        let candidate = accepted.join("/");
        if normalize_path(&candidate).is_err() {
            accepted.pop();
            break;
        }
    }

    if accepted.is_empty() {
        return None;
    }
    Some(accepted.join("/"))
}

fn parse_node_from_admin_reply(
    reply: &QueryReplySample,
    prefix: &str,
) -> Option<(NodeName, NodeId)> {
    parse_node_from_admin_text(&reply.key_expr, prefix).or_else(|| {
        std::str::from_utf8(&reply.payload)
            .ok()
            .and_then(|text| parse_node_from_admin_text(text, prefix))
    })
}

fn parse_node_from_admin_text(text: &str, prefix: &str) -> Option<(NodeName, NodeId)> {
    let remainder = find_after_prefix(text, prefix)?;
    let mut parts = remainder.split('/').filter(|s| !s.is_empty());
    let name = NodeName::new(parts.next()?).ok()?;
    let id = NodeId::new(parts.next()?).ok()?;
    Some((name, id))
}

fn parse_liveliness_key(key: &str, expected_group: &GroupName) -> Option<LivelinessNodeInfo> {
    let prefix = format!("aorta/{}/sys/alive/", expected_group);
    let suffix = key.strip_prefix(&prefix)?;
    let parts: Vec<_> = suffix.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    Some(LivelinessNodeInfo {
        name: NodeName::new(parts[0]).ok()?,
        id: NodeId::new(parts[1]).ok()?,
        group: expected_group.clone(),
    })
}

fn find_after_prefix<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let idx = text.find(prefix)?;
    Some(&text[idx + prefix.len()..])
}

fn compute_rate_snapshot(
    intervals: &VecDeque<f64>,
    duration: Duration,
) -> IntrospectResult<Option<RateSnapshot>> {
    if intervals.is_empty() {
        return Ok(None);
    }

    let window_samples = intervals.len();
    let sum: f64 = intervals.iter().copied().sum();
    let mean = sum / window_samples as f64;
    if mean <= 0.0 {
        return Ok(None);
    }

    let min_interval_s = intervals.iter().copied().fold(f64::INFINITY, f64::min);
    let max_interval_s = intervals.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let variance = intervals
        .iter()
        .copied()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / window_samples as f64;

    Ok(Some(RateSnapshot {
        average_rate_hz: 1.0 / mean.max(duration.as_secs_f64().min(mean)),
        min_interval_s,
        max_interval_s,
        stddev_interval_s: variance.sqrt(),
        window_samples,
    }))
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", byte);
    }
    out
}

impl AdminEntityOwners {
    fn endpoint_ids(&self) -> Vec<EndpointId> {
        let mut ids = Vec::new();
        ids.extend(self.peers.iter().filter_map(|id| EndpointId::new(id).ok()));
        ids.extend(
            self.clients
                .iter()
                .filter_map(|id| EndpointId::new(id).ok()),
        );
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        ids.dedup_by(|a, b| a.as_str() == b.as_str());
        ids
    }

    fn merge_from(&mut self, other: &Self) {
        self.peers.extend(other.peers.iter().cloned());
        self.clients.extend(other.clients.iter().cloned());
        self.routers.extend(other.routers.iter().cloned());
        self.peers.sort();
        self.peers.dedup();
        self.clients.sort();
        self.clients.dedup();
        self.routers.sort();
        self.routers.dedup();
    }
}

impl ZenohRouterOverview {
    fn merge_from(&mut self, other: &Self) {
        if self.version.is_none() {
            self.version = other.version.clone();
        }
        if self.session_count.is_none() {
            self.session_count = other.session_count;
        }
        self.metrics_available |= other.metrics_available;
        if self.metrics.is_none() {
            self.metrics = other.metrics.clone();
        }
        if self.raw_payload_utf8.is_empty() {
            self.raw_payload_utf8 = other.raw_payload_utf8.clone();
        }

        self.locators.extend(other.locators.iter().cloned());
        self.locators.sort();
        self.locators.dedup();

        self.plugin_names.extend(other.plugin_names.iter().cloned());
        self.plugin_names.sort();
        self.plugin_names.dedup();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use super::{
        admin_entities_with_telemetry_fallback, body_exposes_router_stats,
        build_node_runtime_breakdown, build_telemetry_node_index,
        choose_best_service_schema_candidate, compute_latency_snapshot,
        count_entities_by_endpoints, ctx_root_for_group, decode_context_entry, enum_label,
        exact_nodes_with_provenance, json_plugin_names, json_string_list,
        log_record_frame_from_preview, merge_node_id_sets, merge_unique_services,
        merge_unique_topics, parse_action_feedback_json, parse_action_goal_snapshot_json,
        parse_cancel_goal_result_json, parse_liveliness_key, parse_openmetrics_router_counters,
        parse_request_goal_json, parse_router_metrics, parse_router_metrics_zid,
        parse_router_root_key, parse_send_goal_result_json, parse_zenoh_router_overview,
        partition_concrete_suffixes, resolve_action_goal_id, resolve_nodes_for_entity,
        resolve_selected_node_endpoints, schema_hash_present, schemas_from_replies,
        select_schema_by_hash, select_telemetry_node_by_selector,
        summarize_service_runtime_metrics, summarize_topic_runtime_metrics, telemetry_node_detail,
        telemetry_service_names, telemetry_service_suffixes_from_names, telemetry_topic_names,
        telemetry_topic_suffixes_from_names, AdminEntityOwners, AdminNodeEntry, CandidateNodeRef,
        IntrospectClient, IntrospectOptions, LatencyAccumulator, NodeSelector, Provenance,
        QueryReplySample, TelemetryDiscoverySnapshot, TelemetryNodeEntry, TelemetrySnapshotCache,
        TopicPreviewDecodeStatus, TopicPreviewFrame, ZenohConnectivityEventKind,
        ZenohRouterMetrics, AORTA_LOG_LEVELS, FOXGLOVE_LOG_LEVELS, LEGACY_LOG_LEVELS,
    };
    use crate::ids::{EndpointId, GroupName, NodeId, NodeName, ServiceSuffix, TopicSuffix};
    use crate::schema::{
        ResolvedSchema, ResolvedServiceSchema, ResolvedTopicSchema, ResolvedTopicSchemaKind,
    };
    use aorta_core::telemetry::schema::{
        ResourceInventoryTelemetry, RuntimeMetricsTelemetry, TelemetryClientEntry,
        TelemetryExecutorRuntimeMetrics, TelemetryPublisherEntry, TelemetryPublisherRuntimeMetrics,
        TelemetryResourceCommon, TelemetryServiceEntry, TelemetryServiceRuntimeMetrics,
        TelemetrySubscriberEntry, TelemetrySubscriberRuntimeMetrics, TELEMETRY_SCHEMA_VERSION,
    };
    use serde_json::json;

    #[test]
    fn parse_action_feedback_json_lifts_well_known_fields() {
        // Reflection renders the action status enum as its SCREAMING_SNAKE name
        // and emits the action-specific `data` body as a nested object.
        let value = json!({
            "goal_id": "11111111-1111-1111-1111-111111111111",
            "seq": 7,
            "stamp_ns": 1_700_000_000_000_000_000u64,
            "status": "EXECUTING",
            "message": "halfway there",
            "data": { "progress": 0.5, "stage": "scan" },
        });
        let frame = parse_action_feedback_json(&value).expect("feedback frame");
        assert_eq!(frame.goal_id, "11111111-1111-1111-1111-111111111111");
        assert_eq!(frame.seq, 7);
        assert_eq!(frame.stamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(frame.status, "EXECUTING");
        assert_eq!(frame.message, "halfway there");
        assert_eq!(frame.data_json, "{\"progress\":0.5,\"stage\":\"scan\"}");
    }

    #[test]
    fn parse_action_feedback_json_defaults_missing_optionals() {
        let value = json!({
            "goal_id": "22222222-2222-2222-2222-222222222222",
            "status": "ACCEPTED",
        });
        let frame = parse_action_feedback_json(&value).expect("feedback frame");
        assert_eq!(frame.seq, 0);
        assert_eq!(frame.stamp_ns, 0);
        assert_eq!(frame.status, "ACCEPTED");
        assert_eq!(frame.message, "");
        assert_eq!(frame.data_json, "null");
    }

    #[test]
    fn parse_action_feedback_json_requires_goal_id() {
        let value = json!({ "seq": 1, "status": "EXECUTING" });
        assert!(parse_action_feedback_json(&value).is_err());
    }

    fn log_preview(decoded: Option<String>) -> TopicPreviewFrame {
        TopicPreviewFrame {
            topic: TopicSuffix::new("/log").expect("topic"),
            key_expr: "aorta/default/pub/log".to_string(),
            payload_hex: "00".to_string(),
            decoded_json: decoded,
            schema_hash: Some("abcd".to_string()),
            decode_status: TopicPreviewDecodeStatus::DecodedFromTopicSchema,
        }
    }

    #[test]
    fn log_record_frame_from_preview_lifts_foxglove_log_fields() {
        // `/log` decodes via the topic's schema_by_key; the foxglove.Log JSON
        // carries timestamp {sec,nsec}, level, message, name, plus the Aorta
        // extension fields aorta_level and target.
        let decoded = json!({
            "timestamp": { "sec": 1_700_000_000u64, "nsec": 123_456_789u64 },
            "level": "WARNING",
            "aorta_level": "WARN",
            "target": "aorta_core::pubsub",
            "message": "empty route, dropping",
            "name": "router-01",
            "file": "core/src/pubsub.rs",
            "line": 283,
        })
        .to_string();
        let frame = log_record_frame_from_preview(&log_preview(Some(decoded)));
        assert_eq!(frame.timestamp_ns, 1_700_000_000_123_456_789);
        assert_eq!(frame.level, "WARN");
        assert_eq!(frame.target, "aorta_core::pubsub");
        assert_eq!(frame.message, "empty route, dropping");
        assert_eq!(frame.node, "router-01");
    }

    #[test]
    fn log_record_frame_from_preview_keeps_trace_visible() {
        // foxglove.Log.level has to collapse TRACE onto DEBUG; aorta_level is
        // what keeps the two apart, so the TUI must read that field.
        let decoded = json!({
            "timestamp": { "sec": 1u64, "nsec": 0u64 },
            "level": "DEBUG",
            "aorta_level": "TRACE",
            "message": "enter",
            "name": "n",
        })
        .to_string();
        let frame = log_record_frame_from_preview(&log_preview(Some(decoded)));
        assert_eq!(frame.level, "TRACE");
    }

    #[test]
    fn log_record_frame_from_preview_still_decodes_legacy_records() {
        // A node built before the foxglove.Log switch, or an old MCAP capture:
        // timestamp_ns + node + the legacy level enum (0 == TRACE there, but
        // UNKNOWN in both current enums — hence the shape discriminator).
        let decoded = json!({
            "timestamp_ns": 1_700_000_000_000_000_000u64,
            "level": "WARN",
            "target": "aorta_core::pubsub",
            "message": "empty route, dropping",
            "node": "router-01",
        })
        .to_string();
        let frame = log_record_frame_from_preview(&log_preview(Some(decoded)));
        assert_eq!(frame.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(frame.level, "WARN");
        assert_eq!(frame.node, "router-01");
    }

    #[test]
    fn legacy_numeric_level_zero_is_trace_not_unknown() {
        let decoded = json!({ "timestamp_ns": 1u64, "level": 0 }).to_string();
        let frame = log_record_frame_from_preview(&log_preview(Some(decoded)));
        assert_eq!(frame.level, "TRACE");

        // Same numeric 0 on the current shape means UNKNOWN, not TRACE.
        let decoded = json!({ "timestamp": { "sec": 1u64 }, "aorta_level": 0 }).to_string();
        let frame = log_record_frame_from_preview(&log_preview(Some(decoded)));
        assert_eq!(frame.level, "UNKNOWN");
    }

    #[test]
    fn log_record_frame_from_preview_tolerates_raw_only() {
        // RawOnly (schema unavailable): no decoded JSON — every field defaults,
        // level renders as '?', so a schema-less record still shows up.
        let preview = TopicPreviewFrame {
            topic: TopicSuffix::new("/log").expect("topic"),
            key_expr: "aorta/default/pub/log".to_string(),
            payload_hex: "deadbeef".to_string(),
            decoded_json: None,
            schema_hash: None,
            decode_status: TopicPreviewDecodeStatus::RawOnly,
        };
        let frame = log_record_frame_from_preview(&preview);
        assert_eq!(frame.timestamp_ns, 0);
        assert_eq!(frame.level, "?");
        assert_eq!(frame.message, "");
    }

    #[test]
    fn enum_label_maps_strings_and_numbers() {
        assert_eq!(enum_label(&json!("INFO"), AORTA_LOG_LEVELS), "INFO");
        assert_eq!(enum_label(&json!(4), AORTA_LOG_LEVELS), "WARN");
        assert_eq!(enum_label(&json!(3), FOXGLOVE_LOG_LEVELS), "WARNING");
        assert_eq!(enum_label(&json!(3), LEGACY_LOG_LEVELS), "WARN");
        assert_eq!(enum_label(&json!(99), AORTA_LOG_LEVELS), "?");
    }

    async fn open_session_listen(endpoint: &str) -> zenoh::Session {
        let mut config = zenoh::Config::default();
        config
            .listen
            .endpoints
            .set(vec![endpoint.parse().expect("endpoint")])
            .expect("set listen endpoints");
        config
            .scouting
            .multicast
            .set_enabled(Some(false))
            .expect("disable multicast");
        zenoh::open(config).await.expect("open listen session")
    }

    async fn open_session_connect(endpoint: &str) -> zenoh::Session {
        let mut config = zenoh::Config::default();
        config
            .connect
            .endpoints
            .set(vec![endpoint.parse().expect("endpoint")])
            .expect("set connect endpoints");
        config
            .scouting
            .multicast
            .set_enabled(Some(false))
            .expect("disable multicast");
        zenoh::open(config).await.expect("open connect session")
    }

    async fn open_session_listen_local_tcp() -> (zenoh::Session, String) {
        let session = open_session_listen("tcp/127.0.0.1:0").await;
        let started = tokio::time::Instant::now();
        loop {
            let locators = session.info().locators().await;
            if let Some(locator) = locators
                .iter()
                .find(|loc| loc.to_string().starts_with("tcp/127.0.0.1"))
                .or_else(|| {
                    locators
                        .iter()
                        .find(|loc| loc.to_string().starts_with("tcp/"))
                })
            {
                return (session, locator.to_string());
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "listener tcp locator not found within 5s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_zenoh_connectivity_overview_returns_empty_snapshot_without_peers() {
        let mut config = zenoh::Config::default();
        config
            .scouting
            .multicast
            .set_enabled(Some(false))
            .expect("disable multicast");
        let session = Arc::new(zenoh::open(config).await.expect("open session"));
        let expected_zid = session.info().zid().await.to_string();
        let client = IntrospectClient::from_session(
            session.clone(),
            IntrospectOptions::new(GroupName::new("default").expect("group")),
        );

        let overview = client
            .fetch_zenoh_connectivity_overview()
            .await
            .expect("connectivity overview");

        assert_eq!(overview.session_zid, expected_zid);
        assert!(overview.transports.is_empty());
        assert!(overview.links.is_empty());

        session.close().await.expect("close session");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_zenoh_connectivity_overview_does_not_start_connectivity_event_listeners() {
        let mut config = zenoh::Config::default();
        config
            .scouting
            .multicast
            .set_enabled(Some(false))
            .expect("disable multicast");
        let session = Arc::new(zenoh::open(config).await.expect("open session"));
        let client = IntrospectClient::from_session(
            session.clone(),
            IntrospectOptions::new(GroupName::new("default").expect("group")),
        );

        client
            .fetch_zenoh_connectivity_overview()
            .await
            .expect("connectivity overview");

        let state = client.connectivity_event_listeners.lock().await;
        assert!(!state.initialized());

        session.close().await.expect("close session");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fetch_zenoh_connectivity_overview_reports_transport_and_link_for_connected_peer() {
        let (listener, endpoint) = open_session_listen_local_tcp().await;
        let listener = Arc::new(listener);
        let connector = Arc::new(open_session_connect(&endpoint).await);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let listener_client = IntrospectClient::from_session(
            listener.clone(),
            IntrospectOptions::new(GroupName::new("default").expect("group")),
        );
        let overview = listener_client
            .fetch_zenoh_connectivity_overview()
            .await
            .expect("connectivity overview");

        assert_eq!(overview.transports.len(), 1);
        assert_eq!(overview.links.len(), 1);

        let transport = &overview.transports[0];
        assert_eq!(
            transport.remote_zid,
            connector.info().zid().await.to_string()
        );
        assert!(!transport.remote_whatami.is_empty());

        let link = &overview.links[0];
        assert_eq!(link.transport_zid, transport.remote_zid);
        assert!(link.src.starts_with("tcp/"));
        assert!(link.dst.starts_with("tcp/"));
        assert!(link.mtu > 0);

        connector.close().await.expect("close connector");
        listener.close().await.expect("close listener");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fetch_recent_zenoh_connectivity_events_reports_live_transport_and_link_events() {
        let (listener, endpoint) = open_session_listen_local_tcp().await;
        let listener = Arc::new(listener);
        let listener_client = IntrospectClient::from_session(
            listener.clone(),
            IntrospectOptions::new(GroupName::new("default").expect("group")),
        );

        let initial = listener_client
            .fetch_recent_zenoh_connectivity_events(16)
            .await
            .expect("initial connectivity events");
        assert!(initial.is_empty());

        let connector = Arc::new(open_session_connect(&endpoint).await);
        let connector_zid = connector.info().zid().await.to_string();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

        let events = loop {
            let events = listener_client
                .fetch_recent_zenoh_connectivity_events(16)
                .await
                .expect("connectivity events");
            let has_transport_open = events.iter().any(|event| {
                event.kind == ZenohConnectivityEventKind::TransportOpened
                    && event
                        .transport
                        .as_ref()
                        .map(|transport| transport.remote_zid == connector_zid)
                        .unwrap_or(false)
            });
            let has_link_add = events.iter().any(|event| {
                event.kind == ZenohConnectivityEventKind::LinkAdded
                    && event
                        .link
                        .as_ref()
                        .map(|link| link.transport_zid == connector_zid)
                        .unwrap_or(false)
            });
            if has_transport_open && has_link_add {
                break events;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for live connectivity events"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert!(events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));

        connector.close().await.expect("close connector");
        listener.close().await.expect("close listener");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_undeclares_connectivity_event_listeners_and_closes_session() {
        let mut config = zenoh::Config::default();
        config
            .scouting
            .multicast
            .set_enabled(Some(false))
            .expect("disable multicast");
        let session = Arc::new(zenoh::open(config).await.expect("open session"));
        let client = IntrospectClient::from_session(
            session,
            IntrospectOptions::new(GroupName::new("default").expect("group")),
        );

        client
            .fetch_recent_zenoh_connectivity_events(16)
            .await
            .expect("start connectivity listeners");
        {
            let state = client.connectivity_event_listeners.lock().await;
            assert!(state.initialized());
        }

        client.shutdown().await.expect("shutdown client");

        let state = client.connectivity_event_listeners.lock().await;
        assert!(!state.initialized());
    }

    #[test]
    fn choose_best_service_schema_candidate_prefers_richer_root_metadata() {
        let best = choose_best_service_schema_candidate(
            vec![
                ResolvedServiceSchema::new("srv/demo/echo", vec![1], None, None),
                ResolvedServiceSchema::new(
                    "srv/demo/echo",
                    vec![2],
                    Some("demo.Request".to_string()),
                    None,
                ),
                ResolvedServiceSchema::new(
                    "srv/demo/echo",
                    vec![3],
                    Some("demo.Request".to_string()),
                    Some("demo.Response".to_string()),
                ),
            ]
            .into_iter(),
        )
        .expect("best");
        assert_eq!(best.request_root_type.as_deref(), Some("demo.Request"));
        assert_eq!(best.response_root_type.as_deref(), Some("demo.Response"));
    }

    #[test]
    fn describe_schema_rejects_service_selector_for_topic_schema() {
        let schema = ResolvedSchema::Topic(ResolvedTopicSchema {
            key_suffix: "pub/demo/topic".to_string(),
            kind: ResolvedTopicSchemaKind::Topic,
            schema_hash: "0".repeat(64),
            bfbs: Arc::from([1u8, 2u8, 3u8].as_slice()),
            default_root_type: Some("demo.Topic".to_string()),
        });
        let err =
            super::selected_root_type(&schema, &crate::schema::SchemaRootSelector::ServiceResponse)
                .expect_err("invalid selector");
        assert!(err.to_string().contains("invalid for topic schemas"));
    }

    #[test]
    fn parse_liveliness_key_extracts_name_and_id() {
        let group = GroupName::new("default").expect("group");
        let text = "aorta/default/sys/alive/talker/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
        let node = parse_liveliness_key(text, &group).expect("node");
        assert_eq!(node.name.as_str(), "talker");
        assert_eq!(node.id.as_str(), "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6");
    }

    #[test]
    fn count_entities_by_endpoints_counts_intersections() {
        let mut entities = BTreeMap::new();
        entities.insert(
            "topic_a".to_string(),
            AdminEntityOwners {
                peers: vec!["peer_1".to_string()],
                ..Default::default()
            },
        );
        entities.insert(
            "topic_b".to_string(),
            AdminEntityOwners {
                peers: vec!["peer_2".to_string()],
                ..Default::default()
            },
        );
        let endpoints = HashSet::from(["peer_1".to_string()]);
        assert_eq!(count_entities_by_endpoints(&entities, &endpoints), 1);
    }

    #[test]
    fn resolve_nodes_for_entity_returns_candidate_refs() {
        let owners = AdminEntityOwners {
            peers: vec!["peer_a".to_string()],
            ..Default::default()
        };
        let nodes = vec![AdminNodeEntry {
            name: NodeName::new("talker").expect("node name"),
            id: NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id"),
            group: GroupName::new("default").expect("group"),
            owners: AdminEntityOwners {
                peers: vec!["peer_a".to_string()],
                ..Default::default()
            },
        }];
        let resolved = resolve_nodes_for_entity(&owners, &nodes);
        assert_eq!(
            resolved,
            vec![CandidateNodeRef {
                name: NodeName::new("talker").expect("node name"),
                id: NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id"),
                matched_endpoint_ids: vec![EndpointId::new("peer_a").expect("endpoint")],
            }]
        );
    }

    #[test]
    fn exact_nodes_with_provenance_prefers_telemetry_owner_mapping() {
        let node_id = NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id");
        let telemetry = TelemetryDiscoverySnapshot {
            nodes: BTreeMap::from([(
                node_id.as_str().to_string(),
                TelemetryNodeEntry {
                    name: NodeName::new("talker").expect("node name"),
                    id: node_id.clone(),
                    group: GroupName::new("default").expect("group"),
                    inventory: None,
                    runtime: None,
                },
            )]),
            ..TelemetryDiscoverySnapshot::default()
        };
        let node_ids = std::collections::BTreeSet::from([node_id.as_str().to_string()]);

        let exact = exact_nodes_with_provenance(Some(&node_ids), Some(&telemetry), true);

        assert_eq!(exact.provenance, super::Provenance::Exact);
        assert_eq!(exact.value.len(), 1);
        assert_eq!(exact.value[0].name.as_str(), "talker");
        assert!(exact.value[0].matched_endpoint_ids.is_empty());
    }

    #[test]
    fn select_telemetry_node_by_selector_supports_name_and_id() {
        let node_id = NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id");
        let telemetry = TelemetryDiscoverySnapshot {
            nodes: BTreeMap::from([(
                node_id.as_str().to_string(),
                TelemetryNodeEntry {
                    name: NodeName::new("talker").expect("node name"),
                    id: node_id.clone(),
                    group: GroupName::new("default").expect("group"),
                    inventory: None,
                    runtime: None,
                },
            )]),
            ..TelemetryDiscoverySnapshot::default()
        };

        let by_name = select_telemetry_node_by_selector(
            &telemetry,
            &NodeSelector::ByName(NodeName::new("talker").expect("node name")),
            &GroupName::new("default").expect("group"),
        )
        .expect("by name");
        assert_eq!(by_name.id, node_id);

        let by_id = select_telemetry_node_by_selector(
            &telemetry,
            &NodeSelector::ById(NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id")),
            &GroupName::new("default").expect("group"),
        )
        .expect("by id");
        assert_eq!(by_id.name.as_str(), "talker");
    }

    #[test]
    fn telemetry_node_detail_returns_none_when_selected_node_is_missing_from_snapshot() {
        let telemetry = TelemetryDiscoverySnapshot {
            nodes: BTreeMap::from([(
                "11111111111111111111111111111111".to_string(),
                TelemetryNodeEntry {
                    name: NodeName::new("talker").expect("node name"),
                    id: NodeId::new("11111111111111111111111111111111").expect("node id"),
                    group: GroupName::new("default").expect("group"),
                    inventory: None,
                    runtime: None,
                },
            )]),
            ..TelemetryDiscoverySnapshot::default()
        };

        let detail = telemetry_node_detail(
            &telemetry,
            &NodeSelector::ById(NodeId::new("22222222222222222222222222222222").expect("node id")),
            &GroupName::new("default").expect("group"),
        )
        .expect("telemetry detail");

        assert!(detail.is_none());
    }

    #[test]
    fn build_telemetry_node_index_keeps_inventory_and_runtime_without_identity_payload() {
        let group = GroupName::new("default").expect("group");
        let node_id = "11111111111111111111111111111111";
        let inventory = ResourceInventoryTelemetry {
            telemetry_version: TELEMETRY_SCHEMA_VERSION,
            sequence: 1,
            timestamp_ns: 10,
            node_id: node_id.to_string(),
            group: "default".to_string(),
            inventory_revision: 2,
            publishers: vec![TelemetryPublisherEntry {
                common: TelemetryResourceCommon {
                    resource_name: "camera/front".to_string(),
                    resource_instance_id: "publisher:1".to_string(),
                    full_key: "aorta/default/pub/camera/front".to_string(),
                    schema_hash: None,
                    declared_at_ns: Some(1),
                    labels: BTreeMap::new(),
                },
            }],
            subscribers: Vec::new(),
            services: Vec::new(),
            clients: Vec::new(),
            overflow: Default::default(),
        };
        let runtime = RuntimeMetricsTelemetry {
            telemetry_version: TELEMETRY_SCHEMA_VERSION,
            sequence: 2,
            timestamp_ns: 20,
            node_id: node_id.to_string(),
            group: "default".to_string(),
            collection_period_ms: 1000,
            publishers: Vec::new(),
            subscribers: Vec::new(),
            services: Vec::new(),
            clients: Vec::new(),
            executor: None,
        };

        // A vital snapshot whose identity section is gated off (None) but which
        // still carries inventory, plus a separate bulk runtime stream.
        let nodes = build_telemetry_node_index(
            &group,
            vec![QueryReplySample {
                key_expr: "aorta/default/sys/telemetry/vital/snapshot/test".to_string(),
                payload: aorta_core::telemetry::fb::serialize_vital_snapshot(
                    TELEMETRY_SCHEMA_VERSION,
                    1,
                    10,
                    None,
                    Some(&inventory),
                    None,
                ),
            }],
            vec![QueryReplySample {
                key_expr: "aorta/default/sys/telemetry/bulk/runtime/test".to_string(),
                payload: aorta_core::telemetry::fb::serialize_runtime_metrics(&runtime),
            }],
        );

        let entry = nodes.get(node_id).expect("synthetic node entry");
        assert_eq!(entry.id.as_str(), node_id);
        assert_eq!(
            entry.name.as_str(),
            &format!("telemetry/fallback/id{node_id}")
        );
        assert!(entry.inventory.is_some());
        assert!(entry.runtime.is_some());
    }

    #[test]
    fn telemetry_snapshot_cache_returns_fresh_entry_within_ttl() {
        let mut cache = TelemetrySnapshotCache::default();
        let snapshot = TelemetryDiscoverySnapshot::default();
        cache.store(snapshot.clone());

        let cached = cache
            .get_fresh(Duration::from_millis(200))
            .expect("fresh cache entry");

        assert!(cached.is_empty());
    }

    #[test]
    fn telemetry_snapshot_cache_expires_immediately_with_zero_ttl() {
        let mut cache = TelemetrySnapshotCache::default();
        cache.store(TelemetryDiscoverySnapshot::default());

        assert!(cache.get_fresh(Duration::ZERO).is_none());
    }

    #[test]
    fn summarize_runtime_metrics_aggregates_node_level_totals() {
        let summary = super::summarize_runtime_metrics(&RuntimeMetricsTelemetry {
            telemetry_version: TELEMETRY_SCHEMA_VERSION,
            sequence: 7,
            timestamp_ns: 123,
            node_id: "node_a".to_string(),
            group: "default".to_string(),
            collection_period_ms: 1000,
            publishers: vec![TelemetryPublisherRuntimeMetrics {
                resource_name: "camera/front".to_string(),
                resource_instance_id: "publisher:1".to_string(),
                schema_hash: None,
                publish_shm_hit_total: 10,
                publish_shm_fallback_total: 2,
                publish_shm_required_fail_total: 1,
                loan_inflight_current: 3,
                loan_quota_exceeded_total: 4,
            }],
            subscribers: vec![
                aorta_core::telemetry::schema::TelemetrySubscriberRuntimeMetrics {
                    resource_name: "camera/front".to_string(),
                    resource_instance_id: "subscriber:1".to_string(),
                    schema_hash: None,
                    duplicate_drop_count: 1,
                    out_of_order_count: 2,
                    out_of_order_drop_count: 1,
                    executor_drop_count: 3,
                    local_drop_count: 4,
                    overflow_relay_enqueue_count: 5,
                    overflow_relay_drop_count: 6,
                    dropped_for_deadline: 7,
                },
            ],
            services: vec![TelemetryServiceRuntimeMetrics {
                resource_name: "nav/plan".to_string(),
                resource_instance_id: "service:1".to_string(),
                schema_hash: None,
                request_received_count: 11,
                request_dispatched_count: 10,
                reply_ok_count: 9,
                reply_error_count: 1,
                reply_send_failure_count: 2,
                request_dropped_count: 3,
                query_receive_failure_count: 0,
                task_exit_count: 0,
                in_flight_requests: 4,
                last_request_ns: 100,
                last_reply_ns: 101,
            }],
            clients: vec![
                aorta_core::telemetry::schema::TelemetryClientRuntimeMetrics {
                    resource_name: "map/load".to_string(),
                    resource_instance_id: "client:1".to_string(),
                    schema_hash: None,
                    call_count: 12,
                    success_count: 8,
                    timeout_count: 1,
                    concurrent_conflict_count: 2,
                    error_count: 3,
                    retry_count: 4,
                    last_call_started_ns: 200,
                    last_call_finished_ns: 201,
                    last_call_latency_ns: 5,
                },
            ],
            executor: Some(TelemetryExecutorRuntimeMetrics {
                queued_events: 5,
                queued_bytes: 6,
                peak_queued_events: 7,
                peak_queued_bytes: 8,
                dropped_queue_full: 9,
                dropped_queue_closed: 10,
                dropped_budget_events: 11,
                dropped_budget_bytes: 12,
                spin_count: 13,
                lane_occupancy: 14,
                lane_high_water: 15,
                lane_pushed_total: 16,
                dropped_expired: 17,
                offload_queue_depth: 18,
                ..Default::default()
            }),
        });

        assert_eq!(summary.timestamp_ns, 123);
        assert_eq!(summary.collection_period_ms, 1000);
        assert_eq!(summary.publisher_metric_count, 1);
        assert_eq!(summary.subscriber_metric_count, 1);
        assert_eq!(summary.service_metric_count, 1);
        assert_eq!(summary.client_metric_count, 1);
        assert_eq!(summary.publish_shm_hit_total, 10);
        assert_eq!(summary.subscriber_executor_drop_count, 3);
        assert_eq!(summary.service_request_received_count, 11);
        assert_eq!(summary.client_call_count, 12);
        assert_eq!(
            summary
                .executor
                .as_ref()
                .map(|executor| executor.queued_events),
            Some(5)
        );
        assert_eq!(
            summary
                .executor
                .as_ref()
                .map(|executor| executor.spin_count),
            Some(13)
        );
    }

    #[test]
    fn build_node_runtime_breakdown_preserves_per_resource_rows() {
        let breakdown = build_node_runtime_breakdown(&RuntimeMetricsTelemetry {
            telemetry_version: TELEMETRY_SCHEMA_VERSION,
            sequence: 7,
            timestamp_ns: 123,
            node_id: "node_a".to_string(),
            group: "default".to_string(),
            collection_period_ms: 1000,
            publishers: vec![TelemetryPublisherRuntimeMetrics {
                resource_name: "camera/front".to_string(),
                resource_instance_id: "publisher:1".to_string(),
                schema_hash: Some("abcd".repeat(16)),
                publish_shm_hit_total: 10,
                publish_shm_fallback_total: 2,
                publish_shm_required_fail_total: 1,
                loan_inflight_current: 3,
                loan_quota_exceeded_total: 4,
            }],
            subscribers: vec![TelemetrySubscriberRuntimeMetrics {
                resource_name: "camera/front".to_string(),
                resource_instance_id: "subscriber:1".to_string(),
                schema_hash: None,
                duplicate_drop_count: 1,
                out_of_order_count: 2,
                out_of_order_drop_count: 1,
                executor_drop_count: 3,
                local_drop_count: 4,
                overflow_relay_enqueue_count: 5,
                overflow_relay_drop_count: 6,
                dropped_for_deadline: 7,
            }],
            services: vec![TelemetryServiceRuntimeMetrics {
                resource_name: "nav/plan".to_string(),
                resource_instance_id: "service:1".to_string(),
                schema_hash: None,
                request_received_count: 11,
                request_dispatched_count: 10,
                reply_ok_count: 9,
                reply_error_count: 1,
                reply_send_failure_count: 2,
                request_dropped_count: 3,
                query_receive_failure_count: 0,
                task_exit_count: 0,
                in_flight_requests: 4,
                last_request_ns: 100,
                last_reply_ns: 101,
            }],
            clients: vec![
                aorta_core::telemetry::schema::TelemetryClientRuntimeMetrics {
                    resource_name: "nav/plan".to_string(),
                    resource_instance_id: "client:1".to_string(),
                    schema_hash: None,
                    call_count: 12,
                    success_count: 8,
                    timeout_count: 1,
                    concurrent_conflict_count: 2,
                    error_count: 3,
                    retry_count: 4,
                    last_call_started_ns: 200,
                    last_call_finished_ns: 201,
                    last_call_latency_ns: 5,
                },
            ],
            executor: None,
        });

        assert_eq!(breakdown.publishers.len(), 1);
        assert_eq!(breakdown.publishers[0].resource_name, "camera/front");
        assert_eq!(breakdown.publishers[0].publish_shm_hit_total, 10);
        assert_eq!(breakdown.subscribers.len(), 1);
        assert_eq!(breakdown.services.len(), 1);
        assert_eq!(breakdown.services[0].request_received_count, 11);
        assert_eq!(breakdown.clients.len(), 1);
        assert_eq!(breakdown.clients[0].retry_count, 4);
    }

    #[test]
    fn summarize_topic_runtime_metrics_aggregates_across_nodes() {
        let topic = TopicSuffix::new("camera/front").expect("topic");
        let snapshot = TelemetryDiscoverySnapshot {
            nodes: BTreeMap::from([
                (
                    "node_a".to_string(),
                    TelemetryNodeEntry {
                        name: NodeName::new("talker_a").expect("node"),
                        id: NodeId::new("11111111111111111111111111111111").expect("node id"),
                        group: GroupName::new("default").expect("group"),
                        inventory: None,
                        runtime: Some(RuntimeMetricsTelemetry {
                            telemetry_version: TELEMETRY_SCHEMA_VERSION,
                            sequence: 1,
                            timestamp_ns: 123,
                            node_id: "11111111111111111111111111111111".to_string(),
                            group: "default".to_string(),
                            collection_period_ms: 500,
                            publishers: vec![TelemetryPublisherRuntimeMetrics {
                                resource_name: "camera/front".to_string(),
                                resource_instance_id: "publisher:1".to_string(),
                                schema_hash: None,
                                publish_shm_hit_total: 10,
                                publish_shm_fallback_total: 2,
                                publish_shm_required_fail_total: 1,
                                loan_inflight_current: 3,
                                loan_quota_exceeded_total: 4,
                            }],
                            subscribers: vec![],
                            services: vec![],
                            clients: vec![],
                            executor: None,
                        }),
                    },
                ),
                (
                    "node_b".to_string(),
                    TelemetryNodeEntry {
                        name: NodeName::new("listener_b").expect("node"),
                        id: NodeId::new("22222222222222222222222222222222").expect("node id"),
                        group: GroupName::new("default").expect("group"),
                        inventory: None,
                        runtime: Some(RuntimeMetricsTelemetry {
                            telemetry_version: TELEMETRY_SCHEMA_VERSION,
                            sequence: 2,
                            timestamp_ns: 456,
                            node_id: "22222222222222222222222222222222".to_string(),
                            group: "default".to_string(),
                            collection_period_ms: 1000,
                            publishers: vec![],
                            subscribers: vec![TelemetrySubscriberRuntimeMetrics {
                                resource_name: "camera/front".to_string(),
                                resource_instance_id: "subscriber:1".to_string(),
                                schema_hash: None,
                                duplicate_drop_count: 1,
                                out_of_order_count: 2,
                                out_of_order_drop_count: 1,
                                executor_drop_count: 3,
                                local_drop_count: 4,
                                overflow_relay_enqueue_count: 5,
                                overflow_relay_drop_count: 6,
                                dropped_for_deadline: 7,
                            }],
                            services: vec![],
                            clients: vec![],
                            executor: None,
                        }),
                    },
                ),
            ]),
            ..TelemetryDiscoverySnapshot::default()
        };

        let summary = summarize_topic_runtime_metrics(&snapshot, &topic);

        assert_eq!(summary.provenance, Provenance::Exact);
        assert!(summary.note.is_none());
        let value = summary.value.expect("summary");
        assert_eq!(value.latest_timestamp_ns, 456);
        assert_eq!(value.collection_period_ms, 1000);
        assert_eq!(value.publisher_metric_count, 1);
        assert_eq!(value.subscriber_metric_count, 1);
        assert_eq!(value.publish_shm_hit_total, 10);
        assert_eq!(value.subscriber_overflow_relay_drop_count, 6);
    }

    #[test]
    fn summarize_topic_runtime_metrics_reports_unavailable_when_rows_absent() {
        let topic = TopicSuffix::new("camera/front").expect("topic");
        let summary =
            summarize_topic_runtime_metrics(&TelemetryDiscoverySnapshot::default(), &topic);

        assert_eq!(summary.provenance, Provenance::Unavailable);
        assert!(summary.value.is_none());
        assert_eq!(
            summary.note.as_deref(),
            Some("topic runtime metrics unavailable because telemetry rows are absent")
        );
    }

    #[test]
    fn summarize_service_runtime_metrics_aggregates_across_nodes() {
        let service = ServiceSuffix::new("nav/plan").expect("service");
        let snapshot = TelemetryDiscoverySnapshot {
            nodes: BTreeMap::from([
                (
                    "node_a".to_string(),
                    TelemetryNodeEntry {
                        name: NodeName::new("server_a").expect("node"),
                        id: NodeId::new("33333333333333333333333333333333").expect("node id"),
                        group: GroupName::new("default").expect("group"),
                        inventory: None,
                        runtime: Some(RuntimeMetricsTelemetry {
                            telemetry_version: TELEMETRY_SCHEMA_VERSION,
                            sequence: 1,
                            timestamp_ns: 123,
                            node_id: "33333333333333333333333333333333".to_string(),
                            group: "default".to_string(),
                            collection_period_ms: 500,
                            publishers: vec![],
                            subscribers: vec![],
                            services: vec![TelemetryServiceRuntimeMetrics {
                                resource_name: "nav/plan".to_string(),
                                resource_instance_id: "service:1".to_string(),
                                schema_hash: None,
                                request_received_count: 11,
                                request_dispatched_count: 10,
                                reply_ok_count: 9,
                                reply_error_count: 1,
                                reply_send_failure_count: 2,
                                request_dropped_count: 3,
                                query_receive_failure_count: 0,
                                task_exit_count: 0,
                                in_flight_requests: 4,
                                last_request_ns: 100,
                                last_reply_ns: 101,
                            }],
                            clients: vec![],
                            executor: None,
                        }),
                    },
                ),
                (
                    "node_b".to_string(),
                    TelemetryNodeEntry {
                        name: NodeName::new("client_b").expect("node"),
                        id: NodeId::new("44444444444444444444444444444444").expect("node id"),
                        group: GroupName::new("default").expect("group"),
                        inventory: None,
                        runtime: Some(RuntimeMetricsTelemetry {
                            telemetry_version: TELEMETRY_SCHEMA_VERSION,
                            sequence: 2,
                            timestamp_ns: 456,
                            node_id: "44444444444444444444444444444444".to_string(),
                            group: "default".to_string(),
                            collection_period_ms: 1000,
                            publishers: vec![],
                            subscribers: vec![],
                            services: vec![],
                            clients: vec![
                                aorta_core::telemetry::schema::TelemetryClientRuntimeMetrics {
                                    resource_name: "nav/plan".to_string(),
                                    resource_instance_id: "client:1".to_string(),
                                    schema_hash: None,
                                    call_count: 12,
                                    success_count: 8,
                                    timeout_count: 1,
                                    concurrent_conflict_count: 2,
                                    error_count: 3,
                                    retry_count: 4,
                                    last_call_started_ns: 200,
                                    last_call_finished_ns: 201,
                                    last_call_latency_ns: 5,
                                },
                            ],
                            executor: None,
                        }),
                    },
                ),
            ]),
            ..TelemetryDiscoverySnapshot::default()
        };

        let summary = summarize_service_runtime_metrics(&snapshot, &service);

        assert_eq!(summary.provenance, Provenance::Exact);
        assert!(summary.note.is_none());
        let value = summary.value.expect("summary");
        assert_eq!(value.latest_timestamp_ns, 456);
        assert_eq!(value.collection_period_ms, 1000);
        assert_eq!(value.service_metric_count, 1);
        assert_eq!(value.client_metric_count, 1);
        assert_eq!(value.service_request_received_count, 11);
        assert_eq!(value.service_reply_send_failure_count, 2);
        assert_eq!(value.client_call_count, 12);
        assert_eq!(value.client_retry_count, 4);
        // A2: latency = the latency of the most-recently-finished client call
        // (single client row here → its own latency); request/reply = max stamp.
        assert_eq!(value.client_last_call_latency_ns, 5);
        assert_eq!(value.service_last_request_ns, 100);
        assert_eq!(value.service_last_reply_ns, 101);
    }

    #[test]
    fn summarize_service_runtime_metrics_uses_latency_of_most_recent_call() {
        // Two client rows for the same service: an earlier-but-slower call and a
        // later-but-faster call. The summary must report the LATER call's latency
        // (by greatest last_call_finished_ns), not the max latency.
        let service = ServiceSuffix::new("nav/plan").expect("service");
        let make_client = |instance: &str,
                           finished_ns: u64,
                           latency_ns: u64|
         -> aorta_core::telemetry::schema::TelemetryClientRuntimeMetrics {
            aorta_core::telemetry::schema::TelemetryClientRuntimeMetrics {
                resource_name: "nav/plan".to_string(),
                resource_instance_id: instance.to_string(),
                schema_hash: None,
                call_count: 1,
                success_count: 1,
                timeout_count: 0,
                concurrent_conflict_count: 0,
                error_count: 0,
                retry_count: 0,
                last_call_started_ns: finished_ns.saturating_sub(latency_ns),
                last_call_finished_ns: finished_ns,
                last_call_latency_ns: latency_ns,
            }
        };
        let snapshot = TelemetryDiscoverySnapshot {
            nodes: BTreeMap::from([(
                "node_a".to_string(),
                TelemetryNodeEntry {
                    name: NodeName::new("client_a").expect("node"),
                    id: NodeId::new("55555555555555555555555555555555").expect("node id"),
                    group: GroupName::new("default").expect("group"),
                    inventory: None,
                    runtime: Some(RuntimeMetricsTelemetry {
                        telemetry_version: TELEMETRY_SCHEMA_VERSION,
                        sequence: 1,
                        timestamp_ns: 1_000,
                        node_id: "55555555555555555555555555555555".to_string(),
                        group: "default".to_string(),
                        collection_period_ms: 1000,
                        publishers: vec![],
                        subscribers: vec![],
                        services: vec![],
                        // earlier+slower (finished=100, latency=50), later+faster (finished=200, latency=10)
                        clients: vec![
                            make_client("client:1", 100, 50),
                            make_client("client:2", 200, 10),
                        ],
                        executor: None,
                    }),
                },
            )]),
            ..TelemetryDiscoverySnapshot::default()
        };

        let summary = summarize_service_runtime_metrics(&snapshot, &service)
            .value
            .expect("summary");
        assert_eq!(
            summary.client_last_call_latency_ns, 10,
            "must report the most-recent call's latency (10), not the max (50)"
        );
        assert_eq!(
            summary.client_last_call_finished_ns, 200,
            "finished-stamp must pair with the most-recent call (200), used as a poll dedup key"
        );
    }

    #[test]
    fn summarize_service_runtime_metrics_reports_unavailable_when_rows_absent() {
        let service = ServiceSuffix::new("nav/plan").expect("service");
        let summary =
            summarize_service_runtime_metrics(&TelemetryDiscoverySnapshot::default(), &service);

        assert_eq!(summary.provenance, Provenance::Unavailable);
        assert!(summary.value.is_none());
        assert_eq!(
            summary.note.as_deref(),
            Some("service runtime metrics unavailable because telemetry rows are absent")
        );
    }

    #[test]
    fn admin_entities_with_telemetry_fallback_keeps_telemetry_only_mode_alive() {
        let telemetry = TelemetryDiscoverySnapshot {
            publisher_nodes: BTreeMap::from([(
                "camera/front".to_string(),
                BTreeSet::from(["node-a".to_string()]),
            )]),
            ..TelemetryDiscoverySnapshot::default()
        };
        let err = crate::error::IntrospectError::new(
            crate::error::IntrospectErrorKind::Transport,
            "admin unavailable",
        );

        let entities =
            admin_entities_with_telemetry_fallback(Err(err), Some(&telemetry)).expect("fallback");

        assert!(entities.is_empty());
    }

    #[test]
    fn admin_entities_with_telemetry_fallback_preserves_error_without_telemetry() {
        let err = crate::error::IntrospectError::new(
            crate::error::IntrospectErrorKind::Transport,
            "admin unavailable",
        );

        let returned =
            admin_entities_with_telemetry_fallback(Err(err), None).expect_err("should fail");

        assert_eq!(returned.kind, crate::error::IntrospectErrorKind::Transport);
        assert!(returned.to_string().contains("admin unavailable"));
    }

    #[test]
    fn parse_action_goal_snapshot_json_accepts_string_u64_fields() {
        let value = json!({
            "goal_id": "goal-1",
            "status": "SUCCEEDED",
            "code": "OK",
            "accepted_stamp_ns": "11",
            "started_stamp_ns": "12",
            "finished_stamp_ns": "13",
            "last_feedback_seq": "44",
            "server_epoch": "epoch-1"
        });

        let snapshot = parse_action_goal_snapshot_json(&value).expect("snapshot");

        assert_eq!(snapshot.accepted_stamp_ns, 11);
        assert_eq!(snapshot.started_stamp_ns, 12);
        assert_eq!(snapshot.finished_stamp_ns, 13);
        assert_eq!(snapshot.last_feedback_seq, 44);
    }

    #[test]
    fn parse_send_goal_result_json_extracts_goal_id_and_secret() {
        let value = json!({
            "accepted": true,
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "goal_secret": "secret-token",
            "status": "ACCEPTED",
            "code": "OK",
            "message": "",
            "server_epoch": "epoch-1"
        });

        let result = parse_send_goal_result_json(&value).expect("result");

        assert!(result.accepted);
        assert_eq!(result.goal_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(result.goal_secret, "secret-token");
        assert_eq!(result.status, aorta_core::action::ActionStatus::Accepted);
        assert_eq!(result.code, aorta_core::action::ActionReplyCode::Ok);
        assert_eq!(result.server_epoch, "epoch-1");
    }

    #[test]
    fn parse_send_goal_result_json_rejects_out_of_range_status_and_code() {
        // Checked conversion: an out-of-range numeric status/code must
        // error, not truncate into a valid enum.
        let base = json!({
            "accepted": true,
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "goal_secret": "secret",
            "status": 9999,
            "code": "OK",
            "message": "",
            "server_epoch": "e"
        });
        assert!(
            parse_send_goal_result_json(&base).is_err(),
            "status 9999 must error"
        );

        let mut bad_code = base.clone();
        bad_code["status"] = json!("ACCEPTED");
        bad_code["code"] = json!(99999);
        assert!(
            parse_send_goal_result_json(&bad_code).is_err(),
            "code 99999 must error"
        );
    }

    #[test]
    fn parse_send_goal_result_json_requires_message_and_server_epoch() {
        // Reflection emits every fbs field, so a missing field = malformed reply.
        let value = json!({
            "accepted": true,
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "goal_secret": "secret",
            "status": "ACCEPTED",
            "code": "OK"
            // message + server_epoch deliberately absent
        });
        assert!(parse_send_goal_result_json(&value).is_err());
    }

    #[test]
    fn parse_send_goal_result_json_requires_goal_secret() {
        let value = json!({
            "accepted": false,
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "ACCEPTED",
            "code": "BUSY"
        });

        // Missing `goal_secret` is a hard decode error (the caller needs it).
        assert!(parse_send_goal_result_json(&value).is_err());
    }

    #[test]
    fn parse_cancel_goal_result_json_extracts_status_and_message() {
        let value = json!({
            "accepted": true,
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "CANCELING",
            "code": "CANCELED",
            "message": "cancel requested"
        });

        let result = parse_cancel_goal_result_json(&value).expect("result");

        assert!(result.accepted);
        assert_eq!(result.goal_id, "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(result.status, aorta_core::action::ActionStatus::Canceling);
        assert_eq!(result.code, aorta_core::action::ActionReplyCode::Canceled);
        assert_eq!(result.message, "cancel requested");
    }

    #[test]
    fn resolve_action_goal_id_validates_or_mints_uuid() {
        // A valid UUID passes through unchanged.
        let provided = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            resolve_action_goal_id(Some(provided)).expect("goal_id"),
            provided
        );

        // A non-UUID goal_id is rejected (matches the server's goal_id_to_key).
        assert!(resolve_action_goal_id(Some("not-a-uuid")).is_err());

        // `None` mints a fresh hyphenated v4 UUID.
        let minted = resolve_action_goal_id(None).expect("minted goal_id");
        assert_eq!(minted.len(), 36);
        assert!(minted.contains('-'));
    }

    #[test]
    fn parse_request_goal_json_treats_blank_as_empty_object() {
        assert_eq!(parse_request_goal_json("   ").expect("blank"), json!({}));
        assert_eq!(
            parse_request_goal_json("{\"source\":\"quick\"}").expect("body"),
            json!({"source": "quick"})
        );
        assert!(parse_request_goal_json("{not json").is_err());
    }

    #[test]
    fn merge_node_id_sets_unions_role_specific_owner_sets() {
        let left = BTreeSet::from(["node_a".to_string(), "node_b".to_string()]);
        let right = BTreeSet::from(["node_b".to_string(), "node_c".to_string()]);

        let merged = merge_node_id_sets(Some(&left), Some(&right)).expect("merged");

        assert_eq!(
            merged,
            BTreeSet::from([
                "node_a".to_string(),
                "node_b".to_string(),
                "node_c".to_string()
            ])
        );
    }

    #[test]
    fn telemetry_inventory_helpers_merge_pub_sub_and_service_client_names() {
        let inventory = ResourceInventoryTelemetry {
            telemetry_version: TELEMETRY_SCHEMA_VERSION,
            sequence: 1,
            timestamp_ns: 1,
            node_id: "node".to_string(),
            group: "default".to_string(),
            inventory_revision: 2,
            publishers: vec![TelemetryPublisherEntry {
                common: TelemetryResourceCommon {
                    resource_name: "camera/front".to_string(),
                    ..Default::default()
                },
            }],
            subscribers: vec![
                TelemetrySubscriberEntry {
                    common: TelemetryResourceCommon {
                        resource_name: "camera/front".to_string(),
                        ..Default::default()
                    },
                },
                TelemetrySubscriberEntry {
                    common: TelemetryResourceCommon {
                        resource_name: "imu/data".to_string(),
                        ..Default::default()
                    },
                },
            ],
            services: vec![TelemetryServiceEntry {
                common: TelemetryResourceCommon {
                    resource_name: "nav/plan".to_string(),
                    ..Default::default()
                },
                request_root_type: None,
                response_root_type: None,
            }],
            clients: vec![
                TelemetryClientEntry {
                    common: TelemetryResourceCommon {
                        resource_name: "nav/plan".to_string(),
                        ..Default::default()
                    },
                    request_root_type: None,
                    response_root_type: None,
                },
                TelemetryClientEntry {
                    common: TelemetryResourceCommon {
                        resource_name: "map/load".to_string(),
                        ..Default::default()
                    },
                    request_root_type: None,
                    response_root_type: None,
                },
            ],
            overflow: Default::default(),
        };

        assert_eq!(
            telemetry_topic_names(&inventory),
            std::collections::BTreeSet::from(["camera/front", "imu/data"])
        );
        assert_eq!(
            telemetry_service_names(&inventory),
            std::collections::BTreeSet::from(["map/load", "nav/plan"])
        );
    }

    #[test]
    fn telemetry_suffix_helpers_deduplicate_and_merge_inventory_roles() {
        let published =
            telemetry_topic_suffixes_from_names(["camera/front", "camera/front"].into_iter())
                .expect("topics");
        let subscribed =
            telemetry_topic_suffixes_from_names(["imu/data"].into_iter()).expect("topics");
        let provided =
            telemetry_service_suffixes_from_names(["nav/plan"].into_iter()).expect("services");
        let clients = telemetry_service_suffixes_from_names(["nav/plan", "map/load"].into_iter())
            .expect("services");

        let merged_topics = merge_unique_topics(&published, &subscribed);
        let merged_services = merge_unique_services(&provided, &clients);

        assert_eq!(
            merged_topics
                .iter()
                .map(|topic| topic.as_str())
                .collect::<Vec<_>>(),
            vec!["camera/front", "imu/data"]
        );
        assert_eq!(
            merged_services
                .iter()
                .map(|service| service.as_str())
                .collect::<Vec<_>>(),
            vec!["map/load", "nav/plan"]
        );
    }

    #[test]
    fn telemetry_suffix_helpers_tolerate_wildcard_subscriber_patterns() {
        // A node with a wildcard subscriber (a `*/task_report`
        // fan-in) must not abort node-info resource listing. The pattern is a
        // real resource of that node, so it is kept and marked as a pattern.
        let subscribed =
            telemetry_topic_suffixes_from_names(["*/task_report", "imu/data"].into_iter())
                .expect("wildcard subscriber must be tolerated in node info");
        assert_eq!(
            subscribed.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
            vec!["*/task_report", "imu/data"]
        );
        assert!(subscribed.iter().any(|t| t.is_pattern()));

        // merge_unique_* re-validates and must not panic on wildcard suffixes.
        let merged = merge_unique_topics(&subscribed, &[]);
        assert!(merged.iter().any(|t| t.is_pattern()));

        let clients = telemetry_service_suffixes_from_names(["**"].into_iter())
            .expect("wildcard client must be tolerated in node info");
        assert!(clients[0].is_pattern());
        assert_eq!(merge_unique_services(&clients, &[]).len(), 1);
    }

    #[test]
    fn partition_concrete_suffixes_skips_wildcard_patterns_without_aborting() {
        // The aggregate topic/service listing unions admin keys and
        // telemetry resource names. A single wildcard subscriber/client pattern
        // must be dropped, not `?`-fail the whole listing.
        let all = BTreeSet::from([
            "camera/front".to_string(),
            "*/task_report".to_string(),
            "**".to_string(),
            "imu/data".to_string(),
        ]);
        let concrete = partition_concrete_suffixes(
            all,
            |suffix| TopicSuffix::new_subscribe_side(suffix),
            TopicSuffix::is_pattern,
        )
        .expect("wildcard subscriber must not abort the aggregate listing");
        assert_eq!(
            concrete
                .iter()
                .map(|(raw, topic)| {
                    assert_eq!(raw, topic.as_str());
                    topic.as_str()
                })
                .collect::<Vec<_>>(),
            vec!["camera/front", "imu/data"]
        );
    }

    #[test]
    fn resolve_selected_node_endpoints_returns_matching_endpoint_ids() {
        let mut entities = BTreeMap::new();
        entities.insert(
            "demo/topic".to_string(),
            AdminEntityOwners {
                peers: vec!["peer_a".to_string()],
                ..Default::default()
            },
        );
        let nodes = vec![AdminNodeEntry {
            name: NodeName::new("talker").expect("node name"),
            id: NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id"),
            group: GroupName::new("default").expect("group"),
            owners: AdminEntityOwners {
                peers: vec!["peer_a".to_string(), "peer_b".to_string()],
                ..Default::default()
            },
        }];

        let selected = resolve_selected_node_endpoints(
            &entities,
            &Ok(nodes),
            &NodeSelector::ByName(NodeName::new("talker").expect("node name")),
            &GroupName::new("default").expect("group"),
            "topic",
        )
        .expect("selected endpoints");

        assert_eq!(
            selected,
            HashSet::from(["peer_a".to_string(), "peer_b".to_string()])
        );
    }

    #[test]
    fn resolve_selected_node_endpoints_rejects_when_admin_ownership_is_missing() {
        let entities = BTreeMap::from([("demo/topic".to_string(), AdminEntityOwners::default())]);
        let nodes = vec![AdminNodeEntry {
            name: NodeName::new("talker").expect("node name"),
            id: NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id"),
            group: GroupName::new("default").expect("group"),
            owners: AdminEntityOwners {
                peers: vec!["peer_a".to_string()],
                ..Default::default()
            },
        }];

        let err = resolve_selected_node_endpoints(
            &entities,
            &Ok(nodes),
            &NodeSelector::ByName(NodeName::new("talker").expect("node name")),
            &GroupName::new("default").expect("group"),
            "topic",
        )
        .expect_err("missing admin ownership should fail");

        assert_eq!(
            err.kind,
            crate::error::IntrospectErrorKind::CapabilityMissing
        );
        assert!(err.to_string().contains("admin ownership"));
    }

    #[test]
    fn parse_router_root_key_extracts_zid_and_whatami() {
        let parsed = parse_router_root_key("@/abcd1234/router").expect("router root");
        assert_eq!(parsed.0, "abcd1234");
        assert_eq!(parsed.1, "router");
    }

    #[test]
    fn parse_router_metrics_zid_extracts_router_zid() {
        let zid = parse_router_metrics_zid(&QueryReplySample {
            key_expr: "@/abcd1234/router/metrics".to_string(),
            payload: br#"{"tx":1}"#.to_vec(),
        })
        .expect("metrics key");
        assert_eq!(zid, "abcd1234");
    }

    /// A representative OpenMetrics body shaped like `prometheus_client` 0.24.x
    /// actually renders the pinned zenoh fork's `stats` registry: prefix `zenoh`,
    /// registry labels `local_id`/`local_whatami`; unit counters carry the `_bytes`
    /// unit so the **descriptor** is `zenoh_tx_bytes` while the **sample** is
    /// `zenoh_tx_bytes_total` (`# TYPE zenoh_tx_bytes counter` / `# UNIT … bytes`).
    /// Bytes are label-split by `protocol`; network messages by the real
    /// `priority`/`message`/`shm`/`protocol` set (`zenoh-stats/src/labels.rs`).
    /// Includes `_created`/histogram/build-info noise lines that must be ignored.
    const SAMPLE_OPENMETRICS: &str = "\
# HELP zenoh_tx_bytes Count of transport messages bytes sent.
# TYPE zenoh_tx_bytes counter
# UNIT zenoh_tx_bytes bytes
zenoh_tx_bytes_total{local_id=\"abcd\",local_whatami=\"router\",protocol=\"Tcp\"} 1000
zenoh_tx_bytes_total{local_id=\"abcd\",local_whatami=\"router\",protocol=\"Udp\"} 234
zenoh_tx_bytes_created{local_id=\"abcd\",local_whatami=\"router\",protocol=\"Tcp\"} 1.62e9
# TYPE zenoh_rx_bytes counter
# UNIT zenoh_rx_bytes bytes
zenoh_rx_bytes_total{local_id=\"abcd\",local_whatami=\"router\",protocol=\"Tcp\"} 678
# TYPE zenoh_tx_network_message counter
zenoh_tx_network_message_total{local_id=\"abcd\",priority=\"Data\",message=\"Put\",shm=\"false\",protocol=\"Tcp\"} 10
zenoh_tx_network_message_total{local_id=\"abcd\",priority=\"Control\",message=\"Declare\",shm=\"false\",protocol=\"Tcp\"} 5
# TYPE zenoh_rx_network_message counter
zenoh_rx_network_message_total{local_id=\"abcd\",priority=\"Data\",message=\"Reply\",shm=\"false\",protocol=\"Tcp\"} 7
# TYPE zenoh_tx_network_message_payload_bytes histogram
zenoh_tx_network_message_payload_bytes_count{local_id=\"abcd\"} 99
zenoh_tx_network_message_payload_bytes_sum{local_id=\"abcd\"} 4096
# TYPE zenoh_transports_opened gauge
zenoh_transports_opened{local_id=\"abcd\",local_whatami=\"router\"} 3
# TYPE zenoh_build info
zenoh_build_info{local_id=\"abcd\",version=\"1.7.2\"} 1
# EOF
";

    /// A `stats`-enabled router with **zero traffic**: descriptors are present but
    /// no counter sample lines yet. Must still read as available (counters all 0),
    /// not as `metrics: unavailable`.
    const SAMPLE_OPENMETRICS_ZERO_TRAFFIC: &str = "\
# TYPE zenoh_tx_bytes counter
# UNIT zenoh_tx_bytes bytes
# TYPE zenoh_rx_bytes counter
# UNIT zenoh_rx_bytes bytes
# TYPE zenoh_build info
zenoh_build_info{local_id=\"abcd\",version=\"1.7.2\"} 1
# EOF
";

    #[test]
    fn parse_openmetrics_sums_counters_across_label_lines() {
        let metrics = parse_openmetrics_router_counters(SAMPLE_OPENMETRICS);
        assert_eq!(
            metrics.tx_bytes_total, 1234,
            "tx bytes summed across protocols"
        );
        assert_eq!(metrics.rx_bytes_total, 678);
        assert_eq!(
            metrics.tx_msgs_total, 15,
            "tx msgs summed across message/space"
        );
        assert_eq!(metrics.rx_msgs_total, 7);
        assert_eq!(metrics.transports_opened, Some(3));
        assert_eq!(metrics.links_opened, None);
    }

    #[test]
    fn parse_router_metrics_returns_zid_and_counters() {
        let (zid, metrics) = parse_router_metrics(&QueryReplySample {
            key_expr: "@/abcd/router/metrics".to_string(),
            payload: SAMPLE_OPENMETRICS.as_bytes().to_vec(),
        })
        .expect("metrics parsed");
        assert_eq!(zid, "abcd");
        assert_eq!(metrics.tx_bytes_total, 1234);
        assert_eq!(metrics.rx_msgs_total, 7);
    }

    #[test]
    fn parse_router_metrics_rejects_gzip_body() {
        // Router ignored `?compression=false`: body still gzip (magic 0x1f 0x8b).
        let gzip = vec![0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert!(parse_router_metrics(&QueryReplySample {
            key_expr: "@/abcd/router/metrics".to_string(),
            payload: gzip,
        })
        .is_none());
    }

    #[test]
    fn parse_router_metrics_rejects_non_metrics_key() {
        assert!(parse_router_metrics(&QueryReplySample {
            key_expr: "@/abcd/router".to_string(),
            payload: SAMPLE_OPENMETRICS.as_bytes().to_vec(),
        })
        .is_none());
    }

    #[test]
    fn parse_router_metrics_build_info_only_reads_as_unavailable() {
        // A router built WITHOUT the `stats` feature still answers the metrics
        // endpoint, but with only a `zenoh_build_info` line. That must read as
        // "metrics unavailable" (None) — NOT all-zero counters — so the CLI/TUI
        // show "unavailable" instead of a misleading 0 B / 0 msgs readout.
        let body = "\
# TYPE zenoh_build info
zenoh_build_info{local_id=\"abcd\",version=\"1.7.2\"} 1
# EOF
";
        assert!(!body_exposes_router_stats(body));
        assert!(parse_router_metrics(&QueryReplySample {
            key_expr: "@/abcd/router/metrics".to_string(),
            payload: body.as_bytes().to_vec(),
        })
        .is_none());
    }

    #[test]
    fn parse_router_metrics_zero_traffic_stats_router_is_available() {
        // `stats` enabled but no traffic yet: descriptors present, no sample
        // lines. Must read as available with all-zero counters (Some), distinct
        // from the no-stats case above.
        assert!(body_exposes_router_stats(SAMPLE_OPENMETRICS_ZERO_TRAFFIC));
        let (zid, metrics) = parse_router_metrics(&QueryReplySample {
            key_expr: "@/abcd/router/metrics".to_string(),
            payload: SAMPLE_OPENMETRICS_ZERO_TRAFFIC.as_bytes().to_vec(),
        })
        .expect("stats-enabled router is available even at zero traffic");
        assert_eq!(zid, "abcd");
        assert_eq!(metrics, ZenohRouterMetrics::default());
    }

    #[test]
    fn parse_openmetrics_handles_unlabeled_lines() {
        let body = "zenoh_tx_bytes_total 42\nzenoh_links_opened_total 9\n";
        let metrics = parse_openmetrics_router_counters(body);
        assert_eq!(metrics.tx_bytes_total, 42);
        assert_eq!(metrics.links_opened, Some(9));
    }

    #[test]
    fn schemas_from_replies_decodes_dedups_and_sorts() {
        use crate::schema::ResolvedSchema;
        use aorta_core::schema::SchemaHash;
        use aorta_core::schema_registry::{SchemaByKeyValue, ServiceSchemaByKeyValue};

        let prefix = "aorta/default/sys/schema_by_key/";
        let topic_bfbs = vec![1u8, 2, 3, 4];
        let topic_hash = SchemaHash::from_bfbs_bytes(&topic_bfbs);
        let topic_payload = SchemaByKeyValue::topic(topic_hash, topic_bfbs, None).encode();
        let service_bfbs = vec![5u8, 6, 7];
        let service_hash = SchemaHash::from_bfbs_bytes(&service_bfbs);
        let service_payload =
            ServiceSchemaByKeyValue::new(service_hash, service_bfbs, None, None).encode();

        let replies = vec![
            QueryReplySample {
                key_expr: format!("{}pub/imu", prefix),
                payload: topic_payload.clone(),
            },
            QueryReplySample {
                key_expr: format!("{}srv/echo", prefix),
                payload: service_payload,
            },
            // Duplicate provider of pub/imu → collapsed by dedup.
            QueryReplySample {
                key_expr: format!("{}pub/imu", prefix),
                payload: topic_payload,
            },
            // Outside the prefix → ignored.
            QueryReplySample {
                key_expr: "aorta/default/other/x".to_string(),
                payload: vec![9],
            },
        ];

        let schemas = schemas_from_replies(prefix, &replies);
        assert_eq!(schemas.len(), 2, "deduped to two distinct suffixes");
        // Sorted by suffix: pub/imu (topic) before srv/echo (service).
        match &schemas[0] {
            ResolvedSchema::Topic(topic) => assert_eq!(topic.key_suffix, "pub/imu"),
            other => panic!("expected topic first, got {:?}", other),
        }
        match &schemas[1] {
            ResolvedSchema::Service(service) => assert_eq!(service.key_suffix, "srv/echo"),
            other => panic!("expected service second, got {:?}", other),
        }
    }

    #[test]
    fn select_schema_by_hash_returns_only_matching_payload() {
        use aorta_core::schema::SchemaHash;
        let bfbs = vec![1u8, 2, 3, 4];
        let hash = SchemaHash::from_bfbs_bytes(&bfbs);
        let replies = vec![
            QueryReplySample {
                key_expr: "x".to_string(),
                payload: vec![9, 9, 9], // hashes to something else
            },
            QueryReplySample {
                key_expr: "y".to_string(),
                payload: bfbs.clone(),
            },
        ];
        assert_eq!(select_schema_by_hash(hash, &replies), Some(bfbs));

        // No reply hashes to an unrelated key → None.
        let unrelated = SchemaHash::from_bfbs_bytes(&[42u8]);
        assert_eq!(select_schema_by_hash(unrelated, &replies[..1]), None);
    }

    #[test]
    fn parse_zenoh_router_overview_reads_root_payload_fields() {
        let overview = parse_zenoh_router_overview(&QueryReplySample {
            key_expr: "@/router_zid/router".to_string(),
            payload: br#"{
                "version":"1.7.2",
                "locators":["tcp/127.0.0.1:7447"],
                "sessions":[{"peer":"one"},{"peer":"two"}],
                "plugins":{"rest":{},"storage_manager":{}}
            }"#
            .to_vec(),
        })
        .expect("overview");

        assert_eq!(overview.zid, "router_zid");
        assert_eq!(overview.whatami, "router");
        assert_eq!(overview.version.as_deref(), Some("1.7.2"));
        assert_eq!(overview.locators, vec!["tcp/127.0.0.1:7447"]);
        assert_eq!(overview.session_count, Some(2));
        assert_eq!(overview.plugin_names, vec!["rest", "storage_manager"]);
        assert!(!overview.metrics_available);
    }

    #[test]
    fn json_helpers_accept_array_and_object_shapes() {
        assert_eq!(
            json_string_list(&json!(["tcp/1", "tcp/2"])),
            vec!["tcp/1", "tcp/2"]
        );
        assert_eq!(
            json_plugin_names(&json!([
                "rest",
                {"name":"storage_manager"},
                {"ignored":"value"}
            ])),
            vec!["rest", "storage_manager"]
        );
        assert_eq!(
            json_plugin_names(&json!({"rest":{},"storage_manager":{}})),
            vec!["rest", "storage_manager"]
        );
    }

    fn context_reply(key: &str, payload: Vec<u8>) -> QueryReplySample {
        QueryReplySample {
            key_expr: key.to_string(),
            payload,
        }
    }

    fn context_meta(
        source_node: &str,
        source_instance_id: &str,
        expires_at_ns: u64,
    ) -> aorta_core::contextstore_fb::ContextValueMeta {
        aorta_core::contextstore_fb::ContextValueMeta {
            stamp_ns: 1,
            expires_at_ns,
            source_node: source_node.to_string(),
            source_instance_id: source_instance_id.to_string(),
            sequence: 0,
        }
    }

    #[test]
    fn ctx_root_for_group_matches_cli_selector_shape() {
        assert_eq!(ctx_root_for_group("default", None), "aorta/default/ctx/**");
        assert_eq!(
            ctx_root_for_group("robot", Some("demo/state")),
            "aorta/robot/ctx/demo/state"
        );
        // Leading/trailing slashes are trimmed exactly like the CLI helper.
        assert_eq!(
            ctx_root_for_group("robot", Some("/demo/**/")),
            "aorta/robot/ctx/demo/**"
        );
    }

    #[test]
    fn decode_context_entry_decodes_string_value_and_metadata() {
        use aorta_core::contextstore_fb::encode_value_string;
        let payload =
            encode_value_string("running", &context_meta("demo_node", "instance-aaaa", 0));
        let entry = decode_context_entry(&context_reply("aorta/default/ctx/demo/state", payload))
            .expect("string context value decodes");
        assert_eq!(entry.key, "aorta/default/ctx/demo/state");
        assert_eq!(entry.kind, "String");
        assert_eq!(entry.value_text, "running");
        assert_eq!(entry.source_node, "demo_node");
        assert_eq!(entry.source_instance_id, "instance-aaaa");
        assert_eq!(entry.expires_at_ns, 0);
    }

    #[test]
    fn decode_context_entry_renders_numeric_kinds() {
        use aorta_core::contextstore_fb::encode_value_i64;
        let payload = encode_value_i64(-7, &context_meta("planner", "instance-bbbb", 42));
        let entry = decode_context_entry(&context_reply("aorta/default/ctx/plan/goal", payload))
            .expect("i64 context value decodes");
        assert_eq!(entry.kind, "I64");
        assert_eq!(entry.value_text, "-7");
        assert_eq!(entry.expires_at_ns, 42);
    }

    #[test]
    fn decode_context_entry_skips_malformed_payload() {
        assert!(decode_context_entry(&context_reply(
            "aorta/default/ctx/bad",
            vec![0x00, 0x01, 0x02]
        ))
        .is_none());
    }

    #[test]
    fn compute_latency_snapshot_empty_is_none() {
        assert!(compute_latency_snapshot(&[]).is_none());
    }

    #[test]
    fn compute_latency_snapshot_uniform_load_lands_in_expected_bucket() {
        // 100 identical 1000ns samples: every percentile is bucket-bounded to the
        // upper bound of bucket 10 (`[512, 1024)` → 1023), mirroring the core
        // `percentiles_on_uniform_load_land_in_expected_bucket` expectation. count
        // and max are exact; mean is the arithmetic mean of the samples.
        let samples = vec![1_000u64; 100];
        let snapshot = compute_latency_snapshot(&samples).expect("snapshot");
        assert_eq!(snapshot.count, 100);
        assert_eq!(
            snapshot.p50_ns, 1023,
            "bucket-bounded p50 of 1000ns is 1023"
        );
        assert_eq!(snapshot.p95_ns, 1023);
        assert_eq!(snapshot.p99_ns, 1023);
        assert_eq!(
            snapshot.max_ns, 1_000,
            "max is tracked exactly, not bucketed"
        );
        assert_eq!(snapshot.mean_ns, 1_000);
    }

    #[test]
    fn compute_latency_snapshot_spread_is_monotonic_and_bucket_bounded() {
        // A low-skewed spread: 90 small (bucket 9 → ub 511), 9 medium
        // (bucket 16 → ub 65535), 1 large. Mirrors the core
        // `percentiles_are_monotonic_and_ordered_across_a_spread` test.
        let mut samples = vec![500u64; 90];
        samples.extend(std::iter::repeat_n(50_000u64, 9));
        samples.push(5_000_000u64);
        let snapshot = compute_latency_snapshot(&samples).expect("snapshot");
        assert_eq!(snapshot.count, 100);
        assert!(snapshot.p50_ns <= snapshot.p95_ns);
        assert!(snapshot.p95_ns <= snapshot.p99_ns);
        assert_eq!(snapshot.p50_ns, 511, "90% are at 500ns → bucket 9 ub");
        assert!(snapshot.p99_ns >= 65_535);
        assert_eq!(snapshot.max_ns, 5_000_000);
    }

    #[test]
    fn latency_accumulator_dedups_on_finished_stamp() {
        let mut acc = LatencyAccumulator::with_capacity(64);
        // First observation records.
        assert!(acc.observe("nav/plan", 1_000, 100));
        // Same finished stamp (unchanged last-value across a poll) is ignored.
        assert!(!acc.observe("nav/plan", 1_000, 100));
        // Older/equal finished stamp ignored even with a different latency.
        assert!(!acc.observe("nav/plan", 9_999, 50));
        // A newer finished stamp records the fresh sample.
        assert!(acc.observe("nav/plan", 2_000, 200));
        // Zero finished stamp (no call completed) is ignored.
        assert!(!acc.observe("nav/plan", 7_000, 0));

        let snapshot = acc.snapshot_for("nav/plan").expect("snapshot");
        assert_eq!(snapshot.count, 2, "only two distinct observations recorded");
        assert_eq!(snapshot.max_ns, 2_000);
        assert!(acc.snapshot_for("other/service").is_none());
    }

    #[test]
    fn latency_accumulator_rings_are_bounded() {
        let mut acc = LatencyAccumulator::with_capacity(3);
        for finished in 1..=10u64 {
            assert!(acc.observe("svc", finished * 10, finished));
        }
        let snapshot = acc.snapshot_for("svc").expect("snapshot");
        // Cap of 3 → only the three most-recent samples (80, 90, 100) are retained.
        assert_eq!(snapshot.count, 3, "ring is capped at 3 samples");
        assert_eq!(snapshot.max_ns, 100);
    }

    #[test]
    fn schema_hash_present_detects_registered_schema() {
        // A non-empty hash means the resource declared a schema → column `confirmed`.
        assert!(schema_hash_present(Some(
            "535f43f88fc9c5165d2b733e2f7597d703bf2a678da9212f300f79e6fe07ef50"
        )));
        // Absent / blank → `?` (Unknown), never a false "confirmed".
        assert!(!schema_hash_present(None));
        assert!(!schema_hash_present(Some("")));
        assert!(!schema_hash_present(Some("   ")));
    }
}
