use anyhow::Result;
use aorta_introspect::client::{
    ActionDetail, ActionFeedbackFrame, ActionGoalSendResult, ActionGoalSnapshot, ActionSnapshot,
    ContextEntry, IntrospectClient, LogRecordFrame, NodeDetailOptions, NodeDiscoverySource,
    NodeListOptions, NodeListResult, NodeRuntimeSummary, NodeSelector, NodeSnapshot, Provenance,
    ServiceCallRequest, ServiceDetailOptions, ServiceListOptions, ServiceRuntimeSummary,
    ServiceSnapshot, TopicDetailOptions, TopicListOptions, TopicRuntimeSummary, TopicSnapshot,
    ZenohAdminOverview, ZenohConnectivityEvent, ZenohConnectivityEventKind,
    ZenohConnectivityOverview, ZenohRouterMetrics,
};
use aorta_introspect::ids::{NodeId, ServiceSuffix, TopicSuffix};
use aorta_introspect::schema::{
    ResolvedSchema, ResolvedTopicSchemaKind, SchemaField, SchemaModel, SchemaRootSelector,
    SchemaTypeKind,
};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverviewData {
    pub topic_count: usize,
    pub service_count: usize,
    pub node_count: usize,
    pub node_discovery_source: NodeDiscoverySource,
    pub health: OverviewHealthSummary,
    pub highlighted_topics: Vec<HighlightedResource>,
    pub highlighted_services: Vec<HighlightedResource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthLevel {
    Healthy,
    Warn,
    Error,
    Unknown,
}

impl HealthLevel {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Unknown => "unknown",
        }
    }

    pub fn badge(self) -> &'static str {
        match self {
            Self::Healthy => "OK",
            Self::Warn => "WARN",
            Self::Error => "ERR",
            Self::Unknown => "UNK",
        }
    }

    fn priority(self) -> u8 {
        match self {
            Self::Error => 3,
            Self::Warn => 2,
            Self::Unknown => 1,
            Self::Healthy => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityState {
    Full,
    Partial,
    Unavailable,
}

impl CapabilityState {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Partial => "partial",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshnessState {
    Fresh,
    Steady,
    Slow,
    Unavailable,
}

impl FreshnessState {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Steady => "steady",
            Self::Slow => "slow",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverviewHealthSummary {
    pub overall: HealthLevel,
    pub discovery_capability: CapabilityState,
    pub runtime_capability: CapabilityState,
    pub freshness: FreshnessState,
    pub runtime_collection_period_ms: Option<u32>,
    pub runtime_topic_coverage: usize,
    pub runtime_service_coverage: usize,
    pub runtime_node_coverage: usize,
    pub warning_count: usize,
    pub error_count: usize,
    pub unknown_count: usize,
    pub schema_unknown_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HighlightedResource {
    pub name: String,
    pub health: HealthLevel,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaAvailability {
    Confirmed,
    Unknown,
}

impl SchemaAvailability {
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRow {
    pub name: String,
    pub provider_endpoint_count: usize,
    pub candidate_node_count: usize,
    pub publisher_node_count: Option<usize>,
    pub subscriber_node_count: Option<usize>,
    pub schema_availability: SchemaAvailability,
    pub schema_note: Option<String>,
    pub health: HealthLevel,
    pub capability: CapabilityState,
    pub freshness: FreshnessState,
    pub runtime_collection_period_ms: Option<u32>,
    pub status_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRow {
    pub name: String,
    pub provider_endpoint_count: usize,
    pub candidate_node_count: usize,
    pub provider_node_count: Option<usize>,
    pub client_node_count: Option<usize>,
    pub schema_availability: SchemaAvailability,
    pub schema_note: Option<String>,
    pub health: HealthLevel,
    pub capability: CapabilityState,
    pub freshness: FreshnessState,
    pub runtime_collection_period_ms: Option<u32>,
    pub status_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRow {
    pub name: String,
    pub node_id: String,
    pub group: String,
    pub endpoint_count: Option<usize>,
    pub topic_count: Option<usize>,
    pub service_count: Option<usize>,
    pub discovery_source: NodeDiscoverySource,
    pub health: HealthLevel,
    pub capability: CapabilityState,
    pub freshness: FreshnessState,
    pub runtime_collection_period_ms: Option<u32>,
    pub status_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardData {
    pub overview: OverviewData,
    pub topics: Vec<TopicRow>,
    pub services: Vec<ServiceRow>,
    pub nodes: Vec<NodeRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRow {
    pub name: String,
    pub server_node_name: String,
    pub supports_cancel: bool,
    pub max_concurrency: u32,
    pub result_ttl_ms: u64,
    pub dedup_ttl_ms: u64,
    pub schema_hash: String,
    pub status_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionGoalRow {
    pub goal_id: String,
    pub status: String,
    pub code: String,
    pub last_feedback_seq: u64,
    pub server_epoch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionDetailData {
    pub name: String,
    pub key_expr: String,
    pub server_node_name: String,
    pub server_epoch: String,
    pub supports_cancel: bool,
    pub max_concurrency: u32,
    pub result_ttl_ms: u64,
    pub dedup_ttl_ms: u64,
    pub schema_hash: String,
    pub send_goal_request_root: String,
    pub send_goal_response_root: String,
    pub cancel_goal_request_root: String,
    pub cancel_goal_response_root: String,
    pub feedback_root: String,
    pub public_status_root: String,
    pub get_result_request_root: String,
    pub get_result_response_root: String,
    pub goals: Vec<ActionGoalRow>,
}

/// Schema + seed data for the Action Call goal editor. Mirrors
/// [`ServiceCallSchemaData`] but seeded from the action's `SendGoalRequest`
/// root type; `request_model` is built rooted at that type and `starter_json`
/// is the *goal payload* object (the `goal_id`/`goal` wrapper is added by
/// [`invoke_action_send_goal`] at submit time, exactly as the CLI does).
#[derive(Debug, Clone, PartialEq)]
pub struct ActionCallSchemaData {
    pub action_name: String,
    pub key_expr: String,
    pub server_node_name: String,
    pub supports_cancel: bool,
    pub schema_hash: String,
    pub send_goal_request_root: String,
    pub request_model: SchemaModel,
    pub starter_json: String,
}

/// Decoded result of an Action Call send-goal RPC. Carries the `goal_id` and
/// `goal_secret` the user needs to subsequently cancel / track the goal — both
/// are surfaced in the response pane (the secret is the authorization token).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionCallResponseData {
    pub accepted: bool,
    pub goal_id: String,
    pub goal_secret: String,
    pub status: String,
    pub code: String,
    pub message: String,
    pub server_epoch: String,
}

/// Decoded result of an Action Call cancel RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionCancelResponseData {
    pub accepted: bool,
    pub goal_id: String,
    pub status: String,
    pub code: String,
    pub message: String,
}

/// A single row in the live `/log` stream viewer, decoded from a `foxglove.Log`.
/// Mirrors the typed [`aorta_introspect::client::LogRecordFrame`] but is the
/// TUI-owned value pushed into the bounded log buffer on `App`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRow {
    pub timestamp_ns: u64,
    pub level: String,
    pub target: String,
    pub message: String,
    pub node: String,
}

impl From<LogRecordFrame> for LogRow {
    fn from(frame: LogRecordFrame) -> Self {
        Self {
            timestamp_ns: frame.timestamp_ns,
            level: frame.level,
            target: frame.target,
            message: frame.message,
            node: frame.node,
        }
    }
}

/// A single live action-feedback row, decoded from a `Feedback` entry for the
/// goal that was last sent on the Action Call screen. Mirrors the typed
/// [`aorta_introspect::client::ActionFeedbackFrame`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackRow {
    pub goal_id: String,
    pub seq: u64,
    pub stamp_ns: u64,
    pub status: String,
    pub message: String,
    pub data_json: String,
}

impl From<ActionFeedbackFrame> for FeedbackRow {
    fn from(frame: ActionFeedbackFrame) -> Self {
        Self {
            goal_id: frame.goal_id,
            seq: frame.seq,
            stamp_ns: frame.stamp_ns,
            status: frame.status,
            message: frame.message,
            data_json: frame.data_json,
        }
    }
}

/// A single ContextStore entry rendered on the read-only Context screen.
///
/// One row per owner reply (mirrors the CLI `aorta ctx list`): the same `key`
/// can appear in multiple rows when more than one writer is publishing it. When
/// that happens with *distinct* `source_instance_id`s, every row for that key is
/// flagged with `conflict = true` and rendered with a ⚠ marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRow {
    pub key: String,
    pub value_text: String,
    pub kind: String,
    pub source_node: String,
    pub source_instance_id: String,
    pub freshness: &'static str,
    pub conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextData {
    pub rows: Vec<ContextRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExactSchemaAvailability {
    Confirmed { hash: String },
    Missing,
}

impl ExactSchemaAvailability {
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::Confirmed { .. } => "confirmed",
            Self::Missing => "missing",
        }
    }

    pub fn hash(&self) -> Option<&str> {
        match self {
            Self::Confirmed { hash } => Some(hash.as_str()),
            Self::Missing => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDetailData {
    pub name: String,
    pub key_expr: String,
    pub provider_endpoint_count: usize,
    pub candidate_node_count: usize,
    pub publisher_node_count: Option<usize>,
    pub subscriber_node_count: Option<usize>,
    pub schema_availability: ExactSchemaAvailability,
    pub schema_provenance: &'static str,
    pub provider_endpoint_provenance: &'static str,
    pub candidate_node_provenance: &'static str,
    pub publisher_node_provenance: &'static str,
    pub subscriber_node_provenance: &'static str,
    pub provider_endpoint_ids: Vec<String>,
    pub candidate_node_names: Vec<String>,
    pub publisher_node_names: Vec<String>,
    pub subscriber_node_names: Vec<String>,
    pub schema_note: Option<String>,
    pub candidate_nodes_note: Option<String>,
    pub publisher_nodes_note: Option<String>,
    pub subscriber_nodes_note: Option<String>,
    pub runtime_summary: Option<TopicRuntimeSummaryData>,
    pub runtime_provenance: &'static str,
    pub runtime_note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRuntimeSummaryData {
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
pub struct ServiceDetailData {
    pub name: String,
    pub key_expr: String,
    pub provider_endpoint_count: usize,
    pub candidate_node_count: usize,
    pub provider_node_count: Option<usize>,
    pub client_node_count: Option<usize>,
    pub schema_availability: ExactSchemaAvailability,
    pub schema_provenance: &'static str,
    pub provider_endpoint_provenance: &'static str,
    pub candidate_node_provenance: &'static str,
    pub provider_node_provenance: &'static str,
    pub client_node_provenance: &'static str,
    pub schema_hash: Option<String>,
    pub request_root_type: Option<String>,
    pub response_root_type: Option<String>,
    pub provider_endpoint_ids: Vec<String>,
    pub candidate_node_names: Vec<String>,
    pub provider_node_names: Vec<String>,
    pub client_node_names: Vec<String>,
    pub schema_note: Option<String>,
    pub candidate_nodes_note: Option<String>,
    pub provider_nodes_note: Option<String>,
    pub client_nodes_note: Option<String>,
    pub runtime_summary: Option<ServiceRuntimeSummaryData>,
    pub runtime_provenance: &'static str,
    pub runtime_note: Option<String>,
}

/// Whether a [`SchemaDescribeData`] payload describes a topic, a param, or a service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaDescribeKind {
    Topic,
    Param,
    Service,
}

impl SchemaDescribeKind {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::Param => "param",
            Self::Service => "service",
        }
    }
}

/// One root within a schema describe payload. Topic schemas always have exactly
/// one section (the default root). Service schemas have two: request and
/// response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDescribeSection {
    pub label: String,
    pub root_type: Option<String>,
    pub text: String,
}

/// Result of fetching a schema describe view for the currently selected topic
/// or service. The text fields are the human-readable schema definitions
/// rendered by the BFBS reflection bridge (same output as
/// `aorta schema get --describe`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDescribeData {
    pub key_suffix: String,
    pub kind: SchemaDescribeKind,
    pub schema_hash: String,
    pub bfbs_bytes: usize,
    pub sections: Vec<SchemaDescribeSection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRuntimeSummaryData {
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
    /// Latency (true duration) of the most-recently-finished client call; `0` = no call yet.
    pub client_last_call_latency_ns: u64,
    /// Wall-clock `last_call_finished_ns` stamp of that most-recent client call
    /// (`0` = no call yet). Pairs with `client_last_call_latency_ns`; used as a
    /// de-duplication key when the Telemetry screen accumulates latency over refresh ticks.
    pub client_last_call_finished_ns: u64,
    /// Most recent service request/reply wall-clock stamps (`0` = unset).
    pub service_last_request_ns: u64,
    pub service_last_reply_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDetailData {
    pub name: String,
    pub node_id: String,
    pub group: String,
    pub endpoint_count: Option<usize>,
    pub topic_count: Option<usize>,
    pub service_count: Option<usize>,
    pub discovery_source: NodeDiscoverySource,
    pub endpoint_ids: Vec<String>,
    pub topic_names: Vec<String>,
    pub service_names: Vec<String>,
    pub published_topic_count: Option<usize>,
    pub subscribed_topic_count: Option<usize>,
    pub provided_service_count: Option<usize>,
    pub client_service_count: Option<usize>,
    pub published_topic_names: Vec<String>,
    pub subscribed_topic_names: Vec<String>,
    pub provided_service_names: Vec<String>,
    pub client_service_names: Vec<String>,
    pub endpoint_provenance: &'static str,
    pub topic_provenance: &'static str,
    pub service_provenance: &'static str,
    pub published_topic_provenance: &'static str,
    pub subscribed_topic_provenance: &'static str,
    pub provided_service_provenance: &'static str,
    pub client_service_provenance: &'static str,
    pub endpoint_note: Option<String>,
    pub topic_note: Option<String>,
    pub service_note: Option<String>,
    pub published_topic_note: Option<String>,
    pub subscribed_topic_note: Option<String>,
    pub provided_service_note: Option<String>,
    pub client_service_note: Option<String>,
    pub runtime_summary: Option<NodeRuntimeSummaryData>,
    pub runtime_provenance: &'static str,
    pub runtime_note: Option<String>,
    pub runtime_breakdown: Option<NodeRuntimeBreakdownData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRuntimeSummaryData {
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
    pub executor_queued_events: Option<u64>,
    pub executor_queued_bytes: Option<u64>,
    pub executor_peak_queued_events: Option<u64>,
    pub executor_peak_queued_bytes: Option<u64>,
    pub executor_dropped_queue_full: Option<u64>,
    pub executor_dropped_queue_closed: Option<u64>,
    pub executor_dropped_budget_events: Option<u64>,
    pub executor_dropped_budget_bytes: Option<u64>,
    pub executor_spin_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRuntimeBreakdownData {
    pub publishers: Vec<NodePublisherRuntimeEntryData>,
    pub subscribers: Vec<NodeSubscriberRuntimeEntryData>,
    pub services: Vec<NodeServiceRuntimeEntryData>,
    pub clients: Vec<NodeClientRuntimeEntryData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePublisherRuntimeEntryData {
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
pub struct NodeSubscriberRuntimeEntryData {
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
pub struct NodeServiceRuntimeEntryData {
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
pub struct NodeClientRuntimeEntryData {
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
pub struct ZenohInternalsData {
    pub router_count: usize,
    pub routers: Vec<ZenohRouterRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohConnectivityData {
    pub session_zid: String,
    pub local_locator_count: usize,
    pub transport_count: usize,
    pub link_count: usize,
    pub local_locators: Vec<String>,
    pub transports: Vec<ZenohTransportRow>,
    pub links: Vec<ZenohLinkRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZenohConnectivityEventKindData {
    TransportOpened,
    TransportClosed,
    LinkAdded,
    LinkRemoved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohConnectivityEventData {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub kind: ZenohConnectivityEventKindData,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohRouterRow {
    pub zid: String,
    pub whatami: String,
    pub version: Option<String>,
    pub session_count: Option<usize>,
    pub locator_count: usize,
    pub plugin_count: usize,
    pub metrics_available: bool,
    /// Parsed router byte/message counters, when the metrics body was readable.
    pub metrics: Option<ZenohRouterMetrics>,
    pub locators: Vec<String>,
    pub plugin_names: Vec<String>,
    pub raw_payload_utf8: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohTransportRow {
    pub remote_zid: String,
    pub remote_whatami: String,
    pub supports_qos: bool,
    pub is_multicast: bool,
    pub supports_shm: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZenohLinkRow {
    pub transport_zid: String,
    pub src: String,
    pub dst: String,
    pub group: Option<String>,
    pub mtu: u16,
    pub is_streamed: bool,
    pub interface_count: usize,
    pub interfaces: Vec<String>,
    pub auth_identifier: Option<String>,
    pub priorities: Option<(u8, u8)>,
    pub reliability: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceCallSchemaData {
    pub service_name: String,
    pub key_expr: String,
    pub provider_endpoint_count: usize,
    pub candidate_node_count: usize,
    pub candidate_nodes_note: Option<String>,
    pub schema_hash: String,
    pub request_root_type: Option<String>,
    pub response_root_type: Option<String>,
    pub request_model: SchemaModel,
    pub starter_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceCallResponseData {
    pub response_json: Option<String>,
    pub response_utf8: Option<String>,
    pub response_hex: String,
    pub decoded_with_schema: bool,
    pub schema_hash: String,
    pub response_root_type: Option<String>,
}

pub async fn fetch_dashboard_data(client: &IntrospectClient) -> Result<DashboardData> {
    let topics = client
        .list_topics(TopicListOptions {
            include_candidate_nodes: true,
            node_filter: None,
        })
        .await?;
    let services = client
        .list_services(ServiceListOptions {
            include_candidate_nodes: true,
            node_filter: None,
        })
        .await?;
    let nodes = client
        .list_nodes(NodeListOptions {
            include_candidate_resources: true,
        })
        .await?;

    Ok(build_dashboard_data(topics, services, nodes))
}

fn build_dashboard_data(
    topics: Vec<TopicSnapshot>,
    services: Vec<ServiceSnapshot>,
    nodes: NodeListResult,
) -> DashboardData {
    let discovery_source = nodes.source;
    let topic_rows: Vec<_> = topics.into_iter().map(map_topic_row).collect();
    let service_rows: Vec<_> = services.into_iter().map(map_service_row).collect();
    let node_rows: Vec<_> = nodes
        .nodes
        .into_iter()
        .map(|node| map_node_row(node, discovery_source))
        .collect();

    let overview = OverviewData {
        topic_count: topic_rows.len(),
        service_count: service_rows.len(),
        node_count: node_rows.len(),
        node_discovery_source: discovery_source,
        health: build_overview_health_summary(&topic_rows, &service_rows, &node_rows),
        highlighted_topics: highlighted_topics(&topic_rows),
        highlighted_services: highlighted_services(&service_rows),
    };

    DashboardData {
        overview,
        topics: topic_rows,
        services: service_rows,
        nodes: node_rows,
    }
}

fn map_topic_row(topic: TopicSnapshot) -> TopicRow {
    let schema_availability = schema_availability_from_listing(topic.schema_present);
    let runtime_collection_period_ms = topic
        .runtime
        .value
        .as_ref()
        .map(|runtime| runtime.collection_period_ms);
    let health = topic_health(&topic.runtime.value, &schema_availability);
    let capability = topic_capability(&topic);
    let freshness = freshness_from_period(runtime_collection_period_ms);
    let schema_note = topic.candidate_nodes.note.clone();
    let status_summary = topic_status_summary(
        &topic.runtime.value,
        &schema_availability,
        topic.runtime.note.as_deref(),
        schema_note.as_deref(),
    );

    TopicRow {
        name: topic.topic.as_str().to_string(),
        provider_endpoint_count: topic.provider_endpoints.value.len(),
        candidate_node_count: topic.candidate_nodes.value.len(),
        publisher_node_count: topic.publisher_node_count.value,
        subscriber_node_count: topic.subscriber_node_count.value,
        schema_availability,
        schema_note,
        health,
        capability,
        freshness,
        runtime_collection_period_ms,
        status_summary,
    }
}

fn map_service_row(service: ServiceSnapshot) -> ServiceRow {
    let schema_availability = schema_availability_from_listing(service.schema_present);
    let runtime_collection_period_ms = service
        .runtime
        .value
        .as_ref()
        .map(|runtime| runtime.collection_period_ms);
    let health = service_health(&service.runtime.value, &schema_availability);
    let capability = service_capability(&service);
    let freshness = freshness_from_period(runtime_collection_period_ms);
    let schema_note = service.candidate_nodes.note.clone();
    let status_summary = service_status_summary(
        &service.runtime.value,
        &schema_availability,
        service.runtime.note.as_deref(),
        schema_note.as_deref(),
    );

    ServiceRow {
        name: service.service.as_str().to_string(),
        provider_endpoint_count: service.provider_endpoints.value.len(),
        candidate_node_count: service.candidate_nodes.value.len(),
        provider_node_count: service.provider_node_count.value,
        client_node_count: service.client_node_count.value,
        schema_availability,
        schema_note,
        health,
        capability,
        freshness,
        runtime_collection_period_ms,
        status_summary,
    }
}

fn map_node_row(node: NodeSnapshot, discovery_source: NodeDiscoverySource) -> NodeRow {
    let runtime_collection_period_ms = node
        .runtime
        .value
        .as_ref()
        .map(|runtime| runtime.collection_period_ms);
    let health = node_health(&node.runtime.value, discovery_source);
    let capability = node_capability(&node);
    let freshness = freshness_from_period(runtime_collection_period_ms);
    let status_summary = node_status_summary(
        &node.runtime.value,
        discovery_source,
        node.runtime.note.as_deref(),
    );

    NodeRow {
        name: node.name.as_str().to_string(),
        node_id: node.id.as_str().to_string(),
        group: node.group.as_str().to_string(),
        endpoint_count: count_if_available(
            &node.owner_endpoints.provenance,
            node.owner_endpoints.value.len(),
        ),
        topic_count: node.candidate_topic_count.value,
        service_count: node.candidate_service_count.value,
        discovery_source,
        health,
        capability,
        freshness,
        runtime_collection_period_ms,
        status_summary,
    }
}

fn build_overview_health_summary(
    topic_rows: &[TopicRow],
    service_rows: &[ServiceRow],
    node_rows: &[NodeRow],
) -> OverviewHealthSummary {
    let warning_count = topic_rows
        .iter()
        .filter(|row| row.health == HealthLevel::Warn)
        .count()
        + service_rows
            .iter()
            .filter(|row| row.health == HealthLevel::Warn)
            .count()
        + node_rows
            .iter()
            .filter(|row| row.health == HealthLevel::Warn)
            .count();
    let error_count = topic_rows
        .iter()
        .filter(|row| row.health == HealthLevel::Error)
        .count()
        + service_rows
            .iter()
            .filter(|row| row.health == HealthLevel::Error)
            .count()
        + node_rows
            .iter()
            .filter(|row| row.health == HealthLevel::Error)
            .count();
    let unknown_count = topic_rows
        .iter()
        .filter(|row| row.health == HealthLevel::Unknown)
        .count()
        + service_rows
            .iter()
            .filter(|row| row.health == HealthLevel::Unknown)
            .count()
        + node_rows
            .iter()
            .filter(|row| row.health == HealthLevel::Unknown)
            .count();
    let schema_unknown_count = topic_rows
        .iter()
        .filter(|row| row.schema_availability == SchemaAvailability::Unknown)
        .count()
        + service_rows
            .iter()
            .filter(|row| row.schema_availability == SchemaAvailability::Unknown)
            .count();
    let runtime_topic_coverage = topic_rows
        .iter()
        .filter(|row| row.capability == CapabilityState::Full)
        .count();
    let runtime_service_coverage = service_rows
        .iter()
        .filter(|row| row.capability == CapabilityState::Full)
        .count();
    let runtime_node_coverage = node_rows
        .iter()
        .filter(|row| row.capability == CapabilityState::Full)
        .count();
    let discovery_capability = node_rows
        .first()
        .map(|row| discovery_capability(row.discovery_source))
        .unwrap_or(CapabilityState::Unavailable);
    let runtime_capability = summarize_runtime_capability(
        topic_rows.iter().map(|row| row.capability),
        service_rows.iter().map(|row| row.capability),
        node_rows.iter().map(|row| row.capability),
    );
    let runtime_collection_period_ms = topic_rows
        .iter()
        .filter_map(|row| row.runtime_collection_period_ms)
        .chain(
            service_rows
                .iter()
                .filter_map(|row| row.runtime_collection_period_ms),
        )
        .chain(
            node_rows
                .iter()
                .filter_map(|row| row.runtime_collection_period_ms),
        )
        .max();
    let freshness = freshness_from_period(runtime_collection_period_ms);
    let total_resources = topic_rows.len() + service_rows.len() + node_rows.len();
    let overall = if total_resources == 0 {
        HealthLevel::Unknown
    } else if error_count > 0 {
        HealthLevel::Error
    } else if warning_count > 0
        || schema_unknown_count > 0
        || discovery_capability != CapabilityState::Full
        || runtime_capability != CapabilityState::Full
        || matches!(
            freshness,
            FreshnessState::Slow | FreshnessState::Unavailable
        )
    {
        HealthLevel::Warn
    } else if unknown_count > 0 {
        HealthLevel::Unknown
    } else {
        HealthLevel::Healthy
    };

    OverviewHealthSummary {
        overall,
        discovery_capability,
        runtime_capability,
        freshness,
        runtime_collection_period_ms,
        runtime_topic_coverage,
        runtime_service_coverage,
        runtime_node_coverage,
        warning_count,
        error_count,
        unknown_count,
        schema_unknown_count,
    }
}

fn summarize_runtime_capability(
    topics: impl Iterator<Item = CapabilityState>,
    services: impl Iterator<Item = CapabilityState>,
    nodes: impl Iterator<Item = CapabilityState>,
) -> CapabilityState {
    let capabilities: Vec<_> = topics.chain(services).chain(nodes).collect();
    if capabilities.is_empty() {
        return CapabilityState::Unavailable;
    }
    let full_count = capabilities
        .iter()
        .filter(|state| **state == CapabilityState::Full)
        .count();
    let partial_count = capabilities
        .iter()
        .filter(|state| **state == CapabilityState::Partial)
        .count();
    if full_count == capabilities.len() {
        CapabilityState::Full
    } else if full_count > 0 || partial_count > 0 {
        CapabilityState::Partial
    } else {
        CapabilityState::Unavailable
    }
}

fn discovery_capability(source: NodeDiscoverySource) -> CapabilityState {
    match source {
        NodeDiscoverySource::TelemetrySnapshot => CapabilityState::Full,
        NodeDiscoverySource::AdminSnapshot => CapabilityState::Partial,
        NodeDiscoverySource::LivelinessFallback => CapabilityState::Unavailable,
    }
}

fn highlighted_topics(topic_rows: &[TopicRow]) -> Vec<HighlightedResource> {
    let mut highlighted: Vec<_> = topic_rows.iter().collect();
    highlighted.sort_by(|left, right| {
        right
            .health
            .priority()
            .cmp(&left.health.priority())
            .then_with(|| topic_attention_score(right).cmp(&topic_attention_score(left)))
            .then_with(|| {
                right
                    .provider_endpoint_count
                    .cmp(&left.provider_endpoint_count)
            })
            .then_with(|| left.name.cmp(&right.name))
    });
    highlighted.truncate(5);
    highlighted
        .into_iter()
        .map(|row| HighlightedResource {
            name: row.name.clone(),
            health: row.health,
            reason: row.status_summary.clone(),
        })
        .collect()
}

fn highlighted_services(service_rows: &[ServiceRow]) -> Vec<HighlightedResource> {
    let mut highlighted: Vec<_> = service_rows.iter().collect();
    highlighted.sort_by(|left, right| {
        right
            .health
            .priority()
            .cmp(&left.health.priority())
            .then_with(|| service_attention_score(right).cmp(&service_attention_score(left)))
            .then_with(|| {
                right
                    .provider_endpoint_count
                    .cmp(&left.provider_endpoint_count)
            })
            .then_with(|| left.name.cmp(&right.name))
    });
    highlighted.truncate(5);
    highlighted
        .into_iter()
        .map(|row| HighlightedResource {
            name: row.name.clone(),
            health: row.health,
            reason: row.status_summary.clone(),
        })
        .collect()
}

fn topic_capability(topic: &TopicSnapshot) -> CapabilityState {
    if topic.runtime.value.is_some() {
        CapabilityState::Full
    } else if topic.publisher_node_count.value.is_some()
        || topic.subscriber_node_count.value.is_some()
        || !topic.candidate_nodes.value.is_empty()
    {
        CapabilityState::Partial
    } else {
        CapabilityState::Unavailable
    }
}

fn service_capability(service: &ServiceSnapshot) -> CapabilityState {
    if service.runtime.value.is_some() {
        CapabilityState::Full
    } else if service.provider_node_count.value.is_some()
        || service.client_node_count.value.is_some()
        || !service.candidate_nodes.value.is_empty()
    {
        CapabilityState::Partial
    } else {
        CapabilityState::Unavailable
    }
}

fn node_capability(node: &NodeSnapshot) -> CapabilityState {
    if node.runtime.value.is_some() {
        CapabilityState::Full
    } else if node.candidate_topic_count.value.is_some()
        || node.candidate_service_count.value.is_some()
    {
        CapabilityState::Partial
    } else {
        CapabilityState::Unavailable
    }
}

fn freshness_from_period(period_ms: Option<u32>) -> FreshnessState {
    match period_ms {
        Some(period_ms) if period_ms <= 1_000 => FreshnessState::Fresh,
        Some(period_ms) if period_ms <= 5_000 => FreshnessState::Steady,
        Some(_) => FreshnessState::Slow,
        None => FreshnessState::Unavailable,
    }
}

fn topic_health(
    runtime: &Option<TopicRuntimeSummary>,
    _schema_availability: &SchemaAvailability,
) -> HealthLevel {
    match runtime {
        Some(runtime) => topic_runtime_health(runtime),
        None => HealthLevel::Unknown,
    }
}

fn service_health(
    runtime: &Option<ServiceRuntimeSummary>,
    _schema_availability: &SchemaAvailability,
) -> HealthLevel {
    match runtime {
        Some(runtime) => service_runtime_health(runtime),
        None => HealthLevel::Unknown,
    }
}

fn node_health(
    runtime: &Option<NodeRuntimeSummary>,
    _discovery_source: NodeDiscoverySource,
) -> HealthLevel {
    match runtime {
        Some(runtime) => node_runtime_health(runtime),
        None => HealthLevel::Unknown,
    }
}

fn topic_runtime_health(runtime: &TopicRuntimeSummary) -> HealthLevel {
    if topic_error_event_total(runtime) > 0 {
        HealthLevel::Error
    } else if topic_warn_event_total(runtime) > 0 {
        HealthLevel::Warn
    } else {
        HealthLevel::Healthy
    }
}

fn service_runtime_health(runtime: &ServiceRuntimeSummary) -> HealthLevel {
    if service_error_event_total(runtime) > 0 {
        HealthLevel::Error
    } else if service_warn_event_total(runtime) > 0 {
        HealthLevel::Warn
    } else {
        HealthLevel::Healthy
    }
}

fn node_runtime_health(runtime: &NodeRuntimeSummary) -> HealthLevel {
    if node_error_event_total(runtime) > 0 {
        HealthLevel::Error
    } else if node_warn_event_total(runtime) > 0 {
        HealthLevel::Warn
    } else {
        HealthLevel::Healthy
    }
}

fn topic_status_summary(
    runtime: &Option<TopicRuntimeSummary>,
    schema_availability: &SchemaAvailability,
    runtime_note: Option<&str>,
    fallback_note: Option<&str>,
) -> String {
    if let Some(runtime) = runtime {
        let errors = topic_error_event_total(runtime);
        let warnings = topic_warn_event_total(runtime);
        if errors > 0 {
            format!("runtime errors={errors}")
        } else if warnings > 0 {
            format!("runtime warnings={warnings}")
        } else {
            format!("runtime ok (~{} ms)", runtime.collection_period_ms)
        }
    } else if *schema_availability == SchemaAvailability::Unknown {
        "schema not confirmed from list discovery".to_string()
    } else if let Some(note) = runtime_note.filter(|note| !note.is_empty()) {
        note.to_string()
    } else if let Some(note) = fallback_note.filter(|note| !note.is_empty()) {
        note.to_string()
    } else {
        "runtime unavailable — no telemetry summary".to_string()
    }
}

fn service_status_summary(
    runtime: &Option<ServiceRuntimeSummary>,
    schema_availability: &SchemaAvailability,
    runtime_note: Option<&str>,
    fallback_note: Option<&str>,
) -> String {
    if let Some(runtime) = runtime {
        let errors = service_error_event_total(runtime);
        let warnings = service_warn_event_total(runtime);
        if errors > 0 {
            format!("runtime errors={errors}")
        } else if warnings > 0 {
            format!("runtime warnings={warnings}")
        } else {
            format!("runtime ok (~{} ms)", runtime.collection_period_ms)
        }
    } else if *schema_availability == SchemaAvailability::Unknown {
        "schema not confirmed from list discovery".to_string()
    } else if let Some(note) = runtime_note.filter(|note| !note.is_empty()) {
        note.to_string()
    } else if let Some(note) = fallback_note.filter(|note| !note.is_empty()) {
        note.to_string()
    } else {
        "runtime unavailable — no telemetry summary".to_string()
    }
}

fn node_status_summary(
    runtime: &Option<NodeRuntimeSummary>,
    discovery_source: NodeDiscoverySource,
    runtime_note: Option<&str>,
) -> String {
    if let Some(runtime) = runtime {
        let errors = node_error_event_total(runtime);
        let warnings = node_warn_event_total(runtime);
        if errors > 0 {
            format!("runtime errors={errors}")
        } else if warnings > 0 {
            format!("runtime warnings={warnings}")
        } else {
            format!("runtime ok (~{} ms)", runtime.collection_period_ms)
        }
    } else if let Some(note) = runtime_note.filter(|note| !note.is_empty()) {
        note.to_string()
    } else {
        match discovery_source {
            NodeDiscoverySource::TelemetrySnapshot => {
                "runtime unavailable — telemetry summary missing".to_string()
            }
            NodeDiscoverySource::AdminSnapshot => {
                "runtime unavailable — admin snapshot only".to_string()
            }
            NodeDiscoverySource::LivelinessFallback => {
                "runtime unavailable — liveliness fallback only".to_string()
            }
        }
    }
}

fn topic_attention_score(row: &TopicRow) -> u64 {
    let health_bias = match row.health {
        HealthLevel::Error => 1_000_000,
        HealthLevel::Warn => 100_000,
        HealthLevel::Unknown => 10_000,
        HealthLevel::Healthy => 0,
    };
    health_bias + row.provider_endpoint_count as u64 + row.candidate_node_count as u64
}

fn service_attention_score(row: &ServiceRow) -> u64 {
    let health_bias = match row.health {
        HealthLevel::Error => 1_000_000,
        HealthLevel::Warn => 100_000,
        HealthLevel::Unknown => 10_000,
        HealthLevel::Healthy => 0,
    };
    health_bias + row.provider_endpoint_count as u64 + row.candidate_node_count as u64
}

fn topic_error_event_total(runtime: &TopicRuntimeSummary) -> u64 {
    runtime.publish_shm_required_fail_total
        + runtime.subscriber_out_of_order_drop_count
        + runtime.subscriber_executor_drop_count
        + runtime.subscriber_local_drop_count
        + runtime.subscriber_overflow_relay_drop_count
}

fn topic_warn_event_total(runtime: &TopicRuntimeSummary) -> u64 {
    runtime.publish_shm_fallback_total
        + runtime.loan_quota_exceeded_total
        + runtime.subscriber_duplicate_drop_count
        + runtime.subscriber_out_of_order_count
        + runtime.subscriber_overflow_relay_enqueue_count
}

fn service_error_event_total(runtime: &ServiceRuntimeSummary) -> u64 {
    runtime.service_reply_send_failure_count
        + runtime.service_request_dropped_count
        + runtime.client_timeout_count
        + runtime.client_error_count
}

fn service_warn_event_total(runtime: &ServiceRuntimeSummary) -> u64 {
    runtime.service_reply_error_count
        + runtime.client_concurrent_conflict_count
        + runtime.client_retry_count
}

fn node_error_event_total(runtime: &NodeRuntimeSummary) -> u64 {
    let executor_drop_total = runtime
        .executor
        .as_ref()
        .map(|executor| {
            executor.dropped_queue_full
                + executor.dropped_queue_closed
                + executor.dropped_budget_events
                + executor.dropped_budget_bytes
        })
        .unwrap_or(0);
    runtime.publish_shm_required_fail_total
        + runtime.subscriber_out_of_order_drop_count
        + runtime.subscriber_executor_drop_count
        + runtime.subscriber_local_drop_count
        + runtime.subscriber_overflow_relay_drop_count
        + runtime.service_reply_send_failure_count
        + runtime.service_request_dropped_count
        + runtime.client_timeout_count
        + runtime.client_error_count
        + executor_drop_total
}

fn node_warn_event_total(runtime: &NodeRuntimeSummary) -> u64 {
    runtime.publish_shm_fallback_total
        + runtime.loan_quota_exceeded_total
        + runtime.subscriber_duplicate_drop_count
        + runtime.subscriber_out_of_order_count
        + runtime.subscriber_overflow_relay_enqueue_count
        + runtime.service_reply_error_count
        + runtime.client_concurrent_conflict_count
        + runtime.client_retry_count
}

fn candidate_node_label(node: &aorta_introspect::client::CandidateNodeRef) -> String {
    format!("{} ({})", node.name.as_str(), node.id.as_str())
}

pub async fn fetch_topic_detail_data(
    client: &IntrospectClient,
    selected: &TopicRow,
) -> Result<TopicDetailData> {
    let topic = TopicSuffix::new(&selected.name)?;
    let detail = client
        .get_topic_detail(&topic, TopicDetailOptions::default())
        .await?;
    Ok(map_topic_detail_data(detail))
}

fn map_topic_detail_data(detail: aorta_introspect::client::TopicDetail) -> TopicDetailData {
    let schema_availability = exact_schema_availability_from_topic_detail(&detail);
    let runtime_summary = detail
        .runtime
        .value
        .as_ref()
        .map(|runtime| TopicRuntimeSummaryData {
            latest_timestamp_ns: runtime.latest_timestamp_ns,
            collection_period_ms: runtime.collection_period_ms,
            publisher_metric_count: runtime.publisher_metric_count,
            subscriber_metric_count: runtime.subscriber_metric_count,
            publish_shm_hit_total: runtime.publish_shm_hit_total,
            publish_shm_fallback_total: runtime.publish_shm_fallback_total,
            publish_shm_required_fail_total: runtime.publish_shm_required_fail_total,
            loan_inflight_current: runtime.loan_inflight_current,
            loan_quota_exceeded_total: runtime.loan_quota_exceeded_total,
            subscriber_duplicate_drop_count: runtime.subscriber_duplicate_drop_count,
            subscriber_out_of_order_count: runtime.subscriber_out_of_order_count,
            subscriber_out_of_order_drop_count: runtime.subscriber_out_of_order_drop_count,
            subscriber_executor_drop_count: runtime.subscriber_executor_drop_count,
            subscriber_local_drop_count: runtime.subscriber_local_drop_count,
            subscriber_overflow_relay_enqueue_count: runtime
                .subscriber_overflow_relay_enqueue_count,
            subscriber_overflow_relay_drop_count: runtime.subscriber_overflow_relay_drop_count,
        });
    TopicDetailData {
        name: detail.snapshot.topic.as_str().to_string(),
        key_expr: detail.key_expr,
        provider_endpoint_count: detail.snapshot.provider_endpoints.value.len(),
        candidate_node_count: detail.snapshot.candidate_nodes.value.len(),
        publisher_node_count: count_if_available(
            &detail.publisher_nodes.provenance,
            detail.publisher_nodes.value.len(),
        ),
        subscriber_node_count: count_if_available(
            &detail.subscriber_nodes.provenance,
            detail.subscriber_nodes.value.len(),
        ),
        schema_availability,
        schema_provenance: provenance_label(detail.schema.provenance),
        provider_endpoint_provenance: provenance_label(
            detail.snapshot.provider_endpoints.provenance,
        ),
        candidate_node_provenance: provenance_label(detail.snapshot.candidate_nodes.provenance),
        publisher_node_provenance: provenance_label(detail.publisher_nodes.provenance),
        subscriber_node_provenance: provenance_label(detail.subscriber_nodes.provenance),
        provider_endpoint_ids: detail
            .snapshot
            .provider_endpoints
            .value
            .iter()
            .map(|endpoint| endpoint.as_str().to_string())
            .collect(),
        candidate_node_names: detail
            .snapshot
            .candidate_nodes
            .value
            .iter()
            .map(candidate_node_label)
            .collect(),
        publisher_node_names: detail
            .publisher_nodes
            .value
            .iter()
            .map(candidate_node_label)
            .collect(),
        subscriber_node_names: detail
            .subscriber_nodes
            .value
            .iter()
            .map(candidate_node_label)
            .collect(),
        schema_note: detail.schema.note,
        candidate_nodes_note: detail.snapshot.candidate_nodes.note,
        publisher_nodes_note: detail.publisher_nodes.note,
        subscriber_nodes_note: detail.subscriber_nodes.note,
        runtime_summary,
        runtime_provenance: provenance_label(detail.runtime.provenance),
        runtime_note: detail.runtime.note,
    }
}

pub async fn fetch_topic_schema_describe(
    client: &IntrospectClient,
    selected: &TopicRow,
) -> Result<SchemaDescribeData> {
    let topic = TopicSuffix::new(&selected.name)?;
    let key_suffix = format!("pub/{}", topic.as_str());
    let resolved = client.resolve_schema(&key_suffix).await?;
    map_schema_describe(client, &key_suffix, &resolved)
}

pub async fn fetch_service_schema_describe(
    client: &IntrospectClient,
    selected: &ServiceRow,
) -> Result<SchemaDescribeData> {
    let service = ServiceSuffix::new(&selected.name)?;
    let key_suffix = format!("srv/{}", service.as_str());
    let resolved = client.resolve_schema(&key_suffix).await?;
    map_schema_describe(client, &key_suffix, &resolved)
}

fn map_schema_describe(
    client: &IntrospectClient,
    key_suffix: &str,
    resolved: &ResolvedSchema,
) -> Result<SchemaDescribeData> {
    match resolved {
        ResolvedSchema::Topic(topic) => {
            let text = client.describe_schema(resolved, SchemaRootSelector::DefaultRoot)?;
            let kind = match topic.kind {
                ResolvedTopicSchemaKind::Topic => SchemaDescribeKind::Topic,
                ResolvedTopicSchemaKind::Param => SchemaDescribeKind::Param,
            };
            Ok(SchemaDescribeData {
                key_suffix: key_suffix.to_string(),
                kind,
                schema_hash: topic.schema_hash.clone(),
                bfbs_bytes: topic.bfbs.len(),
                sections: vec![SchemaDescribeSection {
                    label: "Schema".to_string(),
                    root_type: topic.default_root_type.clone(),
                    text,
                }],
            })
        }
        ResolvedSchema::Service(service) => {
            let mut sections = Vec::new();
            if service.request_root_type.is_some() {
                let text = client.describe_schema(resolved, SchemaRootSelector::ServiceRequest)?;
                sections.push(SchemaDescribeSection {
                    label: "Request".to_string(),
                    root_type: service.request_root_type.clone(),
                    text,
                });
            } else {
                match client.describe_schema(resolved, SchemaRootSelector::DefaultRoot) {
                    Ok(text) => sections.push(SchemaDescribeSection {
                        label: "Request (BFBS default root; metadata missing)".to_string(),
                        root_type: None,
                        text,
                    }),
                    Err(_) => sections.push(SchemaDescribeSection {
                        label: "Request".to_string(),
                        root_type: None,
                        text: "root type not provided".to_string(),
                    }),
                }
            }

            match service.response_root_type.as_deref() {
                Some(response_root)
                    if service.request_root_type.as_deref() == Some(response_root) =>
                {
                    sections.push(SchemaDescribeSection {
                        label: "Response".to_string(),
                        root_type: Some(response_root.to_string()),
                        text: format!("same root type as request ('{}')", response_root),
                    });
                }
                Some(_) => {
                    let text =
                        client.describe_schema(resolved, SchemaRootSelector::ServiceResponse)?;
                    sections.push(SchemaDescribeSection {
                        label: "Response".to_string(),
                        root_type: service.response_root_type.clone(),
                        text,
                    });
                }
                None => {
                    sections.push(SchemaDescribeSection {
                        label: "Response".to_string(),
                        root_type: None,
                        text: "root type not provided".to_string(),
                    });
                }
            }

            Ok(SchemaDescribeData {
                key_suffix: key_suffix.to_string(),
                kind: SchemaDescribeKind::Service,
                schema_hash: service.service_hash.clone(),
                bfbs_bytes: service.bfbs.len(),
                sections,
            })
        }
    }
}

pub async fn fetch_service_detail_data(
    client: &IntrospectClient,
    selected: &ServiceRow,
) -> Result<ServiceDetailData> {
    let service = ServiceSuffix::new(&selected.name)?;
    let detail = client
        .get_service_detail(&service, ServiceDetailOptions::default())
        .await?;
    Ok(map_service_detail_data(detail))
}

fn map_service_detail_data(detail: aorta_introspect::client::ServiceDetail) -> ServiceDetailData {
    let schema_availability = exact_schema_availability_from_service_detail(&detail);
    let schema_hash = detail
        .schema
        .value
        .as_ref()
        .map(|schema| schema.service_hash.clone());
    let request_root_type = detail
        .schema
        .value
        .as_ref()
        .and_then(|schema| schema.request_root_type.clone());
    let response_root_type = detail
        .schema
        .value
        .as_ref()
        .and_then(|schema| schema.response_root_type.clone());
    let runtime_summary = detail
        .runtime
        .value
        .as_ref()
        .map(|runtime| ServiceRuntimeSummaryData {
            latest_timestamp_ns: runtime.latest_timestamp_ns,
            collection_period_ms: runtime.collection_period_ms,
            service_metric_count: runtime.service_metric_count,
            client_metric_count: runtime.client_metric_count,
            service_request_received_count: runtime.service_request_received_count,
            service_request_dispatched_count: runtime.service_request_dispatched_count,
            service_reply_ok_count: runtime.service_reply_ok_count,
            service_reply_error_count: runtime.service_reply_error_count,
            service_reply_send_failure_count: runtime.service_reply_send_failure_count,
            service_request_dropped_count: runtime.service_request_dropped_count,
            service_in_flight_requests: runtime.service_in_flight_requests,
            client_call_count: runtime.client_call_count,
            client_success_count: runtime.client_success_count,
            client_timeout_count: runtime.client_timeout_count,
            client_concurrent_conflict_count: runtime.client_concurrent_conflict_count,
            client_error_count: runtime.client_error_count,
            client_retry_count: runtime.client_retry_count,
            client_last_call_latency_ns: runtime.client_last_call_latency_ns,
            client_last_call_finished_ns: runtime.client_last_call_finished_ns,
            service_last_request_ns: runtime.service_last_request_ns,
            service_last_reply_ns: runtime.service_last_reply_ns,
        });
    ServiceDetailData {
        name: detail.snapshot.service.as_str().to_string(),
        key_expr: detail.key_expr,
        provider_endpoint_count: detail.snapshot.provider_endpoints.value.len(),
        candidate_node_count: detail.snapshot.candidate_nodes.value.len(),
        provider_node_count: count_if_available(
            &detail.provider_nodes.provenance,
            detail.provider_nodes.value.len(),
        ),
        client_node_count: count_if_available(
            &detail.client_nodes.provenance,
            detail.client_nodes.value.len(),
        ),
        schema_availability,
        schema_provenance: provenance_label(detail.schema.provenance),
        provider_endpoint_provenance: provenance_label(
            detail.snapshot.provider_endpoints.provenance,
        ),
        candidate_node_provenance: provenance_label(detail.snapshot.candidate_nodes.provenance),
        provider_node_provenance: provenance_label(detail.provider_nodes.provenance),
        client_node_provenance: provenance_label(detail.client_nodes.provenance),
        schema_hash,
        request_root_type,
        response_root_type,
        provider_endpoint_ids: detail
            .snapshot
            .provider_endpoints
            .value
            .iter()
            .map(|endpoint| endpoint.as_str().to_string())
            .collect(),
        candidate_node_names: detail
            .snapshot
            .candidate_nodes
            .value
            .iter()
            .map(candidate_node_label)
            .collect(),
        provider_node_names: detail
            .provider_nodes
            .value
            .iter()
            .map(candidate_node_label)
            .collect(),
        client_node_names: detail
            .client_nodes
            .value
            .iter()
            .map(candidate_node_label)
            .collect(),
        schema_note: detail.schema.note,
        candidate_nodes_note: detail.snapshot.candidate_nodes.note,
        provider_nodes_note: detail.provider_nodes.note,
        client_nodes_note: detail.client_nodes.note,
        runtime_summary,
        runtime_provenance: provenance_label(detail.runtime.provenance),
        runtime_note: detail.runtime.note,
    }
}

pub async fn fetch_node_detail_data(
    client: &IntrospectClient,
    selected: &NodeRow,
) -> Result<NodeDetailData> {
    let node_id = NodeId::new(&selected.node_id)?;
    let detail = client
        .get_node_detail(NodeSelector::ById(node_id), NodeDetailOptions)
        .await?;
    Ok(map_node_detail_data(detail, selected.discovery_source))
}

pub async fn fetch_actions_data(client: &IntrospectClient) -> Result<Vec<ActionRow>> {
    let actions = client.list_actions().await?;
    Ok(actions.into_iter().map(map_action_row).collect())
}

pub async fn fetch_context_data(client: &IntrospectClient) -> Result<ContextData> {
    let entries = client.list_context_entries(None).await?;
    let now_ns = context_now_ns();
    Ok(ContextData {
        rows: build_context_rows(entries, now_ns),
    })
}

/// Current wall-clock time in ns, clamped to `0` on a pre-epoch clock — mirrors
/// the CLI `ctx::now_ns` so freshness is computed identically.
fn context_now_ns() -> u64 {
    let ns = aorta_core::time::now_ns();
    if ns < 0 {
        0
    } else {
        ns as u64
    }
}

/// `fresh`/`stale` classification matching the CLI `render_freshness`: a `0`
/// expiry means "never expires" (always fresh); otherwise fresh while `now`
/// has not yet reached the expiry stamp.
fn context_freshness(expires_at_ns: u64, now_ns: u64) -> &'static str {
    if expires_at_ns == 0 || now_ns < expires_at_ns {
        "fresh"
    } else {
        "stale"
    }
}

/// Group decoded [`ContextEntry`]s into rendered [`ContextRow`]s, flagging a
/// per-key conflict whenever the same key has more than one writer with a
/// *distinct* `source_instance_id` (mirrors the CLI `ctx list` ⚠ logic). Rows
/// preserve the (already key-sorted) input order.
fn build_context_rows(entries: Vec<ContextEntry>, now_ns: u64) -> Vec<ContextRow> {
    use std::collections::{HashMap, HashSet};

    // First pass: collect the set of distinct instance ids per key.
    let mut instances_by_key: HashMap<&str, HashSet<&str>> = HashMap::new();
    for entry in &entries {
        instances_by_key
            .entry(entry.key.as_str())
            .or_default()
            .insert(entry.source_instance_id.as_str());
    }

    entries
        .iter()
        .map(|entry| {
            let conflict = instances_by_key
                .get(entry.key.as_str())
                .map(|set| set.len() > 1)
                .unwrap_or(false);
            ContextRow {
                key: entry.key.clone(),
                value_text: entry.value_text.clone(),
                kind: entry.kind.to_string(),
                source_node: entry.source_node.clone(),
                source_instance_id: entry.source_instance_id.clone(),
                freshness: context_freshness(entry.expires_at_ns, now_ns),
                conflict,
            }
        })
        .collect()
}

pub async fn fetch_action_detail_data(
    client: &IntrospectClient,
    selected: &ActionRow,
) -> Result<ActionDetailData> {
    let detail = client.get_action_detail(&selected.name).await?;
    Ok(map_action_detail_data(detail))
}

pub async fn fetch_zenoh_internals_data(client: &IntrospectClient) -> Result<ZenohInternalsData> {
    let overview = client.fetch_zenoh_admin_overview().await?;
    Ok(map_zenoh_internals_data(overview))
}

pub async fn fetch_zenoh_connectivity_data(
    client: &IntrospectClient,
) -> Result<ZenohConnectivityData> {
    let overview = client.fetch_zenoh_connectivity_overview().await?;
    Ok(map_zenoh_connectivity_data(overview))
}

pub async fn fetch_zenoh_connectivity_event_data(
    client: &IntrospectClient,
    limit: usize,
) -> Result<Vec<ZenohConnectivityEventData>> {
    let events = client.fetch_recent_zenoh_connectivity_events(limit).await?;
    Ok(map_zenoh_connectivity_event_data(events))
}

fn map_action_row(action: ActionSnapshot) -> ActionRow {
    let info = action.info;
    let status_summary = format!(
        "cancel={} concurrency={} result_ttl={}ms",
        if info.supports_cancel { "yes" } else { "no" },
        info.max_concurrency,
        info.result_ttl_ms
    );
    ActionRow {
        name: action.action_name,
        server_node_name: info.server_node_name,
        supports_cancel: info.supports_cancel,
        max_concurrency: info.max_concurrency,
        result_ttl_ms: info.result_ttl_ms,
        dedup_ttl_ms: info.dedup_ttl_ms,
        schema_hash: info.schema_hash.to_hex(),
        status_summary,
    }
}

fn map_action_goal_row(goal: ActionGoalSnapshot) -> ActionGoalRow {
    ActionGoalRow {
        goal_id: goal.goal_id,
        status: format!("{:?}", goal.status),
        code: format!("{:?}", goal.code),
        last_feedback_seq: goal.last_feedback_seq,
        server_epoch: goal.server_epoch,
    }
}

fn map_action_detail_data(detail: ActionDetail) -> ActionDetailData {
    // `root_types` moved from `ActionSnapshot` to
    // `ActionDetail` (the snapshot path is now bfbs-free; detail is
    // where the bfbs is already fetched, so root types live there).
    let root_types = detail.root_types;
    let goals = detail.goals;
    let snapshot = detail.snapshot;
    let info = snapshot.info;
    ActionDetailData {
        name: snapshot.action_name,
        key_expr: snapshot.key_expr,
        server_node_name: info.server_node_name,
        server_epoch: info.server_epoch,
        supports_cancel: info.supports_cancel,
        max_concurrency: info.max_concurrency,
        result_ttl_ms: info.result_ttl_ms,
        dedup_ttl_ms: info.dedup_ttl_ms,
        schema_hash: info.schema_hash.to_hex(),
        send_goal_request_root: root_types.send_goal_request,
        send_goal_response_root: root_types.send_goal_response,
        cancel_goal_request_root: root_types.cancel_goal_request,
        cancel_goal_response_root: root_types.cancel_goal_response,
        feedback_root: root_types.feedback,
        public_status_root: root_types.public_status,
        get_result_request_root: root_types.get_result_request,
        get_result_response_root: root_types.get_result_response,
        goals: goals.into_iter().map(map_action_goal_row).collect(),
    }
}

fn map_node_detail_data(
    detail: aorta_introspect::client::NodeDetail,
    discovery_source: NodeDiscoverySource,
) -> NodeDetailData {
    let runtime_summary = detail
        .runtime
        .value
        .as_ref()
        .map(|runtime| NodeRuntimeSummaryData {
            timestamp_ns: runtime.timestamp_ns,
            collection_period_ms: runtime.collection_period_ms,
            publisher_metric_count: runtime.publisher_metric_count,
            subscriber_metric_count: runtime.subscriber_metric_count,
            service_metric_count: runtime.service_metric_count,
            client_metric_count: runtime.client_metric_count,
            publish_shm_hit_total: runtime.publish_shm_hit_total,
            publish_shm_fallback_total: runtime.publish_shm_fallback_total,
            publish_shm_required_fail_total: runtime.publish_shm_required_fail_total,
            loan_inflight_current: runtime.loan_inflight_current,
            loan_quota_exceeded_total: runtime.loan_quota_exceeded_total,
            subscriber_duplicate_drop_count: runtime.subscriber_duplicate_drop_count,
            subscriber_out_of_order_count: runtime.subscriber_out_of_order_count,
            subscriber_out_of_order_drop_count: runtime.subscriber_out_of_order_drop_count,
            subscriber_executor_drop_count: runtime.subscriber_executor_drop_count,
            subscriber_local_drop_count: runtime.subscriber_local_drop_count,
            subscriber_overflow_relay_enqueue_count: runtime
                .subscriber_overflow_relay_enqueue_count,
            subscriber_overflow_relay_drop_count: runtime.subscriber_overflow_relay_drop_count,
            service_request_received_count: runtime.service_request_received_count,
            service_reply_ok_count: runtime.service_reply_ok_count,
            service_reply_error_count: runtime.service_reply_error_count,
            service_reply_send_failure_count: runtime.service_reply_send_failure_count,
            service_request_dropped_count: runtime.service_request_dropped_count,
            service_in_flight_requests: runtime.service_in_flight_requests,
            client_call_count: runtime.client_call_count,
            client_success_count: runtime.client_success_count,
            client_timeout_count: runtime.client_timeout_count,
            client_concurrent_conflict_count: runtime.client_concurrent_conflict_count,
            client_error_count: runtime.client_error_count,
            client_retry_count: runtime.client_retry_count,
            executor_queued_events: runtime
                .executor
                .as_ref()
                .map(|executor| executor.queued_events),
            executor_queued_bytes: runtime
                .executor
                .as_ref()
                .map(|executor| executor.queued_bytes),
            executor_peak_queued_events: runtime
                .executor
                .as_ref()
                .map(|executor| executor.peak_queued_events),
            executor_peak_queued_bytes: runtime
                .executor
                .as_ref()
                .map(|executor| executor.peak_queued_bytes),
            executor_dropped_queue_full: runtime
                .executor
                .as_ref()
                .map(|executor| executor.dropped_queue_full),
            executor_dropped_queue_closed: runtime
                .executor
                .as_ref()
                .map(|executor| executor.dropped_queue_closed),
            executor_dropped_budget_events: runtime
                .executor
                .as_ref()
                .map(|executor| executor.dropped_budget_events),
            executor_dropped_budget_bytes: runtime
                .executor
                .as_ref()
                .map(|executor| executor.dropped_budget_bytes),
            executor_spin_count: runtime
                .executor
                .as_ref()
                .map(|executor| executor.spin_count),
        });
    let runtime_breakdown =
        detail
            .runtime_breakdown
            .value
            .as_ref()
            .map(|runtime| NodeRuntimeBreakdownData {
                publishers: runtime
                    .publishers
                    .iter()
                    .map(|entry| NodePublisherRuntimeEntryData {
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
                    .map(|entry| NodeSubscriberRuntimeEntryData {
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
                    .map(|entry| NodeServiceRuntimeEntryData {
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
                    .map(|entry| NodeClientRuntimeEntryData {
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
            });

    NodeDetailData {
        name: detail.node.name.as_str().to_string(),
        node_id: detail.node.id.as_str().to_string(),
        group: detail.node.group.as_str().to_string(),
        endpoint_count: count_if_available(
            &detail.node.owner_endpoints.provenance,
            detail.node.owner_endpoints.value.len(),
        ),
        topic_count: count_if_available(
            &detail.candidate_topics.provenance,
            detail.candidate_topics.value.len(),
        ),
        service_count: count_if_available(
            &detail.candidate_services.provenance,
            detail.candidate_services.value.len(),
        ),
        discovery_source,
        endpoint_ids: detail
            .node
            .owner_endpoints
            .value
            .iter()
            .map(|endpoint| endpoint.as_str().to_string())
            .collect(),
        topic_names: detail
            .candidate_topics
            .value
            .iter()
            .map(|topic| topic.as_str().to_string())
            .collect(),
        service_names: detail
            .candidate_services
            .value
            .iter()
            .map(|service| service.as_str().to_string())
            .collect(),
        published_topic_count: count_if_available(
            &detail.published_topics.provenance,
            detail.published_topics.value.len(),
        ),
        subscribed_topic_count: count_if_available(
            &detail.subscribed_topics.provenance,
            detail.subscribed_topics.value.len(),
        ),
        provided_service_count: count_if_available(
            &detail.provided_services.provenance,
            detail.provided_services.value.len(),
        ),
        client_service_count: count_if_available(
            &detail.client_services.provenance,
            detail.client_services.value.len(),
        ),
        published_topic_names: detail
            .published_topics
            .value
            .iter()
            .map(|topic| topic.as_str().to_string())
            .collect(),
        subscribed_topic_names: detail
            .subscribed_topics
            .value
            .iter()
            .map(|topic| topic.as_str().to_string())
            .collect(),
        provided_service_names: detail
            .provided_services
            .value
            .iter()
            .map(|service| service.as_str().to_string())
            .collect(),
        client_service_names: detail
            .client_services
            .value
            .iter()
            .map(|service| service.as_str().to_string())
            .collect(),
        endpoint_provenance: provenance_label(detail.node.owner_endpoints.provenance),
        topic_provenance: provenance_label(detail.candidate_topics.provenance),
        service_provenance: provenance_label(detail.candidate_services.provenance),
        published_topic_provenance: provenance_label(detail.published_topics.provenance),
        subscribed_topic_provenance: provenance_label(detail.subscribed_topics.provenance),
        provided_service_provenance: provenance_label(detail.provided_services.provenance),
        client_service_provenance: provenance_label(detail.client_services.provenance),
        endpoint_note: detail.node.owner_endpoints.note,
        topic_note: detail.candidate_topics.note,
        service_note: detail.candidate_services.note,
        published_topic_note: detail.published_topics.note,
        subscribed_topic_note: detail.subscribed_topics.note,
        provided_service_note: detail.provided_services.note,
        client_service_note: detail.client_services.note,
        runtime_summary,
        runtime_provenance: provenance_label(detail.runtime.provenance),
        runtime_note: detail.runtime.note,
        runtime_breakdown,
    }
}

fn map_zenoh_internals_data(overview: ZenohAdminOverview) -> ZenohInternalsData {
    let routers: Vec<_> = overview
        .routers
        .into_iter()
        .map(|router| ZenohRouterRow {
            zid: router.zid,
            whatami: router.whatami,
            version: router.version,
            session_count: router.session_count,
            locator_count: router.locators.len(),
            plugin_count: router.plugin_names.len(),
            metrics_available: router.metrics_available,
            metrics: router.metrics,
            locators: router.locators,
            plugin_names: router.plugin_names,
            raw_payload_utf8: router.raw_payload_utf8,
        })
        .collect();

    ZenohInternalsData {
        router_count: routers.len(),
        routers,
    }
}

fn map_zenoh_connectivity_data(overview: ZenohConnectivityOverview) -> ZenohConnectivityData {
    let local_locator_count = overview.local_locators.len();
    let transports: Vec<_> = overview
        .transports
        .into_iter()
        .map(|transport| ZenohTransportRow {
            remote_zid: transport.remote_zid,
            remote_whatami: transport.remote_whatami,
            supports_qos: transport.supports_qos,
            is_multicast: transport.is_multicast,
            supports_shm: transport.supports_shm,
        })
        .collect();
    let links: Vec<_> = overview
        .links
        .into_iter()
        .map(|link| ZenohLinkRow {
            transport_zid: link.transport_zid,
            src: link.src,
            dst: link.dst,
            group: link.group,
            mtu: link.mtu,
            is_streamed: link.is_streamed,
            interface_count: link.interfaces.len(),
            interfaces: link.interfaces,
            auth_identifier: link.auth_identifier,
            priorities: link.priorities,
            reliability: link.reliability,
        })
        .collect();

    ZenohConnectivityData {
        session_zid: overview.session_zid,
        local_locator_count,
        transport_count: transports.len(),
        link_count: links.len(),
        local_locators: overview.local_locators,
        transports,
        links,
    }
}

fn map_zenoh_connectivity_event_data(
    events: Vec<ZenohConnectivityEvent>,
) -> Vec<ZenohConnectivityEventData> {
    events
        .into_iter()
        .map(|event| {
            let kind = match event.kind {
                ZenohConnectivityEventKind::TransportOpened => {
                    ZenohConnectivityEventKindData::TransportOpened
                }
                ZenohConnectivityEventKind::TransportClosed => {
                    ZenohConnectivityEventKindData::TransportClosed
                }
                ZenohConnectivityEventKind::LinkAdded => ZenohConnectivityEventKindData::LinkAdded,
                ZenohConnectivityEventKind::LinkRemoved => {
                    ZenohConnectivityEventKindData::LinkRemoved
                }
            };

            let message = match (&kind, event.transport.as_ref(), event.link.as_ref()) {
                (
                    ZenohConnectivityEventKindData::TransportOpened
                    | ZenohConnectivityEventKindData::TransportClosed,
                    Some(transport),
                    _,
                ) => format!(
                    "transport {}: {} [{}] qos={} shm={} multicast={}",
                    if matches!(kind, ZenohConnectivityEventKindData::TransportOpened) {
                        "opened"
                    } else {
                        "closed"
                    },
                    transport.remote_zid,
                    transport.remote_whatami,
                    if transport.supports_qos { "yes" } else { "no" },
                    if transport.supports_shm { "yes" } else { "no" },
                    if transport.is_multicast { "yes" } else { "no" },
                ),
                (
                    ZenohConnectivityEventKindData::LinkAdded
                    | ZenohConnectivityEventKindData::LinkRemoved,
                    _,
                    Some(link),
                ) => format!(
                    "link {}: {} -> {} [{}] mtu={} rel={} ifaces={}",
                    if matches!(kind, ZenohConnectivityEventKindData::LinkAdded) {
                        "added"
                    } else {
                        "removed"
                    },
                    link.src,
                    link.dst,
                    link.transport_zid,
                    link.mtu,
                    link.reliability.as_deref().unwrap_or("n/a"),
                    link.interfaces.len(),
                ),
                (ZenohConnectivityEventKindData::TransportOpened, None, _)
                | (ZenohConnectivityEventKindData::TransportClosed, None, _) => {
                    "transport event".to_string()
                }
                (ZenohConnectivityEventKindData::LinkAdded, _, None)
                | (ZenohConnectivityEventKindData::LinkRemoved, _, None) => {
                    "link event".to_string()
                }
            };

            ZenohConnectivityEventData {
                sequence: event.sequence,
                timestamp_ns: event.timestamp_ns,
                kind,
                message,
            }
        })
        .collect()
}

pub async fn fetch_service_call_schema_data(
    client: &IntrospectClient,
    service_name: &str,
) -> Result<ServiceCallSchemaData> {
    let service = ServiceSuffix::new(service_name)?;
    let detail = client
        .get_service_detail(&service, ServiceDetailOptions::default())
        .await?;
    let schema = client.fetch_service_schema(&service).await?;
    let request_root_type = schema.request_root_type.clone();
    let response_root_type = schema.response_root_type.clone();
    let schema_hash = schema.service_hash.clone();
    let request_model = client.build_schema_model(
        &ResolvedSchema::Service(schema.clone()),
        SchemaRootSelector::ServiceRequest,
    )?;
    let starter_json = starter_json_from_schema_model(&request_model)?;

    Ok(ServiceCallSchemaData {
        service_name: detail.snapshot.service.as_str().to_string(),
        key_expr: detail.key_expr,
        provider_endpoint_count: detail.snapshot.provider_endpoints.value.len(),
        candidate_node_count: detail.snapshot.candidate_nodes.value.len(),
        candidate_nodes_note: detail.snapshot.candidate_nodes.note,
        schema_hash,
        request_root_type,
        response_root_type,
        request_model,
        starter_json,
    })
}

pub async fn invoke_service_call(
    client: &IntrospectClient,
    service_name: &str,
    request_json: &str,
) -> Result<ServiceCallResponseData> {
    let service = ServiceSuffix::new(service_name)?;
    let response = client
        .call_service(ServiceCallRequest {
            service,
            timeout: None,
            request_json: request_json.to_string(),
        })
        .await?;

    Ok(ServiceCallResponseData {
        response_json: response.response_json,
        response_utf8: response.response_utf8,
        response_hex: response.response_hex,
        decoded_with_schema: response.decoded_with_schema,
        schema_hash: response.schema.service_hash,
        response_root_type: response.schema.response_root_type,
    })
}

pub async fn fetch_action_call_schema_data(
    client: &IntrospectClient,
    action_name: &str,
) -> Result<ActionCallSchemaData> {
    let detail = map_action_detail_data(client.get_action_detail(action_name).await?);
    let (resolved, send_goal_request_root) =
        client.fetch_action_send_goal_schema(action_name).await?;
    let request_model = client.build_schema_model(
        &resolved,
        SchemaRootSelector::NamedRoot(send_goal_request_root.clone()),
    )?;
    let starter_json = action_goal_starter_json_from_schema_model(&request_model)?;

    Ok(ActionCallSchemaData {
        action_name: detail.name,
        key_expr: detail.key_expr,
        server_node_name: detail.server_node_name,
        supports_cancel: detail.supports_cancel,
        schema_hash: detail.schema_hash,
        send_goal_request_root,
        request_model,
        starter_json,
    })
}

pub async fn invoke_action_send_goal(
    client: &IntrospectClient,
    action_name: &str,
    request_json: &str,
) -> Result<ActionCallResponseData> {
    let result = client
        .send_action_goal(action_name, request_json, None)
        .await?;
    Ok(action_call_response_from_result(result))
}

pub async fn invoke_action_cancel(
    client: &IntrospectClient,
    action_name: &str,
    goal_id: &str,
    goal_secret: &str,
) -> Result<ActionCancelResponseData> {
    let result = client
        .cancel_action_goal(action_name, goal_id, goal_secret)
        .await?;
    Ok(ActionCancelResponseData {
        accepted: result.accepted,
        goal_id: result.goal_id,
        status: format!("{:?}", result.status),
        code: format!("{:?}", result.code),
        message: result.message,
    })
}

fn action_call_response_from_result(result: ActionGoalSendResult) -> ActionCallResponseData {
    ActionCallResponseData {
        accepted: result.accepted,
        goal_id: result.goal_id,
        goal_secret: result.goal_secret,
        status: format!("{:?}", result.status),
        code: format!("{:?}", result.code),
        message: result.message,
        server_epoch: result.server_epoch,
    }
}

/// Build the starter *goal payload* JSON from a `SendGoalRequest`-rooted schema
/// model: the object under the `goal` field (the user edits the goal body, not
/// the `goal_id`/`goal` wrapper). Falls back to an empty object when the model
/// has no `goal` field (defensive — every action schema has one).
fn action_goal_starter_json_from_schema_model(model: &SchemaModel) -> Result<String> {
    let goal_field = model
        .fields
        .iter()
        .find(|field| field.parent_path.is_none() && field.name == "goal");
    let value = match goal_field {
        Some(field) => object_value_for_parent(model, Some(field.path.as_str())),
        None => Value::Object(Map::new()),
    };
    Ok(serde_json::to_string_pretty(&value)?)
}

pub fn field_template_json_from_schema_model(
    model: &SchemaModel,
    field_path: &str,
) -> Result<String> {
    let value = field_template_value_from_schema_model(model, field_path)?;
    Ok(serde_json::to_string_pretty(&value)?)
}

pub fn merge_field_template_into_request_json(
    request_json: &str,
    model: &SchemaModel,
    field_path: &str,
) -> Result<String> {
    let patch = field_template_value_from_schema_model(model, field_path)?;
    let mut current = if request_json.trim().is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_str(request_json)?
    };

    merge_json_value(&mut current, patch);
    Ok(serde_json::to_string_pretty(&current)?)
}

/// Apply a schema-hint template to the Action Call editor, which holds only the
/// GOAL PAYLOAD (the `goal` subtree of `SendGoalRequest`). The schema model
/// describes the full request, so [`field_template_value_from_schema_model`]
/// yields `{ "goal": <value> }`; strip that wrapper so the value merges at the
/// goal-payload root. Without this a `goal.*` hint nests a second `goal`, and
/// `send_action_goal`'s own wrapper then produces `goal.goal…`. Fields outside
/// `goal` (e.g. `goal_id`) are not part of this editor and are rejected.
pub fn merge_goal_field_template_into_goal_json(
    goal_json: &str,
    model: &SchemaModel,
    field_path: &str,
) -> Result<String> {
    if field_path != "goal" && !field_path.starts_with("goal.") {
        return Err(anyhow::anyhow!(
            "field '{field_path}' is not part of the goal payload"
        ));
    }
    let patch = field_template_value_from_schema_model(model, field_path)?;
    let inner = match patch {
        Value::Object(mut map) => map.remove("goal").ok_or_else(|| {
            anyhow::anyhow!("schema template for '{field_path}' is missing the goal wrapper")
        })?,
        other => other,
    };
    let mut current = if goal_json.trim().is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_str(goal_json)?
    };
    merge_json_value(&mut current, inner);
    Ok(serde_json::to_string_pretty(&current)?)
}

fn count_if_available(provenance: &Provenance, count: usize) -> Option<usize> {
    if *provenance == Provenance::Unavailable {
        None
    } else {
        Some(count)
    }
}

fn provenance_label(provenance: Provenance) -> &'static str {
    match provenance {
        Provenance::Exact => "exact",
        Provenance::Declared => "declared",
        Provenance::Observed => "observed",
        Provenance::Resolved => "resolved",
        Provenance::Inferred => "inferred",
        Provenance::Unavailable => "unavailable",
    }
}

fn schema_availability_from_listing(schema_present: bool) -> SchemaAvailability {
    if schema_present {
        SchemaAvailability::Confirmed
    } else {
        // Exact schema confirmation can still be surfaced by callers that already know a
        // listing row came from a concrete schema match. Otherwise, list views should treat
        // absence here as non-authoritative and rely on detail views for exact resolution.
        SchemaAvailability::Unknown
    }
}

fn exact_schema_availability_from_topic_detail(
    detail: &aorta_introspect::client::TopicDetail,
) -> ExactSchemaAvailability {
    match detail.schema.value.as_ref() {
        Some(schema) => ExactSchemaAvailability::Confirmed {
            hash: schema.schema_hash.clone(),
        },
        None => ExactSchemaAvailability::Missing,
    }
}

fn exact_schema_availability_from_service_detail(
    detail: &aorta_introspect::client::ServiceDetail,
) -> ExactSchemaAvailability {
    match detail.schema.value.as_ref() {
        Some(schema) => ExactSchemaAvailability::Confirmed {
            hash: schema.service_hash.clone(),
        },
        None => ExactSchemaAvailability::Missing,
    }
}

fn starter_json_from_schema_model(model: &SchemaModel) -> Result<String> {
    let value = object_value_for_parent(model, None);
    Ok(serde_json::to_string_pretty(&value)?)
}

fn field_template_value_from_schema_model(model: &SchemaModel, field_path: &str) -> Result<Value> {
    let field = model
        .fields
        .iter()
        .find(|field| field.path == field_path)
        .ok_or_else(|| anyhow::anyhow!("field '{}' was not found in schema model", field_path))?;

    let mut wrapped = starter_value_for_field(model, field);
    let mut current_path = field.path.as_str();

    while let Some(parent_path) = model
        .fields
        .iter()
        .find(|candidate| candidate.path == current_path)
        .and_then(|candidate| candidate.parent_path.as_deref())
    {
        let field_name = current_path
            .rsplit('.')
            .next()
            .unwrap_or(current_path)
            .to_string();
        let mut object = Map::new();
        object.insert(field_name, wrapped);
        wrapped = Value::Object(object);
        current_path = parent_path;
    }

    let root_name = current_path
        .split('.')
        .next()
        .expect("field path must include a root segment")
        .to_string();
    let final_value = if current_path == field.path {
        starter_value_for_field(model, field)
    } else {
        wrapped
    };

    let mut root = Map::new();
    root.insert(root_name, final_value);
    Ok(Value::Object(root))
}

fn object_value_for_parent(model: &SchemaModel, parent_path: Option<&str>) -> Value {
    let object = model
        .fields
        .iter()
        .filter(|field| field.parent_path.as_deref() == parent_path)
        .fold(Map::new(), |mut out, field| {
            out.insert(field.name.clone(), starter_value_for_field(model, field));
            out
        });
    Value::Object(object)
}

fn starter_value_for_field(model: &SchemaModel, field: &SchemaField) -> Value {
    if field.repeated
        || matches!(
            field.type_kind,
            SchemaTypeKind::Vector | SchemaTypeKind::Array
        )
    {
        return Value::Array(Vec::new());
    }

    let has_children = model
        .fields
        .iter()
        .any(|candidate| candidate.parent_path.as_deref() == Some(field.path.as_str()));
    if has_children {
        return object_value_for_parent(model, Some(field.path.as_str()));
    }

    if let Some(default_json) = field.default_json.clone() {
        return default_json;
    }

    if let Some(enum_value) = field.enum_values.first() {
        return Value::String(enum_value.name.clone());
    }

    match field.type_kind {
        SchemaTypeKind::Bool => Value::Bool(false),
        SchemaTypeKind::Integer => Value::from(0),
        SchemaTypeKind::Float => Value::from(0.0),
        SchemaTypeKind::String => Value::String(String::new()),
        SchemaTypeKind::Bytes => Value::Array(Vec::new()),
        SchemaTypeKind::Table | SchemaTypeKind::Struct => Value::Object(Map::new()),
        SchemaTypeKind::Enum => Value::String(String::new()),
        SchemaTypeKind::Union => Value::Null,
        SchemaTypeKind::Vector | SchemaTypeKind::Array => Value::Array(Vec::new()),
    }
}

fn merge_json_value(current: &mut Value, patch: Value) {
    match (current, patch) {
        (Value::Object(current), Value::Object(patch)) => {
            for (key, patch_value) in patch {
                if let Some(existing) = current.get_mut(&key) {
                    merge_json_value(existing, patch_value);
                } else {
                    current.insert(key, patch_value);
                }
            }
        }
        (current, patch) => {
            *current = patch;
        }
    }
}

#[cfg(test)]
mod tests {
    use aorta_introspect::client::{
        CandidateNodeRef, ContextEntry, NodeClientRuntimeEntry, NodeDetail,
        NodeExecutorRuntimeSummary, NodeListResult, NodePublisherRuntimeEntry,
        NodeRuntimeBreakdown, NodeRuntimeSummary, NodeServiceRuntimeEntry, NodeSnapshot,
        NodeSubscriberRuntimeEntry, Provenance, ServiceDetail, ServiceRuntimeSummary,
        ServiceSnapshot, TopicDetail, TopicRuntimeSummary, TopicSnapshot, WithProvenance,
        ZenohAdminOverview, ZenohConnectivityEvent, ZenohConnectivityEventKind,
        ZenohConnectivityOverview, ZenohLinkOverview, ZenohRouterMetrics, ZenohRouterOverview,
        ZenohTransportOverview,
    };
    use aorta_introspect::ids::{
        EndpointId, GroupName, NodeId, NodeName, ServiceSuffix, TopicSuffix,
    };
    use aorta_introspect::schema::{
        ResolvedTopicSchema, ResolvedTopicSchemaKind, SchemaEnumValue, SchemaField, SchemaModel,
        SchemaTypeKind,
    };
    use serde_json::{json, Value};

    use super::{
        build_context_rows, build_dashboard_data, context_freshness, count_if_available,
        exact_schema_availability_from_service_detail, exact_schema_availability_from_topic_detail,
        field_template_json_from_schema_model, map_node_detail_data, map_service_detail_data,
        map_topic_detail_data, map_topic_row, map_zenoh_connectivity_data,
        map_zenoh_connectivity_event_data, map_zenoh_internals_data,
        merge_field_template_into_request_json, merge_goal_field_template_into_goal_json,
        schema_availability_from_listing, starter_json_from_schema_model, CapabilityState,
        ExactSchemaAvailability, FreshnessState, HealthLevel, NodeDiscoverySource,
        SchemaAvailability,
    };

    fn context_entry(key: &str, instance: &str, expires_at_ns: u64) -> ContextEntry {
        ContextEntry {
            key: key.to_string(),
            value_text: "running".to_string(),
            kind: "String",
            source_node: "demo_node".to_string(),
            source_instance_id: instance.to_string(),
            expires_at_ns,
        }
    }

    #[test]
    fn build_context_rows_flags_conflict_for_multiple_writers_on_same_key() {
        let entries = vec![
            context_entry("aorta/default/ctx/demo/state", "instance-aaaa", 0),
            context_entry("aorta/default/ctx/demo/state", "instance-bbbb", 0),
            context_entry("aorta/default/ctx/nav/mode", "instance-cccc", 0),
        ];
        let rows = build_context_rows(entries, 100);
        assert_eq!(rows.len(), 3);
        // Both writers of demo/state are flagged.
        assert!(rows[0].conflict);
        assert!(rows[1].conflict);
        // The single-writer key is not.
        assert!(!rows[2].conflict);
    }

    #[test]
    fn build_context_rows_does_not_flag_repeated_same_instance() {
        // Same key, same instance id reported twice (e.g. duplicate reply) is
        // NOT a conflict — conflict requires *distinct* instance ids.
        let entries = vec![
            context_entry("aorta/default/ctx/demo/state", "instance-aaaa", 0),
            context_entry("aorta/default/ctx/demo/state", "instance-aaaa", 0),
        ];
        let rows = build_context_rows(entries, 100);
        assert!(rows.iter().all(|row| !row.conflict));
    }

    #[test]
    fn build_context_rows_computes_freshness_from_expiry() {
        let entries = vec![
            context_entry("aorta/default/ctx/a", "i1", 0), // never expires
            context_entry("aorta/default/ctx/b", "i2", 500), // future
            context_entry("aorta/default/ctx/c", "i3", 50), // past
        ];
        let rows = build_context_rows(entries, 100);
        assert_eq!(rows[0].freshness, "fresh");
        assert_eq!(rows[1].freshness, "fresh");
        assert_eq!(rows[2].freshness, "stale");
    }

    #[test]
    fn context_freshness_matches_cli_semantics() {
        assert_eq!(context_freshness(0, 1_000), "fresh");
        assert_eq!(context_freshness(2_000, 1_000), "fresh");
        assert_eq!(context_freshness(1_000, 1_000), "stale");
        assert_eq!(context_freshness(500, 1_000), "stale");
    }

    #[test]
    fn missing_listing_entry_maps_to_unknown_not_false_absence() {
        assert_eq!(
            schema_availability_from_listing(false),
            SchemaAvailability::Unknown
        );
    }

    #[test]
    fn present_listing_entry_maps_to_confirmed() {
        assert_eq!(
            schema_availability_from_listing(true),
            SchemaAvailability::Confirmed
        );
    }

    fn topic_snapshot_with_schema(schema_present: bool) -> TopicSnapshot {
        TopicSnapshot {
            topic: TopicSuffix::new("camera/front").expect("topic"),
            present_in_admin: true,
            schema_present,
            provider_endpoints: WithProvenance {
                value: vec![EndpointId::new("ep1").expect("endpoint")],
                provenance: Provenance::Declared,
                note: None,
            },
            candidate_nodes: WithProvenance {
                value: Vec::new(),
                provenance: Provenance::Exact,
                note: None,
            },
            publisher_node_count: WithProvenance {
                value: Some(2),
                provenance: Provenance::Exact,
                note: None,
            },
            subscriber_node_count: WithProvenance {
                value: Some(1),
                provenance: Provenance::Exact,
                note: None,
            },
            runtime: WithProvenance {
                value: None,
                provenance: Provenance::Unavailable,
                note: None,
            },
        }
    }

    #[test]
    fn map_topic_row_populates_schema_availability_column_field() {
        // Confirmed when the schema is present by key on the wire snapshot.
        let confirmed = map_topic_row(topic_snapshot_with_schema(true));
        assert_eq!(
            confirmed.schema_availability,
            SchemaAvailability::Confirmed,
            "schema_present=true should map to the Confirmed schema column value"
        );

        // Unknown when the schema is not confirmed by the listing.
        let unknown = map_topic_row(topic_snapshot_with_schema(false));
        assert_eq!(
            unknown.schema_availability,
            SchemaAvailability::Unknown,
            "schema_present=false should map to the Unknown schema column value"
        );
    }

    #[test]
    fn dashboard_mapping_summarizes_runtime_health_and_hot_resources() {
        let dashboard = build_dashboard_data(
            vec![TopicSnapshot {
                topic: TopicSuffix::new("camera/front").expect("topic"),
                present_in_admin: true,
                schema_present: true,
                provider_endpoints: WithProvenance {
                    value: vec![EndpointId::new("ep1").expect("endpoint")],
                    provenance: Provenance::Declared,
                    note: None,
                },
                candidate_nodes: WithProvenance {
                    value: vec![CandidateNodeRef {
                        name: NodeName::new("camera_node").expect("node name"),
                        id: NodeId::new("11111111111111111111111111111111").expect("node id"),
                        matched_endpoint_ids: vec![EndpointId::new("ep1").expect("endpoint")],
                    }],
                    provenance: Provenance::Exact,
                    note: None,
                },
                publisher_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                subscriber_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                runtime: WithProvenance {
                    value: Some(TopicRuntimeSummary {
                        latest_timestamp_ns: 123,
                        collection_period_ms: 1000,
                        publisher_metric_count: 1,
                        subscriber_metric_count: 1,
                        publish_shm_hit_total: 0,
                        publish_shm_fallback_total: 0,
                        publish_shm_required_fail_total: 0,
                        loan_inflight_current: 0,
                        loan_quota_exceeded_total: 0,
                        subscriber_duplicate_drop_count: 0,
                        subscriber_out_of_order_count: 0,
                        subscriber_out_of_order_drop_count: 0,
                        subscriber_executor_drop_count: 2,
                        subscriber_local_drop_count: 0,
                        subscriber_overflow_relay_enqueue_count: 0,
                        subscriber_overflow_relay_drop_count: 0,
                    }),
                    provenance: Provenance::Exact,
                    note: None,
                },
            }],
            vec![ServiceSnapshot {
                service: ServiceSuffix::new("nav/plan").expect("service"),
                present_in_admin: true,
                schema_present: true,
                provider_endpoints: WithProvenance {
                    value: vec![EndpointId::new("svc1").expect("endpoint")],
                    provenance: Provenance::Declared,
                    note: None,
                },
                candidate_nodes: WithProvenance {
                    value: vec![CandidateNodeRef {
                        name: NodeName::new("planner").expect("node name"),
                        id: NodeId::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").expect("node id"),
                        matched_endpoint_ids: vec![EndpointId::new("svc1").expect("endpoint")],
                    }],
                    provenance: Provenance::Exact,
                    note: None,
                },
                provider_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                client_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                runtime: WithProvenance {
                    value: Some(ServiceRuntimeSummary {
                        latest_timestamp_ns: 456,
                        collection_period_ms: 1000,
                        service_metric_count: 1,
                        client_metric_count: 1,
                        service_request_received_count: 10,
                        service_request_dispatched_count: 10,
                        service_reply_ok_count: 10,
                        service_reply_error_count: 0,
                        service_reply_send_failure_count: 0,
                        service_request_dropped_count: 0,
                        service_in_flight_requests: 0,
                        client_call_count: 10,
                        client_success_count: 10,
                        client_timeout_count: 0,
                        client_concurrent_conflict_count: 0,
                        client_error_count: 0,
                        client_retry_count: 0,
                        client_last_call_latency_ns: 0,
                        client_last_call_finished_ns: 0,
                        service_last_request_ns: 0,
                        service_last_reply_ns: 0,
                    }),
                    provenance: Provenance::Exact,
                    note: None,
                },
            }],
            NodeListResult {
                source: NodeDiscoverySource::TelemetrySnapshot,
                nodes: vec![NodeSnapshot {
                    name: NodeName::new("camera_node").expect("node name"),
                    id: NodeId::new("11111111111111111111111111111111").expect("node id"),
                    group: GroupName::new("default").expect("group"),
                    owner_endpoints: WithProvenance {
                        value: Vec::new(),
                        provenance: Provenance::Unavailable,
                        note: None,
                    },
                    candidate_topic_count: WithProvenance {
                        value: Some(1),
                        provenance: Provenance::Exact,
                        note: None,
                    },
                    candidate_service_count: WithProvenance {
                        value: Some(1),
                        provenance: Provenance::Exact,
                        note: None,
                    },
                    runtime: WithProvenance {
                        value: Some(NodeRuntimeSummary {
                            timestamp_ns: 789,
                            collection_period_ms: 1000,
                            publisher_metric_count: 1,
                            subscriber_metric_count: 1,
                            service_metric_count: 0,
                            client_metric_count: 0,
                            publish_shm_hit_total: 0,
                            publish_shm_fallback_total: 0,
                            publish_shm_required_fail_total: 0,
                            loan_inflight_current: 0,
                            loan_quota_exceeded_total: 0,
                            subscriber_duplicate_drop_count: 0,
                            subscriber_out_of_order_count: 0,
                            subscriber_out_of_order_drop_count: 0,
                            subscriber_executor_drop_count: 2,
                            subscriber_local_drop_count: 0,
                            subscriber_overflow_relay_enqueue_count: 0,
                            subscriber_overflow_relay_drop_count: 0,
                            service_request_received_count: 0,
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
                            executor: Some(NodeExecutorRuntimeSummary {
                                queued_events: 0,
                                queued_bytes: 0,
                                peak_queued_events: 0,
                                peak_queued_bytes: 0,
                                dropped_queue_full: 0,
                                dropped_queue_closed: 0,
                                dropped_budget_events: 0,
                                dropped_budget_bytes: 0,
                                spin_count: 0,
                            }),
                        }),
                        provenance: Provenance::Exact,
                        note: None,
                    },
                }],
            },
        );

        assert_eq!(dashboard.overview.health.overall, HealthLevel::Error);
        assert_eq!(dashboard.overview.health.runtime_topic_coverage, 1);
        assert_eq!(dashboard.overview.health.runtime_service_coverage, 1);
        assert_eq!(dashboard.overview.health.runtime_node_coverage, 1);
        assert_eq!(
            dashboard.overview.health.runtime_capability,
            CapabilityState::Full
        );
        assert_eq!(dashboard.overview.health.freshness, FreshnessState::Fresh);
        assert_eq!(dashboard.topics[0].health, HealthLevel::Error);
        assert_eq!(dashboard.services[0].health, HealthLevel::Healthy);
        assert_eq!(dashboard.nodes[0].freshness, FreshnessState::Fresh);
        assert_eq!(
            dashboard.overview.highlighted_topics[0].name,
            "camera/front"
        );
        assert_eq!(
            dashboard.overview.highlighted_topics[0].reason,
            "runtime errors=2"
        );
    }

    #[test]
    fn dashboard_mapping_marks_admin_fallback_as_partial_capability() {
        let dashboard = build_dashboard_data(
            Vec::new(),
            Vec::new(),
            NodeListResult {
                source: NodeDiscoverySource::AdminSnapshot,
                nodes: vec![NodeSnapshot {
                    name: NodeName::new("planner").expect("node name"),
                    id: NodeId::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").expect("node id"),
                    group: GroupName::new("default").expect("group"),
                    owner_endpoints: WithProvenance {
                        value: vec![EndpointId::new("ep1").expect("endpoint")],
                        provenance: Provenance::Declared,
                        note: None,
                    },
                    candidate_topic_count: WithProvenance {
                        value: Some(2),
                        provenance: Provenance::Inferred,
                        note: None,
                    },
                    candidate_service_count: WithProvenance {
                        value: Some(1),
                        provenance: Provenance::Inferred,
                        note: None,
                    },
                    runtime: WithProvenance {
                        value: None,
                        provenance: Provenance::Unavailable,
                        note: Some(
                            "runtime metrics unavailable outside telemetry snapshot mode"
                                .to_string(),
                        ),
                    },
                }],
            },
        );

        assert_eq!(
            dashboard.overview.health.discovery_capability,
            CapabilityState::Partial
        );
        assert_eq!(
            dashboard.overview.health.runtime_capability,
            CapabilityState::Partial
        );
        assert_eq!(dashboard.nodes[0].capability, CapabilityState::Partial);
        assert_eq!(
            dashboard.nodes[0].status_summary,
            "runtime metrics unavailable outside telemetry snapshot mode"
        );
    }

    #[test]
    fn topic_detail_with_resolved_schema_maps_to_confirmed_hash() {
        let detail = TopicDetail {
            key_expr: "aorta/default/pub/demo/all_types".to_string(),
            snapshot: TopicSnapshot {
                topic: TopicSuffix::new("demo/all_types").expect("topic"),
                present_in_admin: true,
                schema_present: false,
                provider_endpoints: WithProvenance {
                    value: vec![EndpointId::new("ep1").expect("endpoint")],
                    provenance: Provenance::Declared,
                    note: None,
                },
                candidate_nodes: WithProvenance {
                    value: vec![CandidateNodeRef {
                        name: NodeName::new("talker").expect("node name"),
                        id: NodeId::new("11111111111111111111111111111111").expect("node id"),
                        matched_endpoint_ids: vec![EndpointId::new("ep1").expect("endpoint")],
                    }],
                    provenance: Provenance::Inferred,
                    note: None,
                },
                publisher_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                subscriber_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                runtime: WithProvenance {
                    value: None,
                    provenance: Provenance::Unavailable,
                    note: Some(
                        "topic runtime metrics unavailable because telemetry rows are absent"
                            .to_string(),
                    ),
                },
            },
            schema: WithProvenance {
                value: Some(ResolvedTopicSchema {
                    key_suffix: "pub/demo/all_types".to_string(),
                    kind: ResolvedTopicSchemaKind::Topic,
                    schema_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_string(),
                    bfbs: vec![1, 2, 3, 4].into(),
                    default_root_type: Some("Demo.AllTypes".to_string()),
                }),
                provenance: Provenance::Resolved,
                note: None,
            },
            approx_rate: WithProvenance {
                value: None,
                provenance: Provenance::Unavailable,
                note: None,
            },
            publisher_nodes: WithProvenance {
                value: vec![CandidateNodeRef {
                    name: NodeName::new("talker").expect("node name"),
                    id: NodeId::new("11111111111111111111111111111111").expect("node id"),
                    matched_endpoint_ids: vec![],
                }],
                provenance: Provenance::Exact,
                note: None,
            },
            subscriber_nodes: WithProvenance {
                value: vec![CandidateNodeRef {
                    name: NodeName::new("listener").expect("node name"),
                    id: NodeId::new("22222222222222222222222222222222").expect("node id"),
                    matched_endpoint_ids: vec![],
                }],
                provenance: Provenance::Exact,
                note: None,
            },
            runtime: WithProvenance {
                value: Some(aorta_introspect::client::TopicRuntimeSummary {
                    latest_timestamp_ns: 456,
                    collection_period_ms: 1000,
                    publisher_metric_count: 1,
                    subscriber_metric_count: 1,
                    publish_shm_hit_total: 10,
                    publish_shm_fallback_total: 2,
                    publish_shm_required_fail_total: 1,
                    loan_inflight_current: 3,
                    loan_quota_exceeded_total: 4,
                    subscriber_duplicate_drop_count: 1,
                    subscriber_out_of_order_count: 2,
                    subscriber_out_of_order_drop_count: 1,
                    subscriber_executor_drop_count: 3,
                    subscriber_local_drop_count: 4,
                    subscriber_overflow_relay_enqueue_count: 5,
                    subscriber_overflow_relay_drop_count: 6,
                }),
                provenance: Provenance::Exact,
                note: None,
            },
        };

        let mapped = map_topic_detail_data(detail.clone());
        let availability = exact_schema_availability_from_topic_detail(&detail);
        assert!(matches!(
            availability,
            ExactSchemaAvailability::Confirmed { .. }
        ));
        assert_eq!(mapped.schema_provenance, "resolved");
        assert_eq!(mapped.provider_endpoint_provenance, "declared");
        assert_eq!(mapped.candidate_node_provenance, "inferred");
        assert_eq!(mapped.publisher_node_provenance, "exact");
        assert_eq!(mapped.subscriber_node_provenance, "exact");
        assert_eq!(mapped.provider_endpoint_ids, vec!["ep1"]);
        assert_eq!(
            mapped.candidate_node_names,
            vec!["talker (11111111111111111111111111111111)"]
        );
        assert_eq!(
            mapped.publisher_node_names,
            vec!["talker (11111111111111111111111111111111)"]
        );
        assert_eq!(
            mapped.subscriber_node_names,
            vec!["listener (22222222222222222222222222222222)"]
        );
        assert_eq!(mapped.runtime_provenance, "exact");
        let runtime = mapped.runtime_summary.expect("runtime summary");
        assert_eq!(runtime.latest_timestamp_ns, 456);
    }

    #[test]
    fn service_detail_without_schema_maps_to_missing() {
        let detail = ServiceDetail {
            key_expr: "aorta/default/srv/cpp/echo".to_string(),
            snapshot: ServiceSnapshot {
                service: ServiceSuffix::new("cpp/echo").expect("service"),
                present_in_admin: true,
                schema_present: false,
                provider_endpoints: WithProvenance {
                    value: Vec::new(),
                    provenance: Provenance::Declared,
                    note: None,
                },
                candidate_nodes: WithProvenance {
                    value: Vec::new(),
                    provenance: Provenance::Inferred,
                    note: None,
                },
                provider_node_count: WithProvenance {
                    value: Some(0),
                    provenance: Provenance::Unavailable,
                    note: None,
                },
                client_node_count: WithProvenance {
                    value: Some(0),
                    provenance: Provenance::Unavailable,
                    note: None,
                },
                runtime: WithProvenance {
                    value: None,
                    provenance: Provenance::Unavailable,
                    note: Some(
                        "service runtime metrics unavailable because telemetry rows are absent"
                            .to_string(),
                    ),
                },
            },
            schema: WithProvenance {
                value: None,
                provenance: Provenance::Unavailable,
                note: Some("service schema not found via schema_by_key".to_string()),
            },
            provider_nodes: WithProvenance {
                value: Vec::new(),
                provenance: Provenance::Unavailable,
                note: Some(
                    "provider nodes unavailable because service was not discovered in telemetry snapshot"
                        .to_string(),
                ),
            },
            client_nodes: WithProvenance {
                value: Vec::new(),
                provenance: Provenance::Unavailable,
                note: Some(
                    "client nodes unavailable because service was not discovered in telemetry snapshot"
                        .to_string(),
                ),
            },
            runtime: WithProvenance {
                value: None,
                provenance: Provenance::Unavailable,
                note: Some("service runtime metrics unavailable because telemetry rows are absent".to_string()),
            },
        };

        assert_eq!(
            exact_schema_availability_from_service_detail(&detail),
            ExactSchemaAvailability::Missing
        );
    }

    #[test]
    fn service_detail_mapping_includes_schema_roots_and_candidates() {
        let detail = ServiceDetail {
            key_expr: "aorta/default/srv/nav/plan".to_string(),
            snapshot: ServiceSnapshot {
                service: ServiceSuffix::new("nav/plan").expect("service"),
                present_in_admin: true,
                schema_present: true,
                provider_endpoints: WithProvenance {
                    value: vec![EndpointId::new("ep1").expect("endpoint")],
                    provenance: Provenance::Declared,
                    note: None,
                },
                candidate_nodes: WithProvenance {
                    value: vec![CandidateNodeRef {
                        name: NodeName::new("planner").expect("node name"),
                        id: NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id"),
                        matched_endpoint_ids: vec![EndpointId::new("ep1").expect("endpoint")],
                    }],
                    provenance: Provenance::Inferred,
                    note: Some("candidate nodes inferred from endpoint ownership".to_string()),
                },
                provider_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                client_node_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Exact,
                    note: None,
                },
                runtime: WithProvenance {
                    value: None,
                    provenance: Provenance::Unavailable,
                    note: Some(
                        "service runtime metrics unavailable because telemetry rows are absent"
                            .to_string(),
                    ),
                },
            },
            schema: WithProvenance {
                value: Some(aorta_introspect::schema::ResolvedServiceSchema {
                    key_suffix: "srv/nav/plan".to_string(),
                    service_hash:
                        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                            .to_string(),
                    bfbs: vec![1, 2, 3].into(),
                    request_root_type: Some("demo.Request".to_string()),
                    response_root_type: Some("demo.Response".to_string()),
                }),
                provenance: Provenance::Resolved,
                note: None,
            },
            provider_nodes: WithProvenance {
                value: vec![CandidateNodeRef {
                    name: NodeName::new("planner").expect("node name"),
                    id: NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id"),
                    matched_endpoint_ids: vec![],
                }],
                provenance: Provenance::Exact,
                note: None,
            },
            client_nodes: WithProvenance {
                value: vec![CandidateNodeRef {
                    name: NodeName::new("controller").expect("node name"),
                    id: NodeId::new("99999999999999999999999999999999").expect("node id"),
                    matched_endpoint_ids: vec![],
                }],
                provenance: Provenance::Exact,
                note: None,
            },
            runtime: WithProvenance {
                value: Some(aorta_introspect::client::ServiceRuntimeSummary {
                    latest_timestamp_ns: 456,
                    collection_period_ms: 1000,
                    service_metric_count: 1,
                    client_metric_count: 1,
                    service_request_received_count: 11,
                    service_request_dispatched_count: 10,
                    service_reply_ok_count: 9,
                    service_reply_error_count: 1,
                    service_reply_send_failure_count: 2,
                    service_request_dropped_count: 3,
                    service_in_flight_requests: 4,
                    client_call_count: 12,
                    client_success_count: 8,
                    client_timeout_count: 1,
                    client_concurrent_conflict_count: 2,
                    client_error_count: 3,
                    client_retry_count: 4,
                    client_last_call_latency_ns: 1_500_000,
                    client_last_call_finished_ns: 460,
                    service_last_request_ns: 400,
                    service_last_reply_ns: 420,
                }),
                provenance: Provenance::Exact,
                note: None,
            },
        };

        let mapped = map_service_detail_data(detail);
        assert_eq!(
            mapped.schema_hash.as_deref(),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        );
        assert_eq!(mapped.schema_provenance, "resolved");
        assert_eq!(mapped.provider_endpoint_provenance, "declared");
        assert_eq!(mapped.candidate_node_provenance, "inferred");
        assert_eq!(mapped.provider_node_provenance, "exact");
        assert_eq!(mapped.client_node_provenance, "exact");
        assert_eq!(mapped.request_root_type.as_deref(), Some("demo.Request"));
        assert_eq!(mapped.response_root_type.as_deref(), Some("demo.Response"));
        assert_eq!(mapped.provider_endpoint_ids, vec!["ep1"]);
        assert_eq!(
            mapped.candidate_node_names,
            vec!["planner (a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6)"]
        );
        assert_eq!(
            mapped.provider_node_names,
            vec!["planner (a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6)"]
        );
        assert_eq!(
            mapped.client_node_names,
            vec!["controller (99999999999999999999999999999999)"]
        );
        assert_eq!(mapped.runtime_provenance, "exact");
        let runtime = mapped.runtime_summary.expect("runtime summary");
        assert_eq!(runtime.latest_timestamp_ns, 456);
        assert_eq!(runtime.service_request_received_count, 11);
        assert_eq!(runtime.client_retry_count, 4);
    }

    #[test]
    fn count_if_available_returns_none_for_unavailable_data() {
        assert_eq!(count_if_available(&Provenance::Unavailable, 3), None);
        assert_eq!(count_if_available(&Provenance::Inferred, 3), Some(3));
        assert_eq!(count_if_available(&Provenance::Exact, 3), Some(3));
    }

    #[test]
    fn provenance_label_maps_exact_to_exact() {
        assert_eq!(super::provenance_label(Provenance::Exact), "exact");
    }

    #[test]
    fn node_detail_mapping_preserves_resource_names_and_provenance() {
        let detail = NodeDetail {
            node: NodeSnapshot {
                name: NodeName::new("planner").expect("node name"),
                id: NodeId::new("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6").expect("node id"),
                group: GroupName::new("default").expect("group"),
                owner_endpoints: WithProvenance {
                    value: vec![EndpointId::new("ep1").expect("endpoint")],
                    provenance: Provenance::Declared,
                    note: Some("admin owners are authoritative at endpoint level".to_string()),
                },
                candidate_topic_count: WithProvenance {
                    value: Some(2),
                    provenance: Provenance::Inferred,
                    note: None,
                },
                candidate_service_count: WithProvenance {
                    value: Some(1),
                    provenance: Provenance::Inferred,
                    note: None,
                },
                runtime: WithProvenance {
                    value: None,
                    provenance: Provenance::Unavailable,
                    note: Some("runtime metrics unavailable in admin snapshot mode".to_string()),
                },
            },
            candidate_topics: WithProvenance {
                value: vec![
                    TopicSuffix::new("camera/front").expect("topic"),
                    TopicSuffix::new("imu/data").expect("topic"),
                ],
                provenance: Provenance::Inferred,
                note: Some("candidate topics inferred by endpoint ownership".to_string()),
            },
            candidate_services: WithProvenance {
                value: vec![ServiceSuffix::new("nav/plan").expect("service")],
                provenance: Provenance::Inferred,
                note: Some("candidate services inferred by endpoint ownership".to_string()),
            },
            published_topics: WithProvenance {
                value: vec![TopicSuffix::new("camera/front").expect("topic")],
                provenance: Provenance::Inferred,
                note: Some("published topics inferred by endpoint ownership".to_string()),
            },
            subscribed_topics: WithProvenance {
                value: Vec::new(),
                provenance: Provenance::Unavailable,
                note: Some("subscriber ownership unavailable in admin snapshot mode".to_string()),
            },
            provided_services: WithProvenance {
                value: vec![ServiceSuffix::new("nav/plan").expect("service")],
                provenance: Provenance::Inferred,
                note: Some("provided services inferred by endpoint ownership".to_string()),
            },
            client_services: WithProvenance {
                value: Vec::new(),
                provenance: Provenance::Unavailable,
                note: Some("client ownership unavailable in admin snapshot mode".to_string()),
            },
            runtime: WithProvenance {
                value: Some(NodeRuntimeSummary {
                    timestamp_ns: 123,
                    collection_period_ms: 1000,
                    publisher_metric_count: 1,
                    subscriber_metric_count: 1,
                    service_metric_count: 1,
                    client_metric_count: 1,
                    publish_shm_hit_total: 10,
                    publish_shm_fallback_total: 2,
                    publish_shm_required_fail_total: 1,
                    loan_inflight_current: 3,
                    loan_quota_exceeded_total: 4,
                    subscriber_duplicate_drop_count: 1,
                    subscriber_out_of_order_count: 2,
                    subscriber_out_of_order_drop_count: 1,
                    subscriber_executor_drop_count: 3,
                    subscriber_local_drop_count: 4,
                    subscriber_overflow_relay_enqueue_count: 5,
                    subscriber_overflow_relay_drop_count: 6,
                    service_request_received_count: 11,
                    service_reply_ok_count: 9,
                    service_reply_error_count: 1,
                    service_reply_send_failure_count: 0,
                    service_request_dropped_count: 1,
                    service_in_flight_requests: 2,
                    client_call_count: 7,
                    client_success_count: 6,
                    client_timeout_count: 1,
                    client_concurrent_conflict_count: 0,
                    client_error_count: 0,
                    client_retry_count: 2,
                    executor: Some(NodeExecutorRuntimeSummary {
                        queued_events: 5,
                        queued_bytes: 1024,
                        peak_queued_events: 8,
                        peak_queued_bytes: 2048,
                        dropped_queue_full: 1,
                        dropped_queue_closed: 0,
                        dropped_budget_events: 2,
                        dropped_budget_bytes: 3,
                        spin_count: 0,
                    }),
                }),
                provenance: Provenance::Exact,
                note: None,
            },
            runtime_breakdown: WithProvenance {
                value: Some(NodeRuntimeBreakdown {
                    publishers: vec![NodePublisherRuntimeEntry {
                        resource_name: "camera/front".to_string(),
                        resource_instance_id: "publisher:1".to_string(),
                        schema_hash: Some("abcd".repeat(16)),
                        publish_shm_hit_total: 10,
                        publish_shm_fallback_total: 2,
                        publish_shm_required_fail_total: 1,
                        loan_inflight_current: 3,
                        loan_quota_exceeded_total: 4,
                    }],
                    subscribers: vec![NodeSubscriberRuntimeEntry {
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
                    }],
                    services: vec![NodeServiceRuntimeEntry {
                        resource_name: "nav/plan".to_string(),
                        resource_instance_id: "service:1".to_string(),
                        schema_hash: None,
                        request_received_count: 11,
                        request_dispatched_count: 10,
                        reply_ok_count: 9,
                        reply_error_count: 1,
                        reply_send_failure_count: 0,
                        request_dropped_count: 1,
                        in_flight_requests: 2,
                        last_request_ns: 100,
                        last_reply_ns: 101,
                    }],
                    clients: vec![NodeClientRuntimeEntry {
                        resource_name: "nav/plan".to_string(),
                        resource_instance_id: "client:1".to_string(),
                        schema_hash: None,
                        call_count: 7,
                        success_count: 6,
                        timeout_count: 1,
                        concurrent_conflict_count: 0,
                        error_count: 0,
                        retry_count: 2,
                        last_call_started_ns: 200,
                        last_call_finished_ns: 201,
                        last_call_latency_ns: 5,
                    }],
                }),
                provenance: Provenance::Exact,
                note: None,
            },
        };

        let mapped = map_node_detail_data(detail, NodeDiscoverySource::AdminSnapshot);
        assert_eq!(mapped.endpoint_ids, vec!["ep1"]);
        assert_eq!(mapped.topic_names, vec!["camera/front", "imu/data"]);
        assert_eq!(mapped.service_names, vec!["nav/plan"]);
        assert_eq!(mapped.published_topic_names, vec!["camera/front"]);
        assert!(mapped.subscribed_topic_names.is_empty());
        assert_eq!(mapped.provided_service_names, vec!["nav/plan"]);
        assert!(mapped.client_service_names.is_empty());
        assert_eq!(mapped.endpoint_provenance, "declared");
        assert_eq!(mapped.topic_provenance, "inferred");
        assert_eq!(mapped.service_provenance, "inferred");
        assert_eq!(mapped.published_topic_provenance, "inferred");
        assert_eq!(mapped.subscribed_topic_provenance, "unavailable");
        assert_eq!(mapped.provided_service_provenance, "inferred");
        assert_eq!(mapped.client_service_provenance, "unavailable");
        assert_eq!(mapped.runtime_provenance, "exact");
        let runtime = mapped.runtime_summary.expect("runtime summary");
        assert_eq!(runtime.collection_period_ms, 1000);
        assert_eq!(runtime.publish_shm_hit_total, 10);
        assert_eq!(runtime.client_call_count, 7);
        assert_eq!(runtime.executor_queued_events, Some(5));
        let breakdown = mapped.runtime_breakdown.expect("runtime breakdown");
        assert_eq!(breakdown.publishers.len(), 1);
        assert_eq!(breakdown.subscribers.len(), 1);
        assert_eq!(breakdown.services.len(), 1);
        assert_eq!(breakdown.clients.len(), 1);
        assert_eq!(breakdown.publishers[0].resource_name, "camera/front");
        assert_eq!(breakdown.clients[0].retry_count, 2);
    }

    #[test]
    fn zenoh_internals_mapping_preserves_router_counts_and_metrics_flag() {
        let mapped = map_zenoh_internals_data(ZenohAdminOverview {
            routers: vec![ZenohRouterOverview {
                zid: "router_zid".to_string(),
                whatami: "router".to_string(),
                version: Some("1.7.2".to_string()),
                locators: vec!["tcp/127.0.0.1:7447".to_string()],
                session_count: Some(2),
                plugin_names: vec!["rest".to_string(), "storage_manager".to_string()],
                metrics_available: true,
                metrics: Some(ZenohRouterMetrics {
                    tx_bytes_total: 1234,
                    rx_bytes_total: 678,
                    tx_msgs_total: 15,
                    rx_msgs_total: 7,
                    transports_opened: Some(3),
                    links_opened: None,
                }),
                raw_payload_utf8: "{\"version\":\"1.7.2\"}".to_string(),
            }],
        });

        assert_eq!(mapped.router_count, 1);
        assert_eq!(mapped.routers[0].zid, "router_zid");
        assert_eq!(mapped.routers[0].locator_count, 1);
        assert_eq!(mapped.routers[0].plugin_count, 2);
        assert!(mapped.routers[0].metrics_available);
        let metrics = mapped.routers[0]
            .metrics
            .as_ref()
            .expect("counters propagate through the mapping");
        assert_eq!(metrics.tx_bytes_total, 1234);
        assert_eq!(metrics.rx_msgs_total, 7);
    }

    #[test]
    fn zenoh_connectivity_mapping_tracks_counts_and_interface_totals() {
        let mapped = map_zenoh_connectivity_data(ZenohConnectivityOverview {
            session_zid: "session-alpha".to_string(),
            local_locators: vec![
                "tcp/127.0.0.1:7447".to_string(),
                "tcp/192.168.1.10:7447".to_string(),
            ],
            transports: vec![ZenohTransportOverview {
                remote_zid: "router-alpha".to_string(),
                remote_whatami: "router".to_string(),
                supports_qos: true,
                is_multicast: false,
                supports_shm: true,
            }],
            links: vec![ZenohLinkOverview {
                transport_zid: "router-alpha".to_string(),
                src: "tcp/192.168.1.10:7447".to_string(),
                dst: "tcp/10.0.0.2:7447".to_string(),
                group: Some("default".to_string()),
                mtu: 65_535,
                is_streamed: true,
                interfaces: vec!["en0".to_string(), "lo0".to_string()],
                auth_identifier: Some("tls:router-alpha".to_string()),
                priorities: Some((1, 7)),
                reliability: Some("Reliable".to_string()),
            }],
        });

        assert_eq!(mapped.session_zid, "session-alpha");
        assert_eq!(mapped.local_locator_count, 2);
        assert_eq!(mapped.transport_count, 1);
        assert_eq!(mapped.link_count, 1);
        assert_eq!(mapped.transports[0].remote_zid, "router-alpha");
        assert!(mapped.transports[0].supports_shm);
        assert_eq!(mapped.links[0].interface_count, 2);
        assert_eq!(mapped.links[0].interfaces, vec!["en0", "lo0"]);
        assert_eq!(mapped.links[0].reliability.as_deref(), Some("Reliable"));
    }

    #[test]
    fn zenoh_connectivity_event_mapping_formats_transport_and_link_messages() {
        let mapped = map_zenoh_connectivity_event_data(vec![
            ZenohConnectivityEvent {
                sequence: 1,
                timestamp_ns: 100,
                kind: ZenohConnectivityEventKind::TransportOpened,
                transport: Some(ZenohTransportOverview {
                    remote_zid: "router-alpha".to_string(),
                    remote_whatami: "router".to_string(),
                    supports_qos: true,
                    is_multicast: false,
                    supports_shm: true,
                }),
                link: None,
            },
            ZenohConnectivityEvent {
                sequence: 2,
                timestamp_ns: 200,
                kind: ZenohConnectivityEventKind::LinkRemoved,
                transport: None,
                link: Some(ZenohLinkOverview {
                    transport_zid: "router-alpha".to_string(),
                    src: "tcp/192.168.1.10:7447".to_string(),
                    dst: "tcp/10.0.0.2:7447".to_string(),
                    group: Some("default".to_string()),
                    mtu: 65_535,
                    is_streamed: true,
                    interfaces: vec!["en0".to_string()],
                    auth_identifier: None,
                    priorities: Some((1, 7)),
                    reliability: Some("Reliable".to_string()),
                }),
            },
        ]);

        assert_eq!(mapped.len(), 2);
        assert_eq!(
            mapped[0].kind,
            super::ZenohConnectivityEventKindData::TransportOpened
        );
        assert!(mapped[0].message.contains("transport opened"));
        assert!(mapped[0].message.contains("router-alpha [router]"));
        assert!(mapped[1].message.contains("link removed"));
        assert!(mapped[1]
            .message
            .contains("tcp/192.168.1.10:7447 -> tcp/10.0.0.2:7447"));
    }

    #[test]
    fn starter_json_from_schema_model_builds_nested_object_defaults() {
        let model = SchemaModel {
            root_type: "demo.Request".to_string(),
            fields: vec![
                SchemaField {
                    path: "message".to_string(),
                    name: "message".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "string".to_string(),
                    type_kind: SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "count".to_string(),
                    name: "count".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "i32".to_string(),
                    type_kind: SchemaTypeKind::Integer,
                    optional: false,
                    repeated: false,
                    default_json: Some(json!(7)),
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "target".to_string(),
                    name: "target".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "demo.Target".to_string(),
                    type_kind: SchemaTypeKind::Table,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "target.name".to_string(),
                    name: "name".to_string(),
                    parent_path: Some("target".to_string()),
                    depth: 1,
                    type_name: "string".to_string(),
                    type_kind: SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "mode".to_string(),
                    name: "mode".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "demo.Mode".to_string(),
                    type_kind: SchemaTypeKind::Enum,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: vec![SchemaEnumValue {
                        name: "AUTO".to_string(),
                        number: 0,
                    }],
                    doc: None,
                },
            ],
        };

        let json = starter_json_from_schema_model(&model).expect("starter json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(
            value,
            json!({
                "message": "",
                "count": 7,
                "target": {
                    "name": ""
                },
                "mode": "AUTO"
            })
        );
    }

    #[test]
    fn field_template_json_from_schema_model_builds_nested_patch() {
        let model = SchemaModel {
            root_type: "demo.Request".to_string(),
            fields: vec![
                SchemaField {
                    path: "options".to_string(),
                    name: "options".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "demo.Options".to_string(),
                    type_kind: SchemaTypeKind::Table,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "options.limit".to_string(),
                    name: "limit".to_string(),
                    parent_path: Some("options".to_string()),
                    depth: 1,
                    type_name: "i32".to_string(),
                    type_kind: SchemaTypeKind::Integer,
                    optional: false,
                    repeated: false,
                    default_json: Some(json!(10)),
                    enum_values: Vec::new(),
                    doc: None,
                },
            ],
        };

        let patch =
            field_template_json_from_schema_model(&model, "options.limit").expect("field patch");
        let value: Value = serde_json::from_str(&patch).expect("valid patch json");
        assert_eq!(value, json!({ "options": { "limit": 10 } }));
    }

    #[test]
    fn merge_field_template_into_request_json_merges_selected_field() {
        let model = SchemaModel {
            root_type: "demo.Request".to_string(),
            fields: vec![
                SchemaField {
                    path: "message".to_string(),
                    name: "message".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "string".to_string(),
                    type_kind: SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: Some(json!("")),
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "options".to_string(),
                    name: "options".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "demo.Options".to_string(),
                    type_kind: SchemaTypeKind::Table,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "options.limit".to_string(),
                    name: "limit".to_string(),
                    parent_path: Some("options".to_string()),
                    depth: 1,
                    type_name: "i32".to_string(),
                    type_kind: SchemaTypeKind::Integer,
                    optional: false,
                    repeated: false,
                    default_json: Some(json!(10)),
                    enum_values: Vec::new(),
                    doc: None,
                },
            ],
        };

        let merged = merge_field_template_into_request_json(
            "{\n  \"message\": \"hello\"\n}",
            &model,
            "options.limit",
        )
        .expect("merged request");
        let value: Value = serde_json::from_str(&merged).expect("valid merged json");
        assert_eq!(
            value,
            json!({ "message": "hello", "options": { "limit": 10 } })
        );
    }

    #[test]
    fn merge_goal_field_template_strips_goal_wrapper_and_rejects_non_goal_fields() {
        let model = SchemaModel {
            root_type: "SendGoalRequest".to_string(),
            fields: vec![
                SchemaField {
                    path: "goal_id".to_string(),
                    name: "goal_id".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "string".to_string(),
                    type_kind: SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: Some(json!("")),
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "goal".to_string(),
                    name: "goal".to_string(),
                    parent_path: None,
                    depth: 0,
                    type_name: "demo.Goal".to_string(),
                    type_kind: SchemaTypeKind::Table,
                    optional: false,
                    repeated: false,
                    default_json: None,
                    enum_values: Vec::new(),
                    doc: None,
                },
                SchemaField {
                    path: "goal.source".to_string(),
                    name: "source".to_string(),
                    parent_path: Some("goal".to_string()),
                    depth: 1,
                    type_name: "string".to_string(),
                    type_kind: SchemaTypeKind::String,
                    optional: false,
                    repeated: false,
                    default_json: Some(json!("demo")),
                    enum_values: Vec::new(),
                    doc: None,
                },
            ],
        };

        // A `goal.*` hint merges at the goal-payload root — no nested `goal`.
        let merged = merge_goal_field_template_into_goal_json(
            "{\n  \"source\": \"\"\n}",
            &model,
            "goal.source",
        )
        .expect("goal field merges");
        let value: Value = serde_json::from_str(&merged).expect("valid json");
        assert_eq!(value, json!({ "source": "demo" }));
        assert!(
            value.get("goal").is_none(),
            "must not nest a second goal wrapper: {value}"
        );

        // A non-goal field (goal_id) is not part of this editor → rejected.
        assert!(
            merge_goal_field_template_into_goal_json("{}", &model, "goal_id").is_err(),
            "goal_id is not part of the goal payload editor"
        );
    }
}
