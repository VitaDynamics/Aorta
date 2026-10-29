use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Telemetry wire-format version, stamped into every telemetry payload.
///
/// v3: the three telemetry families (node_identity,
/// resource_inventory, runtime_metrics) moved from JSON to FlatBuffers on the
/// wire — see `telemetry/fb.rs` and `message/schemas/system/telemetry_*.fbs`.
/// This is a hard encoding change: producers and consumers (notably
/// `aorta-introspect`) must be upgraded together. No consumer currently gates
/// on this constant; it is informational.
///
/// v4: runtime_metrics `ExecutorMetrics` gained `spin_count`, a
/// monotonic executor spin-loop counter (forward-compatible field append).
///
/// v5 (Flow observability): `ExecutorMetrics` gained the lane/offload
/// counters (queue-expired drops, offload pool depth/served); regenerated
/// via `message/tools/regen_telemetry_fb.py`. (An earlier revision of this
/// entry also claimed a subscriber `dropped_for_deadline` wire field —
/// that field only landed in v7; the in-process counter existed since v5.)
///
/// v6 (Flow lane telemetry): `ExecutorMetrics` gained the per-class-lane
/// vectors (depth / high-water / enqueued / shed / expired, one entry per
/// class lane) published only while a lane pool is ATTACHED — empty
/// vectors mean "not in facade mode", never "all zero". Same regen
/// pipeline; consumers must treat the vectors as optional.
///
/// v7: `SubscriberMetrics` gained
/// `dropped_for_deadline` — the deadline-over-messaging drop counter,
/// finally on the wire (forward-compatible trailing append; old readers
/// ignore it, old writers report 0).
pub const TELEMETRY_SCHEMA_VERSION: u32 = 7;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeOptions {
    pub telemetry: TelemetryNodeMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryNodeMetadata {
    pub executable: Option<String>,
    pub language: Option<String>,
    pub sdk_name: Option<String>,
    pub sdk_version: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub build_id: Option<String>,
    pub container_id: Option<String>,
}

impl TelemetryNodeMetadata {
    pub fn normalized(&self) -> Self {
        let labels = self
            .labels
            .iter()
            .filter_map(|(key, value)| {
                let normalized_key = key.trim();
                let normalized_value = value.trim();
                if normalized_key.is_empty() || normalized_value.is_empty() {
                    None
                } else {
                    Some((normalized_key.to_string(), normalized_value.to_string()))
                }
            })
            .collect();

        Self {
            executable: normalize_optional(&self.executable),
            language: normalize_optional(&self.language),
            sdk_name: normalize_optional(&self.sdk_name),
            sdk_version: normalize_optional(&self.sdk_version),
            labels,
            build_id: normalize_optional(&self.build_id),
            container_id: normalize_optional(&self.container_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostIdentity {
    pub host_id: String,
    pub host_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentityTelemetry {
    pub telemetry_version: u32,
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub node_id: String,
    pub node_name: String,
    pub group: String,
    pub host_id: String,
    pub host_name: String,
    pub process_start_time_ns: u64,
    pub pid: u32,
    pub executable: String,
    pub language: String,
    pub sdk_name: String,
    pub sdk_version: String,
    pub session_zid: String,
    pub connected_router_zids: Vec<String>,
    pub container_id: Option<String>,
    pub build_id: Option<String>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceInventoryTelemetry {
    pub telemetry_version: u32,
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub node_id: String,
    pub group: String,
    pub inventory_revision: u64,
    pub publishers: Vec<TelemetryPublisherEntry>,
    pub subscribers: Vec<TelemetrySubscriberEntry>,
    pub services: Vec<TelemetryServiceEntry>,
    pub clients: Vec<TelemetryClientEntry>,
    /// Cumulative count of resource registrations dropped because a per-kind
    /// cardinality cap was hit. Non-zero means the inventory is
    /// no longer complete — some live resources are not represented above.
    /// `#[serde(default)]` so payloads from older publishers still deserialize.
    #[serde(default)]
    pub overflow: ResourceInventoryOverflow,
}

/// Per-kind counters for inventory registrations rejected by the cardinality cap.
/// Counters are monotonic for the lifetime of the node.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResourceInventoryOverflow {
    pub publishers_dropped: u64,
    pub subscribers_dropped: u64,
    pub services_dropped: u64,
    pub clients_dropped: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeMetricsTelemetry {
    pub telemetry_version: u32,
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub node_id: String,
    pub group: String,
    pub collection_period_ms: u32,
    pub publishers: Vec<TelemetryPublisherRuntimeMetrics>,
    #[serde(default)]
    pub subscribers: Vec<TelemetrySubscriberRuntimeMetrics>,
    pub services: Vec<TelemetryServiceRuntimeMetrics>,
    pub clients: Vec<TelemetryClientRuntimeMetrics>,
    pub executor: Option<TelemetryExecutorRuntimeMetrics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryResourceCommon {
    pub resource_instance_id: String,
    pub resource_name: String,
    pub full_key: String,
    pub schema_hash: Option<String>,
    pub declared_at_ns: Option<u64>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryPublisherEntry {
    #[serde(flatten)]
    pub common: TelemetryResourceCommon,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetrySubscriberEntry {
    #[serde(flatten)]
    pub common: TelemetryResourceCommon,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryServiceEntry {
    #[serde(flatten)]
    pub common: TelemetryResourceCommon,
    pub request_root_type: Option<String>,
    pub response_root_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryClientEntry {
    #[serde(flatten)]
    pub common: TelemetryResourceCommon,
    pub request_root_type: Option<String>,
    pub response_root_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryPublisherRuntimeMetrics {
    pub resource_name: String,
    pub resource_instance_id: String,
    pub schema_hash: Option<String>,
    pub publish_shm_hit_total: u64,
    pub publish_shm_fallback_total: u64,
    pub publish_shm_required_fail_total: u64,
    pub loan_inflight_current: u64,
    pub loan_quota_exceeded_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetrySubscriberRuntimeMetrics {
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
    pub dropped_for_deadline: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryServiceRuntimeMetrics {
    pub resource_name: String,
    pub resource_instance_id: String,
    pub schema_hash: Option<String>,
    pub request_received_count: u64,
    pub request_dispatched_count: u64,
    pub reply_ok_count: u64,
    pub reply_error_count: u64,
    pub reply_send_failure_count: u64,
    pub request_dropped_count: u64,
    pub query_receive_failure_count: u64,
    pub task_exit_count: u64,
    pub in_flight_requests: u64,
    pub last_request_ns: u64,
    pub last_reply_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryClientRuntimeMetrics {
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

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TelemetryExecutorRuntimeMetrics {
    pub queued_events: u64,
    pub queued_bytes: u64,
    pub peak_queued_events: u64,
    pub peak_queued_bytes: u64,
    pub dropped_queue_full: u64,
    pub dropped_queue_closed: u64,
    pub dropped_budget_events: u64,
    pub dropped_budget_bytes: u64,
    /// Monotonic executor spin-loop counter; cloud derives spins/sec.
    pub spin_count: u64,
    /// Flow : reserved CoroutineResume lane + Flow drops.
    pub lane_occupancy: u64,
    pub lane_high_water: u64,
    pub lane_pushed_total: u64,
    pub dropped_expired: u64,
    /// Current offload-pool backlog. PROCESS-GLOBAL (the offload pool is a
    /// process-wide singleton), not per-executor — identical across every
    /// executor row of one process.
    pub offload_queue_depth: u64,
    /// Flow (telemetry v6): the ATTACHED lane pool's
    /// per-class-lane counters, index = class (0 realtime .. 3 low).
    /// Empty = executor not in facade mode.
    #[serde(default)]
    pub pool_lane_depth: Vec<u64>,
    #[serde(default)]
    pub pool_lane_high_water: Vec<u64>,
    #[serde(default)]
    pub pool_lane_enqueued_total: Vec<u64>,
    #[serde(default)]
    pub pool_lane_shed_total: Vec<u64>,
    #[serde(default)]
    pub pool_lane_dropped_expired_total: Vec<u64>,
}

/// Executor liveness pulse carried in the vital snapshot (telemetry
/// tiering). Raw cumulative counters; the cloud derives rate and staleness.
/// `alive` is the one robot-computed convenience flag.
///
/// Sampled at the 30 s vital heartbeat, NOT per spin: the hot path keeps the X1
/// clock-free `spin_count` increment, and the publisher compares `spin_count`
/// against the previous tick to set `alive` / `last_progress_ns` off the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuntimeHealth {
    /// Raw cumulative executor spin iterations (the X1 counter); cloud derives spins/sec.
    pub spin_count: u64,
    /// Heartbeat-sampled monotonic ns of the most recent observed spin advance.
    pub last_progress_ns: u64,
    /// Raw cumulative executor/dispatch error count; cloud derives the error rate.
    pub error_count: u64,
    /// Whether `spin_count` advanced within the last heartbeat window.
    pub alive: bool,
}

fn normalize_optional(value: &Option<String>) -> Option<String> {
    value.as_ref().and_then(|value| {
        let normalized = value.trim();
        if normalized.is_empty() {
            None
        } else {
            Some(normalized.to_string())
        }
    })
}
