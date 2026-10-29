use std::collections::BTreeMap;

use crate::telemetry::schema::{
    NodeIdentityTelemetry, ResourceInventoryTelemetry, RuntimeMetricsTelemetry,
    TelemetryClientRuntimeMetrics, TelemetryExecutorRuntimeMetrics, TelemetryNodeMetadata,
    TelemetryPublisherEntry, TelemetryPublisherRuntimeMetrics, TelemetryResourceCommon,
    TelemetryServiceRuntimeMetrics, TelemetrySubscriberRuntimeMetrics, TELEMETRY_SCHEMA_VERSION,
};

#[test]
fn telemetry_node_metadata_normalizes_blank_fields_and_labels() {
    let mut labels = BTreeMap::new();
    labels.insert(" deployment ".to_string(), " dev ".to_string());
    labels.insert("".to_string(), "ignored".to_string());
    labels.insert("ignored".to_string(), "   ".to_string());

    let normalized = TelemetryNodeMetadata {
        executable: Some("  /tmp/aorta ".to_string()),
        language: Some(" rust ".to_string()),
        sdk_name: Some(" aorta_rust ".to_string()),
        sdk_version: Some(" 0.1.0 ".to_string()),
        labels,
        build_id: Some(" build-123 ".to_string()),
        container_id: Some("   ".to_string()),
    }
    .normalized();

    assert_eq!(normalized.executable.as_deref(), Some("/tmp/aorta"));
    assert_eq!(normalized.language.as_deref(), Some("rust"));
    assert_eq!(normalized.sdk_name.as_deref(), Some("aorta_rust"));
    assert_eq!(normalized.sdk_version.as_deref(), Some("0.1.0"));
    assert_eq!(normalized.build_id.as_deref(), Some("build-123"));
    assert_eq!(normalized.container_id, None);
    assert_eq!(normalized.labels.len(), 1);
    assert_eq!(
        normalized.labels.get("deployment").map(String::as_str),
        Some("dev")
    );
}

#[test]
fn telemetry_node_identity_flatbuffer_roundtrip() {
    let payload = NodeIdentityTelemetry {
        telemetry_version: TELEMETRY_SCHEMA_VERSION,
        sequence: 1,
        timestamp_ns: 1773300000000000000,
        node_id: "node-123".to_string(),
        node_name: "talker".to_string(),
        group: "default".to_string(),
        host_id: "host-123".to_string(),
        host_name: "robot-01".to_string(),
        process_start_time_ns: 1773299000000000000,
        pid: 4242,
        executable: "/tmp/aorta".to_string(),
        language: "rust".to_string(),
        sdk_name: "aorta_core".to_string(),
        sdk_version: "0.1.0".to_string(),
        session_zid: "abcd1234".to_string(),
        connected_router_zids: Vec::new(),
        container_id: None,
        build_id: Some("build-123".to_string()),
        labels: BTreeMap::from([(String::from("deployment"), String::from("dev"))]),
    };

    let bytes = crate::telemetry::fb::serialize_node_identity(&payload);
    let decoded =
        crate::telemetry::fb::node_identity_from_fb(&bytes).expect("decode node_identity fb");
    assert_eq!(payload, decoded);
}

#[test]
fn telemetry_resource_inventory_flatbuffer_roundtrip() {
    let payload = ResourceInventoryTelemetry {
        telemetry_version: TELEMETRY_SCHEMA_VERSION,
        sequence: 2,
        timestamp_ns: 1773300000000000100,
        node_id: "node-123".to_string(),
        group: "default".to_string(),
        inventory_revision: 1,
        publishers: vec![TelemetryPublisherEntry {
            common: TelemetryResourceCommon {
                resource_instance_id: "publisher:1".to_string(),
                resource_name: "demo/all_types".to_string(),
                full_key: "aorta/default/pub/demo/all_types".to_string(),
                schema_hash: Some("abcd".repeat(16)),
                declared_at_ns: Some(1773300000000000000),
                labels: BTreeMap::new(),
            },
        }],
        subscribers: Vec::new(),
        services: Vec::new(),
        clients: Vec::new(),
        overflow: Default::default(),
    };

    let bytes = crate::telemetry::fb::serialize_resource_inventory(&payload);
    let decoded = crate::telemetry::fb::resource_inventory_from_fb(&bytes)
        .expect("decode resource_inventory fb");
    assert_eq!(payload, decoded);
}

#[test]
fn telemetry_runtime_metrics_flatbuffer_roundtrip() {
    let payload = RuntimeMetricsTelemetry {
        telemetry_version: TELEMETRY_SCHEMA_VERSION,
        sequence: 3,
        timestamp_ns: 1773300000000000200,
        node_id: "node-123".to_string(),
        group: "default".to_string(),
        collection_period_ms: 1000,
        publishers: vec![TelemetryPublisherRuntimeMetrics {
            resource_name: "demo/all_types".to_string(),
            resource_instance_id: "publisher:1".to_string(),
            schema_hash: Some("abcd".repeat(16)),
            publish_shm_hit_total: 10,
            publish_shm_fallback_total: 2,
            publish_shm_required_fail_total: 0,
            loan_inflight_current: 1,
            loan_quota_exceeded_total: 0,
        }],
        subscribers: vec![TelemetrySubscriberRuntimeMetrics {
            resource_name: "demo/all_types".to_string(),
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
            schema_hash: Some("1234".repeat(16)),
            request_received_count: 20,
            request_dispatched_count: 20,
            reply_ok_count: 19,
            reply_error_count: 1,
            reply_send_failure_count: 0,
            request_dropped_count: 0,
            query_receive_failure_count: 0,
            task_exit_count: 0,
            in_flight_requests: 0,
            last_request_ns: 1773300000000000100,
            last_reply_ns: 1773300000000000150,
        }],
        clients: vec![TelemetryClientRuntimeMetrics {
            resource_name: "map/load".to_string(),
            resource_instance_id: "client:1".to_string(),
            schema_hash: None,
            call_count: 5,
            success_count: 5,
            timeout_count: 0,
            concurrent_conflict_count: 0,
            error_count: 0,
            retry_count: 0,
            last_call_started_ns: 1773300000000000000,
            last_call_finished_ns: 1773300000000000050,
            last_call_latency_ns: 50,
        }],
        executor: Some(TelemetryExecutorRuntimeMetrics {
            queued_events: 0,
            queued_bytes: 0,
            peak_queued_events: 4,
            peak_queued_bytes: 1024,
            dropped_queue_full: 0,
            dropped_queue_closed: 0,
            dropped_budget_events: 1,
            dropped_budget_bytes: 2,
            spin_count: 7,
            lane_occupancy: 1,
            lane_high_water: 4,
            lane_pushed_total: 22,
            dropped_expired: 2,
            offload_queue_depth: 0,
            // v6: per-pool-lane counters roundtrip (index = class lane).
            pool_lane_depth: vec![0, 0, 3, 0],
            pool_lane_high_water: vec![1, 0, 6, 2],
            pool_lane_enqueued_total: vec![9, 0, 250, 12],
            pool_lane_shed_total: vec![0, 0, 2, 0],
            pool_lane_dropped_expired_total: vec![0, 1, 0, 0],
        }),
    };

    let bytes = crate::telemetry::fb::serialize_runtime_metrics(&payload);
    let decoded =
        crate::telemetry::fb::runtime_metrics_from_fb(&bytes).expect("decode runtime_metrics fb");
    assert_eq!(payload, decoded);
}
