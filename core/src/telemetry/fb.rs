//! FlatBuffers wire codec for telemetry families.
//!
//! Telemetry families are FlatBuffers on the wire; the in-memory shape stays
//! the `schema.rs` structs, so consumers (e.g. `aorta-introspect`) keep using
//! the same types and only swap the decode call. These helpers convert between
//! the struct and the wire bytes and expose the committed BFBS so the runtime
//! can register the schema in the by-hash store and attach its hash to each
//! telemetry put (which lets recorders write typed MCAP channels).
//!
//! The shape MUST mirror `telemetry_node_identity.fbs`; the roundtrip tests
//! below plus the `core/build.rs` drift check guard the two against drift.

use std::collections::BTreeMap;

use flatbuffers::FlatBufferBuilder;

use crate::error::{AortaError, AortaResult};
use crate::schema::SchemaHash;
use crate::telemetry::schema::{
    NodeIdentityTelemetry, ResourceInventoryOverflow, ResourceInventoryTelemetry, RuntimeHealth,
    RuntimeMetricsTelemetry, TelemetryClientEntry, TelemetryClientRuntimeMetrics,
    TelemetryExecutorRuntimeMetrics, TelemetryPublisherEntry, TelemetryPublisherRuntimeMetrics,
    TelemetryResourceCommon, TelemetryServiceEntry, TelemetryServiceRuntimeMetrics,
    TelemetrySubscriberEntry, TelemetrySubscriberRuntimeMetrics,
};

// The generated modules are declared at the crate root (see `core/src/lib.rs`)
// because flatc emits cross-schema `include` references as `crate::*` paths.
// `KeyValue` lives in the shared `telemetry_common` schema (factored out so the
// vital snapshot can `include` both families without a duplicate definition).
use crate::telemetry_common_generated::aorta::telemetry as tc;
use crate::telemetry_node_identity_generated::aorta::telemetry as ni;
pub use crate::telemetry_node_identity_generated::TELEMETRY_NODE_IDENTITY_BFBS;

use crate::telemetry_resource_inventory_generated::aorta::telemetry as ri;
pub use crate::telemetry_resource_inventory_generated::TELEMETRY_RESOURCE_INVENTORY_BFBS;

use crate::telemetry_runtime_metrics_generated::aorta::telemetry as rm;
pub use crate::telemetry_runtime_metrics_generated::TELEMETRY_RUNTIME_METRICS_BFBS;

use crate::telemetry_vital_snapshot_generated::aorta::telemetry as vs;
pub use crate::telemetry_vital_snapshot_generated::TELEMETRY_VITAL_SNAPSHOT_BFBS;

/// SHA-256 hash of the committed `node_identity` BFBS. Cheap; recomputed on call.
pub fn node_identity_schema_hash() -> SchemaHash {
    SchemaHash::from_bfbs_bytes(TELEMETRY_NODE_IDENTITY_BFBS)
}

/// Schema-hash attachment (64-byte ASCII hex) carried by every telemetry put so
/// recorders resolve the schema by-hash and write a typed `flatbuffer` MCAP
/// channel. At 5–30s heartbeat cadence the hash recompute is negligible.
pub fn node_identity_schema_attachment() -> crate::ZBytes {
    crate::ZBytes::from(node_identity_schema_hash().to_hex_bytes().to_vec())
}

/// See [`node_identity_schema_attachment`].
pub fn resource_inventory_schema_attachment() -> crate::ZBytes {
    crate::ZBytes::from(resource_inventory_schema_hash().to_hex_bytes().to_vec())
}

/// See [`node_identity_schema_attachment`].
pub fn runtime_metrics_schema_attachment() -> crate::ZBytes {
    crate::ZBytes::from(runtime_metrics_schema_hash().to_hex_bytes().to_vec())
}

/// Build a `NodeIdentity` table into `b`, returning its offset so it can be
/// nested (e.g. inside `VitalSnapshot`) or finished as a standalone root.
fn build_node_identity<'a>(
    b: &mut FlatBufferBuilder<'a>,
    t: &NodeIdentityTelemetry,
) -> flatbuffers::WIPOffset<ni::NodeIdentity<'a>> {
    let node_id = b.create_string(&t.node_id);
    let node_name = b.create_string(&t.node_name);
    let group = b.create_string(&t.group);
    let host_id = b.create_string(&t.host_id);
    let host_name = b.create_string(&t.host_name);
    let executable = b.create_string(&t.executable);
    let language = b.create_string(&t.language);
    let sdk_name = b.create_string(&t.sdk_name);
    let sdk_version = b.create_string(&t.sdk_version);
    let session_zid = b.create_string(&t.session_zid);
    let container_id = t.container_id.as_deref().map(|s| b.create_string(s));
    let build_id = t.build_id.as_deref().map(|s| b.create_string(s));

    let zid_offsets: Vec<_> = t
        .connected_router_zids
        .iter()
        .map(|s| b.create_string(s))
        .collect();
    let connected_router_zids = b.create_vector(&zid_offsets);

    let label_offsets: Vec<_> = t
        .labels
        .iter()
        .map(|(k, v)| {
            let key = b.create_string(k);
            let value = b.create_string(v);
            tc::KeyValue::create(
                b,
                &tc::KeyValueArgs {
                    key: Some(key),
                    value: Some(value),
                },
            )
        })
        .collect::<Vec<_>>();
    let labels = b.create_vector(&label_offsets);

    ni::NodeIdentity::create(
        b,
        &ni::NodeIdentityArgs {
            telemetry_version: t.telemetry_version,
            sequence: t.sequence,
            timestamp_ns: t.timestamp_ns,
            node_id: Some(node_id),
            node_name: Some(node_name),
            group: Some(group),
            host_id: Some(host_id),
            host_name: Some(host_name),
            process_start_time_ns: t.process_start_time_ns,
            pid: t.pid,
            executable: Some(executable),
            language: Some(language),
            sdk_name: Some(sdk_name),
            sdk_version: Some(sdk_version),
            session_zid: Some(session_zid),
            connected_router_zids: Some(connected_router_zids),
            container_id,
            build_id,
            labels: Some(labels),
        },
    )
}

/// Encode `NodeIdentityTelemetry` into standalone `node_identity` wire bytes.
pub fn serialize_node_identity(t: &NodeIdentityTelemetry) -> Vec<u8> {
    let mut b = FlatBufferBuilder::new();
    let root = build_node_identity(&mut b, t);
    b.finish(root, None);
    b.finished_data().to_vec()
}

/// Decode `node_identity` FlatBuffers wire bytes into `NodeIdentityTelemetry`.
///
/// Absent optional strings (`container_id`, `build_id`) decode to `None`;
/// absent required strings decode to an empty `String` (FlatBuffers default).
pub fn node_identity_from_fb(bytes: &[u8]) -> AortaResult<NodeIdentityTelemetry> {
    let root = flatbuffers::root::<ni::NodeIdentity>(bytes).map_err(|err| {
        AortaError::internal(format!("invalid node_identity flatbuffer: {}", err))
    })?;
    Ok(node_identity_from_table(root))
}

/// Map a `NodeIdentity` table accessor (a root, or the `identity` field nested
/// in a `VitalSnapshot`) into the in-memory struct.
fn node_identity_from_table(root: ni::NodeIdentity<'_>) -> NodeIdentityTelemetry {
    let connected_router_zids = root
        .connected_router_zids()
        .map(|v| v.iter().map(|s| s.to_string()).collect())
        .unwrap_or_default();

    let labels = root
        .labels()
        .map(|v| {
            v.iter()
                .map(|kv| {
                    (
                        kv.key().unwrap_or_default().to_string(),
                        kv.value().unwrap_or_default().to_string(),
                    )
                })
                .collect::<BTreeMap<String, String>>()
        })
        .unwrap_or_default();

    NodeIdentityTelemetry {
        telemetry_version: root.telemetry_version(),
        sequence: root.sequence(),
        timestamp_ns: root.timestamp_ns(),
        node_id: root.node_id().unwrap_or_default().to_string(),
        node_name: root.node_name().unwrap_or_default().to_string(),
        group: root.group().unwrap_or_default().to_string(),
        host_id: root.host_id().unwrap_or_default().to_string(),
        host_name: root.host_name().unwrap_or_default().to_string(),
        process_start_time_ns: root.process_start_time_ns(),
        pid: root.pid(),
        executable: root.executable().unwrap_or_default().to_string(),
        language: root.language().unwrap_or_default().to_string(),
        sdk_name: root.sdk_name().unwrap_or_default().to_string(),
        sdk_version: root.sdk_version().unwrap_or_default().to_string(),
        session_zid: root.session_zid().unwrap_or_default().to_string(),
        connected_router_zids,
        container_id: root.container_id().map(|s| s.to_string()),
        build_id: root.build_id().map(|s| s.to_string()),
        labels,
    }
}

/// SHA-256 hash of the committed `resource_inventory` BFBS.
pub fn resource_inventory_schema_hash() -> SchemaHash {
    SchemaHash::from_bfbs_bytes(TELEMETRY_RESOURCE_INVENTORY_BFBS)
}

fn build_resource_common<'a>(
    b: &mut FlatBufferBuilder<'a>,
    c: &TelemetryResourceCommon,
) -> flatbuffers::WIPOffset<ri::ResourceCommon<'a>> {
    let resource_instance_id = b.create_string(&c.resource_instance_id);
    let resource_name = b.create_string(&c.resource_name);
    let full_key = b.create_string(&c.full_key);
    let schema_hash = c.schema_hash.as_deref().map(|s| b.create_string(s));
    let label_offsets: Vec<_> = c
        .labels
        .iter()
        .map(|(k, v)| {
            let key = b.create_string(k);
            let value = b.create_string(v);
            tc::KeyValue::create(
                b,
                &tc::KeyValueArgs {
                    key: Some(key),
                    value: Some(value),
                },
            )
        })
        .collect();
    let labels = b.create_vector(&label_offsets);
    ri::ResourceCommon::create(
        b,
        &ri::ResourceCommonArgs {
            resource_instance_id: Some(resource_instance_id),
            resource_name: Some(resource_name),
            full_key: Some(full_key),
            schema_hash,
            declared_at_ns: c.declared_at_ns,
            labels: Some(labels),
        },
    )
}

/// Build a `ResourceInventory` table into `b`, returning its offset so it can be
/// nested (e.g. inside `VitalSnapshot`) or finished as a standalone root.
fn build_resource_inventory<'a>(
    b: &mut FlatBufferBuilder<'a>,
    inv: &ResourceInventoryTelemetry,
) -> flatbuffers::WIPOffset<ri::ResourceInventory<'a>> {
    let node_id = b.create_string(&inv.node_id);
    let group = b.create_string(&inv.group);

    let publisher_offsets: Vec<_> = inv
        .publishers
        .iter()
        .map(|e| {
            let common = build_resource_common(b, &e.common);
            ri::PublisherEntry::create(
                b,
                &ri::PublisherEntryArgs {
                    common: Some(common),
                },
            )
        })
        .collect();
    let publishers = b.create_vector(&publisher_offsets);

    let subscriber_offsets: Vec<_> = inv
        .subscribers
        .iter()
        .map(|e| {
            let common = build_resource_common(b, &e.common);
            ri::SubscriberEntry::create(
                b,
                &ri::SubscriberEntryArgs {
                    common: Some(common),
                },
            )
        })
        .collect();
    let subscribers = b.create_vector(&subscriber_offsets);

    let service_offsets: Vec<_> = inv
        .services
        .iter()
        .map(|e| {
            let common = build_resource_common(b, &e.common);
            let request_root_type = e.request_root_type.as_deref().map(|s| b.create_string(s));
            let response_root_type = e.response_root_type.as_deref().map(|s| b.create_string(s));
            ri::ServiceEntry::create(
                b,
                &ri::ServiceEntryArgs {
                    common: Some(common),
                    request_root_type,
                    response_root_type,
                },
            )
        })
        .collect();
    let services = b.create_vector(&service_offsets);

    let client_offsets: Vec<_> = inv
        .clients
        .iter()
        .map(|e| {
            let common = build_resource_common(b, &e.common);
            let request_root_type = e.request_root_type.as_deref().map(|s| b.create_string(s));
            let response_root_type = e.response_root_type.as_deref().map(|s| b.create_string(s));
            ri::ClientEntry::create(
                b,
                &ri::ClientEntryArgs {
                    common: Some(common),
                    request_root_type,
                    response_root_type,
                },
            )
        })
        .collect();
    let clients = b.create_vector(&client_offsets);

    let overflow = ri::InventoryOverflow::create(
        b,
        &ri::InventoryOverflowArgs {
            publishers_dropped: inv.overflow.publishers_dropped,
            subscribers_dropped: inv.overflow.subscribers_dropped,
            services_dropped: inv.overflow.services_dropped,
            clients_dropped: inv.overflow.clients_dropped,
        },
    );

    ri::ResourceInventory::create(
        b,
        &ri::ResourceInventoryArgs {
            telemetry_version: inv.telemetry_version,
            sequence: inv.sequence,
            timestamp_ns: inv.timestamp_ns,
            node_id: Some(node_id),
            group: Some(group),
            inventory_revision: inv.inventory_revision,
            publishers: Some(publishers),
            subscribers: Some(subscribers),
            services: Some(services),
            clients: Some(clients),
            overflow: Some(overflow),
        },
    )
}

/// Encode `ResourceInventoryTelemetry` into standalone `resource_inventory` wire bytes.
pub fn serialize_resource_inventory(inv: &ResourceInventoryTelemetry) -> Vec<u8> {
    let mut b = FlatBufferBuilder::new();
    let root = build_resource_inventory(&mut b, inv);
    b.finish(root, None);
    b.finished_data().to_vec()
}

fn resource_common_from_fb(c: ri::ResourceCommon<'_>) -> TelemetryResourceCommon {
    let labels = c
        .labels()
        .map(|v| {
            v.iter()
                .map(|kv| {
                    (
                        kv.key().unwrap_or_default().to_string(),
                        kv.value().unwrap_or_default().to_string(),
                    )
                })
                .collect::<BTreeMap<String, String>>()
        })
        .unwrap_or_default();
    TelemetryResourceCommon {
        resource_instance_id: c.resource_instance_id().unwrap_or_default().to_string(),
        resource_name: c.resource_name().unwrap_or_default().to_string(),
        full_key: c.full_key().unwrap_or_default().to_string(),
        schema_hash: c.schema_hash().map(|s| s.to_string()),
        declared_at_ns: c.declared_at_ns(),
        labels,
    }
}

/// Decode `resource_inventory` wire bytes into `ResourceInventoryTelemetry`.
pub fn resource_inventory_from_fb(bytes: &[u8]) -> AortaResult<ResourceInventoryTelemetry> {
    let root = flatbuffers::root::<ri::ResourceInventory>(bytes).map_err(|err| {
        AortaError::internal(format!("invalid resource_inventory flatbuffer: {}", err))
    })?;
    Ok(resource_inventory_from_table(root))
}

/// Map a `ResourceInventory` table accessor (a root, or the `inventory` field
/// nested in a `VitalSnapshot`) into the in-memory struct.
fn resource_inventory_from_table(root: ri::ResourceInventory<'_>) -> ResourceInventoryTelemetry {
    let publishers = root
        .publishers()
        .map(|v| {
            v.iter()
                .map(|e| TelemetryPublisherEntry {
                    common: e.common().map(resource_common_from_fb).unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default();

    let subscribers = root
        .subscribers()
        .map(|v| {
            v.iter()
                .map(|e| TelemetrySubscriberEntry {
                    common: e.common().map(resource_common_from_fb).unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default();

    let services = root
        .services()
        .map(|v| {
            v.iter()
                .map(|e| TelemetryServiceEntry {
                    common: e.common().map(resource_common_from_fb).unwrap_or_default(),
                    request_root_type: e.request_root_type().map(|s| s.to_string()),
                    response_root_type: e.response_root_type().map(|s| s.to_string()),
                })
                .collect()
        })
        .unwrap_or_default();

    let clients = root
        .clients()
        .map(|v| {
            v.iter()
                .map(|e| TelemetryClientEntry {
                    common: e.common().map(resource_common_from_fb).unwrap_or_default(),
                    request_root_type: e.request_root_type().map(|s| s.to_string()),
                    response_root_type: e.response_root_type().map(|s| s.to_string()),
                })
                .collect()
        })
        .unwrap_or_default();

    let overflow = root
        .overflow()
        .map(|o| ResourceInventoryOverflow {
            publishers_dropped: o.publishers_dropped(),
            subscribers_dropped: o.subscribers_dropped(),
            services_dropped: o.services_dropped(),
            clients_dropped: o.clients_dropped(),
        })
        .unwrap_or_default();

    ResourceInventoryTelemetry {
        telemetry_version: root.telemetry_version(),
        sequence: root.sequence(),
        timestamp_ns: root.timestamp_ns(),
        node_id: root.node_id().unwrap_or_default().to_string(),
        group: root.group().unwrap_or_default().to_string(),
        inventory_revision: root.inventory_revision(),
        publishers,
        subscribers,
        services,
        clients,
        overflow,
    }
}

// ---------------------------------------------------------------------------
// VitalSnapshot — the merged 30 s vital record (telemetry tiering).
//
// Composes the (optional) node identity + resource inventory + the runtime
// health pulse into one self-describing FlatBuffer published at
// `aorta/{group}/sys/telemetry/vital/snapshot/{node}`. `identity`/`inventory`
// reuse the same table builders as their standalone families (so the wire shape
// stays identical, only nested); `health` is new.
// ---------------------------------------------------------------------------

fn build_runtime_health<'a>(
    b: &mut FlatBufferBuilder<'a>,
    h: &RuntimeHealth,
) -> flatbuffers::WIPOffset<vs::RuntimeHealth<'a>> {
    vs::RuntimeHealth::create(
        b,
        &vs::RuntimeHealthArgs {
            spin_count: h.spin_count,
            last_progress_ns: h.last_progress_ns,
            error_count: h.error_count,
            alive: h.alive,
        },
    )
}

fn runtime_health_from_table(h: vs::RuntimeHealth<'_>) -> RuntimeHealth {
    RuntimeHealth {
        spin_count: h.spin_count(),
        last_progress_ns: h.last_progress_ns(),
        error_count: h.error_count(),
        alive: h.alive(),
    }
}

/// Encode a `VitalSnapshot`. `None` sections are omitted (the per-family env
/// gate disables them) — a disabled family leaves a null sub-table, not a
/// dropped topic.
pub fn serialize_vital_snapshot(
    telemetry_version: u32,
    sequence: u64,
    timestamp_ns: u64,
    identity: Option<&NodeIdentityTelemetry>,
    inventory: Option<&ResourceInventoryTelemetry>,
    health: Option<&RuntimeHealth>,
) -> Vec<u8> {
    let mut b = FlatBufferBuilder::new();
    let identity = identity.map(|t| build_node_identity(&mut b, t));
    let inventory = inventory.map(|inv| build_resource_inventory(&mut b, inv));
    let health = health.map(|h| build_runtime_health(&mut b, h));
    let root = vs::VitalSnapshot::create(
        &mut b,
        &vs::VitalSnapshotArgs {
            telemetry_version,
            sequence,
            timestamp_ns,
            identity,
            inventory,
            health,
        },
    );
    b.finish(root, None);
    b.finished_data().to_vec()
}

/// Decoded sections of a `VitalSnapshot`. `introspect` consumes
/// `identity`/`inventory`; `health` is cloud-facing.
#[derive(Debug, Clone)]
pub struct VitalSnapshotParts {
    pub telemetry_version: u32,
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub identity: Option<NodeIdentityTelemetry>,
    pub inventory: Option<ResourceInventoryTelemetry>,
    pub health: Option<RuntimeHealth>,
}

/// Decode `vital/snapshot` wire bytes into its sections, reusing the per-family
/// table decoders on the nested `identity`/`inventory`.
pub fn vital_snapshot_from_fb(bytes: &[u8]) -> AortaResult<VitalSnapshotParts> {
    let root = flatbuffers::root::<vs::VitalSnapshot>(bytes).map_err(|err| {
        AortaError::internal(format!("invalid vital_snapshot flatbuffer: {}", err))
    })?;
    Ok(VitalSnapshotParts {
        telemetry_version: root.telemetry_version(),
        sequence: root.sequence(),
        timestamp_ns: root.timestamp_ns(),
        identity: root.identity().map(node_identity_from_table),
        inventory: root.inventory().map(resource_inventory_from_table),
        health: root.health().map(runtime_health_from_table),
    })
}

/// SHA-256 hash of the committed `vital_snapshot` BFBS.
pub fn vital_snapshot_schema_hash() -> SchemaHash {
    SchemaHash::from_bfbs_bytes(TELEMETRY_VITAL_SNAPSHOT_BFBS)
}

/// See [`node_identity_schema_attachment`].
pub fn vital_snapshot_schema_attachment() -> crate::ZBytes {
    crate::ZBytes::from(vital_snapshot_schema_hash().to_hex_bytes().to_vec())
}

/// SHA-256 hash of the committed `runtime_metrics` BFBS.
pub fn runtime_metrics_schema_hash() -> SchemaHash {
    SchemaHash::from_bfbs_bytes(TELEMETRY_RUNTIME_METRICS_BFBS)
}

fn build_publisher_metrics<'a>(
    b: &mut FlatBufferBuilder<'a>,
    m: &TelemetryPublisherRuntimeMetrics,
) -> flatbuffers::WIPOffset<rm::PublisherMetrics<'a>> {
    let resource_name = b.create_string(&m.resource_name);
    let resource_instance_id = b.create_string(&m.resource_instance_id);
    let schema_hash = m.schema_hash.as_deref().map(|s| b.create_string(s));
    rm::PublisherMetrics::create(
        b,
        &rm::PublisherMetricsArgs {
            resource_name: Some(resource_name),
            resource_instance_id: Some(resource_instance_id),
            schema_hash,
            publish_shm_hit_total: m.publish_shm_hit_total,
            publish_shm_fallback_total: m.publish_shm_fallback_total,
            publish_shm_required_fail_total: m.publish_shm_required_fail_total,
            loan_inflight_current: m.loan_inflight_current,
            loan_quota_exceeded_total: m.loan_quota_exceeded_total,
        },
    )
}

fn build_subscriber_metrics<'a>(
    b: &mut FlatBufferBuilder<'a>,
    m: &TelemetrySubscriberRuntimeMetrics,
) -> flatbuffers::WIPOffset<rm::SubscriberMetrics<'a>> {
    let resource_name = b.create_string(&m.resource_name);
    let resource_instance_id = b.create_string(&m.resource_instance_id);
    let schema_hash = m.schema_hash.as_deref().map(|s| b.create_string(s));
    rm::SubscriberMetrics::create(
        b,
        &rm::SubscriberMetricsArgs {
            resource_name: Some(resource_name),
            resource_instance_id: Some(resource_instance_id),
            schema_hash,
            duplicate_drop_count: m.duplicate_drop_count,
            out_of_order_count: m.out_of_order_count,
            out_of_order_drop_count: m.out_of_order_drop_count,
            executor_drop_count: m.executor_drop_count,
            local_drop_count: m.local_drop_count,
            overflow_relay_enqueue_count: m.overflow_relay_enqueue_count,
            overflow_relay_drop_count: m.overflow_relay_drop_count,
            dropped_for_deadline: m.dropped_for_deadline,
        },
    )
}

fn build_service_metrics<'a>(
    b: &mut FlatBufferBuilder<'a>,
    m: &TelemetryServiceRuntimeMetrics,
) -> flatbuffers::WIPOffset<rm::ServiceMetrics<'a>> {
    let resource_name = b.create_string(&m.resource_name);
    let resource_instance_id = b.create_string(&m.resource_instance_id);
    let schema_hash = m.schema_hash.as_deref().map(|s| b.create_string(s));
    rm::ServiceMetrics::create(
        b,
        &rm::ServiceMetricsArgs {
            resource_name: Some(resource_name),
            resource_instance_id: Some(resource_instance_id),
            schema_hash,
            request_received_count: m.request_received_count,
            request_dispatched_count: m.request_dispatched_count,
            reply_ok_count: m.reply_ok_count,
            reply_error_count: m.reply_error_count,
            reply_send_failure_count: m.reply_send_failure_count,
            request_dropped_count: m.request_dropped_count,
            query_receive_failure_count: m.query_receive_failure_count,
            task_exit_count: m.task_exit_count,
            in_flight_requests: m.in_flight_requests,
            last_request_ns: m.last_request_ns,
            last_reply_ns: m.last_reply_ns,
        },
    )
}

fn build_client_metrics<'a>(
    b: &mut FlatBufferBuilder<'a>,
    m: &TelemetryClientRuntimeMetrics,
) -> flatbuffers::WIPOffset<rm::ClientMetrics<'a>> {
    let resource_name = b.create_string(&m.resource_name);
    let resource_instance_id = b.create_string(&m.resource_instance_id);
    let schema_hash = m.schema_hash.as_deref().map(|s| b.create_string(s));
    rm::ClientMetrics::create(
        b,
        &rm::ClientMetricsArgs {
            resource_name: Some(resource_name),
            resource_instance_id: Some(resource_instance_id),
            schema_hash,
            call_count: m.call_count,
            success_count: m.success_count,
            timeout_count: m.timeout_count,
            concurrent_conflict_count: m.concurrent_conflict_count,
            error_count: m.error_count,
            retry_count: m.retry_count,
            last_call_started_ns: m.last_call_started_ns,
            last_call_finished_ns: m.last_call_finished_ns,
            last_call_latency_ns: m.last_call_latency_ns,
        },
    )
}

/// Encode `RuntimeMetricsTelemetry` into `runtime_metrics` wire bytes.
pub fn serialize_runtime_metrics(rt: &RuntimeMetricsTelemetry) -> Vec<u8> {
    let mut b = FlatBufferBuilder::new();

    let node_id = b.create_string(&rt.node_id);
    let group = b.create_string(&rt.group);

    let publisher_offsets: Vec<_> = rt
        .publishers
        .iter()
        .map(|m| build_publisher_metrics(&mut b, m))
        .collect();
    let publishers = b.create_vector(&publisher_offsets);

    let subscriber_offsets: Vec<_> = rt
        .subscribers
        .iter()
        .map(|m| build_subscriber_metrics(&mut b, m))
        .collect();
    let subscribers = b.create_vector(&subscriber_offsets);

    let service_offsets: Vec<_> = rt
        .services
        .iter()
        .map(|m| build_service_metrics(&mut b, m))
        .collect();
    let services = b.create_vector(&service_offsets);

    let client_offsets: Vec<_> = rt
        .clients
        .iter()
        .map(|m| build_client_metrics(&mut b, m))
        .collect();
    let clients = b.create_vector(&client_offsets);

    let executor = rt.executor.as_ref().map(|e| {
        // v6 per-pool-lane vectors: built BEFORE the table (FB nesting rule);
        // empty vectors are elided to keep v5-shaped payloads byte-stable.
        let mut mk = |v: &Vec<u64>| (!v.is_empty()).then(|| b.create_vector(v.as_slice()));
        let pool_lane_depth = mk(&e.pool_lane_depth);
        let pool_lane_high_water = mk(&e.pool_lane_high_water);
        let pool_lane_enqueued_total = mk(&e.pool_lane_enqueued_total);
        let pool_lane_shed_total = mk(&e.pool_lane_shed_total);
        let pool_lane_dropped_expired_total = mk(&e.pool_lane_dropped_expired_total);
        rm::ExecutorMetrics::create(
            &mut b,
            &rm::ExecutorMetricsArgs {
                pool_lane_depth,
                pool_lane_high_water,
                pool_lane_enqueued_total,
                pool_lane_shed_total,
                pool_lane_dropped_expired_total,
                queued_events: e.queued_events,
                queued_bytes: e.queued_bytes,
                peak_queued_events: e.peak_queued_events,
                peak_queued_bytes: e.peak_queued_bytes,
                dropped_queue_full: e.dropped_queue_full,
                dropped_queue_closed: e.dropped_queue_closed,
                dropped_budget_events: e.dropped_budget_events,
                dropped_budget_bytes: e.dropped_budget_bytes,
                spin_count: e.spin_count,
                lane_occupancy: e.lane_occupancy,
                lane_high_water: e.lane_high_water,
                lane_pushed_total: e.lane_pushed_total,
                dropped_expired: e.dropped_expired,
                offload_queue_depth: e.offload_queue_depth,
            },
        )
    });

    let root = rm::RuntimeMetrics::create(
        &mut b,
        &rm::RuntimeMetricsArgs {
            telemetry_version: rt.telemetry_version,
            sequence: rt.sequence,
            timestamp_ns: rt.timestamp_ns,
            node_id: Some(node_id),
            group: Some(group),
            collection_period_ms: rt.collection_period_ms,
            publishers: Some(publishers),
            subscribers: Some(subscribers),
            services: Some(services),
            clients: Some(clients),
            executor,
        },
    );
    b.finish(root, None);
    b.finished_data().to_vec()
}

fn publisher_metrics_from_fb(m: rm::PublisherMetrics<'_>) -> TelemetryPublisherRuntimeMetrics {
    TelemetryPublisherRuntimeMetrics {
        resource_name: m.resource_name().unwrap_or_default().to_string(),
        resource_instance_id: m.resource_instance_id().unwrap_or_default().to_string(),
        schema_hash: m.schema_hash().map(|s| s.to_string()),
        publish_shm_hit_total: m.publish_shm_hit_total(),
        publish_shm_fallback_total: m.publish_shm_fallback_total(),
        publish_shm_required_fail_total: m.publish_shm_required_fail_total(),
        loan_inflight_current: m.loan_inflight_current(),
        loan_quota_exceeded_total: m.loan_quota_exceeded_total(),
    }
}

fn subscriber_metrics_from_fb(m: rm::SubscriberMetrics<'_>) -> TelemetrySubscriberRuntimeMetrics {
    TelemetrySubscriberRuntimeMetrics {
        resource_name: m.resource_name().unwrap_or_default().to_string(),
        resource_instance_id: m.resource_instance_id().unwrap_or_default().to_string(),
        schema_hash: m.schema_hash().map(|s| s.to_string()),
        duplicate_drop_count: m.duplicate_drop_count(),
        out_of_order_count: m.out_of_order_count(),
        out_of_order_drop_count: m.out_of_order_drop_count(),
        executor_drop_count: m.executor_drop_count(),
        local_drop_count: m.local_drop_count(),
        overflow_relay_enqueue_count: m.overflow_relay_enqueue_count(),
        overflow_relay_drop_count: m.overflow_relay_drop_count(),
        dropped_for_deadline: m.dropped_for_deadline(),
    }
}

fn service_metrics_from_fb(m: rm::ServiceMetrics<'_>) -> TelemetryServiceRuntimeMetrics {
    TelemetryServiceRuntimeMetrics {
        resource_name: m.resource_name().unwrap_or_default().to_string(),
        resource_instance_id: m.resource_instance_id().unwrap_or_default().to_string(),
        schema_hash: m.schema_hash().map(|s| s.to_string()),
        request_received_count: m.request_received_count(),
        request_dispatched_count: m.request_dispatched_count(),
        reply_ok_count: m.reply_ok_count(),
        reply_error_count: m.reply_error_count(),
        reply_send_failure_count: m.reply_send_failure_count(),
        request_dropped_count: m.request_dropped_count(),
        query_receive_failure_count: m.query_receive_failure_count(),
        task_exit_count: m.task_exit_count(),
        in_flight_requests: m.in_flight_requests(),
        last_request_ns: m.last_request_ns(),
        last_reply_ns: m.last_reply_ns(),
    }
}

fn client_metrics_from_fb(m: rm::ClientMetrics<'_>) -> TelemetryClientRuntimeMetrics {
    TelemetryClientRuntimeMetrics {
        resource_name: m.resource_name().unwrap_or_default().to_string(),
        resource_instance_id: m.resource_instance_id().unwrap_or_default().to_string(),
        schema_hash: m.schema_hash().map(|s| s.to_string()),
        call_count: m.call_count(),
        success_count: m.success_count(),
        timeout_count: m.timeout_count(),
        concurrent_conflict_count: m.concurrent_conflict_count(),
        error_count: m.error_count(),
        retry_count: m.retry_count(),
        last_call_started_ns: m.last_call_started_ns(),
        last_call_finished_ns: m.last_call_finished_ns(),
        last_call_latency_ns: m.last_call_latency_ns(),
    }
}

/// Decode `runtime_metrics` wire bytes into `RuntimeMetricsTelemetry`.
pub fn runtime_metrics_from_fb(bytes: &[u8]) -> AortaResult<RuntimeMetricsTelemetry> {
    let root = flatbuffers::root::<rm::RuntimeMetrics>(bytes).map_err(|err| {
        AortaError::internal(format!("invalid runtime_metrics flatbuffer: {}", err))
    })?;

    let publishers = root
        .publishers()
        .map(|v| v.iter().map(publisher_metrics_from_fb).collect())
        .unwrap_or_default();
    let subscribers = root
        .subscribers()
        .map(|v| v.iter().map(subscriber_metrics_from_fb).collect())
        .unwrap_or_default();
    let services = root
        .services()
        .map(|v| v.iter().map(service_metrics_from_fb).collect())
        .unwrap_or_default();
    let clients = root
        .clients()
        .map(|v| v.iter().map(client_metrics_from_fb).collect())
        .unwrap_or_default();

    let executor = root.executor().map(|e| TelemetryExecutorRuntimeMetrics {
        queued_events: e.queued_events(),
        queued_bytes: e.queued_bytes(),
        peak_queued_events: e.peak_queued_events(),
        peak_queued_bytes: e.peak_queued_bytes(),
        dropped_queue_full: e.dropped_queue_full(),
        dropped_queue_closed: e.dropped_queue_closed(),
        dropped_budget_events: e.dropped_budget_events(),
        dropped_budget_bytes: e.dropped_budget_bytes(),
        spin_count: e.spin_count(),
        lane_occupancy: e.lane_occupancy(),
        lane_high_water: e.lane_high_water(),
        lane_pushed_total: e.lane_pushed_total(),
        dropped_expired: e.dropped_expired(),
        offload_queue_depth: e.offload_queue_depth(),
        pool_lane_depth: e
            .pool_lane_depth()
            .map(|v| v.iter().collect())
            .unwrap_or_default(),
        pool_lane_high_water: e
            .pool_lane_high_water()
            .map(|v| v.iter().collect())
            .unwrap_or_default(),
        pool_lane_enqueued_total: e
            .pool_lane_enqueued_total()
            .map(|v| v.iter().collect())
            .unwrap_or_default(),
        pool_lane_shed_total: e
            .pool_lane_shed_total()
            .map(|v| v.iter().collect())
            .unwrap_or_default(),
        pool_lane_dropped_expired_total: e
            .pool_lane_dropped_expired_total()
            .map(|v| v.iter().collect())
            .unwrap_or_default(),
    });

    Ok(RuntimeMetricsTelemetry {
        telemetry_version: root.telemetry_version(),
        sequence: root.sequence(),
        timestamp_ns: root.timestamp_ns(),
        node_id: root.node_id().unwrap_or_default().to_string(),
        group: root.group().unwrap_or_default().to_string(),
        collection_period_ms: root.collection_period_ms(),
        publishers,
        subscribers,
        services,
        clients,
        executor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> NodeIdentityTelemetry {
        let mut labels = BTreeMap::new();
        labels.insert("role".to_string(), "leader".to_string());
        labels.insert("zone".to_string(), "a".to_string());
        NodeIdentityTelemetry {
            telemetry_version: 3,
            sequence: 42,
            timestamp_ns: 1_234_567_890,
            node_id: "node-abc".to_string(),
            node_name: "planner".to_string(),
            group: "0examplesn42".to_string(),
            host_id: "host-1".to_string(),
            host_name: "robot01".to_string(),
            process_start_time_ns: 99,
            pid: 4242,
            executable: "/usr/bin/planner".to_string(),
            language: "rust".to_string(),
            sdk_name: "aorta_rust".to_string(),
            sdk_version: "0.1.0".to_string(),
            session_zid: "abcd1234".to_string(),
            connected_router_zids: vec!["r1".to_string(), "r2".to_string()],
            container_id: Some("ctr-9".to_string()),
            build_id: None,
            labels,
        }
    }

    #[test]
    fn node_identity_fb_roundtrip_full() {
        let original = sample();
        let bytes = serialize_node_identity(&original);
        let decoded = node_identity_from_fb(&bytes).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn node_identity_fb_roundtrip_empty_optionals() {
        let mut t = sample();
        t.container_id = None;
        t.build_id = None;
        t.connected_router_zids = vec![];
        t.labels = BTreeMap::new();
        let bytes = serialize_node_identity(&t);
        let decoded = node_identity_from_fb(&bytes).expect("decode");
        assert_eq!(t, decoded);
    }

    #[test]
    fn node_identity_from_fb_rejects_garbage() {
        assert!(node_identity_from_fb(&[0xde, 0xad, 0xbe, 0xef]).is_err());
    }

    #[test]
    fn node_identity_schema_hash_is_64_hex_and_stable() {
        let h = node_identity_schema_hash();
        assert_eq!(h.to_hex().len(), 64);
        assert_eq!(h, SchemaHash::from_bfbs_bytes(TELEMETRY_NODE_IDENTITY_BFBS));
    }

    // Drift check between the committed TELEMETRY_NODE_IDENTITY_BFBS snapshot and
    // the BFBS freshly compiled by build.rs. Cargo-only (Bazel does not run build
    // scripts, so OUT_DIR / has_fresh_bfbs are absent there).
    #[cfg(has_fresh_bfbs)]
    include!(concat!(
        env!("OUT_DIR"),
        "/telemetry_node_identity_bfbs_fresh.rs"
    ));

    #[cfg(has_fresh_bfbs)]
    #[test]
    fn node_identity_bfbs_constant_matches_compiled_schema() {
        // If this fails, re-generate TELEMETRY_NODE_IDENTITY_BFBS in
        // telemetry_node_identity_generated.rs from the .fbs (see file header).
        assert_eq!(
            TELEMETRY_NODE_IDENTITY_BFBS, TELEMETRY_NODE_IDENTITY_BFBS_FRESH,
            "TELEMETRY_NODE_IDENTITY_BFBS is stale; re-generate from \
             message/schemas/system/telemetry_node_identity.fbs"
        );
    }

    fn sample_common(tag: &str, with_optionals: bool) -> TelemetryResourceCommon {
        let mut labels = BTreeMap::new();
        labels.insert("k".to_string(), tag.to_string());
        TelemetryResourceCommon {
            resource_instance_id: format!("{tag}:1"),
            resource_name: tag.to_string(),
            full_key: format!("aorta/default/pub/{tag}"),
            schema_hash: with_optionals.then(|| "a".repeat(64)),
            declared_at_ns: with_optionals.then_some(123),
            labels,
        }
    }

    fn sample_inventory() -> ResourceInventoryTelemetry {
        ResourceInventoryTelemetry {
            telemetry_version: 3,
            sequence: 7,
            timestamp_ns: 999,
            node_id: "node-x".to_string(),
            group: "g".to_string(),
            inventory_revision: 5,
            publishers: vec![TelemetryPublisherEntry {
                common: sample_common("p", true),
            }],
            subscribers: vec![TelemetrySubscriberEntry {
                common: sample_common("s", true),
            }],
            services: vec![TelemetryServiceEntry {
                common: sample_common("svc", false),
                request_root_type: Some("Req".to_string()),
                response_root_type: None,
            }],
            clients: vec![TelemetryClientEntry {
                common: sample_common("cli", false),
                request_root_type: None,
                response_root_type: Some("Resp".to_string()),
            }],
            overflow: ResourceInventoryOverflow {
                publishers_dropped: 1,
                subscribers_dropped: 0,
                services_dropped: 2,
                clients_dropped: 0,
            },
        }
    }

    #[test]
    fn resource_inventory_fb_roundtrip_full() {
        let original = sample_inventory();
        let bytes = serialize_resource_inventory(&original);
        let decoded = resource_inventory_from_fb(&bytes).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn resource_inventory_fb_roundtrip_empty() {
        let t = ResourceInventoryTelemetry {
            telemetry_version: 3,
            sequence: 0,
            timestamp_ns: 0,
            node_id: String::new(),
            group: String::new(),
            inventory_revision: 0,
            publishers: vec![],
            subscribers: vec![],
            services: vec![],
            clients: vec![],
            overflow: ResourceInventoryOverflow::default(),
        };
        let bytes = serialize_resource_inventory(&t);
        assert_eq!(t, resource_inventory_from_fb(&bytes).expect("decode"));
    }

    #[cfg(has_fresh_bfbs)]
    include!(concat!(
        env!("OUT_DIR"),
        "/telemetry_resource_inventory_bfbs_fresh.rs"
    ));

    #[cfg(has_fresh_bfbs)]
    #[test]
    fn resource_inventory_bfbs_constant_matches_compiled_schema() {
        assert_eq!(
            TELEMETRY_RESOURCE_INVENTORY_BFBS, TELEMETRY_RESOURCE_INVENTORY_BFBS_FRESH,
            "TELEMETRY_RESOURCE_INVENTORY_BFBS is stale; re-generate from \
             message/schemas/system/telemetry_resource_inventory.fbs"
        );
    }

    fn sample_runtime() -> RuntimeMetricsTelemetry {
        RuntimeMetricsTelemetry {
            telemetry_version: 3,
            sequence: 11,
            timestamp_ns: 222,
            node_id: "node-r".to_string(),
            group: "g".to_string(),
            collection_period_ms: 5000,
            publishers: vec![TelemetryPublisherRuntimeMetrics {
                resource_name: "p".to_string(),
                resource_instance_id: "publisher:1".to_string(),
                schema_hash: Some("h".repeat(64)),
                publish_shm_hit_total: 10,
                publish_shm_fallback_total: 1,
                publish_shm_required_fail_total: 0,
                loan_inflight_current: 2,
                loan_quota_exceeded_total: 3,
            }],
            subscribers: vec![TelemetrySubscriberRuntimeMetrics {
                resource_name: "s".to_string(),
                resource_instance_id: "subscriber:1".to_string(),
                schema_hash: None,
                duplicate_drop_count: 1,
                out_of_order_count: 2,
                out_of_order_drop_count: 3,
                executor_drop_count: 4,
                local_drop_count: 5,
                overflow_relay_enqueue_count: 6,
                overflow_relay_drop_count: 7,
                dropped_for_deadline: 8,
            }],
            services: vec![TelemetryServiceRuntimeMetrics {
                resource_name: "svc".to_string(),
                resource_instance_id: "service:1".to_string(),
                schema_hash: None,
                request_received_count: 1,
                request_dispatched_count: 2,
                reply_ok_count: 3,
                reply_error_count: 4,
                reply_send_failure_count: 5,
                request_dropped_count: 6,
                query_receive_failure_count: 7,
                task_exit_count: 8,
                in_flight_requests: 9,
                last_request_ns: 10,
                last_reply_ns: 11,
            }],
            clients: vec![TelemetryClientRuntimeMetrics {
                resource_name: "cli".to_string(),
                resource_instance_id: "client:1".to_string(),
                schema_hash: Some("c".repeat(64)),
                call_count: 1,
                success_count: 2,
                timeout_count: 3,
                concurrent_conflict_count: 4,
                error_count: 5,
                retry_count: 6,
                last_call_started_ns: 7,
                last_call_finished_ns: 8,
                last_call_latency_ns: 9,
            }],
            executor: Some(TelemetryExecutorRuntimeMetrics {
                queued_events: 1,
                queued_bytes: 2,
                peak_queued_events: 3,
                peak_queued_bytes: 4,
                dropped_queue_full: 5,
                dropped_queue_closed: 6,
                dropped_budget_events: 7,
                dropped_budget_bytes: 8,
                spin_count: 9,
                lane_occupancy: 2,
                lane_high_water: 5,
                lane_pushed_total: 40,
                dropped_expired: 3,
                offload_queue_depth: 1,
                pool_lane_depth: vec![1, 0, 7, 0],
                pool_lane_high_water: vec![2, 0, 9, 1],
                pool_lane_enqueued_total: vec![10, 0, 500, 3],
                pool_lane_shed_total: vec![0, 0, 4, 0],
                pool_lane_dropped_expired_total: vec![0, 0, 1, 0],
            }),
        }
    }

    #[test]
    fn runtime_metrics_fb_roundtrip_full() {
        let original = sample_runtime();
        let bytes = serialize_runtime_metrics(&original);
        let decoded = runtime_metrics_from_fb(&bytes).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn runtime_metrics_fb_roundtrip_no_executor_empty_lists() {
        let mut t = sample_runtime();
        t.executor = None;
        t.publishers.clear();
        t.subscribers.clear();
        t.services.clear();
        t.clients.clear();
        let bytes = serialize_runtime_metrics(&t);
        assert_eq!(t, runtime_metrics_from_fb(&bytes).expect("decode"));
    }

    #[cfg(has_fresh_bfbs)]
    include!(concat!(
        env!("OUT_DIR"),
        "/telemetry_runtime_metrics_bfbs_fresh.rs"
    ));

    #[cfg(has_fresh_bfbs)]
    #[test]
    fn runtime_metrics_bfbs_constant_matches_compiled_schema() {
        assert_eq!(
            TELEMETRY_RUNTIME_METRICS_BFBS, TELEMETRY_RUNTIME_METRICS_BFBS_FRESH,
            "TELEMETRY_RUNTIME_METRICS_BFBS is stale; re-generate from \
             message/schemas/system/telemetry_runtime_metrics.fbs"
        );
    }

    #[test]
    fn schema_attachments_are_the_64_hex_schema_hash() {
        for (attachment, hash) in [
            (
                node_identity_schema_attachment(),
                node_identity_schema_hash(),
            ),
            (
                resource_inventory_schema_attachment(),
                resource_inventory_schema_hash(),
            ),
            (
                runtime_metrics_schema_attachment(),
                runtime_metrics_schema_hash(),
            ),
            (
                vital_snapshot_schema_attachment(),
                vital_snapshot_schema_hash(),
            ),
        ] {
            // The attachment is exactly the 64-char ASCII-hex schema hash that a
            // recorder reads back and resolves by-hash.
            assert_eq!(attachment.to_bytes().as_ref(), hash.to_hex().as_bytes());
        }
    }

    fn sample_health() -> RuntimeHealth {
        RuntimeHealth {
            spin_count: 1_234,
            last_progress_ns: 9_876_543_210,
            error_count: 3,
            alive: true,
        }
    }

    #[test]
    fn vital_snapshot_fb_roundtrip_full() {
        let identity = sample();
        let inventory = sample_inventory();
        let health = sample_health();
        let bytes = serialize_vital_snapshot(
            5,
            7,
            1_000_000,
            Some(&identity),
            Some(&inventory),
            Some(&health),
        );
        let parts = vital_snapshot_from_fb(&bytes).expect("decode");
        assert_eq!(parts.telemetry_version, 5);
        assert_eq!(parts.sequence, 7);
        assert_eq!(parts.timestamp_ns, 1_000_000);
        // Nested sections decode through the SAME table decoders as the
        // standalone families, so the in-memory structs round-trip identically.
        assert_eq!(parts.identity.as_ref(), Some(&identity));
        assert_eq!(parts.inventory.as_ref(), Some(&inventory));
        assert_eq!(parts.health, Some(health));
    }

    #[test]
    fn vital_snapshot_fb_omits_disabled_sections() {
        // A per-family env gate set to off leaves a null sub-table (not a
        // dropped topic); `health` is always present.
        let health = sample_health();
        let bytes = serialize_vital_snapshot(5, 1, 0, None, None, Some(&health));
        let parts = vital_snapshot_from_fb(&bytes).expect("decode");
        assert!(parts.identity.is_none());
        assert!(parts.inventory.is_none());
        assert_eq!(parts.health, Some(health));
    }

    #[test]
    fn vital_snapshot_from_fb_rejects_garbage() {
        assert!(vital_snapshot_from_fb(&[0xde, 0xad, 0xbe, 0xef]).is_err());
    }

    #[test]
    fn vital_snapshot_schema_hash_is_64_hex_and_stable() {
        let h = vital_snapshot_schema_hash();
        assert_eq!(h.to_hex().len(), 64);
        assert_eq!(
            h,
            SchemaHash::from_bfbs_bytes(TELEMETRY_VITAL_SNAPSHOT_BFBS)
        );
    }

    #[cfg(has_fresh_bfbs)]
    include!(concat!(
        env!("OUT_DIR"),
        "/telemetry_vital_snapshot_bfbs_fresh.rs"
    ));

    #[cfg(has_fresh_bfbs)]
    #[test]
    fn vital_snapshot_bfbs_constant_matches_compiled_schema() {
        assert_eq!(
            TELEMETRY_VITAL_SNAPSHOT_BFBS, TELEMETRY_VITAL_SNAPSHOT_BFBS_FRESH,
            "TELEMETRY_VITAL_SNAPSHOT_BFBS is stale; re-generate from \
             message/schemas/system/telemetry_vital_snapshot.fbs"
        );
    }
}
