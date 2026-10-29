use sha2::{Digest, Sha256};

use crate::error::AortaResult;
use crate::node::Node;
use crate::runtime_env;
use crate::telemetry::schema::{
    HostIdentity, NodeIdentityTelemetry, TelemetryNodeMetadata, TELEMETRY_SCHEMA_VERSION,
};

const DEFAULT_LANGUAGE: &str = "rust";
const DEFAULT_SDK_NAME: &str = "aorta_core";
const DEFAULT_HOST_NAME: &str = "unknown-host";

pub fn resolve_host_identity() -> HostIdentity {
    let host_name = runtime_env::var("AORTA_HOST_NAME")
        .or_else(|| runtime_env::var("HOSTNAME"))
        .or_else(|| runtime_env::var("COMPUTERNAME"))
        .or_else(read_etc_hostname)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_HOST_NAME.to_string());

    let host_id = runtime_env::var("AORTA_HOST_ID")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| stable_host_id(&host_name));

    HostIdentity { host_id, host_name }
}

pub fn build_node_identity_telemetry(node: &Node) -> NodeIdentityTelemetry {
    build_node_identity_telemetry_base(
        node.id(),
        node.name(),
        node.group(),
        node.host_identity(),
        node.process_start_time_ns(),
        std::process::id(),
        &node.session_zid(),
        node.telemetry_metadata(),
    )
}

pub(crate) fn build_node_identity_telemetry_base(
    node_id: &str,
    node_name: &str,
    group: &str,
    host_identity: &HostIdentity,
    process_start_time_ns: u64,
    pid: u32,
    session_zid: &str,
    metadata: &TelemetryNodeMetadata,
) -> NodeIdentityTelemetry {
    let metadata = metadata.normalized();

    NodeIdentityTelemetry {
        telemetry_version: TELEMETRY_SCHEMA_VERSION,
        sequence: 0,
        timestamp_ns: crate::time::unix_time_ns(),
        node_id: node_id.to_string(),
        node_name: node_name.to_string(),
        group: group.to_string(),
        host_id: host_identity.host_id.clone(),
        host_name: host_identity.host_name.clone(),
        process_start_time_ns,
        pid,
        executable: effective_executable(&metadata),
        language: effective_language(&metadata),
        sdk_name: effective_sdk_name(&metadata),
        sdk_version: effective_sdk_version(&metadata),
        session_zid: session_zid.to_string(),
        // BASE snapshot only — the vital heartbeat worker refreshes this from
        // the live session per tick via [`connected_router_zids`].
        connected_router_zids: Vec::new(),
        container_id: metadata.container_id,
        build_id: metadata.build_id,
        labels: metadata.labels,
    }
}

/// Snapshot of the router ZIDs this session is currently connected to, sorted
/// for deterministic telemetry payloads. Previously a permanent hardcoded
/// empty stub, which left router death and link loss
/// invisible to fleet telemetry.
pub(crate) async fn connected_router_zids(session: &crate::session::Session) -> Vec<String> {
    let mut zids: Vec<String> = session
        .inner()
        .info()
        .routers_zid()
        .await
        .map(|zid| zid.to_string())
        .collect();
    zids.sort();
    zids
}

fn effective_executable(metadata: &TelemetryNodeMetadata) -> String {
    metadata
        .executable
        .clone()
        .or_else(detect_executable)
        .unwrap_or_else(|| "unknown".to_string())
}

fn effective_language(metadata: &TelemetryNodeMetadata) -> String {
    metadata
        .language
        .clone()
        .unwrap_or_else(|| DEFAULT_LANGUAGE.to_string())
}

fn effective_sdk_name(metadata: &TelemetryNodeMetadata) -> String {
    metadata
        .sdk_name
        .clone()
        .unwrap_or_else(|| DEFAULT_SDK_NAME.to_string())
}

fn effective_sdk_version(metadata: &TelemetryNodeMetadata) -> String {
    metadata
        .sdk_version
        .clone()
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

fn detect_executable() -> Option<String> {
    std::env::current_exe()
        .ok()
        .map(|path| path.display().to_string())
        .filter(|path| !path.trim().is_empty())
}

fn read_etc_hostname() -> Option<String> {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|value| value.trim().to_string())
}

fn stable_host_id(host_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(host_name.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Domain/version tag mixed into the derived node id so a future change to the
/// derivation scheme cannot silently collide with v1 ids.
const NODE_ID_DERIVATION_DOMAIN: &[u8] = b"aorta-node-id-v1\0";

/// Derive a stable, fleet-meaningful `node_id`.
///
/// Priority:
/// 1. `AORTA_NODE_ID` operator override — used verbatim after key-segment
///    validation, so operators can assign readable, controllable identities
///    (e.g. `robot7-planner`). A malformed override fails loudly here rather
///    than later at key-construction time.
/// 2. Deterministic `SHA-256(domain ‖ host_id ‖ group ‖ node_name)` truncated to
///    32 hex chars. The same `(host, group, name)` therefore yields the same id
///    across process restarts — enabling cross-restart correlation and fleet
///    identity — while a different host or name yields a different id.
///
/// Truncation to 32 hex (128 bits) matches the historical `Uuid::simple()` width
/// and keeps the collision space far beyond fleet scale. Note two *live* nodes
/// sharing `(host, group, name)` now derive the SAME id: same-process duplicates
/// are already rejected by `NodeIdentity`, and a cross-process duplicate is a
/// genuine misconfiguration that this makes *visible* (the two collide on the
/// telemetry and liveliness keys) instead of silently diverging.
pub fn derive_node_id(host_id: &str, group: &str, node_name: &str) -> AortaResult<String> {
    if let Some(explicit) = runtime_env::var("AORTA_NODE_ID")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return crate::key_expr::validate_node_id(&explicit);
    }

    let mut hasher = Sha256::new();
    hasher.update(NODE_ID_DERIVATION_DOMAIN);
    hasher.update(host_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(group.as_bytes());
    hasher.update(b"\0");
    hasher.update(node_name.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    Ok(digest[..32].to_string())
}
