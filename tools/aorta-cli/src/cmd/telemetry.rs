//! `aorta telemetry watch` — stream the system telemetry plane.
//!
//! Every Node publishes two per-node telemetry keys:
//! - `aorta/{group}/sys/telemetry/vital/snapshot/{node}` — `VitalSnapshot` fbs
//!   (nested identity + inventory + health),
//! - `aorta/{group}/sys/telemetry/bulk/runtime/{node}` — `RuntimeMetrics` fbs.
//!
//! This command subscribes to that plane, optionally filtered by node/family,
//! decodes each sample to JSON via the embedded reflection schemas, and streams
//! one `{key} {json}` line per sample.

use crate::util::to_hex;
use anyhow::{anyhow, Result};
use aorta_core::telemetry::fb::{TELEMETRY_RUNTIME_METRICS_BFBS, TELEMETRY_VITAL_SNAPSHOT_BFBS};
use aorta_introspect::reflect;

/// Build the subscribe key from the `--kind` family and optional `--node` filter.
/// - `kind = None`   → whole plane (`**`, or `*/*/{node}` when a node is pinned),
/// - `kind = vital`  → `…/vital/snapshot/{node|*}`,
/// - `kind = runtime`→ `…/bulk/runtime/{node|*}`.
fn telemetry_watch_key(group: &str, kind: Option<&str>, node: Option<&str>) -> String {
    let node_seg = node.unwrap_or("*");
    match kind {
        Some("vital") => format!("aorta/{}/sys/telemetry/vital/snapshot/{}", group, node_seg),
        Some("runtime") => format!("aorta/{}/sys/telemetry/bulk/runtime/{}", group, node_seg),
        // `None` (clap's `value_parser` already rejects any other `Some(..)`, so this
        // also serves as the catch-all) → whole plane. With a node pinned, `*/*/{node}`
        // (tier/family/node) matches BOTH per-node families, which are exactly two
        // segments deep: `vital/snapshot/{node}` and `bulk/runtime/{node}`. This depth
        // assumption is intentional; a node-less key like the global
        // `vital/ctx/owner_conflict` only collides if a user literally passes
        // `--node owner_conflict`, in which case `decode_to_json` rejects it and the
        // watch loop falls through to the hex display (no crash, just a WARN line).
        _ => match node {
            Some(node) => format!("aorta/{}/sys/telemetry/*/*/{}", group, node),
            None => format!("aorta/{}/sys/telemetry/**", group),
        },
    }
}

/// Decode a telemetry sample to JSON, selecting the reflection schema by the
/// family segment in the key. Errors for keys outside the two known families.
fn decode_to_json(key: &str, payload: &[u8]) -> Result<String> {
    let bfbs: &[u8] = if key.contains("/vital/snapshot/") {
        TELEMETRY_VITAL_SNAPSHOT_BFBS
    } else if key.contains("/bulk/runtime/") {
        TELEMETRY_RUNTIME_METRICS_BFBS
    } else {
        return Err(anyhow!("unrecognized telemetry family"));
    };
    reflect::fb_to_json(bfbs, payload, None).map_err(|err| anyhow!("decode failed: {}", err))
}

pub async fn watch(
    group: Option<&str>,
    node: Option<&str>,
    kind: Option<&str>,
    count: Option<usize>,
    raw: bool,
) -> Result<()> {
    let group = group.unwrap_or("default");
    let key = telemetry_watch_key(group, kind, node);

    let session = crate::cmd::open_session().await?;
    let subscriber = session
        .declare_subscriber(&key)
        .await
        .map_err(|e| anyhow!("Failed to subscribe to {}: {}", key, e))?;

    eprintln!("Watching telemetry '{}' (Ctrl-C to stop)...", key);
    if let Some(n) = count {
        eprintln!("Will print {} messages", n);
    }
    eprintln!("(vital snapshots ~30s, runtime metrics ~5s cadence; may wait silently)");

    let mut printed = 0usize;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                eprintln!("Stopped telemetry watch.");
                break;
            }
            res = subscriber.recv_async() => {
                match res {
                    Ok(sample) => {
                        let key = sample.key_expr().as_str().to_string();
                        let payload = sample.payload().to_bytes();
                        if raw {
                            println!("{} {}", key, to_hex(&payload));
                        } else {
                            match decode_to_json(&key, &payload) {
                                Ok(json) => println!("{} {}", key, json),
                                Err(err) => {
                                    eprintln!("WARN: failed to decode {}: {}", key, err);
                                    println!("{} (hex) {}", key, to_hex(&payload));
                                }
                            }
                        }
                        printed += 1;
                        if let Some(n) = count {
                            if printed >= n {
                                break;
                            }
                        }
                    }
                    Err(err) => return Err(anyhow!("telemetry subscription closed: {}", err)),
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{decode_to_json, telemetry_watch_key};
    use aorta_core::telemetry::fb::{serialize_runtime_metrics, serialize_vital_snapshot};
    use aorta_core::telemetry::schema::RuntimeMetricsTelemetry;

    #[test]
    fn telemetry_watch_key_maps_kind_and_node() {
        assert_eq!(
            telemetry_watch_key("default", None, None),
            "aorta/default/sys/telemetry/**"
        );
        assert_eq!(
            telemetry_watch_key("default", None, Some("node1")),
            "aorta/default/sys/telemetry/*/*/node1"
        );
        assert_eq!(
            telemetry_watch_key("default", Some("vital"), None),
            "aorta/default/sys/telemetry/vital/snapshot/*"
        );
        assert_eq!(
            telemetry_watch_key("robots", Some("runtime"), Some("abc")),
            "aorta/robots/sys/telemetry/bulk/runtime/abc"
        );
        // Documented edge case: `--node owner_conflict` glob still only matches the
        // 2-deep per-node families; the global vital/ctx/owner_conflict key would
        // collide, but decode_to_json rejects it (see decode_to_json_rejects_unknown_family).
        assert_eq!(
            telemetry_watch_key("default", None, Some("owner_conflict")),
            "aorta/default/sys/telemetry/*/*/owner_conflict"
        );
    }

    #[test]
    fn decode_to_json_round_trips_runtime_metrics() {
        let rt = RuntimeMetricsTelemetry {
            telemetry_version: 1,
            sequence: 7,
            timestamp_ns: 123,
            node_id: "node1".to_string(),
            group: "default".to_string(),
            collection_period_ms: 1000,
            publishers: Vec::new(),
            subscribers: Vec::new(),
            services: Vec::new(),
            clients: Vec::new(),
            executor: None,
        };
        let bytes = serialize_runtime_metrics(&rt);
        let key = "aorta/default/sys/telemetry/bulk/runtime/node1";
        let json = decode_to_json(key, &bytes).expect("decode");
        assert!(json.contains("timestamp_ns"), "json was: {}", json);
    }

    #[test]
    fn decode_to_json_round_trips_vital_snapshot() {
        // Minimal vital snapshot (scalar header only) exercises the second BFBS branch.
        let bytes = serialize_vital_snapshot(1, 2, 456, None, None, None);
        let key = "aorta/default/sys/telemetry/vital/snapshot/node1";
        let json = decode_to_json(key, &bytes).expect("decode");
        assert!(json.contains("timestamp_ns"), "json was: {}", json);
    }

    #[test]
    fn decode_to_json_rejects_unknown_family() {
        // The ctx/owner_conflict vital key is not one of the two node families.
        let key = "aorta/default/sys/telemetry/vital/ctx/owner_conflict";
        assert!(decode_to_json(key, &[]).is_err());
    }
}
