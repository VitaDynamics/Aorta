use anyhow::Result;
use aorta_core::contextstore_fb::{
    context_value_schema_hash, root_as_context_value, CONTEXT_VALUE_BFBS,
};
use aorta_core::schema::SchemaHash;
use aorta_core::schema_resolve::SchemaResolver;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};

use crate::config::LogLevel;
use crate::logln;
use crate::server::{ChannelSchema, Server};
use crate::RunSummary;

#[derive(Debug, Clone)]
pub struct Sample {
    pub full_key: String,
    pub key_suffix: String,
    pub attachment_hash: Option<String>,
    pub payload: Vec<u8>,
    pub log_time_ns: u64,
    pub publish_time_ns: u64,
}

pub struct Inputs {
    pub server: Arc<Server>,
    pub resolver: Arc<SchemaResolver>,
    pub sample_rx: mpsc::Receiver<Sample>,
    pub log_level: LogLevel,
}

#[derive(Debug, Default)]
struct WarnState {
    last_emit: HashMap<String, Instant>,
    suppressed: HashMap<String, u64>,
}

impl WarnState {
    fn warn(&mut self, key: &str, message: &str, log_level: LogLevel) {
        let now = Instant::now();
        let last = self.last_emit.get(key).copied();
        if last
            .map(|ts| now.duration_since(ts) < Duration::from_secs(10))
            .unwrap_or(false)
        {
            *self.suppressed.entry(key.to_string()).or_insert(0) += 1;
            return;
        }
        let suppressed = self.suppressed.remove(key).unwrap_or(0);
        self.last_emit.insert(key.to_string(), now);
        if suppressed > 0 {
            logln(
                log_level,
                LogLevel::Warn,
                &format!("WARN: {} (suppressed {} similar)", message, suppressed),
            );
        } else {
            logln(log_level, LogLevel::Warn, &format!("WARN: {}", message));
        }
    }
}

/// Which key suffixes get a runtime schema resolve. Mirrors
/// `aorta-record::should_resolve_schema` — `pub/**` plus the telemetry
/// families that publish with a schema-hash attachment. Keys outside this
/// set (e.g. `sys/telemetry/vital/ctx/owner_conflict`, which publishes
/// without an attachment) would only time out on a by-key GET, so they are
/// forwarded raw. Keep the two lists in sync until the policy moves into
/// `aorta_core` (follow-up noted in the design doc).
fn should_resolve_schema(key_suffix: &str) -> bool {
    key_suffix.starts_with("pub/")
        // Tiered telemetry (current):
        || key_suffix.starts_with("sys/telemetry/vital/snapshot/")
        || key_suffix.starts_with("sys/telemetry/bulk/runtime/")
        // Legacy per-family telemetry (mixed-version deployments):
        || key_suffix.starts_with("sys/telemetry/node_identity/")
        || key_suffix.starts_with("sys/telemetry/resource_inventory/")
        || key_suffix.starts_with("sys/telemetry/runtime/")
}

/// Context keys (`aorta/{group}/ctx/**`).
fn is_ctx_key(key_suffix: &str) -> bool {
    key_suffix.starts_with("ctx/")
}

/// Schema for a context value. ctx puts carry no schema-hash attachment but
/// all share one compile-time-known schema — `aorta.ctx.ContextValue` — so
/// there is nothing to resolve at runtime. Embedding its committed BFBS
/// advertises a typed `flatbuffer` channel that Foxglove can decode instead
/// of the opaque `aorta_raw_bytes` encoding it rejects (same fix as the
/// recorder's `ctx_record_schema`). Returns `None` (→ raw channel)
/// when the payload does not parse as a `ContextValue`, so a corrupt or
/// future non-ContextValue put is forwarded faithfully rather than
/// mislabeled with a schema that would mis-decode it.
pub(crate) fn ctx_channel_schema(payload: &[u8]) -> Option<ChannelSchema> {
    root_as_context_value(payload).map(|_| ChannelSchema {
        hash_hex: context_value_schema_hash().to_hex(),
        bfbs: CONTEXT_VALUE_BFBS.to_vec(),
    })
}

pub async fn run(mut inputs: Inputs, mut stop_rx: broadcast::Receiver<()>) -> Result<RunSummary> {
    let mut warnings = WarnState::default();
    let mut summary = RunSummary::default();

    loop {
        tokio::select! {
            _ = stop_rx.recv() => break,
            sample = inputs.sample_rx.recv() => {
                let Some(sample) = sample else { break; };

                let schema = if is_ctx_key(&sample.key_suffix) {
                    ctx_channel_schema(&sample.payload)
                } else if should_resolve_schema(&sample.key_suffix) {
                    let attachment_hash = sample
                        .attachment_hash
                        .as_deref()
                        .and_then(SchemaHash::from_hex_str);
                    match inputs
                        .resolver
                        .resolve(&sample.key_suffix, attachment_hash.as_ref())
                        .await
                    {
                        Some(resolved) => Some(ChannelSchema {
                            hash_hex: resolved.schema_hash.to_hex(),
                            bfbs: resolved.bfbs,
                        }),
                        None => {
                            summary.schema_miss_count =
                                summary.schema_miss_count.saturating_add(1);
                            warnings.warn(
                                &format!("schema_miss:{}", sample.key_suffix),
                                &format!(
                                    "schema not found for key suffix '{}'",
                                    sample.key_suffix
                                ),
                                inputs.log_level,
                            );
                            None
                        }
                    }
                } else {
                    None
                };

                // Foxglove `log_time` should reflect publisher time when
                // available so the timeline matches what the recorder
                // writes into MCAP. Subscriber-side ingest time is the
                // fallback path inside the sample builder; we keep it on
                // `Sample` for diagnostics only.
                match inputs
                    .server
                    .forward(
                        &sample.full_key,
                        &sample.key_suffix,
                        &sample.payload,
                        sample.publish_time_ns,
                        schema,
                    )
                    .await
                {
                    Ok(advertised_new) => {
                        if advertised_new {
                            summary.topics_advertised =
                                summary.topics_advertised.saturating_add(1);
                            logln(
                                inputs.log_level,
                                LogLevel::Debug,
                                &format!(
                                    "advertised foxglove channel for '{}'",
                                    sample.full_key
                                ),
                            );
                        }
                        summary.messages_forwarded =
                            summary.messages_forwarded.saturating_add(1);
                        summary.bytes_forwarded = summary
                            .bytes_forwarded
                            .saturating_add(sample.payload.len() as u64);
                    }
                    // A per-sample failure (e.g. one channel refuses to
                    // build) must not take the whole bridge down: warn,
                    // drop the sample, keep every other topic flowing.
                    Err(err) => {
                        warnings.warn(
                            &format!("forward_error:{}", sample.full_key),
                            &format!("failed to forward '{}': {}", sample.full_key, err),
                            inputs.log_level,
                        );
                    }
                }
            }
        }
    }

    let stats = inputs.resolver.stats();
    summary.by_hash_lookup_failures = stats.by_hash_failures;
    summary.by_key_lookup_failures = stats.by_key_failures;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_resolve_schema_targets_pub_and_attached_telemetry_only() {
        assert!(should_resolve_schema("pub/demo/topic"));
        assert!(should_resolve_schema("sys/telemetry/vital/snapshot/abc"));
        assert!(should_resolve_schema("sys/telemetry/bulk/runtime/abc"));
        assert!(should_resolve_schema("sys/telemetry/node_identity/abc"));
        // owner_conflict publishes without a schema-hash attachment;
        // resolving it would only time out on a by-key GET.
        assert!(!should_resolve_schema(
            "sys/telemetry/vital/ctx/owner_conflict"
        ));
        assert!(!should_resolve_schema("ctx/robot/state"));
    }

    #[test]
    fn ctx_schema_rejects_non_context_value_payloads() {
        // Garbage bytes must fall back to a raw channel, not be mislabeled
        // with the ContextValue schema.
        assert!(ctx_channel_schema(&[0xde, 0xad, 0xbe, 0xef]).is_none());
    }

    #[test]
    fn ctx_schema_types_real_context_value_payloads() {
        let payload = aorta_core::contextstore_fb::encode_value_i64(
            42,
            &aorta_core::contextstore_fb::ContextValueMeta {
                stamp_ns: 1,
                expires_at_ns: 0,
                source_node: "test".to_string(),
                source_instance_id: "i0".to_string(),
                sequence: 1,
            },
        );
        let schema = ctx_channel_schema(&payload).expect("typed");
        assert_eq!(schema.hash_hex, context_value_schema_hash().to_hex());
        assert!(!schema.bfbs.is_empty());
    }
}
