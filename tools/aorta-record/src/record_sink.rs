//! Reusable Aorta-to-MCAP record sink.
//!
//! Consumers provide complete samples and decide whether each sample is raw or
//! should use Aorta schema resolution. MCAP writer details stay private.

use anyhow::Result;
use std::path::Path;

use crate::writer::{McapRecorderWriter, RecordMessage, RecordSchema};

pub use crate::config::StorageConfig;
pub use crate::writer::SegmentMeta;

/// How the sink should describe a sample in MCAP.
#[derive(Debug, Clone, Default)]
pub enum SchemaMode {
    /// Record bytes without a FlatBuffers schema.
    #[default]
    Raw,
    /// Resolve the FlatBuffers schema through the configured resolver.
    /// A miss is recorded as raw bytes and included in `schema_miss_count`.
    Resolve {
        /// Schema hash taken from the source message attachment, when present.
        attachment_schema_hash_hex: Option<String>,
    },
}

/// A complete sample ready to append to an MCAP recording.
#[derive(Debug, Clone)]
pub struct SinkMessage {
    pub topic_key: String,
    pub key_suffix: String,
    pub payload: Vec<u8>,
    pub log_time_ns: u64,
    pub publish_time_ns: u64,
    pub schema: SchemaMode,
}

/// A resolved FlatBuffers schema.
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    pub hash_hex: String,
    pub bfbs: Vec<u8>,
}

/// Async schema lookup used by [`McapRecordSink`].
pub trait RecordSchemaResolver {
    fn resolve(
        &self,
        key_suffix: &str,
        attachment_hash_hex: Option<&str>,
    ) -> impl std::future::Future<Output = Option<ResolvedSchema>> + Send;
}

/// Adapter for Aorta's canonical schema resolver.
pub struct AortaSchemaResolver {
    inner: aorta_core::schema_resolve::SchemaResolver,
}

impl AortaSchemaResolver {
    pub fn new(inner: aorta_core::schema_resolve::SchemaResolver) -> Self {
        Self { inner }
    }
}

impl RecordSchemaResolver for AortaSchemaResolver {
    async fn resolve(
        &self,
        key_suffix: &str,
        attachment_hash_hex: Option<&str>,
    ) -> Option<ResolvedSchema> {
        let attachment_hash =
            attachment_hash_hex.and_then(aorta_core::schema::SchemaHash::from_hex_str);
        self.inner
            .resolve(key_suffix, attachment_hash.as_ref())
            .await
            .map(|schema| ResolvedSchema {
                hash_hex: schema.schema_hash.to_hex(),
                bfbs: schema.bfbs,
            })
    }
}

/// Metadata recorded when the sink is finalized.
#[derive(Debug, Clone, Default)]
pub struct FinishOptions {
    pub dropped_messages: u64,
    pub rotation_reason: Option<String>,
}

/// Final counters for one MCAP artifact.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct SinkSummary {
    pub accepted_messages: u64,
    pub accepted_bytes: u64,
    pub dropped_messages: u64,
    pub schema_miss_count: u64,
    pub flush_count: u64,
}

/// A schema-aware MCAP sink with a deliberately small public surface.
pub struct McapRecordSink<R> {
    resolver: R,
    writer: McapRecorderWriter,
    schema_miss_count: u64,
}

impl<R> McapRecordSink<R>
where
    R: RecordSchemaResolver,
{
    pub fn create(
        path: &Path,
        group: &str,
        rpc_record: bool,
        storage: &StorageConfig,
        segment: Option<&SegmentMeta>,
        resolver: R,
    ) -> Result<Self> {
        Ok(Self {
            resolver,
            writer: McapRecorderWriter::create(path, group, rpc_record, storage, segment)?,
            schema_miss_count: 0,
        })
    }

    /// Resolve the requested schema and append one sample.
    pub async fn append(&mut self, message: SinkMessage) -> Result<()> {
        let (schema, unresolved_schema_hash) = match &message.schema {
            SchemaMode::Raw => (None, None),
            SchemaMode::Resolve {
                attachment_schema_hash_hex,
            } => {
                let resolved = self
                    .resolver
                    .resolve(&message.key_suffix, attachment_schema_hash_hex.as_deref())
                    .await;
                if resolved.is_none() {
                    self.schema_miss_count = self.schema_miss_count.saturating_add(1);
                }
                let unresolved_schema_hash = resolved
                    .is_none()
                    .then(|| attachment_schema_hash_hex.clone())
                    .flatten();
                (
                    resolved.map(|schema| RecordSchema {
                        hash_hex: schema.hash_hex,
                        bfbs: schema.bfbs,
                    }),
                    unresolved_schema_hash,
                )
            }
        };

        self.writer.write_message(RecordMessage {
            topic_key: message.topic_key,
            key_suffix: message.key_suffix,
            schema,
            unresolved_schema_hash,
            payload: message.payload,
            log_time_ns: message.log_time_ns,
            publish_time_ns: message.publish_time_ns,
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }

    pub fn finish(self, options: FinishOptions) -> Result<SinkSummary> {
        let rotation_reason = options.rotation_reason.as_deref();
        let counters = self.writer.finish(
            self.schema_miss_count,
            options.dropped_messages,
            rotation_reason,
        )?;
        Ok(SinkSummary {
            accepted_messages: counters.accepted_messages,
            accepted_bytes: counters.accepted_bytes,
            dropped_messages: options.dropped_messages,
            schema_miss_count: self.schema_miss_count,
            flush_count: counters.flush_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionKind;
    use aorta_core::contextstore_fb::{
        context_value_schema_hash, encode_value_string, ContextValueMeta, CONTEXT_VALUE_BFBS,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct DummySchemaResolver {
        calls: Arc<Mutex<Vec<(String, Option<String>)>>>,
    }

    impl RecordSchemaResolver for DummySchemaResolver {
        fn resolve(
            &self,
            key_suffix: &str,
            attachment_hash_hex: Option<&str>,
        ) -> impl std::future::Future<Output = Option<ResolvedSchema>> + Send {
            self.calls.lock().expect("calls").push((
                key_suffix.to_string(),
                attachment_hash_hex.map(str::to_string),
            ));
            std::future::ready((key_suffix == "pub/demo/typed").then(|| ResolvedSchema {
                hash_hex: context_value_schema_hash().to_hex(),
                bfbs: CONTEXT_VALUE_BFBS.to_vec(),
            }))
        }
    }

    fn storage_config() -> StorageConfig {
        StorageConfig {
            compression: CompressionKind::None,
            compression_level: 0,
            chunk_size: Some(4096),
            chunking: true,
            summary: true,
        }
    }

    fn temp_mcap(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "aorta-record-sink-{name}-{}-{}.mcap",
            std::process::id(),
            aorta_core::unix_time_ns()
        ))
    }

    #[tokio::test]
    async fn forwards_attachment_hash_and_counts_only_requested_schema_misses() {
        let path = temp_mcap("schema-resolution");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let resolver = DummySchemaResolver {
            calls: calls.clone(),
        };
        let mut sink =
            McapRecordSink::create(&path, "default", false, &storage_config(), None, resolver)
                .expect("sink");

        sink.append(SinkMessage {
            topic_key: "aorta/default/pub/demo/raw".to_string(),
            key_suffix: "pub/demo/raw".to_string(),
            payload: b"raw".to_vec(),
            log_time_ns: 1,
            publish_time_ns: 1,
            schema: SchemaMode::Raw,
        })
        .await
        .expect("raw sample");
        let typed_payload = encode_value_string(
            "typed",
            &ContextValueMeta {
                stamp_ns: 2,
                expires_at_ns: 0,
                source_node: "record-sink-test".to_string(),
                source_instance_id: "test".to_string(),
                sequence: 1,
            },
        );
        let typed_payload_len = typed_payload.len() as u64;
        sink.append(SinkMessage {
            topic_key: "aorta/default/pub/demo/typed".to_string(),
            key_suffix: "pub/demo/typed".to_string(),
            payload: typed_payload,
            log_time_ns: 2,
            publish_time_ns: 2,
            schema: SchemaMode::Resolve {
                attachment_schema_hash_hex: Some("a".repeat(64)),
            },
        })
        .await
        .expect("typed sample");
        sink.append(SinkMessage {
            topic_key: "aorta/default/pub/demo/missing".to_string(),
            key_suffix: "pub/demo/missing".to_string(),
            payload: b"missing".to_vec(),
            log_time_ns: 3,
            publish_time_ns: 3,
            schema: SchemaMode::Resolve {
                attachment_schema_hash_hex: Some("b".repeat(64)),
            },
        })
        .await
        .expect("fallback raw sample");

        let summary = sink
            .finish(FinishOptions {
                dropped_messages: 4,
                rotation_reason: Some("manual".to_string()),
            })
            .expect("finish");
        assert_eq!(summary.accepted_messages, 3);
        assert_eq!(summary.accepted_bytes, typed_payload_len + 10);
        assert_eq!(summary.schema_miss_count, 1);
        assert_eq!(summary.dropped_messages, 4);
        assert_eq!(
            *calls.lock().expect("calls"),
            vec![
                ("pub/demo/typed".to_string(), Some("a".repeat(64))),
                ("pub/demo/missing".to_string(), Some("b".repeat(64))),
            ]
        );

        let bytes = std::fs::read(&path).expect("read mcap");
        let end_metadata = mcap::read::LinearReader::new(&bytes)
            .expect("reader")
            .filter_map(|record| match record.expect("record") {
                mcap::records::Record::Metadata(metadata)
                    if metadata.name == "aorta.recorder.end" =>
                {
                    Some(metadata.metadata.clone())
                }
                _ => None,
            })
            .next()
            .expect("end metadata");
        assert_eq!(
            end_metadata
                .get("aorta.schema_miss_count")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            end_metadata
                .get("aorta.dropped_messages")
                .map(String::as_str),
            Some("4")
        );
        assert_eq!(
            end_metadata
                .get("aorta.rotation.reason")
                .map(String::as_str),
            Some("manual")
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn preserves_attachment_hash_for_schema_less_fallback_channels() {
        let path = temp_mcap("schema-miss-hashes");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let resolver = DummySchemaResolver { calls };
        let mut sink =
            McapRecordSink::create(&path, "default", false, &storage_config(), None, resolver)
                .expect("sink");
        let topic_key = "aorta/default/pub/demo/missing";

        for (timestamp, hash) in [(1, "b".repeat(64)), (2, "c".repeat(64))] {
            sink.append(SinkMessage {
                topic_key: topic_key.to_string(),
                key_suffix: "pub/demo/missing".to_string(),
                payload: vec![timestamp as u8],
                log_time_ns: timestamp,
                publish_time_ns: timestamp,
                schema: SchemaMode::Resolve {
                    attachment_schema_hash_hex: Some(hash),
                },
            })
            .await
            .expect("fallback raw sample");
        }
        sink.finish(FinishOptions::default()).expect("finish");

        let bytes = std::fs::read(&path).expect("read mcap");
        let messages = mcap::MessageStream::new(&bytes)
            .expect("message stream")
            .map(|message| message.expect("message"))
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 2);
        assert_ne!(messages[0].channel.id, messages[1].channel.id);
        assert_eq!(messages[0].channel.message_encoding, "aorta_raw_bytes");
        assert!(messages[0].channel.schema.is_none());
        assert_eq!(
            messages[0]
                .channel
                .metadata
                .get("schema_hash")
                .map(String::as_str),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(
            messages[1]
                .channel
                .metadata
                .get("schema_hash")
                .map(String::as_str),
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
        );

        let _ = std::fs::remove_file(path);
    }
}
