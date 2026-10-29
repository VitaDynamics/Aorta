use crate::config::{CompressionKind, StorageConfig};
use anyhow::{Context, Result};
use aorta_core::schema::bfbs_root_type_name;
use aorta_core::unix_time_ns;
use mcap::records::{MessageHeader, Metadata};
use mcap::{Compression, WriteOptions, Writer};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct RecordMessage {
    pub topic_key: String,
    pub key_suffix: String,
    pub schema: Option<RecordSchema>,
    pub unresolved_schema_hash: Option<String>,
    pub payload: Vec<u8>,
    pub log_time_ns: u64,
    pub publish_time_ns: u64,
}

#[derive(Debug, Clone)]
pub struct RecordSchema {
    pub hash_hex: String,
    pub bfbs: Vec<u8>,
}

/// Per-segment identity written into a rotating segment's start metadata so
/// downstream tooling can regroup segments of one recording. Passing `None` for
/// the segment to [`McapRecorderWriter::create`] selects the legacy single-file
/// path (no extra metadata, truncating `File::create`) for byte-for-byte
/// backward compatibility; passing `Some` selects the rotating path
/// (no-clobber `create_new`, segment metadata injected).
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub recording_id: String,
    pub start_ns: u64,
    pub index: u64,
    pub continues: bool,
}

#[derive(Debug, Default, Clone)]
pub struct WriterCounters {
    pub accepted_messages: u64,
    pub accepted_bytes: u64,
    pub flush_count: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum ChannelKey {
    Typed {
        topic_key: String,
        hash_hex: String,
    },
    Raw {
        topic_key: String,
        schema_hash: Option<String>,
    },
}

pub struct McapRecorderWriter {
    writer: Writer<BufWriter<File>>,
    sequence: u32,
    channel_ids: HashMap<ChannelKey, u16>,
    schema_ids: HashMap<String, u16>,
    counters: WriterCounters,
    pending_messages: usize,
    pending_bytes: usize,
}

impl McapRecorderWriter {
    pub fn create(
        path: &Path,
        group: &str,
        rpc_record: bool,
        storage: &StorageConfig,
        segment: Option<&SegmentMeta>,
    ) -> Result<Self> {
        // Rotating segments use no-clobber create_new so a name collision (prior
        // run / concurrent recorder) fails loudly instead of truncating data.
        // The legacy single-file path keeps File::create for back-compat.
        let file = match segment {
            Some(_) => OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .with_context(|| {
                    format!(
                        "failed to create mcap segment '{}' (exists? no-clobber)",
                        path.display()
                    )
                })?,
            None => File::create(path).with_context(|| {
                format!("failed to create mcap output file '{}'", path.display())
            })?,
        };
        let opts = build_write_options(storage);

        let mut writer = Writer::with_options(BufWriter::new(file), opts)
            .context("failed to create mcap writer")?;

        let mut metadata = BTreeMap::new();
        metadata.insert("aorta.profile".to_string(), "aorta".to_string());
        metadata.insert("aorta.group".to_string(), group.to_string());
        metadata.insert(
            "aorta.rpc_record_enabled".to_string(),
            rpc_record.to_string(),
        );
        metadata.insert(
            "aorta.started_at_ns".to_string(),
            unix_time_ns().to_string(),
        );
        metadata.insert(
            "aorta.storage.compression".to_string(),
            storage.compression.as_str().to_string(),
        );
        metadata.insert(
            "aorta.storage.compression_level".to_string(),
            storage.compression_level.to_string(),
        );
        metadata.insert(
            "aorta.storage.compression_threads".to_string(),
            "0".to_string(),
        );
        if let Some(size) = storage.chunk_size {
            metadata.insert("aorta.storage.chunk_size".to_string(), size.to_string());
        }
        metadata.insert(
            "aorta.storage.chunking".to_string(),
            storage.chunking.to_string(),
        );
        metadata.insert(
            "aorta.storage.summary".to_string(),
            storage.summary.to_string(),
        );
        // Segment identity is only emitted on the rotating path so the legacy
        // single-file output stays byte-for-byte identical to pre-rotation.
        if let Some(seg) = segment {
            metadata.insert("aorta.recording_id".to_string(), seg.recording_id.clone());
            metadata.insert("aorta.segment.index".to_string(), seg.index.to_string());
            metadata.insert(
                "aorta.segment.start_ns".to_string(),
                seg.start_ns.to_string(),
            );
            metadata.insert(
                "aorta.segment.continues".to_string(),
                seg.continues.to_string(),
            );
        }
        writer
            .write_metadata(&Metadata {
                name: "aorta.recorder.start".to_string(),
                metadata,
            })
            .context("failed to write recorder start metadata")?;

        Ok(Self {
            writer,
            sequence: 0,
            channel_ids: HashMap::new(),
            schema_ids: HashMap::new(),
            counters: WriterCounters::default(),
            pending_messages: 0,
            pending_bytes: 0,
        })
    }

    pub fn write_message(&mut self, message: RecordMessage) -> Result<()> {
        let channel_id = self.ensure_channel(&message)?;
        let header = MessageHeader {
            channel_id,
            sequence: self.sequence,
            log_time: message.log_time_ns,
            publish_time: message.publish_time_ns,
        };

        self.writer
            .write_to_known_channel(&header, &message.payload)
            .context("failed to write message to mcap channel")?;

        self.sequence = self.sequence.wrapping_add(1);
        self.counters.accepted_messages = self.counters.accepted_messages.saturating_add(1);
        self.counters.accepted_bytes = self
            .counters
            .accepted_bytes
            .saturating_add(message.payload.len() as u64);
        self.pending_messages = self.pending_messages.saturating_add(1);
        self.pending_bytes = self.pending_bytes.saturating_add(message.payload.len());

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("failed to flush mcap writer")?;
        self.counters.flush_count = self.counters.flush_count.saturating_add(1);
        self.pending_messages = 0;
        self.pending_bytes = 0;
        Ok(())
    }

    pub fn should_flush(&self, batch_max_messages: usize, batch_max_bytes: usize) -> bool {
        self.pending_messages >= batch_max_messages || self.pending_bytes >= batch_max_bytes
    }

    pub fn counters(&self) -> &WriterCounters {
        &self.counters
    }

    pub fn finish(
        mut self,
        schema_miss_count: u64,
        dropped_messages: u64,
        rotation_reason: Option<&str>,
    ) -> Result<WriterCounters> {
        let mut metadata = BTreeMap::new();
        metadata.insert("aorta.ended_at_ns".to_string(), unix_time_ns().to_string());
        // Emitted only on the rotating path (back-compat: off => key absent).
        if let Some(reason) = rotation_reason {
            metadata.insert("aorta.rotation.reason".to_string(), reason.to_string());
        }
        metadata.insert(
            "aorta.accepted_messages".to_string(),
            self.counters.accepted_messages.to_string(),
        );
        metadata.insert(
            "aorta.accepted_bytes".to_string(),
            self.counters.accepted_bytes.to_string(),
        );
        metadata.insert(
            "aorta.schema_miss_count".to_string(),
            schema_miss_count.to_string(),
        );
        metadata.insert(
            "aorta.dropped_messages".to_string(),
            dropped_messages.to_string(),
        );
        self.writer
            .write_metadata(&Metadata {
                name: "aorta.recorder.end".to_string(),
                metadata,
            })
            .context("failed to write recorder end metadata")?;

        self.writer
            .finish()
            .context("failed to finish mcap writer")?;
        Ok(self.counters)
    }

    fn ensure_channel(&mut self, message: &RecordMessage) -> Result<u16> {
        let key = if let Some(schema) = &message.schema {
            ChannelKey::Typed {
                topic_key: message.topic_key.clone(),
                hash_hex: schema.hash_hex.clone(),
            }
        } else {
            ChannelKey::Raw {
                topic_key: message.topic_key.clone(),
                schema_hash: message.unresolved_schema_hash.clone(),
            }
        };

        if let Some(channel_id) = self.channel_ids.get(&key) {
            return Ok(*channel_id);
        }

        let (schema_id, message_encoding, schema_hash_text) = if let Some(schema) = &message.schema
        {
            let schema_id = self.ensure_schema(schema)?;
            (schema_id, "flatbuffer", schema.hash_hex.clone())
        } else {
            (
                0,
                "aorta_raw_bytes",
                message
                    .unresolved_schema_hash
                    .clone()
                    .unwrap_or_else(|| "raw".to_string()),
            )
        };

        let mut metadata = BTreeMap::new();
        metadata.insert("aorta_key".to_string(), message.topic_key.clone());
        metadata.insert("key_suffix".to_string(), message.key_suffix.clone());
        metadata.insert("schema_hash".to_string(), schema_hash_text);

        let channel_id = self
            .writer
            .add_channel(schema_id, &message.topic_key, message_encoding, &metadata)
            .context("failed to add mcap channel")?;

        self.channel_ids.insert(key, channel_id);
        Ok(channel_id)
    }

    fn ensure_schema(&mut self, schema: &RecordSchema) -> Result<u16> {
        if let Some(schema_id) = self.schema_ids.get(&schema.hash_hex) {
            return Ok(*schema_id);
        }

        let schema_name = extract_flatbuffer_root_type_name(&schema.bfbs)
            .unwrap_or_else(|| schema.hash_hex.clone());

        let schema_id = self
            .writer
            // Foxglove/MCAP readers expect FlatBuffers schema encoding to be exactly "flatbuffer".
            .add_schema(&schema_name, "flatbuffer", &schema.bfbs)
            .context("failed to add mcap schema")?;
        self.schema_ids.insert(schema.hash_hex.clone(), schema_id);
        Ok(schema_id)
    }
}

fn extract_flatbuffer_root_type_name(bfbs: &[u8]) -> Option<String> {
    bfbs_root_type_name(bfbs)
}

fn build_write_options(storage: &StorageConfig) -> WriteOptions {
    let compression = match storage.compression {
        CompressionKind::None => None,
        CompressionKind::Zstd => Some(Compression::Zstd),
        CompressionKind::Lz4 => Some(Compression::Lz4),
    };

    WriteOptions::new()
        .profile("aorta")
        .library("aorta-recorder")
        .compression(compression)
        .compression_level(storage.compression_level)
        // mcap creates a fresh zstd encoder per chunk; workers would spawn
        // and join a pool each time. The writer already has a dedicated thread,
        // so compress synchronously.
        .compression_threads(0)
        .use_chunks(storage.chunking)
        .chunk_size(storage.chunk_size)
        .emit_summary_records(storage.summary)
}

#[cfg(test)]
mod tests {
    use super::{
        extract_flatbuffer_root_type_name, McapRecorderWriter, RecordMessage, RecordSchema,
        SegmentMeta,
    };
    use crate::config::{CompressionKind, StorageConfig};
    use std::path::{Path, PathBuf};

    fn segment_meta() -> SegmentMeta {
        SegmentMeta {
            recording_id: "rec-test".to_string(),
            start_ns: 1,
            index: 0,
            continues: false,
        }
    }

    #[test]
    fn rotating_create_is_no_clobber() {
        // A rotating segment (Some(meta)) must refuse to truncate an existing
        // path; the legacy path (None) keeps truncating create.
        let path = temp_output_path("no-clobber");
        let storage = uncompressed_storage();
        let meta = segment_meta();

        let first = McapRecorderWriter::create(&path, "default", false, &storage, Some(&meta))
            .expect("1st");
        let second = McapRecorderWriter::create(&path, "default", false, &storage, Some(&meta));
        assert!(
            second.is_err(),
            "create_new must reject a pre-existing segment path"
        );

        // Legacy single-file path still overwrites the same path.
        let legacy = McapRecorderWriter::create(&path, "default", false, &storage, None);
        assert!(legacy.is_ok(), "legacy create truncates as before");

        let _ = first.finish(0, 0, None);
        let _ = legacy.unwrap().finish(0, 0, None);
        let _ = std::fs::remove_file(path);
    }

    fn temp_output_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = format!(
            "aorta-record-writer-test-{}-{}-{}.mcap",
            name,
            std::process::id(),
            super::unix_time_ns()
        );
        path.push(unique);
        path
    }

    fn uncompressed_storage() -> StorageConfig {
        StorageConfig {
            compression: CompressionKind::None,
            compression_level: 0,
            chunk_size: Some(768 * 1024),
            chunking: true,
            summary: true,
        }
    }

    fn create_writer(path: &Path, storage: StorageConfig) -> McapRecorderWriter {
        McapRecorderWriter::create(path, "default", false, &storage, None).expect("writer")
    }

    fn read_back(path: &Path) -> (usize, Vec<Vec<u8>>) {
        let bytes = std::fs::read(path).expect("read mcap file");
        let mut count = 0usize;
        let mut payloads = Vec::new();
        for msg in mcap::MessageStream::new(&bytes).expect("message stream") {
            let msg = msg.expect("message");
            count += 1;
            payloads.push(msg.data.to_vec());
        }
        (count, payloads)
    }

    fn read_start_metadata(path: &Path) -> std::collections::BTreeMap<String, String> {
        let bytes = std::fs::read(path).expect("read mcap file");
        for record in mcap::read::LinearReader::new(&bytes).expect("reader") {
            if let mcap::records::Record::Metadata(m) = record.expect("record") {
                if m.name == "aorta.recorder.start" {
                    return m.metadata.clone();
                }
            }
        }
        panic!("aorta.recorder.start metadata not found");
    }

    #[test]
    fn typed_message_embeds_flatbuffer_schema_in_mcap() {
        // A RecordMessage carrying a resolved schema (here the real vital-snapshot
        // telemetry bfbs) must be written as a typed `flatbuffer` channel with the
        // bfbs embedded, so MCAP/Foxglove readers can parse the message — otherwise
        // it would be an opaque `aorta_raw_bytes` channel. The recorder obtains this
        // RecordSchema by resolving the put's schema-hash attachment (see the
        // by-hash registration test in core/test/node_tests.rs).
        use aorta_core::telemetry::fb::{
            vital_snapshot_schema_hash, TELEMETRY_VITAL_SNAPSHOT_BFBS,
        };

        let path = temp_output_path("typed-schema");
        let mut writer = create_writer(&path, uncompressed_storage());
        writer
            .write_message(RecordMessage {
                topic_key: "aorta/default/sys/telemetry/vital/snapshot/n1".to_string(),
                key_suffix: "sys/telemetry/vital/snapshot/n1".to_string(),
                schema: Some(RecordSchema {
                    hash_hex: vital_snapshot_schema_hash().to_hex(),
                    bfbs: TELEMETRY_VITAL_SNAPSHOT_BFBS.to_vec(),
                }),
                unresolved_schema_hash: None,
                payload: vec![0u8; 8],
                log_time_ns: 1,
                publish_time_ns: 1,
            })
            .expect("write typed message");
        writer.finish(0, 0, None).expect("finish");

        let bytes = std::fs::read(&path).expect("read mcap");
        let channel = mcap::MessageStream::new(&bytes)
            .expect("message stream")
            .map(|m| m.expect("message"))
            .next()
            .map(|m| m.channel.clone())
            .expect("one recorded message");

        assert_eq!(channel.message_encoding, "flatbuffer");
        let schema = channel.schema.as_ref().expect("schema must be embedded");
        assert_eq!(schema.encoding, "flatbuffer");
        assert_eq!(
            schema.data.as_ref(),
            TELEMETRY_VITAL_SNAPSHOT_BFBS,
            "the exact bfbs must be embedded so readers can parse the message"
        );
        assert!(
            schema.name.contains("VitalSnapshot"),
            "unexpected schema name: {}",
            schema.name
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn both_telemetry_tiers_write_distinct_typed_channels() {
        // The writer half of both-tier capture: the recorder must
        // land BOTH tiers as typed `flatbuffer` MCAP channels — the vital snapshot
        // (`vital/snapshot/`) and the bulk runtime stream (`bulk/runtime/`) — each
        // carrying its own correct embedded bfbs, so a downstream reader can parse
        // either tier. (`should_resolve_schema_targets_pub_and_attached_telemetry_only`
        // covers the resolution half.)
        use aorta_core::telemetry::fb::{
            runtime_metrics_schema_hash, vital_snapshot_schema_hash,
            TELEMETRY_RUNTIME_METRICS_BFBS, TELEMETRY_VITAL_SNAPSHOT_BFBS,
        };

        let vital_topic = "aorta/default/sys/telemetry/vital/snapshot/n1";
        let bulk_topic = "aorta/default/sys/telemetry/bulk/runtime/n1";

        let path = temp_output_path("both-tiers");
        let mut writer = create_writer(&path, uncompressed_storage());
        writer
            .write_message(RecordMessage {
                topic_key: vital_topic.to_string(),
                key_suffix: "sys/telemetry/vital/snapshot/n1".to_string(),
                schema: Some(RecordSchema {
                    hash_hex: vital_snapshot_schema_hash().to_hex(),
                    bfbs: TELEMETRY_VITAL_SNAPSHOT_BFBS.to_vec(),
                }),
                unresolved_schema_hash: None,
                payload: vec![0u8; 8],
                log_time_ns: 1,
                publish_time_ns: 1,
            })
            .expect("write vital message");
        writer
            .write_message(RecordMessage {
                topic_key: bulk_topic.to_string(),
                key_suffix: "sys/telemetry/bulk/runtime/n1".to_string(),
                schema: Some(RecordSchema {
                    hash_hex: runtime_metrics_schema_hash().to_hex(),
                    bfbs: TELEMETRY_RUNTIME_METRICS_BFBS.to_vec(),
                }),
                unresolved_schema_hash: None,
                payload: vec![0u8; 8],
                log_time_ns: 2,
                publish_time_ns: 2,
            })
            .expect("write bulk message");
        writer.finish(0, 0, None).expect("finish");

        // Each tier must be its OWN typed flatbuffer channel with the right bfbs.
        let bytes = std::fs::read(&path).expect("read mcap");
        let mut bfbs_by_topic: std::collections::BTreeMap<String, Vec<u8>> =
            std::collections::BTreeMap::new();
        for m in mcap::MessageStream::new(&bytes).expect("message stream") {
            let m = m.expect("message");
            assert_eq!(
                m.channel.message_encoding, "flatbuffer",
                "both tiers must be typed flatbuffer channels, not raw"
            );
            let schema = m.channel.schema.as_ref().expect("schema must be embedded");
            assert_eq!(schema.encoding, "flatbuffer");
            bfbs_by_topic.insert(m.channel.topic.clone(), schema.data.to_vec());
        }

        assert_eq!(
            bfbs_by_topic.len(),
            2,
            "expected one typed channel per tier"
        );
        assert_eq!(
            bfbs_by_topic.get(vital_topic).map(Vec::as_slice),
            Some(TELEMETRY_VITAL_SNAPSHOT_BFBS),
            "vital tier must embed the VitalSnapshot bfbs"
        );
        assert_eq!(
            bfbs_by_topic.get(bulk_topic).map(Vec::as_slice),
            Some(TELEMETRY_RUNTIME_METRICS_BFBS),
            "bulk tier must embed the RuntimeMetrics bfbs"
        );
        assert_ne!(
            TELEMETRY_VITAL_SNAPSHOT_BFBS, TELEMETRY_RUNTIME_METRICS_BFBS,
            "the two tiers must carry genuinely distinct schemas"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn writer_flush_and_counters_work() {
        let path = temp_output_path("flush-counters");
        let mut writer = create_writer(&path, uncompressed_storage());

        writer
            .write_message(RecordMessage {
                topic_key: "aorta/default/pub/demo/chatter".to_string(),
                key_suffix: "pub/demo/chatter".to_string(),
                schema: None,
                unresolved_schema_hash: None,
                payload: vec![1, 2, 3, 4],
                log_time_ns: 1,
                publish_time_ns: 1,
            })
            .expect("write message 1");
        writer
            .write_message(RecordMessage {
                topic_key: "aorta/default/pub/demo/chatter".to_string(),
                key_suffix: "pub/demo/chatter".to_string(),
                schema: None,
                unresolved_schema_hash: None,
                payload: vec![5, 6, 7, 8],
                log_time_ns: 2,
                publish_time_ns: 2,
            })
            .expect("write message 2");

        assert!(writer.should_flush(2, usize::MAX));
        writer.flush().expect("flush");

        let counters = writer.finish(0, 0, None).expect("finish");
        assert_eq!(counters.accepted_messages, 2);
        assert_eq!(counters.accepted_bytes, 8);
        assert!(counters.flush_count >= 1);

        let size = std::fs::metadata(&path).expect("metadata").len();
        assert!(size > 0);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn writer_supports_raw_and_typed_channels() {
        let path = temp_output_path("raw-typed");
        let mut writer = create_writer(&path, uncompressed_storage());

        writer
            .write_message(RecordMessage {
                topic_key: "aorta/default/pub/demo/chatter".to_string(),
                key_suffix: "pub/demo/chatter".to_string(),
                schema: None,
                unresolved_schema_hash: None,
                payload: vec![0, 1, 2],
                log_time_ns: 10,
                publish_time_ns: 10,
            })
            .expect("write raw");

        writer
            .write_message(RecordMessage {
                topic_key: "aorta/default/pub/demo/chatter".to_string(),
                key_suffix: "pub/demo/chatter".to_string(),
                schema: Some(RecordSchema {
                    hash_hex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                    bfbs: vec![0x42, 0x46, 0x42, 0x53],
                }),
                unresolved_schema_hash: None,
                payload: vec![3, 4, 5],
                log_time_ns: 11,
                publish_time_ns: 11,
            })
            .expect("write typed");

        let counters = writer.finish(0, 0, None).expect("finish");
        assert_eq!(counters.accepted_messages, 2);
        assert_eq!(counters.accepted_bytes, 6);

        let size = std::fs::metadata(&path).expect("metadata").len();
        assert!(size > 0);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn snapshot_and_live_ctx_share_one_typed_channel() {
        // Core correctness invariant for the ctx initial snapshot: a t0
        // snapshot message and a later live put on the SAME ctx key — both
        // carrying the static `ContextValue` schema the recorder attaches
        // (see `ctx_record_schema` in lib.rs) — must land on a single typed
        // `flatbuffer` channel (name = full ctx key, ContextValue bfbs
        // embedded), so MCAP readers like Foxglove parse it instead of
        // erroring on the opaque `aorta_raw_bytes` encoding.
        use aorta_core::contextstore_fb::{context_value_schema_hash, CONTEXT_VALUE_BFBS};

        let path = temp_output_path("ctx-shared-channel");
        let mut writer = create_writer(&path, uncompressed_storage());
        let ctx_key = "aorta/default/ctx/system/estop";
        let ctx_schema = || RecordSchema {
            hash_hex: context_value_schema_hash().to_hex(),
            bfbs: CONTEXT_VALUE_BFBS.to_vec(),
        };

        // t0 snapshot value (from the wildcard GET).
        writer
            .write_message(RecordMessage {
                topic_key: ctx_key.to_string(),
                key_suffix: "ctx/system/estop".to_string(),
                schema: Some(ctx_schema()),
                unresolved_schema_hash: None,
                payload: vec![1, 2, 3],
                log_time_ns: 100,
                publish_time_ns: 100,
            })
            .expect("snapshot write");
        // A later live put on the same key (from the subscriber).
        writer
            .write_message(RecordMessage {
                topic_key: ctx_key.to_string(),
                key_suffix: "ctx/system/estop".to_string(),
                schema: Some(ctx_schema()),
                unresolved_schema_hash: None,
                payload: vec![4, 5, 6],
                log_time_ns: 200,
                publish_time_ns: 200,
            })
            .expect("live write");
        let _ = writer.finish(0, 0, None).expect("finish");

        let bytes = std::fs::read(&path).expect("read mcap file");
        let mut channels = std::collections::HashSet::new();
        let mut count = 0usize;
        let mut embedded_bfbs = None;
        for msg in mcap::MessageStream::new(&bytes).expect("message stream") {
            let msg = msg.expect("message");
            count += 1;
            channels.insert((
                msg.channel.topic.clone(),
                msg.channel.message_encoding.clone(),
            ));
            if let Some(schema) = msg.channel.schema.as_ref() {
                embedded_bfbs = Some(schema.data.to_vec());
            }
        }
        assert_eq!(count, 2, "both snapshot and live messages recorded");
        assert_eq!(
            channels.len(),
            1,
            "snapshot + live must share exactly one channel"
        );
        let (topic, encoding) = channels.into_iter().next().unwrap();
        assert_eq!(topic, ctx_key);
        assert_eq!(encoding, "flatbuffer");
        assert_eq!(
            embedded_bfbs.as_deref(),
            Some(CONTEXT_VALUE_BFBS),
            "the ContextValue bfbs must be embedded so readers can parse ctx values"
        );
        let _ = std::fs::remove_file(path);
    }

    fn write_sample_messages(writer: &mut McapRecorderWriter, payloads: &[&[u8]]) {
        for (i, payload) in payloads.iter().enumerate() {
            writer
                .write_message(RecordMessage {
                    topic_key: "aorta/default/pub/demo/chatter".to_string(),
                    key_suffix: "pub/demo/chatter".to_string(),
                    schema: None,
                    unresolved_schema_hash: None,
                    payload: payload.to_vec(),
                    log_time_ns: (i as u64) + 1,
                    publish_time_ns: (i as u64) + 1,
                })
                .expect("write message");
        }
    }

    #[test]
    fn writer_zstd_roundtrip_preserves_payloads() {
        let path = temp_output_path("zstd-roundtrip");
        let storage = StorageConfig {
            compression: CompressionKind::Zstd,
            compression_level: 1,
            chunk_size: Some(768 * 1024),
            chunking: true,
            summary: true,
        };
        let mut writer = create_writer(&path, storage);
        let payloads: Vec<&[u8]> = vec![b"hello", b"world", b"zstd"];
        write_sample_messages(&mut writer, &payloads);
        let _ = writer.finish(0, 0, None).expect("finish");

        let (count, got) = read_back(&path);
        assert_eq!(count, payloads.len());
        assert_eq!(got, payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn writer_lz4_roundtrip_preserves_payloads() {
        let path = temp_output_path("lz4-roundtrip");
        let storage = StorageConfig {
            compression: CompressionKind::Lz4,
            compression_level: 0,
            chunk_size: Some(768 * 1024),
            chunking: true,
            summary: true,
        };
        let mut writer = create_writer(&path, storage);
        let payloads: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
        write_sample_messages(&mut writer, &payloads);
        let _ = writer.finish(0, 0, None).expect("finish");

        let (count, got) = read_back(&path);
        assert_eq!(count, payloads.len());
        assert_eq!(got, payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn start_metadata_records_compression_threads() {
        for compression in [
            CompressionKind::None,
            CompressionKind::Zstd,
            CompressionKind::Lz4,
        ] {
            let path = temp_output_path("compression-threads-metadata");
            let mut storage = uncompressed_storage();
            storage.compression = compression;
            let writer = create_writer(&path, storage);
            writer.finish(0, 0, None).expect("finish");
            let metadata = read_start_metadata(&path);
            std::fs::remove_file(path).expect("remove mcap file");
            assert_eq!(
                metadata
                    .get("aorta.storage.compression_threads")
                    .map(String::as_str),
                Some("0"),
                "effective compression threads for {compression:?}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn zstd_chunks_spawn_no_compression_threads() {
        fn count_named_tasks(name: &str) -> usize {
            std::fs::read_dir("/proc/self/task")
                .expect("task directory")
                .map(|entry| entry.expect("task entry").path().join("comm"))
                .filter_map(|path| std::fs::read_to_string(path).ok())
                .filter(|comm| comm.trim_end_matches('\n') == name)
                .count()
        }

        std::thread::Builder::new()
            .name("ar-zstd-t0".into())
            .spawn(|| {
                let path = temp_output_path("zstd-threads");
                let storage = StorageConfig {
                    compression: CompressionKind::Zstd,
                    compression_level: 1,
                    chunk_size: Some(8 * 1024 * 1024),
                    chunking: true,
                    summary: true,
                };
                let mut writer = create_writer(&path, storage);
                let baseline = count_named_tasks("ar-zstd-t0");
                for chunk in 1..=2 {
                    if chunk == 2 {
                        writer.flush().expect("flush first chunk");
                    }
                    write_sample_messages(&mut writer, &[b"x"]);
                    let observed = count_named_tasks("ar-zstd-t0");
                    eprintln!("chunk {chunk}: baseline={baseline}, observed={observed}");
                    assert_eq!(
                        observed, baseline,
                        "chunk {chunk}: baseline={baseline}, observed={observed}"
                    );
                }
                writer.finish(0, 0, None).expect("finish");
                std::fs::remove_file(path).expect("remove mcap file");
            })
            .expect("spawn named writer thread")
            .join()
            .expect("named writer thread panicked");
    }

    #[test]
    fn writer_records_storage_metadata() {
        let path = temp_output_path("metadata");
        let storage = StorageConfig {
            compression: CompressionKind::Zstd,
            compression_level: 3,
            chunk_size: Some(4096),
            chunking: true,
            summary: true,
        };
        let mut writer = create_writer(&path, storage);
        write_sample_messages(&mut writer, &[b"x"]);
        let _ = writer.finish(0, 0, None).expect("finish");

        let meta = read_start_metadata(&path);
        assert_eq!(
            meta.get("aorta.storage.compression").map(String::as_str),
            Some("zstd")
        );
        assert_eq!(
            meta.get("aorta.storage.compression_level")
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(
            meta.get("aorta.storage.chunk_size").map(String::as_str),
            Some("4096")
        );
        assert_eq!(
            meta.get("aorta.storage.chunking").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            meta.get("aorta.storage.summary").map(String::as_str),
            Some("true")
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn writer_zstd_compresses_relative_to_none() {
        let payload = vec![0xABu8; 64 * 1024];
        let payloads: Vec<&[u8]> = (0..8).map(|_| payload.as_slice()).collect();

        let none_path = temp_output_path("size-none");
        let mut w = create_writer(&none_path, uncompressed_storage());
        write_sample_messages(&mut w, &payloads);
        let _ = w.finish(0, 0, None).expect("finish");

        let zstd_path = temp_output_path("size-zstd");
        let mut w = create_writer(
            &zstd_path,
            StorageConfig {
                compression: CompressionKind::Zstd,
                compression_level: 1,
                chunk_size: Some(768 * 1024),
                chunking: true,
                summary: true,
            },
        );
        write_sample_messages(&mut w, &payloads);
        let _ = w.finish(0, 0, None).expect("finish");

        let none_size = std::fs::metadata(&none_path).expect("metadata").len();
        let zstd_size = std::fs::metadata(&zstd_path).expect("metadata").len();
        assert!(
            zstd_size * 10 < none_size * 9,
            "expected zstd size < 0.9 * none size; got zstd={} none={}",
            zstd_size,
            none_size
        );
        let _ = std::fs::remove_file(none_path);
        let _ = std::fs::remove_file(zstd_path);
    }

    #[test]
    fn extract_flatbuffer_root_type_name_returns_none_for_invalid_bfbs() {
        assert!(extract_flatbuffer_root_type_name(&[]).is_none());
        assert!(extract_flatbuffer_root_type_name(&[1, 2, 3, 4]).is_none());
    }
}
