//! Minimal record-sink example: write raw and typed messages to an MCAP file.
//!
//! Run with:
//!   cargo run -p aorta-record --example basic_sink -- /tmp/basic_sink.mcap

use aorta_core::contextstore_fb::{
    context_value_schema_hash, encode_value_string, ContextValueMeta, CONTEXT_VALUE_BFBS,
};
use aorta_record::config::CompressionKind;
use aorta_record::record_sink::{
    FinishOptions, McapRecordSink, RecordSchemaResolver, ResolvedSchema, SchemaMode, SinkMessage,
    StorageConfig,
};
use std::env;
use std::path::PathBuf;

struct ExampleSchemaResolver;

impl RecordSchemaResolver for ExampleSchemaResolver {
    fn resolve(
        &self,
        key_suffix: &str,
        _attachment_hash_hex: Option<&str>,
    ) -> impl std::future::Future<Output = Option<ResolvedSchema>> + Send {
        std::future::ready((key_suffix == "ctx/system/demo").then(|| ResolvedSchema {
            hash_hex: context_value_schema_hash().to_hex(),
            bfbs: CONTEXT_VALUE_BFBS.to_vec(),
        }))
    }
}

fn storage_config() -> StorageConfig {
    StorageConfig {
        compression: CompressionKind::Zstd,
        compression_level: 1,
        chunk_size: Some(256 * 1024),
        chunking: true,
        summary: true,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path: PathBuf = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/basic_sink.mcap"));

    let mut sink = McapRecordSink::create(
        &path,
        "default",
        false,
        &storage_config(),
        None,
        ExampleSchemaResolver,
    )?;

    sink.append(SinkMessage {
        topic_key: "aorta/default/pub/demo/chatter".to_string(),
        key_suffix: "pub/demo/chatter".to_string(),
        payload: b"hello sink".to_vec(),
        log_time_ns: 1_000_000,
        publish_time_ns: 1_000_000,
        schema: SchemaMode::Raw,
    })
    .await?;

    let context_payload = encode_value_string(
        "typed value",
        &ContextValueMeta {
            stamp_ns: 2_000_000,
            expires_at_ns: 0,
            source_node: "basic-sink".to_string(),
            source_instance_id: "example".to_string(),
            sequence: 1,
        },
    );
    sink.append(SinkMessage {
        topic_key: "aorta/default/ctx/system/demo".to_string(),
        key_suffix: "ctx/system/demo".to_string(),
        payload: context_payload,
        log_time_ns: 2_000_000,
        publish_time_ns: 2_000_000,
        schema: SchemaMode::Resolve {
            attachment_schema_hash_hex: Some(context_value_schema_hash().to_hex()),
        },
    })
    .await?;

    sink.flush()?;
    let summary = sink.finish(FinishOptions {
        dropped_messages: 0,
        rotation_reason: None,
    })?;

    println!(
        "Wrote {} messages ({} bytes) to {}",
        summary.accepted_messages,
        summary.accepted_bytes,
        path.display()
    );

    Ok(())
}
