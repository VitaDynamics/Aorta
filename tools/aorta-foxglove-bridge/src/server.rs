//! Foxglove WebSocket server + channel registry.
//!
//! All `foxglove`-crate calls live in this module so the rest of the
//! bridge stays Foxglove-SDK-agnostic.
//!
//! Registry model: ONE channel per Aorta key. Unlike the recorder (whose
//! MCAP channels are append-only records, so a schema flap simply opens
//! another channel), a live bridge `close()` is a client-visible
//! `unadvertise` that drops every panel subscription on the channel.
//! Channel transitions are therefore asymmetric:
//!
//! * raw → typed: allowed (schema became known — replace).
//! * typed(h) → typed(h'): allowed (schema evolution — replace).
//! * typed → raw: FORBIDDEN. A transient schema-resolve failure (query
//!   timeout, publisher restart) must not tear down a healthy typed
//!   channel; the sample is forwarded on the existing channel instead.
//!   Without this rule the discovery loop's 2 s poll would flap
//!   unadvertise/advertise on every failed resolve.

use anyhow::{anyhow, Result};
use aorta_core::schema::bfbs_root_type_name;
use foxglove::{
    ChannelBuilder, PartialMetadata, RawChannel, Schema, WebSocketServer, WebSocketServerHandle,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::BridgeConfig;

/// Channel payload + schema, matching `aorta-record::writer::RecordSchema`.
#[derive(Debug, Clone)]
pub struct ChannelSchema {
    pub hash_hex: String,
    pub bfbs: Vec<u8>,
}

struct TopicChannel {
    /// `Some(hash)` for a typed channel, `None` for a raw channel.
    hash_hex: Option<String>,
    channel: Arc<RawChannel>,
}

/// What `ensure_channel_inner` should do for an incoming (existing, wanted)
/// schema pair. Pure decision logic, unit-tested without a live server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelAction {
    /// No channel for this key yet: build one.
    Create,
    /// Compatible channel exists: forward on it as-is.
    Reuse,
    /// Existing channel is superseded (raw→typed or hash change): close + rebuild.
    Replace,
}

fn plan_channel_action(existing: Option<&Option<String>>, wanted: Option<&str>) -> ChannelAction {
    match (existing, wanted) {
        (None, _) => ChannelAction::Create,
        // Never downgrade typed → raw; a schema-less sample rides the
        // existing channel (raw or typed) untouched.
        (Some(_), None) => ChannelAction::Reuse,
        (Some(current), Some(hash)) => {
            if current.as_deref() == Some(hash) {
                ChannelAction::Reuse
            } else {
                ChannelAction::Replace
            }
        }
    }
}

pub struct Server {
    handle: Mutex<Option<WebSocketServerHandle>>,
    channels: Mutex<HashMap<String, TopicChannel>>,
}

impl Server {
    pub async fn start(config: &BridgeConfig) -> Result<Self> {
        let handle = WebSocketServer::new()
            .name(config.server.name.clone())
            .bind(config.server.host.clone(), config.server.port)
            .start()
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to start foxglove websocket server on {}:{}: {}",
                    config.server.host,
                    config.server.port,
                    e
                )
            })?;

        Ok(Self {
            handle: Mutex::new(Some(handle)),
            channels: Mutex::new(HashMap::new()),
        })
    }

    /// Forwards a sample to the matching Foxglove channel, advertising
    /// the channel lazily on first sighting.
    ///
    /// Returns `Ok(true)` if this call advertised a channel (initial
    /// advertise or a replace after a schema change).
    pub async fn forward(
        &self,
        full_key: &str,
        key_suffix: &str,
        payload: &[u8],
        log_time_ns: u64,
        schema: Option<ChannelSchema>,
    ) -> Result<bool> {
        let (channel, newly_advertised) = self
            .ensure_channel_inner(full_key, key_suffix, schema)
            .await?;
        let metadata = PartialMetadata::with_log_time(log_time_ns);
        channel.log_with_meta(payload, metadata);
        Ok(newly_advertised)
    }

    /// Pre-advertises a channel without sending a payload. Used by the
    /// admin-space discovery loop so Foxglove Studio sees idle topics
    /// before their first sample arrives.
    pub async fn ensure_channel(
        &self,
        full_key: &str,
        key_suffix: &str,
        schema: Option<ChannelSchema>,
    ) -> Result<bool> {
        let (_channel, newly_advertised) = self
            .ensure_channel_inner(full_key, key_suffix, schema)
            .await?;
        Ok(newly_advertised)
    }

    async fn ensure_channel_inner(
        &self,
        full_key: &str,
        key_suffix: &str,
        schema: Option<ChannelSchema>,
    ) -> Result<(Arc<RawChannel>, bool)> {
        let mut channels = self.channels.lock().await;
        let action = plan_channel_action(
            channels.get(full_key).map(|c| &c.hash_hex),
            schema.as_ref().map(|s| s.hash_hex.as_str()),
        );
        match action {
            ChannelAction::Reuse => {
                let existing = channels
                    .get(full_key)
                    .expect("Reuse implies an existing channel");
                Ok((existing.channel.clone(), false))
            }
            ChannelAction::Create | ChannelAction::Replace => {
                if let Some(stale) = channels.remove(full_key) {
                    stale.channel.close();
                }
                let built = build_channel(full_key, key_suffix, schema.as_ref())?;
                channels.insert(
                    full_key.to_string(),
                    TopicChannel {
                        hash_hex: schema.as_ref().map(|s| s.hash_hex.clone()),
                        channel: built.clone(),
                    },
                );
                Ok((built, true))
            }
        }
    }

    pub async fn shutdown(&self) {
        let handle = {
            let mut guard = self.handle.lock().await;
            guard.take()
        };
        {
            let mut channels = self.channels.lock().await;
            for (_, topic_channel) in channels.drain() {
                topic_channel.channel.close();
            }
        }
        if let Some(handle) = handle {
            handle.stop().wait().await;
        }
    }
}

fn build_channel(
    full_key: &str,
    key_suffix: &str,
    schema: Option<&ChannelSchema>,
) -> Result<Arc<RawChannel>> {
    let mut builder = ChannelBuilder::new(full_key)
        .add_metadata("aorta_key", full_key)
        .add_metadata("key_suffix", key_suffix);

    builder = match schema {
        Some(s) => {
            let root_name = bfbs_root_type_name(&s.bfbs).unwrap_or_else(|| s.hash_hex.clone());
            let foxglove_schema = Schema::new(root_name, "flatbuffer", s.bfbs.clone());
            builder
                .message_encoding("flatbuffer")
                .schema(foxglove_schema)
                .add_metadata("schema_hash", &s.hash_hex)
        }
        None => builder
            .message_encoding("aorta_raw_bytes")
            .add_metadata("schema_hash", "raw"),
    };

    builder
        .build_raw()
        .map_err(|e| anyhow!("failed to build foxglove channel for '{}': {}", full_key, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_channel_is_created() {
        assert_eq!(plan_channel_action(None, None), ChannelAction::Create);
        assert_eq!(plan_channel_action(None, Some("aa")), ChannelAction::Create);
    }

    #[test]
    fn same_hash_reuses_channel() {
        let typed = Some("aa".to_string());
        assert_eq!(
            plan_channel_action(Some(&typed), Some("aa")),
            ChannelAction::Reuse
        );
    }

    #[test]
    fn typed_channel_never_downgrades_to_raw() {
        // The regression this registry exists to prevent: a transient
        // schema-resolve failure (schema = None) must not close a typed
        // channel.
        let typed = Some("aa".to_string());
        assert_eq!(
            plan_channel_action(Some(&typed), None),
            ChannelAction::Reuse
        );
    }

    #[test]
    fn raw_channel_upgrades_to_typed() {
        let raw: Option<String> = None;
        assert_eq!(
            plan_channel_action(Some(&raw), Some("aa")),
            ChannelAction::Replace
        );
    }

    #[test]
    fn raw_sample_on_raw_channel_reuses() {
        let raw: Option<String> = None;
        assert_eq!(plan_channel_action(Some(&raw), None), ChannelAction::Reuse);
    }

    #[test]
    fn schema_hash_change_replaces_channel() {
        let typed = Some("aa".to_string());
        assert_eq!(
            plan_channel_action(Some(&typed), Some("bb")),
            ChannelAction::Replace
        );
    }
}
