use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::Path;

const DEFAULT_GROUP: &str = "default";
const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8765;
const DEFAULT_SERVER_NAME: &str = "aorta-foxglove-bridge";
const DEFAULT_BY_HASH_TIMEOUT_MS: u64 = 2000;
const DEFAULT_BY_KEY_TIMEOUT_MS: u64 = 2000;
const DEFAULT_CACHE_MAX_ENTRIES: usize = 256;
// Mirrors the recorder's default: an unregistered
// topic costs one query per TTL window, not one per message.
const DEFAULT_NEGATIVE_CACHE_TTL_MS: u64 = 30_000;
const DEFAULT_DISCOVERY_INTERVAL_MS: u64 = 2000;
// Live-viewer equivalent of the recorder's t0 ctx snapshot: refresh
// current ctx values periodically so a late-connecting Foxglove client
// sees context state within one interval instead of waiting for the
// next put. 0 disables.
const DEFAULT_CTX_SNAPSHOT_INTERVAL_MS: u64 = 5000;
const DEFAULT_QUEUE_CAPACITY: usize = 8192;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl LogLevel {
    #[allow(clippy::should_implement_trait)] // fallible parse mirroring aorta-record's config vocabulary
    pub fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => Err(anyhow!("unknown log level '{}'", other)),
        }
    }
}

/// What to do when the forwarder queue is full. Same vocabulary as the
/// recorder's `DropPolicy`, but the default differs: a live viewer prefers
/// fresh frames over completeness, so `DropNewest` is the default here
/// (the recorder defaults to `Block` because a recording must be complete).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DropPolicy {
    /// Backpressure into the Zenoh subscriber (never lose a sample).
    Block,
    /// Drop the incoming sample and count it (never stall the pipeline).
    DropNewest,
}

impl DropPolicy {
    #[allow(clippy::should_implement_trait)] // fallible parse mirroring aorta-record's config vocabulary
    pub fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "block" => Ok(Self::Block),
            "drop_newest" | "drop-newest" => Ok(Self::DropNewest),
            other => Err(anyhow!("unsupported drop_policy: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FileConfig {
    pub group: Option<String>,
    pub zenoh: Option<ZenohSection>,
    pub server: Option<ServerSection>,
    pub topics: Option<TopicsSection>,
    pub schema: Option<SchemaSection>,
    pub discovery: Option<DiscoverySection>,
    pub forwarder: Option<ForwarderSection>,
    pub logging: Option<LoggingSection>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ZenohSection {
    pub config_file: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServerSection {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TopicsSection {
    pub include_suffix_keyexpr: Option<Vec<String>>,
    pub blacklist_suffix_keyexpr: Option<Vec<String>>,
    pub blacklist_full_keyexpr: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SchemaSection {
    pub by_hash_timeout_ms: Option<u64>,
    pub by_key_timeout_ms: Option<u64>,
    pub cache_max_entries: Option<usize>,
    pub negative_cache_ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DiscoverySection {
    pub interval_ms: Option<u64>,
    pub ctx_snapshot_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ForwarderSection {
    pub queue_capacity: Option<usize>,
    pub drop_policy: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LoggingSection {
    pub level: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    pub group: Option<String>,
    pub zenoh_config_file: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub server_name: Option<String>,
    pub include_suffix_keyexpr: Vec<String>,
    pub blacklist_suffix_keyexpr: Vec<String>,
    pub blacklist_full_keyexpr: Vec<String>,
    pub by_hash_timeout_ms: Option<u64>,
    pub by_key_timeout_ms: Option<u64>,
    pub cache_max_entries: Option<usize>,
    pub negative_cache_ttl_ms: Option<u64>,
    pub discovery_interval_ms: Option<u64>,
    pub ctx_snapshot_interval_ms: Option<u64>,
    pub queue_capacity: Option<usize>,
    pub drop_policy: Option<DropPolicy>,
    pub log_level: Option<LogLevel>,
}

#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub group: String,
    pub zenoh_config_file: Option<String>,
    pub server: ServerConfig,
    pub topics: TopicsConfig,
    pub schema: SchemaConfig,
    pub discovery: DiscoveryConfig,
    pub forwarder: ForwarderConfig,
    pub log_level: LogLevel,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct TopicsConfig {
    pub include_suffix: Vec<String>,
    pub blacklist_suffix: Vec<String>,
    pub blacklist_full: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SchemaConfig {
    pub by_hash_timeout_ms: u64,
    pub by_key_timeout_ms: u64,
    pub cache_max_entries: usize,
    pub negative_cache_ttl_ms: u64,
}

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub interval_ms: u64,
    /// 0 disables the periodic ctx snapshot.
    pub ctx_snapshot_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ForwarderConfig {
    pub queue_capacity: usize,
    pub drop_policy: DropPolicy,
}

pub fn load_file_config(path: &Path) -> Result<FileConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config '{}'", path.display()))?;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let parsed: FileConfig = match ext.as_str() {
        "json" => serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse JSON config '{}'", path.display()))?,
        "toml" => toml::from_str(&raw)
            .with_context(|| format!("failed to parse TOML config '{}'", path.display()))?,
        _ => serde_yaml::from_str(&raw).with_context(|| {
            format!(
                "failed to parse YAML config '{}' (use .json or .toml extension to force a different format)",
                path.display()
            )
        })?,
    };
    Ok(parsed)
}

pub fn build_config(file: Option<FileConfig>, overrides: ConfigOverrides) -> Result<BridgeConfig> {
    let file = file.unwrap_or_default();

    let group = overrides
        .group
        .or(file.group)
        .or_else(|| std::env::var("AORTA_GROUP").ok())
        .unwrap_or_else(|| DEFAULT_GROUP.to_string());
    if group.trim().is_empty() {
        return Err(anyhow!("group cannot be empty"));
    }

    let zenoh_config_file = overrides
        .zenoh_config_file
        .or_else(|| file.zenoh.as_ref().and_then(|z| z.config_file.clone()))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let server = ServerConfig {
        host: overrides
            .host
            .or_else(|| file.server.as_ref().and_then(|s| s.host.clone()))
            .unwrap_or_else(|| DEFAULT_HOST.to_string()),
        port: overrides
            .port
            .or_else(|| file.server.as_ref().and_then(|s| s.port))
            .unwrap_or(DEFAULT_PORT),
        name: overrides
            .server_name
            .or_else(|| file.server.as_ref().and_then(|s| s.name.clone()))
            .unwrap_or_else(|| DEFAULT_SERVER_NAME.to_string()),
    };

    let topics_file = file.topics.unwrap_or_default();
    let include_from_file = topics_file.include_suffix_keyexpr.unwrap_or_default();
    let blacklist_from_file = topics_file.blacklist_suffix_keyexpr.unwrap_or_default();
    let blacklist_full_from_file = topics_file.blacklist_full_keyexpr.unwrap_or_default();

    let include_suffix = if !overrides.include_suffix_keyexpr.is_empty() {
        overrides.include_suffix_keyexpr
    } else {
        include_from_file
    };
    let blacklist_suffix = merge_unique(blacklist_from_file, overrides.blacklist_suffix_keyexpr);
    let blacklist_full = merge_unique(blacklist_full_from_file, overrides.blacklist_full_keyexpr);

    let topics = TopicsConfig {
        include_suffix,
        blacklist_suffix,
        blacklist_full,
    };

    let schema_file = file.schema.unwrap_or_default();
    let schema = SchemaConfig {
        by_hash_timeout_ms: overrides
            .by_hash_timeout_ms
            .or(schema_file.by_hash_timeout_ms)
            .unwrap_or(DEFAULT_BY_HASH_TIMEOUT_MS),
        by_key_timeout_ms: overrides
            .by_key_timeout_ms
            .or(schema_file.by_key_timeout_ms)
            .unwrap_or(DEFAULT_BY_KEY_TIMEOUT_MS),
        cache_max_entries: overrides
            .cache_max_entries
            .or(schema_file.cache_max_entries)
            .unwrap_or(DEFAULT_CACHE_MAX_ENTRIES),
        negative_cache_ttl_ms: overrides
            .negative_cache_ttl_ms
            .or(schema_file.negative_cache_ttl_ms)
            .unwrap_or(DEFAULT_NEGATIVE_CACHE_TTL_MS),
    };

    let discovery_file = file.discovery.unwrap_or_default();
    let discovery = DiscoveryConfig {
        interval_ms: overrides
            .discovery_interval_ms
            .or(discovery_file.interval_ms)
            .unwrap_or(DEFAULT_DISCOVERY_INTERVAL_MS),
        ctx_snapshot_interval_ms: overrides
            .ctx_snapshot_interval_ms
            .or(discovery_file.ctx_snapshot_interval_ms)
            .unwrap_or(DEFAULT_CTX_SNAPSHOT_INTERVAL_MS),
    };

    let forwarder_file = file.forwarder.unwrap_or_default();
    let drop_policy = match overrides.drop_policy {
        Some(policy) => policy,
        None => match forwarder_file.drop_policy {
            Some(raw) => DropPolicy::from_str(&raw)?,
            None => DropPolicy::DropNewest,
        },
    };
    let forwarder = ForwarderConfig {
        queue_capacity: overrides
            .queue_capacity
            .or(forwarder_file.queue_capacity)
            .unwrap_or(DEFAULT_QUEUE_CAPACITY),
        drop_policy,
    };
    if forwarder.queue_capacity == 0 {
        return Err(anyhow!("forwarder.queue_capacity must be > 0"));
    }

    let log_level = match overrides.log_level {
        Some(level) => level,
        None => match file.logging.and_then(|l| l.level) {
            Some(raw) => LogLevel::from_str(&raw)?,
            None => LogLevel::Info,
        },
    };

    Ok(BridgeConfig {
        group,
        zenoh_config_file,
        server,
        topics,
        schema,
        discovery,
        forwarder,
        log_level,
    })
}

fn merge_unique(mut a: Vec<String>, b: Vec<String>) -> Vec<String> {
    for value in b {
        if !a.iter().any(|existing| existing == &value) {
            a.push(value);
        }
    }
    a
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_config_uses_defaults_when_nothing_set() {
        let config = build_config(None, ConfigOverrides::default()).expect("config");
        assert_eq!(config.group, "default");
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8765);
        assert_eq!(config.server.name, "aorta-foxglove-bridge");
        assert_eq!(config.discovery.interval_ms, 2000);
        assert_eq!(config.discovery.ctx_snapshot_interval_ms, 5000);
        assert_eq!(config.schema.negative_cache_ttl_ms, 30_000);
        assert_eq!(config.forwarder.drop_policy, DropPolicy::DropNewest);
        assert!(config.topics.include_suffix.is_empty());
        assert_eq!(config.log_level as u8, LogLevel::Info as u8);
    }

    #[test]
    fn cli_overrides_take_precedence_over_file() {
        let file = FileConfig {
            group: Some("file_group".to_string()),
            server: Some(ServerSection {
                host: Some("0.0.0.0".to_string()),
                port: Some(1234),
                name: None,
            }),
            ..Default::default()
        };
        let overrides = ConfigOverrides {
            group: Some("cli_group".to_string()),
            port: Some(9999),
            ..Default::default()
        };
        let config = build_config(Some(file), overrides).expect("config");
        assert_eq!(config.group, "cli_group");
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 9999);
    }

    #[test]
    fn empty_group_is_rejected() {
        let overrides = ConfigOverrides {
            group: Some("   ".to_string()),
            ..Default::default()
        };
        let err = build_config(None, overrides).expect_err("empty group");
        assert!(err.to_string().contains("group"));
    }

    #[test]
    fn merge_unique_dedupes() {
        let merged = merge_unique(
            vec!["a".to_string(), "b".to_string()],
            vec!["b".to_string(), "c".to_string()],
        );
        assert_eq!(merged, vec!["a", "b", "c"]);
    }

    #[test]
    fn zero_queue_capacity_rejected() {
        let overrides = ConfigOverrides {
            queue_capacity: Some(0),
            ..Default::default()
        };
        let err = build_config(None, overrides).expect_err("zero capacity");
        assert!(err.to_string().contains("queue_capacity"));
    }

    #[test]
    fn drop_policy_parses_from_file_and_rejects_unknown() {
        let file = FileConfig {
            forwarder: Some(ForwarderSection {
                queue_capacity: None,
                drop_policy: Some("block".to_string()),
            }),
            ..Default::default()
        };
        let config = build_config(Some(file), ConfigOverrides::default()).expect("config");
        assert_eq!(config.forwarder.drop_policy, DropPolicy::Block);

        let bad = FileConfig {
            forwarder: Some(ForwarderSection {
                queue_capacity: None,
                drop_policy: Some("drop_oldest".to_string()),
            }),
            ..Default::default()
        };
        let err = build_config(Some(bad), ConfigOverrides::default()).expect_err("bad policy");
        assert!(err.to_string().contains("drop_policy"));
    }
}
