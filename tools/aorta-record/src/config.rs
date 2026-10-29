use crate::filter::normalize_suffix_pattern;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_GROUP: &str = "default";
const DEFAULT_QUEUE_CAPACITY: usize = 8192;
const DEFAULT_BATCH_MAX_MESSAGES: usize = 1024;
const DEFAULT_BATCH_MAX_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 200;
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 5000;
const DEFAULT_BY_HASH_TIMEOUT_MS: u64 = 2000;
const DEFAULT_BY_KEY_TIMEOUT_MS: u64 = 2000;
/// Default TTL for the resolver's negative (not-found) cache. An unregistered
/// topic costs one bounded by-key query per TTL window instead of one per
/// message; a publisher that registers late is picked
/// up within this window. `0` disables negative caching.
const DEFAULT_NEGATIVE_CACHE_TTL_MS: u64 = 30_000;
/// Default period for the live stats log line (accepted/dropped/queue depths).
/// `0` disables the periodic line; the end-of-run summary always prints.
const DEFAULT_STATS_INTERVAL_MS: u64 = 10_000;
const DEFAULT_STORAGE_CHUNK_SIZE: u64 = 768 * 1024;

/// Default per-segment split period when rotation is enabled (minutes).
const DEFAULT_ROTATION_SPLIT_MINUTES: u64 = 1;
/// Minimum configured time split, in seconds.
pub const ROTATION_MIN_SPLIT_SECONDS: u64 = 60;
const BYTES_PER_MB: u64 = 1024 * 1024;

const ZSTD_LEVEL_FAST: u32 = 1;
const ZSTD_LEVEL_DEFAULT: u32 = 3;
const ZSTD_LEVEL_BEST: u32 = 19;
const ZSTD_LEVEL_MIN: u32 = 1;
const ZSTD_LEVEL_MAX: u32 = 22;

const LZ4_LEVEL_FAST: u32 = 0;
const LZ4_LEVEL_DEFAULT: u32 = 0;
const LZ4_LEVEL_BEST: u32 = 12;
const LZ4_LEVEL_MAX: u32 = 12;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FileConfig {
    pub group: Option<String>,
    pub zenoh: Option<ZenohConfig>,
    pub record: Option<RecordSection>,
    pub topics: Option<TopicsSection>,
    pub schema: Option<SchemaSection>,
    pub writer: Option<WriterSection>,
    pub storage: Option<StorageSection>,
    pub logging: Option<LoggingSection>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ZenohConfig {
    pub config_file: Option<String>,
    /// Force `mode=client` on the resolved zenoh config (mirrors aorta-cli/tui's
    /// default observer-tool bootstrap). Default: true.
    pub client_mode_override: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RecordSection {
    pub output: Option<String>,
    pub rpc_record: Option<bool>,
    pub duration_sec: Option<u64>,
    pub max_messages: Option<u64>,
    pub ctx_snapshot: Option<bool>,
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
    pub attachment_fallback: Option<bool>,
    pub cache_max_entries: Option<usize>,
    pub negative_cache_ttl_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct WriterSection {
    pub queue_capacity: Option<usize>,
    pub batch_max_messages: Option<usize>,
    pub batch_max_bytes: Option<usize>,
    pub flush_interval_ms: Option<u64>,
    pub shutdown_timeout_ms: Option<u64>,
    pub drop_policy: Option<String>,
    pub stats_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StorageSection {
    pub compression: Option<String>,
    pub compression_level: Option<String>,
    pub chunk_size: Option<u64>,
    pub chunking: Option<bool>,
    pub summary: Option<bool>,
    pub rotation: Option<RotationSection>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RotationSection {
    pub enabled: Option<bool>,
    pub split_minutes: Option<u64>,
    pub max_file_size_mb: Option<u64>,
    pub name_template: Option<String>,
    pub timestamp: Option<String>,
    pub max_files: Option<usize>,
    pub max_total_size_mb: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LoggingSection {
    pub level: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    pub group: Option<String>,
    pub zenoh_config_file: Option<String>,
    pub client_mode_override: Option<bool>,
    pub output: Option<String>,
    pub rpc_record: Option<bool>,
    pub duration_sec: Option<u64>,
    pub max_messages: Option<u64>,
    pub ctx_snapshot: Option<bool>,
    pub include_suffix_keyexpr: Vec<String>,
    pub blacklist_suffix_keyexpr: Vec<String>,
    pub blacklist_full_keyexpr: Vec<String>,
    pub by_hash_timeout_ms: Option<u64>,
    pub by_key_timeout_ms: Option<u64>,
    pub negative_cache_ttl_ms: Option<u64>,
    pub queue_capacity: Option<usize>,
    pub batch_max_messages: Option<usize>,
    pub batch_max_bytes: Option<usize>,
    pub flush_interval_ms: Option<u64>,
    pub shutdown_timeout_ms: Option<u64>,
    pub stats_interval_ms: Option<u64>,
    pub drop_policy: Option<DropPolicy>,
    pub compression: Option<CompressionKind>,
    pub compression_level: Option<String>,
    pub chunk_size: Option<u64>,
    pub chunking: Option<bool>,
    pub summary: Option<bool>,
    pub rotate_enabled: Option<bool>,
    pub split_minutes: Option<u64>,
    pub rotate_max_file_size_mb: Option<u64>,
    pub rotate_name_template: Option<String>,
    pub rotate_timestamp: Option<String>,
    pub rotate_max_files: Option<usize>,
    pub rotate_max_total_size_mb: Option<u64>,
    pub log_level: Option<LogLevel>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DropPolicy {
    Block,
    DropNewest,
}

impl DropPolicy {
    pub fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "block" => Ok(Self::Block),
            "drop_newest" => Ok(Self::DropNewest),
            other => Err(anyhow!("unsupported drop_policy: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CompressionKind {
    None,
    Zstd,
    Lz4,
}

impl CompressionKind {
    pub fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "zstd" => Ok(Self::Zstd),
            "lz4" => Ok(Self::Lz4),
            other => Err(anyhow!(
                "unsupported compression: '{}' (expected none|zstd|lz4)",
                other
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
        }
    }

    fn default_level(self) -> u32 {
        match self {
            Self::None => 0,
            Self::Zstd => ZSTD_LEVEL_FAST,
            Self::Lz4 => LZ4_LEVEL_FAST,
        }
    }
}

pub fn parse_compression_level(kind: CompressionKind, raw: &str) -> Result<u32> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(anyhow!("compression_level must not be empty"));
    }

    let level = match (kind, trimmed.as_str()) {
        (CompressionKind::None, _) => 0,
        (CompressionKind::Zstd, "fast") => ZSTD_LEVEL_FAST,
        (CompressionKind::Zstd, "default") => ZSTD_LEVEL_DEFAULT,
        (CompressionKind::Zstd, "best") => ZSTD_LEVEL_BEST,
        (CompressionKind::Lz4, "fast") => LZ4_LEVEL_FAST,
        (CompressionKind::Lz4, "default") => LZ4_LEVEL_DEFAULT,
        (CompressionKind::Lz4, "best") => LZ4_LEVEL_BEST,
        (kind, other) => {
            let numeric: u32 = other
                .parse()
                .map_err(|_| anyhow!(
                    "unsupported compression_level for {}: '{}' (expected fast|default|best or a number)",
                    kind.as_str(),
                    other
                ))?;
            validate_level_range(kind, numeric)?;
            numeric
        }
    };

    Ok(level)
}

fn validate_level_range(kind: CompressionKind, level: u32) -> Result<()> {
    match kind {
        CompressionKind::None => Ok(()),
        CompressionKind::Zstd => {
            if (ZSTD_LEVEL_MIN..=ZSTD_LEVEL_MAX).contains(&level) {
                Ok(())
            } else {
                Err(anyhow!(
                    "zstd compression_level {} out of range [{}, {}]",
                    level,
                    ZSTD_LEVEL_MIN,
                    ZSTD_LEVEL_MAX
                ))
            }
        }
        CompressionKind::Lz4 => {
            if level <= LZ4_LEVEL_MAX {
                Ok(())
            } else {
                Err(anyhow!(
                    "lz4 compression_level {} out of range [0, {}]",
                    level,
                    LZ4_LEVEL_MAX
                ))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            other => Err(anyhow!("unsupported log level: {}", other)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub compression: CompressionKind,
    pub compression_level: u32,
    pub chunk_size: Option<u64>,
    pub chunking: bool,
    pub summary: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            compression: CompressionKind::Zstd,
            compression_level: ZSTD_LEVEL_FAST,
            chunk_size: Some(DEFAULT_STORAGE_CHUNK_SIZE),
            chunking: true,
            summary: true,
        }
    }
}

/// Wall-clock zone used to render the `{ts}` placeholder in segment filenames.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TimestampZone {
    Utc,
    Local,
}

impl TimestampZone {
    pub fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "utc" => Ok(Self::Utc),
            "local" => Ok(Self::Local),
            other => Err(anyhow!(
                "unsupported rotation.timestamp: '{}' (expected utc|local)",
                other
            )),
        }
    }
}

/// Resolved rotation policy. When `enabled` is false the recorder writes a
/// single file at the literal output path (today's behavior); all other fields
/// are ignored. See `spec/Aorta_Recorder_Rotation_Plan.md`.
#[derive(Debug, Clone)]
pub struct RotationConfig {
    pub enabled: bool,
    /// Optional per-segment split period. Always `>= ROTATION_MIN_SPLIT_SECONDS`.
    pub split: Option<Duration>,
    /// Optional per-segment size cap.
    pub max_file_size_bytes: Option<u64>,
    /// Optional filename template. Supports `{ts}`, `{stem}`, `{ext}`, `{group}`,
    /// `{pid}`, and zero-padded `{index}`; `None` derives `{stem}_{ts}.{ext}`.
    pub name_template: Option<String>,
    pub timestamp: TimestampZone,
    /// Retention: keep at most this many finalized segments (None = keep all).
    pub max_files: Option<usize>,
    /// Retention: aggregate on-disk byte cap across finalized segments.
    pub max_total_size_bytes: Option<u64>,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            split: Some(Duration::from_secs(ROTATION_MIN_SPLIT_SECONDS)),
            max_file_size_bytes: None,
            name_template: None,
            timestamp: TimestampZone::Utc,
            max_files: None,
            max_total_size_bytes: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecorderConfig {
    pub group: String,
    pub zenoh_config_file: Option<String>,
    pub client_mode_override: bool,
    pub output: PathBuf,
    pub rpc_record: bool,
    pub duration_sec: Option<u64>,
    pub max_messages: Option<u64>,
    pub ctx_snapshot: bool,
    pub include_suffix_keyexpr: Vec<String>,
    pub blacklist_suffix_keyexpr: Vec<String>,
    pub blacklist_full_keyexpr: Vec<String>,
    pub by_hash_timeout_ms: u64,
    pub by_key_timeout_ms: u64,
    pub negative_cache_ttl_ms: u64,
    pub attachment_fallback: bool,
    pub cache_max_entries: usize,
    pub queue_capacity: usize,
    pub batch_max_messages: usize,
    pub batch_max_bytes: usize,
    pub flush_interval_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub stats_interval_ms: u64,
    pub drop_policy: DropPolicy,
    pub storage: StorageConfig,
    pub rotation: RotationConfig,
    pub log_level: LogLevel,
}

pub fn load_file_config(path: &Path) -> Result<FileConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file '{}'", path.display()))?;

    match path.extension().and_then(|v| v.to_str()) {
        Some("yaml") | Some("yml") => serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse YAML config '{}'", path.display())),
        Some("json") => serde_json::from_str(&content)
            .with_context(|| format!("failed to parse JSON config '{}'", path.display())),
        Some("toml") => toml::from_str(&content)
            .with_context(|| format!("failed to parse TOML config '{}'", path.display())),
        _ => Err(anyhow!(
            "unsupported config file extension for '{}' (expected .yaml/.yml/.json/.toml)",
            path.display()
        )),
    }
}

pub fn build_config(
    file: Option<FileConfig>,
    mut overrides: ConfigOverrides,
) -> Result<RecorderConfig> {
    let file = file.unwrap_or_default();

    let file_record = file.record.unwrap_or_default();
    let file_topics = file.topics.unwrap_or_default();
    let file_schema = file.schema.unwrap_or_default();
    let file_writer = file.writer.unwrap_or_default();
    let file_storage = file.storage.unwrap_or_default();
    let file_logging = file.logging.unwrap_or_default();

    let storage = build_storage_config(&file_storage, &mut overrides)?;
    let rotation = build_rotation_config(&file_storage, &mut overrides)?;

    let env_group = std::env::var("AORTA_GROUP")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let group = first_some_string(
        overrides.group,
        file.group,
        env_group.or_else(|| Some(DEFAULT_GROUP.to_string())),
    )
    .ok_or_else(|| anyhow!("group cannot be empty"))?;
    let group = group.trim().to_string();
    if group.is_empty() {
        return Err(anyhow!("group cannot be empty"));
    }

    let file_zenoh = file.zenoh.unwrap_or_default();
    let zenoh_config_file =
        first_some_string(overrides.zenoh_config_file, file_zenoh.config_file, None);
    let client_mode_override = overrides
        .client_mode_override
        .or(file_zenoh.client_mode_override)
        .unwrap_or(true);

    let output =
        first_some_string(overrides.output, file_record.output, None).ok_or_else(|| {
            anyhow!("record output path is required (--output or config record.output)")
        })?;

    let mut include_suffix_keyexpr = if !overrides.include_suffix_keyexpr.is_empty() {
        overrides.include_suffix_keyexpr
    } else {
        file_topics.include_suffix_keyexpr.unwrap_or_default()
    };
    let mut blacklist_suffix_keyexpr = if !overrides.blacklist_suffix_keyexpr.is_empty() {
        overrides.blacklist_suffix_keyexpr
    } else {
        file_topics.blacklist_suffix_keyexpr.unwrap_or_default()
    };

    for pattern in &mut include_suffix_keyexpr {
        *pattern = normalize_suffix_pattern(pattern)?;
    }
    for pattern in &mut blacklist_suffix_keyexpr {
        *pattern = normalize_suffix_pattern(pattern)?;
    }

    let blacklist_full_keyexpr = if !overrides.blacklist_full_keyexpr.is_empty() {
        overrides.blacklist_full_keyexpr
    } else {
        file_topics.blacklist_full_keyexpr.unwrap_or_default()
    };

    let drop_policy = match overrides.drop_policy {
        Some(policy) => policy,
        None => match file_writer.drop_policy {
            Some(raw) => DropPolicy::from_str(&raw)?,
            None => DropPolicy::Block,
        },
    };

    let log_level = match overrides.log_level {
        Some(level) => level,
        None => match file_logging.level {
            Some(raw) => LogLevel::from_str(&raw)?,
            None => LogLevel::Info,
        },
    };

    Ok(RecorderConfig {
        group,
        zenoh_config_file,
        client_mode_override,
        output: PathBuf::from(output),
        rpc_record: overrides
            .rpc_record
            .unwrap_or(file_record.rpc_record.unwrap_or(false)),
        duration_sec: overrides.duration_sec.or(file_record.duration_sec),
        max_messages: overrides.max_messages.or(file_record.max_messages),
        ctx_snapshot: overrides
            .ctx_snapshot
            .unwrap_or(file_record.ctx_snapshot.unwrap_or(true)),
        include_suffix_keyexpr,
        blacklist_suffix_keyexpr,
        blacklist_full_keyexpr,
        by_hash_timeout_ms: overrides.by_hash_timeout_ms.unwrap_or(
            file_schema
                .by_hash_timeout_ms
                .unwrap_or(DEFAULT_BY_HASH_TIMEOUT_MS),
        ),
        by_key_timeout_ms: overrides.by_key_timeout_ms.unwrap_or(
            file_schema
                .by_key_timeout_ms
                .unwrap_or(DEFAULT_BY_KEY_TIMEOUT_MS),
        ),
        negative_cache_ttl_ms: overrides.negative_cache_ttl_ms.unwrap_or(
            file_schema
                .negative_cache_ttl_ms
                .unwrap_or(DEFAULT_NEGATIVE_CACHE_TTL_MS),
        ),
        attachment_fallback: file_schema.attachment_fallback.unwrap_or(true),
        cache_max_entries: file_schema.cache_max_entries.unwrap_or(4096),
        queue_capacity: overrides
            .queue_capacity
            .unwrap_or(file_writer.queue_capacity.unwrap_or(DEFAULT_QUEUE_CAPACITY)),
        batch_max_messages: overrides.batch_max_messages.unwrap_or(
            file_writer
                .batch_max_messages
                .unwrap_or(DEFAULT_BATCH_MAX_MESSAGES),
        ),
        batch_max_bytes: overrides.batch_max_bytes.unwrap_or(
            file_writer
                .batch_max_bytes
                .unwrap_or(DEFAULT_BATCH_MAX_BYTES),
        ),
        flush_interval_ms: overrides.flush_interval_ms.unwrap_or(
            file_writer
                .flush_interval_ms
                .unwrap_or(DEFAULT_FLUSH_INTERVAL_MS),
        ),
        shutdown_timeout_ms: overrides.shutdown_timeout_ms.unwrap_or(
            file_writer
                .shutdown_timeout_ms
                .unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT_MS),
        ),
        stats_interval_ms: overrides.stats_interval_ms.unwrap_or(
            file_writer
                .stats_interval_ms
                .unwrap_or(DEFAULT_STATS_INTERVAL_MS),
        ),
        drop_policy,
        storage,
        rotation,
        log_level,
    })
}

/// Resolve `storage.rotation.*` (config file) merged with CLI/env overrides into
/// a `RotationConfig`. Mirrors `build_storage_config`'s precedence
/// (`override > file > default`). Validation is enforced only when rotation is
/// enabled, so a disabled stanza with placeholder values never blocks startup.
fn build_rotation_config(
    file_storage: &StorageSection,
    overrides: &mut ConfigOverrides,
) -> Result<RotationConfig> {
    let file_rotation = file_storage.rotation.clone().unwrap_or_default();
    let defaults = RotationConfig::default();

    let enabled = overrides
        .rotate_enabled
        .or(file_rotation.enabled)
        .unwrap_or(defaults.enabled);

    if !enabled {
        return Ok(defaults);
    }

    let split_minutes = overrides.split_minutes.or(file_rotation.split_minutes);
    if split_minutes == Some(0) {
        return Err(anyhow!(
            "rotation.split_minutes must be >= 1 (the minimum split unit is one minute)"
        ));
    }

    let max_file_size_bytes = overrides
        .rotate_max_file_size_mb
        .or(file_rotation.max_file_size_mb)
        .filter(|mb| *mb > 0)
        .map(|mb| mb.saturating_mul(BYTES_PER_MB));

    // Preserve `--rotate`'s one-minute default, but a size cap on its own means
    // size-only rotation.
    let split = split_minutes
        .or_else(|| {
            max_file_size_bytes
                .is_none()
                .then_some(DEFAULT_ROTATION_SPLIT_MINUTES)
        })
        .map(|minutes| Duration::from_secs(minutes.saturating_mul(60)));

    let max_total_size_bytes = overrides
        .rotate_max_total_size_mb
        .or(file_rotation.max_total_size_mb)
        .filter(|mb| *mb > 0)
        .map(|mb| mb.saturating_mul(BYTES_PER_MB));

    let max_files = overrides
        .rotate_max_files
        .or(file_rotation.max_files)
        .filter(|n| *n > 0);

    let name_template = overrides
        .rotate_name_template
        .take()
        .or_else(|| file_rotation.name_template.clone())
        .filter(|t| !t.trim().is_empty());

    let timestamp = match overrides.rotate_timestamp.take().as_deref() {
        Some(raw) => TimestampZone::from_str(raw)?,
        None => match file_rotation.timestamp.as_deref() {
            Some(raw) => TimestampZone::from_str(raw)?,
            None => defaults.timestamp,
        },
    };
    if timestamp == TimestampZone::Local {
        // Local-time naming reintroduces DST/backward-step collisions and needs
        // a timezone source we deliberately avoid depending on. UTC is the
        // default; local is rejected until a later phase wires it safely.
        return Err(anyhow!(
            "rotation.timestamp=local is not yet supported; use utc \
             (see Aorta_Recorder_Rotation_Plan.md §3.4)"
        ));
    }

    Ok(RotationConfig {
        enabled,
        split,
        max_file_size_bytes,
        name_template,
        timestamp,
        max_files,
        max_total_size_bytes,
    })
}

fn build_storage_config(
    file_storage: &StorageSection,
    overrides: &mut ConfigOverrides,
) -> Result<StorageConfig> {
    let defaults = StorageConfig::default();

    let compression = match overrides.compression {
        Some(kind) => kind,
        None => match file_storage.compression.as_deref() {
            Some(raw) => CompressionKind::from_str(raw)?,
            None => defaults.compression,
        },
    };

    let override_level = overrides.compression_level.take();
    let compression_level = match override_level.as_deref() {
        Some(raw) => parse_compression_level(compression, raw)?,
        None => match file_storage.compression_level.as_deref() {
            Some(raw) => parse_compression_level(compression, raw)?,
            None => compression.default_level(),
        },
    };

    let chunk_size = overrides
        .chunk_size
        .or(file_storage.chunk_size)
        .or(defaults.chunk_size);

    let chunking = overrides
        .chunking
        .or(file_storage.chunking)
        .unwrap_or(defaults.chunking);

    let summary = overrides
        .summary
        .or(file_storage.summary)
        .unwrap_or(defaults.summary);

    if !chunking && compression != CompressionKind::None {
        return Err(anyhow!(
            "storage.compression={} requires chunking=true (MCAP compresses per chunk); \
             disable chunking only with compression=none",
            compression.as_str()
        ));
    }

    Ok(StorageConfig {
        compression,
        compression_level,
        chunk_size,
        chunking,
        summary,
    })
}

fn first_some_string(a: Option<String>, b: Option<String>, c: Option<String>) -> Option<String> {
    a.or(b).or(c)
}

#[cfg(test)]
mod tests {
    use super::{
        build_config, parse_compression_level, CompressionKind, ConfigOverrides, DropPolicy,
        FileConfig, LogLevel, StorageSection, DEFAULT_STORAGE_CHUNK_SIZE, LZ4_LEVEL_BEST,
        LZ4_LEVEL_FAST, ZSTD_LEVEL_BEST, ZSTD_LEVEL_DEFAULT, ZSTD_LEVEL_FAST,
    };
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn build_config_uses_defaults() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert_eq!(config.group, "default");
        assert_eq!(config.output.to_string_lossy(), "/tmp/test.mcap");
        assert_eq!(config.drop_policy, DropPolicy::Block);
        assert_eq!(config.log_level, LogLevel::Info);
    }

    #[test]
    fn build_config_ctx_snapshot_defaults_on_and_respects_overrides() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");

        // Default: enabled.
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert!(config.ctx_snapshot);

        // File config can disable it.
        let file = FileConfig {
            record: Some(super::RecordSection {
                ctx_snapshot: Some(false),
                ..super::RecordSection::default()
            }),
            ..FileConfig::default()
        };
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        let config = build_config(Some(file), overrides).expect("config");
        assert!(!config.ctx_snapshot);

        // CLI override wins over file config.
        let file = FileConfig {
            record: Some(super::RecordSection {
                ctx_snapshot: Some(true),
                ..super::RecordSection::default()
            }),
            ..FileConfig::default()
        };
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        overrides.ctx_snapshot = Some(false);
        let config = build_config(Some(file), overrides).expect("config");
        assert!(!config.ctx_snapshot);
    }

    #[test]
    fn build_config_rob01_dry04_defaults_and_precedence() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");

        // Defaults: negative cache 30s, stats line every 10s, client override on.
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert_eq!(config.negative_cache_ttl_ms, 30_000);
        assert_eq!(config.stats_interval_ms, 10_000);
        assert!(config.client_mode_override);

        // File config sets all three; 0 disables the time-based knobs.
        let file = FileConfig {
            zenoh: Some(super::ZenohConfig {
                client_mode_override: Some(false),
                ..super::ZenohConfig::default()
            }),
            schema: Some(super::SchemaSection {
                negative_cache_ttl_ms: Some(0),
                ..super::SchemaSection::default()
            }),
            writer: Some(super::WriterSection {
                stats_interval_ms: Some(0),
                ..super::WriterSection::default()
            }),
            ..FileConfig::default()
        };
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        let config = build_config(Some(file.clone()), overrides).expect("config");
        assert_eq!(config.negative_cache_ttl_ms, 0);
        assert_eq!(config.stats_interval_ms, 0);
        assert!(!config.client_mode_override);

        // CLI overrides beat the file config.
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        overrides.negative_cache_ttl_ms = Some(5_000);
        overrides.stats_interval_ms = Some(1_000);
        overrides.client_mode_override = Some(true);
        let config = build_config(Some(file), overrides).expect("config");
        assert_eq!(config.negative_cache_ttl_ms, 5_000);
        assert_eq!(config.stats_interval_ms, 1_000);
        assert!(config.client_mode_override);
    }

    #[test]
    fn build_config_prefers_overrides() {
        let _guard = env_lock().lock().expect("lock");
        std::env::set_var("AORTA_GROUP", "env_fleet");
        let mut overrides = ConfigOverrides::default();
        overrides.group = Some("fleet".to_string());
        overrides.output = Some("/tmp/override.mcap".to_string());
        overrides.include_suffix_keyexpr = vec!["demo/**".to_string()];

        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert_eq!(config.group, "fleet");
        assert_eq!(config.output.to_string_lossy(), "/tmp/override.mcap");
        assert_eq!(config.include_suffix_keyexpr, vec!["demo/**".to_string()]);
        std::env::remove_var("AORTA_GROUP");
    }

    #[test]
    fn build_config_uses_aorta_group_env_when_group_not_provided() {
        let _guard = env_lock().lock().expect("lock");
        std::env::set_var("AORTA_GROUP", "env_only_group");

        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/env-group.mcap".to_string());
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert_eq!(config.group, "env_only_group");

        std::env::remove_var("AORTA_GROUP");
    }

    #[test]
    fn storage_defaults_apply_zstd_fast() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert_eq!(config.storage.compression, CompressionKind::Zstd);
        assert_eq!(config.storage.compression_level, ZSTD_LEVEL_FAST);
        assert_eq!(config.storage.chunk_size, Some(DEFAULT_STORAGE_CHUNK_SIZE));
        assert!(config.storage.chunking);
        assert!(config.storage.summary);
    }

    #[test]
    fn storage_overrides_prefer_cli_over_file() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let file = FileConfig {
            storage: Some(StorageSection {
                compression: Some("zstd".to_string()),
                compression_level: Some("best".to_string()),
                chunk_size: Some(1024),
                chunking: Some(true),
                summary: Some(true),
                rotation: None,
            }),
            ..FileConfig::default()
        };
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        overrides.compression = Some(CompressionKind::None);
        overrides.chunk_size = Some(4096);
        overrides.chunking = Some(false);
        overrides.summary = Some(false);
        let config = build_config(Some(file), overrides).expect("config");
        assert_eq!(config.storage.compression, CompressionKind::None);
        assert_eq!(config.storage.chunk_size, Some(4096));
        assert!(!config.storage.chunking);
        assert!(!config.storage.summary);
    }

    #[test]
    fn storage_level_alias_resolves_per_algorithm() {
        assert_eq!(
            parse_compression_level(CompressionKind::Zstd, "fast").unwrap(),
            ZSTD_LEVEL_FAST
        );
        assert_eq!(
            parse_compression_level(CompressionKind::Zstd, "Default").unwrap(),
            ZSTD_LEVEL_DEFAULT
        );
        assert_eq!(
            parse_compression_level(CompressionKind::Zstd, "BEST").unwrap(),
            ZSTD_LEVEL_BEST
        );
        assert_eq!(
            parse_compression_level(CompressionKind::Lz4, "fast").unwrap(),
            LZ4_LEVEL_FAST
        );
        assert_eq!(
            parse_compression_level(CompressionKind::Lz4, "best").unwrap(),
            LZ4_LEVEL_BEST
        );
        assert_eq!(
            parse_compression_level(CompressionKind::None, "ignored").unwrap(),
            0
        );
    }

    #[test]
    fn storage_level_numeric_accepted_in_range() {
        assert_eq!(
            parse_compression_level(CompressionKind::Zstd, "19").unwrap(),
            19
        );
        assert_eq!(
            parse_compression_level(CompressionKind::Lz4, "9").unwrap(),
            9
        );
    }

    #[test]
    fn storage_level_rejects_out_of_range() {
        assert!(parse_compression_level(CompressionKind::Zstd, "23").is_err());
        assert!(parse_compression_level(CompressionKind::Zstd, "0").is_err());
        assert!(parse_compression_level(CompressionKind::Lz4, "13").is_err());
    }

    #[test]
    fn storage_level_rejects_unknown_alias() {
        assert!(parse_compression_level(CompressionKind::Zstd, "turbo").is_err());
        assert!(parse_compression_level(CompressionKind::Lz4, "").is_err());
    }

    #[test]
    fn storage_compression_kind_rejects_unknown() {
        assert!(CompressionKind::from_str("gzip").is_err());
        assert!(CompressionKind::from_str("").is_err());
    }

    #[test]
    fn storage_compression_kind_is_case_insensitive() {
        assert_eq!(
            CompressionKind::from_str(" ZSTD ").unwrap(),
            CompressionKind::Zstd
        );
        assert_eq!(
            CompressionKind::from_str("Lz4").unwrap(),
            CompressionKind::Lz4
        );
        assert_eq!(
            CompressionKind::from_str("none").unwrap(),
            CompressionKind::None
        );
    }

    #[test]
    fn storage_level_default_follows_kind_when_unspecified() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let file = FileConfig {
            storage: Some(StorageSection {
                compression: Some("lz4".to_string()),
                ..StorageSection::default()
            }),
            ..FileConfig::default()
        };
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        let config = build_config(Some(file), overrides).expect("config");
        assert_eq!(config.storage.compression, CompressionKind::Lz4);
        assert_eq!(config.storage.compression_level, LZ4_LEVEL_FAST);
    }

    #[test]
    fn storage_rejects_compression_without_chunking() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        overrides.chunking = Some(false);
        let err = build_config(Some(FileConfig::default()), overrides)
            .expect_err("default zstd + no chunking should fail");
        let msg = format!("{err}");
        assert!(msg.contains("chunking=true"), "unexpected: {msg}");
    }

    #[test]
    fn storage_allows_no_chunking_with_compression_none() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        overrides.compression = Some(CompressionKind::None);
        overrides.chunking = Some(false);
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert_eq!(config.storage.compression, CompressionKind::None);
        assert!(!config.storage.chunking);
    }

    #[test]
    fn storage_level_validated_against_final_kind() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        overrides.compression = Some(CompressionKind::Lz4);
        overrides.compression_level = Some("20".to_string());
        assert!(build_config(Some(FileConfig::default()), overrides).is_err());
    }

    #[test]
    fn build_config_prefers_file_group_over_env() {
        let _guard = env_lock().lock().expect("lock");
        std::env::set_var("AORTA_GROUP", "env_group");

        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/file-group.mcap".to_string());
        let file = FileConfig {
            group: Some("file_group".to_string()),
            ..FileConfig::default()
        };

        let config = build_config(Some(file), overrides).expect("config");
        assert_eq!(config.group, "file_group");

        std::env::remove_var("AORTA_GROUP");
    }

    fn base_overrides() -> ConfigOverrides {
        let mut overrides = ConfigOverrides::default();
        overrides.output = Some("/tmp/test.mcap".to_string());
        overrides
    }

    #[test]
    fn rotation_disabled_by_default() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let config = build_config(Some(FileConfig::default()), base_overrides()).expect("config");
        assert!(!config.rotation.enabled);
    }

    #[test]
    fn rotation_disabled_ignores_invalid_placeholder_values() {
        // A disabled stanza with a 0 split / local timestamp must NOT error —
        // reserved/placeholder values stay inert until rotation is turned on.
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let file = FileConfig {
            storage: Some(StorageSection {
                rotation: Some(super::RotationSection {
                    enabled: Some(false),
                    split_minutes: Some(0),
                    timestamp: Some("local".to_string()),
                    ..super::RotationSection::default()
                }),
                ..StorageSection::default()
            }),
            ..FileConfig::default()
        };
        let config = build_config(Some(file), base_overrides()).expect("config");
        assert!(!config.rotation.enabled);
    }

    #[test]
    fn rotation_enabled_resolves_minutes_and_size_to_bytes() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = base_overrides();
        overrides.rotate_enabled = Some(true);
        overrides.split_minutes = Some(5);
        overrides.rotate_max_file_size_mb = Some(1024);
        overrides.rotate_max_files = Some(10);
        overrides.rotate_max_total_size_mb = Some(8);
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert!(config.rotation.enabled);
        assert_eq!(config.rotation.split, Some(super::Duration::from_secs(300)));
        assert_eq!(
            config.rotation.max_file_size_bytes,
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(config.rotation.max_files, Some(10));
        assert_eq!(config.rotation.max_total_size_bytes, Some(8 * 1024 * 1024));
        assert_eq!(config.rotation.timestamp, super::TimestampZone::Utc);
    }

    #[test]
    fn rotation_split_floor_is_enforced() {
        // split_minutes=1 => 60s, which is exactly the floor.
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = base_overrides();
        overrides.rotate_enabled = Some(true);
        let config = build_config(Some(FileConfig::default()), overrides).expect("config");
        assert_eq!(
            config.rotation.split,
            Some(super::Duration::from_secs(
                super::ROTATION_MIN_SPLIT_SECONDS
            ))
        );
    }

    #[test]
    fn rotation_with_only_size_cap_has_no_time_limit() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = base_overrides();
        overrides.rotate_enabled = Some(true);
        overrides.rotate_max_file_size_mb = Some(2048);

        let config = build_config(Some(FileConfig::default()), overrides).expect("config");

        assert_eq!(config.rotation.split, None);
        assert_eq!(
            config.rotation.max_file_size_bytes,
            Some(2048 * 1024 * 1024)
        );
    }

    #[test]
    fn rotation_enabled_rejects_zero_split_minutes() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = base_overrides();
        overrides.rotate_enabled = Some(true);
        overrides.split_minutes = Some(0);
        let err = build_config(Some(FileConfig::default()), overrides)
            .expect_err("zero split must error when rotation is on");
        assert!(format!("{err}").contains("split_minutes"));
    }

    #[test]
    fn rotation_enabled_rejects_local_timestamp() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let mut overrides = base_overrides();
        overrides.rotate_enabled = Some(true);
        overrides.rotate_timestamp = Some("local".to_string());
        let err = build_config(Some(FileConfig::default()), overrides)
            .expect_err("local timestamp is unsupported in P1");
        assert!(format!("{err}").contains("local"));
    }

    #[test]
    fn rotation_override_beats_file() {
        let _guard = env_lock().lock().expect("lock");
        std::env::remove_var("AORTA_GROUP");
        let file = FileConfig {
            storage: Some(StorageSection {
                rotation: Some(super::RotationSection {
                    enabled: Some(true),
                    split_minutes: Some(2),
                    ..super::RotationSection::default()
                }),
                ..StorageSection::default()
            }),
            ..FileConfig::default()
        };
        let mut overrides = base_overrides();
        overrides.split_minutes = Some(7);
        let config = build_config(Some(file), overrides).expect("config");
        assert!(config.rotation.enabled);
        assert_eq!(config.rotation.split, Some(super::Duration::from_secs(420)));
    }
}
