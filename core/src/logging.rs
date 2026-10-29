#[path = "foxglove_log_generated.rs"]
mod foxglove_log_generated;

use crate::error::{AortaError, AortaResult};
use crate::runtime_env;

use flatbuffers::FlatBufferBuilder;
use foxglove_log_generated::aorta::log::common::AortaLogLevel as FbsAortaLogLevel;
use foxglove_log_generated::foxglove::LogLevel as FbsLogLevel;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};

use time::OffsetDateTime;
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::fmt::writer::{BoxMakeWriter, MakeWriterExt};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

const DEFAULT_FILTER: &str = "aorta_core=info,aorta_sdk=info,zenoh=warn";
const ENV_LOG_AUTO_INIT: &str = "AORTA_LOG_AUTO_INIT";
const ENV_NODE_CONTEXT_POLICY: &str = "AORTA_LOG_NODE_CONTEXT_POLICY";
const ENV_LOG_QUEUE_CAPACITY: &str = "AORTA_LOG_QUEUE_CAPACITY";
const ENV_LOG_FILE: &str = "AORTA_LOG_FILE";
const ENV_LOG_FILE_DIR: &str = "AORTA_LOG_FILE_DIR";
const ENV_LOG_ORIGIN_FIELDS: &str = "AORTA_LOG_ORIGIN_FIELDS";
const ENV_LOG_TOPIC_ENABLED: &str = "AORTA_LOG_TOPIC_ENABLED";
const ENV_LOG_TOPIC: &str = "AORTA_LOG_TOPIC";
const ENV_LOG_TOPIC_QUEUE_SIZE: &str = "AORTA_LOG_TOPIC_QUEUE_SIZE";

const DEFAULT_LOG_QUEUE_CAPACITY: usize = 128_000;
const DEFAULT_LOG_TOPIC: &str = "/log";
const DEFAULT_LOG_TOPIC_QUEUE_SIZE: usize = 10_000;
// Cap user-configurable queue size to bound eager allocation:
// LogTopicRecord is ~88 B (u64 + Level + 3 × String header), so 1M ≈ 88 MB.
// Beyond that, AORTA_LOG_TOPIC_QUEUE_SIZE is almost certainly misconfigured.
const MAX_LOG_TOPIC_QUEUE_SIZE: usize = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeContextPolicy {
    First,
    Last,
}

#[derive(Clone, Debug)]
pub struct LogTopicConfig {
    pub enabled: bool,
    pub topic: String,
    pub max_queue_size: usize,
}

impl Default for LogTopicConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            topic: DEFAULT_LOG_TOPIC.to_string(),
            max_queue_size: DEFAULT_LOG_TOPIC_QUEUE_SIZE,
        }
    }
}

impl LogTopicConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(raw) = runtime_env::var(ENV_LOG_TOPIC_ENABLED) {
            cfg.enabled = parse_bool(&raw).unwrap_or(true);
        }
        if let Some(raw) = runtime_env::var(ENV_LOG_TOPIC) {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                cfg.topic = trimmed.to_string();
            }
        }
        if let Some(raw) = runtime_env::var(ENV_LOG_TOPIC_QUEUE_SIZE) {
            if let Ok(val) = raw.trim().parse::<usize>() {
                cfg.max_queue_size = val.clamp(1, MAX_LOG_TOPIC_QUEUE_SIZE);
            }
        }
        cfg
    }
}

#[derive(Clone, Debug)]
pub struct LogConfig {
    pub filter: String,
    pub format: LogFormat,
    pub file_path: Option<PathBuf>,
    pub stderr_enabled: bool,
    pub topic: LogTopicConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            filter: DEFAULT_FILTER.to_string(),
            format: LogFormat::Text,
            file_path: None,
            stderr_enabled: true,
            topic: LogTopicConfig::default(),
        }
    }
}

impl LogConfig {
    pub fn from_env() -> Self {
        Self::from_env_with_node(None)
    }

    pub fn from_env_with_node(node_name: Option<&str>) -> Self {
        let mut cfg = Self::default();
        let filter = runtime_env::var("AORTA_LOG")
            .or_else(|| runtime_env::var("RUST_LOG"))
            .unwrap_or_else(|| DEFAULT_FILTER.to_string());
        if !filter.trim().is_empty() {
            cfg.filter = filter;
        }

        if let Some(raw) = runtime_env::var("AORTA_LOG_FORMAT") {
            cfg.format = parse_format(&raw).unwrap_or(LogFormat::Text);
        }

        cfg.file_path = resolve_log_file_path_from_env(node_name);

        if let Some(raw) = runtime_env::var("AORTA_LOG_STDERR") {
            cfg.stderr_enabled = parse_bool(&raw).unwrap_or(true);
        }

        cfg.topic = LogTopicConfig::from_env();
        cfg
    }
}

fn resolve_log_file_path_from_env(node_name: Option<&str>) -> Option<PathBuf> {
    if let Some(path) = runtime_env::var(ENV_LOG_FILE)
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
    {
        return Some(PathBuf::from(path));
    }

    let dir = runtime_env::var(ENV_LOG_FILE_DIR)
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())?;
    let sanitized_node = sanitize_file_component(node_name.unwrap_or("unknown_node"));
    let timestamp = timestamp_for_filename();
    let pid = std::process::id();
    let file_name = format!("aorta_{}_{}_p{}.log", sanitized_node, timestamp, pid);
    Some(PathBuf::from(dir).join(file_name))
}

fn sanitize_file_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        String::from("unknown_node")
    } else {
        out
    }
}

fn timestamp_for_filename() -> String {
    let now = OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}_{:02}{:02}{:02}",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

#[derive(Clone, Debug)]
struct LogContext {
    node_name: String,
    group: String,
}

#[derive(Debug)]
struct LoggingRuntime {
    _guards: Vec<WorkerGuard>,
}

static LOGGING_RUNTIME: OnceLock<LoggingRuntime> = OnceLock::new();
static LOG_CONTEXT: OnceLock<Mutex<Option<LogContext>>> = OnceLock::new();
static AUTO_INIT_ENABLED: OnceLock<bool> = OnceLock::new();
static NODE_CONTEXT_POLICY: OnceLock<NodeContextPolicy> = OnceLock::new();
static ORIGIN_FIELDS_ENABLED: OnceLock<bool> = OnceLock::new();
static LOG_CONTEXT_VERSION: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub fn init_from_env_once() -> AortaResult<()> {
    init_with_config_once(LogConfig::from_env())
}

pub fn init_from_env_with_node_once(node_name: &str) -> AortaResult<()> {
    init_with_config_once(LogConfig::from_env_with_node(Some(node_name)))
}

pub fn configure_once(config: LogConfig) -> AortaResult<()> {
    init_with_config_once(config)
}

pub fn is_initialized() -> bool {
    LOGGING_RUNTIME.get().is_some()
}

pub fn auto_init_enabled() -> bool {
    *AUTO_INIT_ENABLED.get_or_init(|| match runtime_env::var(ENV_LOG_AUTO_INIT) {
        Some(raw) => parse_bool(&raw).unwrap_or(true),
        None => true,
    })
}

pub fn set_default_context(node_name: &str, group: &str) -> AortaResult<()> {
    let normalized_node = node_name.trim();
    if normalized_node.is_empty() {
        return Err(AortaError::invalid_argument("node_name is empty"));
    }

    let normalized_group = group.trim();
    if normalized_group.is_empty() {
        return Err(AortaError::invalid_argument("group is empty"));
    }

    let mut guard = lock_context();
    let context = LogContext {
        node_name: normalized_node.to_string(),
        group: normalized_group.to_string(),
    };

    match (&*guard, node_context_policy()) {
        (None, _) => {
            *guard = Some(context);
            LOG_CONTEXT_VERSION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        (Some(_), NodeContextPolicy::First) => {}
        (Some(_), NodeContextPolicy::Last) => {
            *guard = Some(context);
            LOG_CONTEXT_VERSION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    Ok(())
}

pub fn current_context() -> Option<(String, String)> {
    let guard = lock_context();
    guard
        .as_ref()
        .cloned()
        .map(|ctx| (ctx.node_name, ctx.group))
}

pub fn log_enabled(level: LogLevel, target: &str) -> bool {
    if !is_initialized() {
        return true;
    }

    let target = normalize_target(target);
    enabled_for_target(target, level)
}

macro_rules! emit_with_target {
    ($level_macro:ident, $target:expr, $message:expr) => {{
        match $target {
            "aorta_core" => tracing::$level_macro!(target: "aorta_core", "{}", $message),
            "aorta_ffi" => tracing::$level_macro!(target: "aorta_ffi", "{}", $message),
            "zenoh" => tracing::$level_macro!(target: "zenoh", "{}", $message),
            _ => tracing::$level_macro!(target: "aorta_sdk", "{}", $message),
        }
    }};
    ($level_macro:ident, $target:expr, $message:expr, $scope:expr, $file:expr, $line:expr) => {{
        match $target {
            "aorta_core" => tracing::$level_macro!(
                target: "aorta_core",
                origin_scope = %$scope,
                origin_file = %$file,
                origin_line = $line,
                "{}",
                $message
            ),
            "aorta_ffi" => tracing::$level_macro!(
                target: "aorta_ffi",
                origin_scope = %$scope,
                origin_file = %$file,
                origin_line = $line,
                "{}",
                $message
            ),
            "zenoh" => tracing::$level_macro!(
                target: "zenoh",
                origin_scope = %$scope,
                origin_file = %$file,
                origin_line = $line,
                "{}",
                $message
            ),
            _ => tracing::$level_macro!(
                target: "aorta_sdk",
                origin_scope = %$scope,
                origin_file = %$file,
                origin_line = $line,
                "{}",
                $message
            ),
        }
    }};
}

pub fn log_at(
    level: LogLevel,
    target: &str,
    scope: Option<&str>,
    file: Option<&str>,
    line: Option<u32>,
    message: &str,
) {
    if !is_initialized() && auto_init_enabled() {
        let _ = init_from_env_once();
    }

    let target = normalize_target(target);
    let message = strip_aorta_level_prefix(message);
    let scope = scope.unwrap_or("");
    let file = file.unwrap_or("");
    let line = line.unwrap_or_default();

    if origin_fields_enabled() {
        match level {
            LogLevel::Trace => emit_with_target!(trace, target, message, scope, file, line),
            LogLevel::Debug => emit_with_target!(debug, target, message, scope, file, line),
            LogLevel::Info => emit_with_target!(info, target, message, scope, file, line),
            LogLevel::Warn => emit_with_target!(warn, target, message, scope, file, line),
            LogLevel::Error => emit_with_target!(error, target, message, scope, file, line),
        }
    } else {
        match level {
            LogLevel::Trace => emit_with_target!(trace, target, message),
            LogLevel::Debug => emit_with_target!(debug, target, message),
            LogLevel::Info => emit_with_target!(info, target, message),
            LogLevel::Warn => emit_with_target!(warn, target, message),
            LogLevel::Error => emit_with_target!(error, target, message),
        }
    }
}

fn strip_aorta_level_prefix(message: &str) -> &str {
    const PREFIXES: [&str; 5] = [
        "Aorta TRACE: ",
        "Aorta DEBUG: ",
        "Aorta INFO: ",
        "Aorta WARN: ",
        "Aorta ERROR: ",
    ];
    for prefix in PREFIXES {
        if let Some(stripped) = message.strip_prefix(prefix) {
            return stripped;
        }
    }
    message
}

pub fn flush() {
    // `tracing_appender::non_blocking` flushes buffered logs when `WorkerGuard` is dropped.
    // Runtime explicit flush is not exposed by the upstream API.
}

fn init_with_config_once(config: LogConfig) -> AortaResult<()> {
    // See `init_once_with`.
    static INIT_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    let result = init_once_with(&LOGGING_RUNTIME, &INIT_LOCK, || install_subscriber(config));
    if result.is_ok() {
        install_panic_hook_once();
    }
    result
}

/// Install a process-wide panic hook that mirrors every panic into the log
/// plane (previously an uncaught panic left only the
/// default Rust message on stderr — no crash marker, no structured record —
/// and even caught FFI-boundary panics never reached the logging pipeline).
///
/// - Installed at most once, only when Aorta logging is initialized (so pure
///   embedders that never init Aorta logging are untouched).
/// - CHAINS the previously-installed hook (default stderr backtrace, or a
///   host application's own hook registered before Aorta init), so existing
///   behavior is preserved on top of the new structured record.
/// - Opt out with `AORTA_PANIC_HOOK=0`.
/// - The tracing emission is wrapped in `catch_unwind`: a panic hook must
///   never panic (that aborts the process).
pub(crate) fn install_panic_hook_once() {
    static PANIC_HOOK_INSTALLED: OnceLock<bool> = OnceLock::new();
    PANIC_HOOK_INSTALLED.get_or_init(|| {
        let enabled = match runtime_env::var("AORTA_PANIC_HOOK") {
            Some(raw) => parse_bool(&raw).unwrap_or(true),
            None => true,
        };
        if !enabled {
            return false;
        }
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let message = if let Some(s) = info.payload().downcast_ref::<&'static str>() {
                    (*s).to_string()
                } else if let Some(s) = info.payload().downcast_ref::<String>() {
                    s.clone()
                } else {
                    "<non-string panic payload>".to_string()
                };
                let location = info
                    .location()
                    .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
                    .unwrap_or_else(|| "<unknown>".to_string());
                let thread = std::thread::current();
                tracing::error!(
                    target: "aorta::panic",
                    thread = thread.name().unwrap_or("<unnamed>"),
                    location = %location,
                    "panic: {}",
                    message
                );
            }));
            previous(info);
        }));
        true
    });
}

// The prior get-then-set pattern raced under
// concurrent first-callers. Both threads passed the
// `get().is_some()` early-return check, both reached the install
// step (which calls `tracing::subscriber::set_global_default`,
// fatal on second invocation), so the second thread saw `Err(Busy)`
// from a function whose contract is supposed to be idempotent.
//
// `OnceLock::get_or_try_init` would be the textbook fix but is
// still nightly (rust-lang/rust#109737), so serialize attempts
// with a parking_lot::Mutex instead — once the lock is held the
// `get().is_some()` check and the `install -> set` sequence run as
// one atomic block. Contention only happens during first-time init;
// steady-state callers take the fast path on the early return.
//
// Generic over `T` so the unit test can drive the pattern with a
// no-side-effect mock instead of the real `install_subscriber`
// (which mutates the process-global tracing subscriber).
fn init_once_with<T>(
    cell: &OnceLock<T>,
    init_lock: &parking_lot::Mutex<()>,
    install: impl FnOnce() -> AortaResult<T>,
) -> AortaResult<()> {
    if cell.get().is_some() {
        return Ok(());
    }
    let _guard = init_lock.lock();
    if cell.get().is_some() {
        return Ok(());
    }

    let value = install()?;
    // We hold init_lock and re-checked emptiness above, so this
    // set can only race against a non-init_lock caller — and there
    // are none by construction. A failure here would indicate an
    // invariant break worth surfacing rather than swallowing.
    cell.set(value)
        .map_err(|_| AortaError::busy("init_once_with: cell concurrently set despite init_lock"))?;
    Ok(())
}

fn context_cell() -> &'static Mutex<Option<LogContext>> {
    LOG_CONTEXT.get_or_init(|| Mutex::new(None))
}

fn lock_context() -> std::sync::MutexGuard<'static, Option<LogContext>> {
    match context_cell().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn node_context_policy() -> NodeContextPolicy {
    *NODE_CONTEXT_POLICY.get_or_init(|| match runtime_env::var(ENV_NODE_CONTEXT_POLICY) {
        Some(raw) => {
            if raw.trim().eq_ignore_ascii_case("last") {
                NodeContextPolicy::Last
            } else {
                NodeContextPolicy::First
            }
        }
        None => NodeContextPolicy::First,
    })
}

fn origin_fields_enabled() -> bool {
    *ORIGIN_FIELDS_ENABLED.get_or_init(|| match runtime_env::var(ENV_LOG_ORIGIN_FIELDS) {
        Some(raw) => parse_on_off(&raw).unwrap_or(false),
        None => false,
    })
}

fn normalize_target(target: &str) -> &'static str {
    match target {
        "aorta_core" => "aorta_core",
        "aorta_ffi" => "aorta_ffi",
        "zenoh" => "zenoh",
        "aorta_sdk" => "aorta_sdk",
        _ => "aorta_sdk",
    }
}

fn enabled_for_target(target: &'static str, level: LogLevel) -> bool {
    match level {
        LogLevel::Trace => match target {
            "aorta_core" => tracing::enabled!(target: "aorta_core", tracing::Level::TRACE),
            "aorta_ffi" => tracing::enabled!(target: "aorta_ffi", tracing::Level::TRACE),
            "zenoh" => tracing::enabled!(target: "zenoh", tracing::Level::TRACE),
            _ => tracing::enabled!(target: "aorta_sdk", tracing::Level::TRACE),
        },
        LogLevel::Debug => match target {
            "aorta_core" => tracing::enabled!(target: "aorta_core", tracing::Level::DEBUG),
            "aorta_ffi" => tracing::enabled!(target: "aorta_ffi", tracing::Level::DEBUG),
            "zenoh" => tracing::enabled!(target: "zenoh", tracing::Level::DEBUG),
            _ => tracing::enabled!(target: "aorta_sdk", tracing::Level::DEBUG),
        },
        LogLevel::Info => match target {
            "aorta_core" => tracing::enabled!(target: "aorta_core", tracing::Level::INFO),
            "aorta_ffi" => tracing::enabled!(target: "aorta_ffi", tracing::Level::INFO),
            "zenoh" => tracing::enabled!(target: "zenoh", tracing::Level::INFO),
            _ => tracing::enabled!(target: "aorta_sdk", tracing::Level::INFO),
        },
        LogLevel::Warn => match target {
            "aorta_core" => tracing::enabled!(target: "aorta_core", tracing::Level::WARN),
            "aorta_ffi" => tracing::enabled!(target: "aorta_ffi", tracing::Level::WARN),
            "zenoh" => tracing::enabled!(target: "zenoh", tracing::Level::WARN),
            _ => tracing::enabled!(target: "aorta_sdk", tracing::Level::WARN),
        },
        LogLevel::Error => match target {
            "aorta_core" => tracing::enabled!(target: "aorta_core", tracing::Level::ERROR),
            "aorta_ffi" => tracing::enabled!(target: "aorta_ffi", tracing::Level::ERROR),
            "zenoh" => tracing::enabled!(target: "zenoh", tracing::Level::ERROR),
            _ => tracing::enabled!(target: "aorta_sdk", tracing::Level::ERROR),
        },
    }
}

fn install_subscriber(config: LogConfig) -> AortaResult<LoggingRuntime> {
    let env_filter = EnvFilter::try_new(config.filter.clone())
        .unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
    let (writer, guard) = build_writer(&config)?;

    let topic_layer = if config.topic.enabled {
        let _ = LOG_TOPIC_BUFFER.set(Mutex::new(VecDeque::with_capacity(
            config.topic.max_queue_size,
        )));
        let _ = LOG_TOPIC_MAX_SIZE.set(config.topic.max_queue_size);
        let _ = LOG_TOPIC_CONFIG.set(config.topic.clone());
        Some(AortaTopicLayer)
    } else {
        None
    };

    match config.format {
        LogFormat::Text => {
            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(topic_layer)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(writer)
                        .with_ansi(false),
                );
            tracing::subscriber::set_global_default(subscriber)
                .map_err(|_| AortaError::busy("global tracing subscriber already initialized"))?;
        }
        LogFormat::Json => {
            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(topic_layer)
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false),
                );
            tracing::subscriber::set_global_default(subscriber)
                .map_err(|_| AortaError::busy("global tracing subscriber already initialized"))?;
        }
    }

    Ok(LoggingRuntime { _guards: guard })
}

fn build_writer(config: &LogConfig) -> AortaResult<(BoxMakeWriter, Vec<WorkerGuard>)> {
    match (&config.file_path, config.stderr_enabled) {
        (None, true) => {
            let (stderr_nb, stderr_guard) =
                open_non_blocking_writer(std::io::stderr(), "aorta-log-stderr");
            Ok((BoxMakeWriter::new(stderr_nb), vec![stderr_guard]))
        }
        (None, false) => Ok((BoxMakeWriter::new(std::io::sink), Vec::new())),
        (Some(path), false) => {
            let (file_nb, file_guard) = open_non_blocking_file(path)?;
            Ok((BoxMakeWriter::new(file_nb), vec![file_guard]))
        }
        (Some(path), true) => {
            let (file_nb, file_guard) = open_non_blocking_file(path)?;
            let (stderr_nb, stderr_guard) =
                open_non_blocking_writer(std::io::stderr(), "aorta-log-stderr");
            Ok((
                BoxMakeWriter::new(file_nb.and(stderr_nb)),
                vec![file_guard, stderr_guard],
            ))
        }
    }
}

fn open_non_blocking_writer<T: Write + Send + 'static>(
    writer: T,
    thread_name: &str,
) -> (NonBlocking, WorkerGuard) {
    NonBlockingBuilder::default()
        .buffered_lines_limit(log_queue_capacity())
        .lossy(false)
        .thread_name(thread_name)
        .finish(writer)
}

fn open_non_blocking_file(path: &PathBuf) -> AortaResult<(NonBlocking, WorkerGuard)> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|err| {
                AortaError::internal(format!(
                    "failed to create log directory '{}': {}",
                    parent.display(),
                    err
                ))
            })?;
        }
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| {
            AortaError::internal(format!(
                "failed to open log file '{}': {}",
                path.display(),
                err
            ))
        })?;
    Ok(open_non_blocking_writer(file, "aorta-log-file"))
}

fn parse_format(raw: &str) -> Option<LogFormat> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "text" => Some(LogFormat::Text),
        "json" => Some(LogFormat::Json),
        _ => None,
    }
}

pub(crate) fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_on_off(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "on" => Some(true),
        "off" => Some(false),
        _ => None,
    }
}

fn log_queue_capacity() -> usize {
    runtime_env::var(ENV_LOG_QUEUE_CAPACITY)
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LOG_QUEUE_CAPACITY)
}

#[derive(Clone, Debug)]
struct LogTopicRecord {
    timestamp_ns: u64,
    level: tracing::Level,
    target: String,
    message: String,
    node: String,
    // `tracing` call-site metadata is `&'static str` / `u32`, so carrying the
    // origin costs no allocation on the hot logging path — the bytes are only
    // copied when the record is serialized on the drain thread.
    file: Option<&'static str>,
    line: Option<u32>,
}

static LOG_TOPIC_BUFFER: OnceLock<Mutex<VecDeque<LogTopicRecord>>> = OnceLock::new();
static LOG_TOPIC_MAX_SIZE: OnceLock<usize> = OnceLock::new();
static LOG_TOPIC_CONFIG: OnceLock<LogTopicConfig> = OnceLock::new();
// 3-state forwarder gate: IDLE → STARTING (claimed, creation in progress)
// → STARTED (drain task running) or back to IDLE on creation failure so the
// next Node can retry.
const FORWARDER_IDLE: u8 = 0;
const FORWARDER_STARTING: u8 = 1;
const FORWARDER_STARTED: u8 = 2;
static LOG_TOPIC_FORWARDER_STATE: AtomicU8 = AtomicU8::new(FORWARDER_IDLE);

// The process-wide slot is leased to one Node, not to a detached process task.
// RAII also releases a reservation if publisher creation is cancelled or panics.
struct ForwarderSlot;

impl Drop for ForwarderSlot {
    fn drop(&mut self) {
        LOG_TOPIC_FORWARDER_STATE.store(FORWARDER_IDLE, Ordering::Release);
    }
}

#[derive(Debug)]
struct ForwarderWorker {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Default)]
struct ForwarderRuntime {
    closed: bool,
    worker: Option<ForwarderWorker>,
}

#[derive(Debug, Default)]
pub(crate) struct LogTopicForwarder {
    runtime: tokio::sync::Mutex<ForwarderRuntime>,
}

impl LogTopicForwarder {
    pub(crate) async fn stop(&self) {
        let mut runtime = self.runtime.lock().await;
        runtime.closed = true;
        if let Some(worker) = runtime.worker.as_mut() {
            if let Some(stop) = worker.stop.take() {
                let _ = stop.send(());
            }
            // Keep the handle in the shared state until joined. Cancelling a
            // close future must not make the next close forget a live worker.
            if let Err(err) = (&mut worker.task).await {
                eprintln!("aorta: log topic forwarder join failed: {err}");
            }
        }
        runtime.worker = None;
    }
}

thread_local! {
    static IN_FORWARD: Cell<bool> = const { Cell::new(false) };
    static CACHED_NODE: RefCell<(usize, String)> = const { RefCell::new((usize::MAX, String::new())) };
}

struct InForwardReset;
impl Drop for InForwardReset {
    fn drop(&mut self) {
        let _ = IN_FORWARD.try_with(|f| f.set(false));
    }
}

struct AortaTopicLayer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AortaTopicLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let _reset = match IN_FORWARD.try_with(|f| f.replace(true)) {
            Ok(true) => return,
            Ok(false) => Some(InForwardReset),
            Err(_) => None,
        };

        let buffer = match LOG_TOPIC_BUFFER.get() {
            Some(b) => b,
            None => return,
        };

        let metadata = event.metadata();
        let target = metadata.target().to_string();
        let level = *metadata.level();

        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        let message = visitor.0;

        let node = {
            let version = LOG_CONTEXT_VERSION.load(std::sync::atomic::Ordering::Relaxed);
            CACHED_NODE
                .try_with(|cache| {
                    let mut c = cache.borrow_mut();
                    if c.0 != version {
                        let (name, _) = current_context().unwrap_or_default();
                        *c = (version, name);
                    }
                    c.1.clone()
                })
                // Logging can outlive main-thread TLS during static teardown.
                // Read the process-global context directly instead of caching.
                .unwrap_or_else(|_| current_context().unwrap_or_default().0)
        };

        let record = LogTopicRecord {
            timestamp_ns: crate::time::unix_time_ns(),
            level,
            target,
            message,
            node,
            file: metadata.file(),
            line: metadata.line(),
        };

        let mut guard = buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let max_size = *LOG_TOPIC_MAX_SIZE
            .get()
            .unwrap_or(&DEFAULT_LOG_TOPIC_QUEUE_SIZE);
        if guard.len() >= max_size {
            guard.pop_front();
        }
        guard.push_back(record);
    }
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" && self.0.is_empty() {
            self.0 = format!("{:?}", value);
        }
    }
}

/// Publisher options for the `/log` forwarder.
///
/// Extracted so the schema-attachment contract below is unit-testable without
/// standing up a Node.
fn log_publisher_options() -> crate::publisher::PublisherOptions {
    let log_qos = crate::qos::QoS::default()
        .with_priority(crate::qos::Priority::Background)
        .with_reliability(crate::qos::Reliability::BestEffort);
    crate::publisher::PublisherOptions {
        qos: log_qos,
        register_telemetry: false,
        congestion_control: Some(crate::qos::CongestionControl::Drop),
        express: false,
        // Provide the BFBS so the node registers a schema_by_key queryable for
        // this topic.  The bytes are embedded as a Rust constant (not
        // include_bytes!) so no Bazel compile_data or release export script
        // changes are required.
        schema_bfbs: Some(foxglove_log_generated::FOXGLOVE_LOG_BFBS.to_vec()),
        // ATTACH the schema hash to every log message.
        //
        // This used to be `Never`, on the premise that "the recorder's by_key
        // fallback is sufficient".  That premise held only while `/log` had
        // exactly one wire shape forever.  It does not survive a schema
        // migration: during a rolling upgrade, nodes on an older aorta_core
        // publish `aorta.sys.LogRecord` onto the same `pub/log` key that new
        // nodes use for `foxglove.Log`.  `schema_by_key` stores ONE schema per
        // key (last writer wins under the default `warn` conflict action), so
        // with no per-message hash a consumer reflects every sample through
        // whichever shape happens to be registered and the other cohort's logs
        // decode to garbage — the legacy-shape handling in
        // `aorta-introspect` never even gets a chance to run.
        //
        // Both consumers prefer the attachment and fall back to by_key
        // (`aorta-record` `SchemaResolver::resolve`, `aorta-introspect`
        // `decode_topic_payload_with_attachment`), and the recorder keys MCAP
        // channels by `(topic, schema_hash)`, so attaching makes the two shapes
        // coexist correctly instead of silently corrupting one of them.
        //
        // Cost: the attachment is built once at publisher creation, so this is
        // 64 bytes of hex hash per message on the wire and no per-message
        // hashing.  On a Background / BestEffort / Drop / non-express publisher
        // that is a good trade for logs that actually decode.  `Always` cannot
        // fail publisher creation here because `schema_bfbs` is always Some
        // (see `publisher.rs`: the only `Always` rejection is a missing BFBS).
        schema_attach_policy: crate::schema::SchemaAttachPolicy::Always,
        ..Default::default()
    }
}

pub async fn start_topic_forwarding(node: &crate::node::Node) -> AortaResult<()> {
    let config = match LOG_TOPIC_CONFIG.get() {
        Some(cfg) if cfg.enabled => cfg.clone(),
        _ => return Ok(()),
    };

    let mut runtime = node.log_topic_forwarder.runtime.lock().await;
    if runtime.closed {
        return Err(AortaError::Cancelled("log topic owner is closed".into()));
    }
    if let Some(worker) = runtime.worker.as_ref() {
        if !worker.task.is_finished() {
            return Ok(());
        }
    }
    runtime.worker = None;

    // Atomically claim the forwarder slot (IDLE→STARTING) before publisher creation.
    // Only one caller can win the CAS; others bail out immediately so we never
    // spawn a second drain task.  If creation fails we reset to IDLE so the
    // next Node can retry rather than permanently disabling forwarding.
    if LOG_TOPIC_FORWARDER_STATE
        .compare_exchange(
            FORWARDER_IDLE,
            FORWARDER_STARTING,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_err()
    {
        return Ok(());
    }
    let slot = ForwarderSlot;

    let publisher = node
        .create_publisher_with_options(&config.topic, log_publisher_options())
        .await?;

    // The reservation stays occupied until the worker releases its publisher.
    LOG_TOPIC_FORWARDER_STATE.store(FORWARDER_STARTED, Ordering::Release);

    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = crate::runtime::spawn_logged_task("log_topic_flush", async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
        let mut consecutive_errors = 0u32;
        // Task-local scratch buffer.  Each tick we swap it with the shared
        // buffer under the lock — an O(1) operation — then drain the scratch
        // outside the lock.  The two deques ping-pong their capacities, so
        // the steady-state working set is reached after a few ticks and no
        // per-tick allocation happens on the hot publisher path.
        let mut scratch: VecDeque<LogTopicRecord> = VecDeque::new();
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => break,
                _ = interval.tick() => {}
            }
            swap_log_records(&mut scratch);
            if scratch.is_empty() {
                continue;
            }

            for record in scratch.drain(..) {
                let payload = match serialize_log_record(&record) {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                };

                if publisher.publish(payload).is_err() {
                    consecutive_errors += 1;
                } else {
                    consecutive_errors = 0;
                }
            }

            // After processing the batch, give up if the publisher has
            // failed persistently.  Records keep arriving into the shared
            // buffer where the drop-oldest policy keeps it bounded.  Reset
            // the forwarder state to IDLE so the next Node init can re-arm
            // the drain task — otherwise a single burst of transport errors
            // would silently disable topic logging for the entire process.
            //
            // Use eprintln! rather than tracing because emitting a log here
            // would recurse back through AortaTopicLayer and (depending on
            // the reentrancy guard's ordering) churn the queue we just gave
            // up on.
            if consecutive_errors >= 100 {
                eprintln!(
                    "aorta: log topic forwarder hit {} consecutive publish errors; \
                     stopping drain task, will retry on next Node init",
                    consecutive_errors
                );
                break;
            }
        }
        drop(publisher);
        drop(slot);
    });
    runtime.worker = Some(ForwarderWorker {
        stop: Some(stop),
        task,
    });

    Ok(())
}

fn swap_log_records(into: &mut VecDeque<LogTopicRecord>) {
    let buffer = match LOG_TOPIC_BUFFER.get() {
        Some(b) => b,
        None => return,
    };
    let mut guard = buffer
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Swap pointers only — the critical section is O(1) regardless of how
    // many records are queued, so producers on the hot logging path are
    // never blocked behind a large drain.
    std::mem::swap(&mut *guard, into);
}

/// Map a `tracing` level onto the pair of levels carried by `foxglove.Log`.
///
/// `foxglove.LogLevel` has no level below `DEBUG`, so `TRACE` collapses onto
/// `DEBUG` in the Foxglove-visible `level` field and keeps its identity in the
/// Aorta-only `aorta_level` field. Widening `foxglove.LogLevel` instead is not
/// an option: the Log panel filters with `level < minLogLevel -> hide`, so a
/// 6th variant would sort above `FATAL`, could never be filtered out, and
/// would render as "?????". See the LEVEL CONTRACT in
/// `message/schemas/topic/foxglove_schema/Log.fbs`.
fn map_level(level: tracing::Level) -> (FbsLogLevel, FbsAortaLogLevel) {
    match level {
        tracing::Level::TRACE => (FbsLogLevel::DEBUG, FbsAortaLogLevel::TRACE),
        tracing::Level::DEBUG => (FbsLogLevel::DEBUG, FbsAortaLogLevel::DEBUG),
        tracing::Level::INFO => (FbsLogLevel::INFO, FbsAortaLogLevel::INFO),
        tracing::Level::WARN => (FbsLogLevel::WARNING, FbsAortaLogLevel::WARN),
        tracing::Level::ERROR => (FbsLogLevel::ERROR, FbsAortaLogLevel::ERROR),
    }
}

fn serialize_log_record(record: &LogTopicRecord) -> AortaResult<Vec<u8>> {
    use foxglove_log_generated::foxglove::{Log as FbsLog, LogArgs, Time as FbsTime};

    let mut builder = FlatBufferBuilder::new();
    let target = builder.create_string(&record.target);
    let message = builder.create_string(&record.message);
    let node = builder.create_string(&record.node);
    let file = record.file.map(|path| builder.create_string(path));

    let (level, aorta_level) = map_level(record.level);

    // `foxglove.Time.sec` is a uint32 Unix-epoch second count, so this is
    // exact until 2106. Same clock as the old `timestamp_ns: ulong` field —
    // only the encoding changes.
    let timestamp = FbsTime::new(
        (record.timestamp_ns / 1_000_000_000) as u32,
        (record.timestamp_ns % 1_000_000_000) as u32,
    );

    let args = LogArgs {
        timestamp: Some(&timestamp),
        level,
        message: Some(message),
        // The Log panel's grouping and its per-node visibility toggles key off
        // `name`, so the Aorta node name belongs here (not in `target`).
        name: Some(node),
        file,
        line: record.line.unwrap_or_default(),
        aorta_header: None,
        aorta_level,
        target: Some(target),
        function: None,
    };

    let log = FbsLog::create(&mut builder, &args);
    builder.finish(log, None);

    Ok(builder.finished_data().to_vec())
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topic_forwarder_cancelled_stop_retains_join_handle() {
        let forwarder = super::LogTopicForwarder::default();
        let witness = std::sync::Arc::new(());
        let weak = std::sync::Arc::downgrade(&witness);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = stopped.await;
            let _ = released.await;
            drop(witness);
        });
        forwarder.runtime.lock().await.worker = Some(super::ForwarderWorker {
            stop: Some(stop),
            task,
        });
        let mut closing = Box::pin(forwarder.stop());
        assert!(std::future::Future::poll(
            closing.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        )
        .is_pending());
        drop(closing);
        assert!(forwarder.runtime.lock().await.worker.is_some());
        assert!(weak.upgrade().is_some());
        release.send(()).unwrap();
        tokio::join!(forwarder.stop(), forwarder.stop());
        assert!(weak.upgrade().is_none());
        assert!(forwarder.runtime.lock().await.worker.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topic_forwarder_close_joins_owner_and_rearms() {
        // Other Core tests keep Nodes alive in process-global fixtures. Use a
        // fresh process so this test really owns the singleton forwarding slot.
        const CHILD_ENV: &str = "AORTA_TEST_LOG_TOPIC_OWNER_CHILD";
        if std::env::var_os(CHILD_ENV).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "logging::tests::topic_forwarder_close_joins_owner_and_rearms",
                    "--nocapture",
                ])
                .env(CHILD_ENV, "1")
                .env("AORTA_LOG_TOPIC_ENABLED", "true")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "forwarder child failed: {}\n{}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        super::LOG_TOPIC_CONFIG.get_or_init(super::LogTopicConfig::default);
        assert_eq!(
            super::LOG_TOPIC_FORWARDER_STATE.load(super::Ordering::Acquire),
            super::FORWARDER_IDLE
        );
        let config = zenoh::Config::from_json5(
            r#"{mode:"peer",listen:{endpoints:["tcp/127.0.0.1:0"]},
                scouting:{multicast:{enabled:false},gossip:{enabled:false}},
                transport:{shared_memory:{enabled:false}}}"#,
        )
        .unwrap();
        let owner =
            crate::node::Node::with_zenoh_config("forwarder_owner", "default", config.clone())
                .await
                .unwrap();
        let other =
            crate::node::Node::with_zenoh_config("forwarder_other", "default", config.clone())
                .await
                .unwrap();
        assert_eq!(
            super::LOG_TOPIC_FORWARDER_STATE.load(super::Ordering::Acquire),
            super::FORWARDER_STARTED
        );
        other.close().await.unwrap();
        assert_eq!(
            super::LOG_TOPIC_FORWARDER_STATE.load(super::Ordering::Acquire),
            super::FORWARDER_STARTED
        );
        owner.clone().close().await.unwrap();
        assert_eq!(
            super::LOG_TOPIC_FORWARDER_STATE.load(super::Ordering::Acquire),
            super::FORWARDER_IDLE
        );
        owner.close().await.unwrap();
        assert!(super::start_topic_forwarding(&owner).await.is_err());
        let next = crate::node::Node::with_zenoh_config("forwarder_next", "default", config)
            .await
            .unwrap();
        assert_eq!(
            super::LOG_TOPIC_FORWARDER_STATE.load(super::Ordering::Acquire),
            super::FORWARDER_STARTED
        );
        next.close().await.unwrap();
        assert_eq!(
            super::LOG_TOPIC_FORWARDER_STATE.load(super::Ordering::Acquire),
            super::FORWARDER_IDLE
        );
    }

    use super::*;
    use crate::test_support::{EnvVarGuard, ENV_LOCK};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tracing_subscriber::fmt::writer::MakeWriter;

    fn unique_temp_log_path(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{}_{}_{}.log", name, std::process::id(), id))
    }

    // `init_once_with` must run the install closure at
    // most once across N concurrent first-callers, even if every
    // thread observes empty state simultaneously. The prior
    // get-then-set pattern ran the closure once per thread that
    // raced past the check, causing the second thread's `set` to
    // fail silently (and any side effects of `install_subscriber`
    // to repeat). Tested with a no-side-effect counter mock so the
    // real LOGGING_RUNTIME / global tracing subscriber stay
    // untouched by this test.
    #[test]
    fn init_once_with_runs_install_at_most_once_under_contention() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;
        use std::thread;

        const THREADS: usize = 16;

        let cell: Arc<OnceLock<usize>> = Arc::new(OnceLock::new());
        let lock: Arc<parking_lot::Mutex<()>> = Arc::new(parking_lot::Mutex::new(()));
        let install_calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let cell = Arc::clone(&cell);
                let lock = Arc::clone(&lock);
                let install_calls = Arc::clone(&install_calls);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    init_once_with(&cell, &lock, || {
                        install_calls.fetch_add(1, Ordering::SeqCst);
                        // Sleep so the contention window stays open
                        // long enough for racing threads to pile up
                        // behind the lock — without it the install
                        // could finish before a competing thread
                        // even acquires the lock, hiding the bug.
                        std::thread::sleep(std::time::Duration::from_millis(10));
                        Ok(42usize)
                    })
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap().expect("init_once_with must succeed");
        }

        assert_eq!(
            install_calls.load(Ordering::SeqCst),
            1,
            "install closure ran more than once under contention"
        );
        assert_eq!(cell.get(), Some(&42));
    }

    #[test]
    fn init_once_with_propagates_install_error_and_allows_retry() {
        use std::sync::atomic::AtomicUsize;

        let cell: OnceLock<usize> = OnceLock::new();
        let lock = parking_lot::Mutex::new(());
        let calls = AtomicUsize::new(0);

        // First attempt: install returns Err — cell stays empty,
        // error propagates to caller (the contract that was broken
        // by the prior racy implementation, which would have left
        // cell-populated state inconsistent with returned status).
        let first = init_once_with(&cell, &lock, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(AortaError::internal("simulated install failure"))
        });
        assert!(first.is_err());
        assert!(cell.get().is_none());

        // Retry should re-enter the install closure (it didn't
        // succeed last time) and this time succeed.
        let second = init_once_with(&cell, &lock, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(7usize)
        });
        assert!(second.is_ok());
        assert_eq!(cell.get(), Some(&7));
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // Third call: cell is populated, install must not run.
        let third = init_once_with(&cell, &lock, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(999usize)
        });
        assert!(third.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cell.get(), Some(&7));
    }

    #[test]
    fn build_writer_without_file_keeps_one_guard() {
        let cfg = LogConfig {
            file_path: None,
            stderr_enabled: true,
            ..LogConfig::default()
        };
        let (_writer, guards) = build_writer(&cfg).expect("writer");
        assert_eq!(guards.len(), 1);
    }

    #[test]
    fn build_writer_file_only_keeps_one_guard() {
        let path = unique_temp_log_path("aorta_log_file_only");
        let cfg = LogConfig {
            file_path: Some(path.clone()),
            stderr_enabled: false,
            ..LogConfig::default()
        };
        let (_writer, guards) = build_writer(&cfg).expect("writer");
        assert_eq!(guards.len(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn build_writer_file_with_stderr_enabled_keeps_two_guards() {
        let path = unique_temp_log_path("aorta_log_file_and_stderr");
        let cfg = LogConfig {
            file_path: Some(path.clone()),
            stderr_enabled: true,
            ..LogConfig::default()
        };
        let (_writer, guards) = build_writer(&cfg).expect("writer");
        assert_eq!(guards.len(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn build_writer_file_with_stderr_enabled_writes_to_file() {
        let path = unique_temp_log_path("aorta_log_file_and_stderr_write");
        let cfg = LogConfig {
            file_path: Some(path.clone()),
            stderr_enabled: true,
            ..LogConfig::default()
        };
        let (writer, guards) = build_writer(&cfg).expect("writer");

        {
            let mut out = writer.make_writer();
            writeln!(out, "aorta-log-file-and-stderr-test-line").expect("write");
        }

        drop(guards);
        let content = std::fs::read_to_string(&path).expect("read log file");
        assert!(
            content.contains("aorta-log-file-and-stderr-test-line"),
            "expected combined writer to keep file sink writable"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn build_writer_sink_only_keeps_no_guard() {
        let cfg = LogConfig {
            file_path: None,
            stderr_enabled: false,
            ..LogConfig::default()
        };
        let (_writer, guards) = build_writer(&cfg).expect("writer");
        assert_eq!(guards.len(), 0);
    }

    #[test]
    fn log_queue_capacity_reads_env() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = EnvVarGuard::set(ENV_LOG_QUEUE_CAPACITY, "777");
        assert_eq!(log_queue_capacity(), 777);
    }

    #[test]
    fn log_queue_capacity_falls_back_when_invalid() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _env_guard = EnvVarGuard::set(ENV_LOG_QUEUE_CAPACITY, "invalid");
        assert_eq!(log_queue_capacity(), DEFAULT_LOG_QUEUE_CAPACITY);
    }

    #[test]
    fn log_file_env_takes_priority_over_log_file_dir() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let explicit_path = unique_temp_log_path("aorta_log_explicit");
        let temp_dir = std::env::temp_dir().join("aorta_log_dir_ignored");
        let _file_guard = EnvVarGuard::set(ENV_LOG_FILE, explicit_path.to_string_lossy().as_ref());
        let _dir_guard = EnvVarGuard::set(ENV_LOG_FILE_DIR, temp_dir.to_string_lossy().as_ref());

        let cfg = LogConfig::from_env_with_node(Some("demo_node"));
        assert_eq!(cfg.file_path, Some(explicit_path));
    }

    #[test]
    fn log_file_dir_generates_file_name_with_aorta_node_and_timestamp() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let temp_dir = std::env::temp_dir().join("aorta_log_dir_test");
        std::fs::create_dir_all(&temp_dir).expect("create dir");
        let _file_guard = EnvVarGuard::remove(ENV_LOG_FILE);
        let _dir_guard = EnvVarGuard::set(ENV_LOG_FILE_DIR, temp_dir.to_string_lossy().as_ref());

        let cfg = LogConfig::from_env_with_node(Some("talker/node one"));
        let path = cfg.file_path.expect("file path");
        assert_eq!(path.parent(), Some(temp_dir.as_path()));

        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .expect("file name");
        assert!(file_name.starts_with("aorta_talker_node_one_"));
        assert!(file_name.ends_with(".log"));
        let suffix = file_name
            .strip_prefix("aorta_talker_node_one_")
            .and_then(|value| value.strip_suffix(".log"))
            .expect("name suffix");
        let (timestamp, pid) = suffix.split_once("_p").expect("timestamp + pid suffix");
        assert_eq!(timestamp.len(), 15, "expect YYYYMMDD_HHMMSS");
        assert!(timestamp.as_bytes()[8] == b'_');
        assert!(timestamp
            .bytes()
            .enumerate()
            .all(|(idx, b)| idx == 8 || b.is_ascii_digit()));
        assert_eq!(pid, std::process::id().to_string());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn strip_aorta_level_prefix_removes_legacy_prefix() {
        assert_eq!(
            strip_aorta_level_prefix("Aorta WARN: message body"),
            "message body"
        );
        assert_eq!(strip_aorta_level_prefix("plain message"), "plain message");
    }

    #[test]
    fn set_default_context_recovers_from_poisoned_lock() {
        let _ = std::panic::catch_unwind(|| {
            let _guard = context_cell().lock().expect("lock");
            panic!("poison context lock");
        });
        assert!(set_default_context("node_after_poison", "group_after_poison").is_ok());
    }

    #[test]
    fn parse_on_off_accepts_expected_values() {
        assert_eq!(parse_on_off("on"), Some(true));
        assert_eq!(parse_on_off("ON"), Some(true));
        assert_eq!(parse_on_off("off"), Some(false));
        assert_eq!(parse_on_off("OFF"), Some(false));
        assert_eq!(parse_on_off("true"), None);
        assert_eq!(parse_on_off(""), None);
    }

    #[test]
    fn log_topic_config_default_is_enabled() {
        let cfg = LogTopicConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.topic, "/log");
        assert_eq!(cfg.max_queue_size, 10_000);
    }

    #[test]
    fn log_topic_config_from_env_reads_settings() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _enabled = EnvVarGuard::set(ENV_LOG_TOPIC_ENABLED, "true");
        let _topic = EnvVarGuard::set(ENV_LOG_TOPIC, "/custom/log");
        let _queue = EnvVarGuard::set(ENV_LOG_TOPIC_QUEUE_SIZE, "5000");

        let cfg = LogTopicConfig::from_env();
        assert!(cfg.enabled);
        assert_eq!(cfg.topic, "/custom/log");
        assert_eq!(cfg.max_queue_size, 5000);
    }

    #[test]
    fn log_topic_queue_size_is_clamped_to_max() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _enabled = EnvVarGuard::set(ENV_LOG_TOPIC_ENABLED, "true");
        let _queue = EnvVarGuard::set(ENV_LOG_TOPIC_QUEUE_SIZE, "9999999999");

        let cfg = LogTopicConfig::from_env();
        assert_eq!(cfg.max_queue_size, MAX_LOG_TOPIC_QUEUE_SIZE);
    }

    #[test]
    fn log_topic_queue_size_floors_at_one() {
        let _guard = ENV_LOCK.lock().expect("lock");
        let _enabled = EnvVarGuard::set(ENV_LOG_TOPIC_ENABLED, "true");
        let _queue = EnvVarGuard::set(ENV_LOG_TOPIC_QUEUE_SIZE, "0");

        let cfg = LogTopicConfig::from_env();
        assert_eq!(cfg.max_queue_size, 1);
    }

    fn sample_record(level: tracing::Level) -> LogTopicRecord {
        LogTopicRecord {
            timestamp_ns: 1_234_567_890,
            level,
            target: "aorta_core".to_string(),
            message: "hello world".to_string(),
            node: "test_node".to_string(),
            file: Some("core/src/logging.rs"),
            line: Some(42),
        }
    }

    #[test]
    fn serialize_log_record_roundtrip() {
        let bytes = serialize_log_record(&sample_record(tracing::Level::INFO)).expect("serialize");
        assert!(!bytes.is_empty());

        use foxglove_log_generated::foxglove::Log;
        let parsed = flatbuffers::root::<Log>(&bytes).expect("parse");

        // Foxglove carries the stamp as {sec, nsec}, not a ns scalar.
        let timestamp = parsed.timestamp().expect("timestamp");
        assert_eq!(timestamp.sec(), 1);
        assert_eq!(timestamp.nsec(), 234_567_890);

        assert_eq!(parsed.level().0, 2); // foxglove INFO
        assert_eq!(parsed.aorta_level().0, 3); // aorta INFO
        assert_eq!(parsed.message(), Some("hello world"));
        // Node name lands in `name` — the field the Log panel groups/filters on.
        assert_eq!(parsed.name(), Some("test_node"));
        assert_eq!(parsed.target(), Some("aorta_core"));
        assert_eq!(parsed.file(), Some("core/src/logging.rs"));
        assert_eq!(parsed.line(), 42);
    }

    // The `/log` topic is rendered by Foxglove's Log panel, which filters with
    // `level < minLogLevel -> hide` and labels via a 0-5 switch.  A `level`
    // outside 0..=5 can therefore never be filtered out, sorts above FATAL,
    // and renders as "?????".  This test is the guard on that contract — see
    // the LEVEL CONTRACT comment in
    // `message/schemas/topic/foxglove_schema/Log.fbs`.
    #[test]
    fn serialized_level_always_within_foxglove_range() {
        use foxglove_log_generated::foxglove::Log;

        for level in [
            tracing::Level::TRACE,
            tracing::Level::DEBUG,
            tracing::Level::INFO,
            tracing::Level::WARN,
            tracing::Level::ERROR,
        ] {
            let bytes = serialize_log_record(&sample_record(level)).expect("serialize");
            let parsed = flatbuffers::root::<Log>(&bytes).expect("parse");
            assert!(
                parsed.level().0 <= 5,
                "level {} for {:?} is outside Foxglove's 0..=5 range",
                parsed.level().0,
                level
            );
        }
    }

    // TRACE has no Foxglove counterpart, so it must collapse onto DEBUG on the
    // wire while staying distinguishable via `aorta_level` for Aorta tooling.
    #[test]
    fn trace_maps_to_debug_but_stays_distinguishable() {
        use foxglove_log_generated::foxglove::Log;

        let trace = serialize_log_record(&sample_record(tracing::Level::TRACE)).expect("serialize");
        let debug = serialize_log_record(&sample_record(tracing::Level::DEBUG)).expect("serialize");

        let trace = flatbuffers::root::<Log>(&trace).expect("parse");
        let debug = flatbuffers::root::<Log>(&debug).expect("parse");

        assert_eq!(trace.level(), debug.level());
        assert_eq!(trace.level().0, 1); // foxglove DEBUG
        assert_eq!(trace.aorta_level().0, 1); // aorta TRACE
        assert_eq!(debug.aorta_level().0, 2); // aorta DEBUG
    }

    // `/log` carries more than one wire shape during any log-schema migration
    // (older nodes publish the previous root type onto the same key).
    // `schema_by_key` holds ONE schema per key, so without a per-message hash
    // consumers reflect every sample through whichever shape won the race and
    // the other cohort's logs decode to garbage.  Reverting this to `Never`
    // would reintroduce that silently — there is no runtime error to notice.
    #[test]
    fn log_publisher_attaches_schema_hash_per_message() {
        let options = log_publisher_options();
        assert_eq!(
            options.schema_attach_policy,
            crate::schema::SchemaAttachPolicy::Always,
        );
        // `Always` without a BFBS is a publisher-creation error, so the two
        // settings have to stay coupled.
        assert!(
            options.schema_bfbs.is_some(),
            "Always attachment requires schema_bfbs to be present"
        );
    }

    #[test]
    fn log_publisher_excludes_only_internal_telemetry() {
        assert!(crate::publisher::PublisherOptions::default().register_telemetry);
        assert!(!log_publisher_options().register_telemetry);
    }

    // The recorder and the Foxglove bridge both name the MCAP/live channel
    // after the BFBS root type, and the Log panel only dispatches on a known
    // name.  If this drifts, the panel silently falls back to level UNKNOWN
    // and timestamp {0,0} — a failure mode with no error message anywhere.
    #[test]
    fn published_schema_root_type_is_foxglove_log() {
        assert_eq!(
            crate::schema::bfbs_root_type_name(foxglove_log_generated::FOXGLOVE_LOG_BFBS)
                .as_deref(),
            Some("foxglove.Log"),
        );
    }

    #[test]
    fn forwarder_state_machine_supports_retry_after_failure() {
        // Use a local atomic to mirror the state machine semantics that the
        // forwarder relies on for retry-after-failure: a stuck STARTED state
        // would block any subsequent CAS(IDLE→STARTING).
        let state = AtomicU8::new(FORWARDER_IDLE);

        // First claim succeeds.
        assert!(state
            .compare_exchange(
                FORWARDER_IDLE,
                FORWARDER_STARTING,
                Ordering::AcqRel,
                Ordering::Acquire
            )
            .is_ok());

        // Concurrent claim while STARTING fails.
        assert!(state
            .compare_exchange(
                FORWARDER_IDLE,
                FORWARDER_STARTING,
                Ordering::AcqRel,
                Ordering::Acquire
            )
            .is_err());

        // Drain task gives up persistently and resets to IDLE (the fix).
        state.store(FORWARDER_STARTED, Ordering::Release);
        state.store(FORWARDER_IDLE, Ordering::Release);

        // Next Node init can re-arm forwarding.
        assert!(state
            .compare_exchange(
                FORWARDER_IDLE,
                FORWARDER_STARTING,
                Ordering::AcqRel,
                Ordering::Acquire
            )
            .is_ok());
    }

    // Drift check between the committed FOXGLOVE_LOG_BFBS snapshot and the
    // BFBS freshly compiled by build.rs.  Only available under Cargo (Bazel
    // does not execute build scripts and so OUT_DIR is not defined there).
    #[cfg(has_fresh_bfbs)]
    include!(concat!(env!("OUT_DIR"), "/Log_bfbs_fresh.rs"));

    #[cfg(has_fresh_bfbs)]
    #[test]
    fn foxglove_log_bfbs_constant_matches_compiled_schema() {
        // If this fails, re-generate FOXGLOVE_LOG_BFBS in
        // foxglove_log_generated.rs:
        //   flatc --binary --schema --bfbs-builtins \
        //     -I message message/schemas/topic/foxglove_schema/Log.fbs
        assert_eq!(
            foxglove_log_generated::FOXGLOVE_LOG_BFBS,
            FOXGLOVE_LOG_BFBS_FRESH,
            "FOXGLOVE_LOG_BFBS in foxglove_log_generated.rs is stale; \
             re-generate from message/schemas/topic/foxglove_schema/Log.fbs"
        );
    }
}
