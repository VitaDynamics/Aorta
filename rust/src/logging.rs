//! Logging for the Rust SDK.
//!
//! A `tracing`-based unified logging system with five levels
//! (Trace/Debug/Info/Warn/Error), text or JSON output, optional file
//! output, and environment-variable configuration:
//!
//! | Variable | Meaning |
//! |---|---|
//! | `AORTA_LOG` | filter, e.g. `debug` or `aorta_sdk=trace` |
//! | `AORTA_LOG_FILE` | log file path |
//! | `AORTA_LOG_FORMAT` | `text` or `json` |
//!
//! Initialize once via [`init_from_env_once`] (or [`configure_logging`]),
//! then log through the [`crate::log_prelude`] macros
//! (`aorta_info!`, `aorta_warn!`, …). Use [`is_log_enabled`] to guard
//! expensive log-argument construction.

pub use aorta_core::logging::{LogConfig, LogFormat, LogLevel, LogTopicConfig};

use std::sync::atomic::{AtomicU64, Ordering};

use aorta_core::error::AortaResult;

/// Initialize logging from environment variables (`AORTA_LOG`,
/// `AORTA_LOG_FILE`, `AORTA_LOG_FORMAT`). Idempotent — safe to call
/// multiple times; later calls are no-ops once initialized.
pub fn init_from_env_once() -> AortaResult<()> {
    aorta_core::logging::init_from_env_once()
}

/// Programmatically configure logging from a [`LogConfig`]. Idempotent like
/// [`init_from_env_once`]; takes effect on the first call.
pub fn configure_logging(config: LogConfig) -> AortaResult<()> {
    aorta_core::logging::configure_once(config)
}

/// Flush buffered log output.
///
/// Currently a no-op: the underlying `tracing_appender::non_blocking` writer
/// only flushes when its `WorkerGuard` is dropped. Call it for compatibility;
/// for a guaranteed flush, ensure the logging guard is dropped (e.g. at
/// process exit).
pub fn flush_logging() {
    aorta_core::logging::flush();
}

/// Whether logging has been initialized yet.
pub fn is_logging_initialized() -> bool {
    aorta_core::logging::is_initialized()
}

/// Set the default log context (`node_name`, `group`) prepended to
/// subsequent log records.
///
/// May be called before logging is initialized (the context is stored and
/// used once the subscriber is installed). Returns `InvalidArgument` only
/// for empty `node_name` / `group` inputs.
pub fn set_default_context(node_name: &str, group: &str) -> AortaResult<()> {
    aorta_core::logging::set_default_context(node_name, group)
}

/// Check whether a message at `level` under `scope` would be emitted — use
/// to guard expensive formatting:
///
/// ```
/// use aorta_sdk::{is_log_enabled, LogLevel};
///
/// if is_log_enabled(LogLevel::Debug, "aorta_sdk") {
///     // build and log an expensive debug string
/// }
/// ```
pub fn is_log_enabled(level: LogLevel, scope: &str) -> bool {
    aorta_core::logging::log_enabled(level, sdk_target_for_scope(scope))
}

/// Emit a log record at `level` under `scope` with explicit source
/// location. Prefer the `aorta_log*!` macros over calling this directly.
pub fn log_at(level: LogLevel, scope: &str, file: &'static str, line: u32, message: String) {
    aorta_core::logging::log_at(
        level,
        sdk_target_for_scope(scope),
        Some(scope),
        Some(file),
        Some(line),
        &message,
    );
}

fn sdk_target_for_scope(_scope: &str) -> &'static str {
    "aorta_sdk"
}

/// Rate limiter helper backing [`crate::aorta_log_every_n`]: returns `true`
/// on the **first** call and every `n` calls thereafter (i.e. calls
/// 1, n+1, 2n+1, …); always `true` when `n == 0`.
pub fn should_log_every_n(counter: &AtomicU64, n: u64) -> bool {
    if n == 0 {
        return true;
    }
    let current = counter.fetch_add(1, Ordering::Relaxed) + 1;
    (current - 1).is_multiple_of(n)
}

/// Log at an explicit level: `aorta_log!(LogLevel::Warn, "msg {}", x)`.
#[macro_export]
macro_rules! aorta_log {
    ($level:expr, $($arg:tt)+) => {{
        const __AORTA_SCOPE: &str = "aorta_sdk";
        if $crate::logging::is_log_enabled($level, __AORTA_SCOPE) {
            $crate::logging::log_at(
                $level,
                __AORTA_SCOPE,
                file!(),
                line!(),
                format!($($arg)+),
            );
        }
    }};
}

/// Log at Trace level.
#[macro_export]
macro_rules! aorta_trace {
    ($($arg:tt)+) => {
        $crate::aorta_log!($crate::logging::LogLevel::Trace, $($arg)+)
    };
}

/// Log at Debug level.
#[macro_export]
macro_rules! aorta_debug {
    ($($arg:tt)+) => {
        $crate::aorta_log!($crate::logging::LogLevel::Debug, $($arg)+)
    };
}

/// Log at Info level.
#[macro_export]
macro_rules! aorta_info {
    ($($arg:tt)+) => {
        $crate::aorta_log!($crate::logging::LogLevel::Info, $($arg)+)
    };
}

/// Log at Warn level.
#[macro_export]
macro_rules! aorta_warn {
    ($($arg:tt)+) => {
        $crate::aorta_log!($crate::logging::LogLevel::Warn, $($arg)+)
    };
}

/// Log at Error level.
#[macro_export]
macro_rules! aorta_error {
    ($($arg:tt)+) => {
        $crate::aorta_log!($crate::logging::LogLevel::Error, $($arg)+)
    };
}

/// Log only when `condition` is true: `aorta_log_if!(LogLevel::Warn, count > 100, "…")`.
#[macro_export]
macro_rules! aorta_log_if {
    ($level:expr, $condition:expr, $($arg:tt)+) => {{
        if $condition {
            $crate::aorta_log!($level, $($arg)+);
        }
    }};
}

/// Log at most every `n`-th invocation (per call site):
/// `aorta_log_every_n!(LogLevel::Info, 100, "status …")`.
#[macro_export]
macro_rules! aorta_log_every_n {
    ($level:expr, $n:expr, $($arg:tt)+) => {{
        static __AORTA_LOG_EVERY_N_COUNTER: ::std::sync::atomic::AtomicU64 =
            ::std::sync::atomic::AtomicU64::new(0);
        if $crate::logging::should_log_every_n(
            &__AORTA_LOG_EVERY_N_COUNTER,
            ($n) as u64,
        ) {
            $crate::aorta_log!($level, $($arg)+);
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::sdk_target_for_scope;

    #[test]
    fn sdk_target_for_scope_is_always_aorta_sdk() {
        assert_eq!(sdk_target_for_scope("aorta_sdk"), "aorta_sdk");
        assert_eq!(sdk_target_for_scope("zenoh"), "aorta_sdk");
        assert_eq!(sdk_target_for_scope("aorta_core"), "aorta_sdk");
        assert_eq!(sdk_target_for_scope("custom.scope"), "aorta_sdk");
    }
}
