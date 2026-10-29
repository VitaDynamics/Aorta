use crate::logging::{flush_logging, is_log_enabled, log_at, should_log_every_n, LogLevel};
use std::sync::atomic::AtomicU64;

#[test]
fn should_log_every_n_with_zero_always_true() {
    let counter = AtomicU64::new(0);
    assert!(should_log_every_n(&counter, 0));
    assert!(should_log_every_n(&counter, 0));
}

#[test]
fn should_log_every_n_uses_expected_sequence() {
    let counter = AtomicU64::new(0);
    assert!(should_log_every_n(&counter, 2));
    assert!(!should_log_every_n(&counter, 2));
    assert!(should_log_every_n(&counter, 2));
    assert!(!should_log_every_n(&counter, 2));
}

#[test]
fn flush_logging_is_callable() {
    flush_logging();
}

#[test]
fn sdk_logging_api_accepts_non_sdk_scope_inputs() {
    assert!(is_log_enabled(LogLevel::Info, "zenoh"));
    log_at(
        LogLevel::Info,
        "zenoh",
        file!(),
        line!(),
        "sdk log with non-sdk scope".to_string(),
    );
}
