use super::*;
use crate::test_support::{allow_zenoh_env_guard, ENV_LOCK};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn write_temp_config(content: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let name = format!("aorta_zenoh_config_{}_{}.json5", std::process::id(), nanos);
    path.push(name);
    fs::write(&path, content).expect("write config");
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_open_and_close() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session = Session::open_default().await;
    assert!(session.is_ok());

    let session = session.unwrap();
    assert!(!session.is_closed());

    // Close should succeed
    session.close().await.expect("close session");
    assert!(session.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_open_default() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session = Session::open_default().await;
    assert!(session.is_ok());

    let session = session.unwrap();
    session.close().await.expect("close session");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_open_from_env_or_default_no_env() {
    let _guard = ENV_LOCK.lock().expect("lock");

    // Clear environment variables
    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let session = Session::open_from_env_or_default().await;
    assert!(session.is_ok());

    let session = session.unwrap();
    session.close().await.expect("close session");
}

#[test]
fn session_config_from_env_empty_value() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();

    std::env::set_var(Session::SESSION_CONFIG_ENV, "");
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let config = Session::config_from_env();
    assert!(config.is_ok());
    assert!(config.unwrap().is_none());

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
}

#[test]
fn session_config_from_env_whitespace_value() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();

    std::env::set_var(Session::SESSION_CONFIG_ENV, "   ");
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let config = Session::config_from_env();
    assert!(config.is_ok());
    assert!(config.unwrap().is_none());

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
}

#[test]
fn session_config_from_env_invalid_file() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();

    std::env::set_var(
        Session::SESSION_CONFIG_ENV,
        "/nonexistent/path/config.json5",
    );
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let config = Session::config_from_env();
    // Should return an error for invalid file
    assert!(config.is_err());

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
}

#[test]
fn session_config_from_env_file_uri_with_localhost() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let path = write_temp_config("{}");

    let uri = format!("file://localhost/{}", path.to_string_lossy());
    std::env::set_var(Session::SESSION_CONFIG_ENV, &uri);
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let config = Session::config_from_env();
    assert!(config.is_ok());
    assert!(config.unwrap().is_some());

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    let _ = fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_clone_shares_inner() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session1 = Session::open_default().await.expect("open session");
    let session2 = session1.clone();

    // Both should report same closed status
    assert_eq!(session1.is_closed(), session2.is_closed());

    session1.close().await.expect("close session");

    // After closing one, both should be closed (shared state)
    assert!(session1.is_closed());
    assert!(session2.is_closed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_inner_accessor() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session = Session::open_default().await.expect("open session");

    // Should be able to access inner zenoh session
    let inner = session.inner();
    assert!(!inner.is_closed());

    session.close().await.expect("close session");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_into_inner() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session = Session::open_default().await.expect("open session");

    // Consume session to get inner zenoh session
    let inner = session.into_inner();

    // Can use inner directly
    Session::close_inner(inner).await.expect("close inner");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_close_idempotent() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session = Session::open_default().await.expect("open session");

    // Multiple closes should not panic
    session.close().await.expect("first close");

    // Second close may fail but should not panic
    let _ = session.close().await;

    assert!(session.is_closed());
}

#[test]
fn session_constants() {
    assert_eq!(Session::SESSION_CONFIG_ENV, "ZENOH_SESSION_CONFIG_URI");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_debug_format() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let session = Session::open_default().await.expect("open session");

    let debug = format!("{:?}", session);
    assert!(debug.contains("Session"));

    session.close().await.expect("close session");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_with_custom_config() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let config = Session::config_from_env_or_local_default()
        .await
        .expect("config");

    let session = Session::open(config).await;
    assert!(session.is_ok());

    let session = session.unwrap();
    session.close().await.expect("close session");
}

#[test]
fn session_config_env_priority() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();

    let path1 = write_temp_config("{}");
    let path2 = write_temp_config("{}");

    // Set both env vars - SESSION_CONFIG_ENV should take priority
    std::env::set_var(Session::SESSION_CONFIG_ENV, &path1);
    std::env::set_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV, &path2);

    let config = Session::config_from_env().expect("config");
    assert!(config.is_some());

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);
    let _ = fs::remove_file(path1);
    let _ = fs::remove_file(path2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_lifecycle_full() {
    let _guard = ENV_LOCK.lock().expect("lock");
    // Test complete session lifecycle
    let session = Session::open_default().await.expect("open");
    assert!(!session.is_closed());

    // Clone and verify both work
    let session2 = session.clone();
    assert!(!session2.is_closed());

    // Close original
    session.close().await.expect("close");
    assert!(session.is_closed());
    assert!(session2.is_closed());
}

// Suppress unused variable warnings for test data
#[allow(unused_variables)]
fn _suppress_warnings() {
    let _ = Session::open_default();
}
