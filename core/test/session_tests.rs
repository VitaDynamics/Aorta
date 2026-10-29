use super::{
    format_runtime_config_snapshot, parse_configured_namespace_json, ConfigSource, Session,
};
use crate::test_support::{allow_zenoh_env_guard, EnvVarGuard, ENV_LOCK};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn write_temp_config_with(content: &str) -> std::path::PathBuf {
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

fn write_temp_config() -> std::path::PathBuf {
    write_temp_config_with("{}")
}

#[test]
fn configured_namespace_json_parsing_handles_present_null_and_lookup_failure() {
    assert_eq!(
        parse_configured_namespace_json(Some(String::from("\"R1\""))),
        Some(String::from("R1"))
    );
    assert_eq!(
        parse_configured_namespace_json(Some(String::from("null"))),
        None
    );
    assert_eq!(parse_configured_namespace_json(None), None);
    assert_eq!(
        parse_configured_namespace_json(Some(String::from("\"<unavailable>\""))),
        None
    );
}

#[test]
fn config_from_env_reads_session_config_uri() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let path = write_temp_config();
    std::env::set_var(Session::SESSION_CONFIG_ENV, path.to_string_lossy().as_ref());
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let config = Session::config_from_env().expect("config");
    assert!(config.is_some());

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    let _ = fs::remove_file(path);
}

#[test]
fn config_from_env_accepts_file_uri() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let path = write_temp_config();
    let uri = format!("file://{}", path.to_string_lossy());
    std::env::set_var(Session::SESSION_CONFIG_ENV, &uri);
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let config = Session::config_from_env().expect("config");
    assert!(config.is_some());

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    let _ = fs::remove_file(path);
}

#[test]
fn config_from_env_falls_back_to_zenoh_config_env() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let path = write_temp_config();
    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    std::env::set_var(
        zenoh::Config::DEFAULT_CONFIG_PATH_ENV,
        path.to_string_lossy().as_ref(),
    );

    let config = Session::config_from_env().expect("config");
    assert!(config.is_some());

    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);
    let _ = fs::remove_file(path);
}

#[test]
fn runtime_config_snapshot_includes_zenoh_and_aorta_fields() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let _group_guard = EnvVarGuard::set("AORTA_GROUP", "fleet_test");
    let config = zenoh::Config::from_json5(
        r#"
{
  mode: "client",
  connect: {
    endpoints: ["tcp/127.0.0.1:7447"]
  },
  scouting: {
    multicast: { enabled: false },
    gossip: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
    )
    .expect("config");

    let snapshot = format_runtime_config_snapshot(&config, ConfigSource::Explicit);

    assert!(snapshot.contains("source=explicit"));
    assert!(snapshot.contains("zenoh.mode="));
    assert!(snapshot.contains("zenoh.connect.endpoints="));
    assert!(snapshot.contains("zenoh.scouting.multicast.enabled="));
    assert!(snapshot.contains("zenoh.transport.shared_memory.enabled="));
    assert!(snapshot.contains("aorta.group=fleet_test"));
    assert!(snapshot.contains("aorta.log_file_dir="));
    assert!(snapshot.contains("env.ZENOH_SESSION_CONFIG_URI="));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_from_env_or_local_default_prefers_session_config_env() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let path = write_temp_config_with(
        r#"
{
  mode: "router",
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
    );
    std::env::set_var(Session::SESSION_CONFIG_ENV, path.to_string_lossy().as_ref());
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let config = Session::config_from_env_or_local_default()
        .await
        .expect("config");
    let mode = config.get_json("mode").expect("mode");
    assert!(
        mode.contains("\"router\""),
        "expected env config mode router, got: {}",
        mode
    );

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    let _ = fs::remove_file(path);
}

// Config provenance must be reported
// truthfully. The resolver returns the real source, and the snapshot label
// for the insecure stock-Zenoh fallback must say so (the WARN log path keys
// off the same ConfigSource::ZenohDefault branch in the resolver).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolved_config_source_is_env_when_session_config_env_set() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let path = write_temp_config();
    std::env::set_var(Session::SESSION_CONFIG_ENV, path.to_string_lossy().as_ref());
    std::env::remove_var(zenoh::Config::DEFAULT_CONFIG_PATH_ENV);

    let resolved = super::resolve_config_from_env_or_local_default()
        .await
        .expect("resolved config");
    assert_eq!(resolved.source, ConfigSource::Env);

    std::env::remove_var(Session::SESSION_CONFIG_ENV);
    let _ = fs::remove_file(path);
}

#[test]
fn snapshot_labels_zenoh_default_fallback() {
    let _guard = ENV_LOCK.lock().expect("lock");
    let _allow_guard = allow_zenoh_env_guard();
    let snapshot =
        format_runtime_config_snapshot(&zenoh::Config::default(), ConfigSource::ZenohDefault);
    assert!(
        snapshot.contains("source=zenoh_default"),
        "fallback snapshot must not claim an explicit source: {}",
        snapshot
    );
}

#[test]
fn apply_mode_override_changes_peer_to_client() {
    let mut config = zenoh::Config::from_json5(r#"{ mode: "peer" }"#).expect("config");
    Session::apply_mode_override(&mut config, "client").expect("override");
    let mode = config.get_json("mode").expect("mode");
    assert!(
        mode.contains("\"client\""),
        "expected client mode, got: {}",
        mode
    );
}

#[test]
fn apply_mode_override_rejects_invalid_mode() {
    let mut config = zenoh::Config::from_json5(r#"{ mode: "peer" }"#).expect("config");
    let result = Session::apply_mode_override(&mut config, "invalid");
    assert!(result.is_err(), "expected error for invalid mode");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("invalid Zenoh mode"),
        "error should mention invalid mode, got: {}",
        err_msg
    );
}
