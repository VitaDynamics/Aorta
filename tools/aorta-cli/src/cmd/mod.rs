pub mod action;
pub mod clean;
pub mod ctx;
pub mod interface_directory;
pub mod nodes;
pub mod schema;
pub mod service;
pub mod stats;
pub mod telemetry;
pub mod topic;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use aorta_introspect::client::{IntrospectClient, IntrospectOptions, NodeSelector};
use aorta_introspect::ids::{GroupName, NodeId, NodeName};

static CLIENT_MODE_OVERRIDE: AtomicBool = AtomicBool::new(true);

/// Set whether `open_session` should override the Zenoh mode to client.
/// Called once from `main()` based on the `--no-client-override` flag.
pub fn set_client_mode_override(enabled: bool) {
    CLIENT_MODE_OVERRIDE.store(enabled, Ordering::Relaxed);
}

/// Apply the `--no-client-override` policy to `config`.
///
/// Both ordinary commands and the standalone interface-directory server use
/// the same policy; discovery queries reuse the command session.
pub fn apply_client_mode_override(config: &mut zenoh::Config, enabled: bool) -> anyhow::Result<()> {
    if enabled {
        aorta_core::Session::apply_mode_override(config, "client")
            .map_err(|e| anyhow::anyhow!("Failed to apply client mode override: {}", e))?;
    }
    Ok(())
}

/// Whether `--no-client-override` was absent, i.e. sessions should be forced to
/// `client` mode.
pub fn client_mode_override_enabled() -> bool {
    CLIENT_MODE_OVERRIDE.load(Ordering::Relaxed)
}

pub async fn open_session() -> anyhow::Result<zenoh::Session> {
    let config = aorta_core::Session::config_from_env_or_local_default()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to build Zenoh config: {}", e))?;
    open_session_with_config(config).await
}

pub async fn open_session_with_config(mut config: zenoh::Config) -> anyhow::Result<zenoh::Session> {
    apply_client_mode_override(&mut config, client_mode_override_enabled())?;
    zenoh::open(config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Zenoh session: {}", e))
}

pub fn build_introspect_client(
    session: Arc<zenoh::Session>,
    group: &str,
) -> anyhow::Result<IntrospectClient> {
    let group = GroupName::new(group)
        .map_err(|err| anyhow::anyhow!("Invalid group '{}': {}", group, err))?;
    Ok(IntrospectClient::from_session(
        session,
        IntrospectOptions::new(group),
    ))
}

pub fn parse_node_selector(node_query: &str) -> anyhow::Result<NodeSelector> {
    let query = node_query.trim();
    if query.is_empty() {
        return Err(anyhow::anyhow!("Node name/id cannot be empty"));
    }

    if let Ok(node_id) = NodeId::new(query) {
        return Ok(NodeSelector::ById(node_id));
    }

    let node_name = NodeName::new(query)
        .map_err(|err| anyhow::anyhow!("Invalid node '{}': {}", node_query, err))?;
    Ok(NodeSelector::ByName(node_name))
}
#[cfg(test)]
mod tests {
    use super::apply_client_mode_override;

    fn mode_of(config: &zenoh::Config) -> String {
        config.get_json("mode").expect("mode")
    }

    #[test]
    fn client_mode_override_is_skipped_when_disabled() {
        let mut config = zenoh::Config::default();
        config.insert_json5("mode", "\"peer\"").unwrap();
        apply_client_mode_override(&mut config, false).unwrap();
        assert_eq!(mode_of(&config), "\"peer\"");

        apply_client_mode_override(&mut config, true).unwrap();
        assert_eq!(mode_of(&config), "\"client\"");
    }
}
