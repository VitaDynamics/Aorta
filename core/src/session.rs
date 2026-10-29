use crate::error::{AortaError, AortaResult};
use crate::runtime_env;

use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::Mutex as TokioMutex;
use tokio::sync::OnceCell as TokioOnceCell;
use tokio::time::timeout;
use zenoh::config::EndPoint;

use crate::log_prelude::*;

#[derive(Clone, Debug)]
pub struct Session {
    inner: zenoh::Session,
    configured_namespace: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfigSource {
    Env,
    LocalTestDefault,
    ZenohDefault,
    Explicit,
}

impl ConfigSource {
    fn label(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::LocalTestDefault => "local_test_default",
            Self::ZenohDefault => "zenoh_default",
            Self::Explicit => "explicit",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ResolvedSessionConfig {
    pub(crate) config: zenoh::Config,
    pub(crate) source: ConfigSource,
}

fn close_lock() -> &'static TokioMutex<()> {
    static CLOSE_LOCK: OnceLock<TokioMutex<()>> = OnceLock::new();
    CLOSE_LOCK.get_or_init(|| TokioMutex::new(()))
}

const SESSION_CLOSE_TIMEOUT_MS: u64 = 10_000;
const LOCAL_TEST_ROUTER_CONFIG: &str = r#"
{
  mode: "router",
  listen: {
    endpoints: ["tcp/127.0.0.1:0"]
  },
  scouting: {
    multicast: { enabled: false },
    gossip: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
"#;
const LOCAL_TEST_CLIENT_CONFIG: &str = r#"
{
  mode: "client",
  scouting: {
    multicast: { enabled: false },
    gossip: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
"#;

#[derive(Debug)]
struct LocalTestRouter {
    _session: zenoh::Session,
    endpoint: EndPoint,
}

fn should_use_local_test_router() -> bool {
    #[cfg(test)]
    {
        true
    }

    #[cfg(not(test))]
    {
        std::env::var_os("TEST_TMPDIR").is_some()
            || std::env::var_os("AORTA_TEST_LOCAL_ROUTER").is_some()
    }
}

fn local_test_router_cell() -> &'static TokioOnceCell<LocalTestRouter> {
    static ROUTER: TokioOnceCell<LocalTestRouter> = TokioOnceCell::const_new();
    &ROUTER
}

async fn start_local_test_router() -> AortaResult<LocalTestRouter> {
    let config = zenoh::Config::from_json5(LOCAL_TEST_ROUTER_CONFIG).map_err(|err| {
        AortaError::internal(format!("local test router config error: {:?}", err))
    })?;
    let session = zenoh::open(config).await?;

    let started = tokio::time::Instant::now();
    loop {
        let locators = session.info().locators().await;
        if let Some(locator) = locators
            .iter()
            .find(|loc| loc.to_string().starts_with("tcp/127.0.0.1"))
            .or_else(|| {
                locators
                    .iter()
                    .find(|loc| loc.to_string().starts_with("tcp/"))
            })
        {
            return Ok(LocalTestRouter {
                _session: session,
                endpoint: EndPoint::from(locator.clone()),
            });
        }
        if started.elapsed() >= Duration::from_secs(5) {
            return Err(AortaError::timeout(
                "local test router locator not found within 5s".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn local_test_client_config() -> AortaResult<Option<zenoh::Config>> {
    if !should_use_local_test_router() {
        return Ok(None);
    }

    let router = local_test_router_cell()
        .get_or_try_init(start_local_test_router)
        .await?;
    let mut config = zenoh::Config::from_json5(LOCAL_TEST_CLIENT_CONFIG).map_err(|err| {
        AortaError::internal(format!("local test client config error: {:?}", err))
    })?;
    config
        .connect
        .endpoints
        .set(vec![router.endpoint.clone().into()])
        .map_err(|err| {
            AortaError::internal(format!("local test client connect error: {:?}", err))
        })?;
    Ok(Some(config))
}

fn config_json(config: &zenoh::Config, key: &str) -> String {
    config
        .get_json(key)
        .unwrap_or_else(|_| String::from("\"<unavailable>\""))
}

fn parse_configured_namespace_json(config_json: Option<String>) -> Option<String> {
    serde_json::from_str::<Option<String>>(&config_json?)
        .ok()
        .flatten()
        .filter(|namespace| !namespace.is_empty() && namespace != "<unavailable>")
}

fn configured_namespace(config: &zenoh::Config) -> Option<String> {
    parse_configured_namespace_json(config.get_json("namespace").ok())
}

fn env_or_unset(key: &str) -> String {
    runtime_env::var(key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| String::from("<unset>"))
}

pub(crate) fn format_runtime_config_snapshot(
    config: &zenoh::Config,
    source: ConfigSource,
) -> String {
    format!(
        "source={}, zenoh.mode={}, zenoh.connect.endpoints={}, zenoh.listen.endpoints={}, zenoh.scouting.multicast.enabled={}, zenoh.scouting.gossip.enabled={}, zenoh.transport.shared_memory.enabled={}, env.ZENOH_SESSION_CONFIG_URI={}, env.ZENOH_CONFIG={}, aorta.group={}, aorta.log={}, aorta.log_format={}, aorta.log_file={}, aorta.log_file_dir={}, aorta.log_stderr={}, aorta.log_auto_init={}, aorta.log_origin_fields={}, aorta.liveliness={}, aorta.schema_attach={}, aorta.rpc_enforce_single_provider={}, aorta.rpc_multi_reply_check_window_ms={}",
        source.label(),
        config_json(config, "mode"),
        config_json(config, "connect/endpoints"),
        config_json(config, "listen/endpoints"),
        config_json(config, "scouting/multicast/enabled"),
        config_json(config, "scouting/gossip/enabled"),
        config_json(config, "transport/shared_memory/enabled"),
        env_or_unset("ZENOH_SESSION_CONFIG_URI"),
        env_or_unset(zenoh::Config::DEFAULT_CONFIG_PATH_ENV),
        env_or_unset("AORTA_GROUP"),
        env_or_unset("AORTA_LOG"),
        env_or_unset("AORTA_LOG_FORMAT"),
        env_or_unset("AORTA_LOG_FILE"),
        env_or_unset("AORTA_LOG_FILE_DIR"),
        env_or_unset("AORTA_LOG_STDERR"),
        env_or_unset("AORTA_LOG_AUTO_INIT"),
        env_or_unset("AORTA_LOG_ORIGIN_FIELDS"),
        env_or_unset("AORTA_LIVELINESS"),
        env_or_unset("AORTA_SCHEMA_ATTACH"),
        env_or_unset("AORTA_RPC_ENFORCE_SINGLE_PROVIDER"),
        env_or_unset("AORTA_RPC_MULTI_REPLY_CHECK_WINDOW_MS"),
    )
}

pub(crate) async fn resolve_config_from_env_or_local_default() -> AortaResult<ResolvedSessionConfig>
{
    if let Some(config) = Session::config_from_env()? {
        return Ok(ResolvedSessionConfig {
            config,
            source: ConfigSource::Env,
        });
    }

    if let Some(config) = local_test_client_config().await? {
        aorta_core_info!(
            "Aorta INFO: no zenoh config found in {} or {}; fallback to {}",
            Session::SESSION_CONFIG_ENV,
            zenoh::Config::DEFAULT_CONFIG_PATH_ENV,
            ConfigSource::LocalTestDefault.label()
        );
        return Ok(ResolvedSessionConfig {
            config,
            source: ConfigSource::LocalTestDefault,
        });
    }

    // Fail-open path: with zero configuration the
    // session falls back to stock Zenoh peer-mode defaults — multicast
    // scouting enabled, no authentication, no access control — so any host
    // on the local network can peer with this node directly, bypassing any
    // router-level ACLs the deployment relies on. Make that unmistakable.
    aorta_core_warn!(
        "Aorta WARN: no zenoh config found in {} or {}; falling back to stock \
         Zenoh peer defaults (multicast scouting on, no authentication, no \
         access control) — any host on the local network can peer with this \
         node. Set {} to an explicit hardened config for production \
         deployments.",
        Session::SESSION_CONFIG_ENV,
        zenoh::Config::DEFAULT_CONFIG_PATH_ENV,
        Session::SESSION_CONFIG_ENV
    );
    Ok(ResolvedSessionConfig {
        config: zenoh::Config::default(),
        source: ConfigSource::ZenohDefault,
    })
}

impl Session {
    pub const SESSION_CONFIG_ENV: &'static str = "ZENOH_SESSION_CONFIG_URI";

    pub async fn open(config: zenoh::Config) -> AortaResult<Self> {
        Self::open_with_source(config, ConfigSource::Explicit).await
    }

    pub(crate) async fn open_with_source(
        config: zenoh::Config,
        source: ConfigSource,
    ) -> AortaResult<Self> {
        if crate::logging::auto_init_enabled() {
            let _ = crate::logging::init_from_env_once();
        }
        // Tracing process-entry hook (mirrors the logging one above):
        // default-on at T1. `AORTA_TRACE=off` disables
        // recording; `AORTA_TRACE_AUTO_INIT=0` keeps init manual for embedders
        // (call `trace::runtime::init` yourself, before or after this point).
        crate::trace::runtime::auto_init();
        let config_snapshot = format_runtime_config_snapshot(&config, source);
        let configured_namespace = configured_namespace(&config);
        let session = zenoh::open(config).await?;
        aorta_core_info!(
            "Aorta INFO: session created with config snapshot: {}",
            config_snapshot
        );
        Ok(Self {
            inner: session,
            configured_namespace,
        })
    }

    pub async fn open_default() -> AortaResult<Self> {
        let resolved = resolve_config_from_env_or_local_default().await?;
        Self::open_with_source(resolved.config, resolved.source).await
    }

    pub async fn open_from_env_or_default() -> AortaResult<Self> {
        Self::open_default().await
    }

    pub fn config_from_env() -> AortaResult<Option<zenoh::Config>> {
        if let Some(raw) = runtime_env::var(Self::SESSION_CONFIG_ENV) {
            let raw = raw.trim();
            if raw.is_empty() {
                return Ok(None);
            }
            let mut path = raw;
            if let Some(stripped) = raw.strip_prefix("file://") {
                path = stripped;
                if let Some(local) = path.strip_prefix("localhost/") {
                    path = local;
                }
            }
            let config = zenoh::Config::from_file(path)?;
            return Ok(Some(config));
        }

        if runtime_env::var_os(zenoh::Config::DEFAULT_CONFIG_PATH_ENV).is_some() {
            let config = zenoh::Config::from_env()?;
            return Ok(Some(config));
        }

        Ok(None)
    }

    pub async fn config_from_env_or_local_default() -> AortaResult<zenoh::Config> {
        // Fallback logging (INFO for the local-test default, WARN for the
        // insecure stock-Zenoh default) happens inside the resolver so every
        // caller gets it exactly once.
        let resolved = resolve_config_from_env_or_local_default().await?;
        Ok(resolved.config)
    }

    pub async fn close(&self) -> AortaResult<()> {
        let _guard = close_lock().lock().await;
        if self.inner.is_closed() {
            return Ok(());
        }
        match timeout(
            Duration::from_millis(SESSION_CLOSE_TIMEOUT_MS),
            self.inner.close(),
        )
        .await
        {
            Ok(result) => match result {
                Ok(()) => Ok(()),
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("close operation timed out") {
                        aorta_core_warn!("Aorta WARN: session close timed out; proceeding");
                        Ok(())
                    } else {
                        Err(err.into())
                    }
                }
            },
            Err(_) => {
                aorta_core_warn!(
                    "Aorta WARN: session close exceeded {}ms; proceeding",
                    SESSION_CLOSE_TIMEOUT_MS
                );
                Ok(())
            }
        }
    }

    pub async fn close_inner(inner: zenoh::Session) -> AortaResult<()> {
        let _guard = close_lock().lock().await;
        if inner.is_closed() {
            return Ok(());
        }
        match timeout(
            Duration::from_millis(SESSION_CLOSE_TIMEOUT_MS),
            inner.close(),
        )
        .await
        {
            Ok(result) => match result {
                Ok(()) => Ok(()),
                Err(err) => {
                    let msg = err.to_string();
                    if msg.contains("close operation timed out") {
                        aorta_core_warn!("Aorta WARN: inner session close timed out; proceeding");
                        Ok(())
                    } else {
                        Err(err.into())
                    }
                }
            },
            Err(_) => {
                aorta_core_warn!(
                    "Aorta WARN: inner session close exceeded {}ms; proceeding",
                    SESSION_CLOSE_TIMEOUT_MS
                );
                Ok(())
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    pub fn inner(&self) -> &zenoh::Session {
        &self.inner
    }

    pub(crate) fn configured_namespace(&self) -> Option<&str> {
        self.configured_namespace.as_deref()
    }

    pub fn zid(&self) -> String {
        self.inner.zid().to_string()
    }

    pub fn into_inner(self) -> zenoh::Session {
        self.inner
    }

    /// Override the Zenoh session mode in a loaded config.
    ///
    /// Valid modes: `"client"`, `"peer"`, `"router"`.
    /// This is intended for CLI/TUI tools that need to connect in client mode
    /// for faster session startup (avoiding gossip routing table convergence).
    pub fn apply_mode_override(config: &mut zenoh::Config, mode: &str) -> AortaResult<()> {
        match mode {
            "client" | "peer" | "router" => {}
            other => {
                return Err(AortaError::invalid_argument(format!(
                    "invalid Zenoh mode {:?} (expected client|peer|router)",
                    other
                )));
            }
        }
        config
            .insert_json5("mode", &format!("\"{mode}\""))
            .map_err(|err| {
                AortaError::internal(format!(
                    "failed to override zenoh mode to {mode:?}: {err:?}"
                ))
            })?;
        aorta_core_debug!("session mode overridden to {:?}", mode);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../test/session_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../test/session_advanced_tests.rs"]
mod advanced_tests;
