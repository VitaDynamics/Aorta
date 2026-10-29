use std::sync::{Mutex, MutexGuard};

use crate::session::Session;

pub struct EnvLock(Mutex<()>);

pub static ENV_LOCK: EnvLock = EnvLock(Mutex::new(()));

pub struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

pub struct EnvVarGuards {
    _guards: Vec<EnvVarGuard>,
}

// Field order matters: struct fields drop in declaration order, so `_guards`
// must come before `_lock` — env vars are restored while ENV_LOCK is still
// held, otherwise a waiting test could grab the lock and start observing env
// state mid-restoration (notably `AORTA_TEST_ALLOW_ENV`, which gates
// `runtime_env::var` reads in test mode).
pub struct TestEnvGuard<'a> {
    _guards: EnvVarGuards,
    _lock: MutexGuard<'a, ()>,
}

impl EnvVarGuards {
    pub fn new(guards: Vec<EnvVarGuard>) -> Self {
        Self { _guards: guards }
    }
}

impl<'a> TestEnvGuard<'a> {
    fn new(lock: MutexGuard<'a, ()>) -> Self {
        let guards = EnvVarGuards::new(vec![
            EnvVarGuard::set("AORTA_TEST_ALLOW_ENV", "1"),
            EnvVarGuard::remove("AORTA_SCHEMA_ATTACH"),
            EnvVarGuard::remove("AORTA_HOST_ID"),
            EnvVarGuard::remove("AORTA_HOST_NAME"),
            EnvVarGuard::set("AORTA_TELEMETRY_NODE_IDENTITY", "1"),
            EnvVarGuard::remove("AORTA_TELEMETRY_HEARTBEAT_MS"),
            EnvVarGuard::set("AORTA_TELEMETRY_RESOURCE_INVENTORY", "1"),
            EnvVarGuard::remove("AORTA_TELEMETRY_INVENTORY_HEARTBEAT_MS"),
            EnvVarGuard::set("AORTA_TELEMETRY_RUNTIME", "1"),
            EnvVarGuard::remove("AORTA_TELEMETRY_RUNTIME_HEARTBEAT_MS"),
            EnvVarGuard::remove(Session::SESSION_CONFIG_ENV),
            EnvVarGuard::remove(zenoh::Config::DEFAULT_CONFIG_PATH_ENV),
        ]);
        Self {
            _guards: guards,
            _lock: lock,
        }
    }
}

impl EnvLock {
    pub fn lock(&self) -> std::sync::LockResult<TestEnvGuard<'_>> {
        match self.0.lock() {
            Ok(guard) => Ok(TestEnvGuard::new(guard)),
            Err(err) => Ok(TestEnvGuard::new(err.into_inner())),
        }
    }
}

impl EnvVarGuard {
    pub fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }

    pub fn remove(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

pub fn schema_attach_never_guard() -> EnvVarGuards {
    schema_attach_guard("never")
}

pub fn allow_zenoh_env_guard() -> EnvVarGuard {
    EnvVarGuard::set("AORTA_TEST_ALLOW_ZENOH_ENV", "1")
}

pub fn allow_schema_env_guard() -> EnvVarGuard {
    EnvVarGuard::set("AORTA_TEST_ALLOW_SCHEMA_ENV", "1")
}

pub fn schema_attach_guard(value: &str) -> EnvVarGuards {
    EnvVarGuards::new(vec![
        allow_schema_env_guard(),
        EnvVarGuard::set("AORTA_SCHEMA_ATTACH", value),
    ])
}
