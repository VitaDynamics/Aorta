use std::time::Duration;

use crate::log_prelude::*;
use crate::runtime_env;

mod command_sink;
pub mod fb;
pub mod identity;
pub mod inventory;
pub mod publisher;
pub mod runtime;
pub mod schema;
pub mod serde;

#[derive(Debug, Clone)]
pub(crate) struct PendingCommand<T> {
    pub sequence: u64,
    pub command: T,
}

pub(crate) use command_sink::{CommandDispatch, CommandSink, PendingCommands};

pub(crate) fn enabled_from_env(var: &str) -> bool {
    match runtime_env::var(var) {
        Some(raw) => {
            let raw = raw.trim().to_lowercase();
            !(raw == "0" || raw == "false" || raw == "no")
        }
        None => true,
    }
}

/// Lower bound (ms) for any operator-supplied telemetry heartbeat interval.
/// Telemetry is on by default, so a single fat-fingered config push
/// (e.g. `...HEARTBEAT_MS=1` instead of `1000`) would make every node in the
/// fleet serialize+publish full snapshots ~1000x/s, saturating CPU and the
/// Zenoh transport simultaneously — a textbook fleet-wide correlated failure.
/// A 100ms floor still allows up to 10Hz telemetry while making that
/// catastrophe unreachable. The built-in defaults (>=5s) are well above it.
pub(crate) const MIN_HEARTBEAT_INTERVAL_MS: u64 = 100;

pub(crate) fn heartbeat_interval_from_env(var: &str, default_ms: u64) -> Duration {
    let configured = match runtime_env::var(var) {
        Some(raw) => raw.trim().parse::<u64>().ok().filter(|value| *value > 0),
        None => None,
    };
    // Invalid / unset -> use the trusted default verbatim (never clamped).
    let Some(value) = configured else {
        return Duration::from_millis(default_ms);
    };
    if value < MIN_HEARTBEAT_INTERVAL_MS {
        aorta_core_warn!(
            "Aorta WARN: {}={}ms is below the {}ms telemetry heartbeat floor; \
clamping to {}ms to avoid fleet-wide telemetry self-DoS",
            var,
            value,
            MIN_HEARTBEAT_INTERVAL_MS,
            MIN_HEARTBEAT_INTERVAL_MS
        );
        return Duration::from_millis(MIN_HEARTBEAT_INTERVAL_MS);
    }
    Duration::from_millis(value)
}

pub(crate) fn warn_task_join_failure(family: &str, err: &tokio::task::JoinError) {
    aorta_core_warn!("Aorta WARN: {} telemetry task join failed: {}", family, err);
}

#[allow(deprecated)]
pub use identity::{build_node_identity_telemetry, resolve_host_identity};
pub use inventory::{TelemetryResourceGuard, VitalSnapshotTelemetryPublisher};
pub use publisher::NodeIdentityTelemetryPublisher;
pub use runtime::{RuntimeMetricsTelemetryPublisher, RuntimeTelemetryGuard};
pub use schema::{
    HostIdentity, NodeIdentityTelemetry, NodeOptions, ResourceInventoryTelemetry,
    RuntimeMetricsTelemetry, TelemetryClientEntry, TelemetryClientRuntimeMetrics,
    TelemetryExecutorRuntimeMetrics, TelemetryNodeMetadata, TelemetryPublisherEntry,
    TelemetryPublisherRuntimeMetrics, TelemetryResourceCommon, TelemetryServiceEntry,
    TelemetryServiceRuntimeMetrics, TelemetrySubscriberEntry, TelemetrySubscriberRuntimeMetrics,
    TELEMETRY_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests {
    use super::{enabled_from_env, heartbeat_interval_from_env};
    use crate::test_support::ENV_LOCK;
    use std::time::Duration;

    const TEST_HEARTBEAT_VAR: &str = "AORTA_TEST_TLM2_HEARTBEAT_MS";

    #[test]
    fn heartbeat_interval_clamps_value_below_floor() {
        let _guard = ENV_LOCK.lock().expect("lock");
        // A fat-fingered 1ms (vs the intended 1000ms) must NOT produce a 1ms
        // heartbeat — that would make every node publish ~1000x/s and is a
        // fleet-wide correlated self-DoS.
        std::env::set_var(TEST_HEARTBEAT_VAR, "1");
        let interval = heartbeat_interval_from_env(TEST_HEARTBEAT_VAR, 5_000);
        std::env::remove_var(TEST_HEARTBEAT_VAR);
        assert!(
            interval >= Duration::from_millis(100),
            "1ms must be clamped to a sane floor, got {:?}",
            interval
        );
        assert_eq!(interval, Duration::from_millis(100));
    }

    #[test]
    fn heartbeat_interval_keeps_value_at_or_above_floor() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::set_var(TEST_HEARTBEAT_VAR, "100");
        assert_eq!(
            heartbeat_interval_from_env(TEST_HEARTBEAT_VAR, 5_000),
            Duration::from_millis(100)
        );
        std::env::set_var(TEST_HEARTBEAT_VAR, "2000");
        assert_eq!(
            heartbeat_interval_from_env(TEST_HEARTBEAT_VAR, 5_000),
            Duration::from_millis(2000)
        );
        std::env::remove_var(TEST_HEARTBEAT_VAR);
    }

    #[test]
    fn heartbeat_interval_falls_back_to_default_for_zero_or_invalid() {
        let _guard = ENV_LOCK.lock().expect("lock");
        // 0 and non-numeric are invalid -> use the (trusted) default, which is
        // itself well above the floor; never clamp the default.
        std::env::set_var(TEST_HEARTBEAT_VAR, "0");
        assert_eq!(
            heartbeat_interval_from_env(TEST_HEARTBEAT_VAR, 5_000),
            Duration::from_millis(5_000)
        );
        std::env::set_var(TEST_HEARTBEAT_VAR, "not-a-number");
        assert_eq!(
            heartbeat_interval_from_env(TEST_HEARTBEAT_VAR, 5_000),
            Duration::from_millis(5_000)
        );
        std::env::remove_var(TEST_HEARTBEAT_VAR);
        assert_eq!(
            heartbeat_interval_from_env(TEST_HEARTBEAT_VAR, 30_000),
            Duration::from_millis(30_000)
        );
    }

    #[test]
    fn enabled_from_env_defaults_true() {
        let _guard = ENV_LOCK.lock().expect("lock");
        std::env::remove_var("AORTA_TELEMETRY_RUNTIME");
        assert!(enabled_from_env("AORTA_TELEMETRY_RUNTIME"));
    }

    #[test]
    fn enabled_from_env_false_values_disable_telemetry() {
        let _guard = ENV_LOCK.lock().expect("lock");

        std::env::set_var("AORTA_TELEMETRY_RUNTIME", "0");
        assert!(!enabled_from_env("AORTA_TELEMETRY_RUNTIME"));

        std::env::set_var("AORTA_TELEMETRY_RUNTIME", "false");
        assert!(!enabled_from_env("AORTA_TELEMETRY_RUNTIME"));

        std::env::set_var("AORTA_TELEMETRY_RUNTIME", "no");
        assert!(!enabled_from_env("AORTA_TELEMETRY_RUNTIME"));

        std::env::remove_var("AORTA_TELEMETRY_RUNTIME");
    }
}
