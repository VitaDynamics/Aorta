#![allow(dead_code)]
// Unsafe-documentation gate. Every `unsafe { … }` block in the
// `aorta_core` lib must carry a `// SAFETY:` comment justifying the
// invariant being relied on. Hand-written `*_fb.rs` wire-access modules
// and the FlatBuffers `*_generated.rs` modules (`foxglove_log` + `telemetry_*`)
// opt out with module-level `#![allow]`
// (rationale in their respective file headers). Integration tests under
// `core/test/*.rs` are separate compilation units and apply the same
// `#![allow]` exception with a uniform rationale; the gate exists to
// keep PRODUCTION FFI surface honest.
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod action;
pub mod action_client;
mod action_info_fb;
pub mod action_server;
pub mod attachment;
pub mod capture;
pub mod client;
pub mod contextstore;
// Committed BFBS for `aorta.ctx.ContextValue` (regen: message/tools/regen_telemetry_fb.py,
// drift-tested in core/build.rs). Re-exported by `contextstore_fb`.
mod contextstore_bfbs_generated;
pub mod contextstore_fb;
#[cfg(target_os = "linux")]
pub(crate) mod crossbeam_queue;
pub mod error;
pub mod event_queue;
pub mod executor;
pub mod flow;
pub mod key_expr;
pub mod liveliness;
pub mod logging;
pub mod node;
pub mod owner_conflict_fb;
mod payload;
pub mod publisher;
pub mod qos;
pub mod runtime;
mod runtime_env;
pub mod schema;
mod schema_by_key_fb;
pub mod schema_registry;
pub mod schema_resolve;
pub mod service;
pub mod session;
pub mod shm;
pub mod snapshot_queryable;
pub mod subscriber;
pub mod telemetry;
pub mod time;
pub mod timer;
pub mod trace;
mod transport;
pub mod uuid_v7;
pub mod watch;

// FlatBuffers-generated telemetry bindings, committed under `telemetry/` and
// declared at the crate root (not nested in `telemetry::fb`) because flatc emits
// cross-schema `include` references as `crate::<stem>_generated::*` paths. The
// `telemetry_vital_snapshot` record composes `node_identity` + `resource_inventory`
// (+ the shared `telemetry_common` KeyValue) via `include`; each carries its own
// module-level `#![allow]` (see file headers) and a `build.rs` drift guard.
// Regenerate with `message/tools/regen_telemetry_fb.py`.
#[path = "telemetry/telemetry_common_generated.rs"]
mod telemetry_common_generated;
#[path = "telemetry/telemetry_node_identity_generated.rs"]
mod telemetry_node_identity_generated;
#[path = "telemetry/telemetry_resource_inventory_generated.rs"]
mod telemetry_resource_inventory_generated;
#[path = "telemetry/telemetry_runtime_metrics_generated.rs"]
mod telemetry_runtime_metrics_generated;
#[path = "telemetry/telemetry_vital_snapshot_generated.rs"]
mod telemetry_vital_snapshot_generated;

pub use zenoh::bytes::ZBytes;

// Re-export commonly used types at crate root for convenience
pub use client::Client;
pub use flow::{Budget, CancelReason, CancelToken};
pub use key_expr::{KeyExprBuilder, Target};
pub use schema::{
    HeaderInjector, HeaderValues, SchemaHash, TypedPublishMessage, TypedSubscribeMessage,
};
pub use schema_registry::SchemaRegistry;
pub use service::{Service, ServiceSchemaInfo};
pub use session::Session;
pub use telemetry::{HostIdentity, NodeIdentityTelemetry, NodeOptions, TelemetryNodeMetadata};
pub use time::{
    now, now_ns, set_simulation_time_ns, set_time_source, simulation_time_ns, time_source,
    unix_time_ns, TimeSource,
};
pub use transport::{reply_error_is_zenoh_query_timeout, reply_error_text};
pub use uuid_v7::new_uuid_v7;
pub use watch::{Watch, WatchGuard};

#[macro_export]
macro_rules! aorta_core_log {
    ($level:expr, $($arg:tt)+) => {{
        if $crate::logging::log_enabled($level, "aorta_core") {
            $crate::logging::log_at(
                $level,
                "aorta_core",
                None,
                Some(file!()),
                Some(line!()),
                &format!($($arg)+),
            );
        }
    }};
}

#[macro_export]
macro_rules! aorta_core_trace {
    ($($arg:tt)+) => {{
        $crate::aorta_core_log!($crate::logging::LogLevel::Trace, $($arg)+);
    }};
}

#[macro_export]
macro_rules! aorta_core_debug {
    ($($arg:tt)+) => {{
        $crate::aorta_core_log!($crate::logging::LogLevel::Debug, $($arg)+);
    }};
}

#[macro_export]
macro_rules! aorta_core_info {
    ($($arg:tt)+) => {{
        $crate::aorta_core_log!($crate::logging::LogLevel::Info, $($arg)+);
    }};
}

#[macro_export]
macro_rules! aorta_core_warn {
    ($($arg:tt)+) => {{
        $crate::aorta_core_log!($crate::logging::LogLevel::Warn, $($arg)+);
    }};
}

#[macro_export]
macro_rules! aorta_core_error {
    ($($arg:tt)+) => {{
        $crate::aorta_core_log!($crate::logging::LogLevel::Error, $($arg)+);
    }};
}

pub mod log_prelude {
    pub use crate::{
        aorta_core_debug, aorta_core_error, aorta_core_info, aorta_core_log, aorta_core_trace,
        aorta_core_warn,
    };
}

#[cfg(test)]
#[path = "../test/action_server_tests.rs"]
mod action_server_tests;
#[cfg(test)]
#[path = "../test/action_tests.rs"]
mod action_tests;
#[cfg(test)]
#[path = "../test/contextstore_block_qos_tests.rs"]
mod contextstore_block_qos_tests;
#[cfg(test)]
#[path = "../test/contextstore_commit_order_tests.rs"]
mod contextstore_commit_order_tests;
#[cfg(test)]
#[path = "../test/contextstore_conflict_tests.rs"]
mod contextstore_conflict_tests;
#[cfg(test)]
#[path = "../test/contextstore_declare_tests.rs"]
mod contextstore_declare_tests;
#[cfg(test)]
#[path = "../test/contextstore_ffi_tests.rs"]
mod contextstore_ffi_tests;
#[cfg(test)]
#[path = "../test/contextstore_keys_tests.rs"]
mod contextstore_keys_tests;
#[cfg(test)]
#[path = "../test/contextstore_liveliness_tests.rs"]
mod contextstore_liveliness_tests;
#[cfg(test)]
#[path = "../test/contextstore_sequence_tests.rs"]
mod contextstore_sequence_tests;
#[cfg(test)]
#[path = "../test/contextstore_set_get_tests.rs"]
mod contextstore_set_get_tests;
#[cfg(test)]
#[path = "../test/contextstore_snapshot_tests.rs"]
mod contextstore_snapshot_tests;
#[cfg(test)]
#[path = "../test/contextstore_watch_tests.rs"]
mod contextstore_watch_tests;
#[cfg(test)]
#[path = "../test/cross_namespace_tests.rs"]
mod cross_namespace_tests;
#[cfg(test)]
#[path = "../test/ffi_action_tests.rs"]
mod ffi_action_tests;
#[cfg(test)]
#[path = "../test/ffi_capture_tests.rs"]
mod ffi_capture_tests;
#[cfg(test)]
#[path = "../test/ffi_panic_isolation_tests.rs"]
mod ffi_panic_isolation_tests;
#[cfg(test)]
#[path = "../test/ffi_reentrancy_contract_tests.rs"]
mod ffi_reentrancy_contract_tests;
#[cfg(test)]
#[path = "../test/ffi_service_tests.rs"]
mod ffi_service_tests;
#[cfg(test)]
#[path = "../test/integration_tests.rs"]
mod integration_tests;
#[cfg(test)]
#[path = "../test/native_zenoh_interop_tests.rs"]
mod native_zenoh_interop_tests;
#[cfg(test)]
#[path = "../test/performance_tests.rs"]
mod performance_tests;
#[cfg(test)]
#[path = "../test/service_client_cross_session_tests.rs"]
mod service_client_cross_session_tests;
#[cfg(test)]
#[path = "../test/service_client_integration_tests.rs"]
mod service_client_integration_tests;
#[cfg(test)]
#[path = "../test/spawn_panic_logging_test.rs"]
mod spawn_panic_logging_test;
#[cfg(test)]
#[path = "../test/stop_handle_tests.rs"]
mod stop_handle_tests;
#[cfg(test)]
#[path = "../test/telemetry_schema_tests.rs"]
mod telemetry_schema_tests;
#[cfg(test)]
#[path = "../test/test_support.rs"]
mod test_support;
#[cfg(test)]
#[path = "../test/timer_integration_tests.rs"]
mod timer_integration_tests;
#[cfg(test)]
#[path = "../test/trace_export_tests.rs"]
mod trace_export_tests;
#[cfg(test)]
#[path = "../test/trace_ffi_tests.rs"]
mod trace_ffi_tests;
#[cfg(test)]
#[path = "../test/trace_gate_tests.rs"]
mod trace_gate_tests;
#[cfg(test)]
#[path = "../test/trace_histogram_tests.rs"]
mod trace_histogram_tests;
#[cfg(test)]
#[path = "../test/trace_overhead_bench.rs"]
mod trace_overhead_bench;
#[cfg(test)]
#[path = "../test/trace_pubsub_tests.rs"]
mod trace_pubsub_tests;
#[cfg(test)]
#[path = "../test/trace_runtime_tests.rs"]
mod trace_runtime_tests;
#[cfg(test)]
#[path = "../test/trace_sched_tests.rs"]
mod trace_sched_tests;
#[cfg(test)]
#[path = "../test/trace_scope_tests.rs"]
mod trace_scope_tests;
#[cfg(test)]
#[path = "../test/trace_service_tests.rs"]
mod trace_service_tests;
#[cfg(test)]
#[path = "../test/trace_stage_tests.rs"]
mod trace_stage_tests;
#[cfg(test)]
#[path = "../test/trace_test_support.rs"]
mod trace_test_support;
#[cfg(test)]
#[path = "../test/trace_user_tests.rs"]
mod trace_user_tests;

pub mod ffi {
    mod abi_assert;
    pub mod c_api;
    pub mod guard;
    pub mod runtime_helper;
}
