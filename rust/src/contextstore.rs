//! ContextStore SDK — Rust public surface.
//!
//! Re-exports the core ContextStore primitive. The core
//! `aorta_core::contextstore::ContextStore` already provides an
//! ergonomic async API (`new(node)` + typed `set_*` / `get_*` /
//! `watch` / `watch_with_initial_snapshot` / `shutdown`); the SDK
//! module exists so downstream Rust crates have a stable
//! `aorta_sdk::contextstore::ContextStore` import path that mirrors
//! the C++ / Python SDK organisation.
//!
//! The SDK-level `Node::create_context_store()` convenience method is
//! provided in `crate::node::Node`.

pub use aorta_core::contextstore::{
    ContextChange, ContextChangeKind, ContextRead, ContextStore, OwnerLivelinessEvent,
    OwnerLivelinessHandle, SnapshotDoneReason, WatchHandle, WatchSnapshotOptions,
    DEFAULT_GET_DEADLINE, DEFAULT_SNAPSHOT_DEADLINE,
};
pub use aorta_core::contextstore_fb::{ContextKind, ContextValueMeta};

// Note: `OwnerConflictEvent` and the telemetry encode/decode helpers
// live in `aorta_core::owner_conflict_fb` and are not re-exported on
// purpose. Most SDK users only need to subscribe to the telemetry key
// (`aorta/{group}/sys/telemetry/vital/ctx/owner_conflict`) and decode events
// for diagnostics — that's an introspection-tool concern, not a
// writer/reader concern, and Phase E's CLI will provide it natively.
