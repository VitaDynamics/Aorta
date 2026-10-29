//! Aorta header support: re-exports the header contract types from core.
//!
//! - [`HeaderInjector`]: implemented by message schemas that can inject an
//!   Aorta header (publish timestamp, publisher node, sequence) into their
//!   FlatBuffers payloads;
//! - [`HeaderValues`]: the header fields filled in at publish time;
//! - [`TypedPublishMessage`]: schema trait combining header injection with
//!   typed message **building** (decoding is the separate
//!   [`TypedSubscribeMessage`](aorta_core::schema::TypedSubscribeMessage)
//!   contract).
//!
//! Typed publishers/subscribers and the typed RPC wrappers use these under
//! the hood; application code rarely needs them directly except when
//! hand-implementing a [`SchemaMeta`](crate::SchemaMeta) type.

pub use aorta_core::schema::{HeaderInjector, HeaderValues, TypedPublishMessage};
