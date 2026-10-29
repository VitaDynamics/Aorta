//! Aorta system monitor.
//!
//! See the crate `README.md`. This crate exposes:
//! - [`config`] — TOML / CLI / env configuration.
//! - [`identity`] — host name / id resolution and the placeholder
//!   normalization algorithm.
//! - [`capability`] — runtime probe of metric families.
//! - [`collector`] — per-family /proc/PSI/etc collectors that produce
//!   one-shot deltas between two samples.
//! - [`snapshot`] — aggregator that wires collectors together and renders
//!   the JSON debug view emitted by `aorta-sysmon snapshot --pretty`.
//!
//! The snapshot path does not depend on `aorta_core` or `aorta_sdk`; the
//! SDK publishing path (`publisher`) needs the `sdk-publish` feature.

pub mod alert;
pub mod capability;
pub mod collector;
pub mod config;
pub mod identity;
pub mod procfs_root;
pub mod snapshot;

#[cfg(feature = "sdk-publish")]
pub mod publisher;
