//! Procfs / PSI collectors.
//!
//! Each collector exposes:
//! - A `Sample` newtype carrying the raw cumulative counters at one tick.
//! - A `delta(prev, curr, interval_ms)` function that computes the
//!   derived metrics.
//!
//! This split keeps unit tests deterministic — feed in two fixed samples
//! plus a synthetic interval and assert the derived values without
//! sleeping on a real `/proc`.

pub mod cpu;
pub mod disk;
pub mod memory;
pub mod network;
pub mod perf;
pub mod process;
pub mod psi;

mod parse;
pub(crate) use parse::*;

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct CollectorError {
    pub family: &'static str,
    pub detail: &'static str,
}
