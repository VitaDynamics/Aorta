//! Loom-verified concurrency primitives for Aorta.
//!
//! This crate is deliberately **tokio-/zenoh-free**. `loom` (a precise C11
//! memory-model checker) is driven by a global `--cfg loom` via `RUSTFLAGS`,
//! which forces *every* crate in the compile into its loom configuration — and
//! tokio's loom configuration drops the runtime / time / sync API that
//! `aorta_core` depends on, so `aorta_core` cannot compile under `--cfg loom`.
//! Isolating the loom-tested primitives in this leaf crate lets
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p aorta_sync
//! ```
//!
//! recompile only this crate (plus loom) under loom, while `aorta_core`
//! consumes the primitives through plain `std` atomics in every normal build —
//! they are byte-for-byte identical to hand-written `std` code there.
//!
//! CI lane: `.github/workflows/aorta-loom.yml`.

pub mod cancel;
pub mod in_flight;
mod loom_sync;
pub mod readiness;
pub mod wake_slot;

pub use cancel::{CancelReason, CancelToken, Cancelled};
pub use in_flight::InFlightCounter;
pub use readiness::ReadinessGate;
pub use wake_slot::WakeSlot;
