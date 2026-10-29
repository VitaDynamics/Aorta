//! Atomic primitives that swap to `loom`'s model-checked versions under
//! `--cfg loom`, and to `std` otherwise.
//!
//! For any primitive that routes its shared atomics through this module, loom
//! exhaustively explores *every* legal thread interleaving (including
//! `compare_exchange_weak` spurious failures and relaxed-ordering reorderings),
//! letting us **prove** hand-rolled concurrency code — e.g.
//! [`crate::in_flight::InFlightCounter`] — correct, rather than relying on
//! ThreadSanitizer happening to sample the one bad schedule.
//!
//! In normal (non-`loom`) builds these are zero-cost re-exports of
//! `std::sync::atomic`, so production code is byte-for-byte identical to using
//! `std` directly.
//!
//! To bring a new primitive under loom: route ALL of its shared atomics through
//! `crate::loom_sync::{…}` (never `std::sync::atomic` directly, or loom cannot
//! see them), then add a `#[cfg(loom)]` test module that drives it with
//! `loom::model`.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::{Mutex, MutexGuard};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::{Mutex, MutexGuard};
