//! Aorta Flow — structured-concurrency primitives.
//!
//! The Flow golden path — in-task combinators borrow locals and
//! spawn nothing; handles resolve [`Outcome`]; cancellation arrives at
//! dispatch boundaries as [`Stopped`]:
//!
//! ```no_run
//! # async fn demo() -> aorta_core::flow::Outcome<()> {
//! use aorta_core::flow::{Outcome, ScopeOpts, Scope};
//! let scope = Scope::new_root(ScopeOpts::named("golden"));
//! async fn read_imu() -> u32 { 1 }
//! async fn read_lidar() -> u32 { 2 }
//! async fn primary_rpc() -> u32 { 3 }
//! async fn backup_rpc() -> u32 { 4 }
//! async fn plan_path(_m: u32) -> u32 { 5 }
//!
//! let (imu, lidar) = scope.join(read_imu(), read_lidar()).await?; // in-task, borrows
//! let winner = scope.race(primary_rpc(), backup_rpc()).await?;    // loser dropped
//! let h = scope.spawn(plan_path(winner + imu + lidar));           // tokio task
//! let path: Outcome<u32> = h.await;
//! # let _ = path;
//! # scope.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! Core vocabulary: [`Scope`] (spawn/cancel/budget; [`Supervision`]
//! policies; `shutdown_with_grace`), [`Outcome`]/[`Stopped`] (the stopped
//! channel as a value), in-task combinators
//! (`join`/`try_join`/`race`/`timeout` — zero spawns), [`Shield`] (the
//! cleanup primitive; budget still applies), [`CancelToken`] /
//! [`CancelReason`] (the loom-verified cancel tree from `aorta_sync`),
//! [`Budget`] (relative monotonic deadlines).

use std::time::{Duration, Instant};

pub(crate) mod ambient;
pub mod combinators;
pub mod introspect;
pub mod offload;
pub mod outcome;
pub mod readiness;
pub mod scope;

pub use aorta_sync::cancel::{CancelReason, CancelToken, Cancelled, RawCancelReason};
pub use combinators::{when_all, when_any, Either, Shield};
pub use offload::{offload, offload_in};
pub use outcome::{Outcome, StopReason, Stopped};
pub use readiness::{Condition, ReadinessSet, ReadinessWire, WindowStamp, WindowStats, WindowSync};
pub use scope::{ActionScope, ExternalChild, Scope, ScopeOpts, Supervision, TaskHandle};

/// Relative, monotonic-anchored deadline; wire-propagated as
/// relative nanoseconds (a robot fleet has no trustworthy shared wall clock).
///
/// `remaining()` is a vDSO clock read + compare (~tens of ns) — non-blocking,
/// alloc-free, safe from any thread (RT / RX): one of the two RT-safe poll
/// primitives. Expiry is *observed*, not delivered: there is no timer
/// thread arming deadlines — the scheduler observes it at dequeue and
/// handlers at await checkpoints, and the first observer fires the owning
/// scope's token with [`CancelReason::DeadlineExceeded`].
/// Consequence for pollers: check BOTH `CancelToken::is_cancelled()` and
/// `Budget::remaining()`.
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    anchor: Instant,
    dur: Option<Duration>,
}

impl Default for Budget {
    fn default() -> Self {
        Self::unlimited()
    }
}

impl Budget {
    /// No deadline: `remaining()` reports `None`, never expires.
    pub fn unlimited() -> Self {
        Self {
            anchor: Instant::now(),
            dur: None,
        }
    }

    /// A budget of `dur`, anchored now.
    pub fn with_deadline(dur: Duration) -> Self {
        Self {
            anchor: Instant::now(),
            dur: Some(dur),
        }
    }

    /// Rebuild from a wire-propagated relative deadline, re-anchored at the
    /// receiver's now.
    ///
    /// HONEST LIMIT: without a synchronized clock, the time
    /// the message spent in flight/queues between the sender's
    /// [`Self::wire_relative_ns`] and this call is NOT deducted — the
    /// receiver-side budget is *at least* as generous as the sender's true
    /// remaining. It is therefore a cooperative NARROWING hint for the
    /// callee; the authoritative deadline is always the SENDER's own guard
    /// (e.g. `call_in`'s narrowed timeout), which expires on the sender's
    /// clock regardless of what the receiver believes.
    pub fn from_wire_relative_ns(ns: u64) -> Self {
        Self::with_deadline(Duration::from_nanos(ns))
    }

    /// Time left: `None` = unlimited; `Some(ZERO)` = expired. ~one vDSO
    /// clock read + compare; RT-safe.
    #[inline]
    pub fn remaining(&self) -> Option<Duration> {
        self.dur
            .map(|dur| dur.saturating_sub(self.anchor.elapsed()))
    }

    /// `true` iff a deadline exists and has passed.
    #[inline]
    pub fn is_expired(&self) -> bool {
        matches!(self.remaining(), Some(rem) if rem.is_zero())
    }

    /// A child budget of `min(self.remaining(), child)`, anchored now —
    /// a child may narrow, never widen.
    pub fn narrow(&self, child: Duration) -> Budget {
        match self.remaining() {
            Some(remaining) => Self::with_deadline(remaining.min(child)),
            None => Self::with_deadline(child),
        }
    }

    /// The remaining budget as wire-relative nanoseconds (`None` =
    /// unlimited). Saturates at `u64::MAX` ns (~584 years).
    /// This is the wire format of the request-attachment deadline
    /// (`TLV_DEADLINE`, 8-byte LE relative ns — see `attachment.rs`):
    /// the RPC client stamps its narrowed remaining window per attempt
    /// and `send_goal` stamps the ambient cap as the goal budget (both
    /// honoring the `AORTA_RPC_DEADLINE=0` kill-switch); receivers
    /// rebuild via [`Self::from_wire_relative_ns`]. The send paths
    /// compute the same saturating conversion inline on their narrowed
    /// windows; this fn anchors the format and the round-trip tests.
    pub fn wire_relative_ns(&self) -> Option<u64> {
        self.remaining()
            .map(|rem| u64::try_from(rem.as_nanos()).unwrap_or(u64::MAX))
    }
}

/// The explicit reentrancy detector (replaces the
/// panic-substring classifier as the *mechanism* — the classifier remains a
/// fallback for foreign runtime contexts these flags cannot see).
///
/// Two exact, thread-local signals:
/// - [`BlockOnGuard`]: this thread is inside an aorta `block_on` (e.g. a C
///   thread parked in `aorta_executor_spin`; its dispatched callbacks run on
///   this same thread).
/// - [`CallbackContextGuard`]: this thread is inside a dispatched callback
///   (`Event::dispatch_isolated` — the single dispatch point — or an FFI
///   delivery helper running on a runtime worker).
///
/// A blocking FFI call while either flag is set is reentrant by
/// construction and fails with `AORTA_ERR_REENTRANT` without constructing a
/// Tokio panic. `spawn_blocking` threads carry neither flag, so their legal
/// `block_on` stays on the success path.
pub(crate) mod reentrancy {
    use std::cell::Cell;

    thread_local! {
        static IN_AORTA_BLOCK_ON: Cell<bool> = const { Cell::new(false) };
        static IN_AORTA_CALLBACK: Cell<bool> = const { Cell::new(false) };
    }

    /// `true` iff this thread is inside an aorta `block_on` or a dispatched
    /// callback — reentrant by construction.
    #[inline]
    pub(crate) fn explicitly_reentrant() -> bool {
        IN_AORTA_BLOCK_ON.with(Cell::get) || IN_AORTA_CALLBACK.with(Cell::get)
    }

    /// RAII flag for "this thread is inside an aorta block_on". Panic-safe
    /// (restores the previous value on drop, even on unwind).
    pub(crate) struct BlockOnGuard {
        prev: bool,
    }

    impl BlockOnGuard {
        pub(crate) fn enter() -> Self {
            Self {
                prev: IN_AORTA_BLOCK_ON.with(|flag| flag.replace(true)),
            }
        }
    }

    impl Drop for BlockOnGuard {
        fn drop(&mut self) {
            let prev = self.prev;
            IN_AORTA_BLOCK_ON.with(|flag| flag.set(prev));
        }
    }

    thread_local! {
        static STOP_DRAINING: Cell<bool> = const { Cell::new(false) };
    }

    /// `true` while this thread is running lane events drained by an
    /// executor stop/teardown: a posted resumption delivered under
    /// this flag reports `AORTA_ERR_CANCELLED` to its C-side resume fn so
    /// the frame can distinguish teardown from a normal dispatch.
    #[inline]
    pub(crate) fn is_stop_draining() -> bool {
        STOP_DRAINING.with(Cell::get)
    }

    /// RAII flag for the executor stop-drain window.
    pub(crate) struct StopDrainGuard {
        prev: bool,
    }

    impl StopDrainGuard {
        pub(crate) fn enter() -> Self {
            Self {
                prev: STOP_DRAINING.with(|flag| flag.replace(true)),
            }
        }
    }

    impl Drop for StopDrainGuard {
        fn drop(&mut self) {
            let prev = self.prev;
            STOP_DRAINING.with(|flag| flag.set(prev));
        }
    }

    /// Raw set/replace for the C-ABI mark pair (MT lane
    /// workers wrap resumes). Returns the previous value.
    pub(crate) fn callback_context_replace(value: bool) -> bool {
        IN_AORTA_CALLBACK.with(|flag| flag.replace(value))
    }

    /// RAII flag for "this thread is inside a dispatched callback".
    pub(crate) struct CallbackContextGuard {
        prev: bool,
    }

    impl CallbackContextGuard {
        pub(crate) fn enter() -> Self {
            Self {
                prev: IN_AORTA_CALLBACK.with(|flag| flag.replace(true)),
            }
        }
    }

    impl Drop for CallbackContextGuard {
        fn drop(&mut self) {
            let prev = self.prev;
            IN_AORTA_CALLBACK.with(|flag| flag.set(prev));
        }
    }
}

#[cfg(test)]
#[path = "../../test/flow_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../test/flow_scope_tests.rs"]
mod scope_tests;
