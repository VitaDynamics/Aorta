//! The unified stop-observation layer: one
//! task-local ambient context is the single entry point for every
//! FLOW-LAYER stop/budget observation — the spawn dispatch boundary,
//! `checkpoint()`, `run_until_cancelled`, the in-task
//! combinators, and `call_in`.
//!
//! Absent-ambient fallback: every read uses `try_with`
//! and degrades to `{no shield, no cap}` when the calling task is a plain
//! tokio task — flow APIs called outside any flow task keep their pre-Flow
//! behavior bit-for-bit.
//!
//! The raw token surface (`Scope::is_cancelled/cancelled/token`) stays
//! shield-transparent BY DESIGN it is the advanced/interop
//! surface; this module governs the flow-layer entry points only.
//!
//! Two shield counters exist because two different questions are asked:
//! - `live_shields` — "is ANY shield alive in this task?" Read by the
//!   dispatch boundary BETWEEN polls; a shield future holds +1 from its
//!   first poll until drop, so a task suspended inside a shield is not
//!   short-circuited at its next wake.
//! - `poll_shield` — "is the CURRENT poll inside a shield?" RAII around
//!   the shield's inner poll; read by `checkpoint`/awaitables/combinators
//!   so a sibling combinator branch outside the shield still observes
//!   cancellation.

use std::cell::Cell;
use std::time::Instant;

tokio::task_local! {
    static AMBIENT_FLOW: AmbientFlow;
}

#[derive(Default)]
pub(crate) struct AmbientFlow {
    live_shields: Cell<u32>,
    poll_shield: Cell<u32>,
    budget_cap: Cell<Option<Instant>>,
}

/// Seed the ambient for a spawned flow task body (composes with
/// `SCOPE_STACK` seeding in `Scope::spawn`).
pub(crate) fn scoped<F>(fut: F) -> tokio::task::futures::TaskLocalFuture<AmbientFlow, F>
where
    F: std::future::Future,
{
    AMBIENT_FLOW.scope(AmbientFlow::default(), fut)
}

/// Dispatch-boundary read: any live shield in this task?
pub(crate) fn boundary_shielded() -> bool {
    AMBIENT_FLOW
        .try_with(|a| a.live_shields.get() > 0)
        .unwrap_or(false)
}

/// In-poll read: is the current poll inside a shield?
pub(crate) fn poll_shielded() -> bool {
    AMBIENT_FLOW
        .try_with(|a| a.poll_shield.get() > 0)
        .unwrap_or(false)
}

/// In-poll read: the narrowed deadline installed by `scope.timeout`
/// . `None` = no cap (plain tokio callers always see `None`).
/// the ambient cap's remaining window as wire-ready RELATIVE ns
/// (`None` outside flow tasks or with no cap set).
pub(crate) fn budget_cap_remaining_ns() -> Option<u64> {
    budget_cap().map(|deadline| {
        u64::try_from(
            deadline
                .saturating_duration_since(Instant::now())
                .as_nanos(),
        )
        .unwrap_or(u64::MAX)
    })
}

pub(crate) fn budget_cap() -> Option<Instant> {
    AMBIENT_FLOW
        .try_with(|a| a.budget_cap.get())
        .unwrap_or(None)
}

/// Shield lifetime accounting (consumes these; no-ops outside a
/// flow task). Returns whether the ambient was present so the caller
/// only decrements what it incremented.
pub(crate) fn enter_live_shield() -> bool {
    AMBIENT_FLOW
        .try_with(|a| a.live_shields.set(a.live_shields.get() + 1))
        .is_ok()
}

pub(crate) fn exit_live_shield() {
    let _ = AMBIENT_FLOW.try_with(|a| {
        let v = a.live_shields.get();
        debug_assert!(v > 0, "unbalanced exit_live_shield");
        a.live_shields.set(v.saturating_sub(1));
    });
}

/// RAII guard for the poll-scoped shield depth (wraps one inner poll).
pub(crate) struct PollShieldGuard {
    entered: bool,
}

impl PollShieldGuard {
    pub(crate) fn enter() -> Self {
        let entered = AMBIENT_FLOW
            .try_with(|a| a.poll_shield.set(a.poll_shield.get() + 1))
            .is_ok();
        Self { entered }
    }
}

impl Drop for PollShieldGuard {
    fn drop(&mut self) {
        if self.entered {
            let _ = AMBIENT_FLOW.try_with(|a| {
                let v = a.poll_shield.get();
                debug_assert!(v > 0, "unbalanced PollShieldGuard");
                a.poll_shield.set(v.saturating_sub(1));
            });
        }
    }
}

/// Shield-aware cancelled future (the effective view of the stopped
/// channel): resolves when the token fires UNLESS the current poll is
/// inside a shield. Keeps the token waker registered either way so
/// waker chains stay healthy. Shared by `run_until_cancelled` and
/// `call_in`'s in-flight cancel race (the combinators use the
/// checkpoint-based `poll_effective_stop`, which additionally observes
/// the budget — a different semantic, deliberately not unified).
pub(crate) struct EffectiveCancelled {
    pub(crate) cancelled: aorta_sync::Cancelled,
}

impl std::future::Future for EffectiveCancelled {
    type Output = aorta_sync::CancelReason;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let polled = std::pin::Pin::new(&mut self.cancelled).poll(cx);
        if poll_shielded() {
            return std::task::Poll::Pending;
        }
        polled
    }
}

/// RAII guard for a poll-scoped budget cap: installs `min(existing, cap)`
/// for one inner poll, restores the previous value after (backs
/// `timeout`; caps narrow, never widen — the guard stays
/// poll-scoped so it never crosses `spawn` and never leaks to sibling
/// combinator branches).
pub(crate) struct BudgetCapGuard {
    prev: Option<Option<Instant>>,
}

impl BudgetCapGuard {
    pub(crate) fn install(cap: Instant) -> Self {
        let prev = AMBIENT_FLOW
            .try_with(|a| {
                let prev = a.budget_cap.get();
                let effective = match prev {
                    Some(existing) => existing.min(cap),
                    None => cap,
                };
                a.budget_cap.set(Some(effective));
                prev
            })
            .ok();
        Self { prev }
    }
}

impl Drop for BudgetCapGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.take() {
            let _ = AMBIENT_FLOW.try_with(|a| a.budget_cap.set(prev));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absent_ambient_degrades_to_v1() {
        // Plain tokio task: no task_local seeded — every read degrades.
        assert!(!boundary_shielded());
        assert!(!poll_shielded());
        assert_eq!(budget_cap(), None);
        assert!(!enter_live_shield(), "no ambient to account against");
        exit_live_shield(); // must not panic
        let _g = PollShieldGuard::enter(); // must not panic
        let _c = BudgetCapGuard::install(Instant::now()); // must not panic
    }

    #[tokio::test]
    async fn shield_and_cap_accounting_inside_ambient() {
        scoped(async {
            assert!(!boundary_shielded());
            assert!(enter_live_shield());
            assert!(boundary_shielded());
            {
                let _g = PollShieldGuard::enter();
                assert!(poll_shielded());
            }
            assert!(!poll_shielded(), "poll guard is RAII-scoped");
            exit_live_shield();
            assert!(!boundary_shielded());

            let far = Instant::now() + std::time::Duration::from_secs(60);
            let near = Instant::now() + std::time::Duration::from_secs(1);
            {
                let _outer = BudgetCapGuard::install(far);
                assert_eq!(budget_cap(), Some(far));
                {
                    let _inner = BudgetCapGuard::install(near);
                    assert_eq!(budget_cap(), Some(near), "caps narrow");
                }
                assert_eq!(budget_cap(), Some(far), "restore on drop");
                {
                    let _wider = BudgetCapGuard::install(far);
                    assert_eq!(budget_cap(), Some(far), "never widen past existing");
                }
            }
            assert_eq!(budget_cap(), None);
        })
        .await;
    }
}
