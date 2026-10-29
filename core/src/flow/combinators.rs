//! `when_all` / `when_any` — clean-room, resource-agnostic composition
//! They carry no resource-view or skip-on-missing semantics —
//! that stays application policy.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::flow::outcome::{Outcome, StopReason, Stopped};
use crate::flow::scope::TaskHandle;

/// Await every task; fail fast. The first failure (panic or cancellation)
/// aborts the remaining tasks and returns the error; on success the results
/// arrive in input order.
///
/// (Fail-fast is also enforced at the scope level: a panicking child has
/// already cancelled its scope unless supervised — this combinator
/// additionally hard-stops the still-running siblings it owns and returns
/// promptly.)
pub async fn when_all<T>(handles: Vec<TaskHandle<T>>) -> Outcome<Vec<T>> {
    WhenAll {
        slots: handles.into_iter().map(Slot::Pending).collect(),
        errors: Vec::new(),
    }
    .await
}

enum Slot<T> {
    Pending(TaskHandle<T>),
    Done(Option<T>),
}

struct WhenAll<T> {
    slots: Vec<Slot<T>>,
    /// `(input index, stop)` for every failed slot — the ERROR PICK is
    /// fault-outranks-cancel across ALL slots (C++ parity),
    /// never first-in-input-order: under FailFast the faulting child's
    /// cascade cancels its siblings, and a parked sibling at a lower
    /// input index can settle as `Cancelled(ChildFailed)` before the
    /// fault that CAUSED it is observed (the TCK's `when_all_fault` row
    /// caught exactly that on a slow runner).
    errors: Vec<(usize, Stopped)>,
}

impl<T> Unpin for WhenAll<T> {}

impl<T> Future for WhenAll<T> {
    type Output = Outcome<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = Pin::into_inner(self);
        let mut all_done = true;
        for i in 0..this.slots.len() {
            if let Slot::Pending(handle) = &mut this.slots[i] {
                match handle.poll_join(cx) {
                    Poll::Ready(Ok(value)) => this.slots[i] = Slot::Done(Some(value)),
                    Poll::Ready(Err(err)) => {
                        // Fail fast: hard-stop the rest, then keep DRAINING
                        // (aborted siblings settle promptly) so the final
                        // pick can prefer a fault wherever it appears.
                        if this.errors.is_empty() {
                            for slot in &this.slots {
                                if let Slot::Pending(h) = slot {
                                    h.abort();
                                }
                            }
                        }
                        this.errors.push((i, err));
                        this.slots[i] = Slot::Done(None);
                    }
                    Poll::Pending => all_done = false,
                }
            }
        }
        if all_done {
            if !this.errors.is_empty() {
                let pick = pick_when_all_error(&this.errors);
                return Poll::Ready(Err(this.errors.swap_remove(pick).1));
            }
            let results = this
                .slots
                .iter_mut()
                .map(|slot| match slot {
                    Slot::Done(value) => value.take().expect("polled after completion"),
                    Slot::Pending(_) => unreachable!("all_done implies no pending slot"),
                })
                .collect();
            Poll::Ready(Ok(results))
        } else {
            Poll::Pending
        }
    }
}

/// First successful task wins: its losers are aborted (hard-stopped at
/// their next await point — cancelled losers do not trip fail-fast).
/// Returns the winner's input index and value; errs only when EVERY task
/// failed or was cancelled (the
/// `Result` form makes the all-failed case honest instead of a panic).
///
/// CAVEAT — first COMPLETION wins, not first success: "successful" above
/// means the task did not stop (panic/cancel). If the task bodies return
/// `Result`, a fast branch completing with `Err` is a completion and WINS
/// over a slower sibling that was about to succeed (the `select` shape,
/// not `select_ok`; the in-task [`Scope::race`] has the same semantics).
/// A `when_any_ok`/`race_ok` companion that skips value-level failures is
/// planned.
pub async fn when_any<T>(handles: Vec<TaskHandle<T>>) -> Outcome<(usize, T)> {
    WhenAny {
        slots: handles.into_iter().map(Some).collect(),
        last_error: None,
    }
    .await
}

struct WhenAny<T> {
    slots: Vec<Option<TaskHandle<T>>>,
    last_error: Option<Stopped>,
}

impl<T> Unpin for WhenAny<T> {}

impl<T> Future for WhenAny<T> {
    type Output = Outcome<(usize, T)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = Pin::into_inner(self);
        let mut all_done = true;
        for i in 0..this.slots.len() {
            if let Some(handle) = &mut this.slots[i] {
                match handle.poll_join(cx) {
                    Poll::Ready(Ok(value)) => {
                        // First-wins: cancel the losers.
                        for slot in this.slots.iter().flatten() {
                            slot.abort();
                        }
                        return Poll::Ready(Ok((i, value)));
                    }
                    Poll::Ready(Err(err)) => {
                        this.last_error = Some(err);
                        this.slots[i] = None;
                    }
                    Poll::Pending => all_done = false,
                }
            }
        }
        if all_done {
            Poll::Ready(Err(this.last_error.take().unwrap_or_else(|| {
                Stopped::cancelled(crate::flow::CancelReason::Explicit)
            })))
        } else {
            Poll::Pending
        }
    }
}

// ============================================================
// In-task LAZY combinators (the Flow golden path): polled in
// the CALLER's task — borrowing legal, zero spawns. Every combinator
// observes the effective (shield-aware) stop view at its own poll
// boundary only, and registers the scope's cancel waker so a mid-flight
// cancel WAKES the race/join instead of gating its next poll.
//
// Constituents are plain futures: the only `Stopped` source is the SCOPE
// (cancel / budget). For fallible constituents that themselves return
// `Outcome`, `try_join` adds first-Stopped fail-fast (the C++ `when_all`
// shape). Loser/sibling teardown is Rust drop — for Aorta RPC futures,
// drop does NOT abort the router-side query (client.rs contract): racing
// non-idempotent RPCs is an application decision, same constraint culture
// as the RPC single-provider rule.

use std::future::poll_fn;

use crate::flow::ambient;
use crate::flow::scope::Scope;

/// Either side of a heterogeneous [`Scope::race2`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Poll the scope's effective stop view + keep the cancel waker
/// registered. `Some(stopped)` = resolve the combinator NOW.
fn poll_effective_stop(
    scope: &Scope,
    cancelled: &mut Pin<&mut aorta_sync::Cancelled>,
    cx: &mut Context<'_>,
) -> Option<Stopped> {
    // checkpoint() IS the effective view (budget fire + shield masking).
    if let Err(err) = scope.checkpoint() {
        return Some(err);
    }
    // Register/refresh the cancel waker so cancellation WAKES this task;
    // a Ready here re-runs the effective view (shield may mask it).
    if cancelled.as_mut().poll(cx).is_ready() {
        if let Err(err) = scope.checkpoint() {
            return Some(err);
        }
    }
    None
}

impl Scope {
    /// Await both futures IN THIS TASK (zero spawns, borrows legal);
    /// scope cancel / budget expiry drops both and resolves `Stopped`.
    pub async fn join<A: Future, B: Future>(
        &self,
        a: A,
        b: B,
    ) -> crate::flow::Outcome<(A::Output, B::Output)> {
        let mut cancelled = self.cancelled();
        tokio::pin!(a, b);
        let mut cancelled = Pin::new(&mut cancelled);
        let mut ra = None;
        let mut rb = None;
        poll_fn(move |cx| {
            if let Some(stopped) = poll_effective_stop(self, &mut cancelled, cx) {
                return Poll::Ready(Err(stopped));
            }
            if ra.is_none() {
                if let Poll::Ready(v) = a.as_mut().poll(cx) {
                    ra = Some(v);
                }
            }
            if rb.is_none() {
                if let Poll::Ready(v) = b.as_mut().poll(cx) {
                    rb = Some(v);
                }
            }
            if ra.is_some() && rb.is_some() {
                Poll::Ready(Ok((ra.take().unwrap(), rb.take().unwrap())))
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Three-way [`Scope::join`].
    pub async fn join3<A: Future, B: Future, C: Future>(
        &self,
        a: A,
        b: B,
        c: C,
    ) -> crate::flow::Outcome<(A::Output, B::Output, C::Output)> {
        let mut cancelled = self.cancelled();
        tokio::pin!(a, b, c);
        let mut cancelled = Pin::new(&mut cancelled);
        let mut ra = None;
        let mut rb = None;
        let mut rc = None;
        poll_fn(move |cx| {
            if let Some(stopped) = poll_effective_stop(self, &mut cancelled, cx) {
                return Poll::Ready(Err(stopped));
            }
            if ra.is_none() {
                if let Poll::Ready(v) = a.as_mut().poll(cx) {
                    ra = Some(v);
                }
            }
            if rb.is_none() {
                if let Poll::Ready(v) = b.as_mut().poll(cx) {
                    rb = Some(v);
                }
            }
            if rc.is_none() {
                if let Poll::Ready(v) = c.as_mut().poll(cx) {
                    rc = Some(v);
                }
            }
            if ra.is_some() && rb.is_some() && rc.is_some() {
                Poll::Ready(Ok((
                    ra.take().unwrap(),
                    rb.take().unwrap(),
                    rc.take().unwrap(),
                )))
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Await every future IN THIS TASK (zero spawns); results in input
    /// order; scope stop drops the whole set and resolves `Stopped`.
    pub async fn join_all<F: Future>(
        &self,
        futs: impl IntoIterator<Item = F>,
    ) -> crate::flow::Outcome<Vec<F::Output>> {
        let mut cancelled = self.cancelled();
        let mut cancelled = Pin::new(&mut cancelled);
        type Slot<F> = (std::pin::Pin<Box<F>>, Option<<F as Future>::Output>);
        let mut slots: Vec<Slot<F>> = futs.into_iter().map(|f| (Box::pin(f), None)).collect();
        poll_fn(move |cx| {
            if let Some(stopped) = poll_effective_stop(self, &mut cancelled, cx) {
                return Poll::Ready(Err(stopped));
            }
            let mut all_done = true;
            for (fut, slot) in slots.iter_mut() {
                if slot.is_none() {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(v) => *slot = Some(v),
                        Poll::Pending => all_done = false,
                    }
                }
            }
            if all_done {
                Poll::Ready(Ok(slots
                    .iter_mut()
                    .map(|(_, s)| s.take().expect("all done"))
                    .collect()))
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// [`Scope::join`] over FALLIBLE constituents: the first `Stopped`
    /// fail-fasts IMMEDIATELY — the sibling is dropped mid-flight, not
    /// awaited to completion (the C++ `when_all` shape). (Delegating to
    /// `join` would await BOTH before mapping — a 60s sibling would run
    /// its full course; caught by the fail-fast timing test.)
    pub async fn try_join<T, U, A, B>(&self, a: A, b: B) -> crate::flow::Outcome<(T, U)>
    where
        A: Future<Output = crate::flow::Outcome<T>>,
        B: Future<Output = crate::flow::Outcome<U>>,
    {
        let mut cancelled = self.cancelled();
        tokio::pin!(a, b);
        let mut cancelled = Pin::new(&mut cancelled);
        let mut ra: Option<T> = None;
        let mut rb: Option<U> = None;
        poll_fn(move |cx| {
            if let Some(stopped) = poll_effective_stop(self, &mut cancelled, cx) {
                return Poll::Ready(Err(stopped));
            }
            if ra.is_none() {
                if let Poll::Ready(r) = a.as_mut().poll(cx) {
                    match r {
                        Ok(v) => ra = Some(v),
                        Err(stopped) => return Poll::Ready(Err(stopped)),
                    }
                }
            }
            if rb.is_none() {
                if let Poll::Ready(r) = b.as_mut().poll(cx) {
                    match r {
                        Ok(v) => rb = Some(v),
                        Err(stopped) => return Poll::Ready(Err(stopped)),
                    }
                }
            }
            if ra.is_some() && rb.is_some() {
                Poll::Ready(Ok((ra.take().unwrap(), rb.take().unwrap())))
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// First ready wins; the loser is dropped IN PLACE (Rust drop = its
    /// cancellation — see the module note for the RPC-loser contract).
    ///
    /// CAVEAT — first COMPLETION wins: the first constituent to resolve
    /// wins regardless of the value it resolved with — a branch completing
    /// with an application-level `Err` value beats a slower sibling that
    /// was about to succeed (the `select` shape, not `select_ok`). A
    /// `race_ok` companion that skips value-level failures is planned.
    pub async fn race<T, A, B>(&self, a: A, b: B) -> crate::flow::Outcome<T>
    where
        A: Future<Output = T>,
        B: Future<Output = T>,
    {
        match self.race2(a, b).await {
            Ok(Either::Left(v)) | Ok(Either::Right(v)) => Ok(v),
            Err(stopped) => Err(stopped),
        }
    }

    /// Heterogeneous [`Scope::race`]: which side won is part of the value.
    /// Same first-completion-wins caveat as [`Scope::race`].
    pub async fn race2<A: Future, B: Future>(
        &self,
        a: A,
        b: B,
    ) -> crate::flow::Outcome<Either<A::Output, B::Output>> {
        let mut cancelled = self.cancelled();
        tokio::pin!(a, b);
        let mut cancelled = Pin::new(&mut cancelled);
        poll_fn(move |cx| {
            if let Some(stopped) = poll_effective_stop(self, &mut cancelled, cx) {
                return Poll::Ready(Err(stopped));
            }
            if let Poll::Ready(v) = a.as_mut().poll(cx) {
                return Poll::Ready(Ok(Either::Left(v)));
            }
            if let Poll::Ready(v) = b.as_mut().poll(cx) {
                return Poll::Ready(Ok(Either::Right(v)));
            }
            Poll::Pending
        })
        .await
    }

    /// Bound `fut` to `min(d, remaining budget)` IN THIS TASK. Expiry
    /// resolves `Stopped(DeadlineExceeded)` WITHOUT cancelling the scope
    /// (window-local). Every inner poll runs under a poll-scoped ambient
    /// budget cap: nested combinators and `call_in`
    /// observe the narrowed deadline; the cap never widens and never
    /// crosses `spawn`.
    pub async fn timeout<F: Future>(
        &self,
        d: std::time::Duration,
        fut: F,
    ) -> crate::flow::Outcome<F::Output> {
        use crate::flow::CancelReason;
        // The timer is bounded to min(window, scope
        // budget) — the Budget is observed-not-delivered (no wake of its
        // own), so without this a plain-tokio caller with a 5 ms budget
        // and a 60 s window sleeps the full window (run_for's
        // pinned semantics, ported on deletion).
        let remaining = self.remaining();
        let budget_bound = matches!(remaining, Some(r) if r < d);
        let bound = remaining.map_or(d, |r| r.min(d));
        let cap = std::time::Instant::now() + bound;
        let mut cancelled = self.cancelled();
        let sleep = tokio::time::sleep(bound);
        tokio::pin!(fut, sleep);
        let mut cancelled = Pin::new(&mut cancelled);
        poll_fn(move |cx| {
            if let Some(stopped) = poll_effective_stop(self, &mut cancelled, cx) {
                return Poll::Ready(Err(stopped));
            }
            if sleep.as_mut().poll(cx).is_ready() {
                if budget_bound {
                    // The BUDGET was the limiting bound: observe the expiry
                    // exactly like checkpoint() (fires the token + arms the
                    // stall watchdog) — and REPORT checkpoint's actual
                    // reason (an already-explicit cancel must not be
                    // relabelled DeadlineExceeded in the narrow race).
                    if let Err(err) = self.checkpoint() {
                        return Poll::Ready(Err(err));
                    }
                }
                return Poll::Ready(Err(Stopped::cancelled(CancelReason::DeadlineExceeded)));
            }
            let _cap = ambient::BudgetCapGuard::install(cap);
            fut.as_mut().poll(cx).map(Ok)
        })
        .await
    }
}

// ================================================================
/// Cancellation shield (cleanup that must run under cancellation): while
/// polled, `fut` is immune to COOPERATIVE cancellation — `checkpoint()`,
/// `run_until_cancelled`, the in-task combinators, and the spawn dispatch
/// boundary all see an un-stopped scope. The BUDGET is not maskable
/// (deadline stays a hard bound, piercing shields). Shields do not cross
/// `spawn` (a child task gets a fresh ambient), and the raw token surface
/// (`is_cancelled()/cancelled()/token()`) stays shield-transparent BY
/// DESIGN (the advanced/interop surface).
#[must_use = "a shield does nothing unless awaited/polled (lazy-enter)"]
pub struct Shield<F> {
    fut: F,
    /// Holds +1 on the task's live-shield count from first poll until
    /// resolution/drop (the DISPATCH-BOUNDARY view); the POLL-scoped
    /// count is a per-poll RAII guard below.
    entered: bool,
}

impl<F> Shield<F> {
    pub(crate) fn new(fut: F) -> Self {
        Self {
            fut,
            entered: false,
        }
    }

    fn release(&mut self) {
        if self.entered {
            ambient::exit_live_shield();
            self.entered = false;
        }
    }
}

impl<F: Future> Future for Shield<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: structural pin projection to `fut` only; `fut` is never
        // moved out; Drop below touches only the Unpin bookkeeping field.
        let this = unsafe { self.get_unchecked_mut() };
        if !this.entered {
            // Lazy: a constructed-but-never-polled Shield has no effect;
            // outside a flow task there is no ambient (degrade to the pre-Flow behavior).
            this.entered = ambient::enter_live_shield();
        }
        let polled = {
            let _poll_guard = ambient::PollShieldGuard::enter();
            // SAFETY: see above.
            unsafe { Pin::new_unchecked(&mut this.fut) }.poll(cx)
        };
        if polled.is_ready() {
            // Eager exit on resolution: a Shield held (not yet dropped)
            // after completion must not keep masking the task.
            this.release();
        }
        polled
    }
}

impl<F> Drop for Shield<F> {
    fn drop(&mut self) {
        self.release();
    }
}

impl Scope {
    /// Shield `fut` from cooperative cancellation for its duration —
    /// THE cleanup primitive (`scope.shield(flush()).await`). See
    /// [`Shield`] for the exact masking contract (budget still applies).
    #[must_use = "a shield does nothing unless awaited/polled (lazy-enter)"]
    pub fn shield<F: Future>(&self, fut: F) -> Shield<F> {
        Shield::new(fut)
    }
}

/// `when_all`'s error pick: fault outranks cancel; ties (fault-vs-fault
/// OR cancel-vs-cancel) break by INPUT INDEX — C++ parity
/// (flow_combinators.hpp: iterates i=0..N and keeps the FIRST fault it
/// sees, else the first cancel). `errors` need not be sorted by index —
/// entries accumulate in OBSERVATION order across possibly multiple
/// `poll()` calls, which need not match input order (a prior
/// version picked the first-OBSERVED fault
/// instead of the lowest-INDEX fault). Extracted as a pure function so
/// the tie-break can be unit-tested directly against adversarial
/// observation orders, independent of any real async timing race.
fn pick_when_all_error(errors: &[(usize, Stopped)]) -> usize {
    let has_fault = errors
        .iter()
        .any(|(_, e)| matches!(e.reason(), StopReason::Fault { .. }));
    errors
        .iter()
        .enumerate()
        .filter(|(_, (_, e))| !has_fault || matches!(e.reason(), StopReason::Fault { .. }))
        .min_by_key(|(_, (idx, _))| *idx)
        .map(|(pos, _)| pos)
        .expect("errors is non-empty")
}

#[cfg(test)]
mod pick_when_all_error_tests {
    use super::*;
    use aorta_sync::CancelReason;

    #[test]
    fn single_fault_wins_regardless_of_position() {
        let errors = vec![(3, Stopped::fault("only fault"))];
        assert_eq!(pick_when_all_error(&errors), 0);
    }

    #[test]
    fn fault_outranks_cancel_even_when_cancel_is_observed_first() {
        // Cancel (index 5) pushed BEFORE fault (index 2) — observation
        // order, not input order.
        let errors = vec![
            (5, Stopped::cancelled(CancelReason::ChildFailed)),
            (2, Stopped::fault("the real cause")),
        ];
        let pick = pick_when_all_error(&errors);
        assert_eq!(errors[pick].0, 2, "the fault must win regardless of index");
    }

    #[test]
    fn two_faults_pick_the_lowest_input_index_not_the_observation_order() {
        // Regression: index 7's fault is
        // OBSERVED first (pushed to errors[0]); index 1's fault settles
        // later (pushed to errors[1]). The pick must still be index 1 —
        // C++ parity iterates by input index and keeps the first fault.
        let errors = vec![
            (7, Stopped::fault("high index, observed first")),
            (1, Stopped::fault("low index, observed second")),
        ];
        let pick = pick_when_all_error(&errors);
        assert_eq!(
            errors[pick].0, 1,
            "must pick the LOWEST input index among faults, not the first-observed one"
        );
    }

    #[test]
    fn two_cancels_pick_the_lowest_input_index() {
        let errors = vec![
            (4, Stopped::cancelled(CancelReason::Shutdown)),
            (0, Stopped::cancelled(CancelReason::Explicit)),
        ];
        let pick = pick_when_all_error(&errors);
        assert_eq!(errors[pick].0, 0);
    }

    #[test]
    fn three_faults_out_of_order_pick_the_minimum_index() {
        let errors = vec![
            (5, Stopped::fault("mid")),
            (9, Stopped::fault("high")),
            (2, Stopped::fault("low — must win")),
        ];
        let pick = pick_when_all_error(&errors);
        assert_eq!(errors[pick].0, 2);
    }
}
