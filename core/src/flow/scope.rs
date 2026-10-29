//! `Scope` — the lifetime facet of Aorta Flow.
//!
//! A `Scope` owns child tasks, a [`CancelToken`], and a [`Budget`]. The scope
//! rules, as implemented here:
//!
//! 1. **No orphan tasks**: [`Scope::close`] awaits every spawned child;
//!    every child holds the scope alive while it runs.
//! 2. **Cancel propagates down** through the token tree ([`Scope::child`]
//!    creates a child token; [`Scope::cancel`] cascades).
//! 3. **Budget expiry is observed, not delivered**: no timer thread; the
//!    first [`Scope::checkpoint`] (or spawn/`call_in` entry) that observes
//!    expiry fires the token with [`CancelReason::DeadlineExceeded`].
//! 4. **A child may narrow, never widen** (budget min-ed, priority clamped).
//! 5. **Fail-fast**: a panicking child cancels its owning scope (reason
//!    [`CancelReason::ChildFailed`]) — unless the scope is `supervised`, in
//!    which case the failure surfaces only on that child's [`TaskHandle`].
//!    A child that is cancelled/aborted does NOT trip fail-fast
//!    (cancellation is not an error). Upward propagation across scopes is
//!    cooperative: a parent observes a child scope's failure where it joins
//!    it (`when_all` / `TaskHandle::join`), never via an upward cancel.
//!
//! Execution substrate: children run as plain tokio tasks on the
//! ambient runtime (actions already run off-executor);
//! resumption delivery rides the executor's `CoroutineResume`
//! lane without changing this API.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::event_queue::{priority_index, EventPriority};
use crate::flow::outcome::{Outcome, Stopped};
use crate::flow::{Budget, CancelReason, CancelToken};
use aorta_sync::InFlightCounter;
use tokio::sync::Notify;

/// Creation options. Every field inherits/defaults when unset:
/// `budget: None` inherits the parent's budget unchanged (`Some(d)` narrows
/// to `min(parent remaining, d)`); `priority: None` inherits the parent's
/// priority (`Some(p)` clamps to `min(parent, p)` — never raises).
///
/// Construct via [`Default`] plus the chainable setters —
/// `ScopeOpts::named("nav").budget(d).priority(p)` — the struct is
/// `#[non_exhaustive]`, so full literals are reserved for future growth.
/// Fault-supervision policy. Applies to
/// child PANICS (faults) only — cooperative cancellation is never a
/// fault and never restarts anything.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Supervision {
    /// A child fault cancels the scope (`ChildFailed`) — the pre-Flow default,
    /// now named.
    #[default]
    FailFast,
    /// A fault resolves THAT handle `Stopped(Fault)`; siblings continue
    /// (the legacy `supervised` flag).
    Isolate,
    /// [`Scope::spawn_supervised`] children restart on fault with
    /// exponential backoff: up to `max_restarts` within `window`;
    /// exhaustion escalates to FailFast. Plain `spawn` children behave
    /// as Isolate under this policy.
    ///
    /// The window is TUMBLING, anchored at the first fault after a quiet
    /// stretch: a fault arriving more than `window` after the anchor
    /// resets both the anchor and the count. A sustained fault rate just
    /// above the cap therefore keeps restarting (bounded at
    /// `max_restarts + 1` per window) instead of escalating — pick
    /// `window`/`max_restarts` for the burst profile you want to stop.
    /// The `factory` is re-invoked after a fault: captures must tolerate
    /// re-entry after a mid-body panic (`FnMut` + unwind — keep restart
    /// state either fully owned per attempt or fault-safe). A `backoff`
    /// of zero restarts immediately (no floor is imposed).
    Restart {
        max_restarts: u32,
        window: Duration,
        backoff: Duration,
    },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default)]
pub struct ScopeOpts {
    /// `None` = inherit the parent's priority (clamped, never raised).
    pub priority: Option<EventPriority>,
    pub budget: Option<Duration>,
    /// Fail-fast escape hatch: an error inside a supervised scope
    /// terminates that child only, cancelling neither siblings nor parents.
    pub supervision: Supervision,
    pub name: &'static str,
}

impl ScopeOpts {
    /// Start from defaults with a (static) diagnostic name.
    pub fn named(name: &'static str) -> Self {
        Self {
            name,
            ..Self::default()
        }
    }

    /// Narrow the budget to `min(parent remaining, budget)`.
    pub fn budget(mut self, budget: Duration) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Request a priority (clamped to the parent's — never raises).
    pub fn priority(mut self, priority: EventPriority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Opt out of fail-fast: failures stay contained to this child.
    /// Shorthand for `Supervision::Isolate` (the legacy `supervised` flag).
    pub fn supervised(mut self) -> Self {
        self.supervision = Supervision::Isolate;
        self
    }

    /// Set the fault-supervision policy.
    pub fn supervision(mut self, policy: Supervision) -> Self {
        self.supervision = policy;
        self
    }
}

/// close() watchdog cadence (the close-watchdog): only ticks on an already-stuck close.
const CLOSE_WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);

tokio::task_local! {
    /// The scopes whose child bodies the CURRENT tokio task is inside
    /// (ScopeInner addresses) — powers close()'s self-close diagnosis.
    static SCOPE_STACK: std::cell::RefCell<Vec<usize>>;
}

const CHILD_ACTIVE: u8 = 0;
const CHILD_DETACHED: u8 = 1;
const CHILD_DEPARTED: u8 = 2;

/// one slot per spawned task. The single 3-state atomic
/// makes the in_flight/detached accounting race-free: exactly one side
/// wins each transition, so no counter is ever double-decremented.
pub(crate) struct ChildSlot {
    state: std::sync::atomic::AtomicU8,
    inner: Arc<ScopeInner>,
}

impl ChildSlot {
    fn depart(&self) {
        match self
            .state
            .swap(CHILD_DEPARTED, std::sync::atomic::Ordering::AcqRel)
        {
            CHILD_ACTIVE => self.inner.task_departed(),
            CHILD_DETACHED => {
                self.inner
                    .detached
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
            _ => {} // defensive: depart is called exactly once (Drop)
        }
    }
}

pub(crate) struct ScopeInner {
    /// stall-WARN one-shot latch (armed on first cancel observation).
    stall_warn_armed: std::sync::atomic::AtomicBool,
    /// stragglers detached at grace expiry — they no
    /// longer block close; their eventual departure decrements THIS.
    detached: std::sync::atomic::AtomicU64,
    /// per-task slots (Weak — the Depart guard owns the Arc) so a
    /// grace-expired shutdown can detach live children. Pruned
    /// opportunistically on registration.
    task_slots: parking_lot::Mutex<Vec<std::sync::Weak<ChildSlot>>>,
    token: CancelToken,
    budget: Budget,
    priority: EventPriority,
    supervision: Supervision,
    name: &'static str,
    /// Live children; `close()` parks on `idle_notify` until it drains
    /// (register-then-recheck, the loom-verified `InFlightCounter` edge).
    in_flight: InFlightCounter,
    idle_notify: Notify,
    /// Set when `close()` begins (one-way): bars NEW children, so a spawn
    /// racing the drain can never be orphaned past a returned `close()`
    /// (the wait-only form admitted spawn-after-close).
    closing: std::sync::atomic::AtomicBool,
    /// Count of USER `Scope` handles (clones), for the dropped-without-
    /// close warning. ALWAYS-ON (robots run release builds and
    /// this was the one guarantee debug-only): one relaxed RMW per Scope
    /// clone/drop — cold path.
    user_handles: std::sync::atomic::AtomicUsize,
    /// Per-scope trace ids, interned ONCE at
    /// first use (trace-on only) — the open/close instants used to
    /// format!+intern through the global RwLock on EVERY emission.
    trace_ids: std::sync::OnceLock<ScopeTraceIds>,
    /// Tree links: `close()` must account the WHOLE
    /// subtree — a task spawned on a child scope must not outlive the
    /// parent's `close()`, and a child must refuse new spawns once any
    /// ancestor is closing. `parent` is walked upward for idle
    /// notifications; `children` (Weak, pruned on traversal) is walked
    /// downward for the closing bar and the subtree count.
    ///
    /// `parent` is STRONG: a live
    /// descendant — task or scope — must keep every ancestor's accounting
    /// node alive. With a Weak up-link, dropping a pass-through
    /// intermediate `Scope` handle (the canonical nested-`.child()` idiom)
    /// deallocated its inner and silently pruned the whole live subtree
    /// out of every ancestor's `close()`/`shutdown_with_grace()`
    /// accounting. Strong up-links cannot cycle: the downward edge stays
    /// Weak, so an inner is freed once its handles, tasks, and children
    /// are gone — ancestors then unpin naturally, leaf-to-root.
    parent: Option<Arc<ScopeInner>>,
    children: parking_lot::Mutex<Vec<std::sync::Weak<ScopeInner>>>,
}

/// The lifetime facet: owns children + a cancel token + a budget. Clones
/// share the same scope.
pub struct Scope {
    pub(crate) inner: Arc<ScopeInner>,
}

// Manual Clone/Drop (not derived): USER handles are counted in ALL builds
// (the drop warning below is deliberately always-on) so the last user
// drop can warn about live-but-never-closed children. Task-held
// `Arc<ScopeInner>`s never touch the counter, so the
// signal fires exactly when the application loses its last way to close
// the scope. `mem::forget`/cycles are out of scope (no Drop runs).
impl Clone for Scope {
    fn clone(&self) -> Self {
        self.inner
            .user_handles
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ScopeInner {
    /// One task's exit: decrement, and on the 1→0 edge wake THIS scope's
    /// closer plus every ancestor's (a parent `close()` parks on its own
    /// `idle_notify` while counting the subtree — the LAST counter to hit
    /// zero anywhere below it triggers the final recheck).
    fn task_departed(&self) {
        if self.in_flight.decr() {
            // The idle edge is the natural moment to drop dead
            // child Weaks for the "children churned, none created since"
            // profile the push-time sweep misses. Cold path (1→0 only).
            self.children.lock().retain(|weak| weak.strong_count() > 0);
            self.idle_notify.notify_waiters();
            let mut up = self.parent.as_deref();
            while let Some(ancestor) = up {
                ancestor.idle_notify.notify_waiters();
                up = ancestor.parent.as_deref();
            }
        }
    }

    /// Tasks in flight in THIS scope plus every live descendant (dead
    /// child entries are pruned in passing). Tree-shaped: locks are only
    /// taken parent→child, so no ordering hazard.
    pub(crate) fn subtree_in_flight(&self) -> u64 {
        let mut total = self.in_flight.get();
        let mut children = self.children.lock();
        children.retain(|weak| {
            let Some(child) = weak.upgrade() else {
                return false;
            };
            total += child.subtree_in_flight();
            true
        });
        total
    }

    /// One-way closing bar over the WHOLE subtree: new spawns (and new
    /// external children) are refused everywhere below, so nothing can be
    /// orphaned past the parent's returned `close()`.
    /// detach every still-ACTIVE child task in the subtree (grace
    /// expiry). The per-child 3-state atomic arbiters the accounting
    /// exactly once per side: our CAS win moves the child
    /// in_flight→detached; a concurrent real departure wins the swap and
    /// we skip it. Returns the number detached.
    pub(crate) fn detach_subtree(self: &Arc<Self>) -> u64 {
        let mut detached = 0u64;
        {
            let mut slots = self.task_slots.lock();
            slots.retain(|weak| {
                let Some(slot) = weak.upgrade() else {
                    return false;
                };
                // Transient underflow: +1 BEFORE the CAS so a
                // child that departs immediately after our win can never
                // fetch_sub below zero; roll back on CAS failure (a brief
                // +1 over-report is benign, u64::MAX on an observability
                // surface is not).
                self.detached
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                if slot
                    .state
                    .compare_exchange(
                        CHILD_ACTIVE,
                        CHILD_DETACHED,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                    )
                    .is_ok()
                {
                    // Release the close()-blocking registration (notifies).
                    self.task_departed();
                    detached += 1;
                } else {
                    self.detached
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                }
                true
            });
        }
        let children: Vec<Arc<ScopeInner>> = {
            let children = self.children.lock();
            children.iter().filter_map(|w| w.upgrade()).collect()
        };
        for child in children {
            detached += child.detach_subtree();
        }
        detached
    }

    pub(crate) fn detached_now(&self) -> u64 {
        self.detached.load(std::sync::atomic::Ordering::Acquire)
    }

    fn mark_closing_subtree(&self) {
        self.closing
            .store(true, std::sync::atomic::Ordering::Release);
        let mut children = self.children.lock();
        children.retain(|weak| {
            let Some(child) = weak.upgrade() else {
                return false;
            };
            child.mark_closing_subtree();
            true
        });
    }

    /// Lazily interned per-scope trace ids (named scopes; trace-on path).
    fn trace_ids(&self) -> ScopeTraceIds {
        *self.trace_ids.get_or_init(|| ScopeTraceIds {
            open: crate::trace::intern_event(&format!("flow.scope_open:{}", self.name)),
            closed: crate::trace::intern_event(&format!("flow.scope_closed:{}", self.name)),
        })
    }

    /// Test probe (dead-Weak pruning regression): registered child entries,
    /// including dead Weaks.
    #[cfg(test)]
    pub(crate) fn debug_scope_children_len(&self) -> usize {
        self.children.lock().len()
    }

    /// Introspection snapshot .
    pub(crate) fn snapshot(&self) -> crate::flow::introspect::ScopeInfo {
        crate::flow::introspect::ScopeInfo {
            name: self.name,
            live_children: self.in_flight.get(),
            detached: self.detached_now(),
            cancelled: self.token.reason(),
            budget_remaining: self.budget.remaining(),
            closing: self.closing.load(std::sync::atomic::Ordering::Acquire),
            user_handles: self.user_handles.load(std::sync::atomic::Ordering::Acquire),
        }
    }

    /// The drop-warning decision, factored PURE so tests assert the logic
    /// directly ("visually verified" is not a test): `Some(n)`
    /// iff a last-user-handle drop right now would orphan `n` live children.
    pub(crate) fn would_orphan_on_last_drop(&self) -> Option<u64> {
        let live = self.in_flight.get();
        if !self.closing.load(std::sync::atomic::Ordering::Acquire) && live > 0 {
            Some(live)
        } else {
            None
        }
    }

    /// Test-only visibility of the user-handle count.
    pub(crate) fn debug_user_handles(&self) -> usize {
        self.user_handles.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        {
            let remaining = self
                .inner
                .user_handles
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            if remaining == 1 && self.inner.would_orphan_on_last_drop().is_some() {
                crate::aorta_core_warn!(
                    "Aorta WARN: scope '{}' dropped with {} live children and no close() — \
                     they are orphaned past the structured-concurrency contract \
                     (call scope.shutdown(..) or scope.close().await)",
                    self.inner.name,
                    self.inner.in_flight.get()
                );
            }
        }
    }
}

impl std::fmt::Debug for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scope")
            .field("name", &self.inner.name)
            .field("cancelled", &self.is_cancelled())
            .field("live_children", &self.inner.in_flight.get())
            .finish()
    }
}

impl Scope {
    /// A fresh root scope.
    pub fn new_root(opts: ScopeOpts) -> Self {
        Self::build(
            CancelToken::new_root(),
            match opts.budget {
                Some(d) => Budget::with_deadline(d),
                None => Budget::unlimited(),
            },
            opts,
        )
    }

    /// A scope whose cancel parent is an EXISTING [`CancelToken`] — the
    /// one-way bridge from the raw token tree into the scope world
    /// (teams that adopted raw tokens for process lifecycle can
    /// hang scope trees off them without re-plumbing). Cancelling the token
    /// cascades into this scope; the scope's own cancel never touches the
    /// token's other children.
    pub fn child_of_token(token: &CancelToken, opts: ScopeOpts) -> Self {
        Self::build(
            token.child(),
            match opts.budget {
                Some(d) => Budget::with_deadline(d),
                None => Budget::unlimited(),
            },
            opts,
        )
    }

    /// Root a scope on an existing token/budget (the action-as-Scope hook:
    /// the per-goal cancel state backs the token, the goal deadline the
    /// budget).
    pub(crate) fn with_token(token: CancelToken, budget: Budget, opts: ScopeOpts) -> Self {
        Self::build(token, budget, opts)
    }

    fn build_linked(
        token: CancelToken,
        budget: Budget,
        opts: ScopeOpts,
        parent: Option<&Arc<ScopeInner>>,
    ) -> Self {
        let inner = Arc::new(ScopeInner {
            stall_warn_armed: std::sync::atomic::AtomicBool::new(false),
            detached: std::sync::atomic::AtomicU64::new(0),
            task_slots: parking_lot::Mutex::new(Vec::new()),
            token,
            budget,
            // Resolved by child(); at a root, None = Normal.
            priority: opts.priority.unwrap_or(EventPriority::Normal),
            supervision: opts.supervision,
            name: opts.name,
            in_flight: InFlightCounter::new(),
            idle_notify: Notify::new(),
            closing: std::sync::atomic::AtomicBool::new(false),
            user_handles: std::sync::atomic::AtomicUsize::new(1),
            trace_ids: std::sync::OnceLock::new(),
            parent: parent.map(Arc::clone),
            children: parking_lot::Mutex::new(Vec::new()),
        });
        if let Some(parent) = parent {
            let mut children = parent.children.lock();
            children.push(Arc::downgrade(&inner));
            // Long-lived scopes that never close
            // (the node root) used to pin one dead Weak per churned child
            // forever — the only prunes lived on close()-reachable paths.
            // Amortized O(1): sweep when len is a power of two ≥ 8, while
            // the lock is already held. A registry that stops growing
            // stops being scanned.
            let len = children.len();
            if len >= 8 && len & (len - 1) == 0 {
                children.retain(|weak| weak.strong_count() > 0);
            }
            drop(children);
            // Race pin (push-then-check): a parent close() that marked the
            // subtree BEFORE our push missed us — its closing flag is
            // already visible, so we're born closing. Either interleaving
            // leaves the bar intact.
            if parent.closing.load(std::sync::atomic::Ordering::Acquire) {
                inner
                    .closing
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        // Open marker for NAMED scopes (ids interned once per scope —
        // anonymous scopes skip = zero cost).
        if !inner.name.is_empty() && crate::trace::flow_trace_active() {
            crate::trace::flow_instant(inner.trace_ids().open);
        }
        // one OnceLock::get() load until introspection is enabled.
        crate::flow::introspect::register(&inner);
        Self { inner }
    }

    fn build(token: CancelToken, budget: Budget, opts: ScopeOpts) -> Self {
        Self::build_linked(token, budget, opts, None)
    }

    /// A child scope: child token, narrowed budget, clamped (never raised)
    /// priority.
    /// [`spawn`](Self::spawn), minus the clone-before-move dance: the
    /// closure receives an OWNED clone of this scope, ready to be moved
    /// into the async body for `checkpoint`/`run_until_cancelled` calls.
    ///
    /// ```ignore
    /// let handle = scope.spawn_with(|scope| async move {
    ///     loop {
    ///         scope.checkpoint()?;
    ///         step().await;
    ///     }
    /// });
    /// ```
    #[must_use = "the returned TaskHandle carries the child's outcome"]
    pub fn spawn_with<T, F, Fut>(&self, f: F) -> TaskHandle<T>
    where
        F: FnOnce(Scope) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // The body receives a REAL child scope (the doc
        // always said "child scope"; handing `self.clone()` meant
        // abort() had no subtree of its own to cancel and nested
        // children the body created were orphaned warn-only). The
        // handle keeps a clone so abort() can cancel the subtree.
        let task_scope = self.child(ScopeOpts::default());
        let mut handle = self.spawn(f(task_scope.clone()));
        handle.task_scope = Some(task_scope);
        handle
    }

    pub fn child(&self, opts: ScopeOpts) -> Scope {
        let budget = match opts.budget {
            Some(d) => self.inner.budget.narrow(d),
            None => self.inner.budget,
        };
        // `None` inherits; `Some(p)` clamps to the parent (never raises).
        // The ordering compares canonical `priority_index` values: a
        // HIGHER index means a LOWER priority, so `>=` keeps only children
        // at or below the parent's level.
        let priority = match opts.priority {
            Some(p) if priority_index(p) >= priority_index(self.inner.priority) => p,
            _ => self.inner.priority,
        };
        Self::build_linked(
            self.inner.token.child(),
            budget,
            ScopeOpts {
                priority: Some(priority),
                ..opts
            },
            Some(&self.inner),
        )
    }

    pub fn name(&self) -> &'static str {
        self.inner.name
    }

    pub fn priority(&self) -> EventPriority {
        self.inner.priority
    }

    /// Cancel this scope and every descendant (one-way, eager).
    pub fn cancel(&self, reason: CancelReason) {
        arm_stall_warn(&self.inner);
        // Observability: cancel + reason as an aorta.flow instant (event
        // names are the attribute channel; the reason set is static).
        if crate::trace::flow_trace_active() {
            let ids = flow_fixed_ids();
            crate::trace::flow_instant(match reason {
                CancelReason::Explicit => ids.cancel_explicit,
                CancelReason::DeadlineExceeded => ids.cancel_deadline,
                CancelReason::Shutdown => ids.cancel_shutdown,
                CancelReason::ChildFailed => ids.cancel_child_failed,
                _ => ids.cancel_other,
            });
        }
        self.inner.token.cancel(reason);
    }

    /// RT-safe poll: one atomic load.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    pub fn cancel_reason(&self) -> Option<CancelReason> {
        self.inner.token.reason()
    }

    /// Cooperative wait for cancellation.
    pub fn cancelled(&self) -> aorta_sync::Cancelled {
        self.inner.token.cancelled()
    }

    /// The scope's cancel token (e.g. to hand to an external-bus thread for
    /// RT-safe polling).
    pub fn token(&self) -> CancelToken {
        self.inner.token.clone()
    }

    /// RT-safe budget poll: pure read, does NOT observe expiry
    /// into the token — pair with [`Scope::is_cancelled`].
    #[inline]
    pub fn remaining(&self) -> Option<Duration> {
        self.inner.budget.remaining()
    }

    pub(crate) fn budget(&self) -> Budget {
        self.inner.budget
    }

    /// Cooperative checkpoint: observes budget expiry (the
    /// first observer fires the token with `DeadlineExceeded`), then
    /// reports cancellation. Call at loop heads / before long operations.
    pub fn checkpoint(&self) -> Outcome<()> {
        if self.inner.budget.is_expired() && !self.inner.token.is_cancelled() {
            // Observability: the budget-expiry FIRE is a distinct fact
            // from the cancel it causes.
            if crate::trace::flow_trace_active() {
                crate::trace::flow_instant(flow_fixed_ids().budget_expired);
            }
            self.inner.token.cancel(CancelReason::DeadlineExceeded);
            // Review fix: a checkpoint-fired deadline cancel must arm the
            // stall watchdog like every other cancel entry point — a
            // sibling mid-poll staller would otherwise go unreported.
            arm_stall_warn(&self.inner);
        }
        // Shield-aware effective view: inside a shield,
        // cooperative cancellation is masked — but the budget stays a hard
        // bound (deadline pierces shields;  "budget is NOT
        // shield-maskable").
        if crate::flow::ambient::poll_shielded() {
            if self.inner.budget.is_expired() {
                // Report the token's ACTUAL recorded reason when one exists
                // (e.g. an unrelated Shutdown that landed before the budget
                // was observed) — same rule as DispatchBoundary::poll().
                return Err(Stopped::cancelled(
                    self.inner
                        .token
                        .reason()
                        .unwrap_or(CancelReason::DeadlineExceeded),
                ));
            }
            return Ok(());
        }
        match self.inner.token.reason() {
            Some(reason) => Err(Stopped::cancelled(reason)),
            None => Ok(()),
        }
    }

    /// Spawn a child task (requires an AMBIENT tokio runtime — panics
    /// otherwise, exactly like `tokio::spawn`; foreign threads must enter a
    /// runtime `Handle` first or drive the C-ABI surface instead).
    ///
    /// The canonical cancellable worker (six lines, no manual clone, no
    /// manual cancel/close pairing):
    ///
    /// ```ignore
    /// let scope = node.root_scope().child(ScopeOpts::named("worker"));
    /// let handle = scope.spawn_with(|scope| async move {
    ///     while let Some(msg) = scope.run_until_cancelled(rx.recv()).await {
    ///         process(msg?).await;
    ///     }
    /// });
    /// scope.shutdown(CancelReason::Shutdown).await; // cancel + drain
    /// ```
    ///
    /// The child:
    /// - holds the scope open until it finishes (rule 1);
    /// - observes cancellation cooperatively via the scope it captures;
    /// - on panic, cancels the scope (fail-fast, rule 5) unless the scope
    ///   is supervised; the outcome always surfaces on the [`TaskHandle`].
    ///
    /// Spawning on an expired budget fires the token first (rule 3); the
    /// child is then born into a cancelled scope and should exit at its
    /// first checkpoint.
    #[must_use = "the returned TaskHandle carries the child's outcome"]
    pub fn spawn<T, F>(&self, fut: F) -> TaskHandle<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let _ = self.checkpoint();
        if self
            .inner
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
        {
            // The scope is closing: the future is NOT run (running it would
            // orphan it past `close()`); the failure surfaces on the handle.
            drop(fut);
            let join = tokio::spawn(async { Err(Stopped::cancelled(CancelReason::Shutdown)) });
            return TaskHandle {
                join,
                scope: self.clone(),
                task_scope: None,
            };
        }
        self.inner.in_flight.incr();
        // Re-check AFTER registering: a `close()` that began between the
        // check above and the incr sees our registration (its drain waits
        // for us) OR we see its flag here and withdraw — either way no
        // child slips past a returned close().
        if self
            .inner
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.inner.task_departed();
            drop(fut);
            let join = tokio::spawn(async { Err(Stopped::cancelled(CancelReason::Shutdown)) });
            return TaskHandle {
                join,
                scope: self.clone(),
                task_scope: None,
            };
        }
        // Task-spawn counter (trace-gated; one branch when off).
        if crate::trace::flow_trace_active() {
            crate::trace::flow_counter(flow_fixed_ids().tasks_spawned, 1);
        }
        let inner = Arc::clone(&self.inner);
        let scope_addr = Arc::as_ptr(&self.inner) as usize;
        // The SCOPE_STACK task-local wraps the ENTIRE spawned body (the close-watchdog
        // pin): close()'s self-close diagnosis reads it. Nested spawns
        // accumulate (a child of a child sees both ancestors).
        // Departure guard: releases on EVERY exit (return, panic, abort)
        // and wakes `close()` on the 1→0 edge. Constructed HERE (not inside
        // the async body) and moved in: a body dropped before its FIRST
        // poll (abort() racing spawn) still releases the registration —
        // guard-in-body only ran on the first poll and leaked in_flight,
        // hanging close forever (found by the boundary test).
        // the guard owns a ChildSlot; a grace-expired shutdown can
        // DETACH the slot (accounting moves in_flight→detached) and this
        // drop then settles the detached side instead.
        struct Depart(Arc<ChildSlot>);
        impl Drop for Depart {
            fn drop(&mut self) {
                self.0.depart();
            }
        }
        let slot = Arc::new(ChildSlot {
            state: std::sync::atomic::AtomicU8::new(CHILD_ACTIVE),
            inner: Arc::clone(&inner),
        });
        {
            let mut slots = inner.task_slots.lock();
            // Pruning schedule (same as the child-scope registry): prune at
            // power-of-two lengths ≥ 8 — amortized O(1) per spawn, memory
            // bounded at ~2× peak-live under churn.
            let len = slots.len();
            if len >= 8 && len & (len - 1) == 0 {
                slots.retain(|w| w.strong_count() > 0);
            }
            slots.push(Arc::downgrade(&slot));
        }
        let depart = Depart(slot);
        // the ambient-cancellation dispatch boundary.
        // Before every poll of the child future: observe the budget hard
        // bound (with a timer wake so PARKED children learn of expiry),
        // then the cancel token (whose waker wakes parked children on
        // cancel) — short-circuit to Stopped without polling the child.
        // A live shield masks the cancel check, never the budget.
        let boundary = DispatchBoundary {
            cancelled: inner.token.cancelled(),
            expiry: self
                .inner
                .budget
                .remaining()
                .map(|r| Box::pin(tokio::time::sleep(r))),
            inner: Arc::clone(&inner),
            fut,
        };
        let body = async move {
            let _depart = depart;
            match CatchPanic::new(boundary).await {
                Ok(Ok(value)) => Ok(value),
                // Dispatch-boundary cancellation: NOT a child
                // failure — no fail-fast; the scope is unwinding.
                Ok(Err(stop)) => Err(stop),
                Err(panic_msg) => {
                    // Fail-fast (rule 5) — but cancellation is not an error:
                    // a scope already cancelled is unwinding; don't relabel
                    // the teardown as a child failure.
                    if matches!(inner.supervision, Supervision::FailFast)
                        && !inner.token.is_cancelled()
                    {
                        inner.token.cancel(CancelReason::ChildFailed);
                    }
                    // Child-panic visibility (C++ parity):
                    // a dropped TaskHandle must not swallow this silently.
                    if crate::trace::flow_trace_active() {
                        crate::trace::flow_counter(flow_fixed_ids().tasks_panicked, 1);
                    }
                    crate::aorta_core_warn!(
                        "Aorta WARN: scope '{}': child task panicked: {} — \
                         policy {:?} applies",
                        inner.name,
                        panic_msg,
                        inner.supervision
                    );
                    Err(Stopped::fault(panic_msg))
                }
            }
        };
        // Capture the ANCESTOR chain on the spawning task (a grandchild
        // closing its grandparent is the same deadlock), then seed the new
        // task's stack with chain + self. tokio task-locals never cross
        // tokio::spawn, so the fresh task always needs its own scope().
        let mut chain = SCOPE_STACK
            .try_with(|stack| stack.borrow().clone())
            .unwrap_or_default();
        chain.push(scope_addr);
        let join = tokio::spawn(crate::flow::ambient::scoped(
            SCOPE_STACK.scope(std::cell::RefCell::new(chain), body),
        ));
        TaskHandle {
            join,
            scope: self.clone(),
            task_scope: None,
        }
    }

    /// Register a child that runs outside the Rust task system (the C-ABI
    /// spawn path): counted toward [`Scope::close`] until the returned
    /// handle finishes or drops. `None` once the scope is closing (same
    /// bar as [`Scope::spawn`]).
    pub fn begin_external_child(&self) -> Option<ExternalChild> {
        let _ = self.checkpoint();
        if self
            .inner
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return None;
        }
        self.inner.in_flight.incr();
        // FFI grace hole: external children register a
        // ChildSlot like Rust tasks, so shutdown_with_grace can detach a
        // defiant C-plane child instead of hanging the final close().
        // Registered BEFORE the closing re-check: a detach
        // sweep racing this registration must be able to SEE the slot;
        // the withdraw below then settles through the slot's 3-state
        // arbiter (a concurrent detach's CAS win just means the withdraw
        // decrements `detached` instead of in_flight — exactly once
        // either way).
        let slot = Arc::new(ChildSlot {
            state: std::sync::atomic::AtomicU8::new(CHILD_ACTIVE),
            inner: Arc::clone(&self.inner),
        });
        {
            let mut slots = self.inner.task_slots.lock();
            let len = slots.len();
            if len >= 8 && len & (len - 1) == 0 {
                slots.retain(|w| w.strong_count() > 0);
            }
            slots.push(Arc::downgrade(&slot));
        }
        if self
            .inner
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
        {
            // Withdraw: a close() began concurrently (see spawn()).
            slot.depart();
            return None;
        }
        Some(ExternalChild {
            inner: Arc::clone(&self.inner),
            finished: false,
            slot,
        })
    }

    /// Rule 1: bars NEW children over the WHOLE scope subtree (one-way),
    /// then resolves when every task in the subtree — including tasks
    /// spawned on child scopes — has finished or unwound. Does not cancel
    /// anything — pair with [`Scope::cancel`] for teardown. A `spawn`
    /// racing this call either registers before the bar (and is waited
    /// for) or receives an immediately-failed handle; no task anywhere in
    /// the subtree outlives a returned `close()`, and descendants created
    /// after it are born closing. (Scopes derived via `child_of_token`
    /// share only the CANCEL tree, not close accounting.)
    pub async fn close(&self) {
        // Self-close detection : close() awaited from inside a
        // child of THIS scope can never complete (it waits for the caller).
        // Diagnose loudly; the watchdog below keeps reporting.
        if Self::current_task_is_child_of(&self.inner) {
            crate::aorta_core_warn!(
                "Aorta WARN: scope '{}': close() awaited from INSIDE one of its own \
                 children — this deadlocks (close waits for the caller); run teardown \
                 from outside the scope, or use cancel() here",
                self.inner.name
            );
        }
        // The bar and the wait cover the WHOLE
        // subtree — a task on a grandchild scope must not outlive this
        // close(), and no descendant may accept new spawns past it.
        self.inner.mark_closing_subtree();
        let started = std::time::Instant::now();
        loop {
            let notified = self.inner.idle_notify.notified();
            if self.inner.subtree_in_flight() == 0 {
                // Close-complete marker (pairs with flow.scope_open).
                if !self.inner.name.is_empty() && crate::trace::flow_trace_active() {
                    crate::trace::flow_instant(self.inner.trace_ids().closed);
                }
                return;
            }
            // Watchdog (the audience ship-stopper): a child
            // that never observes cancellation would hang this close()
            // FOREVER with no diagnostics. Re-arm-and-recheck keeps the
            // register-then-recheck pattern lost-wakeup-free (the timeout
            // only adds a periodic recheck + WARN on the stuck cold path).
            if tokio::time::timeout(CLOSE_WATCHDOG_INTERVAL, notified)
                .await
                .is_err()
            {
                let reason = self
                    .inner
                    .token
                    .reason()
                    .map(|r| format!("{r:?}"))
                    .unwrap_or_else(|| "not cancelled".to_string());
                crate::aorta_core_warn!(
                    "Aorta WARN: scope '{}' close() still waiting on {} subtree \
                     children ({}s elapsed; {}) — children not observing \
                     cancellation? (checkpoint()/run_until_cancelled in every loop)",
                    self.inner.name,
                    self.inner.subtree_in_flight(),
                    started.elapsed().as_secs(),
                    reason
                );
            }
        }
    }

    /// `true` when the current tokio task is executing INSIDE a child of
    /// `inner` (see the task-local stack pushed by `spawn`).
    fn current_task_is_child_of(inner: &Arc<ScopeInner>) -> bool {
        scope_stack_contains(Arc::as_ptr(inner) as usize)
    }

    /// The teardown pair, named: cancel with `reason`, then await the
    /// drain. Equivalent to `self.cancel(reason); self.close().await` — the
    /// idiom both demos previously wrote by hand.
    ///
    /// Requires an ambient tokio runtime (like [`Scope::close`]).
    /// Two-phase bounded shutdown: (1) closing bar
    /// across the subtree — no new spawns from here on;
    /// (2) cancel; (3) await the drain up to `grace`; (4) stragglers are
    /// DETACHED — they stop blocking close, their outcomes are discarded
    /// on arrival, and a one-shot WARN names the count. Returns how many
    /// were detached (0 = clean drain).
    ///
    /// Rust cannot hard-destroy a running tokio task (honest semantics —
    /// A detached straggler keeps running until its next
    /// dispatch boundary; `detached_children()` tracks it until then.
    pub async fn shutdown_with_grace(&self, reason: CancelReason, grace: Duration) -> u64 {
        self.inner.mark_closing_subtree(); // the bar comes FIRST
        self.cancel(reason);
        if tokio::time::timeout(grace, self.close()).await.is_ok() {
            return 0;
        }
        let detached = self.inner.detach_subtree();
        if detached > 0 {
            crate::aorta_core_warn!(
                "Aorta WARN: scope '{}': shutdown grace ({:?}) expired — {}                  straggler task(s) DETACHED (no longer block close; outcomes                  discarded; they run until their next dispatch boundary)",
                self.inner.name,
                grace,
                detached
            );
        }
        // Registrations were released by the detach — this returns fast.
        self.close().await;
        detached
    }

    /// Stragglers detached by a grace-expired shutdown that have not yet
    /// self-resolved (introspection).
    pub fn detached_children(&self) -> u64 {
        self.inner.detached_now()
    }

    pub async fn shutdown(&self, reason: CancelReason) {
        self.cancel(reason);
        self.close().await;
    }

    /// Race `fut` against this scope's cancellation: `Some(output)` if the
    /// future completes first, `None` on cancel (tokio-util idiom). The
    /// anti-forgotten-`checkpoint` primitive for long awaits:
    ///
    /// ```ignore
    /// while let Some(sample) = scope.run_until_cancelled(rx.recv()).await {
    ///     process(sample);
    /// }
    /// ```
    pub async fn run_until_cancelled<F: Future>(&self, fut: F) -> Option<F::Output> {
        // Shield-aware cancelled arm (, shared impl in ambient.rs).
        tokio::select! {
            biased;
            _ = (crate::flow::ambient::EffectiveCancelled {
                cancelled: self.cancelled(),
            }) => None,
            out = fut => Some(out),
        }
    }

    /// `true` once [`Scope::close`] has begun (new spawns are refused).
    pub fn is_closing(&self) -> bool {
        self.inner
            .closing
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Live (spawned, unfinished) child count.
    pub fn live_children(&self) -> u64 {
        self.inner.in_flight.get()
    }
}

/// An action goal's scope (**an action IS a Scope**). Derefs to
/// [`Scope`]; the newtype reserves room for goal-specific helpers so the
/// action surface can grow without widening `Scope` itself.
#[derive(Clone, Debug)]
pub struct ActionScope(Scope);

impl ActionScope {
    /// Root a goal scope: the per-goal cancel state backs the token; the
    /// caller's wire-stamped goal budget seeds `budget` (absent
    /// on the wire = unlimited).
    pub(crate) fn for_goal(name: &'static str, budget: Budget) -> Self {
        Self(Scope::with_token(
            CancelToken::new_root(),
            budget,
            ScopeOpts {
                name,
                ..ScopeOpts::default()
            },
        ))
    }
}

impl std::ops::Deref for ActionScope {
    type Target = Scope;

    fn deref(&self) -> &Scope {
        &self.0
    }
}

/// Accounting handle for a child that lives OUTSIDE the Rust task system —
/// e.g. a C/C++ coroutine spawned through the C-ABI (`aorta_scope_spawn`).
/// Holds the scope open until [`finish`](Self::finish); drop
/// also releases (so a freed-but-unfinished FFI task handle can never wedge
/// `Scope::close`).
pub struct ExternalChild {
    inner: Arc<ScopeInner>,
    finished: bool,
    slot: Arc<ChildSlot>,
}

impl ExternalChild {
    /// Mark the external child complete (idempotent with drop).
    pub fn finish(self) {
        // Drop runs the release exactly once.
    }

    /// Mark the external child FAILED : the C twin of the
    /// Rust `CatchPanic` arm — fail-fast cancel with `ChildFailed` unless
    /// the scope is supervised, and never relabel an already-cancelled
    /// (unwinding) scope's teardown as a child failure (the same
    /// `!is_cancelled` guard the Rust arm carries). Accounting releases
    /// exactly once, like `finish`.
    pub fn finish_failed(self) {
        if matches!(self.inner.supervision, Supervision::FailFast)
            && !self.inner.token.is_cancelled()
        {
            self.inner.token.cancel(CancelReason::ChildFailed);
        }
        // Drop runs the release exactly once.
    }

    fn release(&mut self) {
        if !self.finished {
            self.finished = true;
            // Via the slot: settles in_flight OR detached, whichever side
            // this child was on when it finished (accounting).
            self.slot.depart();
        }
    }
}

impl Drop for ExternalChild {
    fn drop(&mut self) {
        self.release();
    }
}

/// Handle to a spawned child task.
#[must_use = "dropping a TaskHandle silently discards the child's outcome \
              (including a supervised child's panic) — join/await it, or \
              bind it explicitly if fire-and-forget is intended"]
pub struct TaskHandle<T> {
    join: tokio::task::JoinHandle<Outcome<T>>,
    scope: Scope,
    /// The task's OWN child scope when spawned via
    /// [`Scope::spawn_with`] — `abort()` cancels it before the hard
    /// abort so nested children the body created from its scope are
    /// torn down structurally, not orphaned. `None` for
    /// plain `spawn` (the body never received a scope to build under).
    task_scope: Option<Scope>,
}

/// `TaskHandle` IS a future (tokio `JoinHandle` idiom): `handle.await` ==
/// `handle.join().await`. `JoinHandle` is `Unpin`, so this is safe to poll
/// without projection.
impl<T> Future for TaskHandle<T> {
    type Output = Outcome<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_join(cx)
    }
}

impl<T> Unpin for TaskHandle<T> {}

impl<T> TaskHandle<T> {
    /// Await the child's outcome. Panic → `Stopped` fault (payload
    /// preserved); abort (a `when_any` loser) → `Stopped` cancel.
    pub async fn join(self) -> Outcome<T> {
        (&mut ScopeJoin { handle: self }).await
    }

    /// Hard-stop the child at its next await point (tokio abort). Prefer
    /// cooperative cancellation via the scope; this is the `when_any`
    /// loser-teardown primitive.
    ///
    /// For a `spawn_with` task this FIRST cancels the task's own child
    /// scope: a bare tokio abort drops the future at its
    /// next await, and nested children the body created from its scope
    /// would otherwise be orphaned with only a WARN. Sharp edge that
    /// remains: a plain `spawn(fut)` body that builds scopes from an
    /// OUTER handle is not reachable from here — use `spawn_with` and
    /// derive children from the given scope.
    pub fn abort(&self) {
        if let Some(task_scope) = &self.task_scope {
            task_scope.cancel(CancelReason::Explicit);
        }
        self.join.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Collapse the nested result of a fallible child: checkpoint-style
    /// bodies return `Outcome<U>` (combinator bodies return
    /// `Outcome<U>`); `join()` would nest either. Both error vocabularies
    /// flatten into `Stopped`.
    pub async fn join_flat<U, E>(self) -> Outcome<U>
    where
        T: Into<Result<U, E>>,
        E: Into<Stopped>,
    {
        match self.join().await?.into() {
            Ok(value) => Ok(value),
            Err(err) => Err(err.into()),
        }
    }

    /// Poll-form join used by the combinators (no pinning requirements:
    /// `JoinHandle` is `Unpin`). The legacy↔Flow vocabulary boundary: the spawn
    /// wrapper speaks `Stopped` end to end (the ScopeError bridge is gone).
    pub(crate) fn poll_join(&mut self, cx: &mut Context<'_>) -> Poll<Outcome<T>> {
        match Pin::new(&mut self.join).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(outcome)) => Poll::Ready(outcome),
            Poll::Ready(Err(join_err)) => Poll::Ready(Err(if join_err.is_cancelled() {
                Stopped::cancelled(self.scope.cancel_reason().unwrap_or(CancelReason::Explicit))
            } else {
                Stopped::fault(join_err.to_string())
            })),
        }
    }
}

struct ScopeJoin<T> {
    handle: TaskHandle<T>,
}

impl<T> Future for &mut ScopeJoin<T> {
    type Output = Outcome<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::into_inner(self).handle.poll_join(cx)
    }
}

/// Panic-isolating future wrapper: polls the inner future under
/// `catch_unwind` so a child panic becomes a value (the fail-fast signal)
/// instead of tearing through the tokio task in an uncontrolled way.
/// the dispatch boundary . Wraps every spawned child
/// INSIDE `CatchPanic` (panic semantics unchanged): each poll first
/// observes the effective stop view, so a task woken from any await
/// resolves `Stopped` instead of running post-await user code once its
/// scope is cancelled or its budget expired. Cancellation WAKES parked
/// children via the token waker; budget expiry wakes them via a one-shot
/// timer armed at spawn.
struct DispatchBoundary<F> {
    fut: F,
    inner: Arc<ScopeInner>,
    cancelled: aorta_sync::Cancelled,
    expiry: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl<F: Future> Future for DispatchBoundary<F> {
    type Output = Outcome<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: structural pin projection to `fut` only; `fut` is never
        // moved out and DispatchBoundary has no Drop impl. All other
        // fields are Unpin (Arc, Cancelled, pinned Box).
        let this = unsafe { self.get_unchecked_mut() };
        // The budget is a HARD bound — it pierces shields.
        let expired = match this.expiry.as_mut() {
            Some(sleep) => sleep.as_mut().poll(cx).is_ready(),
            None => false,
        } || this.inner.budget.is_expired();
        if expired {
            if !this.inner.token.is_cancelled() {
                if crate::trace::flow_trace_active() {
                    crate::trace::flow_instant(flow_fixed_ids().budget_expired);
                }
                this.inner.token.cancel(CancelReason::DeadlineExceeded);
            }
            arm_stall_warn(&this.inner);
            return Poll::Ready(Err(Stopped::cancelled(
                this.inner
                    .token
                    .reason()
                    .unwrap_or(CancelReason::DeadlineExceeded),
            )));
        }
        // Cooperative cancellation is maskable by a live shield in this
        // task skip the check but keep the child pollable.
        let was_shielded = crate::flow::ambient::boundary_shielded();
        if !was_shielded {
            if let Poll::Ready(reason) = Pin::new(&mut this.cancelled).poll(cx) {
                arm_stall_warn(&this.inner);
                return Poll::Ready(Err(Stopped::cancelled(reason)));
            }
        }
        // SAFETY: see above.
        let polled = unsafe { Pin::new_unchecked(&mut this.fut) }.poll(cx);
        if polled.is_pending() && was_shielded && !crate::flow::ambient::boundary_shielded() {
            // Review fix (shield-drop wake path): the shield dropped INSIDE
            // this poll and the child parked on a NEW await. The entry
            // check was skipped and the cancel wake may already be spent —
            // re-observe NOW (short-circuit, or re-register the token waker
            // for a future cancel).
            if let Poll::Ready(reason) = Pin::new(&mut this.cancelled).poll(cx) {
                arm_stall_warn(&this.inner);
                return Poll::Ready(Err(Stopped::cancelled(reason)));
            }
        }
        polled.map(Ok)
    }
}

/// stall-WARN: armed ONCE per scope on the first cancellation
/// observation (zero pre-cancel cost). After `AORTA_FLOW_STALL_WARN_MS`
/// (default 200), subtree children that still haven't departed get a
/// one-shot WARN — with the dispatch boundary in place, any child that
/// reached a boundary after cancel has already resolved, so survivors
/// are mid-poll stalls (or long shielded cleanup, named honestly).
fn stall_warn_threshold_ms() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("AORTA_FLOW_STALL_WARN_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200)
    })
}

/// cfg(test) observation hook: WARN events keyed by scope name so tests
/// with unique names are immune to other tests' late-firing watchdogs
/// (a global counter flaked exactly that way).
#[cfg(test)]
pub(crate) static STALL_WARN_NAMES: std::sync::Mutex<Vec<&'static str>> =
    std::sync::Mutex::new(Vec::new());

fn arm_stall_warn(inner: &Arc<ScopeInner>) {
    use std::sync::atomic::Ordering;
    if inner.stall_warn_armed.swap(true, Ordering::AcqRel) {
        return;
    }
    // No runtime (foreign-thread cancel): skip — close()'s slow-path WARN
    // still covers the hang case.
    // Nothing in flight: no one to stall (also keeps scope-churn cheap —
    // the per-request child pattern cancels empty scopes constantly).
    if inner.subtree_in_flight() == 0 {
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    // Weak: the watchdog must not extend the scope's life (an Arc here
    // kept the child token registered in the PARENT registry for the
    // whole threshold — unbounded parent-registry growth under churn).
    let inner = Arc::downgrade(inner);
    let threshold = std::time::Duration::from_millis(stall_warn_threshold_ms());
    handle.spawn(async move {
        tokio::time::sleep(threshold).await;
        let Some(inner) = inner.upgrade() else {
            return; // scope fully gone — nothing stalled
        };
        let live = inner.subtree_in_flight();
        if live > 0 {
            #[cfg(test)]
            STALL_WARN_NAMES.lock().unwrap().push(inner.name);
            crate::aorta_core_warn!(
                "Aorta WARN: scope '{}': {} subtree child(ren) still running                  {}ms after cancellation — not observing the dispatch                  boundary (mid-poll stall or long shielded cleanup);                  checkpoint()/await in long loops",
                inner.name,
                live,
                threshold.as_millis()
            );
        }
    });
}

struct CatchPanic<F> {
    inner: F,
}

impl<F> CatchPanic<F> {
    fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: structural pin projection to the only field; `inner` is
        // never moved out of the pinned wrapper and `CatchPanic` has no
        // `Drop` impl that could observe it unpinned.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(payload) => Poll::Ready(Err(crate::runtime::panic_payload_string(&payload))),
        }
    }
}

/// Test-visible probe of the self-close detection chain.
pub(crate) fn scope_stack_contains(addr: usize) -> bool {
    SCOPE_STACK
        .try_with(|stack| stack.borrow().contains(&addr))
        .unwrap_or(false)
}

/// Per-scope interned event ids (computed once per scope).
#[derive(Clone, Copy)]
pub(crate) struct ScopeTraceIds {
    open: u16,
    closed: u16,
}

/// Fixed `aorta.flow` event ids, interned once per process (the
/// cancel/spawn/panic paths used to re-intern per call).
struct FlowFixedIds {
    cancel_explicit: u16,
    cancel_deadline: u16,
    cancel_shutdown: u16,
    cancel_child_failed: u16,
    cancel_other: u16,
    budget_expired: u16,
    tasks_spawned: u16,
    tasks_panicked: u16,
}

fn flow_fixed_ids() -> &'static FlowFixedIds {
    static IDS: std::sync::OnceLock<FlowFixedIds> = std::sync::OnceLock::new();
    IDS.get_or_init(|| FlowFixedIds {
        cancel_explicit: crate::trace::intern_event("flow.scope_cancel:explicit"),
        cancel_deadline: crate::trace::intern_event("flow.scope_cancel:deadline"),
        cancel_shutdown: crate::trace::intern_event("flow.scope_cancel:shutdown"),
        cancel_child_failed: crate::trace::intern_event("flow.scope_cancel:child_failed"),
        cancel_other: crate::trace::intern_event("flow.scope_cancel:other"),
        budget_expired: crate::trace::intern_event("flow.budget_expired"),
        tasks_spawned: crate::trace::intern_event("flow.tasks_spawned"),
        tasks_panicked: crate::trace::intern_event("flow.tasks_panicked"),
    })
}

impl Scope {
    /// Spawn a RESTARTABLE child: under `Supervision::Restart`,
    /// a child FAULT (panic — never cooperative cancellation) re-invokes
    /// `factory` after exponential backoff, up to `max_restarts` within
    /// `window`; exhaustion escalates to fail-fast (scope cancelled with
    /// `ChildFailed`, handle resolves the final fault). A panic inside
    /// `factory` itself counts as a fault of the child. Under
    /// FailFast/Isolate this is a single run of `factory()` under the
    /// scope's usual policy.
    ///
    /// The backoff sleep needs no special cancel handling: the spawn
    /// dispatch boundary short-circuits the whole wrapper at its next
    /// poll once the scope stops (shutdown during backoff returns
    /// promptly, and a stopped scope never restarts anything).
    #[must_use = "the returned TaskHandle carries the child's outcome"]
    pub fn spawn_supervised<T, F, Fut>(&self, mut factory: F) -> TaskHandle<T>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let Supervision::Restart {
            max_restarts,
            window,
            backoff,
        } = self.inner.supervision
        else {
            // Contain a factory panic under EVERY policy — it
            // must be a fault of the CHILD (resume inside the spawned
            // task, where CatchPanic makes it Stopped(Fault)), never an
            // unwind of the spawn_supervised caller.
            return self.spawn(async move {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut factory)) {
                    Ok(fut) => fut.await,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            });
        };
        let inner = Arc::clone(&self.inner);
        self.spawn(async move {
            let mut restarts = 0u32;
            let mut window_start = tokio::time::Instant::now();
            loop {
                // A factory panic is a fault of this child.
                let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut factory));
                let outcome = match attempt {
                    Ok(fut) => CatchPanic::new(fut).await,
                    Err(panic) => Err(crate::flow::scope::panic_message(panic)),
                };
                let msg = match outcome {
                    Ok(value) => return value,
                    Err(msg) => msg,
                };
                let now = tokio::time::Instant::now();
                if now.duration_since(window_start) > window {
                    restarts = 0;
                    window_start = now;
                }
                restarts += 1;
                if restarts > max_restarts {
                    crate::aorta_core_warn!(
                        "Aorta WARN: scope '{}': supervised child exhausted \
                         {max_restarts} restart(s) in {window:?} — escalating \
                         to fail-fast: {msg}",
                        inner.name
                    );
                    // Escalation = the FailFast arm, applied explicitly.
                    if !inner.token.is_cancelled() {
                        inner.token.cancel(CancelReason::ChildFailed);
                    }
                    // Surface the final fault on the handle.
                    std::panic::resume_unwind(Box::new(msg));
                }
                crate::aorta_core_warn!(
                    "Aorta WARN: scope '{}': supervised child fault \
                     ({restarts}/{max_restarts} in window) — restarting after \
                     {:?}: {msg}",
                    inner.name,
                    backoff_for(backoff, restarts)
                );
                tokio::time::sleep(backoff_for(backoff, restarts)).await;
            }
        })
    }
}

/// Exponential backoff: `base * 2^(n-1)`, saturating, capped at 30 s.
fn backoff_for(base: Duration, restart_n: u32) -> Duration {
    let shift = restart_n.saturating_sub(1).min(16);
    base.saturating_mul(1u32 << shift)
        .min(Duration::from_secs(30))
}

/// Extract the panic payload's message — delegates to the runtime's
/// single downcast ladder, so the logic exists in one place.
pub(crate) fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    crate::runtime::panic_payload_string(&panic)
}
