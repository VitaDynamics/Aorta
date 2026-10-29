//! A model-checked hierarchical cancellation tree.
//!
//! [`CancelToken`] is the one-way, hierarchical cancel primitive behind
//! `Scope`: each token owns its own terminal flag; a parent additionally
//! holds a registry of live children; [`cancel`](CancelToken::cancel) marks
//! its own state then eagerly walks and marks every descendant; a child
//! created under an already-cancelled parent is **born cancelled** (the
//! check runs inside the parent's registry critical section, closing the
//! child-created-during-cancel race). One-way cancel is structural — a child
//! never writes its parent's state — and the poll stays a single `Acquire`
//! load of the token's *own* flag, safe from any thread (RT / Zenoh RX).
//! Dropping the last clone of a token **detaches** it from its parent's
//! registry, adopting its children up to the grandparent (a long-lived root
//! spawning per-goal children must not accumulate registry entries).
//!
//! This is the design every production implementation converged on
//! (tokio-util `CancellationToken`, kotlinx `Job`, .NET linked CTS, Swift
//! `Task`, Trio cancel scopes); the reference for the detach/adoption lock
//! discipline is tokio-util's `tree_node.rs`. We port the *design and its
//! race obligations* rather than depending on the crate, whose
//! `is_cancelled()` is a mutex acquire (fails the RT-safe-poll contract) and
//! which carries no [`CancelReason`].
//!
//! ## Locking protocol (the correctness core, loom-verified)
//!
//! - **Lock order is strictly ancestor → descendant.** `cancel` holds ONE
//!   node lock at a time; creation holds only the parent's; detach holds
//!   parent + node (+ grandchild, top-down) after re-verifying under both
//!   locks that the parent link is unchanged (retry on change).
//! - **Cancel marks state AND takes the children/waiter lists under the same
//!   node lock.** This is load-bearing: it makes "state != 0" equivalent to
//!   "this node's children are owned by a cascade", so a concurrent detach
//!   can never adopt grandchildren *out from under* an in-flight cascade
//!   (which would let them escape cancellation — the race
//!   `detach_racing_cancel_cannot_leak_grandchildren` proves is handled).
//! - **Detach aborts adoption when the parent is already cancelled** (the
//!   parent's cascade owns the subtree; detach just leaves it to finish).
//!
//! Correctness obligations, exhaustively verified by the `#[cfg(loom)]`
//! tests below:
//!
//!   1. A child created concurrently with `cancel` is either cascaded into
//!      or born cancelled — never live under a cancelled parent.
//!   2. A detach racing a cancel can never strand a grandchild uncancelled.
//!   3. Racing cancels: exactly one reason wins, everywhere in the subtree.
//!   4. Concurrent detaches on a chain neither deadlock nor corrupt links.

use crate::loom_sync::{AtomicU8, Mutex, MutexGuard, Ordering};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// An unknown-but-preserved cancellation reason byte: future wire/FFI
/// codes round-trip loss-free
/// instead of aliasing [`CancelReason::Explicit`]. The field is PRIVATE so
/// no caller can construct `Unknown(3)` and shadow a named reason —
/// [`CancelReason::from_raw`] is the only constructor and never wraps a
/// named value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawCancelReason(u8);

impl RawCancelReason {
    /// The exact non-zero wire byte.
    pub fn get(self) -> u8 {
        self.0
    }
}

/// Why a token was cancelled. Carried as a non-zero `u8` in the token's
/// state flag (0 = live) so the poll stays one atomic load; unknown
/// non-zero raw values (e.g. future wire/FFI codes) decode as
/// [`Self::Unknown`] and round-trip loss-free (they are still
/// cancellations, but never impersonate a named reason).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelReason {
    /// An explicit `cancel()` call (application decision).
    Explicit,
    /// A `Budget` deadline expired (fired at an observation point).
    DeadlineExceeded,
    /// The owning runtime/executor is shutting down.
    Shutdown,
    /// A sibling task failed (fail-fast).
    ChildFailed,
    /// A non-zero reason byte this build does not name — preserved
    /// verbatim (only [`Self::from_raw`] constructs it).
    Unknown(RawCancelReason),
}

impl CancelReason {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Explicit => 1,
            Self::DeadlineExceeded => 2,
            Self::Shutdown => 3,
            Self::ChildFailed => 4,
            Self::Unknown(raw) => raw.get(),
        }
    }

    /// Decode a raw state value. `0` (live) → `None`; named values decode
    /// to their variant; anything else is preserved as [`Self::Unknown`]
    /// (still a cancellation — `as_u8()` round-trips the exact byte).
    pub fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0 => None,
            1 => Some(Self::Explicit),
            2 => Some(Self::DeadlineExceeded),
            3 => Some(Self::Shutdown),
            4 => Some(Self::ChildFailed),
            other => Some(Self::Unknown(RawCancelReason(other))),
        }
    }
}

struct NodeInner {
    /// `None` for a root, a detached node, or one already claimed by a
    /// cascade. INVARIANT: `parent == Some(p)` ⟺ `p.children` contains this
    /// node — except transiently while a cascade that has taken `p.children`
    /// has not yet processed this node (detected via `p.state != 0`).
    parent: Option<Arc<CancelNode>>,
    children: Vec<Arc<CancelNode>>,
    waiters: Vec<(u64, Waker)>,
    next_waiter_id: u64,
}

pub(crate) struct CancelNode {
    /// 0 = live; non-zero = the winning cancel's raw [`CancelReason`].
    /// Written only under `inner`'s lock; read lock-free (`Acquire`).
    state: AtomicU8,
    inner: Mutex<NodeInner>,
}

impl CancelNode {
    fn new(state: u8, parent: Option<Arc<CancelNode>>) -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(state),
            inner: Mutex::new(NodeInner {
                parent,
                children: Vec::new(),
                waiters: Vec::new(),
                next_waiter_id: 0,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, NodeInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// Create a child node, registered under `parent` — or born cancelled if the
/// parent already is. The state check runs INSIDE the parent's critical
/// section, so it is serialized against `cancel_node`'s mark-and-take.
fn child_node(parent: &Arc<CancelNode>) -> Arc<CancelNode> {
    let mut guard = parent.lock();
    let parent_state = parent.state.load(Ordering::Acquire);
    if parent_state != 0 {
        // Born cancelled: terminal from birth, never registered.
        return CancelNode::new(parent_state, None);
    }
    let child = CancelNode::new(0, Some(Arc::clone(parent)));
    guard.children.push(Arc::clone(&child));
    child
}

/// Eagerly cancel `node` and every descendant with `raw` (first cancel wins;
/// descendants inherit the ORIGINAL reason). Holds one node lock at a time;
/// state-mark and children/waiter take are atomic per node (see module doc).
fn cancel_node(node: &Arc<CancelNode>, raw: u8) {
    debug_assert_ne!(raw, 0, "0 is the live state, not a cancel reason");
    // Entry-node purge: the cascade `mem::take`s every visited node's
    // children, so DESCENDANTS reclaim naturally when their token clones
    // drop — but the ENTRY node's registration in its (un-cancelled,
    // external) parent used to outlive it forever: `parent = None` erases
    // the back-edge, so detach_node's parentless branch never purges the
    // parent's Vec. Capture that parent here and purge after the cascade.
    let mut external_parent: Option<Arc<CancelNode>> = None;
    let mut stack = vec![Arc::clone(node)];
    while let Some(n) = stack.pop() {
        let (children, waiters) = {
            let mut guard = n.lock();
            if n.state.load(Ordering::Acquire) != 0 {
                // Already cancelled — that cascade owns this subtree.
                continue;
            }
            n.state.store(raw, Ordering::Release);
            if Arc::ptr_eq(&n, node) {
                external_parent = guard.parent.take();
            } else {
                guard.parent = None;
            }
            (
                std::mem::take(&mut guard.children),
                std::mem::take(&mut guard.waiters),
            )
        };
        for (_, waker) in waiters {
            waker.wake();
        }
        stack.extend(children);
    }
    // Purge the entry node from its external parent's registry. One lock,
    // held alone, after the cascade released everything — no new lock
    // pairs, so the loom-verified ancestor→descendant discipline is
    // untouched. Benign races: a concurrent detach already removed the
    // entry (retain is a no-op); a concurrent cancel(parent) cascade took
    // the parent's children Vec (also a no-op).
    if let Some(p) = external_parent {
        let mut parent_guard = p.lock();
        unlink_child(&mut parent_guard.children, node);
    }
}

/// Remove `node` from a parent's children Vec — the one unlink both the
/// cancel purge and detach use.
fn unlink_child(children: &mut Vec<Arc<CancelNode>>, node: &Arc<CancelNode>) {
    children.retain(|c| !Arc::ptr_eq(c, node));
}

/// Detach `node` from its parent's registry, adopting its children up to the
/// grandparent so cancellation still reaches them (grandchild adoption).
/// Called when the last token clone for the node drops.
/// LOCK DISCIPLINE (loom-verified): re-parenting any node X requires holding
/// X's CURRENT parent's lock for the whole restructure — which is why the
/// node's own guard is held THROUGH the adoption loop below. Releasing it
/// early lets a concurrent `detach(child)` verify against this half-detached
/// zombie parent and splice a grandchild under it, stranding that grandchild
/// outside the tree (loom finds this: `concurrent_detaches_keep_the_tree_
/// cancellable`). Acquisition is always ancestor → descendant along current
/// edges, so lock chains cannot cycle.
fn detach_node(node: &Arc<CancelNode>) {
    loop {
        let parent = { node.lock().parent.clone() };
        let Some(p) = parent else {
            // Root, already detached, or claimed by a cascade: release any
            // children as independent roots (nothing above can cancel this
            // node anymore, so keeping them registered here is pure leak).
            // Hold the node's lock through the loop (see discipline above).
            let mut node_guard = node.lock();
            let children = std::mem::take(&mut node_guard.children);
            for child in &children {
                child.lock().parent = None;
            }
            return;
        };
        // Ancestor-before-descendant: parent, then node.
        let mut parent_guard = p.lock();
        let mut node_guard = node.lock();
        match &node_guard.parent {
            Some(current) if Arc::ptr_eq(current, &p) => {
                if p.state.load(Ordering::Acquire) != 0 {
                    // The parent's cascade owns this subtree (it took
                    // p.children atomically with the state mark and will
                    // process this node once we release its lock). Adopting
                    // the grandchildren here would let them escape that
                    // cascade — leave everything to it.
                    return;
                }
                node_guard.parent = None;
                let grandchildren = std::mem::take(&mut node_guard.children);
                unlink_child(&mut parent_guard.children, node);
                for grandchild in grandchildren {
                    // Top-down third lock (p → node → grandchild); both the
                    // node and parent guards stay held, so no concurrent
                    // detach can verify against this node as a stale parent.
                    let mut grandchild_guard = grandchild.lock();
                    if grandchild.state.load(Ordering::Acquire) != 0 {
                        // Already cancelled: its own
                        // cascade claimed its subtree, and — if it was a
                        // cancel ENTRY node — its entry-node purge may already
                        // have removed it from OUR registry; re-adopting it
                        // up to the grandparent would resurrect exactly that
                        // registration until its last clone drops. Nothing
                        // above can un-cancel it, so drop the edge instead.
                        grandchild_guard.parent = None;
                        continue;
                    }
                    grandchild_guard.parent = Some(Arc::clone(&p));
                    drop(grandchild_guard);
                    parent_guard.children.push(grandchild);
                }
                return;
            }
            _ => {
                // Parent changed under us (a concurrent detach restructured
                // the tree, or a cascade claimed this node) — retry.
                drop(node_guard);
                drop(parent_guard);
            }
        }
    }
}

/// Owner-of-record for a node: detaches it when the LAST token clone drops.
struct TokenOwner {
    node: Arc<CancelNode>,
}

impl Drop for TokenOwner {
    fn drop(&mut self) {
        detach_node(&self.node);
    }
}

/// Hierarchical, one-way cancellation token.
///
/// Clones share the same node (all observe the same state); the node
/// detaches from its parent when the last clone drops. `is_cancelled()` is
/// one `Acquire` load — non-blocking, alloc-free, safe from any thread;
/// `cancel()` / `child()` / drop take registry locks and are NOT RT-safe.
#[derive(Clone)]
pub struct CancelToken {
    owner: Arc<TokenOwner>,
}

impl std::fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelToken")
            .field("cancelled", &self.is_cancelled())
            .field("reason", &self.reason())
            .finish()
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new_root()
    }
}

impl CancelToken {
    /// A fresh, live root token.
    pub fn new_root() -> Self {
        Self {
            owner: Arc::new(TokenOwner {
                node: CancelNode::new(0, None),
            }),
        }
    }

    /// Test probe (entry-node purge regression): how many children this node still
    /// holds registered. Not public API.
    #[cfg(any(test, loom))]
    pub fn debug_children_len(&self) -> usize {
        self.owner.node.lock().children.len()
    }

    /// Create a child token, registered under this one; born cancelled if
    /// this token is already cancelled.
    pub fn child(&self) -> CancelToken {
        Self {
            owner: Arc::new(TokenOwner {
                node: child_node(&self.owner.node),
            }),
        }
    }

    /// Cancel this token and, eagerly, every descendant. First cancel wins;
    /// descendants observe the original winner's reason. One-way: never
    /// touches the parent.
    pub fn cancel(&self, reason: CancelReason) {
        cancel_node(&self.owner.node, reason.as_u8());
    }

    /// One `Acquire` load of this token's OWN state — RT-safe poll.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.owner.node.state.load(Ordering::Acquire) != 0
    }

    /// The winning cancel's reason, or `None` while live.
    #[inline]
    pub fn reason(&self) -> Option<CancelReason> {
        CancelReason::from_raw(self.owner.node.state.load(Ordering::Acquire))
    }

    /// Stable pointer to this token's one-byte state flag (`0` = live,
    /// non-zero = the raw [`CancelReason`]), valid while any clone of this
    /// token or an outstanding [`Cancelled`] waiter is alive. This is the
    /// resolve-once-then-poll-lock-free primitive behind the C-ABI's
    /// RT-safe poll: the C side reads it with an acquire atomic load —
    /// no lock, no handle lookup, on any thread. Rust callers should use
    /// [`Self::is_cancelled`] instead.
    ///
    /// (Unavailable under `--cfg loom`: loom atomics carry no `as_ptr`,
    /// and the FFI never builds under loom.)
    #[cfg(not(loom))]
    #[doc(hidden)]
    pub fn state_flag_ptr(&self) -> *const u8 {
        self.owner.node.state.as_ptr()
    }

    /// Cooperative checkpoint: resolves when this token is cancelled
    /// (immediately if it already is). The future retains the token's OWNER
    /// (not just the node), so an outstanding waiter keeps the node attached
    /// to the tree — `let fut = child.cancelled(); drop(child);` must still
    /// resolve when an ancestor cancels (a bare node reference would detach
    /// on the drop and the waiter would hang forever).
    pub fn cancelled(&self) -> Cancelled {
        Cancelled {
            owner: Arc::clone(&self.owner),
            waiter_id: None,
        }
    }
}

/// Future returned by [`CancelToken::cancelled`].
pub struct Cancelled {
    /// Keeps the node ATTACHED while the waiter exists (see
    /// [`CancelToken::cancelled`]); the node detaches only when the last
    /// token clone AND the last waiter are gone.
    owner: Arc<TokenOwner>,
    waiter_id: Option<u64>,
}

impl Future for Cancelled {
    type Output = CancelReason;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = Pin::into_inner(self);
        let node = &this.owner.node;
        // Fast path before touching the lock.
        if let Some(reason) = CancelReason::from_raw(node.state.load(Ordering::Acquire)) {
            return Poll::Ready(reason);
        }
        let mut guard = node.lock();
        // Re-check under the lock: cancel drains waiters and sets state
        // atomically under this same lock, so registering after this check
        // cannot miss the wake.
        if let Some(reason) = CancelReason::from_raw(node.state.load(Ordering::Acquire)) {
            return Poll::Ready(reason);
        }
        match this.waiter_id {
            Some(id) => {
                if let Some(entry) = guard.waiters.iter_mut().find(|(wid, _)| *wid == id) {
                    entry.1.clone_from(cx.waker());
                } else {
                    // Drained by a cancel that raced ahead of our state
                    // re-check — impossible under the lock, but re-arm
                    // defensively rather than hang.
                    guard.waiters.push((id, cx.waker().clone()));
                }
            }
            None => {
                let id = guard.next_waiter_id;
                guard.next_waiter_id += 1;
                guard.waiters.push((id, cx.waker().clone()));
                this.waiter_id = Some(id);
            }
        }
        Poll::Pending
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        if let Some(id) = self.waiter_id {
            let mut guard = self.owner.node.lock();
            guard.waiters.retain(|(wid, _)| *wid != id);
        }
    }
}

// Single-threaded behavioural tests (normal std build; loom proofs below).
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};
    use std::task::Wake;

    struct CountWaker(AtomicUsize);
    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, StdOrdering::SeqCst);
        }
    }

    fn poll_once(fut: &mut Cancelled, waker: &Arc<CountWaker>) -> Poll<CancelReason> {
        let waker = std::task::Waker::from(Arc::clone(waker));
        let mut cx = Context::from_waker(&waker);
        Pin::new(fut).poll(&mut cx)
    }

    #[test]
    fn cancelled_children_are_purged_from_the_parent_registry() {
        // Entry-node purge regression: the canonical per-request idiom
        // (child → cancel → drop) must not grow the parent's registry.
        let root = CancelToken::new_root();
        for _ in 0..1000 {
            let child = root.child();
            child.cancel(CancelReason::Explicit);
            drop(child);
        }
        assert_eq!(
            root.debug_children_len(),
            0,
            "cancel+drop children must leave no registry entries"
        );
        // Live (un-cancelled) children still stay registered.
        let keep = root.child();
        assert_eq!(root.debug_children_len(), 1);
        drop(keep); // detach path purges it
        assert_eq!(root.debug_children_len(), 0);
    }

    #[test]
    fn cancel_marks_self_and_descendants_with_the_original_reason() {
        let root = CancelToken::new_root();
        let child = root.child();
        let grandchild = child.child();
        assert!(!grandchild.is_cancelled());

        root.cancel(CancelReason::Shutdown);
        assert!(root.is_cancelled());
        assert_eq!(child.reason(), Some(CancelReason::Shutdown));
        assert_eq!(grandchild.reason(), Some(CancelReason::Shutdown));
    }

    #[test]
    fn first_cancel_wins() {
        let token = CancelToken::new_root();
        token.cancel(CancelReason::DeadlineExceeded);
        token.cancel(CancelReason::Explicit);
        assert_eq!(token.reason(), Some(CancelReason::DeadlineExceeded));
    }

    #[test]
    fn child_of_a_cancelled_parent_is_born_cancelled() {
        let root = CancelToken::new_root();
        root.cancel(CancelReason::Explicit);
        let child = root.child();
        assert!(child.is_cancelled());
        assert_eq!(child.reason(), Some(CancelReason::Explicit));
    }

    #[test]
    fn cancel_is_one_way_child_never_cancels_parent() {
        let root = CancelToken::new_root();
        let child = root.child();
        child.cancel(CancelReason::Explicit);
        assert!(child.is_cancelled());
        assert!(!root.is_cancelled(), "cancel must never flow upward");
    }

    #[test]
    fn sibling_survives_a_siblings_cancel() {
        let root = CancelToken::new_root();
        let a = root.child();
        let b = root.child();
        a.cancel(CancelReason::Explicit);
        assert!(!b.is_cancelled());
        assert!(!root.is_cancelled());
    }

    #[test]
    fn dropping_a_token_adopts_grandchildren_to_the_grandparent() {
        let root = CancelToken::new_root();
        let middle = root.child();
        let leaf = middle.child();
        drop(middle); // detach: leaf adopted by root
        root.cancel(CancelReason::Shutdown);
        assert_eq!(
            leaf.reason(),
            Some(CancelReason::Shutdown),
            "cancellation must still reach an adopted grandchild"
        );
    }

    #[test]
    fn clones_share_state_and_detach_only_on_last_drop() {
        let root = CancelToken::new_root();
        let child = root.child();
        let child2 = child.clone();
        drop(child); // NOT the last clone — no detach
        let leaf = child2.child();
        root.cancel(CancelReason::Explicit);
        assert!(child2.is_cancelled());
        assert!(leaf.is_cancelled());
    }

    #[test]
    fn cancelled_future_is_ready_immediately_on_a_cancelled_token() {
        let token = CancelToken::new_root();
        token.cancel(CancelReason::Explicit);
        let waker = Arc::new(CountWaker(AtomicUsize::new(0)));
        let mut fut = token.cancelled();
        assert_eq!(
            poll_once(&mut fut, &waker),
            Poll::Ready(CancelReason::Explicit)
        );
    }

    #[test]
    fn cancelled_future_wakes_on_cancel_and_resolves_with_the_reason() {
        let token = CancelToken::new_root();
        let waker = Arc::new(CountWaker(AtomicUsize::new(0)));
        let mut fut = token.cancelled();
        assert_eq!(poll_once(&mut fut, &waker), Poll::Pending);
        token.cancel(CancelReason::DeadlineExceeded);
        assert_eq!(waker.0.load(StdOrdering::SeqCst), 1, "exactly one wake");
        assert_eq!(
            poll_once(&mut fut, &waker),
            Poll::Ready(CancelReason::DeadlineExceeded)
        );
    }

    #[test]
    fn dropping_a_cancelled_future_deregisters_its_waker() {
        let token = CancelToken::new_root();
        let waker = Arc::new(CountWaker(AtomicUsize::new(0)));
        let mut fut = token.cancelled();
        assert_eq!(poll_once(&mut fut, &waker), Poll::Pending);
        drop(fut);
        token.cancel(CancelReason::Explicit);
        assert_eq!(
            waker.0.load(StdOrdering::SeqCst),
            0,
            "a dropped waiter must not be woken (no leaked registration)"
        );
    }

    #[test]
    fn outstanding_cancelled_future_survives_token_drop() {
        // Regression: a waiter built from a token must
        // keep the node attached after the token drops — an ancestor cancel
        // must still resolve it, never strand it off-tree.
        let root = CancelToken::new_root();
        let child = root.child();
        let waker = Arc::new(CountWaker(AtomicUsize::new(0)));
        let mut fut = child.cancelled();
        assert_eq!(poll_once(&mut fut, &waker), Poll::Pending);
        drop(child); // last clone: would detach the node without the fix
        root.cancel(CancelReason::Shutdown);
        assert_eq!(waker.0.load(StdOrdering::SeqCst), 1, "waiter must wake");
        assert_eq!(
            poll_once(&mut fut, &waker),
            Poll::Ready(CancelReason::Shutdown)
        );
    }

    #[test]
    fn named_reasons_round_trip_and_zero_is_live() {
        // (Replaces an earlier lossy-fold characterization, together
        // with `unknown_raw_reason_survives_decode`.)
        assert_eq!(CancelReason::from_raw(0), None);
        for reason in [
            CancelReason::Explicit,
            CancelReason::DeadlineExceeded,
            CancelReason::Shutdown,
            CancelReason::ChildFailed,
        ] {
            assert_eq!(CancelReason::from_raw(reason.as_u8()), Some(reason));
        }
    }

    #[test]
    fn unknown_raw_reason_survives_decode() {
        // Contract: a future/foreign wire
        // byte round-trips through decode — aliasing it to Explicit made
        // `aorta_scope_cancel(h, 200)` indistinguishable from an
        // application cancel at every downstream observation point.
        let decoded = CancelReason::from_raw(200);
        assert_ne!(decoded, Some(CancelReason::Explicit));
        match decoded {
            Some(CancelReason::Unknown(raw)) => assert_eq!(raw.get(), 200),
            other => panic!("raw 200 must decode as Unknown(200), got {other:?}"),
        }
        // And it is still a CANCELLATION: the byte round-trips through the
        // token state exactly.
        assert_eq!(decoded.unwrap().as_u8(), 200);
    }

    #[test]
    fn unknown_reason_cascades_through_a_token_tree() {
        // End-to-end: a foreign byte entering at the root is observable
        // verbatim at every descendant (the state flag stores the raw byte;
        // decode no longer folds it).
        let root = CancelToken::new_root();
        let child = root.child();
        root.cancel(CancelReason::from_raw(200).unwrap());
        assert_eq!(child.reason().map(CancelReason::as_u8), Some(200));
    }
}

// Exhaustive concurrency proofs. Run with:
//   RUSTFLAGS="--cfg loom" cargo test -p aorta_sync cancel::loom_tests
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    /// The registry-purge in `cancel_node` racing a TRULY
    /// concurrent `detach` — of a SIBLING with a grandchild, so the detach
    /// path takes the root→sibling lock pair AND adopts the grandchild
    /// into root.children while the purge retains on the same root lock.
    /// (A same-token cancel-then-drop cannot overlap —
    /// the cancel call itself keeps the token alive — so the raceable pair
    /// is purge vs a sibling's detach/adoption, both contending root.)
    /// Invariants: no deadlock; the cancelled child is purged; the sibling
    /// is detached; the adopted grandchild is the ONLY registry entry and
    /// remains cancellable from the root.
    #[test]
    fn cancel_purge_races_sibling_detach_and_adoption() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let victim = root.child(); // cancel+purge path
            let sibling = root.child(); // detach+adoption path
            let grandchild = sibling.child();
            let canceller = loom::thread::spawn(move || {
                victim.cancel(CancelReason::Explicit);
                drop(victim);
            });
            drop(sibling); // last clone: detach root→sibling, adopt grandchild up
            canceller.join().unwrap();
            assert_eq!(
                root.debug_children_len(),
                1,
                "victim purged + sibling detached; only the adopted grandchild remains"
            );
            root.cancel(CancelReason::Shutdown);
            assert!(
                grandchild.is_cancelled(),
                "the adopted grandchild stayed reachable by the root cascade"
            );
        });
    }

    /// A node SELF-cancels while its own live,
    /// un-cancelled parent's last token clone drops (detach + adoption).
    /// Pre-fix, the adoption re-registered the already-cancelled node
    /// under the grandparent — resurrecting the registration the
    /// entry purge had just removed (until the node's own last clone
    /// dropped). Every interleaving must leave the cancelled node
    /// UNREGISTERED and the tree consistent.
    #[test]
    fn self_cancel_races_parent_detach_without_readoption() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let parent = root.child();
            let node = parent.child();
            let node2 = node.clone();
            let canceller = loom::thread::spawn(move || {
                node2.cancel(CancelReason::Explicit);
            });
            drop(parent); // last clone: detach root→parent, adopt node up
            canceller.join().unwrap();
            assert!(node.is_cancelled());
            assert_eq!(
                root.debug_children_len(),
                0,
                "a cancelled node is never re-adopted (the purge guarantee holds)"
            );
            // The tree stays usable for later operations.
            root.cancel(CancelReason::Shutdown);
        });
    }

    /// The entry-node purge racing `cancel(parent)` — the parent
    /// cascade may take its children Vec concurrently; both orders must
    /// leave the parent registry empty and both tokens cancelled.
    #[test]
    fn cancel_purge_races_parent_cascade() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let child = root.child();
            let root2 = root.clone();
            let parent_cancel = loom::thread::spawn(move || root2.cancel(CancelReason::Shutdown));
            child.cancel(CancelReason::Explicit);
            parent_cancel.join().unwrap();
            assert!(child.is_cancelled());
            assert!(root.is_cancelled());
            assert_eq!(root.debug_children_len(), 0);
        });
    }

    /// The child-created-during-cancel race. Across every
    /// interleaving the child ends up cancelled — either registered in time
    /// for the cascade, or born cancelled inside the registry critical
    /// section. Never live under a cancelled parent.
    #[test]
    fn child_creation_racing_cancel_never_yields_a_live_child() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let root2 = root.clone();
            let creator = loom::thread::spawn(move || root2.child());
            root.cancel(CancelReason::Explicit);
            let child = creator.join().unwrap();
            assert!(
                child.is_cancelled(),
                "a child created while its parent cancels must be cancelled"
            );
        });
    }

    /// THE adoption/cascade race (module doc): dropping the middle token
    /// (detach + grandchild adoption) concurrently with cancelling the root
    /// must never let the grandchild escape cancellation — the failure mode
    /// where detach splices grandchildren out from under an in-flight
    /// cascade.
    #[test]
    fn detach_racing_cancel_cannot_leak_grandchildren() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let middle = root.child();
            let leaf = middle.child();
            let dropper = loom::thread::spawn(move || drop(middle));
            root.cancel(CancelReason::Shutdown);
            dropper.join().unwrap();
            assert!(
                leaf.is_cancelled(),
                "a grandchild must be cancelled whether it was cascaded into \
                 directly or adopted mid-cancel"
            );
        });
    }

    /// Racing cancels with different reasons: exactly one wins and the whole
    /// subtree observes that single winner (no torn/mixed reasons).
    #[test]
    fn racing_cancels_have_exactly_one_winner_subtree_wide() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let child = root.child();
            let root2 = root.clone();
            let t = loom::thread::spawn(move || root2.cancel(CancelReason::DeadlineExceeded));
            root.cancel(CancelReason::Shutdown);
            t.join().unwrap();
            let winner = root.reason().expect("root cancelled");
            assert_eq!(
                child.reason(),
                Some(winner),
                "descendants must observe the single winning reason"
            );
        });
    }

    /// The waker path: polling
    /// `cancelled()` concurrently with a cancel must always resolve — the
    /// register-then-recheck-under-lock protocol admits no lost wake.
    #[test]
    fn cancelled_future_racing_cancel_always_resolves() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let child = root.child();
            let canceller = {
                let root = root.clone();
                loom::thread::spawn(move || root.cancel(CancelReason::Explicit))
            };
            let reason = loom::future::block_on(child.cancelled());
            canceller.join().unwrap();
            assert_eq!(reason, CancelReason::Explicit);
        });
    }

    /// The owner-retention fix under loom: token dropped concurrently with
    /// the ancestor cancel, while a waiter is outstanding — the waiter must
    /// still resolve in every interleaving.
    #[test]
    fn waiter_survives_concurrent_token_drop_and_cancel() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let child = root.child();
            let fut = child.cancelled();
            let dropper = loom::thread::spawn(move || drop(child));
            let canceller = {
                let root = root.clone();
                loom::thread::spawn(move || root.cancel(CancelReason::Shutdown))
            };
            let reason = loom::future::block_on(fut);
            dropper.join().unwrap();
            canceller.join().unwrap();
            assert_eq!(reason, CancelReason::Shutdown);
        });
    }

    /// Concurrent detaches on a parent/child chain: no deadlock (the
    /// ancestor-first lock order + retry-on-change protocol), and the
    /// remaining leaf still receives a subsequent cancel from the root.
    #[test]
    fn concurrent_detaches_keep_the_tree_cancellable() {
        loom::model(|| {
            let root = CancelToken::new_root();
            let a = root.child();
            let b = a.child();
            let leaf = b.child();
            let t1 = loom::thread::spawn(move || drop(a));
            let t2 = loom::thread::spawn(move || drop(b));
            t1.join().unwrap();
            t2.join().unwrap();
            root.cancel(CancelReason::Explicit);
            assert!(
                leaf.is_cancelled(),
                "after arbitrary detach interleavings the leaf must still be \
                 reachable by the root's cancel"
            );
        });
    }
}

#[cfg(loom)]
mod wake_bit_model {
    //! Protocol-level loom model of the SerialGroup wake-bit protocol
    //! (callback groups; implemented in
    //! cpp/include/aorta/lanes.hpp). Models {submit × drain-exit}
    //! interleavings: submit = push + fetch_or(NOTIFIED), enqueue iff
    //! prev==IDLE; drain entry = swap(RUNNING); exit = budget path
    //! (re-enqueue on non-empty) then CAS(RUNNING→IDLE), failure ⇒
    //! re-enqueue. Invariant: no interleaving leaves items queued with
    //! state IDLE and no drain scheduled.
    use loom::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    use loom::sync::Arc;

    const RUNNING: u8 = 1;
    const NOTIFIED: u8 = 2;

    struct Group {
        state: AtomicU8,
        queued: AtomicUsize,
        drains_scheduled: AtomicUsize,
    }

    fn submit(g: &Arc<Group>) {
        g.queued.fetch_add(1, Ordering::AcqRel);
        let prev = g.state.fetch_or(NOTIFIED, Ordering::AcqRel);
        if prev == 0 {
            g.drains_scheduled.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// One drain execution (budget 1): returns true if it re-enqueued.
    fn drain(g: &Arc<Group>) -> bool {
        g.state.swap(RUNNING, Ordering::AcqRel);
        // run up to 1 item
        let _ = g
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |q| {
                if q > 0 {
                    Some(q - 1)
                } else {
                    None
                }
            });
        // budget path: non-empty → re-enqueue
        if g.queued.load(Ordering::Acquire) > 0 {
            g.drains_scheduled.fetch_add(1, Ordering::AcqRel);
            return true;
        }
        // empty path: CAS(RUNNING→IDLE); failure ⇒ re-enqueue
        if g.state
            .compare_exchange(RUNNING, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            g.drains_scheduled.fetch_add(1, Ordering::AcqRel);
            return true;
        }
        false
    }

    #[test]
    fn wake_bit_no_strand_across_interleavings() {
        loom::model(|| {
            let g = Arc::new(Group {
                state: AtomicU8::new(0),
                queued: AtomicUsize::new(0),
                drains_scheduled: AtomicUsize::new(0),
            });
            // One initial submit schedules the first drain.
            submit(&g);
            let g2 = Arc::clone(&g);
            let racer = loom::thread::spawn(move || submit(&g2));
            // Execute scheduled drains to quiescence on this thread.
            let mut budget = 0;
            loop {
                let sched = g.drains_scheduled.load(Ordering::Acquire);
                if sched == 0 {
                    break;
                }
                g.drains_scheduled.fetch_sub(1, Ordering::AcqRel);
                let _ = drain(&g);
                budget += 1;
                assert!(budget < 16, "drain storm");
            }
            racer.join().unwrap();
            // Post-race: run any drains the racer scheduled.
            let mut budget2 = 0;
            while g.drains_scheduled.load(Ordering::Acquire) > 0 {
                g.drains_scheduled.fetch_sub(1, Ordering::AcqRel);
                let _ = drain(&g);
                budget2 += 1;
                assert!(budget2 < 16, "drain storm");
            }
            // THE invariant: nothing queued while IDLE with no drain.
            let queued = g.queued.load(Ordering::Acquire);
            let state = g.state.load(Ordering::Acquire);
            let sched = g.drains_scheduled.load(Ordering::Acquire);
            assert!(
                queued == 0 || state != 0 || sched > 0,
                "STRAND: queued={queued} state={state} sched={sched}"
            );
        });
    }
}
