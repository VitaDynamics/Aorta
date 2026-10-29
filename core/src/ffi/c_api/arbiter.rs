//! The completion arbiter — the cross-plane synchronization primitive
//! between the C++ coroutine plane and core-side producers.
//!
//! A GROUP carries one suspended C++ frame's pending-resolution counter
//! and THE one resume callback; each BRANCH is a registration a core-side
//! producer (RPC reply watcher, the timer bridge) completes exactly once.
//! Protocol (all transitions single CAS, AcqRel):
//!
//! - `complete(handle, status, payload)`: wins `ARMED→CLAIMED` or no-ops
//!   (the CAS loser). The winner invokes the branch's `complete_fn`
//!   SYNCHRONOUSLY — trampoline-class by contract: the SDK copies the
//!   call-scoped payload view into its branch slot, nothing more — then
//!   feeds the last-resolver rule.
//! - `revoke(handle)`: wins `ARMED→REVOKED` or no-ops; aborts a pending
//!   timer producer (abandoned `race` timeouts must not
//!   accumulate sleepers); the winner delivers `complete_fn` with
//!   `AORTA_ERR_CANCELLED` (the branch resolves Stopped) and feeds the
//!   last-resolver rule.
//! - **Last-resolver rule**: every branch resolution does
//!   `pending.fetch_sub(1)`; whoever observes the count hit zero ALONE
//!   invokes the group's `frame_resume_cb` — at most one scheduled
//!   resume ever exists, and by the time the frame runs every branch has
//!   fully resolved (destroy-while-queued is unrepresentable).
//! - Partial-arm: a producer that fails AFTER arming resolves
//!   its branch via `complete(handle, error, NULL)` — same path, same
//!   counter; no resume-before-suspend window exists because arming
//!   happens from the already-suspended frame (suspend-then-arm).
//!
//! Memory: groups live in the process registry keyed by u64; `Arc` keeps
//! any in-flight call safe against a concurrent `group_release` (the
//! refs==0 free point is the registry-entry drop + last in-flight
//! call ending). `release(handle)` is the consumer-side branch ref drop —
//! bookkeeping only; memory is group-owned here.

use std::ffi::c_void;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use super::*;
use std::sync::OnceLock as ArbiterOnceLock;

const BRANCH_UNARMED: u8 = 0;
const BRANCH_ARMED: u8 = 1;
const BRANCH_CLAIMED: u8 = 2;
const BRANCH_REVOKED: u8 = 3;

type CompleteFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    status: aorta_result_t,
    payload: *const aorta_bytes_view_t,
    completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
);
type ResumeFn = unsafe extern "C" fn(ctx: *mut c_void);

pub(crate) struct ArbiterBranch {
    state: AtomicU8,
    complete_fn: Mutex<Option<(CompleteFn, usize)>>,
    /// Abort for a timer-armed producer (revoke kills the sleeper).
    timer_abort: Mutex<Option<tokio::task::AbortHandle>>,
}

pub(crate) struct ArbiterGroup {
    pending: AtomicU32,
    resume_fn: ResumeFn,
    resume_ctx: usize,
    branches: Box<[ArbiterBranch]>,
}

// SAFETY: the raw callback pointers are invoked per the documented
// threading contract (complete_fn trampoline-class on the resolver
// thread; resume_fn once, by the last resolver).
unsafe impl Send for ArbiterGroup {}
// SAFETY: same contract as Send above — interior mutability is atomics
// plus mutexes; the raw pointers are only dereferenced under it.
unsafe impl Sync for ArbiterGroup {}

impl ArbiterGroup {
    /// Feed the last-resolver rule: the observer of pending hitting zero
    /// alone schedules THE one frame resume.
    fn resolve_one(&self) {
        let prev = self.pending.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "arbiter pending underflow");
        if prev == 1 {
            // SAFETY: resume_fn/ctx were provided at group_new; invoked
            // exactly once by construction (the counter is the arbiter).
            unsafe { (self.resume_fn)(self.resume_ctx as *mut c_void) };
        }
    }

    /// Resolve a branch that won its CAS: deliver the payload (or the
    /// stop status) to the SDK slot, then feed the counter.
    fn deliver_and_resolve(
        &self,
        branch: &ArbiterBranch,
        status: aorta_result_t,
        payload: *const aorta_bytes_view_t,
        completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
    ) {
        let cb = *branch.complete_fn.lock().unwrap();
        if let Some((f, ctx)) = cb {
            // SAFETY: the SDK registered (f, ctx) at arm; the view AND the
            // completion record are valid for this call only (the
            // record rides only where a stop reason exists — plain rows
            // pass NULL, the `call::on_done` contract).
            unsafe { f(ctx as *mut c_void, status, payload, completion) };
        }
        self.resolve_one();
    }
}

/// In-flight resolution counter (producer-complete quiesce):
/// the stop-drain loop must not conclude while a producer's
/// complete/revoke — whose resume may land in the pool residue — is
/// still executing on another thread. Guarded via the shared
/// [`InFlightGuard`](super::InFlightGuard) RAII idiom.
static RESOLUTIONS_IN_FLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn resolutions_in_flight() -> u32 {
    RESOLUTIONS_IN_FLIGHT.load(Ordering::Acquire) as u32
}

fn split_handle(handle: u64) -> (u64, usize) {
    (handle >> 8, (handle & 0xff) as usize)
}

fn lookup(handle: u64) -> Option<(Arc<ArbiterGroup>, usize)> {
    let (group_id, branch_idx) = split_handle(handle);
    let groups = registry().arbiters.lock();
    let group = groups.get(&group_id)?.clone();
    if branch_idx >= group.branches.len() {
        return None;
    }
    Some((group, branch_idx))
}

/// The lazy timer runtime for `aorta_timer_arm_arbiter`: C callers have
/// no ambient tokio runtime; one shared single-thread runtime serves all
/// arbiter timers (1 ms effective granularity).
fn timer_runtime() -> &'static tokio::runtime::Runtime {
    static RT: ArbiterOnceLock<tokio::runtime::Runtime> = ArbiterOnceLock::new();
    RT.get_or_init(|| {
        // One worker thread DRIVES the timers (a current_thread runtime
        // with no blocked driver would never fire them).
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .thread_name("aorta-arbiter-timer")
            .build()
            .expect("arbiter timer runtime")
    })
}

/// Create an arbiter group for one suspended frame. `n_branches` fixes
/// the pending counter; `frame_resume_cb(ctx)` is invoked EXACTLY ONCE,
/// by the last resolver. Dispatch priority is NOT recorded here: the SDK
/// frame carries its own class and posts the resume with it.
///
/// # Safety
/// `out_group` must be a valid pointer; `frame_resume_cb` must remain
/// callable until it fires (the frame's lifetime — guaranteed by the
/// last-resolver rule itself).
#[no_mangle]
pub unsafe extern "C" fn aorta_arbiter_group_new(
    frame_resume_cb: Option<unsafe extern "C" fn(ctx: *mut c_void)>,
    resume_ctx: *mut c_void,
    n_branches: u32,
    out_group: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(resume_fn) = frame_resume_cb else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "frame_resume_cb is null");
        };
        if out_group.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_group is null");
        }
        if n_branches == 0 || n_branches > 255 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "n_branches must be 1..=255");
        }
        let branches = (0..n_branches)
            .map(|_| ArbiterBranch {
                state: AtomicU8::new(BRANCH_UNARMED),
                complete_fn: Mutex::new(None),
                timer_abort: Mutex::new(None),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let group = Arc::new(ArbiterGroup {
            pending: AtomicU32::new(n_branches),
            resume_fn,
            resume_ctx: resume_ctx as usize,
            branches,
        });
        let id = registry().next_id.fetch_add(1, Ordering::Relaxed);
        registry().arbiters.lock().insert(id, group);
        // SAFETY: caller contract — out_group is valid.
        unsafe { *out_group = id };
        AORTA_OK
    })
}

/// Arm branch `branch_idx`: registers the PER-BRANCH completion callback
/// (where the payload view gets copied into the SDK's branch slot) and
/// returns the registration handle producers complete/revoke through.
/// Suspend-then-arm: call this from the already-suspended frame only.
///
/// # Safety
/// `out_handle` must be valid; `(complete_fn, complete_ctx)` must stay
/// callable until the branch resolves.
#[no_mangle]
pub unsafe extern "C" fn aorta_arbiter_arm(
    group: u64,
    branch_idx: u32,
    complete_fn: Option<
        unsafe extern "C" fn(
            ctx: *mut c_void,
            status: aorta_result_t,
            payload: *const aorta_bytes_view_t,
            completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
        ),
    >,
    complete_ctx: *mut c_void,
    out_handle: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(cb) = complete_fn else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "complete_fn is null");
        };
        if out_handle.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_handle is null");
        }
        let groups = registry().arbiters.lock();
        let Some(entry) = groups.get(&group) else {
            // Flow handle convention: stale/freed ids
            // no-op as NOT_FOUND — INVALID_ARGUMENT is reserved for
            // malformed inputs.
            return aorta_err(AORTA_ERR_NOT_FOUND, "arbiter group not found");
        };
        let Some(branch) = entry.branches.get(branch_idx as usize) else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "branch index out of range");
        };
        // The callback is stored BEFORE the ARMED publish —
        // a complete/revoke racing the arm window must never resolve a
        // branch with an empty slot.
        *branch.complete_fn.lock().unwrap() = Some((cb, complete_ctx as usize));
        if branch
            .state
            .compare_exchange(
                BRANCH_UNARMED,
                BRANCH_ARMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            *branch.complete_fn.lock().unwrap() = None;
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "branch already armed");
        }
        // Handle packing: bits[8..64] = group id, bits[0..8] = branch
        // index — group ids carry 56 usable bits. next_id()
        // is a process-local counter (2^56 mints at 1 GHz ≈ 2.3 years
        // nonstop); assert the invariant so a future id scheme cannot
        // silently corrupt handles.
        debug_assert!(group < (1u64 << 56), "arbiter group id exceeds 56 bits");
        // SAFETY: caller contract — out_handle is valid.
        unsafe { *out_handle = (group << 8) | u64::from(branch_idx) };
        AORTA_OK
    })
}

/// Producer side: resolve the branch with `status` + optional payload.
/// Wins `ARMED→CLAIMED` or NO-OPS (returns OK — the CAS loser contract:
/// exactly one resolution ever delivers).
///
/// # Safety
/// `payload` is NULL or valid for the duration of this call only.
#[no_mangle]
pub unsafe extern "C" fn aorta_arbiter_complete(
    handle: u64,
    status: aorta_result_t,
    payload: *const aorta_bytes_view_t,
) -> aorta_result_t {
    // External completers carry no stop reason — NULL record (the
    // record rides only where a reason exists, e.g. the scope watcher).
    // SAFETY: forwarded caller contract.
    unsafe { complete_with(handle, status, payload, std::ptr::null()) }
}

/// The one shared resolution path behind [`aorta_arbiter_complete`] and
/// the reason-carrying internal completers.
///
/// # Safety
/// `payload` and `completion` are NULL or valid for this call only.
pub(crate) unsafe fn complete_with(
    handle: u64,
    status: aorta_result_t,
    payload: *const aorta_bytes_view_t,
    completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
) -> aorta_result_t {
    ffi_guard(|| {
        let _guard = super::InFlightGuard::enter(&RESOLUTIONS_IN_FLIGHT);
        let Some((group, idx)) = lookup(handle) else {
            // Group already released (stop raced a late completion): the
            // resolution is moot — a no-op, reported NOT_FOUND per the
            // Flow stale-handle convention (every C++ caller
            // discards this code, so late completes stay harmless).
            return AORTA_ERR_NOT_FOUND;
        };
        let branch = &group.branches[idx];
        if branch
            .state
            .compare_exchange(
                BRANCH_ARMED,
                BRANCH_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return AORTA_OK; // CAS loser: already claimed/revoked
        }
        group.deliver_and_resolve(branch, status, payload, completion);
        AORTA_OK
    })
}

/// Consumer side: cancel the branch. Wins `ARMED→REVOKED` (delivering
/// `AORTA_ERR_CANCELLED` to the branch slot and feeding the counter) or
/// no-ops. Aborts a pending timer producer so abandoned timeouts do not
/// accumulate sleepers.
#[no_mangle]
pub extern "C" fn aorta_arbiter_revoke(handle: u64) -> aorta_result_t {
    ffi_guard(|| {
        let _guard = super::InFlightGuard::enter(&RESOLUTIONS_IN_FLIGHT);
        let Some((group, idx)) = lookup(handle) else {
            return AORTA_ERR_NOT_FOUND; // stale-handle no-op
        };
        let branch = &group.branches[idx];
        if branch
            .state
            .compare_exchange(
                BRANCH_ARMED,
                BRANCH_REVOKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return AORTA_OK;
        }
        if let Some(abort) = branch.timer_abort.lock().unwrap().take() {
            abort.abort(); // the sleeper never fires (no lingering task)
        }
        group.deliver_and_resolve(
            branch,
            AORTA_ERR_CANCELLED,
            std::ptr::null(),
            std::ptr::null(),
        );
        AORTA_OK
    })
}

/// Free the group registration. In-flight completes on other threads
/// finish safely (Arc); late completes against the freed id no-op.
#[no_mangle]
pub extern "C" fn aorta_arbiter_group_release(group: u64) -> aorta_result_t {
    ffi_guard(|| {
        // 0 = never allocated: no-op, like every sibling Flow _free
        // (the family's shared sentinel short-circuit).
        if group == 0 {
            return AORTA_OK;
        }
        // Double-free reports NOT_FOUND, like every other Flow handle
        // table — callers treating release as infallible
        // discard the code.
        match registry().arbiters.lock().remove(&group) {
            Some(_) => AORTA_OK,
            None => AORTA_ERR_NOT_FOUND,
        }
    })
}

/// The timer bridge: a core-side sleeper
/// completes the branch with `AORTA_OK` after `delay_ns`. Revoking the
/// branch aborts the sleeper. 1 ms effective granularity.
#[no_mangle]
pub extern "C" fn aorta_timer_arm_arbiter(handle: u64, delay_ns: u64) -> aorta_result_t {
    ffi_guard(|| {
        let Some((group, idx)) = lookup(handle) else {
            return aorta_err(AORTA_ERR_NOT_FOUND, "arbiter handle not found");
        };
        if group.branches[idx].state.load(Ordering::Acquire) != BRANCH_ARMED {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "branch is not armed");
        }
        let task = timer_runtime().spawn(async move {
            tokio::time::sleep(std::time::Duration::from_nanos(delay_ns)).await;
            // SAFETY: null payload; complete() handles freed groups.
            unsafe {
                let _ = aorta_arbiter_complete(handle, AORTA_OK, std::ptr::null());
            }
        });
        // Store-then-recheck: a revoke landing between the
        // spawn and this store found the abort slot empty — its CANCELLED
        // already delivered, but the sleeper would linger to full term.
        // Re-checking REVOKED after the store closes the window (either
        // the revoke takes the stored handle, or we abort here).
        *group.branches[idx].timer_abort.lock().unwrap() = Some(task.abort_handle());
        if group.branches[idx].state.load(Ordering::Acquire) == BRANCH_REVOKED {
            if let Some(abort) = group.branches[idx].timer_abort.lock().unwrap().take() {
                abort.abort();
            }
        }
        AORTA_OK
    })
}

/// The scope bridge (per-scope cancel parity
/// for `next()`/`next_for()`): a core-side watcher completes the branch
/// with `AORTA_ERR_CANCELLED` when the scope's cancel token fires.
/// Revoking the branch aborts the watcher (the timer bridge's
/// no-lingering-task discipline; the abort shares the branch's producer
/// slot — one producer per branch). Stale arbiter/scope handles
/// `NOT_FOUND`-no-op per the Flow handle convention.
#[no_mangle]
pub extern "C" fn aorta_scope_arm_arbiter(handle: u64, scope: u64) -> aorta_result_t {
    ffi_guard(|| {
        let Some((group, idx)) = lookup(handle) else {
            return aorta_err(AORTA_ERR_NOT_FOUND, "arbiter handle not found");
        };
        if group.branches[idx].state.load(Ordering::Acquire) != BRANCH_ARMED {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "branch is not armed");
        }
        let Some(scope_ref) = crate::ffi::c_api::flow::scope_ref(scope) else {
            return aorta_err(AORTA_ERR_NOT_FOUND, "scope not found");
        };
        let cancelled = scope_ref.token().cancelled();
        let task = timer_runtime().spawn(async move {
            // Flow TCK scenario `budget_expiry_parked`: a frame parked on this
            // watcher must also wake when the scope's BUDGET expires —
            // nothing else fires the token for a parked C-ABI frame
            // (checkpoint() is cooperative; Rust-plane parks get the same
            // wake from DispatchBoundary's per-park deadline arm). The
            // sleep only schedules the observation; checkpoint() stays the
            // ONE budget→token fire point (DRY), and the fresh
            // token().cancelled() await delivers whatever reason actually
            // won the race (an earlier real cancel beats the deadline).
            let reason = match scope_ref.remaining() {
                Some(remaining) => {
                    tokio::select! {
                        r = cancelled => r,
                        _ = tokio::time::sleep(remaining) => {
                            let _ = scope_ref.checkpoint();
                            scope_ref.token().cancelled().await
                        }
                    }
                }
                None => cancelled.await,
            };
            // The watcher is the one arbiter resolver
            // with a REAL stop reason — deliver it as a completion record
            // (closure-scoped; valid for the callback only).
            let stopped = crate::flow::Stopped::cancelled(reason);
            crate::ffi::c_api::flow::with_ffi_completion(&stopped, |completion| {
                // SAFETY: null payload; complete_with() handles freed
                // groups; the record is valid for this call.
                unsafe {
                    let _ =
                        complete_with(handle, AORTA_ERR_CANCELLED, std::ptr::null(), completion);
                }
            });
        });
        // Store-then-recheck — verbatim the timer bridge's window
        // close: a revoke landing between the spawn and this store found
        // the abort slot empty; recheck REVOKED after the store.
        *group.branches[idx].timer_abort.lock().unwrap() = Some(task.abort_handle());
        if group.branches[idx].state.load(Ordering::Acquire) == BRANCH_REVOKED {
            if let Some(abort) = group.branches[idx].timer_abort.lock().unwrap().take() {
                abort.abort();
            }
        }
        AORTA_OK
    })
}

/// Producer-window pin. A combinator's arm window
/// [pre-check .. post-arm recheck/self-revoke] must be COUNTED against
/// the stop drain's producer quiesce — otherwise an instruction-scale
/// gap between the recheck's `closed` load and the revoke's
/// ResolutionGuard lets `drain_to_empty` conclude with the refused
/// resume still en route to the residue (MT-only strand). Pin BEFORE
/// the pre-check; unpin after the recheck settles.
#[no_mangle]
pub extern "C" fn aorta_arbiter_resolutions_pin() {
    RESOLUTIONS_IN_FLIGHT.fetch_add(1, Ordering::AcqRel);
}

/// # Safety-free counterpart of [`aorta_arbiter_resolutions_pin`].
#[no_mangle]
pub extern "C" fn aorta_arbiter_resolutions_unpin() {
    RESOLUTIONS_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
}
