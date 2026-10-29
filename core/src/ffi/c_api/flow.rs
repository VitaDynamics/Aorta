//! Flow C-ABI surface — the `CancelToken`
//! handle + poll functions used by external-bus / RT-adjacent threads for
//! lifecycle coordination (estop / shutdown).
//!
//! Handles are opaque `u64`s in the process registry; bools are `u8*` outs
//! (per the ABI conventions). The `Budget` poll (`aorta_budget_remaining_ns`)
//! lives with its Scope-handle carrier — its signature takes a scope
//! handle.
//!
//! RT note: `aorta_token_is_cancelled` performs one registry-map lookup (a
//! short mutex) plus the token's own single atomic load — convenient, but
//! NOT the RT-safe poll. The RT-safe poll is
//! `aorta_token_cancelled_flag_ptr`: resolve the flag pointer once, then
//! read it lock-free (C11 acquire load) from any thread.

use super::*;

pub type aorta_token_t = u64;

/// `aorta_token_cancel` reason codes (see `aorta_sync::cancel::CancelReason`;
/// unknown non-zero values are accepted and decode as EXPLICIT).
pub const AORTA_CANCEL_REASON_EXPLICIT: u8 = 1;
pub const AORTA_CANCEL_REASON_DEADLINE: u8 = 2;
pub const AORTA_CANCEL_REASON_SHUTDOWN: u8 = 3;
/// Fail-fast cascade: a sibling failed in a non-supervised scope.
/// NOTE for flag-byte consumers: treat ANY unknown non-zero value
/// as "cancelled" — future reasons decode this way by design.
pub const AORTA_CANCEL_REASON_CHILD_FAILED: u8 = 4;

/// `aorta_scope_opts_t.supervision`: fail-fast cascade
/// (the default) — a child fault cancels the siblings.
pub const AORTA_SUPERVISION_FAIL_FAST: u8 = 0;
/// Contain the fault to the subtree (the pre-freeze `supervised` flag).
pub const AORTA_SUPERVISION_ISOLATE: u8 = 1;

/// Create a cancel token: a child of `parent`, or a new root when
/// `parent == 0`. A child of an already-cancelled parent is born cancelled.
///
/// # Safety
/// - `out_token` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_token_child(
    parent: aorta_token_t,
    out_token: *mut aorta_token_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; the inner unsafe covers the
    // `*out_token` writeback, null-checked at the top of the closure.
    ffi_guard(|| unsafe {
        if out_token.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_token is null");
        }
        let token = if parent == 0 {
            crate::flow::CancelToken::new_root()
        } else {
            let tokens = registry().cancel_tokens.lock();
            match tokens.get(&parent) {
                Some(parent_token) => parent_token.child(),
                None => return map_aorta_error(AortaError::not_found("cancel token not found")),
            }
        };
        let handle = alloc_handle();
        registry().cancel_tokens.lock().insert(handle, token);
        *out_token = handle;
        AORTA_OK
    })
}

/// Cancel `token` (and, eagerly, every descendant). First cancel wins;
/// `reason` must be non-zero (see the `AORTA_CANCEL_REASON_*` constants).
/// NOT RT-safe (takes registry locks) — the poll functions are.
#[no_mangle]
pub extern "C" fn aorta_token_cancel(token: aorta_token_t, reason: u8) -> aorta_result_t {
    ffi_guard(|| {
        if reason == 0 {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "reason 0 is the live state, not a cancel reason",
            );
        }
        let token_ref = {
            let tokens = registry().cancel_tokens.lock();
            match tokens.get(&token) {
                Some(t) => t.clone(),
                None => return map_aorta_error(AortaError::not_found("cancel token not found")),
            }
        };
        // Reason passthrough: unknown non-zero codes decode as EXPLICIT.
        let reason = crate::flow::CancelReason::from_raw(reason)
            .unwrap_or(crate::flow::CancelReason::Explicit);
        token_ref.cancel(reason);
        AORTA_OK
    })
}

/// Poll: `*out = 1` iff cancelled. The token-side check is one atomic load
/// (RT-safe); see the module RT note about the registry lookup.
///
/// # Safety
/// - `out` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_token_is_cancelled(
    token: aorta_token_t,
    out: *mut u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; the inner unsafe covers the `*out`
    // writeback, null-checked at the top of the closure.
    ffi_guard(|| unsafe {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let tokens = registry().cancel_tokens.lock();
        match tokens.get(&token) {
            Some(t) => {
                *out = u8::from(t.is_cancelled());
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("cancel token not found")),
        }
    })
}

/// Poll: the winning cancel reason (`0` while live).
///
/// # Safety
/// - `out_reason` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_token_reason(
    token: aorta_token_t,
    out_reason: *mut u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; the inner unsafe covers the
    // `*out_reason` writeback, null-checked at the top of the closure.
    ffi_guard(|| unsafe {
        if out_reason.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_reason is null");
        }
        let tokens = registry().cancel_tokens.lock();
        match tokens.get(&token) {
            Some(t) => {
                *out_reason = t.reason().map(|r| r.as_u8()).unwrap_or(0);
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("cancel token not found")),
        }
    })
}

/// Release a token handle. The node detaches from its parent's registry
/// (children are adopted by the grandparent, so cancellation still reaches
/// them). Freeing a handle does NOT cancel anything.
#[no_mangle]
pub extern "C" fn aorta_token_free(token: aorta_token_t) -> aorta_result_t {
    ffi_guard(|| {
        if token == 0 {
            return AORTA_OK;
        }
        match registry().cancel_tokens.lock().remove(&token) {
            Some(_) => AORTA_OK,
            None => map_aorta_error(AortaError::not_found("cancel token not found")),
        }
    })
}

/// Resolve-once, poll-lock-free RT-safe cancel flag.
///
/// Writes a pointer to the token's one-byte state flag: `0` = live,
/// non-zero = the winning `AORTA_CANCEL_REASON_*`. The pointer stays valid
/// until `aorta_token_free(token)`; after that, reading it is
/// use-after-free. Poll it from RT / external-bus threads with a C11
/// acquire load:
///
/// ```c
/// const uint8_t* flag = NULL;
/// aorta_token_cancelled_flag_ptr(token, &flag);
/// // hot loop: cancelled once the acquire load reads non-zero
/// if (atomic_load_explicit((const _Atomic uint8_t*)flag,
///                          memory_order_acquire) != 0) { }
/// ```
///
/// This — not `aorta_token_is_cancelled`, which takes a registry lock per
/// call — is the poll primitive for the "atomic, any thread"
/// contract.
///
/// # Safety
/// - `out_flag` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_token_cancelled_flag_ptr(
    token: aorta_token_t,
    out_flag: *mut *const u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; the inner unsafe covers the
    // `*out_flag` writeback, null-checked at the top of the closure.
    ffi_guard(|| unsafe {
        if out_flag.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_flag is null");
        }
        let tokens = registry().cancel_tokens.lock();
        match tokens.get(&token) {
            Some(t) => {
                *out_flag = t.state_flag_ptr();
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("cancel token not found")),
        }
    })
}

// ===========================================================================
// The Scope-facing C-ABI — handles + poll reads +
// the register-callback bridge. NO future crosses the boundary: what crosses
// is opaque `u64` handles, atomic poll reads, and `aorta_executor_post_resume`
// (a resumption enqueued on the executor's reserved `CoroutineResume` lane).
// ===========================================================================

pub type aorta_scope_t = u64;
pub type aorta_task_t = u64;

/// Scope creation options (size-asserted). `budget_ns == 0` means
/// unlimited; `name` may be NULL (unnamed). NO callback-group field (a
/// coroutine Scope always gets a private mutually-exclusive group).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct aorta_scope_opts_t {
    /// `aorta_priority_t`-compatible level (0=realtime .. 4=background).
    pub priority: u8,
    /// `AORTA_SUPERVISION_*` — the typed fault policy (it
    /// replaced the pre-freeze u8 bool). Values > 1 are refused
    /// INVALID_ARGUMENT (input-struct policy: a field that exists must
    /// work — no silent folds).
    pub supervision: u8,
    pub _pad: [u8; 2],
    /// Relative budget in ns; 0 = unlimited. A child narrows, never widens.
    pub budget_ns: u64,
    /// Optional scope name (interned once per unique string).
    pub name: *const c_char,
}

/// `AORTA_FLOW_PRIORITY_INHERIT` (255) decodes to `None` — inherit the
/// parent's priority (the C twin of Rust's `Option::None`); any other
/// unknown value clamps to Normal (the old always-Normal decode
/// would silently DEMOTE children whose parent runs above Normal).
pub const AORTA_FLOW_PRIORITY_INHERIT: u8 = 255;

fn priority_from_u8(raw: u8) -> Option<crate::event_queue::EventPriority> {
    match raw {
        AORTA_FLOW_PRIORITY_INHERIT => None,
        // Canonical lossy decode (priority index table): 0..=4 map, any other
        // unknown value clamps to Normal.
        raw => Some(crate::event_queue::event_priority_from_index_lossy(raw)),
    }
}

/// Scope names arrive as runtime C strings but core scope names are
/// `&'static str`: intern each UNIQUE name once (names are static labels in
/// practice; per-goal dynamic names would leak one copy per unique string).
fn intern_scope_name(raw: *const c_char) -> &'static str {
    if raw.is_null() {
        return "";
    }
    // SAFETY: caller contract — `name`, when non-null, is a valid
    // NUL-terminated string for the duration of the call.
    let Ok(s) = unsafe { CStr::from_ptr(raw) }.to_str() else {
        return "";
    };
    if s.is_empty() {
        return "";
    }
    static NAMES: OnceLock<parking_lot::Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(|| parking_lot::Mutex::new(HashMap::new()))
        .lock();
    if let Some(interned) = names.get(s) {
        return interned;
    }
    let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
    names.insert(s.to_string(), leaked);
    // Each UNIQUE name leaks by design (static labels are the
    // contract) — a growing table means per-goal dynamic names. Warn once.
    if names.len() > 1024 {
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            crate::aorta_core_warn!(
                "Aorta WARN: >1024 unique Flow scope names interned — dynamic \
                 (per-goal?) names leak one copy each; use STATIC labels"
            );
        }
    }
    leaked
}

/// The ONE input validation for
/// `aorta_scope_opts_t` — EVERY entry that decodes it calls this first
/// (the decode itself is total and defensive, but silent folding of an
/// unknown supervision byte violates the typed input-boundary contract).
///
/// # Safety
/// `opts` is NULL (defaults) or valid per the callers' contracts.
unsafe fn validate_scope_opts(opts: *const aorta_scope_opts_t) -> Option<aorta_result_t> {
    // SAFETY: see fn-level `# Safety` — `opts` is non-null here (checked
    // by the short-circuit) and valid per the caller's contract.
    if !opts.is_null() && unsafe { (*opts).supervision } > AORTA_SUPERVISION_ISOLATE {
        return Some(aorta_err(
            AORTA_ERR_INVALID_ARGUMENT,
            "aorta_scope_opts_t.supervision: unknown policy value",
        ));
    }
    None
}

fn scope_opts_from_ffi(opts: *const aorta_scope_opts_t) -> crate::flow::ScopeOpts {
    if opts.is_null() {
        return crate::flow::ScopeOpts::default();
    }
    // SAFETY: caller contract — non-null `opts` points at a valid struct.
    let o = unsafe { &*opts };
    crate::flow::ScopeOpts {
        priority: priority_from_u8(o.priority),
        budget: (o.budget_ns > 0).then(|| std::time::Duration::from_nanos(o.budget_ns)),
        supervision: match o.supervision {
            AORTA_SUPERVISION_FAIL_FAST => crate::flow::Supervision::FailFast,
            AORTA_SUPERVISION_ISOLATE => crate::flow::Supervision::Isolate,
            // EVERY caller runs validate_scope_opts() first —
            // this arm is the defensive default, never reachable via the
            // C ABI entries.
            _ => crate::flow::Supervision::FailFast,
        },
        name: intern_scope_name(o.name),
    }
}

/// Create a Flow scope: a child of `parent`, or a fresh root when
/// `parent == 0`. Opts may be NULL (defaults). A child narrows budget and
/// never raises priority.
///
/// # Safety
/// - `opts` is NULL or valid; `out_scope` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_child(
    parent: aorta_scope_t,
    opts: *const aorta_scope_opts_t,
    out_scope: *mut aorta_scope_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; the inner unsafe covers the
    // `*out_scope` writeback, null-checked at the top of the closure.
    ffi_guard(|| unsafe {
        if out_scope.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_scope is null");
        }
        if let Some(code) = validate_scope_opts(opts) {
            return code;
        }
        let scope_opts = scope_opts_from_ffi(opts);
        let scope = if parent == 0 {
            crate::flow::Scope::new_root(scope_opts)
        } else {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&parent) {
                Some(parent_scope) => parent_scope.child(scope_opts),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        let handle = alloc_handle();
        registry().flow_scopes.lock().insert(handle, scope);
        *out_scope = handle;
        AORTA_OK
    })
}

/// Cancel the scope and every descendant (one-way, eager). NOT RT-safe.
#[no_mangle]
pub extern "C" fn aorta_scope_cancel(scope: aorta_scope_t, reason: u8) -> aorta_result_t {
    ffi_guard(|| {
        if reason == 0 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "reason 0 is the live state");
        }
        let scope_ref = {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => s.clone(),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        scope_ref.cancel(
            crate::flow::CancelReason::from_raw(reason)
                .unwrap_or(crate::flow::CancelReason::Explicit),
        );
        AORTA_OK
    })
}

/// Poll: `*out = 1` iff the scope is cancelled (convenience form; the
/// RT-safe poll is [`aorta_scope_cancelled_flag_ptr`]).
///
/// # Safety
/// - `out` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_is_cancelled(
    scope: aorta_scope_t,
    out: *mut u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out`.
    ffi_guard(|| unsafe {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let scopes = registry().flow_scopes.lock();
        match scopes.get(&scope) {
            Some(s) => {
                *out = u8::from(s.is_cancelled());
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("flow scope not found")),
        }
    })
}

/// Resolve-once, poll-lock-free RT-safe cancel flag for a scope's token
/// (same contract as [`aorta_token_cancelled_flag_ptr`]; valid until
/// `aorta_scope_free`).
///
/// # Safety
/// - `out_flag` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_cancelled_flag_ptr(
    scope: aorta_scope_t,
    out_flag: *mut *const u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_flag`.
    ffi_guard(|| unsafe {
        if out_flag.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_flag is null");
        }
        let scopes = registry().flow_scopes.lock();
        match scopes.get(&scope) {
            Some(s) => {
                *out_flag = s.token().state_flag_ptr();
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("flow scope not found")),
        }
    })
}

/// RT-safe budget poll: `*out_has = 0` for an unlimited
/// budget; else `*out_ns` = remaining ns (0 = expired). Pure read — pair
/// with the cancel poll (cooperative checkpoints observe both).
///
/// # Safety
/// - `out_has` and `out_ns` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn aorta_budget_remaining_ns(
    scope: aorta_scope_t,
    out_has: *mut u8,
    out_ns: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the writebacks.
    ffi_guard(|| unsafe {
        if out_has.is_null() || out_ns.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null out pointer");
        }
        let scopes = registry().flow_scopes.lock();
        match scopes.get(&scope) {
            Some(s) => {
                match s.remaining() {
                    Some(rem) => {
                        *out_has = 1;
                        *out_ns = u64::try_from(rem.as_nanos()).unwrap_or(u64::MAX);
                    }
                    None => {
                        *out_has = 0;
                        *out_ns = 0;
                    }
                }
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("flow scope not found")),
        }
    })
}

/// Cooperative checkpoint: observes budget expiry (the first
/// observer fires the token with DEADLINE), then reports cancellation.
/// Returns `AORTA_OK` while live, `AORTA_ERR_CANCELLED` once cancelled.
#[no_mangle]
pub extern "C" fn aorta_scope_checkpoint(scope: aorta_scope_t) -> aorta_result_t {
    ffi_guard(|| {
        let scope_ref = {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => s.clone(),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        match scope_ref.checkpoint() {
            Ok(()) => AORTA_OK,
            Err(err) => map_aorta_error(err.into()),
        }
    })
}

/// BLOCKING: wait until every child (Rust tasks and external C tasks) has
/// finished. Does not cancel anything. Returns
/// `AORTA_ERR_REENTRANT` when called from inside an aorta callback.
#[no_mangle]
pub extern "C" fn aorta_scope_close(scope: aorta_scope_t) -> aorta_result_t {
    ffi_guard(|| {
        let scope_ref = {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => s.clone(),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        match crate::ffi_block_on!(scope_ref.close()) {
            Ok(()) => AORTA_OK,
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

/// Observe the scope's cancel reason. `out_has_reason`
/// = 0 with reason 0 when the scope is live; otherwise the EXACT reason
/// byte (unknown future values verbatim — the loss-free carrier).
///
/// # Safety
/// - `out_has_reason` and `out_reason` must be valid for writes.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_cancel_reason(
    scope: aorta_scope_t,
    out_has_reason: *mut u8,
    out_reason: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_has_reason.is_null() || out_reason.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out pointer is null");
        }
        let scope_ref = {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => s.clone(),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        let reason = scope_ref.cancel_reason();
        // SAFETY: null-checked above; caller contract covers writability.
        unsafe {
            *out_has_reason = u8::from(reason.is_some());
            *out_reason = reason.map(|r| r.as_u8()).unwrap_or(0);
        }
        AORTA_OK
    })
}

/// Bounded shutdown: `cancel(reason)` + `close()` with a
/// grace window; on expiry the still-running subtree is DETACHED and the
/// count reported (never silently folded into success — a non-zero
/// `out_detached` is the caller's signal that stragglers leaked past the
/// grace). BLOCKING; refuses from callback contexts like
/// `aorta_scope_close`.
///
/// # Safety
/// - `out_detached` must be valid for writes.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_shutdown_with_grace(
    scope: aorta_scope_t,
    reason: u8,
    grace_ns: u64,
    out_detached: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_detached.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_detached is null");
        }
        let scope_ref = {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => s.clone(),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        // Reason 0 = the conventional default (Shutdown) for this entry
        // point; any other byte rides the loss-free carrier.
        let reason = crate::flow::CancelReason::from_raw(reason)
            .unwrap_or(crate::flow::CancelReason::Shutdown);
        let grace = std::time::Duration::from_nanos(grace_ns);
        match crate::ffi_block_on!(scope_ref.shutdown_with_grace(reason, grace)) {
            Ok(detached) => {
                // SAFETY: null-checked above.
                unsafe {
                    *out_detached = detached;
                }
                AORTA_OK
            }
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

/// Release a scope handle. Does NOT cancel or wait — pair with
/// `aorta_scope_cancel` / `aorta_scope_close` for lifecycle semantics.
#[no_mangle]
pub extern "C" fn aorta_scope_free(scope: aorta_scope_t) -> aorta_result_t {
    ffi_guard(|| {
        if scope == 0 {
            return AORTA_OK;
        }
        match registry().flow_scopes.lock().remove(&scope) {
            Some(_) => AORTA_OK,
            None => map_aorta_error(AortaError::not_found("flow scope not found")),
        }
    })
}

/// A C task body: runs on an executor dispatch thread. It may
/// suspend itself by arranging a later `aorta_executor_post_resume`; the
/// task stays open for scope accounting until `aorta_task_finish`.
pub type aorta_task_fn = Option<unsafe extern "C" fn(user_data: *mut c_void)>;

/// Spawn a C task body onto `executor`, accounted as a child of `scope`
/// (the register-callback bridge; the executor could be bound
/// implicitly through the scope — the explicit handle here is the honest
/// form until scope↔executor topology lands).
///
/// The body is dispatched as a `CoroutineResume` event (admission-bypassing
/// reserved lane). Completion is NOT inferred from the body returning — a
/// C++ coroutine's first slice returns at its first suspension point — the
/// C side calls `aorta_task_finish(task)` when the logical task completes.
///
/// # Safety
/// - `body` must be non-null; `out_task` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_spawn(
    executor: u64,
    scope: aorta_scope_t,
    body: aorta_task_fn,
    user_data: *mut c_void,
    out_task: *mut aorta_task_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_task`.
    ffi_guard(|| unsafe {
        if out_task.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_task is null");
        }
        let Some(body) = body else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "body is null");
        };
        let scope_ref = {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => s.clone(),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        // The cached sender (never `entry.exec` — `aorta_executor_spin`
        // holds that mutex for the whole spin; taking it here would deadlock
        // a post from inside a dispatched callback).
        let sender = {
            let executors = registry().executors.lock();
            match executors.get(&executor) {
                Some(entry) => entry.sender.clone(),
                None => return map_aorta_error(AortaError::not_found("executor not found")),
            }
        };
        let Some(external) = scope_ref.begin_external_child() else {
            return aorta_err(
                AORTA_ERR_CANCELLED,
                "scope is closing: new tasks are refused",
            );
        };
        let user_data = user_data as usize;
        // Priority wiring: the raw-task dispatch inherits the SCOPE's priority.
        let scope_priority = scope_ref.priority();
        let event = crate::event_queue::Event::new_with_priority(
            crate::event_queue::EventKind::CoroutineResume,
            scope_priority,
            move || {
                // The body observes cancellation via its scope polls; a task
                // spawned into an already-cancelled scope still runs (it
                // must observe and unwind).
                // SAFETY: `body` is the non-null registered task entry; the
                // caller guarantees `user_data` validity for the task's life.
                // (Lexically inside the entry's `unsafe` block, so no nested
                // block — clippy flags redundancy otherwise.)
                body(user_data as *mut c_void);
            },
        );
        // Register + publish the handle BEFORE dispatching: the body may run
        // (and its C++ frame may finish, calling `aorta_task_finish`) before
        // this function returns — the handle must already resolve.
        let handle = alloc_handle();
        registry().flow_tasks.lock().insert(handle, external);
        *out_task = handle;
        if let Err(err) = sender.try_send(event) {
            drop(err);
            if let Some(external) = registry().flow_tasks.lock().remove(&handle) {
                external.finish();
            }
            *out_task = 0;
            return aorta_err(
                AORTA_ERR_CANCELLED,
                "executor stopped: task body not dispatched",
            );
        }
        AORTA_OK
    })
}

/// Pure accounting form of [`aorta_scope_spawn`] for language bindings that
/// dispatch the first slice themselves (the C++20 `spawn`): registers an
/// open task on `scope` WITHOUT dispatching anything. The binding then
/// stores the handle where its completion path can see it and posts the
/// first slice via [`aorta_executor_post_resume`] — closing the race where
/// a dispatched body finishes before the spawn call returns the handle.
/// Balance with exactly one `aorta_task_finish`.
///
/// # Safety
/// - `out_task` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_begin_task(
    scope: aorta_scope_t,
    out_task: *mut aorta_task_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_task`.
    ffi_guard(|| unsafe {
        if out_task.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_task is null");
        }
        let scope_ref = {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => s.clone(),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        let Some(external) = scope_ref.begin_external_child() else {
            return aorta_err(
                AORTA_ERR_CANCELLED,
                "scope is closing: new tasks are refused",
            );
        };
        let handle = alloc_handle();
        registry().flow_tasks.lock().insert(handle, external);
        *out_task = handle;
        AORTA_OK
    })
}

/// Mark a spawned C task complete (releases its scope accounting). Call
/// exactly once, when the logical task (e.g. the C++ coroutine frame)
/// reaches its terminal state.
#[no_mangle]
pub extern "C" fn aorta_task_finish(task: aorta_task_t) -> aorta_result_t {
    ffi_guard(|| match registry().flow_tasks.lock().remove(&task) {
        Some(external) => {
            external.finish();
            AORTA_OK
        }
        None => map_aorta_error(AortaError::not_found("flow task not found")),
    })
}

/// The register-callback bridge: enqueue `resume(handle)` as a
/// `CoroutineResume` event on `executor`'s reserved lane. The executor —
/// never the caller — runs it, which is what makes re-entry from C++/Python
/// scope bodies structurally legal (no reentrancy fence on the Flow path).
///
/// Exactly-once: on success the resumption runs exactly once on a dispatch
/// thread. `AORTA_ERR_CANCELLED` means the executor is stopped and will
/// never dispatch it — the CALLER must then resume inline into cancellation
/// (shutdown rule: frames are resumed-and-unwound, never destroyed).
/// The delivery record handed to a posted resumption (an OUTPUT struct:
/// always allocated by the library; per the Flow C-ABI evolution policy it
/// may GROW by appending fields together with an `AORTA_ABI_VERSION` bump —
/// old callbacks keep reading a valid prefix). The pointer is valid ONLY
/// for the duration of the callback invocation: copy fields out, never
/// retain it.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct aorta_resume_event_t {
    /// The caller's handle, verbatim.
    pub handle: *mut c_void,
    /// `AORTA_OK` = a normal executor dispatch; `AORTA_ERR_CANCELLED` = a
    /// stop-drain delivery (you are inline on the draining thread —
    /// observe your scope and unwind).
    pub status: aorta_result_t,
    /// Grown into the former padding (size unchanged):
    /// `AORTA_STOP_KIND_*` for the delivery. NONE on a normal dispatch;
    /// CANCELLED on the stop-drain path.
    pub stop_kind: u8,
    /// The cancel-reason byte when `stop_kind == CANCELLED` (stop-drain
    /// deliveries are runtime shutdown: `AORTA_CANCEL_REASON_SHUTDOWN`).
    pub cancel_reason: u8,
    pub _pad: [u8; 2],
}

// ===========================================================================
// The canonical Flow completion record:
// OUTPUT-only — always allocated by the library, valid ONLY for the duration
// of the callback invocation (copy fields out, never retain the pointer or
// the detail view). Carries the full stop vocabulary that `(code, payload)`
// callbacks lose: the exact cancel-reason byte (unknown values verbatim) and
// the fault's stable code + UTF-8 diagnostic.
// ===========================================================================

/// `aorta_flow_completion_t.stop_kind`: a successful completion.
pub const AORTA_STOP_KIND_NONE: u8 = 0;
/// Cooperative cancellation (deadline expiry included — the reason byte
/// distinguishes it; a deadline is never a third vocabulary).
pub const AORTA_STOP_KIND_CANCELLED: u8 = 1;
/// A fault: panic (AORTA_ERR_INTERNAL) or a transport/service failure
/// carrying its original stable code.
pub const AORTA_STOP_KIND_FAULT: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct aorta_flow_completion_t {
    /// The stable result code: `AORTA_ERR_CANCELLED` for every
    /// cancellation; the ORIGINAL fault code for faults. (Legacy `code`
    /// callback parameters keep their pre-record mappings bit-for-bit —
    /// this field is the normative vocabulary.)
    pub code: aorta_result_t,
    /// One of the `AORTA_STOP_KIND_*` constants.
    pub stop_kind: u8,
    /// The exact cancel-reason wire byte (`AORTA_CANCEL_REASON_*` or an
    /// unknown future value, preserved verbatim); 0 unless cancelled.
    pub cancel_reason: u8,
    pub _padding: [u8; 2],
    /// Fault diagnostic (UTF-8, no NUL guarantee); empty unless FAULT.
    /// Borrowed — valid only during the callback.
    pub detail: aorta_bytes_view_t,
}

/// Build the completion record for `stopped` and hand it to `f` — the ONE
/// conversion point (bridges MUST NOT construct the record
/// independently). Closure-scoped by design: the record
/// borrows the detail bytes and the slice descriptor lives on this frame,
/// so returning a bare record would dangle.
pub(crate) fn with_ffi_completion<R>(
    stopped: &crate::flow::Stopped,
    f: impl FnOnce(*const aorta_flow_completion_t) -> R,
) -> R {
    use crate::flow::StopReason;
    let (record, _slice_anchor);
    match stopped.reason() {
        StopReason::Cancelled(reason) => {
            _slice_anchor = aorta_bytes_slice_t::default();
            record = aorta_flow_completion_t {
                code: AORTA_ERR_CANCELLED,
                stop_kind: AORTA_STOP_KIND_CANCELLED,
                cancel_reason: reason.as_u8(),
                _padding: [0; 2],
                detail: aorta_bytes_view_t::default(),
            };
            f(&record)
        }
        StopReason::Fault { code, message } => {
            _slice_anchor = aorta_bytes_slice_t {
                data: message.as_ptr(),
                len: message.len(),
            };
            record = aorta_flow_completion_t {
                code: *code,
                stop_kind: AORTA_STOP_KIND_FAULT,
                cancel_reason: 0,
                _padding: [0; 2],
                detail: aorta_bytes_view_t {
                    slices: &_slice_anchor,
                    slice_count: 1,
                    total_len: message.len(),
                },
            };
            f(&record)
        }
    }
}

/// **Memory discipline:** the reserved lane is UNBOUNDED — its
/// bounded-memory property is a caller contract: at most ONE outstanding
/// resume per live frame (post again only after the previous resume was
/// delivered). The shipped C++ awaitables satisfy this structurally; a raw
/// C caller that posts in a loop grows heap without backpressure (a
/// one-shot WARN fires past 65_536 queued resumes).
#[no_mangle]
pub extern "C" fn aorta_executor_post_resume(
    executor: u64,
    resume: Option<unsafe extern "C" fn(event: *const aorta_resume_event_t)>,
    handle: *mut c_void,
    priority: u8,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(resume) = resume else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "resume is null");
        };
        // The cached sender (never `entry.exec` — `aorta_executor_spin`
        // holds that mutex for the whole spin; taking it here would deadlock
        // a post from inside a dispatched callback).
        let sender = {
            let executors = registry().executors.lock();
            match executors.get(&executor) {
                Some(entry) => entry.sender.clone(),
                None => return map_aorta_error(AortaError::not_found("executor not found")),
            }
        };
        let handle = handle as usize;
        // Priority wiring: the resume dispatches at the caller's priority (255 =
        // inherit-default = Normal) — honored by the WRR scheduler in
        // priority mode, ignored in FIFO mode (documented).
        let event_priority =
            priority_from_u8(priority).unwrap_or(crate::event_queue::EventPriority::Normal);
        let event = crate::event_queue::Event::new_with_priority(
            crate::event_queue::EventKind::CoroutineResume,
            event_priority,
            move || {
                // Shutdown rule: a drain-delivered resume reports CANCELLED so the
                // frame can distinguish teardown from a normal dispatch.
                let status = if crate::flow::reentrancy::is_stop_draining() {
                    AORTA_ERR_CANCELLED
                } else {
                    AORTA_OK
                };
                let (stop_kind, cancel_reason) = if status == AORTA_OK {
                    (AORTA_STOP_KIND_NONE, 0)
                } else {
                    (AORTA_STOP_KIND_CANCELLED, AORTA_CANCEL_REASON_SHUTDOWN)
                };
                let event = aorta_resume_event_t {
                    handle: handle as *mut c_void,
                    status,
                    stop_kind,
                    cancel_reason,
                    _pad: [0; 2],
                };
                // SAFETY: `resume` is the non-null registered resumption; the
                // caller guarantees `handle` stays valid until resumed. The
                // event pointer is a stack local, valid for this call only
                // (the documented contract).
                unsafe { resume(&event) };
            },
        );
        match sender.try_send(event) {
            Ok(()) => AORTA_OK,
            Err(_) => aorta_err(
                AORTA_ERR_CANCELLED,
                "executor stopped: resume inline into cancellation",
            ),
        }
    })
}

/// Async RPC bridge (`aorta_client_call_ctx`): issue the call
/// inside `scope` (0 = no scope: plain call) on the shared runtime;
/// `on_done(code, payload_view, user_data)` fires exactly once from a
/// runtime worker (marked as a callback context — post, don't block, from
/// it; the C++ awaitable posts its coroutine resume via
/// `aorta_executor_post_resume`).
///
/// # Safety
/// - `request` must be a valid view; it stays unused after the call
///   returns (the request bytes are copied).
/// - `on_done` must be non-null and, together with `user_data`, must stay
///   valid until the callback fires (exactly once, from a runtime
///   worker) — freeing the caller's context before then is a UAF.
#[no_mangle]
pub unsafe extern "C" fn aorta_client_call_ctx(
    client: u64,
    scope: aorta_scope_t,
    request: *const aorta_bytes_view_t,
    on_done: Option<
        unsafe extern "C" fn(
            code: aorta_result_t,
            payload: *const aorta_bytes_view_t,
            completion: *const aorta_flow_completion_t,
            ud: *mut c_void,
        ),
    >,
    user_data: *mut c_void,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner unsafe covers the request-view
    // deref (null-checked) and the callback dispatch closure.
    ffi_guard(|| unsafe {
        let Some(on_done) = on_done else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "on_done is null");
        };
        if request.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "request is null");
        }
        let client_arc = {
            let clients = registry().clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("client not found")),
            }
        };
        let scope_ref = if scope == 0 {
            None
        } else {
            let scopes = registry().flow_scopes.lock();
            match scopes.get(&scope) {
                Some(s) => Some(s.clone()),
                None => return map_aorta_error(AortaError::not_found("flow scope not found")),
            }
        };
        let request_bytes = match zbytes_from_ffi_view(request, "request") {
            Ok(b) => b,
            Err(err) => return map_aorta_error(err),
        };
        let user_data = user_data as usize;
        runtime().spawn(async move {
            // The ORIGINAL Stopped survives to the
            // callback as a completion record; the legacy `code` parameter
            // keeps its pre-record mapping bit-for-bit (scoped deadline →
            // TIMEOUT, scoped fault → INTERNAL, plain-call errors →
            // their own stable code).
            let outcome: Result<_, crate::flow::Stopped> = match &scope_ref {
                Some(s) => client_arc.call_in(s, request_bytes).await,
                None => client_arc
                    .call(request_bytes)
                    .await
                    .map_err(|err| crate::flow::Stopped::fault_from_error(&err)),
            };
            // Callback context: blocking FFI re-entry from on_done must
            // classify reentrant (post, don't block).
            let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
            let user_data = user_data as *mut c_void;
            match outcome {
                Ok(payload) => {
                    let mut single = aorta_bytes_slice_t::default();
                    let mut slices = Vec::new();
                    let view = bytes_view_from_zbytes_borrowed(&payload, &mut single, &mut slices);
                    ffi_guard_void(|| {
                        // SAFETY: `on_done` is the registered non-null done
                        // callback; the view borrows locals valid for the call
                        // (enclosing entry unsafe block covers the call).
                        on_done(AORTA_OK, ptr::addr_of!(view), ptr::null(), user_data);
                    });
                }
                Err(stopped) => {
                    let legacy = match stopped.reason() {
                        _ if scope_ref.is_none() => {
                            // Plain-call faults keep their own stable code
                            // (fault_from_error already ran the ONE mapping).
                            stopped.fault_code().unwrap_or(AORTA_ERR_INTERNAL)
                        }
                        _ if stopped.is_deadline() => AORTA_ERR_TIMEOUT,
                        crate::flow::StopReason::Fault { .. } => AORTA_ERR_INTERNAL,
                        _ => AORTA_ERR_CANCELLED,
                    };
                    let view = aorta_bytes_view_t::default();
                    with_ffi_completion(&stopped, |completion| {
                        ffi_guard_void(|| {
                            // SAFETY: as above; an error passes an empty view
                            // and the closure-scoped completion record.
                            on_done(legacy, ptr::addr_of!(view), completion, user_data);
                        });
                    });
                }
            }
        });
        AORTA_OK
    })
}

/// Versioned executor options carrying `worker_count` (the executor-kind
/// enum mapping is preserved — `worker_count = 1` on a SINGLE
/// executor is the single-thread inline path, a MULTI executor takes
/// `worker_count` dispatch workers). `sched`: 0 = FIFO (the prior default),
/// 1 = priority. Size-asserted; frozen once Python mirrors it.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct aorta_executor_sched_options_t {
    /// 0 = default (1). On SINGLE executors values > 1 are invalid.
    pub worker_count: u32,
    /// 0 = FIFO, 1 = priority round-robin.
    pub sched: u8,
    pub _pad: [u8; 3],
}

/// Create an executor from the scheduler options. Executor creation
/// stays node-free — attach nodes via
/// `aorta_executor_add_node`.
///
/// # Safety
/// - `opts` is NULL (defaults) or valid; `out` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_executor_new_with_sched_options(
    executor_type: aorta_executor_type_t,
    opts: *const aorta_executor_sched_options_t,
    out: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the opts deref
    // and the `*out` writeback (both null-checked).
    ffi_guard(|| unsafe {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let (worker_count, sched) = if opts.is_null() {
            (1u32, 0u8)
        } else {
            let o = &*opts;
            (o.worker_count.max(1), o.sched)
        };
        if sched > 1 {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "sched must be 0 (fifo) or 1 (priority)",
            );
        }
        let mode = if sched == 1 {
            crate::executor::SchedulerMode::Priority
        } else {
            crate::executor::SchedulerMode::Fifo
        };
        let options = crate::executor::ExecutorOptions {
            mode,
            ..Default::default()
        };
        match executor_type {
            aorta_executor_type_t::AORTA_EXECUTOR_SINGLE => {
                if worker_count > 1 {
                    return aorta_err(
                        AORTA_ERR_INVALID_ARGUMENT,
                        "worker_count > 1 requires a MULTI executor (\
                         a SINGLE executor is the single-thread inline path)",
                    );
                }
                let exec = ExecutorInstance::Single(SingleThreadedExecutor::new_with_options(
                    DEFAULT_EXECUTOR_QUEUE,
                    options,
                ));
                let is_multi = matches!(&exec, ExecutorInstance::Multi(_));
                let stop = exec.stop_handle();
                let handle = alloc_handle();
                registry().executors.lock().insert(
                    handle,
                    Arc::new(ExecutorEntry {
                        sender: exec.queue_sender(),
                        exec: Mutex::new(exec),
                        is_multi,
                        stop,
                    }),
                );
                *out = handle;
                AORTA_OK
            }
            aorta_executor_type_t::AORTA_EXECUTOR_MULTI => {
                let exec = ExecutorInstance::Multi(MultiThreadedExecutor::new_with_options(
                    DEFAULT_EXECUTOR_QUEUE,
                    worker_count as usize,
                    options,
                ));
                let is_multi = matches!(&exec, ExecutorInstance::Multi(_));
                let stop = exec.stop_handle();
                let handle = alloc_handle();
                registry().executors.lock().insert(
                    handle,
                    Arc::new(ExecutorEntry {
                        sender: exec.queue_sender(),
                        exec: Mutex::new(exec),
                        is_multi,
                        stop,
                    }),
                );
                *out = handle;
                AORTA_OK
            }
            // The legacy CURRENT/ASYNC aliases keep the original constructor; the
            // sched-options surface targets the two real engines (the
            // existing enum mapping is preserved, not widened here).
            _ => aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "aorta_executor_new_with_sched_options accepts SINGLE or MULTI",
            ),
        }
    })
}

// ===========================================================================
// The depth-1 latest-value watch. `load()` at the
// C ABI copies the latest value into a caller-owned buffer — simple FFI
// ownership, no lifetime; zero-copy at the C ABI remains the recv() /
// sample-handle borrow path. In Rust, load() returns an Arc handle.
// ===========================================================================

pub type aorta_watch_t = u64;

/// Declare a depth-1 latest-value stream over `topic` (`Node::watch`,
/// Flow Concepts §6.3): the Zenoh RX stores each sample's payload handle into
/// the watch; `aorta_watch_load` reads the latest.
///
/// # Safety
/// - `topic` must be a valid NUL-terminated UTF-8 string; `out_watch` must be
///   a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_watch_create(
    node: u64,
    topic: *const c_char,
    out_watch: *mut aorta_watch_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the CStr deref
    // and `*out_watch` writeback (both null-checked).
    ffi_guard(|| unsafe {
        if topic.is_null() || out_watch.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let Ok(topic) = CStr::from_ptr(topic).to_str() else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in topic");
        };
        let node_obj = { registry().nodes.lock().get(&node).cloned() };
        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };
        let watch = match crate::ffi_block_on!(node_obj.watch(topic)) {
            Ok(Ok(watch)) => watch,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };
        let handle = alloc_handle();
        registry().flow_watches.lock().insert(handle, watch);
        *out_watch = handle;
        AORTA_OK
    })
}

/// Copy the latest value into `out_buf` (soft-RT, one-buffer copy). `*out_has = 0`
/// when the watch is still empty (returns `AORTA_OK`). When the buffer is too
/// small, returns `AORTA_ERR_BUFFER_TOO_SMALL` and sets `*out_len` to the
/// required size.
///
/// # Safety
/// - `out_buf` must be valid for `cap` bytes; `out_len` and `out_has` must
///   be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn aorta_watch_load(
    watch: aorta_watch_t,
    out_buf: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_has: *mut u8,
    out_seq: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the writebacks
    // and the copy into the caller's buffer (bounded by `cap`).
    ffi_guard(|| unsafe {
        if out_len.is_null() || out_has.is_null() || (cap > 0 && out_buf.is_null()) {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let ring_ref = {
            let watches = registry().flow_watches.lock();
            match watches.get(&watch) {
                Some(r) => Arc::clone(r),
                None => return map_aorta_error(AortaError::not_found("flow watch not found")),
            }
        };
        match ring_ref.snapshot() {
            None => {
                *out_has = 0;
                *out_len = 0;
                if !out_seq.is_null() {
                    *out_seq = 0;
                }
                AORTA_OK
            }
            Some(snap) => {
                // seq and payload come from ONE atomic snapshot — safe for
                // consumer change-detection/dedup (nullable out param).
                if !out_seq.is_null() {
                    *out_seq = snap.seq;
                }
                let bytes = snap.payload.to_bytes();
                let len = bytes.len();
                *out_len = len;
                if len > cap {
                    return aorta_err(
                        AORTA_ERR_BUFFER_TOO_SMALL,
                        "value larger than the caller buffer",
                    );
                }
                if len > 0 {
                    // Guarded: copy_nonoverlapping requires non-null pointers
                    // even for zero bytes (out_buf MAY be null when cap == 0).
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, len);
                }
                *out_has = 1;
                AORTA_OK
            }
        }
    })
}

/// Foreign-producer store (Flow Concepts §6.3): thread-safe from any thread; moves a
/// copy of the caller's bytes in as the new latest value.
///
/// # Safety
/// - `data` must be valid for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn aorta_watch_store(
    watch: aorta_watch_t,
    data: *const u8,
    len: usize,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the slice view.
    ffi_guard(|| unsafe {
        if len > 0 && data.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "data is null");
        }
        let ring_ref = {
            let watches = registry().flow_watches.lock();
            match watches.get(&watch) {
                Some(r) => Arc::clone(r),
                None => return map_aorta_error(AortaError::not_found("flow watch not found")),
            }
        };
        // from_raw_parts requires a NON-NULL pointer
        // even for len == 0 — an empty store with data == NULL is legal at
        // this ABI, so special-case it before forming the slice.
        let payload = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(data, len).to_vec()
        };
        ring_ref.store(crate::ZBytes::from(payload));
        AORTA_OK
    })
}

/// Total values ever stored (RX + foreign) — observability.
///
/// # Safety
/// - `out_total` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_watch_stored_total(
    watch: aorta_watch_t,
    out_total: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_total`.
    ffi_guard(|| unsafe {
        if out_total.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_total is null");
        }
        let watches = registry().flow_watches.lock();
        match watches.get(&watch) {
            Some(r) => {
                *out_total = r.stored_total();
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("flow watch not found")),
        }
    })
}

/// Release the watch (undeclares its subscriber).
#[no_mangle]
pub extern "C" fn aorta_watch_free(watch: aorta_watch_t) -> aorta_result_t {
    ffi_guard(|| {
        if watch == 0 {
            return AORTA_OK;
        }
        match registry().flow_watches.lock().remove(&watch) {
            Some(_) => AORTA_OK,
            None => map_aorta_error(AortaError::not_found("flow watch not found")),
        }
    })
}

// ===========================================================================
// Readiness (Flow Concepts §6.5): WindowSync approximate-time alignment and
// ReadinessSet AND-join waves at the C ABI. Payloads copy at the boundary
// (aligned-set views stay valid until `aorta_aligned_set_free`); the
// C++ `co_await` bridge rides the notify hooks (trampoline contract).
// ===========================================================================

pub type aorta_window_sync_t = u64;
pub type aorta_readiness_set_t = u64;
pub type aorta_aligned_set_t = u64;

/// Registry accessor twin of `window_ref`/`readiness_ref` (also used by
/// the arbiter's scope bridge).
pub(crate) fn scope_ref(handle: u64) -> Option<crate::flow::Scope> {
    registry().flow_scopes.lock().get(&handle).cloned()
}

fn window_ref(handle: aorta_window_sync_t) -> Option<Arc<crate::flow::WindowSync>> {
    registry().flow_windows.lock().get(&handle).cloned()
}

fn readiness_ref(handle: aorta_readiness_set_t) -> Option<Arc<crate::flow::ReadinessSet>> {
    registry().flow_readiness.lock().get(&handle).cloned()
}

/// Create an approximate-time alignment window over `inputs` sources
/// within `slop_ns` (Flow Concepts §6.5). `depth` bounds the emitted-set queue
/// (drop-oldest); 0 selects the default (8).
///
/// # Safety
/// - `out_window` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_window_sync_create(
    inputs: u32,
    slop_ns: u64,
    depth: u32,
    out_window: *mut aorta_window_sync_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_window`.
    ffi_guard(|| unsafe {
        if out_window.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_window is null");
        }
        if inputs == 0 || inputs > 64 {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "window_sync supports 1..=64 inputs",
            );
        }
        let window = crate::flow::WindowSync::with_capacity(
            inputs as usize,
            std::time::Duration::from_nanos(slop_ns),
            if depth == 0 { 8 } else { depth as usize },
        );
        let handle = alloc_handle();
        registry().flow_windows.lock().insert(handle, window);
        *out_window = handle;
        AORTA_OK
    })
}

/// Feed input `input` with a sample stamped `ts_ns` (any thread; copies).
/// Out-of-range `input` is a wiring bug: `AORTA_ERR_INVALID_ARGUMENT`.
///
/// # Safety
/// - `data` must be valid for `len` bytes (`data` MAY be null when `len == 0`).
#[no_mangle]
pub unsafe extern "C" fn aorta_window_sync_feed(
    window: aorta_window_sync_t,
    input: u32,
    ts_ns: u64,
    data: *const u8,
    len: usize,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the slice view.
    ffi_guard(|| unsafe {
        if len > 0 && data.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "data is null");
        }
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        if input as usize >= w.inputs() {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "window_sync input index out of range",
            );
        }
        let payload = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(data, len).to_vec()
        };
        w.feed(
            input as usize,
            ts_ns,
            Arc::new(crate::ZBytes::from(payload)),
        );
        AORTA_OK
    })
}

/// Pop the next aligned set if one is queued: `*out_has = 1` and
/// `*out_set` receives a handle whose per-input views stay valid until
/// [`aorta_aligned_set_free`]. `*out_has = 0` when none is ready
/// (returns `AORTA_OK`).
///
/// # Safety
/// - `out_set` and `out_has` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn aorta_window_sync_try_aligned(
    window: aorta_window_sync_t,
    out_set: *mut aorta_aligned_set_t,
    out_has: *mut u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the writebacks.
    ffi_guard(|| unsafe {
        if out_set.is_null() || out_has.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        match w.try_aligned() {
            None => {
                *out_has = 0;
                *out_set = 0;
                AORTA_OK
            }
            Some(set) => {
                // One contiguous copy per input at the boundary: the C views
                // need stable memory, ZBytes may be segmented.
                let owned: Vec<(u64, Vec<u8>)> = set
                    .into_iter()
                    .map(|(ts, payload)| (ts, payload.to_bytes().into_owned()))
                    .collect();
                let handle = alloc_handle();
                registry().flow_aligned_sets.lock().insert(handle, owned);
                *out_set = handle;
                *out_has = 1;
                AORTA_OK
            }
        }
    })
}

/// Number of inputs in an aligned set (== the window's input count).
///
/// # Safety
/// - `out_len` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_aligned_set_len(
    set: aorta_aligned_set_t,
    out_len: *mut u32,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_len`.
    ffi_guard(|| unsafe {
        if out_len.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_len is null");
        }
        match registry().flow_aligned_sets.lock().get(&set) {
            Some(entry) => {
                *out_len = entry.len() as u32;
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("aligned set not found")),
        }
    })
}

/// Borrow input `index`'s timestamp and payload view. The view is valid
/// until [`aorta_aligned_set_free`] — copy out before freeing.
///
/// # Safety
/// - `out_ts`, `out_data`, and `out_data_len` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn aorta_aligned_set_get(
    set: aorta_aligned_set_t,
    index: u32,
    out_ts: *mut u64,
    out_data: *mut *const u8,
    out_data_len: *mut usize,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the writebacks.
    ffi_guard(|| unsafe {
        if out_ts.is_null() || out_data.is_null() || out_data_len.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        match registry().flow_aligned_sets.lock().get(&set) {
            Some(entry) => match entry.get(index as usize) {
                Some((ts, payload)) => {
                    *out_ts = *ts;
                    // The Vec's heap buffer is stable across registry rehash
                    // (only the Vec header moves); entries are never mutated
                    // after insert, so the view lives until free.
                    *out_data = payload.as_ptr();
                    *out_data_len = payload.len();
                    AORTA_OK
                }
                None => aorta_err(
                    AORTA_ERR_INVALID_ARGUMENT,
                    "aligned set input index out of range",
                ),
            },
            None => map_aorta_error(AortaError::not_found("aligned set not found")),
        }
    })
}

/// Release an aligned set (its views become invalid).
#[no_mangle]
pub extern "C" fn aorta_aligned_set_free(set: aorta_aligned_set_t) -> aorta_result_t {
    ffi_guard(|| {
        if set == 0 {
            return AORTA_OK;
        }
        match registry().flow_aligned_sets.lock().remove(&set) {
            Some(_) => AORTA_OK,
            None => map_aorta_error(AortaError::not_found("aligned set not found")),
        }
    })
}

/// NON-POPPING queue probe: writes 1 when at least one aligned set is
/// queued. The C++ `co_await aligned()` recheck uses this (never
/// steal a set the frame cannot deliver; the pop happens in
/// `await_resume`).
///
/// # Safety
/// - `out_pending` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_window_sync_pending(
    window: aorta_window_sync_t,
    out_pending: *mut u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_pending`.
    ffi_guard(|| unsafe {
        if out_pending.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_pending is null");
        }
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        *out_pending = u8::from(w.has_aligned());
        AORTA_OK
    })
}

/// Aggregate window stats: sets emitted / sets dropped to the
/// depth cap. Null out-params are skipped.
///
/// # Safety
/// - Non-null out-params must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn aorta_window_sync_stats(
    window: aorta_window_sync_t,
    out_emitted: *mut u64,
    out_dropped: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the writebacks.
    ffi_guard(|| unsafe {
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        let stats = w.stats();
        if !out_emitted.is_null() {
            *out_emitted = stats.emitted;
        }
        if !out_dropped.is_null() {
            *out_dropped = stats.dropped_sets;
        }
        AORTA_OK
    })
}

/// Per-input window stats (never-aligns debugging): samples fed and
/// the latest timestamp seen. Null out-params are skipped.
///
/// # Safety
/// - Non-null out-params must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn aorta_window_sync_input_stats(
    window: aorta_window_sync_t,
    input: u32,
    out_fed: *mut u64,
    out_last_ts: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the writebacks.
    ffi_guard(|| unsafe {
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        if input as usize >= w.inputs() {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "window_sync input index out of range",
            );
        }
        let stats = w.stats();
        if !out_fed.is_null() {
            *out_fed = stats.fed[input as usize];
        }
        if !out_last_ts.is_null() {
            *out_last_ts = stats.last_ts[input as usize];
        }
        AORTA_OK
    })
}

/// Set (or clear with `func == NULL`) the aligned-set emission hook —
/// TRAMPOLINE-CLASS contract: the hook fires on the FEEDING
/// thread and may only complete an arbiter branch / set a flag. Single
/// awaiter: a different non-null hook over an existing one is
/// refused. Used by the C++ `co_await window.aligned()` awaiter.
///
/// # Safety
/// `(func, ctx)` must stay callable until cleared or the window is freed;
/// stale arbiter ids inside the hook no-op registry-side.
#[no_mangle]
pub unsafe extern "C" fn aorta_window_sync_set_notify(
    window: aorta_window_sync_t,
    func: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
    ctx: *mut std::ffi::c_void,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        if !w.set_notify_hook(func.map(|f| crate::subscriber::PullNotifyHook {
            func: f,
            ctx: ctx as usize,
        })) {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "notify hook already set (one concurrent aligned() awaiter per window)",
            );
        }
        AORTA_OK
    })
}

/// Release the window. A parked awaiter's hook is fired one last time
/// (destroy-wake) so it can observe the stale handle and complete.
#[no_mangle]
pub extern "C" fn aorta_window_sync_free(window: aorta_window_sync_t) -> aorta_result_t {
    ffi_guard(|| {
        if window == 0 {
            return AORTA_OK;
        }
        match registry().flow_windows.lock().remove(&window) {
            Some(w) => {
                w.fire_hook();
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("window_sync not found")),
        }
    })
}

/// Create a readiness AND-join over `count` conditions (Flow Concepts §6.5).
///
/// # Safety
/// - `out_set` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_readiness_set_create(
    count: u32,
    out_set: *mut aorta_readiness_set_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_set`.
    ffi_guard(|| unsafe {
        if out_set.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_set is null");
        }
        let set = match crate::flow::ReadinessSet::new(count as usize) {
            Ok(set) => set,
            Err(err) => return map_aorta_error(err),
        };
        let handle = alloc_handle();
        registry().flow_readiness.lock().insert(handle, set);
        *out_set = handle;
        AORTA_OK
    })
}

/// Mark condition `index` satisfied (any thread; one RMW). The arrival
/// completing the wave fires the notify hook. Out-of-range `index` is
/// `AORTA_ERR_INVALID_ARGUMENT`.
#[no_mangle]
pub extern "C" fn aorta_readiness_set_satisfy(
    set: aorta_readiness_set_t,
    index: u32,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(s) = readiness_ref(set) else {
            return map_aorta_error(AortaError::not_found("readiness set not found"));
        };
        if index as usize >= s.count() {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "readiness condition index out of range",
            );
        }
        s.satisfy(index as usize);
        AORTA_OK
    })
}

/// Completed-wave count — the awaiter's baseline/recheck probe.
///
/// # Safety
/// - `out_waves` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_readiness_set_waves(
    set: aorta_readiness_set_t,
    out_waves: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_waves`.
    ffi_guard(|| unsafe {
        if out_waves.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_waves is null");
        }
        let Some(s) = readiness_ref(set) else {
            return map_aorta_error(AortaError::not_found("readiness set not found"));
        };
        *out_waves = s.waves();
        AORTA_OK
    })
}

/// Observability: the bitmask of conditions still missing.
///
/// # Safety
/// - `out_mask` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_readiness_set_pending_mask(
    set: aorta_readiness_set_t,
    out_mask: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_mask`.
    ffi_guard(|| unsafe {
        if out_mask.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_mask is null");
        }
        let Some(s) = readiness_ref(set) else {
            return map_aorta_error(AortaError::not_found("readiness set not found"));
        };
        *out_mask = s.pending_mask();
        AORTA_OK
    })
}

/// Set (or clear with `func == NULL`) the wave hook — same contract as
/// [`aorta_window_sync_set_notify`] (trampoline-class, single awaiter).
///
/// # Safety
/// `(func, ctx)` must stay callable until cleared or the set is freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_readiness_set_set_notify(
    set: aorta_readiness_set_t,
    func: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
    ctx: *mut std::ffi::c_void,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(s) = readiness_ref(set) else {
            return map_aorta_error(AortaError::not_found("readiness set not found"));
        };
        if !s.set_notify_hook(func.map(|f| crate::subscriber::PullNotifyHook {
            func: f,
            ctx: ctx as usize,
        })) {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "notify hook already set (one concurrent wave() awaiter per set)",
            );
        }
        AORTA_OK
    })
}

/// Release the set (destroy-wake: a parked awaiter's hook fires once).
#[no_mangle]
pub extern "C" fn aorta_readiness_set_free(set: aorta_readiness_set_t) -> aorta_result_t {
    ffi_guard(|| {
        if set == 0 {
            return AORTA_OK;
        }
        match registry().flow_readiness.lock().remove(&set) {
            Some(s) => {
                s.fire_hook();
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("readiness set not found")),
        }
    })
}

/// Wire subscriber `sub` as window input `input` (post-declare bind;
/// any delivery mode). `stamp`: 0 = local receive time, 1 = transport
/// publisher timestamp (falls back to receive when absent). The wire
/// observes the stream — delivery is unaffected. Binds accumulate and
/// live as long as the subscriber.
#[no_mangle]
pub extern "C" fn aorta_subscriber_bind_window(
    sub: u64,
    window: aorta_window_sync_t,
    input: u32,
    stamp: u8,
) -> aorta_result_t {
    ffi_guard(|| {
        let stamp = match stamp {
            0 => crate::flow::WindowStamp::Receive,
            1 => crate::flow::WindowStamp::Publisher,
            _ => {
                return aorta_err(
                    AORTA_ERR_INVALID_ARGUMENT,
                    "stamp must be 0 (receive) or 1 (publisher)",
                )
            }
        };
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        let subs = registry().subscribers.lock();
        match subs.get(&sub) {
            Some(entry) => match entry.subscriber.bind_window(&w, input as usize, stamp) {
                Ok(()) => AORTA_OK,
                Err(err) => map_aorta_error(err),
            },
            None => map_aorta_error(AortaError::not_found("subscriber handle not found")),
        }
    })
}

/// Wire subscriber `sub` to satisfy `set` condition `condition` on every
/// arriving sample (post-declare bind).
#[no_mangle]
pub extern "C" fn aorta_subscriber_bind_readiness(
    sub: u64,
    set: aorta_readiness_set_t,
    condition: u32,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(s) = readiness_ref(set) else {
            return map_aorta_error(AortaError::not_found("readiness set not found"));
        };
        let subs = registry().subscribers.lock();
        match subs.get(&sub) {
            Some(entry) => match entry.subscriber.bind_readiness(&s, condition as usize) {
                Ok(()) => AORTA_OK,
                Err(err) => map_aorta_error(err),
            },
            None => map_aorta_error(AortaError::not_found("subscriber handle not found")),
        }
    })
}

/// Remove the `aorta_subscriber_bind_window` wire(s) feeding `window`
/// input `input` (bind's inverse — without it the
/// subscriber's internal clone kept feeding the window for its whole
/// remaining lifetime after the owner freed its handle). `NOT_FOUND`
/// when no such wire (or a stale sub/window handle).
#[no_mangle]
pub extern "C" fn aorta_subscriber_unbind_window(
    sub: u64,
    window: aorta_window_sync_t,
    input: u32,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(w) = window_ref(window) else {
            return map_aorta_error(AortaError::not_found("window_sync not found"));
        };
        let subs = registry().subscribers.lock();
        match subs.get(&sub) {
            Some(entry) => {
                if entry.subscriber.unbind_window(&w, input as usize) > 0 {
                    AORTA_OK
                } else {
                    map_aorta_error(AortaError::not_found("no matching window wire"))
                }
            }
            None => map_aorta_error(AortaError::not_found("subscriber handle not found")),
        }
    })
}

/// Remove the `aorta_subscriber_bind_readiness` wire(s) satisfying `set`
/// condition `condition`. `NOT_FOUND` when no such wire.
#[no_mangle]
pub extern "C" fn aorta_subscriber_unbind_readiness(
    sub: u64,
    set: aorta_readiness_set_t,
    condition: u32,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(rs) = readiness_ref(set) else {
            return map_aorta_error(AortaError::not_found("readiness set not found"));
        };
        let subs = registry().subscribers.lock();
        match subs.get(&sub) {
            Some(entry) => {
                if entry.subscriber.unbind_readiness(&rs, condition as usize) > 0 {
                    AORTA_OK
                } else {
                    map_aorta_error(AortaError::not_found("no matching readiness wire"))
                }
            }
            None => map_aorta_error(AortaError::not_found("subscriber handle not found")),
        }
    })
}

/// The node's root scope: C/C++ scope trees parented here join
/// the node-shutdown cascade (`aorta_node_free`/close cancels the root with
/// Shutdown). The returned handle is a REFERENCE — `aorta_scope_free`
/// releases the handle, never the root itself.
///
/// # Safety
/// - `out_scope` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_node_root_scope(
    node: u64,
    out_scope: *mut aorta_scope_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_scope`.
    ffi_guard(|| unsafe {
        if out_scope.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_scope is null");
        }
        let node_obj = { registry().nodes.lock().get(&node).cloned() };
        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };
        let scope = node_obj.root_scope();
        let handle = alloc_handle();
        registry().flow_scopes.lock().insert(handle, scope);
        *out_scope = handle;
        AORTA_OK
    })
}

/// Bridge (one-way): a scope whose cancel PARENT is an existing cancel
/// token (`aorta_token_t`). Cancelling the token cascades into the scope;
/// the scope's own cancel never touches the token's other children.
///
/// # Safety
/// - `opts` may be null (defaults); `out_scope` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_child_of_token(
    token: aorta_token_t,
    opts: *const aorta_scope_opts_t,
    out_scope: *mut aorta_scope_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers opts deref (via
    // scope_opts_from_ffi's null handling) and `*out_scope`.
    ffi_guard(|| unsafe {
        if out_scope.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_scope is null");
        }
        if let Some(code) = validate_scope_opts(opts) {
            return code;
        }
        let token_obj = { registry().cancel_tokens.lock().get(&token).cloned() };
        let Some(token_obj) = token_obj else {
            return map_aorta_error(AortaError::not_found("token handle not found"));
        };
        let scope = crate::flow::Scope::child_of_token(&token_obj, scope_opts_from_ffi(opts));
        let handle = alloc_handle();
        registry().flow_scopes.lock().insert(handle, scope);
        *out_scope = handle;
        AORTA_OK
    })
}

/// Finish an external Flow task as FAILED (the C twin
/// of a Rust child panic): releases the accounting like `aorta_task_finish`
/// AND applies fail-fast — the owning scope is cancelled with
/// `AORTA_CANCEL_REASON_CHILD_FAILED` unless it is supervised or already
/// cancelled (an unwinding task's failure is teardown, not a new fault).
/// Used by the C++ coroutine layer when an exception escapes a Task body.
#[no_mangle]
pub extern "C" fn aorta_task_finish_failed(task: aorta_task_t) -> aorta_result_t {
    ffi_guard(|| match registry().flow_tasks.lock().remove(&task) {
        Some(child) => {
            child.finish_failed();
            AORTA_OK
        }
        None => map_aorta_error(AortaError::not_found("flow task not found")),
    })
}

/// The scope's RESOLVED dispatch priority as an index (0=realtime …
/// 4=background) — what `spawn_on` stamps into a C++ root promise so every
/// frame in the chain posts its resumes at the scope's priority.
///
/// # Safety
/// - `out_priority` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_scope_priority(
    scope: aorta_scope_t,
    out_priority: *mut u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_priority`.
    ffi_guard(|| unsafe {
        if out_priority.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_priority is null");
        }
        let scopes = registry().flow_scopes.lock();
        match scopes.get(&scope) {
            Some(s) => {
                *out_priority = crate::event_queue::priority_index(s.priority());
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("scope handle not found")),
        }
    })
}

/// Post a resumption that fires AFTER `delay_ns` (the C++
/// `flow::sleep_on` carrier). NOT an RT-timer path: the delay rides a
/// runtime sleep task. Delivery contract matches `aorta_executor_post_resume`
/// exactly (the event struct, exactly-once, resume-into-cancellation) —
/// with one addition: executor STOP resumes still-sleeping posts into
/// cancellation PROMPTLY (never waiting out the delay).
#[no_mangle]
pub extern "C" fn aorta_executor_post_resume_after(
    executor: u64,
    delay_ns: u64,
    resume: Option<unsafe extern "C" fn(event: *const aorta_resume_event_t)>,
    handle: *mut c_void,
    priority: u8,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(resume) = resume else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "resume is null");
        };
        let (sender, delayed, stopped) = {
            let execs = registry().executors.lock();
            match execs.get(&executor) {
                Some(entry) => (
                    entry.sender.clone(),
                    entry.stop.delayed(),
                    entry.stop.is_stopped(),
                ),
                None => return map_aorta_error(AortaError::not_found("executor not found")),
            }
        };
        if stopped {
            return aorta_err(AORTA_ERR_CANCELLED, "executor is stopped");
        }
        let handle_addr = handle as usize;
        let event_priority =
            priority_from_u8(priority).unwrap_or(crate::event_queue::EventPriority::Normal);
        // fire(cancelled): the single completion closure — the map removal
        // in DelayedResumes is the exactly-once arbiter for who runs it.
        let fire = Box::new(move |cancelled: bool| {
            let invoke_inline = |status: aorta_result_t| {
                let (stop_kind, cancel_reason) = if status == AORTA_OK {
                    (AORTA_STOP_KIND_NONE, 0)
                } else {
                    (AORTA_STOP_KIND_CANCELLED, AORTA_CANCEL_REASON_SHUTDOWN)
                };
                let event = aorta_resume_event_t {
                    handle: handle_addr as *mut c_void,
                    status,
                    stop_kind,
                    cancel_reason,
                    _pad: [0; 2],
                };
                // SAFETY: `resume` is the registered non-null resumption; the
                // caller guarantees `handle` validity until resumed.
                unsafe { resume(&event) };
            };
            if cancelled {
                invoke_inline(AORTA_ERR_CANCELLED);
                return;
            }
            let event = crate::event_queue::Event::new_with_priority(
                crate::event_queue::EventKind::CoroutineResume,
                event_priority,
                move || {
                    let status = if crate::flow::reentrancy::is_stop_draining() {
                        AORTA_ERR_CANCELLED
                    } else {
                        AORTA_OK
                    };
                    let (stop_kind, cancel_reason) = if status == AORTA_OK {
                        (AORTA_STOP_KIND_NONE, 0)
                    } else {
                        (AORTA_STOP_KIND_CANCELLED, AORTA_CANCEL_REASON_SHUTDOWN)
                    };
                    let event = aorta_resume_event_t {
                        handle: handle_addr as *mut c_void,
                        status,
                        stop_kind,
                        cancel_reason,
                        _pad: [0; 2],
                    };
                    // SAFETY: as above; the event pointer is call-scoped.
                    unsafe { resume(&event) };
                },
            );
            if sender.try_send(event).is_err() {
                // Lane closed under us (stop raced the wake): resume inline
                // into cancellation.
                invoke_inline(AORTA_ERR_CANCELLED);
            }
        });
        // A stop racing this post can no longer
        // strand a sleeper for its full delay — the registry's closed flag
        // is checked inside insert's critical section; when closed, the
        // fire closure comes straight back and we deliver the inline
        // cancelled resume NOW instead of spawning the sleeper.
        let id = match delayed.insert(fire) {
            Ok(id) => id,
            Err(fire) => {
                fire(true);
                return AORTA_OK;
            }
        };
        let delayed_for_task = std::sync::Arc::clone(&delayed);
        let join = crate::ffi::runtime_helper::runtime().spawn(async move {
            tokio::time::sleep(std::time::Duration::from_nanos(delay_ns)).await;
            if let Some(fire) = delayed_for_task.take(id) {
                fire(false);
            }
        });
        if !delayed.set_abort(id, join.abort_handle()) {
            // A stop's drain_cancel took the entry between insert and this
            // store — its cancelled fire already delivered; abort the
            // sleeper so it does not linger for its full term.
            join.abort();
        }
        AORTA_OK
    })
}
