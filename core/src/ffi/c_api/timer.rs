// Timer FFI surface.
//
// Extracted from c_api/mod.rs.

use super::*;

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_new(
    node: aorta_node_t,
    period_ns: i64,
    cb: aorta_timer_cb,
    user_data: *mut c_void,
    out: *mut aorta_timer_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `aorta_timer_new_with_options`; forwards the
    // caller's pointer arguments unchanged under that fn's matching
    // `# Safety` contract.
    ffi_guard(|| unsafe {
        aorta_timer_new_with_options(node, period_ns, cb, user_data, ptr::null(), out)
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. The caller retains ownership of all input buffers;
/// this function does not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_new_with_options(
    node: aorta_node_t,
    period_ns: i64,
    cb: aorta_timer_cb,
    user_data: *mut c_void,
    opts: *const aorta_timer_options_t,
    out: *mut aorta_timer_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the optional
    // `opts.as_ref()` deref + `*out = handle` writeback below; `out` is
    // null-checked at entry, and `opts` is treated as optional via `as_ref`.
    ffi_guard(|| unsafe {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }

        let cb = match cb {
            Some(cb) => cb,
            None => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null"),
        };

        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        let period = if period_ns <= 0 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "period_ns must be positive");
        } else {
            Duration::from_nanos(period_ns as u64)
        };

        let mut timer_options = crate::timer::TimerOptions::default();
        if let Some(opts_ref) = opts.as_ref() {
            timer_options.callback_group = callback_group_from_ffi(opts_ref.callback_group);
            timer_options.callback_group_handle =
                callback_group_handle_from_ffi(opts_ref.callback_group_handle);
            timer_options.auto_start = opts_ref.auto_start != 0;
            timer_options.invoke_on_missed = opts_ref.invoke_on_missed != 0;
            timer_options.overflow_policy =
                timer_overflow_policy_from_ffi(opts_ref.overflow_policy);
            timer_options.overflow_block_timeout =
                Duration::from_millis(opts_ref.overflow_block_timeout_ms);
        }

        let class_defaults = node.executor_class_policy_defaults();
        let (resolved_policy, resolved_timeout) = class_defaults.resolve_timer_overflow_compat(
            timer_options.overflow_policy,
            timer_options.overflow_block_timeout,
        );
        timer_options.overflow_policy = resolved_policy;
        timer_options.overflow_block_timeout = resolved_timeout;

        let callback_user_data = Arc::new(AtomicUsize::new(user_data as usize));
        let callback_user_data_for_timer = Arc::clone(&callback_user_data);
        // Teardown gate (see FfiSubscriberEntry): count in-flight
        // C++ dispatcher (`cb`) invocations so `aorta_timer_free` can drain
        // them before the C++ side frees its CallbackState.
        let in_flight = Arc::new(AtomicUsize::new(0));
        let in_flight_cb = Arc::clone(&in_flight);
        let timer = match crate::ffi_block_on!(node.create_timer_with_options(
            period,
            move || {
                let user_data = callback_user_data_for_timer.load(Ordering::Acquire) as *mut c_void;
                if !user_data.is_null() {
                    // Timer tick dispatch — wrap so a Rust-side panic in OUR
                    // dispatcher closure code (the user_data load + null
                    // check above, future additions) cannot unwind into the
                    // tokio timer task. NOTE: this does NOT catch panics
                    // that originate INSIDE the user's `cb` — Rust ≥ 1.81
                    // marks extern "C" as nounwind, so a panic in `cb`
                    // aborts before our catch_unwind sees it. The C++ SDK's
                    // try/catch is what protects C++ users.
                    //
                    // Bracket `cb` with the in-flight gate so aorta_timer_free
                    // waits for a running tick to finish before the C++ side
                    // frees its CallbackState.
                    in_flight_cb.fetch_add(1, Ordering::AcqRel);
                    // (&Arc auto-derefs to &AtomicUsize for the guard's borrow.)
                    let _drain_guard = InFlightGuard(&in_flight_cb);
                    ffi_guard_void(|| {
                        cb(user_data);
                    });
                }
            },
            timer_options,
        )) {
            Ok(Ok(timer)) => timer,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };

        let handle = alloc_handle();
        registry().timers.lock().insert(
            handle,
            FfiTimerEntry {
                timer,
                callback_user_data,
                in_flight,
            },
        );

        *out = handle;

        AORTA_OK
    })
}

/// PAUSES the timer (the name is a historical
/// misnomer — nothing is cancelled or destroyed; the handle stays valid and
/// `aorta_timer_reset` resumes ticking). Kept for ABI stability; SDKs expose
/// it as pause (C++ `Timer::Pause`).
#[no_mangle]
pub extern "C" fn aorta_timer_cancel(timer: aorta_timer_t) -> aorta_result_t {
    ffi_guard(|| {
        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        timer.timer.pause();
        AORTA_OK
    })
}

/// RESUMES the timer (historical misnomer — nothing
/// is reset; this is the counterpart of `aorta_timer_cancel`'s pause and the
/// way to start an `auto_start=false` timer). Kept for ABI stability; SDKs
/// expose it as resume (C++ `Timer::Resume`).
#[no_mangle]
pub extern "C" fn aorta_timer_reset(timer: aorta_timer_t) -> aorta_result_t {
    ffi_guard(|| {
        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        timer.timer.resume();
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_coalesced_tick_count(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.coalesced_tick_count();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_overflow_timeout_count(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.overflow_timeout_count();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_deadline_slip_total_ns(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.deadline_slip_total_ns();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_deadline_slip_max_ns(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.deadline_slip_max_ns();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_due_firings_total(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.due_firings_total();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_due_firings_max(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.due_firings_max();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_due_sample_count(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.due_sample_count();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_coalesce_requeue_count(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.coalesce_requeue_count();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_coalesce_inline_dispatch_total(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.coalesce_inline_dispatch_total();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_coalesce_inline_dispatch_max(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.coalesce_inline_dispatch_max();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_coalesce_event_wait_total_ns(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.coalesce_event_wait_total_ns();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_coalesce_event_wait_max_ns(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.coalesce_event_wait_max_ns();
        }
        AORTA_OK
    })
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_coalesce_event_wait_sample_count(
    timer: aorta_timer_t,
    out_count: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_count.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_count is null");
        }

        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // SAFETY: `out_count` was null-checked above; the caller's
        // `# Safety` contract requires it to point at writable storage for
        // `u64`. The accessor on the registry-owned `Timer` is a safe call.
        unsafe {
            *out_count = timer.timer.coalesce_event_wait_sample_count();
        }
        AORTA_OK
    })
}

/// Whether the timer is currently running (started and not stopped/paused).
///
/// Backs the Rust SDK `Timer::is_running`; the C++ SDK does
/// not expose it. Writes `1`/`0` to `out_running`.
///
/// # Safety
/// - `timer` must be a valid `aorta_timer_t` handle
/// - `out_running` must point to a writable `u8`
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_is_running(
    timer: aorta_timer_t,
    out_running: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_running.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_running is null");
        }
        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };
        // SAFETY: `out_running` was null-checked above; caller contract requires
        // it point at a writable `u8`.
        unsafe {
            *out_running = u8::from(timer.timer.is_running());
        }
        AORTA_OK
    })
}

/// The timer's configured period in nanoseconds.
///
/// Backs the Rust SDK `Timer::period`; the C++ SDK does not
/// expose it.
///
/// # Safety
/// - `timer` must be a valid `aorta_timer_t` handle
/// - `out_period_ns` must point to a writable `u64`
#[no_mangle]
pub unsafe extern "C" fn aorta_timer_get_period_ns(
    timer: aorta_timer_t,
    out_period_ns: *mut u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_period_ns.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_period_ns is null");
        }
        let timers = registry().timers.lock();
        let Some(timer) = timers.get(&timer) else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };
        // SAFETY: `out_period_ns` was null-checked above; caller contract
        // requires it point at a writable `u64`.
        unsafe {
            *out_period_ns = timer.timer.period().as_nanos() as u64;
        }
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_timer_free(timer: aorta_timer_t) -> aorta_result_t {
    ffi_guard(|| {
        let timer_entry = { registry().timers.lock().remove(&timer) };

        let Some(timer_entry) = timer_entry else {
            return map_aorta_error(AortaError::not_found("timer handle not found"));
        };

        // Stop future ticks from dispatching (queued ticks load 0 → no-op)...
        timer_entry.callback_user_data.store(0, Ordering::Release);
        // ...then wait for a tick already inside `cb` to finish, so the C++
        // destructor can free its CallbackState without a UAF.
        // Bounded — a hung callback falls through to the C++ side's own
        // wait+leak-on-timeout. NOTE: if free is (mis)used from within the
        // timer's own callback the drain self-waits to the timeout; that
        // anti-pattern already stalls the C++ ~Timer the same way.
        if !drain_in_flight(&timer_entry.in_flight, FREE_DRAIN_TIMEOUT) {
            aorta_core_warn!(
                "aorta_timer_free: timed out draining in-flight callbacks; \
                 C++ destructor will fall back to its own wait+leak"
            );
        }
        drop(timer_entry);

        AORTA_OK
    })
}
