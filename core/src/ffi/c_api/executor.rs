// Executor FFI surface.
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
pub unsafe extern "C" fn aorta_executor_new(
    executor_type: aorta_executor_type_t,
    out: *mut aorta_exec_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `aorta_executor_new_with_options`; forwards
    // the caller's `out` pointer unchanged under that fn's own `# Safety`
    // contract (which matches this one).
    ffi_guard(|| unsafe { aorta_executor_new_with_options(executor_type, ptr::null(), out) })
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
pub unsafe extern "C" fn aorta_executor_new_with_options(
    executor_type: aorta_executor_type_t,
    opts: *const aorta_executor_options_t,
    out: *mut aorta_exec_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the three
    // `*out = handle` writebacks (one per executor variant), each gated by
    // the `out.is_null()` check at the top under the caller's contract that
    // `out` points at writable storage for `aorta_exec_t`.
    ffi_guard(|| unsafe {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }

        match executor_type {
            aorta_executor_type_t::AORTA_EXECUTOR_SINGLE => {
                let options = match executor_options_from_ffi(opts) {
                    Ok(options) => options,
                    Err(err) => return map_aorta_error(err),
                };
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
                let options = match executor_options_from_ffi(opts) {
                    Ok(options) => options,
                    Err(err) => return map_aorta_error(err),
                };
                let exec = ExecutorInstance::Multi(MultiThreadedExecutor::new_with_options(
                    DEFAULT_EXECUTOR_QUEUE,
                    DEFAULT_MULTI_EXECUTOR_WORKERS,
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
            aorta_executor_type_t::AORTA_EXECUTOR_ASYNC => {
                // Compatibility backend: reuse multi-thread executor with the
                // same admission/scheduler contract and a bounded in-flight cap.
                let options = match executor_options_from_ffi(opts) {
                    Ok(options) => options,
                    Err(err) => return map_aorta_error(err),
                };
                let exec = ExecutorInstance::Multi(MultiThreadedExecutor::new_with_options(
                    DEFAULT_EXECUTOR_QUEUE,
                    async_executor_in_flight_limit(),
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
            _ => aorta_err(AORTA_ERR_INVALID_ARGUMENT, "executor type not supported"),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_executor_add_node(
    exec_handle: aorta_exec_t,
    node: aorta_node_t,
) -> aorta_result_t {
    ffi_guard(|| {
        // Mirror `aorta_executor_spin_once`: reject reentrant invocation
        // BEFORE taking `entry.exec.lock()`. The outer spin still holds
        // that (non-reentrant parking_lot) mutex; without this guard a
        // callback that calls `add_node` would block on the lock acquire
        // instead of surfacing AORTA_ERR_REENTRANT.
        if in_async_context() {
            return aorta_reentrant(module_path!());
        }

        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        // `aorta_executor_spin` holds this mutex for the WHOLE spin, so a
        // blocking lock here never returns while another thread spins —
        // callers used to win or lose a startup race against the spin
        // thread and hang on loss (TCK wave-3 finding; flow_demo.cc was
        // winning it by thread-startup luck). Refuse loudly instead: add
        // nodes BEFORE spinning (flow::Runtime: pass them at creation).
        let Some(mut exec) = entry.exec.try_lock() else {
            return aorta_err(
                AORTA_ERR_BUSY,
                "executor is spinning: add nodes before the spin starts \
                 (flow::Runtime — pass node handles at creation), or stop \
                 the executor first",
            );
        };
        if let Err(err) = exec.try_add_node(node) {
            return map_aorta_error(err);
        }
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_executor_spin(exec_handle: aorta_exec_t) -> aorta_result_t {
    ffi_guard(|| {
        // `spin` is a long-running blocking call. If invoked from within
        // an Aorta callback (Tokio worker thread), it would deadlock or
        // panic — early-return so the C caller can see the misuse.
        if in_async_context() {
            return aorta_reentrant(module_path!());
        }

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        let mut exec = entry.exec.lock();
        runtime().block_on(exec.spin());
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_executor_spin_once(
    exec_handle: aorta_exec_t,
    timeout_ms: i64,
) -> aorta_result_t {
    ffi_guard(|| {
        // Mirror `aorta_executor_spin`: reject reentrant invocation
        // BEFORE taking `entry.exec.lock()`. If this call were
        // dispatched from inside the outer spin's callback on the same
        // executor, the outer spin still holds that mutex; reaching the
        // `ffi_block_on!` path would deadlock on the lock acquire
        // instead of surfacing AORTA_ERR_REENTRANT.
        if in_async_context() {
            return aorta_reentrant(module_path!());
        }

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        let mut exec = entry.exec.lock();
        let timeout = if timeout_ms <= 0 {
            Duration::from_millis(0)
        } else {
            Duration::from_millis(timeout_ms as u64)
        };

        let outcome = match crate::ffi_block_on!(exec.spin_once(timeout)) {
            Ok(out) => out,
            Err(_) => return aorta_reentrant(module_path!()),
        };
        match outcome {
            SpinOutcome::Event | SpinOutcome::Stopped => AORTA_OK,
            SpinOutcome::Timeout => {
                map_aorta_error(AortaError::timeout("executor spin_once timeout"))
            }
            SpinOutcome::QueueClosed => aorta_err(AORTA_ERR_INTERNAL, "executor queue closed"),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_executor_stop(exec_handle: aorta_exec_t) -> aorta_result_t {
    ffi_guard(|| {
        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        entry.stop.stop();
        AORTA_OK
    })
}

/// Obtain a stop handle from an executor. The returned handle is a u64 key
/// into the global registry; the underlying `Arc<StopHandle>` lives in
/// `registry().stop_handles` until `aorta_stop_handle_free` is called. The
/// caller must eventually call `aorta_stop_handle_free` to release the
/// handle; double-free is safe (returns `AORTA_ERR_NOT_FOUND` on the second
/// call).
///
/// Returns `AORTA_OK` and writes a non-zero handle to `*out` on success.
/// Returns `AORTA_ERR_INVALID_ARGUMENT` if `out` is null.
/// Returns `AORTA_ERR_NOT_FOUND` if `exec_handle` does not resolve.
///
/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_executor_get_stop_handle(
    exec_handle: aorta_exec_t,
    out: *mut aorta_stop_handle_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return map_aorta_error(AortaError::invalid_argument("out is null"));
        }

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        let stop_arc = Arc::new(entry.stop.clone());
        let id = alloc_handle();
        debug_assert_ne!(
            id, AORTA_STOP_HANDLE_INVALID,
            "alloc_handle returned reserved 0 sentinel"
        );
        registry().stop_handles.lock().insert(id, stop_arc);
        // SAFETY: caller guarantees `out` is non-null and points to writable storage
        // for `aorta_stop_handle_t` (checked above).
        unsafe {
            *out = id;
        }
        AORTA_OK
    })
}

/// Stop an executor via a stop handle. Performs a brief registry lookup
/// (Mutex) to resolve the handle to an `Arc<StopHandle>`, then drops the
/// lock before invoking `.stop()`.
///
/// **Not async-signal-safe** (prior versions documented it as such). In a
/// signal handler, post to a self-pipe / signalfd and invoke
/// `aorta_stop_handle_stop` from a normal thread.
///
/// Returns `AORTA_OK` on success.
/// Returns `AORTA_ERR_NOT_FOUND` if the handle was never allocated or has
/// already been freed.
#[no_mangle]
pub extern "C" fn aorta_stop_handle_stop(handle: aorta_stop_handle_t) -> aorta_result_t {
    ffi_guard(|| {
        if handle == AORTA_STOP_HANDLE_INVALID {
            return map_aorta_error(AortaError::invalid_argument("stop handle is invalid (0)"));
        }
        let stop = { registry().stop_handles.lock().get(&handle).cloned() };
        let Some(stop) = stop else {
            return map_aorta_error(AortaError::not_found("stop handle not found"));
        };
        stop.stop();
        AORTA_OK
    })
}

/// Free a stop handle previously obtained from `aorta_executor_get_stop_handle`.
///
/// - Passing `AORTA_STOP_HANDLE_INVALID` (0) is a no-op (`AORTA_OK`).
/// - Freeing the same handle twice returns `AORTA_ERR_NOT_FOUND` on the
///   second call instead of triggering use-after-free.
/// - The underlying `StopHandle` is dropped when its last `Arc` strong ref
///   goes out of scope (typically immediately after this call).
#[no_mangle]
pub extern "C" fn aorta_stop_handle_free(handle: aorta_stop_handle_t) -> aorta_result_t {
    ffi_guard(|| {
        if handle == AORTA_STOP_HANDLE_INVALID {
            return AORTA_OK;
        }
        let removed = registry().stop_handles.lock().remove(&handle);
        match removed {
            Some(_) => AORTA_OK,
            None => map_aorta_error(AortaError::not_found("stop handle not found")),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_executor_free(exec_handle: aorta_exec_t) -> aorta_result_t {
    ffi_guard(|| {
        // Unlike `aorta_node_free` / `aorta_subscriber_free`, this FREE path
        // is fully synchronous (`detach_nodes()` + registry remove) — there
        // is no async cleanup work to spawn-and-detach. Returning REENTRANT
        // is therefore the correct response: the outer spin still holds
        // `entry.exec.lock()`, so the inner acquire below would deadlock.
        // The C caller must retry from a non-callback thread (typically the
        // same thread that owns the spin loop, after spin has returned).
        if in_async_context() {
            return aorta_reentrant(module_path!());
        }

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        entry.stop.stop();
        let exec = entry.exec.lock();
        match &*exec {
            ExecutorInstance::Single(exec) => exec.detach_nodes(),
            ExecutorInstance::Multi(exec) => exec.detach_nodes(),
        }

        registry().executors.lock().remove(&exec_handle);

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
pub unsafe extern "C" fn aorta_executor_create_callback_group(
    exec_handle: aorta_exec_t,
    group: aorta_callback_group_t,
    out: *mut aorta_callback_group_handle_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return map_aorta_error(AortaError::invalid_argument("out is null"));
        }

        if in_async_context() {
            return aorta_reentrant(module_path!());
        }

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        let exec = entry.exec.lock();
        let handle = exec.create_callback_group(callback_group_from_ffi(group));
        // SAFETY: `out` was null-checked above; caller contract requires it
        // point at writable storage for `aorta_callback_group_handle_t`.
        unsafe {
            *out = handle;
        }
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_executor_free_callback_group(
    exec_handle: aorta_exec_t,
    callback_group: aorta_callback_group_handle_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if in_async_context() {
            return aorta_reentrant(module_path!());
        }

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        let exec = entry.exec.lock();
        if exec.remove_callback_group(callback_group) {
            AORTA_OK
        } else {
            map_aorta_error(AortaError::not_found("callback group handle not found"))
        }
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
pub unsafe extern "C" fn aorta_executor_get_admission_stats(
    exec_handle: aorta_exec_t,
    out: *mut aorta_executor_admission_stats_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return map_aorta_error(AortaError::invalid_argument("out is null"));
        }

        if in_async_context() {
            return aorta_reentrant(module_path!());
        }

        let entry = { registry().executors.lock().get(&exec_handle).cloned() };

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };

        // On an ATTACHED executor the lane
        // budgets are THE admission tier — report the lane-backed
        // equivalents field-for-field from the SDK's telemetry report
        // (depth/enqueued/shed rows; summed across class lanes).
        // Mapping: queued_events ← Σdepth; dropped_queue_full ← Σshed
        // (the lane's only Full-shed counter — mirrors the executor-tier meaning);
        // dropped_budget_events ← Σexpired (deadline sheds are the lane
        // tier's budget-class drop); peak_queued_events ← Σhigh_water.
        // Byte columns stay core-side (the lane report carries none —
        // documented in the v6 schema).
        // Read the admission counters through the
        // cached sender (same `AdmissionState` Arc as the queue), NEVER
        // through `entry.exec` — `aorta_executor_spin` holds that mutex
        // for its whole run, so locking it here blocks a monitor thread
        // until stop and self-deadlocks a dispatched callback (the cached
        // `sender`/`is_multi` rule applied to this entry point).
        if let Some(lanes) = entry.stop.lane_hooks().lane_telemetry() {
            let sum = |row: usize| lanes[row].iter().sum::<u64>();
            let core = entry.sender.admission_stats();
            let mut stats = executor_admission_stats_from_core(core);
            stats.queued_events = sum(0);
            stats.peak_queued_events = sum(1);
            stats.dropped_queue_full = sum(3);
            stats.dropped_budget_events = sum(4);
            // SAFETY: `out` null-checked above.
            unsafe {
                *out = stats;
            }
            return AORTA_OK;
        }
        // SAFETY: `out` was null-checked above; caller contract requires it
        // point at writable storage for `aorta_executor_admission_stats_t`.
        unsafe {
            *out = executor_admission_stats_from_core(entry.sender.admission_stats());
        }
        AORTA_OK
    })
}

/// Telemetry v6 (ABI bump 8): report the attached pool's
/// per-class-lane counters. `stats` points at 20 u64 values — 5 rows
/// [depth, high_water, enqueued, shed, expired] × 4 class lanes. The
/// SDK's facade drain hook calls this opportunistically (throttled);
/// the telemetry snapshot reads the latest report.
///
/// # Safety
/// `stats` must point at 20 readable u64 values; `exec_handle` must
/// originate from `aorta_executor_new` and not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_executor_update_lane_telemetry(
    exec_handle: aorta_exec_t,
    stats: *const u64,
) -> aorta_result_t {
    ffi_guard(|| {
        if stats.is_null() {
            return map_aorta_error(AortaError::invalid_argument("stats is null"));
        }
        let entry = { registry().executors.lock().get(&exec_handle).cloned() };
        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };
        // SAFETY: caller contract — 20 readable u64 values.
        let flat = unsafe { std::slice::from_raw_parts(stats, 20) };
        let mut snapshot = [[0u64; 4]; 5];
        for (row, chunk) in snapshot.iter_mut().zip(flat.chunks_exact(4)) {
            row.copy_from_slice(chunk);
        }
        entry.stop.lane_hooks().set_lane_telemetry(snapshot);
        AORTA_OK
    })
}

/// ABI bump 8: create a callback group with a DECLARED lane
/// class (0..=3; the lane-class vocabulary). Grouped callbacks on an ATTACHED
/// executor execute at this class (monotone rule — members
/// of a higher class trip the SDK's one-shot WARN).
///
/// # Safety
/// Same contract as `aorta_executor_create_callback_group`.
#[no_mangle]
pub unsafe extern "C" fn aorta_executor_create_callback_group_with_class(
    exec_handle: aorta_exec_t,
    group: aorta_callback_group_t,
    lane_class: u8,
    out: *mut aorta_callback_group_handle_t,
) -> aorta_result_t {
    // SAFETY: same caller contract as create_callback_group (docs above).
    if lane_class > 3 {
        // Reject instead of silently clamping.
        return aorta_err(
            AORTA_ERR_INVALID_ARGUMENT,
            "lane_class must be 0..=3 (realtime/high/normal/low)",
        );
    }
    // SAFETY: same caller contract as create_callback_group (docs above).
    let rc = unsafe { aorta_executor_create_callback_group(exec_handle, group, out) };
    if rc != AORTA_OK {
        return rc;
    }
    ffi_guard(|| {
        let entry = { registry().executors.lock().get(&exec_handle).cloned() };
        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };
        // SAFETY: create succeeded, so `out` is valid and initialized.
        entry
            .stop
            .lane_hooks()
            .set_group_class(unsafe { *out }, lane_class);
        AORTA_OK
    })
}

// =========================================================== Flow lane facade

/// The attach hooks (ABI 8). `drain_fn`
/// returns its remaining-work report; `on_stop` is TRAMPOLINE-CLASS
/// (pool close + revoke sweep + wake — no user code, stop returns
/// immediately).
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct aorta_lane_hooks_t {
    pub drain_fn: Option<unsafe extern "C" fn(ctx: *mut std::ffi::c_void, max_items: u32) -> u64>,
    pub ctx: *mut std::ffi::c_void,
    pub on_stop: Option<unsafe extern "C" fn(ctx: *mut std::ffi::c_void)>,
    /// Forward an admitted C++ callback item on the
    /// producing thread. NULL = no forwarding (zero-init convention).
    /// Returns 1 accepted, 2 refused-full (caller keeps the item), 0
    /// refused-closed.
    pub forward_fn: Option<
        unsafe extern "C" fn(
            ctx: *mut std::ffi::c_void,
            run: unsafe extern "C" fn(*mut std::ffi::c_void),
            drop: unsafe extern "C" fn(*mut std::ffi::c_void),
            item: *mut std::ffi::c_void,
            lane_class: u8,
            group: u64,
            deadline_rel_ns: u64,
            cost_bytes: u32,
        ) -> u8,
    >,
    /// Pool worker count (0 = ST) for attach validation.
    /// (No reserved tail — INPUT structs must not ship speculative
    /// fields per the evolution policy in `mod.rs`; growth happens via
    /// a `_v2` symbol + ABI bump.)
    pub pool_workers: u32,
}

/// Enter facade mode: the executor's spin loops drain the
/// attached lane pool each iteration (never parking while it reports
/// backlog) and every stop origin bridges through `on_stop`. Without
/// this call the executor is bit-for-bit the queue path — permanently, for the
/// Rust/Python planes.
///
/// # Safety
/// `hooks` must be valid for the duration of the call; the registered
/// (drain_fn, ctx, on_stop) must stay callable until
/// `aorta_executor_detach_lane_pool` returns (its write lock is the
/// quiesce barrier).
#[no_mangle]
pub unsafe extern "C" fn aorta_executor_attach_lane_pool(
    exec_handle: aorta_exec_t,
    hooks: *const aorta_lane_hooks_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if hooks.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "hooks is null");
        }
        // SAFETY: caller contract — hooks points at a valid struct.
        let h = unsafe { &*hooks };
        let Some(drain_fn) = h.drain_fn else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "drain_fn is null");
        };
        let entry = { registry().executors.lock().get(&exec_handle).cloned() };
        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };
        // Validate the threading pairing at attach — an MT executor
        // with an ST pool has zero drainers once spin parks; an ST
        // executor with an MT pool destroys the ST no-concurrency
        // guarantee.
        let is_mt = entry.is_multi;
        if is_mt && h.pool_workers == 0 {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "MT executor requires an MT lane pool (workers > 0)",
            );
        }
        if !is_mt && h.pool_workers > 0 {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "ST executor requires an ST lane pool (workers == 0)",
            );
        }
        // Double-attach guard: a second attach
        // while forward()/drain() may still be mid-flight on the OLD hooks
        // used to return OK silently — the caller then freed the old pool
        // under a live callback (UAF). Attach is check-and-set under the
        // slot's write lock; re-attach requires an explicit detach (whose
        // in-flight spin IS the quiesce barrier).
        if !entry.stop.lane_hooks().attach(crate::executor::LaneHooks {
            drain_fn,
            ctx: h.ctx as usize,
            on_stop: h.on_stop,
            forward_fn: h.forward_fn,
            pool_workers: h.pool_workers,
        }) {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "executor already has a lane pool attached — call \
                 aorta_executor_detach_lane_pool first",
            );
        }
        // Nudge the spin loop so an already-parked executor notices the
        // pool immediately.
        entry.stop.wake();
        AORTA_OK
    })
}

/// Leave facade mode. The internal write lock IS the quiesce barrier:
/// when this returns, no in-flight `drain_fn`/`on_stop` invocation
/// exists and none will start.
#[no_mangle]
pub extern "C" fn aorta_executor_detach_lane_pool(exec_handle: aorta_exec_t) -> aorta_result_t {
    ffi_guard(|| {
        // Reentrancy guard: detach's quiesce
        // spin waits for every in-flight hook frame to return — invoked
        // from INSIDE one (a drain_fn/lane-worker callback on this thread,
        // e.g. dynamic pool teardown reacting to a message under the ST
        // `LaneRuntime::SingleThread` default, where drain_fn runs inline
        // on the spin thread), it waits on a counter only the blocked
        // outer frame can decrement: a silent 100%-CPU hang. Same guard
        // every sibling blocking entry point already carries.
        if crate::flow::reentrancy::explicitly_reentrant() {
            return aorta_reentrant(module_path!());
        }
        let entry = { registry().executors.lock().get(&exec_handle).cloned() };
        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };
        entry.stop.lane_hooks().detach();
        AORTA_OK
    })
}

/// Wake the executor's spin loop WITHOUT stopping it — the combined ST
/// wait's wake path the lane pool's wake hook calls this
/// after every post so a parked spin thread drains the new item.
#[no_mangle]
pub extern "C" fn aorta_executor_wake(exec_handle: aorta_exec_t) -> aorta_result_t {
    ffi_guard(|| {
        let entry = { registry().executors.lock().get(&exec_handle).cloned() };
        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };
        entry.stop.wake();
        AORTA_OK
    })
}

/// Resolve-once, poll-lock-free RT-safe pointer to the executor's
/// monotonic stop flag (the SerialDomain stop-bridge arming edge; the
/// `aorta_scope_cancelled_flag_ptr` precedent). The pointee is a 0→1
/// monotonic atomic byte — the SAME atomic the spin thread observes to
/// enter its final drain — stored with release order; read it with
/// acquire order. This is the arming edge of the SDK's stop-bridge
/// state machine: the facade resolves the pointer ONCE at attach and
/// polls it from the drain hook without any registry lookup (the
/// lock-free-poll doctrine, `flow.rs`).
///
/// Pointer valid until `aorta_executor_free` (the registry entry keeps
/// the flag's `Arc` alive); the SDK's attach/detach lifecycle brackets
/// every read inside that window.
///
/// # Safety
/// - `out_flag` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_executor_stop_flag_ptr(
    exec_handle: aorta_exec_t,
    out_flag: *mut *const u8,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers `*out_flag`.
    ffi_guard(|| unsafe {
        if out_flag.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_flag is null");
        }
        let entry = { registry().executors.lock().get(&exec_handle).cloned() };
        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("executor handle not found"));
        };
        *out_flag = entry.stop.stop_flag_ptr();
        AORTA_OK
    })
}

/// TEST-ONLY (SerialDomain stop-window tests, `test-seams` cargo feature): install or
/// clear (`cb = NULL`) the stop-window seam — a pause point both
/// executor stop paths hit BETWEEN the stop-flag store and the lane
/// `on_stop` bridge. The C++ stop-flag-window family parks a barrier
/// here to hold the window open deterministically. Absent from release
/// cores (the symbol only exists when the feature is on; the dedicated
/// `//core:aorta_core_ffi_test_seams` target is the sole enabler) and
/// excluded from the public header by name (`cbindgen.toml`).
///
/// # Safety
/// - `cb` + `ctx` must stay valid until cleared (single-writer, test-only).
#[cfg(feature = "test-seams")]
#[no_mangle]
pub unsafe extern "C" fn aorta_test_stop_window_seam(
    cb: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
    ctx: *mut std::ffi::c_void,
) -> aorta_result_t {
    ffi_guard(|| {
        crate::executor::stop_window_seam::set(cb, ctx);
        AORTA_OK
    })
}

/// Callback-context marks for MT lane WORKERS: wrap each resume so the
/// reentrancy guards (`scope_close`/`spin`/`executor_free` →
/// `AORTA_ERR_REENTRANT`) keep firing inside lane-dispatched frames.
/// Returns the previous mark; pass it back to `exit` (nesting-safe).
#[no_mangle]
pub extern "C" fn aorta_callback_context_enter() -> u8 {
    // Trivial TLS swap — panic-free by construction; no ffi_guard (its
    // return type is the result code, not the mark).
    u8::from(crate::flow::reentrancy::callback_context_replace(true))
}

/// # Safety
/// Pass the value returned by the matching `enter` (restores nesting).
#[no_mangle]
pub extern "C" fn aorta_callback_context_exit(prev: u8) -> aorta_result_t {
    ffi_guard(|| {
        crate::flow::reentrancy::callback_context_replace(prev != 0);
        AORTA_OK
    })
}
