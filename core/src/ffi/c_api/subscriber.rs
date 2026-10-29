// Subscriber FFI surface.
//
// Extracted from c_api/mod.rs.

use super::*;

/// Subscriber constructor (executor/callback delivery, no buffering knobs).
///
/// QoS (including `express` + `congestion_control`) rides `qos`. The QoS &
/// buffering knobs do **not** ride `opts` — `aorta_sub_options_t` is
/// layout-frozen (its size never changes); pass an `aorta_sub_buffering_t`
/// to `aorta_subscriber_new_buffered` instead.
#[no_mangle]
pub extern "C" fn aorta_subscriber_new(
    node: aorta_node_t,
    topic: *const c_char,
    qos: *const aorta_qos_t,
    opts: *const aorta_sub_options_t,
    cb: aorta_on_msg_cb,
    user_data: *mut c_void,
    out: *mut aorta_sub_t,
) -> aorta_result_t {
    ffi_guard(|| subscriber_new_inner(node, topic, qos, opts, std::ptr::null(), cb, user_data, out))
}

/// Subscriber constructor with buffering knobs (`aorta_sub_buffering_t`).
///
/// `buffering == NULL` — or a zero-initialized struct — behaves exactly like
/// `aorta_subscriber_new`. When `buffering->delivery_mode == 1` (InlinePull)
/// the `cb` is ignored and a native keep-last ring subscriber is created —
/// drain it with `aorta_sub_recv` / `aorta_sub_try_recv`.
#[no_mangle]
pub extern "C" fn aorta_subscriber_new_buffered(
    node: aorta_node_t,
    topic: *const c_char,
    qos: *const aorta_qos_t,
    opts: *const aorta_sub_options_t,
    buffering: *const aorta_sub_buffering_t,
    cb: aorta_on_msg_cb,
    user_data: *mut c_void,
    out: *mut aorta_sub_t,
) -> aorta_result_t {
    ffi_guard(|| subscriber_new_inner(node, topic, qos, opts, buffering, cb, user_data, out))
}

fn subscriber_new_inner(
    node: aorta_node_t,
    topic: *const c_char,
    qos: *const aorta_qos_t,
    opts: *const aorta_sub_options_t,
    buffering: *const aorta_sub_buffering_t,
    cb: aorta_on_msg_cb,
    user_data: *mut c_void,
    out: *mut aorta_sub_t,
) -> aorta_result_t {
    if out.is_null() {
        return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
    }

    // Buffering knobs ride the dedicated `aorta_sub_buffering_t`; a null
    // pointer (the no-buffering default) reproduces today's behavior
    // (receive_depth=0, delivery=Executor, overflow=DropNewest).
    // SAFETY: `buffering` is an optional caller pointer; `as_ref` handles null.
    let (receive_depth, delivery_mode, receive_overflow) = match unsafe { buffering.as_ref() } {
        Some(b) => (b.receive_depth, b.delivery_mode, b.receive_overflow),
        None => (0, 0, 0),
    };

    // SAFETY: `topic` is the caller-supplied `*const c_char`; `cstr_to_string`
    // null-checks and validates UTF-8, so this is safe as long as a non-null
    // `topic` points at a NUL-terminated byte sequence per the FFI contract.
    let topic_str = unsafe { cstr_to_string(topic, "topic") };
    let topic_str = match topic_str {
        Ok(value) => value,
        Err(err) => return map_aorta_error(err),
    };

    let node = { registry().nodes.lock().get(&node).cloned() };

    let Some(node) = node else {
        return map_aorta_error(AortaError::not_found("node handle not found"));
    };

    let qos_value = match qos_from_ffi(qos) {
        Ok(value) => value,
        Err(err) => return map_aorta_error(err),
    };

    let mut options = SubscriberOptions::default().with_qos(qos_value);
    // SAFETY: `opts` is an optional pointer from FFI caller; `as_ref` safely
    // handles null and returns `None` in that case.
    if let Some(opts) = unsafe { opts.as_ref() } {
        options.callback_group = callback_group_from_ffi(opts.callback_group);
        options.callback_group_handle = callback_group_handle_from_ffi(opts.callback_group_handle);
        options.prefetch_schema_on_init = opts.prefetch_schema_on_init != 0;
        options.enable_header_context = opts.enable_header_context != 0;
        options.include_topic_in_context = opts.include_topic_in_context != 0;
        options.include_receive_timestamp = opts.include_receive_timestamp != 0;
        options.include_priority_in_context = opts.include_priority_in_context != 0;
        options.include_publisher_timestamp = opts.include_publisher_timestamp != 0;
        options.enable_dedup = opts.enable_dedup != 0;
        if opts.dedup_window > 0 {
            options.dedup_window = opts.dedup_window;
        }
        options.enable_reorder_check = opts.enable_reorder_check != 0;
        options.drop_out_of_order = opts.drop_out_of_order != 0;
        options.overflow_policy = subscriber_overflow_policy_from_ffi(opts.overflow_policy);
        if opts.overflow_relay_capacity > 0 {
            options.overflow_relay_capacity = opts.overflow_relay_capacity;
        }
        if !opts.schema_bfbs.is_null() && opts.schema_bfbs_len > 0 {
            // SAFETY: `schema_bfbs` is non-null (checked) and `schema_bfbs_len`
            // describes a contiguous BFBS byte buffer owned by the FFI caller
            // for the duration of this call (FFI options contract).
            let schema_bfbs =
                unsafe { std::slice::from_raw_parts(opts.schema_bfbs, opts.schema_bfbs_len) };
            if let Some(layout) = infer_aorta_header_layout_from_bfbs(schema_bfbs) {
                options = options.with_header_layout_hint(layout);
            }
        }
    }

    // QoS & buffering knobs. Reject unknown discriminants (fail loud)
    // instead of silently coercing them to a default. `0` reproduces
    // today's behavior (Executor / DropNewest / receive_depth=0).
    options.receive_depth = receive_depth as usize;
    options.delivery = match delivery_mode {
        0 => DeliveryMode::Executor,
        1 => DeliveryMode::InlinePull,
        2 => DeliveryMode::InlinePush,
        other => {
            return map_aorta_error(AortaError::invalid_argument(format!(
                "invalid delivery_mode {other} (expected 0=executor, 1=inline_pull, 2=inline_push)"
            )))
        }
    };
    options.receive_overflow = match receive_overflow {
        0 => ReceiveOverflow::DropNewest,
        1 => ReceiveOverflow::DropOldest,
        other => {
            return map_aorta_error(AortaError::invalid_argument(format!(
                "invalid receive_overflow {other} (expected 0=drop_newest, 1=drop_oldest)"
            )))
        }
    };

    // The legacy overflow compat rewrite (node class-policy defaulting Drop →
    // Relay) only makes sense for the executor/callback path: an InlinePull
    // subscriber has no overflow relay (validate_delivery rejects Relay), so
    // rewriting its untouched defaults would make every pull creation on a
    // Relay-defaulting node fail with an error the caller never asked for —
    // and the pure-Rust `create_subscriber_pull` path never applies it either.
    if options.delivery == DeliveryMode::Executor {
        let class_defaults = node.executor_class_policy_defaults();
        class_defaults.apply_subscriber_overflow_compat(&mut options);
    }

    // Pull mode: no callback; the application drains via `aorta_sub_recv`. A
    // non-null callback here is a caller error — reject it rather than silently
    // dropping the callback (InlinePull + a callback is an error).
    if options.delivery == DeliveryMode::InlinePull {
        if cb.is_some() {
            return map_aorta_error(AortaError::invalid_argument(
                "delivery=InlinePull takes no callback; drain via aorta_sub_recv",
            ));
        }
        let subscriber =
            match crate::ffi_block_on!(Subscriber::new_pull(&node, &topic_str, options)) {
                Ok(Ok(subscriber)) => subscriber,
                Ok(Err(err)) => return map_aorta_error(err),
                Err(_) => return aorta_reentrant(module_path!()),
            };
        let handle = alloc_handle();
        registry().subscribers.lock().insert(
            handle,
            FfiSubscriberEntry {
                subscriber,
                in_flight: Arc::new(AtomicUsize::new(0)),
            },
        );
        // SAFETY: `out` was null-checked at the top of the fn.
        unsafe {
            *out = handle;
        }
        return AORTA_OK;
    }

    // Executor (callback) mode requires a callback.
    if cb.is_none() {
        return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
    }
    let cb = cb.unwrap();
    let user_data = user_data as usize;

    // Teardown gate: count in-flight C++ dispatcher (`cb`)
    // invocations so `aorta_subscriber_free` can drain them before the C++
    // destructor frees its CallbackState. The closure increments before `cb`
    // and the `InFlightGuard` decrements after; one Arc clone lives in the
    // closure, the other in the registry entry for `free` to observe.
    let in_flight = Arc::new(AtomicUsize::new(0));
    let in_flight_cb = Arc::clone(&in_flight);

    let subscriber = match crate::ffi_block_on!(Subscriber::new_with_options(
        &node,
        &topic_str,
        move |payload, context| {
            let msg_ctx = build_msg_context(context);

            let schema_view: aorta_schema_hash_view_t = aorta_schema_hash_view_t {
                hex: ptr::null(),
                len: 0,
            };
            let mut single_slice = aorta_bytes_slice_t::default();
            let mut payload_slices = Vec::new();
            let payload_view =
                bytes_view_from_zbytes_borrowed(payload, &mut single_slice, &mut payload_slices);
            let user_data = user_data as *mut c_void;
            // Subscriber on-message dispatch — wrap so a Rust-side panic in
            // OUR marshaling code (bytes_view_from_zbytes_borrowed, the
            // schema_view setup, the user_data cast above) cannot unwind
            // across the extern "C" frame back into the tokio task that
            // drives the subscriber. NOTE: this does NOT catch panics that
            // originate INSIDE the user's `cb` itself — Rust ≥ 1.81 marks
            // every extern "C" function as nounwind, so a panic in `cb`
            // aborts the process before our catch_unwind can observe it.
            // The C++ SDK's try/catch around the dispatcher is
            // what protects C++ users end-to-end.
            //
            // Bracket the dispatcher call with the in-flight gate so
            // `aorta_subscriber_free` can wait for `cb` to return before the
            // C++ side frees its CallbackState. The increment
            // must surround the WHOLE `cb` call; the guard decrements on every
            // exit (ffi_guard_void converts our marshaling panics to a normal
            // return, so the decrement always runs in the non-abort case).
            in_flight_cb.fetch_add(1, Ordering::AcqRel);
            // (&Arc auto-derefs to &AtomicUsize for the guard's borrow.)
            let _drain_guard = InFlightGuard(&in_flight_cb);
            ffi_guard_void(|| {
                cb(
                    ptr::addr_of!(payload_view),
                    ptr::addr_of!(schema_view),
                    ptr::addr_of!(msg_ctx),
                    user_data,
                );
            });
        },
        options,
    )) {
        Ok(Ok(subscriber)) => subscriber,
        Ok(Err(err)) => return map_aorta_error(err),
        Err(_) => return aorta_reentrant(module_path!()),
    };

    let handle = alloc_handle();
    registry().subscribers.lock().insert(
        handle,
        FfiSubscriberEntry {
            subscriber,
            in_flight,
        },
    );

    // SAFETY: `out` was null-checked at the top of the fn; caller contract
    // requires it point at writable storage for `aorta_sub_t`.
    unsafe {
        *out = handle;
    }

    AORTA_OK
}

#[no_mangle]
pub extern "C" fn aorta_subscriber_free(sub_handle: aorta_sub_t) -> aorta_result_t {
    ffi_guard(|| {
        let entry = { registry().subscribers.lock().remove(&sub_handle) };

        let Some(FfiSubscriberEntry {
            subscriber,
            in_flight,
        }) = entry
        else {
            return map_aorta_error(AortaError::not_found("subscriber handle not found"));
        };

        // See `aorta_node_free`: FREE paths use spawn-and-detach on
        // reentrant invocation rather than returning REENTRANT. The in-flight
        // drain is NOT applied here: this path is reached when free is called
        // from within an async/callback context (e.g. dropping a subscriber
        // inside its own callback), where blocking would deadlock. The C++
        // destructor's wait + leak-on-timeout remains the fallback for this
        // rarer case.
        if in_async_context() {
            runtime().spawn(async move {
                let _ = subscriber.undeclare().await;
            });
            return AORTA_OK;
        }

        // `undeclare` consumes the `Subscriber` and drops its Zenoh subscriber.
        // For a pull subscriber that drops the ring's sender side, so a thread
        // currently parked in `aorta_sub_recv` (which holds only the lightweight
        // `PullRecvHandle`, not this `Subscriber`) is unblocked and returns an
        // error — bounded teardown, no leak.
        let result = match runtime().block_on(subscriber.undeclare()) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
        };
        // `undeclare()` guarantees no NEW dispatches, but an already-entered
        // C++ dispatcher may still be running `cb` (subscriber.rs documents
        // this). Wait for it to finish before returning so the C++ destructor
        // can free its CallbackState without a UAF.
        // Bounded — a hung user callback falls through to the C++ side's own
        // wait+leak-on-timeout.
        if !drain_in_flight(&in_flight, FREE_DRAIN_TIMEOUT) {
            aorta_core_warn!(
                "aorta_subscriber_free: timed out draining in-flight callbacks; \
                 C++ destructor will fall back to its own wait+leak"
            );
        }
        result
    })
}

/// Read a subscriber's runtime drop/reorder counters.
///
/// Backs the Rust SDK `Subscriber::runtime_stats` — it had
/// no C-ABI bridge before. Fills `out` with the 7 counters.
///
/// # Safety
/// - `sub_handle` must be a valid `aorta_sub_t` handle
/// - `out` must point to writable storage for `aorta_subscriber_runtime_stats_t`
#[no_mangle]
pub unsafe extern "C" fn aorta_subscriber_runtime_stats(
    sub_handle: aorta_sub_t,
    out: *mut aorta_subscriber_runtime_stats_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let subscribers = registry().subscribers.lock();
        let Some(entry) = subscribers.get(&sub_handle) else {
            return map_aorta_error(AortaError::not_found("subscriber handle not found"));
        };
        let stats = entry.subscriber.runtime_stats();
        // SAFETY: `out` null-checked above; caller contract requires it point at
        // writable storage for `aorta_subscriber_runtime_stats_t`.
        unsafe {
            *out = aorta_subscriber_runtime_stats_t {
                duplicate_drop_count: stats.duplicate_drop_count,
                out_of_order_count: stats.out_of_order_count,
                out_of_order_drop_count: stats.out_of_order_drop_count,
                executor_drop_count: stats.executor_drop_count,
                local_drop_count: stats.local_drop_count,
                overflow_relay_enqueue_count: stats.overflow_relay_enqueue_count,
                overflow_relay_drop_count: stats.overflow_relay_drop_count,
            };
        }
        AORTA_OK
    })
}

/// Pull the next sample from an `InlinePull` (Tier B-pull) subscriber.
///
/// `timeout_ns` selects the blocking mode:
/// - `< 0` → block until a sample arrives (or the subscriber disconnects).
/// - `0`   → non-blocking (equivalent to `aorta_sub_try_recv`).
/// - `> 0` → block up to `timeout_ns` nanoseconds.
///
/// Call it from a dedicated consumer thread. A blocking pull (`timeout_ns !=
/// 0`) from inside an Aorta callback would park the executor's worker, so it
/// returns `AORTA_ERR_REENTRANT` there — use `aorta_sub_try_recv` (or
/// `timeout_ns == 0`) if you must probe the ring from a callback.
///
/// On success fills `*out` and returns `AORTA_OK`; the caller MUST release the
/// sample with `aorta_pull_sample_free`. Returns `AORTA_ERR_TIMEOUT` when no sample
/// is available within the deadline (nothing is written to `*out`),
/// `AORTA_ERR_INVALID_ARGUMENT` for a non-pull subscriber, `AORTA_ERR_NOT_FOUND`
/// for an unknown handle, or `AORTA_ERR_INTERNAL` if the ring disconnected.
///
/// The blocking `recv` does NOT hold the global subscriber registry lock (the
/// shared `Arc<Subscriber>` is cloned out first), so other subscriber FFI calls
/// are never blocked by a pending pull.
///
/// # Safety
/// `out` must be null or point at writable storage for an `aorta_pull_sample_t`.
#[no_mangle]
pub unsafe extern "C" fn aorta_sub_recv(
    sub_handle: aorta_sub_t,
    out: *mut aorta_pull_sample_t,
    timeout_ns: i64,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        // Reentrancy contract: this is a blocking entry point, and every
        // blocking entry on the C-ABI surface must refuse to park a runtime
        // worker (invariant: callbacks get AORTA_ERR_REENTRANT, never a silent
        // wedge). Non-blocking probes (timeout_ns == 0) stay legal in-callback.
        if timeout_ns != 0 && in_async_context() {
            return aorta_reentrant(module_path!());
        }
        // Clone the lightweight pull drain handle out from under the lock, then
        // block on it WITHOUT holding the lock and WITHOUT pinning the whole
        // `Subscriber`. This lets a concurrent `aorta_subscriber_free` reclaim
        // and undeclare the subscriber — dropping the ring's sender side, which
        // unblocks this `recv`. A non-pull handle yields None.
        let handle = {
            let subs = registry().subscribers.lock();
            match subs.get(&sub_handle) {
                Some(entry) => match entry.subscriber.pull_recv_handle() {
                    Some(h) => h,
                    None => {
                        return map_aorta_error(AortaError::invalid_argument(
                            "aorta_sub_recv is only valid on a pull (InlinePull) subscriber",
                        ))
                    }
                },
                None => {
                    return map_aorta_error(AortaError::not_found("subscriber handle not found"))
                }
            }
        };

        let result = if timeout_ns < 0 {
            handle.recv().map(Some)
        } else if timeout_ns == 0 {
            handle.try_recv()
        } else {
            handle.recv_timeout(std::time::Duration::from_nanos(timeout_ns as u64))
        };

        match result {
            Ok(Some(sample)) => {
                let ffi_sample = sample_from_pull(sample);
                // SAFETY: `out` null-checked above; caller contract requires it
                // point at writable storage for an `aorta_pull_sample_t`.
                unsafe {
                    *out = ffi_sample;
                }
                AORTA_OK
            }
            Ok(None) => AORTA_ERR_TIMEOUT,
            Err(err) => map_aorta_error(err),
        }
    })
}

/// Non-blocking pull. Convenience wrapper over `aorta_sub_recv` with a zero
/// timeout; `AORTA_ERR_TIMEOUT` means the ring is currently empty.
///
/// # Safety
/// See `aorta_sub_recv`.
#[no_mangle]
pub unsafe extern "C" fn aorta_sub_try_recv(
    sub_handle: aorta_sub_t,
    out: *mut aorta_pull_sample_t,
) -> aorta_result_t {
    // SAFETY: forwards the same `out` contract.
    unsafe { aorta_sub_recv(sub_handle, out, 0) }
}

/// Number of events shed by a subscriber's Tier A `receive_depth` gate
/// (drop-newest). `0` for unknown handles, uncapped subscribers, or pull
/// subscribers (whose drops happen natively inside the ring).
#[no_mangle]
pub extern "C" fn aorta_sub_dropped_count(sub_handle: aorta_sub_t) -> usize {
    // ffi_guard_value like every other getter: unwinding across the FFI
    // boundary aborts the host process.
    crate::ffi::guard::ffi_guard_value(0, || {
        let subs = registry().subscribers.lock();
        match subs.get(&sub_handle) {
            Some(entry) => entry.subscriber.dropped_for_depth(),
            None => 0,
        }
    })
}

// ===========================================================================
// Flow per-endpoint capability setters.
// ===========================================================================

/// Per-endpoint counting-gate options (Flow Concepts §6.4; size-asserted 16/8).
/// `max_in_flight` (0 = ungated) is the settable form of `receive_depth`
/// (one mechanism); `deadline_ns` (0 = none) attaches a per-event deadline —
/// the scheduler drops the event at dequeue once it expires.
///
/// Queue depth / overflow direction are CREATION-time options
/// (`aorta_sub_buffering_t`), deliberately NOT fields here: per the Flow
/// C-ABI evolution policy (see the module docs in `c_api`), an INPUT
/// struct's every field must work — no reserved/rejected fields. Growth
/// happens via `_v2` symbols.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct aorta_admission_t {
    pub max_in_flight: u32,
    // 4 bytes implicit repr(C) padding — deadline_ns is 8-aligned.
    pub deadline_ns: u64,
}

/// Set this subscriber's Flow dispatch priority (Flow Concepts §6.4):
/// 0=realtime, 1=high, 2=normal, 3=low, 4=background; 255 restores the
/// QoS-derived mapping. Takes effect for samples arriving after the call.
#[no_mangle]
pub extern "C" fn aorta_subscriber_set_priority(
    subscriber: aorta_sub_t,
    priority: u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if priority > 4 && priority != u8::MAX {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "priority must be 0..=4 or 255 (inherit)",
            );
        }
        // Decode via the canonical priority map (the range guard
        // above makes the lossy clamp a dead path), then delegate to the
        // CORE setter — pull-mode rejection lives there (one semantic, both
        // surfaces).
        let decoded = match priority {
            u8::MAX => None,
            raw => Some(crate::event_queue::event_priority_from_index_lossy(raw)),
        };
        let subs = registry().subscribers.lock();
        match subs.get(&subscriber) {
            Some(entry) => match entry.subscriber.set_flow_priority(decoded) {
                Ok(()) => AORTA_OK,
                Err(err) => map_aorta_error(err),
            },
            None => map_aorta_error(AortaError::not_found("subscriber handle not found")),
        }
    })
}

/// Apply the Flow counting gate. See [`aorta_admission_t`].
///
/// # Safety
/// - `admission` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn aorta_subscriber_set_admission(
    subscriber: aorta_sub_t,
    admission: *const aorta_admission_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the deref,
    // null-checked at the top of the closure.
    ffi_guard(|| unsafe {
        if admission.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "admission is null");
        }
        let a = &*admission;
        let subs = registry().subscribers.lock();
        match subs.get(&subscriber) {
            Some(entry) => {
                // Delegate to the CORE setter — pull-mode rejection lives
                // there (one semantic, both surfaces).
                let deadline = match a.deadline_ns {
                    0 => None,
                    ns => Some(std::time::Duration::from_nanos(ns)),
                };
                match entry
                    .subscriber
                    .set_flow_admission(crate::subscriber::FlowAdmission::gated(
                        a.max_in_flight as usize,
                        deadline,
                    )) {
                    Ok(()) => AORTA_OK,
                    Err(err) => map_aorta_error(err),
                }
            }
            None => map_aorta_error(AortaError::not_found("subscriber handle not found")),
        }
    })
}

/// ABI bump 8: set (or clear with `func == NULL`) the
/// data-available notify hook on an `InlinePull` subscriber. The hook
/// fires ON THE RX THREAD after each ring store — TRAMPOLINE-CLASS
/// contract: it may only complete an arbiter branch / set a flag; no
/// user work. Used by the C++ `co_await sub.next()` awaiter.
///
/// # Safety
/// `(func, ctx)` must stay callable until cleared or the subscriber is
/// freed; stale arbiter ids inside the hook no-op registry-side.
#[no_mangle]
pub unsafe extern "C" fn aorta_sub_set_notify(
    sub_handle: aorta_sub_t,
    func: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
    ctx: *mut std::ffi::c_void,
) -> aorta_result_t {
    ffi_guard(|| {
        let pull = {
            let subs = registry().subscribers.lock();
            match subs.get(&sub_handle) {
                Some(entry) => match entry.subscriber.pull_recv_handle() {
                    Some(h) => h,
                    None => {
                        return aorta_err(
                            AORTA_ERR_INVALID_ARGUMENT,
                            "notify hook requires an InlinePull subscriber",
                        );
                    }
                },
                None => {
                    return map_aorta_error(AortaError::not_found("subscriber handle not found"));
                }
            }
        };
        if !pull.set_notify_hook(func.map(|f| crate::subscriber::PullNotifyHook {
            func: f,
            ctx: ctx as usize,
        })) {
            // Single-awaiter contract — refusing beats two
            // concurrent next() awaiters silently stranding each other.
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "notify hook already set (one concurrent next()/awaiter per \
                 pull subscriber)",
            );
        }
        AORTA_OK
    })
}

/// NON-POPPING data-available probe for an
/// `InlinePull` subscriber. Writes 1 to `*out_pending` when the ring
/// holds at least one sample. The C++ `next()` recheck uses this so it
/// never steals a sample it cannot deliver and never touches its
/// coroutine frame after publishing the notify hook.
///
/// # Safety
/// `out_pending` must be valid for a write; `sub_handle` must originate
/// from a subscriber constructor and not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_sub_pending(
    sub_handle: aorta_sub_t,
    out_pending: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_pending.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_pending is null");
        }
        let pull = {
            let subs = registry().subscribers.lock();
            match subs.get(&sub_handle) {
                Some(entry) => match entry.subscriber.pull_recv_handle() {
                    Some(h) => h,
                    None => {
                        return aorta_err(
                            AORTA_ERR_INVALID_ARGUMENT,
                            "pending probe requires an InlinePull subscriber",
                        );
                    }
                },
                None => {
                    return map_aorta_error(AortaError::not_found("subscriber handle not found"));
                }
            }
        };
        // SAFETY: null-checked above.
        unsafe { *out_pending = u8::from(pull.has_pending()) };
        AORTA_OK
    })
}
