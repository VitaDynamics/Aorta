// Action Server FFI surface.
//
// Extracted from c_api/mod.rs. Pure code-move;
// every `pub extern "C" fn` keeps its `#[no_mangle]` symbol and the
// parent module re-exports `action_server::*`.

use super::*;

/// Create an info-only action server for a typed action schema.
///
/// # Safety
/// - `node` must be a valid aorta_node_t handle
/// - `action_name` must be a valid null-terminated UTF-8 string
/// - `schema_bfbs` must point to the action schema BFBS payload
#[no_mangle]
pub unsafe extern "C" fn aorta_action_server_info_new_with_options(
    node: aorta_node_t,
    action_name: *const c_char,
    schema_bfbs: *const u8,
    schema_bfbs_len: usize,
    opts: *const aorta_action_server_options_t,
    out_server: *mut aorta_action_server_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*out_* = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if action_name.is_null() || out_server.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }

        let action_name = match CStr::from_ptr(action_name).to_str() {
            Ok(s) => s,
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in action name"),
        };
        let action_name = match normalize_path(action_name) {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        let schema_bfbs = match schema_bfbs_from_ffi(schema_bfbs, schema_bfbs_len, "schema_bfbs") {
            Ok(Some(value)) => value,
            Ok(None) => {
                return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "schema_bfbs is required");
            }
            Err(err) => return map_aorta_error(err),
        };

        let node_obj = { registry().nodes.lock().get(&node).cloned() };

        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        let options = match action_server_config_from_ffi(opts) {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        let result_ttl_ms = match u64::try_from(options.result_ttl.as_millis()) {
            Ok(value) => value,
            Err(_) => {
                return aorta_err(
                    AORTA_ERR_INVALID_ARGUMENT,
                    "action result_ttl exceeds supported millisecond range",
                );
            }
        };
        let dedup_ttl_ms = match u64::try_from(options.dedup_ttl.as_millis()) {
            Ok(value) => value,
            Err(_) => {
                return aorta_err(
                    AORTA_ERR_INVALID_ARGUMENT,
                    "action dedup_ttl exceeds supported millisecond range",
                );
            }
        };

        let ensure_result = match crate::ffi_block_on!(async {
            ensure_single_action_provider(
                node_obj.session(),
                node_obj.key_builder(),
                &action_name,
                Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS),
            )
            .await
        }) {
            Ok(r) => r,
            Err(_) => return aorta_reentrant(module_path!()),
        };
        if let Err(err) = ensure_result {
            return map_aorta_error(err);
        }

        let schema_hash = SchemaHash::from_bfbs_bytes(&schema_bfbs);
        let ensure_schema_result = crate::ffi_block_on!(node_obj
            .schema_registry()
            .ensure_schema(schema_hash, schema_bfbs.clone()));
        match ensure_schema_result {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        }

        let info = ActionInfo {
            action_name: action_name.clone(),
            schema_hash,
            supports_cancel: options.supports_cancel,
            max_concurrency: match u32::try_from(options.max_concurrency) {
                Ok(value) => value,
                Err(_) => {
                    return aorta_err(
                        AORTA_ERR_INVALID_ARGUMENT,
                        "action max_concurrency exceeds u32 range",
                    );
                }
            },
            result_ttl_ms,
            dedup_ttl_ms,
            server_node_name: node_obj.name().to_string(),
            server_epoch: Uuid::new_v4().hyphenated().to_string(),
        };
        let payload = match info.encode() {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };
        let info_key: String = match node_obj.key_builder().action_info_key(&action_name) {
            Ok(value) => value.into(),
            Err(err) => return map_aorta_error(err),
        };
        let payload = Arc::new(TokioRwLock::new(Some(crate::ZBytes::from(payload))));
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = match crate::ffi_block_on!(spawn_action_info_queryable(
            node_obj.session().clone(),
            info_key,
            payload,
            crate::ZBytes::from(schema_bfbs),
            stop_rx,
        )) {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };

        let handle = alloc_handle();
        registry().action_servers.lock().insert(
            handle,
            FfiActionServerHandle::InfoOnly(FfiActionInfoServer {
                _node: node_obj,
                stop_tx: Some(stop_tx),
                task: Some(task),
            }),
        );

        *out_server = handle;
        AORTA_OK
    })
}

/// Create a full action server with typed codec callbacks supplied by the SDK.
///
/// Equivalent to [`aorta_action_server_new_with_callbacks`] with no
/// cancel callback and no `user_data` release (the caller keeps ownership
/// of `user_data` and must keep it alive until `aorta_action_server_free`
/// returns and every in-flight `execute_cb` has returned).
///
/// # Safety
/// - `node`, `action_name`, `schema_bfbs`, `codec`, and `out_server` must be valid
/// - all callbacks inside `codec` must remain valid for the lifetime of the server
#[no_mangle]
pub unsafe extern "C" fn aorta_action_server_new_with_options(
    node: aorta_node_t,
    action_name: *const c_char,
    schema_bfbs: *const u8,
    schema_bfbs_len: usize,
    opts: *const aorta_action_server_options_t,
    codec: *const aorta_action_server_codec_t,
    execute_cb: aorta_action_execute_cb,
    user_data: *mut c_void,
    out_server: *mut aorta_action_server_t,
) -> aorta_result_t {
    let callbacks = aorta_action_server_callbacks_t {
        execute: execute_cb,
        cancel_requested: None,
        user_data,
        user_data_release: None,
    };
    // SAFETY: forwards the caller's pointers unchanged under the same
    // contract; `callbacks` lives on this stack frame for the whole call.
    unsafe {
        aorta_action_server_new_with_callbacks(
            node,
            action_name,
            schema_bfbs,
            schema_bfbs_len,
            opts,
            codec,
            ptr::addr_of!(callbacks),
            out_server,
        )
    }
}

/// Create a full action server with typed codec callbacks and a callback
/// set (`execute`, optional `cancel_requested`, `user_data` and its
/// optional release).
///
/// Dispatch: each accepted goal's `execute` runs on its own dedicated
/// thread, so a handler may block until its goal reaches a terminal state
/// without delaying other goals. At most `max_concurrency` goals are
/// active and at most `max_concurrency` handlers run at once: a handler
/// that keeps running after its goal is terminal still holds its slot, so
/// the next accepted goal stays ACCEPTED until it returns. Blocking aorta FFI calls are legal inside
/// `execute`. `cancel_requested` runs on a Flow offload worker, at most
/// once per goal, when a cancel request is first accepted.
///
/// Ownership: when `callbacks->user_data_release` is non-NULL, the library
/// owns `user_data` from the moment this function is called and releases
/// it exactly once, never while a callback is using it:
/// - after `aorta_action_server_free`, once the last admitted callback has
///   returned (immediately if none is running);
/// - on a non-OK result, before returning — unless startup failed after a
///   goal had already been admitted, in which case it is released when
///   that goal's callback returns. Callers must not assume the release has
///   happened when this function returns an error.
///
/// `aorta_action_server_free` closes admission synchronously: no callback
/// is admitted after it begins. A callback admitted just before may still
/// be running, or about to enter user code, when it returns.
///
/// # Safety
/// - `node`, `action_name`, `schema_bfbs`, `codec`, `callbacks`, and
///   `out_server` must be valid
/// - all callbacks inside `codec` and `callbacks` must remain valid for
///   the lifetime of the server
#[no_mangle]
pub unsafe extern "C" fn aorta_action_server_new_with_callbacks(
    node: aorta_node_t,
    action_name: *const c_char,
    schema_bfbs: *const u8,
    schema_bfbs_len: usize,
    opts: *const aorta_action_server_options_t,
    codec: *const aorta_action_server_codec_t,
    callbacks: *const aorta_action_server_callbacks_t,
    out_server: *mut aorta_action_server_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*out_* = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if callbacks.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callbacks is null");
        }
        let callbacks = *callbacks;
        // Created first: until construction commits, every exit — an early
        // return or an unwind — closes the gate, which releases `user_data`
        // exactly once.
        let gate = FfiCallbackGate::new(callbacks.user_data_release, callbacks.user_data as usize);
        let close_on_exit = FfiCallbackGateCloseGuard::new(Arc::clone(&gate));

        if action_name.is_null() || codec.is_null() || out_server.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }

        let action_name = match CStr::from_ptr(action_name).to_str() {
            Ok(s) => s,
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in action name"),
        };
        let action_name = match normalize_path(action_name) {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        let schema_bfbs = match schema_bfbs_from_ffi(schema_bfbs, schema_bfbs_len, "schema_bfbs") {
            Ok(Some(value)) => value,
            Ok(None) => {
                return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "schema_bfbs is required");
            }
            Err(err) => return map_aorta_error(err),
        };

        let node_obj = { registry().nodes.lock().get(&node).cloned() };
        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        let options = match action_server_config_from_ffi(opts) {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        let codec = *codec;
        if codec.decode_send_goal_request_goal_id.is_none()
            || codec.decode_cancel_goal_request.is_none()
            || codec.decode_get_result_request.is_none()
            || codec.encode_send_goal_response.is_none()
            || codec.encode_cancel_goal_response.is_none()
            || codec.encode_public_status.is_none()
            || codec.encode_feedback.is_none()
            || codec.encode_get_result_response.is_none()
            || callbacks.execute.is_none()
        {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "action server codec/execute callbacks must not be null",
            );
        }
        if callbacks.cancel_requested.is_some() && !options.supports_cancel {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "cancel_requested callback requires supports_cancel",
            );
        }

        let server_callbacks = FfiActionServerCallbacks {
            codec,
            execute: callbacks.execute,
            cancel_requested: callbacks.cancel_requested,
            user_data: callbacks.user_data as usize,
        };
        let server = match crate::ffi_block_on!(FfiRawActionServer::new(
            &node_obj,
            action_name,
            schema_bfbs,
            options,
            server_callbacks,
            Arc::clone(&gate),
        )) {
            Ok(Ok(server)) => server,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };

        let start_result = crate::ffi_block_on!(server.start());
        match start_result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                // Best-effort teardown on start failure. If the helper now
                // reports reentrancy on this path it means we're already
                // inside a runtime, which the outer guard would have caught
                // — this is dead defense, but matching the new contract
                // shape is cheaper than special-casing.
                gate.close();
                server.execute_slots.close();
                let _ = crate::ffi_block_on!(async {
                    server.request_stop().await;
                    server.join(Duration::from_secs(2)).await;
                });
                return map_aorta_error(err);
            }
            Err(_) => return aorta_reentrant(module_path!()),
        }

        close_on_exit.disarm();
        let handle = alloc_handle();
        registry()
            .action_servers
            .lock()
            .insert(handle, FfiActionServerHandle::Raw(server));
        *out_server = handle;
        AORTA_OK
    })
}

fn shutdown_ffi_action_server(server: FfiActionServerHandle) {
    match server {
        FfiActionServerHandle::InfoOnly(mut server) => {
            if let Some(stop_tx) = server.stop_tx.take() {
                let _ = stop_tx.send(());
            }

            if let Some(task) = server.task.take() {
                let join_future = async move {
                    let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
                };
                // Full reentrancy check: the
                // explicit detector also covers dispatched-callback threads
                // that `try_current()` alone cannot see — blocking there
                // would wedge the callback's own dispatcher.
                if in_async_context() {
                    runtime().spawn(join_future);
                } else {
                    runtime().block_on(join_future);
                }
            }
        }
        FfiActionServerHandle::Raw(server) => {
            // Synchronously, before any (possibly detached) teardown: no
            // user callback is admitted after free begins, and `user_data`
            // is released once the admitted ones return.
            server.callback_gate.close();
            server.execute_slots.close();
            let join_future = async move {
                server.request_stop().await;
                server.join(Duration::from_secs(2)).await;
            };
            // See above: the explicit detector covers callback threads
            // `try_current()` cannot see.
            if in_async_context() {
                runtime().spawn(join_future);
            } else {
                runtime().block_on(join_future);
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn aorta_action_server_free(server: aorta_action_server_t) -> aorta_result_t {
    ffi_guard(|| {
        if server == 0 {
            return AORTA_OK;
        }

        let server_entry = { registry().action_servers.lock().remove(&server) };

        if let Some(server_entry) = server_entry {
            shutdown_ffi_action_server(server_entry);
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
pub unsafe extern "C" fn aorta_action_context_is_cancel_requested(
    ctx: aorta_action_context_t,
    out_requested: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_requested.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_requested is null");
        }

        let ctx = { registry().action_contexts.lock().get(&ctx).cloned() };
        let Some(ctx) = ctx else {
            return map_aorta_error(AortaError::not_found("action context not found"));
        };

        // SAFETY: `out_requested` was null-checked above; caller contract
        // requires writable storage for `u8`.
        unsafe {
            *out_requested = if ctx.goal.cancel_requested.load(Ordering::Relaxed) {
                1
            } else {
                0
            };
        }
        AORTA_OK
    })
}

/// Read the trace context identifying this goal: the 16-byte trace id plus
/// the span id to parent deferred work under (the per-goal server root when
/// minted, else the caller's propagated span). Returns `1` and fills the
/// outputs when the goal carries a context, else `0` (outputs untouched —
/// also for an unknown handle, or when tracing was disabled at accept).
/// The action twin of `aorta_service_request_trace_context`; feeds
/// `aorta_trace_set_context` on a worker thread.
///
/// # Safety
/// - `ctx` must be a valid `aorta_action_context_t` handle (or stale —
///   handled as not-found)
/// - when non-NULL, `out_trace_id` must point to ≥16 writable bytes and
///   `out_span_id` to a writable `u64` for the duration of the call
#[no_mangle]
pub unsafe extern "C" fn aorta_action_context_trace_context(
    ctx: aorta_action_context_t,
    out_trace_id: *mut u8,
    out_span_id: *mut u64,
) -> u8 {
    ffi_guard_value(0, || {
        let ctx = { registry().action_contexts.lock().get(&ctx).cloned() };
        let Some(ctx) = ctx else {
            return 0;
        };
        let Some(tp) = ctx.goal.trace_context() else {
            return 0;
        };
        if !out_trace_id.is_null() {
            // SAFETY: caller guarantees ≥16 writable bytes at `out_trace_id`.
            unsafe { ptr::copy_nonoverlapping(tp.trace_id.as_ptr(), out_trace_id, 16) };
        }
        if !out_span_id.is_null() {
            // SAFETY: caller guarantees a writable u64 at `out_span_id`;
            // `write_unaligned` tolerates a non-naturally-aligned pointer
            // handed across the C ABI.
            unsafe { out_span_id.write_unaligned(tp.span_id) };
        }
        1
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
pub unsafe extern "C" fn aorta_action_context_publish_feedback(
    ctx: aorta_action_context_t,
    feedback_data: *const aorta_bytes_view_t,
    message: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*out_* = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if feedback_data.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "feedback_data is null");
        }

        let payload = match zbytes_from_ffi_view(feedback_data, "feedback_data") {
            Ok(value) => value.to_bytes().to_vec(),
            Err(err) => return map_aorta_error(err),
        };
        let message = if message.is_null() {
            String::new()
        } else {
            match CStr::from_ptr(message).to_str() {
                Ok(value) => value.to_string(),
                Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in message"),
            }
        };
        let ctx = { registry().action_contexts.lock().get(&ctx).cloned() };
        let Some(ctx) = ctx else {
            return map_aorta_error(AortaError::not_found("action context not found"));
        };

        match crate::ffi_block_on!(ctx.publish_feedback(&payload, &message)) {
            Ok(Ok(_)) => AORTA_OK,
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
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
pub unsafe extern "C" fn aorta_action_context_succeed(
    ctx: aorta_action_context_t,
    result_payload: *const aorta_bytes_view_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*out_* = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if result_payload.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "result_payload is null");
        }
        let payload = match zbytes_from_ffi_view(result_payload, "result_payload") {
            Ok(value) => value.to_bytes().to_vec(),
            Err(err) => return map_aorta_error(err),
        };
        let ctx = { registry().action_contexts.lock().get(&ctx).cloned() };
        let Some(ctx) = ctx else {
            return map_aorta_error(AortaError::not_found("action context not found"));
        };

        match crate::ffi_block_on!(ctx.succeed(payload)) {
            Ok(Ok(_)) => AORTA_OK,
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
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
pub unsafe extern "C" fn aorta_action_context_abort(
    ctx: aorta_action_context_t,
    code: i16,
    message: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-supplied optional `message` C-string under the
    // NUL-termination contract (null is handled with an empty string).
    ffi_guard(|| unsafe {
        let message = if message.is_null() {
            String::new()
        } else {
            match CStr::from_ptr(message).to_str() {
                Ok(value) => value.to_string(),
                Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in message"),
            }
        };
        let code = match ActionReplyCode::try_from(code) {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };
        let ctx = { registry().action_contexts.lock().get(&ctx).cloned() };
        let Some(ctx) = ctx else {
            return map_aorta_error(AortaError::not_found("action context not found"));
        };

        match crate::ffi_block_on!(ctx.abort(code, &message)) {
            Ok(Ok(_)) => AORTA_OK,
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
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
pub unsafe extern "C" fn aorta_action_context_cancel(
    ctx: aorta_action_context_t,
    code: i16,
    message: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-supplied optional `message` C-string under the
    // NUL-termination contract (null is handled with an empty string).
    ffi_guard(|| unsafe {
        let message = if message.is_null() {
            String::new()
        } else {
            match CStr::from_ptr(message).to_str() {
                Ok(value) => value.to_string(),
                Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in message"),
            }
        };
        let code = match ActionReplyCode::try_from(code) {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };
        let ctx = { registry().action_contexts.lock().get(&ctx).cloned() };
        let Some(ctx) = ctx else {
            return map_aorta_error(AortaError::not_found("action context not found"));
        };

        match crate::ffi_block_on!(ctx.cancel(code, &message)) {
            Ok(Ok(_)) => AORTA_OK,
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_action_context_free(ctx: aorta_action_context_t) -> aorta_result_t {
    ffi_guard(|| {
        if ctx == 0 {
            return AORTA_OK;
        }
        registry().action_contexts.lock().remove(&ctx);
        AORTA_OK
    })
}
