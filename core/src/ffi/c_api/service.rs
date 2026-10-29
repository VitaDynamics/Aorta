// Service FFI surface.
//
// Extracted from c_api/mod.rs.

use super::*;

/// Create a new service
///
/// # Safety
/// - `node` must be a valid aorta_node_t handle
/// - `service_name` must be a valid null-terminated UTF-8 string
/// - `opts` can be null to use defaults
/// - `callback` must be a valid function pointer if provided
/// - `user_data` will be passed to `callback` and may be null if `callback` is None
/// - `out_service` must point to valid memory
#[no_mangle]
pub unsafe extern "C" fn aorta_service_new_with_options(
    node: aorta_node_t,
    service_name: *const c_char,
    opts: *const aorta_service_options_t,
    callback: aorta_service_callback_t,
    user_data: *mut c_void,
    out_service: *mut aorta_service_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. `ffi_guard` enters the FFI panic
    // boundary; the inner `unsafe` is needed because the closure isn't an
    // `unsafe fn`, but every pointer op here (`is_null`, `CStr::from_ptr`,
    // `*out_service` writeback below) operates on extern "C" pointer
    // arguments whose validity is the documented caller contract.
    ffi_guard(|| unsafe {
        if service_name.is_null() || out_service.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }

        let name = match CStr::from_ptr(service_name).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in service name")
            }
        };

        let node_obj = { registry().nodes.lock().get(&node).cloned() };

        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        let (schema_attach_policy, schema_info) =
            match service_config_from_ffi(opts, node_obj.schema_attach_policy()) {
                Ok(v) => v,
                Err(e) => return map_aorta_error(e),
            };

        let (
            callback_group,
            callback_group_handle,
            mut rpc_overflow_policy,
            mut rpc_overflow_block_timeout,
        ) = if let Some(opts_ref) = opts.as_ref() {
            (
                callback_group_from_ffi(opts_ref.callback_group),
                callback_group_handle_from_ffi(opts_ref.callback_group_handle),
                rpc_overflow_policy_from_ffi(opts_ref.rpc_overflow_policy),
                Duration::from_millis(opts_ref.rpc_overflow_block_timeout_ms),
            )
        } else {
            (
                CallbackGroupType::MutuallyExclusive,
                None,
                crate::service::RpcOverflowPolicy::FastFailBusy,
                Duration::from_millis(0),
            )
        };

        let class_defaults = node_obj.executor_class_policy_defaults();
        let (resolved_policy, resolved_timeout) = class_defaults
            .resolve_rpc_overflow_compat(rpc_overflow_policy, rpc_overflow_block_timeout);
        rpc_overflow_policy = resolved_policy;
        rpc_overflow_block_timeout = resolved_timeout;

        let mut builder = Service::builder(node_obj.session().clone(), &name)
            .key_builder(node_obj.key_builder().clone())
            .node_id(node_obj.id())
            .default_schema_attach_policy(node_obj.schema_attach_policy())
            .schema_registry(node_obj.schema_registry().clone())
            .inventory_telemetry(node_obj.vital_telemetry())
            .runtime_telemetry(node_obj.runtime_metrics_telemetry())
            .schema_attach_policy(schema_attach_policy)
            .callback_group(callback_group)
            .rpc_overflow_policy(rpc_overflow_policy)
            .rpc_overflow_block_timeout(rpc_overflow_block_timeout);

        if let Some(handle) = callback_group_handle {
            builder = builder.callback_group_handle(handle);
        }

        // Same snapshot-producer registration as the Rust SDK path: the
        // serving task captures this sender and cannot follow the node to
        // another executor.
        if let Some((sender, producer_guard)) =
            node_obj.callback_event_sender_for_snapshot_producer()
        {
            builder = builder
                .event_sender(sender)
                .snapshot_producer_guard(producer_guard);
        }

        if let Some(schema_info) = schema_info {
            builder = builder.schema(schema_info);
        }

        if callback.is_some() {
            let user_data = user_data as usize;
            builder = builder.handler(move |request: ServiceRequest| {
                let req_handle = alloc_handle();
                let mut single_slice = aorta_bytes_slice_t::default();
                let mut payload_slices = Vec::new();
                let payload_view = bytes_view_from_zbytes_borrowed(
                    request.payload(),
                    &mut single_slice,
                    &mut payload_slices,
                );

                registry()
                    .service_requests
                    .lock()
                    .insert(req_handle, Arc::new(Mutex::new(Some(request))));

                if let Some(cb) = callback {
                    let user_data = user_data as *mut c_void;
                    // Service request dispatch — wrap so a Rust-side panic
                    // in OUR marshaling code (the payload_view setup above,
                    // the cast, registry inserts on the spawn closure path)
                    // cannot unwind into the service-request task. NOTE:
                    // this does NOT catch panics that originate INSIDE the
                    // user's `cb` — Rust ≥ 1.81 marks extern "C" as
                    // nounwind, so a panic in `cb` aborts before our
                    // catch_unwind sees it. The C++ SDK's try/catch
                    // is what protects C++ users.
                    // Replaces the previous hand-rolled catch_unwind that
                    // pre-dated the ffi_guard helpers.
                    ffi_guard_void(|| {
                        cb(ptr::addr_of!(payload_view), req_handle, user_data);
                    });
                }
            });
        }

        let service = builder.build();
        let running_service = match crate::ffi_block_on!(service.start()) {
            Ok(Ok(rs)) => rs,
            Ok(Err(e)) => return map_aorta_error(e),
            Err(_) => return aorta_reentrant(module_path!()),
        };

        let handle = alloc_handle();
        registry()
            .services
            .lock()
            .insert(handle, Arc::new(Mutex::new(Some(running_service))));

        *out_service = handle;
        AORTA_OK
    })
}

/// Free a service
#[no_mangle]
pub extern "C" fn aorta_service_free(service: aorta_service_t) -> aorta_result_t {
    ffi_guard(|| {
        if service == 0 {
            return AORTA_OK;
        }

        let entry = registry().services.lock().remove(&service);

        if let Some(entry) = entry {
            let mut guard = entry.lock();
            if let Some(mut running) = guard.take() {
                // Avoid calling block_on when invoked from within the FFI runtime itself.
                // For typical callers (C++), this runs on a non-runtime thread and we can
                // deterministically join before returning to reduce flaky shutdown behavior.
                let join_timeout = Duration::from_secs(2);
                // Stop synchronously before returning from free so new requests are no longer
                // dispatched to user callbacks after the handle is released.
                running.request_stop();
                // Full reentrancy check: the
                // explicit detector also covers dispatched-callback threads
                // that `try_current()` alone cannot see.
                if in_async_context() {
                    runtime().spawn(async move {
                        running.stop_and_join(join_timeout).await;
                    });
                } else {
                    runtime().block_on(running.stop_and_join(join_timeout));
                }
            }
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
pub unsafe extern "C" fn aorta_service_get_stats(
    service: aorta_service_t,
    out: *mut aorta_service_stats_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return map_aorta_error(AortaError::invalid_argument("out is null"));
        }

        let entry = registry().services.lock().get(&service).cloned();

        let Some(entry) = entry else {
            return map_aorta_error(AortaError::not_found("service handle not found"));
        };

        let stats = {
            let guard = entry.lock();
            let Some(running) = guard.as_ref() else {
                return map_aorta_error(AortaError::not_found("service handle not found"));
            };
            running.stats()
        };

        // SAFETY: `out` was null-checked above; caller contract (`# Safety`)
        // requires it to point at writable storage for `aorta_service_stats_t`.
        unsafe {
            *out = service_stats_from_core(stats);
        }
        AORTA_OK
    })
}

/// Reply to a service request.
///
/// The request handle is **consumed** by this call regardless of the return
/// value:
/// - On `AORTA_OK`, the reply has been queued for transmission and the
///   handle has been removed from the in-memory registry.
/// - On any error (`AORTA_ERR_NOT_FOUND`, `AORTA_ERR_ALREADY_REPLIED`,
///   payload-conversion errors, etc.), the handle has either been removed
///   from the registry as part of the error path or was never valid.
///
/// In all cases the caller MUST NOT call `aorta_service_request_release` on
/// the same handle afterwards. The C++ SDK `ServiceRequest` enforces this
/// in both directions: `Reply()` zeroes its handle once the FFI returns, so
/// the destructor's defensive `Release()` becomes a silent no-op (see the
/// `ServiceRequest::Release` documentation).
///
/// # Safety
/// - `request_handle` must be a valid `aorta_service_request_t` handle
/// - `response` may be null to reply with an empty payload
#[no_mangle]
pub unsafe extern "C" fn aorta_service_request_reply(
    request_handle: aorta_service_request_t,
    response: *const aorta_owned_bytes_view_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` is required because the
    // closure is safe; `response` may be null (handled explicitly) and is
    // otherwise dereffed via `zbytes_from_owned_ffi_view` under the caller's
    // pointer-validity contract. `request_handle` is just a `u64` — its
    // validity is checked by the registry lookup.
    ffi_guard(|| unsafe {
        // Consume the registry entry BEFORE parsing the response so the
        // doc contract ("handle consumed on every outcome") holds even
        // when the response view is malformed. A malformed payload is a
        // programmer error, not a transient condition — failing the
        // request and dropping the handle is safer than leaving a
        // dangling in-flight entry that nothing will ever clean up.
        let request_arc = registry().service_requests.lock().remove(&request_handle);

        let Some(request_arc) = request_arc else {
            return map_aorta_error(AortaError::not_found("request handle not found"));
        };

        let request = request_arc.lock().take();
        let Some(request) = request else {
            return map_aorta_error(AortaError::already_replied("request already replied"));
        };

        let response_payload = if response.is_null() {
            crate::ZBytes::new()
        } else {
            match zbytes_from_owned_ffi_view(response, "response") {
                Ok(payload) => payload,
                Err(err) => return map_aorta_error(err),
            }
        };

        let payload_len = response_payload.len();
        aorta_core_debug!(
            "ffi service reply queued: request_handle={}, payload_len={}",
            request_handle,
            payload_len
        );
        runtime().spawn(async move {
            aorta_core_debug!(
                "ffi service reply send starting: request_handle={}, payload_len={}",
                request_handle,
                payload_len
            );
            if let Err(err) = request.reply(response_payload).await {
                aorta_core_warn!(
                    "ffi service reply failed: request_handle={}, payload_len={}, error={}",
                    request_handle,
                    payload_len,
                    err
                );
            } else {
                aorta_core_debug!(
                    "ffi service reply send completed: request_handle={}, payload_len={}",
                    request_handle,
                    payload_len
                );
            }
        });

        AORTA_OK
    })
}

/// Reply to a service request with an error (a Zenoh query *error* reply,
/// distinct from the OK-payload `aorta_service_request_reply`).
///
/// The request handle is **consumed** by this call regardless of the return
/// value, exactly like `aorta_service_request_reply`: on every outcome the
/// caller MUST NOT call `aorta_service_request_release` on the same handle
/// afterwards.
///
/// Backs the Rust SDK `ServiceRequest::reply_error` / `TypedServiceResponder::
/// reply_error` (`rust/src/service.rs`, `rust/src/typed_service.rs`) — these
/// had no C-ABI bridge before, so the Python SDK could not surface a real error
/// reply (only an empty OK payload). Added for the Python SDK RPC binding.
///
/// # Safety
/// - `request_handle` must be a valid `aorta_service_request_t` handle
/// - `error_msg` must be a valid NUL-terminated UTF-8 string, or null (treated
///   as an empty error message)
#[no_mangle]
pub unsafe extern "C" fn aorta_service_request_reply_error(
    request_handle: aorta_service_request_t,
    error_msg: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-provided `error_msg` (null handled explicitly).
    // `request_handle` is a `u64` whose validity is checked by the registry
    // lookup. Mirrors `aorta_service_request_reply`: consume the registry
    // entry BEFORE parsing `error_msg` so the "handle consumed on every
    // outcome" contract holds even for a malformed string.
    ffi_guard(|| unsafe {
        let request_arc = registry().service_requests.lock().remove(&request_handle);

        let Some(request_arc) = request_arc else {
            return map_aorta_error(AortaError::not_found("request handle not found"));
        };

        let request = request_arc.lock().take();
        let Some(request) = request else {
            return map_aorta_error(AortaError::already_replied("request already replied"));
        };

        let error_msg = if error_msg.is_null() {
            String::new()
        } else {
            match CStr::from_ptr(error_msg).to_str() {
                Ok(s) => s.to_string(),
                Err(_) => {
                    return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in error_msg")
                }
            }
        };

        aorta_core_debug!(
            "ffi service error reply queued: request_handle={}, error={}",
            request_handle,
            error_msg
        );
        runtime().spawn(async move {
            if let Err(err) = request.reply_error(&error_msg).await {
                aorta_core_warn!(
                    "ffi service error reply failed: request_handle={}, error={}",
                    request_handle,
                    err
                );
            }
        });

        AORTA_OK
    })
}

/// Release a service request handle without replying
///
/// This is used for cleanup when no reply is needed (e.g., error cases).
/// It is safe to call multiple times with the same handle (idempotent).
#[no_mangle]
pub extern "C" fn aorta_service_request_release(
    request_handle: aorta_service_request_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if request_handle == 0 {
            return AORTA_OK;
        }

        let removed = registry().service_requests.lock().remove(&request_handle);

        aorta_core_debug!(
            "ffi service request released without reply: request_handle={}, had_pending_request={}",
            request_handle,
            removed.is_some()
        );

        AORTA_OK
    })
}

/// Read the trace context identifying an in-flight request: the 16-byte trace
/// id plus the span id to parent deferred work under (the per-request server
/// root when one was minted, else the caller's propagated span). Returns `1`
/// and fills the outputs when the request carries a context, else `0` (outputs
/// untouched — also for an unknown/already-replied handle, or when tracing was
/// disabled at receipt).
///
/// Does NOT consume the handle. Designed to feed `aorta_trace_set_context` on
/// a worker thread (the deferred-reply pattern), so spans recorded there stay
/// attached to this request's trace.
///
/// # Safety
/// - `request_handle` must be a valid `aorta_service_request_t` handle (or
///   stale — handled as not-found)
/// - when non-NULL, `out_trace_id` must point to ≥16 writable bytes and
///   `out_span_id` to a writable `u64` for the duration of the call
#[no_mangle]
pub unsafe extern "C" fn aorta_service_request_trace_context(
    request_handle: aorta_service_request_t,
    out_trace_id: *mut u8,
    out_span_id: *mut u64,
) -> u8 {
    ffi_guard_value(0, || {
        let entry = registry()
            .service_requests
            .lock()
            .get(&request_handle)
            .cloned();
        let Some(entry) = entry else {
            return 0;
        };
        let ctx = entry.lock().as_ref().and_then(|req| req.trace_context());
        let Some(ctx) = ctx else {
            return 0;
        };
        if !out_trace_id.is_null() {
            // SAFETY: caller guarantees ≥16 writable bytes at `out_trace_id`.
            unsafe { ptr::copy_nonoverlapping(ctx.trace_id.as_ptr(), out_trace_id, 16) };
        }
        if !out_span_id.is_null() {
            // SAFETY: caller guarantees a writable u64 at `out_span_id`;
            // `write_unaligned` tolerates a non-naturally-aligned pointer
            // handed across the C ABI.
            unsafe { out_span_id.write_unaligned(ctx.span_id) };
        }
        1
    })
}
