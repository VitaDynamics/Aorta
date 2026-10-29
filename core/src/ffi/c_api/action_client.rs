// Action Client FFI surface.
//
// Extracted from c_api/mod.rs. Pure code-move;
// every `pub extern "C" fn` keeps its `#[no_mangle]` symbol and the
// parent module re-exports `action_client::*`.

use super::*;

/// Create a new action client.
///
/// # Safety
/// - `node` must be a valid aorta_node_t handle
/// - `action_name` must be a valid null-terminated UTF-8 string
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_new_with_options(
    node: aorta_node_t,
    action_name: *const c_char,
    opts: *const aorta_action_client_options_t,
    out_client: *mut aorta_action_client_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*response = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if action_name.is_null() || out_client.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }

        let action_name = match CStr::from_ptr(action_name).to_str() {
            Ok(s) => s.trim().to_string(),
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in action name"),
        };
        if action_name.is_empty() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "action name is empty");
        }

        let node_obj = { registry().nodes.lock().get(&node).cloned() };

        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        let (control_timeout, info_timeout) = match action_client_config_from_ffi(opts) {
            Ok(v) => v,
            Err(err) => return map_aorta_error(err),
        };

        let handle = alloc_handle();
        registry().action_clients.lock().insert(
            handle,
            Arc::new(FfiActionClient {
                session: node_obj.session().clone(),
                key_builder: node_obj.key_builder().clone(),
                action_name,
                control_timeout,
                info_timeout,
                event_sender_cell: node_obj.event_sender_cell(),
            }),
        );

        *out_client = handle;
        AORTA_OK
    })
}

/// Send a raw action send_goal request payload and return the raw response payload.
///
/// Tracing: when the calling thread has an ACTIVE trace context (an open span or
/// an installed inbound context), it rides the send_goal query attachment as
/// the traceparent, so the server's goal lifecycle spans parent to that
/// already-recorded span. No context → no attachment (this legacy ABI cannot
/// record a span above the FFI, so nothing is minted — use
/// [`aorta_action_client_send_goal_traced`] for the full client-side stage).
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `request` and `response` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_send_goal(
    client: aorta_action_client_t,
    request: *const aorta_bytes_view_t,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `action_client_send_goal_impl`; forwards
    // the caller's pointer contracts, discards the minted span identity.
    ffi_guard(|| unsafe {
        action_client_send_goal_impl(
            client,
            request,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            response,
        )
    })
}

/// Like [`aorta_action_client_send_goal`], additionally returning the minted
/// `goal_sent_to_accepted` span identity (the parent the server's goal root
/// joins): writes the 16-byte trace id to `out_trace_id`, the span id to
/// `out_span_id`, and the span's own parent (the caller's enclosing span, or
/// `0`) to `out_parent_span_id` when tracing was enabled at send (outputs
/// zeroed otherwise — `*out_span_id == 0` means "no trace"). The SDK records
/// the span itself after decoding ACCEPT via `aorta_trace_record_stage_at`
/// (the FFI cannot see accept/reject in the opaque response payload), and
/// parents `result_wait` / deferred work to it.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `request` and `response` must be valid pointers
/// - when non-NULL, `out_trace_id` must point to ≥16 writable bytes and
///   `out_span_id` / `out_parent_span_id` to writable `u64`s for the call
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_send_goal_traced(
    client: aorta_action_client_t,
    request: *const aorta_bytes_view_t,
    out_trace_id: *mut u8,
    out_span_id: *mut u64,
    out_parent_span_id: *mut u64,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `action_client_send_goal_impl`; forwards
    // the caller's pointer contracts.
    ffi_guard(|| unsafe {
        action_client_send_goal_impl(
            client,
            request,
            out_trace_id,
            out_span_id,
            out_parent_span_id,
            response,
        )
    })
}

/// Shared send_goal implementation. MUST run on the FFI caller's thread up to
/// the `ffi_block_on!` (the outbound link joins the caller's thread-local
/// trace context / open spans).
///
/// # Safety
/// Caller contracts of [`aorta_action_client_send_goal_traced`].
unsafe fn action_client_send_goal_impl(
    client: aorta_action_client_t,
    request: *const aorta_bytes_view_t,
    out_trace_id: *mut u8,
    out_span_id: *mut u64,
    out_parent_span_id: *mut u64,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // pointer-view derefs and `*response = …` / out-param writebacks below;
    // each is null-checked under the FFI caller's pointer-validity contract.
    unsafe {
        if client == 0 || request.is_null() || response.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        *response = aorta_owned_bytes_t::default();
        if !out_trace_id.is_null() {
            ptr::write_bytes(out_trace_id, 0, 16);
        }
        if !out_span_id.is_null() {
            out_span_id.write_unaligned(0);
        }
        if !out_parent_span_id.is_null() {
            out_parent_span_id.write_unaligned(0);
        }

        let request_payload = match zbytes_from_ffi_view(request, "request") {
            Ok(payload) => payload,
            Err(err) => return map_aorta_error(err),
        };
        if request_payload.is_empty() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty request");
        }

        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(client) => Arc::clone(client),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };

        let key = match client_arc.send_goal_key() {
            Ok(key) => key,
            Err(err) => return map_aorta_error(err),
        };

        // Traceparent for the query, ON THE CALLER'S THREAD (joins an open
        // SDK span or installed context). Two regimes:
        // - the _traced ABI gets a PRE-MINTED goal_sent identity back and
        //   records it on ACCEPT — the server parents to that span;
        // - the legacy ABI cannot record anything above the FFI, so it must
        //   NOT mint: it attaches the thread's CURRENT context (an
        //   already-recorded enclosing span), or nothing when no trace is
        //   active — never a dangling never-emitted id.
        let wants_link = !out_trace_id.is_null() || !out_span_id.is_null();
        let link = if !crate::trace::trace_enabled() {
            None
        } else if wants_link {
            crate::trace::outbound_link()
        } else {
            crate::trace::current_outbound_context().map(|(trace_id, span_id)| {
                crate::trace::SpanLink {
                    trace_id,
                    span_id,
                    parent_span_id: 0,
                }
            })
        };
        let request_attachment = link.and_then(|l| {
            crate::attachment::encode(&crate::attachment::AttachmentParts {
                schema_hash: None,
                traceparent: Some(crate::trace::TraceParent {
                    trace_id: l.trace_id,
                    span_id: l.span_id,
                    flags: crate::trace::TRACEPARENT_SAMPLED,
                }),
                reply_busy_ns: None,
                deadline_budget_ns: None,
            })
        });

        let result = match crate::ffi_block_on!(async {
            client_arc
                .query_optional_bytes_attached(
                    &key,
                    Some(request_payload),
                    request_attachment,
                    client_arc.control_timeout,
                )
                .await?
                .ok_or_else(|| {
                    AortaError::timeout(format!(
                        "action '{}' query '{}' timed out after {:?}",
                        client_arc.action_name, key, client_arc.control_timeout
                    ))
                })
        }) {
            Ok(r) => r,
            Err(_) => return aorta_reentrant(module_path!()),
        };

        match result {
            Ok(payload) => {
                if let Some(l) = link {
                    if !out_trace_id.is_null() {
                        ptr::copy_nonoverlapping(l.trace_id.as_ptr(), out_trace_id, 16);
                    }
                    if !out_span_id.is_null() {
                        out_span_id.write_unaligned(l.span_id);
                    }
                    if !out_parent_span_id.is_null() {
                        out_parent_span_id.write_unaligned(l.parent_span_id);
                    }
                }
                *response = owned_bytes_from_zbytes(payload);
                AORTA_OK
            }
            Err(err) => map_aorta_error(err),
        }
    }
}

/// Query the public status record for a goal.
///
/// Returns `AORTA_ERR_NOT_FOUND` when the status record does not exist.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `goal_id` and `response` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_query_status(
    client: aorta_action_client_t,
    goal_id: *const c_char,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `action_client_query_status_impl`; forwards
    // the caller's pointers unchanged under that fn's own `# Safety` contract.
    ffi_guard(|| unsafe { action_client_query_status_impl(client, goal_id, None, response) })
}

unsafe fn action_client_query_status_impl(
    client: aorta_action_client_t,
    goal_id: *const c_char,
    timeout_ms_override: Option<u64>,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    if client == 0 || goal_id.is_null() || response.is_null() {
        return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
    }
    *response = aorta_owned_bytes_t::default();

    let goal_id = match CStr::from_ptr(goal_id).to_str() {
        Ok(s) => s.trim().to_string(),
        Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in goal_id"),
    };

    let client_arc = {
        let clients = registry().action_clients.lock();
        match clients.get(&client) {
            Some(client) => Arc::clone(client),
            None => return map_aorta_error(AortaError::not_found("action client not found")),
        }
    };

    let key = match client_arc.status_key(&goal_id) {
        Ok(key) => key,
        Err(err) => return map_aorta_error(err),
    };
    let timeout_duration = timeout_ms_override
        .map(Duration::from_millis)
        .unwrap_or(client_arc.info_timeout);
    let result = match crate::ffi_block_on!(
        client_arc.query_optional_snapshot_bytes(&key, timeout_duration)
    ) {
        Ok(r) => r,
        Err(_) => return aorta_reentrant(module_path!()),
    };

    match result {
        Ok(Some(payload)) => {
            *response = owned_bytes_from_zbytes(payload);
            AORTA_OK
        }
        Ok(None) => aorta_err(
            AORTA_ERR_NOT_FOUND,
            format!(
                "public action status not found for '{}' goal '{}'",
                client_arc.action_name, goal_id
            ),
        ),
        Err(err) => map_aorta_error(err),
    }
}

/// Query the public status record for a goal with an explicit timeout override.
///
/// Returns `AORTA_ERR_NOT_FOUND` when the status record does not exist.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `goal_id` and `response` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_query_status_with_timeout(
    client: aorta_action_client_t,
    goal_id: *const c_char,
    timeout_ms: u64,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `action_client_query_status_impl`; forwards
    // the caller's pointers unchanged under that fn's own `# Safety` contract.
    ffi_guard(|| unsafe {
        action_client_query_status_impl(client, goal_id, Some(timeout_ms), response)
    })
}

/// Send a raw cancel request payload and return the raw response payload.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `request` and `response` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_cancel(
    client: aorta_action_client_t,
    request: *const aorta_bytes_view_t,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*response = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if client == 0 || request.is_null() || response.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        *response = aorta_owned_bytes_t::default();

        let request_payload = match zbytes_from_ffi_view(request, "request") {
            Ok(payload) => payload,
            Err(err) => return map_aorta_error(err),
        };
        if request_payload.is_empty() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty request");
        }

        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(client) => Arc::clone(client),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };

        let key = match client_arc.cancel_goal_key() {
            Ok(key) => key,
            Err(err) => return map_aorta_error(err),
        };
        let result = match crate::ffi_block_on!(client_arc.query_bytes(
            &key,
            Some(request_payload),
            client_arc.control_timeout,
        )) {
            Ok(r) => r,
            Err(_) => return aorta_reentrant(module_path!()),
        };

        match result {
            Ok(payload) => {
                *response = owned_bytes_from_zbytes(payload);
                AORTA_OK
            }
            Err(err) => map_aorta_error(err),
        }
    })
}

/// Send a raw get_result request payload and return the raw response payload.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `request` and `response` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_get_result(
    client: aorta_action_client_t,
    request: *const aorta_bytes_view_t,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `action_client_get_result_impl`; forwards
    // the caller's pointers unchanged under that fn's own `# Safety` contract.
    ffi_guard(|| unsafe { action_client_get_result_impl(client, request, None, response) })
}

unsafe fn action_client_get_result_impl(
    client: aorta_action_client_t,
    request: *const aorta_bytes_view_t,
    timeout_ms_override: Option<u64>,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    if client == 0 || request.is_null() || response.is_null() {
        return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
    }
    *response = aorta_owned_bytes_t::default();

    let request_payload = match zbytes_from_ffi_view(request, "request") {
        Ok(payload) => payload,
        Err(err) => return map_aorta_error(err),
    };
    if request_payload.is_empty() {
        return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty request");
    }

    let client_arc = {
        let clients = registry().action_clients.lock();
        match clients.get(&client) {
            Some(client) => Arc::clone(client),
            None => return map_aorta_error(AortaError::not_found("action client not found")),
        }
    };

    let key = match client_arc.get_result_key() {
        Ok(key) => key,
        Err(err) => return map_aorta_error(err),
    };
    let timeout_duration = timeout_ms_override
        .map(Duration::from_millis)
        .unwrap_or(client_arc.control_timeout);
    let result = match crate::ffi_block_on!(client_arc.query_bytes(
        &key,
        Some(request_payload),
        timeout_duration
    )) {
        Ok(r) => r,
        Err(_) => return aorta_reentrant(module_path!()),
    };

    match result {
        Ok(payload) => {
            *response = owned_bytes_from_zbytes(payload);
            AORTA_OK
        }
        Err(err) => map_aorta_error(err),
    }
}

/// Send a raw get_result request payload and return the raw response payload,
/// using an explicit timeout override.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `request` and `response` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_get_result_with_timeout(
    client: aorta_action_client_t,
    request: *const aorta_bytes_view_t,
    timeout_ms: u64,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `action_client_get_result_impl`; forwards
    // the caller's pointers unchanged under that fn's own `# Safety` contract.
    ffi_guard(|| unsafe {
        action_client_get_result_impl(client, request, Some(timeout_ms), response)
    })
}

/// Query action info for the bound action name.
///
/// Returns `AORTA_ERR_NOT_FOUND` when no action server is registered.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `response` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_info(
    client: aorta_action_client_t,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*response = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if client == 0 || response.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        *response = aorta_owned_bytes_t::default();

        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(client) => Arc::clone(client),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };

        let result = match crate::ffi_block_on!(fetch_action_info(
            &client_arc.session,
            &client_arc.key_builder,
            &client_arc.action_name,
            client_arc.info_timeout,
        )) {
            Ok(r) => r,
            Err(_) => return aorta_reentrant(module_path!()),
        };

        match result {
            Ok(Some(info)) => match info.encode() {
                Ok(payload) => {
                    *response = owned_bytes_from_zbytes(crate::ZBytes::from(payload));
                    AORTA_OK
                }
                Err(err) => map_aorta_error(err),
            },
            Ok(None) => aorta_err(
                AORTA_ERR_NOT_FOUND,
                format!("action '{}' info not found", client_arc.action_name),
            ),
            Err(err) => map_aorta_error(err),
        }
    })
}

/// Free an action client handle.
#[no_mangle]
pub extern "C" fn aorta_action_client_free(client: aorta_action_client_t) -> aorta_result_t {
    ffi_guard(|| {
        if client == 0 {
            return AORTA_OK;
        }

        registry().action_clients.lock().remove(&client);

        AORTA_OK
    })
}

/// Subscribe to raw action feedback payloads for the bound action.
///
/// The callback receives the protocol wrapper payload for the action feedback
/// topic. Goal-specific filtering is left to the caller.
///
/// # Safety
/// - `client` must be a valid action client handle
/// - `cb` must be a valid callback
/// - `out_sub` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_subscribe_feedback(
    client: aorta_action_client_t,
    cb: aorta_on_msg_cb,
    user_data: *mut c_void,
    out_sub: *mut aorta_action_feedback_sub_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the
    // CStr / pointer-view derefs and `*response = …` writebacks below;
    // each is null-checked at the top of the closure under the FFI
    // caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if client == 0 || out_sub.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        if cb.is_none() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
        }

        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(client) => Arc::clone(client),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };

        let key = match client_arc.feedback_key() {
            Ok(key) => key,
            Err(err) => return map_aorta_error(err),
        };
        let cb = cb.unwrap();
        let topic = key.clone();
        let user_data = user_data as usize;
        // Interned once; the receive marker parents to the SERVER's
        // feedback_pub span when its traceparent rides the attachment.
        let ev_feedback_recv =
            crate::trace::marker_event_for("action.feedback_recv", &client_arc.action_name);
        // Subscriber teardown-gate twin for the action feedback path: free drains this before
        // returning, so the caller's callback state cannot be freed while a
        // late delivery is still inside this closure.
        let in_flight = Arc::new(AtomicUsize::new(0));
        let in_flight_cb = Arc::clone(&in_flight);

        let subscriber = match crate::ffi_block_on!(async {
            client_arc
                .session
                .inner()
                .declare_subscriber(key)
                .callback(move |sample| {
                    // Bracket the WHOLE body (marker emission + `cb`) with the
                    // in-flight gate; the guard decrements on every exit.
                    in_flight_cb.fetch_add(1, Ordering::AcqRel);
                    let _drain_guard = InFlightGuard(&in_flight_cb);
                    // Explicit reentrancy detector: this body
                    // runs on the Zenoh RX thread — blocking FFI re-entry
                    // from the user callback must classify reentrant.
                    let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
                    let fields: zenoh::sample::SampleFields = sample.into();
                    if fields.payload.is_empty() {
                        return;
                    }

                    // Per-feedback receive marker, parented to the
                    // publishing side's pre-minted feedback_pub span — pub →
                    // recv renders as the per-message transit hop. Legacy
                    // publishers (no traceparent on the attachment) emit no
                    // marker here (the FFI stream is not goal-scoped, so
                    // there is no goal trace to fall back to).
                    if crate::trace::trace_enabled() {
                        let wire_parent = fields.attachment.as_ref().and_then(|attachment| {
                            crate::attachment::decode(attachment.to_bytes().as_ref()).traceparent
                        });
                        if let Some(tp) = wire_parent {
                            let now = crate::time::monotonic_ns();
                            crate::trace::emit_stage_span_under(
                                ev_feedback_recv,
                                now,
                                now,
                                &crate::trace::SpanLink {
                                    trace_id: tp.trace_id,
                                    span_id: tp.span_id,
                                    parent_span_id: 0,
                                },
                            );
                        }
                    }

                    let mut single_slice = aorta_bytes_slice_t::default();
                    let mut payload_slices = Vec::new();
                    let payload_view = bytes_view_from_zbytes_borrowed(
                        &fields.payload,
                        &mut single_slice,
                        &mut payload_slices,
                    );
                    let schema_view = aorta_schema_hash_view_t {
                        hex: ptr::null(),
                        len: 0,
                    };
                    let msg_ctx = aorta_msg_context_t {
                        topic: string_view_from_str(&topic),
                        receive_stamp_ns: 0,
                        priority: 0,
                        publisher_node: aorta_string_view_t::default(),
                        sequence: 0,
                    };
                    let user_data = user_data as *mut c_void;
                    // Action feedback dispatch — wrap so a Rust-side panic
                    // in OUR marshaling code (bytes_view_from_zbytes_borrowed,
                    // string_view_from_str, the schema_view + msg_ctx setup
                    // above) cannot unwind into the action client's feedback
                    // subscription task. NOTE: this does NOT catch panics
                    // that originate INSIDE the user's `cb` — Rust ≥ 1.81
                    // marks extern "C" as nounwind, so a panic in `cb`
                    // aborts before our catch_unwind sees it. The C++ SDK's
                    // try/catch is what protects C++ users.
                    ffi_guard_void(|| {
                        cb(
                            ptr::addr_of!(payload_view),
                            ptr::addr_of!(schema_view),
                            ptr::addr_of!(msg_ctx),
                            user_data,
                        );
                    });
                })
                .await
        }) {
            Ok(Ok(subscriber)) => subscriber,
            Ok(Err(err)) => return map_aorta_error(AortaError::internal(err.to_string())),
            Err(_) => return aorta_reentrant(module_path!()),
        };

        let handle = alloc_handle();
        registry().action_feedback_subscribers.lock().insert(
            handle,
            FfiActionFeedbackSubEntry {
                subscriber,
                in_flight,
            },
        );
        *out_sub = handle;
        AORTA_OK
    })
}

/// Free an action feedback subscription handle.
#[no_mangle]
pub extern "C" fn aorta_action_feedback_sub_free(
    sub_handle: aorta_action_feedback_sub_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if sub_handle == 0 {
            return AORTA_OK;
        }

        let entry = {
            registry()
                .action_feedback_subscribers
                .lock()
                .remove(&sub_handle)
        };

        let Some(FfiActionFeedbackSubEntry {
            subscriber,
            in_flight,
        }) = entry
        else {
            return map_aorta_error(AortaError::not_found(
                "action feedback subscriber handle not found",
            ));
        };

        // FREE path: spawn-and-detach on reentrant invocation (see
        // `aorta_node_free`); no drain here — blocking inside a callback
        // context would deadlock (same caveat as `aorta_subscriber_free`).
        if in_async_context() {
            runtime().spawn(async move {
                let _ = subscriber.undeclare().await;
            });
            return AORTA_OK;
        }

        let result = match runtime().block_on(async { subscriber.undeclare().await }) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(AortaError::internal(err.to_string())),
        };
        // `undeclare()` stops NEW dispatches, but a late delivery may still be
        // running the callback — wait for it before returning so the caller
        // can free its callback state without a use-after-free (the
        // subscriber-teardown contract, applied to the action feedback path).
        if !drain_in_flight(&in_flight, FREE_DRAIN_TIMEOUT) {
            aorta_core_warn!(
                "aorta_action_feedback_sub_free: timed out draining in-flight callbacks"
            );
        }
        result
    })
}

// ===========================================================================
// Event-driven action result notification.
// `aorta_action_client_subscribe_result` hosts the result WATCHER on
// the shared FFI runtime (no node-executor coupling): it subscribes to the
// goal's status topic and, on a terminal status, fetches get_result ONCE and
// hands the raw GetResultResponse bytes to the C callback. Because the FFI is
// type-erased, terminal detection uses a pure C-provided decode hook.
//
// Delivery: the terminal outcome becomes a
// `CoroutineResume` event on the node executor's reserved lane when the client's
// node is attached to a spinning executor — the executor dispatches the C
// callback like any other callback. With no executor attached, the callback
// runs inline on the watcher task (a shared-runtime worker). The former
// PER-SUBSCRIPTION off-runtime delivery thread is deleted: callbacks now run
// on real runtime/executor workers, so a BLOCKING re-entry
// into aorta FFI from the result callback returns AORTA_ERR_REENTRANT like
// every other callback — waiting on another goal from inside a
// callback must use the non-blocking subscribe/cancel surfaces.
// ===========================================================================

/// Status-record kinds passed to the decode hook.
pub const AORTA_ACTION_STATUS_KIND_PUBLIC_STATUS: u8 = 0;
pub const AORTA_ACTION_STATUS_KIND_GET_RESULT: u8 = 1;

/// Pure decode hook supplied by the typed binding: given a status record of
/// `kind` it returns the `ActionStatus` as i8, or a negative value on decode
/// error. `user_data` is the same opaque pointer passed to
/// `aorta_action_client_subscribe_result` (so one type-erased trampoline can
/// serve all action types). MUST be pure — it MUST NOT call back into aorta FFI.
pub type aorta_action_status_decode_fn = Option<
    unsafe extern "C" fn(kind: u8, bytes: *const u8, len: usize, user_data: *mut c_void) -> i8,
>;

/// Terminal-result payload handed to `aorta_action_result_cb`. All views are
/// BORROWED and valid ONLY for the duration of the callback.
#[repr(C)]
pub struct aorta_action_result_view_t {
    /// `AORTA_OK` → a terminal arrived and `result_payload` holds the raw
    /// GetResultResponse bytes. Otherwise the WAIT failed (timeout/transport)
    /// and `message` carries the cause; `result_payload` is empty.
    pub error: aorta_result_t,
    pub result_payload: aorta_bytes_view_t,
    pub message: aorta_string_view_t,
}

pub type aorta_action_result_cb =
    Option<unsafe extern "C" fn(ev: *const aorta_action_result_view_t, ctx: *mut c_void)>;

pub type aorta_action_result_sub_t = u64;

thread_local! {
    /// The result subscription handle whose terminal callback is CURRENTLY
    /// running on this (per-subscription) delivery thread, or 0 when none. Lets
    /// `aorta_action_result_sub_free` detect a reentrant free — the C callback
    /// dropping *its own* handle — and detach instead of self-joining, while a
    /// callback that drops a DIFFERENT subscription's handle still joins normally
    /// (no spurious leak). Per-subscription identity (not a global bool) is what
    /// avoids leaking unrelated subscriptions closed from inside a callback.
    static CURRENT_RESULT_DELIVERY: std::cell::Cell<aorta_action_result_sub_t> =
        const { std::cell::Cell::new(0) };
}

/// Raw watcher outcome handed to the delivery step: terminal GetResultResponse
/// bytes, or a (code, message) wait failure.
type ActionResultOutcome = Result<crate::ZBytes, (aorta_result_t, String)>;

/// Invoke the terminal-result C callback exactly once, honoring the free-race
/// protocol: skip when `cancelled` (the caller may already have freed its
/// callback state), account the invocation in `in_flight` so `free` can drain
/// it, and stamp the delivery-identity thread-local so a reentrant free from
/// inside the callback is detected on whichever thread runs the delivery
/// (an executor dispatch thread or a shared-runtime worker).
fn deliver_action_result_once(
    outcome: ActionResultOutcome,
    cb: unsafe extern "C" fn(ev: *const aorta_action_result_view_t, ctx: *mut c_void),
    user_data: usize,
    handle: aorta_action_result_sub_t,
    cancelled: &std::sync::atomic::AtomicBool,
    in_flight: &AtomicUsize,
) {
    if cancelled.load(Ordering::SeqCst) {
        return;
    }
    in_flight.fetch_add(1, Ordering::AcqRel);
    let _drain_guard = InFlightGuard(in_flight);
    // Re-check after taking the in-flight gate: if free won the race it set
    // `cancelled` before reading in_flight, so we must not touch the
    // (possibly freed) C callback state.
    if cancelled.load(Ordering::SeqCst) {
        return;
    }
    // Explicit reentrancy detector: covers the inline
    // watcher-task delivery (the executor-lane path is already marked at
    // Event::dispatch_isolated; nesting the guard is idempotent).
    let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
    CURRENT_RESULT_DELIVERY.with(|f| f.set(handle));
    let user_data = user_data as *mut c_void;
    match outcome {
        Ok(payload) => {
            let mut single = aorta_bytes_slice_t::default();
            let mut slices = Vec::new();
            let view = aorta_action_result_view_t {
                error: AORTA_OK,
                result_payload: bytes_view_from_zbytes_borrowed(&payload, &mut single, &mut slices),
                message: aorta_string_view_t::default(),
            };
            ffi_guard_void(|| {
                // SAFETY: `cb` is the non-null C callback registered by
                // `subscribe_result`; the view and its borrowed payload slices
                // are valid for the duration of this call.
                unsafe { cb(ptr::addr_of!(view), user_data) };
            });
        }
        Err((code, msg)) => {
            let view = aorta_action_result_view_t {
                error: code,
                result_payload: aorta_bytes_view_t::default(),
                message: string_view_from_str(&msg),
            };
            ffi_guard_void(|| {
                // SAFETY: as above; the message view borrows `msg`, which
                // outlives the call.
                unsafe { cb(ptr::addr_of!(view), user_data) };
            });
        }
    }
    CURRENT_RESULT_DELIVERY.with(|f| f.set(0));
}

fn action_result_timeout_msg(goal_id: &str) -> String {
    format!("timed out waiting for terminal action result for goal '{goal_id}'")
}

fn action_status_closed_msg(goal_id: &str) -> String {
    format!("action status subscriber closed for goal '{goal_id}'")
}

/// Returns `Some(is_terminal)` for a decodable status, or `None` when the decode
/// hook reports an error (negative i8) — callers treat `None` as a hard failure
/// rather than silently looping (which would hang until timeout on a corrupt
/// status), mirroring the reference `wait_result_inner` which propagates decode
/// errors.
fn decode_status_terminal(
    decode: unsafe extern "C" fn(u8, *const u8, usize, *mut c_void) -> i8,
    kind: u8,
    bytes: &[u8],
    user_data: usize,
) -> Option<bool> {
    // SAFETY: `decode` is the non-null C decode hook validated by
    // `subscribe_result`; per its contract it is pure and must not re-enter aorta
    // FFI. `bytes` is a valid slice (valid ptr + len), and `user_data` is the
    // opaque pointer the caller registered with the subscription.
    let st = unsafe { decode(kind, bytes.as_ptr(), bytes.len(), user_data as *mut c_void) };
    if st < 0 {
        return None;
    }
    Some(
        crate::action::ActionStatus::try_from(st)
            .map(|s| s.is_terminal())
            .unwrap_or(false),
    )
}

fn action_status_decode_err_msg(goal_id: &str) -> String {
    format!("failed to decode action status for goal '{goal_id}'")
}

async fn get_result_once(
    client: &Arc<FfiActionClient>,
    key: &str,
    request: &[u8],
    deadline: Option<std::time::Instant>,
    goal_id: &str,
) -> Result<crate::ZBytes, (aorta_result_t, String)> {
    // After a terminal status the result fetch gets the caller's FULL remaining
    // budget — NOT a `control_timeout`-capped slice — so a long `WaitResult(30s)`
    // does not fail early at the default control timeout. With no deadline it
    // falls back to `control_timeout`.
    let to = match deadline {
        Some(d) => {
            let now = std::time::Instant::now();
            if now >= d {
                return Err((AORTA_ERR_TIMEOUT, action_result_timeout_msg(goal_id)));
            }
            d - now
        }
        None => client.control_timeout,
    };
    client
        .query_bytes(key, Some(crate::ZBytes::from(request.to_vec())), to)
        .await
        // Preserve the AortaError's semantic code (Timeout / NotFound /
        // Unauthorized→InvalidArgument / ...) so the callback sees the real
        // failure rather than a flattened INTERNAL, and so the status-absent
        // probe below can distinguish a transient timeout from a definitive
        // negative answer.
        .map_err(|e| (super::aorta_result_code_of(&e), e.to_string()))
}

/// The type-erased result watcher. Returns the raw GetResultResponse bytes on a
/// terminal status, or an (error-code, message) wait failure.
async fn run_action_result_watcher(
    client: Arc<FfiActionClient>,
    goal_id: String,
    get_result_request: Vec<u8>,
    timeout_duration: Option<Duration>,
    decode_status: unsafe extern "C" fn(u8, *const u8, usize, *mut c_void) -> i8,
    decode_user_data: usize,
) -> Result<crate::ZBytes, (aorta_result_t, String)> {
    let status_key = client
        .status_key(&goal_id)
        .map_err(|e| (AORTA_ERR_INTERNAL, e.to_string()))?;
    let get_result_key = client
        .get_result_key()
        .map_err(|e| (AORTA_ERR_INTERNAL, e.to_string()))?;
    let deadline = timeout_duration.map(|d| std::time::Instant::now() + d);

    // Subscribe FIRST so the terminal sample cannot slip between the initial
    // check and the loop.
    let subscriber = client
        .session
        .inner()
        .declare_subscriber(status_key.clone())
        .await
        .map_err(|e| (AORTA_ERR_INTERNAL, e.to_string()))?;

    // Initial check against the current status snapshot.
    let info_to = match deadline {
        Some(d) => {
            let now = std::time::Instant::now();
            if now >= d {
                return Err((AORTA_ERR_TIMEOUT, action_result_timeout_msg(&goal_id)));
            }
            (d - now).min(client.info_timeout)
        }
        None => client.info_timeout,
    };
    match client
        .query_optional_snapshot_bytes(&status_key, info_to)
        .await
        .map_err(|e| (AORTA_ERR_INTERNAL, e.to_string()))?
    {
        Some(payload) => {
            match decode_status_terminal(
                decode_status,
                AORTA_ACTION_STATUS_KIND_PUBLIC_STATUS,
                payload.to_bytes().as_ref(),
                decode_user_data,
            ) {
                Some(true) => {
                    return get_result_once(
                        &client,
                        &get_result_key,
                        &get_result_request,
                        deadline,
                        &goal_id,
                    )
                    .await;
                }
                Some(false) => {}
                None => return Err((AORTA_ERR_INTERNAL, action_status_decode_err_msg(&goal_id))),
            }
        }
        None => {
            // Status record absent — the goal may already be terminal with its
            // status tombstoned but the result still retained, so probe
            // get_result. Crucially, do NOT blanket-swallow the probe outcome
            // (the old bug): a swallowed error or decode failure fell through to
            // the sample loop and then hung forever (no timeout) or false-timed
            // out, instead of surfacing expired/not-found/corrupt.
            match get_result_once(
                &client,
                &get_result_key,
                &get_result_request,
                deadline,
                &goal_id,
            )
            .await
            {
                Ok(gr) => match decode_status_terminal(
                    decode_status,
                    AORTA_ACTION_STATUS_KIND_GET_RESULT,
                    gr.to_bytes().as_ref(),
                    decode_user_data,
                ) {
                    Some(true) => return Ok(gr),
                    // Non-terminal: the goal is still running and simply has not
                    // published a status snapshot to this client yet — wait for a
                    // status sample.
                    Some(false) => {}
                    // Corrupt result bytes are never transient — fail fast.
                    None => {
                        return Err((AORTA_ERR_INTERNAL, action_status_decode_err_msg(&goal_id)))
                    }
                },
                // A transient TIMEOUT may just mean the status has not propagated
                // yet — fall through to the subscription loop (bounded by the
                // caller's deadline). Any other error is a DEFINITIVE negative
                // answer (not-found / expired / unauthorized / transport) — fail
                // fast rather than hang for a status sample that will never come.
                Err((AORTA_ERR_TIMEOUT, _)) => {}
                Err((code, msg)) => return Err((code, msg)),
            }
        }
    }

    // Event loop on status samples.
    loop {
        let sample = match deadline {
            Some(d) => {
                let now = std::time::Instant::now();
                if now >= d {
                    return Err((AORTA_ERR_TIMEOUT, action_result_timeout_msg(&goal_id)));
                }
                match tokio::time::timeout(d - now, subscriber.recv_async()).await {
                    Ok(Ok(s)) => s,
                    Ok(Err(_)) => {
                        return Err((AORTA_ERR_NOT_FOUND, action_status_closed_msg(&goal_id)))
                    }
                    Err(_) => return Err((AORTA_ERR_TIMEOUT, action_result_timeout_msg(&goal_id))),
                }
            }
            None => subscriber
                .recv_async()
                .await
                .map_err(|_| (AORTA_ERR_NOT_FOUND, action_status_closed_msg(&goal_id)))?,
        };
        let fields: zenoh::sample::SampleFields = sample.into();
        if fields.payload.is_empty() {
            // Empty payload = the status record was DELETED (zenoh tombstone):
            // the result is gone and no further samples will arrive, so fail
            // rather than hang forever (matches the reference wait_result_inner).
            return Err((
                AORTA_ERR_NOT_FOUND,
                format!("action status for goal '{goal_id}' was removed before result retrieval"),
            ));
        }
        match decode_status_terminal(
            decode_status,
            AORTA_ACTION_STATUS_KIND_PUBLIC_STATUS,
            fields.payload.to_bytes().as_ref(),
            decode_user_data,
        ) {
            Some(true) => {
                return get_result_once(
                    &client,
                    &get_result_key,
                    &get_result_request,
                    deadline,
                    &goal_id,
                )
                .await;
            }
            Some(false) => {}
            None => return Err((AORTA_ERR_INTERNAL, action_status_decode_err_msg(&goal_id))),
        }
    }
}

/// Subscribe for the terminal result of a goal (event-driven, non-blocking).
/// Fires `cb` exactly once with the terminal result OR a wait failure — on the
/// node executor's dispatch thread (via the reserved `CoroutineResume` lane) when
/// the node is attached to a spinning executor, otherwise on a shared-runtime
/// worker. `cb` must not make BLOCKING re-entrant aorta FFI calls (they return
/// `AORTA_ERR_REENTRANT`). `get_result_request`/`len` is the pre-encoded
/// (typed) GetResultRequest the watcher replays on terminal.
///
/// # Safety
/// - `client` is a valid action client handle
/// - `goal_id` is a valid null-terminated UTF-8 string
/// - `get_result_request` points to `get_result_request_len` valid bytes
/// - `decode_status`/`cb` are non-null; `out_sub` is a valid pointer
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_subscribe_result(
    client: aorta_action_client_t,
    goal_id: *const c_char,
    get_result_request: *const u8,
    get_result_request_len: usize,
    timeout_ms: u64,
    decode_status: aorta_action_status_decode_fn,
    cb: aorta_action_result_cb,
    user_data: *mut c_void,
    out_sub: *mut aorta_action_result_sub_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. The inner `unsafe` covers the CStr /
    // pointer-view derefs (`goal_id`, `get_result_request`) and the `*out_sub`
    // writeback below; each pointer is null-checked at the top of the closure
    // under the FFI caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if client == 0 || goal_id.is_null() || out_sub.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let (Some(cb), Some(decode_status)) = (cb, decode_status) else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback or decoder is null");
        };
        if get_result_request.is_null() || get_result_request_len == 0 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty get_result request");
        }
        let goal_id = match CStr::from_ptr(goal_id).to_str() {
            Ok(s) => s.trim().to_string(),
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in goal_id"),
        };
        let request =
            std::slice::from_raw_parts(get_result_request, get_result_request_len).to_vec();

        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };
        // `u64::MAX` is the no-timeout sentinel: every OTHER value — INCLUDING 0,
        // which means "expire immediately" — is a real deadline. (Encoding "no
        // timeout" as 0, as before, silently turned an explicit `WaitResult(0ms)`
        // into an unbounded wait.)
        let timeout_duration = (timeout_ms != u64::MAX).then(|| Duration::from_millis(timeout_ms));

        let handle = alloc_handle();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let user_data = user_data as usize;

        // Delivery: the outcome rides the node executor's
        // reserved `CoroutineResume` lane when one is attached — the lane bypasses
        // admission (the delivery is the continuation of already-admitted
        // work) and the executor dispatches the callback. With no executor the
        // callback runs inline on this watcher task. Exactly-once holds on
        // every path: the lane push is infallible except on a torn-down queue,
        // where the event is recovered and run inline (panic-isolated).
        let in_flight_d = Arc::clone(&in_flight);
        let cancelled_d = Arc::clone(&cancelled);
        let event_sender_cell = Arc::clone(&client_arc.event_sender_cell);
        let task = runtime().spawn(async move {
            let outcome = run_action_result_watcher(
                client_arc,
                goal_id,
                request,
                timeout_duration,
                decode_status,
                user_data,
            )
            .await;
            let deliver = move || {
                deliver_action_result_once(
                    outcome,
                    cb,
                    user_data,
                    handle,
                    &cancelled_d,
                    &in_flight_d,
                );
            };
            let lane_sender = event_sender_cell
                .read()
                .ok()
                .and_then(|guard| guard.as_ref().cloned())
                .filter(|sender| !sender.is_closed());
            match lane_sender {
                Some(sender) => {
                    let event = crate::event_queue::Event::new(
                        crate::event_queue::EventKind::CoroutineResume,
                        deliver,
                    );
                    if let Err(err) = sender.try_send(event) {
                        // The executor was stopped/torn down between the
                        // closed-check and the push: recover the event and
                        // deliver inline so the exactly-once contract holds.
                        // (`Full` is unreachable by construction — the lane
                        // has no capacity bound — the arm exists only to
                        // destructure the error type exhaustively.)
                        let (tokio::sync::mpsc::error::TrySendError::Closed(event)
                        | tokio::sync::mpsc::error::TrySendError::Full(event)) = err;
                        event.run();
                    }
                }
                None => deliver(),
            }
        });

        registry().action_result_subscribers.lock().insert(
            handle,
            FfiActionResultSubEntry {
                join: task,
                cancelled,
                in_flight,
            },
        );
        *out_sub = handle;
        AORTA_OK
    })
}

/// Free a result subscription: cancel a still-pending watcher and drain any
/// in-flight terminal callback so the caller can free its callback state.
#[no_mangle]
pub extern "C" fn aorta_action_result_sub_free(sub: aorta_action_result_sub_t) -> aorta_result_t {
    ffi_guard(|| {
        if sub == 0 {
            return AORTA_OK;
        }
        let entry = registry().action_result_subscribers.lock().remove(&sub);
        let Some(FfiActionResultSubEntry {
            join,
            cancelled,
            in_flight,
        }) = entry
        else {
            return map_aorta_error(AortaError::not_found(
                "action result subscriber handle not found",
            ));
        };
        // Bar any not-yet-started terminal callback — including a `CoroutineResume`
        // lane event still queued on a busy executor — then stop the watcher.
        cancelled.store(true, Ordering::SeqCst);
        join.abort();

        // The return code tells the caller whether its callback state is safe to
        // FREE (AORTA_OK) or must be LEAKED (non-OK) because a callback may still
        // be touching it — the C++ ActionResultSubscription leaks state_ on a
        // non-OK return, mirroring ActionFeedbackSubscription's leak-on-timeout.
        //
        // Reentrant free from INSIDE *this* subscription's terminal callback
        // (running on an executor dispatch thread or a runtime worker) cannot
        // wait for itself; a free from another async (tokio) context must not
        // block either. In both cases detach and report BUSY (leak);
        // `cancelled` still bars any future delivery. The identity compare
        // (`== sub`) is what lets a callback that frees a DIFFERENT
        // subscription drain it normally instead of leaking it.
        if CURRENT_RESULT_DELIVERY.with(|f| f.get()) == sub || in_async_context() {
            runtime().spawn(async move {
                let _ = join.await;
            });
            return AORTA_ERR_BUSY;
        }

        // Await the watcher (which also covers an inline delivery running on
        // it), then drain a callback actively running on an executor dispatch.
        // Only once the drain confirms zero in-flight callbacks is the
        // caller's state safe to free.
        //
        // BOUND the await: `abort()` cannot interrupt a SYNCHRONOUS decode-hook
        // or callback call (it only cancels at await points), so a pathological
        // hook that never returns would otherwise wedge teardown forever. On
        // timeout we cannot prove the hook has stopped touching the C callback
        // state, so the caller must leak it (TIMEOUT) — but teardown itself
        // stays bounded.
        let watcher_stopped = runtime()
            .block_on(async move { tokio::time::timeout(FREE_DRAIN_TIMEOUT, join).await.is_ok() });
        if !watcher_stopped {
            aorta_core_warn!(
                "aorta_action_result_sub_free: timed out awaiting watcher (decode hook stuck); \
                 caller must leak its callback state to avoid a use-after-free"
            );
            return AORTA_ERR_TIMEOUT;
        }
        if drain_in_flight(&in_flight, FREE_DRAIN_TIMEOUT) {
            AORTA_OK
        } else {
            aorta_core_warn!(
                "aorta_action_result_sub_free: timed out draining in-flight callback; \
                 caller must leak its callback state to avoid a use-after-free"
            );
            AORTA_ERR_TIMEOUT
        }
    })
}

// ===========================================================================
// Flow action ctx bridge: the coroutine-plane counterparts of the
// blocking send_goal/cancel/get_result calls, shaped exactly like
// `aorta_client_call_ctx` — resolve on the caller thread, run on the shared
// runtime raced against scope cancellation, deliver via `on_done` exactly
// once from a runtime worker (a marked callback context: post, don't block).
// One-shot by design: no subscription handle, nothing to free, so the C++
// awaiter can live entirely in the coroutine frame (the `flow::call` shape).
// ===========================================================================

/// `on_done(code, payload_view, completion, user_data)` for the action ctx
/// bridge — the same shape `aorta_client_call_ctx` uses. `completion` is
/// NULL on success (and on raw-code failures with no stop context);
/// otherwise a completion record valid for the callback only.
pub type aorta_action_ctx_done_cb = Option<
    unsafe extern "C" fn(
        code: aorta_result_t,
        payload: *const aorta_bytes_view_t,
        completion: *const crate::ffi::c_api::flow::aorta_flow_completion_t,
        user_data: *mut c_void,
    ),
>;

/// Resolve the optional scope handle (0 = none) into a live Scope.
fn resolve_ctx_scope(scope: u64) -> Result<Option<crate::flow::Scope>, aorta_result_t> {
    if scope == 0 {
        return Ok(None);
    }
    let scopes = registry().flow_scopes.lock();
    match scopes.get(&scope) {
        Some(s) => Ok(Some(s.clone())),
        None => Err(AORTA_ERR_NOT_FOUND),
    }
}

/// The action-ctx error channel: the legacy code plus, when the failure
/// has flow stop context, the original `Stopped` (the
/// completion record's source).
type ActionCtxError = (aorta_result_t, Option<crate::flow::Stopped>);

/// EVERY action-bridge failure carries stop context — a
/// transport/service fault keeps its original code + message; an
/// operation timeout is a DEADLINE cancellation (legacy code stays
/// TIMEOUT, the record speaks the normative vocabulary).
fn ctx_err_from_error(err: &crate::error::AortaError) -> ActionCtxError {
    (
        aorta_result_code_of(err),
        Some(crate::flow::Stopped::fault_from_error(err)),
    )
}

fn ctx_err_timeout() -> ActionCtxError {
    (
        AORTA_ERR_TIMEOUT,
        Some(crate::flow::Stopped::cancelled(
            crate::flow::CancelReason::DeadlineExceeded,
        )),
    )
}

/// Deliver an action-ctx outcome exactly once (reentrancy-marked; borrowed
/// payload views AND the completion record valid only for the call — the
/// SDK copies out).
unsafe fn deliver_action_ctx_done(
    on_done: unsafe extern "C" fn(
        aorta_result_t,
        *const aorta_bytes_view_t,
        *const crate::ffi::c_api::flow::aorta_flow_completion_t,
        *mut c_void,
    ),
    result: Result<crate::ZBytes, ActionCtxError>,
    user_data: usize,
) {
    let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
    let user_data = user_data as *mut c_void;
    match result {
        Ok(payload) => {
            let mut single = aorta_bytes_slice_t::default();
            let mut slices = Vec::new();
            let view = bytes_view_from_zbytes_borrowed(&payload, &mut single, &mut slices);
            ffi_guard_void(|| {
                // SAFETY: `on_done` is the registered non-null callback; the
                // view borrows locals valid for the duration of the call.
                unsafe { on_done(AORTA_OK, ptr::addr_of!(view), ptr::null(), user_data) };
            });
        }
        Err((code, stopped)) => {
            let view = aorta_bytes_view_t::default();
            match stopped {
                Some(stopped) => {
                    crate::ffi::c_api::flow::with_ffi_completion(&stopped, |completion| {
                        ffi_guard_void(|| {
                            // SAFETY: as above; errors pass an empty view and
                            // the closure-scoped completion record.
                            unsafe { on_done(code, ptr::addr_of!(view), completion, user_data) };
                        });
                    });
                }
                None => {
                    ffi_guard_void(|| {
                        // SAFETY: as above; no stop context — NULL record,
                        // the code is the whole story.
                        unsafe { on_done(code, ptr::addr_of!(view), ptr::null(), user_data) };
                    });
                }
            }
        }
    }
}

/// Run `fut` under the optional scope: refuse on an already-cancelled scope,
/// resolve as CANCELLED the moment the scope cancels mid-flight — carrying
/// the scope's EXACT cancel reason as stop context.
async fn run_in_ctx_scope<F>(
    scope: Option<crate::flow::Scope>,
    fut: F,
) -> Result<crate::ZBytes, ActionCtxError>
where
    F: std::future::Future<Output = Result<crate::ZBytes, ActionCtxError>>,
{
    match scope {
        Some(s) => {
            let cancelled_here = |s: &crate::flow::Scope| {
                let reason = s
                    .cancel_reason()
                    .unwrap_or(crate::flow::CancelReason::Explicit);
                (
                    AORTA_ERR_CANCELLED,
                    Some(crate::flow::Stopped::cancelled(reason)),
                )
            };
            if s.checkpoint().is_err() {
                return Err(cancelled_here(&s));
            }
            match s.run_until_cancelled(fut).await {
                Some(result) => result,
                None => Err(cancelled_here(&s)),
            }
        }
        None => fut.await,
    }
}

/// Async send_goal bridge (`co_await flow::send_goal`): issues the send-goal
/// control query inside `scope` (0 = plain call). The scope's remaining
/// budget both NARROWS the control timeout and is stamped on the wire as the
/// goal's deadline budget (the server seeds the goal scope from it) — the
/// C++-plane counterpart of the Rust typed client's ambient stamping.
/// Honors the `AORTA_RPC_DEADLINE=0` kill-switch.
///
/// # Safety
/// - `request` must be a valid view; it stays unused after the call
///   returns (the request bytes are copied).
/// - `on_done` must be non-null and, together with `user_data`, must stay
///   valid until the callback FIRES — it is invoked later from a runtime
///   worker, not during this call (the old text said
///   "unused after the call returns", which would make freeing the
///   caller's context immediately look legal — a UAF when it fires).
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_send_goal_ctx(
    client: aorta_action_client_t,
    scope: u64,
    request: *const aorta_bytes_view_t,
    on_done: aorta_action_ctx_done_cb,
    user_data: *mut c_void,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the view deref.
    ffi_guard(|| unsafe {
        let Some(on_done) = on_done else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "on_done is null");
        };
        let request_payload = match zbytes_from_ffi_view(request, "request") {
            Ok(p) => p,
            Err(err) => return map_aorta_error(err),
        };
        if request_payload.is_empty() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty request");
        }
        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };
        let scope_ref = match resolve_ctx_scope(scope) {
            Ok(s) => s,
            Err(code) => return aorta_err(code, "flow scope not found"),
        };
        let key = match client_arc.send_goal_key() {
            Ok(key) => key,
            Err(err) => return map_aorta_error(err),
        };

        // Caller-thread capture: traceparent (joins an open SDK span) and the
        // scope's remaining budget for the wire TLV + timeout narrowing.
        let link = crate::trace::current_outbound_context().map(|(trace_id, span_id)| {
            crate::trace::TraceParent {
                trace_id,
                span_id,
                flags: crate::trace::TRACEPARENT_SAMPLED,
            }
        });
        let budget_ns = scope_ref
            .as_ref()
            .and_then(|s| s.remaining())
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .filter(|_| crate::client::rpc_deadline_stamping_enabled());
        let request_attachment = if link.is_none() && budget_ns.is_none() {
            None
        } else {
            crate::attachment::encode(&crate::attachment::AttachmentParts {
                schema_hash: None,
                traceparent: link,
                reply_busy_ns: None,
                deadline_budget_ns: budget_ns,
            })
        };
        let timeout = scope_ref
            .as_ref()
            .and_then(|s| s.remaining())
            .map_or(client_arc.control_timeout, |r| {
                client_arc.control_timeout.min(r)
            });

        let user_data = user_data as usize;
        runtime().spawn(async move {
            let key = key;
            let fut = async {
                client_arc
                    .query_optional_bytes_attached(
                        &key,
                        Some(request_payload),
                        request_attachment,
                        timeout,
                    )
                    .await
                    .map_err(|e| ctx_err_from_error(&e))?
                    .ok_or_else(ctx_err_timeout)
            };
            let result = run_in_ctx_scope(scope_ref, fut).await;
            // SAFETY: exactly-once delivery per the ctx-bridge contract
            // (covered by the enclosing entry unsafe block).
            deliver_action_ctx_done(on_done, result, user_data);
        });
        AORTA_OK
    })
}

/// Async cancel bridge: like [`aorta_action_client_send_goal_ctx`] over the
/// cancel-goal control key. `on_done` MAY be null — fire-and-forget (the
/// `GoalGuard` destructor shape: best-effort cancel, nothing to wait on).
///
/// # Safety
/// - `request` must be a valid view (copied before return).
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_cancel_ctx(
    client: aorta_action_client_t,
    scope: u64,
    request: *const aorta_bytes_view_t,
    on_done: aorta_action_ctx_done_cb,
    user_data: *mut c_void,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the view deref.
    ffi_guard(|| unsafe {
        let request_payload = match zbytes_from_ffi_view(request, "request") {
            Ok(p) => p,
            Err(err) => return map_aorta_error(err),
        };
        if request_payload.is_empty() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty request");
        }
        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };
        let scope_ref = match resolve_ctx_scope(scope) {
            Ok(s) => s,
            Err(code) => return aorta_err(code, "flow scope not found"),
        };
        let key = match client_arc.cancel_goal_key() {
            Ok(key) => key,
            Err(err) => return map_aorta_error(err),
        };
        let timeout = client_arc.control_timeout;
        let user_data = user_data as usize;
        runtime().spawn(async move {
            let fut = async {
                client_arc
                    .query_optional_bytes_attached(&key, Some(request_payload), None, timeout)
                    .await
                    .map_err(|e| ctx_err_from_error(&e))?
                    .ok_or_else(ctx_err_timeout)
            };
            let result = run_in_ctx_scope(scope_ref, fut).await;
            if let Some(on_done) = on_done {
                // SAFETY: exactly-once delivery per the ctx-bridge contract
                // (covered by the enclosing entry unsafe block).
                deliver_action_ctx_done(on_done, result, user_data);
            } else if let Err((code, _stopped)) = result {
                // Fire-and-forget cancels still surface failures loudly.
                aorta_core_warn!("Aorta WARN: fire-and-forget action cancel failed (code {code})");
            }
        });
        AORTA_OK
    })
}

/// Async terminal-result bridge (`co_await flow::wait_result`): runs the
/// SAME status-watching machinery as `aorta_action_client_subscribe_result`,
/// raced against `scope` cancellation, and delivers the raw get_result
/// response payload exactly once via `on_done`. One-shot: no subscription
/// handle, nothing to free. `timeout_ms == u64::MAX` = no timeout (0 expires
/// immediately — same sentinel contract as subscribe_result).
///
/// # Safety
/// - `goal_id` must be a valid NUL-terminated UTF-8 string; `get_result_request`
///   valid for `len` bytes; `decode_status`/`on_done` non-null; `(decode_status,
///   on_done, user_data)` must stay callable/valid until `on_done` fires.
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_wait_result_ctx(
    client: aorta_action_client_t,
    scope: u64,
    goal_id: *const c_char,
    get_result_request: *const u8,
    get_result_request_len: usize,
    timeout_ms: u64,
    decode_status: aorta_action_status_decode_fn,
    on_done: aorta_action_ctx_done_cb,
    user_data: *mut c_void,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`; inner unsafe covers the CStr/slice
    // derefs (null-checked).
    ffi_guard(|| unsafe {
        let (Some(on_done), Some(decode_status)) = (on_done, decode_status) else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback or decoder is null");
        };
        if goal_id.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "goal_id is null");
        }
        if get_result_request.is_null() || get_result_request_len == 0 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty get_result request");
        }
        let goal_id = match CStr::from_ptr(goal_id).to_str() {
            Ok(s) => s.trim().to_string(),
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in goal_id"),
        };
        let request =
            std::slice::from_raw_parts(get_result_request, get_result_request_len).to_vec();
        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };
        let scope_ref = match resolve_ctx_scope(scope) {
            Ok(s) => s,
            Err(code) => return aorta_err(code, "flow scope not found"),
        };
        let timeout_duration = (timeout_ms != u64::MAX).then(|| Duration::from_millis(timeout_ms));

        let user_data = user_data as usize;
        runtime().spawn(async move {
            let fut = async {
                run_action_result_watcher(
                    client_arc,
                    goal_id,
                    request,
                    timeout_duration,
                    decode_status,
                    user_data,
                )
                .await
                .map_err(|(code, msg)| {
                    aorta_core_warn!("Aorta WARN: action wait_result failed: {msg}");
                    if code == AORTA_ERR_TIMEOUT {
                        ctx_err_timeout()
                    } else {
                        // Gate 1.1: the diagnostic crosses the ABI in the
                        // record instead of dying in this log line.
                        (code, Some(crate::flow::Stopped::fault_with_code(code, msg)))
                    }
                })
            };
            let result = run_in_ctx_scope(scope_ref, fut).await;
            // SAFETY: exactly-once delivery per the ctx-bridge contract
            // (covered by the enclosing entry unsafe block).
            deliver_action_ctx_done(on_done, result, user_data);
        });
        AORTA_OK
    })
}

// ===========================================================================
// Bounded, off-RX action feedback (subscribe_feedback2). The raw zenoh
// subscriber's inline RX callback ONLY
// enqueues into a bounded queue (applying the overflow policy + a dropped
// counter) — never the user callback — so a slow consumer can no longer stall
// the link. Delivery mode is fixed at subscribe time: with
// the node attached to an executor, drains are coalesced by a WakeSlot and
// dispatched as `CoroutineResume` events on the executor's reserved lane (the
// callback runs on an executor dispatch thread — blocking re-entry returns
// AORTA_ERR_REENTRANT like every other callback); with no executor, a
// dedicated drain thread invokes the C callback OFF the RX task (and off any
// tokio runtime), where it may still re-enter aorta FFI.
// ===========================================================================

/// Overflow policy when the bounded feedback queue is full.
pub const AORTA_FB_DROP_OLDEST: u8 = 0;
pub const AORTA_FB_DROP_NEW: u8 = 1;

thread_local! {
    /// The feedback2 subscription handle whose drain thread is running on this
    /// thread, or 0 when none. `aorta_action_feedback_sub2_free` compares it to
    /// the handle being freed: a callback dropping *its own* handle (reentrant)
    /// must detach instead of self-joining the drain thread, but a callback that
    /// closes a DIFFERENT stream still joins it normally (no spurious leak).
    static ON_FEEDBACK2_DELIVERY: std::cell::Cell<aorta_action_feedback_sub_t> =
        const { std::cell::Cell::new(0) };
}

struct FfiFeedbackQueueInner {
    deque: std::collections::VecDeque<Vec<u8>>,
    stopped: bool,
}

/// Bounded raw-feedback queue shared between the RX enqueue callback and the
/// drain thread.
pub struct FfiFeedbackQueue {
    inner: std::sync::Mutex<FfiFeedbackQueueInner>,
    cv: std::sync::Condvar,
    capacity: usize,
    drop_oldest: bool,
    dropped: AtomicU64,
}

impl FfiFeedbackQueue {
    fn push(&self, payload: Vec<u8>) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.stopped {
            return;
        }
        if g.deque.len() >= self.capacity {
            if self.drop_oldest {
                g.deque.pop_front();
                g.deque.push_back(payload);
            }
            // DROP_NEW: silently drop the incoming payload.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            g.deque.push_back(payload);
        }
        drop(g);
        self.cv.notify_one();
    }

    fn stop(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.stopped = true;
        drop(g);
        self.cv.notify_all();
    }

    /// Block until an item is available; returns None once stopped and drained.
    fn pop_blocking(&self) -> Option<Vec<u8>> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(p) = g.deque.pop_front() {
                return Some(p);
            }
            if g.stopped {
                return None;
            }
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Non-blocking pop for the executor-lane drain.
    fn try_pop(&self) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deque
            .pop_front()
    }
}

/// Shared state for a lane-delivered feedback2 subscription: the RX callback
/// pushes into `queue` and arms `slot`; the winning arm enqueues one
/// `CoroutineResume` event whose dispatch runs [`drain_feedback2_lane`].
struct Feedback2LaneState {
    queue: Arc<FfiFeedbackQueue>,
    slot: aorta_sync::WakeSlot,
    topic: String,
    cb: unsafe extern "C" fn(
        payload: *const aorta_bytes_view_t,
        schema: *const aorta_schema_hash_view_t,
        ctx: *const aorta_msg_context_t,
        user_data: *mut c_void,
    ),
    user_data: usize,
    handle: aorta_action_feedback_sub_t,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    in_flight: Arc<AtomicUsize>,
}

/// Build the borrowed views and invoke the feedback2 C callback for one
/// payload (shared by the drain thread and the executor-lane drain).
fn invoke_feedback2_cb(
    payload: Vec<u8>,
    cb: unsafe extern "C" fn(
        *const aorta_bytes_view_t,
        *const aorta_schema_hash_view_t,
        *const aorta_msg_context_t,
        *mut c_void,
    ),
    user_data: usize,
    topic: &str,
) {
    let user_data = user_data as *mut c_void;
    let zbytes = crate::ZBytes::from(payload);
    let mut single_slice = aorta_bytes_slice_t::default();
    let mut payload_slices = Vec::new();
    let payload_view =
        bytes_view_from_zbytes_borrowed(&zbytes, &mut single_slice, &mut payload_slices);
    let schema_view = aorta_schema_hash_view_t {
        hex: ptr::null(),
        len: 0,
    };
    let msg_ctx = aorta_msg_context_t {
        topic: string_view_from_str(topic),
        receive_stamp_ns: 0,
        priority: 0,
        publisher_node: aorta_string_view_t::default(),
        sequence: 0,
    };
    ffi_guard_void(|| {
        // SAFETY: `cb` is the non-null C callback registered by
        // `subscribe_feedback2`; the views borrow locals valid for the call.
        unsafe {
            cb(
                ptr::addr_of!(payload_view),
                ptr::addr_of!(schema_view),
                ptr::addr_of!(msg_ctx),
                user_data,
            )
        };
    });
}

/// Executor-lane drain (one `CoroutineResume` dispatch): disarm FIRST
/// (clear-before-poll — a sample pushed after this re-arms and enqueues a
/// follow-up event, so nothing is stranded; `aorta_sync::WakeSlot`'s loom
/// proofs pin exactly this order), then pop at most `capacity` items — the
/// bounded queue holds at most that many at entry, and anything pushed
/// mid-drain has re-armed its own follow-up — invoking the callback per item.
fn drain_feedback2_lane(st: &Feedback2LaneState) {
    st.slot.disarm();
    // Bound = the queue's own capacity (one source of truth): the bounded
    // queue holds at most that many items at entry.
    for _ in 0..st.queue.capacity {
        let Some(payload) = st.queue.try_pop() else {
            return;
        };
        if st.cancelled.load(Ordering::SeqCst) {
            return;
        }
        st.in_flight.fetch_add(1, Ordering::AcqRel);
        let _drain_guard = InFlightGuard(&st.in_flight);
        // Re-check after taking the in-flight gate (free-race protocol, same
        // as the result delivery).
        if st.cancelled.load(Ordering::SeqCst) {
            return;
        }
        ON_FEEDBACK2_DELIVERY.with(|f| f.set(st.handle));
        invoke_feedback2_cb(payload, st.cb, st.user_data, &st.topic);
        ON_FEEDBACK2_DELIVERY.with(|f| f.set(0));
    }
}

/// Subscribe to action feedback with a bounded, off-RX delivery queue. Mirrors
/// `aorta_action_client_subscribe_feedback` (client-wide raw feedback to
/// `aorta_on_msg_cb`) but the callback fires off the RX task — on the node
/// executor's dispatch thread (reserved-lane delivery) when
/// an executor is attached at subscribe time, else on a dedicated drain
/// thread — and `capacity`/`policy` bound the queue with a dropped counter.
///
/// # Safety
/// - `client` is a valid action client handle
/// - `cb` is non-null; `out_sub` is a valid pointer
#[no_mangle]
pub unsafe extern "C" fn aorta_action_client_subscribe_feedback2(
    client: aorta_action_client_t,
    capacity: u32,
    policy: u8,
    cb: aorta_on_msg_cb,
    user_data: *mut c_void,
    out_sub: *mut aorta_action_feedback_sub_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. The inner `unsafe` covers the `*out_sub`
    // writeback below; `out_sub` is null-checked at the top of the closure under
    // the FFI caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if client == 0 || out_sub.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let Some(cb) = cb else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
        };

        let client_arc = {
            let clients = registry().action_clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("action client not found")),
            }
        };
        let key = match client_arc.feedback_key() {
            Ok(key) => key,
            Err(err) => return map_aorta_error(err),
        };
        let topic = key.clone();

        let queue = Arc::new(FfiFeedbackQueue {
            inner: std::sync::Mutex::new(FfiFeedbackQueueInner {
                deque: std::collections::VecDeque::new(),
                stopped: false,
            }),
            cv: std::sync::Condvar::new(),
            capacity: (capacity as usize).max(1),
            drop_oldest: policy != AORTA_FB_DROP_NEW,
            dropped: AtomicU64::new(0),
        });

        // Delivery mode is decided ONCE here (never mixed, so the C callback
        // is never invoked from two threads concurrently): with the client's
        // node attached to an executor, drains dispatch on the executor via
        // the reserved `CoroutineResume` lane; otherwise the
        // legacy dedicated drain thread serves as the executor-less fallback.
        let lane_sender = client_arc
            .event_sender_cell
            .read()
            .ok()
            .and_then(|guard| guard.as_ref().cloned())
            .filter(|sender| !sender.is_closed());
        let handle = alloc_handle();
        let user_data = user_data as usize;

        let (rx_callback, delivery): (
            Box<dyn Fn(zenoh::sample::Sample) + Send + Sync + 'static>,
            Feedback2Delivery,
        ) = match lane_sender {
            Some(sender) => {
                let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let in_flight = Arc::new(AtomicUsize::new(0));
                let state = Arc::new(Feedback2LaneState {
                    queue: Arc::clone(&queue),
                    slot: aorta_sync::WakeSlot::new(),
                    topic,
                    cb,
                    user_data,
                    handle,
                    cancelled: Arc::clone(&cancelled),
                    in_flight: Arc::clone(&in_flight),
                });
                let rx = move |sample: zenoh::sample::Sample| {
                    let fields: zenoh::sample::SampleFields = sample.into();
                    if fields.payload.is_empty() {
                        return;
                    }
                    state.queue.push(fields.payload.to_bytes().into_owned());
                    // Publish-then-arm (WakeSlot protocol): only the winning
                    // arm enqueues the single coalesced drain event.
                    if state.slot.arm() {
                        let st = Arc::clone(&state);
                        let event = crate::event_queue::Event::new(
                            crate::event_queue::EventKind::CoroutineResume,
                            move || drain_feedback2_lane(&st),
                        );
                        if sender.try_send(event).is_err() {
                            // Executor torn down: nothing will drain; disarm
                            // so a later (impossible today) revival re-arms.
                            state.slot.disarm();
                        }
                    }
                };
                (
                    Box::new(rx),
                    Feedback2Delivery::Lane {
                        cancelled,
                        in_flight,
                    },
                )
            }
            None => {
                // Dedicated drain thread: invoke the C callback OFF the RX
                // task and off any tokio runtime. The handle is allocated
                // above so the thread can stamp the delivery-identity
                // thread-local (used by free's reentrancy check).
                let queue_drain = Arc::clone(&queue);
                let drain = std::thread::Builder::new()
                    .name("aorta-act-feedback2".to_string())
                    .spawn(move || {
                        ON_FEEDBACK2_DELIVERY.with(|f| f.set(handle));
                        while let Some(payload) = queue_drain.pop_blocking() {
                            invoke_feedback2_cb(payload, cb, user_data, &topic);
                        }
                    })
                    .expect("spawn aorta-act-feedback2 thread");
                let queue_rx = Arc::clone(&queue);
                let rx = move |sample: zenoh::sample::Sample| {
                    let fields: zenoh::sample::SampleFields = sample.into();
                    if fields.payload.is_empty() {
                        return;
                    }
                    queue_rx.push(fields.payload.to_bytes().into_owned());
                };
                (Box::new(rx), Feedback2Delivery::Thread(drain))
            }
        };

        // Inline RX callback: ONLY enqueue + (lane mode) a non-blocking
        // arm/try_send. Never runs user code on the RX thread.
        let subscriber = match crate::ffi_block_on!(async {
            client_arc
                .session
                .inner()
                .declare_subscriber(key)
                .callback(rx_callback)
                .await
        }) {
            Ok(Ok(subscriber)) => subscriber,
            Ok(Err(err)) => {
                if let Feedback2Delivery::Thread(drain) = delivery {
                    queue.stop();
                    let _ = drain.join();
                }
                return map_aorta_error(AortaError::internal(err.to_string()));
            }
            Err(_) => {
                if let Feedback2Delivery::Thread(drain) = delivery {
                    queue.stop();
                    let _ = drain.join();
                }
                return aorta_reentrant(module_path!());
            }
        };

        registry().action_feedback2_subscribers.lock().insert(
            handle,
            FfiActionFeedback2SubEntry {
                subscriber,
                queue,
                delivery,
            },
        );
        *out_sub = handle;
        AORTA_OK
    })
}

/// Total feedback samples dropped due to bounded-queue overflow since creation.
///
/// # Safety
/// - `out_dropped` must be a valid pointer
#[no_mangle]
pub unsafe extern "C" fn aorta_action_feedback_sub2_dropped_count(
    sub: aorta_action_feedback_sub_t,
    out_dropped: *mut u64,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. The inner `unsafe` covers the
    // `*out_dropped` writeback below; `out_dropped` is null-checked at the top of
    // the closure under the FFI caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if out_dropped.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let subs = registry().action_feedback2_subscribers.lock();
        match subs.get(&sub) {
            Some(entry) => {
                *out_dropped = entry.queue.dropped.load(Ordering::Relaxed);
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found(
                "action feedback2 subscriber handle not found",
            )),
        }
    })
}

/// Free a bounded feedback subscription: stop the queue, undeclare the
/// subscriber, and join the drain thread (draining the in-flight callback).
/// Returns non-OK if the in-flight callback cannot be confirmed finished (a
/// reentrant free from inside the callback) — the caller must then leak its
/// callback state to avoid a use-after-free.
#[no_mangle]
pub extern "C" fn aorta_action_feedback_sub2_free(
    sub: aorta_action_feedback_sub_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if sub == 0 {
            return AORTA_OK;
        }
        let entry = registry().action_feedback2_subscribers.lock().remove(&sub);
        let Some(FfiActionFeedback2SubEntry {
            subscriber,
            queue,
            delivery,
        }) = entry
        else {
            return map_aorta_error(AortaError::not_found(
                "action feedback2 subscriber handle not found",
            ));
        };

        // Stop the queue (wakes a drain thread so it exits; bars new pushes)
        // and stop NEW enqueues by undeclaring the subscriber.
        queue.stop();
        if in_async_context() {
            runtime().spawn(async move {
                let _ = subscriber.undeclare().await;
            });
        } else {
            let _ = runtime().block_on(async move { subscriber.undeclare().await });
        }

        match delivery {
            Feedback2Delivery::Thread(drain) => {
                // Reentrant free from inside THIS stream's feedback callback
                // runs ON its drain thread: joining would self-deadlock.
                // Detach and report BUSY so the caller leaks its state (the
                // callback is still using it). The identity compare (`== sub`)
                // lets a callback that closes a DIFFERENT stream join that
                // stream's drain thread normally instead of leaking it.
                if ON_FEEDBACK2_DELIVERY.with(|f| f.get()) == sub {
                    std::mem::drop(drain); // detach: the thread exits on its own
                    return AORTA_ERR_BUSY;
                }
                let _ = drain.join();
                AORTA_OK
            }
            Feedback2Delivery::Lane {
                cancelled,
                in_flight,
            } => {
                // Bar any queued-but-undispatched lane drain (a stale wake),
                // then wait out a callback actively running on the executor.
                cancelled.store(true, Ordering::SeqCst);
                // Unlike the Thread arm (whose drain thread makes independent
                // progress), lane deliveries progress ONLY on executor /
                // runtime threads — blocking one of those in drain_in_flight
                // could be waiting on itself (guaranteed on a single-worker
                // executor). Reentrant self-free AND any free from an async
                // context take the non-blocking BUSY path (caller leaks its
                // state; `cancelled` bars future delivery).
                if ON_FEEDBACK2_DELIVERY.with(|f| f.get()) == sub || in_async_context() {
                    return AORTA_ERR_BUSY;
                }
                if drain_in_flight(&in_flight, FREE_DRAIN_TIMEOUT) {
                    AORTA_OK
                } else {
                    aorta_core_warn!(
                        "aorta_action_feedback_sub2_free: timed out draining in-flight \
                         callback; caller must leak its callback state to avoid a \
                         use-after-free"
                    );
                    AORTA_ERR_TIMEOUT
                }
            }
        }
    })
}
