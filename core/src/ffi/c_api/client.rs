// Client (synchronous RPC) FFI surface.
//
// Extracted from c_api/mod.rs.

use super::*;

/// Create a new client
///
/// # Safety
/// - `node` must be a valid aorta_node_t handle
/// - `service_name` must be a valid null-terminated UTF-8 string
#[no_mangle]
pub unsafe extern "C" fn aorta_client_new_with_options(
    node: aorta_node_t,
    service_name: *const c_char,
    opts: *const aorta_client_options_t,
    out_client: *mut aorta_client_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Closure is safe; the inner `unsafe`
    // covers `CStr::from_ptr(service_name)` and `*out_client = handle`, both
    // operating on caller-provided pointers under the documented contract.
    ffi_guard(|| unsafe {
        if service_name.is_null() || out_client.is_null() {
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

        let (
            timeout,
            schema_attach_policy,
            service_schema,
            enforce_single_provider,
            multi_reply_check_window,
            allow_retry,
        ) = match client_config_from_ffi(opts, 5000, node_obj.schema_attach_policy()) {
            Ok(v) => v,
            Err(e) => return map_aorta_error(e),
        };

        let mut builder = Client::builder(node_obj.session().clone(), &name)
            .key_builder(node_obj.key_builder().clone())
            .default_schema_attach_policy(node_obj.schema_attach_policy())
            .inventory_telemetry(node_obj.vital_telemetry())
            .runtime_telemetry(node_obj.runtime_metrics_telemetry())
            .schema_attach_policy(schema_attach_policy)
            .timeout(timeout);

        if let Some(service_schema) = service_schema {
            builder = builder.service_schema(service_schema);
        }
        if let Some(enforce_single_provider) = enforce_single_provider {
            builder = builder.enforce_single_provider(enforce_single_provider);
        }
        if let Some(multi_reply_check_window) = multi_reply_check_window {
            builder = builder.multi_reply_check_window(multi_reply_check_window);
        }
        if let Some(allow_retry) = allow_retry {
            builder = builder.allow_retry(allow_retry);
        }

        let client = builder.build();

        let handle = alloc_handle();
        registry().clients.lock().insert(handle, Arc::new(client));

        *out_client = handle;
        AORTA_OK
    })
}

/// Call a service synchronously
///
/// # Safety
/// - `client` must be a valid aorta_client_t handle
/// - `request` and `response` must be valid pointers
#[no_mangle]
pub unsafe extern "C" fn aorta_client_call(
    client: aorta_client_t,
    request: *const aorta_owned_bytes_view_t,
    response: *mut aorta_owned_bytes_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. The inner `unsafe` covers
    // `zbytes_from_owned_ffi_view(request, …)` (which deref-reads `request`)
    // and `*response = owned_bytes_from_zbytes(data)` writeback, both under
    // the caller's pointer-validity contract.
    ffi_guard(|| unsafe {
        if client == 0 || request.is_null() || response.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }

        let request_payload = match zbytes_from_owned_ffi_view(request, "request") {
            Ok(payload) => payload,
            Err(err) => return map_aorta_error(err),
        };
        if request_payload.is_empty() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "empty request");
        }

        let client_arc = {
            let clients = registry().clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("client not found")),
            }
        };

        let result = match crate::ffi_block_on!(client_arc.call(request_payload)) {
            Ok(r) => r,
            Err(_) => return aorta_reentrant(module_path!()),
        };

        match result {
            Ok(data) => {
                *response = owned_bytes_from_zbytes(data);
                AORTA_OK
            }
            Err(e) => map_aorta_error(e),
        }
    })
}

/// Free owned bytes returned by FFI APIs.
///
/// Idempotent: safe to call repeatedly on the same struct — the whole
/// struct is zeroed before returning so a subsequent call observes a null
/// `internal` and a zeroed `view`, and is a no-op.
///
/// # Safety
/// - `bytes` must have been initialized by an Aorta FFI API, or be null
#[no_mangle]
pub unsafe extern "C" fn aorta_owned_bytes_free(bytes: *mut aorta_owned_bytes_t) {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers the null check,
    // `Box::from_raw` reclaim of an `OwnedFfiBytes` previously leaked by an
    // Aorta FFI API, and the final zero-write — all under the caller's
    // contract that `bytes` is either null or an Aorta-FFI-issued pointer.
    ffi_guard_void(|| unsafe {
        if bytes.is_null() {
            return;
        }
        if !(*bytes).internal.is_null() {
            let _ = Box::from_raw((*bytes).internal as *mut OwnedFfiBytes);
        }
        // Zero the whole struct in one shot — equivalent to clearing
        // `internal` + resetting `view` separately, and harder to forget
        // a field on if the struct grows.
        *bytes = aorta_owned_bytes_t::default();
    })
}

/// Free a client
#[no_mangle]
pub extern "C" fn aorta_client_free(client: aorta_client_t) -> aorta_result_t {
    ffi_guard(|| {
        if client == 0 {
            return AORTA_OK;
        }

        registry().clients.lock().remove(&client);

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
pub unsafe extern "C" fn aorta_client_get_stats(
    client: aorta_client_t,
    out: *mut aorta_client_stats_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return map_aorta_error(AortaError::invalid_argument("out is null"));
        }

        let client = registry().clients.lock().get(&client).cloned();

        let Some(client) = client else {
            return map_aorta_error(AortaError::not_found("client not found"));
        };

        // SAFETY: `out` was null-checked above; caller contract requires it
        // point at writable storage for `aorta_client_stats_t`.
        unsafe {
            *out = client_stats_from_core(client.stats());
        }
        AORTA_OK
    })
}

/// One-shot readiness check for an RPC client: writes `1` to `*out_matching`
/// if at least one matching service provider (queryable) currently exists for
/// the client's service key, `0` otherwise.
///
/// # Safety
/// `out_matching` must be non-null and point at writable storage for a `u8`.
/// `client` must originate from a client constructor and not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_client_matching_status(
    client: aorta_client_t,
    out_matching: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if client == 0 || out_matching.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let client_arc = {
            let clients = registry().clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("client not found")),
            }
        };
        match crate::ffi_block_on!(client_arc.matching_status()) {
            Ok(Ok(matching)) => {
                // SAFETY: `out_matching` null-checked above.
                unsafe {
                    *out_matching = u8::from(matching);
                }
                AORTA_OK
            }
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

/// Wait (bounded by `timeout_ms`) until a matching service provider appears,
/// then write `1`/`0` to `*out_matching`. A timeout writes `0` and returns
/// `AORTA_OK` (not an error).
///
/// # Safety
/// `out_matching` must be non-null and point at writable storage for a `u8`.
/// `client` must originate from a client constructor and not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_client_wait_for_matching(
    client: aorta_client_t,
    timeout_ms: u64,
    out_matching: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if client == 0 || out_matching.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "null pointer");
        }
        let client_arc = {
            let clients = registry().clients.lock();
            match clients.get(&client) {
                Some(c) => Arc::clone(c),
                None => return map_aorta_error(AortaError::not_found("client not found")),
            }
        };
        let timeout = std::time::Duration::from_millis(timeout_ms);
        match crate::ffi_block_on!(client_arc.wait_for_matching(timeout)) {
            Ok(Ok(matching)) => {
                // SAFETY: `out_matching` null-checked above.
                unsafe {
                    *out_matching = u8::from(matching);
                }
                AORTA_OK
            }
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}
