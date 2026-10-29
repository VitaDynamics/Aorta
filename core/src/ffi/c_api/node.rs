// Node FFI surface.
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
pub unsafe extern "C" fn aorta_node_new(
    name: *const c_char,
    group: *const c_char,
    out: *mut aorta_node_t,
) -> aorta_result_t {
    // SAFETY: thin trampoline to `aorta_node_new_with_options`; forwards the
    // caller's pointer arguments unchanged under that fn's matching
    // `# Safety` contract.
    ffi_guard(|| unsafe { aorta_node_new_with_options(name, group, ptr::null(), out) })
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
pub unsafe extern "C" fn aorta_node_new_with_options(
    name: *const c_char,
    group: *const c_char,
    opts: *const aorta_node_options_t,
    out: *mut aorta_node_t,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `cstr_to_string`
    // on `name`/`group`, the optional `opts.as_ref()` via `node_options_from_ffi`,
    // and the `*out = handle` writeback under the caller's pointer contract.
    ffi_guard(|| unsafe {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }

        let name = cstr_to_string(name, "name");
        let name = match name {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        let group = cstr_to_string_optional(group);
        let group = group.unwrap_or_else(|| "default".to_string());
        let options = match node_options_from_ffi(opts) {
            Ok(options) => options,
            Err(err) => return map_aorta_error(err),
        };

        let node = match crate::ffi_block_on!(Node::with_options(name, group, options)) {
            Ok(Ok(node)) => node,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };

        let handle = alloc_handle();
        registry().nodes.lock().insert(handle, node);

        *out = handle;

        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_node_free(node: aorta_node_t) -> aorta_result_t {
    ffi_guard(|| {
        let node = { registry().nodes.lock().remove(&node) };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        // FREE paths use spawn-and-detach on reentrant invocation (matches
        // `aorta_service_free` / `shutdown_ffi_action_server`). Returning
        // REENTRANT here would orphan the registry-removed `node` — the
        // caller has no way to retry the free. Background-closing keeps
        // the "free always succeeds" semantic at the cost of
        // non-deterministic shutdown timing when called from a callback.
        if in_async_context() {
            runtime().spawn(async move {
                let _ = node.close().await;
            });
            return AORTA_OK;
        }

        match runtime().block_on(node.close()) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
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
pub unsafe extern "C" fn aorta_node_now(node: aorta_node_t, out_ns: *mut i64) -> aorta_result_t {
    ffi_guard(|| {
        if out_ns.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_ns is null");
        }

        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        // SAFETY: `out_ns` was null-checked above; caller contract requires
        // it point at writable storage for `i64` (the `.as_nanos()` value
        // truncates into the `*out_ns` cell).
        unsafe {
            *out_ns = node.now().as_nanos();
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
pub unsafe extern "C" fn aorta_now_ns(out_ns: *mut i64) -> aorta_result_t {
    ffi_guard(|| {
        if out_ns.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_ns is null");
        }

        // SAFETY: `out_ns` was null-checked above; caller contract requires
        // it point at writable storage for `i64`.
        unsafe {
            *out_ns = crate::time::now_ns();
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
pub unsafe extern "C" fn aorta_node_get_id(
    node: aorta_node_t,
    out_buf: *mut c_char,
    buf_size: usize,
    out_len: *mut usize,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_buf.is_null() || out_len.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_buf or out_len is null");
        }

        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        let id = node.id();
        let id_bytes = id.as_bytes();

        if id_bytes.len() + 1 > buf_size {
            // SAFETY: `out_len` was null-checked above; caller contract
            // requires it point at writable storage for `usize`.
            unsafe {
                *out_len = id_bytes.len() + 1;
            }
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "buffer too small");
        }

        // SAFETY: caller's `# Safety` contract requires `out_buf` to point at
        // a writable `c_char` buffer of at least `buf_size` bytes; the check
        // above guarantees `id_bytes.len() + 1 <= buf_size` so the copy plus
        // the trailing NUL stay within the buffer. `out_len` was null-checked.
        unsafe {
            std::ptr::copy_nonoverlapping(id_bytes.as_ptr(), out_buf as *mut u8, id_bytes.len());
            *out_buf.add(id_bytes.len()) = 0;
            *out_len = id_bytes.len();
        }

        AORTA_OK
    })
}

/// Writes the node's **effective** group (after env resolution + key-expr
/// normalization) into `out_buf` as a NUL-terminated UTF-8 string. The
/// constructor `group` argument is ignored by Core (the group comes from
/// `AORTA_GROUP`/default), so SDKs must read the resolved value here rather
/// than echo the constructor argument. Mirrors `aorta_node_get_id`.
///
/// # Safety
///
/// Same contract as `aorta_node_get_id`: `out_buf` must be writable for
/// `buf_size` bytes, `out_len` must be a writable `usize`, and `node` must be a
/// live handle.
#[no_mangle]
pub unsafe extern "C" fn aorta_node_get_group(
    node: aorta_node_t,
    out_buf: *mut c_char,
    buf_size: usize,
    out_len: *mut usize,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_buf.is_null() || out_len.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_buf or out_len is null");
        }

        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        let group = node.group();
        let group_bytes = group.as_bytes();

        if group_bytes.len() + 1 > buf_size {
            // SAFETY: `out_len` was null-checked above.
            unsafe {
                *out_len = group_bytes.len() + 1;
            }
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "buffer too small");
        }

        // SAFETY: the check above guarantees the copy plus trailing NUL fit;
        // `out_len` was null-checked.
        unsafe {
            std::ptr::copy_nonoverlapping(
                group_bytes.as_ptr(),
                out_buf as *mut u8,
                group_bytes.len(),
            );
            *out_buf.add(group_bytes.len()) = 0;
            *out_len = group_bytes.len();
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
pub unsafe extern "C" fn aorta_node_has_event_sender(
    node: aorta_node_t,
    out_has_event_sender: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_has_event_sender.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_has_event_sender is null");
        }

        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        // SAFETY: `out_has_event_sender` was null-checked above; caller
        // contract requires writable storage for `u8`.
        unsafe {
            *out_has_event_sender = u8::from(node.has_event_sender());
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
/// pointed-to type. The caller retains ownership of all input buffers;
/// this function does not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_node_validate_topic_name(
    node: aorta_node_t,
    topic: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-supplied `topic` C-string under the NUL-termination
    // contract; `topic` is null-checked at entry.
    ffi_guard(|| unsafe {
        if topic.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "topic is null");
        }

        let topic = match CStr::from_ptr(topic).to_str() {
            Ok(s) => s,
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in topic"),
        };

        let node_obj = { registry().nodes.lock().get(&node).cloned() };
        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        // Publish-side (concrete topic) validator — wildcards rejected.
        // The C++ SDK's `Node::CreatePublisher` doesn't currently call
        // this helper, but the contract still applies if downstream
        // bindings want a fast-fail check before driving the publisher
        // factory. See `aorta_node_validate_topic_pattern` for the
        // subscribe-side variant that accepts wildcards.
        match node_obj.key_builder().topic_key_pub(topic) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
        }
    })
}

/// Normalize an action name into its canonical key form (publish-side
/// normalization: trims and collapses `/`, applies the same per-segment rules
/// the runtime uses for action keys) and write it (NUL-terminated) into
/// `out_buf`. Lets header-only SDK bindings store the *same* normalized name
/// the core runtime uses, so client-side key construction (e.g. an action
/// status subscription built in the binding) matches the keys the server
/// actually publishes under. The normalized form is never longer than the
/// input, so a buffer of `strlen(action_name) + 1` always suffices; if the
/// buffer is too small `AORTA_ERR_INVALID_ARGUMENT` is returned and `out_len`
/// is set to the required length (excluding the NUL).
///
/// # Safety
///
/// `action_name` must reference a NUL-terminated valid UTF-8 sequence.
/// `out_buf` must be valid for writes of `buf_size` bytes and `out_len` must
/// be valid for a `usize` write. The caller retains ownership of all buffers.
#[no_mangle]
pub unsafe extern "C" fn aorta_normalize_action_name(
    action_name: *const c_char,
    out_buf: *mut c_char,
    buf_size: usize,
    out_len: *mut usize,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-supplied `action_name` and the buffer write below; all
    // three pointers are null-checked at entry.
    ffi_guard(|| unsafe {
        if action_name.is_null() || out_buf.is_null() || out_len.is_null() {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "action_name, out_buf or out_len is null",
            );
        }

        let action_name = match CStr::from_ptr(action_name).to_str() {
            Ok(s) => s,
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in action name"),
        };

        let normalized = match crate::key_expr::normalize_path(action_name) {
            Ok(n) => n,
            Err(err) => return map_aorta_error(err),
        };
        let bytes = normalized.as_bytes();

        if bytes.len() + 1 > buf_size {
            *out_len = bytes.len();
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "out_buf too small for normalized action name",
            );
        }

        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf as *mut u8, bytes.len());
        *out_buf.add(bytes.len()) = 0;
        *out_len = bytes.len();
        AORTA_OK
    })
}

/// Subscribe-side counterpart of `aorta_node_validate_topic_name`.
/// Accepts the whole-segment Zenoh wildcards `*` and `**`; otherwise
/// runs the same per-segment rules.
///
/// C++ `Node::CreateSubscriber` calls this before spinning up the
/// internal executor so wildcard subscriptions (e.g. `camera/**`)
/// reach the actual subscriber constructor instead of being rejected
/// by the publish-side validator.
///
/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. The
/// caller retains ownership of all input buffers; this function does
/// not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_node_validate_topic_pattern(
    node: aorta_node_t,
    pattern: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-supplied `pattern` C-string under the NUL-termination
    // contract; `pattern` is null-checked at entry.
    ffi_guard(|| unsafe {
        if pattern.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "pattern is null");
        }

        let pattern = match CStr::from_ptr(pattern).to_str() {
            Ok(s) => s,
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in pattern"),
        };

        let node_obj = { registry().nodes.lock().get(&node).cloned() };
        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        match node_obj.key_builder().topic_key_sub(pattern) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
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
pub unsafe extern "C" fn aorta_node_validate_service_name(
    node: aorta_node_t,
    service_name: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-supplied `service_name` C-string under the NUL-termination
    // contract; `service_name` is null-checked at entry.
    ffi_guard(|| unsafe {
        if service_name.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "service_name is null");
        }

        let service_name = match CStr::from_ptr(service_name).to_str() {
            Ok(s) => s,
            Err(_) => {
                return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in service name")
            }
        };

        let node_obj = { registry().nodes.lock().get(&node).cloned() };
        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        // Server-side (concrete service) validator — wildcards rejected.
        // See `aorta_node_validate_service_pattern` for the client-side
        // variant that accepts query globs.
        match node_obj.key_builder().service_key_server(service_name) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
        }
    })
}

/// Client/query-side counterpart of `aorta_node_validate_service_name`.
/// Accepts whole-segment wildcards for fan-out queries.
///
/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. The
/// caller retains ownership of all input buffers; this function does
/// not free or take ownership of pointer arguments.
#[no_mangle]
pub unsafe extern "C" fn aorta_node_validate_service_pattern(
    node: aorta_node_t,
    pattern: *const c_char,
) -> aorta_result_t {
    // SAFETY: see fn-level `# Safety`. Inner `unsafe` covers `CStr::from_ptr`
    // on the caller-supplied `pattern` C-string under the NUL-termination
    // contract; `pattern` is null-checked at entry.
    ffi_guard(|| unsafe {
        if pattern.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "pattern is null");
        }

        let pattern = match CStr::from_ptr(pattern).to_str() {
            Ok(s) => s,
            Err(_) => return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "invalid UTF-8 in pattern"),
        };

        let node_obj = { registry().nodes.lock().get(&node).cloned() };
        let Some(node_obj) = node_obj else {
            return map_aorta_error(AortaError::not_found("node not found"));
        };

        match node_obj.key_builder().service_key_client(pattern) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
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
pub unsafe extern "C" fn aorta_node_create_callback_group(
    node: aorta_node_t,
    group: aorta_callback_group_t,
    out: *mut aorta_callback_group_handle_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }

        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        match node.create_callback_group(callback_group_from_ffi(group)) {
            Ok(handle) => {
                // SAFETY: `out` was null-checked above; caller contract
                // requires writable storage for `aorta_callback_group_handle_t`.
                unsafe {
                    *out = handle;
                }
                AORTA_OK
            }
            Err(err) => map_aorta_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_node_free_callback_group(
    node: aorta_node_t,
    callback_group: aorta_callback_group_handle_t,
) -> aorta_result_t {
    ffi_guard(|| {
        let node = { registry().nodes.lock().get(&node).cloned() };

        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        match node.remove_callback_group(callback_group) {
            Ok(true) => AORTA_OK,
            Ok(false) => map_aorta_error(AortaError::not_found("callback group handle not found")),
            Err(err) => map_aorta_error(err),
        }
    })
}
