// Sample FFI surface.
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
pub unsafe extern "C" fn aorta_sample_clone(
    data: *const u8,
    len: usize,
    ctx: *const aorta_msg_context_t,
    out: *mut aorta_sample_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        if ctx.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "ctx is null");
        }
        if data.is_null() && len > 0 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "data is null");
        }

        let payload = if len == 0 {
            Vec::new()
        } else {
            // SAFETY: `data` is non-null (checked when `len > 0`) and `len`
            // describes a readable byte buffer for the duration of the call
            // (FFI caller contract); the slice is immediately copied via
            // `to_vec()` so no lifetime tail escapes.
            unsafe { std::slice::from_raw_parts(data, len).to_vec() }
        };

        // SAFETY: `ctx` was null-checked at the top; caller contract requires
        // it point at a valid `aorta_msg_context_t` for the duration of this
        // call. Subsequent field access stays within that borrow.
        let ctx_ref = unsafe { &*ctx };
        // SAFETY: `ctx_ref.topic` is a `string_view_t` populated by an Aorta
        // FFI caller; `string_view_to_string` validates pointer + length.
        let topic = match unsafe { string_view_to_string(ctx_ref.topic, "topic") } {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        // SAFETY: same contract as the `topic` view above; the optional
        // variant tolerates a null pointer inside the view.
        let publisher_node = match unsafe {
            string_view_to_string_optional(ctx_ref.publisher_node, "publisher_node")
        } {
            Ok(value) => value,
            Err(err) => return map_aorta_error(err),
        };

        let entry = SampleEntry {
            payload,
            topic,
            receive_stamp_ns: ctx_ref.receive_stamp_ns,
            priority: ctx_ref.priority,
            publisher_node,
            sequence: ctx_ref.sequence,
        };

        let handle = alloc_handle();
        registry().samples.lock().insert(handle, entry);

        // SAFETY: `out` was null-checked above; caller contract requires it
        // point at writable storage for `aorta_sample_t`.
        unsafe {
            *out = handle;
        }

        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_sample_release(sample: aorta_sample_t) -> aorta_result_t {
    ffi_guard(|| {
        let removed = { registry().samples.lock().remove(&sample) };

        if removed.is_none() {
            return map_aorta_error(AortaError::not_found("sample handle not found"));
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
pub unsafe extern "C" fn aorta_sample_payload(
    sample: aorta_sample_t,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_ptr.is_null() || out_len.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_ptr/out_len is null");
        }

        let samples = registry().samples.lock();
        let Some(entry) = samples.get(&sample) else {
            return map_aorta_error(AortaError::not_found("sample handle not found"));
        };

        // SAFETY: both `out_ptr` and `out_len` were null-checked above; caller
        // contract requires writable storage for the respective types. The
        // returned `*const u8` borrows from the registry-owned `entry.payload`
        // and stays valid until the matching `aorta_sample_release`.
        unsafe {
            *out_ptr = entry.payload.as_ptr();
            *out_len = entry.payload.len();
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
pub unsafe extern "C" fn aorta_sample_context(
    sample: aorta_sample_t,
    out: *mut aorta_msg_context_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }

        let samples = registry().samples.lock();
        let Some(entry) = samples.get(&sample) else {
            return map_aorta_error(AortaError::not_found("sample handle not found"));
        };

        // SAFETY: `out` was null-checked above; caller contract requires it
        // point at writable storage for `aorta_msg_context_t`. The string
        // views inside the constructed struct borrow from registry-owned
        // `entry` strings and stay valid until `aorta_sample_release`.
        unsafe {
            *out = aorta_msg_context_t {
                topic: string_view_from_str(entry.topic.as_str()),
                receive_stamp_ns: entry.receive_stamp_ns,
                priority: entry.priority,
                publisher_node: string_view_from_optional(entry.publisher_node.as_deref()),
                sequence: entry.sequence,
            };
        }

        AORTA_OK
    })
}
