// Publisher FFI surface.
//
// Extracted from c_api/mod.rs.

use super::*;

#[no_mangle]
pub extern "C" fn aorta_publisher_new(
    node: aorta_node_t,
    topic: *const c_char,
    qos: *const aorta_qos_t,
    opts: *const aorta_pub_options_t,
    out: *mut aorta_pub_t,
) -> aorta_result_t {
    ffi_guard(|| aorta_publisher_new_impl(node, topic, qos, opts, out))
}

fn aorta_publisher_new_impl(
    node: aorta_node_t,
    topic: *const c_char,
    qos: *const aorta_qos_t,
    opts: *const aorta_pub_options_t,
    out: *mut aorta_pub_t,
) -> aorta_result_t {
    if out.is_null() {
        return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
    }

    // SAFETY: `topic` is the FFI caller-supplied `*const c_char`;
    // `cstr_to_string` null-checks and validates UTF-8 under the
    // NUL-termination contract.
    let topic = unsafe { cstr_to_string(topic, "topic") };
    let topic = match topic {
        Ok(value) => value,
        Err(err) => return map_aorta_error(err),
    };

    let node = { registry().nodes.lock().get(&node).cloned() };

    let Some(node) = node else {
        return map_aorta_error(AortaError::not_found("node handle not found"));
    };

    // SAFETY: `qos` and `opts` are FFI caller-supplied optional pointers;
    // `publisher_options_from_ffi` null-checks each and dereferences under
    // the caller's pointer-validity contract.
    let options = match unsafe { publisher_options_from_ffi(qos, opts) } {
        Ok(options) => options,
        Err(err) => return map_aorta_error(err),
    };

    let publisher = match crate::ffi_block_on!(Publisher::new_with_options(&node, &topic, options))
    {
        Ok(Ok(publisher)) => publisher,
        Ok(Err(err)) => return map_aorta_error(err),
        Err(_) => return aorta_reentrant(module_path!()),
    };

    let handle = alloc_handle();
    registry()
        .publishers
        .lock()
        .insert(handle, Arc::new(publisher));

    // SAFETY: `out` was null-checked at entry; caller contract requires it
    // point at writable storage for `aorta_pub_t`.
    unsafe {
        *out = handle;
    }

    AORTA_OK
}

/// Look up a publisher by handle, using the thread-local single-slot cache
/// to avoid locking the global registry on the hot path.
fn resolve_publisher(pub_handle: aorta_pub_t) -> Option<Arc<Publisher>> {
    // Fast path: try the thread-local single-slot cache first.
    let cached: Option<Arc<Publisher>> = PUB_CACHE
        .try_with(|cell| {
            let cache = cell.borrow();
            if let Some((cached_handle, weak)) = cache.as_ref() {
                if *cached_handle == pub_handle {
                    return weak.upgrade();
                }
            }
            None
        })
        .ok()
        .flatten();

    if let Some(p) = cached {
        return Some(p);
    }

    // Slow path: lock global registry, clone Arc, populate TLS cache.
    let arc = { registry().publishers.lock().get(&pub_handle).cloned() }?;

    // A publisher may be freed from a static destructor after main-thread TLS
    // teardown. The registry remains authoritative; skip the cache when its
    // slot is no longer available instead of reinitializing thread state.
    let _ = PUB_CACHE.try_with(|cell| {
        *cell.borrow_mut() = Some((pub_handle, Arc::downgrade(&arc)));
    });

    Some(arc)
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
pub unsafe extern "C" fn aorta_publisher_publish(
    pub_handle: aorta_pub_t,
    data: *const u8,
    len: usize,
) -> aorta_result_t {
    // Hot path. We previously skipped ffi_guard here on a "panic-free body"
    // claim, but that claim does not hold: `to_vec()` can panic on allocator failure and
    // `publisher.publish()` can hit unwind paths in the zenoh transport.
    // ffi_guard's catch_unwind is sub-ns on the success path with the
    // `#[inline]` attribute, well below the per-publish budget of this
    // call. Wrap for the same safety guarantee as every other entry point.
    ffi_guard(|| {
        if data.is_null() && len > 0 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "data is null");
        }

        let Some(publisher) = resolve_publisher(pub_handle) else {
            return map_aorta_error(AortaError::not_found("publisher handle not found"));
        };

        let payload = if len == 0 {
            Vec::new()
        } else {
            // SAFETY: `data` is non-null (checked when `len > 0`) and `len`
            // describes a readable byte buffer for the duration of the call
            // (FFI caller contract); the slice is immediately copied via
            // `to_vec()` so no borrow escapes.
            unsafe { std::slice::from_raw_parts(data, len).to_vec() }
        };

        match publisher.publish(payload) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
        }
    })
}

/// Zero-copy publish. Ownership of each slice that carries a `release` callback
/// is transferred to Zenoh; the callback fires after the payload has been sent.
/// Slices without a `release` callback are memcpied as a fallback.
///
/// Uses the same thread-local publisher cache as `aorta_publisher_publish`.
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
pub unsafe extern "C" fn aorta_publisher_publish_owned(
    pub_handle: aorta_pub_t,
    payload: *const aorta_owned_bytes_view_t,
) -> aorta_result_t {
    // Same hot-path / safety reasoning as aorta_publisher_publish above.
    // The drop chain on the borrowed `aorta_owned_bytes_view_t` can also
    // invoke a user-supplied release callback (see ForeignOwnedSlice::drop),
    // which is precisely the kind of path ffi_guard exists to wrap.
    ffi_guard(|| {
        // SAFETY: `payload` is the FFI caller-supplied owned-bytes view;
        // `zbytes_from_owned_ffi_view` null-checks and validates the slice
        // table under the caller's pointer-validity contract.
        let zbytes = match unsafe { zbytes_from_owned_ffi_view(payload, "payload") } {
            Ok(z) => z,
            Err(err) => return map_aorta_error(err),
        };

        let Some(publisher) = resolve_publisher(pub_handle) else {
            return map_aorta_error(AortaError::not_found("publisher handle not found"));
        };

        match publisher.publish(zbytes) {
            Ok(_) => AORTA_OK,
            Err(err) => map_aorta_error(err),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_publisher_free(pub_handle: aorta_pub_t) -> aorta_result_t {
    ffi_guard(|| {
        let publisher = { registry().publishers.lock().remove(&pub_handle) };

        let Some(arc) = publisher else {
            return map_aorta_error(AortaError::not_found("publisher handle not found"));
        };

        // Try to unwrap the Arc. If this thread's TLS cache (or a concurrent
        // publish on another thread) still holds a temporary strong reference,
        // try_unwrap will fail. In that case the Publisher will be dropped when
        // the last Arc goes out of scope, and the zenoh publisher will be
        // undeclared automatically on drop.
        match Arc::try_unwrap(arc) {
            Ok(publisher) => {
                // See `aorta_node_free`: FREE paths use spawn-and-detach
                // on reentrant invocation rather than returning REENTRANT,
                // because the registry entry has already been removed.
                if in_async_context() {
                    runtime().spawn(async move {
                        let _ = publisher.undeclare().await;
                    });
                    return AORTA_OK;
                }
                match runtime().block_on(publisher.undeclare()) {
                    Ok(_) => AORTA_OK,
                    Err(err) => map_aorta_error(err),
                }
            }
            Err(_arc) => {
                // Cannot unwrap — concurrent references exist. The publisher
                // will be cleaned up when the last reference is dropped.
                AORTA_OK
            }
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
pub unsafe extern "C" fn aorta_publisher_get_stats(
    pub_handle: aorta_pub_t,
    out: *mut aorta_publisher_stats_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return map_aorta_error(AortaError::invalid_argument("out is null"));
        }

        let publisher = { registry().publishers.lock().get(&pub_handle).cloned() };

        let Some(publisher) = publisher else {
            return map_aorta_error(AortaError::not_found("publisher handle not found"));
        };

        let stats = publisher_stats_from_core(&publisher);
        // SAFETY: `out` was null-checked above; caller contract requires it
        // point at writable storage for `aorta_publisher_stats_t`.
        unsafe {
            *out = stats;
        }
        AORTA_OK
    })
}

/// One-shot matching-status check: writes `1` to `*out_matching` if at least
/// one matching subscriber currently exists for the publisher's key
/// expression, `0` otherwise.
///
/// # Safety
///
/// `out_matching` must be non-null and point at writable storage for a `u8`.
/// `pub_handle` must originate from `aorta_publisher_new*` and not have been
/// freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_publisher_matching_status(
    pub_handle: aorta_pub_t,
    out_matching: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_matching.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_matching is null");
        }

        let Some(publisher) = resolve_publisher(pub_handle) else {
            return map_aorta_error(AortaError::not_found("publisher handle not found"));
        };

        match crate::ffi_block_on!(publisher.matching_status()) {
            Ok(Ok(matching)) => {
                // SAFETY: `out_matching` was null-checked above.
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

/// Wait (bounded by `timeout_ms`) until a matching subscriber appears, then
/// write `1`/`0` to `*out_matching`. A timeout writes `0` and returns
/// `AORTA_OK` (not an error) so callers can "fire anyway".
///
/// # Safety
///
/// `out_matching` must be non-null and point at writable storage for a `u8`.
/// `pub_handle` must originate from `aorta_publisher_new*` and not have been
/// freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_publisher_wait_for_matching(
    pub_handle: aorta_pub_t,
    timeout_ms: u64,
    out_matching: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_matching.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_matching is null");
        }

        let Some(publisher) = resolve_publisher(pub_handle) else {
            return map_aorta_error(AortaError::not_found("publisher handle not found"));
        };

        let timeout = std::time::Duration::from_millis(timeout_ms);
        match crate::ffi_block_on!(publisher.wait_for_matching(timeout)) {
            Ok(Ok(matching)) => {
                // SAFETY: `out_matching` was null-checked above.
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
