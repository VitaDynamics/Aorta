// Capture + SchemaResolver C-ABI surface.
//
// `aorta_capture_*`  — raw recorder/observer snoop (CaptureSubscriber) +
//                      one-shot ctx snapshot.
// `aorta_schema_resolver_*` — schema_hash/key → bfbs resolution.
//
// Every entry point dereferences caller-supplied raw pointers behind a
// documented `# Safety` contract; the `not_unsafe_ptr_arg_deref` lint is allowed
// module-wide rather than marking each `extern "C"` fn `unsafe` (which would not
// change the C ABI but would churn every Rust-side caller).
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use super::*;
use crate::capture::{
    capture_context_snapshot, CaptureEvent, CaptureKind, CaptureSelector, CaptureSubscriber,
    SnapshotDoneReason,
};
use crate::schema::SchemaHash;
use crate::schema_resolve::{ResolvedSchema, SchemaResolver, SchemaResolverConfig};
use std::sync::atomic::Ordering;
use std::time::Duration;

pub type aorta_capture_sub_t = u64;
pub type aorta_schema_resolver_t = u64;

// Selector kinds (mirror `CaptureSelector`).
pub const AORTA_CAPTURE_SELECTOR_TOPIC: i32 = 0;
pub const AORTA_CAPTURE_SELECTOR_CONTEXT: i32 = 1;
pub const AORTA_CAPTURE_SELECTOR_TELEMETRY: i32 = 2;
pub const AORTA_CAPTURE_SELECTOR_RPC_RECORD: i32 = 3;
pub const AORTA_CAPTURE_SELECTOR_RAW_KEYEXPR: i32 = 4;

// Sample kinds.
pub const AORTA_CAPTURE_KIND_LIVE: i32 = 0;
pub const AORTA_CAPTURE_KIND_SNAPSHOT: i32 = 1;

// SnapshotDone reasons.
pub const AORTA_SNAPSHOT_DONE_QUERY_DRAINED: i32 = 0;
pub const AORTA_SNAPSHOT_DONE_DEADLINE: i32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct aorta_traceparent_t {
    pub trace_id: [u8; 16],
    pub span_id: u64,
    pub flags: u8,
}

/// A captured message handed to `aorta_on_capture_cb`. All views are valid ONLY
/// for the duration of the callback — copy what you keep.
#[repr(C)]
pub struct aorta_captured_sample_t {
    pub key: aorta_string_view_t,
    pub key_suffix: aorta_string_view_t,
    pub payload: aorta_bytes_view_t,
    pub has_schema_hash: bool,
    /// Hex view (ptr+len), valid only when `has_schema_hash`. NOT a C-string.
    pub schema_hash: aorta_schema_hash_view_t,
    pub has_traceparent: bool,
    pub traceparent: aorta_traceparent_t,
    pub has_publish_time: bool,
    pub publish_time_ns: u64,
    pub recv_time_ns: u64,
    pub kind: i32,
}

/// `is_snapshot_done` is the SINGLE authoritative discriminator. When `false`,
/// `sample` is non-NULL and `done_reason` is unspecified; when `true`, this is a
/// SnapshotDone — `sample` is NULL (ignore it) and `done_reason` is set.
/// Wrappers branch on `is_snapshot_done` only.
pub type aorta_on_capture_cb = Option<
    unsafe extern "C" fn(
        sample: *const aorta_captured_sample_t,
        is_snapshot_done: bool,
        done_reason: i32,
        user_data: *mut c_void,
    ),
>;

#[repr(C)]
pub struct aorta_capture_selector_t {
    pub kind: i32,
    pub pattern: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct aorta_schema_resolver_opts_t {
    pub cache_max_entries: usize,
    pub by_hash_timeout_ms: u32,
    pub by_key_timeout_ms: u32,
}

/// Owns its backing buffers (`internal`); the views point into them and stay
/// valid until `aorta_resolved_schema_free`.
#[repr(C)]
pub struct aorta_resolved_schema_t {
    pub schema_hash: aorta_schema_hash_view_t,
    pub root_type_name: aorta_string_view_t,
    pub bfbs: aorta_bytes_view_t,
    pub internal: *mut c_void,
}

pub(super) struct FfiCaptureEntry {
    pub(super) _subscriber: CaptureSubscriber,
    pub(super) in_flight: Arc<AtomicUsize>,
}

// Heap-owned backing for an `aorta_resolved_schema_t` (stable addresses).
struct ResolvedSchemaOwned {
    hash_hex: String,
    root_type_name: String,
    bfbs: Vec<u8>,
    slice: aorta_bytes_slice_t,
}

// ---------------------------------------------------------------------------
// Capture subscriber
// ---------------------------------------------------------------------------

/// # Safety
/// `selectors` must point at `n_selectors` valid `aorta_capture_selector_t`,
/// each with a NUL-terminated `pattern`. `out` must be writable. `cb` must not
/// unwind across the FFI boundary and its `user_data` must outlive the
/// subscriber (the caller owns callback-state lifetime).
#[no_mangle]
pub extern "C" fn aorta_capture_subscriber_new(
    node: aorta_node_t,
    selectors: *const aorta_capture_selector_t,
    n_selectors: usize,
    cb: aorta_on_capture_cb,
    user_data: *mut c_void,
    out: *mut aorta_capture_sub_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let Some(cb) = cb else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
        };
        if selectors.is_null() || n_selectors == 0 {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "no selectors provided");
        }

        let parsed = match parse_selectors(selectors, n_selectors) {
            Ok(v) => v,
            Err(err) => return map_aorta_error(err),
        };

        let node = { registry().nodes.lock().get(&node).cloned() };
        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        let user_data = user_data as usize;
        let in_flight = Arc::new(AtomicUsize::new(0));
        let in_flight_cb = Arc::clone(&in_flight);

        let result = crate::ffi_block_on!(CaptureSubscriber::new(
            node.session().inner(),
            node.key_builder(),
            &parsed,
            move |event| {
                let user_data = user_data as *mut c_void;
                in_flight_cb.fetch_add(1, Ordering::AcqRel);
                let _drain_guard = InFlightGuard(&in_flight_cb);
                dispatch_capture_event(cb, user_data, event);
            },
        ));

        let subscriber = match result {
            Ok(Ok(sub)) => sub,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };

        let handle = alloc_handle();
        registry().capture_subscribers.lock().insert(
            handle,
            FfiCaptureEntry {
                _subscriber: subscriber,
                in_flight,
            },
        );
        // SAFETY: `out` null-checked above.
        unsafe {
            *out = handle;
        }
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_capture_subscriber_free(handle: aorta_capture_sub_t) -> aorta_result_t {
    ffi_guard(|| {
        let entry = { registry().capture_subscribers.lock().remove(&handle) };
        let Some(FfiCaptureEntry {
            _subscriber,
            in_flight,
        }) = entry
        else {
            return map_aorta_error(AortaError::not_found("capture subscriber handle not found"));
        };

        // Dropping the subscriber undeclares the Zenoh subscribers (no NEW
        // dispatches). An already-entered callback may still be running: drain
        // it (bounded), UNLESS we are inside an async/callback context where
        // blocking would deadlock — then we leak-on-timeout (UAF-safe, not
        // leak-free; mirrors `aorta_subscriber_free`).
        drop(_subscriber);
        if in_async_context() {
            return AORTA_OK;
        }
        if !drain_in_flight(&in_flight, FREE_DRAIN_TIMEOUT) {
            aorta_core_warn!(
                "aorta_capture_subscriber_free: timed out draining in-flight callbacks; \
                 wrapper must fall back to wait+leak"
            );
        }
        AORTA_OK
    })
}

/// One-shot ctx snapshot capture. Blocks until `SnapshotDone` is
/// delivered. The callback receives `Snapshot` samples then exactly one
/// `is_snapshot_done=true`.
///
/// # Safety
/// `pattern` must be a NUL-terminated string; `cb` must not unwind.
#[no_mangle]
pub extern "C" fn aorta_capture_context_snapshot(
    node: aorta_node_t,
    pattern: *const c_char,
    deadline_ms: u32,
    cb: aorta_on_capture_cb,
    user_data: *mut c_void,
) -> aorta_result_t {
    ffi_guard(|| {
        let Some(cb) = cb else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
        };
        // SAFETY: caller contract — NUL-terminated UTF-8.
        let pattern = match unsafe { cstr_to_string(pattern, "pattern") } {
            Ok(v) => v,
            Err(err) => return map_aorta_error(err),
        };
        let node = { registry().nodes.lock().get(&node).cloned() };
        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };
        let user_data = user_data as usize;

        let result = crate::ffi_block_on!(capture_context_snapshot(
            node.session().inner(),
            node.key_builder(),
            &pattern,
            Duration::from_millis(deadline_ms as u64),
            move |event| {
                let user_data = user_data as *mut c_void;
                dispatch_capture_event(cb, user_data, event);
            },
        ));
        match result {
            Ok(Ok(())) => AORTA_OK,
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

// ---------------------------------------------------------------------------
// Schema resolver
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn aorta_schema_resolver_new(
    node: aorta_node_t,
    opts: *const aorta_schema_resolver_opts_t,
    out: *mut aorta_schema_resolver_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let node = { registry().nodes.lock().get(&node).cloned() };
        let Some(node) = node else {
            return map_aorta_error(AortaError::not_found("node handle not found"));
        };

        let mut config = SchemaResolverConfig::default();
        // SAFETY: optional pointer; `as_ref` handles null.
        if let Some(opts) = unsafe { opts.as_ref() } {
            if opts.cache_max_entries > 0 {
                config.cache_max_entries = opts.cache_max_entries;
            }
            if opts.by_hash_timeout_ms > 0 {
                config.by_hash_timeout = Duration::from_millis(opts.by_hash_timeout_ms as u64);
            }
            if opts.by_key_timeout_ms > 0 {
                config.by_key_timeout = Duration::from_millis(opts.by_key_timeout_ms as u64);
            }
        }

        let resolver = SchemaResolver::new(
            node.session().inner().clone(),
            node.group().to_string(),
            config,
        );
        let handle = alloc_handle();
        registry()
            .schema_resolvers
            .lock()
            .insert(handle, Arc::new(resolver));
        // SAFETY: `out` null-checked above.
        unsafe {
            *out = handle;
        }
        AORTA_OK
    })
}

/// # Safety
/// `hash.hex` must point at `hash.len` hex bytes; `out` must be writable and is
/// owned by the caller until `aorta_resolved_schema_free`.
#[no_mangle]
pub extern "C" fn aorta_schema_resolver_resolve_by_hash(
    handle: aorta_schema_resolver_t,
    hash: aorta_schema_hash_view_t,
    out: *mut aorta_resolved_schema_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let hash_str = match hash_view_to_str(&hash) {
            Ok(v) => v,
            Err(err) => return map_aorta_error(err),
        };
        let Some(parsed) = SchemaHash::from_hex_str(&hash_str) else {
            return aorta_err(
                AORTA_ERR_INVALID_ARGUMENT,
                "schema hash is not 64 lowercase hex",
            );
        };
        let resolver = { registry().schema_resolvers.lock().get(&handle).cloned() };
        let Some(resolver) = resolver else {
            return map_aorta_error(AortaError::not_found("schema resolver handle not found"));
        };
        match crate::ffi_block_on!(async move { resolver.resolve_by_hash(&parsed).await }) {
            Ok(Ok(resolved)) => fill_resolved(out, resolved),
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

/// # Safety
/// `key_suffix` must be a NUL-terminated string; `out` is owned by the caller
/// until `aorta_resolved_schema_free`.
#[no_mangle]
pub extern "C" fn aorta_schema_resolver_resolve_by_key(
    handle: aorta_schema_resolver_t,
    key_suffix: *const c_char,
    out: *mut aorta_resolved_schema_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: caller contract — `key_suffix` is a NUL-terminated string.
        let key = match unsafe { cstr_to_string(key_suffix, "key_suffix") } {
            Ok(v) => v,
            Err(err) => return map_aorta_error(err),
        };
        let resolver = { registry().schema_resolvers.lock().get(&handle).cloned() };
        let Some(resolver) = resolver else {
            return map_aorta_error(AortaError::not_found("schema resolver handle not found"));
        };
        match crate::ffi_block_on!(async move { resolver.resolve_by_key(&key).await }) {
            Ok(Ok(resolved)) => fill_resolved(out, resolved),
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

/// # Safety
/// `s` must be a pointer previously filled by a successful `resolve_*` call and
/// not yet freed. Idempotent: a second call (after `internal` is nulled) is a
/// no-op.
#[no_mangle]
pub extern "C" fn aorta_resolved_schema_free(s: *mut aorta_resolved_schema_t) -> aorta_result_t {
    ffi_guard(|| {
        if s.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "schema pointer is null");
        }
        // SAFETY: `s` is non-null (checked); `internal` is either null or a
        // `Box<ResolvedSchemaOwned>` we created in `fill_resolved`.
        unsafe {
            let internal = (*s).internal;
            if !internal.is_null() {
                drop(Box::from_raw(internal as *mut ResolvedSchemaOwned));
                // Null every view so a defensive after-free read sees null views
                // rather than pointers into the just-freed box (mirrors
                // `aorta_owned_bytes_free`); preserves idempotency (a second
                // free sees `internal == null` and bails).
                (*s).schema_hash = aorta_schema_hash_view_t {
                    hex: std::ptr::null(),
                    len: 0,
                };
                (*s).root_type_name = aorta_string_view_t {
                    data: std::ptr::null(),
                    len: 0,
                };
                (*s).bfbs = aorta_bytes_view_t::default();
                (*s).internal = std::ptr::null_mut();
            }
        }
        AORTA_OK
    })
}

/// Drop the mutable key→schema cache (e.g. after a known re-registration). The
/// content-addressed by-hash cache is left intact (always valid).
#[no_mangle]
pub extern "C" fn aorta_schema_resolver_clear_key_cache(
    handle: aorta_schema_resolver_t,
) -> aorta_result_t {
    ffi_guard(|| {
        let resolver = { registry().schema_resolvers.lock().get(&handle).cloned() };
        match resolver {
            Some(resolver) => {
                resolver.clear_key_cache();
                AORTA_OK
            }
            None => map_aorta_error(AortaError::not_found("schema resolver handle not found")),
        }
    })
}

#[no_mangle]
pub extern "C" fn aorta_schema_resolver_free(handle: aorta_schema_resolver_t) -> aorta_result_t {
    ffi_guard(|| {
        if registry().schema_resolvers.lock().remove(&handle).is_some() {
            AORTA_OK
        } else {
            map_aorta_error(AortaError::not_found("schema resolver handle not found"))
        }
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_selectors(
    selectors: *const aorta_capture_selector_t,
    n: usize,
) -> AortaResult<Vec<CaptureSelector>> {
    // SAFETY: caller contract — `selectors` points at `n` valid entries.
    let slice = unsafe { std::slice::from_raw_parts(selectors, n) };
    let mut out = Vec::with_capacity(n);
    for sel in slice {
        // SAFETY: each `pattern` is a NUL-terminated string per the contract.
        let pattern = unsafe { cstr_to_string(sel.pattern, "selector pattern") }?;
        out.push(match sel.kind {
            AORTA_CAPTURE_SELECTOR_TOPIC => CaptureSelector::Topic(pattern),
            AORTA_CAPTURE_SELECTOR_CONTEXT => CaptureSelector::Context(pattern),
            AORTA_CAPTURE_SELECTOR_TELEMETRY => CaptureSelector::Telemetry(pattern),
            AORTA_CAPTURE_SELECTOR_RPC_RECORD => CaptureSelector::RpcRecord(pattern),
            AORTA_CAPTURE_SELECTOR_RAW_KEYEXPR => CaptureSelector::RawKeyExpr(pattern),
            other => {
                return Err(AortaError::invalid_argument(format!(
                    "unknown capture selector kind: {}",
                    other
                )))
            }
        });
    }
    Ok(out)
}

fn hash_view_to_str(hash: &aorta_schema_hash_view_t) -> AortaResult<String> {
    if hash.hex.is_null() || hash.len == 0 {
        return Err(AortaError::invalid_argument("schema hash view is empty"));
    }
    // SAFETY: caller contract — `hex` points at `len` valid bytes.
    let bytes = unsafe { std::slice::from_raw_parts(hash.hex, hash.len) };
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|_| AortaError::invalid_argument("schema hash view is not UTF-8"))
}

/// Build the C sample view as locals and invoke `cb`. Borrows live for the call.
fn dispatch_capture_event(
    cb: unsafe extern "C" fn(*const aorta_captured_sample_t, bool, i32, *mut c_void),
    user_data: *mut c_void,
    event: &CaptureEvent<'_>,
) {
    // Explicit reentrancy detector: capture callbacks run
    // on a Zenoh RX / runtime thread — a blocking FFI re-entry from them
    // must classify reentrant, not silently block the transport thread.
    let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
    match event {
        CaptureEvent::Sample { kind, sample } => {
            let hash_hex: Option<String> = sample.schema_hash.map(|h| h.to_hex());
            let schema_hash = match &hash_hex {
                Some(s) => aorta_schema_hash_view_t {
                    hex: s.as_ptr(),
                    len: s.len(),
                },
                None => aorta_schema_hash_view_t {
                    hex: std::ptr::null(),
                    len: 0,
                },
            };
            let payload_slice = aorta_bytes_slice_t {
                data: sample.payload.as_ptr(),
                len: sample.payload.len(),
            };
            let payload = aorta_bytes_view_t {
                slices: std::ptr::addr_of!(payload_slice),
                slice_count: 1,
                total_len: sample.payload.len(),
            };
            let traceparent = sample
                .traceparent
                .map(|t| aorta_traceparent_t {
                    trace_id: t.trace_id,
                    span_id: t.span_id,
                    flags: t.flags,
                })
                .unwrap_or_default();
            let c_sample = aorta_captured_sample_t {
                key: string_view_from_str(sample.key),
                key_suffix: string_view_from_str(sample.key_suffix),
                payload,
                has_schema_hash: hash_hex.is_some(),
                schema_hash,
                has_traceparent: sample.traceparent.is_some(),
                traceparent,
                has_publish_time: sample.publish_time_ns.is_some(),
                publish_time_ns: sample.publish_time_ns.unwrap_or(0),
                recv_time_ns: sample.recv_time_ns,
                kind: match kind {
                    CaptureKind::Live => AORTA_CAPTURE_KIND_LIVE,
                    CaptureKind::Snapshot => AORTA_CAPTURE_KIND_SNAPSHOT,
                },
            };
            // ffi_guard_void converts a panic in OUR marshaling into a normal
            // return; a panic INSIDE the user's `cb` aborts (extern "C" nounwind).
            // SAFETY: `cb` is the caller-registered, non-null callback; the
            // `c_sample` view and its borrows are valid for this synchronous call.
            ffi_guard_void(|| unsafe {
                cb(std::ptr::addr_of!(c_sample), false, 0, user_data);
            });
        }
        CaptureEvent::SnapshotDone { reason } => {
            let r = match reason {
                SnapshotDoneReason::QueryDrained => AORTA_SNAPSHOT_DONE_QUERY_DRAINED,
                SnapshotDoneReason::DeadlineElapsed => AORTA_SNAPSHOT_DONE_DEADLINE,
            };
            // SAFETY: `cb` is the caller-registered, non-null callback; a
            // SnapshotDone passes a NULL sample per the documented contract.
            ffi_guard_void(|| unsafe {
                cb(std::ptr::null(), true, r, user_data);
            });
        }
    }
}

fn fill_resolved(out: *mut aorta_resolved_schema_t, resolved: ResolvedSchema) -> aorta_result_t {
    let owned = Box::new(ResolvedSchemaOwned {
        hash_hex: resolved.schema_hash.to_hex(),
        root_type_name: resolved.root_type_name,
        bfbs: resolved.bfbs,
        slice: aorta_bytes_slice_t::default(),
    });
    let owned_ptr = Box::into_raw(owned);
    // SAFETY: `owned_ptr` is a fresh, uniquely-owned heap allocation with no
    // aliases yet, so a unique reborrow is sound. We fill `slice` to point into
    // the box's `bfbs` (stable while boxed), then build views over the box's
    // stable String/Vec buffers. `out` is non-null (checked by callers).
    unsafe {
        let o = &mut *owned_ptr;
        o.slice = aorta_bytes_slice_t {
            data: o.bfbs.as_ptr(),
            len: o.bfbs.len(),
        };
        *out = aorta_resolved_schema_t {
            schema_hash: aorta_schema_hash_view_t {
                hex: o.hash_hex.as_ptr(),
                len: o.hash_hex.len(),
            },
            root_type_name: string_view_from_str(&o.root_type_name),
            bfbs: aorta_bytes_view_t {
                slices: std::ptr::addr_of!(o.slice),
                slice_count: 1,
                total_len: o.bfbs.len(),
            },
            internal: owned_ptr as *mut c_void,
        };
    }
    AORTA_OK
}
