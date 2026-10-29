// ContextStore FFI surface.
//
// Extracted from c_api/mod.rs. The split is a
// pure code-move: every `pub extern "C" fn` keeps the same `#[no_mangle]`
// symbol, and `c_api/mod.rs` re-exports `contextstore::*` so any caller
// of `crate::ffi::c_api::aorta_ctx_*` still resolves.

use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::sync::Arc;

use super::*;
use crate::error::AortaError;
use crate::ffi::guard::{ffi_guard, ffi_guard_void};
use crate::ffi::runtime_helper::{in_async_context, runtime};

// ============================================================================

pub type aorta_ctx_t = u64;
pub type aorta_ctx_watch_t = u64;

/// Test-only panic seam: when set, the NEXT
/// `aorta_ctx_watch_free` / `aorta_ctx_owner_liveliness_watch_free`
/// panics immediately after removing the registry entry (before the
/// shutdown) — deterministically reproducing the AORTA_ERR_INTERNAL
/// path where the entry is already gone but the Rust watch is only
/// best-effort-dropped, so a retry would see NOT_FOUND while a
/// callback may still be in flight. One-shot (consumed on the first
/// free).
#[cfg(test)]
pub(crate) static TEST_PANIC_AFTER_WATCH_REMOVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// `expires_at_ns` sentinel meaning "no expiry" (matches the wire schema).
pub const AORTA_CTX_NO_EXPIRY: u64 = 0;

// ContextChange.kind discriminants. Mirror `ContextChangeKind`.
pub const AORTA_CTX_CHANGE_LIVE: u8 = 0;
pub const AORTA_CTX_CHANGE_SNAPSHOT: u8 = 1;
pub const AORTA_CTX_CHANGE_SNAPSHOT_DONE: u8 = 2;

// SnapshotDoneReason discriminants. Mirror `SnapshotDoneReason`.
pub const AORTA_CTX_SNAPSHOT_DONE_QUERY_DRAINED: u8 = 0;
pub const AORTA_CTX_SNAPSHOT_DONE_DEADLINE_ELAPSED: u8 = 1;
// Sentinel used in change events whose kind != SnapshotDone.
pub const AORTA_CTX_SNAPSHOT_DONE_NONE: u8 = 0xff;

// ContextKind discriminants. Mirror `ContextKind`.
pub const AORTA_CTX_KIND_NONE: u8 = 0;
pub const AORTA_CTX_KIND_BOOL: u8 = 1;
pub const AORTA_CTX_KIND_I64: u8 = 2;
pub const AORTA_CTX_KIND_U64: u8 = 3;
pub const AORTA_CTX_KIND_F64: u8 = 4;
pub const AORTA_CTX_KIND_STRING: u8 = 5;

/// Out-parameter container for `aorta_ctx_get_bool` / `_i64` / `_u64` /
/// `_f64` / `_string`.
///
/// # Lifecycle contract
///
/// 1. **First use — zero-initialize.** Before the address of an
///    `aorta_ctx_read_t` is passed to any `aorta_ctx_get_*` or to
///    `aorta_ctx_read_free` for the first time, the caller MUST
///    zero-initialize it. The portable forms are
///    `aorta_ctx_read_t r = {0};` in C11/C17,
///    `aorta_ctx_read_t r{};` in C++11+, and
///    `aorta_ctx_read_t::default()` in Rust.
///
///    The reason is **failure-path safety**, not anything the getter's
///    success path reads: every `aorta_ctx_get_*` may early-return
///    (invalid handle, key parse failure, transport error, REENTRANT)
///    *without* having touched the three owned-pointer fields
///    (`string_value`, `source_node`, `source_instance_id`). If the
///    caller then invokes `aorta_ctx_read_free` to tidy up, that
///    function will `CString::from_raw()` whichever pointers are
///    non-null. Uninitialized stack memory there is undefined behavior;
///    `{0}` / `{}` / `default()` guarantees they start as `NULL`, which
///    `aorta_ctx_read_free` treats as a no-op.
///
/// 2. **Between consecutive calls — `aorta_ctx_read_free` first.**
///    A successful `aorta_ctx_get_*` populates the three pointer
///    fields with crate-owned `CString::into_raw()` allocations. Before
///    the next `aorta_ctx_get_*`, the caller MUST invoke
///    `aorta_ctx_read_free` to release them. The struct is then back to
///    its zero-initialized state and may be reused.
///
///    Skipping `aorta_ctx_read_free` here leaks the prior strings —
///    the getter overwrites the pointer fields without freeing them.
///    Auto-reclaim would require either trusting (1) on the success
///    path (which we do, but is fragile to refactor) or adding an ABI
///    sentinel field — deferred to a follow-up PR.
///
/// 3. **Final cleanup — `aorta_ctx_read_free` once more.** After the
///    last successful `aorta_ctx_get_*`, the caller MUST invoke
///    `aorta_ctx_read_free` to release the last set of strings.
#[repr(C)]
pub struct aorta_ctx_read_t {
    /// One of `AORTA_CTX_KIND_*`. Always populated.
    pub value_kind: u8,
    pub bool_value: bool,
    pub i64_value: i64,
    pub u64_value: u64,
    pub f64_value: f64,
    /// Heap-allocated NUL-terminated UTF-8 string when value_kind ==
    /// AORTA_CTX_KIND_STRING; NULL otherwise. Caller MUST eventually
    /// invoke `aorta_ctx_read_free` to release this and the metadata
    /// strings below.
    pub string_value: *mut c_char,
    pub is_fresh: bool,
    pub stamp_ns: u64,
    pub expires_at_ns: u64,
    /// Heap-allocated NUL-terminated UTF-8 string. Free via
    /// `aorta_ctx_read_free`.
    pub source_node: *mut c_char,
    /// Heap-allocated NUL-terminated UTF-8 string. Free via
    /// `aorta_ctx_read_free`.
    pub source_instance_id: *mut c_char,
    pub sequence: u64,
}

impl Default for aorta_ctx_read_t {
    fn default() -> Self {
        Self {
            value_kind: AORTA_CTX_KIND_NONE,
            bool_value: false,
            i64_value: 0,
            u64_value: 0,
            f64_value: 0.0,
            string_value: std::ptr::null_mut(),
            is_fresh: false,
            stamp_ns: 0,
            expires_at_ns: 0,
            source_node: std::ptr::null_mut(),
            source_instance_id: std::ptr::null_mut(),
            sequence: 0,
        }
    }
}

#[repr(C)]
pub struct aorta_ctx_change_t {
    /// One of `AORTA_CTX_CHANGE_LIVE | _SNAPSHOT | _SNAPSHOT_DONE`.
    pub kind: u8,
    /// Valid only when kind == AORTA_CTX_CHANGE_SNAPSHOT_DONE; otherwise
    /// equals AORTA_CTX_SNAPSHOT_DONE_NONE.
    pub snapshot_done_reason: u8,
    pub key: aorta_string_view_t,
    pub value_kind: u8,
    pub bool_value: bool,
    pub i64_value: i64,
    pub u64_value: u64,
    pub f64_value: f64,
    pub string_value: aorta_string_view_t,
    pub stamp_ns: u64,
    pub expires_at_ns: u64,
    pub source_node: aorta_string_view_t,
    pub source_instance_id: aorta_string_view_t,
    pub sequence: u64,
}

/// User callback invoked for every `ContextChange` (Live, Snapshot, or
/// SnapshotDone) the SDK delivers to a watch.
///
/// Threading contract:
/// - The callback may fire on any Zenoh worker thread; concurrent
///   invocations on different keys are possible. The user is
///   responsible for synchronising any shared `user_data` state.
/// - All `aorta_string_view_t` pointers in `*change` (key, string_value,
///   source_node, source_instance_id) are NOT NUL-terminated. Use
///   `(data, len)` as a byte slice. The pointers are valid only for
///   the duration of the callback; copy out anything you need to
///   retain.
/// - For `kind == AORTA_CTX_CHANGE_SNAPSHOT_DONE`, the value-bearing
///   fields are zeroed/empty and `snapshot_done_reason` carries the
///   reason. Other kinds set `snapshot_done_reason ==
///   AORTA_CTX_SNAPSHOT_DONE_NONE`.
/// - Drain guarantee is scoped to AORTA_OK: when
///   `aorta_ctx_watch_free` returns AORTA_OK, no further callback fires
///   for that watch — the FFI awaited the undeclare AND the in-flight
///   callbacks. On ANY non-OK result the drain is UNCONFIRMED: the
///   callback may still fire with this `user_data`, so the caller MUST
///   keep `user_data` (and everything it points at) alive. Drop without
///   `_free` is best-effort only.
pub type aorta_ctx_watch_callback_t =
    Option<unsafe extern "C" fn(*const aorta_ctx_change_t, *mut c_void)>;

/// Wrapper that lets us move a raw C function pointer + opaque user
/// pointer into a `Send + Sync` Fn closure. Caller is responsible for
/// guaranteeing user_data is safe to share across threads.
struct CtxWatchCallback {
    callback: unsafe extern "C" fn(*const aorta_ctx_change_t, *mut c_void),
    user_data: *mut c_void,
}

// SAFETY: `callback` is an `extern "C" fn` pointer and is therefore
// inherently Send + Sync (function pointers contain no thread-affine
// state). `user_data` is `*mut c_void` — opaque to Rust and treated as
// thread-safe by API contract: the C caller of `aorta_ctx_watch / aorta_ctx_watch_with_snapshot` is
// responsible for ensuring the data pointed to by `user_data` is either
// thread-safe or externally synchronized, since the watch callback may
// fire on Zenoh worker threads. This contract is documented on the
// `aorta_ctx_watch / aorta_ctx_watch_with_snapshot` FFI entry point and on `ContextStore::Watch` in
// the C++ SDK; violating it is undefined behavior on the C side, not
// the Rust side.
// SAFETY: see the doc comment above — `cb` and `user_data` are raw
// function/data pointers, which lack `Send + Sync` autotraits in Rust;
// the actual cross-thread safety is the caller's responsibility per the
// `aorta_ctx_watch*` FFI contract.
unsafe impl Send for CtxWatchCallback {}
// SAFETY: same justification as `Send` above — concurrent invocation
// safety lives on the FFI caller side.
unsafe impl Sync for CtxWatchCallback {}

fn ctx_kind_to_byte(k: crate::contextstore_fb::ContextKind) -> u8 {
    use crate::contextstore_fb::ContextKind;
    match k {
        ContextKind::None => AORTA_CTX_KIND_NONE,
        ContextKind::Bool => AORTA_CTX_KIND_BOOL,
        ContextKind::I64 => AORTA_CTX_KIND_I64,
        ContextKind::U64 => AORTA_CTX_KIND_U64,
        ContextKind::F64 => AORTA_CTX_KIND_F64,
        ContextKind::String => AORTA_CTX_KIND_STRING,
    }
}

fn ctx_change_kind_to_byte(k: crate::contextstore::ContextChangeKind) -> u8 {
    use crate::contextstore::ContextChangeKind;
    match k {
        ContextChangeKind::Live => AORTA_CTX_CHANGE_LIVE,
        ContextChangeKind::Snapshot => AORTA_CTX_CHANGE_SNAPSHOT,
        ContextChangeKind::SnapshotDone => AORTA_CTX_CHANGE_SNAPSHOT_DONE,
    }
}

fn ctx_snapshot_done_reason_to_byte(r: Option<crate::contextstore::SnapshotDoneReason>) -> u8 {
    use crate::contextstore::SnapshotDoneReason;
    match r {
        None => AORTA_CTX_SNAPSHOT_DONE_NONE,
        Some(SnapshotDoneReason::QueryDrained) => AORTA_CTX_SNAPSHOT_DONE_QUERY_DRAINED,
        Some(SnapshotDoneReason::DeadlineElapsed) => AORTA_CTX_SNAPSHOT_DONE_DEADLINE_ELAPSED,
    }
}

/// Convert `&str` → owned `*mut c_char` (NUL-terminated). Empty string
/// produces a non-NULL pointer to a single NUL byte. Caller must free
/// with `CString::from_raw` (done by `aorta_ctx_read_free`).
///
/// **Embedded NULs**: NUL-terminated C strings cannot represent
/// internal `\0` bytes. ContextStore's owner-side strings (`source_node`,
/// `source_instance_id`) are SDK-controlled and never contain NULs.
/// User-supplied string values that happen to contain `\0` are silently
/// truncated at the first NUL on the C side; producing a strict error
/// is left to a future hardening pass — the typed Rust API rejects
/// neither of them either, so the failure mode is consistent across
/// surfaces. If you need binary data with embedded NULs, use a future
/// `Bytes` value kind (future work).
fn cstring_to_owned_raw(s: &str) -> *mut c_char {
    CString::new(s)
        .unwrap_or_else(|_| {
            CString::new(s.replace('\0', "")).expect("string contains valid utf-8 minus nuls")
        })
        .into_raw()
}

fn ctx_lookup_store(handle: aorta_ctx_t) -> Option<crate::contextstore::ContextStore> {
    registry().context_stores.lock().get(&handle).cloned()
}

/// Release the three heap-allocated NUL-terminated strings that
/// `aorta_ctx_get_*` parked on `read` (`string_value`, `source_node`,
/// `source_instance_id`), nulling each pointer afterwards. Shared with
/// `aorta_ctx_read_free` so both call sites stay in lockstep — see the
/// struct doc on `aorta_ctx_read_t` for the init/reuse contract this
/// relies on.
///
/// # Safety
///
/// Each non-null pointer field MUST refer to a `CString::into_raw()`
/// allocation owned by this crate (the only path that writes them, via
/// `cstring_to_owned_raw`). Calling this on a struct that holds
/// uninitialized stack memory or foreign-allocated pointers is undefined
/// behavior.
unsafe fn free_ctx_read_fields(read: &mut aorta_ctx_read_t) {
    // SAFETY: see fn-level `# Safety`. Each `CString::from_raw` reclaims a
    // `CString` previously leaked via `cstring_to_owned_raw`; the caller
    // contract on `aorta_ctx_read_t` lifecycle guarantees these pointers
    // refer to that exact allocation type.
    unsafe {
        if !read.string_value.is_null() {
            let _ = CString::from_raw(read.string_value);
            read.string_value = std::ptr::null_mut();
        }
        if !read.source_node.is_null() {
            let _ = CString::from_raw(read.source_node);
            read.source_node = std::ptr::null_mut();
        }
        if !read.source_instance_id.is_null() {
            let _ = CString::from_raw(read.source_instance_id);
            read.source_instance_id = std::ptr::null_mut();
        }
    }
}

fn build_ctx_read_from(
    read: &mut aorta_ctx_read_t,
    value_kind: crate::contextstore_fb::ContextKind,
    bool_value: bool,
    i64_value: i64,
    u64_value: u64,
    f64_value: f64,
    string_value: Option<&str>,
    is_fresh: bool,
    stamp_ns: u64,
    expires_at_ns: u64,
    source_node: &str,
    source_instance_id: &str,
    sequence: u64,
) {
    // NOTE: we intentionally do NOT free the prior pointer fields here.
    // The lifecycle contract on `aorta_ctx_read_t` (see its struct doc)
    // already places that responsibility on the caller via
    // `aorta_ctx_read_free` between consecutive `aorta_ctx_get_*` calls.
    // Replicating the reclaim here would be redundant on the happy path
    // (the contract guarantees the pointers are NULL when we reach this
    // function — either from initial zero-init or from the caller's
    // preceding `_free`), and unsafe to add unconditionally without an
    // ABI sentinel because a contract-violating caller can still hand us
    // stale-but-non-null pointers that look freeable but were never our
    // allocations. Violations leak; they do not UB. A sentinel-based
    // reclaim is deferred.
    read.value_kind = ctx_kind_to_byte(value_kind);
    read.bool_value = bool_value;
    read.i64_value = i64_value;
    read.u64_value = u64_value;
    read.f64_value = f64_value;
    read.string_value = match string_value {
        Some(s) => cstring_to_owned_raw(s),
        None => std::ptr::null_mut(),
    };
    read.is_fresh = is_fresh;
    read.stamp_ns = stamp_ns;
    read.expires_at_ns = expires_at_ns;
    read.source_node = cstring_to_owned_raw(source_node);
    read.source_instance_id = cstring_to_owned_raw(source_instance_id);
    read.sequence = sequence;
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
pub unsafe extern "C" fn aorta_ctx_new(
    node: aorta_node_t,
    out: *mut aorta_ctx_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let node = match registry().nodes.lock().get(&node).cloned() {
            Some(n) => n,
            None => return map_aorta_error(AortaError::not_found("node handle not found")),
        };
        let store = crate::contextstore::ContextStore::new(&node);
        let handle = alloc_handle();
        registry().context_stores.lock().insert(handle, store);
        // SAFETY: `out` was null-checked above; caller contract requires it
        // point at writable storage for `aorta_ctx_t`.
        unsafe {
            *out = handle;
        }
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_ctx_free(handle: aorta_ctx_t) -> aorta_result_t {
    ffi_guard(|| {
        let removed = { registry().context_stores.lock().remove(&handle) };
        let Some(store) = removed else {
            return map_aorta_error(AortaError::not_found("context store handle not found"));
        };
        // Deterministic teardown: await Zenoh undeclare on every declared
        // Queryable + Subscriber. The C ABI is synchronous and callers will
        // typically free immediately on RAII destructor — without this, a
        // late owner_conflict subscriber callback could touch state owned
        // by the caller that has already gone out of scope.
        // FREE path: spawn-and-detach on reentrant invocation (see
        // `aorta_node_free`).
        if in_async_context() {
            runtime().spawn(async move {
                let _ = store.shutdown().await;
            });
            return AORTA_OK;
        }

        if let Err(err) = runtime().block_on(store.shutdown()) {
            return map_aorta_error(err);
        }
        drop(store);
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_ctx_shutdown(handle: aorta_ctx_t) -> aorta_result_t {
    ffi_guard(|| {
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match crate::ffi_block_on!(store.shutdown()) {
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_declare_owned_prefix(
    handle: aorta_ctx_t,
    prefix: *const c_char,
) -> aorta_result_t {
    ffi_guard(|| {
        // SAFETY: `prefix` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let prefix = match unsafe { cstr_to_string(prefix, "prefix") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match crate::ffi_block_on!(store.declare_owned_prefix(&prefix)) {
            Ok(Ok(_)) => AORTA_OK,
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

/// Whether `relative_key` falls under a prefix this store has declared.
///
/// Backs the Rust SDK `ContextStore::is_key_declared`; the
/// C++ SDK does not expose it. Synchronous (no Zenoh I/O). Writes `1`/`0` to
/// `out_declared`.
///
/// # Safety
/// - `handle` must be a valid `aorta_ctx_t`
/// - `relative_key` must be a valid NUL-terminated UTF-8 string
/// - `out_declared` must point to a writable `u8`
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_is_key_declared(
    handle: aorta_ctx_t,
    relative_key: *const c_char,
    out_declared: *mut u8,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_declared.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_declared is null");
        }
        // SAFETY: `relative_key` is the caller-supplied C string; `cstr_to_string`
        // null-checks + validates UTF-8 under the NUL-termination contract.
        let key = match unsafe { cstr_to_string(relative_key, "relative_key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match store.is_key_declared(&key) {
            Ok(declared) => {
                // SAFETY: `out_declared` null-checked above; writable `u8` per
                // the caller contract.
                unsafe {
                    *out_declared = u8::from(declared);
                }
                AORTA_OK
            }
            Err(err) => map_aorta_error(err),
        }
    })
}

/// Write this store's declared prefixes, newline-joined UTF-8, into `out_buf`
/// (same buffer/size/out_len contract as `aorta_node_get_id`). `*out_len` is set
/// to the full byte length even when it exceeds `buf_size`; at most `buf_size`
/// bytes are written.
///
/// Backs the Rust SDK `ContextStore::declared_prefixes`,
/// a diagnostic accessor the C++ SDK does not expose.
///
/// # Safety
/// - `handle` must be a valid `aorta_ctx_t`
/// - `out_buf` must be writable for `buf_size` bytes (or null if `buf_size == 0`)
/// - `out_len` must point to a writable `usize`
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_declared_prefixes(
    handle: aorta_ctx_t,
    out_buf: *mut c_char,
    buf_size: usize,
    out_len: *mut usize,
) -> aorta_result_t {
    ffi_guard(|| {
        if out_len.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out_len is null");
        }
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let joined = store.declared_prefixes().join("\n");
        let bytes = joined.as_bytes();
        // SAFETY: `out_len` null-checked above; `out_buf` is writable for
        // `buf_size` bytes per the caller contract.
        unsafe {
            *out_len = bytes.len();
            if !out_buf.is_null() && buf_size > 0 {
                let n = bytes.len().min(buf_size);
                ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf as *mut u8, n);
            }
        }
        AORTA_OK
    })
}

// ----- typed setters ------------------------------------------------------

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_set_bool(
    handle: aorta_ctx_t,
    key: *const c_char,
    value: bool,
    expires_at_ns: u64,
) -> aorta_result_t {
    ffi_guard(|| {
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let res = if expires_at_ns == AORTA_CTX_NO_EXPIRY {
            crate::ffi_block_on!(store.set_bool(&key, value))
        } else {
            crate::ffi_block_on!(store.set_bool_with_expiry(&key, value, expires_at_ns))
        };
        match res {
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_set_i64(
    handle: aorta_ctx_t,
    key: *const c_char,
    value: i64,
    expires_at_ns: u64,
) -> aorta_result_t {
    ffi_guard(|| {
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let res = if expires_at_ns == AORTA_CTX_NO_EXPIRY {
            crate::ffi_block_on!(store.set_i64(&key, value))
        } else {
            crate::ffi_block_on!(store.set_i64_with_expiry(&key, value, expires_at_ns))
        };
        match res {
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_set_u64(
    handle: aorta_ctx_t,
    key: *const c_char,
    value: u64,
    expires_at_ns: u64,
) -> aorta_result_t {
    ffi_guard(|| {
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let res = if expires_at_ns == AORTA_CTX_NO_EXPIRY {
            crate::ffi_block_on!(store.set_u64(&key, value))
        } else {
            crate::ffi_block_on!(store.set_u64_with_expiry(&key, value, expires_at_ns))
        };
        match res {
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_set_f64(
    handle: aorta_ctx_t,
    key: *const c_char,
    value: f64,
    expires_at_ns: u64,
) -> aorta_result_t {
    ffi_guard(|| {
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let res = if expires_at_ns == AORTA_CTX_NO_EXPIRY {
            crate::ffi_block_on!(store.set_f64(&key, value))
        } else {
            crate::ffi_block_on!(store.set_f64_with_expiry(&key, value, expires_at_ns))
        };
        match res {
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_set_string(
    handle: aorta_ctx_t,
    key: *const c_char,
    value: *const c_char,
    expires_at_ns: u64,
) -> aorta_result_t {
    ffi_guard(|| {
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        // SAFETY: `value` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let value = match unsafe { cstr_to_string(value, "value") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let res = if expires_at_ns == AORTA_CTX_NO_EXPIRY {
            crate::ffi_block_on!(store.set_string(&key, &value))
        } else {
            crate::ffi_block_on!(store.set_string_with_expiry(&key, &value, expires_at_ns))
        };
        match res {
            Ok(Ok(_)) => AORTA_OK,
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

// ----- typed getters ------------------------------------------------------

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. C-string
/// arguments must reference a NUL-terminated valid UTF-8 sequence. Output
/// pointers (`out_*`) must be valid for writes for the size of their
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_get_bool(
    handle: aorta_ctx_t,
    key: *const c_char,
    out: *mut aorta_ctx_read_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match crate::ffi_block_on!(store.get_bool(&key)) {
            Ok(Ok(read)) => {
                // SAFETY: `out` was null-checked at entry; caller contract
                // requires it point at writable storage for `aorta_ctx_read_t`.
                // `build_ctx_read_from` performs the field-wise init under
                // that borrow, then returns.
                unsafe {
                    let r = &mut *out;
                    build_ctx_read_from(
                        r,
                        crate::contextstore_fb::ContextKind::Bool,
                        read.value,
                        0,
                        0,
                        0.0,
                        None,
                        read.is_fresh,
                        read.stamp_ns,
                        read.expires_at_ns,
                        &read.source_node,
                        &read.source_instance_id,
                        read.sequence,
                    );
                }
                AORTA_OK
            }
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_get_i64(
    handle: aorta_ctx_t,
    key: *const c_char,
    out: *mut aorta_ctx_read_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match crate::ffi_block_on!(store.get_i64(&key)) {
            Ok(Ok(read)) => {
                // SAFETY: `out` was null-checked at entry; caller contract
                // requires it point at writable storage for `aorta_ctx_read_t`.
                // `build_ctx_read_from` performs the field-wise init under
                // that borrow, then returns.
                unsafe {
                    let r = &mut *out;
                    build_ctx_read_from(
                        r,
                        crate::contextstore_fb::ContextKind::I64,
                        false,
                        read.value,
                        0,
                        0.0,
                        None,
                        read.is_fresh,
                        read.stamp_ns,
                        read.expires_at_ns,
                        &read.source_node,
                        &read.source_instance_id,
                        read.sequence,
                    );
                }
                AORTA_OK
            }
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_get_u64(
    handle: aorta_ctx_t,
    key: *const c_char,
    out: *mut aorta_ctx_read_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match crate::ffi_block_on!(store.get_u64(&key)) {
            Ok(Ok(read)) => {
                // SAFETY: `out` was null-checked at entry; caller contract
                // requires it point at writable storage for `aorta_ctx_read_t`.
                // `build_ctx_read_from` performs the field-wise init under
                // that borrow, then returns.
                unsafe {
                    let r = &mut *out;
                    build_ctx_read_from(
                        r,
                        crate::contextstore_fb::ContextKind::U64,
                        false,
                        0,
                        read.value,
                        0.0,
                        None,
                        read.is_fresh,
                        read.stamp_ns,
                        read.expires_at_ns,
                        &read.source_node,
                        &read.source_instance_id,
                        read.sequence,
                    );
                }
                AORTA_OK
            }
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_get_f64(
    handle: aorta_ctx_t,
    key: *const c_char,
    out: *mut aorta_ctx_read_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match crate::ffi_block_on!(store.get_f64(&key)) {
            Ok(Ok(read)) => {
                // SAFETY: `out` was null-checked at entry; caller contract
                // requires it point at writable storage for `aorta_ctx_read_t`.
                // `build_ctx_read_from` performs the field-wise init under
                // that borrow, then returns.
                unsafe {
                    let r = &mut *out;
                    build_ctx_read_from(
                        r,
                        crate::contextstore_fb::ContextKind::F64,
                        false,
                        0,
                        0,
                        read.value,
                        None,
                        read.is_fresh,
                        read.stamp_ns,
                        read.expires_at_ns,
                        &read.source_node,
                        &read.source_instance_id,
                        read.sequence,
                    );
                }
                AORTA_OK
            }
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
/// pointed-to type. Handle arguments must originate from the corresponding
/// constructor and must not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_get_string(
    handle: aorta_ctx_t,
    key: *const c_char,
    out: *mut aorta_ctx_read_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        // SAFETY: `key` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let key = match unsafe { cstr_to_string(key, "key") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        match crate::ffi_block_on!(store.get_string(&key)) {
            Ok(Ok(read)) => {
                // SAFETY: `out` was null-checked at entry; caller contract
                // requires it point at writable storage for `aorta_ctx_read_t`.
                // `build_ctx_read_from` performs the field-wise init under
                // that borrow, then returns.
                unsafe {
                    let r = &mut *out;
                    build_ctx_read_from(
                        r,
                        crate::contextstore_fb::ContextKind::String,
                        false,
                        0,
                        0,
                        0.0,
                        Some(read.value.as_str()),
                        read.is_fresh,
                        read.stamp_ns,
                        read.expires_at_ns,
                        &read.source_node,
                        &read.source_instance_id,
                        read.sequence,
                    );
                }
                AORTA_OK
            }
            Ok(Err(err)) => map_aorta_error(err),
            Err(_) => aorta_reentrant(module_path!()),
        }
    })
}

/// Free heap allocations carried by `aorta_ctx_read_t`. Safe to call on a
/// zero-initialised struct (NULL pointers are skipped) and idempotent.
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
pub unsafe extern "C" fn aorta_ctx_read_free(read: *mut aorta_ctx_read_t) -> aorta_result_t {
    ffi_guard(|| {
        if read.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "read is null");
        }
        // SAFETY: `read` is non-null per the guard above. The lifecycle
        // contract on `aorta_ctx_read_t` (see its struct doc) requires
        // the struct to be zero-initialised on first use; thereafter the
        // pointer fields are either NULL or `CString::into_raw()`
        // allocations owned by this crate, populated by a preceding
        // `aorta_ctx_get_*`. Both shapes satisfy `free_ctx_read_fields`
        // (it skips NULL and reclaims the rest). Calling this on a
        // freshly zero-initialised struct that has never seen a getter
        // is a documented no-op.
        unsafe { free_ctx_read_fields(&mut *read) };
        AORTA_OK
    })
}

// ----- watch / watch_with_initial_snapshot --------------------------------

fn dispatch_change_to_callback(cb: &CtxWatchCallback, change: &crate::contextstore::ContextChange) {
    let key = change.key.as_str();
    let source_node = change.source_node.as_str();
    let source_instance_id = change.source_instance_id.as_str();
    let string_value_view = match &change.string_value {
        Some(s) => string_view_from_str(s.as_str()),
        None => aorta_string_view_t::default(),
    };
    let c_change = aorta_ctx_change_t {
        kind: ctx_change_kind_to_byte(change.kind),
        snapshot_done_reason: ctx_snapshot_done_reason_to_byte(change.snapshot_done_reason),
        key: string_view_from_str(key),
        value_kind: ctx_kind_to_byte(change.value_kind),
        bool_value: change.bool_value,
        i64_value: change.i64_value,
        u64_value: change.u64_value,
        f64_value: change.f64_value,
        string_value: string_value_view,
        stamp_ns: change.stamp_ns,
        expires_at_ns: change.expires_at_ns,
        source_node: string_view_from_str(source_node),
        source_instance_id: string_view_from_str(source_instance_id),
        sequence: change.sequence,
    };
    // Context-store change dispatch — wrap so a Rust-side panic in OUR
    // marshaling code above (the c_change struct construction,
    // string_view_from_str, the byte conversions) cannot unwind into the
    // contextstore watcher's tokio task. NOTE: this does NOT catch panics
    // that originate INSIDE the user's `cb.callback` — Rust ≥ 1.81 marks
    // extern "C" as nounwind, so a panic in the callback aborts before our
    // catch_unwind sees it. The C++ SDK's try/catch is what
    // protects C++ users. (Both aorta_ctx_watch entry points funnel here,
    // so wrapping at this helper covers both.)
    // Explicit reentrancy detector: watch callbacks run on a
    // runtime task thread; blocking FFI re-entry must classify reentrant.
    let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
    // SAFETY: `cb.callback` is the FFI caller-installed `extern "C" fn`;
    // `&c_change` borrows a locally-built `aorta_ctx_change_t` whose owner
    // (this fn) outlives the dispatch. `cb.user_data` is the caller's
    // opaque pointer registered at watch declaration.
    ffi_guard_void(|| unsafe {
        (cb.callback)(&c_change as *const _, cb.user_data);
    });
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
pub unsafe extern "C" fn aorta_ctx_watch(
    handle: aorta_ctx_t,
    prefix: *const c_char,
    callback: aorta_ctx_watch_callback_t,
    user_data: *mut c_void,
    out: *mut aorta_ctx_watch_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let Some(callback) = callback else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
        };
        // SAFETY: `prefix` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let prefix = match unsafe { cstr_to_string(prefix, "prefix") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let cb_arc = Arc::new(CtxWatchCallback {
            callback,
            user_data,
        });
        let cb_for_closure = Arc::clone(&cb_arc);
        let watch = match crate::ffi_block_on!(store.watch(&prefix, move |change| {
            dispatch_change_to_callback(&cb_for_closure, &change);
        })) {
            Ok(Ok(w)) => w,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };
        let watch_handle = alloc_handle();
        registry()
            .context_watches
            .lock()
            .insert(watch_handle, watch);
        // SAFETY: `out` was null-checked above; caller contract requires
        // it point at writable storage for `aorta_ctx_watch_t`.
        unsafe {
            *out = watch_handle;
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
pub unsafe extern "C" fn aorta_ctx_watch_with_snapshot(
    handle: aorta_ctx_t,
    prefix: *const c_char,
    snapshot_deadline_ms: u64,
    callback: aorta_ctx_watch_callback_t,
    user_data: *mut c_void,
    out: *mut aorta_ctx_watch_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let Some(callback) = callback else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
        };
        // SAFETY: `prefix` is the FFI caller-supplied `*const c_char`;
        // `cstr_to_string` null-checks and validates UTF-8 under the
        // NUL-termination contract.
        let prefix = match unsafe { cstr_to_string(prefix, "prefix") } {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let opts = crate::contextstore::WatchSnapshotOptions {
            snapshot_deadline: Duration::from_millis(snapshot_deadline_ms),
        };
        let cb_arc = Arc::new(CtxWatchCallback {
            callback,
            user_data,
        });
        let cb_for_closure = Arc::clone(&cb_arc);
        let watch = match crate::ffi_block_on!(store.watch_with_initial_snapshot(
            &prefix,
            opts,
            move |change| {
                dispatch_change_to_callback(&cb_for_closure, &change);
            },
        )) {
            Ok(Ok(w)) => w,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };
        let watch_handle = alloc_handle();
        registry()
            .context_watches
            .lock()
            .insert(watch_handle, watch);
        // SAFETY: `out` was null-checked above; caller contract requires
        // it point at writable storage for `aorta_ctx_watch_t`.
        unsafe {
            *out = watch_handle;
        }
        AORTA_OK
    })
}

#[no_mangle]
pub extern "C" fn aorta_ctx_watch_free(handle: aorta_ctx_watch_t) -> aorta_result_t {
    ffi_guard(|| {
        let removed = { registry().context_watches.lock().remove(&handle) };
        let Some(watch) = removed else {
            return map_aorta_error(AortaError::not_found("context watch handle not found"));
        };
        // Test seam: panic AFTER the registry removal — the entry
        // is gone, the watch drops best-effort during unwind.
        #[cfg(test)]
        if TEST_PANIC_AFTER_WATCH_REMOVE.swap(false, std::sync::atomic::Ordering::SeqCst) {
            panic!("test-injected panic after context watch registry removal");
        }
        // Deterministic teardown: abort the snapshot task and await Zenoh
        // undeclare on the subscriber so no further callback can touch the
        // caller's user_data after this function returns.
        // `WatchHandle::shutdown` holds a `std::sync::Mutex` guard across
        // await internally, so its future is not `Send` and cannot be
        // background-spawned the way other FREE paths can. Re-insert the
        // handle on reentrant invocation so the caller can retry from a
        // non-runtime thread, then surface REENTRANT.
        if in_async_context() {
            registry().context_watches.lock().insert(handle, watch);
            return aorta_reentrant(module_path!());
        }

        if let Err(err) = runtime().block_on(watch.shutdown()) {
            return map_aorta_error(err);
        }
        drop(watch);
        AORTA_OK
    })
}

// ----- owner liveliness watch -------------------------------------------------

pub type aorta_ctx_owner_liveliness_watch_t = u64;

/// Event delivered to an owner-liveliness watch callback: one owning
/// ContextStore became alive or went away.
///
/// Threading contract:
/// - The callback may fire on any Zenoh worker thread.
/// - `node_name` and `source_instance_id` are NOT NUL-terminated
///   `aorta_string_view_t`s. Use `(data, len)` as a byte slice; the
///   pointers are valid only for the duration of the callback.
/// - Drain guarantee is scoped to AORTA_OK: when
///   `aorta_ctx_owner_liveliness_watch_free` returns AORTA_OK, no
///   further callback fires for that watch — the FFI awaited the
///   undeclare AND the in-flight callbacks. On ANY non-OK result the
///   drain is UNCONFIRMED: the callback may still fire with this
///   `user_data`, so the caller MUST keep `user_data` (and everything
///   it points at) alive. Drop without `_free` is best-effort.
#[repr(C)]
pub struct aorta_ctx_owner_liveliness_event_t {
    pub node_name: aorta_string_view_t,
    pub source_instance_id: aorta_string_view_t,
    pub alive: bool,
}

pub type aorta_ctx_owner_liveliness_callback_t =
    Option<unsafe extern "C" fn(*const aorta_ctx_owner_liveliness_event_t, *mut c_void)>;

/// Wrapper that lets us move a raw C function pointer + opaque user
/// pointer into a `Send + Sync` Fn closure. Caller is responsible for
/// guaranteeing user_data is safe to share across threads (same
/// contract as `CtxWatchCallback`).
struct CtxOwnerLivelinessCallback {
    callback: unsafe extern "C" fn(*const aorta_ctx_owner_liveliness_event_t, *mut c_void),
    user_data: *mut c_void,
}

// SAFETY: `callback` is an `extern "C" fn` pointer (inherently Send +
// Sync); `user_data` is opaque and treated as thread-safe by API
// contract — the FFI caller is responsible for synchronising the data
// it points at, since the callback may fire on Zenoh worker threads.
// Mirrors the `CtxWatchCallback` justification.
unsafe impl Send for CtxOwnerLivelinessCallback {}
// SAFETY: same justification as `Send` above.
unsafe impl Sync for CtxOwnerLivelinessCallback {}

fn dispatch_owner_liveliness_to_callback(
    cb: &CtxOwnerLivelinessCallback,
    event: &crate::contextstore::OwnerLivelinessEvent,
) {
    let c_event = aorta_ctx_owner_liveliness_event_t {
        node_name: string_view_from_str(event.node_name.as_str()),
        source_instance_id: string_view_from_str(event.source_instance_id.as_str()),
        alive: event.alive,
    };
    // Explicit reentrancy detector: watch callbacks run
    // on a runtime task thread; blocking FFI re-entry must classify
    // reentrant. Same classification as the ctx watch callbacks.
    let _reentrancy_guard = crate::flow::reentrancy::CallbackContextGuard::enter();
    // SAFETY: `cb.callback` is the FFI caller-installed `extern "C" fn`;
    // `&c_event` borrows a locally-built struct whose owner (this fn)
    // outlives the dispatch; `cb.user_data` is the caller's opaque
    // pointer registered at watch declaration.
    ffi_guard_void(|| unsafe {
        (cb.callback)(&c_event as *const _, cb.user_data);
    });
}

/// # Safety
///
/// All pointer arguments must be NULL where the parameter docs allow it,
/// or point to a valid object for the duration of the call. Handle
/// arguments must originate from the corresponding constructor and must
/// not have been freed.
#[no_mangle]
pub unsafe extern "C" fn aorta_ctx_owner_liveliness_watch(
    handle: aorta_ctx_t,
    callback: aorta_ctx_owner_liveliness_callback_t,
    user_data: *mut c_void,
    out: *mut aorta_ctx_owner_liveliness_watch_t,
) -> aorta_result_t {
    ffi_guard(|| {
        if out.is_null() {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "out is null");
        }
        let Some(callback) = callback else {
            return aorta_err(AORTA_ERR_INVALID_ARGUMENT, "callback is null");
        };
        let store = match ctx_lookup_store(handle) {
            Some(s) => s,
            None => {
                return map_aorta_error(AortaError::not_found("context store handle not found"))
            }
        };
        let cb_arc = Arc::new(CtxOwnerLivelinessCallback {
            callback,
            user_data,
        });
        let cb_for_closure = Arc::clone(&cb_arc);
        let watch = match crate::ffi_block_on!(store.watch_ctx_owner_liveliness(move |event| {
            dispatch_owner_liveliness_to_callback(&cb_for_closure, &event);
        })) {
            Ok(Ok(w)) => w,
            Ok(Err(err)) => return map_aorta_error(err),
            Err(_) => return aorta_reentrant(module_path!()),
        };
        let watch_handle = alloc_handle();
        registry()
            .ctx_owner_liveliness_watches
            .lock()
            .insert(watch_handle, watch);
        // SAFETY: `out` was null-checked above; caller contract requires
        // it point at writable storage for `aorta_ctx_owner_liveliness_watch_t`.
        unsafe {
            *out = watch_handle;
        }
        AORTA_OK
    })
}

/// # Safety
///
/// Handle arguments must originate from the corresponding constructor
/// and must not have been freed.
#[no_mangle]
pub extern "C" fn aorta_ctx_owner_liveliness_watch_free(
    handle: aorta_ctx_owner_liveliness_watch_t,
) -> aorta_result_t {
    ffi_guard(|| {
        let removed = {
            registry()
                .ctx_owner_liveliness_watches
                .lock()
                .remove(&handle)
        };
        let Some(watch) = removed else {
            return map_aorta_error(AortaError::not_found(
                "context owner liveliness watch handle not found",
            ));
        };
        // Test seam: see aorta_ctx_watch_free.
        #[cfg(test)]
        if TEST_PANIC_AFTER_WATCH_REMOVE.swap(false, std::sync::atomic::Ordering::SeqCst) {
            panic!("test-injected panic after owner liveliness watch registry removal");
        }
        // Deterministic teardown: await Zenoh's undeclare on the
        // liveliness subscriber so no callback can touch the caller's
        // user_data after this function returns. Same discipline as
        // `aorta_ctx_watch_free` — re-insert the handle on reentrant
        // invocation so the caller can retry from a non-runtime thread.
        if in_async_context() {
            registry()
                .ctx_owner_liveliness_watches
                .lock()
                .insert(handle, watch);
            return aorta_reentrant(module_path!());
        }

        if let Err(err) = runtime().block_on(watch.shutdown()) {
            return map_aorta_error(err);
        }
        drop(watch);
        AORTA_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Direct round-trip for the shared deallocation helper: after a
    /// `build_ctx_read_from` populates the struct, `free_ctx_read_fields`
    /// must return every owned-pointer field to NULL. This is the
    /// post-condition both `aorta_ctx_read_free` and any future
    /// auto-reclaim path rely on.
    #[test]
    fn free_ctx_read_fields_nulls_all_owned_pointers() {
        let mut read = aorta_ctx_read_t::default();
        build_ctx_read_from(
            &mut read,
            crate::contextstore_fb::ContextKind::String,
            false,
            0,
            0,
            0.0,
            Some("value"),
            true,
            42,
            0,
            "node-id",
            "instance-id",
            7,
        );
        assert!(!read.string_value.is_null());
        assert!(!read.source_node.is_null());
        assert!(!read.source_instance_id.is_null());

        // SAFETY: every non-null pointer above was produced by
        // `cstring_to_owned_raw` inside `build_ctx_read_from`, so the
        // helper's contract is satisfied.
        unsafe { free_ctx_read_fields(&mut read) };
        assert!(read.string_value.is_null());
        assert!(read.source_node.is_null());
        assert!(read.source_instance_id.is_null());
    }
}
