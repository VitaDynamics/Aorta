//! Tracing C-ABI surface.
//!
//! Exposes the single core tracing engine to the C++/Python SDKs and end users,
//! mirroring `log_ffi`'s panic-guard + C-string contract. Independent of
//! telemetry. The gate-byte pointer accessors ([`aorta_trace_master_gate`],
//! [`aorta_trace_scope_gate`]) let the SDKs inline the near-zero disabled check
//! WITHOUT an FFI call on the hot path (the pointee is a plain `u8`; the SDK
//! reads it atomically C-side, e.g. via `std::atomic_ref<uint8_t>` /
//! `atomic_load_explicit`). This is the opposite of the `aorta_log_enabled`
//! anti-pattern, which crosses the FFI (and allocates) on every disabled call.

use super::*;
use crate::trace::{self, LogLevel};

fn level_from_u8(v: u8) -> LogLevel {
    match v {
        0 => LogLevel::Trace,
        1 => LogLevel::Debug,
        2 => LogLevel::Info,
        3 => LogLevel::Warn,
        _ => LogLevel::Error,
    }
}

/// Intern a scope name and return its stable handle (`0` = invalid/sentinel).
///
/// # Safety
/// `name` must be NULL or a NUL-terminated valid UTF-8 C string valid for the
/// duration of the call; ownership is not taken.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_scope(name: *const c_char) -> u16 {
    // SAFETY: `cstr_to_string` reads `name` under the documented C-string contract.
    ffi_guard_value(trace::SCOPE_NONE, || unsafe {
        match cstr_to_string(name, "scope") {
            Ok(s) => trace::intern_scope(&s),
            Err(_) => trace::SCOPE_NONE,
        }
    })
}

/// Enable (`on != 0`) or disable the master tracing switch.
#[no_mangle]
pub extern "C" fn aorta_trace_set_enabled(on: u8) {
    ffi_guard_void(|| trace::set_trace_enabled(on != 0));
}

/// Master switch state (`1` = on).
#[no_mangle]
pub extern "C" fn aorta_trace_is_enabled() -> u8 {
    ffi_guard_value(0, || u8::from(trace::trace_enabled()))
}

/// Enable (`on != 0`, at all levels) or disable a single scope. Ignores the
/// sentinel scope.
#[no_mangle]
pub extern "C" fn aorta_trace_set_scope_enabled(scope: u16, on: u8) {
    ffi_guard_void(|| {
        if scope != trace::SCOPE_NONE {
            trace::set_scope_enabled(scope, on != 0);
        }
    });
}

/// Apply the `AORTA_TRACE` environment directive (master + per-scope config).
/// Tracing's own config grammar, independent of `AORTA_LOG`/`AORTA_TELEMETRY_*`.
#[no_mangle]
pub extern "C" fn aorta_trace_init_from_env() -> aorta_result_t {
    ffi_guard(|| {
        trace::scope::init_from_env();
        AORTA_OK
    })
}

/// Is `(scope, level)` enabled? (master AND scope-threshold AND compiled-in).
/// The non-inlined predicate; the SDK's hot path should instead read the cached
/// gate-byte pointers. `0` on panic/invalid scope.
#[no_mangle]
pub extern "C" fn aorta_trace_enabled(scope: u16, level: u8) -> u8 {
    ffi_guard_value(0, || {
        if scope == trace::SCOPE_NONE {
            return 0;
        }
        u8::from(trace::scope_enabled(scope, level_from_u8(level)))
    })
}

/// Record a user latency (ns) for `(scope, name, level)`. Gated internally
/// (no-op when disabled). Prefer recording only after the inline gate check.
///
/// # Safety
/// `name` must be NULL or a NUL-terminated valid UTF-8 C string valid for the
/// duration of the call; ownership is not taken.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_record(
    scope: u16,
    name: *const c_char,
    level: u8,
    latency_ns: u64,
) -> aorta_result_t {
    // SAFETY: `cstr_to_string` reads `name` under the documented C-string contract.
    ffi_guard(|| unsafe {
        if scope == trace::SCOPE_NONE {
            return AORTA_OK;
        }
        let name = match cstr_to_string(name, "name") {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        trace::record_user(
            scope,
            &name,
            trace::EventType::Span,
            level_from_u8(level),
            latency_ns,
        );
        AORTA_OK
    })
}

/// Record a user event of an explicit type (`event_type`: 0=span/latency,
/// 1=instant, 2=counter) with `value` (latency ns / `1` for instant / counter
/// value) for `(scope, name, level)`. Gated internally.
///
/// # Safety
/// `name` must be NULL or a NUL-terminated valid UTF-8 C string valid for the
/// duration of the call; ownership is not taken.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_record_event(
    scope: u16,
    name: *const c_char,
    event_type: u8,
    level: u8,
    value: u64,
) -> aorta_result_t {
    // SAFETY: `cstr_to_string` reads `name` under the documented C-string contract.
    ffi_guard(|| unsafe {
        if scope == trace::SCOPE_NONE {
            return AORTA_OK;
        }
        let name = match cstr_to_string(name, "name") {
            Ok(s) => s,
            Err(err) => return map_aorta_error(err),
        };
        trace::record_user(
            scope,
            &name,
            trace::EventType::from_u8(event_type),
            level_from_u8(level),
            value,
        );
        AORTA_OK
    })
}

/// Recorded sample count for a `(scope, name)` user metric (`0` if absent). For
/// read-back so the C++/Python SDKs (and their tests) can verify the real engine
/// recorded.
///
/// # Safety
/// `name` must be NULL or a NUL-terminated valid UTF-8 C string valid for the
/// duration of the call; ownership is not taken.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_user_metric_count(scope: u16, name: *const c_char) -> u64 {
    // SAFETY: `cstr_to_string` reads `name` under the documented C-string contract.
    ffi_guard_value(0, || unsafe {
        if scope == trace::SCOPE_NONE {
            return 0;
        }
        match cstr_to_string(name, "name") {
            Ok(s) => trace::user_metric_count(scope, &s),
            Err(_) => 0,
        }
    })
}

/// Intern a `(scope, name)` user metric and return an opaque **handle** — its
/// leaked, process-lifetime histogram address — for the lock-free record path.
/// NULL for the sentinel scope or a bad name. The SDK resolves this ONCE (cold;
/// locks the registry) and then calls [`aorta_trace_record_by_handle`] per event,
/// so the high-volume hot path has NO FFI string conversion, NO global lock, and
/// NO allocation — unlike the by-name [`aorta_trace_record`], which does all
/// three on every call. `event_type` matches [`aorta_trace_record_event`].
///
/// # Safety
/// `name` must be NULL or a NUL-terminated valid UTF-8 C string valid for the
/// duration of the call; ownership is not taken.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_metric(
    scope: u16,
    name: *const c_char,
    event_type: u8,
) -> *const core::ffi::c_void {
    // SAFETY: `cstr_to_string` reads `name` under the documented C-string contract.
    ffi_guard_value(ptr::null(), || unsafe {
        if scope == trace::SCOPE_NONE {
            return ptr::null();
        }
        match cstr_to_string(name, "name") {
            Ok(s) => {
                let hist =
                    trace::register_user_metric(scope, &s, trace::EventType::from_u8(event_type));
                (hist as *const trace::FixedHistogram).cast::<core::ffi::c_void>()
            }
            Err(_) => ptr::null(),
        }
    })
}

/// Record `value` into a metric handle from [`aorta_trace_metric`]. Lock-free and
/// allocation-free — the hot path for high-volume SDK recording. NOT gated here:
/// the SDK gates inline (via the cached gate bytes) BEFORE calling, exactly like
/// the Rust macros. A NULL handle is a no-op. No panic guard: recording is
/// branch-free atomic RMWs (panic-free, alloc-free), so the cross-FFI cost is one
/// call + the record.
///
/// # Safety
/// `metric` must be NULL or a handle previously returned by [`aorta_trace_metric`]
/// in this process (a process-lifetime histogram address); passing any other
/// pointer is undefined behaviour.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_record_by_handle(
    metric: *const core::ffi::c_void,
    value: u64,
) {
    if metric.is_null() {
        return;
    }
    // SAFETY: per the contract, `metric` is a process-lifetime `FixedHistogram`
    // address from `aorta_trace_metric`; `record` is panic-free and alloc-free.
    unsafe {
        (*(metric as *const trace::FixedHistogram)).record(value);
    }
}

/// Record an internal stage latency (ns) by [`trace::StageId`] discriminant.
/// Unknown ids are ignored. Gated internally.
#[no_mangle]
pub extern "C" fn aorta_trace_record_stage(stage_id: u8, latency_ns: u64) -> aorta_result_t {
    ffi_guard(|| {
        if let Some(stage) = trace::StageId::from_u8(stage_id) {
            trace::record_stage(stage, latency_ns);
        }
        AORTA_OK
    })
}

/// Address of the process-global master gate byte (`0` = off). The SDK caches
/// this once and reads it inline (relaxed) to skip work when tracing is globally
/// off — no FFI call on the disabled path. NULL only on panic.
#[no_mangle]
pub extern "C" fn aorta_trace_master_gate() -> *const u8 {
    ffi_guard_value(ptr::null(), trace::master_gate_ptr)
}

/// Address of a scope's threshold gate byte (min enabled level, or `255` = off).
/// For finer SDK-side inline gating. NULL for the sentinel scope or on panic.
#[no_mangle]
pub extern "C" fn aorta_trace_scope_gate(scope: u16) -> *const u8 {
    ffi_guard_value(ptr::null(), || {
        if scope == trace::SCOPE_NONE {
            return ptr::null();
        }
        (trace::scope_gate(scope) as *const std::sync::atomic::AtomicU8).cast::<u8>()
    })
}

// ===========================================================================
// Per-event spans / instants / counters (the waterfall ring path).
//
// These forward to the ring recorder. They are gated only by the MASTER switch
// (checked inside); the per-scope tier filter is the SDK's job, done inline
// against the cached scope gate BEFORE calling — exactly like the
// `aorta_trace_record_by_handle` hot path. `event_id` is an interned event
// name from `aorta_trace_intern_event`, cached at the call site.
// ===========================================================================

/// Intern an event name → a stable `u16` id (cache at the call site). `0` on a
/// bad/NULL name is a valid id; SDKs always have a literal name here.
///
/// # Safety
/// `name` must be NULL or a NUL-terminated valid UTF-8 C string valid for the
/// duration of the call; ownership is not taken.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_intern_event(name: *const c_char) -> u16 {
    // SAFETY: `cstr_to_string` reads `name` under the documented C-string contract.
    ffi_guard_value(0, || unsafe {
        match cstr_to_string(name, "event") {
            Ok(s) => trace::intern_event(&s),
            Err(_) => 0,
        }
    })
}

/// Begin a timed span; returns a token for [`aorta_trace_span_end`] (`0` when
/// tracing is off — passing it back is a safe no-op). `flags` bit0 = async.
#[no_mangle]
pub extern "C" fn aorta_trace_span_begin(scope: u16, event_id: u16, level: u8, flags: u8) -> u64 {
    ffi_guard_value(0, || trace::span_begin(scope, event_id, level, flags))
}

/// End the span for `token`, flushing its begin/end records, and return its
/// wall duration in ns (so the SDK can also feed an aggregate histogram). `0`
/// token or no match → `0`.
#[no_mangle]
pub extern "C" fn aorta_trace_span_end(token: u64) -> u64 {
    ffi_guard_value(0, || trace::span_end(token))
}

/// Record a point INSTANT event for `(scope, event_id, level)`.
#[no_mangle]
pub extern "C" fn aorta_trace_record_instant(scope: u16, event_id: u16, level: u8) {
    ffi_guard_void(|| trace::instant(scope, event_id, level));
}

/// Record a COUNTER sample `value` for `(scope, event_id, level)`.
#[no_mangle]
pub extern "C" fn aorta_trace_record_counter(scope: u16, event_id: u16, level: u8, value: u64) {
    ffi_guard_void(|| trace::counter(scope, event_id, level, value));
}

/// Install inbound trace context: the 16-byte `trace_id` (NULL ⇒ all-zero) and
/// the `parent_span_id` of the next root span. Used by RPC/action propagation.
///
/// # Safety
/// `trace_id` must be NULL or point to at least 16 readable bytes for the call.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_set_context(trace_id: *const u8, parent_span_id: u64) {
    ffi_guard_void(|| {
        let mut id = [0u8; 16];
        if !trace_id.is_null() {
            // SAFETY: caller guarantees 16 readable bytes at `trace_id`.
            unsafe { ptr::copy_nonoverlapping(trace_id, id.as_mut_ptr(), 16) };
        }
        trace::set_trace_context(id, parent_span_id);
    });
}

/// Clear any inbound trace context.
#[no_mangle]
pub extern "C" fn aorta_trace_clear_context() {
    ffi_guard_void(trace::clear_trace_context);
}

/// Read the active outbound context to inject into a hop. Writes the 16-byte
/// `trace_id` and the current `span_id`; returns `1` if a trace is active, else
/// `0` (outputs untouched).
///
/// # Safety
/// When non-NULL, `out_trace_id` must point to ≥16 writable bytes and
/// `out_span_id` to a writable `u64` for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_current_context(
    out_trace_id: *mut u8,
    out_span_id: *mut u64,
) -> u8 {
    ffi_guard_value(0, || match trace::current_outbound_context() {
        Some((trace_id, span_id)) => {
            if !out_trace_id.is_null() {
                // SAFETY: caller guarantees ≥16 writable bytes at `out_trace_id`.
                unsafe { ptr::copy_nonoverlapping(trace_id.as_ptr(), out_trace_id, 16) };
            }
            if !out_span_id.is_null() {
                // SAFETY: caller guarantees a writable u64 at `out_span_id`.
                // `write_unaligned` tolerates a non-naturally-aligned pointer
                // handed across the C ABI (e.g. from a packed struct field).
                unsafe { out_span_id.write_unaligned(span_id) };
            }
            1
        }
        None => 0,
    })
}

/// Enable/disable `thread_dur` (CPU-time-in-span) capture (off by default; the
/// thread-CPU clock is a syscall).
#[no_mangle]
pub extern "C" fn aorta_trace_set_thread_time_enabled(on: u8) {
    ffi_guard_void(|| trace::set_thread_time_enabled(on != 0));
}

/// The tracing engine's monotonic clock (ns) — the time base every stage/span
/// record uses. For SDK call sites that measure an interval ABOVE the FFI
/// (e.g. a wait loop spanning several calls) and then record it via
/// [`aorta_trace_record_stage_at`] / [`aorta_trace_record_stage_under`].
#[no_mangle]
pub extern "C" fn aorta_trace_monotonic_ns() -> u64 {
    ffi_guard_value(0, crate::time::monotonic_ns)
}

/// Record built-in pipeline stage `stage_id` (`start_mono_ns` → now) with an
/// EXPLICIT span identity — the SDK-side mirror of the core's
/// `record_stage_elapsed_linked`, for spans whose id was pre-minted and
/// shipped on the wire (e.g. the action `goal_sent_to_accepted` span returned
/// by `aorta_action_client_send_goal_traced`). Feeds BOTH tiers (histogram +
/// waterfall). `name` is the primitive name (service/action), canonicalized
/// exactly like the core call sites so the converter sees identical events.
/// No-op when tracing is disabled or `stage_id` is unknown.
///
/// # Safety
/// - `name` must be a NUL-terminated valid UTF-8 C string for the call
/// - `trace_id` must be NULL (treated as all-zero) or point to ≥16 readable
///   bytes for the call
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_record_stage_at(
    stage_id: u8,
    name: *const c_char,
    start_mono_ns: u64,
    trace_id: *const u8,
    span_id: u64,
    parent_span_id: u64,
) {
    ffi_guard_void(|| {
        if !trace::trace_enabled() {
            return;
        }
        let Some(stage) = trace::StageId::from_u8(stage_id) else {
            return;
        };
        // SAFETY: `cstr_to_string` reads `name` under the documented contract.
        let Ok(name) = (unsafe { cstr_to_string(name, "name") }) else {
            return;
        };
        let mut id = [0u8; 16];
        if !trace_id.is_null() {
            // SAFETY: caller guarantees 16 readable bytes at `trace_id`.
            unsafe { ptr::copy_nonoverlapping(trace_id, id.as_mut_ptr(), 16) };
        }
        let event = trace::stage_event_for(stage, &name);
        trace::record_stage_elapsed_linked(
            stage,
            start_mono_ns,
            event,
            trace::SpanLink {
                trace_id: id,
                span_id,
                parent_span_id,
            },
        );
    });
}

/// Record built-in pipeline stage `stage_id` (`start_mono_ns` → now) as a
/// fresh CHILD span of `(trace_id, parent_span_id)` — the SDK-side mirror of
/// the core's `record_stage_elapsed_under` (e.g. the action `result_wait`
/// recorded around the C++ `WaitResult` loop, parented to the goal's
/// `goal_sent` span). Feeds BOTH tiers. A NULL/zero `trace_id` falls back to
/// the calling thread's local context. No-op when tracing is disabled or
/// `stage_id` is unknown.
///
/// # Safety
/// Same contracts as [`aorta_trace_record_stage_at`].
#[no_mangle]
pub unsafe extern "C" fn aorta_trace_record_stage_under(
    stage_id: u8,
    name: *const c_char,
    start_mono_ns: u64,
    trace_id: *const u8,
    parent_span_id: u64,
) {
    ffi_guard_void(|| {
        if !trace::trace_enabled() {
            return;
        }
        let Some(stage) = trace::StageId::from_u8(stage_id) else {
            return;
        };
        // SAFETY: `cstr_to_string` reads `name` under the documented contract.
        let Ok(name) = (unsafe { cstr_to_string(name, "name") }) else {
            return;
        };
        let mut id = [0u8; 16];
        if !trace_id.is_null() {
            // SAFETY: caller guarantees 16 readable bytes at `trace_id`.
            unsafe { ptr::copy_nonoverlapping(trace_id, id.as_mut_ptr(), 16) };
        }
        let event = trace::stage_event_for(stage, &name);
        let parent = (id != [0u8; 16]).then_some(trace::SpanLink {
            trace_id: id,
            span_id: parent_span_id,
            parent_span_id: 0,
        });
        trace::record_stage_elapsed_under(stage, start_mono_ns, event, parent.as_ref());
    });
}

/// Tracer bootstrap: configure from the environment (`AORTA_TRACE`,
/// `AORTA_TRACE_DIR`, …) and, if enabled, spawn the drain thread + file sink.
/// Idempotent. Call once at process startup; the CLI/SDKs call this.
#[no_mangle]
pub extern "C" fn aorta_trace_init() -> aorta_result_t {
    ffi_guard(|| {
        trace::runtime::init();
        AORTA_OK
    })
}

/// Flush + stop the trace drain thread (idempotent). Call at clean shutdown.
#[no_mangle]
pub extern "C" fn aorta_trace_shutdown() {
    ffi_guard_void(trace::runtime::shutdown);
}
