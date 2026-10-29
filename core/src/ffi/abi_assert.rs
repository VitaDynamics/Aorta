//! Compile-time lock on every public `#[repr(C)]` C-ABI type's
//! layout.
//!
//! Each `const _: () = { ... };` block runs at compile time and aborts
//! the build the moment a field reorder, type change, or new field
//! shifts `size_of::<T>()` or `align_of::<T>()`. The C++ side carries
//! the matching `static_assert(sizeof(...) == N)` + `offsetof` set in
//! `cpp/test/abi_layout_test.cc`; the two together prevent silent ABI
//! breakage between the Rust producer and the C/C++/Python consumer
//! of cbindgen-generated headers.
//!
//! ## When this file fires
//!
//! A failed assertion means you (probably accidentally) changed the
//! on-the-wire layout of a struct that crosses the FFI boundary. The
//! correct fix is **not** to update the number — it is to ask:
//!
//!   1. Was the change intentional? If no, revert the source struct.
//!   2. If yes, this is an **ABI break**. Bump the SO_NAME of
//!      `libaorta_core` and update every consumer
//!      crate / SDK pin in lockstep. Update the asserted constant in
//!      the same commit so future readers see the link between the
//!      layout change and the version bump.
//!
//! Values were baked from `x86_64-apple-darwin` (and verified equal on
//! `aarch64-apple-darwin` / `*-unknown-linux-gnu` — all our supported
//! triples share the same layout for these types because every struct
//! is composed of fixed-width primitives, pointers, `usize`, and
//! `#[repr(C)]` enums tagged as plain `int`). If a future port adds a
//! triple with different pointer or `usize` width, gate the constants
//! per `#[cfg(target_pointer_width = ...)]`.

use std::mem::{align_of, offset_of, size_of};

#[allow(unused_imports)]
use crate::ffi::c_api::*;

// -------------------------------------------------------------------
// logging
// -------------------------------------------------------------------

const _: () = {
    // `aorta_log_format_t` — `#[repr(C)]` C enum, fixed at 4 bytes.
    assert!(size_of::<aorta_log_format_t>() == 4);
    assert!(align_of::<aorta_log_format_t>() == 4);
    // `aorta_log_level_t` — same.
    assert!(size_of::<aorta_log_level_t>() == 4);
    assert!(align_of::<aorta_log_level_t>() == 4);
    // `aorta_log_config_t` — { *const c_char, *const c_char, enum, u8 }
    // = 8 + 8 + 4 + (1 + 3 padding) = 24, aligned to 8 by pointer.
    assert!(size_of::<aorta_log_config_t>() == 24);
    assert!(align_of::<aorta_log_config_t>() == 8);
};

// -------------------------------------------------------------------
// string / bytes views
// -------------------------------------------------------------------

const _: () = {
    // (*const u8, usize) = 16, aligned 8.
    assert!(size_of::<aorta_string_view_t>() == 16);
    assert!(align_of::<aorta_string_view_t>() == 8);
    // (*const c_char, *const c_char).
    assert!(size_of::<aorta_string_kv_t>() == 16);
    assert!(align_of::<aorta_string_kv_t>() == 8);
    // (*const u8, usize).
    assert!(size_of::<aorta_schema_hash_view_t>() == 16);
    assert!(align_of::<aorta_schema_hash_view_t>() == 8);
    // (*const u8, usize).
    assert!(size_of::<aorta_bytes_slice_t>() == 16);
    assert!(align_of::<aorta_bytes_slice_t>() == 8);
    // (*const slice, usize, usize).
    assert!(size_of::<aorta_bytes_view_t>() == 24);
    assert!(align_of::<aorta_bytes_view_t>() == 8);
    // (*const u8, usize, fn-ptr, *mut c_void).
    assert!(size_of::<aorta_owned_bytes_slice_t>() == 32);
    assert!(align_of::<aorta_owned_bytes_slice_t>() == 8);
    // (*const owned-slice, usize, usize).
    assert!(size_of::<aorta_owned_bytes_view_t>() == 24);
    assert!(align_of::<aorta_owned_bytes_view_t>() == 8);
    // (aorta_bytes_view_t = 24, *mut c_void = 8) = 32.
    assert!(size_of::<aorta_owned_bytes_t>() == 32);
    assert!(align_of::<aorta_owned_bytes_t>() == 8);
};

// -------------------------------------------------------------------
// message context + stats
// -------------------------------------------------------------------

const _: () = {
    // string_view(16) + i64(8) + i32(4)+pad(4) + string_view(16) + u64(8).
    assert!(size_of::<aorta_msg_context_t>() == 56);
    assert!(align_of::<aorta_msg_context_t>() == 8);
    // pull sample: bytes_view(24) + msg_context(56) + *mut c_void(8) = 88.
    assert!(size_of::<aorta_pull_sample_t>() == 88);
    assert!(align_of::<aorta_pull_sample_t>() == 8);
    // 7 × u64/usize fields.
    assert!(size_of::<aorta_publisher_stats_t>() == 56);
    assert!(align_of::<aorta_publisher_stats_t>() == 8);
    // 12 × u64.
    assert!(size_of::<aorta_service_stats_t>() == 96);
    assert!(align_of::<aorta_service_stats_t>() == 8);
    // 9 × u64.
    assert!(size_of::<aorta_client_stats_t>() == 72);
    assert!(align_of::<aorta_client_stats_t>() == 8);
    // 7 × u64.
    assert!(size_of::<aorta_subscriber_runtime_stats_t>() == 56);
    assert!(align_of::<aorta_subscriber_runtime_stats_t>() == 8);
};

// -------------------------------------------------------------------
// capture + schema resolver (ABI v6)
// -------------------------------------------------------------------

const _: () = {
    // [u8;16](16) + u64(8) + u8(1)+pad(7).
    assert!(size_of::<aorta_traceparent_t>() == 32);
    assert!(align_of::<aorta_traceparent_t>() == 8);
    // i32(4)+pad(4) + *const c_char(8).
    assert!(size_of::<aorta_capture_selector_t>() == 16);
    assert!(align_of::<aorta_capture_selector_t>() == 8);
    // string_view(16) + string_view(16) + bytes_view(24) + bool(1)+pad(7)
    // + hash_view(16) + bool(1)+pad(7) + traceparent(32) + bool(1)+pad(7)
    // + u64(8) + u64(8) + i32(4)+pad(4).
    assert!(size_of::<aorta_captured_sample_t>() == 152);
    assert!(align_of::<aorta_captured_sample_t>() == 8);
    // usize(8) + u32(4) + u32(4).
    assert!(size_of::<aorta_schema_resolver_opts_t>() == 16);
    assert!(align_of::<aorta_schema_resolver_opts_t>() == 8);
    // hash_view(16) + string_view(16) + bytes_view(24) + *mut c_void(8).
    assert!(size_of::<aorta_resolved_schema_t>() == 64);
    assert!(align_of::<aorta_resolved_schema_t>() == 8);
};

// -------------------------------------------------------------------
// pub / sub / service / client / action options
// -------------------------------------------------------------------

const _: () = {
    assert!(size_of::<aorta_schema_attach_policy_t>() == 4);
    assert!(align_of::<aorta_schema_attach_policy_t>() == 4);
    // enum(4)+u8+pad(3) + *const u8(8) + usize(8) + usize(8) + *const c_char(8).
    assert!(size_of::<aorta_pub_options_t>() == 40);
    assert!(align_of::<aorta_pub_options_t>() == 8);
    assert!(offset_of!(aorta_pub_options_t, schema_attach_policy) == 0);
    assert!(offset_of!(aorta_pub_options_t, congestion_control) == 4);
    assert!(offset_of!(aorta_pub_options_t, schema_bfbs) == 8);
    assert!(offset_of!(aorta_pub_options_t, schema_bfbs_len) == 16);
    assert!(offset_of!(aorta_pub_options_t, max_message_size) == 24);
    assert!(offset_of!(aorta_pub_options_t, schema_root_type) == 32);
    // 6 × *const c_char + *const kv + usize.
    assert!(size_of::<aorta_node_telemetry_options_t>() == 64);
    assert!(align_of::<aorta_node_telemetry_options_t>() == 8);
    // single pointer.
    assert!(size_of::<aorta_node_options_t>() == 8);
    assert!(align_of::<aorta_node_options_t>() == 8);
};

const _: () = {
    assert!(size_of::<aorta_callback_group_t>() == 4);
    assert!(align_of::<aorta_callback_group_t>() == 4);
    assert!(size_of::<aorta_sub_overflow_policy_t>() == 4);
    assert!(align_of::<aorta_sub_overflow_policy_t>() == 4);
    assert!(size_of::<aorta_rpc_overflow_policy_t>() == 4);
    assert!(align_of::<aorta_rpc_overflow_policy_t>() == 4);
    // Padding-heavy: enum(4)+pad(4)+u64(8)+i64(8)+8×u8(packed into one 8B
    // word after rust's natural reorder is disabled by repr(C))+enum(4)+
    // pad(4)+usize(8)+*const u8(8)+usize(8) = 72. Layout-frozen (a shipped
    // C ABI struct never grows): the QoS & buffering knobs live in the companion
    // aorta_sub_buffering_t, NOT appended here.
    assert!(size_of::<aorta_sub_options_t>() == 72);
    assert!(align_of::<aorta_sub_options_t>() == 8);
    // u64(8)+u32(4)+u32(4) = 16, no padding.
    assert!(size_of::<aorta_sub_buffering_t>() == 16);
    assert!(align_of::<aorta_sub_buffering_t>() == 8);
    assert!(size_of::<aorta_service_options_t>() == 104);
    assert!(align_of::<aorta_service_options_t>() == 8);
    assert!(size_of::<aorta_client_options_t>() == 48);
    assert!(align_of::<aorta_client_options_t>() == 8);
    assert!(size_of::<aorta_action_client_options_t>() == 16);
    assert!(align_of::<aorta_action_client_options_t>() == 8);
    assert!(size_of::<aorta_action_server_options_t>() == 32);
    assert!(align_of::<aorta_action_server_options_t>() == 8);
    // 8 × fn-ptr (each `Option<unsafe extern "C" fn(...)>` = 8B).
    assert!(size_of::<aorta_action_server_codec_t>() == 64);
    assert!(align_of::<aorta_action_server_codec_t>() == 8);
    // execute(8) + cancel_requested(8) + user_data(8) + user_data_release(8).
    assert!(size_of::<aorta_action_server_callbacks_t>() == 32);
    assert!(align_of::<aorta_action_server_callbacks_t>() == 8);
    assert!(offset_of!(aorta_action_server_callbacks_t, execute) == 0);
    assert!(offset_of!(aorta_action_server_callbacks_t, cancel_requested) == 8);
    assert!(offset_of!(aorta_action_server_callbacks_t, user_data) == 16);
    assert!(offset_of!(aorta_action_server_callbacks_t, user_data_release) == 24);
};

// -------------------------------------------------------------------
// executor / timer
// -------------------------------------------------------------------

const _: () = {
    assert!(size_of::<aorta_executor_type_t>() == 4);
    assert!(align_of::<aorta_executor_type_t>() == 4);
    assert!(size_of::<aorta_scheduler_mode_t>() == 4);
    assert!(align_of::<aorta_scheduler_mode_t>() == 4);
    // u32(4) + [u8; 5] + u8(1) + pad(2) — natural align 4 because no
    // 8-byte field is present.
    assert!(size_of::<aorta_priority_scheduler_options_t>() == 12);
    assert!(align_of::<aorta_priority_scheduler_options_t>() == 4);
    // 2 × usize.
    assert!(size_of::<aorta_executor_admission_options_t>() == 16);
    assert!(align_of::<aorta_executor_admission_options_t>() == 8);
    // enum(4)+pad(4) + 3 × *const.
    assert!(size_of::<aorta_executor_options_t>() == 32);
    assert!(align_of::<aorta_executor_options_t>() == 8);
    // 8 × u64.
    assert!(size_of::<aorta_executor_admission_stats_t>() == 64);
    assert!(align_of::<aorta_executor_admission_stats_t>() == 8);
    assert!(size_of::<aorta_timer_overflow_policy_t>() == 4);
    assert!(align_of::<aorta_timer_overflow_policy_t>() == 4);
    // enum(4)+usize(8)+enum(4)+pad(4)+u64(8)+enum(4)+pad(4)+u64(8) = 48.
    assert!(size_of::<aorta_executor_class_policy_options_t>() == 48);
    assert!(align_of::<aorta_executor_class_policy_options_t>() == 8);
    // enum(4)+handle(usize)(8)+u8+u8+enum(4)+pad+u64(8) packed: 32.
    assert!(size_of::<aorta_timer_options_t>() == 32);
    assert!(align_of::<aorta_timer_options_t>() == 8);
};

// -------------------------------------------------------------------
// QoS
// -------------------------------------------------------------------

const _: () = {
    assert!(size_of::<aorta_priority_t>() == 4);
    assert!(align_of::<aorta_priority_t>() == 4);
    assert!(size_of::<aorta_reliability_t>() == 4);
    assert!(align_of::<aorta_reliability_t>() == 4);
    // enum(4) + enum(4) + u8 + u8 + pad(2) = 12, aligned 4.
    assert!(size_of::<aorta_qos_t>() == 12);
    assert!(align_of::<aorta_qos_t>() == 4);
    assert!(size_of::<aorta_qos_preset_t>() == 4);
    assert!(align_of::<aorta_qos_preset_t>() == 4);
};

// -------------------------------------------------------------------
// context store
// -------------------------------------------------------------------

const _: () = {
    // u8 + bool + i64 + u64 + f64 + *mut + bool + u64 + u64 + *mut + *mut + u64.
    // Natural repr(C) layout with padding works out to 88.
    assert!(size_of::<aorta_ctx_read_t>() == 88);
    assert!(align_of::<aorta_ctx_read_t>() == 8);
    // Larger because every metadata string is a (data,len) view (16B).
    assert!(size_of::<aorta_ctx_change_t>() == 128);
    assert!(align_of::<aorta_ctx_change_t>() == 8);
    // `aorta_ctx_owner_liveliness_event_t` — { aorta_string_view_t (16),
    //   aorta_string_view_t (16), bool (1) } = 33 bytes -> padded to 40
    //   (align 8). Added in ABI generation 10 (ContextStore owner liveliness).
    assert!(size_of::<aorta_ctx_owner_liveliness_event_t>() == 40);
    assert!(align_of::<aorta_ctx_owner_liveliness_event_t>() == 8);
};

// -------------------------------------------------------------------
// flow
// -------------------------------------------------------------------

const _: () = {
    // `aorta_scope_opts_t` — { u8, u8, [u8;2], u64, *const c_char }
    // = (1+1+2 + 4 padding) + 8 + 8 = 24, aligned to 8 by u64/pointer.
    assert!(size_of::<aorta_scope_opts_t>() == 24);
    assert!(align_of::<aorta_scope_opts_t>() == 8);
    // `aorta_executor_sched_options_t` — { u32, u8, [u8;3] } = 8, align 4.
    assert!(size_of::<aorta_executor_sched_options_t>() == 8);
    assert!(align_of::<aorta_executor_sched_options_t>() == 4);
};

const _: () = {
    // `aorta_admission_t` — { u32, (4B implicit pad), u64 } = 16, align 8.
    assert!(size_of::<aorta_admission_t>() == 16);
    assert!(align_of::<aorta_admission_t>() == 8);
};

const _: () = {
    // `aorta_resume_event_t` — { *mut c_void, i32, u8, u8, [u8;2] } = 16/8.
    // stop_kind/cancel_reason were grown INTO the former padding: the size
    // is unchanged, old callbacks keep reading a valid prefix.
    assert!(size_of::<aorta_resume_event_t>() == 16);
    assert!(align_of::<aorta_resume_event_t>() == 8);
};

const _: () = {
    // `aorta_flow_completion_t` — { i32, u8, u8, [u8;2],
    // aorta_bytes_view_t(24) } = 32/8. OUTPUT-only, callback-scoped.
    assert!(size_of::<crate::ffi::c_api::flow::aorta_flow_completion_t>() == 32);
    assert!(align_of::<crate::ffi::c_api::flow::aorta_flow_completion_t>() == 8);
};

const _: () = {
    // Flow: `aorta_lane_hooks_t` — 3 fn/ptr
    // fields + Option<fn> + u32 (4B implicit tail pad) = 40/8. The
    // `_reserved` tail was removed: INPUT structs
    // must not ship speculative fields (evolution policy, mod.rs).
    // The C++ twin locks the field offsets (cpp/test/abi_layout_test.cc).
    assert!(size_of::<crate::ffi::c_api::executor::aorta_lane_hooks_t>() == 40);
    assert!(align_of::<crate::ffi::c_api::executor::aorta_lane_hooks_t>() == 8);
};
