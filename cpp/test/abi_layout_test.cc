// C-ABI layout regression test.
//
// This file is the C++ side of the layout lock. The Rust side lives at
// `core/src/ffi/abi_assert.rs` and uses `const _: () = { assert!(...) };`
// to pin `size_of::<T>()` + `align_of::<T>()`. Here we use
// `static_assert(sizeof(T) == N)` plus a `offsetof(T, field) == N`
// pass per struct to lock field positions — Rust's stable
// `core::mem::offset_of!` predates 1.77 but offsetof in C++ has been
// the canonical way to express the same contract since K&R, and the
// numbers are easier for downstream C consumers to spot-check.
//
// When a test in this file fails, the on-the-wire layout of a struct
// that crosses the FFI boundary has changed. The correct response is
// NOT to update the constant — it is to:
//
//   1. Verify the change was intentional.
//   2. If yes, this is an ABI break. Bump `libaorta_core`'s SO_NAME
//      and update every SDK / language binding in
//      lockstep, then update the asserts here AND the matching
//      `abi_assert.rs` block in the same commit.
//
// All layouts here were measured on the four supported triples
// (x86_64 / aarch64 × linux-gnu / apple-darwin) and verified equal —
// every public C-ABI struct is composed of fixed-width primitives,
// 8-byte pointers (we don't target 32-bit), `size_t` (8B on all 4
// triples), and `repr(C)` enums tagged as `int`. If a future port
// introduces a triple with a different pointer / size_t width, gate
// the asserts per `#if defined(__LP64__)` etc.

#include <aorta.h>

#include <cstddef>

#include <gtest/gtest.h>

namespace {

// -----------------------------------------------------------------------------
// logging
// -----------------------------------------------------------------------------

TEST(AbiLayout, LogTypes) {
  static_assert(sizeof(aorta_log_format_t) == 4);
  static_assert(sizeof(aorta_log_level_t) == 4);
  static_assert(sizeof(aorta_log_config_t) == 24);
  static_assert(alignof(aorta_log_config_t) == 8);
  static_assert(offsetof(aorta_log_config_t, filter) == 0);
  static_assert(offsetof(aorta_log_config_t, file_path) == 8);
  static_assert(offsetof(aorta_log_config_t, format) == 16);
  static_assert(offsetof(aorta_log_config_t, stderr_enabled) == 20);
}

// -----------------------------------------------------------------------------
// string / bytes views
// -----------------------------------------------------------------------------

TEST(AbiLayout, StringAndBytesViews) {
  static_assert(sizeof(aorta_string_view_t) == 16);
  static_assert(offsetof(aorta_string_view_t, data) == 0);
  static_assert(offsetof(aorta_string_view_t, len) == 8);

  static_assert(sizeof(aorta_string_kv_t) == 16);
  static_assert(offsetof(aorta_string_kv_t, key) == 0);
  static_assert(offsetof(aorta_string_kv_t, value) == 8);

  static_assert(sizeof(aorta_schema_hash_view_t) == 16);
  static_assert(offsetof(aorta_schema_hash_view_t, hex) == 0);
  static_assert(offsetof(aorta_schema_hash_view_t, len) == 8);

  static_assert(sizeof(aorta_bytes_slice_t) == 16);
  static_assert(offsetof(aorta_bytes_slice_t, data) == 0);
  static_assert(offsetof(aorta_bytes_slice_t, len) == 8);

  static_assert(sizeof(aorta_bytes_view_t) == 24);
  static_assert(offsetof(aorta_bytes_view_t, slices) == 0);
  static_assert(offsetof(aorta_bytes_view_t, slice_count) == 8);
  static_assert(offsetof(aorta_bytes_view_t, total_len) == 16);

  static_assert(sizeof(aorta_owned_bytes_slice_t) == 32);
  static_assert(offsetof(aorta_owned_bytes_slice_t, data) == 0);
  static_assert(offsetof(aorta_owned_bytes_slice_t, len) == 8);
  static_assert(offsetof(aorta_owned_bytes_slice_t, release) == 16);
  static_assert(offsetof(aorta_owned_bytes_slice_t, release_ctx) == 24);

  static_assert(sizeof(aorta_owned_bytes_view_t) == 24);
  static_assert(offsetof(aorta_owned_bytes_view_t, slices) == 0);
  static_assert(offsetof(aorta_owned_bytes_view_t, slice_count) == 8);
  static_assert(offsetof(aorta_owned_bytes_view_t, total_len) == 16);
  static_assert(sizeof(aorta_owned_bytes_t) == 32);
  static_assert(offsetof(aorta_owned_bytes_t, view) == 0);
  static_assert(offsetof(aorta_owned_bytes_t, internal) == 24);
}

// -----------------------------------------------------------------------------
// message context + stats
// -----------------------------------------------------------------------------

TEST(AbiLayout, MsgContextAndStats) {
  static_assert(sizeof(aorta_msg_context_t) == 56);
  static_assert(offsetof(aorta_msg_context_t, topic) == 0);
  static_assert(offsetof(aorta_msg_context_t, receive_stamp_ns) == 16);
  static_assert(offsetof(aorta_msg_context_t, priority) == 24);
  static_assert(offsetof(aorta_msg_context_t, publisher_node) == 32);
  static_assert(offsetof(aorta_msg_context_t, sequence) == 48);

  static_assert(sizeof(aorta_publisher_stats_t) == 56);
  static_assert(sizeof(aorta_service_stats_t) == 96);
  static_assert(sizeof(aorta_client_stats_t) == 72);

  // Per-field offsets — the Python ctypes mirror cross-checks these (see
  // python/aorta/_ffi/structs.py EXPECTED_OFFSETS / test_structs.py
  // test_no_field_offset_drift_vs_cpp_lock). 12 × u64.
  static_assert(offsetof(aorta_service_stats_t, request_received_count) == 0);
  static_assert(offsetof(aorta_service_stats_t, request_dispatched_count) == 8);
  static_assert(offsetof(aorta_service_stats_t, reply_ok_count) == 16);
  static_assert(offsetof(aorta_service_stats_t, reply_error_count) == 24);
  static_assert(offsetof(aorta_service_stats_t, reply_send_failure_count) == 32);
  static_assert(offsetof(aorta_service_stats_t, request_dropped_count) == 40);
  static_assert(offsetof(aorta_service_stats_t, query_receive_failure_count) == 48);
  static_assert(offsetof(aorta_service_stats_t, task_exit_count) == 56);
  static_assert(offsetof(aorta_service_stats_t, in_flight_requests) == 64);
  static_assert(offsetof(aorta_service_stats_t, last_request_ns) == 72);
  static_assert(offsetof(aorta_service_stats_t, last_reply_ns) == 80);
  static_assert(
      offsetof(aorta_service_stats_t, request_dropped_deadline_count) == 88);
  // 9 × u64.
  static_assert(offsetof(aorta_client_stats_t, call_count) == 0);
  static_assert(offsetof(aorta_client_stats_t, success_count) == 8);
  static_assert(offsetof(aorta_client_stats_t, timeout_count) == 16);
  static_assert(offsetof(aorta_client_stats_t, concurrent_conflict_count) == 24);
  static_assert(offsetof(aorta_client_stats_t, error_count) == 32);
  static_assert(offsetof(aorta_client_stats_t, retry_count) == 40);
  static_assert(offsetof(aorta_client_stats_t, last_call_started_ns) == 48);
  static_assert(offsetof(aorta_client_stats_t, last_call_finished_ns) == 56);
  static_assert(offsetof(aorta_client_stats_t, last_call_latency_ns) == 64);
  // 7 × u64 (Python SDK Phase 3 — aorta_subscriber_runtime_stats).
  static_assert(sizeof(aorta_subscriber_runtime_stats_t) == 56);
  static_assert(offsetof(aorta_subscriber_runtime_stats_t, duplicate_drop_count) == 0);
  static_assert(offsetof(aorta_subscriber_runtime_stats_t, out_of_order_count) == 8);
  static_assert(offsetof(aorta_subscriber_runtime_stats_t, out_of_order_drop_count) == 16);
  static_assert(offsetof(aorta_subscriber_runtime_stats_t, executor_drop_count) == 24);
  static_assert(offsetof(aorta_subscriber_runtime_stats_t, local_drop_count) == 32);
  static_assert(
      offsetof(aorta_subscriber_runtime_stats_t, overflow_relay_enqueue_count) == 40);
  static_assert(
      offsetof(aorta_subscriber_runtime_stats_t, overflow_relay_drop_count) == 48);
}

// -----------------------------------------------------------------------------
// pub / sub / service / client / action options
// -----------------------------------------------------------------------------

TEST(AbiLayout, PubAndNodeOptions) {
  static_assert(sizeof(aorta_schema_attach_policy_t) == 4);
  static_assert(sizeof(aorta_pub_options_t) == 40);
  static_assert(offsetof(aorta_pub_options_t, schema_attach_policy) == 0);
  // congestion_control is a u8 occupying the former padding after
  // schema_attach_policy, so the struct stays 32 bytes and every following
  // offset is unchanged. It is a raw u8 (not the aorta_congestion_control_t
  // enum) so a stale-layout caller's uninitialized byte can be validated
  // rather than decoded as an invalid enum discriminant.
  static_assert(sizeof(((aorta_pub_options_t*)nullptr)->congestion_control) == 1);
  static_assert(offsetof(aorta_pub_options_t, congestion_control) == 4);
  static_assert(offsetof(aorta_pub_options_t, schema_bfbs) == 8);
  static_assert(offsetof(aorta_pub_options_t, schema_bfbs_len) == 16);
  static_assert(offsetof(aorta_pub_options_t, max_message_size) == 24);
  static_assert(offsetof(aorta_pub_options_t, schema_root_type) == 32);

  static_assert(sizeof(aorta_node_telemetry_options_t) == 64);
  static_assert(offsetof(aorta_node_telemetry_options_t, executable) == 0);
  static_assert(offsetof(aorta_node_telemetry_options_t, language) == 8);
  static_assert(offsetof(aorta_node_telemetry_options_t, sdk_name) == 16);
  static_assert(offsetof(aorta_node_telemetry_options_t, sdk_version) == 24);
  static_assert(offsetof(aorta_node_telemetry_options_t, build_id) == 32);
  static_assert(offsetof(aorta_node_telemetry_options_t, container_id) == 40);
  static_assert(offsetof(aorta_node_telemetry_options_t, labels) == 48);
  static_assert(offsetof(aorta_node_telemetry_options_t, labels_len) == 56);
  static_assert(sizeof(aorta_node_options_t) == 8);
  static_assert(offsetof(aorta_node_options_t, telemetry) == 0);
}

TEST(AbiLayout, SubServiceClientOptions) {
  static_assert(sizeof(aorta_callback_group_t) == 4);
  static_assert(sizeof(aorta_sub_overflow_policy_t) == 4);
  static_assert(sizeof(aorta_rpc_overflow_policy_t) == 4);

  // Layout-frozen at the pre-Flow 72-byte layout; the
  // QoS & buffering redesign knobs live in aorta_sub_buffering_t below.
  static_assert(sizeof(aorta_sub_options_t) == 72);
  static_assert(offsetof(aorta_sub_options_t, callback_group) == 0);
  static_assert(offsetof(aorta_sub_options_t, callback_group_handle) == 8);
  static_assert(offsetof(aorta_sub_options_t, deadline_ns) == 16);
  static_assert(offsetof(aorta_sub_options_t, prefetch_schema_on_init) == 24);
  static_assert(offsetof(aorta_sub_options_t, enable_header_context) == 25);
  static_assert(offsetof(aorta_sub_options_t, include_topic_in_context) == 26);
  static_assert(offsetof(aorta_sub_options_t, include_receive_timestamp) == 27);
  static_assert(offsetof(aorta_sub_options_t, include_priority_in_context) == 28);
  static_assert(offsetof(aorta_sub_options_t, include_publisher_timestamp) == 29);
  static_assert(offsetof(aorta_sub_options_t, enable_dedup) == 30);
  static_assert(offsetof(aorta_sub_options_t, dedup_window) == 32);
  static_assert(offsetof(aorta_sub_options_t, enable_reorder_check) == 40);
  static_assert(offsetof(aorta_sub_options_t, drop_out_of_order) == 41);
  static_assert(offsetof(aorta_sub_options_t, overflow_policy) == 44);
  static_assert(offsetof(aorta_sub_options_t, overflow_relay_capacity) == 48);
  static_assert(offsetof(aorta_sub_options_t, schema_bfbs) == 56);
  static_assert(offsetof(aorta_sub_options_t, schema_bfbs_len) == 64);
  // Buffering companion (aorta_subscriber_new_buffered): u64 + u32 + u32 = 16.
  static_assert(sizeof(aorta_sub_buffering_t) == 16);
  static_assert(offsetof(aorta_sub_buffering_t, receive_depth) == 0);
  static_assert(offsetof(aorta_sub_buffering_t, delivery_mode) == 8);
  static_assert(offsetof(aorta_sub_buffering_t, receive_overflow) == 12);
  // pull sample: bytes_view(24) + msg_context(56) + ptr(8) = 88.
  static_assert(sizeof(aorta_pull_sample_t) == 88);
  static_assert(offsetof(aorta_pull_sample_t, payload) == 0);
  static_assert(offsetof(aorta_pull_sample_t, context) == 24);
  static_assert(offsetof(aorta_pull_sample_t, internal) == 80);
  static_assert(sizeof(aorta_service_options_t) == 104);
  static_assert(sizeof(aorta_client_options_t) == 48);
  static_assert(sizeof(aorta_action_client_options_t) == 16);
  static_assert(sizeof(aorta_action_server_options_t) == 32);
  static_assert(sizeof(aorta_action_server_codec_t) == 64);

  // Per-field offsets — the Python ctypes mirror cross-checks these (see
  // python/aorta/_ffi/structs.py EXPECTED_OFFSETS / test_structs.py).
  static_assert(offsetof(aorta_service_options_t, schema_attach_policy) == 0);
  static_assert(offsetof(aorta_service_options_t, callback_group) == 4);
  static_assert(offsetof(aorta_service_options_t, callback_group_handle) == 8);
  static_assert(offsetof(aorta_service_options_t, deadline_ns) == 16);
  static_assert(offsetof(aorta_service_options_t, service_schema_hash_hex) == 24);
  static_assert(offsetof(aorta_service_options_t, service_schema_hash_len) == 32);
  static_assert(offsetof(aorta_service_options_t, service_schema_bfbs) == 40);
  static_assert(offsetof(aorta_service_options_t, service_schema_bfbs_len) == 48);
  static_assert(offsetof(aorta_service_options_t, request_root_type) == 56);
  static_assert(offsetof(aorta_service_options_t, request_root_type_len) == 64);
  static_assert(offsetof(aorta_service_options_t, response_root_type) == 72);
  static_assert(offsetof(aorta_service_options_t, response_root_type_len) == 80);
  static_assert(offsetof(aorta_service_options_t, rpc_overflow_policy) == 88);
  static_assert(
      offsetof(aorta_service_options_t, rpc_overflow_block_timeout_ms) == 96);

  static_assert(offsetof(aorta_client_options_t, schema_attach_policy) == 0);
  static_assert(offsetof(aorta_client_options_t, timeout_ms) == 8);
  static_assert(offsetof(aorta_client_options_t, enforce_single_provider) == 16);
  static_assert(offsetof(aorta_client_options_t, allow_retry) == 17);
  static_assert(
      offsetof(aorta_client_options_t, multi_reply_check_window_ms) == 24);
  static_assert(offsetof(aorta_client_options_t, service_schema_hash_hex) == 32);
  static_assert(offsetof(aorta_client_options_t, service_schema_hash_len) == 40);

  // Action options + codec vtable (Python SDK Phase 4 mirror cross-checks).
  static_assert(offsetof(aorta_action_client_options_t, control_timeout_ms) == 0);
  static_assert(offsetof(aorta_action_client_options_t, info_timeout_ms) == 8);
  static_assert(offsetof(aorta_action_server_options_t, supports_cancel) == 0);
  static_assert(offsetof(aorta_action_server_options_t, max_concurrency) == 8);
  static_assert(offsetof(aorta_action_server_options_t, result_ttl_ms) == 16);
  static_assert(offsetof(aorta_action_server_options_t, dedup_ttl_ms) == 24);
  static_assert(
      offsetof(aorta_action_server_codec_t, decode_send_goal_request_goal_id) == 0);
  static_assert(offsetof(aorta_action_server_codec_t, decode_cancel_goal_request) == 8);
  static_assert(offsetof(aorta_action_server_codec_t, decode_get_result_request) == 16);
  static_assert(offsetof(aorta_action_server_codec_t, encode_send_goal_response) == 24);
  static_assert(offsetof(aorta_action_server_codec_t, encode_cancel_goal_response) == 32);
  static_assert(offsetof(aorta_action_server_codec_t, encode_public_status) == 40);
  static_assert(offsetof(aorta_action_server_codec_t, encode_feedback) == 48);
  static_assert(offsetof(aorta_action_server_codec_t, encode_get_result_response) == 56);
  static_assert(sizeof(aorta_action_server_callbacks_t) == 32);
  static_assert(alignof(aorta_action_server_callbacks_t) == 8);
  static_assert(offsetof(aorta_action_server_callbacks_t, execute) == 0);
  static_assert(offsetof(aorta_action_server_callbacks_t, cancel_requested) == 8);
  static_assert(offsetof(aorta_action_server_callbacks_t, user_data) == 16);
  static_assert(offsetof(aorta_action_server_callbacks_t, user_data_release) == 24);
}

// -----------------------------------------------------------------------------
// executor / timer
// -----------------------------------------------------------------------------

TEST(AbiLayout, Executor) {
  static_assert(sizeof(aorta_executor_type_t) == 4);
  static_assert(sizeof(aorta_scheduler_mode_t) == 4);
  static_assert(sizeof(aorta_priority_scheduler_options_t) == 12);
  static_assert(alignof(aorta_priority_scheduler_options_t) == 4);
  static_assert(sizeof(aorta_executor_admission_options_t) == 16);
  static_assert(sizeof(aorta_executor_options_t) == 32);
  static_assert(sizeof(aorta_executor_admission_stats_t) == 64);
  // Per-field offsets for the admission-stats out-struct (Python ctypes mirror
  // aorta.Executor.get_admission_stats cross-checks these).
  static_assert(offsetof(aorta_executor_admission_stats_t, queued_events) == 0);
  static_assert(offsetof(aorta_executor_admission_stats_t, queued_bytes) == 8);
  static_assert(offsetof(aorta_executor_admission_stats_t, peak_queued_events) == 16);
  static_assert(offsetof(aorta_executor_admission_stats_t, peak_queued_bytes) == 24);
  static_assert(offsetof(aorta_executor_admission_stats_t, dropped_queue_full) == 32);
  static_assert(offsetof(aorta_executor_admission_stats_t, dropped_queue_closed) == 40);
  static_assert(offsetof(aorta_executor_admission_stats_t, dropped_budget_events) == 48);
  static_assert(offsetof(aorta_executor_admission_stats_t, dropped_budget_bytes) == 56);
  static_assert(sizeof(aorta_timer_overflow_policy_t) == 4);
  static_assert(sizeof(aorta_executor_class_policy_options_t) == 48);
  static_assert(sizeof(aorta_timer_options_t) == 32);
  // Per-field offsets (Python ctypes mirror cross-checks these).
  static_assert(offsetof(aorta_timer_options_t, callback_group) == 0);
  static_assert(offsetof(aorta_timer_options_t, callback_group_handle) == 8);
  static_assert(offsetof(aorta_timer_options_t, auto_start) == 16);
  static_assert(offsetof(aorta_timer_options_t, invoke_on_missed) == 17);
  static_assert(offsetof(aorta_timer_options_t, overflow_policy) == 20);
  static_assert(offsetof(aorta_timer_options_t, overflow_block_timeout_ms) == 24);
}

// -----------------------------------------------------------------------------
// QoS
// -----------------------------------------------------------------------------

TEST(AbiLayout, QoS) {
  static_assert(sizeof(aorta_priority_t) == 4);
  static_assert(sizeof(aorta_reliability_t) == 4);
  // priority(4) + reliability(4) + congestion_control(1) + express(1) + pad(2).
  static_assert(sizeof(aorta_qos_t) == 12);
  static_assert(alignof(aorta_qos_t) == 4);
  static_assert(offsetof(aorta_qos_t, priority) == 0);
  static_assert(offsetof(aorta_qos_t, reliability) == 4);
  static_assert(offsetof(aorta_qos_t, congestion_control) == 8);
  static_assert(offsetof(aorta_qos_t, express) == 9);
  static_assert(sizeof(aorta_qos_preset_t) == 4);
}

// -----------------------------------------------------------------------------
// context store
// -----------------------------------------------------------------------------

TEST(AbiLayout, ContextStore) {
  static_assert(sizeof(aorta_ctx_read_t) == 88);
  static_assert(sizeof(aorta_ctx_change_t) == 128);
  static_assert(offsetof(aorta_ctx_change_t, kind) == 0);
  static_assert(offsetof(aorta_ctx_change_t, snapshot_done_reason) == 1);
  static_assert(offsetof(aorta_ctx_change_t, key) == 8);
  // Remaining ctx_change fields + full ctx_read offsets (Python ctypes mirror
  // cross-checks these).
  static_assert(offsetof(aorta_ctx_change_t, value_kind) == 24);
  static_assert(offsetof(aorta_ctx_change_t, bool_value) == 25);
  static_assert(offsetof(aorta_ctx_change_t, i64_value) == 32);
  static_assert(offsetof(aorta_ctx_change_t, u64_value) == 40);
  static_assert(offsetof(aorta_ctx_change_t, f64_value) == 48);
  static_assert(offsetof(aorta_ctx_change_t, string_value) == 56);
  static_assert(offsetof(aorta_ctx_change_t, stamp_ns) == 72);
  static_assert(offsetof(aorta_ctx_change_t, expires_at_ns) == 80);
  static_assert(offsetof(aorta_ctx_change_t, source_node) == 88);
  static_assert(offsetof(aorta_ctx_change_t, source_instance_id) == 104);
  static_assert(offsetof(aorta_ctx_change_t, sequence) == 120);

  static_assert(offsetof(aorta_ctx_read_t, value_kind) == 0);
  static_assert(offsetof(aorta_ctx_read_t, bool_value) == 1);
  static_assert(offsetof(aorta_ctx_read_t, i64_value) == 8);
  static_assert(offsetof(aorta_ctx_read_t, u64_value) == 16);

  // ABI generation 10: owner-liveliness event.
  static_assert(sizeof(aorta_ctx_owner_liveliness_event_t) == 40);
  static_assert(offsetof(aorta_ctx_owner_liveliness_event_t, node_name) == 0);
  static_assert(offsetof(aorta_ctx_owner_liveliness_event_t, source_instance_id) == 16);
  static_assert(offsetof(aorta_ctx_owner_liveliness_event_t, alive) == 32);
  static_assert(offsetof(aorta_ctx_read_t, f64_value) == 24);
  static_assert(offsetof(aorta_ctx_read_t, string_value) == 32);
  static_assert(offsetof(aorta_ctx_read_t, is_fresh) == 40);
  static_assert(offsetof(aorta_ctx_read_t, stamp_ns) == 48);
  static_assert(offsetof(aorta_ctx_read_t, expires_at_ns) == 56);
  static_assert(offsetof(aorta_ctx_read_t, source_node) == 64);
  static_assert(offsetof(aorta_ctx_read_t, source_instance_id) == 72);
  static_assert(offsetof(aorta_ctx_read_t, sequence) == 80);
}

// -----------------------------------------------------------------------------
// capture + schema resolver (ABI v6) — matching the abi_assert.rs block.
// -----------------------------------------------------------------------------

TEST(AbiLayout, CaptureAndSchemaResolver) {
  // [u8;16](16) + u64(8) + u8(1)+pad(7).
  static_assert(sizeof(aorta_traceparent_t) == 32);
  static_assert(alignof(aorta_traceparent_t) == 8);
  static_assert(offsetof(aorta_traceparent_t, trace_id) == 0);
  static_assert(offsetof(aorta_traceparent_t, span_id) == 16);
  static_assert(offsetof(aorta_traceparent_t, flags) == 24);

  // i32(4)+pad(4) + *const c_char(8).
  static_assert(sizeof(aorta_capture_selector_t) == 16);
  static_assert(alignof(aorta_capture_selector_t) == 8);
  static_assert(offsetof(aorta_capture_selector_t, kind) == 0);
  static_assert(offsetof(aorta_capture_selector_t, pattern) == 8);

  // string_view(16) + string_view(16) + bytes_view(24) + bool(1)+pad(7)
  // + hash_view(16) + bool(1)+pad(7) + traceparent(32) + bool(1)+pad(7)
  // + u64(8) + u64(8) + i32(4)+pad(4).
  static_assert(sizeof(aorta_captured_sample_t) == 152);
  static_assert(alignof(aorta_captured_sample_t) == 8);
  static_assert(offsetof(aorta_captured_sample_t, key) == 0);
  static_assert(offsetof(aorta_captured_sample_t, key_suffix) == 16);
  static_assert(offsetof(aorta_captured_sample_t, payload) == 32);
  static_assert(offsetof(aorta_captured_sample_t, has_schema_hash) == 56);
  static_assert(offsetof(aorta_captured_sample_t, schema_hash) == 64);
  static_assert(offsetof(aorta_captured_sample_t, has_traceparent) == 80);
  static_assert(offsetof(aorta_captured_sample_t, traceparent) == 88);
  static_assert(offsetof(aorta_captured_sample_t, has_publish_time) == 120);
  static_assert(offsetof(aorta_captured_sample_t, publish_time_ns) == 128);
  static_assert(offsetof(aorta_captured_sample_t, recv_time_ns) == 136);
  static_assert(offsetof(aorta_captured_sample_t, kind) == 144);

  // usize(8) + u32(4) + u32(4).
  static_assert(sizeof(aorta_schema_resolver_opts_t) == 16);
  static_assert(alignof(aorta_schema_resolver_opts_t) == 8);
  static_assert(offsetof(aorta_schema_resolver_opts_t, cache_max_entries) == 0);
  static_assert(offsetof(aorta_schema_resolver_opts_t, by_hash_timeout_ms) == 8);
  static_assert(offsetof(aorta_schema_resolver_opts_t, by_key_timeout_ms) == 12);

  // hash_view(16) + string_view(16) + bytes_view(24) + *mut c_void(8).
  static_assert(sizeof(aorta_resolved_schema_t) == 64);
  static_assert(alignof(aorta_resolved_schema_t) == 8);
  static_assert(offsetof(aorta_resolved_schema_t, schema_hash) == 0);
  static_assert(offsetof(aorta_resolved_schema_t, root_type_name) == 16);
  static_assert(offsetof(aorta_resolved_schema_t, bfbs) == 32);
  static_assert(offsetof(aorta_resolved_schema_t, internal) == 56);
}

// Body-less TEST cases are intentional — every assertion is a
// `static_assert` so it fires at compile time. The Bazel test target
// runs the gtest binary so the assertions are exercised against the
// SAME headers the C++ SDK ships with — if cbindgen emits a header
// that diverges from the Rust source struct, this test fails to
// compile.

}  // namespace

// ---------------------------------------------------------------------------
// Flow C-ABI structs: the C++-side locks matching
// core/src/ffi/abi_assert.rs.
// ---------------------------------------------------------------------------
static_assert(sizeof(aorta_scope_opts_t) == 24);
static_assert(alignof(aorta_scope_opts_t) == 8);
static_assert(offsetof(aorta_scope_opts_t, priority) == 0);
static_assert(offsetof(aorta_scope_opts_t, supervision) == 1);
static_assert(offsetof(aorta_scope_opts_t, budget_ns) == 8);
static_assert(offsetof(aorta_scope_opts_t, name) == 16);

static_assert(sizeof(aorta_executor_sched_options_t) == 8);
static_assert(alignof(aorta_executor_sched_options_t) == 4);
static_assert(offsetof(aorta_executor_sched_options_t, worker_count) == 0);
static_assert(offsetof(aorta_executor_sched_options_t, sched) == 4);

static_assert(sizeof(aorta_admission_t) == 16);
static_assert(alignof(aorta_admission_t) == 8);
static_assert(offsetof(aorta_admission_t, max_in_flight) == 0);
static_assert(offsetof(aorta_admission_t, deadline_ns) == 8);

static_assert(sizeof(aorta_resume_event_t) == 16);
static_assert(alignof(aorta_resume_event_t) == 8);
static_assert(offsetof(aorta_resume_event_t, handle) == 0);
static_assert(offsetof(aorta_resume_event_t, status) == 8);
// stop_kind/cancel_reason grew INTO the former tail
// padding — sizeof/alignof unchanged (see abi_assert.rs's matching
// comment on the same struct).
static_assert(offsetof(aorta_resume_event_t, stop_kind) == 12);
static_assert(offsetof(aorta_resume_event_t, cancel_reason) == 13);

// ---------------------------------------------------------------------------
// The canonical Flow completion record — OUTPUT-only,
// callback-scoped (never retained past the call). i32(4) + u8(1) + u8(1) +
// [u8;2](2) + bytes_view(24) = 32.
// ---------------------------------------------------------------------------
static_assert(sizeof(aorta_flow_completion_t) == 32);
static_assert(alignof(aorta_flow_completion_t) == 8);
static_assert(offsetof(aorta_flow_completion_t, code) == 0);
static_assert(offsetof(aorta_flow_completion_t, stop_kind) == 4);
static_assert(offsetof(aorta_flow_completion_t, cancel_reason) == 5);
static_assert(offsetof(aorta_flow_completion_t, detail) == 8);

// ---------------------------------------------------------------------------
// Flow: the facade hooks struct.
// drain_fn(8) + ctx(8) + on_stop(8) + forward_fn(8) + pool_workers(4+pad)
// = 40. The `_reserved` tail was removed: INPUT
// structs must not ship speculative fields (evolution policy).
// ---------------------------------------------------------------------------
static_assert(sizeof(aorta_lane_hooks_t) == 40);
static_assert(alignof(aorta_lane_hooks_t) == 8);
static_assert(offsetof(aorta_lane_hooks_t, drain_fn) == 0);
static_assert(offsetof(aorta_lane_hooks_t, ctx) == 8);
static_assert(offsetof(aorta_lane_hooks_t, on_stop) == 16);
static_assert(offsetof(aorta_lane_hooks_t, forward_fn) == 24);
static_assert(offsetof(aorta_lane_hooks_t, pool_workers) == 32);
