#pragma once

#include <cstdint>

#include <aorta.h>

// FFI declarations not guaranteed by older generated headers.
extern "C" aorta_result_t aorta_publisher_get_stats(
    aorta_pub_t handle, aorta_publisher_stats_t* out);

extern "C" aorta_result_t aorta_node_get_id(
    aorta_node_t node, char* out_buf, size_t buf_size, size_t* out_len);
extern "C" aorta_result_t aorta_node_get_group(
    aorta_node_t node, char* out_buf, size_t buf_size, size_t* out_len);
extern "C" aorta_result_t aorta_node_has_event_sender(
    aorta_node_t node, uint8_t* out_has_event_sender);
extern "C" aorta_result_t aorta_node_validate_topic_name(
    aorta_node_t node, const char* topic);
extern "C" aorta_result_t aorta_node_validate_service_name(
    aorta_node_t node, const char* service_name);
extern "C" aorta_result_t aorta_node_create_callback_group(
    aorta_node_t node, aorta_callback_group_t group,
    aorta_callback_group_handle_t* out);
extern "C" aorta_result_t aorta_node_free_callback_group(
    aorta_node_t node, aorta_callback_group_handle_t callback_group);
extern "C" aorta_result_t aorta_now_ns(int64_t* out_ns);
extern "C" aorta_result_t aorta_uuid_v7(char* out_text, size_t out_size);

extern "C" aorta_result_t aorta_publisher_publish_owned(
    aorta_pub_t handle, const aorta_owned_bytes_view_t* payload);

extern "C" aorta_result_t aorta_service_new_with_options(
    aorta_node_t node, const char* service_name,
    const aorta_service_options_t* opts,
    void (*callback)(const aorta_bytes_view_t*, aorta_service_request_t, void*),
    void* user_data, aorta_service_t* out_service);
extern "C" aorta_result_t aorta_service_free(aorta_service_t service);
extern "C" aorta_result_t aorta_service_request_reply(
    aorta_service_request_t request, const aorta_owned_bytes_view_t* response);
extern "C" aorta_result_t aorta_service_request_reply_error(
    aorta_service_request_t request, const char* error_msg);
extern "C" aorta_result_t aorta_service_request_release(
    aorta_service_request_t request);
extern "C" uint8_t aorta_service_request_trace_context(
    aorta_service_request_t request, uint8_t* out_trace_id,
    uint64_t* out_span_id);
extern "C" aorta_result_t aorta_service_get_stats(
    aorta_service_t service, aorta_service_stats_t* out);

extern "C" aorta_result_t aorta_client_new_with_options(
    aorta_node_t node, const char* service_name,
    const aorta_client_options_t* opts, aorta_client_t* out_client);
extern "C" aorta_result_t aorta_client_call(
    aorta_client_t client, const aorta_owned_bytes_view_t* request,
    aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_client_get_stats(aorta_client_t client,
                                                   aorta_client_stats_t* out);
extern "C" void aorta_owned_bytes_free(aorta_owned_bytes_t* bytes);
extern "C" aorta_result_t aorta_client_free(aorta_client_t client);

extern "C" aorta_result_t aorta_action_client_new_with_options(
    aorta_node_t node, const char* action_name,
    const aorta_action_client_options_t* opts,
    aorta_action_client_t* out_client);
extern "C" aorta_result_t aorta_action_server_new_with_options(
    aorta_node_t node, const char* action_name, const uint8_t* schema_bfbs,
    size_t schema_bfbs_len, const aorta_action_server_options_t* opts,
    const aorta_action_server_codec_t* codec,
    void (*execute_cb)(const aorta_bytes_view_t* request,
                       aorta_action_context_t ctx, void* user_data),
    void* user_data, aorta_action_server_t* out_server);
extern "C" aorta_result_t aorta_action_server_new_with_callbacks(
    aorta_node_t node, const char* action_name, const uint8_t* schema_bfbs,
    size_t schema_bfbs_len, const aorta_action_server_options_t* opts,
    const aorta_action_server_codec_t* codec,
    const aorta_action_server_callbacks_t* callbacks,
    aorta_action_server_t* out_server);
extern "C" aorta_result_t aorta_action_client_send_goal(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_action_client_query_status(
    aorta_action_client_t client, const char* goal_id,
    aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_action_client_query_status_with_timeout(
    aorta_action_client_t client, const char* goal_id, uint64_t timeout_ms,
    aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_action_client_cancel(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_action_client_get_result(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_action_client_get_result_with_timeout(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    uint64_t timeout_ms, aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_action_client_info(
    aorta_action_client_t client, aorta_owned_bytes_t* response);
extern "C" aorta_result_t aorta_action_client_subscribe_feedback(
    aorta_action_client_t client, aorta_on_msg_cb cb, void* user_data,
    aorta_action_feedback_sub_t* out_sub);
extern "C" aorta_result_t aorta_action_feedback_sub_free(
    aorta_action_feedback_sub_t sub_handle);
extern "C" aorta_result_t aorta_action_client_free(
    aorta_action_client_t client);
extern "C" aorta_result_t aorta_action_context_is_cancel_requested(
    aorta_action_context_t ctx, uint8_t* out_requested);
extern "C" aorta_result_t aorta_action_context_publish_feedback(
    aorta_action_context_t ctx, const aorta_bytes_view_t* feedback_data,
    const char* message);
extern "C" aorta_result_t aorta_action_context_succeed(
    aorta_action_context_t ctx, const aorta_bytes_view_t* result_payload);
extern "C" aorta_result_t aorta_action_context_abort(
    aorta_action_context_t ctx, int16_t code, const char* message);
extern "C" aorta_result_t aorta_action_context_cancel(
    aorta_action_context_t ctx, int16_t code, const char* message);
extern "C" aorta_result_t aorta_action_context_free(
    aorta_action_context_t ctx);

extern "C" aorta_result_t aorta_executor_new_with_options(
    aorta_executor_type_t type, const aorta_executor_options_t* opts,
    aorta_exec_t* out);
extern "C" aorta_result_t aorta_executor_get_admission_stats(
    aorta_exec_t exec, aorta_executor_admission_stats_t* out);

// Stop handle: executor stop mechanism.
//
// aorta_stop_handle_t is uint64_t — an opaque key into the Rust-side registry.
// A value of 0 is reserved as "invalid" (never returned by get_stop_handle).
// Double-free is safe and returns AORTA_ERR_NOT_FOUND on the second call.
//
// Both `aorta_stop_handle_t` typedef and `AORTA_STOP_HANDLE_INVALID` macro are
// already declared by the cbindgen-generated header (`aorta.h`), which this
// file is co-included with. Forward-declare only the function prototypes here
// to avoid macro redefinition warnings.
extern "C" aorta_result_t aorta_executor_get_stop_handle(
    aorta_exec_t exec, aorta_stop_handle_t* out);
extern "C" aorta_result_t aorta_stop_handle_stop(
    aorta_stop_handle_t handle);
extern "C" aorta_result_t aorta_stop_handle_free(
    aorta_stop_handle_t handle);

extern "C" aorta_result_t aorta_timer_cancel(aorta_timer_t timer);
extern "C" aorta_result_t aorta_timer_reset(aorta_timer_t timer);
extern "C" aorta_result_t aorta_timer_get_coalesced_tick_count(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_overflow_timeout_count(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_deadline_slip_total_ns(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_deadline_slip_max_ns(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_due_firings_total(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_due_firings_max(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_due_sample_count(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_coalesce_requeue_count(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_coalesce_inline_dispatch_total(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_coalesce_inline_dispatch_max(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_coalesce_event_wait_total_ns(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_coalesce_event_wait_max_ns(
    aorta_timer_t timer, uint64_t* out_count);
extern "C" aorta_result_t aorta_timer_get_coalesce_event_wait_sample_count(
    aorta_timer_t timer, uint64_t* out_count);

extern "C" aorta_result_t aorta_log_init_from_env();
extern "C" aorta_result_t aorta_log_configure(const aorta_log_config_t* config);
extern "C" aorta_result_t aorta_log_flush();
extern "C" aorta_result_t aorta_log_write(aorta_log_level_t level, const char* scope,
                                           const char* message);
extern "C" aorta_result_t aorta_log_write_at(aorta_log_level_t level, const char* scope,
                                              const char* file, uint32_t line,
                                              const char* message);
extern "C" aorta_result_t aorta_log_set_context(const char* node_name, const char* group);
extern "C" uint8_t aorta_log_enabled(aorta_log_level_t level, const char* scope);
