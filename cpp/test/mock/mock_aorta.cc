// Copyright (c) 2025-2026 VitaDynamics
// Mock implementation of Aorta FFI layer for unit testing

#include "mock_aorta.h"

// Provide C linkage implementations that delegate to mock
extern "C" {

uint8_t aorta_reentrant_context() {
  return aorta::g_mock_reentrant_context.load(std::memory_order_relaxed);
}

aorta_result_t aorta_node_new(const char* name, const char* group,
                              aorta_node_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_new(name, group, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_free(aorta_node_t node) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_free(node);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_now(aorta_node_t node, int64_t* out_ns) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_now(node, out_ns);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_now_ns(int64_t* out_ns) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_now_ns(out_ns);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_uuid_v7(char* out_text, size_t out_size) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_uuid_v7(out_text, out_size);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_get_id(aorta_node_t node, char* out_buf,
                                  size_t buf_size, size_t* out_len) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_get_id(node, out_buf, buf_size, out_len);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_get_group(aorta_node_t node, char* out_buf,
                                     size_t buf_size, size_t* out_len) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_get_group(node, out_buf, buf_size,
                                                     out_len);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_has_event_sender(aorta_node_t node,
                                           uint8_t* out_has_event_sender) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_has_event_sender(
        node, out_has_event_sender);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_validate_topic_name(aorta_node_t node,
                                              const char* topic) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_validate_topic_name(node, topic);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_validate_topic_pattern(aorta_node_t node,
                                                 const char* pattern) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_validate_topic_pattern(node, pattern);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_validate_service_name(aorta_node_t node,
                                                const char* service_name) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_validate_service_name(node,
                                                                 service_name);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_validate_service_pattern(aorta_node_t node,
                                                   const char* pattern) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_validate_service_pattern(node, pattern);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_normalize_action_name(const char* action_name,
                                           char* out_buf, size_t buf_size,
                                           size_t* out_len) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_normalize_action_name(
        action_name, out_buf, buf_size, out_len);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_create_callback_group(
    aorta_node_t node, aorta_callback_group_t group,
    aorta_callback_group_handle_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_create_callback_group(node, group,
                                                                  out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_node_free_callback_group(
    aorta_node_t node, aorta_callback_group_handle_t callback_group) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_node_free_callback_group(node,
                                                               callback_group);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_publisher_new(aorta_node_t node, const char* topic,
                                   const aorta_qos_t* qos,
                                   const aorta_pub_options_t* opts,
                                   aorta_pub_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_publisher_new(node, topic, qos, opts,
                                                    out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_publisher_publish(aorta_pub_t pub, const uint8_t* data,
                                       size_t len) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_publisher_publish(pub, data, len);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_publisher_publish_owned(aorta_pub_t pub,
                                             const aorta_owned_bytes_view_t* payload) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_publisher_publish_owned(pub, payload);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_publisher_free(aorta_pub_t pub) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_publisher_free(pub);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_publisher_get_stats(aorta_pub_t pub,
                                         aorta_publisher_stats_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_publisher_get_stats(pub, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_publisher_matching_status(aorta_pub_t pub,
                                               uint8_t* out_matching) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_publisher_matching_status(pub,
                                                                out_matching);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_publisher_wait_for_matching(aorta_pub_t pub,
                                                 uint64_t timeout_ms,
                                                 uint8_t* out_matching) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_publisher_wait_for_matching(
        pub, timeout_ms, out_matching);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_subscriber_new(aorta_node_t node, const char* topic,
                                    const aorta_qos_t* qos,
                                    const aorta_sub_options_t* opts,
                                    aorta_on_msg_cb cb, void* user_data,
                                    aorta_sub_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_subscriber_new(node, topic, qos, opts, cb,
                                                     user_data, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_subscriber_new_buffered(
    aorta_node_t node, const char* topic, const aorta_qos_t* qos,
    const aorta_sub_options_t* opts, const aorta_sub_buffering_t* buffering,
    aorta_on_msg_cb cb, void* user_data, aorta_sub_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_subscriber_new_buffered(
        node, topic, qos, opts, buffering, cb, user_data, out);
  }
  return AORTA_ERR_INTERNAL;
}

// Pull-mode FFI: default no-op stubs so the mock library links. Pull semantics
// are covered by the real-library integration tests, not the mock unit tests.
aorta_result_t aorta_sub_recv(aorta_sub_t, aorta_pull_sample_t*, int64_t) {
  return AORTA_ERR_TIMEOUT;
}

aorta_result_t aorta_sub_try_recv(aorta_sub_t, aorta_pull_sample_t*) {
  return AORTA_ERR_TIMEOUT;
}

size_t aorta_sub_dropped_count(aorta_sub_t) { return 0; }

void aorta_pull_sample_free(aorta_pull_sample_t*) {}

aorta_result_t aorta_subscriber_free(aorta_sub_t sub) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_subscriber_free(sub);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_new(aorta_executor_type_t type,
                                  aorta_exec_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_new(type, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_new_with_options(
    aorta_executor_type_t type, const aorta_executor_options_t* opts,
    aorta_exec_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_new_with_options(type, opts,
                                                                 out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_add_node(aorta_exec_t exec, aorta_node_t node) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_add_node(exec, node);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_spin(aorta_exec_t exec) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_spin(exec);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_spin_once(aorta_exec_t exec, int64_t timeout_ms) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_spin_once(exec, timeout_ms);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_stop(aorta_exec_t exec) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_stop(exec);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_get_stop_handle(aorta_exec_t exec, uint64_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_get_stop_handle(exec, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_stop_handle_stop(uint64_t handle) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_stop_handle_stop(handle);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_stop_handle_free(uint64_t handle) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_stop_handle_free(handle);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_get_admission_stats(
    aorta_exec_t exec, aorta_executor_admission_stats_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_get_admission_stats(exec, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_free(aorta_exec_t exec) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_free(exec);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_create_callback_group(
    aorta_exec_t exec, aorta_callback_group_t group,
    aorta_callback_group_handle_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_create_callback_group(exec,
                                                                      group, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_executor_free_callback_group(
    aorta_exec_t exec, aorta_callback_group_handle_t group) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_executor_free_callback_group(exec, group);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_new(aorta_node_t node, int64_t period_ns,
                               aorta_timer_cb cb, void* user_data,
                               aorta_timer_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_new(node, period_ns, cb, user_data,
                                                out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_new_with_options(aorta_node_t node, int64_t period_ns,
                                            aorta_timer_cb cb, void* user_data,
                                            const aorta_timer_options_t* opts,
                                            aorta_timer_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_new_with_options(
        node, period_ns, cb, user_data, opts, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_cancel(aorta_timer_t timer) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_cancel(timer);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_reset(aorta_timer_t timer) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_reset(timer);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_coalesced_tick_count(aorta_timer_t timer,
                                                    uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_coalesced_tick_count(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_overflow_timeout_count(aorta_timer_t timer,
                                                      uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_overflow_timeout_count(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_deadline_slip_total_ns(aorta_timer_t timer,
                                                      uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_deadline_slip_total_ns(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_deadline_slip_max_ns(aorta_timer_t timer,
                                                    uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_deadline_slip_max_ns(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_due_firings_total(aorta_timer_t timer,
                                                 uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_due_firings_total(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_due_firings_max(aorta_timer_t timer,
                                               uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_due_firings_max(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_due_sample_count(aorta_timer_t timer,
                                                uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_due_sample_count(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_coalesce_requeue_count(aorta_timer_t timer,
                                                      uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_coalesce_requeue_count(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_coalesce_inline_dispatch_total(aorta_timer_t timer,
                                                              uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_coalesce_inline_dispatch_total(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_coalesce_inline_dispatch_max(aorta_timer_t timer,
                                                            uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_coalesce_inline_dispatch_max(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_coalesce_event_wait_total_ns(aorta_timer_t timer,
                                                            uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_coalesce_event_wait_total_ns(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_coalesce_event_wait_max_ns(aorta_timer_t timer,
                                                          uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_coalesce_event_wait_max_ns(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_get_coalesce_event_wait_sample_count(aorta_timer_t timer,
                                                                uint64_t* out_count) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_get_coalesce_event_wait_sample_count(
        timer, out_count);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_timer_free(aorta_timer_t timer) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_timer_free(timer);
  }
  return AORTA_ERR_INTERNAL;
}

const char* aorta_last_error() {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_last_error();
  }
  return "mock not initialized";
}

aorta_result_t aorta_service_new_with_options(
    aorta_node_t node, const char* service_name,
    const aorta_service_options_t* opts,
    void (*callback)(const aorta_bytes_view_t*, aorta_service_request_t,
                     void*),
    void* user_data, aorta_service_t* out_service) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_service_new_with_options(
        node, service_name, opts, callback, user_data, out_service);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_service_free(aorta_service_t service) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_service_free(service);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_service_request_reply(aorta_service_request_t request,
                                           const aorta_owned_bytes_view_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_service_request_reply(request, response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_service_request_reply_error(aorta_service_request_t request,
                                                 const char* error_msg) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_service_request_reply_error(request,
                                                                  error_msg);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_service_request_release(aorta_service_request_t request) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_service_request_release(request);
  }
  return AORTA_ERR_INTERNAL;
}

uint8_t aorta_service_request_trace_context(aorta_service_request_t /*request*/,
                                            uint8_t* /*out_trace_id*/,
                                            uint64_t* /*out_span_id*/) {
  // Mock: tracing is never active — report "no context" (outputs untouched),
  // matching the real FFI's disabled-path contract.
  return 0;
}

// Trace stubs: without an installed MockTraceState the mock engine has no
// tracer — gates read closed, clocks read zero, recorders are no-ops (the real
// FFI's disabled-path contract). With one installed (aorta::g_mock_trace) the
// stubs report its `enabled` gate, serve `now_ns`, and capture recorded stages
// so tests can assert on span emission.
uint8_t aorta_trace_is_enabled(void) {
  return (aorta::g_mock_trace != nullptr && aorta::g_mock_trace->enabled) ? 1
                                                                          : 0;
}

uint64_t aorta_trace_monotonic_ns(void) {
  return aorta::g_mock_trace != nullptr ? aorta::g_mock_trace->now_ns : 0;
}

void aorta_trace_record_stage_at(uint8_t stage_id, const char* name,
                                 uint64_t start_mono_ns,
                                 const uint8_t* trace_id, uint64_t span_id,
                                 uint64_t parent_span_id) {
  if (aorta::g_mock_trace == nullptr) {
    return;
  }
  aorta::g_mock_trace->stages_at.push_back(aorta::MockTraceStageRecord{
      stage_id, name != nullptr ? name : "", start_mono_ns,
      trace_id != nullptr, span_id, parent_span_id});
}

void aorta_trace_record_stage_under(uint8_t stage_id, const char* name,
                                    uint64_t start_mono_ns,
                                    const uint8_t* trace_id,
                                    uint64_t parent_span_id) {
  if (aorta::g_mock_trace == nullptr) {
    return;
  }
  aorta::g_mock_trace->stages_under.push_back(aorta::MockTraceStageRecord{
      stage_id, name != nullptr ? name : "", start_mono_ns,
      trace_id != nullptr, /*span_id=*/0, parent_span_id});
}

uint8_t aorta_action_context_trace_context(aorta_action_context_t /*ctx*/,
                                           uint8_t* /*out_trace_id*/,
                                           uint64_t* /*out_span_id*/) {
  return 0;
}

aorta_result_t aorta_service_get_stats(aorta_service_t service,
                                       aorta_service_stats_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_service_get_stats(service, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_client_new_with_options(
    aorta_node_t node, const char* service_name,
    const aorta_client_options_t* opts, aorta_client_t* out_client) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_client_new_with_options(
        node, service_name, opts, out_client);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_client_call(aorta_client_t client,
                                 const aorta_owned_bytes_view_t* request,
                                 aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_client_call(client, request, response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_client_get_stats(aorta_client_t client,
                                      aorta_client_stats_t* out) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_client_get_stats(client, out);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_client_matching_status(aorta_client_t client,
                                            uint8_t* out_matching) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_client_matching_status(client,
                                                             out_matching);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_client_wait_for_matching(aorta_client_t client,
                                              uint64_t timeout_ms,
                                              uint8_t* out_matching) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_client_wait_for_matching(client,
                                                               timeout_ms,
                                                               out_matching);
  }
  return AORTA_ERR_INTERNAL;
}

void aorta_owned_bytes_free(aorta_owned_bytes_t* bytes) {
  if (aorta::g_mock_aorta) {
    aorta::g_mock_aorta->aorta_owned_bytes_free(bytes);
  }
}

aorta_result_t aorta_client_free(aorta_client_t client) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_client_free(client);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_server_info_new_with_options(
    aorta_node_t node, const char* action_name, const uint8_t* schema_bfbs,
    size_t schema_bfbs_len, const aorta_action_server_options_t* opts,
    aorta_action_server_t* out_server) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_server_info_new_with_options(
        node, action_name, schema_bfbs, schema_bfbs_len, opts, out_server);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_server_new_with_options(
    aorta_node_t node, const char* action_name, const uint8_t* schema_bfbs,
    size_t schema_bfbs_len, const aorta_action_server_options_t* opts,
    const aorta_action_server_codec_t* codec,
    void (*execute_cb)(const aorta_bytes_view_t* request,
                       aorta_action_context_t ctx, void* user_data),
    void* user_data, aorta_action_server_t* out_server) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_server_new_with_options(
        node, action_name, schema_bfbs, schema_bfbs_len, opts, codec,
        execute_cb, user_data, out_server);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_server_new_with_callbacks(
    aorta_node_t node, const char* action_name, const uint8_t* schema_bfbs,
    size_t schema_bfbs_len, const aorta_action_server_options_t* opts,
    const aorta_action_server_codec_t* codec,
    const aorta_action_server_callbacks_t* callbacks,
    aorta_action_server_t* out_server) {
  if (aorta::g_mock_aorta) {
    const auto rc = aorta::g_mock_aorta->aorta_action_server_new_with_callbacks(
        node, action_name, schema_bfbs, schema_bfbs_len, opts, codec,
        callbacks, out_server);
    aorta::g_mock_aorta->TrackActionServerRelease(
        rc, out_server != nullptr ? *out_server : 0, callbacks);
    return rc;
  }
  if (callbacks != nullptr && callbacks->user_data_release != nullptr) {
    callbacks->user_data_release(callbacks->user_data);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_server_free(aorta_action_server_t server) {
  if (aorta::g_mock_aorta) {
    aorta::g_mock_aorta->ActionServerFreed(server);
    return aorta::g_mock_aorta->aorta_action_server_free(server);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_new_with_options(
    aorta_node_t node, const char* action_name,
    const aorta_action_client_options_t* opts,
    aorta_action_client_t* out_client) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_new_with_options(
        node, action_name, opts, out_client);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_send_goal(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_send_goal(client, request,
                                                              response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_send_goal_traced(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    uint8_t* out_trace_id, uint64_t* out_span_id, uint64_t* out_parent_span_id,
    aorta_owned_bytes_t* response) {
  // Mock: mint a fake goal_sent identity when a MockTraceState reports tracing
  // active; otherwise zero the identity outputs (tracing never active). Either
  // way delegate to the untraced mock path so existing expectations keep
  // matching.
  const bool tracing =
      aorta::g_mock_trace != nullptr && aorta::g_mock_trace->enabled;
  if (out_trace_id != nullptr) {
    for (int i = 0; i < 16; ++i) {
      out_trace_id[i] = tracing ? static_cast<uint8_t>(i + 1) : 0;
    }
  }
  if (out_span_id != nullptr) {
    *out_span_id = tracing ? aorta::g_mock_trace->goal_span_id : 0;
  }
  if (out_parent_span_id != nullptr) *out_parent_span_id = 0;
  return aorta_action_client_send_goal(client, request, response);
}

aorta_result_t aorta_action_client_query_status(
    aorta_action_client_t client, const char* goal_id,
    aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_query_status(client, goal_id,
                                                                 response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_query_status_with_timeout(
    aorta_action_client_t client, const char* goal_id, uint64_t timeout_ms,
    aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_query_status_with_timeout(
        client, goal_id, timeout_ms, response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_cancel(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_cancel(client, request,
                                                           response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_get_result(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_get_result(client, request,
                                                               response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_get_result_with_timeout(
    aorta_action_client_t client, const aorta_bytes_view_t* request,
    uint64_t timeout_ms, aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_get_result_with_timeout(
        client, request, timeout_ms, response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_info(aorta_action_client_t client,
                                        aorta_owned_bytes_t* response) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_info(client, response);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_subscribe_feedback(
    aorta_action_client_t client, aorta_on_msg_cb cb, void* user_data,
    aorta_action_feedback_sub_t* out_sub) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_subscribe_feedback(
        client, cb, user_data, out_sub);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_feedback_sub_free(
    aorta_action_feedback_sub_t sub_handle) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_feedback_sub_free(sub_handle);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_subscribe_feedback2(
    aorta_action_client_t client, uint32_t capacity, uint8_t policy,
    aorta_on_msg_cb cb, void* user_data,
    aorta_action_feedback_sub_t* out_sub) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_subscribe_feedback2(
        client, capacity, policy, cb, user_data, out_sub);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_feedback_sub2_free(
    aorta_action_feedback_sub_t sub_handle) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_feedback_sub2_free(sub_handle);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_subscribe_result(
    aorta_action_client_t client, const char* goal_id,
    const uint8_t* get_result_request, uintptr_t get_result_request_len,
    uint64_t timeout_ms, aorta_action_status_decode_fn decode_status,
    aorta_action_result_cb cb, void* user_data,
    aorta_action_result_sub_t* out_sub) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_subscribe_result(
        client, goal_id, get_result_request, get_result_request_len, timeout_ms,
        decode_status, cb, user_data, out_sub);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_result_sub_free(aorta_action_result_sub_t sub) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_result_sub_free(sub);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_client_free(aorta_action_client_t client) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_client_free(client);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_context_is_cancel_requested(
    aorta_action_context_t ctx, uint8_t* out_requested) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_context_is_cancel_requested(
        ctx, out_requested);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_context_publish_feedback(
    aorta_action_context_t ctx, const aorta_bytes_view_t* feedback_data,
    const char* message) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_context_publish_feedback(
        ctx, feedback_data, message);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_context_succeed(
    aorta_action_context_t ctx, const aorta_bytes_view_t* result_payload) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_context_succeed(ctx,
                                                             result_payload);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_context_abort(aorta_action_context_t ctx,
                                          int16_t code,
                                          const char* message) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_context_abort(ctx, code, message);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_context_cancel(aorta_action_context_t ctx,
                                           int16_t code,
                                           const char* message) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_context_cancel(ctx, code,
                                                            message);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_action_context_free(aorta_action_context_t ctx) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_action_context_free(ctx);
  }
  return AORTA_ERR_INTERNAL;
}

aorta_result_t aorta_log_init_from_env() {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_log_init_from_env();
  }
  return AORTA_OK;
}

aorta_result_t aorta_log_configure(const aorta_log_config_t* config) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_log_configure(config);
  }
  if (config == nullptr) {
    return AORTA_ERR_INVALID_ARGUMENT;
  }
  return AORTA_OK;
}

aorta_result_t aorta_log_flush() {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_log_flush();
  }
  return AORTA_OK;
}

aorta_result_t aorta_log_write(aorta_log_level_t level, const char* scope,
                               const char* message) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_log_write(level, scope, message);
  }
  (void)level;
  if (scope == nullptr || message == nullptr) {
    return AORTA_ERR_INVALID_ARGUMENT;
  }
  return AORTA_OK;
}

aorta_result_t aorta_log_write_at(aorta_log_level_t level, const char* scope,
                                  const char* file, uint32_t line,
                                  const char* message) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_log_write_at(level, scope, file, line,
                                                   message);
  }
  (void)level;
  (void)file;
  (void)line;
  if (scope == nullptr || message == nullptr) {
    return AORTA_ERR_INVALID_ARGUMENT;
  }
  return AORTA_OK;
}

aorta_result_t aorta_log_set_context(const char* node_name, const char* group) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_log_set_context(node_name, group);
  }
  if (node_name == nullptr || group == nullptr) {
    return AORTA_ERR_INVALID_ARGUMENT;
  }
  return AORTA_OK;
}

uint8_t aorta_log_enabled(aorta_log_level_t level, const char* scope) {
  if (aorta::g_mock_aorta) {
    return aorta::g_mock_aorta->aorta_log_enabled(level, scope);
  }
  (void)level;
  return scope == nullptr ? 0 : 1;
}

}  // extern "C"
