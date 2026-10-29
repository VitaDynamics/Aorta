// Copyright (c) 2025-2026 VitaDynamics
// Mock implementation of Aorta FFI layer for unit testing

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <aorta.h>
#include "gmock/gmock.h"

namespace aorta {

// Forward declare the MockAortaFFI class
class MockAortaFFI;

// Forward declare the global mock instance pointer
inline MockAortaFFI* g_mock_aorta = nullptr;

inline std::atomic<uint8_t> g_mock_reentrant_context{0};

inline void SetMockReentrantContext(bool reentrant) {
  g_mock_reentrant_context.store(reentrant ? 1 : 0,
                                 std::memory_order_relaxed);
}

// Test-controllable trace-engine double. While no MockTraceState is installed
// (g_mock_trace == nullptr, the default) the trace stubs keep the real FFI's
// disabled-path contract: the gate reads closed, clocks read zero, recorders
// are no-ops — existing tests are unaffected. A test that asserts on span
// emission installs one for its scope (RAII); the stubs then report tracing
// enabled, serve `now_ns` as the monotonic clock, and capture every recorded
// stage. `aorta_action_client_send_goal_traced` additionally mints a fake
// goal_sent identity (`goal_span_id`) so goal handles carry a valid trace
// context.
struct MockTraceState;
inline MockTraceState* g_mock_trace = nullptr;

struct MockTraceStageRecord {
  uint8_t stage_id;
  std::string name;
  uint64_t start_mono_ns;
  bool has_trace_id;
  uint64_t span_id;  // record_stage_at only; 0 for record_stage_under
  uint64_t parent_span_id;
};

struct MockTraceState {
  MockTraceState() { g_mock_trace = this; }
  ~MockTraceState() { g_mock_trace = nullptr; }
  MockTraceState(const MockTraceState&) = delete;
  MockTraceState& operator=(const MockTraceState&) = delete;

  bool enabled = true;
  uint64_t now_ns = 0;
  uint64_t goal_span_id = 0xABCD;
  std::vector<MockTraceStageRecord> stages_at;
  std::vector<MockTraceStageRecord> stages_under;
};

// Mock class for Aorta FFI functions
class MockAortaFFI {
 public:
  // Constructor: auto-register this mock instance
  MockAortaFFI() {
    g_mock_aorta = this;
    ON_CALL(*this, aorta_node_get_id(testing::_, testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [](aorta_node_t, char* out_buf, size_t buf_size, size_t* out_len) {
              static constexpr char kNodeId[] = "mock-node-id";
              const size_t id_len = sizeof(kNodeId) - 1;
              if (out_buf == nullptr || out_len == nullptr ||
                  buf_size < sizeof(kNodeId)) {
                return AORTA_ERR_INVALID_ARGUMENT;
              }
              std::memcpy(out_buf, kNodeId, sizeof(kNodeId));
              *out_len = id_len;
              return AORTA_OK;
            }));
    ON_CALL(*this, aorta_node_get_group(testing::_, testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [](aorta_node_t, char* out_buf, size_t buf_size, size_t* out_len) {
              // Model Core's authoritative env resolution: AORTA_GROUP is
              // authoritative for the group, trimmed, fallback "default". Individual tests can
              // override to simulate Core-side normalization differing from
              // the SDK's local view.
              const char* env = std::getenv("AORTA_GROUP");
              std::string group = env ? env : "";
              const auto begin = group.find_first_not_of(" \t\r\n");
              if (begin == std::string::npos) {
                group = "default";
              } else {
                const auto end = group.find_last_not_of(" \t\r\n");
                group = group.substr(begin, end - begin + 1);
              }
              if (out_buf == nullptr || out_len == nullptr ||
                  buf_size < group.size() + 1) {
                return AORTA_ERR_INVALID_ARGUMENT;
              }
              std::memcpy(out_buf, group.c_str(), group.size() + 1);
              *out_len = group.size();
              return AORTA_OK;
            }));
    ON_CALL(*this, aorta_node_has_event_sender(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_node_t, uint8_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_node_validate_topic_name(testing::_, testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_node_validate_topic_pattern(testing::_, testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_node_validate_service_name(testing::_, testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    // Default: a simplified key normalization (trim + collapse '/') written
    // straight into the caller buffer. Enough for the mocked C++ tests; the
    // authoritative normalization is exercised by the Rust c_api tests.
    ON_CALL(*this, aorta_normalize_action_name(testing::_, testing::_,
                                               testing::_, testing::_))
        .WillByDefault(testing::Invoke([](const char* action_name, char* out_buf,
                                          size_t buf_size, size_t* out_len) {
          if (out_buf == nullptr || out_len == nullptr || buf_size == 0) {
            return AORTA_ERR_INVALID_ARGUMENT;
          }
          size_t n = 0;
          bool prev_slash = true;  // drop leading slashes
          for (const char* p = action_name; p != nullptr && *p != '\0'; ++p) {
            if (*p == '/') {
              if (!prev_slash && n + 1 < buf_size) {
                out_buf[n++] = '/';
              }
              prev_slash = true;
            } else {
              if (n + 1 < buf_size) {
                out_buf[n++] = *p;
              }
              prev_slash = false;
            }
          }
          while (n > 0 && out_buf[n - 1] == '/') {
            --n;  // drop trailing slashes
          }
          out_buf[n] = '\0';
          *out_len = n;
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_node_validate_service_pattern(testing::_, testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_node_create_callback_group(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [](aorta_node_t, aorta_callback_group_t, aorta_callback_group_handle_t* out) {
              if (out != nullptr) {
                *out = 7;
              }
              return AORTA_OK;
            }));
    ON_CALL(*this, aorta_node_free_callback_group(testing::_, testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_executor_new(testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [](aorta_executor_type_t, aorta_exec_t* out) {
              if (out != nullptr) {
                *out = 9000;
              }
              return AORTA_OK;
            }));
    ON_CALL(*this, aorta_executor_add_node(testing::_, testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_executor_spin(testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_executor_stop(testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_executor_free(testing::_))
        .WillByDefault(testing::Return(AORTA_OK));
    ON_CALL(*this, aorta_publisher_get_stats(testing::_, testing::_))
        .WillByDefault(testing::Invoke(
            [](aorta_pub_t, aorta_publisher_stats_t* out) {
              if (out != nullptr) {
                *out = aorta_publisher_stats_t{};
              }
              return AORTA_OK;
            }));
    ON_CALL(*this, aorta_publisher_matching_status(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_pub_t, uint8_t* out) {
          if (out != nullptr) {
            *out = 1;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_publisher_wait_for_matching(testing::_, testing::_,
                                                     testing::_))
        .WillByDefault(testing::Invoke([](aorta_pub_t, uint64_t, uint8_t* out) {
          if (out != nullptr) {
            *out = 1;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_client_matching_status(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_client_t, uint8_t* out) {
          if (out != nullptr) {
            *out = 1;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_client_wait_for_matching(testing::_, testing::_,
                                                  testing::_))
        .WillByDefault(testing::Invoke(
            [](aorta_client_t, uint64_t, uint8_t* out) {
              if (out != nullptr) {
                *out = 1;
              }
              return AORTA_OK;
            }));
    ON_CALL(*this, aorta_timer_get_coalesced_tick_count(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_overflow_timeout_count(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_deadline_slip_total_ns(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_deadline_slip_max_ns(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_due_firings_total(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_due_firings_max(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_due_sample_count(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_coalesce_requeue_count(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_coalesce_inline_dispatch_total(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_coalesce_inline_dispatch_max(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_coalesce_event_wait_total_ns(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_coalesce_event_wait_max_ns(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
    ON_CALL(*this, aorta_timer_get_coalesce_event_wait_sample_count(testing::_, testing::_))
        .WillByDefault(testing::Invoke([](aorta_timer_t, uint64_t* out) {
          if (out != nullptr) {
            *out = 0;
          }
          return AORTA_OK;
        }));
  }

  // Destructor: auto-unregister, then run the action-server
  // `user_data_release` hooks the emulated library still owes.
  ~MockAortaFFI() {
    g_mock_aorta = nullptr;
    std::vector<std::pair<aorta_user_data_release_cb, void*>> owed;
    {
      std::lock_guard<std::mutex> lock(action_server_release_mutex_);
      owed.swap(freed_action_server_releases_);
      for (auto& [handle, release] : live_action_server_releases_) {
        owed.push_back(release);
      }
      live_action_server_releases_.clear();
    }
    for (auto& [release, user_data] : owed) {
      if (release != nullptr) {
        release(user_data);
      }
    }
  }

  // Emulates the core's `user_data_release` contract for
  // aorta_action_server_new_with_callbacks: released at once when
  // creation fails; otherwise deferred past aorta_action_server_free
  // (the real library waits for in-flight callbacks, which a mock
  // cannot observe) and run when this mock is destroyed.
  void TrackActionServerRelease(aorta_result_t rc, aorta_action_server_t handle,
                                const aorta_action_server_callbacks_t* callbacks) {
    if (callbacks == nullptr || callbacks->user_data_release == nullptr) {
      return;
    }
    if (rc != AORTA_OK) {
      callbacks->user_data_release(callbacks->user_data);
      return;
    }
    std::lock_guard<std::mutex> lock(action_server_release_mutex_);
    live_action_server_releases_[handle] = {callbacks->user_data_release,
                                            callbacks->user_data};
  }

  void ActionServerFreed(aorta_action_server_t handle) {
    std::lock_guard<std::mutex> lock(action_server_release_mutex_);
    auto it = live_action_server_releases_.find(handle);
    if (it != live_action_server_releases_.end()) {
      freed_action_server_releases_.push_back(it->second);
      live_action_server_releases_.erase(it);
    }
  }

  // Node operations
  MOCK_METHOD(aorta_result_t, aorta_node_new,
              (const char* name, const char* group, aorta_node_t* out));
  MOCK_METHOD(aorta_result_t, aorta_node_free, (aorta_node_t node));
  MOCK_METHOD(aorta_result_t, aorta_node_now,
              (aorta_node_t node, int64_t* out_ns));
  MOCK_METHOD(aorta_result_t, aorta_now_ns, (int64_t* out_ns));
  MOCK_METHOD(aorta_result_t, aorta_uuid_v7, (char* out_text, size_t out_size));
  MOCK_METHOD(aorta_result_t, aorta_node_get_id,
              (aorta_node_t node, char* out_buf, size_t buf_size, size_t* out_len));
  MOCK_METHOD(aorta_result_t, aorta_node_get_group,
              (aorta_node_t node, char* out_buf, size_t buf_size, size_t* out_len));
  MOCK_METHOD(aorta_result_t, aorta_node_has_event_sender,
              (aorta_node_t node, uint8_t* out_has_event_sender));
  MOCK_METHOD(aorta_result_t, aorta_node_validate_topic_name,
              (aorta_node_t node, const char* topic));
  MOCK_METHOD(aorta_result_t, aorta_node_validate_topic_pattern,
              (aorta_node_t node, const char* pattern));
  MOCK_METHOD(aorta_result_t, aorta_node_validate_service_name,
              (aorta_node_t node, const char* service_name));
  MOCK_METHOD(aorta_result_t, aorta_node_validate_service_pattern,
              (aorta_node_t node, const char* pattern));
  MOCK_METHOD(aorta_result_t, aorta_normalize_action_name,
              (const char* action_name, char* out_buf, size_t buf_size,
               size_t* out_len));
  MOCK_METHOD(aorta_result_t, aorta_node_create_callback_group,
              (aorta_node_t node, aorta_callback_group_t group,
               aorta_callback_group_handle_t* out));
  MOCK_METHOD(aorta_result_t, aorta_node_free_callback_group,
              (aorta_node_t node, aorta_callback_group_handle_t callback_group));

  // Publisher operations
  MOCK_METHOD(aorta_result_t, aorta_publisher_new,
              (aorta_node_t node, const char* topic, const aorta_qos_t* qos,
               const aorta_pub_options_t* opts, aorta_pub_t* out));
  MOCK_METHOD(aorta_result_t, aorta_publisher_publish,
              (aorta_pub_t pub, const uint8_t* data, size_t len));
  MOCK_METHOD(aorta_result_t, aorta_publisher_publish_owned,
              (aorta_pub_t pub, const aorta_owned_bytes_view_t* payload));
  MOCK_METHOD(aorta_result_t, aorta_publisher_free, (aorta_pub_t pub));
  MOCK_METHOD(aorta_result_t, aorta_publisher_get_stats,
              (aorta_pub_t pub, aorta_publisher_stats_t* out));
  MOCK_METHOD(aorta_result_t, aorta_publisher_matching_status,
              (aorta_pub_t pub, uint8_t* out_matching));
  MOCK_METHOD(aorta_result_t, aorta_publisher_wait_for_matching,
              (aorta_pub_t pub, uint64_t timeout_ms, uint8_t* out_matching));

  // Subscriber operations
  MOCK_METHOD(aorta_result_t, aorta_subscriber_new,
              (aorta_node_t node, const char* topic, const aorta_qos_t* qos,
               const aorta_sub_options_t* opts, aorta_on_msg_cb cb,
               void* user_data, aorta_sub_t* out));
  MOCK_METHOD(aorta_result_t, aorta_subscriber_new_buffered,
              (aorta_node_t node, const char* topic, const aorta_qos_t* qos,
               const aorta_sub_options_t* opts,
               const aorta_sub_buffering_t* buffering, aorta_on_msg_cb cb,
               void* user_data, aorta_sub_t* out));
  MOCK_METHOD(aorta_result_t, aorta_subscriber_free, (aorta_sub_t sub));

  // Executor operations
  MOCK_METHOD(aorta_result_t, aorta_executor_new,
              (aorta_executor_type_t type, aorta_exec_t* out));
  MOCK_METHOD(aorta_result_t, aorta_executor_new_with_options,
              (aorta_executor_type_t type,
               const aorta_executor_options_t* opts, aorta_exec_t* out));
  MOCK_METHOD(aorta_result_t, aorta_executor_add_node,
              (aorta_exec_t exec, aorta_node_t node));
  MOCK_METHOD(aorta_result_t, aorta_executor_spin, (aorta_exec_t exec));
  MOCK_METHOD(aorta_result_t, aorta_executor_spin_once,
              (aorta_exec_t exec, int64_t timeout_ms));
  MOCK_METHOD(aorta_result_t, aorta_executor_stop, (aorta_exec_t exec));
  MOCK_METHOD(aorta_result_t, aorta_executor_get_admission_stats,
              (aorta_exec_t exec, aorta_executor_admission_stats_t* out));
  MOCK_METHOD(aorta_result_t, aorta_executor_free, (aorta_exec_t exec));
  MOCK_METHOD(aorta_result_t, aorta_executor_create_callback_group,
              (aorta_exec_t exec, aorta_callback_group_t group,
               aorta_callback_group_handle_t* out));
  MOCK_METHOD(aorta_result_t, aorta_executor_free_callback_group,
              (aorta_exec_t exec, aorta_callback_group_handle_t group));

  // Stop handle operations (handle is uint64_t key into Rust registry,
  // not a raw pointer).
  MOCK_METHOD(aorta_result_t, aorta_executor_get_stop_handle,
              (aorta_exec_t exec, uint64_t* out));
  MOCK_METHOD(aorta_result_t, aorta_stop_handle_stop, (uint64_t handle));
  MOCK_METHOD(aorta_result_t, aorta_stop_handle_free, (uint64_t handle));

  // Timer operations
  MOCK_METHOD(aorta_result_t, aorta_timer_new,
              (aorta_node_t node, int64_t period_ns, aorta_timer_cb cb,
               void* user_data, aorta_timer_t* out));
  MOCK_METHOD(aorta_result_t, aorta_timer_new_with_options,
              (aorta_node_t node, int64_t period_ns, aorta_timer_cb cb,
               void* user_data, const aorta_timer_options_t* opts,
               aorta_timer_t* out));
  MOCK_METHOD(aorta_result_t, aorta_timer_cancel, (aorta_timer_t timer));
  MOCK_METHOD(aorta_result_t, aorta_timer_reset, (aorta_timer_t timer));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_coalesced_tick_count,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_overflow_timeout_count,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_deadline_slip_total_ns,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_deadline_slip_max_ns,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_due_firings_total,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_due_firings_max,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_due_sample_count,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_coalesce_requeue_count,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_coalesce_inline_dispatch_total,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_coalesce_inline_dispatch_max,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_coalesce_event_wait_total_ns,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_coalesce_event_wait_max_ns,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_get_coalesce_event_wait_sample_count,
              (aorta_timer_t timer, uint64_t* out_count));
  MOCK_METHOD(aorta_result_t, aorta_timer_free, (aorta_timer_t timer));

  MOCK_METHOD(aorta_result_t, aorta_service_new_with_options,
              (aorta_node_t node, const char* service_name,
               const aorta_service_options_t* opts,
               void (*callback)(const aorta_bytes_view_t*, aorta_service_request_t,
                                void*),
               void* user_data, aorta_service_t* out_service));
  MOCK_METHOD(aorta_result_t, aorta_service_free, (aorta_service_t service));

  MOCK_METHOD(aorta_result_t, aorta_service_request_reply,
              (aorta_service_request_t request,
               const aorta_owned_bytes_view_t* response));
  MOCK_METHOD(aorta_result_t, aorta_service_request_reply_error,
              (aorta_service_request_t request, const char* error_msg));
  MOCK_METHOD(aorta_result_t, aorta_service_request_release,
              (aorta_service_request_t request));
  MOCK_METHOD(aorta_result_t, aorta_service_get_stats,
              (aorta_service_t service, aorta_service_stats_t* out));

  MOCK_METHOD(aorta_result_t, aorta_client_new_with_options,
              (aorta_node_t node, const char* service_name,
               const aorta_client_options_t* opts, aorta_client_t* out_client));
  MOCK_METHOD(aorta_result_t, aorta_client_call,
              (aorta_client_t client, const aorta_owned_bytes_view_t* request,
               aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_client_get_stats,
              (aorta_client_t client, aorta_client_stats_t* out));
  MOCK_METHOD(aorta_result_t, aorta_client_matching_status,
              (aorta_client_t client, uint8_t* out_matching));
  MOCK_METHOD(aorta_result_t, aorta_client_wait_for_matching,
              (aorta_client_t client, uint64_t timeout_ms, uint8_t* out_matching));
  MOCK_METHOD(void, aorta_owned_bytes_free, (aorta_owned_bytes_t* bytes));
  MOCK_METHOD(aorta_result_t, aorta_client_free, (aorta_client_t client));

  MOCK_METHOD(aorta_result_t, aorta_action_server_info_new_with_options,
              (aorta_node_t node, const char* action_name,
               const uint8_t* schema_bfbs, size_t schema_bfbs_len,
               const aorta_action_server_options_t* opts,
               aorta_action_server_t* out_server));
  MOCK_METHOD(aorta_result_t, aorta_action_server_new_with_options,
              (aorta_node_t node, const char* action_name,
               const uint8_t* schema_bfbs, size_t schema_bfbs_len,
               const aorta_action_server_options_t* opts,
               const aorta_action_server_codec_t* codec,
               void (*execute_cb)(const aorta_bytes_view_t* request,
                                  aorta_action_context_t ctx,
                                  void* user_data),
               void* user_data, aorta_action_server_t* out_server));
  MOCK_METHOD(aorta_result_t, aorta_action_server_new_with_callbacks,
              (aorta_node_t node, const char* action_name,
               const uint8_t* schema_bfbs, size_t schema_bfbs_len,
               const aorta_action_server_options_t* opts,
               const aorta_action_server_codec_t* codec,
               const aorta_action_server_callbacks_t* callbacks,
               aorta_action_server_t* out_server));
  MOCK_METHOD(aorta_result_t, aorta_action_server_free,
              (aorta_action_server_t server));

  MOCK_METHOD(aorta_result_t, aorta_action_client_new_with_options,
              (aorta_node_t node, const char* action_name,
               const aorta_action_client_options_t* opts,
               aorta_action_client_t* out_client));
  MOCK_METHOD(aorta_result_t, aorta_action_client_send_goal,
              (aorta_action_client_t client, const aorta_bytes_view_t* request,
               aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_action_client_query_status,
              (aorta_action_client_t client, const char* goal_id,
               aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_action_client_query_status_with_timeout,
              (aorta_action_client_t client, const char* goal_id,
               uint64_t timeout_ms, aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_action_client_cancel,
              (aorta_action_client_t client, const aorta_bytes_view_t* request,
               aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_action_client_get_result,
              (aorta_action_client_t client, const aorta_bytes_view_t* request,
               aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_action_client_get_result_with_timeout,
              (aorta_action_client_t client, const aorta_bytes_view_t* request,
               uint64_t timeout_ms, aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_action_client_info,
              (aorta_action_client_t client, aorta_owned_bytes_t* response));
  MOCK_METHOD(aorta_result_t, aorta_action_client_subscribe_feedback,
              (aorta_action_client_t client, aorta_on_msg_cb cb, void* user_data,
               aorta_action_feedback_sub_t* out_sub));
  MOCK_METHOD(aorta_result_t, aorta_action_feedback_sub_free,
              (aorta_action_feedback_sub_t sub_handle));
  MOCK_METHOD(aorta_result_t, aorta_action_client_subscribe_feedback2,
              (aorta_action_client_t client, uint32_t capacity, uint8_t policy,
               aorta_on_msg_cb cb, void* user_data,
               aorta_action_feedback_sub_t* out_sub));
  MOCK_METHOD(aorta_result_t, aorta_action_feedback_sub2_free,
              (aorta_action_feedback_sub_t sub_handle));
  MOCK_METHOD(aorta_result_t, aorta_action_client_subscribe_result,
              (aorta_action_client_t client, const char* goal_id,
               const uint8_t* get_result_request,
               uintptr_t get_result_request_len, uint64_t timeout_ms,
               aorta_action_status_decode_fn decode_status,
               aorta_action_result_cb cb, void* user_data,
               aorta_action_result_sub_t* out_sub));
  MOCK_METHOD(aorta_result_t, aorta_action_result_sub_free,
              (aorta_action_result_sub_t sub));
  MOCK_METHOD(aorta_result_t, aorta_action_client_free,
              (aorta_action_client_t client));
  MOCK_METHOD(aorta_result_t, aorta_action_context_is_cancel_requested,
              (aorta_action_context_t ctx, uint8_t* out_requested));
  MOCK_METHOD(aorta_result_t, aorta_action_context_publish_feedback,
              (aorta_action_context_t ctx,
               const aorta_bytes_view_t* feedback_data, const char* message));
  MOCK_METHOD(aorta_result_t, aorta_action_context_succeed,
              (aorta_action_context_t ctx,
               const aorta_bytes_view_t* result_payload));
  MOCK_METHOD(aorta_result_t, aorta_action_context_abort,
              (aorta_action_context_t ctx, int16_t code, const char* message));
  MOCK_METHOD(aorta_result_t, aorta_action_context_cancel,
              (aorta_action_context_t ctx, int16_t code, const char* message));
  MOCK_METHOD(aorta_result_t, aorta_action_context_free,
              (aorta_action_context_t ctx));

  // Logging operations
  MOCK_METHOD(aorta_result_t, aorta_log_init_from_env, ());
  MOCK_METHOD(aorta_result_t, aorta_log_configure,
              (const aorta_log_config_t* config));
  MOCK_METHOD(aorta_result_t, aorta_log_flush, ());
  MOCK_METHOD(aorta_result_t, aorta_log_write,
              (aorta_log_level_t level, const char* scope,
               const char* message));
  MOCK_METHOD(aorta_result_t, aorta_log_write_at,
              (aorta_log_level_t level, const char* scope, const char* file,
               uint32_t line, const char* message));
  MOCK_METHOD(aorta_result_t, aorta_log_set_context,
              (const char* node_name, const char* group));
  MOCK_METHOD(uint8_t, aorta_log_enabled,
              (aorta_log_level_t level, const char* scope));

  // Error handling
  MOCK_METHOD(const char*, aorta_last_error, ());

 private:
  std::mutex action_server_release_mutex_;
  std::map<aorta_action_server_t,
           std::pair<aorta_user_data_release_cb, void*>>
      live_action_server_releases_;
  std::vector<std::pair<aorta_user_data_release_cb, void*>>
      freed_action_server_releases_;
};

// Test helper: model the core's owned-bytes ownership contract. A mocked FFI
// function that consumes an `aorta_owned_bytes_view_t` (publish_owned, service
// reply, client call, …) MUST invoke each slice's release callback before
// returning — the real core always does (it drops the owning ZBytes on every
// path, success or error), and skipping it leaks the C++-side ctx (the Tier-3
// LSAN lane catches it). Call this once the mock is done READING the slice data
// (release frees it, so reading after would be a use-after-free).
inline void ReleaseOwnedSlices(const aorta_owned_bytes_view_t* view) {
  if (view == nullptr || view->slices == nullptr) {
    return;
  }
  for (std::size_t i = 0; i < view->slice_count; ++i) {
    const aorta_owned_bytes_slice_t& slice = view->slices[i];
    if (slice.release != nullptr) {
      slice.release(slice.release_ctx, slice.data, slice.len);
    }
  }
}

}  // namespace aorta
