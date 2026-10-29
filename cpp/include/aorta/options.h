#pragma once

#include <chrono>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <aorta.h>
#include "aorta/qos.h"

namespace aorta {

class Node;
class Status;

enum class SchemaAttachPolicy {
  kInherit,
  kAlways,
  kNever,
};

enum class CallbackGroup {
  kMutuallyExclusive,
  kReentrant,
};
using CallbackGroupHandle = uint64_t;

enum class ExecutionPolicy {
  kSerialized,
  kConcurrent,
};

enum class SubscriberOverflowPolicy {
  kDrop,
  kRelay,
};

/// How received samples are delivered.
/// Mirrors core `DeliveryMode` and the `aorta_sub_options_t.delivery_mode`
/// discriminants (0=executor, 1=inline_pull, 2=inline_push).
enum class DeliveryMode {
  kExecutor = 0,    ///< Callback on the shared executor (default); Tier A cap.
  kInlinePull = 1,  ///< Native keep-last ring; drain via PullSubscriber::Recv.
  kInlinePush = 2,  ///< Inline on RX thread (deferred; rejected at declare).
};

/// Drop direction when `receive_depth` is exceeded.
enum class ReceiveOverflow {
  kDropNewest = 0,  ///< Reject the newest at the gate (Tier A; cheap).
  kDropOldest = 1,  ///< Keep newest N (requires kInlinePull / native ring).
};

enum class RpcOverflowPolicy {
  kFastFailBusy,
  kBlock,
};

enum class TimerOverflowPolicy {
  kCoalesceLatest,
  kBlock,
};

enum class ExecutorType {
  kSingleThreaded,
  kMultiThreaded,
  // Not implemented: Executor::Create() fails with AORTA_ERR_INVALID_ARGUMENT.
  kCurrentThread,
  // Compatibility backend: multi-threaded executor with a bounded in-flight
  // cap (AORTA_ASYNC_EXECUTOR_MAX_IN_FLIGHT, default 32) — not a separate
  // event-loop model.
  kAsync,
};

enum class SchedulerMode {
  kFifo,
  kPriority,
};

struct PrioritySchedulerOptions {
  uint32_t max_drain_per_cycle = 32;
  std::array<uint8_t, 5> weights{{8, 4, 2, 1, 1}};
  bool map_subscription_qos = true;
};

struct ExecutorAdmissionOptions {
  std::size_t max_queued_events = 0;
  std::size_t max_queued_bytes = 0;
};

struct ExecutorClassPolicyOptions {
  SubscriberOverflowPolicy sub_overflow_policy = SubscriberOverflowPolicy::kDrop;
  std::size_t sub_overflow_relay_capacity = 1024;
  RpcOverflowPolicy rpc_overflow_policy = RpcOverflowPolicy::kFastFailBusy;
  uint64_t rpc_overflow_block_timeout_ms = 0;
  TimerOverflowPolicy timer_overflow_policy = TimerOverflowPolicy::kCoalesceLatest;
  uint64_t timer_overflow_block_timeout_ms = 0;
};

struct ExecutorOptions {
  ExecutorType type = ExecutorType::kSingleThreaded;
  SchedulerMode scheduler_mode = SchedulerMode::kFifo;
  PrioritySchedulerOptions priority;
  ExecutorAdmissionOptions admission;
  ExecutorClassPolicyOptions class_policy;

  aorta_executor_options_t ToFfi(
      aorta_priority_scheduler_options_t* priority_out,
      aorta_executor_admission_options_t* admission_out,
      aorta_executor_class_policy_options_t* class_policy_out) const {
    aorta_executor_options_t ffi{};
    switch (scheduler_mode) {
      case SchedulerMode::kFifo:
        ffi.scheduler_mode = AORTA_SCHEDULER_FIFO;
        break;
      case SchedulerMode::kPriority:
        ffi.scheduler_mode = AORTA_SCHEDULER_PRIORITY;
        break;
    }

    if (priority_out != nullptr) {
      priority_out->max_drain_per_cycle = priority.max_drain_per_cycle;
      for (std::size_t i = 0; i < priority.weights.size(); ++i) {
        priority_out->weights[i] = priority.weights[i];
      }
      priority_out->map_subscription_qos = priority.map_subscription_qos ? 1 : 0;
      ffi.priority = priority_out;
    } else {
      ffi.priority = nullptr;
    }

    if (admission_out != nullptr) {
      admission_out->max_queued_events = admission.max_queued_events;
      admission_out->max_queued_bytes = admission.max_queued_bytes;
      ffi.admission = admission_out;
    } else {
      ffi.admission = nullptr;
    }

    if (class_policy_out != nullptr) {
      switch (class_policy.sub_overflow_policy) {
        case SubscriberOverflowPolicy::kDrop:
          class_policy_out->sub_overflow_policy = AORTA_SUB_OVERFLOW_DROP;
          break;
        case SubscriberOverflowPolicy::kRelay:
          class_policy_out->sub_overflow_policy = AORTA_SUB_OVERFLOW_RELAY;
          break;
      }
      class_policy_out->sub_overflow_relay_capacity =
          class_policy.sub_overflow_relay_capacity;
      switch (class_policy.rpc_overflow_policy) {
        case RpcOverflowPolicy::kFastFailBusy:
          class_policy_out->rpc_overflow_policy =
              AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY;
          break;
        case RpcOverflowPolicy::kBlock:
          class_policy_out->rpc_overflow_policy = AORTA_RPC_OVERFLOW_BLOCK;
          break;
      }
      class_policy_out->rpc_overflow_block_timeout_ms =
          class_policy.rpc_overflow_block_timeout_ms;
      switch (class_policy.timer_overflow_policy) {
        case TimerOverflowPolicy::kCoalesceLatest:
          class_policy_out->timer_overflow_policy =
              AORTA_TIMER_OVERFLOW_COALESCE_LATEST;
          break;
        case TimerOverflowPolicy::kBlock:
          class_policy_out->timer_overflow_policy = AORTA_TIMER_OVERFLOW_BLOCK;
          break;
      }
      class_policy_out->timer_overflow_block_timeout_ms =
          class_policy.timer_overflow_block_timeout_ms;
      ffi.class_policy = class_policy_out;
    } else {
      ffi.class_policy = nullptr;
    }

    return ffi;
  }
};

struct PublisherOptions {
  SchemaAttachPolicy schema_attach_policy = SchemaAttachPolicy::kInherit;
  std::vector<uint8_t> schema_bfbs;
  std::string schema_root_type;
  std::optional<std::size_t> max_message_size;
  // Per-publisher congestion-control OVERRIDE of `qos.congestion_control`.
  // std::nullopt (the default) inherits the
  // QoS value; a set value wins. Node::CreatePublisher resolves the effective
  // value against the QoS.
  std::optional<CongestionControl> congestion_control;

  aorta_pub_options_t ToFfi() const {
    aorta_pub_options_t ffi{};
    // Encode the per-publisher override directly as the inherit/Drop/Block
    // sentinel (AORTA_PUB_CONGESTION_*, distinct from the QoS byte). std::nullopt
    // (the default) → INHERIT, so Core resolves against qos.congestion_control;
    // a set value overrides. No QoS is needed here — resolution lives in Core,
    // so CreatePublisher no longer has to recompute the effective value.
    ffi.congestion_control =
        !congestion_control.has_value()
            ? AORTA_PUB_CONGESTION_INHERIT
            : (*congestion_control == CongestionControl::kBlock ? AORTA_PUB_CONGESTION_BLOCK
                                                               : AORTA_PUB_CONGESTION_DROP);
    switch (schema_attach_policy) {
      case SchemaAttachPolicy::kAlways:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_ALWAYS;
        break;
      case SchemaAttachPolicy::kNever:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_NEVER;
        break;
      case SchemaAttachPolicy::kInherit:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_INHERIT;
        break;
    }
    ffi.schema_bfbs = schema_bfbs.empty() ? nullptr : schema_bfbs.data();
    ffi.schema_bfbs_len = schema_bfbs.size();
    ffi.max_message_size = max_message_size.value_or(0);
    // Borrowed from this options object for the synchronous FFI call, matching
    // the schema_bfbs pointer lifetime above.
    ffi.schema_root_type =
        schema_root_type.empty() ? nullptr : schema_root_type.c_str();
    return ffi;
  }
};

struct SubscriberOptions {
  QoS qos = QoS();
  CallbackGroup callback_group = CallbackGroup::kMutuallyExclusive;
  CallbackGroupHandle callback_group_handle = 0;
  int64_t deadline_ns = 0;
  bool prefetch_schema_on_init = true;
  bool enable_header_context = false;
  /// Include topic key expression in MessageContext.
  bool include_topic_in_context = false;
  /// Include receive timestamp in MessageContext.
  bool include_receive_timestamp = false;
  /// Include priority in MessageContext.
  bool include_priority_in_context = false;
  /// Include publisher timestamp in MessageContext.
  bool include_publisher_timestamp = false;
  bool enable_dedup = false;
  std::size_t dedup_window = 1024;
  bool enable_reorder_check = false;
  bool drop_out_of_order = false;
  SubscriberOverflowPolicy overflow_policy = SubscriberOverflowPolicy::kDrop;
  std::size_t overflow_relay_capacity = 1024;
  std::vector<uint8_t> schema_bfbs;

  /// Per-topic in-flight cap (Tier A) / native ring capacity (Tier B-pull).
  /// `0` (default) disables the gate — byte-for-byte today's behavior.
  std::size_t receive_depth = 0;
  /// Delivery mode: executor callback (default) vs native pull ring.
  DeliveryMode delivery = DeliveryMode::kExecutor;
  /// Drop direction for `receive_depth`.
  ReceiveOverflow receive_overflow = ReceiveOverflow::kDropNewest;

  aorta_sub_options_t ToFfi() const {
    aorta_sub_options_t ffi{};
    switch (callback_group) {
      case CallbackGroup::kMutuallyExclusive:
        ffi.callback_group = AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE;
        break;
      case CallbackGroup::kReentrant:
        ffi.callback_group = AORTA_CALLBACK_GROUP_REENTRANT;
        break;
    }
    ffi.callback_group_handle = callback_group_handle;
    ffi.deadline_ns = deadline_ns;
    ffi.prefetch_schema_on_init = prefetch_schema_on_init ? 1 : 0;
    ffi.enable_header_context = enable_header_context ? 1 : 0;
    ffi.include_topic_in_context = include_topic_in_context ? 1 : 0;
    ffi.include_receive_timestamp = include_receive_timestamp ? 1 : 0;
    ffi.include_priority_in_context = include_priority_in_context ? 1 : 0;
    ffi.include_publisher_timestamp = include_publisher_timestamp ? 1 : 0;
    ffi.enable_dedup = enable_dedup ? 1 : 0;
    ffi.dedup_window = dedup_window;
    ffi.enable_reorder_check = enable_reorder_check ? 1 : 0;
    ffi.drop_out_of_order = drop_out_of_order ? 1 : 0;
    switch (overflow_policy) {
      case SubscriberOverflowPolicy::kDrop:
        ffi.overflow_policy = AORTA_SUB_OVERFLOW_DROP;
        break;
      case SubscriberOverflowPolicy::kRelay:
        ffi.overflow_policy = AORTA_SUB_OVERFLOW_RELAY;
        break;
    }
    ffi.overflow_relay_capacity = overflow_relay_capacity;
    ffi.schema_bfbs = schema_bfbs.empty() ? nullptr : schema_bfbs.data();
    ffi.schema_bfbs_len = schema_bfbs.size();
    // NOTE: aorta_sub_options_t is layout-frozen (ABI-stable C struct);
    // the buffering knobs travel via ToFfiBuffering() + the
    // aorta_subscriber_new_buffered constructor, never this struct.
    return ffi;
  }

  /// The QoS & buffering redesign knobs, as the dedicated
  /// `aorta_sub_buffering_t` companion struct for
  /// `aorta_subscriber_new_buffered`. A zero-initialized struct (the defaults)
  /// reproduces today's behavior byte-for-byte.
  aorta_sub_buffering_t ToFfiBuffering() const {
    aorta_sub_buffering_t ffi{};
    ffi.receive_depth = static_cast<uint64_t>(receive_depth);
    ffi.delivery_mode = static_cast<uint32_t>(delivery);
    ffi.receive_overflow = static_cast<uint32_t>(receive_overflow);
    return ffi;
  }
};

struct ServiceOptions {
  SchemaAttachPolicy schema_attach_policy = SchemaAttachPolicy::kNever;
  CallbackGroup callback_group = CallbackGroup::kMutuallyExclusive;
  CallbackGroupHandle callback_group_handle = 0;
  int64_t deadline_ns = 0;
  RpcOverflowPolicy rpc_overflow_policy = RpcOverflowPolicy::kFastFailBusy;
  uint64_t rpc_overflow_block_timeout_ms = 0;
  std::string service_schema_hash_hex;
  std::vector<uint8_t> service_schema_bfbs;
  std::string request_root_type;
  std::string response_root_type;

  aorta_service_options_t ToFfi() const {
    aorta_service_options_t ffi{};
    switch (schema_attach_policy) {
      case SchemaAttachPolicy::kAlways:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_ALWAYS;
        break;
      case SchemaAttachPolicy::kNever:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_NEVER;
        break;
      case SchemaAttachPolicy::kInherit:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_INHERIT;
        break;
    }
    switch (callback_group) {
      case CallbackGroup::kMutuallyExclusive:
        ffi.callback_group = AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE;
        break;
      case CallbackGroup::kReentrant:
        ffi.callback_group = AORTA_CALLBACK_GROUP_REENTRANT;
        break;
    }
    ffi.callback_group_handle = callback_group_handle;
    ffi.deadline_ns = deadline_ns;
    ffi.service_schema_hash_hex = service_schema_hash_hex.empty()
                                      ? nullptr
                                      : reinterpret_cast<const uint8_t*>(
                                            service_schema_hash_hex.data());
    ffi.service_schema_hash_len = service_schema_hash_hex.size();
    ffi.service_schema_bfbs = service_schema_bfbs.empty()
                                  ? nullptr
                                  : reinterpret_cast<const uint8_t*>(
                                        service_schema_bfbs.data());
    ffi.service_schema_bfbs_len = service_schema_bfbs.size();
    ffi.request_root_type = request_root_type.empty()
                                ? nullptr
                                : reinterpret_cast<const uint8_t*>(
                                      request_root_type.data());
    ffi.request_root_type_len = request_root_type.size();
    ffi.response_root_type = response_root_type.empty()
                                 ? nullptr
                                 : reinterpret_cast<const uint8_t*>(
                                       response_root_type.data());
    ffi.response_root_type_len = response_root_type.size();
    switch (rpc_overflow_policy) {
      case RpcOverflowPolicy::kFastFailBusy:
        ffi.rpc_overflow_policy = AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY;
        break;
      case RpcOverflowPolicy::kBlock:
        ffi.rpc_overflow_policy = AORTA_RPC_OVERFLOW_BLOCK;
        break;
    }
    ffi.rpc_overflow_block_timeout_ms = rpc_overflow_block_timeout_ms;
    return ffi;
  }
};

struct TimerOptions {
  CallbackGroup callback_group = CallbackGroup::kMutuallyExclusive;
  CallbackGroupHandle callback_group_handle = 0;
  bool auto_start = true;
  bool invoke_on_missed = false;
  TimerOverflowPolicy overflow_policy = TimerOverflowPolicy::kCoalesceLatest;
  uint64_t overflow_block_timeout_ms = 0;

  aorta_timer_options_t ToFfi() const {
    aorta_timer_options_t ffi{};
    switch (callback_group) {
      case CallbackGroup::kMutuallyExclusive:
        ffi.callback_group = AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE;
        break;
      case CallbackGroup::kReentrant:
        ffi.callback_group = AORTA_CALLBACK_GROUP_REENTRANT;
        break;
    }
    ffi.callback_group_handle = callback_group_handle;
    ffi.auto_start = auto_start ? 1 : 0;
    ffi.invoke_on_missed = invoke_on_missed ? 1 : 0;
    switch (overflow_policy) {
      case TimerOverflowPolicy::kCoalesceLatest:
        ffi.overflow_policy = AORTA_TIMER_OVERFLOW_COALESCE_LATEST;
        break;
      case TimerOverflowPolicy::kBlock:
        ffi.overflow_policy = AORTA_TIMER_OVERFLOW_BLOCK;
        break;
    }
    ffi.overflow_block_timeout_ms = overflow_block_timeout_ms;
    return ffi;
  }
};

struct ClientOptions {
  uint64_t timeout_ms = 5000;  // Default: 5 seconds
  SchemaAttachPolicy schema_attach_policy = SchemaAttachPolicy::kInherit;
  std::optional<bool> enforce_single_provider;
  std::optional<uint64_t> multi_reply_check_window_ms;
  // Per-client RPC retry opt-in. Unset inherits the core default (OFF: retries
  // are disabled because they silently re-invoke non-idempotent providers).
  // Set to true only for a provably idempotent service.
  std::optional<bool> allow_retry;
  std::string service_schema_hash_hex;

  aorta_client_options_t ToFfi() const {
    constexpr uint8_t kOptionalBoolInherit = 0;
    constexpr uint8_t kOptionalBoolFalse = 1;
    constexpr uint8_t kOptionalBoolTrue = 2;

    aorta_client_options_t ffi{};
    ffi.timeout_ms = timeout_ms;
    ffi.enforce_single_provider = kOptionalBoolInherit;
    if (enforce_single_provider.has_value()) {
      ffi.enforce_single_provider = enforce_single_provider.value()
                                        ? kOptionalBoolTrue
                                        : kOptionalBoolFalse;
    }
    ffi.multi_reply_check_window_ms = multi_reply_check_window_ms.value_or(0);
    ffi.allow_retry = kOptionalBoolInherit;
    if (allow_retry.has_value()) {
      ffi.allow_retry =
          allow_retry.value() ? kOptionalBoolTrue : kOptionalBoolFalse;
    }
    switch (schema_attach_policy) {
      case SchemaAttachPolicy::kAlways:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_ALWAYS;
        break;
      case SchemaAttachPolicy::kNever:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_NEVER;
        break;
      case SchemaAttachPolicy::kInherit:
        ffi.schema_attach_policy = AORTA_SCHEMA_ATTACH_INHERIT;
        break;
    }
    ffi.service_schema_hash_hex = service_schema_hash_hex.empty()
                                      ? nullptr
                                      : reinterpret_cast<const uint8_t*>(
                                            service_schema_hash_hex.data());
    ffi.service_schema_hash_len = service_schema_hash_hex.size();
    return ffi;
  }
};

struct ActionClientOptions {
  std::chrono::milliseconds control_timeout{10000};
  std::chrono::milliseconds info_timeout{5000};

  aorta_action_client_options_t ToFfi() const {
    aorta_action_client_options_t ffi{};
    ffi.control_timeout_ms =
        static_cast<uint64_t>(control_timeout.count() < 0 ? 0
                                                          : control_timeout.count());
    ffi.info_timeout_ms =
        static_cast<uint64_t>(info_timeout.count() < 0 ? 0 : info_timeout.count());
    return ffi;
  }
};

struct ActionServerOptions {
  bool supports_cancel = true;
  std::size_t max_concurrency = 1;
  std::chrono::milliseconds result_ttl{60000};
  std::chrono::milliseconds dedup_ttl{300000};

  aorta_action_server_options_t ToFfi() const {
    aorta_action_server_options_t ffi{};
    ffi.supports_cancel = supports_cancel ? 1 : 0;
    ffi.max_concurrency = max_concurrency;
    ffi.result_ttl_ms =
        static_cast<uint64_t>(result_ttl.count() < 0 ? 0 : result_ttl.count());
    ffi.dedup_ttl_ms =
        static_cast<uint64_t>(dedup_ttl.count() < 0 ? 0 : dedup_ttl.count());
    return ffi;
  }
};

inline CallbackGroup CallbackGroupFromExecutionPolicy(ExecutionPolicy policy) {
  switch (policy) {
    case ExecutionPolicy::kSerialized:
      return CallbackGroup::kMutuallyExclusive;
    case ExecutionPolicy::kConcurrent:
      return CallbackGroup::kReentrant;
  }
  return CallbackGroup::kMutuallyExclusive;
}

class ExecutionGroup {
 public:
  ExecutionGroup() = default;
  ~ExecutionGroup();

  ExecutionGroup(const ExecutionGroup&) = delete;
  ExecutionGroup& operator=(const ExecutionGroup&) = delete;

  ExecutionGroup(ExecutionGroup&& other) noexcept;
  ExecutionGroup& operator=(ExecutionGroup&& other) noexcept;

  bool valid() const { return handle_ != 0; }
  ExecutionPolicy policy() const { return policy_; }
  CallbackGroupHandle handle() const { return handle_; }

  Status Release();

  SubscriberOptions Bind(SubscriberOptions options = SubscriberOptions()) const {
    options.callback_group = CallbackGroupFromExecutionPolicy(policy_);
    options.callback_group_handle = handle_;
    return options;
  }

  ServiceOptions Bind(ServiceOptions options = ServiceOptions()) const {
    options.callback_group = CallbackGroupFromExecutionPolicy(policy_);
    options.callback_group_handle = handle_;
    return options;
  }

  TimerOptions Bind(TimerOptions options = TimerOptions()) const {
    options.callback_group = CallbackGroupFromExecutionPolicy(policy_);
    options.callback_group_handle = handle_;
    return options;
  }

 private:
  friend class Node;

  ExecutionGroup(std::weak_ptr<Node> owner, ExecutionPolicy policy,
                 CallbackGroupHandle handle)
      : owner_(std::move(owner)), policy_(policy), handle_(handle) {}

  void ResetWithoutRelease() noexcept {
    owner_.reset();
    policy_ = ExecutionPolicy::kSerialized;
    handle_ = 0;
  }

  std::weak_ptr<Node> owner_;
  ExecutionPolicy policy_ = ExecutionPolicy::kSerialized;
  CallbackGroupHandle handle_ = 0;
};

}  // namespace aorta
