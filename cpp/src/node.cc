#include "aorta/node.h"

#include "aorta/detail/reentrancy_check.h"

#include <utility>

#include "aorta/client.h"
#include "aorta/executor.h"
#include "aorta/logging.h"
#include "aorta/publisher.h"
#include "aorta/service.h"
#include "aorta/subscriber.h"
#include "aorta/timer.h"
#include "internal/common.h"
#include "internal/ffi.h"

namespace aorta {
namespace {

StatusOr<bool> NodeHasEventSender(aorta_node_t handle) {
  uint8_t out_has_event_sender = 0;
  aorta_result_t rc =
      aorta_node_has_event_sender(handle, &out_has_event_sender);
  if (rc != AORTA_OK) {
    return StatusOr<bool>(Status::FromResult(rc));
  }
  return StatusOr<bool>(out_has_event_sender != 0);
}

// Publish-side validator: concrete topic names only, no wildcards.
Status ValidateTopicName(aorta_node_t handle, const std::string& topic) {
  return Status::FromResult(
      aorta_node_validate_topic_name(handle, topic.c_str()));
}

// Subscribe-side validator: accepts whole-segment Zenoh wildcards
// (`*` / `**`) alongside concrete topic names. Used by
// `CreateSubscriber` so wildcard subscriptions (e.g. `camera/**`)
// reach the actual subscriber constructor instead of being rejected
// by the publish-side gate.
Status ValidateTopicPattern(aorta_node_t handle, const std::string& pattern) {
  return Status::FromResult(
      aorta_node_validate_topic_pattern(handle, pattern.c_str()));
}

Status ValidateServiceName(aorta_node_t handle,
                           const std::string& service_name) {
  return Status::FromResult(
      aorta_node_validate_service_name(handle, service_name.c_str()));
}

aorta_callback_group_t ToFfiCallbackGroup(ExecutionPolicy policy) {
  switch (policy) {
    case ExecutionPolicy::kSerialized:
      return AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE;
    case ExecutionPolicy::kConcurrent:
      return AORTA_CALLBACK_GROUP_REENTRANT;
  }
  return AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE;
}

}  // namespace

ExecutionGroup::~ExecutionGroup() {
  (void)Release();
}

ExecutionGroup::ExecutionGroup(ExecutionGroup&& other) noexcept
    : owner_(std::move(other.owner_)),
      policy_(other.policy_),
      handle_(other.handle_) {
  other.ResetWithoutRelease();
}

ExecutionGroup& ExecutionGroup::operator=(ExecutionGroup&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  (void)Release();
  owner_ = std::move(other.owner_);
  policy_ = other.policy_;
  handle_ = other.handle_;
  other.ResetWithoutRelease();
  return *this;
}

Status ExecutionGroup::Release() {
  if (handle_ == 0) {
    return Status::Ok();
  }

  Status status = Status::Ok();
  if (auto owner = owner_.lock()) {
    status = owner->FreeExecutionGroup(handle_);
  }

  ResetWithoutRelease();
  return status;
}

StatusOr<std::shared_ptr<Node>> Node::Create(const std::string& name,
                                             const std::string& group) {
  // AORTA_GROUP is authoritative and Core resolves it itself
  // (the constructor argument is accepted for API symmetry but ignored by
  // Core). The local env read below is only a hint passed through; the value
  // this Node caches — what Node::group() returns forever after — is read
  // BACK from Core so it carries Core's full normalization (NFC, charset,
  // length), not the ASCII-trim-only local variant (Python already does
  // this).
  (void)group;
  const std::string requested_group = internal::ResolveGroupFromEnv();
  aorta_node_t handle = 0;
  AORTA_CHECK_NOT_REENTRANT("Node::Create");
  aorta_result_t rc =
      aorta_node_new(name.c_str(), requested_group.c_str(), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<Node>>(Status::FromResult(rc));
  }

  char id_buf[64];
  size_t id_len = 0;
  rc = aorta_node_get_id(handle, id_buf, sizeof(id_buf), &id_len);
  if (rc != AORTA_OK) {
    aorta_node_free(handle);
    return StatusOr<std::shared_ptr<Node>>(Status::FromResult(rc));
  }
  std::string id(id_buf, id_len);

  char group_buf[256];
  size_t group_len = 0;
  rc = aorta_node_get_group(handle, group_buf, sizeof(group_buf), &group_len);
  if (rc != AORTA_OK) {
    aorta_node_free(handle);
    return StatusOr<std::shared_ptr<Node>>(Status::FromResult(rc));
  }
  std::string effective_group(group_buf, group_len);

  return StatusOr<std::shared_ptr<Node>>(
      std::shared_ptr<Node>(new Node(handle, name, id, effective_group)));
}

Node::~Node() {
  {
    std::lock_guard<std::mutex> lock(executor_mutex_);
    if (executor_) {
      (void)executor_->Stop();  // dtor: best-effort, nowhere to surface
    }
  }
  if (executor_thread_ && executor_thread_->joinable()) {
    AORTA_CHECK_NOT_REENTRANT("Node::~Node");
    AORTA_LOG_SCOPE(DEBUG, "aorta_sdk")
        << "node '" << name_ << "' waiting for executor thread to join";
    executor_thread_->join();
    AORTA_LOG_SCOPE(DEBUG, "aorta_sdk")
        << "node '" << name_ << "' executor thread joined";
  }
  executor_thread_.reset();
  executor_.reset();
  if (handle_ != 0) {
    aorta_node_free(handle_);
    handle_ = 0;
  }
}

Status Node::EnsureExecutor() {
  auto has_event_sender_or = NodeHasEventSender(handle_);
  if (!has_event_sender_or.ok()) {
    return has_event_sender_or.status();
  }
  if (has_event_sender_or.value()) {
    return Status::Ok();
  }

  std::lock_guard<std::mutex> lock(executor_mutex_);
  if (executor_) {
    return Status::Ok();
  }

  has_event_sender_or = NodeHasEventSender(handle_);
  if (!has_event_sender_or.ok()) {
    return has_event_sender_or.status();
  }
  if (has_event_sender_or.value()) {
    return Status::Ok();
  }

  auto exec_or = Executor::Create(ExecutorType::kSingleThreaded);
  if (!exec_or.ok()) {
    return exec_or.status();
  }

  executor_ = exec_or.value();
  Status status = executor_->AddNode(shared_from_this());
  if (!status.ok()) {
    executor_.reset();
    return status;
  }

  auto exec = executor_;
  // Surface a failed Spin instead of discarding it (an async Core-side
  // error otherwise silently and permanently stops every
  // timer/subscriber/service callback on this node).
  executor_thread_ = std::make_unique<std::thread>([exec]() {
    Status status = exec->Spin();
    if (!status.ok()) {
      AORTA_LOG_SCOPE(ERROR, "aorta_sdk")
          << "auto-executor spin exited with error (callbacks stopped): code="
          << status.code() << " message=" << status.message();
    }
  });
  return Status::Ok();
}

StatusOr<int64_t> Node::Now() const {
  int64_t out = 0;
  aorta_result_t rc = aorta_node_now(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<int64_t>(Status::FromResult(rc));
  }
  return StatusOr<int64_t>(out);
}

StatusOr<std::shared_ptr<Publisher>> Node::CreatePublisher(
    const std::string& topic, const QoS& qos, const PublisherOptions& options) {
  // congestion_control is canonical on QoS; PublisherOptions carries only an
  // optional per-publisher override. ToFfi()
  // already encodes that override as the inherit/Drop/Block sentinel on
  // aorta_pub_options_t, and Core resolves it against qos.congestion_control at
  // publish — no need to recompute the effective value here.
  aorta_pub_options_t opts_ffi = options.ToFfi();
  aorta_qos_t qos_ffi = qos.ToFfi();

  // express now rides qos_ffi (set by QoS::ToFfi).
  aorta_pub_t handle = 0;
  AORTA_CHECK_NOT_REENTRANT("Node::CreatePublisher");
  aorta_result_t rc =
      aorta_publisher_new(handle_, topic.c_str(), &qos_ffi, &opts_ffi, &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<Publisher>>(Status::FromResult(rc));
  }
  return StatusOr<std::shared_ptr<Publisher>>(std::shared_ptr<Publisher>(
      new Publisher(handle, shared_from_this(), topic, name_, options)));
}

StatusOr<std::shared_ptr<Subscriber>> Node::CreateSubscriber(
    const std::string& topic, Subscriber::Callback callback,
    const SubscriberOptions& options) {
  // Subscribers may legitimately be wildcard patterns (`camera/**`,
  // `sensors/*`). Use the subscribe-side validator so the wildcard
  // surface introduced by keyexpr relaxation is reachable.
  Status status = ValidateTopicPattern(handle_, topic);
  if (!status.ok()) {
    return StatusOr<std::shared_ptr<Subscriber>>(status);
  }

  status = EnsureExecutor();
  if (!status.ok()) {
    return StatusOr<std::shared_ptr<Subscriber>>(status);
  }

  auto state = std::make_unique<Subscriber::CallbackState>();
  state->cb = std::move(callback);

  aorta_qos_t qos_ffi = options.qos.ToFfi();
  // aorta_sub_options_t is layout-frozen (ABI-stable C struct); the
  // Tier A `receive_depth` / delivery knobs ride the dedicated
  // aorta_sub_buffering_t companion. All-zero buffering (the defaults)
  // reproduces the exact pre-redesign behavior.
  aorta_sub_options_t opts_ffi = options.ToFfi();
  aorta_sub_buffering_t buffering_ffi = options.ToFfiBuffering();

  // express now rides qos_ffi (set by QoS::ToFfi).
  aorta_sub_t handle = 0;
  AORTA_CHECK_NOT_REENTRANT("Node::CreateSubscriber");
  aorta_result_t rc = aorta_subscriber_new_buffered(
      handle_, topic.c_str(), &qos_ffi, &opts_ffi, &buffering_ffi,
      &aorta_cpp_on_msg, state.get(), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<Subscriber>>(Status::FromResult(rc));
  }

  return StatusOr<std::shared_ptr<Subscriber>>(std::shared_ptr<Subscriber>(
      new Subscriber(handle, std::move(state), shared_from_this())));
}

StatusOr<std::shared_ptr<Subscriber>> Node::CreateSubscriber(
    const std::string& topic, Subscriber::Callback callback, const QoS& qos,
    const SubscriberOptions& options) {
  SubscriberOptions merged = options;
  merged.qos = qos;
  return CreateSubscriber(topic, std::move(callback), merged);
}

StatusOr<PullSubscriber> Node::CreatePullSubscriber(const std::string& topic,
                                                    std::size_t depth) {
  return CreatePullSubscriber(topic, depth, SubscriberOptions());
}

StatusOr<PullSubscriber> Node::CreatePullSubscriber(const std::string& topic,
                                                    std::size_t depth,
                                                    SubscriberOptions options) {
  Status status = ValidateTopicPattern(handle_, topic);
  if (!status.ok()) {
    return StatusOr<PullSubscriber>(status);
  }

  // Pull subscribers own a native ring; no executor hop is required, but
  // EnsureExecutor() is harmless and keeps node lifecycle uniform.
  options.delivery = DeliveryMode::kInlinePull;
  options.receive_overflow = ReceiveOverflow::kDropOldest;
  options.receive_depth = depth;

  aorta_qos_t qos_ffi = options.qos.ToFfi();
  aorta_sub_options_t opts_ffi = options.ToFfi();
  aorta_sub_buffering_t buffering_ffi = options.ToFfiBuffering();

  // express now rides qos_ffi (set by QoS::ToFfi).
  aorta_sub_t handle = 0;
  AORTA_CHECK_NOT_REENTRANT("Node::CreatePullSubscriber");
  aorta_result_t rc = aorta_subscriber_new_buffered(
      handle_, topic.c_str(), &qos_ffi, &opts_ffi, &buffering_ffi,
      /*cb=*/nullptr, /*user_data=*/nullptr, &handle);
  if (rc != AORTA_OK) {
    return StatusOr<PullSubscriber>(Status::FromResult(rc));
  }
  return StatusOr<PullSubscriber>(
      PullSubscriber(handle, shared_from_this()));
}

StatusOr<std::shared_ptr<Timer>> Node::CreateTimer(
    std::chrono::nanoseconds period, Timer::Callback callback) {
  return CreateTimer(period, std::move(callback), TimerOptions());
}

StatusOr<std::shared_ptr<Timer>> Node::CreateTimer(
    std::chrono::nanoseconds period, Timer::Callback callback,
    const TimerOptions& options) {
  Status status = EnsureExecutor();
  if (!status.ok()) {
    return StatusOr<std::shared_ptr<Timer>>(status);
  }
  auto state = std::make_unique<Timer::CallbackState>();
  state->cb = std::move(callback);

  aorta_timer_t handle = 0;
  auto period_ns = static_cast<int64_t>(period.count());
  aorta_timer_options_t opts_ffi = options.ToFfi();
  AORTA_CHECK_NOT_REENTRANT("Node::CreateTimer");
  aorta_result_t rc = aorta_timer_new_with_options(
      handle_, period_ns, &aorta_cpp_on_timer, state.get(), &opts_ffi, &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<Timer>>(Status::FromResult(rc));
  }

  return StatusOr<std::shared_ptr<Timer>>(std::shared_ptr<Timer>(
      new Timer(handle, std::move(state), shared_from_this())));
}

StatusOr<std::shared_ptr<Service>> Node::CreateService(
    const std::string& service_name, Service::RequestCallback callback,
    const ServiceOptions& options) {
  Status status = ValidateServiceName(handle_, service_name);
  if (!status.ok()) {
    return StatusOr<std::shared_ptr<Service>>(status);
  }

  status = EnsureExecutor();
  if (!status.ok()) {
    return StatusOr<std::shared_ptr<Service>>(status);
  }

  auto state = std::make_unique<Service::CallbackState>();
  state->cb = std::move(callback);

  aorta_service_t handle = 0;
  auto opts_ffi = options.ToFfi();
  AORTA_CHECK_NOT_REENTRANT("Node::CreateService");
  auto result = aorta_service_new_with_options(
      handle_, service_name.c_str(), &opts_ffi, aorta_cpp_on_service_request,
      state.get(), &handle);

  if (result != AORTA_OK) {
    return StatusOr<std::shared_ptr<Service>>(Status::FromResult(result));
  }

  return StatusOr<std::shared_ptr<Service>>(std::shared_ptr<Service>(
      new Service(handle, service_name, std::move(state), shared_from_this())));
}

StatusOr<ExecutionGroup> Node::CreateExecutionGroup(ExecutionPolicy policy) {
  Status status = EnsureExecutor();
  if (!status.ok()) {
    return StatusOr<ExecutionGroup>(status);
  }

  aorta_callback_group_handle_t handle = 0;
  aorta_result_t rc =
      aorta_node_create_callback_group(handle_, ToFfiCallbackGroup(policy), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<ExecutionGroup>(Status::FromResult(rc));
  }

  return StatusOr<ExecutionGroup>(
      ExecutionGroup(shared_from_this(), policy, handle));
}

Status Node::FreeExecutionGroup(CallbackGroupHandle handle) {
  return Status::FromResult(aorta_node_free_callback_group(handle_, handle));
}

StatusOr<std::shared_ptr<Client>> Node::CreateClient(
    const std::string& service_name, const ClientOptions& options) {
  aorta_client_t handle = 0;
  auto opts_ffi = options.ToFfi();
  auto result = aorta_client_new_with_options(handle_, service_name.c_str(),
                                              &opts_ffi, &handle);

  if (result != AORTA_OK) {
    return StatusOr<std::shared_ptr<Client>>(Status::FromResult(result));
  }

  return StatusOr<std::shared_ptr<Client>>(std::shared_ptr<Client>(
      new Client(handle, service_name, shared_from_this())));
}

StatusOr<std::shared_ptr<Client>> Node::CreateClient(
    const std::string& service_name, uint64_t timeout_ms) {
  ClientOptions opts;
  opts.timeout_ms = timeout_ms;
  return CreateClient(service_name, opts);
}

}  // namespace aorta
