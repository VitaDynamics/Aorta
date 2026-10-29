#include "aorta/executor.h"

#include "aorta/detail/reentrancy_check.h"
#include "aorta/node.h"
#include "internal/ffi.h"

namespace aorta {

StatusOr<std::shared_ptr<Executor>> Executor::Create(ExecutorType type) {
  aorta_executor_type_t ffi_type = AORTA_EXECUTOR_SINGLE;
  switch (type) {
    case ExecutorType::kSingleThreaded:
      ffi_type = AORTA_EXECUTOR_SINGLE;
      break;
    case ExecutorType::kMultiThreaded:
      ffi_type = AORTA_EXECUTOR_MULTI;
      break;
    case ExecutorType::kCurrentThread:
      ffi_type = AORTA_EXECUTOR_CURRENT;
      break;
    case ExecutorType::kAsync:
      ffi_type = AORTA_EXECUTOR_ASYNC;
      break;
  }

  aorta_exec_t handle = 0;
  aorta_result_t rc = aorta_executor_new(ffi_type, &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<Executor>>(Status::FromResult(rc));
  }
  return StatusOr<std::shared_ptr<Executor>>(
      std::shared_ptr<Executor>(new Executor(handle)));
}

StatusOr<std::shared_ptr<Executor>> Executor::Create(
    const ExecutorOptions& options) {
  aorta_executor_type_t ffi_type = AORTA_EXECUTOR_SINGLE;
  switch (options.type) {
    case ExecutorType::kSingleThreaded:
      ffi_type = AORTA_EXECUTOR_SINGLE;
      break;
    case ExecutorType::kMultiThreaded:
      ffi_type = AORTA_EXECUTOR_MULTI;
      break;
    case ExecutorType::kCurrentThread:
      ffi_type = AORTA_EXECUTOR_CURRENT;
      break;
    case ExecutorType::kAsync:
      ffi_type = AORTA_EXECUTOR_ASYNC;
      break;
  }

  aorta_priority_scheduler_options_t ffi_priority{};
  aorta_executor_admission_options_t ffi_admission{};
  aorta_executor_class_policy_options_t ffi_class_policy{};
  aorta_executor_options_t ffi_options =
      options.ToFfi(&ffi_priority, &ffi_admission, &ffi_class_policy);

  aorta_exec_t handle = 0;
  aorta_result_t rc =
      aorta_executor_new_with_options(ffi_type, &ffi_options, &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<Executor>>(Status::FromResult(rc));
  }
  return StatusOr<std::shared_ptr<Executor>>(
      std::shared_ptr<Executor>(new Executor(handle)));
}

Executor::~Executor() {
  if (handle_ != 0) {
    AORTA_CHECK_NOT_REENTRANT("Executor::~Executor");
    aorta_executor_free(handle_);
    handle_ = 0;
  }
}

Status Executor::AddNode(const std::shared_ptr<Node>& node) {
  if (!node) {
    return Status(AORTA_ERR_INVALID_ARGUMENT, "node is null");
  }
  AORTA_CHECK_NOT_REENTRANT("Executor::AddNode");
  aorta_result_t rc = aorta_executor_add_node(handle_, node->handle());
  return Status::FromResult(rc);
}

StatusOr<CallbackGroupHandle> Executor::CreateCallbackGroup(
    CallbackGroup group) {
  aorta_callback_group_t ffi_group = AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE;
  switch (group) {
    case CallbackGroup::kMutuallyExclusive:
      ffi_group = AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE;
      break;
    case CallbackGroup::kReentrant:
      ffi_group = AORTA_CALLBACK_GROUP_REENTRANT;
      break;
  }

  aorta_callback_group_handle_t out = 0;
  AORTA_CHECK_NOT_REENTRANT("Executor::CreateCallbackGroup");
  aorta_result_t rc =
      aorta_executor_create_callback_group(handle_, ffi_group, &out);
  if (rc != AORTA_OK) {
    return StatusOr<CallbackGroupHandle>(Status::FromResult(rc));
  }
  return StatusOr<CallbackGroupHandle>(out);
}

Status Executor::FreeCallbackGroup(CallbackGroupHandle handle) {
  AORTA_CHECK_NOT_REENTRANT("Executor::FreeCallbackGroup");
  return Status::FromResult(aorta_executor_free_callback_group(handle_, handle));
}

StatusOr<ExecutorAdmissionStats> Executor::GetAdmissionStats() const {
  AORTA_CHECK_NOT_REENTRANT("Executor::GetAdmissionStats");
  aorta_executor_admission_stats_t ffi{};
  aorta_result_t rc = aorta_executor_get_admission_stats(handle_, &ffi);
  if (rc != AORTA_OK) {
    return StatusOr<ExecutorAdmissionStats>(Status::FromResult(rc));
  }

  ExecutorAdmissionStats out{};
  out.queued_events = ffi.queued_events;
  out.queued_bytes = ffi.queued_bytes;
  out.peak_queued_events = ffi.peak_queued_events;
  out.peak_queued_bytes = ffi.peak_queued_bytes;
  out.dropped_queue_full = ffi.dropped_queue_full;
  out.dropped_queue_closed = ffi.dropped_queue_closed;
  out.dropped_budget_events = ffi.dropped_budget_events;
  out.dropped_budget_bytes = ffi.dropped_budget_bytes;
  return StatusOr<ExecutorAdmissionStats>(out);
}

Status Executor::Spin() {
  AORTA_CHECK_NOT_REENTRANT("Executor::Spin");
  return Status::FromResult(aorta_executor_spin(handle_));
}

Status Executor::SpinOnce(std::chrono::milliseconds timeout) {
  AORTA_CHECK_NOT_REENTRANT("Executor::SpinOnce");
  auto timeout_ms = static_cast<int64_t>(timeout.count());
  return Status::FromResult(aorta_executor_spin_once(handle_, timeout_ms));
}

Status Executor::Stop() {
  return Status::FromResult(aorta_executor_stop(handle_));
}

StatusOr<StopHandle> Executor::GetStopHandle() {
  aorta_stop_handle_t out = AORTA_STOP_HANDLE_INVALID;
  aorta_result_t rc = aorta_executor_get_stop_handle(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<StopHandle>(Status::FromResult(rc));
  }
  return StatusOr<StopHandle>(StopHandle(out));
}

// --- StopHandle ---

StopHandle::StopHandle(uint64_t handle) : handle_(handle) {}

StopHandle::~StopHandle() {
  if (handle_ != 0) {
    aorta_stop_handle_free(handle_);
    handle_ = 0;
  }
}

StopHandle::StopHandle(StopHandle&& other) noexcept : handle_(other.handle_) {
  other.handle_ = 0;
}

StopHandle& StopHandle::operator=(StopHandle&& other) noexcept {
  if (this != &other) {
    if (handle_ != 0) {
      aorta_stop_handle_free(handle_);
    }
    handle_ = other.handle_;
    other.handle_ = 0;
  }
  return *this;
}

Status StopHandle::Stop() {
  return Status::FromResult(aorta_stop_handle_stop(handle_));
}

}  // namespace aorta
