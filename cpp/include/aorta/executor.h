#pragma once

#include <chrono>
#include <cstdint>
#include <memory>

#include "aorta/options.h"
#include "aorta/status.h"

namespace aorta {

class Node;

struct ExecutorAdmissionStats {
  uint64_t queued_events = 0;
  uint64_t queued_bytes = 0;
  uint64_t peak_queued_events = 0;
  uint64_t peak_queued_bytes = 0;
  uint64_t dropped_queue_full = 0;
  uint64_t dropped_queue_closed = 0;
  uint64_t dropped_budget_events = 0;
  uint64_t dropped_budget_bytes = 0;
};

/// Stop handle for an Executor.
///
/// Obtained via `Executor::GetStopHandle()`. The `Stop()` method performs a
/// brief registry lookup (Mutex) on the Rust side, then calls the underlying
/// atomic store. It is safe to call from the same thread that is running
/// `Executor::Spin()` (e.g., from a Timer callback), but **not** async-signal-
/// safe — in a signal handler, post to a self-pipe / signalfd and call
/// `Stop()` from a normal thread.
///
/// Double-free safety: destroying a moved-from `StopHandle` is a no-op
/// (handle_ == 0 sentinel); duplicate frees on the Rust side return
/// AORTA_ERR_NOT_FOUND rather than triggering use-after-free.
///
/// Move-only; the handle must eventually be destroyed to release resources.
class StopHandle {
 public:
  ~StopHandle();

  StopHandle(StopHandle&& other) noexcept;
  StopHandle& operator=(StopHandle&& other) noexcept;

  StopHandle(const StopHandle&) = delete;
  StopHandle& operator=(const StopHandle&) = delete;

  /// Stop the associated executor. Takes a brief registry mutex on the Rust
  /// side, then invokes the underlying lock-free notification. **Not** async-
  /// signal-safe (use a self-pipe from signal handlers).
  Status Stop();

 private:
  friend class Executor;
  explicit StopHandle(uint64_t handle);

  uint64_t handle_{0};
};

class Executor {
 public:
  static StatusOr<std::shared_ptr<Executor>> Create(
      ExecutorType type = ExecutorType::kSingleThreaded);
  static StatusOr<std::shared_ptr<Executor>> Create(
      const ExecutorOptions& options);

  ~Executor();

  Status AddNode(const std::shared_ptr<Node>& node);
  StatusOr<CallbackGroupHandle> CreateCallbackGroup(CallbackGroup group);
  Status FreeCallbackGroup(CallbackGroupHandle handle);
  StatusOr<ExecutorAdmissionStats> GetAdmissionStats() const;
  Status Spin();
  Status SpinOnce(std::chrono::milliseconds timeout);
  Status Stop();

  /// Obtain a stop handle. The returned `StopHandle::Stop()` takes a brief
  /// registry mutex on the Rust side, then performs the underlying
  /// notification — safe from any normal thread context (including Timer
  /// callbacks running inside Spin()), but **not** async-signal-safe. From
  /// signal handlers, post to a self-pipe / sigwait thread and call
  /// `Stop()` from there. (Prior versions of this API documented `Stop()`
  /// as lock-free and signal-safe; that was changed to
  /// eliminate a double-free UAF path on the raw-pointer handle.)
  StatusOr<StopHandle> GetStopHandle();

  /// The raw C-ABI handle — for the Flow C++20 layer (flow::spawn_on /
  /// flow::resume_on) and other C-interop. OWNED by this wrapper.
  aorta_exec_t handle() const { return handle_; }

 private:
  explicit Executor(aorta_exec_t handle) : handle_(handle) {}

  aorta_exec_t handle_{0};
};

}  // namespace aorta
