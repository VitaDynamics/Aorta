#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

#include "aorta/status.h"

namespace aorta {

class Node;

namespace test_access {
struct TimerCallbackTestAccess;
struct TimerDestructorRaceAccess;
}  // namespace test_access

// Dispatcher is `noexcept`. See subscriber.h
// header note for rationale; same try { ... } catch (...) pattern
// here.
extern "C" void aorta_cpp_on_timer(void* user_data) noexcept;

class Timer {
 public:
  using Callback = std::function<void()>;

  ~Timer();

  /// Pause ticking. A paused timer keeps its handle and stats; resume with
  /// `Resume()`. Previously the C++ SDK exposed no lifecycle control at all,
  /// so a timer built with `auto_start=false` was permanently inert.
  /// Maps to the C-ABI's `aorta_timer_cancel` — a historical misnomer for
  /// pause; the timer is NOT destroyed.
  Status Pause();

  /// Resume (or first-start) ticking — the counterpart of `Pause()`, and the
  /// way to start a timer created with `auto_start=false`. Maps to the
  /// C-ABI's `aorta_timer_reset` (historical misnomer for resume).
  Status Resume();

  StatusOr<uint64_t> CoalescedTickCount() const;
  StatusOr<uint64_t> OverflowTimeoutCount() const;
  StatusOr<uint64_t> DeadlineSlipTotalNs() const;
  StatusOr<uint64_t> DeadlineSlipMaxNs() const;
  StatusOr<uint64_t> DueFiringsTotal() const;
  StatusOr<uint64_t> DueFiringsMax() const;
  StatusOr<uint64_t> DueSampleCount() const;
  StatusOr<uint64_t> CoalesceRequeueCount() const;
  StatusOr<uint64_t> CoalesceInlineDispatchTotal() const;
  StatusOr<uint64_t> CoalesceInlineDispatchMax() const;
  StatusOr<uint64_t> CoalesceEventWaitTotalNs() const;
  StatusOr<uint64_t> CoalesceEventWaitMaxNs() const;
  StatusOr<uint64_t> CoalesceEventWaitSampleCount() const;

 private:
  friend class Node;
  friend void aorta_cpp_on_timer(void* user_data) noexcept;
  friend struct ::aorta::test_access::TimerCallbackTestAccess;
  friend struct ::aorta::test_access::TimerDestructorRaceAccess;

  struct CallbackState {
    Callback cb;
    std::atomic<uint32_t> in_flight{0};
    std::atomic<bool> shutting_down{false};
    std::mutex in_flight_mutex;
    std::condition_variable in_flight_cv;
  };

  Timer(aorta_timer_t handle, std::unique_ptr<CallbackState> state,
        std::shared_ptr<Node> node)
      : handle_(handle), state_(std::move(state)), node_(std::move(node)) {}

  aorta_timer_t handle_{0};
  std::unique_ptr<CallbackState> state_;
  std::shared_ptr<Node> node_;
};

}  // namespace aorta
