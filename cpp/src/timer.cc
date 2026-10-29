#include "aorta/timer.h"

#include "aorta/detail/reentrancy_check.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <mutex>

#include "aorta/convenience.h"
#include "internal/common.h"
#include "internal/ffi.h"

namespace aorta {
namespace {

static std::atomic<uint64_t> g_last_timer_exception_log_ns{0};
constexpr auto kTimerDestructorWaitTimeout = std::chrono::seconds(2);

}  // namespace

extern "C" void aorta_cpp_on_timer(void* user_data) noexcept {
  auto* state = static_cast<Timer::CallbackState*>(user_data);
  if (state == nullptr) {
    return;
  }
  // Reserve an in-flight slot under the SAME mutex the destructor uses to set
  // shutting_down + wait, closing the TOCTOU UAF the prior "check then
  // fetch_add outside the lock" pattern left open (same single-source
  // discipline as service.cc — see subscriber.cc for the full interleaving).
  {
    std::lock_guard<std::mutex> lock(state->in_flight_mutex);
    if (state->shutting_down.load(std::memory_order_acquire)) {
      return;
    }
    state->in_flight.fetch_add(1, std::memory_order_acq_rel);
  }

  // RAII guard decrements in_flight on every exit path, under the mutex so the
  // destructor's cv wakeup sees a consistent count.
  struct CallbackScopeGuard {
    Timer::CallbackState* state;
    ~CallbackScopeGuard() noexcept {
      {
        std::lock_guard<std::mutex> lock(state->in_flight_mutex);
        // notify_all UNDER the lock: once we unlock, the destructor can observe
        // in_flight==0 and pthread_cond_destroy in_flight_cv, which would race a
        // notify_all issued after unlock (TSAN: data race in
        // CallbackState::~CallbackState). Notifying while holding the lock makes
        // the destructor's wait re-acquire only after this returns.
        if (state->in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
          state->in_flight_cv.notify_all();
        }
      }
    }
  } callback_scope_guard{state};

  try {
    if (!state->cb) {
      return;
    }
    state->cb();
  } catch (const FatalError& e) {
    // Dispatcher escape hatch — see cpp/src/subscriber.cc for rationale.
    std::fprintf(stderr, "Aorta fatal (from timer callback): %s\n", e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    internal::MaybeLogCallbackException("timer", g_last_timer_exception_log_ns);
  }
}

Timer::~Timer() {
  // Signal shutdown first so in-flight and future callbacks see the flag
  // before we tear down the core handle (the previous ordering set the flag
  // OUTSIDE the lock, racing the dispatcher's check+fetch_add). The store now
  // happens UNDER in_flight_mutex so it is mutually exclusive with the
  // dispatcher's locked "check shutting_down + fetch_add" (see
  // aorta_cpp_on_timer above). The free stays AFTER the drain (timer's
  // existing, correct ordering — only the store moved into the lock).
  bool drained = true;
  if (state_) {
    AORTA_CHECK_NOT_REENTRANT("Timer::~Timer");
    {
      std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
      state_->shutting_down.store(true, std::memory_order_release);
    }
    std::unique_lock<std::mutex> lock(state_->in_flight_mutex);
    drained = state_->in_flight_cv.wait_for(
        lock, kTimerDestructorWaitTimeout, [state = state_.get()] {
          return state->in_flight.load(std::memory_order_acquire) == 0;
        });
  }
  // Now safe to free the core timer — no NEW callbacks can be dispatched.
  if (handle_ != 0) {
    aorta_timer_free(handle_);
    handle_ = 0;
  }
  if (drained) {
    state_.reset();
  } else {
    // INTENTIONAL LEAK on timeout: a hung in-flight callback still holds a
    // pointer into state_ and will run its CallbackScopeGuard after we return;
    // reset() would make that a UAF (the old code reset()
    // unconditionally). Releasing without delete keeps the state alive.
    internal::LogWarnf(
        "aorta_sdk",
        "timer destructor timed out waiting for in-flight callbacks; "
        "leaking CallbackState to avoid UAF (a callback may be hung)");
    (void)state_.release();
  }
}

Status Timer::Pause() {
  // C-ABI name is a historical misnomer: aorta_timer_cancel pauses the
  // timer; it does not destroy it.
  return Status::FromResult(aorta_timer_cancel(handle_));
}

Status Timer::Resume() {
  // C-ABI name is a historical misnomer: aorta_timer_reset resumes the
  // timer.
  return Status::FromResult(aorta_timer_reset(handle_));
}

StatusOr<uint64_t> Timer::CoalescedTickCount() const {
  uint64_t out = 0;
  aorta_result_t rc =
      aorta_timer_get_coalesced_tick_count(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::OverflowTimeoutCount() const {
  uint64_t out = 0;
  aorta_result_t rc =
      aorta_timer_get_overflow_timeout_count(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::DeadlineSlipTotalNs() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_deadline_slip_total_ns(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::DeadlineSlipMaxNs() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_deadline_slip_max_ns(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::DueFiringsTotal() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_due_firings_total(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::DueFiringsMax() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_due_firings_max(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::DueSampleCount() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_due_sample_count(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::CoalesceRequeueCount() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_coalesce_requeue_count(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::CoalesceInlineDispatchTotal() const {
  uint64_t out = 0;
  aorta_result_t rc =
      aorta_timer_get_coalesce_inline_dispatch_total(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::CoalesceInlineDispatchMax() const {
  uint64_t out = 0;
  aorta_result_t rc =
      aorta_timer_get_coalesce_inline_dispatch_max(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::CoalesceEventWaitTotalNs() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_coalesce_event_wait_total_ns(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::CoalesceEventWaitMaxNs() const {
  uint64_t out = 0;
  aorta_result_t rc = aorta_timer_get_coalesce_event_wait_max_ns(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

StatusOr<uint64_t> Timer::CoalesceEventWaitSampleCount() const {
  uint64_t out = 0;
  aorta_result_t rc =
      aorta_timer_get_coalesce_event_wait_sample_count(handle_, &out);
  if (rc != AORTA_OK) {
    return StatusOr<uint64_t>(Status::FromResult(rc));
  }
  return StatusOr<uint64_t>(out);
}

}  // namespace aorta
