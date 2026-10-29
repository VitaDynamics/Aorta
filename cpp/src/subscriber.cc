#include "aorta/subscriber.h"

#include "aorta/detail/reentrancy_check.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <mutex>
#include <vector>

#include "aorta/convenience.h"
#include "internal/common.h"

namespace aorta {
namespace {

static std::atomic<uint64_t> g_last_sub_exception_log_ns{0};
constexpr auto kSubscriberDestructorWaitTimeout = std::chrono::seconds(2);

}  // namespace

extern "C" void aorta_cpp_on_msg(const aorta_bytes_view_t* payload,
                                 const aorta_schema_hash_view_t* schema,
                                 const aorta_msg_context_t* ctx,
                                 void* user_data) noexcept {
  (void)schema;
  try {
    auto* state = static_cast<Subscriber::CallbackState*>(user_data);
    if (state == nullptr) {
      return;
    }
    // Reserve an in-flight slot under the SAME mutex the destructor uses to
    // set shutting_down + wait. This closes the TOCTOU UAF that the earlier
    // "load shutting_down, then fetch_add(in_flight)" outside the lock left
    // open (adopts the single-source pattern already in service.cc):
    //   1. dispatcher reads shutting_down=false (passes the outer check)
    //   2. destructor sets shutting_down=true and observes in_flight==0
    //      (dispatcher has not fetch_added yet) → resets/frees CallbackState
    //   3. dispatcher resumes, fetch_add on freed memory → UAF
    // Holding the check + increment inside the destructor's locked region
    // makes the destructor either observe a non-zero in_flight (and wait for
    // our decrement) or makes us observe shutting_down=true and bail without
    // touching any state member after the unlock.
    {
      std::lock_guard<std::mutex> lock(state->in_flight_mutex);
      if (state->shutting_down.load(std::memory_order_acquire)) {
        return;
      }
      state->in_flight.fetch_add(1, std::memory_order_acq_rel);
    }

    // RAII guard decrements in_flight on every exit path (including exception
    // unwind), under the mutex so the destructor's cv wakeup sees a
    // consistent count. Mirrors service.cc's CallbackScopeGuard.
    struct CallbackScopeGuard {
      Subscriber::CallbackState* state;
      ~CallbackScopeGuard() noexcept {
        {
          std::lock_guard<std::mutex> lock(state->in_flight_mutex);
          // notify_all UNDER the lock: once we unlock, the destructor can
          // observe in_flight==0 and pthread_cond_destroy in_flight_cv, which
          // would race a notify_all issued after unlock (TSAN: data race in
          // CallbackState::~CallbackState). Notifying while holding the lock
          // makes the destructor's wait re-acquire only after this returns.
          if (state->in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            state->in_flight_cv.notify_all();
          }
        }
      }
    } callback_scope_guard{state};

    if (!state->cb) {
      return;
    }
    MessageSlice single_slice{};
    std::vector<MessageSlice> multi_slices;
    MessageView view =
        internal::ToMessageView(payload, &single_slice, &multi_slices);
    state->cb(view, internal::ToMessageContextView(ctx));
  } catch (const FatalError& e) {
    // Dispatcher escape hatch: user callback invoked
    // aorta::Must / CheckOk which threw. The contract on those helpers is
    // "fatal on error", so silent suppression by the catch-all below would
    // surprise the user. Stack already unwound through user callback frames
    // (RAII fired) before reaching here; we can't propagate past the FFI
    // boundary (Rust 1.81+ extern "C" is nounwind) so log + terminate. Net
    // outcome is strictly better than the earlier std::abort() path because
    // user-scoped destructors actually ran before exit.
    std::fprintf(stderr, "Aorta fatal (from subscriber callback): %s\n",
                 e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    internal::MaybeLogCallbackException("subscriber",
                                        g_last_sub_exception_log_ns);
  }
}

Subscriber::~Subscriber() {
  // Drain in-flight callbacks before destroying the state, mirroring
  // service.cc:
  //   1. set shutting_down UNDER the mutex — synchronizes with the
  //      dispatcher's "check shutting_down + fetch_add in_flight" sequence
  //      (see aorta_cpp_on_msg above for the TOCTOU explanation); the old
  //      ordering set it AFTER aorta_subscriber_free and OUTSIDE the lock.
  //   2. free the FFI handle so Rust stops dispatching new callbacks
  //   3. wait on cv until already-dispatching callbacks return
  //   4. if drained: reset the state; if timed out: deliberately LEAK the
  //      state (release without delete) so a hung in-flight callback's scope
  //      guard does not touch freed memory (the old code reset()
  //      unconditionally, a UAF once the decrement runs under the mutex).
  if (state_) {
    std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
    state_->shutting_down.store(true, std::memory_order_release);
  }
  if (handle_ != 0) {
    // reentrancy: F (spawn-and-detach free — OK from a callback)
    aorta_subscriber_free(handle_);
    handle_ = 0;
  }
  bool drained = true;
  if (state_) {
    AORTA_CHECK_NOT_REENTRANT("Subscriber::~Subscriber");
    std::unique_lock<std::mutex> lock(state_->in_flight_mutex);
    drained = state_->in_flight_cv.wait_for(
        lock, kSubscriberDestructorWaitTimeout, [state = state_.get()] {
          return state->in_flight.load(std::memory_order_acquire) == 0;
        });
  }
  if (drained) {
    state_.reset();
  } else {
    // INTENTIONAL LEAK: in-flight callbacks still hold pointers into state_
    // and will run their CallbackScopeGuard after we return. reset() now would
    // let them touch freed memory; releasing ownership keeps the state alive
    // until process exit (one allocation per hung destructor — far preferable
    // to UAF).
    internal::LogWarnf(
        "aorta_sdk",
        "subscriber destructor timed out waiting for in-flight callbacks; "
        "leaking CallbackState to avoid UAF (a callback may be hung)");
    (void)state_.release();
  }
}

}  // namespace aorta
