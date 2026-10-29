#include "aorta/service.h"

#include "aorta/detail/reentrancy_check.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <mutex>
#include <utility>
#include <vector>

#include "aorta/convenience.h"
#include "internal/common.h"
#include "internal/ffi.h"

namespace aorta {
namespace {

static std::atomic<uint64_t> g_last_service_exception_log_ns{0};
constexpr auto kServiceDestructorWaitTimeout = std::chrono::seconds(2);

}  // namespace

extern "C" void aorta_cpp_on_service_request(const aorta_bytes_view_t* payload,
                                             aorta_service_request_t request,
                                             void* user_data) noexcept {
  // Construct the ServiceRequest immediately so that ALL early-return paths
  // below release the Rust-side request handle via ServiceRequest's
  // destructor (without this, registry().service_requests on the Rust side
  // would leak — see core/src/ffi/c_api.rs:5242-5262). The user callback
  // path moves `req` out, so the destructor only fires on early-return
  // paths.
  ServiceRequest req(request);

  try {
    auto* state = static_cast<Service::CallbackState*>(user_data);
    if (state == nullptr) {
      // Internal bail (no user callback installed yet). Silent release.
      req.Release();
      return;
    }
    // Reserve an in-flight slot under the SAME mutex the destructor uses
    // to set `shutting_down` + wait. This closes a TOCTOU race that the
    // earlier "load shutting_down, then fetch_add(in_flight)" pattern
    // (mirrored from subscriber.cc — which has the same bug; pre-existing,
    // tracked separately) left open:
    //   1. callback reads shutting_down=false (passes check)
    //   2. destructor sets shutting_down=true under mutex
    //   3. destructor wait_for sees in_flight==0 (callback hasn't
    //      fetched-added yet) → returns immediately → state_.reset()
    //   4. callback resumes, fetch_add on freed memory → UAF
    // Putting the check + increment in the same locked region as the
    // destructor's set+wait ensures the destructor either observes a
    // non-zero in_flight (and waits for our decrement) or we observe
    // shutting_down=true and bail without touching any state member.
    //
    // The same race and fix apply to the action dispatchers in
    // cpp/include/aorta/action.h.
    {
      std::lock_guard<std::mutex> lock(state->in_flight_mutex);
      if (state->shutting_down.load(std::memory_order_acquire)) {
        // Internal bail during shutdown. Silent release.
        req.Release();
        return;
      }
      state->in_flight.fetch_add(1, std::memory_order_acq_rel);
    }

    // RAII guard decrements in_flight on every exit path (including
    // exception unwind). Decrement also runs under the mutex so the cv
    // wakeup in the destructor sees a consistent count.
    struct CallbackScopeGuard {
      Service::CallbackState* state;
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
      // Internal bail (callback not yet set). Silent release.
      req.Release();
      return;
    }

    MessageSlice single_slice{};
    std::vector<MessageSlice> multi_slices;
    MessageView msg =
        internal::ToMessageView(payload, &single_slice, &multi_slices);
    state->cb(msg, std::move(req));
  } catch (const FatalError& e) {
    // Dispatcher escape hatch — see cpp/src/subscriber.cc for rationale.
    // ServiceRequest moved into state->cb(); throwing inside the user
    // callback unwinds its scope and either Release()s or completes the
    // request based on what the user did before the throw.
    std::fprintf(stderr, "Aorta fatal (from service callback): %s\n",
                 e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    // Must not propagate exceptions across FFI boundary into Rust.
    internal::MaybeLogCallbackException("service",
                                        g_last_service_exception_log_ns);
  }
}

Service::~Service() {
  // Signal in-flight callbacks to drain before we destroy the state.
  //   1. set shutting_down (under mutex — synchronizes with dispatcher's
  //      "check shutting_down + fetch_add in_flight" sequence; see
  //      aorta_cpp_on_service_request above for the TOCTOU explanation)
  //   2. free the FFI handle so Rust stops invoking new callbacks
  //   3. wait on cv until all already-dispatching callbacks return
  //   4. if drained: reset the state; if timed out: deliberately LEAK
  //      the state so in-flight callbacks' scope guards don't UAF on
  //      a freed CallbackState
  //
  // This closes the destructor use-after-free, the TOCTOU race, and the
  // timeout-then-reset hazard.
  if (state_) {
    std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
    state_->shutting_down.store(true, std::memory_order_release);
  }
  if (handle_ != 0) {
    // reentrancy: F (spawn-and-detach free — OK from a callback)
    aorta_service_free(handle_);
    handle_ = 0;
  }
  bool drained = true;
  if (state_) {
    AORTA_CHECK_NOT_REENTRANT("Service::~Service");
    std::unique_lock<std::mutex> lock(state_->in_flight_mutex);
    drained = state_->in_flight_cv.wait_for(
        lock, kServiceDestructorWaitTimeout,
        [state = state_.get()] {
          return state->in_flight.load(std::memory_order_acquire) == 0;
        });
  }
  if (drained) {
    state_.reset();
  } else {
    // INTENTIONAL LEAK: in-flight callbacks still hold pointers into
    // state_ and will execute the CallbackScopeGuard after we return.
    // Calling reset() now would let them touch freed memory. Releasing
    // ownership (without delete) keeps the state alive until process
    // exit; the worst case is a single allocation leak per hung Service
    // destructor, which is far preferable to UAF.
    internal::LogWarnf(
        "aorta_sdk",
        "service destructor timed out waiting for in-flight callbacks; "
        "leaking CallbackState to avoid UAF (a callback may be hung)");
    (void)state_.release();
  }
}

Service::Stats Service::GetStats() const {
  Stats out;
  if (handle_ == 0) {
    return out;
  }

  aorta_service_stats_t ffi{};
  if (aorta_service_get_stats(handle_, &ffi) != AORTA_OK) {
    return out;
  }

  out.request_received_count = ffi.request_received_count;
  out.request_dispatched_count = ffi.request_dispatched_count;
  out.reply_ok_count = ffi.reply_ok_count;
  out.reply_error_count = ffi.reply_error_count;
  out.reply_send_failure_count = ffi.reply_send_failure_count;
  out.request_dropped_count = ffi.request_dropped_count;
  out.query_receive_failure_count = ffi.query_receive_failure_count;
  out.task_exit_count = ffi.task_exit_count;
  out.in_flight_requests = ffi.in_flight_requests;
  out.last_request_ns = ffi.last_request_ns;
  out.last_reply_ns = ffi.last_reply_ns;
  out.request_dropped_deadline_count = ffi.request_dropped_deadline_count;
  return out;
}

ServiceRequest::ServiceRequest(aorta_service_request_t handle)
    : handle_(handle) {}

ServiceRequest::~ServiceRequest() {
  // Reaching the destructor with a live handle means the user neither
  // Reply()'d nor explicitly Release()'d — usually a callback bug. Warn,
  // then release. Explicit Release() does not hit this path.
  if (IsValid()) {
    internal::LogWarnf(
        "aorta_sdk",
        "service request dropped without Reply() or Release(): handle=%llu",
        static_cast<unsigned long long>(handle_));
    aorta_service_request_release(handle_);
    handle_ = 0;
  }
}

ServiceRequest::ServiceRequest(ServiceRequest&& other) noexcept
    : handle_(other.handle_) {
  other.handle_ = 0;
}

ServiceRequest& ServiceRequest::operator=(ServiceRequest&& other) noexcept {
  if (this != &other) {
    // The current handle (if any) is being replaced by an explicit move,
    // not silently lost — release silently.
    Release();
    handle_ = other.handle_;
    other.handle_ = 0;
  }
  return *this;
}

Status ServiceRequest::Reply(std::vector<uint8_t>&& response) {
  if (!IsValid()) {
    return Status(AORTA_ERR_INVALID_ARGUMENT, "invalid request handle");
  }

  aorta_owned_bytes_slice_t ffi_slice{};
  aorta_owned_bytes_view_t ffi_view{};
  internal::BuildOwnedBytesView(std::move(response), &ffi_slice, &ffi_view);

  auto result = aorta_service_request_reply(handle_, &ffi_view);
  // Reply consumes the handle on every outcome (success or error). See
  // ServiceRequest doc comment and aorta_service_request_reply rustdoc.
  handle_ = 0;
  return Status::FromResult(result);
}

Status ServiceRequest::ReplyError(const std::string& error_msg) {
  if (!IsValid()) {
    return Status(AORTA_ERR_INVALID_ARGUMENT, "invalid request handle");
  }

  auto result = aorta_service_request_reply_error(handle_, error_msg.c_str());
  // ReplyError consumes the handle on every outcome, same as Reply(). See
  // aorta_service_request_reply_error rustdoc.
  handle_ = 0;
  return Status::FromResult(result);
}

void ServiceRequest::Release() {
  // Silent: this is the user-facing opt-out path. The destructor's warning
  // covers the "forgot to do anything" case.
  if (IsValid()) {
    aorta_service_request_release(handle_);
    handle_ = 0;
  }
}

trace::Context ServiceRequest::TraceContext() const {
  trace::Context ctx;
  if (!IsValid()) {
    return ctx;
  }
  ctx.valid =
      aorta_service_request_trace_context(handle_, ctx.trace_id, &ctx.span_id) != 0;
  return ctx;
}

}  // namespace aorta
