#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "aorta/status.h"
#include "aorta/trace.h"
#include "aorta/types.h"

namespace aorta {

class Node;

namespace test_access {
struct ServiceCallbackTestAccess;
}  // namespace test_access

// Forward declaration of FFI types
extern "C" {
typedef uint64_t aorta_service_request_t;
}

// Forward declaration of C callback wrapper.
// Dispatcher is `noexcept`. The body's
// `try { ... } catch (...)` already swallows every exception;
// marking `noexcept` makes the contract
// compiler-checked against future maintainers.
extern "C" void aorta_cpp_on_service_request(const aorta_bytes_view_t* payload,
                                             aorta_service_request_t request,
                                             void* user_data) noexcept;

/// Represents an in-flight service request that must be replied to or released.
///
/// Move-only: the handle is transferred on move; the moved-from object becomes
/// invalid (IsValid() returns false). The caller should either call Reply() to
/// send a response or Release() to drop the request without replying. If
/// neither is called, the destructor performs an explicit drop and emits a
/// warning to flag the likely callback bug — the FFI handle is not leaked.
///
/// **Reply() consumes the handle on every outcome.** Whether Reply() returns
/// OK or any error, the underlying FFI handle has been consumed and must not
/// be re-used. The implementation clears handle_ on return from Reply()
/// regardless of status, so the destructor's defensive Release() reduces to
/// a no-op on that path.
class ServiceRequest {
 public:
  explicit ServiceRequest(aorta_service_request_t handle);
  ~ServiceRequest();

  ServiceRequest(ServiceRequest&& other) noexcept;
  ServiceRequest& operator=(ServiceRequest&& other) noexcept;
  ServiceRequest(const ServiceRequest&) = delete;
  ServiceRequest& operator=(const ServiceRequest&) = delete;

  /// Send a response. Consumes the handle in all outcomes — do not call
  /// Release() afterwards. See class doc for full semantics.
  Status Reply(std::vector<uint8_t>&& response);

  /// Send an error reply (a genuine Zenoh query *error* reply, distinct from
  /// an OK reply with an empty payload — the caller's Client surfaces it as a
  /// call failure carrying `error_msg`). Consumes the handle in all outcomes,
  /// exactly like Reply(). Mirrors Rust `ServiceRequest::reply_error` and
  /// Python `ServiceRequest.reply_error`.
  Status ReplyError(const std::string& error_msg);

  /// Explicitly drop the request without replying. Silent: use this when
  /// the callback intentionally chooses not to reply (e.g. validation
  /// failed, fall-back path). The destructor performs the same FFI call
  /// but additionally warns, since reaching the destructor with a live
  /// handle usually indicates a forgotten Reply().
  void Release();

  /// The trace context identifying THIS request (valid only while tracing was
  /// enabled at receipt): the trace id plus the span deferred work should
  /// parent under — the per-request server root. For the deferred-reply
  /// pattern, capture it in the callback, move it with the work to a worker
  /// thread, and install it there with `aorta::trace::ContextGuard` so
  /// AORTA_TRACE_SPAN spans recorded during the deferred processing stay
  /// attached to this request's trace. Does not consume the handle.
  trace::Context TraceContext() const;

  bool IsValid() const { return handle_ != 0; }

 private:
  aorta_service_request_t handle_{0};
};

/// RPC server that handles incoming requests and sends responses.
class Service {
 public:
  struct Stats {
    uint64_t request_received_count = 0;
    uint64_t request_dispatched_count = 0;
    uint64_t reply_ok_count = 0;
    uint64_t reply_error_count = 0;
    uint64_t reply_send_failure_count = 0;
    uint64_t request_dropped_count = 0;
    uint64_t query_receive_failure_count = 0;
    uint64_t task_exit_count = 0;
    uint64_t in_flight_requests = 0;
    uint64_t last_request_ns = 0;
    uint64_t last_reply_ns = 0;
    // Deadline-expired queue drops — a SUBSET of request_dropped_count.
    uint64_t request_dropped_deadline_count = 0;
  };

  using RequestCallback =
      std::function<void(const MessageView& request, ServiceRequest&& req)>;

  ~Service();

  const std::string& Name() const { return service_name_; }
  Stats GetStats() const;

 private:
  friend class Node;
  friend void aorta_cpp_on_service_request(const aorta_bytes_view_t* payload,
                                           aorta_service_request_t request,
                                           void* user_data) noexcept;
  friend struct ::aorta::test_access::ServiceCallbackTestAccess;

  // CallbackState lifetime is protected against use-after-free during
  // destructor running concurrent with in-flight callbacks via the same
  // pattern Subscriber/Timer use:
  //   - shutting_down: latch set before destruction
  //   - in_flight + in_flight_mutex + in_flight_cv: destructor waits for
  //     all dispatching callbacks to return before resetting state.
  // Prevents a Service destructor use-after-free.
  struct CallbackState {
    RequestCallback cb;
    std::atomic<uint32_t> in_flight{0};
    std::atomic<bool> shutting_down{false};
    std::mutex in_flight_mutex;
    std::condition_variable in_flight_cv;
  };

  Service(aorta_service_t handle, std::string service_name,
          std::unique_ptr<CallbackState> state, std::shared_ptr<Node> node)
      : handle_(handle),
        service_name_(std::move(service_name)),
        state_(std::move(state)),
        node_(std::move(node)) {}

  aorta_service_t handle_{0};
  std::string service_name_;
  std::unique_ptr<CallbackState> state_;
  std::shared_ptr<Node> node_;
};

}  // namespace aorta
