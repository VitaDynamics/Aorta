#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>

#include "aorta/options.h"
#include "aorta/status.h"
#include "aorta/types.h"

namespace aorta {

class Node;

namespace test_access {
struct SubscriberCallbackTestAccess;
struct SubscriberDestructorRaceAccess;
}  // namespace test_access

// Dispatcher is `noexcept`. The function body
// `try { ... } catch (...) { /* log only */ }` already swallows
// every exception, so making `noexcept`
// part of the signature is a contract assertion that the Rust
// `extern "C"` caller (which is `nounwind` since Rust 1.81) cannot
// observe an unwinding exit, plus a compiler-checked guarantee
// against future maintainers accidentally introducing one.
extern "C" void aorta_cpp_on_msg(const aorta_bytes_view_t* payload,
                                 const aorta_schema_hash_view_t* schema,
                                 const aorta_msg_context_t* ctx,
                                 void* user_data) noexcept;

class Subscriber {
 public:
  using Callback =
      std::function<void(const MessageView&, const MessageContextView&)>;

  ~Subscriber();

  /// Number of events shed by this subscriber's Tier A `receive_depth` gate
  /// (drop-newest). Always 0 when `receive_depth == 0` (uncapped, the default).
  [[nodiscard]] std::size_t DroppedCount() const {
    return handle_ == 0 ? 0 : aorta_sub_dropped_count(handle_);
  }

  /// The C-ABI handle (readiness binds — `flow::bind_window` /
  /// `flow::bind_readiness` — and other C-level configuration setters).
  [[nodiscard]] aorta_sub_t native_handle() const { return handle_; }

 private:
  friend class Node;
  friend void aorta_cpp_on_msg(const aorta_bytes_view_t* payload,
                               const aorta_schema_hash_view_t* schema,
                               const aorta_msg_context_t* ctx,
                               void* user_data) noexcept;
  friend struct ::aorta::test_access::SubscriberCallbackTestAccess;
  friend struct ::aorta::test_access::SubscriberDestructorRaceAccess;

  struct CallbackState {
    Callback cb;
    std::atomic<uint32_t> in_flight{0};
    std::atomic<bool> shutting_down{false};
    std::mutex in_flight_mutex;
    std::condition_variable in_flight_cv;
  };

  Subscriber(aorta_sub_t handle, std::unique_ptr<CallbackState> state,
             std::shared_ptr<Node> node)
      : handle_(handle), state_(std::move(state)), node_(std::move(node)) {}

  aorta_sub_t handle_{0};
  std::unique_ptr<CallbackState> state_;
  std::shared_ptr<Node> node_;
};

}  // namespace aorta
