#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdio>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "action_common_generated.h"
#include "aorta/convenience.h"
#include "aorta/detail/reentrancy_check.h"
#include "aorta/node.h"
#include "aorta/trace.h"

#include <exception>

namespace aorta {

enum class ActionStatus : int8_t {
  kUnknown = 0,
  kAccepted = 1,
  kExecuting = 2,
  kCanceling = 3,
  kSucceeded = 4,
  kAborted = 5,
  kCanceled = 6,
};

inline bool IsTerminalActionStatus(ActionStatus status) {
  switch (status) {
    case ActionStatus::kSucceeded:
    case ActionStatus::kAborted:
    case ActionStatus::kCanceled:
      return true;
    case ActionStatus::kUnknown:
    case ActionStatus::kAccepted:
    case ActionStatus::kExecuting:
    case ActionStatus::kCanceling:
      return false;
  }
  return false;
}

enum class ActionReplyCode : int16_t {
  kNone = 0,
  kOk = 1,
  kDuplicateAccepted = 2,
  kSucceeded = 100,
  kCanceled = 101,
  kBusy = 200,
  kPayloadConflict = 201,
  kUnauthorized = 202,
  kNotFound = 203,
  kAlreadyTerminal = 204,
  kGoalExpired = 205,
  kHandlerError = 300,
  kHandlerPanic = 301,
  kHandlerCompletedWithoutResult = 302,
  kContextDropped = 303,
  kServerCrash = 304,
  kConflict = 400,
};

struct ActionInfo {
  std::string action_name;
  std::string schema_hash_hex;
  bool supports_cancel = true;
  uint32_t max_concurrency = 0;
  uint64_t result_ttl_ms = 0;
  uint64_t dedup_ttl_ms = 0;
  std::string server_node_name;
  std::string server_epoch;
};

struct ActionPublicStatus {
  std::string goal_id;
  ActionStatus status = ActionStatus::kUnknown;
  ActionReplyCode code = ActionReplyCode::kNone;
  uint64_t accepted_stamp_ns = 0;
  uint64_t started_stamp_ns = 0;
  uint64_t finished_stamp_ns = 0;
  uint64_t last_feedback_seq = 0;
  std::string server_epoch;
};

struct ActionSendGoalResponse {
  bool accepted = false;
  std::string goal_id;
  std::string goal_secret;
  ActionReplyCode code = ActionReplyCode::kNone;
  ActionStatus status = ActionStatus::kUnknown;
  std::string message;
  std::string server_epoch;
};

struct ActionCancelGoalResponse {
  bool accepted = false;
  std::string goal_id;
  ActionReplyCode code = ActionReplyCode::kNone;
  ActionStatus status = ActionStatus::kUnknown;
  std::string message;
};

template <typename Action>
struct ActionGetResultResponse {
  std::string goal_id;
  ActionStatus status = ActionStatus::kUnknown;
  uint64_t accepted_stamp_ns = 0;
  uint64_t started_stamp_ns = 0;
  uint64_t finished_stamp_ns = 0;
  uint64_t last_feedback_seq = 0;
  typename Action::FeedbackDataMsg feedback_snapshot{};
  std::optional<typename Action::ResultMsg> result;
  ActionReplyCode code = ActionReplyCode::kNone;
  std::string message;
  std::string server_epoch;
};

template <typename Action>
class RetainedActionGoal;

template <typename Action>
class ActionExecutionContext;

template <typename Action>
class GoalHandle;

template <typename Action>
class ActionClient;

namespace detail {

/// Normalize an action name to the same canonical key form the core runtime
/// uses, so a name the caller passes (which may be slash-qualified, e.g.
/// `/foo/`) is stored and used for client-side key building (action status
/// subscriptions) exactly as the server publishes it. Without this, a
/// non-normalized name makes `GoalHandle::WaitResult` subscribe to a status
/// key the server never publishes, so the goal waits until timeout. The
/// normalized form is never longer than the input, so `size()+1` always fits.
inline StatusOr<std::string> NormalizeActionName(const std::string& name) {
  std::string out;
  out.resize(name.size() + 1);
  std::size_t out_len = 0;
  const auto rc =
      aorta_normalize_action_name(name.c_str(), out.data(), out.size(), &out_len);
  if (rc != AORTA_OK) {
    return StatusOr<std::string>(Status::FromResult(rc));
  }
  out.resize(out_len);
  return StatusOr<std::string>(std::move(out));
}

/// Base for action feedback callback state.
///
/// Includes in-flight tracking (in_flight + shutting_down + cv) so that
/// `ActionFeedbackSubscription::~ActionFeedbackSubscription` can wait for
/// any callback currently running on a Zenoh worker thread to return
/// before destroying the state. Without this, destruction concurrent with
/// dispatch is a use-after-free path.
class ActionFeedbackCallbackStateBase {
 public:
  virtual ~ActionFeedbackCallbackStateBase() = default;
  virtual void OnFeedback(const aorta_bytes_view_t* payload) = 0;

  std::atomic<uint32_t> in_flight{0};
  std::atomic<bool> shutting_down{false};
  std::mutex in_flight_mutex;
  std::condition_variable in_flight_cv;
};

/// Base for action server callback state.
///
/// Same in-flight tracking pattern as ActionFeedbackCallbackStateBase
/// `ActionServer::~ActionServer` waits on cv.
///
/// Ownership is shared with the library: `library_ref` holds the
/// library's reference from server creation until Aorta invokes the
/// `user_data_release` hook, which happens exactly once, after the last
/// callback has returned. The state therefore never dies under a running
/// callback, however long the destructor's bounded wait gives up early.
class ActionServerCallbackStateBase {
 public:
  virtual ~ActionServerCallbackStateBase() = default;
  virtual void OnExecute(const aorta_bytes_view_t* payload,
                         aorta_action_context_t ctx) = 0;
  virtual void OnCancelRequested(const std::string& /*goal_id*/) {}

  std::shared_ptr<ActionServerCallbackStateBase> library_ref;
  std::atomic<uint32_t> in_flight{0};
  std::atomic<bool> shutting_down{false};
  std::mutex in_flight_mutex;
  std::condition_variable in_flight_cv;
};

#ifndef NDEBUG
inline ActionServerCallbackStateBase*& CurrentActionServerCallbackState() {
  static thread_local ActionServerCallbackStateBase* state = nullptr;
  return state;
}

class ActionServerCallbackIdentityGuard {
 public:
  explicit ActionServerCallbackIdentityGuard(
      ActionServerCallbackStateBase* state)
      : previous_(CurrentActionServerCallbackState()) {
    CurrentActionServerCallbackState() = state;
  }

  ~ActionServerCallbackIdentityGuard() {
    CurrentActionServerCallbackState() = previous_;
  }

 private:
  ActionServerCallbackStateBase* previous_;
};
#endif

}  // namespace detail

template <typename Action>
class ActionServer {
 public:
  ~ActionServer() {
    // Destructor wait (in-flight drain) + TOCTOU/leak-on-timeout fix.
    // See cpp/src/service.cc for the detailed rationale.
    if (state_) {
      std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
      state_->shutting_down.store(true, std::memory_order_release);
    }
    if (handle_ != 0) {
      // reentrancy: F (spawn-and-detach free — OK from a callback)
      aorta_action_server_free(handle_);
      handle_ = 0;
    }
#ifndef NDEBUG
    static std::atomic<bool> warned{false};
    // reentrancy: N (the detector call is the guard mechanism itself)
    if (aorta_reentrant_context() ||
        detail::CurrentActionServerCallbackState() == state_.get()) {
      detail::HandleReentrantHit("ActionServer::~ActionServer", warned);
    }
#endif
    bool drained = true;
    if (state_) {
      std::unique_lock<std::mutex> lock(state_->in_flight_mutex);
      drained = state_->in_flight_cv.wait_for(
          lock, std::chrono::seconds(2),
          [s = state_.get()] {
            return s->in_flight.load(std::memory_order_acquire) == 0;
          });
    }
    if (!drained) {
      // The library keeps its own reference (`library_ref`) until the
      // still-running callbacks return, so dropping ours is safe.
      std::fprintf(stderr,
                   "[aorta] action server destructor timed out waiting "
                   "for in-flight callbacks; their state is released when "
                   "they return (a callback may be hung)\n");
    }
    state_.reset();
  }

  ActionServer(const ActionServer&) = delete;
  ActionServer& operator=(const ActionServer&) = delete;

  const std::string& ActionName() const { return action_name_; }

  static StatusOr<std::shared_ptr<ActionServer<Action>>> ServeInfoOnly(
      const std::shared_ptr<Node>& node, const std::string& action_name,
      ActionServerOptions options = ActionServerOptions()) {
    if (node == nullptr) {
      return StatusOr<std::shared_ptr<ActionServer<Action>>>(
          Status(AORTA_ERR_INVALID_ARGUMENT, "action node is null"));
    }

    const auto* schema_bfbs = SchemaMeta<typename Action::Goal>::SchemaBfbs();
    const auto schema_bfbs_len =
        SchemaMeta<typename Action::Goal>::SchemaBfbsSize();
    if (schema_bfbs == nullptr || schema_bfbs_len == 0) {
      return StatusOr<std::shared_ptr<ActionServer<Action>>>(Status(
          AORTA_ERR_INVALID_ARGUMENT,
          "typed action requires action schema bfbs"));
    }

    auto ffi_options = options.ToFfi();
    aorta_action_server_t handle = 0;
    auto normalized_or = detail::NormalizeActionName(action_name);
    if (!normalized_or.ok()) {
      return StatusOr<std::shared_ptr<ActionServer<Action>>>(
          normalized_or.status());
    }
    const std::string& normalized = normalized_or.value();
    AORTA_CHECK_NOT_REENTRANT("ActionServer::ServeInfoOnly");
    const auto result = aorta_action_server_info_new_with_options(
        node->handle(), normalized.c_str(), schema_bfbs, schema_bfbs_len,
        &ffi_options, &handle);
    if (result != AORTA_OK) {
      return StatusOr<std::shared_ptr<ActionServer<Action>>>(
          Status::FromResult(result));
    }

    return std::shared_ptr<ActionServer<Action>>(new ActionServer<Action>(
        handle, normalized, std::move(options), nullptr, node));
  }

  template <typename Handler>
  static StatusOr<std::shared_ptr<ActionServer<Action>>> Serve(
      const std::shared_ptr<Node>& node, const std::string& action_name,
      ActionServerOptions options, Handler&& handler);

  /// Like Serve(), plus `on_cancel_requested(goal_id)`: invoked at most
  /// once per goal, when a cancel request moves it to CANCELING. It runs
  /// on an Aorta worker thread, concurrently with that goal's handler,
  /// and must return promptly — signal the handler and return. It may
  /// fire before the handler has seen the goal, or after the goal has
  /// already finished; a handler that registers its goal somewhere for
  /// the callback to find should check
  /// `ActionExecutionContext::IsCancelRequested()` after registering.
  /// Requires `options.supports_cancel`.
  template <typename Handler, typename CancelHandler>
  static StatusOr<std::shared_ptr<ActionServer<Action>>> Serve(
      const std::shared_ptr<Node>& node, const std::string& action_name,
      ActionServerOptions options, Handler&& handler,
      CancelHandler&& on_cancel_requested);

 private:
  ActionServer(aorta_action_server_t handle, std::string action_name,
               ActionServerOptions options,
               std::shared_ptr<detail::ActionServerCallbackStateBase> state,
               std::shared_ptr<Node> node)
      : handle_(handle),
        action_name_(std::move(action_name)),
        options_(std::move(options)),
        state_(std::move(state)),
        node_(std::move(node)) {}

  aorta_action_server_t handle_{0};
  std::string action_name_;
  ActionServerOptions options_;
  std::shared_ptr<detail::ActionServerCallbackStateBase> state_;
  std::shared_ptr<Node> node_;
};

namespace detail {

class ActionFeedbackSubscription {
 public:
  ActionFeedbackSubscription(
      aorta_action_feedback_sub_t handle,
      std::unique_ptr<ActionFeedbackCallbackStateBase> state)
      : handle_(handle), state_(std::move(state)) {}

  ~ActionFeedbackSubscription() {
    // Destructor wait (in-flight drain) + TOCTOU/leak-on-timeout fix.
    // See cpp/src/service.cc for the detailed rationale.
    if (state_) {
      std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
      state_->shutting_down.store(true, std::memory_order_release);
    }
    if (handle_ != 0) {
      // reentrancy: F (spawn-and-detach free — OK from a callback)
      aorta_action_feedback_sub_free(handle_);
      handle_ = 0;
    }
    AORTA_CHECK_NOT_REENTRANT(
        "ActionFeedbackSubscription::~ActionFeedbackSubscription");
    bool drained = true;
    if (state_) {
      std::unique_lock<std::mutex> lock(state_->in_flight_mutex);
      drained = state_->in_flight_cv.wait_for(
          lock, std::chrono::seconds(2),
          [s = state_.get()] {
            return s->in_flight.load(std::memory_order_acquire) == 0;
          });
    }
    if (drained) {
      state_.reset();
    } else {
      // INTENTIONAL LEAK on timeout — see ActionServer::~ActionServer.
      std::fprintf(
          stderr,
          "[aorta] action feedback subscription destructor timed out "
          "waiting for in-flight callbacks; leaking CallbackState to "
          "avoid UAF (a callback may be hung)\n");
      (void)state_.release();
    }
  }

  ActionFeedbackSubscription(const ActionFeedbackSubscription&) = delete;
  ActionFeedbackSubscription& operator=(const ActionFeedbackSubscription&) =
      delete;

 private:
  aorta_action_feedback_sub_t handle_{0};
  std::unique_ptr<ActionFeedbackCallbackStateBase> state_;
};

/// Rate-limited (~1s) logger for action callback exceptions, used by
/// the FFI dispatchers below to log instead of letting a throw cross into
/// Rust (which would be UB). Defined here rather than in cpp/src/internal/
/// because action.h is a public, header-only API.
///
/// Marked `noexcept` so a logging failure can never re-enter the catch.
inline void LogActionCallbackException(const char* kind) noexcept {
  // action.h must be includable from an -fno-exceptions
  // TU (flow_action.hpp's inclusion gate) — but `try`/`catch` themselves
  // fail to PARSE there (independent of instantiation), so the wrapper is
  // conditional rather than the catch clause. No behavior change with
  // exceptions on; under -fno-exceptions a throw in the body would
  // already be fatal (terminate), so there is nothing a catch could do.
#if defined(__cpp_exceptions)
  try {
#endif
    static std::atomic<uint64_t> last_log_ns{0};
    constexpr uint64_t kIntervalNs = 1'000'000'000ULL;
    const auto now = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    auto last = last_log_ns.load(std::memory_order_relaxed);
    if (now - last < kIntervalNs && last != 0) {
      return;
    }
    if (!last_log_ns.compare_exchange_strong(last, now,
                                             std::memory_order_relaxed,
                                             std::memory_order_relaxed)) {
      return;
    }
    std::fprintf(stderr,
                 "[aorta] %s callback threw an exception (suppressed to "
                 "prevent UB across FFI boundary)\n",
                 kind);
#if defined(__cpp_exceptions)
  } catch (...) {
  }
#endif
}

extern "C" inline void aorta_cpp_on_action_feedback(
    const aorta_bytes_view_t* payload, const aorta_schema_hash_view_t*,
    const aorta_msg_context_t*, void* user_data) {
  // In-flight tracking + TOCTOU fix. The check +
  // increment must happen under the same mutex the destructor uses, or
  // the destructor can race past in_flight==0 between (load shutting_down)
  // and (fetch_add in_flight). See cpp/src/service.cc for the detailed
  // rationale.
  // Conditional try (see LogActionCallbackException) — the
  // FatalError escape hatch requires a `throw` at the user callback site,
  // which itself does not compile under -fno-exceptions, so this catch is
  // unreachable there by construction; run the body directly.
#if defined(__cpp_exceptions)
  try {
#endif
    auto* state = static_cast<ActionFeedbackCallbackStateBase*>(user_data);
    if (state == nullptr) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(state->in_flight_mutex);
      if (state->shutting_down.load(std::memory_order_acquire)) {
        return;
      }
      state->in_flight.fetch_add(1, std::memory_order_acq_rel);
    }

    struct CallbackScopeGuard {
      ActionFeedbackCallbackStateBase* state;
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

    state->OnFeedback(payload);
#if defined(__cpp_exceptions)
  } catch (const FatalError& e) {
    // Dispatcher escape hatch — see cpp/src/subscriber.cc for rationale.
    std::fprintf(stderr,
                 "Aorta fatal (from action.feedback callback): %s\n",
                 e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    // Must not propagate exceptions across FFI into Rust (UB).
    LogActionCallbackException("action.feedback");
  }
#endif
}

// Admits one callback into `state` unless the server is shutting down;
// the destructor's bounded wait observes the count. Shared by the action
// server trampolines below.
class ActionServerInFlightScope {
 public:
  explicit ActionServerInFlightScope(ActionServerCallbackStateBase* state) {
    if (state == nullptr) {
      return;
    }
    std::lock_guard<std::mutex> lock(state->in_flight_mutex);
    if (state->shutting_down.load(std::memory_order_acquire)) {
      return;
    }
    state->in_flight.fetch_add(1, std::memory_order_acq_rel);
    state_ = state;
  }

  ~ActionServerInFlightScope() noexcept {
    if (state_ == nullptr) {
      return;
    }
    std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
    // notify_all UNDER the lock: once we unlock, the destructor can
    // observe in_flight==0 and pthread_cond_destroy in_flight_cv, which
    // would race a notify_all issued after unlock (TSAN: data race in
    // CallbackState::~CallbackState). Notifying while holding the lock
    // makes the destructor's wait re-acquire only after this returns.
    if (state_->in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      state_->in_flight_cv.notify_all();
    }
  }

  ActionServerInFlightScope(const ActionServerInFlightScope&) = delete;
  ActionServerInFlightScope& operator=(const ActionServerInFlightScope&) =
      delete;

  bool admitted() const { return state_ != nullptr; }

 private:
  ActionServerCallbackStateBase* state_ = nullptr;
};

extern "C" inline void aorta_cpp_on_action_execute(
    const aorta_bytes_view_t* payload, aorta_action_context_t ctx,
    void* user_data) {
  // In-flight tracking, mirrored from feedback dispatcher above.
  // Conditional try — see aorta_cpp_on_action_feedback
  // above for the rationale (identical shape).
#if defined(__cpp_exceptions)
  try {
#endif
    auto* state = static_cast<ActionServerCallbackStateBase*>(user_data);
    ActionServerInFlightScope scope(state);
    if (!scope.admitted()) {
      return;
    }

#ifndef NDEBUG
    ActionServerCallbackIdentityGuard identity_guard(state);
#endif

    state->OnExecute(payload, ctx);
#if defined(__cpp_exceptions)
  } catch (const FatalError& e) {
    // Dispatcher escape hatch — see cpp/src/subscriber.cc for rationale.
    std::fprintf(stderr,
                 "Aorta fatal (from action.execute callback): %s\n",
                 e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    // Must not propagate exceptions across FFI into Rust (UB).
    LogActionCallbackException("action.execute");
  }
#endif
}

extern "C" inline void aorta_cpp_on_action_cancel_requested(
    aorta_string_view_t goal_id, void* user_data) {
#if defined(__cpp_exceptions)
  try {
#endif
    auto* state = static_cast<ActionServerCallbackStateBase*>(user_data);
    ActionServerInFlightScope scope(state);
    if (!scope.admitted()) {
      return;
    }

#ifndef NDEBUG
    ActionServerCallbackIdentityGuard identity_guard(state);
#endif

    state->OnCancelRequested(
        goal_id.data == nullptr
            ? std::string()
            : std::string(reinterpret_cast<const char*>(goal_id.data),
                          goal_id.len));
#if defined(__cpp_exceptions)
  } catch (const FatalError& e) {
    std::fprintf(stderr,
                 "Aorta fatal (from action.cancel_requested callback): %s\n",
                 e.what());
    std::fflush(stderr);
    std::terminate();
  } catch (...) {
    // Must not propagate exceptions across FFI into Rust (UB).
    LogActionCallbackException("action.cancel_requested");
  }
#endif
}

// Drops the library's reference to the callback state. Aorta calls it
// exactly once, after the last callback has returned.
extern "C" inline void aorta_cpp_on_action_server_release(void* user_data) {
  auto* state = static_cast<ActionServerCallbackStateBase*>(user_data);
  if (state == nullptr) {
    return;
  }
  // Move out first: the reset may destroy `state` itself.
  auto library_ref = std::move(state->library_ref);
  library_ref.reset();
}

inline aorta_bytes_view_t BorrowedBytesView(const std::vector<uint8_t>& payload,
                                            aorta_bytes_slice_t* slice);
inline std::vector<uint8_t> CopyBytesView(const aorta_bytes_view_t* view);
inline std::vector<uint8_t> CopyOwnedBytesAndFree(aorta_owned_bytes_t* bytes);
inline StatusOr<ActionInfo> DecodeActionInfoPayload(
    const std::vector<uint8_t>& payload);

template <typename Action>
StatusOr<ActionSendGoalResponse> DecodeSendGoalResponse(
    const std::vector<uint8_t>& payload);

template <typename Action>
StatusOr<ActionCancelGoalResponse> DecodeCancelGoalResponse(
    const std::vector<uint8_t>& payload);

template <typename Action>
StatusOr<ActionPublicStatus> DecodePublicStatus(
    const std::vector<uint8_t>& payload);

template <typename Action>
StatusOr<ActionGetResultResponse<Action>> DecodeGetResultResponse(
    const std::vector<uint8_t>& payload);

inline Status SendGoalRejectedStatus(const std::string& action_name,
                                     const ActionSendGoalResponse& response);
inline Status CancelRejectedStatus(const std::string& action_name,
                                   const ActionCancelGoalResponse& response);

template <typename Action>
StatusOr<ActionGetResultResponse<Action>> ValidateGetResultResponse(
    const std::string& action_name, const std::string& goal_id,
    const std::string& expected_server_epoch,
    ActionGetResultResponse<Action> response);

template <typename Table>
StatusOr<const Table*> DecodeActionTable(const std::vector<uint8_t>& payload,
                                         const char* what);

inline bool ServerEpochMismatch(const std::string& expected,
                                const std::string& observed);
inline Status GoalEpochMismatchStatus(const std::string& action_name,
                                      const std::string& goal_id,
                                      const std::string& expected,
                                      const std::string& observed);
inline StatusOr<std::optional<std::chrono::milliseconds>>
RemainingWaitResultTimeout(
    const std::optional<std::chrono::steady_clock::time_point>& deadline,
    const std::string& goal_id);
inline StatusOr<std::string> GoalIdToStatusKeySegment(
    const std::string& goal_id);
inline StatusOr<std::string> BuildActionStatusTopic(
    const std::string& group, const std::string& action_name,
    const std::string& goal_id);

template <typename Action>
class ActionFeedbackCallbackState final : public ActionFeedbackCallbackStateBase {
 public:
  ActionFeedbackCallbackState(
      std::function<void(const typename Action::Feedback&)> callback,
      std::optional<std::string> goal_id_filter)
      : callback_(std::move(callback)),
        goal_id_filter_(std::move(goal_id_filter)) {}

  void OnFeedback(const aorta_bytes_view_t* payload) override {
    if (!callback_ || payload == nullptr) {
      return;
    }

    auto bytes = CopyBytesView(payload);
    auto root_or = DecodeActionTable<typename Action::Feedback>(
        bytes, "action feedback");
    if (!root_or.ok()) {
      return;
    }
    const auto* root = root_or.value();

    if (goal_id_filter_.has_value()) {
      const auto* goal_id = root->goal_id();
      if (goal_id == nullptr || goal_id->str() != goal_id_filter_.value()) {
        return;
      }
    }

    callback_(*root);
  }

 private:
  std::function<void(const typename Action::Feedback&)> callback_;
  std::optional<std::string> goal_id_filter_;
};

template <typename Action>
StatusOr<std::shared_ptr<ActionFeedbackSubscription>>
CreateActionFeedbackSubscription(
    aorta_action_client_t client_handle,
    std::function<void(const typename Action::Feedback&)> callback,
    std::optional<std::string> goal_id_filter) {
  auto state = std::make_unique<ActionFeedbackCallbackState<Action>>(
      std::move(callback), std::move(goal_id_filter));
  aorta_action_feedback_sub_t handle = 0;
  AORTA_CHECK_NOT_REENTRANT("detail::CreateActionFeedbackSubscription");
  const auto rc = aorta_action_client_subscribe_feedback(
      client_handle, &aorta_cpp_on_action_feedback, state.get(), &handle);
  if (rc != AORTA_OK) {
    return StatusOr<std::shared_ptr<ActionFeedbackSubscription>>(
        Status::FromResult(rc));
  }
  return std::make_shared<ActionFeedbackSubscription>(handle, std::move(state));
}

// ---------------------------------------------------------------------------
// Event-driven result subscription (the C++ binding over
// aorta_action_client_subscribe_result). One pair of extern
// "C" trampolines serves all Action types; the Action-specific decode and
// delivery run through the std::functions in ActionResultCallbackState.
// ---------------------------------------------------------------------------
struct ActionResultCallbackState {
  // Pure: returns ActionStatus as i8 from a status record (kind), or -1 on
  // decode error. Runs on the SDK watcher thread; must not touch aorta FFI.
  std::function<int8_t(uint8_t kind, const uint8_t* bytes, size_t len)> decode;
  // Decodes the raw GetResultResponse / surfaces the wait error, then invokes
  // the user callback. Runs on the SDK off-runtime delivery thread.
  std::function<void(const aorta_action_result_view_t* ev)> deliver;
};

// Shared status-record decoder: the subscribe_result decode hook and Flow's
// `wait_result` bridge classify terminality with the SAME rules. Returns the
// ActionStatus as int8, or -1 on decode error.
template <typename Action>
inline int8_t DecodeStatusRecord(uint8_t kind, const uint8_t* bytes,
                                 size_t len) {
  std::vector<uint8_t> payload(bytes, bytes + len);
  if (kind == AORTA_ACTION_STATUS_KIND_PUBLIC_STATUS) {
    auto decoded = detail::DecodePublicStatus<Action>(payload);
    if (!decoded.ok()) return -1;
    return static_cast<int8_t>(decoded->status);
  }
  auto decoded = detail::DecodeGetResultResponse<Action>(payload);
  if (!decoded.ok()) return -1;
  // A get_result response can carry a control-plane REJECTION (the goal is
  // unknown/expired, or the secret is wrong) with a NON-terminal status. It
  // is still a DEFINITIVE answer, so report it as terminal: the watcher then
  // stops and returns the payload, and the deliver path validates it into the
  // proper rejection Status (NotFound / InvalidArgument) — rather than waiting
  // for a status sample that will never arrive and reporting a false timeout.
  // These are exactly the codes ValidateGetResultResponse turns into an error.
  if (!IsTerminalActionStatus(decoded->status) &&
      (decoded->code == ActionReplyCode::kUnauthorized ||
       decoded->code == ActionReplyCode::kNotFound ||
       decoded->code == ActionReplyCode::kGoalExpired)) {
    return static_cast<int8_t>(ActionStatus::kAborted);
  }
  return static_cast<int8_t>(decoded->status);
}

// "No timeout" sentinel for the subscribe_result FFI: UINT64_MAX. The FFI treats
// every OTHER value — INCLUDING 0, which means "expire immediately" — as a real
// deadline, so 0 must NOT be reused for "wait forever" (that silently turned an
// explicit WaitResult(0ms) into an unbounded wait).
inline constexpr uint64_t kNoResultTimeoutMs = UINT64_MAX;

inline std::string CopyStringView(const aorta_string_view_t& view) {
  if (view.data == nullptr || view.len == 0) {
    return std::string();
  }
  return std::string(reinterpret_cast<const char*>(view.data),
                     static_cast<size_t>(view.len));
}

extern "C" inline int8_t aorta_cpp_action_decode_status(
    uint8_t kind, const uint8_t* bytes, uintptr_t len, void* user_data) {
  auto* state = static_cast<ActionResultCallbackState*>(user_data);
  if (state == nullptr || !state->decode) {
    return -1;
  }
  // Conditional try — see LogActionCallbackException above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    return state->decode(kind, bytes, static_cast<size_t>(len));
#if defined(__cpp_exceptions)
  } catch (...) {
    // Never let an exception cross the C ABI.
    return -1;
  }
#endif
}

extern "C" inline void aorta_cpp_action_on_result(
    const aorta_action_result_view_t* ev, void* user_data) {
  auto* state = static_cast<ActionResultCallbackState*>(user_data);
  if (state == nullptr || ev == nullptr || !state->deliver) {
    return;
  }
  // Conditional try — see LogActionCallbackException above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    state->deliver(ev);
#if defined(__cpp_exceptions)
  } catch (...) {
    // Never let an exception cross the C ABI.
  }
#endif
}

// RAII owner of a result subscription. Destruction frees the FFI handle, which
// cancels the watcher and drains any in-flight callback, and only THEN is
// `state_` (the std::functions the trampolines call) destroyed — member
// destruction runs after the destructor body.
class ActionResultSubscription {
 public:
  ActionResultSubscription(aorta_action_result_sub_t handle,
                           std::unique_ptr<ActionResultCallbackState> state)
      : handle_(handle), state_(std::move(state)) {}

  ~ActionResultSubscription() {
    if (handle_ != 0) {
      // reentrancy: F (BUSY-variant free — detaches, never blocks)
      const aorta_result_t rc = aorta_action_result_sub_free(handle_);
      handle_ = 0;
      if (rc != AORTA_OK) {
        // The SDK could not confirm the terminal callback has finished (it is
        // still running — e.g. a >2s handler — or this is a reentrant free from
        // inside the callback). Destroying `state_` now would be a
        // use-after-free, so INTENTIONALLY LEAK it (mirrors
        // ActionFeedbackSubscription / ActionServer leak-on-timeout; the leak is
        // bounded — at most one per hung/self-destructing wait).
        (void)state_.release();
      }
    }
  }

  ActionResultSubscription(const ActionResultSubscription&) = delete;
  ActionResultSubscription& operator=(const ActionResultSubscription&) = delete;

 private:
  aorta_action_result_sub_t handle_{0};
  std::unique_ptr<ActionResultCallbackState> state_;
};

// ---------------------------------------------------------------------------
// Bounded feedback stream (OpenFeedbackStream over subscribe_feedback2). The
// FFI delivers raw feedback bytes on its off-RX drain thread to one extern "C"
// trampoline, which runs the type-erased `ingest` (goal-id filter + push into
// the pull buffer below). The user drains via FeedbackStream::Recv/TryRecv.
// ---------------------------------------------------------------------------
struct FeedbackStreamShared {
  std::mutex mutex;
  std::condition_variable cv;
  std::deque<std::vector<uint8_t>> queue;  // raw feedback payloads (decoded on Recv)
  bool closed = false;
  // This goal's bound, applied AFTER the goal-id filter (the FFI queue bounds
  // the action-wide RX→drain hop; this bounds the per-goal consumer buffer, so a
  // slow consumer cannot grow C++ memory without limit). `drop_oldest` mirrors
  // FeedbackOverflowPolicy; `dropped` counts this goal's drops at this layer.
  std::size_t capacity = 256;
  bool drop_oldest = true;
  uint64_t dropped = 0;
};

struct FeedbackStreamState {
  // Filters a raw feedback payload by goal_id and pushes it into `shared`.
  std::function<void(std::vector<uint8_t>)> ingest;
};

extern "C" inline void aorta_cpp_action_feedback_stream_on_msg(
    const aorta_bytes_view_t* payload, const aorta_schema_hash_view_t*,
    const aorta_msg_context_t*, void* user_data) {
  auto* state = static_cast<FeedbackStreamState*>(user_data);
  if (state == nullptr || payload == nullptr || !state->ingest) {
    return;
  }
  // Conditional try — see LogActionCallbackException above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    state->ingest(CopyBytesView(payload));
#if defined(__cpp_exceptions)
  } catch (...) {
    // Never let an exception cross the C ABI.
  }
#endif
}

}  // namespace detail

template <typename Action>
class RetainedActionGoal {
 public:
  explicit RetainedActionGoal(std::vector<uint8_t> payload)
      : payload_(std::move(payload)) {
    if (!TypedSubscribeTraits<typename Action::SendGoalRequest>::Verify(
            payload_.data(), payload_.size())) {
      return;
    }
    request_ = TypedSubscribeTraits<typename Action::SendGoalRequest>::GetRoot(
        payload_.data());
    if (request_ == nullptr || request_->goal_id() == nullptr ||
        request_->goal() == nullptr) {
      request_ = nullptr;
      return;
    }
    goal_id_ = request_->goal_id()->str();
    goal_ = request_->goal();
  }

  const std::string& goal_id() const { return goal_id_; }
  const typename Action::SendGoalRequest* request() const { return request_; }
  const typename Action::Goal* message() const { return goal_; }
  const std::vector<uint8_t>& payload() const { return payload_; }
  bool valid() const { return request_ != nullptr && goal_ != nullptr; }

 private:
  std::vector<uint8_t> payload_;
  std::string goal_id_;
  const typename Action::SendGoalRequest* request_ = nullptr;
  const typename Action::Goal* goal_ = nullptr;
};

// Owns a raw feedback payload and exposes the decoded typed root (valid while
// this object lives). Returned by FeedbackStream::Recv/TryRecv.
template <typename Action>
class RetainedActionFeedback {
 public:
  explicit RetainedActionFeedback(std::vector<uint8_t> payload)
      : payload_(std::move(payload)) {
    auto root_or =
        detail::DecodeActionTable<typename Action::Feedback>(payload_, "action feedback");
    if (root_or.ok()) {
      feedback_ = root_or.value();
    }
  }

  // Move-only: a COPY would reallocate `payload_` while `feedback_` still points
  // into the source buffer (dangling). A move steals the buffer, so the root
  // pointer stays valid — the defaulted move is correct.
  RetainedActionFeedback(const RetainedActionFeedback&) = delete;
  RetainedActionFeedback& operator=(const RetainedActionFeedback&) = delete;
  RetainedActionFeedback(RetainedActionFeedback&&) noexcept = default;
  RetainedActionFeedback& operator=(RetainedActionFeedback&&) noexcept = default;

  const typename Action::Feedback* message() const { return feedback_; }
  const std::vector<uint8_t>& payload() const { return payload_; }
  bool valid() const { return feedback_ != nullptr; }

 private:
  std::vector<uint8_t> payload_;
  const typename Action::Feedback* feedback_ = nullptr;
};

enum class FeedbackOverflowPolicy : uint8_t {
  kDropOldest = 0,  // AORTA_FB_DROP_OLDEST
  kDropNew = 1,     // AORTA_FB_DROP_NEW
};

struct FeedbackStreamOptions {
  std::size_t capacity = 256;
  FeedbackOverflowPolicy overflow = FeedbackOverflowPolicy::kDropOldest;
};

// A bounded, off-RX feedback stream the consumer DRAINS at its own pace
// (Recv/TryRecv) — so a slow consumer no longer stalls the zenoh link the way
// an inline SetFeedbackCallback does. Built over subscribe_feedback2. Move-only.
//
// Loss model (two layers): `DroppedCount()` reports THIS goal's drops when the
// consumer falls behind the per-goal `capacity`. Separately, the FFI's
// action-wide RX→drain queue may drop samples (counting all goals, pre-filter)
// ONLY under extreme CPU pressure when the decode/drain cannot keep up; that is
// rare and not reflected in DroppedCount. In the common case (drain keeps up, or
// max_concurrency==1) the FFI layer drops nothing, so DroppedCount is the goal's
// total loss.
template <typename Action>
class FeedbackStream {
 public:
  FeedbackStream(FeedbackStream&& other) noexcept { *this = std::move(other); }
  FeedbackStream& operator=(FeedbackStream&& other) noexcept {
    if (this != &other) {
      Close();
      handle_ = other.handle_;
      shared_ = std::move(other.shared_);
      state_ = std::move(other.state_);
      other.handle_ = 0;
    }
    return *this;
  }
  FeedbackStream(const FeedbackStream&) = delete;
  FeedbackStream& operator=(const FeedbackStream&) = delete;
  ~FeedbackStream() { Close(); }

  // Blocking pop; returns nullopt only once the stream is closed AND drained.
  std::optional<RetainedActionFeedback<Action>> Recv() {
    auto shared = shared_;
    if (!shared) return std::nullopt;
    std::unique_lock<std::mutex> lock(shared->mutex);
    if (shared->queue.empty() && !shared->closed) {
      // Would block on the cv — explicit wait.
      AORTA_CHECK_NOT_REENTRANT("FeedbackStream::Recv");
    }
    shared->cv.wait(lock, [&] { return !shared->queue.empty() || shared->closed; });
    if (shared->queue.empty()) {
      return std::nullopt;
    }
    auto bytes = std::move(shared->queue.front());
    shared->queue.pop_front();
    lock.unlock();
    return RetainedActionFeedback<Action>(std::move(bytes));
  }

  // Non-blocking pop.
  std::optional<RetainedActionFeedback<Action>> TryRecv() {
    auto shared = shared_;
    if (!shared) return std::nullopt;
    std::lock_guard<std::mutex> lock(shared->mutex);
    if (shared->queue.empty()) {
      return std::nullopt;
    }
    auto bytes = std::move(shared->queue.front());
    shared->queue.pop_front();
    return RetainedActionFeedback<Action>(std::move(bytes));
  }

  // Samples dropped for THIS goal because the consumer fell behind the bounded
  // per-goal buffer — the "am I draining fast enough?" signal. (Action-wide
  // drops at the FFI RX→drain queue under extreme CPU pressure are separate and
  // not counted here; in the common case the drain keeps up and that count is
  // zero, so this equals total loss for the goal.)
  uint64_t DroppedCount() const {
    if (!shared_) {
      return 0;
    }
    std::lock_guard<std::mutex> lock(shared_->mutex);
    return shared_->dropped;
  }

 private:
  friend class GoalHandle<Action>;
  friend class ActionClient<Action>;
  FeedbackStream(aorta_action_feedback_sub_t handle,
                 std::shared_ptr<detail::FeedbackStreamShared> shared,
                 std::unique_ptr<detail::FeedbackStreamState> state)
      : handle_(handle), shared_(std::move(shared)), state_(std::move(state)) {}

  void Close() {
    if (handle_ == 0) {
      return;
    }
    AORTA_CHECK_NOT_REENTRANT("FeedbackStream::Close");
    const aorta_result_t rc = aorta_action_feedback_sub2_free(handle_);
    handle_ = 0;
    if (shared_) {
      {
        std::lock_guard<std::mutex> lock(shared_->mutex);
        shared_->closed = true;
      }
      shared_->cv.notify_all();
    }
    if (rc != AORTA_OK) {
      // Reentrant/hung free: the ingest callback may still touch state_ (which
      // captures `shared_`) — leak it to avoid a use-after-free.
      (void)state_.release();
    }
  }

  aorta_action_feedback_sub_t handle_{0};
  std::shared_ptr<detail::FeedbackStreamShared> shared_;
  std::unique_ptr<detail::FeedbackStreamState> state_;
};

template <typename Action>
class ActionExecutionContext {
 public:
  ActionExecutionContext(aorta_action_context_t handle, std::string goal_id)
      : handle_(handle), goal_id_(std::move(goal_id)) {}

  ~ActionExecutionContext() {
    if (handle_ != 0) {
      aorta_action_context_free(handle_);
      handle_ = 0;
    }
  }

  ActionExecutionContext(const ActionExecutionContext&) = delete;
  ActionExecutionContext& operator=(const ActionExecutionContext&) = delete;

  ActionExecutionContext(ActionExecutionContext&& other) noexcept
      : handle_(other.handle_), goal_id_(std::move(other.goal_id_)) {
    other.handle_ = 0;
  }

  ActionExecutionContext& operator=(ActionExecutionContext&& other) noexcept {
    if (this != &other) {
      if (handle_ != 0) {
        aorta_action_context_free(handle_);
      }
      handle_ = other.handle_;
      goal_id_ = std::move(other.goal_id_);
      other.handle_ = 0;
    }
    return *this;
  }

  const std::string& goal_id() const { return goal_id_; }

  bool IsCancelRequested() const {
    if (handle_ == 0) {
      return false;
    }
    uint8_t requested = 0;
    if (aorta_action_context_is_cancel_requested(handle_, &requested) !=
        AORTA_OK) {
      return false;
    }
    return requested != 0;
  }

  Status PublishFeedback(const typename Action::FeedbackDataMsg& feedback,
                         const std::string& message = std::string()) const {
    if (handle_ == 0) {
      return Status(AORTA_ERR_INVALID_ARGUMENT, "invalid action context");
    }
    AORTA_CHECK_NOT_REENTRANT("ActionExecutionContext::PublishFeedback");
    auto payload = Action::EncodeFeedbackData(feedback);
    aorta_bytes_slice_t slice{};
    auto view = detail::BorrowedBytesView(payload, &slice);
    return Status::FromResult(aorta_action_context_publish_feedback(
        handle_, &view, message.c_str()));
  }

  Status PublishFeedback(const typename Action::FeedbackData& feedback,
                         const std::string& message = std::string()) const {
    return PublishFeedback(Action::CopyFeedbackData(feedback), message);
  }

  Status Succeed(const typename Action::ResultMsg& result) const {
    if (handle_ == 0) {
      return Status(AORTA_ERR_INVALID_ARGUMENT, "invalid action context");
    }
    AORTA_CHECK_NOT_REENTRANT("ActionExecutionContext::Succeed");
    auto payload = Action::EncodeResult(result);
    aorta_bytes_slice_t slice{};
    auto view = detail::BorrowedBytesView(payload, &slice);
    return Status::FromResult(aorta_action_context_succeed(handle_, &view));
  }

  Status Succeed(const typename Action::Result& result) const {
    return Succeed(Action::CopyResult(result));
  }

  Status Abort(ActionReplyCode code, const std::string& message) const {
    if (handle_ == 0) {
      return Status(AORTA_ERR_INVALID_ARGUMENT, "invalid action context");
    }
    AORTA_CHECK_NOT_REENTRANT("ActionExecutionContext::Abort");
    return Status::FromResult(aorta_action_context_abort(
        handle_, static_cast<int16_t>(code), message.c_str()));
  }

  Status Cancel(ActionReplyCode code, const std::string& message) const {
    if (handle_ == 0) {
      return Status(AORTA_ERR_INVALID_ARGUMENT, "invalid action context");
    }
    AORTA_CHECK_NOT_REENTRANT("ActionExecutionContext::Cancel");
    return Status::FromResult(aorta_action_context_cancel(
        handle_, static_cast<int16_t>(code), message.c_str()));
  }

  /// The trace context identifying THIS goal (valid only while tracing was
  /// enabled at accept): the trace id plus the span deferred work should
  /// parent under — the per-goal server root. The synchronous execute
  /// callback already runs under an adopted context (AORTA_TRACE_SPAN spans
  /// nest automatically); use this for work moved to OTHER threads, installed
  /// there via `aorta::trace::ContextGuard`.
  trace::Context TraceContext() const {
    trace::Context ctx;
    if (handle_ == 0) {
      return ctx;
    }
    ctx.valid =
        aorta_action_context_trace_context(handle_, ctx.trace_id, &ctx.span_id) != 0;
    return ctx;
  }

 private:
  aorta_action_context_t handle_{0};
  std::string goal_id_;
};

template <typename Action>
class ActionClient : public std::enable_shared_from_this<ActionClient<Action>> {
 public:
  /// The C-ABI handle (Flow's action ctx bridge — `flow::send_goal` /
  /// `flow::wait_result` / `flow::cancel_goal` — and other C-level
  /// composition).
  aorta_action_client_t native_handle() const { return handle_; }

  /// Fresh RFC-4122-shaped goal id (public so Flow's awaitable
  /// `flow::send_goal` mints ids with the same recipe).
  static std::string GenerateGoalId() {
    thread_local std::mt19937_64 rng(std::random_device{}());
    uint8_t bytes[16];
    for (size_t i = 0; i < 16; i += 8) {
      const uint64_t chunk = rng();
      for (size_t j = 0; j < 8; ++j) {
        bytes[i + j] = static_cast<uint8_t>((chunk >> ((7 - j) * 8)) & 0xFF);
      }
    }
    bytes[6] = static_cast<uint8_t>((bytes[6] & 0x0F) | 0x40);
    bytes[8] = static_cast<uint8_t>((bytes[8] & 0x3F) | 0x80);

    char out[37];
    std::snprintf(
        out, sizeof(out),
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
        bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
        bytes[13], bytes[14], bytes[15]);
    return std::string(out);
  }

  ~ActionClient() {
    feedback_subscription_.reset();
    if (handle_ != 0) {
      aorta_action_client_free(handle_);
      handle_ = 0;
    }
  }

  ActionClient(const ActionClient&) = delete;
  ActionClient& operator=(const ActionClient&) = delete;

  const std::string& ActionName() const { return action_name_; }

  StatusOr<GoalHandle<Action>> SendGoal(const typename Action::GoalMsg& goal) {
    return SendGoalWithId(GenerateGoalId(), goal);
  }

  StatusOr<GoalHandle<Action>> SendGoalWithId(const std::string& goal_id,
                                              const typename Action::GoalMsg& goal) {
    std::vector<uint8_t> request = Action::EncodeSendGoalRequest(goal_id, goal);
    // The FFI mints the goal_sent span identity and injects it on the
    // send_goal query attachment (the server's goal root parents to it); the
    // span itself is recorded HERE once ACCEPT is decoded — the FFI cannot
    // see accept/reject inside the opaque response payload.
    const uint64_t sent_mono_ns =
        aorta_trace_is_enabled() != 0 ? aorta_trace_monotonic_ns() : 0;
    trace::Context goal_trace;
    uint64_t goal_trace_parent = 0;
    auto response_or =
        SendGoalPayloadTraced(std::move(request), &goal_trace, &goal_trace_parent);
    if (!response_or.ok()) {
      return StatusOr<GoalHandle<Action>>(response_or.status());
    }

    auto decoded = detail::DecodeSendGoalResponse<Action>(response_or.value());
    if (!decoded.ok()) {
      return StatusOr<GoalHandle<Action>>(decoded.status());
    }
    const auto& response = decoded.value();
    if (response.goal_id != goal_id) {
      return StatusOr<GoalHandle<Action>>(Status(
          AORTA_ERR_INTERNAL,
          "action send_goal reply goal_id mismatch"));
    }
    if (!response.accepted) {
      return StatusOr<GoalHandle<Action>>(
          detail::SendGoalRejectedStatus(action_name_, response));
    }

    if (goal_trace.valid) {
      trace::record_stage_at(trace::StageId::kActionGoalSentToAccepted,
                             action_name_.c_str(), sent_mono_ns,
                             goal_trace.trace_id, goal_trace.span_id,
                             goal_trace_parent);
    }

    return GoalHandle<Action>(
        this->shared_from_this(), response.goal_id, response.goal_secret,
        response.server_epoch, goal_trace, sent_mono_ns);
  }

  StatusOr<ActionInfo> Info() const {
    AORTA_CHECK_NOT_REENTRANT("ActionClient::Info");
    aorta_owned_bytes_t response{};
    const auto rc = aorta_action_client_info(handle_, &response);
    if (rc != AORTA_OK) {
      return StatusOr<ActionInfo>(Status::FromResult(rc));
    }
    auto payload = detail::CopyOwnedBytesAndFree(&response);
    return detail::DecodeActionInfoPayload(payload);
  }

  StatusOr<std::optional<ActionPublicStatus>> QueryStatus(
      const std::string& goal_id) const {
    return QueryStatusWithTimeout(goal_id, std::nullopt);
  }

  Status SetFeedbackCallbackAll(
      std::function<void(const typename Action::Feedback&)> callback) {
    if (!callback) {
      feedback_subscription_.reset();
      return Status::Ok();
    }

    AORTA_CHECK_NOT_REENTRANT("ActionClient::SetFeedbackCallbackAll");
    auto subscription_or = detail::CreateActionFeedbackSubscription<Action>(
        handle_, std::move(callback), std::nullopt);
    if (!subscription_or.ok()) {
      return subscription_or.status();
    }
    feedback_subscription_ = std::move(subscription_or).value();
    return Status::Ok();
  }

  // Open a goal-scoped feedback stream for `goal_id` WITHOUT a GoalHandle —
  // pair it with SendGoalWithId(GenerateGoalId(), ...) and subscribe BEFORE
  // sending, so feedback the server publishes as its first handler action
  // (e.g. a business "accepted" frame) can never race the subscription
  // (SendGoal + GoalHandle::OpenFeedbackStream leaves a window where such
  // early samples are lost — topic subscriptions do not replay).
  //
  // Like GoalHandle::OpenFeedbackStream, the stream ends itself once THIS
  // client observes the goal's terminal result (via
  // GetResult/WaitResult/SetResultCallback): Recv() drains buffered samples,
  // then returns nullopt.
  StatusOr<FeedbackStream<Action>> OpenFeedbackStreamFor(
      const std::string& goal_id, FeedbackStreamOptions options = {}) {
    AORTA_CHECK_NOT_REENTRANT("ActionClient::OpenFeedbackStreamFor");
    auto shared = std::make_shared<detail::FeedbackStreamShared>();
    shared->capacity = options.capacity > 0 ? options.capacity : 1;
    shared->drop_oldest =
        options.overflow == FeedbackOverflowPolicy::kDropOldest;
    auto state = std::make_unique<detail::FeedbackStreamState>();
    state->ingest = [shared, goal_id](std::vector<uint8_t> bytes) {
      auto root_or =
          detail::DecodeActionTable<typename Action::Feedback>(bytes, "action feedback");
      if (!root_or.ok()) {
        return;
      }
      const auto* root = root_or.value();
      if (root->goal_id() == nullptr || root->goal_id()->str() != goal_id) {
        return;  // feedback for a different goal
      }
      {
        std::lock_guard<std::mutex> lock(shared->mutex);
        if (shared->closed) {
          return;
        }
        // Bound the per-goal buffer: a slow consumer cannot grow C++ memory
        // without limit. On overflow, honor the overflow policy and count the
        // drop against THIS goal (so DroppedCount is the consumer-behind signal,
        // not the action-wide FFI count).
        if (shared->queue.size() >= shared->capacity) {
          if (shared->drop_oldest) {
            shared->queue.pop_front();
            shared->queue.push_back(std::move(bytes));
          }
          // kDropNew: drop the incoming sample.
          ++shared->dropped;
        } else {
          shared->queue.push_back(std::move(bytes));
        }
      }
      shared->cv.notify_one();
    };

    aorta_action_feedback_sub_t handle = 0;
    const aorta_result_t rc = aorta_action_client_subscribe_feedback2(
        handle_, static_cast<uint32_t>(options.capacity),
        static_cast<uint8_t>(options.overflow),
        &detail::aorta_cpp_action_feedback_stream_on_msg, state.get(), &handle);
    if (rc != AORTA_OK) {
      return StatusOr<FeedbackStream<Action>>(Status::FromResult(rc));
    }
    if (!RegisterGoalStream(goal_id, shared)) {
      // Terminal already observed for this goal: no feedback can arrive
      // anymore, so the stream is born closed (Recv() -> nullopt) instead
      // of parking forever. The subscription stays until the user closes
      // the stream; its ingest drops everything via the closed flag.
      {
        std::lock_guard<std::mutex> lock(shared->mutex);
        shared->closed = true;
      }
      shared->cv.notify_all();
    }
    return StatusOr<FeedbackStream<Action>>(
        FeedbackStream<Action>(handle, std::move(shared), std::move(state)));
  }

 private:
  friend class GoalHandle<Action>;
  friend class Node;

  // Per-goal end-of-stream: goal-scoped feedback streams awaiting this
  // goal's terminal observation — goal_id → weak refs to every open stream's
  // shared queue, plus a bounded FIFO memory of observed terminals so a
  // stream opened AFTER (or racing) the terminal observation is born closed
  // instead of parking forever. One lock covers both halves, so the terminal
  // check and the registration are atomic w.r.t. each other. Pruned when
  // streams are gone (weak); consumed by CloseGoalStreams. The client-wide
  // SetFeedbackCallback / feedback-all paths never register here.
  //
  // The FIFO cap makes the memory a race-window guard, not a permanent
  // archive: once evicted, a late-opened stream behaves like any
  // subscription with no publisher (an unbounded set would reintroduce
  // completed-goal growth).
  static constexpr std::size_t kRecentTerminalGoalsCap = 128;

  struct GoalStreamRegistry {
    std::unordered_map<std::string,
                       std::vector<std::weak_ptr<detail::FeedbackStreamShared>>>
        streams;
    std::deque<std::string> recent_terminals;

    bool IsRecentlyTerminal(const std::string& goal_id) const {
      return std::find(recent_terminals.begin(), recent_terminals.end(),
                       goal_id) != recent_terminals.end();
    }

    void RecordTerminal(const std::string& goal_id) {
      if (IsRecentlyTerminal(goal_id)) {
        return;
      }
      if (recent_terminals.size() >= kRecentTerminalGoalsCap) {
        recent_terminals.pop_front();
      }
      recent_terminals.push_back(goal_id);
    }
  };

  // Returns false when this goal's terminal was already observed — the
  // caller must hand back a born-closed stream. Returns true after
  // registering the stream for terminal-driven closure.
  bool RegisterGoalStream(
      const std::string& goal_id,
      const std::shared_ptr<detail::FeedbackStreamShared>& shared) const {
    std::lock_guard<std::mutex> lock(goal_streams_mutex_);
    if (goal_streams_.IsRecentlyTerminal(goal_id)) {
      return false;
    }
    auto& streams = goal_streams_.streams;
    for (auto it = streams.begin(); it != streams.end();) {
      auto& entries = it->second;
      entries.erase(std::remove_if(entries.begin(), entries.end(),
                                   [](const auto& weak) { return weak.expired(); }),
                    entries.end());
      it = entries.empty() ? streams.erase(it) : std::next(it);
    }
    streams[goal_id].push_back(shared);
    return true;
  }

  // Close every goal-scoped feedback stream registered for `goal_id`, and
  // remember the terminal so a stream opened after (or racing) this
  // observation is born closed: mark the shared queue closed and wake parked
  // Recv() calls, which drain the buffered samples and then return nullopt.
  // Idempotent.
  void CloseGoalStreams(const std::string& goal_id) const {
    std::vector<std::weak_ptr<detail::FeedbackStreamShared>> entries;
    {
      std::lock_guard<std::mutex> lock(goal_streams_mutex_);
      goal_streams_.RecordTerminal(goal_id);
      auto it = goal_streams_.streams.find(goal_id);
      if (it == goal_streams_.streams.end()) {
        return;
      }
      entries = std::move(it->second);
      goal_streams_.streams.erase(it);
    }
    for (auto& weak : entries) {
      if (auto shared = weak.lock()) {
        {
          std::lock_guard<std::mutex> lock(shared->mutex);
          shared->closed = true;
        }
        shared->cv.notify_all();
      }
    }
  }

  // The single place the client conclusively learns "this goal
  // publishes no more feedback" — a terminal status, or a NotFound /
  // GoalExpired rejection (the goal is gone). Unauthorized says nothing
  // about the goal, so it does not close. Mirrors the Rust core hook in
  // get_result_with_timeouts.
  void NotifyResultObserved(
      const std::string& goal_id,
      const ActionGetResultResponse<Action>& response) const {
    const bool no_more_feedback =
        response.goal_id == goal_id &&
        (IsTerminalActionStatus(response.status) ||
         response.code == ActionReplyCode::kNotFound ||
         response.code == ActionReplyCode::kGoalExpired);
    if (no_more_feedback) {
      CloseGoalStreams(goal_id);
    }
  }

  ActionClient(aorta_action_client_t handle, std::string action_name,
               ActionClientOptions options, std::shared_ptr<Node> node)
      : handle_(handle),
        action_name_(std::move(action_name)),
        options_(std::move(options)),
        node_(std::move(node)) {}

  StatusOr<ActionCancelGoalResponse> CancelGoal(
      const std::string& goal_id, const std::string& goal_secret) const {
    AORTA_CHECK_NOT_REENTRANT("ActionClient::CancelGoal");
    std::vector<uint8_t> request =
        Action::EncodeCancelGoalRequest(goal_id, goal_secret);
    auto response_or = CallWithPayload(aorta_action_client_cancel,
                                       std::move(request));
    if (!response_or.ok()) {
      return StatusOr<ActionCancelGoalResponse>(response_or.status());
    }

    auto decoded = detail::DecodeCancelGoalResponse<Action>(response_or.value());
    if (!decoded.ok()) {
      return StatusOr<ActionCancelGoalResponse>(decoded.status());
    }
    const auto& response = decoded.value();
    if (response.goal_id != goal_id) {
      return StatusOr<ActionCancelGoalResponse>(Status(
          AORTA_ERR_INTERNAL,
          "action cancel reply goal_id mismatch"));
    }
    if (!response.accepted) {
      return StatusOr<ActionCancelGoalResponse>(
          detail::CancelRejectedStatus(action_name_, response));
    }
    return decoded;
  }

  StatusOr<std::optional<ActionPublicStatus>> QueryStatusWithTimeout(
      const std::string& goal_id,
      std::optional<std::chrono::milliseconds> timeout) const {
    // The shared query commit point — covers QueryStatus (and any
    // timeout variant) exactly once per call.
    AORTA_CHECK_NOT_REENTRANT("ActionClient::QueryStatus");
    aorta_owned_bytes_t response{};
    const auto rc = timeout.has_value()
                        ? aorta_action_client_query_status_with_timeout(
                              handle_, goal_id.c_str(),
                              static_cast<uint64_t>(timeout->count()), &response)
                        : aorta_action_client_query_status(handle_, goal_id.c_str(),
                                                           &response);
    if (rc == AORTA_ERR_NOT_FOUND) {
      return std::optional<ActionPublicStatus>();
    }
    if (rc != AORTA_OK) {
      return StatusOr<std::optional<ActionPublicStatus>>(Status::FromResult(rc));
    }

    auto payload = detail::CopyOwnedBytesAndFree(&response);
    auto decoded = detail::DecodePublicStatus<Action>(payload);
    if (!decoded.ok()) {
      return StatusOr<std::optional<ActionPublicStatus>>(decoded.status());
    }
    return std::optional<ActionPublicStatus>(std::move(decoded.value()));
  }

  StatusOr<ActionGetResultResponse<Action>> GetResultGoal(
      const std::string& goal_id, const std::string& goal_secret,
      const std::string& expected_server_epoch) const {
    return GetResultGoalWithTimeout(goal_id, goal_secret, expected_server_epoch,
                                    std::nullopt);
  }

  StatusOr<ActionGetResultResponse<Action>> GetResultGoalWithTimeout(
      const std::string& goal_id, const std::string& goal_secret,
      const std::string& expected_server_epoch,
      std::optional<std::chrono::milliseconds> timeout) const {
    std::vector<uint8_t> request =
        Action::EncodeGetResultRequest(goal_id, goal_secret);
    auto response_or = timeout.has_value()
                           ? CallWithPayloadAndTimeout(
                                 aorta_action_client_get_result_with_timeout,
                                 std::move(request), timeout.value())
                           : CallWithPayload(aorta_action_client_get_result,
                                             std::move(request));
    if (!response_or.ok()) {
      return StatusOr<ActionGetResultResponse<Action>>(response_or.status());
    }

    auto decoded = detail::DecodeGetResultResponse<Action>(response_or.value());
    if (!decoded.ok()) {
      return StatusOr<ActionGetResultResponse<Action>>(decoded.status());
    }
    NotifyResultObserved(goal_id, decoded.value());
    return detail::ValidateGetResultResponse(
        action_name_, goal_id, expected_server_epoch, std::move(decoded.value()));
  }

  using RawActionCall = aorta_result_t (*)(aorta_action_client_t,
                                           const aorta_bytes_view_t*,
                                           aorta_owned_bytes_t*);
  using RawActionCallWithTimeout = aorta_result_t (*)(aorta_action_client_t,
                                                      const aorta_bytes_view_t*,
                                                      uint64_t,
                                                      aorta_owned_bytes_t*);

  StatusOr<std::vector<uint8_t>> CallWithPayload(RawActionCall fn,
                                                 std::vector<uint8_t> request) const {
    aorta_bytes_slice_t slice{};
    auto view = detail::BorrowedBytesView(request, &slice);
    aorta_owned_bytes_t response{};
    const auto rc = fn(handle_, &view, &response);
    if (rc != AORTA_OK) {
      return StatusOr<std::vector<uint8_t>>(Status::FromResult(rc));
    }
    return detail::CopyOwnedBytesAndFree(&response);
  }

  // send_goal with the minted goal_sent span identity returned.
  // `out_ctx->valid` is false when tracing was disabled at send.
  StatusOr<std::vector<uint8_t>> SendGoalPayloadTraced(
      std::vector<uint8_t> request, trace::Context* out_ctx,
      uint64_t* out_parent_span_id) const {
    // The shared send-goal commit point — covers BOTH public entries
    // (SendGoal / SendGoalWithId) exactly once per call.
    AORTA_CHECK_NOT_REENTRANT("ActionClient::SendGoal");
    aorta_bytes_slice_t slice{};
    auto view = detail::BorrowedBytesView(request, &slice);
    aorta_owned_bytes_t response{};
    uint64_t span_id = 0;
    const auto rc = aorta_action_client_send_goal_traced(
        handle_, &view, out_ctx->trace_id, &span_id, out_parent_span_id,
        &response);
    if (rc != AORTA_OK) {
      return StatusOr<std::vector<uint8_t>>(Status::FromResult(rc));
    }
    out_ctx->span_id = span_id;
    out_ctx->valid = span_id != 0;
    return detail::CopyOwnedBytesAndFree(&response);
  }

  StatusOr<std::vector<uint8_t>> CallWithPayloadAndTimeout(
      RawActionCallWithTimeout fn, std::vector<uint8_t> request,
      std::chrono::milliseconds timeout) const {
    aorta_bytes_slice_t slice{};
    auto view = detail::BorrowedBytesView(request, &slice);
    aorta_owned_bytes_t response{};
    const auto rc =
        fn(handle_, &view, static_cast<uint64_t>(timeout.count()), &response);
    if (rc != AORTA_OK) {
      return StatusOr<std::vector<uint8_t>>(Status::FromResult(rc));
    }
    return detail::CopyOwnedBytesAndFree(&response);
  }


  aorta_action_client_t handle_{0};
  std::string action_name_;
  ActionClientOptions options_;
  std::shared_ptr<Node> node_;
  std::shared_ptr<detail::ActionFeedbackSubscription> feedback_subscription_;
  // Per-goal end-of-stream registry (see RegisterGoalStream /
  // CloseGoalStreams). `mutable` because the terminal observation arrives
  // through const result paths (GetResultGoal is const).
  mutable std::mutex goal_streams_mutex_;
  mutable GoalStreamRegistry goal_streams_;
};

template <typename Action>
class GoalHandle {
 public:
  static StatusOr<GoalHandle<Action>> FromPersisted(
      const std::shared_ptr<ActionClient<Action>>& client,
      const std::string& goal_id, const std::string& goal_secret) {
    if (client == nullptr) {
      return StatusOr<GoalHandle<Action>>(
          Status(AORTA_ERR_INVALID_ARGUMENT, "action client is null"));
    }
    if (goal_id.empty() || goal_secret.empty()) {
      return StatusOr<GoalHandle<Action>>(Status(
          AORTA_ERR_INVALID_ARGUMENT,
          "goal_id and goal_secret must not be empty"));
    }
    return GoalHandle<Action>(client, goal_id, goal_secret, std::string());
  }

  const std::string& goal_id() const { return goal_id_; }
  const std::string& goal_secret() const { return goal_secret_; }
  const std::string& server_epoch() const { return server_epoch_; }

  StatusOr<std::optional<ActionPublicStatus>> QueryStatus() const {
    return client_->QueryStatus(goal_id_);
  }

  StatusOr<ActionCancelGoalResponse> Cancel() const {
    return client_->CancelGoal(goal_id_, goal_secret_);
  }

  StatusOr<ActionGetResultResponse<Action>> GetResult() const {
    AORTA_CHECK_NOT_REENTRANT("GoalHandle::GetResult");
    auto result = client_->GetResultGoal(goal_id_, goal_secret_, server_epoch_);
    // Close the goal-lifetime span the first time a poll observes a TERMINAL
    // result; a non-terminal ("still executing") reply is just a poll.
    if (result.ok() && IsTerminalActionStatus(result.value().status)) {
      RecordGoalLifetime();
    }
    return result;
  }

  StatusOr<ActionGetResultResponse<Action>> WaitResult(
      std::optional<std::chrono::milliseconds> timeout = std::nullopt) const {
    // Tracing (client half): wait entry -> result obtained,
    // recorded for every outcome as a child of the goal_sent span (or the
    // local context for persisted handles) — mirrors the Rust GoalHandle.
    const bool tracing = aorta_trace_is_enabled() != 0;
    const uint64_t wait_start_ns = tracing ? aorta_trace_monotonic_ns() : 0;
    auto result = WaitResultInner(timeout);
    if (tracing) {
      trace::record_stage_under(trace::StageId::kActionResultWait,
                                client_->action_name_.c_str(), wait_start_ns,
                                goal_trace_.valid ? goal_trace_.trace_id : nullptr,
                                goal_trace_.valid ? goal_trace_.span_id : 0);
    }
    // The goal-lifetime span is closed by the shared watcher deliver path
    // (MakeResultState), which fulfills this wait's promise.
    return result;
  }

  /// The goal's trace context (the goal_sent span identity) — valid only
  /// when tracing was enabled at SendGoal. Install it on a worker thread via
  /// `aorta::trace::ContextGuard` to attach follow-up spans to this goal's
  /// trace. Invalid for persisted handles.
  trace::Context TraceContext() const { return goal_trace_; }

 private:
  // Blocking wait built on the SAME event-driven watcher as SetResultCallback
  // (subscribe_result). The watcher fires exactly once (terminal status, or a
  // timeout/transport error) and fulfills the promise on the SDK delivery
  // thread; the caller blocks on the future. There is NO status-topic
  // subscriber + condvar on the node executor, so this cannot falsely time out
  // on a goal the server already succeeded — the defect of the old design.
  StatusOr<ActionGetResultResponse<Action>> WaitResultInner(
      std::optional<std::chrono::milliseconds> timeout) const {
    std::promise<StatusOr<ActionGetResultResponse<Action>>> promise;
    auto future = promise.get_future();

    std::vector<uint8_t> get_result_request =
        Action::EncodeGetResultRequest(goal_id_, goal_secret_);
    auto state = MakeResultState(
        [&promise](StatusOr<ActionGetResultResponse<Action>> result) {
          promise.set_value(std::move(result));
        });

    aorta_action_result_sub_t handle = 0;
    AORTA_CHECK_NOT_REENTRANT("GoalHandle::WaitResult");
    const aorta_result_t rc = aorta_action_client_subscribe_result(
        client_->handle_, goal_id_.c_str(), get_result_request.data(),
        get_result_request.size(),
        timeout.has_value() ? static_cast<uint64_t>(timeout->count())
                            : detail::kNoResultTimeoutMs,
        &detail::aorta_cpp_action_decode_status,
        &detail::aorta_cpp_action_on_result, state.get(), &handle);
    if (rc != AORTA_OK) {
      return StatusOr<ActionGetResultResponse<Action>>(Status::FromResult(rc));
    }
    // Local RAII: on return, frees the watcher (cancel + drain). The drain
    // waits for the in-flight deliver, which set the promise before the future
    // unblocked, so `promise` (captured by ref) is never touched after teardown.
    detail::ActionResultSubscription sub(handle, std::move(state));
    AORTA_CHECK_NOT_REENTRANT("GoalHandle::WaitResult");
    future.wait();
    return future.get();
  }

  // Builds the result-callback state (the decode hook + a deliver that decodes
  // the raw GetResultResponse, validates the epoch, closes the goal-lifetime
  // span on a terminal success, and forwards to `sink`) shared by
  // SetResultCallback (sink = the user callback) and WaitResult (sink =
  // fulfilling a promise).
  std::unique_ptr<detail::ActionResultCallbackState> MakeResultState(
      std::function<void(StatusOr<ActionGetResultResponse<Action>>)> sink) const {
    auto state = std::make_unique<detail::ActionResultCallbackState>();
    state->decode = [](uint8_t kind, const uint8_t* bytes,
                       size_t len) -> int8_t {
      return detail::DecodeStatusRecord<Action>(kind, bytes, len);
    };
    const std::string action_name = client_->action_name_;
    const std::string goal_id = goal_id_;
    const std::string expected_epoch = server_epoch_;
    // Lifetime-span state is captured BY VALUE (never `this`): the deliver
    // callback runs on the SDK delivery thread and the result subscription is
    // shared across handle copies, so it can outlive this GoalHandle.
    state->deliver = [sink = std::move(sink), action_name, goal_id,
                      expected_epoch, sent_mono_ns = sent_mono_ns_,
                      lifetime_recorded = lifetime_recorded_,
                      goal_trace = goal_trace_,
                      weak_client =
                          std::weak_ptr<ActionClient<Action>>(client_)](
                         const aorta_action_result_view_t* ev) {
      if (ev->error != AORTA_OK) {
        std::string msg = detail::CopyStringView(ev->message);
        sink(StatusOr<ActionGetResultResponse<Action>>(Status(
            ev->error, msg.empty() ? "action result wait failed" : msg)));
        return;
      }
      std::vector<uint8_t> payload = detail::CopyBytesView(&ev->result_payload);
      auto decoded = detail::DecodeGetResultResponse<Action>(payload);
      if (!decoded.ok()) {
        sink(StatusOr<ActionGetResultResponse<Action>>(decoded.status()));
        return;
      }
      // Same conclusive-observation hook as GetResultGoal — the
      // watcher path (WaitResult / SetResultCallback) must also end the
      // goal's feedback streams. Weak: this callback can outlive the client.
      if (auto client = weak_client.lock()) {
        client->NotifyResultObserved(goal_id, decoded.value());
      }
      auto validated = detail::ValidateGetResultResponse<Action>(
          action_name, goal_id, expected_epoch, std::move(decoded).value());
      // Close the goal-lifetime span on a successful terminal result, BEFORE
      // the sink so the span ends at the receipt instant rather than after
      // the user callback returns. Covers BOTH watcher consumers (WaitResult
      // and SetResultCallback); a validation rejection arrives here as an
      // error Status and does not close the span.
      if (validated.ok() && IsTerminalActionStatus(validated.value().status)) {
        RecordGoalLifetimeAt(action_name, sent_mono_ns, lifetime_recorded,
                             goal_trace);
      }
      sink(std::move(validated));
    };
    return state;
  }

 public:
  Status SetFeedbackCallback(
      std::function<void(const typename Action::Feedback&)> callback) {
    if (!client_) {
      return Status(AORTA_ERR_INVALID_ARGUMENT, "action client is null");
    }
    if (!callback) {
      feedback_subscription_.reset();
      return Status::Ok();
    }

    AORTA_CHECK_NOT_REENTRANT("GoalHandle::SetFeedbackCallback");
    auto subscription_or = detail::CreateActionFeedbackSubscription<Action>(
        client_->handle_, std::move(callback), goal_id_);
    if (!subscription_or.ok()) {
      return subscription_or.status();
    }
    feedback_subscription_ = std::move(subscription_or).value();
    return Status::Ok();
  }

  using ResultCallback =
      std::function<void(StatusOr<ActionGetResultResponse<Action>>)>;

  // Event-driven, one-shot result delivery.
  // The SDK watches the goal's status on the shared runtime — NO per-goal
  // thread, NO node-executor coupling — and fires `callback` EXACTLY ONCE on an
  // internal off-runtime delivery thread with the terminal result, or a non-ok
  // Status (timeout/transport). Non-blocking; feedback delivery is unaffected.
  // Pass a null callback (or destroy the handle) to cancel a pending wait; a
  // re-call replaces the prior wait. Do NOT destroy this handle from inside the
  // callback (it would self-destruct the callback state).
  Status SetResultCallback(
      ResultCallback callback,
      std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
    if (!client_) {
      return Status(AORTA_ERR_INVALID_ARGUMENT, "action client is null");
    }
    if (!callback) {
      result_subscription_.reset();
      return Status::Ok();
    }

    AORTA_CHECK_NOT_REENTRANT("GoalHandle::SetResultCallback");

    // Pre-encode the typed GetResultRequest the type-erased watcher replays on
    // terminal; it is copied across the FFI boundary, so it need not outlive
    // this call.
    std::vector<uint8_t> get_result_request =
        Action::EncodeGetResultRequest(goal_id_, goal_secret_);

    auto state = MakeResultState(std::move(callback));

    aorta_action_result_sub_t handle = 0;
    const aorta_result_t rc = aorta_action_client_subscribe_result(
        client_->handle_, goal_id_.c_str(), get_result_request.data(),
        get_result_request.size(),
        timeout.has_value() ? static_cast<uint64_t>(timeout->count())
                            : detail::kNoResultTimeoutMs,
        &detail::aorta_cpp_action_decode_status,
        &detail::aorta_cpp_action_on_result, state.get(), &handle);
    if (rc != AORTA_OK) {
      return Status::FromResult(rc);
    }
    result_subscription_ = std::make_shared<detail::ActionResultSubscription>(
        handle, std::move(state));
    return Status::Ok();
  }

  // Open a bounded, off-RX feedback stream for THIS goal, drained at the
  // consumer's pace via FeedbackStream::Recv/TryRecv — so a slow consumer
  // cannot stall the zenoh link the way an inline SetFeedbackCallback does.
  // `options` bound the queue capacity + overflow policy (with a dropped count).
  StatusOr<FeedbackStream<Action>> OpenFeedbackStream(
      FeedbackStreamOptions options = {}) const {
    if (!client_) {
      return StatusOr<FeedbackStream<Action>>(
          Status(AORTA_ERR_INVALID_ARGUMENT, "action client is null"));
    }
    // Delegates to the client-level goal-scoped opener (single source for
    // the ingest filter + the per-goal end-of-stream registration).
    return client_->OpenFeedbackStreamFor(goal_id_, options);
  }

 private:
  friend class ActionClient<Action>;

  GoalHandle(std::shared_ptr<ActionClient<Action>> client, std::string goal_id,
             std::string goal_secret, std::string server_epoch,
             trace::Context goal_trace = trace::Context{},
             uint64_t sent_mono_ns = 0)
      : client_(std::move(client)),
        goal_id_(std::move(goal_id)),
        goal_secret_(std::move(goal_secret)),
        server_epoch_(std::move(server_epoch)),
        goal_trace_(goal_trace),
        sent_mono_ns_(sent_mono_ns),
        lifetime_recorded_(std::make_shared<std::atomic<bool>>(false)) {}

  // Emit the client end-to-end goal-lifetime span (`action.goal_lifetime`):
  // SendGoal (request) -> this terminal result — the one client slice spanning
  // the whole action. Joined to the goal trace under the goal_sent span (which
  // nests at its head) and anchored at the EXACT send instant so the short
  // handshake nests inside it rather than clamping it to ~5 ms. Emitted from
  // EVERY result-delivery path — GetResult() (the polling path) via
  // RecordGoalLifetime(), and the shared watcher deliver (WaitResult /
  // SetResultCallback) via the static overload — at most once per goal: the
  // guard is shared across handle copies and paths. No-op when tracing was off
  // at SendGoal (`sent_mono_ns == 0`). Mirrors the Rust GoalHandle.
  static void RecordGoalLifetimeAt(
      const std::string& action_name, uint64_t sent_mono_ns,
      const std::shared_ptr<std::atomic<bool>>& lifetime_recorded,
      const trace::Context& goal_trace) {
    if (sent_mono_ns == 0 || !lifetime_recorded) {
      return;
    }
    bool expected = false;
    if (!lifetime_recorded->compare_exchange_strong(expected, true)) {
      return;  // already recorded (a repeat terminal poll, or a handle copy)
    }
    trace::record_stage_under(trace::StageId::kActionGoalLifetime,
                              action_name.c_str(), sent_mono_ns,
                              goal_trace.valid ? goal_trace.trace_id : nullptr,
                              goal_trace.valid ? goal_trace.span_id : 0);
  }

  void RecordGoalLifetime() const {
    RecordGoalLifetimeAt(client_->action_name_, sent_mono_ns_,
                         lifetime_recorded_, goal_trace_);
  }

  std::shared_ptr<ActionClient<Action>> client_;
  std::string goal_id_;
  std::string goal_secret_;
  std::string server_epoch_;
  // The goal_sent span identity (the goal's trace anchor on this client).
  // Invalid for persisted handles — they re-attach by goal_id only, matching
  // the Rust GoalHandle contract.
  trace::Context goal_trace_;
  // Monotonic SendGoal timestamp (the request instant) — the START of the
  // goal-lifetime span. 0 when tracing was off at send / for persisted handles.
  uint64_t sent_mono_ns_ = 0;
  // One-shot guard shared across handle copies so the goal-lifetime span is
  // emitted at most once per goal.
  std::shared_ptr<std::atomic<bool>> lifetime_recorded_;
  std::shared_ptr<detail::ActionFeedbackSubscription> feedback_subscription_;
  // Event-driven result subscription (SetResultCallback). Reset on destruction /
  // SetResultCallback(nullptr), which frees the FFI handle (cancel + drain).
  std::shared_ptr<detail::ActionResultSubscription> result_subscription_;
};

namespace detail {

inline aorta_bytes_view_t BorrowedBytesView(const std::vector<uint8_t>& payload,
                                            aorta_bytes_slice_t* slice) {
  if (slice == nullptr || payload.empty()) {
    return aorta_bytes_view_t{};
  }
  *slice = aorta_bytes_slice_t{payload.data(), payload.size()};
  return aorta_bytes_view_t{slice, 1, payload.size()};
}

inline std::vector<uint8_t> CopyBytesView(const aorta_bytes_view_t* view) {
  std::vector<uint8_t> out;
  if (view == nullptr || view->slice_count == 0 || view->slices == nullptr) {
    return out;
  }

  out.reserve(view->total_len);
  for (size_t i = 0; i < view->slice_count; ++i) {
    const auto& slice = view->slices[i];
    if (slice.data != nullptr && slice.len > 0) {
      out.insert(out.end(), slice.data, slice.data + slice.len);
    }
  }
  return out;
}

inline std::vector<uint8_t> CopyOwnedBytesAndFree(aorta_owned_bytes_t* bytes) {
  std::vector<uint8_t> out;
  if (bytes == nullptr) {
    return out;
  }

  if (bytes->view.slice_count > 0 && bytes->view.slices != nullptr) {
    out.reserve(bytes->view.total_len);
    for (size_t i = 0; i < bytes->view.slice_count; ++i) {
      const auto& slice = bytes->view.slices[i];
      if (slice.data != nullptr && slice.len > 0) {
        out.insert(out.end(), slice.data, slice.data + slice.len);
      }
    }
  }

  aorta_owned_bytes_free(bytes);
  return out;
}

struct OwnedActionBytesStorage {
  std::vector<uint8_t> payload;
  aorta_owned_bytes_slice_t slice{};
};

extern "C" inline void aorta_cpp_release_action_bytes(void* ctx, const uint8_t*,
                                                      size_t) {
  // Conditional try — see LogActionCallbackException above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto* storage = static_cast<OwnedActionBytesStorage*>(ctx);
    delete storage;
#if defined(__cpp_exceptions)
  } catch (...) {
    // delete is normally noexcept, but a custom destructor could throw under
    // pathological conditions. Never let it cross FFI back into Rust.
    LogActionCallbackException("action.release_bytes");
  }
#endif
}

inline aorta_result_t FillOwnedActionBytesView(
    std::vector<uint8_t> payload, aorta_owned_bytes_view_t* out) {
  if (out == nullptr) {
    return AORTA_ERR_INVALID_ARGUMENT;
  }
  // Conditional try — under -fno-exceptions a failed
  // allocation here terminates instead of throwing std::bad_alloc, so
  // there is no AORTA_ERR_INTERNAL to surface; see
  // LogActionCallbackException above for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto* storage = new OwnedActionBytesStorage();
    storage->payload = std::move(payload);
    storage->slice.data = storage->payload.empty() ? nullptr : storage->payload.data();
    storage->slice.len = storage->payload.size();
    storage->slice.release = &aorta_cpp_release_action_bytes;
    storage->slice.release_ctx = storage;
    out->slices = &storage->slice;
    out->slice_count = 1;
    out->total_len = storage->payload.size();
    return AORTA_OK;
#if defined(__cpp_exceptions)
  } catch (...) {
    // `new` and vector ops can throw std::bad_alloc. Surface as AORTA_ERR_INTERNAL
    // rather than unwinding across the FFI boundary into Rust (UB).
    return AORTA_ERR_INTERNAL;
  }
#endif
}

inline StatusOr<std::string> DecodeActionStringView(aorta_string_view_t view,
                                                    const char* field) {
  if (view.data == nullptr) {
    if (view.len == 0) {
      return std::string();
    }
    return StatusOr<std::string>(Status(AORTA_ERR_INVALID_ARGUMENT,
                                        std::string(field) +
                                            " data is null"));
  }
  return std::string(reinterpret_cast<const char*>(view.data), view.len);
}

template <typename Action>
inline aorta_result_t DecodeSendGoalRequestGoalId(
    const aorta_bytes_view_t* payload, aorta_string_view_t* out_goal_id) {
  if (out_goal_id == nullptr) {
    return AORTA_ERR_INVALID_ARGUMENT;
  }
  // Conditional try — see FillOwnedActionBytesView above
  // for the general rationale (this is also a decode/allocate path).
#if defined(__cpp_exceptions)
  try {
#endif
    auto bytes = CopyBytesView(payload);
    auto root_or = DecodeActionTable<typename Action::SendGoalRequest>(
        bytes, "send_goal request");
    if (!root_or.ok()) {
      return root_or.status().code();
    }
    const auto* goal_id = root_or.value()->goal_id();
    if (goal_id == nullptr) {
      return AORTA_ERR_INVALID_ARGUMENT;
    }
    thread_local std::string scratch_goal_id;
    scratch_goal_id = goal_id->str();
    *out_goal_id = aorta_string_view_t{
        reinterpret_cast<const uint8_t*>(scratch_goal_id.data()),
        scratch_goal_id.size()};
    return AORTA_OK;
#if defined(__cpp_exceptions)
  } catch (...) {
    return AORTA_ERR_INTERNAL;
  }
#endif
}

template <typename Request>
inline aorta_result_t DecodeGoalSecretRequestImpl(
    const aorta_bytes_view_t* payload, aorta_string_view_t* out_goal_id,
    aorta_string_view_t* out_goal_secret, const char* what) {
  if (out_goal_id == nullptr || out_goal_secret == nullptr) {
    return AORTA_ERR_INVALID_ARGUMENT;
  }
  // Conditional try — see FillOwnedActionBytesView above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto bytes = CopyBytesView(payload);
    auto root_or = DecodeActionTable<Request>(bytes, what);
    if (!root_or.ok()) {
      return root_or.status().code();
    }
    const auto* goal_id = root_or.value()->goal_id();
    const auto* goal_secret = root_or.value()->goal_secret();
    if (goal_id == nullptr || goal_secret == nullptr) {
      return AORTA_ERR_INVALID_ARGUMENT;
    }
    thread_local std::string scratch_goal_id;
    thread_local std::string scratch_goal_secret;
    scratch_goal_id = goal_id->str();
    scratch_goal_secret = goal_secret->str();
    *out_goal_id = aorta_string_view_t{
        reinterpret_cast<const uint8_t*>(scratch_goal_id.data()),
        scratch_goal_id.size()};
    *out_goal_secret = aorta_string_view_t{
        reinterpret_cast<const uint8_t*>(scratch_goal_secret.data()),
        scratch_goal_secret.size()};
    return AORTA_OK;
#if defined(__cpp_exceptions)
  } catch (...) {
    return AORTA_ERR_INTERNAL;
  }
#endif
}

template <typename Action>
inline aorta_result_t DecodeCancelGoalRequest(
    const aorta_bytes_view_t* payload, aorta_string_view_t* out_goal_id,
    aorta_string_view_t* out_goal_secret) {
  return DecodeGoalSecretRequestImpl<typename Action::CancelGoalRequest>(
      payload, out_goal_id, out_goal_secret, "cancel request");
}

template <typename Action>
inline aorta_result_t DecodeGetResultRequest(
    const aorta_bytes_view_t* payload, aorta_string_view_t* out_goal_id,
    aorta_string_view_t* out_goal_secret) {
  return DecodeGoalSecretRequestImpl<typename Action::GetResultRequest>(
      payload, out_goal_id, out_goal_secret, "get_result request");
}

inline ::aorta::action::common::ActionStatus ToWireActionStatus(int8_t status) {
  return static_cast<::aorta::action::common::ActionStatus>(status);
}

inline ::aorta::action::common::ActionReplyCode ToWireActionReplyCode(
    int16_t code) {
  return static_cast<::aorta::action::common::ActionReplyCode>(code);
}

template <typename Action>
inline aorta_result_t EncodeSendGoalResponse(
    uint8_t accepted, aorta_string_view_t goal_id, aorta_string_view_t goal_secret,
    int16_t code, int8_t status, aorta_string_view_t message,
    aorta_string_view_t server_epoch, aorta_owned_bytes_view_t* out) {
  // Conditional try — see FillOwnedActionBytesView above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto goal_id_or = DecodeActionStringView(goal_id, "goal_id");
    auto goal_secret_or = DecodeActionStringView(goal_secret, "goal_secret");
    auto message_or = DecodeActionStringView(message, "message");
    auto server_epoch_or = DecodeActionStringView(server_epoch, "server_epoch");
    if (!goal_id_or.ok()) return goal_id_or.status().code();
    if (!goal_secret_or.ok()) return goal_secret_or.status().code();
    if (!message_or.ok()) return message_or.status().code();
    if (!server_epoch_or.ok()) return server_epoch_or.status().code();
    return FillOwnedActionBytesView(
        Action::EncodeSendGoalResponse(
            accepted != 0, goal_id_or.value(), goal_secret_or.value(),
            ToWireActionReplyCode(code), ToWireActionStatus(status),
            message_or.value(), server_epoch_or.value()),
        out);
#if defined(__cpp_exceptions)
  } catch (...) {
    return AORTA_ERR_INTERNAL;
  }
#endif
}

template <typename Action>
inline aorta_result_t EncodeCancelGoalResponse(
    uint8_t accepted, aorta_string_view_t goal_id, int16_t code, int8_t status,
    aorta_string_view_t message, aorta_owned_bytes_view_t* out) {
  // Conditional try — see FillOwnedActionBytesView above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto goal_id_or = DecodeActionStringView(goal_id, "goal_id");
    auto message_or = DecodeActionStringView(message, "message");
    if (!goal_id_or.ok()) return goal_id_or.status().code();
    if (!message_or.ok()) return message_or.status().code();
    return FillOwnedActionBytesView(
        Action::EncodeCancelGoalResponse(
            accepted != 0, goal_id_or.value(), ToWireActionReplyCode(code),
            ToWireActionStatus(status), message_or.value()),
        out);
#if defined(__cpp_exceptions)
  } catch (...) {
    return AORTA_ERR_INTERNAL;
  }
#endif
}

template <typename Action>
inline aorta_result_t EncodePublicStatus(
    aorta_string_view_t goal_id, int8_t status, int16_t code,
    uint64_t accepted_stamp_ns, uint64_t started_stamp_ns,
    uint64_t finished_stamp_ns, uint64_t last_feedback_seq,
    aorta_string_view_t server_epoch, aorta_owned_bytes_view_t* out) {
  // Conditional try — see FillOwnedActionBytesView above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto goal_id_or = DecodeActionStringView(goal_id, "goal_id");
    auto server_epoch_or = DecodeActionStringView(server_epoch, "server_epoch");
    if (!goal_id_or.ok()) return goal_id_or.status().code();
    if (!server_epoch_or.ok()) return server_epoch_or.status().code();
    return FillOwnedActionBytesView(
        Action::EncodePublicStatus(goal_id_or.value(), ToWireActionStatus(status),
                                   ToWireActionReplyCode(code), accepted_stamp_ns,
                                   started_stamp_ns, finished_stamp_ns,
                                   last_feedback_seq, server_epoch_or.value()),
        out);
#if defined(__cpp_exceptions)
  } catch (...) {
    return AORTA_ERR_INTERNAL;
  }
#endif
}

template <typename Action>
inline aorta_result_t EncodeFeedback(
    aorta_string_view_t goal_id, uint64_t seq, uint64_t stamp_ns, int8_t status,
    const aorta_bytes_view_t* feedback_data, aorta_string_view_t message,
    aorta_owned_bytes_view_t* out) {
  // Conditional try — see FillOwnedActionBytesView above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto goal_id_or = DecodeActionStringView(goal_id, "goal_id");
    auto message_or = DecodeActionStringView(message, "message");
    if (!goal_id_or.ok()) return goal_id_or.status().code();
    if (!message_or.ok()) return message_or.status().code();

    typename Action::FeedbackDataMsg data{};
    auto bytes = CopyBytesView(feedback_data);
    if (!bytes.empty()) {
      auto root_or = DecodeActionTable<typename Action::FeedbackData>(
          bytes, "feedback_data");
      if (!root_or.ok()) {
        return root_or.status().code();
      }
      data = Action::CopyFeedbackData(*root_or.value());
    }

    return FillOwnedActionBytesView(
        Action::EncodeFeedback(goal_id_or.value(), seq, stamp_ns,
                               ToWireActionStatus(status), data,
                               message_or.value()),
        out);
#if defined(__cpp_exceptions)
  } catch (...) {
    return AORTA_ERR_INTERNAL;
  }
#endif
}

template <typename Action>
inline aorta_result_t EncodeGetResultResponse(
    aorta_string_view_t goal_id, int8_t status, uint64_t accepted_stamp_ns,
    uint64_t started_stamp_ns, uint64_t finished_stamp_ns,
    uint64_t last_feedback_seq, const aorta_bytes_view_t* feedback_snapshot,
    const aorta_bytes_view_t* result_payload, int16_t code,
    aorta_string_view_t message, aorta_string_view_t server_epoch,
    aorta_owned_bytes_view_t* out) {
  // Conditional try — see FillOwnedActionBytesView above
  // for the general rationale.
#if defined(__cpp_exceptions)
  try {
#endif
    auto goal_id_or = DecodeActionStringView(goal_id, "goal_id");
    auto message_or = DecodeActionStringView(message, "message");
    auto server_epoch_or = DecodeActionStringView(server_epoch, "server_epoch");
    if (!goal_id_or.ok()) return goal_id_or.status().code();
    if (!message_or.ok()) return message_or.status().code();
    if (!server_epoch_or.ok()) return server_epoch_or.status().code();

    typename Action::FeedbackDataMsg feedback_msg{};
    auto feedback_bytes = CopyBytesView(feedback_snapshot);
    if (!feedback_bytes.empty()) {
      auto feedback_or = DecodeActionTable<typename Action::FeedbackData>(
          feedback_bytes, "feedback_snapshot");
      if (!feedback_or.ok()) {
        return feedback_or.status().code();
      }
      feedback_msg = Action::CopyFeedbackData(*feedback_or.value());
    }

    std::optional<typename Action::ResultMsg> result_msg;
    auto result_bytes = CopyBytesView(result_payload);
    if (!result_bytes.empty()) {
      auto result_or =
          DecodeActionTable<typename Action::Result>(result_bytes, "result");
      if (!result_or.ok()) {
        return result_or.status().code();
      }
      result_msg = Action::CopyResult(*result_or.value());
    }

    return FillOwnedActionBytesView(
        Action::EncodeGetResultResponse(
            goal_id_or.value(), ToWireActionStatus(status), accepted_stamp_ns,
            started_stamp_ns, finished_stamp_ns, last_feedback_seq,
            feedback_msg, result_msg, ToWireActionReplyCode(code),
            message_or.value(), server_epoch_or.value()),
        out);
#if defined(__cpp_exceptions)
  } catch (...) {
    return AORTA_ERR_INTERNAL;
  }
#endif
}

template <typename Action>
class ActionServerCallbackState final : public ActionServerCallbackStateBase {
 public:
  using Callback = std::function<void(RetainedActionGoal<Action>,
                                      ActionExecutionContext<Action>)>;
  using CancelCallback = std::function<void(const std::string& goal_id)>;

  explicit ActionServerCallbackState(Callback cb,
                                     CancelCallback on_cancel = nullptr)
      : cb_(std::move(cb)), on_cancel_(std::move(on_cancel)) {}

  void OnCancelRequested(const std::string& goal_id) override {
    if (on_cancel_) {
      on_cancel_(goal_id);
    }
  }

  void OnExecute(const aorta_bytes_view_t* payload,
                 aorta_action_context_t ctx) override {
    if (!cb_) {
      return;
    }
    auto bytes = CopyBytesView(payload);
    RetainedActionGoal<Action> goal(std::move(bytes));
    ActionExecutionContext<Action> exec_ctx(ctx, goal.goal_id());
    if (!goal.valid()) {
      auto _ = exec_ctx.Abort(ActionReplyCode::kHandlerError,
                              "invalid action send_goal payload");
      return;
    }
    cb_(std::move(goal), std::move(exec_ctx));
  }

 private:
  Callback cb_;
  CancelCallback on_cancel_;
};

inline StatusOr<ActionStatus> DecodeActionStatus(int8_t value) {
  switch (value) {
    case 0:
      return ActionStatus::kUnknown;
    case 1:
      return ActionStatus::kAccepted;
    case 2:
      return ActionStatus::kExecuting;
    case 3:
      return ActionStatus::kCanceling;
    case 4:
      return ActionStatus::kSucceeded;
    case 5:
      return ActionStatus::kAborted;
    case 6:
      return ActionStatus::kCanceled;
    default:
      return StatusOr<ActionStatus>(Status(
          AORTA_ERR_INVALID_ARGUMENT,
          "invalid action status value"));
  }
}

inline StatusOr<ActionReplyCode> DecodeActionReplyCode(int16_t value) {
  switch (value) {
    case 0:
      return ActionReplyCode::kNone;
    case 1:
      return ActionReplyCode::kOk;
    case 2:
      return ActionReplyCode::kDuplicateAccepted;
    case 100:
      return ActionReplyCode::kSucceeded;
    case 101:
      return ActionReplyCode::kCanceled;
    case 200:
      return ActionReplyCode::kBusy;
    case 201:
      return ActionReplyCode::kPayloadConflict;
    case 202:
      return ActionReplyCode::kUnauthorized;
    case 203:
      return ActionReplyCode::kNotFound;
    case 204:
      return ActionReplyCode::kAlreadyTerminal;
    case 205:
      return ActionReplyCode::kGoalExpired;
    case 300:
      return ActionReplyCode::kHandlerError;
    case 301:
      return ActionReplyCode::kHandlerPanic;
    case 302:
      return ActionReplyCode::kHandlerCompletedWithoutResult;
    case 303:
      return ActionReplyCode::kContextDropped;
    case 304:
      return ActionReplyCode::kServerCrash;
    case 400:
      return ActionReplyCode::kConflict;
    default:
      return StatusOr<ActionReplyCode>(Status(
          AORTA_ERR_INVALID_ARGUMENT,
          "invalid action reply code value"));
  }
}

inline bool ServerEpochMismatch(const std::string& expected,
                                const std::string& observed) {
  return !expected.empty() && expected != observed;
}

inline Status GoalEpochMismatchStatus(const std::string& action_name,
                                      const std::string& goal_id,
                                      const std::string& expected,
                                      const std::string& observed) {
  return Status(
      AORTA_ERR_NOT_FOUND,
      "action '" + action_name + "' goal '" + goal_id +
          "' belongs to previous server epoch '" + expected +
          "', current status epoch is '" + observed + "'");
}

inline StatusOr<std::optional<std::chrono::milliseconds>>
RemainingWaitResultTimeout(
    const std::optional<std::chrono::steady_clock::time_point>& deadline,
    const std::string& goal_id) {
  if (!deadline.has_value()) {
    return std::optional<std::chrono::milliseconds>();
  }

  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline.value()) {
    return StatusOr<std::optional<std::chrono::milliseconds>>(Status(
        AORTA_ERR_TIMEOUT,
        "timed out waiting for terminal action result '" + goal_id + "'"));
  }

  auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline.value() - now);
  if (remaining <= std::chrono::milliseconds::zero()) {
    remaining = std::chrono::milliseconds(1);
  }
  return std::optional<std::chrono::milliseconds>(remaining);
}

inline StatusOr<std::string> GoalIdToStatusKeySegment(
    const std::string& goal_id) {
  std::string out;
  out.reserve(goal_id.size());
  for (unsigned char ch : goal_id) {
    if (ch == '-') {
      continue;
    }
    const char lower = static_cast<char>(std::tolower(ch));
    if ((lower < '0' || lower > '9') && (lower < 'a' || lower > 'f')) {
      return StatusOr<std::string>(Status(
          AORTA_ERR_INVALID_ARGUMENT,
          "goal_id must be a UUID-compatible hex string"));
    }
    out.push_back(lower);
  }
  if (out.size() != 32) {
    return StatusOr<std::string>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "goal_id must contain 32 hexadecimal digits"));
  }
  return out;
}

inline StatusOr<std::string> BuildActionStatusTopic(
    const std::string& group, const std::string& action_name,
    const std::string& goal_id) {
  auto goal_key = GoalIdToStatusKeySegment(goal_id);
  if (!goal_key.ok()) {
    return StatusOr<std::string>(goal_key.status());
  }
  return "aorta/" + group + "/act/status/" + action_name + "/" +
         goal_key.value();
}

inline aorta_result_t MapReplyCodeToStatus(ActionReplyCode code) {
  switch (code) {
    case ActionReplyCode::kBusy:
      return AORTA_ERR_BUSY;
    case ActionReplyCode::kConflict:
      return AORTA_ERR_CONCURRENT_CONFLICT;
    case ActionReplyCode::kNotFound:
    case ActionReplyCode::kGoalExpired:
      return AORTA_ERR_NOT_FOUND;
    case ActionReplyCode::kNone:
    case ActionReplyCode::kOk:
    case ActionReplyCode::kDuplicateAccepted:
    case ActionReplyCode::kSucceeded:
    case ActionReplyCode::kCanceled:
    case ActionReplyCode::kPayloadConflict:
    case ActionReplyCode::kUnauthorized:
    case ActionReplyCode::kAlreadyTerminal:
    case ActionReplyCode::kHandlerError:
    case ActionReplyCode::kHandlerPanic:
    case ActionReplyCode::kHandlerCompletedWithoutResult:
    case ActionReplyCode::kContextDropped:
    case ActionReplyCode::kServerCrash:
      return AORTA_ERR_INVALID_ARGUMENT;
  }
  return AORTA_ERR_INVALID_ARGUMENT;
}

template <typename Table>
StatusOr<const Table*> DecodeActionTable(const std::vector<uint8_t>& payload,
                                         const char* what) {
  const uint8_t* data = payload.empty() ? nullptr : payload.data();
  if (!TypedSubscribeTraits<Table>::Verify(data, payload.size())) {
    return StatusOr<const Table*>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        std::string(what) + " failed schema verification"));
  }
  const Table* root = TypedSubscribeTraits<Table>::GetRoot(data);
  if (root == nullptr) {
    return StatusOr<const Table*>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        std::string(what) + " decode returned null"));
  }
  return root;
}

inline StatusOr<ActionInfo> DecodeActionInfoPayload(
    const std::vector<uint8_t>& payload) {
  const uint8_t* data = payload.empty() ? nullptr : payload.data();
  if (data == nullptr || payload.empty()) {
    return StatusOr<ActionInfo>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "action info payload is empty"));
  }
  flatbuffers::Verifier verifier(data, payload.size());
  if (!::aorta::action::common::VerifyActionInfoBuffer(verifier)) {
    return StatusOr<ActionInfo>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "action info failed schema verification"));
  }
  const auto* root = ::aorta::action::common::GetActionInfo(data);
  if (root == nullptr) {
    return StatusOr<ActionInfo>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "action info decode returned null"));
  }

  ActionInfo info;
  if (const auto* value = root->action_name()) {
    info.action_name = value->str();
  }
  if (const auto* value = root->schema_hash_hex()) {
    info.schema_hash_hex = value->str();
  }
  info.supports_cancel = root->supports_cancel();
  info.max_concurrency = root->max_concurrency();
  info.result_ttl_ms = root->result_ttl_ms();
  info.dedup_ttl_ms = root->dedup_ttl_ms();
  if (const auto* value = root->server_node_name()) {
    info.server_node_name = value->str();
  }
  if (const auto* value = root->server_epoch()) {
    info.server_epoch = value->str();
  }

  if (info.action_name.empty() || info.schema_hash_hex.empty() ||
      info.server_node_name.empty() || info.server_epoch.empty()) {
    return StatusOr<ActionInfo>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "action info missing required fields"));
  }
  return info;
}

template <typename Action>
StatusOr<ActionSendGoalResponse> DecodeSendGoalResponse(
    const std::vector<uint8_t>& payload) {
  auto root_or = DecodeActionTable<typename Action::SendGoalResponse>(
      payload, "action send_goal response");
  if (!root_or.ok()) {
    return StatusOr<ActionSendGoalResponse>(root_or.status());
  }
  const auto* root = root_or.value();
  auto code_or = DecodeActionReplyCode(static_cast<int16_t>(root->code()));
  if (!code_or.ok()) {
    return StatusOr<ActionSendGoalResponse>(code_or.status());
  }
  auto status_or = DecodeActionStatus(static_cast<int8_t>(root->status()));
  if (!status_or.ok()) {
    return StatusOr<ActionSendGoalResponse>(status_or.status());
  }

  ActionSendGoalResponse out;
  out.accepted = root->accepted();
  if (const auto* value = root->goal_id()) {
    out.goal_id = value->str();
  }
  if (const auto* value = root->goal_secret()) {
    out.goal_secret = value->str();
  }
  out.code = code_or.value();
  out.status = status_or.value();
  if (const auto* value = root->message()) {
    out.message = value->str();
  }
  if (const auto* value = root->server_epoch()) {
    out.server_epoch = value->str();
  }
  return out;
}

template <typename Action>
StatusOr<ActionCancelGoalResponse> DecodeCancelGoalResponse(
    const std::vector<uint8_t>& payload) {
  auto root_or = DecodeActionTable<typename Action::CancelGoalResponse>(
      payload, "action cancel response");
  if (!root_or.ok()) {
    return StatusOr<ActionCancelGoalResponse>(root_or.status());
  }
  const auto* root = root_or.value();
  auto code_or = DecodeActionReplyCode(static_cast<int16_t>(root->code()));
  if (!code_or.ok()) {
    return StatusOr<ActionCancelGoalResponse>(code_or.status());
  }
  auto status_or = DecodeActionStatus(static_cast<int8_t>(root->status()));
  if (!status_or.ok()) {
    return StatusOr<ActionCancelGoalResponse>(status_or.status());
  }

  ActionCancelGoalResponse out;
  out.accepted = root->accepted();
  if (const auto* value = root->goal_id()) {
    out.goal_id = value->str();
  }
  out.code = code_or.value();
  out.status = status_or.value();
  if (const auto* value = root->message()) {
    out.message = value->str();
  }
  return out;
}

template <typename Action>
StatusOr<ActionPublicStatus> DecodePublicStatus(
    const std::vector<uint8_t>& payload) {
  auto root_or = DecodeActionTable<typename Action::PublicStatus>(
      payload, "action public status");
  if (!root_or.ok()) {
    return StatusOr<ActionPublicStatus>(root_or.status());
  }
  const auto* root = root_or.value();
  auto code_or = DecodeActionReplyCode(static_cast<int16_t>(root->code()));
  if (!code_or.ok()) {
    return StatusOr<ActionPublicStatus>(code_or.status());
  }
  auto status_or = DecodeActionStatus(static_cast<int8_t>(root->status()));
  if (!status_or.ok()) {
    return StatusOr<ActionPublicStatus>(status_or.status());
  }

  ActionPublicStatus out;
  if (const auto* value = root->goal_id()) {
    out.goal_id = value->str();
  }
  out.status = status_or.value();
  out.code = code_or.value();
  out.accepted_stamp_ns = root->accepted_stamp_ns();
  out.started_stamp_ns = root->started_stamp_ns();
  out.finished_stamp_ns = root->finished_stamp_ns();
  out.last_feedback_seq = root->last_feedback_seq();
  if (const auto* value = root->server_epoch()) {
    out.server_epoch = value->str();
  }
  return out;
}

template <typename Action>
StatusOr<ActionGetResultResponse<Action>> DecodeGetResultResponse(
    const std::vector<uint8_t>& payload) {
  auto root_or = DecodeActionTable<typename Action::GetResultResponse>(
      payload, "action get_result response");
  if (!root_or.ok()) {
    return StatusOr<ActionGetResultResponse<Action>>(root_or.status());
  }
  const auto* root = root_or.value();
  auto code_or = DecodeActionReplyCode(static_cast<int16_t>(root->code()));
  if (!code_or.ok()) {
    return StatusOr<ActionGetResultResponse<Action>>(code_or.status());
  }
  auto status_or = DecodeActionStatus(static_cast<int8_t>(root->status()));
  if (!status_or.ok()) {
    return StatusOr<ActionGetResultResponse<Action>>(status_or.status());
  }

  ActionGetResultResponse<Action> out;
  if (const auto* value = root->goal_id()) {
    out.goal_id = value->str();
  }
  out.status = status_or.value();
  out.accepted_stamp_ns = root->accepted_stamp_ns();
  out.started_stamp_ns = root->started_stamp_ns();
  out.finished_stamp_ns = root->finished_stamp_ns();
  out.last_feedback_seq = root->last_feedback_seq();
  if (const auto* value = root->feedback_snapshot()) {
    out.feedback_snapshot = Action::CopyFeedbackData(*value);
  }
  if (const auto* value = root->result()) {
    out.result = Action::CopyResult(*value);
  }
  out.code = code_or.value();
  if (const auto* value = root->message()) {
    out.message = value->str();
  }
  if (const auto* value = root->server_epoch()) {
    out.server_epoch = value->str();
  }
  return out;
}

inline Status SendGoalRejectedStatus(const std::string& action_name,
                                     const ActionSendGoalResponse& response) {
  return Status(
      MapReplyCodeToStatus(response.code),
      "action '" + action_name + "' send_goal rejected: message=" +
          response.message);
}

inline Status CancelRejectedStatus(const std::string& action_name,
                                   const ActionCancelGoalResponse& response) {
  return Status(
      response.code == ActionReplyCode::kNotFound ? AORTA_ERR_NOT_FOUND
                                                  : AORTA_ERR_INVALID_ARGUMENT,
      "action '" + action_name + "' cancel rejected: message=" +
          response.message);
}

template <typename Action>
StatusOr<ActionGetResultResponse<Action>> ValidateGetResultResponse(
    const std::string& action_name, const std::string& goal_id,
    const std::string& expected_server_epoch,
    ActionGetResultResponse<Action> response) {
  if (response.goal_id != goal_id) {
    return StatusOr<ActionGetResultResponse<Action>>(Status(
        AORTA_ERR_INTERNAL,
        "action get_result reply goal_id mismatch"));
  }
  if (ServerEpochMismatch(expected_server_epoch, response.server_epoch)) {
    return StatusOr<ActionGetResultResponse<Action>>(Status(
        AORTA_ERR_NOT_FOUND,
        "action goal belongs to a previous server epoch"));
  }
  switch (response.code) {
    case ActionReplyCode::kUnauthorized:
      return StatusOr<ActionGetResultResponse<Action>>(Status(
          AORTA_ERR_INVALID_ARGUMENT,
          "action '" + action_name + "' get_result rejected: message=" +
              response.message));
    case ActionReplyCode::kNotFound:
    case ActionReplyCode::kGoalExpired:
      return StatusOr<ActionGetResultResponse<Action>>(Status(
          AORTA_ERR_NOT_FOUND,
          "action '" + action_name + "' get_result rejected: message=" +
              response.message));
    case ActionReplyCode::kNone:
    case ActionReplyCode::kOk:
    case ActionReplyCode::kDuplicateAccepted:
    case ActionReplyCode::kSucceeded:
    case ActionReplyCode::kCanceled:
    case ActionReplyCode::kBusy:
    case ActionReplyCode::kPayloadConflict:
    case ActionReplyCode::kAlreadyTerminal:
    case ActionReplyCode::kHandlerError:
    case ActionReplyCode::kHandlerPanic:
    case ActionReplyCode::kHandlerCompletedWithoutResult:
    case ActionReplyCode::kContextDropped:
    case ActionReplyCode::kServerCrash:
    case ActionReplyCode::kConflict:
      return response;
  }
  return response;
}

}  // namespace detail

template <typename Action>
template <typename Handler>
StatusOr<std::shared_ptr<ActionServer<Action>>> ActionServer<Action>::Serve(
    const std::shared_ptr<Node>& node, const std::string& action_name,
    ActionServerOptions options, Handler&& handler) {
  return Serve(node, action_name, std::move(options),
               std::forward<Handler>(handler), nullptr);
}

template <typename Action>
template <typename Handler, typename CancelHandler>
StatusOr<std::shared_ptr<ActionServer<Action>>> ActionServer<Action>::Serve(
    const std::shared_ptr<Node>& node, const std::string& action_name,
    ActionServerOptions options, Handler&& handler,
    CancelHandler&& on_cancel_requested) {
  if (node == nullptr) {
    return StatusOr<std::shared_ptr<ActionServer<Action>>>(
        Status(AORTA_ERR_INVALID_ARGUMENT, "action node is null"));
  }

  const auto* schema_bfbs = SchemaMeta<typename Action::Goal>::SchemaBfbs();
  const auto schema_bfbs_len = SchemaMeta<typename Action::Goal>::SchemaBfbsSize();
  if (schema_bfbs == nullptr || schema_bfbs_len == 0) {
    return StatusOr<std::shared_ptr<ActionServer<Action>>>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "typed action requires action schema bfbs"));
  }

  using CallbackState = detail::ActionServerCallbackState<Action>;
  typename CallbackState::CancelCallback cancel_cb(
      std::forward<CancelHandler>(on_cancel_requested));
  if (cancel_cb && !options.supports_cancel) {
    return StatusOr<std::shared_ptr<ActionServer<Action>>>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "on_cancel_requested requires ActionServerOptions::supports_cancel"));
  }
  const bool has_cancel_cb = static_cast<bool>(cancel_cb);
  auto state = std::make_shared<CallbackState>(
      typename CallbackState::Callback(std::forward<Handler>(handler)),
      std::move(cancel_cb));

  aorta_action_server_codec_t codec{};
  codec.decode_send_goal_request_goal_id =
      &detail::DecodeSendGoalRequestGoalId<Action>;
  codec.decode_cancel_goal_request = &detail::DecodeCancelGoalRequest<Action>;
  codec.decode_get_result_request = &detail::DecodeGetResultRequest<Action>;
  codec.encode_send_goal_response = &detail::EncodeSendGoalResponse<Action>;
  codec.encode_cancel_goal_response = &detail::EncodeCancelGoalResponse<Action>;
  codec.encode_public_status = &detail::EncodePublicStatus<Action>;
  codec.encode_feedback = &detail::EncodeFeedback<Action>;
  codec.encode_get_result_response =
      &detail::EncodeGetResultResponse<Action>;

  auto ffi_options = options.ToFfi();
  aorta_action_server_t handle = 0;
  auto normalized_or = detail::NormalizeActionName(action_name);
  if (!normalized_or.ok()) {
    return StatusOr<std::shared_ptr<ActionServer<Action>>>(
        normalized_or.status());
  }
  const std::string& normalized = normalized_or.value();

  aorta_action_server_callbacks_t callbacks{};
  callbacks.execute = &detail::aorta_cpp_on_action_execute;
  callbacks.cancel_requested =
      has_cancel_cb ? &detail::aorta_cpp_on_action_cancel_requested : nullptr;
  callbacks.user_data = state.get();
  callbacks.user_data_release = &detail::aorta_cpp_on_action_server_release;
  AORTA_CHECK_NOT_REENTRANT("ActionServer::Serve");
  // From here the library owns a reference; it drops it through
  // `user_data_release`, on success and on failure alike.
  state->library_ref = state;
  const auto result = aorta_action_server_new_with_callbacks(
      node->handle(), normalized.c_str(), schema_bfbs, schema_bfbs_len,
      &ffi_options, &codec, &callbacks, &handle);
  if (result != AORTA_OK) {
    return StatusOr<std::shared_ptr<ActionServer<Action>>>(
        Status::FromResult(result));
  }

  return std::shared_ptr<ActionServer<Action>>(new ActionServer<Action>(
      handle, action_name, std::move(options), std::move(state),
      node));
}

template <typename Action>
StatusOr<std::shared_ptr<ActionClient<Action>>> Node::CreateActionClient(
    const std::string& action_name, ActionClientOptions options) {
  auto normalized_or = detail::NormalizeActionName(action_name);
  if (!normalized_or.ok()) {
    return StatusOr<std::shared_ptr<ActionClient<Action>>>(
        normalized_or.status());
  }
  const std::string& normalized = normalized_or.value();
  aorta_action_client_t handle = 0;
  auto ffi_options = options.ToFfi();
  const auto result = aorta_action_client_new_with_options(
      handle_, normalized.c_str(), &ffi_options, &handle);
  if (result != AORTA_OK) {
    return StatusOr<std::shared_ptr<ActionClient<Action>>>(
        Status::FromResult(result));
  }
  return std::shared_ptr<ActionClient<Action>>(
      new ActionClient<Action>(handle, normalized, std::move(options),
                               shared_from_this()));
}

}  // namespace aorta
