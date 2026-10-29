// Flow — awaitable ACTION verbs: `co_await flow::send_goal{...}`,
// `co_await flow::wait_result{...}`, `co_await flow::cancel_goal{...}`,
// plus the RAII `flow::GoalGuard` (cancel-on-drop, opt-in).
//
// All three verbs ride the `flow::call` shape over the one-shot action ctx
// bridge (`aorta_action_client_*_ctx`): encode on construction, register a
// single completion callback, park; the callback copies (code, payload) into
// the frame and posts the resume onto `executor` (refused post =
// teardown delivery, unwinds `cancelled_unwind`). The scope both gates entry
// (already-cancelled = refused) and cancels a parked wait mid-flight; on
// send_goal the scope's remaining budget is stamped on the wire as the
// goal's deadline budget (the server seeds the goal scope from it).
//
// The verbs speak the ONE Outcome<T> vocabulary — scope
// cancellation/deadline as `Stopped::Cancelled` with the exact reason
// (via the completion record), action rejections and transport faults
// as `Stopped::Fault{original code, detail}` —
// decoded with the SAME helpers the blocking client uses. (The old
// StatusOr shape's rationale is gone: Fault now carries code AND
// message.) Exceptions-default like `recv()`: under -fno-exceptions a
// teardown delivery aborts loudly.
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "aorta/action.h"
#include "aorta/flow.hpp"

namespace aorta::flow {

/// An ACCEPTED goal's identity — the coroutine-plane counterpart of
/// `GoalHandle` (a plain value: copyable, no destructor behavior; pair with
/// `GoalGuard` for cancel-on-drop). The shared_ptr keeps the client alive
/// across parked waits.
template <typename Action>
struct AcceptedGoal {
  std::shared_ptr<aorta::ActionClient<Action>> client;
  std::string goal_id;
  std::string goal_secret;
  std::string server_epoch;
};

namespace detail {

/// Common frame-resident machinery for the three action verbs (the
/// `flow::call` protocol): one-shot completion → copy out → post_resume.
class action_ctx_awaiter {
 public:
  bool await_ready() const noexcept { return false; }

 protected:
  action_ctx_awaiter(Scope scope, uint64_t executor,
                     std::vector<uint8_t> request)
      : scope_(std::move(scope)),
        executor_(executor),
        request_(std::move(request)) {}

  /// Build the borrowed request view (valid for the registration call —
  /// the ctx bridge copies the bytes before returning).
  aorta_bytes_view_t request_view() {
    slice_ = aorta_bytes_slice_t{request_.data(), request_.size()};
    return aorta_bytes_view_t{&slice_, 1, request_.size()};
  }

  /// Shared await_suspend tail: entry refusal resolves inline; OK parks.
  bool finish_suspend(aorta_result_t rc) {
    if (rc != AORTA_OK) {
      code_ = rc;
      // Same-thread, right after the FFI returned: aorta_last_error()
      // is still THIS refusal's detail — capture it into the stop
      // channel (re-gate should-fix: outcome_status()'s canonical text
      // dropped e.g. "flow scope not found").
      stop_.emplace(Stopped::Fault(Status::FromResult(rc)));
      return false;  // resolve inline on this live thread (RPC-layer error)
    }
    return true;  // FRAME PARKED — the callback owns the next touch
  }

  template <typename P>
  void stamp(std::coroutine_handle<P> h) {
    if constexpr (detail::FlowPromise<P>) {
      // The pre-existing priority read, widened to the whole frame
      // context — an AMBIENT verb binds scope + exec from the
      // promise here; explicit arguments stay untouched.
      const auto& ctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      priority_ = ctx.priority;
      if (ambient_) {
        scope_ = ctx.scope;
        executor_ = ctx.exec;
        context_bound_ = true;
      }
      // Bound frames' completions re-enter their
      // domain (fctx_); the EXPLICIT forms compare their executor
      // argument against the domain runtime's fallback executor at this
      // stamp — foreign is recorded here and refused by
      // refuse_serial_foreign() before any goal machinery arms.
      if (detail::SerialDomainState* sst = detail::serial_binding_of(&ctx)) {
        if (!ambient_ && executor_ != 0 && sst->rt != nullptr &&
            executor_ != sst->rt->fallback_executor()) {
          serial_foreign_ = true;
        } else {
          fctx_ = &ctx;
        }
      }
    } else if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      priority_ = h.promise().priority;
    }
    continuation_ = h;
  }

  /// Ambient misuse guard: an ambient action verb awaited from a
  /// NON-flow coroutine has no context to bind — refuse INVALID_ARGUMENT
  /// inline (RPC-layer, the entry-refusal shape) and log at the refusal
  /// site. Returns true when refused; call AFTER stamp().
  bool refuse_unbound_ambient(const char* surface) {
    if (!ambient_ || context_bound_) {
      return false;
    }
    detail::log_refused_await(surface, AORTA_ERR_INVALID_ARGUMENT);
    code_ = AORTA_ERR_INVALID_ARGUMENT;
    stop_.emplace(Stopped::Fault(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        std::string(surface) + ": ambient verb awaited from a non-flow "
                               "coroutine — use the explicit overload")));
    return true;
  }

  /// The foreign-executor stamp recorded by stamp() —
  /// refuse INLINE before any goal registration exists. Returns true
  /// when refused; call right after refuse_unbound_ambient().
  bool refuse_serial_foreign(const char* surface) {
    if (!serial_foreign_) {
      return false;
    }
    detail::log_refused_await(surface, AORTA_ERR_INVALID_ARGUMENT);
    code_ = AORTA_ERR_INVALID_ARGUMENT;
    stop_.emplace(Stopped::Fault(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        std::string(surface) +
            " [serial]: bound frame names a foreign executor")));
    return true;
  }

  /// Entry cancel-gate: the action verbs
  /// gate INLINE at await_suspend like every other ambient verb — one
  /// atomic load; the Rust-side checkpoint/run_until_cancelled remains
  /// the second (mid-flight) layer. Returns true when refused (resolve
  /// inline; await_resume delivers the cancelled Outcome).
  bool refuse_cancelled_entry() {
    // Fail-safe: is_cancelled() reads TRUE for
    // an invalid/moved-from scope — that MUST refuse here too, or an
    // invalid Scope becomes a scope-less action escape (the Rust bridge
    // would treat handle 0 as "no scope" and just run).
    if (!scope_.is_cancelled()) {
      return false;
    }
    code_ = AORTA_ERR_CANCELLED;
    const uint8_t raw = scope_.raw_cancel_reason();
    stop_.emplace(raw != 0 ? Stopped::CancelledRaw(raw)
                           : Stopped::Cancelled(CancelReason::kExplicit));
    return true;
  }

  /// The one-shot ctx callback (runtime worker, marked callback context):
  /// copy out, then post the resume — never run user code here.
  static void on_done(aorta_result_t code, const aorta_bytes_view_t* payload,
                      const aorta_flow_completion_t* completion,
                      void* ud) noexcept {
    auto* self = static_cast<action_ctx_awaiter*>(ud);
#if defined(__cpp_exceptions)
    try {
#endif
      self->code_ = code;
      if (code == AORTA_OK && payload != nullptr) {
        for (size_t i = 0; i < payload->slice_count; ++i) {
          const auto& s = payload->slices[i];
          self->payload_.insert(self->payload_.end(), s.data, s.data + s.len);
        }
      } else if (code != AORTA_OK) {
        // The completion record is copied HERE (it dies
        // with the callback) — exact reason / fault code+detail survive
        // into the verb's Outcome. Built in a LOCAL first so an
        // allocation failure leaves the member untouched (re-gate
        // residual): Stopped's move is noexcept, the emplace cannot throw.
        auto stop = stopped_from_completion(completion, code);
        self->stop_.emplace(std::move(stop));
      }
#if defined(__cpp_exceptions)
    } catch (...) {
      if (code == AORTA_OK) {
        // Lost the PAYLOAD copy: cannot claim success.
        self->code_ = AORTA_ERR_INTERNAL;
        self->payload_.clear();
      } else {
        // Lost only the record copy: keep the ORIGINAL failure code —
        // take_stop() degrades to classifying it (kind survives; the
        // detail text is the casualty), never a fabricated INTERNAL.
        self->code_ = code;
        self->stop_.reset();
      }
    }
#endif
    // The completion's continuation delivery routes through THE router.
    // Unbound: the pre-existing executor-post machinery, verbatim.
    // Bound: the completion becomes a domain member — the outcome is
    // already parked in the awaiter, so the member run resumes plainly
    // inside the strand's slice; the drop stamps the teardown first.
    detail::route_resume(
        self->fctx_, self->continuation_,
        detail::RouteDelivery{&action_ctx_awaiter::deliver_unbound_thunk,
                              &action_ctx_awaiter::stamp_teardown_thunk,
                              &action_ctx_awaiter::deliver_bound_run_thunk,
                              &action_ctx_awaiter::deliver_bound_drop_thunk,
                              self});
  }

  /// The pre-router delivery machinery, verbatim (unbound path).
  static void deliver_unbound_thunk(void* ud) {
    auto* self = static_cast<action_ctx_awaiter*>(ud);
    auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
      auto* inner = static_cast<action_ctx_awaiter*>(ev->handle);
      inner->delivery_status_ = ev->status;
      inner->continuation_.resume();
    };
    const aorta_result_t post_rc = aorta_executor_post_resume(
        self->executor_, trampoline, self, self->priority_);
    if (post_rc != AORTA_OK) {
      // A refusal code (NOT_FOUND = stale executor handle) is misuse
      // — logged before the CANCELLED flatten below; a stopped executor
      // (the normal teardown) stays quiet.
      detail::log_refused_await("flow::action", post_rc);
      action_ctx_awaiter::stamp_teardown_thunk(self);
      self->continuation_.resume();
    }
  }

  /// The caller-specific cancel stamp.
  static void stamp_teardown_thunk(void* ud) {
    static_cast<action_ctx_awaiter*>(ud)->delivery_status_ =
        AORTA_ERR_CANCELLED;
  }

  /// Bound member channels (see flow::call's twins).
  static void deliver_bound_run_thunk(void* ud) {
    static_cast<action_ctx_awaiter*>(ud)->continuation_.resume();
  }
  static void deliver_bound_drop_thunk(void* ud) {
    action_ctx_awaiter::stamp_teardown_thunk(ud);
    static_cast<action_ctx_awaiter*>(ud)->continuation_.resume();
  }

  /// Shared await_resume head: a teardown delivery unwinds the frame.
  void check_delivery() {
#if defined(__cpp_exceptions)
    if (delivery_status_ != AORTA_OK) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    if (delivery_status_ != AORTA_OK) {
      flow_requires_exceptions_unreachable();  // see flow.hpp
    }
#endif
  }

  /// The flow-stop channel for the verbs' Outcome results — the
  /// captured record if one rode the callback, else the code classified.
  Stopped take_stop() {
    return stop_.has_value() ? std::move(*stop_)
                             : stopped_from_completion(nullptr, code_);
  }

  Scope scope_;  // co-owned across the suspension
  uint64_t executor_;
  std::vector<uint8_t> request_;
  std::optional<Stopped> stop_;
  /// Bound frame's promise context (null = unbound) + the
  /// foreign-executor verdict recorded at stamp().
  const detail::promise_flow_context* fctx_ = nullptr;
  bool serial_foreign_ = false;
  aorta_bytes_slice_t slice_{};
  aorta_result_t code_ = AORTA_OK;
  std::vector<uint8_t> payload_;
  aorta_result_t delivery_status_ = AORTA_OK;
  uint8_t priority_ = 255;
  bool ambient_ = false;        // scope/exec resolve from the promise
  bool context_bound_ = false;  // set when a flow frame supplied the context
  std::coroutine_handle<> continuation_ = nullptr;
};

}  // namespace detail

// ---------------------------------------------------------------------------
// co_await send_goal{client, executor, goal, scope} → Outcome<AcceptedGoal>
// ---------------------------------------------------------------------------
template <typename Action>
class send_goal : public detail::action_ctx_awaiter {
 public:
  // Frozen order: client, plane, goal, scope LAST.
  send_goal(std::shared_ptr<aorta::ActionClient<Action>> client,
            const aorta::Executor& executor,
            const typename Action::GoalMsg& goal, Scope scope)
      : send_goal(std::move(client), executor.handle(), goal,
                  std::move(scope)) {}

  send_goal(std::shared_ptr<aorta::ActionClient<Action>> client,
            uint64_t executor, const typename Action::GoalMsg& goal,
            Scope scope)
      : send_goal(std::move(client), std::move(scope), executor,
                  aorta::ActionClient<Action>::GenerateGoalId(), goal) {}

  /// AMBIENT form: `co_await send_goal{client, goal}` —
  /// scope + executor resolve from the frame's promise context at
  /// await_suspend; the wire budget stamp still comes from that scope's
  /// remaining budget (semantics unchanged).
  send_goal(std::shared_ptr<aorta::ActionClient<Action>> client,
            const typename Action::GoalMsg& goal)
      : send_goal(std::move(client), /*executor=*/uint64_t{0}, goal,
                  Scope()) {
    ambient_ = true;
  }

  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    stamp(h);
    if (refuse_serial_foreign("flow::send_goal")) {
      return false;
    }
    if (refuse_unbound_ambient("flow::send_goal")) {
      return false;  // ambient form on a non-flow frame: inline refusal
    }
    if (refuse_cancelled_entry()) {
      return false;  // entry cancel-gate: inline refusal
    }
    auto view = request_view();
    return finish_suspend(aorta_action_client_send_goal_ctx(
        client_->native_handle(), scope_.handle(), &view,
        &detail::action_ctx_awaiter::on_done, this));
  }

  /// ONE vocabulary — scope cancel/deadline as Stopped::Cancelled
  /// (exact reason via the completion record), rejections/transport faults as
  /// Stopped::Fault{original code, detail}.
  [[nodiscard]] Outcome<AcceptedGoal<Action>> await_resume() {
    using Out = Outcome<AcceptedGoal<Action>>;
    check_delivery();
    if (code_ != AORTA_OK) {
      return Out::Stop(take_stop());
    }
    auto decoded = aorta::detail::DecodeSendGoalResponse<Action>(payload_);
    if (!decoded.ok()) {
      return Out::Stop(Stopped::Fault(decoded.status()));
    }
    const auto& response = decoded.value();
    if (!response.accepted) {
      return Out::Stop(Stopped::Fault(aorta::detail::SendGoalRejectedStatus(
          client_->ActionName(), response)));
    }
    if (response.goal_id != goal_id_) {
      return Out::Stop(Stopped::Fault(Status(
          AORTA_ERR_INTERNAL, "action send_goal reply goal_id mismatch")));
    }
    return Out::Value(AcceptedGoal<Action>{client_, response.goal_id,
                                           response.goal_secret,
                                           response.server_epoch});
  }

 private:
  send_goal(std::shared_ptr<aorta::ActionClient<Action>> client, Scope scope,
            uint64_t executor, std::string goal_id,
            const typename Action::GoalMsg& goal)
      : detail::action_ctx_awaiter(
            std::move(scope), executor,
            Action::EncodeSendGoalRequest(goal_id, goal)),
        client_(std::move(client)),
        goal_id_(std::move(goal_id)) {}

  std::shared_ptr<aorta::ActionClient<Action>> client_;
  std::string goal_id_;
};

template <typename Action>
send_goal(std::shared_ptr<aorta::ActionClient<Action>>,
          const aorta::Executor&, const typename Action::GoalMsg&, Scope)
    -> send_goal<Action>;
template <typename Action>
send_goal(std::shared_ptr<aorta::ActionClient<Action>>, uint64_t,
          const typename Action::GoalMsg&, Scope) -> send_goal<Action>;
template <typename Action>  // ambient
send_goal(std::shared_ptr<aorta::ActionClient<Action>>,
          const typename Action::GoalMsg&) -> send_goal<Action>;

// ---------------------------------------------------------------------------
// co_await wait_result{goal, executor[, timeout], scope}
//   → Outcome<ActionGetResultResponse<Action>>
// Parks until the goal reaches a TERMINAL state (push-based — the same
// status-watcher the blocking WaitResult uses), the timeout elapses, or the
// scope cancels.
// ---------------------------------------------------------------------------
template <typename Action>
class wait_result : public detail::action_ctx_awaiter {
 public:
  // Frozen order: goal, plane, timeout, scope LAST.
  wait_result(const AcceptedGoal<Action>& goal,
               const aorta::Executor& executor,
               std::optional<std::chrono::milliseconds> timeout, Scope scope)
      : wait_result(goal, executor.handle(), timeout, std::move(scope)) {}

  wait_result(const AcceptedGoal<Action>& goal,
               const aorta::Executor& executor, Scope scope)
      : wait_result(goal, executor.handle(), std::nullopt,
                    std::move(scope)) {}

  wait_result(const AcceptedGoal<Action>& goal, uint64_t executor,
               Scope scope)
      : wait_result(goal, executor, std::nullopt, std::move(scope)) {}

  wait_result(const AcceptedGoal<Action>& goal, uint64_t executor,
               std::optional<std::chrono::milliseconds> timeout, Scope scope)
      : detail::action_ctx_awaiter(
            std::move(scope), executor,
            Action::EncodeGetResultRequest(goal.goal_id, goal.goal_secret)),
        client_(goal.client),
        goal_id_(goal.goal_id),
        server_epoch_(goal.server_epoch),
        timeout_ms_(timeout.has_value()
                        ? static_cast<uint64_t>(timeout->count())
                        : aorta::detail::kNoResultTimeoutMs) {}

  /// AMBIENT form: `co_await wait_result{goal[, ms]}` —
  /// scope + executor from the frame's promise context at await_suspend;
  /// the optional timeout stays explicit.
  explicit wait_result(
      const AcceptedGoal<Action>& goal,
      std::optional<std::chrono::milliseconds> timeout = std::nullopt)
      : wait_result(goal, /*executor=*/uint64_t{0}, timeout, Scope()) {
    ambient_ = true;
  }

  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    stamp(h);
    if (refuse_serial_foreign("flow::wait_result")) {
      return false;
    }
    if (refuse_unbound_ambient("flow::wait_result")) {
      return false;  // ambient form on a non-flow frame: inline refusal
    }
    if (refuse_cancelled_entry()) {
      return false;  // entry cancel-gate: inline refusal
    }
    return finish_suspend(aorta_action_client_wait_result_ctx(
        client_->native_handle(), scope_.handle(), goal_id_.c_str(),
        request_.data(), request_.size(), timeout_ms_, &decode_thunk,
        &detail::action_ctx_awaiter::on_done, this));
  }

  [[nodiscard]] Outcome<ActionGetResultResponse<Action>> await_resume() {
    using Out = Outcome<ActionGetResultResponse<Action>>;
    check_delivery();
    if (code_ != AORTA_OK) {
      return Out::Stop(take_stop());
    }
    auto decoded = aorta::detail::DecodeGetResultResponse<Action>(payload_);
    if (!decoded.ok()) {
      return Out::Stop(Stopped::Fault(decoded.status()));
    }
    auto validated = aorta::detail::ValidateGetResultResponse<Action>(
        client_->ActionName(), goal_id_, server_epoch_,
        std::move(decoded).value());
    if (!validated.ok()) {
      return Out::Stop(Stopped::Fault(validated.status()));
    }
    return Out::Value(std::move(validated).value());
  }

 private:
  static int8_t decode_thunk(uint8_t kind, const uint8_t* bytes,
                             uintptr_t len, void* /*user_data*/) {
    // Stateless: terminality classification shares the blocking client's
    // rules (one decoder, two consumers).
    return aorta::detail::DecodeStatusRecord<Action>(
        kind, bytes, static_cast<size_t>(len));
  }

  std::shared_ptr<aorta::ActionClient<Action>> client_;
  std::string goal_id_;
  std::string server_epoch_;
  uint64_t timeout_ms_;
};

// ---------------------------------------------------------------------------
// co_await cancel_goal{goal, executor, scope}
//   → Outcome<ActionCancelGoalResponse> (awaited, checked cancel)
// ---------------------------------------------------------------------------
template <typename Action>
class cancel_goal : public detail::action_ctx_awaiter {
 public:
  // Frozen order: goal, plane, scope LAST.
  cancel_goal(const AcceptedGoal<Action>& goal,
              const aorta::Executor& executor, Scope scope)
      : cancel_goal(goal, executor.handle(), std::move(scope)) {}

  cancel_goal(const AcceptedGoal<Action>& goal, uint64_t executor, Scope scope)
      : detail::action_ctx_awaiter(
            std::move(scope), executor,
            Action::EncodeCancelGoalRequest(goal.goal_id, goal.goal_secret)),
        client_(goal.client) {}

  /// AMBIENT form: `co_await cancel_goal{goal}` —
  /// scope + executor from the frame's promise context at await_suspend.
  explicit cancel_goal(const AcceptedGoal<Action>& goal)
      : cancel_goal(goal, /*executor=*/uint64_t{0}, Scope()) {
    ambient_ = true;
  }

  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    stamp(h);
    if (refuse_serial_foreign("flow::cancel_goal")) {
      return false;
    }
    if (refuse_unbound_ambient("flow::cancel_goal")) {
      return false;  // ambient form on a non-flow frame: inline refusal
    }
    if (refuse_cancelled_entry()) {
      return false;  // entry cancel-gate: inline refusal
    }
    auto view = request_view();
    return finish_suspend(aorta_action_client_cancel_ctx(
        client_->native_handle(), scope_.handle(), &view,
        &detail::action_ctx_awaiter::on_done, this));
  }

  [[nodiscard]] Outcome<ActionCancelGoalResponse> await_resume() {
    using Out = Outcome<ActionCancelGoalResponse>;
    check_delivery();
    if (code_ != AORTA_OK) {
      return Out::Stop(take_stop());
    }
    auto decoded = aorta::detail::DecodeCancelGoalResponse<Action>(payload_);
    if (!decoded.ok()) {
      return Out::Stop(Stopped::Fault(decoded.status()));
    }
    const auto& response = decoded.value();
    if (!response.accepted) {
      return Out::Stop(Stopped::Fault(aorta::detail::CancelRejectedStatus(
          client_->ActionName(), response)));
    }
    return Out::Value(std::move(decoded).value());
  }

 private:
  std::shared_ptr<aorta::ActionClient<Action>> client_;
};

template <typename Action>
wait_result(const AcceptedGoal<Action>&, const aorta::Executor&,
             std::optional<std::chrono::milliseconds>, Scope)
    -> wait_result<Action>;
template <typename Action>
wait_result(const AcceptedGoal<Action>&, const aorta::Executor&, Scope)
    -> wait_result<Action>;
template <typename Action>
wait_result(const AcceptedGoal<Action>&, uint64_t,
             std::optional<std::chrono::milliseconds>, Scope)
    -> wait_result<Action>;
template <typename Action>
wait_result(const AcceptedGoal<Action>&, uint64_t, Scope)
    -> wait_result<Action>;
template <typename Action>  // ambient
wait_result(const AcceptedGoal<Action>&) -> wait_result<Action>;
template <typename Action>  // ambient + explicit timeout
wait_result(const AcceptedGoal<Action>&,
             std::optional<std::chrono::milliseconds>) -> wait_result<Action>;
template <typename Action>  // ambient
cancel_goal(const AcceptedGoal<Action>&) -> cancel_goal<Action>;

// ---------------------------------------------------------------------------
// GoalGuard — RAII cancel-on-drop (OPT-IN sugar; mirrors the Rust
// `GoalGuard`). Scope cancellation alone stops only the LOCAL wait; the
// goal keeps running server-side unless someone cancels it. Destruction
// fires a FIRE-AND-FORGET cancel through the ctx bridge (non-blocking —
// safe from any destructor; failures are logged core-side). `disarm()`
// releases the goal without cancelling (hand-over shape); for an awaited,
// checked cancel use `co_await cancel_goal{guard.disarm(), ...}`.
// ---------------------------------------------------------------------------
template <typename Action>
class GoalGuard {
 public:
  explicit GoalGuard(AcceptedGoal<Action> goal) : goal_(std::move(goal)) {}

  GoalGuard(GoalGuard&& other) noexcept
      : goal_(std::move(other.goal_)), armed_(std::exchange(other.armed_, false)) {}
  GoalGuard& operator=(GoalGuard&& other) noexcept {
    if (this != &other) {
      fire();
      goal_ = std::move(other.goal_);
      armed_ = std::exchange(other.armed_, false);
    }
    return *this;
  }
  GoalGuard(const GoalGuard&) = delete;
  GoalGuard& operator=(const GoalGuard&) = delete;

  ~GoalGuard() { fire(); }

  /// The guarded goal (feedback/result composition while guarded).
  const AcceptedGoal<Action>& goal() const { return goal_; }

  /// Release WITHOUT cancelling.
  AcceptedGoal<Action> disarm() {
    armed_ = false;
    return std::move(goal_);
  }

 private:
  void fire() noexcept {
    if (!armed_ || goal_.client == nullptr) {
      return;
    }
    armed_ = false;
#if defined(__cpp_exceptions)
    try {
      fire_impl();
    } catch (...) {
      // fire() runs from the (noexcept) destructor and move-assign: an
      // allocation/encode failure must not std::terminate() the process
      // — cancel-on-drop is best-effort by contract, so the goal simply
      // stays uncancelled.
    }
#else
    fire_impl();
#endif
  }

  void fire_impl() {
    std::vector<uint8_t> request =
        Action::EncodeCancelGoalRequest(goal_.goal_id, goal_.goal_secret);
    aorta_bytes_slice_t slice{request.data(), request.size()};
    aorta_bytes_view_t view{&slice, 1, request.size()};
    // Fire-and-forget: the bridge copies the request before returning and
    // logs failures; nothing to wait on, nothing blocks this destructor.
    (void)aorta_action_client_cancel_ctx(goal_.client->native_handle(),
                                         /*scope=*/0, &view,
                                         /*on_done=*/nullptr,
                                         /*user_data=*/nullptr);
  }

  AcceptedGoal<Action> goal_;
  bool armed_ = true;
};

}  // namespace aorta::flow
