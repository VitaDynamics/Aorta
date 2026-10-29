// Flow — `co_await flow::recv`.
//
// The awaiter bridges the pull subscriber's data-available notify hook
// (aorta_sub_set_notify — fires on the RX thread, trampoline-class) to
// an arbiter group, so a lane frame parks until a sample arrives, the
// executor stops it (the stop sweep resolves the branch), a deadline
// passes (recv_for's second branch), or — the mandatory scope parameter
// (recv_in parity) — the given Scope's cancel
// token fires (a core-side watcher branch via aorta_scope_arm_arbiter).
//
// FRAME-SAFETY PROTOCOL: the moment the notify
// hook is published, a sample can complete the branch and an MT lane
// worker can resume + destroy this frame concurrently. Everything after
// the publish therefore runs on LOCALS ONLY: the recheck is the
// NON-POPPING `aorta_sub_pending` probe (never steals a sample it
// cannot deliver, never touches the frame) and self-completes through
// the copied handle (CAS-once collapses hook/recheck/sweep races to
// exactly one resumption). `await_resume` performs the only pop on the
// parked path; an OK-but-empty resume (sample stolen by a concurrent
// consumer) LOOPS instead of misreporting cancellation.
//
// SINGLE-AWAITER CONTRACT: one concurrent recv() per pull
// subscriber — the core refuses a second hook registration and the
// refused awaiter resolves Stopped (a failed set_notify
// self-completes; the awaiter never parks without a wake source).
#pragma once

#include <cstdlib>
#include <optional>

#include "aorta/flow_combinators.hpp"
#include "aorta/pull_subscriber.h"

namespace aorta::flow {

namespace detail {

struct next_once {
  // HANDLE-ONLY after construction (TSan-caught hazard): the awaiter
  // never dereferences the PullSubscriber once parked — a subscriber
  // freed mid-park (the drop-wake path) must resolve the frame without
  // a UAF. Freed handles no-op registry-side on every call below.
  aorta_sub_t sub_handle;
  LaneRuntime* runtime;
  /// 0 = plain recv(); non-zero = recv_for()'s deadline window. The
  /// timer is a SECOND BRANCH of the SAME group — composing the raw
  /// timeout() combinator around recv() would strand: the loser recv()
  /// branch has no data and no cancel path until executor stop.
  uint64_t deadline_window_ns = 0;
  /// 0 = no scope race; non-zero = a Scope handle whose cancel token
  /// resolves the park (another branch of the same group — the recv_in
  /// per-scope cancel parity).
  uint64_t scope_handle = 0;
  std::optional<PullSample> got;
  aorta_result_t status = AORTA_OK;
  bool deadline_won = false;
  uint64_t group_ = 0;
  uint64_t handle_ = 0;
  bool hook_owned_ = true;
  ArbiterFrame frame_{};
  /// Winner protocol (in-frame is SAFE here: branch cbs run strictly
  /// before the group's resume posts, and the arming thread only
  /// touches these before the hook publish): first resolver claims,
  /// then revokes the sibling so the group resolves.
  struct Duel {
    std::atomic<int> winner{-1};
    uint64_t handles[3] = {0, 0, 0};
    BranchSlot slots[3] = {};
  };
  Duel duel_{};

  bool await_ready() {
    take_sample();
    return got.has_value();
  }

  void take_sample() {
    aorta_pull_sample_t raw{};
    // reentrancy: probe (timeout-0 — never blocks)
    if (aorta_sub_recv(sub_handle, &raw, 0) == AORTA_OK) {
      got.emplace(PullSample(raw));
    }
  }

  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    frame_.rt = runtime;
    frame_.fallback_executor = runtime->fallback_executor();
    frame_.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      frame_.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // The queued-sample fast path (await_ready's
    // take_sample) never arms and stays legal; a bound frame ARMING a
    // foreign-plane subscriber wait refuses before the arm exists.
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, runtime)) {
        detail::log_refused_await("flow::recv [serial: foreign runtime arm]",
                                  AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      frame_.fctx = &fctx;
    }
    // Stop-arm gate: a frame resumed-into-cancellation
    // must not park on a closing pool. Pin: the arm window counts
    // against the stop drain's producer quiesce.
    ArmWindowPin pin;
    if (runtime->enabled() && runtime->pool().closed()) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    const bool has_deadline = deadline_window_ns != 0;
    const bool has_scope = scope_handle != 0;
    const uint32_t branches =
        1 + (has_deadline ? 1 : 0) + (has_scope ? 1 : 0);
    if (aorta_arbiter_group_new(&ArbiterFrame::resume_cb, &frame_, branches,
                                &group_) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    if (aorta_arbiter_arm(group_, 0, &duel_complete_0, &duel_, &handle_) !=
        AORTA_OK) {
      aorta_arbiter_group_release(group_);
      group_ = 0;
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    duel_.handles[0] = handle_;
    // Any auxiliary-branch arm failure resolves EVERY armed branch
    // CANCELLED and lets the normal resume path deliver (frame-safe) —
    // the caller unwinds. A stale scope handle refuses the same way
    // (dead scope == refused entry, the flow_action convention).
    const auto fail_armed = [&] {
      for (uint64_t h : duel_.handles) {
        if (h != 0) {
          (void)aorta_arbiter_complete(h, AORTA_ERR_CANCELLED, nullptr);
        }
      }
    };
    if (has_deadline) {
      if (aorta_arbiter_arm(group_, 1, &duel_complete_1, &duel_,
                            &duel_.handles[1]) != AORTA_OK ||
          aorta_timer_arm_arbiter(duel_.handles[1], deadline_window_ns) !=
              AORTA_OK) {
        // Unreachable with current arm failure modes — kept as
        // defense so a future failure
        // mode degrades to a clean cancel instead of a strand.
        fail_armed();
        return true; // resolve via the normal resume path (frame-safe)
      }
      runtime->register_arm(duel_.handles[1]);
    }
    if (has_scope) {
      const uint32_t si = has_deadline ? 2 : 1;
      aorta_result_t scope_rc = aorta_arbiter_arm(
          group_, si, si == 2 ? &duel_complete_2 : &duel_complete_1, &duel_,
          &duel_.handles[si]);
      if (scope_rc == AORTA_OK) {
        scope_rc = aorta_scope_arm_arbiter(duel_.handles[si], scope_handle);
      }
      if (scope_rc != AORTA_OK) {
        // A stale Scope handle refuses NOT_FOUND — a programming
        // error the CANCELLED flatten below would otherwise hide.
        detail::log_refused_await("flow::recv", scope_rc);
        fail_armed();
        return true;
      }
      runtime->register_arm(duel_.handles[si]);
    }
    // The stop sweep resolves this registration Stopped.
    runtime->register_arm(handle_);
    // Frame-safety: copy every datum the post-publish steps need into
    // LOCALS — after set_notify succeeds the frame may be
    // resumed/destroyed by another thread at any instant.
    const uint64_t local_handle = handle_;
    LaneRuntime* rt_local = runtime;
    const aorta_result_t notify_rc = aorta_sub_set_notify(
        sub_handle, &notify_thunk, reinterpret_cast<void*>(local_handle));
    if (notify_rc != AORTA_OK) {
      // No wake source (freed handle or a concurrent awaiter
      // holds the slot) — self-resolve Stopped through the arbiter (the
      // resume arrives frame-safely via the normal post path). The
      // raw refusal code (second concurrent recv() = INVALID_ARGUMENT,
      // freed subscriber = NOT_FOUND) is logged before the flatten.
      detail::log_refused_await("flow::recv", notify_rc);
      (void)aorta_arbiter_complete(local_handle, AORTA_ERR_CANCELLED,
                                   nullptr);
      hook_owned_ = false;  // never clear a hook we don't own
      return true;
    }
    // FRAME PUBLISHED — locals only from here.
    uint8_t pending = 0;
    if (aorta_sub_pending(sub_handle, &pending) == AORTA_OK && pending != 0) {
      // A sample landed before the hook was set and will never fire it —
      // claim the branch (CAS-once; a concurrent notify losing the race
      // is a harmless no-op).
      (void)aorta_arbiter_complete(local_handle, AORTA_OK, nullptr);
    } else if (rt_local->enabled() && rt_local->pool().closed()) {
      // close() raced the arm — the sweep may have missed it (the
      // stop-arm recheck, verbatim).
      (void)aorta_arbiter_revoke(local_handle);
    }
    return true;
  }

  /// One round's outcome (returned by value so the caller never reads
  /// awaiter members after resumption — also what lets GCC compile the
  /// non-template coroutines below: it chokes on lvalue awaitables
  /// whose members are read post-await).
  struct Round {
    std::optional<PullSample> sample;
    aorta_result_t status = AORTA_OK;
    bool deadline_won = false;
  };

  Round await_resume() {
    if (group_ != 0) {
      // One-shot: clear the hook BEFORE releasing the group (a stale
      // in-flight notify no-ops registry-side on the dead handle id;
      // a FREED subscriber handle no-ops the same way). Only OUR hook:
      // a refused concurrent awaiter must not clear the winner's.
      if (hook_owned_) {
        (void)aorta_sub_set_notify(sub_handle, nullptr, nullptr);
      }
      status = duel_.slots[0].status.load(std::memory_order_acquire);
      deadline_won = duel_.winner.load(std::memory_order_acquire) == 1 &&
                     duel_.slots[1].status.load(std::memory_order_acquire) ==
                         AORTA_OK;
      runtime->deregister_arm(handle_);
      for (int i = 1; i < 3; i++) {
        if (duel_.handles[i] != 0) {
          runtime->deregister_arm(duel_.handles[i]);
        }
      }
      aorta_arbiter_group_release(group_);
      group_ = 0;
      if (status == AORTA_OK && plane_torn(frame_, runtime)) {
        // Torn delivery carrying a data-ready OK:
        // report Stopped — and leave any queued sample UNPOPPED (never
        // deliver data on a drain/destructor thread; the sample stays
        // for a future consumer). next_loop unwinds on the status.
        status = AORTA_ERR_CANCELLED;
      }
    }
    if (!got.has_value() && status == AORTA_OK && !deadline_won) {
      take_sample(); // the only pop on the parked path
    }
    return Round{std::move(got), status, deadline_won};
  }

  static void duel_complete(Duel* d, int idx, aorta_result_t st) {
    int expected = -1;
    if (d->winner.compare_exchange_strong(expected, idx,
                                          std::memory_order_acq_rel)) {
      // First resolver: revoke every armed sibling so the group
      // resolves (unarmed slots are 0 and skip).
      for (int i = 0; i < 3; i++) {
        if (i != idx && d->handles[i] != 0) {
          (void)aorta_arbiter_revoke(d->handles[i]);
        }
      }
    }
    d->slots[idx].status.store(st, std::memory_order_release);
  }

  static void duel_complete_0(void* ctx, aorta_result_t st,
                              const aorta_bytes_view_t* payload,
                              const aorta_flow_completion_t* /*completion*/) {
    (void)payload;
    duel_complete(static_cast<Duel*>(ctx), 0, st);
  }
  static void duel_complete_1(void* ctx, aorta_result_t st,
                              const aorta_bytes_view_t* payload,
                              const aorta_flow_completion_t* /*completion*/) {
    (void)payload;
    duel_complete(static_cast<Duel*>(ctx), 1, st);
  }
  static void duel_complete_2(void* ctx, aorta_result_t st,
                              const aorta_bytes_view_t* payload,
                              const aorta_flow_completion_t* /*completion*/) {
    (void)payload;
    duel_complete(static_cast<Duel*>(ctx), 2, st);
  }

  static void notify_thunk(void* ctx) {
    // RX thread, trampoline-class: one CAS-guarded branch completion.
    (void)aorta_arbiter_complete(reinterpret_cast<uint64_t>(ctx), AORTA_OK,
                                 nullptr);
  }
};

/// The one park-pop-or-unwind loop behind every recv()/recv_for()
/// shape (handle-only; the public verbs keep the Scope alive in THEIR
/// frames). Empty+OK = spurious wake (a concurrent consumer took the
/// sample) — re-park; a deadline win, a stop, or a scope cancel unwind
/// `cancelled_unwind`. Endpoint-tagged: timeout()/race() reject it
/// at compile time.
//
// The return type is now a (defaulted) template parameter `R`, not the
// concrete `EndpointTask<PullSample>` directly.
// An ORDINARY (non-template) coroutine function's promise_type is
// resolved EAGERLY, at the point its definition is parsed; even a
// function TEMPLATE whose signature does not actually mention its
// template parameter is no better — two-phase lookup only defers
// constructs that are syntactically DEPENDENT on that parameter, and a
// concrete `EndpointTask<PullSample>` return type is not dependent on an
// unrelated dummy parameter. Either way `Task<PullSample,
// task_kind_endpoint>::promise_type` gets instantiated (tripping the
// choke-gate static_assert) merely from INCLUDING this header under
// -fno-exceptions — breaking the "header inclusion stays legal" contract
// the choke gate promises. Making the RETURN TYPE ITSELF the template
// parameter makes the whole body genuinely dependent, deferring
// instantiation to the (only ever exceptions-enabled) call site — for a
// normal call site with no explicit template argument, `R` defaults to
// the original `EndpointTask<PullSample>`, so behavior is unchanged.
template <typename R = EndpointTask<PullSample>>
inline R next_loop(aorta_sub_t handle, LaneRuntime* rt, uint64_t window_ns,
                   uint64_t scope_h) {
  for (;;) {
    auto r = co_await detail::next_once{handle, rt, window_ns, scope_h};
    if (r.sample.has_value()) {
      co_return std::move(*r.sample);
    }
    if (r.deadline_won || r.status != AORTA_OK) {
#if defined(__cpp_exceptions)
      detail::arm_unwind();
      throw cancelled_unwind{r.deadline_won ? static_cast<uint8_t>(CancelReason::kDeadlineExceeded) : uint8_t{0}};
#else
      // Never deref an empty optional — no-exceptions builds abort
      // loudly (the cancellation contract has no unwind channel here). This
      // is a statically-unreachable trap (see flow_requires_exceptions_
      // unreachable in flow.hpp).
      flow_requires_exceptions_unreachable();
#endif
    }
    // Spurious: re-park (with the SAME window when one is set —
    // conservative: a stolen sample restarts the deadline; callers
    // needing exact windows track elapsed themselves).
  }
}

}  // namespace detail

/// `co_await recv(sub, rt, scope)` — the consumption verb for lane
/// frames (Rust parity: `sub.recv_in(&scope)`). Completes with the next
/// sample and races the Scope's cancel token: `scope.cancel()` resolves
/// the park promptly and the frame unwinds `cancelled_unwind`. Queued
/// samples still deliver first (the await_ready fast-path pops before
/// parking — recv_in's drain-friendly ordering). ONE concurrent
/// recv() per subscriber (single-awaiter contract above).
///
/// The scope is MANDATORY: a scope-less form would park
/// cancellation-blind until the executor's stop sweep. A genuinely
/// teardown-scoped waiter passes its root scope explicitly.
///
/// Returns an ENDPOINT-tagged task: valid for direct co_await (and
/// join()), but timeout()/race() reject it at compile time — use recv_for.
//
// Return-type-as-template-parameter — see next_loop above (defers
// promise_type instantiation so mere header inclusion stays legal under
// -fno-exceptions).
template <typename R = EndpointTask<PullSample>>
inline R recv(PullSubscriber& sub, LaneRuntime& rt,
                                     Scope scope) {
  // The coroutine frame holds `scope` (shared ownership) alive for the
  // whole wait; the awaiter itself stays handle-only.
  co_return co_await detail::next_loop(sub.native_handle(), &rt, 0,
                                       scope.handle());
}

/// `co_await recv_for(sub, rt, window, scope)` — recv() with a deadline.
/// The timer is a SECOND BRANCH of the same arbiter group (first
/// resolver wins, siblings revoked) — composing the raw `timeout()`
/// combinator around `recv()` is a COMPILE error: its loser branch
/// would park with no data and no cancel path until executor stop.
/// Deadline (and stop) unwind `cancelled_unwind`.
//
// Return-type-as-template-parameter — see next_loop above.
template <typename R = EndpointTask<PullSample>>
inline R recv_for(PullSubscriber& sub, LaneRuntime& rt,
                                         std::chrono::nanoseconds window,
                                         Scope scope) {
  co_return co_await detail::next_loop(sub.native_handle(), &rt,
                                       static_cast<uint64_t>(window.count()),
                                       scope.handle());
}

/// `co_await recv(sub)` — the AMBIENT form:
/// {rt, scope} resolve from the frame's promise context; a thin
/// EndpointTask wrapper delegates to the explicit overload, so the
/// endpoint KIND is preserved (timeout()/race() reject the ambient form
/// at compile time exactly like the explicit one) and the wait is
/// cancel-safe BY DEFAULT (the context scope always rides the arbiter
/// group). Executor-only frames refuse.
/// Awaiting from a non-flow coroutine is a compile error (the Task
/// await_suspend concept gate).
//
// Return-type-as-template-parameter — see next_loop above.
template <typename R = EndpointTask<PullSample>>
inline R recv(PullSubscriber& sub) {
  auto ctx = co_await this_coro::context;
  if (ctx.rt == nullptr) {
    detail::refuse_null_rt_endpoint("flow::recv");
  }
  if (ctx.scope.is_cancelled()) {
    detail::arm_unwind();  // Gate BEFORE the delegate — the
                           // explicit form's ready fast-path would consume
                           // data on a cancelled frame. Reason from scope.
    throw cancelled_unwind{ctx.scope.raw_cancel_reason()};
  }
  co_return co_await recv(sub, *ctx.rt, std::move(ctx.scope));
}

/// `co_await recv_for(sub, window)` — ambient recv() with a deadline
/// (the window stays explicit; {rt, scope} from context).
//
// Return-type-as-template-parameter — see next_loop above.
template <typename R = EndpointTask<PullSample>>
inline R recv_for(PullSubscriber& sub,
                                         std::chrono::nanoseconds window) {
  auto ctx = co_await this_coro::context;
  if (ctx.rt == nullptr) {
    detail::refuse_null_rt_endpoint("flow::recv_for");
  }
  if (ctx.scope.is_cancelled()) {
    detail::arm_unwind();  // Gate BEFORE the delegate — the
                           // explicit form's ready fast-path would consume
                           // data on a cancelled frame. Reason from scope.
    throw cancelled_unwind{ctx.scope.raw_cancel_reason()};
  }
  co_return co_await recv_for(sub, *ctx.rt, window, std::move(ctx.scope));
}

}  // namespace aorta::flow
