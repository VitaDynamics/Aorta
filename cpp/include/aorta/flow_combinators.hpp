// Flow C++ combinators: the
// arbiter is the SYNCHRONIZATION SPINE — typed values never cross the
// ABI (they land in C++-side slots); a branch resolution is the only
// thing the core sees. The frame resumes exactly once, scheduled by the
// LAST resolver, and the resume itself posts to the lane pool
// (or, in rollback mode, to the core executor).
//
// Shapes (explicit; each verb also has a context-free AMBIENT overload —
// `sleep(50ms)`, `timeout(50ms, task)`, `join(ta, tb)`,
// `race(ta, tb)`, `offload(lane, fn)` — that binds {rt, scope} from the
// frame's promise context at await_suspend):
//   co_await flow::sleep(rt, 50ms);                    // arbiter timer
//   co_await flow::timeout(rt, 50ms, task, scope);     // race vs sleep
//   auto [a, b] = co_await flow::join(rt, scope, ta, tb);   // order EXEMPT
//   auto winner  = co_await flow::race(rt, ta, tb, scope);
//   auto value   = co_await flow::offload(rt, lane, fn);    // compute hop
//
// race semantics (Cobalt-style interrupt-then-drain): the
// winner's completion revokes the losers' REGISTRATIONS immediately (the
// frame resumes at the last resolution); the loser TASKS keep draining
// structurally under `scope` — their late completes are CAS losers and
// no-op. Racing side-effecting work is therefore an application
// decision, exactly like the Rust plane's documented loser contract.
#pragma once

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <optional>
#include <tuple>
#include <utility>

#include "aorta/flow.hpp"

namespace aorta {
namespace flow {

// NOTE: ArmWindowPin, detail::ArbiterFrame and detail::BranchSlot
// moved to flow.hpp — TaskHandle's await rides the same machinery. Same
// namespaces; every reference below resolves unchanged.

namespace detail {

struct cancelled_awaiter;  // fwd (the one ambient verb that must NOT
                           // entry-refuse on a cancelled scope — resolving
                           // immediately IS its contract)

/// The ONE arbiter-timer sleep machinery (DRY: the explicit
/// `sleep(rt, d)` awaiter and the ambient `sleep(d)`'s arbiter leg share
/// this arm/settle pair, so the two paths cannot drift). The sequence is
/// the hardened original: stop-arm gate + ArmWindowPin, suspend-
/// then-arm, register_arm, close-race self-revoke.
struct arbiter_sleep_core {
  aorta_result_t status = AORTA_OK;
  uint64_t group_ = 0;
  uint64_t handle_ = 0;  // the armed branch handle (stored,
                         // not reconstructed via the group<<8 packing)
  ArbiterFrame frame_{};
  BranchSlot slot_{};

  /// Returns true when parked; false = resolved inline (status set).
  /// `fctx`: the awaiting frame's flow context — bound frames'
  /// timer resumes re-enter their domain through the resume router.
  bool arm(LaneRuntime* runtime, std::chrono::nanoseconds delay,
           std::coroutine_handle<> h, uint8_t prio_clamped,
           const promise_flow_context* fctx = nullptr) {
    frame_.rt = runtime;
    frame_.fallback_executor = runtime->fallback_executor();
    frame_.frame = h;
    frame_.priority = prio_clamped;
    frame_.fctx = fctx;
    // Stop-arm gate: a frame resumed-into-cancellation must not
    // arm a NEW timer — the pool is closing and a late completion would
    // strand its residue after the final stop-drain. The arm-window pin
    // makes the whole window visible to the producer quiesce.
    ArmWindowPin pin;
    if (runtime->enabled() && runtime->pool().closed()) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    // Suspend-then-arm: we are already suspended here.
    if (aorta_arbiter_group_new(&ArbiterFrame::resume_cb, &frame_, 1,
                                &group_) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;  // resume inline: arm failure resolves Stopped
    }
    uint64_t handle = 0;
    if (aorta_arbiter_arm(group_, 0, &BranchSlot::complete_cb, &slot_,
                          &handle) != AORTA_OK) {
      aorta_arbiter_group_release(group_);
      group_ = 0;
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    // EVERY awaiter write happens BEFORE any publication (the same
    // window as cancelled()): from the
    // register_arm below, the stop sweep can revoke this branch and the
    // resumed frame can destroy this awaiter while this thread still
    // runs the tail; from the timer arm, a short-delay timer can do the
    // same from an MT worker. handle_ is final here; everything below
    // runs on LOCALS.
    handle_ = handle;
    LaneRuntime* rt_local = runtime;
    rt_local->register_arm(handle);
    if (aorta_timer_arm_arbiter(
            handle, static_cast<uint64_t>(delay.count())) != AORTA_OK) {
      // The branch is already PUBLISHED to the sweep — the awaiter may
      // be gone. Resolve by REVOKE-AND-PARK (CAS-idempotent): the frame
      // resumes through the arbiter exactly once; settle() reads
      // slot=CANCELLED, retires the registration and releases the group.
      (void)aorta_arbiter_revoke(handle);
      return true;
    }
    if (rt_local->enabled() && rt_local->pool().closed()) {
      // close() raced our arm and the stop sweep may have missed the
      // registration — self-revoke (CAS-idempotent: if a resolver beat
      // us, it owns the resume). Either way the resume lands in the
      // residue, which the still-running stop-drain loop delivers (the
      // producer-quiesce counts the revoke in flight).
      (void)aorta_arbiter_revoke(handle);
    }
    return true;
  }

  /// The await_resume head shared by both sleeps: retire the arm, read
  /// the branch status, release the group. Never throws — the caller
  /// decides between unwind (default) and raw status (.nothrow()).
  aorta_result_t settle(LaneRuntime* runtime) noexcept {
    if (group_ != 0) {
      status = slot_.status.load(std::memory_order_acquire);
      // The resolved handle retires from the stop-sweep registry.
      runtime->deregister_arm(handle_);
      aorta_arbiter_group_release(group_);
      group_ = 0;
      if (status == AORTA_OK && plane_torn(frame_, runtime)) {
        // Torn delivery carrying the timer's OK: the
        // resume travelled a teardown channel — report Stopped so the
        // default mode unwinds and the .nothrow() caller reads CANCELLED,
        // never OK, on a drain/destructor thread.
        status = AORTA_ERR_CANCELLED;
      }
    }
    return status;
  }
};

// ---------------------------------------------------------------------------
// sleep — the arbiter-native delay (lanes have no
// timer; the core sleeper completes a 1-branch group; revoking the
// branch aborts the sleeper). sleep_on(exec, d) remains the queue-path shim.
//
// TWO forms: the EXPLICIT `sleep(rt, d)` — this struct
// (its historical -fno-exceptions silent-status return is revoked: the
// await is now compile-gated there) — and the AMBIENT `sleep(d)` factory
// below, which picks its
// machinery from the frame's promise context. The awaiter type lives in
// `detail` (surface diet); the `flow::sleep(rt, d)` factory overloads
// below are the public entry.
// ---------------------------------------------------------------------------
struct arbiter_sleep {
  LaneRuntime* runtime;
  std::chrono::nanoseconds delay;
  detail::arbiter_sleep_core core_{};

  arbiter_sleep(LaneRuntime& rt, std::chrono::nanoseconds d)
      : runtime(&rt), delay(d) {}

  bool await_ready() const noexcept { return delay.count() <= 0; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    uint8_t prio = 3;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      prio = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // A bound frame may not ARM a foreign-plane timer (the zero-delay
    // ready fast path above never arms and stays legal).
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, runtime)) {
        detail::log_refused_await("flow::sleep [serial: foreign runtime arm]",
                                  AORTA_ERR_INVALID_ARGUMENT);
        core_.status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      return core_.arm(runtime, delay, h, prio, &fctx);
    }
    return core_.arm(runtime, delay, h, prio);
  }
  // Belt gate: the historical "silent status return" under
  // -fno-exceptions (the explicit sleep(rt, d) form, unlike the ambient
  // sleep(d) form, used to fall through without an unwind channel AND
  // without a compile-time rejection) is REVOKED — Flow is not on a
  // -fno-exceptions consumer's surface at all, explicit or
  // ambient. Dependent static_assert so mere header inclusion stays legal.
  template <typename Unused = void>
  aorta_result_t await_resume() {
    const aorta_result_t st = core_.settle(runtime);
#if defined(__cpp_exceptions)
    if (st != AORTA_OK) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "flow::sleep(rt, d)'s historical -fno-exceptions "
                  "silent-status-return is revoked — build this "
                  "translation unit with exceptions enabled, or use the "
                  "C++17 baseline aorta SDK (callbacks/executors/pull "
                  "subscribers)");
#endif
    return st;
  }
};

}  // namespace detail

// ---------------------------------------------------------------------------
// ambient_sleep — `co_await flow::sleep(d)`: ONE verb,
// machinery picked from the promise context at await_suspend:
//
//   rt != nullptr (enabled OR Disabled) → the arbiter-timer machinery
//     (arbiter_sleep_core — the resume plane is picked inside by
//     enabled(): lane pool, or the fallback executor on the rollback
//     leg, exactly like the explicit sleep's ArbiterFrame);
//   rt == nullptr (executor-only frame)  → the sleep_on machinery on the
//     context executor (queue-path timer; stop-drained promptly).
//
// The 0-delay fast path covers BOTH legs (await_ready — the ambient form
// fixes sleep_on's always-round-trips-the-queue inconsistency; the
// explicit sleep_on is unchanged). Queue-leg refusals are logged at the
// refusal site with the raw status preserved. `.nothrow()` returns
// the raw status (exceptions-ON ergonomics); the default mode unwinds.
// BOTH modes are a COMPILE error under -fno-exceptions (C++ Flow
// requires exception support program-wide; there is no silent-status
// escape left anywhere).
// ---------------------------------------------------------------------------
namespace detail {

struct ambient_sleep {
  std::chrono::nanoseconds delay;

  explicit ambient_sleep(std::chrono::nanoseconds d) : delay(d) {}

  // Type-level status-branch opt-out — see resume_on::nothrow_awaiter.
  struct nothrow_awaiter;
  nothrow_awaiter nothrow() const noexcept;

  // NEVER ready — the zero-delay fast path
  // previously bypassed the entry cancel-gate through await_ready (which
  // cannot see the promise), letting a cancel-swallowing loop
  // `sleep(0)` forever. await_suspend gates first, THEN resolves
  // zero-delay inline (return false — no park, same cost class).
  bool await_ready() const noexcept { return false; }
  template <typename P>
    requires detail::FlowPromise<P>
  bool await_suspend(std::coroutine_handle<P> h) {
    const auto& ctx =
        static_cast<const detail::promise_flow_context&>(h.promise());
    if (ctx.scope.is_cancelled()) {
      // Entry gate: an ambient verb on a CANCELLED frame
      // scope refuses the await (one atomic load) — this is what bounds a
      // swallowed cancellation to a single synchronous window instead of
      // letting the task sleep past its own cancellation. The unwind's
      // reason snapshots from the scope at the wrapper's catch.
      qleg_.status = AORTA_ERR_CANCELLED;  // settle() reads this (arbiter_
      return false;                        // stays false — nothing armed)
    }
    if (delay.count() <= 0) {
      // Zero/negative delay: resolve inline AFTER the gate (the old
      // await_ready fast path never reached the gate).
      return false;
    }
    if (ctx.rt != nullptr) {
      // Arbiter leg — enabled AND Disabled runtimes both (a Disabled
      // runtime is a valid plane; its arm registry and fallback-executor
      // resume routing work).
      arbiter_ = true;
      rt_ = ctx.rt;
      const uint8_t prio = ctx.priority <= 3 ? ctx.priority : 3;
      // Ambient sleep is same-plane by construction — pass the
      // frame context so a bound frame's timer resume re-enters its
      // domain (no foreign-arm case exists here).
      return core_.arm(ctx.rt, delay, h, prio, &ctx);
    }
    // Executor-only plane: the sleep_on machinery on the context
    // executor (byte-identical arm sequence after the bind).
    qleg_.executor = ctx.exec;
    qleg_.delay = delay;
    const bool parked = qleg_.await_suspend(h);
    if (!parked) {
      // Refusal logged at the refusal site, raw status preserved
      // (NOT_FOUND = stale executor handle; lifecycle codes no-op).
      detail::log_refused_await("flow::sleep", qleg_.status);
    }
    return parked;
  }
  template <typename P>  // teaching fallback
  bool await_suspend(std::coroutine_handle<P>) {
    static_assert(
        detail::dependent_false<P>::value,
        "flow ambient verbs require a flow coroutine: the frame must be "
        "spawned via flow::spawn_on (or awaited from one). Use the "
        "explicit-argument overload (sleep(rt, d) / sleep_on{exec, d}) "
        "outside flow tasks.");
    return false;
  }
  /// Shared settle head (never throws): retire arms / read the delivered
  /// status for whichever leg armed.
  aorta_result_t settle() noexcept {
    return arbiter_ ? core_.settle(rt_) : qleg_.status;
  }
  // Default: unwind on any non-OK delivery (refusals already logged at
  // the site). Dependent -fno-exceptions gate.
  template <typename Unused = void>
  aorta_result_t await_resume() {
    const aorta_result_t st = settle();
#if defined(__cpp_exceptions)
    if (st != AORTA_OK) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // `.nothrow()` is NOT an -fno-exceptions
    // escape hatch — its own await_resume (ambient_sleep::nothrow_awaiter)
    // hits the same belt gate below. There is no in-mode workaround.
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK "
                  "(callbacks/executors/pull subscribers)");
#endif
    return st;
  }

 private:
  detail::arbiter_sleep_core core_{};
  sleep_on qleg_{0, std::chrono::nanoseconds(0)};
  LaneRuntime* rt_ = nullptr;
  bool arbiter_ = false;
};

struct ambient_sleep::nothrow_awaiter {
  ambient_sleep inner;
  bool await_ready() const noexcept { return inner.await_ready(); }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    return inner.await_suspend(h);  // foreign frames hit the teaching gate
  }
  // Belt gate: the .nothrow() opt-out's historical silent-status
  // return under -fno-exceptions is REVOKED — Flow is not on a
  // -fno-exceptions consumer's surface at all, .nothrow()
  // included. Dependent static_assert so mere header inclusion (and a
  // non-instantiated declaration) stays legal.
  template <typename Unused = void>
  aorta_result_t await_resume() noexcept {
#if !defined(__cpp_exceptions)
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "flow::sleep(d).nothrow() is not reachable under "
                  "-fno-exceptions either — build this translation unit "
                  "with exceptions enabled, or use the C++17 baseline "
                  "aorta SDK (callbacks/executors/pull subscribers)");
#endif
    return inner.settle();
  }
};
inline ambient_sleep::nothrow_awaiter ambient_sleep::nothrow()
    const noexcept {
  // Fresh inner from the pre-arm parameters (the armed machinery holds
  // atomics and is deliberately not copyable); prvalue-materialized in
  // the awaiting frame — the inner-by-value pattern.
  return nothrow_awaiter{ambient_sleep(delay)};
}

}  // namespace detail

// The `sleep` overload set — the public entry; awaiter types live in
// `detail` (surface diet).

/// Explicit form (unchanged semantics): arbiter timer on `rt`.
inline detail::arbiter_sleep sleep(LaneRuntime& rt,
                                   std::chrono::nanoseconds d) {
  return detail::arbiter_sleep(rt, d);
}
template <typename Rep, typename Period>
detail::arbiter_sleep sleep(LaneRuntime& rt,
                            std::chrono::duration<Rep, Period> d) {
  return detail::arbiter_sleep(
      rt, std::chrono::duration_cast<std::chrono::nanoseconds>(d));
}

/// Ambient form: machinery picked from the frame's
/// promise context — see ambient_sleep above.
inline detail::ambient_sleep sleep(std::chrono::nanoseconds d) {
  return detail::ambient_sleep(d);
}
template <typename Rep, typename Period>
detail::ambient_sleep sleep(std::chrono::duration<Rep, Period> d) {
  return detail::ambient_sleep(
      std::chrono::duration_cast<std::chrono::nanoseconds>(d));
}

// ---------------------------------------------------------------------------
// join / race over lane tasks — each branch is a wrapper task that
// runs the payload task, stores its value C++-side, then completes its
// arbiter branch (the resolution signal). The enclosing scope owns every
// wrapper (structured: scope close waits for them).
// ---------------------------------------------------------------------------
namespace detail {

template <typename T>
struct ValueSlot {
  std::optional<T> value;
  /// A REAL exception escaping the branch task,
  /// parked here by branch_wrapper before the failure propagates — the
  /// winner's await_resume rethrows it instead of a generic cancel.
  std::exception_ptr error;
};
template <>
struct ValueSlot<void> {
  bool completed = false;
  std::exception_ptr error;
};

/// Resolve guard: a branch RESOLVES on every exit path. A stop-drained
/// wrapper unwinds via cancelled_unwind past the trailing complete —
/// without this the branch never resolves, pending never reaches zero,
/// and the parent frame suspends forever (scope.close() hangs).
struct BranchCompleteGuard {
  uint64_t handle;
  bool done = false;
  void complete_ok() {
    (void)aorta_arbiter_complete(handle, AORTA_OK, nullptr);
    done = true;
  }
  ~BranchCompleteGuard() {
    if (!done) {
      (void)aorta_arbiter_complete(handle, AORTA_ERR_CANCELLED, nullptr);
    }
  }
};

/// Heap-state rule: the wrapper holds a SHARED ref on the combinator's
/// heap state — value slots live there, not in the (possibly already
/// destroyed) awaiter frame. A loser draining long after the winner resumed the
/// parent writes into refcounted memory, never a freed frame.
/// Kind-generic: join() legitimately runs endpoint tasks — the wrapper
/// only ever consumes the payload by direct co_await.
template <typename T, typename K, typename State>
Task<void> branch_wrapper(Task<T, K> payload, std::shared_ptr<State> state,
                          ValueSlot<T>* slot, uint64_t handle) {
  BranchCompleteGuard guard{handle};
#if defined(__cpp_exceptions)
  try {
    if constexpr (std::is_void_v<T>) {
      co_await std::move(payload);
      slot->completed = true;
    } else {
      slot->value.emplace(co_await std::move(payload));
    }
  } catch (const cancelled_unwind&) {
    throw;  // teardown control flow, not a user failure
  } catch (...) {
    // Park the REAL exception on the slot BEFORE the failure propagates
    // The rethrow keeps today's supervision semantics
    // bit-for-bit: the wrapper still fails at its spawned root
    // (ChildFailed fail-fast), the guard still resolves the branch.
    slot->error = std::current_exception();
    throw;
  }
#else
  if constexpr (std::is_void_v<T>) {
    co_await std::move(payload);
    slot->completed = true;
  } else {
    slot->value.emplace(co_await std::move(payload));
  }
#endif
  guard.complete_ok();
  (void)state;  // keeps the state alive across every suspension above
}

/// Ambient combinator door: wraps an explicit
/// combinator awaiter whose {rt, scope} stay UNBOUND until await_suspend,
/// where they resolve from the frame's promise context — then the inner
/// awaiter runs its own (unchanged, hardened) arm sequence. The
/// branch wrappers it spawns inherit {combinator's plane, combinator's
/// scope} exactly as today (for ambient forms these ARE the
/// caller's). Zero extra frames, zero new suspension points; the
/// heap-state / no-this-after-resolution disciplines live untouched
/// inside the inner awaiter.
///
/// Null-context refusal: on an executor-only frame
/// (ctx.rt == nullptr) the arbiter machinery is unavailable — ERROR log
/// at the refusal site, then the standard unwind (the inner awaiter was
/// never armed, so its await_resume delivers the cancelled_unwind).
template <typename Inner>
struct ambient_combinator_awaiter {
  Inner inner;
  const char* surface;

  bool await_ready() const noexcept { return false; }
  template <typename P>
    requires FlowPromise<P>
  bool await_suspend(std::coroutine_handle<P> h) {
    const auto& ctx =
        static_cast<const promise_flow_context&>(h.promise());
    if constexpr (!std::is_same_v<Inner, ::aorta::flow::detail::cancelled_awaiter>) {
      if (ctx.scope.is_cancelled()) {
        // Entry gate (see ambient_sleep): the unarmed
        // inner's await_resume delivers the unwind. cancelled() is the
        // one exclusion — resolving immediately on an already-cancelled
        // scope IS its contract.
        return false;
      }
    }
    if (ctx.rt == nullptr) {
      log_null_rt_refused_await(surface);
      return false;  // resume inline; the unarmed inner unwinds below
    }
    inner.rt = ctx.rt;
    if constexpr (requires { inner.scope; }) {
      inner.scope = ctx.scope;  // offload carries no scope
    }
    return inner.await_suspend(h);
  }
  template <typename P>  // teaching fallback
  bool await_suspend(std::coroutine_handle<P>) {
    static_assert(
        dependent_false<P>::value,
        "flow ambient verbs require a flow coroutine: the frame must be "
        "spawned via flow::spawn_on (or awaited from one). Use the "
        "explicit-argument overload (join/race/timeout/offload with an "
        "explicit LaneRuntime and Scope) outside flow tasks.");
    return false;
  }
  decltype(auto) await_resume() { return inner.await_resume(); }
};

/// Task pack traits: extract T/Kind from a Task<T, K> pack element.
template <typename>
struct task_traits;
template <typename T, typename K>
struct task_traits<Task<T, K>> {
  using value = T;
  using kind = K;
};
template <typename TaskT>
using task_value_t = typename task_traits<std::remove_cvref_t<TaskT>>::value;

}  // namespace detail

/// Await ALL tasks (structured under `scope`); resolves to
/// std::tuple<T0, T1, ...>. N-ary (the binary spelling is the
/// N == 2 case). A teardown delivery unwinds (the
/// default). All mutable state lives in a HEAP block shared with the
/// wrappers (heap-state rule); nothing touches the awaiter after a
/// possibly-last resolution (no-this-after-resolution rule).
namespace detail {

template <typename... TaskTs>
struct join_state {
  static constexpr size_t kN = sizeof...(TaskTs);
  detail::ArbiterFrame frame{};
  uint64_t group = 0;
  uint64_t handles[kN] = {};
  detail::BranchSlot slots[kN] = {};
  std::tuple<detail::ValueSlot<detail::task_value_t<TaskTs>>...> values{};
  ~join_state() {
    if (group != 0) {
      (void)aorta_arbiter_group_release(group);
    }
  }
};

template <typename... TaskTs>
struct join_awaiter {
  static constexpr size_t kN = sizeof...(TaskTs);
  static_assert(kN >= 2, "join needs at least two tasks");
  static_assert(kN <= 255,
                "the arbiter caps a group at 255 branches — use join_all "
                "for large dynamic sets (it refuses >255 loudly)");
  static_assert(
      (!std::is_void_v<detail::task_value_t<TaskTs>> && ...),
      "join over Task<void>: co_await them sequentially, spawn handles "
      "and when_all them, or wrap a value type");

  LaneRuntime* rt;
  Scope scope;
  std::tuple<TaskTs...> tasks;
  aorta_result_t status = AORTA_OK;
  std::shared_ptr<join_state<TaskTs...>> st;

  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    st = std::make_shared<join_state<TaskTs...>>();
    st->frame.rt = rt;
    st->frame.fallback_executor = rt->fallback_executor();
    st->frame.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      st->frame.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // A bound frame's combinator branches INHERIT the
    // domain (correct by default, merely not parallel); a foreign-plane
    // join refuses BEFORE any arm exists. The branch binding is a
    // local OWNING copy — no-this-after-resolution: the parent frame (and
    // its promise) may be destroyed the instant the first spawn publishes.
    SerialDomain branch_serial{};
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, rt)) {
        detail::log_refused_await("flow::join [serial: foreign runtime arm]",
                                  AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      st->frame.fctx = &fctx;
      branch_serial = fctx.serial;
    }
    if (aorta_arbiter_group_new(&detail::ArbiterFrame::resume_cb,
                                &st->frame, kN, &st->group) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    for (size_t i = 0; i < kN; ++i) {
      if (aorta_arbiter_arm(st->group, static_cast<uint32_t>(i),
                            &detail::BranchSlot::complete_cb, &st->slots[i],
                            &st->handles[i]) != AORTA_OK) {
        status = AORTA_ERR_CANCELLED;
        return false;  // nothing published can fire; inline resume
      }
    }
    // No-this-after-resolution: NOTHING below may touch `this` — a
    // spawn-failure complete can be the LAST resolution, whose resume
    // path may destroy the frame (and this awaiter) before control
    // returns here. All state the resume needs lives in `st`; the local
    // copies keep this frame untouched past the first complete.
    auto st_local = st;
    auto* rt_local = rt;
    Scope scope_local = scope;
    [&]<size_t... Is>(std::index_sequence<Is...>) {
      (spawn_branch<Is>(st_local, rt_local, scope_local,
                        std::move(std::get<Is>(tasks)), &branch_serial),
       ...);
    }(std::make_index_sequence<kN>{});
    return true;
  }
  std::tuple<detail::task_value_t<TaskTs>...> await_resume() {
    // The last resolver scheduled this resume AFTER every branch fully
    // resolved (arbiter ordering) — reading the state is safe.
#if defined(__cpp_exceptions)
    if (st != nullptr && detail::plane_torn(st->frame, rt)) {
      // Torn delivery: unwind BEFORE surfacing values
      // or branch failures — a rethrown user exception could be caught in
      // the frame and run continuation code on the teardown thread.
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
    if (st) {
      // Surface a REAL branch failure over the generic unwind.
      std::apply(
          [](auto&... slot) {
            ((slot.error ? std::rethrow_exception(slot.error) : void()), ...);
          },
          st->values);
    }
    const bool complete =
        st != nullptr && std::apply(
                             [](const auto&... slot) {
                               return (slot.value.has_value() && ...);
                             },
                             st->values);
    if (status != AORTA_OK || !complete) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // Teardown/failure delivery here is an UNWIND with no .nothrow()
    // form — without exceptions the empty-slot deref below would be UB.
    static_assert(detail::dependent_false<std::tuple<TaskTs...>>::value,
                  "flow::join() delivers teardown and branch failure by "
                  "unwinding and is not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 baseline aorta SDK (D1).");
#endif
    return std::apply(
        [](auto&... slot) {
          return std::tuple<detail::task_value_t<TaskTs>...>{
              std::move(*slot.value)...};
        },
        st->values);
  }

 private:
  template <size_t I, typename State, typename TaskT>
  static void spawn_branch(const std::shared_ptr<State>& st_local,
                           LaneRuntime* rt_local, const Scope& scope_local,
                           TaskT task, const SerialDomain* serial) {
    auto spawned = detail::spawn_detached(
        *rt_local, scope_local,
        detail::branch_wrapper(std::move(task), st_local,
                               &std::get<I>(st_local->values),
                               st_local->handles[I]),
        serial);
    if (!spawned.ok()) {
      // An invalid/stale scope refuses NOT_FOUND (misuse) — logged
      // before the CANCELLED flatten; a closing scope stays quiet.
      detail::log_refused_await("flow::join", spawned.status);
      (void)aorta_arbiter_complete(st_local->handles[I], AORTA_ERR_CANCELLED,
                                   nullptr);
    }
  }
};

}  // namespace detail

/// `co_await join(rt, scope, t0, t1, ...)` — await all, fail fast. N-ary
/// two or more tasks, mixed value types, tuple result in input
/// order.
///
/// RECORDED EXEMPTION: the scope sits after the plane, NOT last —
/// C++ deduction cannot place a fixed parameter behind the task pack.
///
/// SEMANTICS NOTE (cross-language honesty): this join
/// SPAWNS its branches as wrapper tasks under `scope` and a branch
/// failure cancels the others — mechanically the closest Rust relative
/// is `scope.try_join` (fail-fast), NOT Rust's in-task zero-spawn
/// `scope.join` (which polls both futures inside the calling task and
/// can borrow locals). The shared verb names the user intent — "all
/// results, one await" — not the dispatch mechanics.
///
/// Endpoint tasks (recv/aligned/wait_wave) ARE accepted here, unlike
/// timeout()/race(): join resumes only after EVERY branch resolves, so no
/// parked loser is ever stranded.
template <typename... TaskTs>
  requires(detail::is_flow_task<std::remove_cvref_t<TaskTs>>::value && ...)
detail::join_awaiter<std::remove_cvref_t<TaskTs>...> join(LaneRuntime& rt,
                                                  Scope scope,
                                                  TaskTs... tasks) {
  return detail::join_awaiter<std::remove_cvref_t<TaskTs>...>{
      &rt, std::move(scope), {std::move(tasks)...}};
}

/// `co_await join(t0, t1, ...)` — the AMBIENT form:
/// {rt, scope} resolve from the frame's promise context at await_suspend;
/// the arm sequence and semantics are the explicit join's, unchanged.
/// Kind-generic (endpoint tasks legal, as today). Refuses on
/// executor-only frames (the arbiter machinery needs the
/// runtime object).
template <typename... TaskTs>
  requires(detail::is_flow_task<std::remove_cvref_t<TaskTs>>::value && ...)
detail::ambient_combinator_awaiter<detail::join_awaiter<std::remove_cvref_t<TaskTs>...>>
join(TaskTs... tasks) {
  return {detail::join_awaiter<std::remove_cvref_t<TaskTs>...>{
              nullptr, Scope(), {std::move(tasks)...}},
          "flow::join"};
}

/// First completed task wins (same T both sides); the losers'
/// REGISTRATIONS are revoked immediately (the frame resumes at the last
/// resolution). Losing does NOT cancel the loser: its task keeps running
/// to natural completion, draining structurally under `scope` — writing
/// into the HEAP state, never a destroyed frame.
///
/// CAVEAT (the `select` shape, NOT `select_ok`): race settles on the
/// first COMPLETION — including a branch that completes by FAILING. A
/// fast failure propagates from the branch's spawned root (ChildFailed
/// fail-fast under `scope` cancels the sibling) and the race rethrows
/// it; a slower about-to-succeed sibling never turns an earlier failure
/// into a win. A `race_ok` companion (first SUCCESS wins, failures
/// skipped while a branch remains) is planned.
namespace detail {

template <typename T>
struct race_state {
  detail::ArbiterFrame frame{};
  uint64_t group = 0;
  uint64_t handles[2]{0, 0};
  detail::BranchSlot slots[2]{};
  detail::ValueSlot<T> values[2]{};
  std::atomic<int> winner{-1};
  struct Ctx {
    race_state* state;
    int idx;
  };
  Ctx ctxs[2]{};
  ~race_state() {
    if (group != 0) {
      (void)aorta_arbiter_group_release(group);
    }
  }

  static void complete_cb(void* ctx, aorta_result_t status,
                          const aorta_bytes_view_t* payload,
                          const aorta_flow_completion_t* completion) {
    auto* c = static_cast<Ctx*>(ctx);
    detail::BranchSlot::complete_cb(&c->state->slots[c->idx], status,
                                       payload, completion);
    if (status == AORTA_OK) {
      int expect = -1;
      if (c->state->winner.compare_exchange_strong(
              expect, c->idx, std::memory_order_acq_rel)) {
        // Winner revokes the OTHER registration (trampoline-class).
        (void)aorta_arbiter_revoke(c->state->handles[1 - c->idx]);
      }
    }
  }
};

template <typename T, typename KA = task_kind_regular,
          typename KB = task_kind_regular>
struct race_awaiter {
  // Teaching gate: racing a PARKED endpoint verb strands the loser —
  // it keeps the surface's notify hook parked with no data and no cancel
  // path until executor stop (and the single-awaiter contract then refuses
  // every later recv/aligned/wait_wave on that surface).
  static_assert(
      !is_endpoint_task_v<Task<T, KA>> && !is_endpoint_task_v<Task<T, KB>>,
      "use recv_for / aligned_for / wait_wave_for — wrapping a parked "
      "endpoint verb in timeout()/race() strands the loser branch");

  LaneRuntime* rt;
  Scope scope;
  Task<T, KA> ta;
  Task<T, KB> tb;
  aorta_result_t status = AORTA_OK;
  std::shared_ptr<race_state<T>> st;

  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    st = std::make_shared<race_state<T>>();
    st->frame.rt = rt;
    st->frame.fallback_executor = rt->fallback_executor();
    st->frame.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      st->frame.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // Bound-frame branch inheritance + foreign-plane refusal (see
    // join_awaiter).
    SerialDomain branch_serial{};
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, rt)) {
        detail::log_refused_await("flow::race [serial: foreign runtime arm]",
                                  AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      st->frame.fctx = &fctx;
      branch_serial = fctx.serial;
    }
    if (aorta_arbiter_group_new(&detail::ArbiterFrame::resume_cb,
                                &st->frame, 2, &st->group) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    st->ctxs[0] = {st.get(), 0};
    st->ctxs[1] = {st.get(), 1};
    if (aorta_arbiter_arm(st->group, 0, &race_state<T>::complete_cb,
                          &st->ctxs[0], &st->handles[0]) != AORTA_OK ||
        aorta_arbiter_arm(st->group, 1, &race_state<T>::complete_cb,
                          &st->ctxs[1], &st->handles[1]) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    // No `this` access past the first possible resolution.
    auto st_local = st;
    auto* rt_local = rt;
    Scope scope_local = scope;
    auto sa = detail::spawn_detached(*rt_local, scope_local,
                       detail::branch_wrapper(std::move(ta), st_local,
                                                 &st_local->values[0],
                                                 st_local->handles[0]),
                       &branch_serial);
    if (!sa.ok()) {
      // Invalid/stale scope = NOT_FOUND (misuse) — see join_awaiter.
      detail::log_refused_await("flow::race", sa.status);
      (void)aorta_arbiter_complete(st_local->handles[0],
                                   AORTA_ERR_CANCELLED, nullptr);
    }
    auto sb = detail::spawn_detached(*rt_local, scope_local,
                       detail::branch_wrapper(std::move(tb), st_local,
                                                 &st_local->values[1],
                                                 st_local->handles[1]),
                       &branch_serial);
    if (!sb.ok()) {
      detail::log_refused_await("flow::race", sb.status);
      (void)aorta_arbiter_complete(st_local->handles[1],
                                   AORTA_ERR_CANCELLED, nullptr);
    }
    return true;
  }
  T await_resume() {
    int w = st ? st->winner.load(std::memory_order_acquire) : -1;
#if defined(__cpp_exceptions)
    if (st != nullptr && detail::plane_torn(st->frame, rt)) {
      // Torn delivery: teardown outranks the winner —
      // unwind before touching values or rethrowing branch failures.
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
    if (w < 0 || !st->values[w].value.has_value()) {
      // No clean winner: surface a REAL branch failure over the generic
      // unwind. Race semantics are unchanged — a failed
      // branch never wins while the other can still succeed.
      if (st && st->values[0].error)
        std::rethrow_exception(st->values[0].error);
      if (st && st->values[1].error)
        std::rethrow_exception(st->values[1].error);
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // No-winner delivery is an UNWIND with no .nothrow() form.
    static_assert(detail::dependent_false<T>::value,
                  "flow::race() delivers teardown and branch failure by "
                  "unwinding and is not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 baseline aorta SDK (D1).");
#endif
    return std::move(*st->values[w].value);
  }
};

}  // namespace detail

/// Explicit form — frozen order: plane, operation args,
/// scope LAST.
template <typename T, typename KA, typename KB>
detail::race_awaiter<T, KA, KB> race(LaneRuntime& rt, Task<T, KA> a, Task<T, KB> b,
                             Scope scope) {
  return detail::race_awaiter<T, KA, KB>{&rt, std::move(scope), std::move(a),
                                 std::move(b)};
}

/// `co_await race(a, b)` — the AMBIENT form: {rt, scope}
/// from the frame's context at await_suspend. The endpoint-kind rejection
/// fires exactly as on the explicit form — the inner awaiter carries
/// the class-scope static_assert. Refuses on executor-only frames.
template <typename T, typename KA, typename KB>
detail::ambient_combinator_awaiter<detail::race_awaiter<T, KA, KB>> race(
    Task<T, KA> a, Task<T, KB> b) {
  return {detail::race_awaiter<T, KA, KB>{nullptr, Scope(), std::move(a),
                                  std::move(b)},
          "flow::race"};
}

// ---------------------------------------------------------------------------
// on — hop a closure onto a lane class (compute) and resume with its
// result. `on(blocking, …)` belongs to the CORE offload pool and
// arrives with the facade integration.
// ---------------------------------------------------------------------------
namespace detail {

template <typename F>
struct offload_awaiter {
  LaneRuntime* rt;
  EventPriority lane;
  F fn;
  using R = std::invoke_result_t<F>;
  std::optional<R> result;
  std::exception_ptr error;
  aorta_result_t status = AORTA_OK;
  std::coroutine_handle<> frame_;
  /// The frame's promise context (null for non-flow frames) —
  /// stamped at await_suspend, read by BOTH thunks so a bound frame's
  /// out-and-back re-enters its domain through the router (the
  /// closure itself runs OUTSIDE the domain — that is offload's point).
  const promise_flow_context* fctx = nullptr;

  static void run_thunk(void* ctx) {
    auto* self = static_cast<offload_awaiter*>(ctx);
#if defined(__cpp_exceptions)
    try {
      self->result.emplace(self->fn());
    } catch (...) {
      // C-boundary safety: an exception escaping here
      // would unwind into the lane worker's extern-"C" dispatch
      // (std::terminate); park it for await_resume instead.
      self->error = std::current_exception();
    }
#else
    self->result.emplace(self->fn());
#endif
    // The completion continuation routes through THE router — BOTH
    // thunks (the drop channel too). Unbound: the inline resume is
    // today's behavior, verbatim (offload deliberately continues ON the
    // worker — no auto-rehop). Bound: the continuation re-enters
    // the frame's domain as a member — run resumes plainly in the
    // strand's slice; drop stamps then resumes.
    detail::route_resume(
        self->fctx, self->frame_,
        detail::RouteDelivery{&offload_awaiter::deliver_run_unbound,
                              &offload_awaiter::stamp_teardown_thunk,
                              &offload_awaiter::deliver_run_unbound,
                              &offload_awaiter::deliver_drop_unbound, self});
  }
  static void drop_thunk(void* ctx) {
    auto* self = static_cast<offload_awaiter*>(ctx);
    detail::route_resume(
        self->fctx, self->frame_,
        detail::RouteDelivery{&offload_awaiter::deliver_drop_unbound,
                              &offload_awaiter::stamp_teardown_thunk,
                              &offload_awaiter::deliver_drop_unbound,
                              &offload_awaiter::deliver_drop_unbound, self});
  }

  /// Unbound paths, verbatim pre-router behavior.
  static void deliver_run_unbound(void* ctx) {
    static_cast<offload_awaiter*>(ctx)->frame_.resume();
  }
  static void deliver_drop_unbound(void* ctx) {
    auto* self = static_cast<offload_awaiter*>(ctx);
    offload_awaiter::stamp_teardown_thunk(self);
    self->frame_.resume();
  }
  static void stamp_teardown_thunk(void* ctx) {
    static_cast<offload_awaiter*>(ctx)->status = AORTA_ERR_CANCELLED;
  }

  bool await_ready() const noexcept { return false; }
  // Implementation requirement: promise-TEMPLATED so the bound detour can
  // read the frame's flow context; a type-erased handle cannot. Non-flow
  // promises (the explicit forms are the surface for non-flow
  // coroutines) erase exactly as before.
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    frame_ = h;
    if constexpr (std::is_base_of_v<promise_flow_context, P>) {
      fctx = &static_cast<const promise_flow_context&>(h.promise());
    }
    if (!rt->enabled()) {
      // Rollback: run the closure inside a core executor event.
      auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
        auto* self = static_cast<offload_awaiter*>(ev->handle);
        if (ev->status == AORTA_OK) {
          run_thunk(self);
        } else {
          drop_thunk(self);
        }
      };
      const aorta_result_t post_rc = aorta_executor_post_resume(
          rt->fallback_executor(), trampoline, this, 255);
      if (post_rc != AORTA_OK) {
        // NOT_FOUND (stale executor) is misuse — logged before the
        // CANCELLED flatten; a stopped executor stays quiet.
        detail::log_refused_await("flow::offload", post_rc);
        status = AORTA_ERR_CANCELLED;
        return false;
      }
      return true;
    }
    aorta::lanes::Work w;
    w.run = &run_thunk;
    w.drop = &drop_thunk;
    w.ctx = this;
    // Ordinary budget-GOVERNED post: `on` is compute work, not a resume.
    if (rt->post_work(lane, w) != aorta::lanes::Post::OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    return true;
  }
  R await_resume() {
#if defined(__cpp_exceptions)
    if (error) std::rethrow_exception(error);
    if (status != AORTA_OK || !result.has_value()) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // A dropped/cancelled offload is an UNWIND with no .nothrow() form.
    static_assert(detail::dependent_false<F>::value,
                  "flow::offload() delivers teardown by unwinding and is "
                  "not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 baseline aorta SDK (D1).");
#endif
    return std::move(*result);
  }
};

}  // namespace detail

template <typename F>
detail::offload_awaiter<F> offload(LaneRuntime& rt, EventPriority lane,
                                   F fn) {
  return detail::offload_awaiter<F>{&rt, lane, std::move(fn)};
}

/// `co_await offload(prio, fn)` — the AMBIENT form: the
/// runtime resolves from the frame's context; the lane EventPriority and
/// the closure stay explicit. Resumes on the lane worker — documented, no
/// auto-rehop (with ambient verbs the plane switch is
/// invisible at the call site; hop back explicitly if you need to).
/// Refuses on executor-only frames.
template <typename F>
detail::ambient_combinator_awaiter<detail::offload_awaiter<F>> offload(
    EventPriority lane, F fn) {
  return {detail::offload_awaiter<F>{nullptr, lane, std::move(fn)}, "flow::offload"};
}

// ---------------------------------------------------------------------------
// timeout — a 2-branch group: branch 0 = the task's wrapper, branch 1 =
// the arbiter timer. Task SETTLES (success or failure) → the timer
// registration is revoked (the core sleeper aborts) and the
// frame resumes at once, Rust parity with `Scope::timeout`; timer wins →
// the frame unwinds with the deadline delivery at resume (the task keeps
// draining structurally under `scope`, same loser contract as race).
// Heap state per the heap-state rule (see branch_wrapper).
// ---------------------------------------------------------------------------
namespace detail {

template <typename T>
struct timeout_state {
  detail::ArbiterFrame frame{};
  LaneRuntime* rt = nullptr;
  uint64_t group = 0;
  uint64_t handles[2]{0, 0};
  detail::BranchSlot slots[2]{};
  detail::ValueSlot<T> value{};
  std::atomic<int> winner{-1};
  struct Ctx {
    timeout_state* state;
    int idx;
  };
  Ctx ctxs[2]{};
  ~timeout_state() {
    if (group != 0) {
      (void)aorta_arbiter_group_release(group);
    }
  }

  static void complete_cb(void* ctx, aorta_result_t status,
                          const aorta_bytes_view_t* payload,
                          const aorta_flow_completion_t* completion) {
    auto* c = static_cast<Ctx*>(ctx);
    detail::BranchSlot::complete_cb(&c->state->slots[c->idx], status,
                                       payload, completion);
    // The TASK branch (idx 0) settling — success OR failure — resolves
    // the group: revoke the timer so the caller learns about a failure
    // the moment it happens instead of at the window edge (Rust parity:
    // `Scope::timeout` resolves the instant the task settles). The TIMER
    // branch (idx 1) claims the race only when it actually FIRED (status
    // OK): its CANCELLED self-resolutions on the closing-pool/arm-failure
    // paths must not revoke a still-running task's registration. The CAS
    // keeps the revoke single-shot when both branches settle concurrently.
    if (status == AORTA_OK || c->idx == 0) {
      int expect = -1;
      if (c->state->winner.compare_exchange_strong(
              expect, c->idx, std::memory_order_acq_rel)) {
        (void)aorta_arbiter_revoke(c->state->handles[1 - c->idx]);
      }
    }
    // NOTE: the timer handle's registry retirement happens in
    // await_resume (frame context — the runtime is provably alive
    // there), NOT here: this callback can run on the core timer thread
    // concurrently with ~LaneRuntime, and touching rt through the raw
    // pointer would be a narrow-window UAF.
  }
};

template <typename T, typename K = task_kind_regular>
struct timeout_awaiter {
  // Teaching gate: a timed-out endpoint verb is a stranded loser — it
  // keeps the surface's notify hook parked with no data and no cancel path
  // until executor stop. The endpoint verbs carry their own fused deadline
  // branch instead (same arbiter group, first resolver wins).
  static_assert(!is_endpoint_task_v<Task<T, K>>,
                "use recv_for / aligned_for / wait_wave_for — wrapping a "
                "parked endpoint verb in timeout()/race() strands the loser "
                "branch");

  LaneRuntime* rt;
  Scope scope;
  std::chrono::nanoseconds window;
  Task<T, K> task;
  aorta_result_t status = AORTA_OK;
  std::shared_ptr<timeout_state<T>> st;

  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    st = std::make_shared<timeout_state<T>>();
    st->rt = rt;
    st->frame.rt = rt;
    st->frame.fallback_executor = rt->fallback_executor();
    st->frame.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      st->frame.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // Bound-frame branch inheritance + foreign-plane refusal (see
    // join_awaiter).
    SerialDomain branch_serial{};
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, rt)) {
        detail::log_refused_await(
            "flow::timeout [serial: foreign runtime arm]",
            AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      st->frame.fctx = &fctx;
      branch_serial = fctx.serial;
    }
    if (aorta_arbiter_group_new(&detail::ArbiterFrame::resume_cb,
                                &st->frame, 2, &st->group) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    st->ctxs[0] = {st.get(), 0};
    st->ctxs[1] = {st.get(), 1};
    if (aorta_arbiter_arm(st->group, 0, &timeout_state<T>::complete_cb,
                          &st->ctxs[0], &st->handles[0]) != AORTA_OK ||
        aorta_arbiter_arm(st->group, 1, &timeout_state<T>::complete_cb,
                          &st->ctxs[1], &st->handles[1]) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    // Register the timer handle BEFORE arming the producer (same class as
    // the no-this-after-resolution rule: the timer completing can be the
    // last resolution; nothing may touch `this` or even `st` fields after
    // it fires).
    rt->register_arm(st->handles[1]);
    // No-this-after-resolution: locals only from here.
    auto st_local = st;
    auto* rt_local = rt;
    Scope scope_local = scope;
    auto window_local = window;
    auto spawned = detail::spawn_detached(*rt_local, scope_local,
                            detail::branch_wrapper(std::move(task),
                                                      st_local,
                                                      &st_local->value,
                                                      st_local->handles[0]),
                            &branch_serial);
    if (!spawned.ok()) {
      // Invalid/stale scope = NOT_FOUND (misuse) — see join_awaiter.
      detail::log_refused_await("flow::timeout", spawned.status);
      (void)aorta_arbiter_complete(st_local->handles[0],
                                   AORTA_ERR_CANCELLED, nullptr);
    }
    ArmWindowPin timer_pin;  // arm-window pin: covers gate..recheck
    if (rt_local->enabled() && rt_local->pool().closed()) {
      // Stop-arm gate: don't arm a timer against a closing pool;
      // resolve the timer branch inline instead (the payload wrapper
      // resolves its own branch via the refused spawn above).
      (void)aorta_arbiter_complete(st_local->handles[1],
                                   AORTA_ERR_CANCELLED, nullptr);
    } else if (aorta_timer_arm_arbiter(
                   st_local->handles[1],
                   static_cast<uint64_t>(window_local.count())) != AORTA_OK) {
      (void)aorta_arbiter_complete(st_local->handles[1],
                                   AORTA_ERR_CANCELLED, nullptr);
    } else if (rt_local->enabled() && rt_local->pool().closed()) {
      // close() raced the arm — self-revoke; a lost CAS means a real
      // resolver owns the resume (see the sleep awaiter note).
      (void)aorta_arbiter_revoke(st_local->handles[1]);
    }
    return true;
  }
  T await_resume() {
    int w = st ? st->winner.load(std::memory_order_acquire) : -1;
    if (st && st->rt != nullptr) {
      // Stop-sweep retirement of the timer handle here — frame context,
      // runtime provably alive (its pool just delivered this resume).
      st->rt->deregister_arm(st->handles[1]);
    }
#if defined(__cpp_exceptions)
    if (st != nullptr && detail::plane_torn(st->frame, rt)) {
      // Torn delivery: teardown outranks a task win —
      // unwind before touching the value or rethrowing failures.
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
    if (w != 0 || !st->value.value.has_value()) {
      // A task that FAILED (rather than timed out) reports its real
      // exception, not a deadline unwind. Its settling
      // also revoked the timer branch in complete_cb, so this resume
      // landed promptly — never parked out to the window edge (w == 0
      // with an empty value slot is exactly that settled-by-failure
      // shape).
      if (st && st->value.error) std::rethrow_exception(st->value.error);
      // Timer won (w==1) or nothing resolved cleanly: deadline unwind —
      // the throw site stamps the reason (a per-wait timeout is
      // NOT a scope cancellation; the scope reads live at catch time).
      detail::arm_unwind();
      throw cancelled_unwind{w == 1 ? static_cast<uint8_t>(CancelReason::kDeadlineExceeded) : uint8_t{0}};
    }
#else
    // The deadline delivery is an UNWIND with no .nothrow() form.
    static_assert(detail::dependent_false<T>::value,
                  "flow::timeout() delivers the deadline and teardown by "
                  "unwinding and is not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 "
                  "baseline aorta SDK (D1).");
#endif
    return std::move(*st->value.value);
  }
};

}  // namespace detail

/// Explicit form — frozen order (scope LAST).
template <typename T, typename K>
detail::timeout_awaiter<T, K> timeout(LaneRuntime& rt,
                              std::chrono::nanoseconds window,
                              Task<T, K> task, Scope scope) {
  static_assert(!std::is_void_v<T>,
                "timeout over Task<void>: wrap a value type");
  return detail::timeout_awaiter<T, K>{&rt, std::move(scope), window,
                               std::move(task)};
}

/// `co_await timeout(window, task)` — the AMBIENT form:
/// {rt, scope} from the frame's context at await_suspend; the window and
/// the task stay explicit. Endpoint-kind rejection fires exactly as
/// on the explicit form. Refuses on executor-only frames.
template <typename T, typename K>
detail::ambient_combinator_awaiter<detail::timeout_awaiter<T, K>> timeout(
    std::chrono::nanoseconds window, Task<T, K> task) {
  static_assert(!std::is_void_v<T>,
                "timeout over Task<void>: wrap a value type");
  return {detail::timeout_awaiter<T, K>{nullptr, Scope(), window, std::move(task)},
          "flow::timeout"};
}

// ---------------------------------------------------------------------------
// join_all — the dynamic-N join: await a whole vector of same-typed
// tasks; resolves to std::vector<T> in input order. Same spawned-wrapper
// mechanics, fail-fast semantics and unwind contract as join().
// ---------------------------------------------------------------------------
namespace detail {

template <typename T>
struct join_all_state {
  detail::ArbiterFrame frame{};
  uint64_t group = 0;
  // Sized ONCE at arm time and never resized after: the wrappers hold raw
  // pointers into these (the vector<atomic> members are why — they are
  // in-place constructed, never moved).
  std::vector<uint64_t> handles;
  std::vector<detail::BranchSlot> slots;
  std::vector<detail::ValueSlot<T>> values;
  ~join_all_state() {
    if (group != 0) {
      (void)aorta_arbiter_group_release(group);
    }
  }
};

template <typename T, typename K = task_kind_regular>
struct join_all_awaiter {
  static_assert(!std::is_void_v<T>,
                "join_all over Task<void>: spawn handles and when_all them");

  LaneRuntime* rt;
  Scope scope;
  std::vector<Task<T, K>> tasks;
  aorta_result_t status = AORTA_OK;
  bool empty = false;
  std::shared_ptr<join_all_state<T>> st;

  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    const size_t n = tasks.size();
    if (n == 0) {
      empty = true;
      return false;  // resolve inline: an empty join_all is vacuously done
    }
    if (n > 255) {
      // The arbiter caps a group at 255 branches: refuse loudly —
      // nothing spawns, the unwind carries it.
      detail::log_refused_await("flow::join_all", AORTA_ERR_INVALID_ARGUMENT);
      status = AORTA_ERR_INVALID_ARGUMENT;
      return false;
    }
    st = std::make_shared<join_all_state<T>>();
    st->frame.rt = rt;
    st->frame.fallback_executor = rt->fallback_executor();
    st->frame.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      st->frame.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    st->handles.assign(n, 0);
    st->slots = std::vector<detail::BranchSlot>(n);
    st->values = std::vector<detail::ValueSlot<T>>(n);
    // Bound-frame branch inheritance + foreign-plane refusal (see
    // join_awaiter).
    SerialDomain branch_serial{};
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, rt)) {
        detail::log_refused_await(
            "flow::join_all [serial: foreign runtime arm]",
            AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      st->frame.fctx = &fctx;
      branch_serial = fctx.serial;
    }
    if (aorta_arbiter_group_new(&detail::ArbiterFrame::resume_cb,
                                &st->frame, n, &st->group) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    for (size_t i = 0; i < n; ++i) {
      if (aorta_arbiter_arm(st->group, static_cast<uint32_t>(i),
                            &detail::BranchSlot::complete_cb, &st->slots[i],
                            &st->handles[i]) != AORTA_OK) {
        status = AORTA_ERR_CANCELLED;
        return false;  // nothing published can fire; inline resume
      }
    }
    // Locals only past the first spawn — see join_awaiter.
    auto st_local = st;
    auto* rt_local = rt;
    Scope scope_local = scope;
    for (size_t i = 0; i < n; ++i) {
      auto spawned = detail::spawn_detached(
          *rt_local, scope_local,
          detail::branch_wrapper(std::move(tasks[i]), st_local,
                                 &st_local->values[i], st_local->handles[i]),
          &branch_serial);
      if (!spawned.ok()) {
        detail::log_refused_await("flow::join_all", spawned.status);
        (void)aorta_arbiter_complete(st_local->handles[i],
                                     AORTA_ERR_CANCELLED, nullptr);
      }
    }
    return true;
  }
  std::vector<T> await_resume() {
    if (empty) {
      return {};
    }
#if defined(__cpp_exceptions)
    if (st != nullptr && detail::plane_torn(st->frame, rt)) {
      // Torn delivery: unwind before values/failures
      // — see join_awaiter.
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
    if (st) {
      for (auto& slot : st->values) {
        if (slot.error) std::rethrow_exception(slot.error);
      }
    }
    bool complete = st != nullptr;
    if (complete) {
      for (auto& slot : st->values) {
        complete = complete && slot.value.has_value();
      }
    }
    if (status != AORTA_OK || !complete) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    static_assert(detail::dependent_false<T>::value,
                  "flow::join_all() delivers teardown and branch failure by "
                  "unwinding and is not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 "
                  "baseline aorta SDK (D1).");
#endif
    std::vector<T> out;
    out.reserve(st->values.size());
    for (auto& slot : st->values) {
      out.push_back(std::move(*slot.value));
    }
    return out;
  }
};

}  // namespace detail

/// `co_await join_all(rt, std::move(tasks), scope)` — dynamic-N join
/// (frozen order: scope LAST).
template <typename T, typename K>
detail::join_all_awaiter<T, K> join_all(LaneRuntime& rt,
                                std::vector<Task<T, K>> tasks, Scope scope) {
  return detail::join_all_awaiter<T, K>{&rt, std::move(scope), std::move(tasks)};
}

/// `co_await join_all(std::move(tasks))` — the AMBIENT form: {rt, scope}
/// from the frame's context. Refuses on executor-only frames.
template <typename T, typename K>
detail::ambient_combinator_awaiter<detail::join_all_awaiter<T, K>> join_all(
    std::vector<Task<T, K>> tasks) {
  return {detail::join_all_awaiter<T, K>{nullptr, Scope(), std::move(tasks)},
          "flow::join_all"};
}

// ---------------------------------------------------------------------------
// when_all / when_any over TaskHandles (Rust combinators.rs
// parity). The handles are ALREADY spawned — these await completions, they
// spawn nothing. Each handle's state arms one branch of a single N-branch
// group (the same publication protocol as `co_await handle`).
//
// C++ delta vs Rust (documented, honest): Rust's forms hard-abort the
// not-yet-needed tasks (when_all on first failure, when_any's losers); a
// C++ frame is never destroyed while suspended, so the siblings/
// losers keep DRAINING structurally under their scope — fail-fast still
// propagates through scope ChildFailed (an unsupervised failing child has
// already cancelled its scope), and when_any resumes at the first clean
// completion without waiting for the drain (its loser REGISTRATIONS are
// revoked immediately, the race contract).
// ---------------------------------------------------------------------------
namespace detail {

template <typename T>
struct when_state {
  ArbiterFrame frame{};
  uint64_t group = 0;
  bool any_mode = false;
  std::vector<std::shared_ptr<handle_state<T>>> states;
  std::vector<uint64_t> branches;
  std::vector<BranchSlot> slots;  // in-place constructed, never resized
  std::atomic<int> winner{-1};    // when_any only
  struct Ctx {
    when_state* state;
    size_t idx;
  };
  std::vector<Ctx> ctxs;
  ~when_state() {
    if (group != 0) {
      (void)aorta_arbiter_group_release(group);
    }
  }

  static void complete_cb(void* ctx, aorta_result_t status,
                          const aorta_bytes_view_t* payload,
                          const aorta_flow_completion_t* completion) {
    auto* c = static_cast<Ctx*>(ctx);
    auto* ws = c->state;
    BranchSlot::complete_cb(&ws->slots[c->idx], status, payload, completion);
    if (!ws->any_mode || status != AORTA_OK) {
      return;  // when_all never claims; a revoked branch never wins
    }
    // when_any: the first branch whose CHILD DELIVERED A CLEAN OUTCOME
    // claims the race and revokes the other registrations (their tasks
    // keep draining — the race loser contract). The publication protocol
    // guarantees child_status is visible here (armed/done are seq_cst and
    // this callback runs after one of the two sides observed the other).
    const auto cs = static_cast<aorta_result_t>(
        ws->states[c->idx]->child_status.load(std::memory_order_acquire));
    if (cs != AORTA_OK) {
      return;  // a stopped child resolves its branch but never wins
    }
    int expect = -1;
    if (ws->winner.compare_exchange_strong(expect, static_cast<int>(c->idx),
                                           std::memory_order_acq_rel)) {
      for (size_t i = 0; i < ws->branches.size(); ++i) {
        if (i != c->idx) {
          (void)aorta_arbiter_revoke(ws->branches[i]);
        }
      }
    }
  }
};

/// The shared when_* arm sequence: one N-branch group over the handles'
/// states. Returns false with `status` set on a pre-publication refusal
/// (inline resume); true when parked.
template <typename T, typename P>
bool when_arm(std::shared_ptr<when_state<T>>& st, bool any_mode,
              LaneRuntime* rt, std::vector<TaskHandle<T>>& handles,
              std::coroutine_handle<P> h, aorta_result_t& status) {
  const size_t n = handles.size();
  st = std::make_shared<when_state<T>>();
  st->any_mode = any_mode;
  st->frame.rt = rt;
  st->frame.fallback_executor = rt->fallback_executor();
  st->frame.frame = h;
  if constexpr (std::is_base_of_v<promise_priority, P>) {
    st->frame.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
  }
  // when_* consume already-spawned handles — they neither add
  // nor remove binding; only the AWAITING frame's own resume re-enters
  // its domain (stamp + foreign-plane refusal, see join_awaiter).
  if constexpr (std::is_base_of_v<promise_flow_context, P>) {
    const auto& fctx =
        static_cast<const promise_flow_context&>(h.promise());
    if (serial_foreign_arm(fctx, rt)) {
      log_refused_await("flow::when_* [serial: foreign runtime arm]",
                        AORTA_ERR_INVALID_ARGUMENT);
      status = AORTA_ERR_INVALID_ARGUMENT;
      return false;
    }
    st->frame.fctx = &fctx;
  }
  // NOTE: the handle states are consumed HERE, before
  // the refusal gates below — deliberate. The handles were moved into the
  // awaiter, so on any refusal the unwind destroys them either way; the
  // children keep draining structurally under their scope and their
  // outcomes are unrecoverable on the teardown-refusal path BY DESIGN
  // (the >255/empty misuse gates in when_all/when_any run before any
  // consumption and do preserve the caller's handles).
  st->states.resize(n);
  for (size_t i = 0; i < n; ++i) {
    st->states[i] = handle_access::take_state(handles[i]);
  }
  st->branches.assign(n, 0);
  st->slots = std::vector<BranchSlot>(n);
  st->ctxs = std::vector<typename when_state<T>::Ctx>(n);
  ArmWindowPin pin;
  if (rt->enabled() && rt->pool().closed()) {
    status = AORTA_ERR_CANCELLED;
    return false;
  }
  if (aorta_arbiter_group_new(&ArbiterFrame::resume_cb, &st->frame, n,
                              &st->group) != AORTA_OK) {
    status = AORTA_ERR_CANCELLED;
    return false;
  }
  for (size_t i = 0; i < n; ++i) {
    st->ctxs[i] = {st.get(), i};
    if (aorta_arbiter_arm(st->group, static_cast<uint32_t>(i),
                          &when_state<T>::complete_cb, &st->ctxs[i],
                          &st->branches[i]) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;  // nothing published or registered yet; inline
                     // resume
    }
  }
  // Stop-sweep registration: the children
  // may live on OTHER runtimes — if THIS awaiting runtime tears down
  // first, its sweep must revoke these branches, or a late child
  // completion would resume through a dangling ArbiterFrame::rt. The
  // awaiter retires them in await_resume.
  //
  // No-this-after-resolution: the FIRST register_arm is already a
  // publication — the sweep can revoke it (and, for it being the group's
  // only live branch or the whole set, resume and destroy the awaiter)
  // while this function still runs. LOCALS ONLY from here: `st` and `rt`
  // are the awaiter's members, copied before the first publication.
  auto st_local = st;
  auto* rt_local = rt;
  for (size_t i = 0; i < n; ++i) {
    rt_local->register_arm(st_local->branches[i]);
  }
  for (size_t i = 0; i < n; ++i) {
    arm_handle_completion(st_local->states[i].get(), st_local->branches[i]);
  }
  if (rt_local->enabled() && rt_local->pool().closed()) {
    // close() raced the publication — self-revoke everything armed; a
    // lost CAS means a real resolver owns that branch.
    for (size_t i = 0; i < n; ++i) {
      (void)aorta_arbiter_revoke(st_local->branches[i]);
    }
  }
  return true;
}

/// The when_* await_resume head: retire every branch from the awaiting
/// runtime's stop-sweep registry (frame context — the runtime is provably
/// alive here). Runs on EVERY resume path, including teardown unwinds.
template <typename T>
inline void when_retire(LaneRuntime* rt,
                        const std::shared_ptr<when_state<T>>& st) {
  if (rt == nullptr || st == nullptr) {
    return;
  }
  for (uint64_t branch : st->branches) {
    rt->deregister_arm(branch);
  }
}

}  // namespace detail

/// Await EVERY handle; `Outcome<std::vector<T>>` in input order.
/// Error pick when not all delivered clean: fault outranks cancel (the
/// fault is what tripped scope fail-fast — the resulting sibling cancels
/// are noise); Rust returns the first error it polls, same intent.
/// A teardown of the WAIT itself (stop sweep / closing pool) unwinds.
namespace detail {

template <typename T>
struct when_all_awaiter {
  LaneRuntime* rt;
  std::vector<TaskHandle<T>> handles;
  aorta_result_t status = AORTA_OK;
  bool empty = false;
  std::shared_ptr<detail::when_state<T>> st;

  using result_t = std::conditional_t<std::is_void_v<T>, Outcome<void>,
                                      Outcome<std::vector<T>>>;

  bool await_ready() const noexcept { return false; }
  template <typename P>
    requires detail::FlowPromise<P>
  bool await_suspend(std::coroutine_handle<P> h) {
    if (handles.empty()) {
      empty = true;
      return false;
    }
    if (handles.size() > 255) {
      // The arbiter caps a group at 255 branches — refuse loudly BEFORE
      // consuming any handle.
      detail::log_refused_await("flow::when_all", AORTA_ERR_INVALID_ARGUMENT);
      status = AORTA_ERR_INVALID_ARGUMENT;
      return false;
    }
    return detail::when_arm(st, /*any_mode=*/false, rt, handles, h, status);
  }
  template <typename P>  // teaching fallback
  bool await_suspend(std::coroutine_handle<P>) {
    static_assert(
        detail::dependent_false<P>::value,
        "flow ambient verbs require a flow coroutine: the frame must be "
        "spawned via flow::spawn_on (or awaited from one). Use the "
        "explicit-argument overload (when_all(rt, handles)) outside flow "
        "tasks.");
    return false;
  }
  template <typename Unused = void>
  result_t await_resume() {
    detail::when_retire(rt, st);  // sweep pairing: every resume path
    if (empty) {
      if constexpr (std::is_void_v<T>) {
        return Outcome<void>::Value();
      } else {
        return Outcome<std::vector<T>>::Value({});
      }
    }
#if defined(__cpp_exceptions)
    if (status != AORTA_OK) {
      // Misuse refusal (>255 arbiter cap) reports EXPLICIT — same
      // programmatic-misuse convention as when_any's empty/cap legs.
      detail::arm_unwind();
      throw cancelled_unwind{static_cast<uint8_t>(CancelReason::kExplicit)};
    }
    if (st == nullptr || detail::plane_torn(st->frame, rt)) {
      // The plane_torn leg: a torn delivery with
      // every slot OK must unwind, not report outcomes — see cancelled().
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
    // The error pick keeps the FULL Stopped vocabulary — the
    // offending child's build_stopped() (fault code+text, exact cancel
    // reason), never a bare code. Fault outranks cancel, as before.
    int first_cancel = -1;
    int first_fault = -1;
    aorta_result_t fault_cs = AORTA_OK;
    aorta_result_t cancel_cs = AORTA_OK;
    for (size_t i = 0; i < st->branches.size(); ++i) {
      if (st->slots[i].status.load(std::memory_order_acquire) != AORTA_OK) {
        // The BRANCH resolved without a child delivery: the wait itself
        // was torn down — teardown outranks results.
        detail::arm_unwind();
        throw cancelled_unwind{};
      }
      const auto cs = static_cast<aorta_result_t>(
          st->states[i]->child_status.load(std::memory_order_acquire));
      if (cs == AORTA_OK) {
        continue;
      }
      if (detail::classify_stop(cs) == StopKind::kFault) {
        if (first_fault < 0) {
          first_fault = static_cast<int>(i);
          fault_cs = cs;
        }
      } else if (first_cancel < 0) {
        first_cancel = static_cast<int>(i);
        cancel_cs = cs;
      }
    }
    if (first_fault >= 0) {
      return result_t::Stop(st->states[first_fault]->build_stopped(fault_cs));
    }
    if (first_cancel >= 0) {
      return result_t::Stop(
          st->states[first_cancel]->build_stopped(cancel_cs));
    }
    if constexpr (std::is_void_v<T>) {
      return Outcome<void>::Value();
    } else {
      std::vector<T> values;
      values.reserve(st->states.size());
      for (auto& hs : st->states) {
        values.push_back(std::move(*hs->value));
      }
      return Outcome<std::vector<T>>::Value(std::move(values));
    }
#else
    static_assert(detail::dependent_false<Unused>::value,
                  "flow::when_all() delivers teardown by unwinding and is "
                  "not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 "
                  "baseline aorta SDK (D1).");
    return result_t{};
#endif
  }
};

/// Await the FIRST cleanly-completing handle; `Outcome<pair<index, T>>`
/// (`Outcome<size_t>` for void handles). Stopped children never win; when
/// every child stopped, the LAST stop code reports (Rust last_error
/// parity). Losers: registrations revoked immediately; the loser TASKS
/// keep draining structurally under their scope (C++ race contract — no
/// hard abort; the cooperative-cancel + grace upgrade is a planned
/// follow-up). COST NOTE: the winner's completion callback revokes
/// the N-1 loser registrations SYNCHRONOUSLY on the completing thread
/// (registry lookups + locks) — for large N that is a scheduling spike
/// on that lane worker/spin thread; keep when_any sets small.
template <typename T>
struct when_any_awaiter {
  LaneRuntime* rt;
  std::vector<TaskHandle<T>> handles;
  aorta_result_t status = AORTA_OK;
  bool empty = false;
  std::shared_ptr<detail::when_state<T>> st;

  using result_t = std::conditional_t<std::is_void_v<T>, Outcome<size_t>,
                                      Outcome<std::pair<size_t, T>>>;

  bool await_ready() const noexcept { return false; }
  template <typename P>
    requires detail::FlowPromise<P>
  bool await_suspend(std::coroutine_handle<P> h) {
    if (handles.empty()) {
      // An empty when_any can never produce a winner: refuse loudly
      // rather than park forever (programmatic misuse).
      detail::log_refused_await("flow::when_any", AORTA_ERR_INVALID_ARGUMENT);
      empty = true;
      return false;
    }
    if (handles.size() > 255) {
      // Arbiter group cap — refuse BEFORE consuming any handle.
      detail::log_refused_await("flow::when_any", AORTA_ERR_INVALID_ARGUMENT);
      status = AORTA_ERR_INVALID_ARGUMENT;
      return false;
    }
    return detail::when_arm(st, /*any_mode=*/true, rt, handles, h, status);
  }
  template <typename P>  // teaching fallback
  bool await_suspend(std::coroutine_handle<P>) {
    static_assert(
        detail::dependent_false<P>::value,
        "flow ambient verbs require a flow coroutine: the frame must be "
        "spawned via flow::spawn_on (or awaited from one). Use the "
        "explicit-argument overload (when_any(rt, handles)) outside flow "
        "tasks.");
    return false;
  }
  template <typename Unused = void>
  result_t await_resume() {
    detail::when_retire(rt, st);  // sweep pairing: every resume path
#if defined(__cpp_exceptions)
    if (empty || status != AORTA_OK) {
      // Misuse refusal (empty vector / >255 cap) reports EXPLICIT —
      // Rust parity: when_any(vec![]) == Err(Stopped::cancelled(Explicit))
      // (core/src/flow/combinators.rs). Leaving the default 0 here let
      // build_stopped fold a programmatic misuse into "shutdown" (the
      // when_any_empty conformance case).
      detail::arm_unwind();
      throw cancelled_unwind{static_cast<uint8_t>(CancelReason::kExplicit)};
    }
    if (st == nullptr || detail::plane_torn(st->frame, rt)) {
      // plane_torn leg: teardown outranks the winner.
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
    const int w = st->winner.load(std::memory_order_acquire);
    if (w >= 0) {
      if constexpr (std::is_void_v<T>) {
        return Outcome<size_t>::Value(static_cast<size_t>(w));
      } else {
        return Outcome<std::pair<size_t, T>>::Value(
            {static_cast<size_t>(w),
             std::move(*st->states[static_cast<size_t>(w)]->value)});
      }
    }
    // No winner. Distinguish "every child stopped" (an OUTCOME) from
    // "the wait was torn down" (unwind): a stopped child still
    // DELIVERED (its branch status is OK); a revoked branch did not.
    int last_stop = -1;
    aorta_result_t last_cs = AORTA_OK;
    for (size_t i = 0; i < st->branches.size(); ++i) {
      if (st->slots[i].status.load(std::memory_order_acquire) != AORTA_OK) {
        detail::arm_unwind();
        throw cancelled_unwind{};
      }
      const auto cs = static_cast<aorta_result_t>(
          st->states[i]->child_status.load(std::memory_order_acquire));
      if (cs != AORTA_OK) {
        last_stop = static_cast<int>(i);
        last_cs = cs;
      }
    }
    // The LAST stop reports with its full vocabulary (Rust
    // last_error parity); the every-child-stopped fallback keeps the old
    // generic-cancel shape.
    if (last_stop >= 0) {
      return result_t::Stop(st->states[last_stop]->build_stopped(last_cs));
    }
    return result_t::Stop(Stopped::Cancelled(CancelReason::kExplicit));
#else
    static_assert(detail::dependent_false<Unused>::value,
                  "flow::when_any() delivers teardown by unwinding and is "
                  "not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 "
                  "baseline aorta SDK (D1).");
    return result_t{};
#endif
  }
};

}  // namespace detail

/// Explicit forms: `co_await when_all(rt, std::move(handles))` /
/// `co_await when_any(rt, std::move(handles))`.
template <typename T>
detail::when_all_awaiter<T> when_all(LaneRuntime& rt,
                             std::vector<TaskHandle<T>> handles) {
  return detail::when_all_awaiter<T>{&rt, std::move(handles)};
}
template <typename T>
detail::when_any_awaiter<T> when_any(LaneRuntime& rt,
                             std::vector<TaskHandle<T>> handles) {
  return detail::when_any_awaiter<T>{&rt, std::move(handles)};
}

/// Ambient forms (the golden path): the runtime resolves from the frame's
/// promise context. Refuses on executor-only frames.
template <typename T>
detail::ambient_combinator_awaiter<detail::when_all_awaiter<T>> when_all(
    std::vector<TaskHandle<T>> handles) {
  return {detail::when_all_awaiter<T>{nullptr, std::move(handles)}, "flow::when_all"};
}
template <typename T>
detail::ambient_combinator_awaiter<detail::when_any_awaiter<T>> when_any(
    std::vector<TaskHandle<T>> handles) {
  return {detail::when_any_awaiter<T>{nullptr, std::move(handles)}, "flow::when_any"};
}

// ---------------------------------------------------------------------------
// cancelled — the cooperative cancellation wait (Rust
// `scope.cancelled().await` parity). Parks on an arbiter branch that the
// core-side scope watcher resolves when the scope's cancel token fires
// (`aorta_scope_arm_arbiter`); returning normally MEANS "the scope is
// cancelled" — run your cleanup, then let the next verb unwind. A
// teardown of the wait itself (stop sweep, closing pool) unwinds instead;
// the two resolutions are told apart by the scope's own flag, and a TORN
// PLANE outranks the flag: when the pool is closed by the time the frame
// resumes, the wait unwinds EVEN IF the scope also reads cancelled —
// user cleanup never runs on a stop-drain/destructor thread. Cleanup
// that must run through teardown belongs in destructors.
//
//   co_await flow::cancelled();            // ambient: the frame's scope
//   co_await flow::cancelled(rt, scope);   // explicit: any scope
// ---------------------------------------------------------------------------
namespace detail {

struct cancelled_awaiter {
  LaneRuntime* rt;
  Scope scope;
  aorta_result_t status = AORTA_OK;
  uint64_t group_ = 0;
  uint64_t handle_ = 0;
  detail::ArbiterFrame frame_{};
  detail::BranchSlot slot_{};

  bool await_ready() const noexcept {
    // Fast path (explicit form; the ambient wrapper binds scope later and
    // its inner suspend below covers it): already cancelled — no park.
    return scope.valid() && scope.is_cancelled();
  }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    if (!scope.valid()) {
      // Invalid scope reads as cancelled (fail-safe) — resolve now.
      return false;
    }
    if (scope.is_cancelled()) {
      return false;  // the ambient path lands here when already cancelled
    }
    frame_.rt = rt;
    frame_.fallback_executor = rt->fallback_executor();
    frame_.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      frame_.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // AFTER the ready fast paths (an already-cancelled
    // read never arms and stays legal) — a bound frame may not ARM a
    // foreign-plane scope watcher; same-plane bound frames stamp so the
    // watcher's resume re-enters the domain.
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, rt)) {
        detail::log_refused_await(
            "flow::cancelled [serial: foreign runtime arm]",
            AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      frame_.fctx = &fctx;
    }
    // The hardened arm sequence (arbiter_sleep_core's, with the
    // scope watcher in place of the timer — the SAME core-side producer
    // slot, so revoke aborts the watcher identically).
    ArmWindowPin pin;
    if (rt->enabled() && rt->pool().closed()) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    if (aorta_arbiter_group_new(&detail::ArbiterFrame::resume_cb, &frame_, 1,
                                &group_) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    uint64_t handle = 0;
    if (aorta_arbiter_arm(group_, 0, &detail::BranchSlot::complete_cb,
                          &slot_, &handle) != AORTA_OK) {
      status = AORTA_ERR_CANCELLED;
      return false;  // group released by the awaiter's own release below
    }
    // EVERY awaiter write happens BEFORE any publication:
    // register_arm exposes the branch to
    // the stop sweep and aorta_scope_arm_arbiter spawns the core
    // watcher — either can complete-and-resume (and destroy this frame,
    // and this awaiter with it) the instant it happens. handle_ is final
    // here; everything below runs on LOCALS.
    handle_ = handle;
    LaneRuntime* rt_local = rt;
    const uint64_t scope_handle_local = scope.handle();
    rt_local->register_arm(handle);  // publication #1: the stop sweep
                                     // (it must be able to abort
                                     // the watcher)
    const aorta_result_t arm_rc =
        aorta_scope_arm_arbiter(handle, scope_handle_local);
    if (arm_rc != AORTA_OK) {
      // NOT_FOUND = stale scope handle: misuse, ERROR-logged (lifecycle
      // codes no-op inside the helper). The branch is already PUBLISHED —
      // the sweep may have revoked (and resumed the frame); the awaiter
      // may be gone. Resolve by REVOKE-AND-PARK (CAS-idempotent — if a
      // sweep beat us, it owns the resume): the frame resumes through
      // the arbiter exactly once, settles slot=CANCELLED and unwinds.
      detail::log_refused_await("flow::cancelled", arm_rc);
      (void)aorta_arbiter_revoke(handle);
      return true;
    }
    if (rt_local->enabled() && rt_local->pool().closed()) {
      (void)aorta_arbiter_revoke(handle);  // close raced the arm
    }
    return true;
  }
  template <typename Unused = void>
  void await_resume() {
    if (group_ != 0) {
      status = static_cast<aorta_result_t>(
          slot_.status.load(std::memory_order_acquire));
      rt->deregister_arm(handle_);
      (void)aorta_arbiter_group_release(group_);
      group_ = 0;
    }
#if defined(__cpp_exceptions)
    // The watcher resolves the branch CANCELLED when the token fires —
    // for THIS verb that is the success signal. A resolution without the
    // scope flag set is a teardown/refusal delivery: unwind. A TORN
    // PLANE also unwinds even when the flag reads cancelled: a stop
    // sweep may have revoked this
    // wait, with the cancel landing just after — running user cleanup on
    // the stop-drain/destructor/timer thread must not happen. Tornness
    // is judged at the DELIVERY layer (frame_.torn — covers the Disabled
    // rollback plane, whose executor stop is invisible to a pool probe)
    // plus the enabled plane's pool state.
    const bool torn = detail::plane_torn(frame_, rt);
    if (!torn && scope.valid() && scope.is_cancelled()) {
      return;
    }
    detail::arm_unwind();
    throw cancelled_unwind{};
#else
    static_assert(detail::dependent_false<Unused>::value,
                  "flow::cancelled() delivers teardown by unwinding and is "
                  "not available under -fno-exceptions."
                  " Build with exceptions enabled, or use the C++17 "
                  "baseline aorta SDK (D1).");
#endif
  }
};

}  // namespace detail

/// Explicit form: wait for `scope`'s cancellation on `rt`'s plane.
inline detail::cancelled_awaiter cancelled(LaneRuntime& rt, Scope scope) {
  return detail::cancelled_awaiter{&rt, std::move(scope)};
}

/// Ambient form: wait for the FRAME's own scope. Refuses on executor-only
/// frames.
inline detail::ambient_combinator_awaiter<detail::cancelled_awaiter> cancelled() {
  return {detail::cancelled_awaiter{nullptr, Scope()}, "flow::cancelled"};
}

#if defined(__cpp_exceptions)
/// End THIS task now, reporting CANCELLED — the public
/// spelling for "observed the cancel (e.g. after `co_await cancelled()`),
/// cleanup done, stop here" (returning normally would complete the task's
/// handle OK). Unwinds the frame; the outcome's reason snapshots from the
/// frame's scope at the wrapper's catch — the exact byte the scope was
/// cancelled with. Mid-task cancellation needs no spelling: every ambient
/// verb entry-gates on the cancelled scope (bounded window).
[[noreturn]] inline void stop_now() {
  detail::arm_unwind();
  throw cancelled_unwind{};
}
#endif  // stop_now() is unwind-only — absent under -fno-exceptions
        // (Flow there is compile-gated anyway).

}  // namespace flow
}  // namespace aorta
