#pragma once

// Aorta Flow — the opt-in C++20 coroutine surface.
//
// This header is an ADDITIVE layer above the C++17 baseline SDK: it is only
// reachable through the separate `-std=c++20` Bazel target
// (`//cpp:aorta_cpp20_flow`); the repo-wide C++ standard stays C++17 and
// nothing in the base SDK includes this file. The boundary underneath is the
// C ABI, so mixing the two targets in one binary is ABI-safe.
//
// Model: no future crosses the FFI. A coroutine suspends by
// posting its own `coroutine_handle` through `aorta_executor_post_resume`;
// the EXECUTOR — never the caller — resumes it, which is what makes re-entry
// from a coroutine body structurally legal (no reentrancy fence on this
// path). Cancellation is always "resume and observe the flag" — a
// suspended frame is never destroy()ed from the outside.
//
// Residual C++ tax (honest): no borrow checker — a dangling capture
// across co_await is UB. The ONE remaining discipline: a capturing lambda
// may only be used as a coroutine THROUGH co_invoke/spawn_with (the
// wrapper frame owns the closure); raw capturing-lambda coroutines dangle.
//
//   // WRONG — the closure dies with the statement; the frame lives on:
//   auto bad = [&x]() -> Task<void> { co_await resume(); use(x); ... };
//
//   // RIGHT (named fn — ambient context: no rt/exec/scope params;
//   // the promise carries them, this_coro::scope reads the scope):
//   Task<void> body() { co_await resume(); ... }
//   // RIGHT (lambda via the wrapper; explicit args only at the spawn):
//   spawn_with(exec, scope, [x](Scope s) -> Task<void> { ... });
//
// Engineered-in safety (know it, don't carry it):
// - Teardown UNWINDS: a teardown delivery makes resume_on/call throw the
//   internal cancelled_unwind (caught by the promise; never crosses the
//   ABI) — post-await code cannot run through teardown. `.nothrow()`
//   opts back into raw-status checking. Do NOT swallow with a bare
//   non-rethrowing `catch (...)` inside Task bodies.
// - An exception escaping a Task body = task FAILURE (ChildFailed
//   fail-fast, supervised containment — Rust panic parity), not
//   std::terminate. Nested co_await chains propagate failures upward;
//   classification happens once, at the spawned root.
// - `spawn_on`'s returned task id is OPAQUE accounting — never call
//   `aorta_task_finish` on it (that un-counts a live frame and lets
//   `Scope::close()` return early).
// - Combinators (`join`/`race`/`timeout`/`offload`) live in
//   flow_combinators.hpp; readiness surfaces (`aligned`/`wave`) in
//   flow_readiness.hpp; action verbs in flow_action.hpp. This header is
//   the base target: Task/Scope/spawn/resume/call.

#if __cplusplus < 202002L
#error "aorta/flow.hpp requires -std=c++20 (use the //cpp:aorta_cpp20_flow target)"
#endif

#include <aorta.h>

#include "aorta/client.h"
#include "aorta/detail/reentrancy_check.h"
#include "aorta/executor.h"
#include "aorta/flow_serial.hpp"
#include "aorta/lane_runtime.h"
#include "aorta/logging.h"
#include "aorta/status.h"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <span>
#include <optional>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace aorta::flow {

// ---------------------------------------------------------------------------
// Outcome<T> — THE Flow completion vocabulary at the await boundary
// (Rust parity: `Outcome<T> = Result<T, Stopped>`). Carries
// the value or WHY the wait stopped. No exceptions across the FFI;
// std::expected is C++23, the SDK baseline predates it.
// ---------------------------------------------------------------------------

// Why an Outcome carries no value (Rust `StopReason` parity, classified
// from the completion code — the C layer delivers (code, payload)).
enum class StopKind : uint8_t {
  kNone = 0,        // not stopped: ok() is true
  kCancelled = 1,   // cooperative cancel (scope/explicit/shutdown)
  kDeadline = 2,    // budget/deadline expiry
  kFault = 3,       // everything else: transport/service/infra failure
};

namespace detail {
// The ONE code→StopKind fold and code→Status bridge (DRY: shared by
// Outcome<T> and the Outcome<void> specialization below).
inline StopKind classify_stop(aorta_result_t status) {
  switch (status) {
    case AORTA_OK:
      return StopKind::kNone;
    case AORTA_ERR_CANCELLED:
      return StopKind::kCancelled;
    case AORTA_ERR_TIMEOUT:
      return StopKind::kDeadline;
    default:
      return StopKind::kFault;
  }
}

// The message is the canonical code meaning ONLY: the async completion
// callback receives just (code, payload), and by resume time the
// thread-local `aorta_last_error` belongs to another thread — detailed
// async messages are a recorded follow-up.
inline aorta::Status outcome_status(aorta_result_t status) {
  if (status == AORTA_OK) {
    return aorta::Status::Ok();
  }
  const char* name = "error";
  switch (status) {
    case AORTA_ERR_CANCELLED:
      name = "cancelled";
      break;
    case AORTA_ERR_TIMEOUT:
      name = "timeout";
      break;
    case AORTA_ERR_INVALID_ARGUMENT:
      name = "invalid argument";
      break;
    case AORTA_ERR_NOT_FOUND:
      name = "not found";
      break;
    default:
      break;
  }
  return aorta::Status(status, name);
}
}  // namespace detail

// Typed cancel-reason vocabulary (Rust parity: `CancelReason::{Explicit,
// DeadlineExceeded, Shutdown, ChildFailed}`). Values ARE the C constants;
// unknown/future non-zero values still mean "cancelled" and are carried
// VERBATIM (loss-free rule) — treat the enum as open:
// `static_cast<CancelReason>(raw)` may hold an unnamed value; compare
// through `Stopped::raw_reason()` when exactness matters.
enum class CancelReason : uint8_t {
  kExplicit = AORTA_CANCEL_REASON_EXPLICIT,
  kDeadlineExceeded = AORTA_CANCEL_REASON_DEADLINE,
  kShutdown = AORTA_CANCEL_REASON_SHUTDOWN,
  kChildFailed = AORTA_CANCEL_REASON_CHILD_FAILED,
};

/// Fault-supervision policy (Rust `Supervision` parity; replaced the
/// pre-freeze boolean). Restart supervision is a RUST-PLANE capability:
/// it has no C++ spelling in this release.
enum class Supervision : uint8_t {
  /// A child fault cancels the siblings (ChildFailed cascade) — default.
  kFailFast = AORTA_SUPERVISION_FAIL_FAST,
  /// Contain the fault to this subtree.
  kIsolate = AORTA_SUPERVISION_ISOLATE,
};

namespace detail {
[[noreturn]] inline void FatalOutcomeAccess(const char* what) {
  // The StatusOr wrong-side-access discipline (FROZEN — one
  // fatal helper in ALL build modes, death-tested both directions).
  std::fprintf(stderr, "Aorta fatal: %s\n", what);
  std::fflush(stderr);
  std::abort();  // FatalOutcomeAccess: contract trap, abort-lint exempt
}
}  // namespace detail

class Stopped;
namespace detail {
/// Build a `Stopped` from a completion record (NULL-tolerant: falls
/// back to classifying the legacy code — the pre-record fidelity).
Stopped stopped_from_completion(const aorta_flow_completion_t* c,
                                aorta_result_t fallback_code);
}  // namespace detail

/// WHY a task/combinator stopped instead of producing a value — the full
/// Rust `Stopped` vocabulary. A
/// DISCRIMINATED type: only the factories construct it, so `kCancelled`
/// with no reason, a fault with an OK status, or both-set are
/// UNREPRESENTABLE.
class Stopped {
 public:
  /// Cooperative cancellation with a NAMED reason. A zero byte is a
  /// caller bug (it would masquerade as the fault discriminant) — fatal,
  /// never silently rewritten.
  static Stopped Cancelled(CancelReason reason) {
    return CancelledRaw(static_cast<uint8_t>(reason));
  }
  /// Cancellation carrying an unknown/future wire byte VERBATIM (the
  /// completion-record carrier). `raw_reason` must be non-zero — 0 is not a
  /// cancellation reason and is refused fatally, not invented as
  /// Explicit.
  static Stopped CancelledRaw(uint8_t raw_reason) {
    if (raw_reason == 0) {
      detail::FatalOutcomeAccess(
          "Stopped::Cancelled/CancelledRaw with reason byte 0");
    }
    return Stopped(raw_reason, aorta::Status());
  }
  /// A fault: the ORIGINAL stable code + diagnostic. `fault.ok()` is a
  /// caller bug (a fault cannot be OK) — fatal, not silently misfiled.
  static Stopped Fault(aorta::Status fault) {
    if (fault.ok()) {
      detail::FatalOutcomeAccess("Stopped::Fault called with an OK status");
    }
    return Stopped(0, std::move(fault));
  }

  /// kCancelled / kDeadline / kFault (never kNone — a Stopped exists
  /// only for a stop).
  StopKind kind() const noexcept {
    if (raw_reason_ == 0) {
      return StopKind::kFault;
    }
    return raw_reason_ == static_cast<uint8_t>(CancelReason::kDeadlineExceeded)
               ? StopKind::kDeadline
               : StopKind::kCancelled;
  }
  bool is_cancel() const noexcept { return raw_reason_ != 0; }
  bool is_deadline() const noexcept { return kind() == StopKind::kDeadline; }
  bool is_fault() const noexcept { return raw_reason_ == 0; }

  /// The reason (possibly an unnamed open-enum value — see CancelReason).
  /// Precondition: is_cancel(); fatal otherwise.
  CancelReason cancel_reason() const {
    if (!is_cancel()) {
      detail::FatalOutcomeAccess("Stopped::cancel_reason() on a fault");
    }
    return static_cast<CancelReason>(raw_reason_);
  }
  /// The exact wire byte (0 when this stop is a fault).
  uint8_t raw_reason() const noexcept { return raw_reason_; }
  /// The fault's stable code + message. Precondition: is_fault().
  const aorta::Status& fault() const {
    if (!is_fault()) {
      detail::FatalOutcomeAccess("Stopped::fault() on a cancellation");
    }
    return fault_;
  }

 private:
  Stopped(uint8_t raw_reason, aorta::Status fault)
      : raw_reason_(raw_reason), fault_(std::move(fault)) {}
  // Tagged repr: raw_reason_ != 0 <=> cancelled (the byte itself);
  // raw_reason_ == 0 <=> fault_ holds a non-OK status. Cheaper in
  // coroutine frames than std::variant and the invariant is enforced by
  // the factories (the ONLY constructors).
  uint8_t raw_reason_;
  aorta::Status fault_;
};

namespace detail {
inline Stopped stopped_from_completion(const aorta_flow_completion_t* c,
                                       aorta_result_t fallback_code) {
  if (c != nullptr) {
    if (c->stop_kind == AORTA_STOP_KIND_CANCELLED) {
      return Stopped::CancelledRaw(c->cancel_reason);
    }
    if (c->stop_kind == AORTA_STOP_KIND_FAULT) {
      std::string message;
      if (c->detail.slices != nullptr) {
        for (size_t i = 0; i < c->detail.slice_count; ++i) {
          const auto& sl = c->detail.slices[i];
          message.append(reinterpret_cast<const char*>(sl.data), sl.len);
        }
      }
      return Stopped::Fault(aorta::Status(c->code, std::move(message)));
    }
  }
  // No record (plain rows / pre-record completers): classify the code.
  switch (classify_stop(fallback_code)) {
    case StopKind::kDeadline:
      return Stopped::Cancelled(CancelReason::kDeadlineExceeded);
    case StopKind::kCancelled:
      return Stopped::Cancelled(CancelReason::kExplicit);
    default:
      return Stopped::Fault(outcome_status(fallback_code));
  }
}
}  // namespace detail

/// The Flow result vocabulary: a VALUE or a `Stopped` — never a
/// default-constructed `T` on the error path, so move-only and
/// non-default-constructible payloads work. No `ToStatus()`:
/// a Status-flattening adapter would re-lose the reason vocabulary;
/// map explicitly at terminal edges via `stopped()`.
template <typename T>
class [[nodiscard]] Outcome {
 public:
  static Outcome Value(T value) { return Outcome(std::move(value)); }
  static Outcome Stop(Stopped stopped) { return Outcome(std::move(stopped)); }

  bool ok() const noexcept { return repr_.index() == 0; }
  explicit operator bool() const noexcept { return ok(); }

  T& value() & {
    if (!ok()) {
      detail::FatalOutcomeAccess("Outcome::value() on a stopped outcome");
    }
    return std::get<0>(repr_);
  }
  const T& value() const& {
    if (!ok()) {
      detail::FatalOutcomeAccess("Outcome::value() on a stopped outcome");
    }
    return std::get<0>(repr_);
  }
  T&& value() && {
    if (!ok()) {
      detail::FatalOutcomeAccess("Outcome::value() on a stopped outcome");
    }
    return std::get<0>(std::move(repr_));
  }
  const Stopped& stopped() const {
    if (ok()) {
      detail::FatalOutcomeAccess("Outcome::stopped() on a value outcome");
    }
    return std::get<1>(repr_);
  }

  // StatusOr-shaped access sugar (same fatal precondition as value()).
  T* operator->() { return &value(); }
  const T* operator->() const { return &value(); }
  T& operator*() & { return value(); }
  const T& operator*() const& { return value(); }
  T&& operator*() && { return std::move(*this).value(); }

 private:
  explicit Outcome(T value) : repr_(std::in_place_index<0>, std::move(value)) {}
  explicit Outcome(Stopped stopped)
      : repr_(std::in_place_index<1>, std::move(stopped)) {}
  std::variant<T, Stopped> repr_;
};

// Outcome<void> — completion-only (`co_await TaskHandle<>` yields
// one). Success or `Stopped`; no value accessor.
template <>
class [[nodiscard]] Outcome<void> {
 public:
  static Outcome Value() { return Outcome(std::nullopt); }
  static Outcome Stop(Stopped stopped) { return Outcome(std::move(stopped)); }

  bool ok() const noexcept { return !stopped_.has_value(); }
  explicit operator bool() const noexcept { return ok(); }

  const Stopped& stopped() const {
    if (ok()) {
      detail::FatalOutcomeAccess("Outcome::stopped() on a value outcome");
    }
    return *stopped_;
  }

 private:
  explicit Outcome(std::optional<Stopped> stopped)
      : stopped_(std::move(stopped)) {}
  std::optional<Stopped> stopped_;
};

// ---------------------------------------------------------------------------
// cancelled_unwind — the internal teardown-unwind token: thrown by
// awaitables when a teardown delivery resumes the
// frame, caught by the coroutine machinery (promise), NEVER visible across
// the C ABI. Intentionally NOT derived from std::exception so that
// `catch (std::exception&)` in user code cannot swallow a cancellation by
// accident. CAVEAT (documented contract): a bare `catch (...)` that does
// not rethrow WILL swallow it and keep the frame running — don't do that
// inside Task bodies.
// ---------------------------------------------------------------------------
struct cancelled_unwind {
  /// Stop-time reason snapshot: non-zero when the
  /// THROW SITE knows the reason better than the frame's scope can tell —
  /// today that is the deadline family (a per-wait timeout is not a scope
  /// cancellation, so the scope reads live). 0 = "derive from the frame's
  /// scope at catch time" (handle_wrapper's snapshot rule). Internal token:
  /// never crosses the C ABI.
  uint8_t raw_reason = 0;
};

namespace detail {
// Swallowed-unwind canary: armed where cancelled_unwind is THROWN,
// cleared where the promise catches it. A bare `catch (...)` that swallows
// the unwind leaves it armed — detected at the frame's next suspend/finish
// and WARNed once. KNOWN LIMIT: the flag is thread_local, so a frame
// that swallows the unwind and then suspends/migrates leaves it armed on
// the OLD thread — the WARN then fires at an unrelated frame's next
// final_suspend there. The swallow is real; only the attribution ("at
// <where>") can point at the wrong frame. Accepted: a per-frame flag
// would cost promise space on every task for a diagnostics-only signal.
inline thread_local bool unwind_armed = false;
// Monotonic count of detected swallows. The WARN below rides the async
// log worker (no runtime flush exists — logging::flush() is a documented
// no-op), so the log TEXT is not a deterministic observable; the swallow
// detection test asserts THIS counter instead. Diagnostics-only: bumped
// exclusively on the already-detected swallow branch, so the unarmed hot
// path is untouched.
inline std::atomic<uint64_t> swallow_canary_count{0};

inline void arm_unwind() noexcept { unwind_armed = true; }
inline void clear_unwind() noexcept { unwind_armed = false; }
inline void check_swallowed_unwind(const char* where) noexcept {
  // -fno-exceptions: every arm site is inside a `#if __cpp_exceptions`
  // block, so the canary can never be armed — and `try` would not compile.
#if defined(__cpp_exceptions)
  if (unwind_armed) {
    unwind_armed = false;
    swallow_canary_count.fetch_add(1, std::memory_order_relaxed);
    try {
      AORTA_LOG(WARN)
          << "flow: a cancelled_unwind was SWALLOWED before reaching the "
             "promise (bare catch(...) in a Task body?) — teardown was "
             "suppressed at "
          << where;
    } catch (...) {
    }
  }
#else
  (void)where;
#endif
}

/// Always-false but instantiation-DEPENDENT: lets a static_assert inside a
/// (member) function template fire only when that exact path is
/// instantiated (the -fno-exceptions gates below), never on mere header
/// inclusion.
template <typename>
struct dependent_false : std::false_type {};

/// The single CANCELLATION/teardown-branch abort trap
/// for the whole Flow surface. Every call site that reaches this function
/// sits behind the choke gate (Task's promise_type static_assert) or a
/// belt gate (a dependent static_assert in a nothrow_awaiter/explicit
/// await_resume) — under -fno-exceptions, none of those TUs compile in the
/// first place, so this is statically unreachable in any TU built with
/// exceptions disabled. It is kept as a trap for ABI-mixed builds (a
/// -fno-exceptions TU linked against a flow.hpp compiled with exceptions
/// elsewhere) rather than deleted, so a contract violation that slips past
/// the compile gates still fails loud instead of undefined-behaving.
[[noreturn]] inline void flow_requires_exceptions_unreachable() noexcept {
  std::abort();  // flow_requires_exceptions_unreachable: the one sanctioned trap
}

// Misuse distinguishability: the ABI refuses SYNCHRONOUS misuse with
// INVALID_ARGUMENT (second concurrent awaiter on a single-awaiter
// surface) or NOT_FOUND (stale/freed handle) — codes the awaiters below
// then flatten into the same value-less `cancelled_unwind` a normal
// cancel uses. `log_refused_await` restores the distinction at ERROR
// severity right before the flatten; lifecycle codes (CANCELLED /
// TIMEOUT / teardown) stay quiet.
inline bool is_refusal_status(aorta_result_t st) noexcept {
  return st == AORTA_ERR_INVALID_ARGUMENT || st == AORTA_ERR_NOT_FOUND;
}

inline const char* refusal_code_name(aorta_result_t st) noexcept {
  switch (st) {
    case AORTA_ERR_INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case AORTA_ERR_NOT_FOUND:
      return "NOT_FOUND";
    default:
      return "?";
  }
}

/// Log a refused await at ERROR, per occurrence (refusals resolve the
/// await — they never re-loop, so there is nothing to throttle; unlike
/// the swallow canary this is not one-shot). No-op for lifecycle codes, so
/// call sites with mixed status sources can call it unconditionally.
/// Same facility as the swallow-canary WARN; never throws (some callers
/// are noexcept callback contexts).
inline void log_refused_await(const char* surface,
                              aorta_result_t st) noexcept {
  if (!is_refusal_status(st)) {
    return;
  }
#if defined(__cpp_exceptions)
  try {
#endif
    AORTA_LOG(ERROR)
        << surface << ": await REFUSED (status " << st << " / "
        << refusal_code_name(st)
        << ") — second concurrent awaiter on a single-awaiter surface or "
           "a stale/freed handle: a programming error, not a "
           "cancellation. The frame unwinds as cancelled_unwind; where a "
           ".nothrow() form exists it carries the raw status for "
           "programmatic handling";
#if defined(__cpp_exceptions)
  } catch (...) {
  }
#endif
}

/// Null-context refusal: an ambient arbiter-family
/// verb (join/race/timeout/offload/recv/aligned/wait_wave) on a frame
/// whose promise context carries NO LaneRuntime (`rt == nullptr`,
/// executor-only spawn) refuses at runtime — the machinery needs the
/// runtime object (stop-arm gate, arm registry, pool residue routing).
/// Refusal-class event: ERROR log naming the fix, then the standard
/// unwind (nullness is a runtime value; it cannot be a compile error).
inline void log_null_rt_refused_await(const char* surface) noexcept {
#if defined(__cpp_exceptions)
  try {
#endif
    AORTA_LOG(ERROR)
        << surface << ": await REFUSED (status " << AORTA_ERR_INVALID_ARGUMENT
        << " / INVALID_ARGUMENT) — this ambient verb needs the lane-runtime "
           "plane, but the frame's context has none (rt == nullptr: the "
           "task was spawned on a bare executor). Spawn this task on a "
           "LaneRuntime, or use the explicit overload. The frame unwinds "
           "as cancelled_unwind";
#if defined(__cpp_exceptions)
  } catch (...) {
  }
#endif
}

/// The endpoint-wrapper null-context refusal tail: ERROR log
/// at the refusal site, then the standard unwind. Used by the ambient
/// endpoint wrappers (flow_subscriber.hpp / flow_readiness.hpp).
[[noreturn]] inline void refuse_null_rt_endpoint(const char* surface) {
  log_null_rt_refused_await(surface);
#if defined(__cpp_exceptions)
  arm_unwind();
  throw cancelled_unwind{};
#else
  flow_requires_exceptions_unreachable();  // no unwind channel here
#endif
}
}  // namespace detail

// ---------------------------------------------------------------------------
// Scope — shared-ownership wrapper over `aorta_scope_t`.
// COPYABLE: copies share the same scope (exactly the Rust `Scope::clone`
// semantics); the LAST copy frees the C handle. Pass `Scope` BY VALUE into
// coroutines — the frame co-owns the scope, so no dangling is possible by
// construction. Freeing never cancels: pair `cancel()`/`close()` — or call
// `shutdown()`, which spells the pair.
// ---------------------------------------------------------------------------
class Scope {
 public:
  // Named SDK-level constant for the inherit sentinel.
  struct Options {
    // nullopt = inherit the parent's priority (a root resolves to
    // normal). Explicit values clamp, never raise — the exact shape of
    // Rust `ScopeOpts.priority: Option<EventPriority>` (the raw
    // 255 sentinel is a C-plane detail, not SDK surface).
    std::optional<EventPriority> priority{};
    Supervision supervision = Supervision::kFailFast;
    // Accepts any std::chrono duration; zero = unlimited.
    std::chrono::nanoseconds budget{0};
    // Use STATIC label strings: each unique name is interned once for the
    // process lifetime (a per-goal dynamic name leaks one copy per string).
    const char* name = nullptr;
  };

  static Scope root(const Options& options) {
    return Scope(RootTag{}, options);
  }
  // No-arg overloads instead of `= {}` defaults: a default argument would
  // need Options' member initializers before Scope is complete (Clang
  // rejects that); a function BODY is compiled as-if after the class.
  static Scope root() { return root(Options{}); }

  // A child of THIS scope. An invalid/moved-from scope yields an invalid
  // Scope (handle 0 must never silently mean "new root").
  Scope child(const Options& options) const {
    if (!valid()) {
      return Scope();
    }
    return Scope(owner_->handle, options);
  }
  Scope child() const { return child(Options{}); }

  Scope() = default;                      // invalid
  Scope(const Scope&) = default;          // shared ownership
  Scope& operator=(const Scope&) = default;
  Scope(Scope&&) noexcept = default;
  Scope& operator=(Scope&&) noexcept = default;
  ~Scope() = default;

  // The raw C handle (0 when invalid). The handle is owned by the shared
  // state: never pass it to `aorta_scope_free`; use it only as a
  // spawn/call parameter.
  uint64_t handle() const { return owner_ ? owner_->handle : 0; }
  bool valid() const { return owner_ != nullptr && owner_->handle != 0; }

  void cancel(CancelReason reason = CancelReason::kExplicit) const {
    cancel(static_cast<uint8_t>(reason));
  }
  // Raw-u8 form for C interop (accepts future reason values verbatim).
  void cancel(uint8_t reason) const {
    if (valid()) {
      aorta_scope_cancel(owner_->handle, reason);
    }
  }

  // The scope's cancel reason (loss-free rule): nullopt
  // while live; the EXACT reason otherwise — possibly an unnamed open-enum
  // value for foreign wire bytes (compare via raw_cancel_reason() for
  // exactness). An invalid scope reads as Explicit (fail-safe, like
  // is_cancelled()).
  std::optional<CancelReason> cancel_reason() const {
    const uint8_t raw = raw_cancel_reason();
    if (raw == 0) {
      return std::nullopt;
    }
    return static_cast<CancelReason>(raw);
  }
  // The raw reason byte (0 = live). Invalid scope reads Explicit (1).
  uint8_t raw_cancel_reason() const {
    if (!valid()) {
      return static_cast<uint8_t>(CancelReason::kExplicit);
    }
    uint8_t has = 0;
    uint8_t reason = 0;
    if (aorta_scope_cancel_reason(owner_->handle, &has, &reason) != AORTA_OK) {
      return 0;
    }
    return has != 0 ? reason : 0;
  }

  // RT-safe poll: one acquire load of the resolve-once flag.
  // FAIL-SAFE direction: an INVALID scope reads as CANCELLED —
  // a `while (!scope.is_cancelled())` loop over a failed/moved-from scope
  // exits instead of spinning forever.
  bool is_cancelled() const {
    if (!valid()) {
      return true;
    }
    return owner_->flag != nullptr &&
           reinterpret_cast<const std::atomic<uint8_t>*>(owner_->flag)
                   ->load(std::memory_order_acquire) != 0;
  }

  // Cooperative checkpoint: observes budget expiry.
  // Typed status: `.ok()` / `.code()` — no raw result_t leaks.
  [[nodiscard]] aorta::Status checkpoint() const {
    return aorta::Status::FromResult(valid()
                                         ? aorta_scope_checkpoint(owner_->handle)
                                         : AORTA_ERR_INVALID_ARGUMENT);
  }

  std::optional<std::chrono::nanoseconds> remaining() const {
    uint8_t has = 0;
    uint64_t ns = 0;
    if (!valid() ||
        aorta_budget_remaining_ns(owner_->handle, &has, &ns) != AORTA_OK ||
        has == 0) {
      return std::nullopt;
    }
    return std::chrono::nanoseconds(ns);
  }

  // BLOCKING: wait for every child. Idempotent; safe to call
  // from any copy. Not callable from inside an aorta callback (returns
  // AORTA_ERR_REENTRANT there).
  [[nodiscard]] aorta::Status close() const {
    if (!valid()) {
      return aorta::Status::FromResult(AORTA_ERR_INVALID_ARGUMENT);
    }
    AORTA_CHECK_NOT_REENTRANT("Scope::close");
    return aorta::Status::FromResult(aorta_scope_close(owner_->handle));
  }

  // `cancel(reason)` + `close()` — the normal teardown, one call (Rust
  // parity: `scope.shutdown(reason).await`). Same BLOCKING/reentrancy
  // rules as close().
  [[nodiscard]] aorta::Status shutdown(
      CancelReason reason = CancelReason::kShutdown) const {
    cancel(reason);
    return close();
  }

  // Bounded shutdown (Rust parity: `scope.shutdown_with_grace(reason,
  // grace).await`): `cancel(reason)` + close with a grace window. On
  // expiry the still-running subtree is DETACHED and the count returned
  // — a non-zero value is the caller's signal that stragglers leaked
  // past the grace, never folded into success. Same BLOCKING/reentrancy
  // rules as close(). An invalid scope refuses (fail-safe — handle 0
  // must never reach the bridge).
  [[nodiscard]] aorta::StatusOr<uint64_t> shutdown_with_grace(
      CancelReason reason, std::chrono::nanoseconds grace) const {
    if (!valid()) {
      return aorta::Status::FromResult(AORTA_ERR_INVALID_ARGUMENT);
    }
    const uint64_t grace_ns =
        grace.count() > 0 ? static_cast<uint64_t>(grace.count()) : 0;
    uint64_t detached = 0;
    AORTA_CHECK_NOT_REENTRANT("Scope::shutdown_with_grace");
    const aorta_result_t rc = aorta_scope_shutdown_with_grace(
        owner_->handle, static_cast<uint8_t>(reason), grace_ns, &detached);
    if (rc != AORTA_OK) {
      return aorta::Status::FromResult(rc);
    }
    return detached;
  }

 private:
  struct RootTag {};

  // The shared state: the LAST owner frees the C handle (and with it the
  // cached flag pointer, which is valid exactly as long as the handle is).
  struct ScopeOwner {
    uint64_t handle = 0;
    const uint8_t* flag = nullptr;
    ~ScopeOwner() {
      if (handle != 0) {
        aorta_scope_free(handle);
      }
    }
  };

  Scope(RootTag, const Options& options) { init(0, options); }
  Scope(uint64_t parent, const Options& options) { init(parent, options); }

  void init(uint64_t parent, const Options& options) {
    aorta_scope_opts_t opts{};
    opts.priority = options.priority
                        ? static_cast<uint8_t>(*options.priority)
                        : AORTA_FLOW_PRIORITY_INHERIT;
    opts.supervision = static_cast<uint8_t>(options.supervision);
    opts.budget_ns = static_cast<uint64_t>(options.budget.count());
    opts.name = options.name;
    uint64_t handle = 0;
    if (aorta_scope_child(parent, &opts, &handle) == AORTA_OK) {
      auto owner = std::make_shared<ScopeOwner>();
      owner->handle = handle;
      aorta_scope_cancelled_flag_ptr(handle, &owner->flag);
      owner_ = std::move(owner);
    } else {
      // Construction failure is loud, once — the resulting invalid
      // Scope reads as cancelled (fail-safe) and refuses operations.
      AORTA_LOG(WARN) << "flow::Scope construction failed (parent="
                      << parent << ") — the scope is INVALID and reads as "
                         "cancelled";
    }
  }

  std::shared_ptr<ScopeOwner> owner_;
};

// ---------------------------------------------------------------------------
// Task<T, Kind> — a bare C++20 coroutine (no P2300 dependency).
// Lazy: runs when awaited (or driven by `spawn`). final_suspend resumes the
// continuation by symmetric transfer, so awaiting a completed chain never
// pays a queue hop.
//
// The KIND tag is part of the task's TYPE so that composing
// a parked endpoint verb with timeout()/race() is a compile error, not a
// runtime strand:
// - task_kind_regular: an ordinary composable Task (the default; `Task<T>`
//   spells it, nothing changes for existing code).
// - task_kind_endpoint: returned by the PARKED endpoint verbs — recv /
//   recv_for, aligned / aligned_for, wait_wave / wait_wave_for. An endpoint
//   task parks on its surface's notify hook: wrapped in timeout()/race(),
//   the losing branch keeps the hook parked with no data and no cancel path
//   until executor stop (a rejected pattern), so those
//   combinators reject the endpoint kind via static_assert. Endpoint tasks
//   remain valid for direct co_await and for join() (join resumes only
//   after EVERY branch resolves — no loser is ever stranded).
// ---------------------------------------------------------------------------
struct task_kind_regular {};
struct task_kind_endpoint {};

template <typename T, typename Kind>
class Task;

/// The endpoint verbs' return type (see task_kind_endpoint above).
template <typename T>
using EndpointTask = Task<T, task_kind_endpoint>;

/// True when TaskT is an endpoint-kind task. Public so user code can pin
/// composition contracts in its own static_asserts.
template <typename>
struct is_endpoint_task : std::false_type {};
template <typename T>
struct is_endpoint_task<Task<T, task_kind_endpoint>> : std::true_type {};
template <typename TaskT>
inline constexpr bool is_endpoint_task_v =
    is_endpoint_task<std::remove_cvref_t<TaskT>>::value;

namespace detail {

template <typename T>
struct promise_storage {
  std::optional<T> value;
  void return_value(T v) { value.emplace(std::move(v)); }
  T take() { return std::move(*value); }
};

template <>
struct promise_storage<void> {
  void return_void() {}
  void take() {}
};

// Priority wiring: every promise carries the scope's dispatch priority
// (index; 255 = default/Normal). spawn_on stamps the ROOT from the scope;
// Task<T>::await_suspend copies parent→child at every co_await link, so
// ANY frame's own resume_on/sleep_on posts at the scope's priority.
struct promise_priority {
  uint8_t priority = 255;
};

// Promise-carried context: ONE promise base carrying all cross-cutting
// frame state — the priority byte (above) plus the ambient {plane, scope}.
// Extends (not replaces) promise_priority so every existing
// `is_base_of_v<promise_priority, P>` reader keeps working unchanged.
//
// Field regimes: `rt` is NON-owning (frames resolve before
// the LaneRuntime object's lifetime ends — lane_runtime.h teardown
// contract); `exec` is a registry handle, so staleness is memory-safe and
// loud (NOT_FOUND → refusal log); `scope` is an OWNING copy — the
// cached cancel-flag pointer lives exactly as long as the handle, and a
// raw u64 here would reintroduce the dangling-handle discipline the Scope
// wrapper removed. {rt, exec} form ONE "plane" element:
// exec is always the plane's fallback/queue executor, never mixed
// per-field with a different runtime.
struct promise_flow_context : promise_priority {
  aorta::flow::LaneRuntime* rt = nullptr;
  uint64_t exec = 0;
  Scope scope{};
  /// SerialDomain binding: an OWNING copy, stamped by
  /// the bound spawn overloads, inherited parent→child at every
  /// co_await link (the whole-context assignment above), read by
  /// `detail::route_resume` and the arm-point gates. Default =
  /// unbound; every routing branch below degrades to the pre-domain
  /// path on an unbound frame.
  SerialDomain serial{};
  /// The pre-start cancellation mark: set by
  /// the bound initial-dispatch CLOSED/drop channels BEFORE the root's first
  /// resume. The root is always an SDK wrapper (handle_wrapper /
  /// co_invoke / branch_wrapper — the bound spawn surfaces guarantee
  /// it); its SDK body runs (arming its RAII delivery obligations), and
  /// the co_await LINK refuses to start the USER child — the teardown
  /// cancel delivers without one user statement executing.
  bool start_cancelled = false;
};

/// The BINDING probe: non-null for EVERY
/// valid domain binding — including the ST/Disabled/rollback
/// no-op domains, which carry no strand but keep the full BINDING
/// semantics (runtime-pairing refusals, call/action executor
/// stamps). The routing probe below is the narrower one.
inline SerialDomainState* serial_binding_of(
    const promise_flow_context* ctx) noexcept {
  if (ctx == nullptr) {
    return nullptr;
  }
  const auto& st = serial_access::state(ctx->serial);
  return st.get();
}

/// The ROUTING probe: non-null ONLY for a frame bound to a live MT
/// domain (a no-op domain has no group and stays unbound-routed —
/// its routing short-circuits; its binding does NOT, see
/// serial_binding_of).
inline SerialDomainState* serial_state_of(
    const promise_flow_context* ctx) noexcept {
  SerialDomainState* st = serial_binding_of(ctx);
  return (st != nullptr && st->group != nullptr) ? st : nullptr;
}

/// The NO-THROW diagnostics emitter:
/// diagnostics must never change behavior, so the emit path must be
/// exception-proof INCLUDING the logger internals — the macro builder's
/// destructor copies the stream (allocating inside an implicitly
/// noexcept dtor = terminate on failure). This helper pre-builds the
/// message and calls WriteLogAt inside one try, where every allocation
/// failure lands in the catch as ordinary propagation.
inline void serial_diag_log(LogLevel level, const char* prefix,
                            const char* name, const char* body) noexcept {
#if defined(__cpp_exceptions)
  try {
#endif
    std::string msg(prefix);
    if (name != nullptr) {
      msg += '\'';
      msg += name;
      msg += "' ";
    }
    msg += body;
    (void)aorta::WriteLogAt(level, "aorta.flow", __FILE__, __LINE__, msg);
#if defined(__cpp_exceptions)
  } catch (...) {  // swallowed: a diagnostic must never alter behavior
  }
#endif
}

/// Occupancy canary — the out-of-slice detection checkpoint: a bound frame
/// executing WITHOUT its domain's slice mark WARNs once per domain,
/// naming the likely cause (a custom awaiter resumed the raw handle —
/// out of contract). Diagnostics only, best-effort at
/// checkpoints; sanctioned out-of-slice deliveries (the inline
/// teardown) carry the mark and never trip it.
inline void serial_canary_check(const promise_flow_context& ctx) noexcept {
#ifdef AORTA_LANES_BENCH_NO_SLICE_MARK
  // Measurement knob: the nomark bench
  // binary removes the mark's WRITE (lanes.hpp) and this, its per-verb
  // READER, together — with the mark gone the TLS stays null, so this
  // check would otherwise take the mismatch path with a canary_warned
  // atomic RMW at EVERY checkpoint, contaminating the with/without
  // delta. Removing both makes the pair a single-variable experiment:
  // the whole mark feature (one TLS write per slice + one TLS compare
  // per verb) vs its absence. Never defined by production targets.
  (void)ctx;
  return;
#else
  SerialDomainState* st = serial_state_of(&ctx);
  if (st == nullptr ||
      lanes::g_serial_slice_group ==
          static_cast<const void*>(st->group.get())) {
    return;
  }
  if (st->canary_warned.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  serial_diag_log(
      LogLevel::kWarn, "serial-domain canary: a frame bound to domain ",
      st->name.c_str(),
      "is executing OUTSIDE its domain's drain slice — a custom awaiter "
      "likely resumed the raw coroutine handle directly (out of "
      "contract); the serial-domain exclusivity guarantee does not "
      "cover this resumption");
#endif  // AORTA_LANES_BENCH_NO_SLICE_MARK
}
/// Debug aid: an UNBOUND spawn issued from inside a domain
/// drain slice is the documented, LEGAL parallel spelling — log once at
/// DEBUG (never WARN: recommended usage must not read as anomalous).
/// The latch is process-wide and observable (test seam).
inline std::atomic<bool> g_serial_f4_logged{false};
inline void serial_unbound_spawn_debug_aid() noexcept {
  if (lanes::g_serial_slice_group == nullptr) {
    return;
  }
  if (g_serial_f4_logged.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  serial_diag_log(
      LogLevel::kDebug,
      "unbound spawn from inside a serial-domain drain slice — the ",
      nullptr,
      "legal parallel escape (F-4); bind the child "
      "(spawn(scope, domain, task)) if you meant to stay serialized "
      "(logged once)");
}

/// Blocking-wait aid: a BLOCKING wait inside a domain drain slice stalls
/// the whole strand — and self-deadlocks if the awaited task shares the
/// domain (Flow Concepts rule R4 bans it). Best-effort detection via the slice
/// mark; WARN once, process-wide, observable (test seam).
inline std::atomic<bool> g_serial_f6_warned{false};
inline void serial_blocking_wait_check() noexcept {
  if (lanes::g_serial_slice_group == nullptr) {
    return;
  }
  if (g_serial_f6_warned.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  serial_diag_log(
      LogLevel::kWarn,
      "blocking TaskHandle::wait_for inside a serial-domain drain ",
      nullptr,
      "slice — this stalls the strand and SELF-DEADLOCKS if the awaited "
      "task shares the domain (F-6, Concepts R4: co_await the handle "
      "instead; logged once)");
}

// The compile-time guard: flow verbs' ambient forms and
// Task's own await path accept ONLY frames whose promise carries the flow
// context. Foreign coroutines get the teaching static_assert fallback.
template <typename P>
concept FlowPromise = std::is_base_of_v<promise_flow_context, P>;

// Root stamp — the ONE helper all three spawn sites
// call, so the Disabled-runtime rollback delegation cannot accidentally
// stamp `rt = nullptr` (a Disabled runtime is a VALID plane whose verbs
// route via the fallback executor; an ABSENT runtime refuses ambient
// arbiter verbs).
template <typename Promise>
inline void stamp_spawn_context(Promise& p, aorta::flow::LaneRuntime* rt,
                                uint64_t exec, const Scope& scope,
                                uint8_t priority) {
  p.priority = priority;
  p.rt = rt;
  p.exec = exec;
  p.scope = scope;
}

// The DETACHED spawn (today's zero-extra-state spawn path,
// verbatim). The public `spawn_on` wraps the task in a handle_wrapper and
// delegates here; the combinators' internal branch wrappers call this
// directly — they carry their own heap state and must not pay a second
// one. Declared here so Task can befriend it; defined after Task.
/// Spawn admission result (plain status+id pair — the old
/// `Outcome<uint64_t>` spelling abused the value-or-stopped vocabulary;
/// an admission failure can still carry a live accounting id when the
/// refused frame ran inline).
struct SpawnResult {
  aorta_result_t status = AORTA_OK;
  uint64_t id = 0;
  bool ok() const { return status == AORTA_OK; }
};

// The payload parameter's type is now a (defaulted)
// template parameter, not the concrete `Task<void, task_kind_regular>`
// directly. An ordinary (non-template) function — or a function template
// whose signature does not actually MENTION its template parameter —
// still has its NON-dependent statements checked at definition time
// (two-phase lookup only defers DEPENDENT constructs); `coro.promise()`
// on a fixed-type `coro` is non-dependent, so it would still force
// `Task<void, task_kind_regular>::promise_type` to be instantiated
// merely from including this header under -fno-exceptions, tripping the
// exceptions choke gate, even with an unrelated dummy template parameter.
// Making the PARAMETER TYPE ITSELF the template parameter (`TaskT`,
// defaulting to the original concrete type) makes every use of `task`
// inside the body genuinely dependent, so the whole body — including the
// promise access — defers to the (exceptions-enabled-only) call site.
// Same idiom as the endpoint verbs in flow_subscriber.hpp /
// flow_readiness.hpp (keyed off their return type instead).
template <typename TaskT = Task<void, task_kind_regular>>
SpawnResult spawn_detached(uint64_t executor, Scope scope, TaskT task);
// `serial`: non-null stamps the root promise's domain
// binding BEFORE the first dispatch — the bound spawn overloads and the
// combinators' branch stamping ride this one parameter.
template <typename TaskT = Task<void, task_kind_regular>>
SpawnResult spawn_detached(aorta::flow::LaneRuntime& runtime, Scope scope,
                           TaskT task, const SerialDomain* serial = nullptr);

struct final_awaiter {
  bool await_ready() noexcept { return false; }
  template <typename Promise>
  std::coroutine_handle<> await_suspend(
      std::coroutine_handle<Promise> h) noexcept {
    auto& promise = h.promise();
    detail::check_swallowed_unwind("final_suspend");  // swallow canary
    // The final-suspend occupancy checkpoint (out-of-slice detection —
    // noexcept-safe inside; a bound frame ending outside its domain's
    // slice WARNs once).
    detail::serial_canary_check(
        static_cast<const detail::promise_flow_context&>(promise));
    // Root/detached classification of the stored exception (ONE promise
    // model): an AWAITED frame hands its eptr upward via await_resume and
    // skips this block (continuation set). Only the detached root decides.
    bool failed = false;
#if defined(__cpp_exceptions)
    if (promise.eptr && !promise.continuation) {
      try {
        std::rethrow_exception(promise.eptr);
      } catch (const cancelled_unwind&) {
        // A cancel is a normal ending: fall through to plain finish.
      } catch (const std::exception& e) {
        failed = true;
        // Best-effort WARN; logging must never throw out of this noexcept
        // path.
        try {
          AORTA_LOG(WARN) << "flow::Task failed with exception: " << e.what()
                          << " — scope fail-fast (ChildFailed) applies";
        } catch (...) {
        }
      } catch (...) {
        failed = true;
        try {
          AORTA_LOG(WARN)
              << "flow::Task failed with a non-std exception — scope "
                 "fail-fast (ChildFailed) applies";
        } catch (...) {
        }
      }
    }
#endif
    // Task-accounting: a frame spawned via `spawn()` closes its scope slot
    // exactly once, at true completion. A FAILED root applies
    // the Rust panic semantics via aorta_task_finish_failed.
    if (promise.task_accounting != 0) {
      if (failed) {
        aorta_task_finish_failed(promise.task_accounting);
      } else {
        aorta_task_finish(promise.task_accounting);
      }
      promise.task_accounting = 0;
    }
    if (promise.continuation) {
      return promise.continuation;  // symmetric transfer
    }
    if (promise.detached) {
      // A spawn()ed frame has no owning Task and no awaiter: it destroys
      // itself here, at true completion — the one place that is provably
      // past every co_await (the frame is never destroyed while suspended
      // anywhere else).
      h.destroy();
    }
    return std::noop_coroutine();
  }
  void await_resume() noexcept {}
};

}  // namespace detail

template <typename T = void, typename Kind = task_kind_regular>
class [[nodiscard(
    "a lazy Task that is never co_awaited or spawn_on'ed never runs")]] Task {
 public:
  struct promise_type : detail::promise_storage<T>,
                        detail::promise_flow_context {
#if !defined(__cpp_exceptions)
    // CHOKE POINT: C++ Flow normatively REQUIRES exception
    // support — cancellation is delivered by unwinding. This assert fires
    // on INSTANTIATION (defining or awaiting any flow::Task coroutine in
    // a -fno-exceptions TU); merely including the headers stays legal.
    // Per-surface belt asserts cover foreign-frame awaits of the
    // .nothrow()/status forms. Use the C++17 baseline SDK instead.
    static_assert(detail::dependent_false<T>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK "
                  "(callbacks/executors/pull subscribers)");
#endif
    std::coroutine_handle<> continuation = nullptr;
    uint64_t task_accounting = 0;
    /// Set by `spawn()`: no Task owns this frame; it self-destroys at
    /// final_suspend.
    bool detached = false;
    /// The ONE promise model: any exception —
    /// including the internal cancelled_unwind — is stored here uniformly;
    /// classification happens only at Task::await_resume (rethrow upward)
    /// or at the detached root's final_awaiter. Never rethrown from the
    /// promise itself, so no unwind can cross coroutine_handle::resume()
    /// and therefore none can cross the C ABI.
    std::exception_ptr eptr = nullptr;

    Task get_return_object() {
      return Task(std::coroutine_handle<promise_type>::from_promise(*this));
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    detail::final_awaiter final_suspend() noexcept { return {}; }
#if defined(__cpp_exceptions)
    void unhandled_exception() noexcept {
      detail::clear_unwind();  // the promise caught it — not swallowed
      eptr = std::current_exception();
    }
#else
    void unhandled_exception() noexcept { std::terminate(); }
#endif
  };

  Task(Task&& other) noexcept : coro_(std::exchange(other.coro_, nullptr)) {}
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      destroy();
      coro_ = std::exchange(other.coro_, nullptr);
    }
    return *this;
  }
  // Destroying a held frame is safe ONLY because the exposed API gives a
  // frame away exactly once (spawn() nulls the wrapper before posting;
  // co_await transfers via symmetric transfer) — a Task that still owns
  // its coro_ here was never started, so no resume can be in flight.
  ~Task() { destroy(); }

  bool await_ready() const noexcept { return coro_.done(); }
  // The parent→child copy now carries the WHOLE
  // flow context (priority + plane + scope), and awaiting a flow Task from
  // a FOREIGN coroutine — which previously skipped the copy SILENTLY — is
  // a compile error (the P3552 rationale: fail-compile beats silently
  // changing context). ABI 8 (7 + this PR's single bump) is unreleased;
  // this is the window.
  template <typename P>
    requires detail::FlowPromise<P>
  std::coroutine_handle<> await_suspend(std::coroutine_handle<P> awaiter) {
    // Context flows parent → child at every co_await link (the "only the
    // root posts" assumption is false — inner frames post their own
    // resumes, and ambient verbs read their own frame's context).
    static_cast<detail::promise_flow_context&>(coro_.promise()) =
        static_cast<const detail::promise_flow_context&>(awaiter.promise());
    coro_.promise().continuation = awaiter;
    // A start-cancelled root (the bound initial-dispatch
    // CLOSED/drop delivery) refuses to START the child here — the SDK
    // wrapper body already ran its delivery obligations; the user
    // child's body must not execute one statement. The parent continues
    // (transfer to self) and await_resume throws the teardown cancel; the
    // never-started child frame stays owned by this Task and is
    // destroyed normally.
    if (static_cast<const detail::promise_flow_context&>(awaiter.promise())
            .start_cancelled) {
      start_refused_ = true;
      return awaiter;
    }
    return coro_;  // start the child by symmetric transfer
  }
  template <typename P>  // teaching fallback
  std::coroutine_handle<> await_suspend(std::coroutine_handle<P>) {
    static_assert(
        detail::dependent_false<P>::value,
        "awaiting a flow Task requires a flow coroutine: the awaiting "
        "frame must be spawned via flow::spawn_on (or awaited from one). "
        "Foreign coroutines cannot receive the promise-carried context — "
        "drive flow work through flow::spawn_on and the explicit-argument "
        "surface instead.");
    return std::noop_coroutine();
  }
  T await_resume() {
#if defined(__cpp_exceptions)
    if (start_refused_) {
      // The link refused a start-cancelled child — deliver the
      // teardown cancel; the child frame never ran and holds no value.
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
    // Rethrow BEFORE touching the value slot: on failure/cancel the value
    // was never emplaced, and the exception must climb the co_await chain
    // (nested Task propagation).
    if (coro_.promise().eptr) {
      std::rethrow_exception(coro_.promise().eptr);
    }
#endif
    return coro_.promise().take();
  }

 private:
  friend struct detail::final_awaiter;
  // spawn_detached's payload parameter is now a (defaulted) template
  // parameter `TaskT` (see the definitions in the `spawn`/`spawn_on`
  // sections below) — friend the whole template, not one non-template
  // overload. The parameter name must match the template's own signature
  // for the friend match to bind to the right template.
  template <typename TaskT>
  friend detail::SpawnResult detail::spawn_detached(uint64_t executor,
                                                     Scope scope, TaskT task);
  template <typename TaskT>
  friend detail::SpawnResult detail::spawn_detached(
      aorta::flow::LaneRuntime& runtime, Scope scope, TaskT task,
      const SerialDomain* serial);

  explicit Task(std::coroutine_handle<promise_type> coro) : coro_(coro) {}

  void destroy() {
    if (coro_) {
      coro_.destroy();
      coro_ = nullptr;
    }
  }

  std::coroutine_handle<promise_type> coro_ = nullptr;
  /// Set when the link refused to start a child under a
  /// start-cancelled parent; await_resume delivers the cancel.
  bool start_refused_ = false;
};

// ---------------------------------------------------------------------------
// this_coro — ambient context readers. The public
// tags are STATELESS inline constexpr objects; `operator co_await() const`
// returns a FRESH prvalue awaiter materialized in the awaiting frame (a
// single stateful constexpr awaiter cannot work: a const tag cannot host a
// mutating await_suspend, and a mutable one would be shared across
// coroutines). The awaiter's await_suspend returns false — execution
// continues immediately, no reschedule — and results return BY VALUE (the
// gcc-11 Round-by-value rule).
//
//   auto ctx  = co_await flow::this_coro::context;   // whole payload
//   Scope s   = co_await flow::this_coro::scope;     // owning copy
//   auto* rt  = co_await flow::this_coro::runtime;   // may be null
//   uint64_t e = co_await flow::this_coro::executor;
// ---------------------------------------------------------------------------
namespace this_coro {
namespace detail_tags {

using aorta::flow::detail::dependent_false;
using aorta::flow::detail::FlowPromise;
using aorta::flow::detail::promise_flow_context;

template <typename Extract>
struct reader_awaiter {
  using result_t = std::invoke_result_t<Extract, const promise_flow_context&>;
  result_t out_{};
  bool await_ready() const noexcept { return false; }
  template <typename P>
    requires FlowPromise<P>
  bool await_suspend(std::coroutine_handle<P> h) noexcept {
    out_ = Extract{}(
        static_cast<const promise_flow_context&>(h.promise()));
    return false;  // continue immediately — no hop, no reschedule
  }
  template <typename P>  // teaching fallback
  bool await_suspend(std::coroutine_handle<P>) noexcept {
    static_assert(
        dependent_false<P>::value,
        "flow::this_coro::* requires a flow coroutine (spawn via "
        "flow::spawn_on or await from one); use the explicit-argument verb "
        "overloads outside flow tasks.");
    return false;
  }
  result_t await_resume() noexcept { return std::move(out_); }
};

struct extract_context {
  promise_flow_context operator()(const promise_flow_context& c) const {
    return c;
  }
};
struct extract_scope {
  Scope operator()(const promise_flow_context& c) const { return c.scope; }
};
struct extract_runtime {
  aorta::flow::LaneRuntime* operator()(
      const promise_flow_context& c) const {
    return c.rt;
  }
};
struct extract_executor {
  uint64_t operator()(const promise_flow_context& c) const { return c.exec; }
};
struct extract_serial_domain {
  SerialDomain operator()(const promise_flow_context& c) const {
    return c.serial;  // owning copy; invalid() when unbound
  }
};

template <typename Extract>
struct reader_tag {
  reader_awaiter<Extract> operator co_await() const noexcept { return {}; }
};

}  // namespace detail_tags

inline constexpr detail_tags::reader_tag<detail_tags::extract_context>
    context{};
inline constexpr detail_tags::reader_tag<detail_tags::extract_scope> scope{};
inline constexpr detail_tags::reader_tag<detail_tags::extract_runtime>
    runtime{};
inline constexpr detail_tags::reader_tag<detail_tags::extract_executor>
    executor{};
/// `co_await flow::this_coro::serial_domain` → the frame's SerialDomain
/// binding by value (invalid when unbound) — zero-suspend, like every
/// reader here.
inline constexpr detail_tags::reader_tag<detail_tags::extract_serial_domain>
    serial_domain{};

}  // namespace this_coro

// ---------------------------------------------------------------------------
// The arbiter spine's C++-side plumbing (moved up from
// flow_combinators.hpp so TaskHandle below rides the same reviewed
// machinery; the combinators keep using these — one home, no twins).
// ---------------------------------------------------------------------------

/// RAII producer-window pin — the arm window
/// [pre-check .. post-arm recheck/self-revoke] counts against the stop
/// drain's producer quiesce, closing the MT strand gap between the
/// recheck's `closed` load and the revoke's in-flight guard.
struct ArmWindowPin {
  ArmWindowPin() { aorta_arbiter_resolutions_pin(); }
  ~ArmWindowPin() { aorta_arbiter_resolutions_unpin(); }
  ArmWindowPin(const ArmWindowPin&) = delete;
  ArmWindowPin& operator=(const ArmWindowPin&) = delete;
};

namespace detail {

// ---------------------------------------------------------------------------
// route_resume — THE single continuation router. Every EXTERNAL
// resumption of a flow frame funnels through here; in-slice symmetric
// transfers are exempt by definition — they never leave the current
// drain occupancy.
//
// UNBOUND frames take `deliver_unbound`, the call site's EXISTING
// delivery mechanism, invoked verbatim, so unbound behavior is
// bit-identical BY CONSTRUCTION (the router never re-implements
// tracing/counter/stop-status semantics). The bound detour
// (admission-gate pin → budget-exempt domain member {run = resume,
// drop = stamp_teardown + resume}; gate CLOSED → stamp_teardown +
// inline resume, the last resort) reads the promise-carried
// SerialDomain; the frame-context parameter is nullable (null =
// unbound).
// ---------------------------------------------------------------------------
struct RouteDelivery {
  /// The site's existing delivery, run verbatim for unbound frames.
  void (*deliver_unbound)(void* ctx);
  /// Caller-specific cancel stamping (`ArbiterFrame::torn`, call
  /// `delivery_status_`, action `delivery_status_`): every teardown
  /// delivery runs this strictly before the frame resumes (sequenced-
  /// before on the delivering thread).
  void (*stamp_teardown)(void* ctx);
  /// The bound-detour channels: the domain member's run
  /// (plain in-domain resume of the frame through `ctx`) and drop
  /// (stamp strictly before resume — the teardown delivery on the
  /// draining thread). Null `deliver_bound_run` marks a site that can
  /// never carry a bound frame (a defensive shape); the router
  /// then falls through to `deliver_unbound`.
  void (*deliver_bound_run)(void* ctx);
  void (*deliver_bound_drop)(void* ctx);
  void* ctx;
};

#ifdef AORTA_LANES_TEST_SEAMS
/// Pinned-window test seam (single-TU seam test target only, fully
/// excluded from release builds): fires between a successful gate pin
/// and the member post — the deterministic window for the
/// external-stop pinned-producer row. ATOMIC: a
/// test installs while armed producers may read — the install must be
/// a release-store the acquire-load pairs with, not a plain write.
inline std::atomic<void (*)(void*)> route_pin_seam{nullptr};
inline std::atomic<void*> route_pin_seam_ctx{nullptr};
/// Admit-vs-stop test seam: fires in the PUBLIC bound
/// spawn after the d.valid()/pairing checks, before the dispatch — the
/// deterministic window for the teardown-start race rows.
inline std::atomic<void (*)(void*)> bound_spawn_admit_seam{nullptr};
inline std::atomic<void*> bound_spawn_admit_seam_ctx{nullptr};
#endif

inline void route_resume(const promise_flow_context* frame_ctx,
                         std::coroutine_handle<> h, const RouteDelivery& d) {
  SerialDomainState* st = serial_state_of(frame_ctx);
  if (st == nullptr || d.deliver_bound_run == nullptr) {
    // Unbound (or a site that cannot host bound frames): the EXISTING
    // delivery mechanism, verbatim — bit-identical by construction.
    d.deliver_unbound(d.ctx);
    return;
  }
  // STRONG local ownership BEFORE the pin: a
  // posted member can be drained — and the frame resumed, completed, and
  // its promise (possibly the LAST owner of the state) destroyed —
  // before post_resume_member returns to this thread. From here on, only
  // these locals; `st`, `frame_ctx`, and the awaiter behind `d.ctx` are
  // off-limits after the post.
  std::shared_ptr<AdmissionGate> gate = st->gate;
  std::shared_ptr<lanes::SerialGroup> group = st->group;
  st = nullptr;
  // Bound detour: gate-pin BEFORE any runtime/pool touch.
  if (!gate->pin()) {
    // Gate CLOSED — the inline-cancel last resort: a
    // CANCEL delivery (the stamp precedes the resume), never a value
    // delivery, and never a pool touch (the pool may already be gone).
    // The slice mark makes this SANCTIONED out-of-slice execution —
    // the occupancy canary must not WARN on the documented degradation.
    lanes::SerialSliceMark teardown_mark(group.get());
    d.stamp_teardown(d.ctx);
    h.resume();
    return;
  }
#ifdef AORTA_LANES_TEST_SEAMS
  if (auto* seam = route_pin_seam.load(std::memory_order_acquire)) {
    seam(route_pin_seam_ctx.load(std::memory_order_acquire));
  }
#endif
  // Admitted: a budget-exempt domain member — mutual exclusion comes
  // from the SerialGroup wake-bit machinery; ownership of the delivery
  // transfers unconditionally (post_resume_member always accepts; a
  // closed pool routes the marker to the residue).
  lanes::Work w;
  w.run = d.deliver_bound_run;
  w.drop = d.deliver_bound_drop;
  w.ctx = d.ctx;
  (void)group->post_resume_member(w);
  gate->unpin();
}

/// The ARM-POINT router variant: await_suspend sites cannot
/// take route_resume's gate-CLOSED branch — an inline `h.resume()`
/// there would resume a frame whose suspension has not completed (UB);
/// the correct degradation is the return-false shape (stamp, then
/// let the caller `return false` so the frame continues inline and
/// observes the cancel). kUnbound → the caller runs its existing body
/// verbatim; kPosted → the domain member owns the delivery, caller
/// returns true and never touches the frame again.
enum class SerialArmRoute : uint8_t { kUnbound = 0, kPosted = 1, kClosed = 2 };

inline SerialArmRoute route_arm(const promise_flow_context* frame_ctx,
                                void (*bound_run)(void*),
                                void (*bound_drop)(void*), void* ctx) {
  SerialDomainState* st = serial_state_of(frame_ctx);
  if (st == nullptr) {
    return SerialArmRoute::kUnbound;
  }
  serial_canary_check(*frame_ctx);  // verb-entry occupancy checkpoint
  // Strong locals before the pin — same rule as route_resume: the
  // posted member can resume and destroy the frame (the state's last
  // owner) before the post returns.
  std::shared_ptr<AdmissionGate> gate = st->gate;
  std::shared_ptr<lanes::SerialGroup> group = st->group;
  st = nullptr;
  if (!gate->pin()) {
    return SerialArmRoute::kClosed;  // caller stamps + returns false
  }
  lanes::Work w;
  w.run = bound_run;
  w.drop = bound_drop;
  w.ctx = ctx;
  (void)group->post_resume_member(w);
  gate->unpin();
  return SerialArmRoute::kPosted;
}

/// The arm-point foreign-plane probe — a bound frame may only
/// ARM machinery on its own domain's runtime; the refusal fires BEFORE
/// any foreign arm exists. (Ready-fast-path completions never reach an
/// arm and are deliberately allowed.) Doubles as an occupancy-canary
/// CHECKPOINT: every arbiter-family verb entry passes through here on
/// the frame's own thread.
inline bool serial_foreign_arm(const promise_flow_context& ctx,
                               const LaneRuntime* target_rt) noexcept {
  serial_canary_check(ctx);
  // BINDING probe, not the routing one: a no-op
  // domain skips the strand but keeps the runtime-pairing contract — an
  // ST/Disabled bound frame hopping to a foreign plane would let two
  // same-domain segments run concurrently.
  SerialDomainState* st = serial_binding_of(&ctx);
  return st != nullptr && target_rt != st->rt;
}

/// The frame-resume trampoline: posts the suspended frame back onto the
/// lane pool (budget-exempt resume item), or the core executor in rollback
/// mode. Invoked EXACTLY ONCE by the arbiter's last resolver.
struct ArbiterFrame {
  LaneRuntime* rt;
  uint64_t fallback_executor = 0;  // rollback mode target (0 = none)
  std::coroutine_handle<> frame;
  uint8_t priority = 3;
  /// SerialDomain binding probe: the awaiting frame's promise
  /// context, stamped at the arm alongside `rt`. Valid for the whole
  /// park — the frame (and thus its promise) cannot be destroyed while
  /// armed; read on the resolver thread strictly before the resume.
  const promise_flow_context* fctx = nullptr;
  /// DELIVERY-layer teardown mark: set on every
  /// resume that reached the frame through a teardown channel — the
  /// pool drop path, a refused fallback post (inline resume), or a
  /// stop-drain delivery on the fallback executor. Plane-independent
  /// (the enabled plane's pool().closed() probe cannot see the Disabled/
  /// rollback plane's executor stop). Written on the resolver/delivery
  /// thread strictly before frame.resume(); read by the awaiter after
  /// resuming — ordered by the resume itself.
  bool torn = false;

  // The routed delivery thunks — every external resumption of
  // the frame flows through route_resume with these as the unbound
  // delivery bodies (verbatim pre-router behavior).
  static void deliver_plain(void* ctx) {
    static_cast<ArbiterFrame*>(ctx)->frame.resume();
  }
  static void deliver_torn(void* ctx) {
    auto* self = static_cast<ArbiterFrame*>(ctx);
    self->torn = true;
    self->frame.resume();
  }
  static void stamp_torn(void* ctx) {
    static_cast<ArbiterFrame*>(ctx)->torn = true;
  }

  static void run_thunk(void* ctx) {
    auto* self = static_cast<ArbiterFrame*>(ctx);
    route_resume(nullptr, self->frame,
                 RouteDelivery{&deliver_plain, &stamp_torn, nullptr, nullptr,
                               self});
  }
  static void drop_thunk(void* ctx) {
    // Pool closing: resume-into-cancellation is the FRAME's job — the
    // combinator awaiter observes its branch statuses; the teardown unwind
    // happens at the awaiter's resume path.
    auto* self = static_cast<ArbiterFrame*>(ctx);
    route_resume(nullptr, self->frame,
                 RouteDelivery{&deliver_torn, &stamp_torn, nullptr, nullptr,
                               self});
  }

  /// The unbound delivery BODY (verbatim pre-domain resume_cb): lane
  /// post, residue on a closed pool, executor rollback, inline
  /// last resort. Bound frames never reach it — the entry router takes
  /// them into their domain first.
  static void deliver_unbound_body(void* ctx) {
    auto* self = static_cast<ArbiterFrame*>(ctx);
    if (self->rt != nullptr && self->rt->enabled()) {
      aorta::lanes::Work w;
      w.run = &run_thunk;
      w.drop = &drop_thunk;
      w.ctx = self;
      if (self->rt->post_resume_class(self->priority, w) ==
          aorta::lanes::Post::OK) {
        return;
      }
      // Closed pool: hand the USER unwind to the residue —
      // the draining thread (spin final pass / pool teardown)
      // delivers the drop; resuming inline here would run user code on
      // the resolver (a stop-origin or core timer thread — never allowed).
      self->rt->pool().post_residue(w);
      return;
    }
    // Rollback mode: the core executor carries the resume. The posted
    // handle is the ArbiterFrame itself so a stop-drain delivery
    // (ev->status != OK) can stamp the torn mark.
    auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
      auto* fr = static_cast<ArbiterFrame*>(ev->handle);
      if (ev->status != AORTA_OK) {
        fr->torn = true;
      }
      route_resume(nullptr, fr->frame,
                   RouteDelivery{&deliver_plain, &stamp_torn, nullptr,
                                 nullptr, fr});
    };
    if (self->fallback_executor == 0 ||
        aorta_executor_post_resume(self->fallback_executor, trampoline,
                                   self, self->priority) != AORTA_OK) {
      // Last resort: inline (refused-post) — a stamped teardown
      // delivery through the router.
      route_resume(nullptr, self->frame,
                   RouteDelivery{&deliver_torn, &stamp_torn, nullptr,
                                 nullptr, self});
    }
  }

  static void resume_cb(void* ctx) {
    // The routing decision sits at the RESOLVER —
    // a bound frame becomes a domain member here (one hop, no
    // intermediate lane post); an unbound frame takes the verbatim
    // pre-domain body. The frame is parked, so the router's gate-CLOSED
    // stamped inline resume is a legal teardown delivery.
    auto* self = static_cast<ArbiterFrame*>(ctx);
    route_resume(self->fctx, self->frame,
                 RouteDelivery{&deliver_unbound_body, &stamp_torn,
                               &deliver_plain, &deliver_torn, self});
  }
};

/// One branch's C++-side slot: records the resolution status; the typed
/// value (if any) lives in the combinator's own storage.
struct BranchSlot {
  std::atomic<int32_t> status{AORTA_OK};
  static void complete_cb(void* ctx, aorta_result_t status,
                          const aorta_bytes_view_t* /*payload*/,
                          const aorta_flow_completion_t* /*completion*/) {
    static_cast<BranchSlot*>(ctx)->status.store(status,
                                                std::memory_order_release);
  }
};

/// Delivery-layer teardown test, shared by EVERY arbiter consumer:
/// a resume that
/// reached the frame through a teardown channel (`frame.torn`: the pool
/// drop path, a stop-drain delivery on the fallback executor, or a
/// refused-post inline resume) — or an enabled plane whose pool has
/// closed by resume time — OUTRANKS any OK-reading branch slot. A child
/// that completed OK just before teardown must still unwind here: value
/// returns and post-await user statements must never run on a
/// stop-drain/destructor/timer thread.
inline bool plane_torn(const ArbiterFrame& frame, LaneRuntime* rt) noexcept {
  return frame.torn ||
         (rt != nullptr && rt->enabled() && rt->pool().closed());
}

// ---------------------------------------------------------------------------
// TaskHandle plumbing. The heap state is shared between the
// spawned wrapper (child side) and the handle/awaiter (consumer side) —
// the heap-state shared-ref discipline: a consumer resuming and
// destroying its frame can never leave the child writing into freed
// memory, and vice versa.
// ---------------------------------------------------------------------------

template <typename T>
struct handle_value_slot {
  std::optional<T> value;
};
template <>
struct handle_value_slot<void> {};

inline constexpr uint8_t kHandleIdle = 0;
inline constexpr uint8_t kHandleClaimed = 1;

template <typename T>
struct handle_state : handle_value_slot<T> {
  // Child side (written exactly once by the wrapper's deliver guard).
  // Outcome layer: WHY the child stopped (OK / CANCELLED / fault).
  std::atomic<int32_t> child_status{AORTA_ERR_CANCELLED};
  std::atomic<bool> done{false};
  // Consumer side. `armed` is the publication point of the parked branch
  // (seq_cst pairs with `done` — at least one of the two sides always
  // observes the other; both completing is safe, the branch CAS is
  // exactly-once). `claimed` enforces the one-consumer contract across
  // co_await and take_outcome().
  std::atomic<uint64_t> armed{0};
  std::atomic<uint8_t> claimed{kHandleIdle};
  // The single awaiter's machinery lives HERE (one await per handle), so
  // the awaiter object in the frame holds locals only past the arm.
  ArbiterFrame frame{};
  BranchSlot slot{};
  uint64_t group = 0;
  // Fidelity: the child's STOP-TIME
  // reason snapshot and fault text — both written by the wrapper's catch
  // (or the spawn-refusal path) BEFORE the deliver guard publishes
  // `done` (ordered by that flag). take() never re-queries a mutable
  // scope: a later parent cancel cannot re-attribute a completed child,
  // and a completed handle holds no scope handle alive (gate 3.1).
  uint8_t stop_raw_reason = 0;
  std::string fault_message;

  ~handle_state() {
    if (group != 0) {
      (void)aorta_arbiter_group_release(group);
    }
  }

  /// Classify the child's terminal status into the full Stopped
  /// vocabulary — THE one mapping (take() and the when_* combinators
  /// share it; no lossy folding). Reads only the STOP-TIME
  /// snapshot (never a live scope, which could misattribute the reason).
  Stopped build_stopped(aorta_result_t cs) {
    if (detail::classify_stop(cs) == StopKind::kFault) {
      return Stopped::Fault(aorta::Status(
          cs, fault_message.empty() ? std::string("task fault")
                                    : std::move(fault_message)));
    }
    if (stop_raw_reason != 0) {
      return Stopped::CancelledRaw(stop_raw_reason);
    }
    // No snapshot: the frame was stopped by plane teardown (its scope
    // never cancelled) or refused before any reason existed — runtime
    // shutdown is the honest classification.
    return Stopped::Cancelled(CancelReason::kShutdown);
  }

  /// Move the delivered outcome out (call once, after done).
  Outcome<T> take() {
    const auto cs = static_cast<aorta_result_t>(
        child_status.load(std::memory_order_acquire));
    if constexpr (std::is_void_v<T>) {
      if (cs == AORTA_OK) {
        return Outcome<void>::Value();
      }
      return Outcome<void>::Stop(build_stopped(cs));
    } else {
      if (cs == AORTA_OK && this->value.has_value()) {
        return Outcome<T>::Value(std::move(*this->value));
      }
      if (cs == AORTA_OK) {
        // A clean status with no value cannot happen (the wrapper
        // emplaces before setting OK); classify defensively as fault.
        return Outcome<T>::Stop(Stopped::Fault(
            aorta::Status(AORTA_ERR_INTERNAL, "clean status, no value")));
      }
      return Outcome<T>::Stop(build_stopped(cs));
    }
  }
};

/// The wrapper's RAII delivery: runs on EVERY exit path of the wrapper
/// body — success, cancel unwind, real failure — so the branch always
/// resolves and a parked consumer can never strand.
template <typename T>
struct HandleDeliverGuard {
  handle_state<T>* hs;
  aorta_result_t status = AORTA_ERR_CANCELLED;
  ~HandleDeliverGuard() {
    hs->child_status.store(status, std::memory_order_relaxed);
    hs->done.store(true, std::memory_order_seq_cst);
    const uint64_t branch = hs->armed.load(std::memory_order_seq_cst);
    if (branch != 0) {
      // The branch resolution is the DELIVERY layer (always OK from here —
      // the child terminated); the outcome travels in child_status.
      // Idempotent against the awaiter's own self-complete.
      (void)aorta_arbiter_complete(branch, AORTA_OK, nullptr);
    }
  }
};

/// The one wrapper frame a handle-spawn adds (branch_wrapper precedent):
/// runs the payload, parks the outcome on the shared state, resolves the
/// consumer's branch. Rethrows real failures so the spawned root still
/// classifies ChildFailed fail-fast (Rust panic parity) — the handle
/// carries the outcome IN ADDITION to, not instead of, supervision.
template <typename T, typename K>
Task<void> handle_wrapper(Task<T, K> payload,
                          std::shared_ptr<handle_state<T>> hs) {
  // Stop-time reason source (gate 2.2): the frame's own scope, read in
  // the catch below — AT the stop, never at take() time. No suspension:
  // the context reader resumes inline.
  auto ctx = co_await this_coro::context;
  HandleDeliverGuard<T> guard{hs.get()};
#if defined(__cpp_exceptions)
  try {
    if constexpr (std::is_void_v<T>) {
      co_await std::move(payload);
    } else {
      hs->value.emplace(co_await std::move(payload));
    }
    guard.status = AORTA_OK;
  } catch (const cancelled_unwind& e) {
    // Snapshot the reason NOW (before the guard publishes `done`): the
    // throw site's stamp wins (deadline family); otherwise the scope's
    // reason as of THIS instant (the cancel happened-before the unwind);
    // 0 (plane teardown, scope live) classifies as Shutdown in take().
    hs->stop_raw_reason = e.raw_reason != 0
                              ? e.raw_reason
                              : ctx.scope.raw_cancel_reason();
    throw;  // teardown/cancel control flow — the guard delivers
            // CANCELLED on this path
  } catch (const std::exception& e) {
    // The child's exception TEXT survives into the outcome
    // (written before the guard publishes `done` — ordered by that flag).
    hs->fault_message = e.what();
    guard.status = AORTA_ERR_INTERNAL;  // fault (Rust panic parity)
    throw;
  } catch (...) {
    hs->fault_message = "non-std exception";
    guard.status = AORTA_ERR_INTERNAL;
    throw;
  }
#else
  if constexpr (std::is_void_v<T>) {
    co_await std::move(payload);
  } else {
    hs->value.emplace(co_await std::move(payload));
  }
  guard.status = AORTA_OK;
#endif
}

struct handle_access;

}  // namespace detail

// ---------------------------------------------------------------------------
// TaskHandle<T> — the joinable spawn (Rust `TaskHandle` parity).
// Every `spawn_on` returns one:
//
//   auto h = flow::spawn_on(rt, scope, work());   // TaskHandle<int>
//   Outcome<int> o = co_await std::move(h);       // in a flow task
//
// - `co_await handle` yields `Outcome<T>`: the child's value, or WHY it
//   stopped — the child's own cancellation and failure are VALUES here
//   (Rust `handle.join().await` parity), never an unwind. Only a
//   teardown of the AWAIT itself unwinds.
// - A discarded handle is fire-and-forget: the scope owns the task's
//   lifetime; the handle only carries the result.
// - ONE consumer per handle: a second co_await (or a take after a take)
//   refuses loudly (misuse ERROR log + unwind).
// - The await is ambient (FlowPromise): the resume rides the awaiting
//   frame's own plane; executor-only frames refuse. Awaiting from
//   a non-flow coroutine is a compile error (teaching static_assert).
// - `take_outcome()` is the synchronous seam for non-coroutine contexts:
//   after `scope.close()` returned, every child has delivered — read the
//   outcome without an executor hop (one-shot; also the -fno-exceptions
//   story, where the awaiting form is unavailable).
// ---------------------------------------------------------------------------
template <typename T = void>
class [[nodiscard(
    "a discarded TaskHandle silently discards the child's outcome — "
    "co_await it, take_outcome() after close, or bind it explicitly if "
    "fire-and-forget is intended")]] TaskHandle {
 public:
  /// Spawn ADMISSION status (scope closing / executor stopped — the
  /// earlier `Outcome<uint64_t>` contract): ok() == the task was
  /// accepted. Refused handles still RESOLVE when awaited (immediately,
  /// with the refusal as the outcome) — they never park.
  aorta_result_t status = AORTA_OK;

  TaskHandle() = default;
  TaskHandle(TaskHandle&&) noexcept = default;
  TaskHandle& operator=(TaskHandle&&) noexcept = default;
  TaskHandle(const TaskHandle&) = delete;
  TaskHandle& operator=(const TaskHandle&) = delete;

  bool ok() const { return status == AORTA_OK; }
  explicit operator bool() const { return ok(); }

  /// Opaque accounting id (diagnostics only — never call
  /// `aorta_task_finish` on it; the frame owns its slot).
  uint64_t id() const { return id_; }

  /// RT-safe completion poll (one acquire load).
  bool is_finished() const {
    return state_ != nullptr && state_->done.load(std::memory_order_acquire);
  }

  /// BLOCKING bounded wait for the child's completion — the seam for
  /// NON-coroutine contexts (main, tests): wait, then take_outcome(),
  /// THEN close the scope. Closing first would bar the still-running
  /// child's own spawns (combinator branches included) — the scope's
  /// closing bar is subtree-wide and one-way. Never call from inside a
  /// flow task (co_await the handle there) or from an aorta callback: it
  /// parks the calling THREAD. Millisecond-class granularity (teardown
  /// bookkeeping, not RT timing — a coarse poll keeps it portable below
  /// the platforms' futex-wait availability lines). Returns true when
  /// the child completed inside the budget.
  bool wait_for(std::chrono::nanoseconds budget) const {
    if (state_ == nullptr) {
      return true;
    }
    if (budget != std::chrono::nanoseconds::zero()) {
      // Would park the calling thread — explicit wait (zero
      // budget is a pure probe).
      AORTA_CHECK_NOT_REENTRANT("flow::TaskHandle::wait_for");
    }
    detail::serial_blocking_wait_check();  // blocking-wait aid
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (!state_->done.load(std::memory_order_seq_cst)) {
      if (std::chrono::steady_clock::now() >= deadline) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
  }

  /// One-shot synchronous read AFTER completion. Empty when the child has
  /// not finished yet — the probe does NOT consume — or when the outcome
  /// was already consumed (an earlier take, or a co_await).
  std::optional<Outcome<T>> take_outcome() {
    if (state_ == nullptr ||
        !state_->done.load(std::memory_order_seq_cst)) {
      return std::nullopt;
    }
    if (state_->claimed.exchange(detail::kHandleClaimed,
                                 std::memory_order_acq_rel) !=
        detail::kHandleIdle) {
      return std::nullopt;
    }
    return state_->take();
  }

  struct awaiter {
    std::shared_ptr<detail::handle_state<T>> st;
    // Synchronous refusal classification, decided in await_suspend:
    // INVALID_ARGUMENT = misuse (double await / empty handle / no rt),
    // CANCELLED = pre-park teardown. AORTA_OK = fast path or parked.
    aorta_result_t refusal = AORTA_OK;
    bool parked = false;
    // Written pre-publication, read at await_resume (frame context):
    // the stop-sweep registration pair.
    LaneRuntime* rt_ = nullptr;
    uint64_t branch_ = 0;

    bool await_ready() const noexcept { return false; }

    template <typename P>
      requires detail::FlowPromise<P>
    bool await_suspend(std::coroutine_handle<P> frame) {
      if (st == nullptr ||
          st->claimed.exchange(detail::kHandleClaimed,
                               std::memory_order_acq_rel) !=
              detail::kHandleIdle) {
        // Empty (moved-from) handle, or a second consumer: misuse.
        detail::log_refused_await("flow::TaskHandle",
                                  AORTA_ERR_INVALID_ARGUMENT);
        st = nullptr;  // never read already-consumed slots
        refusal = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      const auto& ctx =
          static_cast<const detail::promise_flow_context&>(frame.promise());
      if (ctx.rt == nullptr) {
        // Ambient arbiter-family verb on an executor-only frame.
        // Checked BEFORE the done fast path so the refusal is a property
        // of the FRAME, not of completion timing.
        detail::log_null_rt_refused_await("flow::TaskHandle");
        refusal = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      if (st->done.load(std::memory_order_seq_cst)) {
        return false;  // fast path: deliver inline, no group ever exists
      }
      LaneRuntime* rt = ctx.rt;
      st->frame.rt = rt;
      st->frame.fallback_executor = rt->fallback_executor();
      st->frame.frame = frame;
      st->frame.priority = ctx.priority <= 3 ? ctx.priority : 3;
      // TaskHandle awaits are same-plane by construction
      // (the frame resumes via its OWN runtime) — stamp so a bound
      // awaiting frame's completion re-enters its domain.
      st->frame.fctx = &ctx;
      ArmWindowPin pin;
      // Stop-arm gate: don't park against a closing pool.
      if (rt->enabled() && rt->pool().closed()) {
        refusal = AORTA_ERR_CANCELLED;
        return false;
      }
      if (aorta_arbiter_group_new(&detail::ArbiterFrame::resume_cb,
                                  &st->frame, 1, &st->group) != AORTA_OK) {
        refusal = AORTA_ERR_CANCELLED;
        return false;
      }
      uint64_t branch = 0;
      if (aorta_arbiter_arm(st->group, 0, &detail::BranchSlot::complete_cb,
                            &st->slot, &branch) != AORTA_OK) {
        // Nothing armed can fire; the state's destructor releases the
        // group. Inline resume.
        refusal = AORTA_ERR_CANCELLED;
        return false;
      }
      parked = true;
      // Stop-sweep registration: the CHILD
      // may live on a DIFFERENT runtime — if this awaiting runtime tears
      // down first, its sweep must revoke this branch, or a late child
      // completion would resume through a dangling ArbiterFrame::rt.
      // Retired in await_resume.
      rt_ = rt;
      branch_ = branch;
      // register_arm is ALREADY a publication — the
      // stop sweep can revoke, resume and destroy this awaiter from the
      // instant it returns. Every awaiter write above is final and every
      // access below goes through LOCALS hoisted here, BEFORE the first
      // publication.
      auto st_local = st;
      LaneRuntime* rt_local = rt;
      rt_local->register_arm(branch);
      st_local->armed.store(branch, std::memory_order_seq_cst);
      if (st_local->done.load(std::memory_order_seq_cst)) {
        // The child finished during the arm: self-complete (idempotent —
        // if the guard also saw the branch, the arbiter CAS picks one).
        (void)aorta_arbiter_complete(branch, AORTA_OK, nullptr);
      }
      if (rt_local->enabled() && rt_local->pool().closed()) {
        // close() raced the arm and the sweep may have missed it —
        // self-revoke (CAS-idempotent; the resume lands in the residue).
        (void)aorta_arbiter_revoke(branch);
      }
      return true;
    }

    template <typename P>  // teaching fallback
    bool await_suspend(std::coroutine_handle<P>) {
      static_assert(
          detail::dependent_false<P>::value,
          "co_await TaskHandle requires a flow coroutine: the frame must "
          "be spawned via flow::spawn_on (or awaited from one). Outside "
          "flow tasks use take_outcome() after scope.close().");
      return false;
    }

    template <typename Unused = void>
    Outcome<T> await_resume() {
      if (parked) {
        // Retire the branch from the stop-sweep registry — frame
        // context, the awaiting runtime is provably alive here.
        rt_->deregister_arm(branch_);
      }
#if defined(__cpp_exceptions)
      if (refusal != AORTA_OK || st == nullptr) {
        // Misuse (already ERROR-logged) or pre-park teardown: unwind.
        detail::arm_unwind();
        throw cancelled_unwind{};
      }
      if (parked &&
          st->slot.status.load(std::memory_order_acquire) != AORTA_OK) {
        // The BRANCH resolved without a child delivery (teardown revoke /
        // stop sweep): the await itself was torn down — teardown
        // outranks results, exactly like flow::call.
        detail::arm_unwind();
        throw cancelled_unwind{};
      }
      if (parked && detail::plane_torn(st->frame, rt_)) {
        // Torn DELIVERY with an OK slot (the child completed just before
        // teardown and the resume travelled the drop/residue channel):
        // still an unwind — the outcome must not run user code on the
        // drain/destructor thread (the cancelled() rule applied to every
        // consumer).
        detail::arm_unwind();
        throw cancelled_unwind{};
      }
      return st->take();
#else
      static_assert(
          detail::dependent_false<Unused>::value,
          "co_await TaskHandle delivers teardown by unwinding and is not "
          "available under -fno-exceptions — use take_outcome() after "
          "scope.close() instead");
      return Outcome<T>{};
#endif
    }
  };

  awaiter operator co_await() noexcept { return awaiter{state_}; }

 private:
  friend struct detail::handle_access;

  uint64_t id_ = 0;
  std::shared_ptr<detail::handle_state<T>> state_;
};

namespace detail {
struct handle_access {
  template <typename T>
  static TaskHandle<T> make(aorta_result_t status, uint64_t id,
                            std::shared_ptr<handle_state<T>> st) {
    TaskHandle<T> h;
    h.status = status;
    h.id_ = id;
    h.state_ = std::move(st);
    return h;
  }

  /// Consume a handle's state (when_all/when_any: the combinator becomes
  /// the handle's one consumer — the claim itself happens at arm time).
  template <typename T>
  static std::shared_ptr<handle_state<T>> take_state(TaskHandle<T>& h) {
    return std::move(h.state_);
  }

  /// A refusal handle that RESOLVES (never parks): pre-delivered with the
  /// refusal code so awaits, take_outcome() and wait_for() all behave —
  /// the invalid-Runtime spawn path.
  template <typename T>
  static TaskHandle<T> refused(aorta_result_t rc) {
    auto hs = std::make_shared<handle_state<T>>();
    hs->child_status.store(rc, std::memory_order_relaxed);
    hs->done.store(true, std::memory_order_seq_cst);
    return make<T>(rc, 0, std::move(hs));
  }
};

/// Arm one (already spawned) handle's completion into an EXTERNAL arbiter
/// branch — the when_all/when_any building block; the same publication
/// protocol as TaskHandle's own awaiter. The branch ALWAYS resolves:
/// an empty or already-consumed handle resolves it immediately with
/// INVALID_ARGUMENT (misuse — logged by the caller's classification),
/// a finished child resolves through the self-complete, and a running
/// child resolves through its deliver guard.
template <typename T>
inline void arm_handle_completion(handle_state<T>* hs, uint64_t branch) {
  if (hs == nullptr || hs->claimed.exchange(kHandleClaimed,
                                            std::memory_order_acq_rel) !=
                           kHandleIdle) {
    log_refused_await("flow::when_all/when_any", AORTA_ERR_INVALID_ARGUMENT);
    (void)aorta_arbiter_complete(branch, AORTA_ERR_INVALID_ARGUMENT,
                                 nullptr);
    return;
  }
  hs->armed.store(branch, std::memory_order_seq_cst);
  if (hs->done.load(std::memory_order_seq_cst)) {
    (void)aorta_arbiter_complete(branch, AORTA_OK, nullptr);  // idempotent
  }
}
}  // namespace detail

// ---------------------------------------------------------------------------
// resume_on — suspend and continue on `executor`'s dispatch thread (the
// register-callback bridge over the reserved CoroutineResume lane).
// If the executor is stopped, the post is refused and the coroutine resumes
// INLINE (shutdown rule: resume-into-cancellation, never destroy) —
// observe your scope's flag after resuming.
// ---------------------------------------------------------------------------
// resume_on — hop onto `executor`'s dispatch thread. DEFAULT semantics:
// a teardown delivery (stop-drain, or a refused
// post) UNWINDS the frame via `cancelled_unwind` — code after the await
// simply does not run, locals destruct, the scope accounting balances at
// final_suspend. Forgetting to check a status can therefore no longer
// keep a frame running through teardown. Experts who want a status branch
// (exceptions-ON ergonomics — NOT an -fno-exceptions escape, C++ Flow
// requires exceptions program-wide; both modes are a COMPILE error under
// -fno-exceptions) instead use `co_await resume_on{exec}.nothrow()`
// (identical to the old behavior).
struct resume_on {
  uint64_t executor;
  aorta_result_t status = AORTA_OK;
  std::coroutine_handle<> handle_ = nullptr;

  // Opt-out: return the raw delivery status instead of unwinding. A
  // DISTINCT awaiter type: the -fno-exceptions gate in the default
  // await_resume below can tell the two modes apart at compile time. The
  // inner resume_on lives in this awaiter, which the co_await expression
  // materializes in the coroutine frame — stable across the suspension,
  // same pattern as call::nothrow_awaiter. (Defined after the class: it
  // holds a resume_on by value.)
  struct nothrow_awaiter;
  nothrow_awaiter nothrow() const noexcept;

  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    handle_ = h;
    // Post at the FRAME's priority when the promise carries one.
    uint8_t prio = 255;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      prio = h.promise().priority;
    }
    // "continue on that executor thread" cannot be
    // honored inside a serialization domain — ANY bound frame refuses
    // at the arm point, before the post exists (the BINDING probe:
    // no-op domains keep this contract too).
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      if (detail::serial_binding_of(
              &static_cast<const detail::promise_flow_context&>(
                  h.promise())) != nullptr) {
        detail::log_refused_await(
            "flow::resume_on [serial: executor-thread verb on a bound "
            "frame]",
            AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
    }
    // The posted handle is THIS awaiter (it lives in the frame): the
    // trampoline stores the delivery status — AORTA_OK for a normal
    // dispatch, AORTA_ERR_CANCELLED when a stop-drain delivered it
    // — before resuming, so the frame can tell teardown apart.
    // The resume flows through the router (unbound
    // passthrough; bound frames never reach this verb — refused
    // above).
    auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
      auto* self = static_cast<resume_on*>(ev->handle);
      self->status = ev->status;  // copy out: ev is invocation-scoped
      detail::route_resume(
          nullptr, self->handle_,
          detail::RouteDelivery{&resume_on::deliver_unbound_thunk,
                                &resume_on::stamp_teardown_thunk, nullptr, nullptr, self});
    };
    aorta_result_t rc =
        aorta_executor_post_resume(executor, trampoline, this, prio);
    if (rc != AORTA_OK) {
      // Post refused (executor stopped): record the refusal, then resume
      // INLINE. Writing the frame here is safe — the refused post
      // enqueued nothing, so no other thread can be resuming us.
      status = rc;
      return false;
    }
    // On success: do NOT touch the frame past this point — the executor may
    // already be resuming it on another thread.
    return true;
  }
  /// Routed delivery (verbatim pre-router behavior).
  static void deliver_unbound_thunk(void* ud) {
    static_cast<resume_on*>(ud)->handle_.resume();
  }
  static void stamp_teardown_thunk(void* ud) {
    static_cast<resume_on*>(ud)->status = AORTA_ERR_CANCELLED;
  }
  // Default: AORTA_OK is the only value that RETURNS — teardown deliveries
  // throw cancelled_unwind (caught by the promise; never crosses the ABI),
  // so DISCARDING the return is safe by construction (no [[nodiscard]]:
  // the unwind supersedes the old must-check contract). nothrow(): returns
  // the raw status like the pre-hardening API — check it. Template-ized:
  // the -fno-exceptions static_assert below is DEPENDENT, firing only
  // when this default mode is actually awaited in a no-exceptions TU.
  template <typename Unused = void>
  aorta_result_t await_resume() const {
#if defined(__cpp_exceptions)
    if (status != AORTA_OK) {
      // A refusal code here (NOT_FOUND = stale executor handle) is
      // misuse, not teardown — logged before the generic unwind
      // (lifecycle codes no-op inside the helper).
      detail::log_refused_await("flow::resume_on", status);
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // `.nothrow()` is NOT an -fno-exceptions
    // escape hatch — its own await_resume (resume_on::nothrow_awaiter)
    // hits the same belt gate below. There is no in-mode workaround.
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK "
                  "(callbacks/executors/pull subscribers)");
#endif
    return status;
  }
};

struct resume_on::nothrow_awaiter {
  resume_on inner;
  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    return inner.await_suspend(h);
  }
  // Belt gate: the .nothrow() opt-out's historical silent-status
  // return under -fno-exceptions is REVOKED — dependent
  // static_assert so mere header inclusion stays legal.
  template <typename Unused = void>
  aorta_result_t await_resume() const noexcept {
#if !defined(__cpp_exceptions)
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "flow::resume_on{exec}.nothrow() is not reachable "
                  "under -fno-exceptions either — build this translation "
                  "unit with exceptions enabled, or use the C++17 "
                  "baseline aorta SDK (callbacks/executors/pull "
                  "subscribers)");
#endif
    return inner.status;
  }
};
inline resume_on::nothrow_awaiter resume_on::nothrow() const noexcept {
  return nothrow_awaiter{*this};
}

// sleep_on — awaitable delay THEN hop onto `executor` (kills
// the resume_on busy-spin anti-pattern for periodic work). Rides a runtime
// sleep task, NOT the RT timer machinery (non-RT plane). Same teardown
// semantics as resume_on: executor stop resumes still-sleeping frames into
// cancellation PROMPTLY (never waiting out the delay) — the default
// unwinds; `.nothrow()` returns the raw status. Carries the frame's
// priority like resume_on.
struct sleep_on {
  uint64_t executor;
  std::chrono::nanoseconds delay;
  aorta_result_t status = AORTA_OK;
  std::coroutine_handle<> handle_ = nullptr;

  sleep_on(uint64_t exec, std::chrono::nanoseconds d)
      : executor(exec), delay(d) {}
  template <typename Rep, typename Period>
  sleep_on(uint64_t exec, std::chrono::duration<Rep, Period> d)
      : executor(exec),
        delay(std::chrono::duration_cast<std::chrono::nanoseconds>(d)) {}

  // Type-level status-branch opt-out — see resume_on::nothrow_awaiter.
  struct nothrow_awaiter;
  nothrow_awaiter nothrow() const noexcept;

  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    handle_ = h;
    uint8_t prio = 255;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      prio = h.promise().priority;
    }
    // Executor-thread verb — ANY bound frame refuses
    // at the arm point (binding probe; see resume_on).
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      if (detail::serial_binding_of(
              &static_cast<const detail::promise_flow_context&>(
                  h.promise())) != nullptr) {
        detail::log_refused_await(
            "flow::sleep_on [serial: executor-thread verb on a bound "
            "frame]",
            AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
    }
    // Routed like resume_on (unbound passthrough).
    auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
      auto* self = static_cast<sleep_on*>(ev->handle);
      self->status = ev->status;
      detail::route_resume(
          nullptr, self->handle_,
          detail::RouteDelivery{&sleep_on::deliver_unbound_thunk,
                                &sleep_on::stamp_teardown_thunk, nullptr, nullptr, self});
    };
    aorta_result_t rc = aorta_executor_post_resume_after(
        executor, static_cast<uint64_t>(delay.count()), trampoline, this,
        prio);
    if (rc != AORTA_OK) {
      status = rc;  // refused up front (stopped executor): resume inline
      return false;
    }
    return true;
  }
  /// Routed delivery (verbatim pre-router behavior).
  static void deliver_unbound_thunk(void* ud) {
    static_cast<sleep_on*>(ud)->handle_.resume();
  }
  static void stamp_teardown_thunk(void* ud) {
    static_cast<sleep_on*>(ud)->status = AORTA_ERR_CANCELLED;
  }
  // Default unwinding mode; dependent -fno-exceptions gate (see
  // resume_on::await_resume).
  template <typename Unused = void>
  aorta_result_t await_resume() const {
#if defined(__cpp_exceptions)
    if (status != AORTA_OK) {
      // Refusal codes (stale executor handle) are misuse — logged
      // before the generic unwind; lifecycle codes stay quiet.
      detail::log_refused_await("flow::sleep_on", status);
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // `.nothrow()` is NOT an -fno-exceptions
    // escape hatch — its own await_resume (sleep_on::nothrow_awaiter)
    // hits the same belt gate below. There is no in-mode workaround.
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK "
                  "(callbacks/executors/pull subscribers)");
#endif
    return status;
  }
};

struct sleep_on::nothrow_awaiter {
  sleep_on inner;
  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    return inner.await_suspend(h);
  }
  // Belt gate: the .nothrow() opt-out's historical silent-status
  // return under -fno-exceptions is REVOKED — dependent
  // static_assert so mere header inclusion stays legal.
  template <typename Unused = void>
  aorta_result_t await_resume() const noexcept {
#if !defined(__cpp_exceptions)
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "flow::sleep_on{exec, delay}.nothrow() is not "
                  "reachable under -fno-exceptions either — build this "
                  "translation unit with exceptions enabled, or use the "
                  "C++17 baseline aorta SDK (callbacks/executors/pull "
                  "subscribers)");
#endif
    return inner.status;
  }
};
inline sleep_on::nothrow_awaiter sleep_on::nothrow() const noexcept {
  return nothrow_awaiter{*this};
}

// ---------------------------------------------------------------------------
// spawn — run a Task<T> as a child of `scope` on `executor` (structured:
// the scope's close() waits for it; its slot closes at final_suspend).
// Returns a TaskHandle<T>: `.ok()`/`.status` report the ADMISSION
// (scope closing / executor stopped — the earlier contract), and the handle
// additionally carries the child's outcome — co_await it, or
// take_outcome() after the scope closed. Discarding the handle is
// fire-and-forget, exactly the earlier behavior.
// ---------------------------------------------------------------------------
namespace detail {
/// The initial-dispatch routed delivery — resume the frame at
/// `ctx` (the coroutine address). The stamp is a recorded no-op: the
/// first-slice contract delivers cancellation through the body's first
/// verb, not through a pre-resume stamp.
inline void deliver_address_resume(void* ctx) {
  std::coroutine_handle<>::from_address(ctx).resume();
}
inline void stamp_first_slice_noop(void* /*ctx*/) {}


/// The bound initial-dispatch delivery channels: unlike the
/// unbound first slice (whose stamp is a recorded no-op — the body's
/// first verb observes the cancellation), a BOUND start's CLOSED/drop
/// delivery must not execute ONE user statement (two racing CLOSED
/// deliveries would run first segments concurrently, violating the
/// domain's serial guarantee).
/// The stamp/drop channels set `start_cancelled` on the root promise;
/// the root is always an SDK wrapper whose co_await LINK refuses to
/// start the user child (Task::await_suspend) and delivers the teardown
/// cancel. Templated on the concrete promise so the type-erased address
/// can reach the promise.
template <typename P>
struct bound_start {
  static void run(void* addr) {
    std::coroutine_handle<P>::from_address(addr).resume();
  }
  static void drop(void* addr) {
    auto h = std::coroutine_handle<P>::from_address(addr);
    static_cast<promise_flow_context&>(h.promise()).start_cancelled = true;
    h.resume();
  }
  static void stamp(void* addr) {
    auto h = std::coroutine_handle<P>::from_address(addr);
    static_cast<promise_flow_context&>(h.promise()).start_cancelled = true;
  }
  /// The rollback-leg executor trampoline: a stop-drain
  /// delivery (ev->status != OK) stamps the pre-start cancel instead of
  /// running the body — the no-op domain keeps the zero-user-statement
  /// teardown contract on the executor plane too.
  static void trampoline(const aorta_resume_event_t* ev) noexcept {
    if (ev->status == AORTA_OK) {
      run(ev->handle);
    } else {
      drop(ev->handle);
    }
  }
};

// Dummy template parameter — see the forward declaration above.
template <typename TaskT>
inline SpawnResult spawn_detached(uint64_t executor, Scope scope,
                                  TaskT task) {
  serial_unbound_spawn_debug_aid();  // debug aid: executor-plane spawns
                                     // are unbound by construction
  uint64_t accounting = 0;
  aorta_result_t rc = aorta_scope_begin_task(scope.handle(), &accounting);
  if (rc != AORTA_OK) {
    return {rc, 0};
  }
  auto coro = std::exchange(task.coro_, nullptr);  // frame outlives the Task
  coro.promise().task_accounting = accounting;
  coro.promise().detached = true;  // self-destroys at final_suspend
  // Stamp the ROOT promise from the scope — priority AND the
  // ambient context (executor-only plane: rt = nullptr); co_await links
  // propagate the whole base.
  uint8_t scope_prio = 255;
  (void)aorta_scope_priority(scope.handle(), &scope_prio);
  detail::stamp_spawn_context(coro.promise(), nullptr, executor, scope,
                              scope_prio);
  // The first slice ignores the delivery status: a stop-drained start runs
  // its body inline on the draining thread, and its first resume_on reports
  // the cancellation. The initial dispatch routes
  // (unbound passthrough; the first-slice contract IS the stamp — a
  // cancelled start observes it at its first verb, so the stamp thunk is
  // a recorded no-op).
  auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
    route_resume(nullptr, std::coroutine_handle<>::from_address(ev->handle),
                 RouteDelivery{&deliver_address_resume,
                               &stamp_first_slice_noop, nullptr, nullptr, ev->handle});
  };
  rc = aorta_executor_post_resume(executor, trampoline, coro.address(),
                                  scope_prio);
  if (rc != AORTA_OK) {
    // Executor stopped: run the frame inline so it observes cancellation and
    // unwinds (final_suspend still balances the accounting). Routed.
    route_resume(nullptr, coro,
                 RouteDelivery{&deliver_address_resume,
                               &stamp_first_slice_noop, nullptr, nullptr, coro.address()});
    return {rc, accounting};
  }
  return {AORTA_OK, accounting};
}

/// The handle plumbing shared by both spawn planes: wrap the payload,
/// spawn detached, deliver a never-started refusal so a refused handle
/// resolves instead of parking forever.
template <typename T, typename K, typename SpawnFn>
TaskHandle<T> spawn_with_handle(Task<T, K> task, const Scope& scope,
                                SpawnFn&& do_spawn) {
  // Move-only / non-default-constructible T is fully supported —
  // the Outcome variant never constructs a T on the stop paths.
  auto hs = std::make_shared<handle_state<T>>();
  SpawnResult admitted = do_spawn(handle_wrapper(std::move(task), hs));
  if (!admitted.ok() && admitted.id == 0) {
    // The scope refused the slot: the wrapper frame never STARTED (a
    // refused first post still runs the frame inline — that path
    // delivers through the guard). Deliver the refusal here so an await
    // resolves immediately — with the refusal-time reason snapshot
    // (gate 2.2: this IS the stop time for a never-started child).
    hs->stop_raw_reason = scope.raw_cancel_reason();
    hs->child_status.store(admitted.status, std::memory_order_relaxed);
    hs->done.store(true, std::memory_order_seq_cst);
  }
  return handle_access::make<T>(admitted.status, admitted.id, std::move(hs));
}
}  // namespace detail

/// PARAMETER-ORDER EXEMPTION: `spawn_on` takes `scope` right after the
/// plane, NOT last — spawn has no ambient form (a root task has no
/// frame to inherit `{rt, scope}` from at await_suspend), so the
/// "scope LAST" convention for ambient-eligible verbs does not apply
/// here. `scope` is the STRUCTURAL OWNER of the spawned child (nursery
/// pattern; Rust `Scope::spawn` parity: `scope.spawn(...)` reads owner
/// first) — every overload below (executor plane, SDK-wrapper, and the
/// LaneRuntime plane further down) keeps this shape. Mirrors `join`'s
/// recorded exemption in flow_combinators.hpp for the identical
/// plane-then-scope reasoning.
template <typename T = void, typename K = task_kind_regular>
[[nodiscard]] TaskHandle<T> spawn_on(uint64_t executor, Scope scope,
                                     Task<T, K> task) {
  static_assert(!is_endpoint_task_v<Task<T, K>>,
                "spawn the CONSUMER task and co_await the endpoint verb "
                "inside it — a bare endpoint verb is not a root task");
  return detail::spawn_with_handle(
      std::move(task), scope, [executor, &scope](Task<void> wrapper) {
        // COPY the scope into spawn_detached (shared-ownership refcount
        // bump): spawn_with_handle reads scope.raw_cancel_reason() on the
        // refusal path AFTER this call — moving here turned every
        // closed-scope refusal into the invalid-scope kExplicit
        // fail-safe (caught by the close_vs_racing_spawn TCK case).
        return detail::spawn_detached(executor, scope, std::move(wrapper));
      });
}

// SDK-wrapper overload (Flow must compose with the C++ SDK's
// own objects — no second raw-handle plumbing next to aorta::Executor).
template <typename T = void, typename K = task_kind_regular>
[[nodiscard]] TaskHandle<T> spawn_on(const aorta::Executor& executor,
                                     Scope scope, Task<T, K> task) {
  return spawn_on(executor.handle(), std::move(scope), std::move(task));
}

// ---------------------------------------------------------------------------
// resume_on_lane: hop onto the SDK lane
// pool instead of the core executor. Same teardown semantics as resume_on: a
// teardown delivery (`drop` = resume-into-cancellation with the DELIVERY
// status set to CANCELLED) unwinds via cancelled_unwind; a
// REFUSED post (pool closed) resumes INLINE with the status recorded.
// Posts are budget-EXEMPT (post_resume_item; bounded by live frames).
// ---------------------------------------------------------------------------
struct resume_on_lane {
  aorta::flow::LaneRuntime* runtime;
  aorta_result_t status = AORTA_OK;
  std::coroutine_handle<> handle_ = nullptr;

  // Type-level status-branch opt-out — see resume_on::nothrow_awaiter.
  struct nothrow_awaiter;
  nothrow_awaiter nothrow() const noexcept;

  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    handle_ = h;
    uint8_t prio = 255;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      prio = h.promise().priority;
    }
    // A BOUND frame either re-enters its own
    // domain (same runtime — the router owns the delivery) or is refused
    // BEFORE any foreign arm exists (the verb's "continue on that
    // lane pool" contract cannot be honored inside the domain when the
    // pool is a different runtime's).
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_binding_of(&fctx) != nullptr) {
        if (detail::serial_foreign_arm(fctx, runtime)) {
          detail::log_refused_await("flow::resume_on_lane [serial: foreign "
                                    "runtime arm]",
                                    AORTA_ERR_INVALID_ARGUMENT);
          status = AORTA_ERR_INVALID_ARGUMENT;
          return false;
        }
        switch (detail::route_arm(&fctx,
                                  &resume_on_lane::deliver_bound_run_thunk,
                                  &resume_on_lane::deliver_cancel_thunk,
                                  this)) {
          case detail::SerialArmRoute::kPosted:
            return true;  // the domain member owns the delivery
          case detail::SerialArmRoute::kClosed:
            status = AORTA_ERR_CANCELLED;  // return-false degradation
            return false;
          case detail::SerialArmRoute::kUnbound:
            break;  // no-op domain: bound, not routed — verbatim body
        }
      }
    }
    aorta::lanes::Work w;
    w.ctx = this;
    // Both delivery channels route (unbound passthrough).
    // run: a normal dispatch — deliver AORTA_OK and resume.
    w.run = [](void* ctx) {
      auto* self = static_cast<resume_on_lane*>(ctx);
      self->status = AORTA_OK;
      detail::route_resume(
          nullptr, self->handle_,
          detail::RouteDelivery{&resume_on_lane::deliver_unbound_thunk,
                                &resume_on_lane::stamp_teardown_thunk, nullptr, nullptr, self});
    };
    // drop: resume-into-cancellation — the frame unwinds
    // because THIS DELIVERY was cancelled (executor stop ≠ scope cancel).
    w.drop = [](void* ctx) {
      auto* self = static_cast<resume_on_lane*>(ctx);
      detail::route_resume(
          nullptr, self->handle_,
          detail::RouteDelivery{&resume_on_lane::deliver_cancel_thunk,
                                &resume_on_lane::stamp_teardown_thunk, nullptr, nullptr, self});
    };
    if (!runtime->enabled()) {
      // Rollback mode: the core executor carries the hop.
      auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
        auto* self = static_cast<resume_on_lane*>(ev->handle);
        self->status = ev->status;
        detail::route_resume(
            nullptr, self->handle_,
            detail::RouteDelivery{&resume_on_lane::deliver_unbound_thunk,
                                  &resume_on_lane::stamp_teardown_thunk, nullptr, nullptr,
                                  self});
      };
      const aorta_result_t post_rc = aorta_executor_post_resume(
          runtime->fallback_executor(), trampoline, this, prio);
      if (post_rc != AORTA_OK) {
        // NOT_FOUND (stale executor) is misuse — logged before the
        // CANCELLED flatten below; a stopped executor stays quiet.
        detail::log_refused_await("flow::resume_on_lane", post_rc);
        status = AORTA_ERR_CANCELLED;
        return false;
      }
      return true;
    }
    aorta::lanes::Post rc = runtime->post_resume_class(prio, w);
    if (rc != aorta::lanes::Post::OK) {
      // Pool closed: record the refusal, resume INLINE (refused-post
      // path — the caller-owned drop; nothing was enqueued).
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    return true;  // do NOT touch the frame past the post
  }
  /// Routed deliveries (verbatim pre-router behavior).
  static void deliver_unbound_thunk(void* ud) {
    static_cast<resume_on_lane*>(ud)->handle_.resume();
  }
  static void deliver_cancel_thunk(void* ud) {
    auto* self = static_cast<resume_on_lane*>(ud);
    self->status = AORTA_ERR_CANCELLED;
    self->handle_.resume();
  }
  static void stamp_teardown_thunk(void* ud) {
    static_cast<resume_on_lane*>(ud)->status = AORTA_ERR_CANCELLED;
  }
  /// Bound member run: an in-domain dispatch delivers AORTA_OK
  /// and resumes inside the strand's drain slice.
  static void deliver_bound_run_thunk(void* ud) {
    auto* self = static_cast<resume_on_lane*>(ud);
    self->status = AORTA_OK;
    self->handle_.resume();
  }
  // Default unwinding mode; dependent -fno-exceptions gate (see
  // resume_on::await_resume).
  template <typename Unused = void>
  aorta_result_t await_resume() const {
#if defined(__cpp_exceptions)
    if (status != AORTA_OK) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // `.nothrow()` is NOT an -fno-exceptions
    // escape hatch — its own await_resume (resume_on_lane::nothrow_awaiter)
    // hits the same belt gate below. There is no in-mode workaround.
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK "
                  "(callbacks/executors/pull subscribers)");
#endif
    return status;
  }
};

struct resume_on_lane::nothrow_awaiter {
  resume_on_lane inner;
  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    return inner.await_suspend(h);
  }
  // Belt gate: the .nothrow() opt-out's historical silent-status
  // return under -fno-exceptions is REVOKED — dependent
  // static_assert so mere header inclusion stays legal.
  template <typename Unused = void>
  aorta_result_t await_resume() const noexcept {
#if !defined(__cpp_exceptions)
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "flow::resume_on_lane{&rt}.nothrow() is not "
                  "reachable under -fno-exceptions either — build this "
                  "translation unit with exceptions enabled, or use the "
                  "C++17 baseline aorta SDK (callbacks/executors/pull "
                  "subscribers)");
#endif
    return inner.status;
  }
};
inline resume_on_lane::nothrow_awaiter resume_on_lane::nothrow()
    const noexcept {
  return nothrow_awaiter{*this};
}

// ---------------------------------------------------------------------------
// resume — the ambient unified hop: re-queue this frame
// on ITS OWN plane, resolved from the promise context at await_suspend.
// Lane plane (rt present + enabled()): a budget-exempt lane post —
// resume_on_lane's enabled arm sequence, byte-identical after the bind.
// Executor plane (rt null OR Disabled): a queue post on the context
// executor — resume_on's arm sequence. NOTE: on an enabled
// MT runtime this is a yield/requeue-at-frame-priority — the frame may
// resume on a lane worker again; a caller that needs the executor
// dispatch thread specifically uses the explicit resume_on{exec}.
//
// Refusal semantics (information-preserving): the raw
// refused-post status is preserved end-to-end (`.nothrow()` can tell
// NOT_FOUND from CANCELLED) and `log_refused_await` fires AT THE REFUSAL
// SITE, so misuse observability is plane-independent; lifecycle codes
// stay quiet. The default mode unwinds on any non-OK delivery;
// `.nothrow()` returns the raw status (exceptions-ON ergonomics). BOTH
// modes are a COMPILE error under -fno-exceptions (C++ Flow requires
// exception support program-wide).
// ---------------------------------------------------------------------------
struct resume {
  aorta_result_t status = AORTA_OK;
  std::coroutine_handle<> handle_ = nullptr;

  // Type-level status-branch opt-out — see resume_on::nothrow_awaiter.
  struct nothrow_awaiter;
  nothrow_awaiter nothrow() const noexcept;

  bool await_ready() const noexcept { return false; }
  template <typename P>
    requires detail::FlowPromise<P>
  bool await_suspend(std::coroutine_handle<P> h) {
    handle_ = h;
    const auto& ctx =
        static_cast<const detail::promise_flow_context&>(h.promise());
    if (ctx.scope.is_cancelled()) {
      // Entry gate (ambient_sleep parity): a hop from a
      // CANCELLED frame scope refuses — one atomic load; the unwind's
      // reason snapshots from the scope at the wrapper's catch.
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    const uint8_t prio = ctx.priority;
    // Ambient resume is ALWAYS same-plane (the
    // promise's own runtime) — a bound frame re-enters its domain, no
    // foreign-arm case exists.
    switch (detail::route_arm(&ctx, &resume::deliver_bound_run_thunk,
                              &resume::deliver_cancel_thunk, this)) {
      case detail::SerialArmRoute::kPosted:
        return true;  // the domain member owns the delivery
      case detail::SerialArmRoute::kClosed:
        status = AORTA_ERR_CANCELLED;  // return-false degradation
        return false;
      case detail::SerialArmRoute::kUnbound:
        break;  // verbatim pre-domain body below
    }
    if (ctx.rt != nullptr && ctx.rt->enabled()) {
      // Lane plane: budget-exempt resume post (resume_on_lane semantics).
      // Both channels route.
      aorta::lanes::Work w;
      w.ctx = this;
      w.run = [](void* c) {
        auto* self = static_cast<resume*>(c);
        self->status = AORTA_OK;
        detail::route_resume(
            nullptr, self->handle_,
            detail::RouteDelivery{&resume::deliver_unbound_thunk,
                                  &resume::stamp_teardown_thunk, nullptr, nullptr, self});
      };
      w.drop = [](void* c) {
        auto* self = static_cast<resume*>(c);
        detail::route_resume(
            nullptr, self->handle_,
            detail::RouteDelivery{&resume::deliver_cancel_thunk,
                                  &resume::stamp_teardown_thunk, nullptr, nullptr, self});
      };
      if (ctx.rt->post_resume_class(prio, w) != aorta::lanes::Post::OK) {
        // Pool closed: a lifecycle teardown — quiet, resume INLINE
        // (refused-post path; nothing was enqueued).
        status = AORTA_ERR_CANCELLED;
        return false;
      }
      return true;  // do NOT touch the frame past the post
    }
    // Executor plane (rt null or Disabled): the context executor carries
    // the hop (resume_on semantics).
    auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
      auto* self = static_cast<resume*>(ev->handle);
      self->status = ev->status;
      detail::route_resume(
          nullptr, self->handle_,
          detail::RouteDelivery{&resume::deliver_unbound_thunk,
                                &resume::stamp_teardown_thunk, nullptr, nullptr, self});
    };
    const aorta_result_t rc =
        aorta_executor_post_resume(ctx.exec, trampoline, this, prio);
    if (rc != AORTA_OK) {
      // Raw status preserved for .nothrow(); the refusal (NOT_FOUND =
      // stale executor handle) is logged AT THE REFUSAL SITE so misuse
      // observability is plane-independent (lifecycle codes no-op inside
      // the helper). Resume INLINE.
      detail::log_refused_await("flow::resume", rc);
      status = rc;
      return false;
    }
    return true;
  }
  template <typename P>  // teaching fallback
  bool await_suspend(std::coroutine_handle<P>) {
    static_assert(
        detail::dependent_false<P>::value,
        "flow ambient verbs require a flow coroutine: the frame must be "
        "spawned via flow::spawn_on (or awaited from one). Use the "
        "explicit-argument overload (resume_on / resume_on_lane) outside "
        "flow tasks.");
    return false;
  }
  /// Ambient routed deliveries (verbatim pre-router behavior).
  static void deliver_unbound_thunk(void* ud) {
    static_cast<resume*>(ud)->handle_.resume();
  }
  static void deliver_cancel_thunk(void* ud) {
    auto* self = static_cast<resume*>(ud);
    self->status = AORTA_ERR_CANCELLED;
    self->handle_.resume();
  }
  static void stamp_teardown_thunk(void* ud) {
    static_cast<resume*>(ud)->status = AORTA_ERR_CANCELLED;
  }
  /// Bound member run (ambient half) — in-domain dispatch.
  static void deliver_bound_run_thunk(void* ud) {
    auto* self = static_cast<resume*>(ud);
    self->status = AORTA_OK;
    self->handle_.resume();
  }
  // Default: unwind on any non-OK delivery. A refusal was already logged
  // at the refusal site — no second log here. Dependent -fno-exceptions
  // gate (see resume_on::await_resume).
  template <typename Unused = void>
  aorta_result_t await_resume() const {
#if defined(__cpp_exceptions)
    if (status != AORTA_OK) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // `.nothrow()` is NOT an -fno-exceptions
    // escape hatch — its own await_resume (resume::nothrow_awaiter) hits
    // the same belt gate below. There is no in-mode workaround.
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK "
                  "(callbacks/executors/pull subscribers)");
#endif
    return status;
  }
};

struct resume::nothrow_awaiter {
  resume inner;
  bool await_ready() const noexcept { return false; }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    return inner.await_suspend(h);  // foreign frames hit the teaching gate
  }
  // Belt gate: the .nothrow() opt-out's historical silent-status
  // return under -fno-exceptions is REVOKED — dependent
  // static_assert so mere header inclusion stays legal.
  template <typename Unused = void>
  aorta_result_t await_resume() const noexcept {
#if !defined(__cpp_exceptions)
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "flow::resume().nothrow() is not reachable under "
                  "-fno-exceptions either — build this translation unit "
                  "with exceptions enabled, or use the C++17 baseline "
                  "aorta SDK (callbacks/executors/pull subscribers)");
#endif
    return inner.status;
  }
};
inline resume::nothrow_awaiter resume::nothrow() const noexcept {
  return nothrow_awaiter{*this};
}

// spawn_on — lane-pool overload: run a Task<T> as a child
// of `scope` with its first slice and every subsequent resume dispatched
// on the lane pool. Same accounting/refusal contract as the executor
// overload; a refused first post runs the frame inline so it observes
// cancellation and unwinds.
namespace detail {
// Dummy template parameter — see the forward declaration above.
template <typename TaskT>
inline SpawnResult spawn_detached(aorta::flow::LaneRuntime& runtime,
                                  Scope scope, TaskT task,
                                  const SerialDomain* serial) {
  uint64_t accounting = 0;
  aorta_result_t rc = aorta_scope_begin_task(scope.handle(), &accounting);
  if (rc != AORTA_OK) {
    return {rc, 0};
  }
  auto coro = std::exchange(task.coro_, nullptr);
  coro.promise().task_accounting = accounting;
  coro.promise().detached = true;
  uint8_t scope_prio = 255;
  (void)aorta_scope_priority(scope.handle(), &scope_prio);
  // The lane plane stamps {&runtime, fallback executor} — ALSO on
  // the rollback leg below (the old delegation to the executor overload
  // would have stamped rt = nullptr, collapsing "disabled" into "absent"
  // and refusing ambient arbiter verbs exactly when rollback parity needs
  // them).
  detail::stamp_spawn_context(coro.promise(), &runtime,
                              runtime.fallback_executor(), scope, scope_prio);
  if (serial != nullptr) {
    // The domain binding rides the root stamp — an OWNING copy,
    // in place before the first dispatch below so the initial router sees
    // it (first-segment exclusion).
    coro.promise().serial = *serial;
    // Monotone rule: a bound task declared MORE urgent
    // than its domain executes at the DOMAIN class (the strand rides
    // the domain's lane) — WARN once per domain. ROUTING probe, not the
    // binding one: a no-op domain has no
    // strand — its tasks genuinely dispatch at their own class, so the
    // WARN's claim would be untrue there.
    if (detail::SerialDomainState* sst =
            detail::serial_state_of(&coro.promise())) {
      if (lane_class_for_u8(scope_prio) < lane_class_for_u8(sst->klass) &&
          !sst->monotone_warned.exchange(true, std::memory_order_acq_rel)) {
        detail::serial_diag_log(
            LogLevel::kWarn, "serial-domain ", sst->name.c_str(),
            ": a bound task's priority is above the domain's declared "
            "class — it executes at the DOMAIN class (monotone rule, "
            "F-3; warned once per domain)");
      }
    }
  } else {
    serial_unbound_spawn_debug_aid();  // unbound-spawn debug aid
  }
  // The delivery SEMANTICS follow the BINDING probe
  // — a no-op domain (ST/Disabled, no strand) skips the domain
  // ROUTING but keeps the pre-start-cancel contract: its teardown
  // starts (stop-drain, queued drop, refused post) must not execute one
  // user statement either, or racing refused posts run first segments
  // concurrently on producer threads. Only `bound` selects the thunks;
  // `routed` (group present) additionally selects the domain member
  // path.
  using RootPromise = typename TaskT::promise_type;
  const bool bound = detail::serial_binding_of(&coro.promise()) != nullptr;
  if (!runtime.enabled()) {
    // Rollback mode: core executor posting (same first-slice contract as
    // the executor overload — the twin lives there). Routed.
    // Bound: the trampoline selects run/drop by the
    // DELIVERY status — a stop-drain delivery stamps the pre-start
    // cancel instead of running the body.
    if (bound) {
      rc = aorta_executor_post_resume(runtime.fallback_executor(),
                                      &bound_start<RootPromise>::trampoline,
                                      coro.address(), scope_prio);
      if (rc != AORTA_OK) {
        bound_start<RootPromise>::drop(coro.address());  // zero user stmts
        return {rc, accounting};
      }
      return {AORTA_OK, accounting};
    }
    auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
      route_resume(nullptr,
                   std::coroutine_handle<>::from_address(ev->handle),
                   RouteDelivery{&deliver_address_resume,
                                 &stamp_first_slice_noop, nullptr, nullptr, ev->handle});
    };
    rc = aorta_executor_post_resume(runtime.fallback_executor(), trampoline,
                                    coro.address(), scope_prio);
    if (rc != AORTA_OK) {
      // refused: inline, observes cancellation, unwinds — routed.
      route_resume(nullptr, coro,
                   RouteDelivery{&deliver_address_resume,
                                 &stamp_first_slice_noop, nullptr, nullptr, coro.address()});
      return {rc, accounting};
    }
    return {AORTA_OK, accounting};
  }
  // A BOUND spawn's first slice is a DOMAIN
  // MEMBER — the frame enters its domain BEFORE the first user segment
  // (first-segment exclusion). The CLOSED/drop channels stamp the
  // pre-start cancellation: the SDK wrapper root's body
  // runs its delivery obligations, and the co_await link refuses to
  // start the user child — no user statement executes on a teardown
  // delivery, inline or queued-drop alike.
  if (detail::serial_state_of(&coro.promise()) != nullptr) {
    route_resume(&coro.promise(), coro,
                 RouteDelivery{&deliver_address_resume,
                               &bound_start<RootPromise>::stamp,
                               &bound_start<RootPromise>::run,
                               &bound_start<RootPromise>::drop,
                               coro.address()});
    return {AORTA_OK, accounting};
  }
  aorta::lanes::Work w;
  w.ctx = coro.address();
  if (bound) {
    // Enabled-ST no-op domain: the lane post rides the
    // unrouted machinery, but the DROP channel and the refused-post
    // inline degradation stamp the pre-start cancel.
    w.run = &bound_start<RootPromise>::run;
    w.drop = &bound_start<RootPromise>::drop;
    aorta::lanes::Post prc = runtime.post_resume_class(scope_prio, w);
    if (prc != aorta::lanes::Post::OK) {
      bound_start<RootPromise>::drop(coro.address());  // zero user stmts
      return {AORTA_ERR_CANCELLED, accounting};
    }
    return {AORTA_OK, accounting};
  }
  // First slice: run and drop both RESUME (a stop-drained start runs its
  // body inline on the draining thread; its first resume_on_lane reports
  // the cancellation — same contract as the executor overload).
  // Both channels route.
  w.run = [](void* ctx) {
    route_resume(nullptr, std::coroutine_handle<>::from_address(ctx),
                 RouteDelivery{&deliver_address_resume,
                               &stamp_first_slice_noop, nullptr, nullptr, ctx});
  };
  w.drop = [](void* ctx) {
    route_resume(nullptr, std::coroutine_handle<>::from_address(ctx),
                 RouteDelivery{&deliver_address_resume,
                               &stamp_first_slice_noop, nullptr, nullptr, ctx});
  };
  aorta::lanes::Post prc = runtime.post_resume_class(scope_prio, w);
  if (prc != aorta::lanes::Post::OK) {
    // refused: inline, observes cancellation, unwinds — routed.
    route_resume(nullptr, coro,
                 RouteDelivery{&deliver_address_resume,
                               &stamp_first_slice_noop, nullptr, nullptr, coro.address()});
    return {AORTA_ERR_CANCELLED, accounting};
  }
  return {AORTA_OK, accounting};
}
}  // namespace detail

template <typename T = void, typename K = task_kind_regular>
[[nodiscard]] TaskHandle<T> spawn_on(aorta::flow::LaneRuntime& runtime,
                                     Scope scope, Task<T, K> task) {
  static_assert(!is_endpoint_task_v<Task<T, K>>,
                "spawn the CONSUMER task and co_await the endpoint verb "
                "inside it — a bare endpoint verb is not a root task");
  return detail::spawn_with_handle(
      std::move(task), scope, [&runtime, &scope](Task<void> wrapper) {
        // COPY, not move — see the executor overload: the refusal path
        // reads the scope after this lambda runs.
        return detail::spawn_detached(runtime, scope, std::move(wrapper));
      });
}

/// The DOMAIN-BOUND spawn (explicit
/// spawn does not inherit; binding is a spawn-site decision, and THIS
/// overload is the binding spelling; `Runtime::spawn(scope, d, task)`
/// delegates here). An invalid or runtime-dead domain refuses with
/// a handle that RESOLVES (INVALID_ARGUMENT), never parks; a domain
/// created by a DIFFERENT runtime is the same loud-invalid shape.
template <typename T = void, typename K = task_kind_regular>
[[nodiscard]] TaskHandle<T> spawn_on(aorta::flow::LaneRuntime& runtime,
                                     Scope scope, const SerialDomain& d,
                                     Task<T, K> task) {
  static_assert(!is_endpoint_task_v<Task<T, K>>,
                "spawn the CONSUMER task and co_await the endpoint verb "
                "inside it — a bare endpoint verb is not a root task");
  if (!d.valid()) {
    detail::log_refused_await("flow::spawn [serial: invalid or torn-down "
                              "domain]",
                              AORTA_ERR_INVALID_ARGUMENT);
    return detail::handle_access::refused<T>(AORTA_ERR_INVALID_ARGUMENT);
  }
  const auto& st = detail::serial_access::state(d);
  if (st->rt != nullptr && st->rt != &runtime) {
    detail::log_refused_await("flow::spawn [serial: domain belongs to a "
                              "different runtime]",
                              AORTA_ERR_INVALID_ARGUMENT);
    return detail::handle_access::refused<T>(AORTA_ERR_INVALID_ARGUMENT);
  }
#ifdef AORTA_LANES_TEST_SEAMS
  // Test seam: the admit-vs-stop race window — a stop landing
  // AFTER the d.valid()/pairing checks and BEFORE the dispatch is the
  // race arm the teardown-start rows must drive through the PUBLIC
  // spawn. Seam-test targets only.
  if (auto* seam =
          detail::bound_spawn_admit_seam.load(std::memory_order_acquire)) {
    seam(detail::bound_spawn_admit_seam_ctx.load(std::memory_order_acquire));
  }
#endif
  return detail::spawn_with_handle(
      std::move(task), scope, [&runtime, &scope, &d](Task<void> wrapper) {
        return detail::spawn_detached(runtime, scope, std::move(wrapper), &d);
      });
}

// ---------------------------------------------------------------------------
// co_invoke — makes CAPTURING-LAMBDA coroutines safe (
// the folly::coro::co_invoke pattern). The wrapper coroutine copies the
// callable and its arguments into ITS OWN frame, then invokes and awaits
// from inside: the closure now outlives every suspension of the inner
// frame, so captures cannot dangle. Failure/cancel from the inner Task
// propagates outward through the stored exception_ptr (see promise_type).
// ---------------------------------------------------------------------------
namespace detail {
template <typename>
struct is_flow_task : std::false_type {};
template <typename T, typename K>
struct is_flow_task<Task<T, K>> : std::true_type {};
}  // namespace detail

template <typename F, typename... Args>
Task<void> co_invoke(F f, Args... args) {  // parameters COPY into the frame
  static_assert(
      detail::is_flow_task<
          std::remove_cvref_t<decltype(std::move(f)(std::move(args)...))>>::value,
      "co_invoke requires F(Args...) to return an aorta::flow::Task<T>");
  co_await std::move(f)(std::move(args)...);
}

// ---------------------------------------------------------------------------
// spawn_with — the Rust `Scope::spawn_with` shape, ported: injects a REAL
// CHILD scope as the callable's first parameter and routes the callable
// through co_invoke. One call shape that makes both capturing-lambda
// lifetime and Scope lifetime non-issues:
//
//   flow::spawn_with(*exec, scope,
//       [](flow::Scope s, int x) -> flow::Task<void> { ... }, 42);
//
// The body's `s` is `scope.child()` — Rust `Scope::spawn_with` parity:
// `s.cancel()` inside the body cancels only THIS task's subtree, never the
// parent scope or its sibling tasks, and the body's own nested spawns hang
// off the child. The task itself still spawns under the PARENT `scope`
// (the structural owner — close()/shutdown wait for it there).
//
// PARAMETER-ORDER EXEMPTION: `scope` sits before the callable, not last —
// same structural-owner reasoning as the spawn_on group above (nursery
// pattern, Rust `Scope::spawn_with` parity), reinforced here by a
// second, independent constraint: the callable's forwarded pack
// `Args...` MUST be the trailing parameters for C++ template argument
// deduction to work at all, so `scope` could not move last even if the
// owner convention alone allowed it.
// ---------------------------------------------------------------------------
template <typename F, typename... Args>
[[nodiscard]] TaskHandle<> spawn_with(uint64_t executor, Scope scope, F f,
                                      Args... args) {
  Scope for_body = scope.child();  // REAL child scope handed to the body
  return spawn_on(executor, std::move(scope),
                  co_invoke(std::move(f), std::move(for_body),
                            std::move(args)...));
}

template <typename F, typename... Args>
[[nodiscard]] TaskHandle<> spawn_with(const aorta::Executor& executor,
                                      Scope scope, F f, Args... args) {
  return spawn_with(executor.handle(), std::move(scope), std::move(f),
                    std::move(args)...);
}

// Lane-plane overload (parity: every spawn shape exists on both
// planes — the executor overloads above, this one on the LaneRuntime).
template <typename F, typename... Args>
[[nodiscard]] TaskHandle<> spawn_with(aorta::flow::LaneRuntime& runtime,
                                      Scope scope, F f, Args... args) {
  Scope for_body = scope.child();  // REAL child scope handed to the body
  return spawn_on(runtime, std::move(scope),
                  co_invoke(std::move(f), std::move(for_body),
                            std::move(args)...));
}

// ---------------------------------------------------------------------------
// call — awaitable RPC inside a scope (aorta_client_call_ctx):
// suspends, lets the runtime drive the query, resumes on `executor` with the
// reply (or the error code — cancellation surfaces AORTA_ERR_CANCELLED).
// ---------------------------------------------------------------------------
class call {
 public:
  // `scope` is taken BY VALUE: the awaitable (which lives in the coroutine
  // frame across the suspension) CO-OWNS the scope, so a caller handing in
  // a temporary or dropping their copy before resume is safe by
  // construction.
  // Frozen order: primary object, plane, payload, scope LAST.
  call(uint64_t client, uint64_t executor, const uint8_t* payload,
       size_t len, Scope scope)
      : client_(client),
        scope_(std::move(scope)),
        executor_(executor),
        payload_(payload),
        len_(len) {}

  // SDK-wrapper + span constructor: use your existing
  // aorta::Client / aorta::Executor objects directly.
  call(const aorta::Client& client, const aorta::Executor& executor,
       std::span<const uint8_t> payload, Scope scope)
      : call(client.handle(), executor.handle(), payload.data(),
             payload.size(), std::move(scope)) {}

  // AMBIENT constructors: scope + executor resolve from
  // the frame's promise context at await_suspend; client and payload stay
  // explicit. Same entry-gating and mid-flight cancel semantics as the
  // explicit form — only who writes the plumbing changes.
  call(uint64_t client, const uint8_t* payload, size_t len)
      : client_(client), executor_(0), payload_(payload), len_(len) {
    ambient_ = true;
  }
  call(const aorta::Client& client, std::span<const uint8_t> payload)
      : call(client.handle(), payload.data(), payload.size()) {}

  /// Result shape for the `.nothrow()` opt-out: both layers explicit.
  struct call_result {
    /// Delivery layer: AORTA_OK = resumed by a live executor;
    /// AORTA_ERR_CANCELLED = stop-drain / refused-post (teardown).
    aorta_result_t delivery = AORTA_OK;
    /// RPC layer: the reply or the call-level error (cancel/timeout/...).
    /// No default: the Outcome is value-or-stopped by construction.
    Outcome<std::vector<uint8_t>> reply;
  };

  // Opt-out of the unwind default (exceptions-build ergonomics — Flow
  // compile-gates ALL its awaits under -fno-exceptions):
  // `co_await call{...}.nothrow()` yields BOTH layers as a
  // call_result — nothing thrown, nothing lost. (Defined after the class:
  // it returns a dedicated awaiter that owns this call.)
  class nothrow_awaiter;
  nothrow_awaiter nothrow() && noexcept;

  bool await_ready() const noexcept { return false; }

  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    if constexpr (detail::FlowPromise<P>) {
      // The pre-existing priority read, widened to the whole
      // frame context — an AMBIENT call binds scope + exec
      // from the promise here; explicit arguments stay untouched
      // (explicit wins per call).
      const auto& ctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      priority_ = ctx.priority;
      if (ambient_) {
        scope_ = ctx.scope;
        executor_ = ctx.exec;
        context_bound_ = true;
      }
      // A bound frame's completion re-enters its
      // domain (fctx_ read by on_done's router call); the EXPLICIT form
      // compares its executor argument against the domain runtime's
      // fallback executor HERE, at the stamp — foreign → refusal
      // before any RPC is issued.
      if (detail::SerialDomainState* sst = detail::serial_binding_of(&ctx)) {
        if (!ambient_ && executor_ != 0 && sst->rt != nullptr &&
            executor_ != sst->rt->fallback_executor()) {
          detail::log_refused_await(
              "flow::call [serial: foreign executor stamp]",
              AORTA_ERR_INVALID_ARGUMENT);
          result_.emplace(Outcome<std::vector<uint8_t>>::Stop(
              Stopped::Fault(aorta::Status(
                  AORTA_ERR_INVALID_ARGUMENT,
                  "flow::call on a bound frame names a foreign executor"))));
          return false;
        }
        fctx_ = &ctx;
      }
    } else if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      priority_ = h.promise().priority;  // reply resumes at frame prio
    }
    continuation_ = h;
    if (ambient_ && !context_bound_) {
      // An AMBIENT call awaited from a non-flow coroutine has no context
      // to bind (the explicit form is the surface for those frames).
      // Loud RPC-layer refusal, inline on this live thread — same shape
      // as an entry refusal; `.nothrow()` sees the raw code.
      detail::log_refused_await("flow::call", AORTA_ERR_INVALID_ARGUMENT);
      result_.emplace(Outcome<std::vector<uint8_t>>::Stop(Stopped::Fault(
          aorta::Status(AORTA_ERR_INVALID_ARGUMENT,
                        "ambient flow::call on a non-flow frame"))));
      return false;
    }
    slice_ = aorta_bytes_slice_t{payload_, len_};
    aorta_bytes_view_t request{&slice_, 1, len_};
    aorta_result_t rc = aorta_client_call_ctx(client_, scope_.handle(),
                                              &request, &call::on_done, this);
    if (rc != AORTA_OK) {
      // Entry refusal (scope already cancelled/expired): an RPC-layer
      // error, delivered inline on a live thread — NOT a teardown.
      // A cancelled-scope refusal keeps the scope's EXACT reason.
      if (rc == AORTA_ERR_CANCELLED) {
        const uint8_t raw = scope_.raw_cancel_reason();
        result_.emplace(Outcome<std::vector<uint8_t>>::Stop(
            raw != 0 ? Stopped::CancelledRaw(raw)
                     : Stopped::Cancelled(CancelReason::kExplicit)));
      } else {
        result_.emplace(Outcome<std::vector<uint8_t>>::Stop(
            Stopped::Fault(detail::outcome_status(rc))));
      }
      return false;
    }
    return true;
  }

  // DEFAULT: a teardown delivery unwinds the frame
  // (the reply, if any, is deliberately dropped — teardown outranks
  // results); RPC-layer errors stay explicit values in Outcome. Under
  // -fno-exceptions this default CANNOT signal teardown, so awaiting it
  // there is a COMPILE error (the static_assert is dependent — it
  // fires only when this mode is actually awaited in a no-exceptions TU).
  template <typename Unused = void>
  [[nodiscard]] Outcome<std::vector<uint8_t>> await_resume() {
#if defined(__cpp_exceptions)
    if (delivery_status_ != AORTA_OK) {
      detail::arm_unwind();
      throw cancelled_unwind{};
    }
#else
    // `.nothrow()` is NOT an -fno-exceptions
    // escape hatch — its own await_resume (call::nothrow_awaiter) hits
    // the same belt gate (its dependent static_assert). There is no
    // in-mode workaround.
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK "
                  "(callbacks/executors/pull subscribers)");
#endif
    if (!result_.has_value()) {
      // Unreachable by protocol (every resume path emplaced first);
      // classify defensively rather than deref an empty optional.
      return Outcome<std::vector<uint8_t>>::Stop(Stopped::Fault(
          aorta::Status(AORTA_ERR_INTERNAL, "call resumed with no result")));
    }
    return std::move(*result_);
  }

 private:
  static void on_done(aorta_result_t code, const aorta_bytes_view_t* payload,
                      const aorta_flow_completion_t* completion,
                      void* ud) noexcept {
    auto* self = static_cast<call*>(ud);
    // noexcept boundary: this is the actual extern-C
    // callback invoked FROM Rust — nothing may unwind across it. The
    // throwing work (vector growth) degrades to an RPC-level error.
#if defined(__cpp_exceptions)
    try {
#endif
      if (code == AORTA_OK) {
        std::vector<uint8_t> bytes;
        if (payload != nullptr) {
          bytes.reserve(payload->total_len);
          for (size_t i = 0; i < payload->slice_count; ++i) {
            const auto& s = payload->slices[i];
            bytes.insert(bytes.end(), s.data, s.data + s.len);
          }
        }
        self->result_.emplace(
            Outcome<std::vector<uint8_t>>::Value(std::move(bytes)));
      } else {
        // The completion record carries the exact reason /
        // fault code+text — consumed here, NULL-tolerant.
        self->result_.emplace(Outcome<std::vector<uint8_t>>::Stop(
            detail::stopped_from_completion(completion, code)));
      }
#if defined(__cpp_exceptions)
    } catch (...) {
      self->result_.emplace(Outcome<std::vector<uint8_t>>::Stop(
          Stopped::Fault(aorta::Status(AORTA_ERR_INTERNAL,
                                       "reply copy failed"))));
    }
#endif
    // The completion's continuation
    // delivery routes through THE router. Unbound: the pre-existing
    // executor-post machinery, verbatim. Bound: the reply
    // continuation becomes a domain member — the RPC result is already
    // parked in result_, so the member run resumes plainly inside the
    // strand's slice; the drop stamps the teardown first.
    detail::route_resume(
        self->fctx_, self->continuation_,
        detail::RouteDelivery{&call::deliver_unbound_thunk,
                              &call::stamp_teardown_thunk,
                              &call::deliver_bound_run_thunk,
                              &call::deliver_bound_drop_thunk, self});
  }

  /// The pre-router delivery machinery, verbatim (unbound path):
  /// resume on the executor (on_done runs on a runtime worker, a marked
  /// callback context — post, don't run user code there). Post refused →
  /// resume inline into cancellation. The posted handle
  /// is the awaiter (it lives in the frame). The two status LAYERS stay
  /// separate: the trampoline writes ONLY delivery_status_ — the RPC
  /// result is never overwritten.
  static void deliver_unbound_thunk(void* ud) {
    auto* self = static_cast<call*>(ud);
    auto trampoline = [](const aorta_resume_event_t* ev) noexcept {
      auto* inner = static_cast<call*>(ev->handle);
      inner->delivery_status_ = ev->status;
      inner->continuation_.resume();
    };
    const aorta_result_t post_rc = aorta_executor_post_resume(
        self->executor_, trampoline, self, self->priority_);
    if (post_rc != AORTA_OK) {
      // Refused post: uniform "you are not on the executor" teardown
      // delivery — same classification as a stop-drain.
      // A refusal code (stale executor handle) is logged before the
      // flatten; a stopped executor stays quiet.
      detail::log_refused_await("flow::call", post_rc);
      call::stamp_teardown_thunk(self);
      self->continuation_.resume();
    }
  }

  /// The caller-specific cancel stamp:
  /// strictly sequenced-before the resume on every teardown delivery.
  static void stamp_teardown_thunk(void* ud) {
    static_cast<call*>(ud)->delivery_status_ = AORTA_ERR_CANCELLED;
  }

  /// Bound member channels: run resumes plainly in the domain
  /// slice (the RPC outcome is already in result_); drop stamps the
  /// teardown strictly before the resume.
  static void deliver_bound_run_thunk(void* ud) {
    static_cast<call*>(ud)->continuation_.resume();
  }
  static void deliver_bound_drop_thunk(void* ud) {
    call::stamp_teardown_thunk(ud);
    static_cast<call*>(ud)->continuation_.resume();
  }

  uint64_t client_;
  Scope scope_;  // co-owned across the suspension (never a raw handle)
  uint64_t executor_;
  aorta_result_t delivery_status_ = AORTA_OK;  // delivery, trampoline-owned
  uint8_t priority_ = 255;                     // stamped at await_suspend
  bool ambient_ = false;        // scope/exec resolve from the promise
  bool context_bound_ = false;  // set when a flow frame supplied the context
  friend class nothrow_awaiter;
  const uint8_t* payload_;
  size_t len_;
  aorta_bytes_slice_t slice_{};
  std::coroutine_handle<> continuation_ = nullptr;
  /// The bound frame's promise context (null = unbound);
  /// stamped at await_suspend, read by on_done's router call.
  const detail::promise_flow_context* fctx_ = nullptr;
  // Emplaced exactly once (refusal inline, or on_done). optional because
  // the Outcome has no default construction (by design).
  std::optional<Outcome<std::vector<uint8_t>>> result_{};
};

// The .nothrow() awaiter: owns the call, returns BOTH status layers from
// await_resume directly — nothing thrown (the earlier
// take_result() shape read the value slot after it was moved).
class call::nothrow_awaiter {
 public:
  explicit nothrow_awaiter(call&& inner) noexcept : inner_(std::move(inner)) {}

  bool await_ready() const noexcept { return inner_.await_ready(); }
  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    // Registration happens HERE, against the awaiter-owned call's final
    // address — safe: the move into this awaiter completed at construction.
    // Templated: type-erasing the handle would drop the promise
    // priority the inner call reads.
    return inner_.await_suspend(h);
  }
  template <typename Unused = void>
  [[nodiscard]] call_result await_resume() noexcept {
#if !defined(__cpp_exceptions)
    // BELT gate: a foreign -fno-exceptions frame
    // never instantiates the flow promise choke gate — reject here.
    static_assert(detail::dependent_false<Unused>::value,
                  "C++ Flow requires exception support: "
                  "build this translation unit with exceptions "
                  "enabled, or use the C++17 baseline aorta SDK");
#endif
    if (!inner_.result_.has_value()) {
      // Teardown delivered before any RPC resolution: both layers say so
      // (the old aggregate silently read OK+empty here — the Outcome shape
      // makes the stop explicit).
      return call_result{
          inner_.delivery_status_,
          Outcome<std::vector<uint8_t>>::Stop(detail::stopped_from_completion(
              nullptr, inner_.delivery_status_ != AORTA_OK
                           ? inner_.delivery_status_
                           : AORTA_ERR_INTERNAL))};
    }
    return call_result{inner_.delivery_status_, std::move(*inner_.result_)};
  }

 private:
  call inner_;
};

inline call::nothrow_awaiter call::nothrow() && noexcept {
  return nothrow_awaiter(std::move(*this));
}

}  // namespace aorta::flow
