// Flow readiness for lane frames: `co_await aligned(window)`
// (approximate-time multi-sensor fusion) and `co_await wait_wave(set)`
// (AND-join over conditions), plus the subscriber→readiness bind
// helpers. Explicit forms take a MANDATORY scope (`aligned(window, rt,
// scope)`); the cancellation-blind scope-less overloads were removed.
//
// The awaiters follow flow_subscriber.hpp's proven protocol verbatim:
// a 1-branch arbiter group bridged by the surface's notify hook
// (trampoline-class, fires on the FEEDING thread), the stop-arm gate +
// close recheck, and the frame-safety rule — after the hook is
// published everything runs on LOCALS ONLY; the recheck uses the
// NON-POPPING pending/waves probes and the only pop happens in
// `await_resume`. Rounds return BY VALUE (GCC 11: lvalue awaitables with
// post-await member reads miscompile in non-template coroutines).
//
// SINGLE-AWAITER CONTRACT: one concurrent aligned()/wave() per
// window/set — the core refuses a second hook and the refused awaiter
// resolves Stopped. Destroy-wake: freeing the window/set fires a parked
// hook once; the awaiter observes the stale handle and unwinds.
#pragma once

#include <cstdint>
#include <optional>
#include <utility>

#include "aorta/flow_combinators.hpp"

namespace aorta::flow {

/// Timestamp source for a subscriber→window bind: the transport-
/// level choice only — sensor-capture stamps live in the payload and are
/// fed by the application (`WindowSync::feed`).
enum class WindowStampSource : uint8_t {
  /// Local receive time (node clock, unix ns) — always mutually
  /// comparable across this process's inputs. The default.
  kReceive = 0,
  /// Transport publisher timestamp (Zenoh HLC → unix ns); falls back to
  /// receive time when the sample carries none.
  kPublisher = 1,
};

/// One popped aligned set (RAII): per-input `(ts_ns, payload)` views stay
/// valid until this object is destroyed. Move-only.
class AlignedSet {
 public:
  struct Sample {
    uint64_t ts_ns = 0;
    const uint8_t* data = nullptr;
    size_t len = 0;
  };

  AlignedSet() = default;
  explicit AlignedSet(aorta_aligned_set_t handle) : handle_(handle) {}
  AlignedSet(AlignedSet&& other) noexcept
      : handle_(std::exchange(other.handle_, 0)) {}
  AlignedSet& operator=(AlignedSet&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, 0);
    }
    return *this;
  }
  AlignedSet(const AlignedSet&) = delete;
  AlignedSet& operator=(const AlignedSet&) = delete;
  ~AlignedSet() { reset(); }

  [[nodiscard]] bool valid() const { return handle_ != 0; }

  /// Input count (== the window's input count).
  [[nodiscard]] uint32_t size() const {
    uint32_t n = 0;
    if (handle_ != 0) {
      (void)aorta_aligned_set_len(handle_, &n);
    }
    return n;
  }

  /// Input `index`'s stamped payload view (empty Sample when out of
  /// range or invalid — check `data != nullptr`).
  [[nodiscard]] Sample at(uint32_t index) const {
    Sample s{};
    if (handle_ != 0) {
      (void)aorta_aligned_set_get(handle_, index, &s.ts_ns, &s.data, &s.len);
    }
    return s;
  }

  void reset() {
    if (handle_ != 0) {
      (void)aorta_aligned_set_free(handle_);
      handle_ = 0;
    }
  }

 private:
  aorta_aligned_set_t handle_{0};
};

/// Approximate-time alignment over K inputs within `slop` — the
/// camera/lidar/imu fusion mechanism. Feed manually (sensor-capture
/// stamps) or bind subscribers (`bind_window`); consume via
/// `try_aligned()` (poll) or `co_await aligned(window, rt, scope)` (park).
/// Move-only RAII; destroying the window destroy-wakes a parked awaiter.
class WindowSync {
 public:
  /// `depth` bounds the emitted-set queue (drop-oldest); 0 = default (8).
  // Reasoned factory: the failure code survives — `.ok()` /
  // `.status` instead of a reasonless nullopt.
  static Outcome<WindowSync> create(uint32_t inputs,
                                    std::chrono::nanoseconds slop,
                                    uint32_t depth = 0) {
    aorta_window_sync_t handle = 0;
    const aorta_result_t rc = aorta_window_sync_create(
        inputs, static_cast<uint64_t>(slop.count()), depth, &handle);
    if (rc != AORTA_OK) {
      return Outcome<WindowSync>::Stop(
          Stopped::Fault(aorta::Status::FromResult(rc)));
    }
    return Outcome<WindowSync>::Value(WindowSync(handle));
  }

  WindowSync() = default;
  WindowSync(WindowSync&& other) noexcept
      : handle_(std::exchange(other.handle_, 0)) {}
  WindowSync& operator=(WindowSync&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, 0);
    }
    return *this;
  }
  WindowSync(const WindowSync&) = delete;
  WindowSync& operator=(const WindowSync&) = delete;
  ~WindowSync() { reset(); }

  [[nodiscard]] bool valid() const { return handle_ != 0; }
  [[nodiscard]] aorta_window_sync_t native_handle() const { return handle_; }

  /// Feed input `input` with a sample stamped `ts_ns` (any thread;
  /// copies). Out-of-range `input` returns `AORTA_ERR_INVALID_ARGUMENT`.
  aorta_result_t feed(uint32_t input, uint64_t ts_ns, const uint8_t* data,
                      size_t len) {
    return aorta_window_sync_feed(handle_, input, ts_ns, data, len);
  }

  /// Pop the next aligned set if one is queued (non-blocking).
  std::optional<AlignedSet> try_aligned() {
    aorta_aligned_set_t set = 0;
    uint8_t has = 0;
    if (aorta_window_sync_try_aligned(handle_, &set, &has) != AORTA_OK ||
        has == 0) {
      return std::nullopt;
    }
    return AlignedSet(set);
  }

  struct Stats {
    uint64_t emitted = 0;
    uint64_t dropped_sets = 0;
  };
  [[nodiscard]] Stats stats() const {
    Stats s{};
    (void)aorta_window_sync_stats(handle_, &s.emitted, &s.dropped_sets);
    return s;
  }

  void reset() {
    if (handle_ != 0) {
      (void)aorta_window_sync_free(handle_);
      handle_ = 0;
    }
  }

 private:
  explicit WindowSync(aorta_window_sync_t handle) : handle_(handle) {}
  aorta_window_sync_t handle_{0};
};

/// AND-join over up to 64 conditions: sources `satisfy(i)`;
/// consumers `co_await wave(set, rt)`. Move-only RAII; destroying the
/// set destroy-wakes a parked awaiter.
class ReadinessSet {
 public:
  // Reasoned factory: failure keeps its code.
  static Outcome<ReadinessSet> create(uint32_t count) {
    aorta_readiness_set_t handle = 0;
    const aorta_result_t rc = aorta_readiness_set_create(count, &handle);
    if (rc != AORTA_OK) {
      return Outcome<ReadinessSet>::Stop(
          Stopped::Fault(aorta::Status::FromResult(rc)));
    }
    return Outcome<ReadinessSet>::Value(ReadinessSet(handle));
  }

  ReadinessSet() = default;
  ReadinessSet(ReadinessSet&& other) noexcept
      : handle_(std::exchange(other.handle_, 0)) {}
  ReadinessSet& operator=(ReadinessSet&& other) noexcept {
    if (this != &other) {
      reset();
      handle_ = std::exchange(other.handle_, 0);
    }
    return *this;
  }
  ReadinessSet(const ReadinessSet&) = delete;
  ReadinessSet& operator=(const ReadinessSet&) = delete;
  ~ReadinessSet() { reset(); }

  [[nodiscard]] bool valid() const { return handle_ != 0; }
  [[nodiscard]] aorta_readiness_set_t native_handle() const { return handle_; }

  /// Mark condition `index` satisfied (any thread; one RMW). Out-of-range
  /// `index` returns `AORTA_ERR_INVALID_ARGUMENT`.
  aorta_result_t satisfy(uint32_t index) {
    return aorta_readiness_set_satisfy(handle_, index);
  }

  /// Completed-wave count.
  [[nodiscard]] uint64_t waves() const {
    uint64_t w = 0;
    (void)aorta_readiness_set_waves(handle_, &w);
    return w;
  }

  /// Observability: the bitmask of conditions still missing.
  [[nodiscard]] uint64_t pending_mask() const {
    uint64_t m = 0;
    (void)aorta_readiness_set_pending_mask(handle_, &m);
    return m;
  }

  void reset() {
    if (handle_ != 0) {
      (void)aorta_readiness_set_free(handle_);
      handle_ = 0;
    }
  }

 private:
  explicit ReadinessSet(aorta_readiness_set_t handle) : handle_(handle) {}
  aorta_readiness_set_t handle_{0};
};

namespace detail {

#ifdef AORTA_LANES_TEST_SEAMS
/// Pre-armed-wave seam (seam test targets only): fires between
/// wave_loop's baseline snapshot and the first probe. Atomic — see
/// flow.hpp route_pin_seam.
inline std::atomic<void (*)(void*)> wave_baseline_seam{nullptr};
inline std::atomic<void*> wave_baseline_seam_ctx{nullptr};
#endif

/// One park on the window's emission hook (next_once protocol; see the
/// header comment). Round by value (GCC 11).
// Winner protocol shared by the readiness awaiters — next_once's Duel,
// verbatim: first resolver claims via CAS, then revokes every
// armed sibling so the group resolves exactly once. Branch cbs run
// strictly before the group's resume posts, so in-frame stores are safe.
struct ReadinessDuel {
  std::atomic<int> winner{-1};
  uint64_t handles[3] = {0, 0, 0};
  BranchSlot slots[3] = {};

  static void complete(ReadinessDuel* d, int idx, aorta_result_t st) {
    int expected = -1;
    if (d->winner.compare_exchange_strong(expected, idx,
                                          std::memory_order_acq_rel)) {
      for (int i = 0; i < 3; i++) {
        if (i != idx && d->handles[i] != 0) {
          (void)aorta_arbiter_revoke(d->handles[i]);
        }
      }
    }
    d->slots[idx].status.store(st, std::memory_order_release);
  }
  static void complete_0(void* ctx, aorta_result_t st,
                         const aorta_bytes_view_t* payload,
                         const aorta_flow_completion_t* /*completion*/) {
    (void)payload;
    complete(static_cast<ReadinessDuel*>(ctx), 0, st);
  }
  static void complete_1(void* ctx, aorta_result_t st,
                         const aorta_bytes_view_t* payload,
                         const aorta_flow_completion_t* /*completion*/) {
    (void)payload;
    complete(static_cast<ReadinessDuel*>(ctx), 1, st);
  }
  static void complete_2(void* ctx, aorta_result_t st,
                         const aorta_bytes_view_t* payload,
                         const aorta_flow_completion_t* /*completion*/) {
    (void)payload;
    complete(static_cast<ReadinessDuel*>(ctx), 2, st);
  }
};

struct aligned_once {
  aorta_window_sync_t ws_handle;
  LaneRuntime* runtime;
  /// 0 = no deadline; non-zero = aligned_for()'s window (a SECOND
  /// arbiter branch — never compose raw timeout() around aligned()).
  uint64_t deadline_window_ns = 0;
  /// 0 = no scope race; non-zero = a Scope handle whose cancel token
  /// resolves the park (the recv scope-parity).
  uint64_t scope_handle = 0;
  std::optional<AlignedSet> got;
  aorta_result_t status = AORTA_OK;
  bool deadline_won = false;
  uint64_t group_ = 0;
  uint64_t handle_ = 0;
  bool hook_owned_ = true;
  ArbiterFrame frame_{};
  ReadinessDuel duel_{};

  void try_pop() {
    aorta_aligned_set_t set = 0;
    uint8_t has = 0;
    if (aorta_window_sync_try_aligned(ws_handle, &set, &has) == AORTA_OK &&
        has != 0) {
      got.emplace(AlignedSet(set));
    }
  }

  bool await_ready() {
    try_pop();
    return got.has_value();
  }

  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    frame_.rt = runtime;
    frame_.fallback_executor = runtime->fallback_executor();
    frame_.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      frame_.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // The ready fast path (await_ready) never arms and
    // stays legal; a bound frame ARMING foreign-plane readiness
    // machinery refuses before any arm exists.
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, runtime)) {
        detail::log_refused_await(
            "flow::readiness [serial: foreign runtime arm]",
            AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      frame_.fctx = &fctx;
    }
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
    if (aorta_arbiter_arm(group_, 0, &ReadinessDuel::complete_0, &duel_,
                          &handle_) != AORTA_OK) {
      aorta_arbiter_group_release(group_);
      group_ = 0;
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    duel_.handles[0] = handle_;
    // Aux-branch arm failure resolves EVERY armed branch CANCELLED and
    // lets the normal resume path deliver (frame-safe) — next_once's
    // fail_armed ladder verbatim. The window's notify hook was NEVER
    // installed on this path (set_notify runs later), so mark it
    // not-owned: await_resume must not clear a hook belonging to a
    // different, legitimately-parked awaiter.
    const auto fail_armed = [&] {
      hook_owned_ = false;
      for (uint64_t bh : duel_.handles) {
        if (bh != 0) {
          (void)aorta_arbiter_complete(bh, AORTA_ERR_CANCELLED, nullptr);
        }
      }
    };
    if (has_deadline) {
      if (aorta_arbiter_arm(group_, 1, &ReadinessDuel::complete_1, &duel_,
                            &duel_.handles[1]) != AORTA_OK ||
          aorta_timer_arm_arbiter(duel_.handles[1], deadline_window_ns) !=
              AORTA_OK) {
        fail_armed();
        return true;
      }
      runtime->register_arm(duel_.handles[1]);
    }
    if (has_scope) {
      const uint32_t si = has_deadline ? 2 : 1;
      aorta_result_t scope_rc =
          aorta_arbiter_arm(group_, si,
                            si == 2 ? &ReadinessDuel::complete_2
                                    : &ReadinessDuel::complete_1,
                            &duel_, &duel_.handles[si]);
      if (scope_rc == AORTA_OK) {
        scope_rc = aorta_scope_arm_arbiter(duel_.handles[si], scope_handle);
      }
      if (scope_rc != AORTA_OK) {
        // A stale Scope handle refuses NOT_FOUND — a programming
        // error the CANCELLED flatten below would otherwise hide.
        detail::log_refused_await("flow::aligned", scope_rc);
        fail_armed();
        return true;
      }
      runtime->register_arm(duel_.handles[si]);
    }
    runtime->register_arm(handle_);
    // Frame-safety: locals only after the hook publish.
    const uint64_t local_handle = handle_;
    const aorta_window_sync_t ws_local = ws_handle;
    LaneRuntime* rt_local = runtime;
    const aorta_result_t notify_rc = aorta_window_sync_set_notify(
        ws_local, &notify_thunk, reinterpret_cast<void*>(local_handle));
    if (notify_rc != AORTA_OK) {
      // No wake source (stale window or a concurrent awaiter) —
      // self-resolve Stopped via the arbiter (frame-safe resume path).
      // The raw refusal code (second concurrent aligned() =
      // INVALID_ARGUMENT, freed window = NOT_FOUND) is logged first.
      detail::log_refused_await("flow::aligned", notify_rc);
      (void)aorta_arbiter_complete(local_handle, AORTA_ERR_CANCELLED,
                                   nullptr);
      hook_owned_ = false;
      return true;
    }
    // FRAME PUBLISHED — locals only.
    uint8_t pending = 0;
    if (aorta_window_sync_pending(ws_local, &pending) == AORTA_OK &&
        pending != 0) {
      (void)aorta_arbiter_complete(local_handle, AORTA_OK, nullptr);
    } else if (rt_local->enabled() && rt_local->pool().closed()) {
      (void)aorta_arbiter_revoke(local_handle);
    }
    return true;
  }

  struct Round {
    std::optional<AlignedSet> set;
    aorta_result_t status = AORTA_OK;
    bool deadline_won = false;
  };

  Round await_resume() {
    if (group_ != 0) {
      if (hook_owned_) {
        (void)aorta_window_sync_set_notify(ws_handle, nullptr, nullptr);
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
        // Torn delivery carrying an aligned-ready OK: report Stopped
        // and leave the set unpopped — aligned_loop unwinds on the
        // status (next_once parity).
        status = AORTA_ERR_CANCELLED;
      }
    }
    if (!got.has_value() && status == AORTA_OK && !deadline_won) {
      // The only pop on the parked path. A freed window reports
      // NOT_FOUND here (wave() parity): without the probe
      // a destroy-wake read as OK-empty and cost one spurious re-park.
      aorta_aligned_set_t set = 0;
      uint8_t has = 0;
      const aorta_result_t rc =
          aorta_window_sync_try_aligned(ws_handle, &set, &has);
      if (rc == AORTA_OK) {
        if (has != 0) {
          got.emplace(AlignedSet(set));
        }
      } else {
        status = AORTA_ERR_NOT_FOUND;
      }
    }
    return Round{std::move(got), status, deadline_won};
  }

  static void notify_thunk(void* ctx) {
    (void)aorta_arbiter_complete(reinterpret_cast<uint64_t>(ctx), AORTA_OK,
                                 nullptr);
  }
};

/// One park on the set's wave hook. `baseline` is the caller's snapshot:
/// resolves once `waves() > baseline` (trigger-then-wait race-free).
struct wave_once {
  aorta_readiness_set_t set_handle;
  LaneRuntime* runtime;
  uint64_t baseline;
  /// 0 = no deadline; non-zero = wait_wave_for()'s window (second
  /// arbiter branch — never compose raw timeout() around wait_wave()).
  uint64_t deadline_window_ns = 0;
  /// 0 = no scope race; non-zero = a Scope whose cancel resolves the park.
  uint64_t scope_handle = 0;
  aorta_result_t status = AORTA_OK;
  bool deadline_won = false;
  uint64_t group_ = 0;
  uint64_t handle_ = 0;
  bool hook_owned_ = true;
  ArbiterFrame frame_{};
  ReadinessDuel duel_{};

  [[nodiscard]] bool wave_passed() const {
    uint64_t w = 0;
    return aorta_readiness_set_waves(set_handle, &w) == AORTA_OK &&
           w > baseline;
  }

  bool await_ready() { return wave_passed(); }

  template <typename P>
  bool await_suspend(std::coroutine_handle<P> h) {
    frame_.rt = runtime;
    frame_.fallback_executor = runtime->fallback_executor();
    frame_.frame = h;
    if constexpr (std::is_base_of_v<detail::promise_priority, P>) {
      frame_.priority = h.promise().priority <= 3 ? h.promise().priority : 3;
    }
    // The ready fast path (await_ready) never arms and
    // stays legal; a bound frame ARMING foreign-plane readiness
    // machinery refuses before any arm exists.
    if constexpr (std::is_base_of_v<detail::promise_flow_context, P>) {
      const auto& fctx =
          static_cast<const detail::promise_flow_context&>(h.promise());
      if (detail::serial_foreign_arm(fctx, runtime)) {
        detail::log_refused_await(
            "flow::readiness [serial: foreign runtime arm]",
            AORTA_ERR_INVALID_ARGUMENT);
        status = AORTA_ERR_INVALID_ARGUMENT;
        return false;
      }
      frame_.fctx = &fctx;
    }
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
    if (aorta_arbiter_arm(group_, 0, &ReadinessDuel::complete_0, &duel_,
                          &handle_) != AORTA_OK) {
      aorta_arbiter_group_release(group_);
      group_ = 0;
      status = AORTA_ERR_CANCELLED;
      return false;
    }
    duel_.handles[0] = handle_;
    // Failed-aux early return never owned the set's notify hook — mark it
    // not-owned so await_resume leaves a parked sibling's hook alone
    // (aligned_once parity).
    const auto fail_armed = [&] {
      hook_owned_ = false;
      for (uint64_t bh : duel_.handles) {
        if (bh != 0) {
          (void)aorta_arbiter_complete(bh, AORTA_ERR_CANCELLED, nullptr);
        }
      }
    };
    if (has_deadline) {
      if (aorta_arbiter_arm(group_, 1, &ReadinessDuel::complete_1, &duel_,
                            &duel_.handles[1]) != AORTA_OK ||
          aorta_timer_arm_arbiter(duel_.handles[1], deadline_window_ns) !=
              AORTA_OK) {
        fail_armed();
        return true;
      }
      runtime->register_arm(duel_.handles[1]);
    }
    if (has_scope) {
      const uint32_t si = has_deadline ? 2 : 1;
      aorta_result_t scope_rc =
          aorta_arbiter_arm(group_, si,
                            si == 2 ? &ReadinessDuel::complete_2
                                    : &ReadinessDuel::complete_1,
                            &duel_, &duel_.handles[si]);
      if (scope_rc == AORTA_OK) {
        scope_rc = aorta_scope_arm_arbiter(duel_.handles[si], scope_handle);
      }
      if (scope_rc != AORTA_OK) {
        // Stale Scope handle — see aligned_once.
        detail::log_refused_await("flow::wait_wave", scope_rc);
        fail_armed();
        return true;
      }
      runtime->register_arm(duel_.handles[si]);
    }
    runtime->register_arm(handle_);
    const uint64_t local_handle = handle_;
    const aorta_readiness_set_t set_local = set_handle;
    const uint64_t baseline_local = baseline;
    LaneRuntime* rt_local = runtime;
    const aorta_result_t notify_rc = aorta_readiness_set_set_notify(
        set_local, &notify_thunk, reinterpret_cast<void*>(local_handle));
    if (notify_rc != AORTA_OK) {
      // Second concurrent wait_wave() = INVALID_ARGUMENT, freed set
      // = NOT_FOUND — logged before the CANCELLED flatten.
      detail::log_refused_await("flow::wait_wave", notify_rc);
      (void)aorta_arbiter_complete(local_handle, AORTA_ERR_CANCELLED,
                                   nullptr);
      hook_owned_ = false;
      return true;
    }
    // FRAME PUBLISHED — locals only.
    uint64_t w = 0;
    if (aorta_readiness_set_waves(set_local, &w) == AORTA_OK &&
        w > baseline_local) {
      (void)aorta_arbiter_complete(local_handle, AORTA_OK, nullptr);
    } else if (rt_local->enabled() && rt_local->pool().closed()) {
      (void)aorta_arbiter_revoke(local_handle);
    }
    return true;
  }

  struct Round {
    uint64_t waves = 0;
    aorta_result_t status = AORTA_OK;
    bool passed = false;
    bool deadline_won = false;
  };

  Round await_resume() {
    if (group_ != 0) {
      if (hook_owned_) {
        (void)aorta_readiness_set_set_notify(set_handle, nullptr, nullptr);
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
        // Torn delivery carrying a wave OK: report
        // Stopped — wave_loop unwinds on the status.
        status = AORTA_ERR_CANCELLED;
      }
    }
    Round r{};
    r.status = status;
    r.deadline_won = deadline_won;
    if (status == AORTA_OK && !deadline_won) {
      uint64_t w = 0;
      if (aorta_readiness_set_waves(set_handle, &w) == AORTA_OK) {
        r.waves = w;
        r.passed = w > baseline;
      } else {
        // Destroy-wake landed between resume and this read.
        r.status = AORTA_ERR_NOT_FOUND;
      }
    }
    return r;
  }

  static void notify_thunk(void* ctx) {
    (void)aorta_arbiter_complete(reinterpret_cast<uint64_t>(ctx), AORTA_OK,
                                 nullptr);
  }
};

}  // namespace detail

namespace detail {
// The one park-pop-or-unwind loop behind every aligned()/aligned_for()
// shape (deadline and scope ride the same arbiter group as extra
// branches — recv's Duel protocol). Deadline win, stop, scope
// cancel, and window destruction all unwind `cancelled_unwind`.
// Endpoint-tagged: timeout()/race() reject it at compile time.
//
// The return type is now a (defaulted) template parameter `R` — an
// ordinary (non-template) coroutine function's promise_type is resolved
// eagerly at definition-parse time, and a function TEMPLATE whose
// signature doesn't actually mention its template parameter is no
// better (two-phase lookup only defers DEPENDENT constructs); a concrete
// `EndpointTask<AlignedSet>` return type would instantiate
// `Task<AlignedSet, task_kind_endpoint>::promise_type` (tripping the
// exceptions choke gate) merely from including this header under
// -fno-exceptions. Making the return type itself the template parameter
// defers instantiation to the (exceptions-enabled-only) call site — a
// normal call with no explicit template argument still gets the
// original `EndpointTask<AlignedSet>` via the default.
// See flow_subscriber.hpp's next_loop for the twin rationale.
template <typename R = EndpointTask<AlignedSet>>
inline R aligned_loop(aorta_window_sync_t handle, LaneRuntime* rt,
                      uint64_t window_ns,
                                             uint64_t scope_handle) {
  for (;;) {
    auto r = co_await detail::aligned_once{handle, rt, window_ns,
                                           scope_handle};
    if (r.set.has_value()) {
      co_return std::move(*r.set);
    }
    if (r.status != AORTA_OK || r.deadline_won) {
#if defined(__cpp_exceptions)
      detail::arm_unwind();
      throw cancelled_unwind{r.deadline_won ? static_cast<uint8_t>(CancelReason::kDeadlineExceeded) : uint8_t{0}};
#else
      flow_requires_exceptions_unreachable();  // see flow.hpp
#endif
    }
    // Empty + OK: a concurrent consumer popped the set — re-park (with
    // the SAME window when one is set: a stolen set restarts the
    // deadline; exact-window callers track elapsed themselves).
  }
}
}  // namespace detail

/// `co_await aligned(window, rt, scope)` — park until the next aligned
/// set, racing the Scope's cancel token (recv parity):
/// `scope.cancel()` resolves the park promptly, and executor stop or
/// window destruction likewise unwind `cancelled_unwind`. ONE concurrent
/// aligned() per window. Composing the raw `timeout()` combinator
/// around aligned() is a COMPILE error: use `aligned_for` (the
/// deadline is a branch of the SAME arbiter group). The explicit tier is
/// scope-full ONLY — the cancellation-blind scope-less overload (which
/// parked until stop-sweep/destroy) was removed.
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<AlignedSet>>
inline R aligned(WindowSync& window, LaneRuntime& rt,
                                        Scope scope) {
  // The coroutine frame co-owns `scope` for the whole wait.
  co_return co_await detail::aligned_loop(window.native_handle(), &rt, 0,
                                          scope.handle());
}

/// `co_await aligned_for(window, rt, window_d, scope)` — aligned() with
/// a deadline as a SECOND BRANCH of the same arbiter group (first
/// resolver wins, siblings revoked) plus the per-scope cancel branch.
/// Deadline (and stop) unwind `cancelled_unwind`.
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<AlignedSet>>
inline R aligned_for(WindowSync& window,
                                            LaneRuntime& rt,
                                            std::chrono::nanoseconds window_d,
                                            Scope scope) {
  co_return co_await detail::aligned_loop(
      window.native_handle(), &rt, static_cast<uint64_t>(window_d.count()),
      scope.handle());
}

/// `co_await aligned(window)` — the AMBIENT form: {rt, scope} resolve
/// from the frame's promise context; the wrapper delegates to the
/// explicit scope-armed overload, so the
/// endpoint KIND is preserved and the wait is ALWAYS scope-armed — the
/// scope-less tier no longer exists anywhere.
/// Executor-only frames refuse; non-flow coroutines fail to
/// compile (the Task await_suspend concept gate).
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<AlignedSet>>
inline R aligned(WindowSync& window) {
  auto ctx = co_await this_coro::context;
  if (ctx.rt == nullptr) {
    detail::refuse_null_rt_endpoint("flow::aligned");
  }
  if (ctx.scope.is_cancelled()) {
    detail::arm_unwind();  // Gate BEFORE the delegate — the
                           // explicit form's ready fast-path would consume
                           // data on a cancelled frame. Reason from scope.
    throw cancelled_unwind{ctx.scope.raw_cancel_reason()};
  }
  co_return co_await aligned(window, *ctx.rt, std::move(ctx.scope));
}

/// `co_await aligned_for(window, window_d)` — ambient aligned() with a
/// deadline (the window stays explicit; {rt, scope} from context).
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<AlignedSet>>
inline R aligned_for(
    WindowSync& window, std::chrono::nanoseconds window_d) {
  auto ctx = co_await this_coro::context;
  if (ctx.rt == nullptr) {
    detail::refuse_null_rt_endpoint("flow::aligned_for");
  }
  if (ctx.scope.is_cancelled()) {
    detail::arm_unwind();  // Gate BEFORE the delegate — the
                           // explicit form's ready fast-path would consume
                           // data on a cancelled frame. Reason from scope.
    throw cancelled_unwind{ctx.scope.raw_cancel_reason()};
  }
  co_return co_await aligned_for(window, *ctx.rt, window_d,
                                 std::move(ctx.scope));
}

/// `co_await wait_wave(set, rt, scope)` — park until a wave completes
/// after this call; returns the wave number (Rust parity:
/// `set.wait_wave()`). Races the Scope's cancel token (recv parity);
/// unwinds `cancelled_unwind` on scope cancel / stop / set
/// destruction. ONE concurrent wait_wave() per set. The explicit
/// tier is scope-full ONLY — the cancellation-blind scope-less overload
/// was removed.
namespace detail {
// wait_wave's park loop (deadline/scope = extra Duel branches).
// Endpoint-tagged: timeout()/race() reject it at compile time.
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<uint64_t>>
inline R wave_loop(aorta_readiness_set_t handle,
                                        LaneRuntime* rt, uint64_t window_ns,
                                        uint64_t scope_handle) {
  uint64_t baseline = 0;
  const aorta_result_t baseline_rc =
      aorta_readiness_set_waves(handle, &baseline);
#ifdef AORTA_LANES_TEST_SEAMS
  // The pre-armed-wave window — a wave landing between this
  // baseline snapshot and the probe below completes on the ready fast
  // path (never arms). The window is nanoseconds wide in production;
  // the seam makes the fast-path test row deterministic.
  if (auto* seam = wave_baseline_seam.load(std::memory_order_acquire)) {
    seam(wave_baseline_seam_ctx.load(std::memory_order_acquire));
  }
#endif
  if (baseline_rc != AORTA_OK) {
    // Entry-time misuse (an already-freed set refuses NOT_FOUND) —
    // distinct from the parked destroy-wake, which is a designed
    // lifecycle path and stays quiet.
    detail::log_refused_await("flow::wait_wave", baseline_rc);
#if defined(__cpp_exceptions)
    detail::arm_unwind();
    throw cancelled_unwind{};
#else
    flow_requires_exceptions_unreachable();  // see flow.hpp
#endif
  }
  for (;;) {
    auto r =
        co_await detail::wave_once{handle, rt, baseline, window_ns,
                                   scope_handle};
    if (r.passed) {
      co_return r.waves;
    }
    if (r.status != AORTA_OK || r.deadline_won) {
#if defined(__cpp_exceptions)
      detail::arm_unwind();
      throw cancelled_unwind{r.deadline_won ? static_cast<uint8_t>(CancelReason::kDeadlineExceeded) : uint8_t{0}};
#else
      flow_requires_exceptions_unreachable();  // see flow.hpp
#endif
    }
    // Spurious (hook fired but the recheck read a stale count): re-park.
  }
}
}  // namespace detail

// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<uint64_t>>
inline R wait_wave(ReadinessSet& set, LaneRuntime& rt,
                                        Scope scope) {
  co_return co_await detail::wave_loop(set.native_handle(), &rt, 0,
                                       scope.handle());
}

/// `co_await wait_wave_for(set, rt, window, scope)` — deadline as a
/// SECOND arbiter branch plus the per-scope cancel (a third branch of
/// the same group); composing raw `timeout()` around wait_wave() is a
/// COMPILE error.
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<uint64_t>>
inline R wait_wave_for(ReadinessSet& set, LaneRuntime& rt,
                                            std::chrono::nanoseconds window,
                                            Scope scope) {
  co_return co_await detail::wave_loop(set.native_handle(), &rt,
                                       static_cast<uint64_t>(window.count()),
                                       scope.handle());
}

/// `co_await wait_wave(set)` — the AMBIENT form: {rt, scope} from the
/// frame's promise context; ALWAYS scope-armed, endpoint kind
/// preserved. Executor-only frames refuse.
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<uint64_t>>
inline R wait_wave(ReadinessSet& set) {
  auto ctx = co_await this_coro::context;
  if (ctx.rt == nullptr) {
    detail::refuse_null_rt_endpoint("flow::wait_wave");
  }
  if (ctx.scope.is_cancelled()) {
    detail::arm_unwind();  // Gate BEFORE the delegate — the
                           // explicit form's ready fast-path would consume
                           // data on a cancelled frame. Reason from scope.
    throw cancelled_unwind{ctx.scope.raw_cancel_reason()};
  }
  co_return co_await wait_wave(set, *ctx.rt, std::move(ctx.scope));
}

/// `co_await wait_wave_for(set, window)` — ambient wait_wave() with a
/// deadline (the window stays explicit; {rt, scope} from context).
//
// Return-type-as-template-parameter — see aligned_loop above.
template <typename R = EndpointTask<uint64_t>>
inline R wait_wave_for(ReadinessSet& set,
                                            std::chrono::nanoseconds window) {
  auto ctx = co_await this_coro::context;
  if (ctx.rt == nullptr) {
    detail::refuse_null_rt_endpoint("flow::wait_wave_for");
  }
  if (ctx.scope.is_cancelled()) {
    detail::arm_unwind();  // Gate BEFORE the delegate — the
                           // explicit form's ready fast-path would consume
                           // data on a cancelled frame. Reason from scope.
    throw cancelled_unwind{ctx.scope.raw_cancel_reason()};
  }
  co_return co_await wait_wave_for(set, *ctx.rt, window,
                                   std::move(ctx.scope));
}

/// Wire a subscriber as window input `input`: every arriving
/// sample feeds the window on the RX path, stamped per `stamp`. The wire
/// observes the stream — delivery (callback/pull) is unaffected. Indices
/// validate here. Works for any subscriber exposing `native_handle()`.
template <typename S>
inline aorta_result_t bind_window(
    S& sub, WindowSync& window, uint32_t input,
    WindowStampSource stamp = WindowStampSource::kReceive) {
  return aorta_subscriber_bind_window(sub.native_handle(),
                                      window.native_handle(), input,
                                      static_cast<uint8_t>(stamp));
}

/// Wire a subscriber to satisfy `set` condition `condition` on every
/// arriving sample — the `MessageAvailable` carrier.
template <typename S>
inline aorta_result_t bind_readiness(S& sub, ReadinessSet& set,
                                     uint32_t condition) {
  return aorta_subscriber_bind_readiness(sub.native_handle(),
                                         set.native_handle(), condition);
}

/// Remove the wire(s) feeding `window` input `input` (bind's inverse;
/// without it the subscriber kept feeding the window
/// for its whole remaining lifetime). NOT_FOUND when nothing matched.
template <typename S>
inline aorta_result_t unbind_window(S& sub, WindowSync& window,
                                    uint32_t input) {
  return aorta_subscriber_unbind_window(sub.native_handle(),
                                        window.native_handle(), input);
}

/// Remove the wire(s) satisfying `set` condition `condition`.
template <typename S>
inline aorta_result_t unbind_readiness(S& sub, ReadinessSet& set,
                                       uint32_t condition) {
  return aorta_subscriber_unbind_readiness(sub.native_handle(),
                                           set.native_handle(), condition);
}

}  // namespace aorta::flow
