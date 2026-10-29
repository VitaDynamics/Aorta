// LaneRuntime — the C++ SDK's dispatch-pool facade for Flow.
//
// Owns ONE AortaLanes pool with the fleet-uniform 4-class vocabulary
// (all-SCHED_OTHER defaults) and the class mapping
// (Realtime→0, High→1, Normal→2, Low+Background→3). Single-threaded is
// the DEFAULT (fleet code is written against the single-threaded executor
// semantics); MT is the explicit opt-in. `disabled()` is the
// rollback flag: task-layer awaitables fall back to the queue path
// executor posting when no runtime is enabled — also selectable at
// process level via env AORTA_CPP_LANES=0.
//
// This layer also owns the live arbiter arm-handle registry: the stop
// bridge's revoke sweep walks it so no frame parks on
// a never-completing branch past an executor stop.
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "aorta/detail/reentrancy_check.h"
#include "aorta/lanes.hpp"
#include "aorta/logging.h"
#include "aorta/qos.h"
#include "aorta/status.h"

namespace aorta {
namespace flow {

namespace detail {

/// SerialDomain admission gate: ONE
/// atomic word — bit 63 = CLOSED, bits 0..62 = pin count — so the close
/// (`fetch_or`) and every pin (CAS that fails on CLOSED) are totally
/// ordered on the same variable. A frame-side router pins before ANY
/// runtime/pool touch; a pin refused (CLOSED) degrades to the stamped
/// inline-cancel.
struct AdmissionGate {
  static constexpr uint64_t kClosedBit = uint64_t{1} << 63;

  std::atomic<uint64_t> state{0};

  bool pin() {  // false = CLOSED, do not enter
    uint64_t s = state.load(std::memory_order_acquire);
    for (;;) {
      if (s & kClosedBit) {
        return false;
      }
      if (state.compare_exchange_weak(s, s + 1, std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
        return true;  // admitted; runtime use is now safe
      }
    }
  }
  void unpin() { state.fetch_sub(1, std::memory_order_release); }

  /// The fetch_or HALF only — instant and trampoline-legal; the winning
  /// on_stop's FIRST bridge side effect. New pins are impossible
  /// past the OR; the pin-count quiesce stays OUT of on_stop (the drain
  /// hook reports live pins as remaining work instead).
  void close() { state.fetch_or(kClosedBit, std::memory_order_acq_rel); }

  /// Teardown belt (`~Core`; idempotent — a second fetch_or is a no-op):
  /// close, then wait out the admitted pin windows. Deadlock-free: the
  /// caller holds none of the producer-side locks while spinning, and a
  /// pinned producer's post is bounded.
  void close_and_quiesce() {
    close();
#ifdef AORTA_LANES_TEST_SEAMS
    bool counted = false;
#endif
    while ((state.load(std::memory_order_acquire) & ~kClosedBit) != 0) {
#ifdef AORTA_LANES_TEST_SEAMS
      // Test seam: observable proof the QUIESCE loop itself is
      // waiting on a live pin — a degenerate close()-only implementation
      // never reaches here, and later barriers (the facade final drain's
      // pin report) cannot stand in for it.
      if (!counted) {
        counted = true;
        dbg_quiesce_waits.fetch_add(1, std::memory_order_seq_cst);
      }
#endif
      std::this_thread::yield();
    }
  }

#ifdef AORTA_LANES_TEST_SEAMS
  /// Test seam: how many close_and_quiesce calls OBSERVED a live pin and
  /// entered the wait loop (seam test targets only).
  static inline std::atomic<uint64_t> dbg_quiesce_waits{0};
#endif

  bool closed() const {
    return (state.load(std::memory_order_acquire) & kClosedBit) != 0;
  }
  uint64_t live_pins() const {
    return state.load(std::memory_order_acquire) & ~kClosedBit;
  }
};

}  // namespace detail

/// SerialDomain control block — shared between the Runtime (the
/// only creator of domains), every domain state (which co-owns it via
/// an aliasing shared_ptr to the gate), and the facade's stop bridge.
/// `control_m` serializes the control-plane ops (`create_serial` vs a
/// winning `detach`) off every hot path; `live_domains` counts domain
/// STATES still alive (maintained by the state deleter), the detach
/// refusal predicate's first half.
struct SerialControl {
  detail::AdmissionGate gate;
  std::atomic<uint64_t> live_domains{0};
  std::mutex control_m;
};

/// Map the SDK DISPATCH vocabulary (`EventPriority` — the wire
/// `Priority` is publish-side QoS and does not reach the pool) onto the 4
/// lane classes — the 5→4 fold (Realtime→0, High→1, Normal→2, Low→3,
/// Background→3).
///
/// Rust twin (emit/forward path): `lane_class_for_priority` in
/// core/src/event_queue.rs. The two implementations must agree forever;
/// the drift guards are `LaneRuntime.ClassMappingMatchesSpecPriorityTable` +
/// `.EventPriorityBytesMatchTheSpecTable` (this side, in
/// cpp/test/lane_runtime_test.cc — the latter pins the enum
/// discriminants to the C-ABI byte directly) and
/// `priority_index_and_lane_class_tables_pinned` (Rust side, in
/// core/test/event_queue_tests.rs).
inline size_t lane_class_for(EventPriority p) {
  switch (p) {
    case EventPriority::kRealtime:
      return 0;
    case EventPriority::kHigh:
      return 1;
    case EventPriority::kNormal:
      return 2;
    case EventPriority::kLow:
    case EventPriority::kBackground:
    default:
      return 3;
  }
}

/// Map the wire/ABI priority byte (core executor vocabulary: 255 =
/// inherit-resolved-before-post; 0..=3 class ids on the lane path).
/// Byte form of the same fold as `lane_class_for` above (index 4
/// = Background folds into class 3 with Low; the Rust twin clamps with
/// `min(3)` identically); pinned by
/// `LaneRuntime.ClassByteMappingMatchesSpecPriorityTable`.
inline size_t lane_class_for_u8(uint8_t priority) {
  // The flow.hpp promise resolves inherit before posting; by the time an
  // item reaches the pool the byte is a concrete class. Clamp defensively.
  return priority <= 3 ? priority : 3;
}

class LaneRuntime {
 public:
  /// Single-threaded (the fleet default): no pool workers —
  /// the executor facade's spin thread drains via the attach hooks.
  /// Honors the AORTA_CPP_LANES=0 process-level rollback (the env
  /// override lives in EVERY lane-enabling constructor, not
  /// just FromEnv(), or the "transparent rollback" guarantee only
  /// protected callers who happened to construct through FromEnv()).
  static std::unique_ptr<LaneRuntime> SingleThread() {
    if (rollback_via_env()) {
      return Disabled();
    }
    return std::unique_ptr<LaneRuntime>(new LaneRuntime(default_config(), false));
  }

  /// Multi-threaded (explicit performance opt-in): `workers_per_lane`
  /// pool threads per class lane (index = class id; missing = 0).
  /// AORTA_CPP_LANES=0 overrides to Disabled() — same rollback contract
  /// as SingleThread().
  static std::unique_ptr<LaneRuntime> MultiThread(
      std::vector<uint32_t> workers_per_lane) {
    if (rollback_via_env()) {
      return Disabled();
    }
    lanes::PoolConfig cfg = default_config();
    cfg.workers_per_lane = std::move(workers_per_lane);
    return std::unique_ptr<LaneRuntime>(new LaneRuntime(std::move(cfg), false));
  }

  /// Total pool workers (0 = ST) — the ST/MT attach pairing datum.
  uint32_t pool_workers() const { return pool_workers_; }

  /// The rollback mode: no pool; task-layer posts
  /// fall back to core executor posting; arbiter resumes post via
  /// aorta_executor_post_resume.
  static std::unique_ptr<LaneRuntime> Disabled() {
    return std::unique_ptr<LaneRuntime>(new LaneRuntime({}, true));
  }

  /// Honors AORTA_CPP_LANES=0 (process-level rollback), else ST default.
  /// (Every lane-enabling constructor honors the env var; this form
  /// just names the intent explicitly.)
  static std::unique_ptr<LaneRuntime> FromEnv() { return SingleThread(); }

  /// The AORTA_CPP_LANES=0 process-level rollback probe.
  static bool rollback_via_env() {
    const char* v = std::getenv("AORTA_CPP_LANES");
    return v != nullptr && v[0] == '0' && v[1] == '\0';
  }

  bool enabled() const { return !disabled_; }

  /// Rollback routing: the core executor that carries posts
  /// when the runtime is DISABLED. Set by FacadeAttachment.
  void set_fallback_executor(uint64_t exec) {
    fallback_executor_.store(exec, std::memory_order_release);
  }
  uint64_t fallback_executor() const {
    return fallback_executor_.load(std::memory_order_acquire);
  }

  /// The pool. Precondition: enabled().
  lanes::LanePool& pool() { return *pool_; }

  /// Post a task-layer RESUME item at an SDK priority (budget-exempt).
  /// Returns REFUSED_CLOSED after close; the caller owns the drop path.
  lanes::Post post_resume(EventPriority p, lanes::Work w) {
    return pool_->post_resume_item(lane_class_for(p), w);
  }

  lanes::Post post_resume_class(uint8_t priority_byte, lanes::Work w) {
    return pool_->post_resume_item(lane_class_for_u8(priority_byte), w);
  }

  /// Ordinary (budget-governed) work post, e.g. `on(lane::X, fn)`.
  lanes::Post post_work(EventPriority p, lanes::Work w) {
    return pool_->post(lane_class_for(p), w);
  }

  // ---- arbiter arm-handle registry ------------------------------
  void register_arm(uint64_t handle) {
    std::lock_guard<std::mutex> lk(arms_m_);
    // Amortized (power-of-two size) prune of resolved (zeroed) entries.
    const size_t len = arms_.size();
    if (len >= 8 && (len & (len - 1)) == 0) {
      arms_.erase(std::remove(arms_.begin(), arms_.end(), 0u), arms_.end());
    }
    arms_.push_back(handle);
    // The registration LEDGER — unlike arms_ (which a
    // sweep clears, at-most-once contract), a ledger entry lives until the
    // owning frame's own deregister_arm, so the destructor quiesce below
    // can wait out a resolution racing the sweep on another thread.
    arm_ledger_.insert(handle);
    arms_outstanding_.fetch_add(1, std::memory_order_acq_rel);
  }

  void deregister_arm(uint64_t handle) {
    if (handle == 0) {
      return;  // unarmed slot (fail-armed paths pass 0) — never registered
    }
    bool registered = false;
    {
      std::lock_guard<std::mutex> lk(arms_m_);
      for (auto& h : arms_) {
        if (h == handle) {
          h = 0;  // tombstone; pruned on the amortized schedule
          break;
        }
      }
      registered = arm_ledger_.erase(handle) > 0;
    }
    if (registered) {
      // The frame's LAST touch of this runtime object — an ATOMIC (not
      // the mutex), so ~LaneRuntime's quiesce can observe it and destroy
      // the members without racing a mutex-unlock tail. Handles the
      // awaiters deregister without having registered (armed-but-refused
      // aux branches) never reach this decrement (ledger miss).
      arms_outstanding_.fetch_sub(1, std::memory_order_release);
    }
  }

  /// Read-only observability probe: how many
  /// arbiter arms are currently registered. Non-consuming — the TCK's
  /// teardown row uses it as an ARM-COMPLETE rendezvous (a task that
  /// signalled entry may not have finished its await_suspend yet; the
  /// registered arm is the proof it parked). No semantic change: a
  /// mutex-guarded size read beside the existing registry.
  size_t live_arm_count() {
    std::lock_guard<std::mutex> lk(arms_m_);
    size_t n = 0;
    for (uint64_t h : arms_) {
      if (h != 0) n++;
    }
    return n;
  }

  /// Snapshot for the stop bridge's revoke sweep: the caller
  /// revokes each returned handle through the arbiter ABI.
  std::vector<uint64_t> drain_live_arms() {
    std::lock_guard<std::mutex> lk(arms_m_);
    std::vector<uint64_t> live;
    live.reserve(arms_.size());
    for (uint64_t h : arms_) {
      if (h != 0) live.push_back(h);
    }
    arms_.clear();
    return live;
  }

  // TEARDOWN PRECONDITION: the owning
  // executor must be STOPPED and its stop-drain completed BEFORE this
  // destructor runs. With a live fallback executor, a revoke's resume
  // post merely QUEUES (try_send + return) — the frame would resume
  // after this object is gone and call deregister_arm on freed memory.
  // After stop-drain, every revoke below takes the refused-post path and
  // resumes INLINE on this thread, synchronously, while the arm registry
  // is still alive. Destroying a LaneRuntime under a live executor is a
  // contract violation. (flow::Runtime's destructor encodes this
  // ordering structurally.)
  ~LaneRuntime() {
    // CLOSE the pool before the revoke sweep. On the
    // enabled facade-less path an OPEN pool lets each revoke's resume
    // POST into a lane nothing drains (the teardown precondition says no
    // workers/spin thread remain), so `arms_outstanding_` never reaches
    // zero and the quiesce below spins forever. Closed, every resume
    // lands in the residue this destructor delivers itself. Idempotent
    // when a facade stop (or a test harness) already closed it.
    if (pool_) {
      pool_->close();
    }
    // Revoke in-flight arbiter registrations BEFORE the pool
    // tears down — a core sleeper firing after this object is freed
    // would otherwise call back into a dangling runtime. The revoke
    // aborts sleepers; their branches resolve Stopped; resume posts
    // refuse on the closing pool and drain via the residue.
    // UNCONDITIONAL: Disabled runtimes register arms too
    // (arbiter verbs on the rollback leg), and no attach hook ever sweeps
    // them — the old `enabled()` gate left their frames parked past
    // destruction (dangling rt*).
    for (uint64_t handle : drain_live_arms()) {
      (void)aorta_arbiter_revoke(handle);
    }
    // Ledger quiesce (found by FacadeRollback.
    // DisabledRuntimeDestructorSweepRacesTimerFire): a timer/scope
    // completion that won its branch CAS just before the sweep above may
    // still be FINISHING the frame's inline resolution on the resolver
    // thread — and that frame's last touch of this object is
    // deregister_arm. Quiesce on the ledger before the members die.
    //
    // Bounded, in two parts: entries whose revoke reports NOT_FOUND have
    // NO live frame (a frame releases its group only AFTER deregistering,
    // so a missing group means the deregister already ran — or the handle
    // was synthetic/stale) and are reaped here; every remaining entry's
    // resolution is already executing (revoked INLINE above on this
    // thread, or in flight on its resolver thread — a µs-scale unwind,
    // never a timer wait).
    //
    // Residue delivery (found by
    // FlowHandle.PoolCloseResolvesAParkedHandleAwait):
    // on the ENABLED facade-less path the sweep's revokes do NOT resume
    // inline — resume_cb refuses on the closed pool and hands the frame
    // to the RESIDUE (user unwinds are banned from resolver
    // threads). With a facade, the spin thread's final stop-drain
    // delivered that residue long before this destructor; WITHOUT one,
    // this destructor thread is the "closer" and must deliver it
    // itself — otherwise the parked frame never runs deregister_arm and
    // this loop spins forever.
    uint64_t quiesce_spins = 0;
    while (arms_outstanding_.load(std::memory_order_acquire) != 0) {
      std::vector<uint64_t> leftover;
      {
        std::lock_guard<std::mutex> lk(arms_m_);
        leftover.assign(arm_ledger_.begin(), arm_ledger_.end());
      }
      for (uint64_t handle : leftover) {
        if (aorta_arbiter_revoke(handle) == AORTA_ERR_NOT_FOUND) {
          bool reaped = false;
          {
            std::lock_guard<std::mutex> lk(arms_m_);
            reaped = arm_ledger_.erase(handle) > 0;
          }
          if (reaped) {  // the erase guard keeps the pairing exact even
            arms_outstanding_.fetch_sub(1, std::memory_order_release);
          }  // if the owning frame's deregister raced this reap
        }
      }
      if (pool_) {
        (void)pool_->drain_residue_now();  // the closer (see above)
      }
      // The quiesce is expected to settle in microseconds — a
      // long spin means a resolver thread is stalled (or the teardown
      // precondition was violated). Say so ONCE instead of hanging mute.
      if (++quiesce_spins == 10000) {
        AORTA_LOG_SCOPE(WARN, "aorta.flow")
            << "~LaneRuntime arm quiesce still spinning after 10k rounds ("
            << arms_outstanding_.load(std::memory_order_acquire)
            << " arms outstanding) — a resolver thread may be stalled, or "
               "the executor was not stopped before destroying the runtime "
               "(stop-then-destroy precondition)";
      }
      std::this_thread::yield();
    }
    // Chained unwinds above may have parked NEW refused resumes in the
    // residue (an unwinding frame resolves its consumer's branch, whose
    // resume refuses on the closed pool). Deliver until dry — every
    // producer is quiesced by now, so one internal-loop pass settles it.
    if (pool_) {
      (void)pool_->drain_residue_now();
    }
  }  // then pool dtor = close() + join()

  LaneRuntime(const LaneRuntime&) = delete;
  LaneRuntime& operator=(const LaneRuntime&) = delete;

 private:
  LaneRuntime(lanes::PoolConfig cfg, bool disabled) : disabled_(disabled) {
    if (!disabled_) {
      for (uint32_t w : cfg.workers_per_lane) {
        pool_workers_ += w;
      }
      pool_ = std::make_unique<lanes::LanePool>(std::move(cfg));
    }
  }

  /// Defaults: 4 class lanes, all SCHED_OTHER, executor-parity
  /// budgets; ST WRR weights are [8,4,2,1].
  static uint8_t ctx_enter_thunk() { return aorta_callback_context_enter(); }
  static void ctx_exit_thunk(uint8_t prev) {
    (void)aorta_callback_context_exit(prev);
  }

  static lanes::PoolConfig default_config() {
    lanes::PoolConfig cfg;
    cfg.ctx_enter = &ctx_enter_thunk;  // worker callback-context marks
    cfg.ctx_exit = &ctx_exit_thunk;
    static const char* kNames[4] = {"realtime", "high", "normal", "low"};
    for (int i = 0; i < 4; i++) {
      lanes::LaneConfig lane;
      lane.name = kNames[i];
      cfg.lanes.push_back(lane);
    }
    return cfg;
  }

  bool disabled_ = false;
  uint32_t pool_workers_ = 0;
  std::atomic<uint64_t> fallback_executor_{0};
  // Declaration ORDER is load-bearing: the arm registry
  // (mutex + vector) must be declared BEFORE pool_ so reverse-order
  // destruction tears the POOL down first — a frame resumed-into-
  // cancellation during pool close/join can still call deregister_arm,
  // which must find a live mutex. (The old order destroyed arms_m_/arms_
  // first: locking a destroyed mutex is UB.)
  std::mutex arms_m_;
  std::vector<uint64_t> arms_;
  // The registration ledger + quiesce counter (see
  // register_arm/deregister_arm and the destructor). Same declaration-
  // order rule as arms_ above.
  std::unordered_set<uint64_t> arm_ledger_;
  std::atomic<uint64_t> arms_outstanding_{0};
  std::unique_ptr<lanes::LanePool> pool_;
};

}  // namespace flow
}  // namespace aorta

namespace aorta {
namespace flow {

/// Facade attachment: binds a
/// LaneRuntime to a core executor — the spin thread drains the pool each
/// iteration (combined ST wait) and every stop origin bridges through
/// the trampoline-class on_stop (pool close + arbiter revoke sweep +
/// wake; the DRAIN then runs on the spin thread's final stop-drain
/// pass). RAII: detaching quiesces in-flight hook invocations.
class FacadeAttachment {
 public:
  FacadeAttachment(LaneRuntime& rt, uint64_t executor)
      : rt_(&rt), executor_(executor) {
    aorta_lane_hooks_t hooks{};
    hooks.drain_fn = &drain_thunk;
    hooks.ctx = this;
    hooks.on_stop = &on_stop_thunk;
    hooks.forward_fn = &forward_thunk;
    hooks.pool_workers = rt.pool_workers();  // ST/MT pairing datum
    rt_->set_fallback_executor(executor);  // rollback routing
    attached_ = rt_->enabled() &&
                aorta_executor_attach_lane_pool(executor, &hooks) == AORTA_OK;
    if (attached_) {
      // Stop-bridge arming edge: resolve the executor's monotonic
      // stop-flag pointer ONCE — the drain hook polls it lock-free; a
      // per-call registry lookup is the shape the RT doctrine bans.
      if (aorta_executor_stop_flag_ptr(executor_, &stop_flag_) != AORTA_OK) {
        detach_unconditional();
        attach_refusal_ = "executor stop flag unresolvable";
        return;
      }
      // Lane posts wake the executor's combined wait.
      rt_->pool().set_wake_hook(&wake_thunk, this);
      // Attach handshake — DETACH AND FAIL: a stop that COMPLETED before
      // the hook was
      // installed never invoked on_stop (slot read None), so the bridge
      // would sit UNBRIDGED forever while the spin thread may already
      // have run its hookless final drain and exited. The monotonic
      // flag means this recheck cannot miss such a stop; a stop that
      // BEGINS after a false recheck finds the installed hook and
      // bridges normally. Invariant: no successful facade is ever
      // attached to a stopped executor.
      if (stop_flag_read()) {
        detach_unconditional();
        attach_refusal_ = "executor already stopped";
        return;
      }
    }
  }

  ~FacadeAttachment() {
    const aorta::Status st = detach();
    if (!st.ok()) {
      // A refused detach cannot stand in the destructor — dying with
      // hooks installed leaves the executor calling into freed memory.
      // The refusal names a contract violation (live domains at facade
      // death, same class as lane_runtime.h teardown-order breaches):
      // log it loudly, then force the quiesce barrier.
      AORTA_LOG_SCOPE(ERROR, "aorta.flow")
          << "~FacadeAttachment forced past a refused detach: "
          << st.message();
      detach_unconditional();
    }
  }

  FacadeAttachment(const FacadeAttachment&) = delete;
  FacadeAttachment& operator=(const FacadeAttachment&) = delete;

  bool attached() const { return attached_; }

  /// Why the constructor's attach was refused (nullptr = not refused) —
  /// the detach-and-fail handshake reason `Runtime::create` surfaces.
  const char* attach_refusal() const { return attach_refusal_; }

  /// SerialDomain wiring: the Runtime binds its control block
  /// right after construction, BEFORE any domain exists and before the
  /// spin thread starts (single-writer, then read-only).
  void bind_serial_control(std::shared_ptr<SerialControl> control) {
    serial_ = std::move(control);
  }

  /// Detach with the SerialDomain refusal contract: while live
  /// domains exist on an OPEN gate, detaching would strand their bound
  /// frames' routing — refuse with INVALID_ARGUMENT naming the count. A
  /// CLOSED gate means every domain is inert, so the `~Core` destructor
  /// detach (strictly after close_and_quiesce) always passes. The C-ABI
  /// `aorta_executor_detach_lane_pool` is UNCHANGED — a direct C-ABI
  /// detach under live SDK domains is a documented contract violation.
  [[nodiscard]] aorta::Status detach() {
    if (serial_ != nullptr) {
      std::lock_guard<std::mutex> lk(serial_->control_m);
      const uint64_t live =
          serial_->live_domains.load(std::memory_order_acquire);
      if (live > 0 && !serial_->gate.closed()) {
        return aorta::Status(AORTA_ERR_INVALID_ARGUMENT,
                             "refused: " + std::to_string(live) +
                                 " live serial domains");
      }
      detach_unconditional();
      return aorta::Status::Ok();
    }
    detach_unconditional();
    return aorta::Status::Ok();
  }

 private:
  void detach_unconditional() {
    if (attached_) {
      rt_->pool().set_wake_hook(nullptr, nullptr);
      AORTA_CHECK_NOT_REENTRANT("FacadeAttachment::detach");
      (void)aorta_executor_detach_lane_pool(executor_);  // quiesce barrier
      attached_ = false;
      // tear down the group serializers (their queued members
      // drain to drop via the SerialGroup destructor contract).
      std::lock_guard<std::mutex> lk(groups_m_);
      groups_.clear();
    }
  }

  bool stop_flag_read() const {
    return stop_flag_ != nullptr &&
           reinterpret_cast<const std::atomic<uint8_t>*>(stop_flag_)
                   ->load(std::memory_order_acquire) != 0;
  }

 private:
  void report_lane_telemetry() {
    // Telemetry v6: 5 rows × 4 class lanes, row-major
    // [depth, high_water, enqueued, shed, expired].
    uint64_t flat[20] = {0};
    for (size_t lane = 0; lane < 4; ++lane) {
      const auto st = rt_->pool().stats(lane);
      flat[0 * 4 + lane] = st.queued_now;
      flat[1 * 4 + lane] = st.high_water;
      flat[2 * 4 + lane] = st.enqueued;
      flat[3 * 4 + lane] = st.shed_full;
      flat[4 * 4 + lane] = st.dropped_expired;
    }
    (void)aorta_executor_update_lane_telemetry(executor_, flat);
  }

  static uint64_t drain_thunk(void* ctx, uint32_t max_items) {
    auto* self = static_cast<FacadeAttachment*>(ctx);
    // ENTRY CHECK, before ANY pool touch: while
    // a stop is published but the bridge has not completed, running the
    // drain body would take `w.run` off a not-yet-closed pool and
    // produce VALUES inside the stop window (close-before-drain
    // violation). Report 1 — no drain, no residue, no stats — so the
    // spin thread's final drain cannot zero-exit before the winning
    // on_stop release-stores BRIDGED (the completion latch, armed by the
    // monotonic stop flag itself).
    if (self->stop_flag_read() &&
        self->bridge_state_.load(std::memory_order_acquire) != kBridged) {
      return 1;
    }
    // opportunistic telemetry v6 report, throttled off the hot
    // path (every 64th drain).
    if ((self->drain_calls_.fetch_add(1, std::memory_order_relaxed) & 63) ==
        0) {
      self->report_lane_telemetry();
    }
    auto& pool = self->rt_->pool();
    uint32_t drained = 0;
    // Worker-owned lanes are the workers' to drain: the spin
    // thread draining them ran arbitrary-duration lane callbacks at
    // spin-thread scheduling — the exact isolation MT mode exists to
    // provide. Remaining-work still counts ALL
    // lanes below, so the stop-drain wait covers worker progress too.
    while (drained < max_items && pool.drain_one_unworked()) {
      drained++;
    }
    // The residue (refused user-code drops — e.g. arbiter resumes that
    // hit the closed pool) delivers HERE too: this is the draining
    // thread. Without this, stop-refused frame resumes sat undelivered
    // until pool teardown and scope.close() hung.
    pool.drain_residue_now();
    // Remaining-work report: lanes + residue + admitted pin windows
    // (a pinned producer is bounded, in-flight work the final
    // stop-drain must wait out — the drain loop holds none of the
    // producer-side locks, so no cycle exists). An estimate is fine —
    // the spin loop only needs zero/non-zero.
    uint64_t remaining = pool.residue_size();
    for (size_t i = 0; i < pool.lane_count(); i++) {
      remaining += pool.stats(i).queued_now;
    }
    if (self->serial_ != nullptr) {
      remaining += self->serial_->gate.live_pins();
    }
    return remaining;
  }

  static void on_stop_thunk(void* ctx) {
    // TRAMPOLINE-CLASS (no user code runs here): bar posts, sweep arbiter
    // registrations (aborts core sleepers; resolves branches Stopped —
    // their resume posts REFUSE on the closed pool and land in the
    // residue), and wake the spin thread for its final stop-drain pass.
    // NO user code runs here; the stop call returns immediately.
    //
    // Stop bridge — a single-owner idempotent state machine, not a
    // boolean (on_stop can run concurrently or
    // repeatedly — StopHandle::stop has no first-winner guard, stop is
    // repeatable, and free stops again). Every invocation CASes
    // UNBRIDGED → BRIDGING; ONLY the winner runs the bridge — the gate
    // close (fetch_or alone) as its FIRST bridge side effect, strictly
    // sequenced-before pool close / arm sweep / wake — then
    // release-stores BRIDGED. Losers return immediately (public stop is
    // signaling-only; completion-needing callers hold their own
    // barriers).
    auto* self = static_cast<FacadeAttachment*>(ctx);
    uint8_t expected = kUnbridged;
    if (!self->bridge_state_.compare_exchange_strong(
            expected, kBridging, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }
    if (self->serial_ != nullptr) {
      self->serial_->gate.close();  // first: every producer is now
                                    // pre-OR (counted) or post-OR
                                    // (stamped inline-cancel)
    }
#ifdef AORTA_LANES_TEST_SEAMS
    // Double-stop test row: exactly ONE winner runs the bridge (sweep).
    self->dbg_sweeps_.fetch_add(1, std::memory_order_seq_cst);
#endif
    self->rt_->pool().close();
    for (uint64_t handle : self->rt_->drain_live_arms()) {
      (void)aorta_arbiter_revoke(handle);
    }
    (void)aorta_executor_wake(self->executor_);
    self->bridge_state_.store(kBridged, std::memory_order_release);
  }

  /// the emit-side forward — an admitted C++ callback
  /// item posts to its class lane ON THE PRODUCING THREAD. Returns
  /// 1 accepted, 2 refused-full (caller keeps the item), 0
  /// refused-closed (caller falls back to the queue path).
  static uint8_t forward_thunk(void* ctx, void (*run)(void*),
                               void (*item_drop)(void*), void* item,
                               uint8_t klass, uint64_t group,
                               uint64_t deadline_rel_ns,
                               uint32_t cost_bytes) {
    auto* self = static_cast<FacadeAttachment*>(ctx);
    aorta::lanes::Work w;
    w.run = run;
    w.drop = item_drop;
    w.ctx = item;
    // The deadline crosses as RELATIVE remaining
    // ns (the two sides anchor different monotonic epochs) and is
    // re-anchored HERE, in the lane's own steady_clock domain.
    if (deadline_rel_ns != 0) {
      w.deadline_ns =
          static_cast<uint64_t>(std::chrono::duration_cast<
                                    std::chrono::nanoseconds>(
                                    std::chrono::steady_clock::now()
                                        .time_since_epoch())
                                    .count()) +
          deadline_rel_ns;
    }
    w.cost_bytes = cost_bytes;
    // group routing group_desc bits[0..2] = type
    // (0 default-ME, 1 explicit-ME, 2 reentrant), bits[2..10] = declared
    // class, bits[10..] = handle. ST mode (no pool workers): the single
    // drainer serializes — groups are a no-op (ST queue-path parity, zero cost).
    // the resume sentinel — budget-EXEMPT post (at most one outstanding
    // resume per frame, so the population is bounded by live frames).
    if (group == UINT64_MAX) {
      switch (self->rt_->pool().post_resume_item(
          aorta::flow::lane_class_for_u8(klass), w)) {
        case aorta::lanes::Post::OK:
          return 1;
        default:
          return 0;  // closed → the core falls back to the reserved lane
      }
    }
    const uint8_t gtype = static_cast<uint8_t>(group & 0x3);
    if (self->rt_->pool_workers() > 0 && gtype != 2) {
      const uint8_t declared = static_cast<uint8_t>((group >> 2) & 0xff);
      const uint64_t key = group >> 10;  // 0 = the default-ME group
      // Monotone WARN: a member above its group's class.
      if (aorta::flow::lane_class_for_u8(klass) <
              aorta::flow::lane_class_for_u8(declared) &&
          !self->monotone_warned_.exchange(true, std::memory_order_acq_rel)) {
        AORTA_LOG_SCOPE(WARN, "aorta.flow")
            << "callback-group member class above its group's declared "
               "class — it executes at the GROUP class (monotone rule)";
      }
      lanes::SerialGroup* sg = self->group_for(key, declared);
      // The member post is budget-checked BEFORE consumption
      // — refusals never consume, so the verdicts below are truthful
      // (verdict 2 hands the item back; the old sg->post() consumed
      // unconditionally and could double-free on shed).
      switch (sg->post_callback_member(w)) {
        case aorta::lanes::Post::OK:
          return 1;
        case aorta::lanes::Post::REFUSED_FULL:
          return 2;
        default:
          return 0;
      }
    }
    switch (self->rt_->pool().post_callback_item(
        aorta::flow::lane_class_for_u8(klass), w)) {
      case aorta::lanes::Post::OK:
        return 1;
      case aorta::lanes::Post::REFUSED_FULL:
        return 2;
      default:
        return 0;
    }
  }

 public:
  /// Test seam: drive the forward hook exactly as the core would.
  uint8_t forward_for_test(void (*run)(void*), void (*item_drop)(void*),
                           void* item, uint8_t klass, uint64_t group,
                           uint64_t deadline_rel_ns = 0,
                           uint32_t cost_bytes = 0) {
    return forward_thunk(this, run, item_drop, item, klass, group,
                         deadline_rel_ns, cost_bytes);
  }

 private:
  lanes::SerialGroup* group_for(uint64_t key, uint8_t declared_class) {
    std::lock_guard<std::mutex> lk(groups_m_);
    auto it = groups_.find(key);
    if (it == groups_.end()) {
      auto sp = std::make_shared<lanes::SerialGroup>(
          rt_->pool(), aorta::flow::lane_class_for_u8(declared_class));
      // Queued drain markers hold owning refs — the group outlives
      // a live-executor detach that clears this map.
      sp->bind_self(sp);
      it = groups_.emplace(key, std::move(sp)).first;
    }
    return it->second.get();
  }

  static void wake_thunk(void* ctx) {
    auto* self = static_cast<FacadeAttachment*>(ctx);
    (void)aorta_executor_wake(self->executor_);
  }

  // Stop-bridge states: UNBRIDGED → (CAS, one winner) BRIDGING →
  // (release-store after the sweep) BRIDGED.
  static constexpr uint8_t kUnbridged = 0;
  static constexpr uint8_t kBridging = 1;
  static constexpr uint8_t kBridged = 2;

  LaneRuntime* rt_;
  uint64_t executor_ = 0;
  bool attached_ = false;
  const char* attach_refusal_ = nullptr;
  /// Resolve-once monotonic stop-flag pointer (aorta_executor_stop_flag_ptr;
  /// valid until executor free — the facade's life is bracketed inside it).
  const uint8_t* stop_flag_ = nullptr;
  std::atomic<uint8_t> bridge_state_{kUnbridged};
  /// SerialDomain control block (null on standalone facades — every
  /// serial branch below degrades to the pre-domain behavior).
  std::shared_ptr<SerialControl> serial_;
  std::atomic<bool> monotone_warned_{false};

 public:
  /// Test seam: the one-shot monotone WARN latch.
  bool monotone_warned_for_test() const {
    return monotone_warned_.load(std::memory_order_acquire);
  }

  /// Test probes (stop-window family): the bridge state machine and
  /// the drain hook, driven exactly as the core would.
  uint8_t bridge_state_for_test() const {
    return bridge_state_.load(std::memory_order_acquire);
  }
  uint64_t drain_for_test(uint32_t max_items) {
    return drain_thunk(this, max_items);
  }
  void on_stop_for_test() { on_stop_thunk(this); }
#ifdef AORTA_LANES_TEST_SEAMS
  /// Double-stop test row: how many times the WINNING bridge body ran.
  uint64_t dbg_sweeps() const {
    return dbg_sweeps_.load(std::memory_order_seq_cst);
  }

 private:
  std::atomic<uint64_t> dbg_sweeps_{0};

 public:
#endif

 private:
  std::atomic<uint64_t> drain_calls_{0};
  std::mutex groups_m_;
  std::unordered_map<uint64_t, std::shared_ptr<lanes::SerialGroup>> groups_;
};

}  // namespace flow
}  // namespace aorta
