// flow::Runtime — the ONE setup object for the C++ flow golden path.
// Owns the four pieces the demos
// used to wire by hand — core executor, LaneRuntime, FacadeAttachment,
// spin thread — and encodes the teardown ORDER structurally (the
// lane_runtime.h stop-then-destroy precondition cannot be mis-ordered
// through this type; the one remaining contract on YOU is the teardown
// THREAD — destroy the Runtime from an external thread, never from a
// task or callback it is dispatching, see ~Core):
//
//   auto rt = flow::Runtime::FromEnv();            // setup: one line
//   if (!rt.ok()) return 1;
//   flow::Scope scope = rt->root_scope();
//   auto h = rt->spawn(scope, work());             // TaskHandle<T>
//   ...
//   (void)scope.close();                           // teardown: the scope,
// }                                                // then Runtime's dtor
//
// The pieces stay reachable for expert composition — `lanes()`,
// `executor()` — and the raw FacadeAttachment surface remains public in
// lane_runtime.h (the expert tier: cross-executor wiring, detach-rollback
// mid-process). This type is the golden path, not a cage.
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aorta/detail/reentrancy_check.h"
#include "aorta/flow.hpp"

namespace aorta {
namespace flow {

#ifdef AORTA_LANES_TEST_SEAMS
/// Stopped-before-attach seam (seam test target only): invoked by
/// Runtime::create with the fresh executor handle, before the facade
/// attaches. Atomic — see flow.hpp route_pin_seam.
inline std::atomic<void (*)(uint64_t)> create_pre_attach_seam{nullptr};
#endif

class Runtime {
 public:
  /// Single-threaded lanes (the fleet default) — honors the
  /// AORTA_CPP_LANES=0 rollback like every lane-enabling constructor.
  /// `nodes`: node handles whose callbacks (service handlers, push
  /// subscribers, timers) this runtime's executor dispatches —
  /// `Executor::AddNode` parity, applied BEFORE the spin thread starts.
  /// That ordering is the only sound one: `aorta_executor_spin` holds
  /// the dispatch mutex for the whole spin, so post-creation add_node
  /// was a startup RACE against the spin thread (hang on loss); the C
  /// ABI now refuses it with AORTA_ERR_BUSY, and this parameter is the
  /// supported spelling. Handles only (`node->handle()`) — this header
  /// stays independent of the full SDK surface.
  static Outcome<Runtime> SingleThread(std::vector<uint64_t> nodes = {}) {
    return create(LaneRuntime::SingleThread(), std::move(nodes));
  }

  /// Multi-threaded lanes (explicit performance opt-in): pool workers per
  /// class lane (index = class id; missing = 0).
  static Outcome<Runtime> MultiThread(std::vector<uint32_t> workers_per_lane,
                                      std::vector<uint64_t> nodes = {}) {
    return create(LaneRuntime::MultiThread(std::move(workers_per_lane)),
                  std::move(nodes));
  }

  /// Env-controlled: AORTA_CPP_LANES=0 = queue-path rollback, else the
  /// single-threaded default.
  static Outcome<Runtime> FromEnv(std::vector<uint64_t> nodes = {}) {
    return create(LaneRuntime::FromEnv(), std::move(nodes));
  }

  Runtime() = default;  // invalid (moved-from shape); valid() is false
  Runtime(Runtime&&) noexcept = default;
  Runtime& operator=(Runtime&&) noexcept = default;
  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;
  ~Runtime() = default;  // Core's destructor encodes the teardown order

  bool valid() const { return core_ != nullptr; }

  /// A fresh root scope (Rust `node.root_scope()` parity). Scopes are
  /// core-side objects — this is naming sugar, kept on the Runtime so the
  /// golden path reads top-to-bottom off one object.
  Scope root_scope(const Scope::Options& options) const {
    return Scope::root(options);
  }
  Scope root_scope() const { return Scope::root(); }

  /// Spawn a task on the lane plane (ambient context stamped: this
  /// runtime + its executor + `scope`). Returns the joinable handle. An
  /// invalid (moved-from) Runtime refuses with a handle that RESOLVES
  /// (never parks): `!ok()`, awaits and take_outcome() deliver the
  /// refusal code.
  template <typename T = void, typename K = task_kind_regular>
  [[nodiscard]] TaskHandle<T> spawn(Scope scope, Task<T, K> task) const {
    if (!valid()) {
      return detail::handle_access::refused<T>(AORTA_ERR_INVALID_ARGUMENT);
    }
    return spawn_on(*core_->rt, std::move(scope), std::move(task));
  }

  /// Capturing-lambda-safe spawn (the co_invoke wrapper — flow.hpp):
  /// the callable and its arguments COPY into the wrapper frame; the body
  /// receives a co-owned Scope as its first parameter.
  template <typename F, typename... Args>
  [[nodiscard]] TaskHandle<> spawn_with(Scope scope, F f, Args... args) const {
    if (!valid()) {
      return detail::handle_access::refused<void>(AORTA_ERR_INVALID_ARGUMENT);
    }
    return ::aorta::flow::spawn_with(*core_->rt, std::move(scope),
                                     std::move(f), std::move(args)...);
  }

  /// A fresh SerialDomain executing at `klass` on this runtime's pool
  /// Tasks spawned into it never run concurrently with each other
  /// (within the supported-awaitables boundary);
  /// co_await keeps you in the domain; explicit spawn + when_all is the
  /// parallel spelling. On an ST or Disabled/rollback runtime this
  /// returns a VALID no-op domain (binding stamps, routing
  /// short-circuits — the single consumer already serializes). `name` is
  /// COPIED into the domain state.
  ///
  /// Failure model: invalid-handle, loud in
  /// the logs — the caller's contract is `valid()`. Every failure path
  /// returns a handle whose `valid()` reads false, and the reasons are
  /// deliberately NOT programmatically distinguishable (all of them are
  /// startup/lifecycle programming errors with no per-reason recovery;
  /// the WARN log names the cause). The paths:
  ///   - invalid/moved-from Runtime            -> INVALID handle, WARN;
  ///   - enabled MT runtime whose facade was
  ///     detached mid-life (expert path)       -> INVALID handle, WARN;
  ///   - executor already stopped at creation  -> INVALID handle, WARN.
  /// A stop landing AFTER creation is NOT a create failure: the
  /// returned domain reads invalid through the same stop flag, and
  /// spawns with it refuse.
  /// `Status`/`Outcome` returns stay reserved for runtime-recoverable
  /// refusals (the `detach()` precedent); an additive Outcome-returning
  /// sibling remains possible without breaking this signature.
  SerialDomain create_serial(EventPriority klass = EventPriority::kNormal,
                             std::string_view name = {}) const {
    if (!valid()) {
      AORTA_LOG_SCOPE(WARN, "aorta.flow")
          << "create_serial on an invalid/moved-from Runtime — returning "
             "an INVALID domain (spawns with it will refuse)";
      return SerialDomain{};
    }
    auto& ctrl = core_->serial;
    // Control-plane linearization: create_serial racing a winning
    // detach observes the detached facade UNDER THE SAME MUTEX and
    // refuses.
    std::lock_guard<std::mutex> lk(ctrl->control_m);
    const bool mt = core_->rt->enabled() && core_->rt->pool_workers() > 0;
    if (mt && (core_->facade == nullptr || !core_->facade->attached())) {
      AORTA_LOG_SCOPE(WARN, "aorta.flow")
          << "create_serial on a detached MT runtime — returning an "
             "INVALID domain (spawns with it will refuse)";
      return SerialDomain{};
    }
    // Resolve the executor's monotonic stop flag ONCE — validity
    // observes it directly, so a Disabled/rollback runtime (no on_stop
    // bridge) still invalidates on ANY stop origin. Valid until
    // aorta_executor_free (~Core step 5, after every domain is inert).
    // Resolved BEFORE allocation: a stop already observable HERE
    // refuses loudly like the other create_serial failure
    // paths — default handle, WARN. A stop landing after this check is
    // the normal lifecycle instead: the returned domain reads invalid
    // through this same flag, never a create "failure".
    const uint8_t* stop_flag = nullptr;
    (void)aorta_executor_stop_flag_ptr(core_->exec, &stop_flag);
    if (stop_flag != nullptr &&
        reinterpret_cast<const std::atomic<uint8_t>*>(stop_flag)
                ->load(std::memory_order_acquire) != 0) {
      AORTA_LOG_SCOPE(WARN, "aorta.flow")
          << "create_serial on an already-stopped executor — returning "
             "an INVALID domain (spawns with it will refuse)";
      return SerialDomain{};
    }
    auto* st = new detail::SerialDomainState();
    st->rt = core_->rt.get();
    st->klass = static_cast<uint8_t>(klass);
    st->name.assign(name);
    // Aliasing pointer: the gate is a member of the control block; the
    // block outlives every domain state that points into it.
    st->gate = std::shared_ptr<detail::AdmissionGate>(ctrl, &ctrl->gate);
    st->stop_flag = stop_flag;
    if (mt) {
      auto sg = std::make_shared<lanes::SerialGroup>(
          core_->rt->pool(), lane_class_for(klass), /*batch_budget=*/16,
          /*resume_domain=*/true);
      // Queued drain markers hold owning refs — the strand
      // outlives a dropped user handle.
      sg->bind_self(sg);
      st->group = std::move(sg);
    }
    ctrl->live_domains.fetch_add(1, std::memory_order_acq_rel);
    std::shared_ptr<detail::SerialDomainState> sp(
        st, [ctrl_sp = ctrl](detail::SerialDomainState* p) {
          // The live-domain count follows STATE lifetime (frames
          // co-own the state — a domain is live while any owner,
          // user handle or bound promise, remains).
          ctrl_sp->live_domains.fetch_sub(1, std::memory_order_acq_rel);
          delete p;
        });
    return detail::serial_access::make(std::move(sp));
  }

  /// Domain-bound spawn: binds `task` (and, by inheritance, its
  /// whole co_await descent) to `d`. Refusals (invalid/foreign/dead
  /// domain) come back as a handle that RESOLVES with
  /// INVALID_ARGUMENT — never parks.
  template <typename T = void, typename K = task_kind_regular>
  [[nodiscard]] TaskHandle<T> spawn(Scope scope, const SerialDomain& d,
                                    Task<T, K> task) const {
    if (!valid()) {
      return detail::handle_access::refused<T>(AORTA_ERR_INVALID_ARGUMENT);
    }
    return spawn_on(*core_->rt, std::move(scope), d, std::move(task));
  }

  /// Domain-bound capturing-lambda spawn (the co_invoke wrapper).
  template <typename F, typename... Args>
  [[nodiscard]] TaskHandle<> spawn_with(Scope scope, const SerialDomain& d,
                                        F f, Args... args) const {
    if (!valid()) {
      return detail::handle_access::refused<void>(AORTA_ERR_INVALID_ARGUMENT);
    }
    Scope for_body = scope.child();  // REAL child scope handed to the body
    return spawn_on(*core_->rt, std::move(scope), d,
                    co_invoke(std::move(f), std::move(for_body),
                              std::move(args)...));
  }

  // NOTE deliberately NO add_node method: the spin thread starts inside
  // create() and aorta_executor_spin holds the dispatch mutex for the
  // whole spin, so a post-creation add was a startup race (hang on loss
  // before the C ABI's BUSY refusal). Pass node handles to the factory.

  /// Expert access: the lane runtime (explicit-form verbs, stats).
  /// Precondition: valid().
  LaneRuntime& lanes() const { return *core_->rt; }

  /// Expert access: the core executor handle (explicit resume_on{exec},
  /// cross-executor composition). 0 when invalid.
  uint64_t executor() const { return core_ ? core_->exec : 0; }

  /// False in the AORTA_CPP_LANES=0 rollback (queue-path posting through
  /// the same executor — every verb still works).
  bool lanes_attached() const {
    return core_ != nullptr && core_->facade != nullptr &&
           core_->facade->attached();
  }

 private:
  // Heap-held so Runtime itself is freely movable: the attachment's hooks
  // and the spin thread capture addresses/handles that live HERE, never
  // in the (movable) Runtime shell.
  struct Core {
    uint64_t exec = 0;
    std::unique_ptr<LaneRuntime> rt;
    std::unique_ptr<FacadeAttachment> facade;
    std::thread spin;
    /// SerialDomain control block: the admission gate + live
    /// domain count, shared with the facade's stop bridge and (via
    /// aliasing pointers) with every domain state — shared_ptr-owned so
    /// a domain handle outliving the Runtime still reads a valid
    /// (CLOSED) gate.
    std::shared_ptr<SerialControl> serial = std::make_shared<SerialControl>();

    ~Core() {
      AORTA_CHECK_NOT_REENTRANT("flow::Runtime::Core::~Core");
      // Self-teardown guard:
      // destroying the Runtime ON one of its own dispatch threads — the
      // spin thread, or an MT lane WORKER (a task/callback owning the
      // last reference) — would self-join (here for spin, in ~LanePool
      // for a worker): resource_deadlock_would_occur out of a noexcept
      // destructor, i.e. std::terminate with no explanation. Fail loud
      // and teachable instead: teardown must run on an external thread.
      const bool on_own_spin =
          spin.joinable() && spin.get_id() == std::this_thread::get_id();
      const bool on_own_worker =
          rt != nullptr && rt->enabled() && rt->pool().on_worker_thread();
      if (on_own_spin || on_own_worker) {
        AORTA_LOG(ERROR)
            << "flow::Runtime destroyed on its own "
            << (on_own_spin ? "spin thread" : "lane worker")
            << " — a task or callback must not own the last Runtime "
               "reference; run the teardown from an external thread";
        std::abort();  // self-teardown contract abort, D1 lint-exempt
      }
      // The structural stop-then-destroy ordering (the lane_runtime.h
      // teardown contract — encoded here so users cannot get it
      // wrong). SerialDomain support prepends step 0: the gate
      // closes FIRST — from here no new domain member can exist
      // anywhere; late completions take the stamped inline-cancel. The
      // quiesce is the idempotent belt (an external stop already closed
      // the gate through the on_stop bridge; a second fetch_or is a
      // no-op) and waits out admitted pin windows before the pool goes.
      if (serial != nullptr) {
        serial->gate.close_and_quiesce();  // 0. gate first
      }
      if (exec != 0) {
        (void)aorta_executor_stop(exec);  // 1. stop bridge: pool close +
                                          //    arm sweep + wake
      }
      if (spin.joinable()) {
        spin.join();  // 2. the final stop-drain completes (residue
                      //    delivered on the spin thread)
      }
      facade.reset();  // 3. detach (quiesce in-flight hook invocations;
                       //    the CLOSED gate passes the refusal
                       //    predicate — normal teardown never
                       //    self-refuses)
      rt.reset();      // 4. ~LaneRuntime under a stopped, drained executor
      if (exec != 0) {
        (void)aorta_executor_free(exec);  // 5. release the handle
      }
    }
  };

  static Outcome<Runtime> create(std::unique_ptr<LaneRuntime> rt,
                                 std::vector<uint64_t> nodes = {}) {
    AORTA_CHECK_NOT_REENTRANT("flow::Runtime::create");
    auto core = std::make_unique<Core>();
    // Executor kind follows the pool shape:
    // the core REFUSES mismatched pairings (ST executor + MT pool and
    // vice versa) at attach time — pick the matching kind up front.
    const bool mt = rt->enabled() && rt->pool_workers() > 0;
    const aorta_result_t rc = aorta_executor_new(
        mt ? AORTA_EXECUTOR_MULTI : AORTA_EXECUTOR_SINGLE, &core->exec);
    if (rc != AORTA_OK) {
      return Outcome<Runtime>::Stop(
          Stopped::Fault(aorta::Status::FromResult(rc)));
    }
    // Node dispatch wiring happens HERE — before the facade and the spin
    // thread — the one point where the executor mutex is free by
    // construction.
    for (uint64_t node_handle : nodes) {
      const aorta_result_t nrc = aorta_executor_add_node(core->exec, node_handle);
      if (nrc != AORTA_OK) {
        return Outcome<Runtime>::Stop(
            Stopped::Fault(aorta::Status::FromResult(nrc)));
      }
    }
    core->rt = std::move(rt);
#ifdef AORTA_LANES_TEST_SEAMS
    // Stopped-before-attach test row: the seam lets the
    // test stop the executor BETWEEN creation and the facade attach —
    // the real Runtime::create path then surfaces the detach-and-fail
    // handshake reason. Seam-test targets only.
    if (auto* seam = create_pre_attach_seam.load(std::memory_order_acquire)) {
      seam(core->exec);
    }
#endif
    // Attaches when the runtime is enabled; on the Disabled/rollback leg
    // it still wires the fallback-executor routing — every flow
    // verb then rides the queue path transparently.
    core->facade = std::make_unique<FacadeAttachment>(*core->rt, core->exec);
    if (core->rt->enabled() && !core->facade->attached()) {
      // An ENABLED runtime that failed to attach would silently run the
      // queue path while claiming lanes — fail creation instead (the
      // Core destructor unwinds the pieces in order; the spin thread was
      // never started). Detach-and-fail: the facade's handshake
      // reason (e.g. "executor already stopped") rides the message.
      const char* why = core->facade->attach_refusal();
      return Outcome<Runtime>::Stop(Stopped::Fault(aorta::Status(
          AORTA_ERR_INVALID_ARGUMENT,
          why != nullptr
              ? std::string("enabled runtime failed to attach its facade: ") +
                    why
              : std::string("enabled runtime failed to attach its facade"))));
    }
    // SerialDomain wiring: the facade's stop bridge closes THIS
    // gate as its first bridge side effect; bound before the spin
    // thread starts and before any domain can exist.
    core->facade->bind_serial_control(core->serial);
    core->spin = std::thread([exec = core->exec] {
      (void)aorta_executor_spin(exec);
    });
    Runtime r;
    r.core_ = std::move(core);
    return Outcome<Runtime>::Value(std::move(r));
  }

  std::unique_ptr<Core> core_;
};

}  // namespace flow
}  // namespace aorta
