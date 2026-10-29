// flow::SerialDomain — a task-tier serialization domain (strand), the
// callback-group successor for the coroutine tier. Tasks bound to one
// domain never run concurrently with each other — as long as they suspend
// only through Flow's own awaitables (the stated boundary; hand-rolled
// awaiters that capture the raw coroutine
// handle are outside the contract and best-effort-detected by the
// occupancy canary). `co_await` keeps you in the domain — including
// `join`/`race`/`timeout`/`join_all` (branches inherit); explicit
// spawn + `when_all` is the parallel spelling. Cross-executor and
// cross-runtime ARMS are refused loudly, never silently rerouted.
//
// Creation is a `Runtime` method (`rt->create_serial(klass, name)`,
// flow_runtime.hpp) — the only object that can prove the executor
// pairing, the ST/Disabled no-op claim, and the teardown order. This
// header carries the handle + state types only; the routing lives in
// the flow.hpp router (`detail::route_resume`), the lifetime regime in
// the admission gate (lane_runtime.h).
#pragma once

#if __cplusplus < 202002L
#error "aorta/flow_serial.hpp requires -std=c++20 (use the //cpp:aorta_cpp20_flow target)"
#endif

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "aorta/lane_runtime.h"

namespace aorta::flow {

namespace detail {

/// The domain state — co-owned by every user handle copy AND by
/// every bound frame's promise (a domain handle dropped while
/// frames are still bound keeps the strand alive through the promise
/// copies; queued markers additionally keep the SerialGroup alive via
/// its bound self-reference).
struct SerialDomainState {
  /// The lanes-layer strand. NULL on an ST / Disabled / rollback
  /// runtime: binding stamps, routing short-circuits to the
  /// existing single-consumer paths — a valid no-op domain.
  std::shared_ptr<lanes::SerialGroup> group;
  /// The runtime's admission gate (aliasing pointer into the
  /// Runtime's SerialControl — the control block outlives every state).
  std::shared_ptr<AdmissionGate> gate;
  /// The executor's monotonic stop-flag
  /// pointer (resolve-once at create_serial; valid until executor
  /// free). A Disabled/rollback runtime has NO on_stop bridge to close
  /// the gate, so validity/admission additionally observe the flag —
  /// every stop origin invalidates the domain on every runtime shape.
  const uint8_t* stop_flag = nullptr;
  /// Guarded by the gate: only a PINNED producer may touch it.
  LaneRuntime* rt = nullptr;
  /// The domain's declared class byte (monotone check).
  uint8_t klass = 0;
  std::atomic<bool> monotone_warned{false};
  /// Canary latch: the one-shot "bound frame executing outside its
  /// domain's drain slice" WARN (best-effort detection at checkpoints —
  /// the supported-awaitables boundary's honest limit).
  std::atomic<bool> canary_warned{false};
  std::string name;  // owned copy
};

struct serial_access;  // the SDK-internal constructor/reader key

#ifdef AORTA_LANES_TEST_SEAMS
/// valid()-pin seam (seam test target only): fires between the
/// successful pin and the stop-flag read — the deterministic window for
/// the concurrent-teardown UAF regression. Atomic — see
/// route_pin_seam.
inline std::atomic<void (*)(void*)> valid_pin_seam{nullptr};
inline std::atomic<void*> valid_pin_seam_ctx{nullptr};
#endif

}  // namespace detail

/// The user handle: copyable shared ownership (Scope-shape). All
/// mutation happens through spawns and the router; the handle itself is
/// only identity + liveness.
class SerialDomain {
 public:
  SerialDomain() = default;  // invalid; binding refuses

  /// False for a default-constructed handle and once the owning
  /// Runtime's teardown (or ANY executor stop — the stop flag is
  /// observed directly, so Disabled/rollback runtimes with no on_stop
  /// bridge invalidate too) closed the domain.
  ///
  /// Lifetime of the flag read: the read happens under a
  /// gate PIN — a mere program-order "gate check first" cannot stop a
  /// concurrent `~Core` from completing between the check and the read.
  /// With the pin admitted, `~Core`'s step-0 `close_and_quiesce()`
  /// BLOCKS until the unpin, and the executor is freed strictly after
  /// (step 5) — so the flag is only ever dereferenced while the
  /// executor is alive. CLOSED gate → pin refused → false, no read.
  bool valid() const {
    if (state_ == nullptr || state_->gate == nullptr) {
      return false;
    }
    if (!state_->gate->pin()) {
      return false;  // CLOSED — torn down
    }
#ifdef AORTA_LANES_TEST_SEAMS
    if (auto* seam = detail::valid_pin_seam.load(std::memory_order_acquire)) {
      seam(detail::valid_pin_seam_ctx.load(std::memory_order_acquire));
    }
#endif
    const bool stopped =
        state_->stop_flag != nullptr &&
        reinterpret_cast<const std::atomic<uint8_t>*>(state_->stop_flag)
                ->load(std::memory_order_acquire) != 0;
    state_->gate->unpin();
    return !stopped;
  }

  /// Backed by the state-owned string; "" for an invalid handle.
  const char* name() const {
    return state_ != nullptr ? state_->name.c_str() : "";
  }

 private:
  friend struct detail::serial_access;
  std::shared_ptr<detail::SerialDomainState> state_;
};

namespace detail {

/// SDK-internal access to the private state (the handle_access
/// precedent): spawn sites stamp it, the router reads it.
struct serial_access {
  static SerialDomain make(std::shared_ptr<SerialDomainState> st) {
    SerialDomain d;
    d.state_ = std::move(st);
    return d;
  }
  static const std::shared_ptr<SerialDomainState>& state(
      const SerialDomain& d) {
    return d.state_;
  }
};

}  // namespace detail

}  // namespace aorta::flow
