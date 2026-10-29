#pragma once

// Test-only accessors that construct the private `CallbackState` of each
// C++ wrapper (Subscriber / Timer / Service / WatchHandle) and drive the
// corresponding `extern "C"` dispatcher directly. This mirrors the
// `aorta::detail::ActionFeedback/ExecuteCallbackStateBase` pattern in
// `cpp/test/action_throwing_callback_test.cc` — Action has its base
// class exposed in the public header by design (so user templates can
// inherit from it); for the other dispatchers the state struct is
// private, and these test-access structs are befriended one-per-class
// in the public headers.
//
// Each `*CallbackTestAccess` struct holds a real, fully-initialised
// `CallbackState` so the dispatcher's `shutting_down` /
// `in_flight` / mutex paths are exercised end-to-end. A caller can:
//
//   1. Install a callback (typically one that throws),
//   2. invoke `Dispatch()` to fire the dispatcher once,
//   3. read `in_flight_count()` to verify the RAII scope guard
//      decremented the counter even on exceptional return.
//
// All implementations are inline header-only so the test target depends
// only on the corresponding aorta library; no extra `cc_library` is
// needed.

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include <aorta.h>

#include "aorta/contextstore.h"
#include "aorta/service.h"
#include "aorta/subscriber.h"
#include "aorta/timer.h"

namespace aorta {
namespace test_access {

struct SubscriberCallbackTestAccess {
  Subscriber::CallbackState state;

  void SetCallback(Subscriber::Callback cb) { state.cb = std::move(cb); }

  void Dispatch() {
    aorta_bytes_view_t payload{};
    aorta_schema_hash_view_t schema{};
    aorta_msg_context_t ctx{};
    aorta_cpp_on_msg(&payload, &schema, &ctx, &state);
  }

  uint32_t in_flight_count() const {
    return state.in_flight.load(std::memory_order_acquire);
  }
};

struct TimerCallbackTestAccess {
  Timer::CallbackState state;

  void SetCallback(Timer::Callback cb) { state.cb = std::move(cb); }

  void Dispatch() { aorta_cpp_on_timer(&state); }

  uint32_t in_flight_count() const {
    return state.in_flight.load(std::memory_order_acquire);
  }
};

struct ServiceCallbackTestAccess {
  Service::CallbackState state;

  void SetCallback(Service::RequestCallback cb) { state.cb = std::move(cb); }

  // The dispatcher takes an `aorta_service_request_t` (uint64). A handle
  // of 0 makes `ServiceRequest::IsValid()` return false, so its
  // destructor / Release() / Reply() are all no-ops — the throwing-path
  // tests don't need a live Rust-side handle. Tests that DO need the
  // request to look live (e.g. for verifying Release-on-bail behavior
  // would require real FFI) can pass a non-zero sentinel and mock-out
  // `aorta_service_request_release` via the test mock layer.
  void Dispatch(aorta_service_request_t request = 0) {
    aorta_bytes_view_t payload{};
    aorta_cpp_on_service_request(&payload, request, &state);
  }

  uint32_t in_flight_count() const {
    return state.in_flight.load(std::memory_order_acquire);
  }
};

// Destructor-race access: builds a REAL Subscriber / Timer with handle=0 — so
// the destructor's `aorta_*_free(handle_)` is skipped (no live Rust handle and
// no FFI mock expectation required) — wrapping a heap CallbackState. A
// concurrent dispatcher thread can then race the real `~Subscriber` /
// `~Timer` drain+reset+leak path that the callback/destructor race fix
// hardens. The raw `state` pointer is what the dispatcher operates on (exactly
// as the Rust side passes it as `user_data`); `Destroy()` runs the real
// destructor.
struct SubscriberDestructorRaceAccess {
  std::unique_ptr<Subscriber> sub;
  Subscriber::CallbackState* state = nullptr;  // owned by `sub` until Destroy()

  void Build(Subscriber::Callback cb) {
    auto owned = std::unique_ptr<Subscriber::CallbackState>(
        new Subscriber::CallbackState());
    owned->cb = std::move(cb);
    state = owned.get();
    sub.reset(new Subscriber(/*handle=*/0, std::move(owned), /*node=*/nullptr));
  }

  void Dispatch() {
    aorta_bytes_view_t payload{};
    aorta_schema_hash_view_t schema{};
    aorta_msg_context_t ctx{};
    aorta_cpp_on_msg(&payload, &schema, &ctx, state);
  }

  void Destroy() { sub.reset(); }

  uint32_t in_flight_count() const {
    return state->in_flight.load(std::memory_order_acquire);
  }

  // Reclaims a state that the destructor intentionally LEAKED on drain timeout
  // (the test's non-friend scope cannot name the private CallbackState type to
  // delete it). Only valid once no callback is in flight.
  void DeleteLeakedState() {
    delete state;
    state = nullptr;
  }
};

struct TimerDestructorRaceAccess {
  std::unique_ptr<Timer> timer;
  Timer::CallbackState* state = nullptr;  // owned by `timer` until Destroy()

  void Build(Timer::Callback cb) {
    auto owned =
        std::unique_ptr<Timer::CallbackState>(new Timer::CallbackState());
    owned->cb = std::move(cb);
    state = owned.get();
    timer.reset(new Timer(/*handle=*/0, std::move(owned), /*node=*/nullptr));
  }

  void Dispatch() { aorta_cpp_on_timer(state); }

  void Destroy() { timer.reset(); }

  uint32_t in_flight_count() const {
    return state->in_flight.load(std::memory_order_acquire);
  }
};

struct WatchCallbackTestAccess {
  WatchHandle::State state;

  void SetCallback(ContextStore::ChangeCallback cb) {
    state.callback = std::move(cb);
  }

  void Dispatch() {
    aorta_ctx_change_t change{};
    aorta_cpp_on_ctx_change(&change, &state);
  }

  // Test-only: raw FFI handle of a live WatchHandle (the friend
  // relationship grants access) — used to force teardown failures
  // (freeing the FFI watch behind the wrapper's back).
  static aorta_ctx_watch_t RawHandle(const WatchHandle& w) {
    return w.handle_;
  }
};

struct OwnerLivelinessWatchCallbackTestAccess {
  OwnerLivelinessWatchHandle::State state;

  void SetCallback(ContextStore::OwnerLivelinessCallback cb) {
    state.callback = std::move(cb);
  }

  void Dispatch() {
    aorta_ctx_owner_liveliness_event_t event{};
    aorta_cpp_on_ctx_owner_liveliness(&event, &state);
  }

  static aorta_ctx_owner_liveliness_watch_t RawHandle(
      const OwnerLivelinessWatchHandle& w) {
    return w.handle_;
  }
};

}  // namespace test_access
}  // namespace aorta
