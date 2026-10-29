// Regression tests for the callback/destructor race — the C++
// Subscriber/Timer callback dispatchers reserve their in-flight slot under the
// SAME mutex the destructor uses to set `shutting_down` + drain, and the
// destructor LEAKS the CallbackState on drain timeout instead of reset()-ing it
// (which would UAF a still-running callback's scope guard). This mirrors the
// long-standing single-source pattern in service.cc.
//
// What these tests pin down:
//   1. DrainWaitsForInFlightCallback — the destructor blocks until an
//      already-dispatching callback returns before it destroys the state
//      (drain correctness, for both Subscriber and Timer).
//   2. LeaksStateOnDrainTimeout — a callback that outlives the destructor's
//      wait window makes the destructor LEAK (release without delete) rather
//      than reset(), so the hung callback's scope guard never touches freed
//      memory (no use-after-free).
//   3. ConcurrentDispatchIsRaceFree — many threads hammering the dispatcher on
//      one live CallbackState keep the in_flight accounting consistent and
//      never deadlock under the locked check+increment+decrement discipline
//      (mechanism guard for the check-then-increment race fix).
//
// LIMITATION (intentional, documented): the *raw* check-vs-increment TOCTOU
// that the fix closes is a timing-dependent free-vs-use window. Deterministically
// reproducing it requires either a production test-seam (to pause a dispatcher
// between the shutting_down check and the fetch_add) or a ThreadSanitizer build
// that models the adversarial fleet schedule. The clang/compiler-rt TSAN Tier 3
// is the home for that dynamic check; until it lands, the fix is
// validated structurally (single-source lock discipline) plus the mechanism
// guards below, which run under the ASAN hard gate.
//
// The wrappers are built with handle=0 via the befriended *DestructorRaceAccess
// structs, so the destructor's `aorta_*_free(handle_)` is skipped — no live
// Rust handle and no FFI mock expectation are required.

#include "throwing_callback_test_access.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace aorta {
namespace test_access {
namespace {

using namespace std::chrono_literals;

// Spin until `pred` is true or the deadline elapses; returns pred's final value.
template <typename Pred>
bool SpinUntil(Pred pred, std::chrono::milliseconds budget) {
  const auto deadline = std::chrono::steady_clock::now() + budget;
  while (!pred()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(200us);
  }
  return true;
}

// ---- 1. Destructor drains an in-flight callback before destroying state ----

TEST(SubscriberDestructorRace, DrainWaitsForInFlightCallback) {
  SubscriberDestructorRaceAccess access;
  std::atomic<bool> in_callback{false};
  std::atomic<bool> release{false};
  access.Build([&](const MessageView&, const MessageContextView&) {
    in_callback.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(200us);
    }
  });

  std::thread dispatcher([&] { access.Dispatch(); });
  ASSERT_TRUE(SpinUntil([&] { return in_callback.load(); }, 2s))
      << "callback never entered — dispatcher did not increment in_flight";
  EXPECT_EQ(access.in_flight_count(), 1u);

  std::atomic<bool> destroyed{false};
  std::thread destroyer([&] {
    access.Destroy();
    destroyed.store(true, std::memory_order_release);
  });

  // The destructor must NOT finish while the callback is still in flight.
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(destroyed.load(std::memory_order_acquire))
      << "destructor returned before the in-flight callback drained (UAF risk)";

  release.store(true, std::memory_order_release);
  destroyer.join();
  dispatcher.join();
  EXPECT_TRUE(destroyed.load());
}

TEST(TimerDestructorRace, DrainWaitsForInFlightCallback) {
  TimerDestructorRaceAccess access;
  std::atomic<bool> in_callback{false};
  std::atomic<bool> release{false};
  access.Build([&] {
    in_callback.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(200us);
    }
  });

  std::thread dispatcher([&] { access.Dispatch(); });
  ASSERT_TRUE(SpinUntil([&] { return in_callback.load(); }, 2s));
  EXPECT_EQ(access.in_flight_count(), 1u);

  std::atomic<bool> destroyed{false};
  std::thread destroyer([&] {
    access.Destroy();
    destroyed.store(true, std::memory_order_release);
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(destroyed.load(std::memory_order_acquire))
      << "timer destructor returned before in-flight callback drained";

  release.store(true, std::memory_order_release);
  destroyer.join();
  dispatcher.join();
  EXPECT_TRUE(destroyed.load());
}

// ---- 2. Destructor leaks (does not reset) the state on drain timeout --------

TEST(SubscriberDestructorRace, LeaksStateOnDrainTimeout) {
  // A callback that outlives the destructor's 2s wait window must force the
  // LEAK branch: reset() would free the CallbackState out from under the still
  // running callback's scope guard (a use-after-free). The leaked state stays alive,
  // so the scope guard's fetch_sub + cv notify land on valid memory.
  SubscriberDestructorRaceAccess access;
  std::atomic<bool> in_callback{false};
  std::atomic<bool> release{false};
  access.Build([&](const MessageView&, const MessageContextView&) {
    in_callback.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(500us);
    }
  });

  std::thread dispatcher([&] { access.Dispatch(); });
  ASSERT_TRUE(SpinUntil([&] { return in_callback.load(); }, 2s));

  // Destroy returns after the ~2s drain timeout, having LEAKED the state
  // (the unique_ptr released ownership; `access.state` still points to it).
  const auto t0 = std::chrono::steady_clock::now();
  access.Destroy();
  const auto elapsed = std::chrono::steady_clock::now() - t0;
  EXPECT_GE(elapsed, 1500ms)
      << "destructor returned too early — it should have waited out the drain "
         "timeout before leaking";

  // Let the still-running callback finish; its scope guard touches the leaked
  // (still valid) state. No UAF expected.
  release.store(true, std::memory_order_release);
  dispatcher.join();
  ASSERT_TRUE(
      SpinUntil([&] { return access.in_flight_count() == 0; }, 2s));

  // The production leak is intentional (one allocation per hung destructor).
  // Reclaim it here, inside the friend struct, so the test is leak-clean.
  access.DeleteLeakedState();
}

// ---- 3. Concurrent dispatch keeps in_flight accounting consistent ----------

TEST(SubscriberDestructorRace, ConcurrentDispatchIsRaceFree) {
  // Hammer the locked check+increment+decrement from many threads on one live
  // state (no concurrent destructor — the state outlives all threads). Pins
  // that the mutex discipline neither loses an increment/decrement nor
  // deadlocks. Runs clean under the ASAN gate; under Tier 3 TSAN this is where
  // the dispatch-vs-shutdown data-race check will live.
  SubscriberDestructorRaceAccess access;
  std::atomic<int> calls{0};
  access.Build([&](const MessageView&, const MessageContextView&) {
    calls.fetch_add(1, std::memory_order_relaxed);
  });

  constexpr int kThreads = 8;
  constexpr int kPerThread = 2000;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kPerThread; ++i) {
        access.Dispatch();
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(calls.load(), kThreads * kPerThread);
  EXPECT_EQ(access.in_flight_count(), 0u)
      << "in_flight must converge to zero after all dispatches drain";
  // access.Destroy() runs here via the wrapper's own teardown — state has no
  // in-flight callbacks, so it resets cleanly.
}

TEST(TimerDestructorRace, ConcurrentDispatchIsRaceFree) {
  TimerDestructorRaceAccess access;
  std::atomic<int> calls{0};
  access.Build([&] { calls.fetch_add(1, std::memory_order_relaxed); });

  constexpr int kThreads = 8;
  constexpr int kPerThread = 2000;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kPerThread; ++i) {
        access.Dispatch();
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(calls.load(), kThreads * kPerThread);
  EXPECT_EQ(access.in_flight_count(), 0u);
}

}  // namespace
}  // namespace test_access
}  // namespace aorta
