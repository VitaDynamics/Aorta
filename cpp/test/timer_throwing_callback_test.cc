// Regression tests: proves the
// `aorta_cpp_on_timer` dispatcher in `cpp/src/timer.cc` catches
// user-callback exceptions instead of unwinding across the FFI
// boundary into Rust.

#include "throwing_callback_test_access.h"

#include <gtest/gtest.h>

#include <atomic>
#include <new>
#include <stdexcept>

namespace aorta {
namespace test_access {
namespace {

TEST(TimerThrowingCallback, DispatcherCatchesStdRuntimeError) {
  TimerCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&]() {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("intentional throw from timer callback");
  });

  EXPECT_NO_THROW(probe.Dispatch());

  EXPECT_EQ(hits.load(), 1);
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(TimerThrowingCallback, DispatcherCatchesBadAlloc) {
  TimerCallbackTestAccess probe;
  probe.SetCallback([]() { throw std::bad_alloc(); });
  EXPECT_NO_THROW(probe.Dispatch());
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(TimerThrowingCallback, DispatcherCatchesNonStdThrow) {
  TimerCallbackTestAccess probe;
  probe.SetCallback([]() { throw 42; });
  EXPECT_NO_THROW(probe.Dispatch());
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(TimerThrowingCallback, DispatcherSurvivesNullUserData) {
  EXPECT_NO_THROW(aorta_cpp_on_timer(nullptr));
}

TEST(TimerThrowingCallback, DispatcherSurvivesRepeatedThrows) {
  TimerCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&]() {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("repeat");
  });

  for (int i = 0; i < 100; ++i) {
    EXPECT_NO_THROW(probe.Dispatch());
  }
  EXPECT_EQ(hits.load(), 100);
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(TimerThrowingCallback, ShuttingDownLatchSkipsCallback) {
  TimerCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&]() {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("must not fire");
  });
  probe.state.shutting_down.store(true, std::memory_order_release);
  EXPECT_NO_THROW(probe.Dispatch());
  EXPECT_EQ(hits.load(), 0);
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

}  // namespace
}  // namespace test_access
}  // namespace aorta
