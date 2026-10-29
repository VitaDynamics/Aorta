// Regression tests: proves the
// `aorta_cpp_on_ctx_change` dispatcher in `cpp/src/contextstore.cc`
// catches user-callback exceptions instead of unwinding across the
// FFI boundary into Rust.
//
// Unlike subscriber/timer/service, the contextstore dispatcher has no
// `in_flight` counter — the watch teardown contract relies on
// `aorta_ctx_watch_free` awaiting Zenoh's undeclare in Rust before
// returning. So the C++ side only has to satisfy: (a) catch user
// throws so they don't unwind into Rust, (b) survive null user_data,
// (c) survive repeated throws without leaking state.

#include "throwing_callback_test_access.h"

#include <gtest/gtest.h>

#include <atomic>
#include <new>
#include <stdexcept>

namespace aorta {
namespace test_access {
namespace {

TEST(ContextStoreThrowingCallback, DispatcherCatchesStdRuntimeError) {
  WatchCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const ContextChange&) {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("intentional throw from ctx watch callback");
  });

  EXPECT_NO_THROW(probe.Dispatch());
  EXPECT_EQ(hits.load(), 1);
}

TEST(ContextStoreThrowingCallback, DispatcherCatchesBadAlloc) {
  WatchCallbackTestAccess probe;
  probe.SetCallback(
      [](const ContextChange&) { throw std::bad_alloc(); });
  EXPECT_NO_THROW(probe.Dispatch());
}

TEST(ContextStoreThrowingCallback, DispatcherCatchesNonStdThrow) {
  WatchCallbackTestAccess probe;
  probe.SetCallback([](const ContextChange&) { throw 42; });
  EXPECT_NO_THROW(probe.Dispatch());
}

TEST(ContextStoreThrowingCallback, DispatcherSurvivesNullUserData) {
  aorta_ctx_change_t change{};
  EXPECT_NO_THROW(aorta_cpp_on_ctx_change(&change, nullptr));
}

TEST(ContextStoreThrowingCallback, DispatcherSurvivesNullCallback) {
  // State with no callback installed — dispatcher must early-return
  // without dereferencing the empty std::function.
  WatchCallbackTestAccess probe;
  // intentionally leave probe.state.callback default-constructed (empty)
  EXPECT_NO_THROW(probe.Dispatch());
}

TEST(ContextStoreThrowingCallback, DispatcherSurvivesRepeatedThrows) {
  WatchCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const ContextChange&) {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("repeat");
  });

  for (int i = 0; i < 100; ++i) {
    EXPECT_NO_THROW(probe.Dispatch());
  }
  EXPECT_EQ(hits.load(), 100);
}

}  // namespace
}  // namespace test_access
}  // namespace aorta
