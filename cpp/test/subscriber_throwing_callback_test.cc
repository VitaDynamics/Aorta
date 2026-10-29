// Regression tests: proves the `aorta_cpp_on_msg`
// dispatcher in `cpp/src/subscriber.cc` catches user-callback exceptions
// instead of unwinding across the FFI boundary into Rust.
//
// Mirrors `action_throwing_callback_test.cc`. Both check the same
// invariant: the `extern "C"` dispatcher must convert any user throw
// (std exception, non-std exception, bad_alloc, repeated throws) into
// a clean return, and must keep its `in_flight` accounting consistent
// so the destructor's drain logic does not deadlock.

#include "throwing_callback_test_access.h"

#include <gtest/gtest.h>

#include <atomic>
#include <new>
#include <stdexcept>

namespace aorta {
namespace test_access {
namespace {

TEST(SubscriberThrowingCallback, DispatcherCatchesStdRuntimeError) {
  SubscriberCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const MessageView&, const MessageContextView&) {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("intentional throw from subscriber callback");
  });

  EXPECT_NO_THROW(probe.Dispatch());

  EXPECT_EQ(hits.load(), 1);
  EXPECT_EQ(probe.in_flight_count(), 0u)
      << "RAII scope guard must decrement in_flight even on exceptional return";
}

TEST(SubscriberThrowingCallback, DispatcherCatchesBadAlloc) {
  SubscriberCallbackTestAccess probe;
  probe.SetCallback([](const MessageView&, const MessageContextView&) {
    throw std::bad_alloc();
  });
  EXPECT_NO_THROW(probe.Dispatch());
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(SubscriberThrowingCallback, DispatcherCatchesNonStdThrow) {
  SubscriberCallbackTestAccess probe;
  probe.SetCallback(
      [](const MessageView&, const MessageContextView&) { throw 42; });
  EXPECT_NO_THROW(probe.Dispatch());
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(SubscriberThrowingCallback, DispatcherSurvivesNullUserData) {
  // null user_data path: the dispatcher must early-return before any
  // CallbackState dereference. (Pre-existing safety; covered here so a
  // future refactor doesn't accidentally regress it.)
  aorta_bytes_view_t payload{};
  aorta_schema_hash_view_t schema{};
  aorta_msg_context_t ctx{};
  EXPECT_NO_THROW(aorta_cpp_on_msg(&payload, &schema, &ctx, nullptr));
}

TEST(SubscriberThrowingCallback, DispatcherSurvivesRepeatedThrows) {
  // 100 throws back-to-back: catches MUST be idempotent and in_flight
  // MUST converge to zero. A leaked in_flight increment here would
  // deadlock the Subscriber destructor in production.
  SubscriberCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const MessageView&, const MessageContextView&) {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("repeat");
  });

  for (int i = 0; i < 100; ++i) {
    EXPECT_NO_THROW(probe.Dispatch());
  }
  EXPECT_EQ(hits.load(), 100);
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(SubscriberThrowingCallback, ShuttingDownLatchSkipsCallback) {
  // Once shutting_down is latched, the dispatcher must short-circuit
  // before invoking the user callback (in-production: the Subscriber
  // destructor sets this and waits for in_flight to drain).
  SubscriberCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const MessageView&, const MessageContextView&) {
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
