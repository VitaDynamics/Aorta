// Regression tests: proves the
// `aorta_cpp_on_service_request` dispatcher in `cpp/src/service.cc`
// catches user-callback exceptions instead of unwinding across the
// FFI boundary into Rust.
//
// The service dispatcher has more surface than the others: it
// constructs a `ServiceRequest` from the FFI handle BEFORE the
// try/catch, and uses an RAII scope guard that decrements in_flight
// under the mutex (closing the TOCTOU race fixed in C-3). The tests
// here verify both legs: the catch on user throw, and the
// in_flight-counter integrity after the unwind.

#include "throwing_callback_test_access.h"

#include <gtest/gtest.h>

#include <atomic>
#include <new>
#include <stdexcept>

namespace aorta {
namespace test_access {
namespace {

TEST(ServiceThrowingCallback, DispatcherCatchesStdRuntimeError) {
  ServiceCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const MessageView&, ServiceRequest&& req) {
    hits.fetch_add(1, std::memory_order_relaxed);
    // Discard the request without replying — the test does not exercise
    // a live FFI handle (we pass request=0 below), so the destructor of
    // the moved-into ServiceRequest is a no-op.
    (void)req;
    throw std::runtime_error("intentional throw from service callback");
  });

  EXPECT_NO_THROW(probe.Dispatch(/*request=*/0));

  EXPECT_EQ(hits.load(), 1);
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(ServiceThrowingCallback, DispatcherCatchesBadAlloc) {
  ServiceCallbackTestAccess probe;
  probe.SetCallback([](const MessageView&, ServiceRequest&&) {
    throw std::bad_alloc();
  });
  EXPECT_NO_THROW(probe.Dispatch(/*request=*/0));
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(ServiceThrowingCallback, DispatcherCatchesNonStdThrow) {
  ServiceCallbackTestAccess probe;
  probe.SetCallback(
      [](const MessageView&, ServiceRequest&&) { throw 42; });
  EXPECT_NO_THROW(probe.Dispatch(/*request=*/0));
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(ServiceThrowingCallback, DispatcherSurvivesNullUserData) {
  // null user_data with request=0: dispatcher must early-return after
  // its Release path on the ServiceRequest (request=0 keeps it invalid,
  // so Release is a no-op).
  aorta_bytes_view_t payload{};
  EXPECT_NO_THROW(
      aorta_cpp_on_service_request(&payload, /*request=*/0, nullptr));
}

TEST(ServiceThrowingCallback, DispatcherSurvivesRepeatedThrows) {
  ServiceCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const MessageView&, ServiceRequest&&) {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("repeat");
  });

  for (int i = 0; i < 100; ++i) {
    EXPECT_NO_THROW(probe.Dispatch(/*request=*/0));
  }
  EXPECT_EQ(hits.load(), 100);
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

TEST(ServiceThrowingCallback, ShuttingDownLatchSkipsCallback) {
  // Mirrors Service::~Service: setting shutting_down=true under the
  // mutex must cause the dispatcher to bail before invoking the user
  // callback. The test exercises the mutex-protected read on the
  // dispatcher side without standing up a real destructor.
  ServiceCallbackTestAccess probe;
  std::atomic<int> hits{0};
  probe.SetCallback([&](const MessageView&, ServiceRequest&&) {
    hits.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("must not fire");
  });
  probe.state.shutting_down.store(true, std::memory_order_release);
  EXPECT_NO_THROW(probe.Dispatch(/*request=*/0));
  EXPECT_EQ(hits.load(), 0);
  EXPECT_EQ(probe.in_flight_count(), 0u);
}

}  // namespace
}  // namespace test_access
}  // namespace aorta
