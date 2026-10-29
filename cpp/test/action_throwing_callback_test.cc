// Regression tests for action-dispatcher exception safety:
//
// Before this fix, the action layer's extern "C" callback dispatchers
// (`aorta_cpp_on_action_feedback`, `aorta_cpp_on_action_execute`) invoked
// user callbacks directly with no try/catch. A throwing user callback would
// unwind across the FFI boundary into Rust, which is undefined behavior
// (typically std::terminate, sometimes silent stack corruption).
//
// After the fix, both dispatchers wrap the call in try/catch, log via
// `LogActionCallbackException`, and return cleanly. The codec functions
// (Decode*/Encode*/FillOwnedActionBytesView) similarly catch and return
// AORTA_ERR_INTERNAL.

#include "aorta/action.h"

#include <gtest/gtest.h>

#include <atomic>
#include <stdexcept>

namespace aorta::detail {
namespace {

// --- aorta_cpp_on_action_feedback path -------------------------------------

class ThrowingFeedbackState : public ActionFeedbackCallbackStateBase {
 public:
  std::atomic<int> on_feedback_calls{0};

  void OnFeedback(const aorta_bytes_view_t*) override {
    on_feedback_calls.fetch_add(1, std::memory_order_relaxed);
    throw std::runtime_error("intentional throw from action feedback callback");
  }
};

TEST(ActionThrowingCallback, FeedbackDispatcherCatchesUserException) {
  ThrowingFeedbackState state;
  aorta_bytes_view_t payload{};
  payload.slices = nullptr;
  payload.slice_count = 0;
  payload.total_len = 0;

  // Must not unwind through the dispatcher into the test framework
  // (the test would crash if try/catch is missing).
  EXPECT_NO_THROW(
      aorta_cpp_on_action_feedback(&payload, nullptr, nullptr, &state));

  EXPECT_EQ(state.on_feedback_calls.load(), 1);
}

TEST(ActionThrowingCallback, FeedbackDispatcherSurvivesNullState) {
  aorta_bytes_view_t payload{};
  // Even with null user_data, the dispatcher must not crash or throw.
  EXPECT_NO_THROW(
      aorta_cpp_on_action_feedback(&payload, nullptr, nullptr, nullptr));
}

TEST(ActionThrowingCallback, FeedbackDispatcherSurvivesRepeatedThrows) {
  ThrowingFeedbackState state;
  aorta_bytes_view_t payload{};
  for (int i = 0; i < 50; ++i) {
    EXPECT_NO_THROW(
        aorta_cpp_on_action_feedback(&payload, nullptr, nullptr, &state));
  }
  EXPECT_EQ(state.on_feedback_calls.load(), 50);
}

// --- aorta_cpp_on_action_execute path --------------------------------------

class ThrowingExecuteState : public ActionServerCallbackStateBase {
 public:
  std::atomic<int> on_execute_calls{0};

  void OnExecute(const aorta_bytes_view_t*,
                 aorta_action_context_t) override {
    on_execute_calls.fetch_add(1, std::memory_order_relaxed);
    throw std::length_error("intentional throw from action execute callback");
  }
};

TEST(ActionThrowingCallback, ExecuteDispatcherCatchesUserException) {
  ThrowingExecuteState state;
  aorta_bytes_view_t payload{};

  // aorta_action_context_t is u64 in the FFI; 0 is invalid but acceptable
  // for this test since the throwing callback fires before any use.
  aorta_action_context_t fake_ctx = 0;

  EXPECT_NO_THROW(
      aorta_cpp_on_action_execute(&payload, fake_ctx, &state));

  EXPECT_EQ(state.on_execute_calls.load(), 1);
}

TEST(ActionThrowingCallback, ExecuteDispatcherSurvivesNullState) {
  aorta_bytes_view_t payload{};
  aorta_action_context_t fake_ctx = 0;
  EXPECT_NO_THROW(aorta_cpp_on_action_execute(&payload, fake_ctx, nullptr));
}

// --- release_action_bytes path ---------------------------------------------

TEST(ActionThrowingCallback, ReleaseActionBytesAcceptsNullCtx) {
  // Defensive: delete on nullptr is a no-op and must not throw.
  EXPECT_NO_THROW(aorta_cpp_release_action_bytes(nullptr, nullptr, 0));
}

TEST(ActionThrowingCallback, ReleaseActionBytesDestroysStorage) {
  auto* storage = new OwnedActionBytesStorage();
  storage->payload.assign({0xDE, 0xAD, 0xBE, 0xEF});
  EXPECT_NO_THROW(aorta_cpp_release_action_bytes(storage, nullptr, 0));
  // After this, ASAN should not flag any leak; double-free not exercised here
  // (would itself be UB).
}

// --- Different throw types -------------------------------------------------

class BadAllocFeedbackState : public ActionFeedbackCallbackStateBase {
 public:
  void OnFeedback(const aorta_bytes_view_t*) override {
    throw std::bad_alloc();
  }
};

TEST(ActionThrowingCallback, FeedbackDispatcherCatchesBadAlloc) {
  BadAllocFeedbackState state;
  aorta_bytes_view_t payload{};
  EXPECT_NO_THROW(
      aorta_cpp_on_action_feedback(&payload, nullptr, nullptr, &state));
}

class IntegerThrowingFeedbackState : public ActionFeedbackCallbackStateBase {
 public:
  void OnFeedback(const aorta_bytes_view_t*) override {
    // Non-std-exception throw (deliberately weird user behavior).
    throw 42;
  }
};

TEST(ActionThrowingCallback, FeedbackDispatcherCatchesNonStdThrow) {
  IntegerThrowingFeedbackState state;
  aorta_bytes_view_t payload{};
  EXPECT_NO_THROW(
      aorta_cpp_on_action_feedback(&payload, nullptr, nullptr, &state));
}

}  // namespace
}  // namespace aorta::detail
