// D5 contract detection:
//
// A Flow task body that catches `...` MUST rethrow `cancelled_unwind`.
// A NON-conforming body gets the documented BOUNDED WINDOW: synchronous
// code runs until the next await, where the cancelled scope unwinds
// again — the cancellation is delayed, never lost. This test pins that
// bound (the swallow does not let the task run to a normal completion),
// plus the conforming-body baseline.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "aorta/flow_combinators.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

class SwallowHarness {
 public:
  SwallowHarness() : rt_(flow::LaneRuntime::SingleThread()) {
    drainer_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        if (!rt_->pool().drain_one()) {
          rt_->pool().wait_for_work(std::chrono::milliseconds(1));
        }
      }
    });
  }
  ~SwallowHarness() {
    rt_->pool().close();
    stop_.store(true, std::memory_order_release);
    drainer_.join();
    rt_->pool().drain_all();
  }
  flow::LaneRuntime& rt() { return *rt_; }

 private:
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::atomic<bool> stop_{false};
  std::thread drainer_;
};

template <typename Pred>
bool wait_until(Pred pred, std::chrono::seconds budget = 5s) {
  auto deadline = std::chrono::steady_clock::now() + budget;
  while (!pred() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}

// NON-CONFORMING body: swallows the unwind with a bare catch, then runs
// one synchronous marker (the documented bounded window) and awaits
// again — the cancelled scope must re-unwind at that await.
flow::Task<int> swallowing_body(std::atomic<bool>& parked,
                                std::atomic<bool>& window_ran,
                                std::atomic<bool>& escaped_window) {
  parked.store(true, std::memory_order_seq_cst);
  try {
    co_await flow::cancelled();  // resolves when the scope cancels
    // cancelled() returning normally IS the cancel signal; a conforming
    // body would clean up and finish. This body misbehaves instead:
    throw flow::cancelled_unwind{};
  } catch (...) {
    // D5 violation: swallowed, no rethrow.
  }
  window_ran.store(true, std::memory_order_seq_cst);  // the bounded window
  co_await flow::sleep(1ms);  // next await: the cancelled scope re-unwinds
  escaped_window.store(true, std::memory_order_seq_cst);  // must NOT run
  co_return 7;
}

TEST(FlowSwallowDetection, SwallowedUnwindIsReboundedAtTheNextAwait) {
  SwallowHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> window_ran{false};
  std::atomic<bool> escaped{false};
  auto child = flow::spawn_on(h.rt(), scope,
                              swallowing_body(parked, window_ran, escaped));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(20ms);  // let the cancelled() park arm
  scope.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  EXPECT_TRUE(window_ran.load(std::memory_order_seq_cst))
      << "the bounded window (sync code after the swallow) is documented";
  EXPECT_FALSE(escaped.load(std::memory_order_seq_cst))
      << "D5: the next await must re-unwind — a swallow delays the "
         "cancellation by one window, it can never suppress it";
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok()) << "a swallowed cancel must not become a value";
  EXPECT_TRUE(o->stopped().is_cancel());
  EXPECT_EQ(o->stopped().raw_reason(),
            static_cast<uint8_t>(flow::CancelReason::kShutdown));
  ASSERT_TRUE(scope.close().ok());
}

// Shared bound for the zero-delay regression pin below (function-body
// and TEST scopes both need it).
constexpr int kZeroDelaySwallowMaxIterations = 200;

// Gate 2.1 regression pin: the SAME D5 violation as swallowing_body
// above, but the bounded window loops a ZERO-DELAY `flow::sleep(0ms)`
// instead of a single 1ms sleep. Before the fix, `ambient_sleep::
// await_ready()` returned `delay.count() <= 0` — true for a 0ms delay —
// which skips `await_suspend` (and therefore the entry cancel-gate
// living there) ENTIRELY: a swallow followed by a zero-delay sleep loop
// never re-observed cancellation and ran to normal completion, an
// unbounded escape from the D5 bounded-window guarantee. The fix makes
// `await_ready()` unconditionally false so every `sleep(0ms)` — zero
// delay or not — still goes through `await_suspend`'s entry gate before
// resolving inline.
flow::Task<int> swallowing_body_zero_delay(std::atomic<bool>& parked,
                                            std::atomic<bool>& window_ran,
                                            std::atomic<int>& iterations,
                                            std::atomic<bool>& escaped_all) {
  parked.store(true, std::memory_order_seq_cst);
  try {
    co_await flow::cancelled();  // resolves when the scope cancels
    throw flow::cancelled_unwind{};
  } catch (...) {
    // D5 violation: swallowed, no rethrow.
  }
  window_ran.store(true, std::memory_order_seq_cst);  // the bounded window
  for (int i = 0; i < kZeroDelaySwallowMaxIterations; ++i) {
    iterations.store(i, std::memory_order_seq_cst);
    // The scope is ALREADY cancelled by the time this loop starts (the
    // cancel() that woke flow::cancelled() above already fired) — a
    // conforming gate must refuse/unwind on the very first iteration.
    co_await flow::sleep(0ms);
  }
  escaped_all.store(true, std::memory_order_seq_cst);  // must NOT run
  co_return 7;
}

TEST(FlowSwallowDetection, ZeroDelaySleepStillGatesAtNextAwait) {
  SwallowHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> window_ran{false};
  std::atomic<int> iterations{-1};
  std::atomic<bool> escaped{false};
  auto child = flow::spawn_on(
      h.rt(), scope,
      swallowing_body_zero_delay(parked, window_ran, iterations, escaped));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(20ms);  // let the cancelled() park arm
  scope.cancel(flow::CancelReason::kShutdown);
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  EXPECT_TRUE(window_ran.load(std::memory_order_seq_cst))
      << "the bounded window (sync code after the swallow) is documented";
  EXPECT_FALSE(escaped.load(std::memory_order_seq_cst))
      << "gate 2.1: a zero-delay flow::sleep(0ms) must still hit the "
         "entry cancel-gate at await_suspend — the old await_ready() "
         "fast path (delay.count() <= 0) skipped await_suspend entirely, "
         "letting a swallow-then-zero-delay-loop escape cancellation "
         "forever, violating the D5 bounded-window guarantee";
  EXPECT_LT(iterations.load(std::memory_order_seq_cst),
            kZeroDelaySwallowMaxIterations)
      << "the loop must re-unwind on an early iteration, not exhaust the "
         "whole bound";
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok()) << "a swallowed cancel must not become a value";
  EXPECT_TRUE(o->stopped().is_cancel());
  EXPECT_EQ(o->stopped().raw_reason(),
            static_cast<uint8_t>(flow::CancelReason::kShutdown));
  ASSERT_TRUE(scope.close().ok());
}

// CONFORMING body baseline: catch-all that rethrows the unwind — the
// task terminates as cancelled with zero extra windows.
flow::Task<int> conforming_body(std::atomic<bool>& parked,
                                std::atomic<bool>& cleanup_ran) {
  parked.store(true, std::memory_order_seq_cst);
  try {
    // A short-sleep loop: after the cancel, the NEXT sleep's ambient
    // ENTRY GATE refuses and unwinds. (A parked long
    // sleep is NOT woken mid-flight by a bare scope cancel — that is the
    // documented plane semantics; the entry gate is what bounds it.)
    for (;;) {
      co_await flow::sleep(5ms);
    }
  } catch (...) {
    cleanup_ran.store(true, std::memory_order_seq_cst);
    throw;  // the D5 contract
  }
  co_return 1;
}

TEST(FlowSwallowDetection, ConformingCatchAllRethrowTerminatesAsCancelled) {
  SwallowHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> cleanup{false};
  auto child =
      flow::spawn_on(h.rt(), scope, conforming_body(parked, cleanup));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(20ms);
  scope.cancel(flow::CancelReason::kExplicit);
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  EXPECT_TRUE(cleanup.load(std::memory_order_seq_cst));
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok());
  EXPECT_TRUE(o->stopped().is_cancel());
  ASSERT_TRUE(scope.close().ok());
}


// D7: the public "stop now, report cancelled" spelling.
flow::Task<int> cleanup_then_stop(std::atomic<bool>& parked,
                                  std::atomic<bool>& cleanup_ran) {
  parked.store(true, std::memory_order_seq_cst);
  co_await flow::cancelled();  // resolves when the scope cancels
  cleanup_ran.store(true, std::memory_order_seq_cst);  // the cleanup leg
  flow::stop_now();  // end AS CANCELLED (normal return would be OK)
}

TEST(FlowSwallowDetection, StopNowReportsCancelledWithTheScopeReason) {
  SwallowHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  std::atomic<bool> cleanup{false};
  auto child =
      flow::spawn_on(h.rt(), scope, cleanup_then_stop(parked, cleanup));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(20ms);
  scope.cancel(flow::CancelReason::kChildFailed);
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  EXPECT_TRUE(cleanup.load(std::memory_order_seq_cst));
  auto o = child.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_FALSE(o->ok()) << "stop_now() must not complete the handle OK";
  EXPECT_TRUE(o->stopped().is_cancel());
  EXPECT_EQ(o->stopped().raw_reason(),
            static_cast<uint8_t>(flow::CancelReason::kChildFailed))
      << "the reason snapshots from the scope at the stop";
  ASSERT_TRUE(scope.close().ok());
}


// The swallow-detection canary itself — a REAL armed unwind
// (produced by the ambient entry gate, not a hand-thrown token) swallowed
// by a bare catch must WARN at the frame's next suspension point.
flow::Task<int> swallow_armed_unwind(std::atomic<bool>& parked) {
  parked.store(true, std::memory_order_seq_cst);
  co_await flow::cancelled();  // resolves on cancel
  try {
    // Entry gate refuses on the cancelled scope: settle() reports
    // CANCELLED and await_resume ARMS the canary before throwing — the
    // real production path, unlike a hand-thrown cancelled_unwind{}.
    co_await flow::sleep(5ms);
  } catch (...) {
    // D5 violation: the ARMED token dies here.
  }
  co_return 3;  // final_suspend runs check_swallowed_unwind -> WARN
}

TEST(FlowSwallowDetection, SwallowedArmedUnwindFiresTheCanaryWarn) {
  SwallowHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> parked{false};
  // Assert the canary MECHANISM (the detection counter), not the WARN
  // text: the WARN rides the async log worker, which CI load can starve
  // past any fixed stderr-capture budget (two real gate reds, 2026-07-14
  // — first at a 50ms sleep, then again with a 2s capture-poll loop; the
  // second run showed the text nowhere at all within the budget). The
  // counter is bumped synchronously in check_swallowed_unwind on the
  // frame's own thread, so waiting on it is deterministic.
  const uint64_t canary_before =
      flow::detail::swallow_canary_count.load(std::memory_order_relaxed);
  auto child = flow::spawn_on(h.rt(), scope, swallow_armed_unwind(parked));
  ASSERT_TRUE(child.ok());
  ASSERT_TRUE(
      wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
  std::this_thread::sleep_for(20ms);  // let cancelled() arm
  scope.cancel(flow::CancelReason::kExplicit);
  ASSERT_TRUE(wait_until([&] { return child.is_finished(); }));
  EXPECT_TRUE(wait_until([&] {
    return flow::detail::swallow_canary_count.load(
               std::memory_order_relaxed) > canary_before;
  })) << "the swallow canary must fire when an ARMED unwind is swallowed "
         "(swallow_canary_count did not advance)";
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
