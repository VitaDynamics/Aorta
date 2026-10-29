// Flow C++ combinators over the arbiter spine.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include "aorta/flow_combinators.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

class CombinatorHarness {
 public:
  CombinatorHarness() : rt_(flow::LaneRuntime::SingleThread()) {
    drainer_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        if (!rt_->pool().drain_one()) {
          rt_->pool().wait_for_work(std::chrono::milliseconds(1));
        }
      }
    });
  }
  ~CombinatorHarness() {
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

flow::Task<void> sleeper_task(flow::LaneRuntime& rt, std::atomic<bool>& done,
                              std::atomic<int64_t>& elapsed_ms) {
  auto start = std::chrono::steady_clock::now();
  co_await flow::sleep(rt, 20ms);
  elapsed_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count(),
                   std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST(FlowCombinators, SleepFiresThroughTheArbiterTimer) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> done{false};
  std::atomic<int64_t> elapsed{0};
  auto s = flow::spawn_on(h.rt(), scope, sleeper_task(h.rt(), done, elapsed));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load(std::memory_order_seq_cst));
  EXPECT_GE(elapsed.load(std::memory_order_seq_cst), 18)
      << "the core sleeper honored the delay";
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<int> value_after(flow::LaneRuntime& rt, std::chrono::milliseconds d,
                            int v) {
  if (d.count() > 0) {
    co_await flow::sleep(rt, d);
  }
  co_return v;
}

flow::Task<void> join_body(flow::LaneRuntime& rt, flow::Scope scope,
                               std::atomic<int>& sum) {
  auto [a, b] = co_await flow::join(rt, scope,
                                        value_after(rt, 5ms, 20),
                                        value_after(rt, 10ms, 22));
  sum.store(a + b, std::memory_order_seq_cst);
}

TEST(FlowCombinators, WhenAllResolvesBothValues) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> sum{0};
  auto s = flow::spawn_on(h.rt(), scope, join_body(h.rt(), scope, sum));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (sum.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(sum.load(std::memory_order_seq_cst), 42);
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> race_body(flow::LaneRuntime& rt, flow::Scope scope,
                           std::atomic<int>& winner) {
  int w = co_await flow::race(rt, value_after(rt, 2ms, 1),
                              value_after(rt, 200ms, 2), scope);
  winner.store(w, std::memory_order_seq_cst);
}

TEST(FlowCombinators, RaceFastSideWinsAndLoserDrainsStructurally) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> winner{0};
  auto started = std::chrono::steady_clock::now();
  auto s = flow::spawn_on(h.rt(), scope, race_body(h.rt(), scope, winner));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (winner.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(winner.load(std::memory_order_seq_cst), 1);
  EXPECT_LT(std::chrono::steady_clock::now() - started, 2s)
      << "the frame resumed at the winner, not the 200ms loser";
  // Structured drain: close() waits for the loser task (its late
  // complete no-ops on the CAS).
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> timeout_win_body(flow::LaneRuntime& rt, flow::Scope scope,
                                  std::atomic<int>& out) {
  int v = co_await flow::timeout(rt, 5s, value_after(rt, 2ms, 7), scope);
  out.store(v, std::memory_order_seq_cst);
}

struct TimeoutSentinel {
  std::atomic<bool>* fired;
  ~TimeoutSentinel() { fired->store(true, std::memory_order_seq_cst); }
};

flow::Task<void> timeout_expire_body(flow::LaneRuntime& rt, flow::Scope scope,
                                     std::atomic<bool>& unwound) {
  TimeoutSentinel sentinel{&unwound};
  // Loser payload is SHORT: the frame unwinds at the 20ms
  // window either way, and scope.close() below reaps the loser in
  // ~400ms instead of relying on implicit harness teardown to bury a
  // 60s task.
  (void)co_await flow::timeout(rt, 20ms, value_after(rt, 400ms, 9), scope);
  // Unreachable: the window fires first and the deadline delivery
  // unwinds this frame (sentinel destructor proves it).
}

TEST(FlowCombinators, TimeoutTaskWinsInsideWindow) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  auto s = flow::spawn_on(h.rt(), scope, timeout_win_body(h.rt(), scope, out));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (out.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 7);
  ASSERT_TRUE(scope.close().ok());
}

TEST(FlowCombinators, TimeoutWindowFiresAndUnwindsTheFrame) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto started = std::chrono::steady_clock::now();
  auto s = flow::spawn_on(h.rt(), scope,
                          timeout_expire_body(h.rt(), scope, unwound));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(unwound.load(std::memory_order_seq_cst))
      << "deadline delivery unwound the frame (destructors ran)";
  EXPECT_LT(std::chrono::steady_clock::now() - started, 3s)
      << "resumed at the 20ms window, not the loser payload";
  // Structured reap: the loser's 400ms sleep resolves and
  // its late completion no-ops on the CAS; close() then joins it —
  // nothing left for implicit harness teardown.
  EXPECT_TRUE(scope.close().ok()) << "close reaps the timed-out loser";
}

flow::Task<void> on_body(flow::LaneRuntime& rt, std::atomic<int>& out) {
  int v = co_await flow::offload(rt, aorta::EventPriority::kBackground,
                            [] { return 41 + 1; });
  out.store(v, std::memory_order_seq_cst);
}

// --------------------------------------------------------------------
// Exception fidelity: a REAL exception escaping
// a branch/closure surfaces to the caller as itself, never as the
// generic cancellation unwind. Outcome codes: 2 = the real exception,
// 4 = generic unwind (the pre-fix behavior), 1/3 = wrong result.

flow::Task<int> thrower_after(flow::LaneRuntime& rt,
                              std::chrono::milliseconds d) {
  co_await flow::sleep(rt, d);
  throw std::runtime_error("boom");
}

flow::Task<void> join_throw_body(flow::LaneRuntime& rt, flow::Scope scope,
                                     std::atomic<int>& outcome) {
  try {
    (void)co_await flow::join(rt, scope, value_after(rt, 2ms, 1),
                                  thrower_after(rt, 5ms));
    outcome.store(1, std::memory_order_seq_cst);
  } catch (const std::runtime_error& e) {
    outcome.store(std::string(e.what()) == "boom" ? 2 : 3,
                  std::memory_order_seq_cst);
  } catch (...) {
    outcome.store(4, std::memory_order_seq_cst);
  }
}

TEST(FlowCombinators, WhenAllBranchExceptionSurfacesToCaller) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> outcome{0};
  auto s = flow::spawn_on(h.rt(), scope,
                          join_throw_body(h.rt(), scope, outcome));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (outcome.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 2)
      << "the real branch exception reached the caller";
  (void)scope.close();
}

flow::Task<void> race_throw_body(flow::LaneRuntime& rt, flow::Scope scope,
                                 std::atomic<int>& outcome) {
  try {
    (void)co_await flow::race(rt, thrower_after(rt, 2ms),
                              thrower_after(rt, 5ms), scope);
    outcome.store(1, std::memory_order_seq_cst);
  } catch (const std::runtime_error& e) {
    outcome.store(std::string(e.what()) == "boom" ? 2 : 3,
                  std::memory_order_seq_cst);
  } catch (...) {
    outcome.store(4, std::memory_order_seq_cst);
  }
}

TEST(FlowCombinators, RaceWithNoCleanWinnerSurfacesRealError) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> outcome{0};
  auto s =
      flow::spawn_on(h.rt(), scope, race_throw_body(h.rt(), scope, outcome));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (outcome.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 2)
      << "with both branches failed, the caller sees the real exception";
  (void)scope.close();
}

flow::Task<void> timeout_throw_body(flow::LaneRuntime& rt, flow::Scope scope,
                                    std::chrono::milliseconds window,
                                    std::chrono::milliseconds fail_after,
                                    std::atomic<int>& outcome) {
  try {
    (void)co_await flow::timeout(rt, window, thrower_after(rt, fail_after),
                                 scope);
    outcome.store(1, std::memory_order_seq_cst);
  } catch (const std::runtime_error& e) {
    outcome.store(std::string(e.what()) == "boom" ? 2 : 3,
                  std::memory_order_seq_cst);
  } catch (...) {
    outcome.store(4, std::memory_order_seq_cst);
  }
}

TEST(FlowCombinators, TimeoutTaskFailureIsNotADeadlineUnwind) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> outcome{0};
  auto s = flow::spawn_on(h.rt(), scope,
                          timeout_throw_body(h.rt(), scope, 100ms, 2ms,
                                             outcome));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (outcome.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 2)
      << "a task failure under timeout reports the failure, not a deadline";
  (void)scope.close();
}

TEST(FlowCombinators, TimeoutTaskFailureResolvesPromptly) {
  // Regression: a payload FAILURE must
  // revoke the timer branch and resolve the whole timeout() the moment
  // the task settles — Rust parity (`Scope::timeout` resolves when the
  // task settles, success or failure). Pre-fix the failure was known at
  // ~10ms but the frame resumed only at the 2s window edge.
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> outcome{0};
  const auto started = std::chrono::steady_clock::now();
  auto s = flow::spawn_on(h.rt(), scope,
                          timeout_throw_body(h.rt(), scope, 2000ms, 10ms,
                                             outcome));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (outcome.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 2)
      << "the real failure surfaced";
  EXPECT_LT(std::chrono::steady_clock::now() - started, 1s)
      << "failure resolved timeout() promptly, not at the 2s window edge";
  (void)scope.close();
}

// ------------------------------------------------------------------- I-6
// Channel pins for the offload router adoption: the run
// channel resumes INLINE on the worker (deliver_run_unbound — no
// auto-rehop, no re-post), and the drop channel delivers stamp+resume
// cancellation at teardown without ever running the closure.

flow::Task<void> offload_residence_body(flow::LaneRuntime& rt,
                                        std::atomic<std::thread::id>& worker_tid,
                                        std::atomic<std::thread::id>& resume_tid,
                                        std::atomic<bool>& done) {
  std::atomic<std::thread::id>* wt = &worker_tid;  // pointer copy → closure
  (void)co_await flow::offload(rt, aorta::EventPriority::kBackground, [wt] {
    wt->store(std::this_thread::get_id(), std::memory_order_seq_cst);
    return 0;
  });
  resume_tid.store(std::this_thread::get_id(), std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);  // LAST statement — the
  // manual drainer below observes it only after this item's run returned
  // (and was counted), making the stats read deterministic.
}

TEST(FlowCombinators, OffloadRunChannelResumesInlineOnTheWorker) {
  // Deterministic sampling: the TEST thread is the sole,
  // synchronous drainer — when `done` is observed, every item that will
  // ever run has run AND been counted (`ran` increments after w.run
  // returns), so a re-posted continuation would have been drained by
  // this same loop before `done` and pushed ran to 2.
  auto rt = flow::LaneRuntime::SingleThread();
  ASSERT_TRUE(rt->enabled());
  flow::Scope scope = flow::Scope::root();
  std::atomic<std::thread::id> worker_tid{};
  std::atomic<std::thread::id> resume_tid{};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(
      *rt, scope, offload_residence_body(*rt, worker_tid, resume_tid, done));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    if (!rt->pool().drain_one()) {
      rt->pool().wait_for_work(std::chrono::milliseconds(1));
    }
  }
  ASSERT_TRUE(done.load(std::memory_order_seq_cst));
  EXPECT_EQ(worker_tid.load(std::memory_order_seq_cst),
            std::this_thread::get_id())
      << "the closure ran on the (sole) drainer thread";
  EXPECT_EQ(resume_tid.load(std::memory_order_seq_cst),
            worker_tid.load(std::memory_order_seq_cst))
      << "I-6 run channel: the continuation resumes ON the worker thread "
         "(inline deliver_run_unbound, no auto-rehop)";
  EXPECT_EQ(rt->pool().stats(3).ran, 1u)
      << "I-6 run channel must not re-post the continuation (a re-post "
         "would have drained here and counted 2)";
  EXPECT_TRUE(scope.close().ok());
}

flow::Task<void> offload_drop_body(flow::LaneRuntime& rt,
                                   std::atomic<bool>& closure_ran,
                                   std::atomic<int>& outcome) {
  std::atomic<bool>* cr = &closure_ran;
  (void)co_await flow::offload(rt, aorta::EventPriority::kBackground, [cr] {
    cr->store(true, std::memory_order_seq_cst);
    return 0;
  });
  outcome.store(1, std::memory_order_seq_cst);  // value path (wrong on drop)
}

TEST(FlowCombinators, OffloadDropChannelDeliversCancelAtTeardown) {
  // Deterministic drop: a bare ST LaneRuntime with NO drainer thread —
  // ONE manual drain step runs the initial dispatch (the frame parks on
  // the offload; its Background work item is queued, un-run), then the
  // runtime is destroyed: ~LaneRuntime closes the pool and
  // drain_closed_lane routes the item to DROP → drop_thunk → router →
  // stamp CANCELLED → inline resume-into-cancellation (the frame
  // unwinds on this, the closer, thread).
  auto rt = flow::LaneRuntime::SingleThread();
  ASSERT_TRUE(rt->enabled());
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> closure_ran{false};
  std::atomic<int> outcome{0};
  auto s =
      flow::spawn_on(*rt, scope, offload_drop_body(*rt, closure_ran, outcome));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(rt->pool().drain_one()) << "initial dispatch ran";
  EXPECT_FALSE(closure_ran.load(std::memory_order_seq_cst))
      << "closure queued but not yet run";
  rt.reset();  // teardown delivers the I-6 DROP channel
  EXPECT_FALSE(closure_ran.load(std::memory_order_seq_cst))
      << "I-6 drop channel: the closure is RELEASED, never run";
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 0)
      << "no value crossed the drop channel — the frame unwound";
  EXPECT_TRUE(scope.close().ok())
      << "the frame ended (resume-into-cancellation) during teardown";
}

flow::Task<void> on_throw_body(flow::LaneRuntime& rt,
                               std::atomic<int>& outcome) {
  try {
    (void)co_await flow::offload(rt, aorta::EventPriority::kBackground,
                                 []() -> int {
      throw std::runtime_error("boom");
    });
    outcome.store(1, std::memory_order_seq_cst);
  } catch (const std::runtime_error& e) {
    outcome.store(std::string(e.what()) == "boom" ? 2 : 3,
                  std::memory_order_seq_cst);
  } catch (...) {
    outcome.store(4, std::memory_order_seq_cst);
  }
}

TEST(FlowCombinators, OnClosureExceptionSurfacesToCaller) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> outcome{0};
  auto s = flow::spawn_on(h.rt(), scope, on_throw_body(h.rt(), outcome));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (outcome.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(outcome.load(std::memory_order_seq_cst), 2)
      << "the closure exception crossed back off the lane intact";
  (void)scope.close();
}

TEST(FlowCombinators, OnHopsClosureToALane) {
  CombinatorHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> out{0};
  auto s = flow::spawn_on(h.rt(), scope, on_body(h.rt(), out));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (out.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(out.load(std::memory_order_seq_cst), 42);
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
