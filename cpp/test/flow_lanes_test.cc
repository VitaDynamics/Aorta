// Flow the C++ task layer on the LANE pool — the resume-into-cancellation
// stop-drain contract mirrored bit-for-bit from the executor path
// (flow_coroutine_test.cc is the reference; these are the lane twins).
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "aorta/flow.hpp"

namespace flow = aorta::flow;

namespace {

// Minimal ST drainer standing in for the facade: drains the pool
// on one dedicated thread until told to stop, then lets join() finish
// the drop-path residue.
class LaneHarness {
 public:
  LaneHarness() : rt_(flow::LaneRuntime::SingleThread()) {
    drainer_ = std::thread([this] {
      while (!stop_.load(std::memory_order_acquire)) {
        if (!rt_->pool().drain_one()) {
          rt_->pool().wait_for_work(std::chrono::milliseconds(1));
        }
      }
    });
  }

  ~LaneHarness() {
    rt_->pool().close();
    stop_.store(true, std::memory_order_release);
    drainer_.join();
    // Closer==drainer for what remains: drop-path residue.
    rt_->pool().drain_all();
  }

  flow::LaneRuntime& rt() { return *rt_; }

  void close_pool() { rt_->pool().close(); }

 private:
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::atomic<bool> stop_{false};
  std::thread drainer_;
};

flow::Task<void> lane_two_stage(flow::LaneRuntime& rt, std::atomic<int>& stage) {
  stage.store(1, std::memory_order_seq_cst);
  co_await flow::resume_on_lane{&rt};
  stage.store(2, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowLanes, SpawnedCoroutineRunsOnLanesAndCloseWaits) {
  LaneHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<int> stage{0};
  auto spawned = flow::spawn_on(h.rt(), scope, lane_two_stage(h.rt(), stage));
  ASSERT_TRUE(spawned.ok());
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (stage.load(std::memory_order_seq_cst) < 2 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(stage.load(std::memory_order_seq_cst), 2);
  ASSERT_TRUE(scope.close().ok()) << "accounting balanced";
}

struct UnwindSentinel {
  std::atomic<bool>* flag;
  ~UnwindSentinel() { flag->store(true, std::memory_order_seq_cst); }
};

flow::Task<void> lane_ping_pong_until_torn_down(flow::LaneRuntime& rt,
                                                std::atomic<bool>& unwound) {
  UnwindSentinel sentinel{&unwound};
  for (;;) {
    // Teardown on lanes: a pool-close drop delivery UNWINDS here — the
    // sentinel destructor is the proof (bit-for-bit with the executor
    // stop-drain test).
    co_await flow::resume_on_lane{&rt};
  }
}

flow::Task<void> lane_ping_pong_nothrow(flow::LaneRuntime& rt,
                                        std::atomic<bool>& cancelled) {
  for (;;) {
    if (co_await flow::resume_on_lane{&rt}.nothrow() != AORTA_OK) {
      cancelled.store(true, std::memory_order_seq_cst);
      co_return;
    }
  }
}

TEST(FlowLanes, PoolCloseResumesInFlightFramesIntoCancellation) {
  LaneHarness h;
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  std::atomic<bool> cancelled{false};
  auto a = flow::spawn_on(h.rt(), scope,
                          lane_ping_pong_until_torn_down(h.rt(), unwound));
  ASSERT_TRUE(a.ok());
  auto b = flow::spawn_on(h.rt(), scope,
                          lane_ping_pong_nothrow(h.rt(), cancelled));
  ASSERT_TRUE(b.ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(30));  // spinning
  h.close_pool();  // the stop: queued resumes now drain to DROP
  // No timeout crutch: a leaked frame would hang close() and trip the
  // bazel test timeout (same discipline as the executor twin).
  ASSERT_TRUE(scope.close().ok())
      << "every accepted resume was resumed into cancellation";
  EXPECT_TRUE(unwound.load(std::memory_order_seq_cst))
      << "unwind default: destructors ran during teardown";
  EXPECT_TRUE(cancelled.load(std::memory_order_seq_cst))
      << "nothrow opt-out observed the CANCELLED delivery";
}

flow::Task<void> lane_mark_ran(std::atomic<bool>& flag) {
  flag.store(true, std::memory_order_seq_cst);
  co_return;
}

TEST(FlowLanes, ClosedPoolPostIsRefusedAndResumesInline) {
  auto rt = flow::LaneRuntime::SingleThread();
  rt->pool().close();
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> ran_inline{false};
  auto spawned = flow::spawn_on(*rt, scope, lane_mark_ran(ran_inline));
  EXPECT_EQ(spawned.status, AORTA_ERR_CANCELLED);
  EXPECT_TRUE(ran_inline.load(std::memory_order_seq_cst))
      << "a refused post resumes inline (resume, never destroy)";
  ASSERT_TRUE(scope.close().ok());
  rt->pool().join();
}

}  // namespace
