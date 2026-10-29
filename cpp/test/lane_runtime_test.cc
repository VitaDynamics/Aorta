// LaneRuntime tests.
#include "aorta/lane_runtime.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_set>
#include <vector>

using aorta::flow::lane_class_for;
using aorta::flow::lane_class_for_u8;
using aorta::flow::LaneRuntime;
using aorta::lanes::Post;
using aorta::lanes::Work;

namespace {

// Cross-language drift guard: pins the full 5-priority → 4-lane-class fold
// against the authoritative priority-mapping table.
// The Rust twin (emit/forward path) is
// `lane_class_for_priority` in core/src/event_queue.rs, pinned by
// `priority_index_and_lane_class_tables_pinned` in
// core/test/event_queue_tests.rs. The two implementations must agree
// forever; change one only together with the other AND the spec table.
TEST(LaneRuntime, ClassMappingMatchesSpecPriorityTable) {
  EXPECT_EQ(lane_class_for(aorta::EventPriority::kRealtime), 0u);
  EXPECT_EQ(lane_class_for(aorta::EventPriority::kHigh), 1u);
  EXPECT_EQ(lane_class_for(aorta::EventPriority::kNormal), 2u);
  EXPECT_EQ(lane_class_for(aorta::EventPriority::kLow), 3u);
  EXPECT_EQ(lane_class_for(aorta::EventPriority::kBackground), 3u);
}

// The enum DISCRIMINANTS are themselves contract: they are the C-ABI
// priority byte / Rust `priority_index`. The two mapping
// tests above cannot catch a shifted discriminant (name→class and
// byte→class would both still pass), so pin each value directly.
TEST(LaneRuntime, EventPriorityBytesMatchTheSpecTable) {
  EXPECT_EQ(static_cast<uint8_t>(aorta::EventPriority::kRealtime), 0);
  EXPECT_EQ(static_cast<uint8_t>(aorta::EventPriority::kHigh), 1);
  EXPECT_EQ(static_cast<uint8_t>(aorta::EventPriority::kNormal), 2);
  EXPECT_EQ(static_cast<uint8_t>(aorta::EventPriority::kLow), 3);
  EXPECT_EQ(static_cast<uint8_t>(aorta::EventPriority::kBackground), 4);
}

// Same drift guard for the wire/ABI byte form: priority indices 0..=3 are
// the class ids; 4 (Background's index) folds into class 3 with Low, and
// any other out-of-range byte clamps defensively to 3 (the flow.hpp
// promise resolves the 255 inherit sentinel before posting, so no
// non-concrete byte reaches the pool in practice).
TEST(LaneRuntime, ClassByteMappingMatchesSpecPriorityTable) {
  EXPECT_EQ(lane_class_for_u8(0), 0u);
  EXPECT_EQ(lane_class_for_u8(1), 1u);
  EXPECT_EQ(lane_class_for_u8(2), 2u);
  EXPECT_EQ(lane_class_for_u8(3), 3u);
  EXPECT_EQ(lane_class_for_u8(4), 3u);  // Background's index folds into class 3
  EXPECT_EQ(lane_class_for_u8(5), 3u);
  EXPECT_EQ(lane_class_for_u8(254), 3u);
  EXPECT_EQ(lane_class_for_u8(255), 3u);
}

// Same drift guard for the DEFAULT WRR weight vector (previously
// the class mapping was pinned on both sides, the weights only on the
// Rust side). The 4-lane {8,4,2,1} is the deliberate
// fold of the executor's 5-class DEFAULT_PRIORITY_WEIGHTS = [8,4,2,1,1]
// (core/src/executor.rs, pinned by the Rust table test) — Low and
// Background share lane 3, so the tail weights collapse. Change one only
// together with the other AND the spec table.
TEST(LaneRuntime, DefaultWrrWeightsMatchTheSpecTable) {
  aorta::lanes::PoolConfig cfg;
  EXPECT_EQ(cfg.st_wrr_weights, (std::vector<uint32_t>{8, 4, 2, 1}));
}

TEST(LaneRuntime, SingleThreadDefaultPostsAndDrains) {
  auto rt = LaneRuntime::SingleThread();
  ASSERT_TRUE(rt->enabled());
  std::atomic<int> ran{0};
  Work w;
  w.run = [](void* ctx) { static_cast<std::atomic<int>*>(ctx)->fetch_add(1); };
  w.ctx = &ran;
  ASSERT_EQ(rt->post_resume(aorta::EventPriority::kNormal, w), Post::OK);
  ASSERT_EQ(rt->post_work(aorta::EventPriority::kBackground, w), Post::OK);
  EXPECT_EQ(rt->pool().drain_all(), 2u);
  EXPECT_EQ(ran.load(), 2);
}

TEST(LaneRuntime, DisabledModeHasNoPool) {
  auto rt = LaneRuntime::Disabled();
  EXPECT_FALSE(rt->enabled());
}

TEST(LaneRuntime, FromEnvHonorsRollback) {
  setenv("AORTA_CPP_LANES", "0", 1);
  EXPECT_FALSE(LaneRuntime::FromEnv()->enabled());
  unsetenv("AORTA_CPP_LANES");
  EXPECT_TRUE(LaneRuntime::FromEnv()->enabled());
}

TEST(LaneRuntime, EveryConstructorHonorsEnvRollback) {
  // AORTA_CPP_LANES=0 is billed as PROCESS-LEVEL rollback —
  // it must gate SingleThread()/MultiThread() too, not just callers who
  // happened to construct through FromEnv().
  setenv("AORTA_CPP_LANES", "0", 1);
  EXPECT_FALSE(LaneRuntime::SingleThread()->enabled());
  EXPECT_FALSE(LaneRuntime::MultiThread({0, 0, 2, 0})->enabled());
  unsetenv("AORTA_CPP_LANES");
  EXPECT_TRUE(LaneRuntime::SingleThread()->enabled());
  auto mt = LaneRuntime::MultiThread({0, 0, 2, 0});
  EXPECT_TRUE(mt->enabled());
  EXPECT_EQ(mt->pool_workers(), 2u);
}

TEST(LaneRuntime, ArmRegistrySurvivesConcurrentMutationAndSweep) {
  // The registry's REAL usage is cross-thread: awaiters register/
  // deregister from lane workers while the stop bridge's revoke sweep
  // drains from the stopping thread (this file
  // previously exercised it single-threaded only). Invariant under the
  // race: every handle a registrar left LIVE is drained by exactly one
  // sweep — none lost, none duplicated, no crash/TSan report.
  auto rt = LaneRuntime::SingleThread();
  constexpr int kThreads = 4;
  constexpr uint64_t kPerThread = 500;
  std::atomic<uint64_t> live_left{0};
  std::vector<std::thread> registrars;
  registrars.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    registrars.emplace_back([&rt, &live_left, t] {
      for (uint64_t i = 0; i < kPerThread; ++i) {
        const uint64_t handle = 1 + t * kPerThread + i;
        rt->register_arm(handle);
        if (i % 2 == 0) {
          rt->deregister_arm(handle);  // resolved before any sweep
        } else {
          live_left.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  // Concurrent sweeps while registrars run (the stop-bridge shape).
  // NOTE the race semantics: a sweep may legitimately claim a handle the
  // registrar was ABOUT to deregister (production absorbs that via the
  // arbiter's CAS — revoking a resolved handle no-ops). The registry's
  // own contract is therefore: at-most-once per handle across all
  // sweeps, and every handle left LIVE is swept eventually.
  std::atomic<bool> stop_sweeping{false};
  std::vector<uint64_t> swept;  // single-sweeper-owned
  std::thread sweeper([&rt, &stop_sweeping, &swept] {
    while (!stop_sweeping.load(std::memory_order_acquire)) {
      for (uint64_t h : rt->drain_live_arms()) swept.push_back(h);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
  for (auto& th : registrars) th.join();
  stop_sweeping.store(true, std::memory_order_release);
  sweeper.join();
  for (uint64_t h : rt->drain_live_arms()) swept.push_back(h);

  std::unordered_set<uint64_t> unique(swept.begin(), swept.end());
  EXPECT_EQ(unique.size(), swept.size())
      << "no handle is drained twice across concurrent sweeps";
  uint64_t live_seen = 0;
  for (int t = 0; t < kThreads; ++t) {
    for (uint64_t i = 1; i < kPerThread; i += 2) {  // the never-deregistered
      const uint64_t handle = 1 + t * kPerThread + i;
      live_seen += unique.count(handle);
    }
  }
  EXPECT_EQ(live_seen, live_left.load())
      << "every live registration reaches exactly one sweep";
  EXPECT_TRUE(rt->drain_live_arms().empty());
}

TEST(LaneRuntime, ArmRegistrySnapshotAndTombstones) {
  auto rt = LaneRuntime::SingleThread();
  rt->register_arm(11);
  rt->register_arm(22);
  rt->register_arm(33);
  rt->deregister_arm(22);  // resolved before any stop
  auto live = rt->drain_live_arms();
  ASSERT_EQ(live.size(), 2u);
  EXPECT_EQ(live[0], 11u);
  EXPECT_EQ(live[1], 33u);
  EXPECT_TRUE(rt->drain_live_arms().empty()) << "drain clears";
}

TEST(LaneRuntime, ResumePostBudgetExemptThroughRuntime) {
  auto rt = LaneRuntime::SingleThread();
  // Fill the normal lane's budget with plain work…
  std::atomic<int> ran{0};
  std::atomic<int> dropped{0};
  auto mk = [&](bool) {
    Work w;
    w.run = [](void* ctx) { static_cast<std::atomic<int>*>(ctx)->fetch_add(1); };
    w.drop = [](void* ctx) { static_cast<std::atomic<int>*>(ctx)->fetch_add(1); };
    w.ctx = &ran;
    return w;
  };
  auto& pool = rt->pool();
  // Use a tiny dedicated pool to exercise the budget instead of 4096.
  aorta::lanes::PoolConfig cfg;
  aorta::lanes::LaneConfig lane;
  lane.max_queued_events = 1;
  cfg.lanes.push_back(lane);
  aorta::lanes::LanePool tiny(cfg);
  Work drop_probe = mk(true);
  drop_probe.drop = [](void* ctx) {
    static_cast<std::atomic<int>*>(ctx)->fetch_add(1);
  };
  drop_probe.ctx = &dropped;
  ASSERT_EQ(tiny.post(0, mk(true)), Post::OK);
  EXPECT_EQ(tiny.post(0, drop_probe), Post::SHED_FULL);
  EXPECT_EQ(dropped.load(), 1);
  EXPECT_EQ(tiny.post_resume_item(0, mk(true)), Post::OK) << "exempt";
  tiny.drain_all();
  (void)pool;
}

// Residue drops inside callback context (the REAL path): a facade-less
// ~LaneRuntime delivers residue drops itself (as the closer) — those
// drops must run inside the REAL aorta callback-context marks (the
// LaneRuntime pool config wires ctx hooks to the C-ABI thunks). Probe:
// aorta_callback_context_enter() returns the PREVIOUS depth token —
// >= 1 inside a mark.
TEST(LaneRuntime, FacadeLessTeardownResidueCarriesRealMarks) {
  auto rt = aorta::flow::LaneRuntime::SingleThread();
  ASSERT_TRUE(rt->enabled());
  static std::atomic<int> observed_prev{-1};
  observed_prev.store(-1);
  aorta::lanes::Work w{};
  w.drop = [](void*) {
    const uint8_t prev = aorta_callback_context_enter();
    observed_prev.store(static_cast<int>(prev));
    (void)aorta_callback_context_exit(prev);
  };
  rt->pool().post_residue(w);
  rt.reset();  // ~LaneRuntime: close → sweep → quiesce → residue drain
  EXPECT_GE(observed_prev.load(), 1)
      << "the facade-less closer's residue delivery must carry "
         "the callback-context mark";
}

}  // namespace
