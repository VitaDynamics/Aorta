// flow::Runtime: the one setup object (executor + LaneRuntime +
// FacadeAttachment + spin thread) whose destructor encodes the
// stop-then-destroy teardown order structurally.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <thread>

#include "aorta/flow_combinators.hpp"
#include "aorta/flow_runtime.hpp"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

template <typename Pred>
bool wait_until(Pred pred, std::chrono::seconds budget = 5s) {
  auto deadline = std::chrono::steady_clock::now() + budget;
  while (!pred() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}

flow::Task<int> golden(int x) {
  co_await flow::sleep(2ms);           // ambient arbiter timer
  auto [a, b] = co_await flow::join(   // ambient {rt, scope}
      [](int v) -> flow::Task<int> { co_return v; }(x),
      [](int v) -> flow::Task<int> { co_return v + 1; }(x));
  co_return a + b;
}

TEST(FlowRuntime, GoldenPathSpawnWaitTakeOutcomeClose) {
  auto rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(rt.ok());
  ASSERT_TRUE(rt->valid());
  flow::Scope scope = rt->root_scope();
  auto h = rt->spawn(scope, golden(20));
  ASSERT_TRUE(h.ok());
  // Wait on the HANDLE first, then close: closing first would bar the
  // still-running task's own combinator branch spawns (the scope's
  // closing bar is subtree-wide).
  ASSERT_TRUE(h.wait_for(5s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_TRUE(o->ok());
  EXPECT_EQ(**o, 41);
  ASSERT_TRUE(scope.close().ok());
}

// Shutdown family — Scope::shutdown_with_grace (Rust parity:
// `scope.shutdown_with_grace(reason, grace).await` -> detached count).
// Free function, NOT a capturing lambda: a capturing-lambda coroutine
// called as a temporary dangles its captures on first resume (the
// co_invoke trap, flow.hpp) — ASAN caught exactly that in the first
// version of this test.
flow::Task<void> grace_straggler(std::atomic<bool>* entered) {
  entered->store(true, std::memory_order_seq_cst);
  std::this_thread::sleep_for(500ms);
  co_return;
}

TEST(FlowRuntime, GraceShutdownCleanWhenChildrenCooperate) {
  auto rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(rt.ok());
  flow::Scope scope = rt->root_scope().child();
  auto h = rt->spawn(scope, []() -> flow::Task<int> { co_return 7; }());
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(5s));
  auto detached =
      scope.shutdown_with_grace(flow::CancelReason::kShutdown, 2s);
  ASSERT_TRUE(detached.ok());
  EXPECT_EQ(*detached, 0u);
}

TEST(FlowRuntime, GraceShutdownDetachesStragglersAndReportsTheCount) {
  auto rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(rt.ok());
  flow::Scope scope = rt->root_scope().child();
  // The straggler BLOCKS its lane thread after signalling entry — no
  // co_await, so no entry gate can refuse it and no cancel delivery can
  // shorten it. It deliberately outlives the 50ms grace window.
  std::atomic<bool> entered{false};
  auto h = rt->spawn(scope, grace_straggler(&entered));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(wait_until(
      [&] { return entered.load(std::memory_order_seq_cst); }));
  auto detached =
      scope.shutdown_with_grace(flow::CancelReason::kShutdown, 50ms);
  ASSERT_TRUE(detached.ok());
  EXPECT_EQ(*detached, 1u);
  // Let the straggler drain before the Runtime teardown asserts anything.
  ASSERT_TRUE(h.wait_for(5s));
}

TEST(FlowRuntime, InvalidScopeGraceShutdownRefuses) {
  // B8 fail-safe: an invalid/moved-from Scope refuses instead of passing
  // handle 0 to the bridge.
  flow::Scope invalid;
  auto detached =
      invalid.shutdown_with_grace(flow::CancelReason::kShutdown, 1ms);
  EXPECT_FALSE(detached.ok());
}

TEST(FlowRuntime, SpawnWithInjectsACoOwnedScope) {
  auto rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(rt.ok());
  flow::Scope scope = rt->root_scope();
  std::atomic<bool> saw_valid{false};
  auto h = rt->spawn_with(
      scope,
      [](flow::Scope s, std::atomic<bool>* saw) -> flow::Task<void> {
        saw->store(s.valid(), std::memory_order_seq_cst);
        co_return;
      },
      &saw_valid);
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(scope.close().ok());
  EXPECT_TRUE(saw_valid.load(std::memory_order_seq_cst));
}

// The acceptance shape: teardown is the DESTRUCTOR — a Runtime destroyed
// with frames still parked (a sleeper, a handle await) must resolve every
// one of them through the stop bridge, never hang, never leak. This is
// the stop-then-destroy contract exercised through the one object
// that encodes it.
flow::Task<void> park_forever(std::atomic<bool>& parked,
                              std::atomic<bool>& resolved) {
  struct Sentinel {
    std::atomic<bool>* flag;
    ~Sentinel() { flag->store(true, std::memory_order_seq_cst); }
  } sentinel{&resolved};
  parked.store(true, std::memory_order_seq_cst);
  co_await flow::sleep(60s);  // teardown must revoke this, not wait it out
}

TEST(FlowRuntime, DestructorResolvesParkedFramesStructurally) {
  std::atomic<bool> parked{false};
  std::atomic<bool> resolved{false};
  flow::Scope scope = flow::Scope::root();
  {
    auto rt = flow::Runtime::SingleThread();
    ASSERT_TRUE(rt.ok());
    auto h = rt->spawn(scope, park_forever(parked, resolved));
    ASSERT_TRUE(h.ok());
    ASSERT_TRUE(
        wait_until([&] { return parked.load(std::memory_order_seq_cst); }));
    // No scope.close(), no explicit stop: the Runtime destructor alone
    // must run the ordered teardown (stop → drain → detach → destroy).
  }
  EXPECT_TRUE(resolved.load(std::memory_order_seq_cst))
      << "the parked frame must resolve at Runtime teardown";
  // The frame unwound and balanced its scope slot — close() returns.
  ASSERT_TRUE(scope.close().ok());
}

TEST(FlowRuntime, MoveTransfersOwnershipAndInvalidSpawnRefuses) {
  auto rt = flow::Runtime::SingleThread();
  ASSERT_TRUE(rt.ok());
  flow::Runtime owner = std::move(*rt);
  ASSERT_TRUE(owner.valid());
  EXPECT_FALSE(rt->valid());  // moved-from shell
  EXPECT_EQ(rt->executor(), 0u);

  flow::Scope scope = owner.root_scope();
  std::atomic<bool> ran{false};
  auto h = owner.spawn(scope, [](std::atomic<bool>* flag) -> flow::Task<void> {
    flag->store(true, std::memory_order_seq_cst);
    co_return;
  }(&ran));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(scope.close().ok());
  EXPECT_TRUE(ran.load(std::memory_order_seq_cst));

  // The moved-from shell refuses spawns with a handle that RESOLVES:
  // the refusal is a consumable outcome, not
  // an empty shell that waits forever.
  auto refused = rt->spawn(scope, [](void) -> flow::Task<void> { co_return; }());
  EXPECT_FALSE(refused.ok());
  EXPECT_TRUE(refused.wait_for(0ms)) << "pre-delivered — never parks";
  auto ro = refused.take_outcome();
  ASSERT_TRUE(ro.has_value());
  EXPECT_FALSE(ro->ok());
}

TEST(FlowRuntime, MultiThreadPairsAnMtExecutorAndAttaches) {
  // The core refuses ST-executor + MT-pool
  // pairings — MultiThread must create an MT executor, and a failed
  // attach must fail creation instead of silently running the queue path.
  auto rt = flow::Runtime::MultiThread({0, 0, 1, 0});
  ASSERT_TRUE(rt.ok());
  EXPECT_TRUE(rt->lanes_attached());
  flow::Scope scope = rt->root_scope();
  auto h = rt->spawn(scope, golden(3));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(5s));
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_TRUE(o->ok());
  EXPECT_EQ(**o, 7);
  ASSERT_TRUE(scope.close().ok());
}

TEST(FlowRuntime, RollbackViaEnvStillRunsEveryVerb) {
  ::setenv("AORTA_CPP_LANES", "0", 1);
  auto rt = flow::Runtime::FromEnv();
  ::unsetenv("AORTA_CPP_LANES");
  ASSERT_TRUE(rt.ok());
  EXPECT_FALSE(rt->lanes_attached()) << "rollback: queue path, no facade";
  flow::Scope scope = rt->root_scope();
  // Disabled runtime is a VALID plane: the ambient sleep
  // rides the arbiter with the fallback-executor resume routing.
  auto h = rt->spawn(scope, golden(1));
  ASSERT_TRUE(h.ok());
  ASSERT_TRUE(h.wait_for(5s));  // wait, THEN close (closing bars spawns)
  auto o = h.take_outcome();
  ASSERT_TRUE(o.has_value());
  ASSERT_TRUE(o->ok());
  EXPECT_EQ(**o, 3);
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
