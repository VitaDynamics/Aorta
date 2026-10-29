// Flow: WindowSync / ReadinessSet C++ surface — RAII round trips,
// subscriber binds, the co_await aligned()/wave() bridge (park, fast
// path, stop, destroy-wake, F5 single-awaiter), and the Watch ring sugar.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "aorta/flow_readiness.hpp"
#include "aorta/flow_watch.hpp"
#include "aorta/node.h"
#include "aorta/publisher.h"
#include "aorta/pull_subscriber.h"
#include "aorta/subscriber.h"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

// ---------------------------------------------------------------------
// RAII round trips — no lane runtime needed.
// ---------------------------------------------------------------------

TEST(FlowReadiness, WindowSyncRoundTrip) {
  auto ws = flow::WindowSync::create(2, 1s);
  ASSERT_TRUE(ws.ok());
  ASSERT_TRUE(ws->valid());
  EXPECT_FALSE(flow::WindowSync::create(0, 1ms).ok())
      << "zero inputs refused at create";

  const uint8_t a[] = {1, 2};
  const uint8_t b[] = {3};
  EXPECT_EQ(ws->feed(2, 1, a, 2), AORTA_ERR_INVALID_ARGUMENT)
      << "input 2 of a 2-input window";
  EXPECT_FALSE(ws->try_aligned().has_value());
  EXPECT_EQ(ws->feed(0, 100, a, 2), AORTA_OK);
  EXPECT_EQ(ws->feed(1, 150, b, 1), AORTA_OK);

  auto set = ws->try_aligned();
  ASSERT_TRUE(set.has_value());
  EXPECT_EQ(set->size(), 2u);
  auto s0 = set->at(0);
  ASSERT_NE(s0.data, nullptr);
  EXPECT_EQ(s0.ts_ns, 100u);
  ASSERT_EQ(s0.len, 2u);
  EXPECT_EQ(s0.data[1], 2);
  EXPECT_EQ(set->at(9).data, nullptr) << "out-of-range view is empty";

  const auto stats = ws->stats();
  EXPECT_EQ(stats.emitted, 1u);
  EXPECT_EQ(stats.dropped_sets, 0u);

  // Move semantics: the moved-from window is inert.
  flow::WindowSync moved = std::move(*ws);
  EXPECT_TRUE(moved.valid());
  EXPECT_FALSE(ws->valid());
}

TEST(FlowReadiness, ReadinessSetRoundTrip) {
  auto set = flow::ReadinessSet::create(2);
  ASSERT_TRUE(set.ok());
  EXPECT_FALSE(flow::ReadinessSet::create(65).ok()) << "1..=64 bound";

  EXPECT_EQ(set->satisfy(2), AORTA_ERR_INVALID_ARGUMENT)
      << "condition 2 of a 2-condition set";
  EXPECT_EQ(set->satisfy(0), AORTA_OK);
  EXPECT_EQ(set->pending_mask(), 0b10u);
  EXPECT_EQ(set->waves(), 0u);
  EXPECT_EQ(set->satisfy(1), AORTA_OK);
  EXPECT_EQ(set->waves(), 1u);
  EXPECT_EQ(set->pending_mask(), 0b11u) << "mask reset for the next wave";
}

// ---------------------------------------------------------------------
// Subscriber binds + Watch — a node, no lane runtime.
// ---------------------------------------------------------------------

TEST(FlowReadiness, BindsWireSubscribersToWindowAndSet) {
  auto node = aorta::Node::Create("flow_rdy_bind_cpp");
  ASSERT_TRUE(node.ok()) << node.status().message();

  auto pull = node.value()->CreatePullSubscriber("test/rdy_bind_cpp", 4);
  ASSERT_TRUE(pull.ok()) << pull.status().message();
  auto cb_sub = node.value()->CreateSubscriber(
      "test/rdy_bind_cpp_cb",
      [](const aorta::MessageView&, const aorta::MessageContextView&) {});
  ASSERT_TRUE(cb_sub.ok()) << cb_sub.status().message();

  auto ws = flow::WindowSync::create(1, 1s);
  ASSERT_TRUE(ws.ok());
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());

  // Foolproofing at the bind boundary.
  EXPECT_EQ(flow::bind_window(pull.value(), *ws, 1),
            AORTA_ERR_INVALID_ARGUMENT);
  EXPECT_EQ(flow::bind_readiness(*cb_sub.value(), *set, 1),
            AORTA_ERR_INVALID_ARGUMENT);

  ASSERT_EQ(flow::bind_window(pull.value(), *ws, 0), AORTA_OK);
  ASSERT_EQ(flow::bind_readiness(*cb_sub.value(), *set, 0), AORTA_OK);

  auto pub = node.value()->CreatePublisher("test/rdy_bind_cpp");
  ASSERT_TRUE(pub.ok());
  auto pub_cb = node.value()->CreatePublisher("test/rdy_bind_cpp_cb");
  ASSERT_TRUE(pub_cb.ok());

  const uint8_t payload[] = {0x5A};
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  std::optional<flow::AlignedSet> aligned;
  while (std::chrono::steady_clock::now() < deadline) {
    ASSERT_TRUE(pub.value()->PublishBytes(payload, 1).ok());
    ASSERT_TRUE(pub_cb.value()->PublishBytes(payload, 1).ok());
    aligned = ws->try_aligned();
    if (aligned.has_value() && set->waves() >= 1) {
      break;
    }
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_TRUE(aligned.has_value()) << "bound pull subscriber feeds the window";
  auto s0 = aligned->at(0);
  ASSERT_EQ(s0.len, 1u);
  EXPECT_EQ(s0.data[0], 0x5A);
  EXPECT_GT(s0.ts_ns, 0u) << "receive stamp populated";
  EXPECT_GE(set->waves(), 1u) << "bound callback subscriber satisfies the set";
}

TEST(FlowReadiness, WatchRoundTrip) {
  auto node = aorta::Node::Create("flow_watch_cpp");
  ASSERT_TRUE(node.ok()) << node.status().message();

  auto watch = flow::Watch::create(node.value()->handle(), "test/watch_cpp");
  ASSERT_TRUE(watch.ok());
  EXPECT_FALSE(watch->load().has_value()) << "empty until the first store";

  // Foreign-producer store (no zenoh) — tiny size_hint exercises the
  // BUFFER_TOO_SMALL retry.
  std::vector<uint8_t> big(1024, 7);
  ASSERT_EQ(watch->store(big.data(), big.size()), AORTA_OK);
  auto v = watch->load(/*size_hint=*/4);
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->payload.size(), 1024u);
  EXPECT_EQ(v->payload[1023], 7);
  EXPECT_EQ(v->seq, 1u);

  // RX path: a publish lands as the latest value.
  auto pub = node.value()->CreatePublisher("test/watch_cpp");
  ASSERT_TRUE(pub.ok());
  const uint8_t payload[] = {9, 9};
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  for (;;) {
    ASSERT_TRUE(pub.value()->PublishBytes(payload, 2).ok());
    auto latest = watch->load();
    if (latest.has_value() && latest->payload.size() == 2) {
      EXPECT_EQ(latest->payload[0], 9);
      break;
    }
    ASSERT_LT(std::chrono::steady_clock::now(), deadline)
        << "published value must reach the watch";
    std::this_thread::sleep_for(20ms);
  }
  EXPECT_GE(watch->stored_total(), 2u);
}

// ---------------------------------------------------------------------
// The co_await bridge — lane runtime + facade harness (NextHarness shape).
// ---------------------------------------------------------------------

class ReadinessHarness : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec_), AORTA_OK);
    rt_ = flow::LaneRuntime::SingleThread();
    attachment_ = std::make_unique<flow::FacadeAttachment>(*rt_, exec_);
    ASSERT_TRUE(attachment_->attached());
    spin_ = std::thread([this] { aorta_executor_spin(exec_); });
  }

  void TearDown() override {
    (void)aorta_executor_stop(exec_);
    if (spin_.joinable()) spin_.join();
    attachment_.reset();
    ASSERT_EQ(aorta_executor_free(exec_), AORTA_OK);
  }

  uint64_t exec_ = 0;
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::unique_ptr<flow::FacadeAttachment> attachment_;
  std::thread spin_;
};

// The explicit tier is scope-full only (the scope-less overloads were
// removed): every helper below arms the test's own scope alongside the
// window/set branch. The scope stays un-cancelled in the park/wake/
// fast-path/destroy/stop tests, so the pinned behavior is unchanged.
flow::Task<void> consume_aligned_once(flow::LaneRuntime& rt,
                                      flow::WindowSync& ws, flow::Scope scope,
                                      std::atomic<uint64_t>& got_ts,
                                      std::atomic<bool>& done) {
  co_await flow::resume_on_lane{&rt};
  flow::AlignedSet set = co_await flow::aligned(ws, rt, scope);
  got_ts.store(set.at(0).ts_ns, std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

flow::Task<void> await_one_wave(flow::LaneRuntime& rt, flow::ReadinessSet& set,
                                flow::Scope scope,
                                std::atomic<uint64_t>& got_wave,
                                std::atomic<bool>& done) {
  co_await flow::resume_on_lane{&rt};
  const uint64_t wave = co_await flow::wait_wave(set, rt, scope);
  got_wave.store(wave, std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

TEST_F(ReadinessHarness, AlignedDeliversAParkedSet) {
  auto ws = flow::WindowSync::create(2, 1s);
  ASSERT_TRUE(ws.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<uint64_t> got_ts{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          consume_aligned_once(*rt_, *ws, scope, got_ts, done));
  ASSERT_TRUE(s.ok());
  // Fixed-sleep "parked" cadence: the sleep only biases
  // toward the parked-first ordering — every test in this file also
  // passes on the lost race (an arm against an already-fired/freed
  // target self-resolves; assertions are order-free per the F5 sibling).
  std::this_thread::sleep_for(100ms);  // frame parks on aligned()

  const uint8_t a[] = {1};
  ASSERT_EQ(ws->feed(0, 500, a, 1), AORTA_OK);
  ASSERT_EQ(ws->feed(1, 510, a, 1), AORTA_OK);

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load()) << "parked aligned() woke on emission";
  EXPECT_EQ(got_ts.load(), 500u);
  ASSERT_TRUE(scope.close().ok());
}

// Pin: the deadline is a SECOND branch of the SAME arbiter group —
// nothing fed, the window unwinds the wait instead of parking forever.
// (The un-cancelled scope branch is a bystander; the deadline resolves.)
flow::Task<void> aligned_with_deadline(flow::LaneRuntime& rt,
                                       flow::WindowSync& ws, flow::Scope scope,
                                       std::atomic<bool>& done) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::aligned_for(ws, rt, std::chrono::milliseconds(80),
                                     scope);
  } catch (const flow::cancelled_unwind&) {
    done.store(true, std::memory_order_seq_cst);
    co_return;
  }
#endif
  co_return;
}

TEST_F(ReadinessHarness, AlignedForDeadlineFires) {
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          aligned_with_deadline(*rt_, *ws, scope, done));
  ASSERT_TRUE(s.ok());
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(done.load()) << "deadline resolved the parked aligned_for()";
  ASSERT_TRUE(scope.close().ok());
}

// Pin: scope.cancel() resolves a parked aligned(ws, rt, scope).
flow::Task<void> aligned_scoped_forever(flow::LaneRuntime& rt,
                                        flow::WindowSync& ws,
                                        flow::Scope scope,
                                        std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    for (;;) {
      (void)co_await flow::aligned(ws, rt, scope);
    }
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    throw;
  }
#else
  co_return;
#endif
}

TEST_F(ReadinessHarness, ScopeCancelResolvesAParkedAligned) {
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());
  flow::Scope root = flow::Scope::root();
  flow::Scope watch = root.child({.name = "aligned-watch"});
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, root,
                          aligned_scoped_forever(*rt_, *ws, watch, unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked, nothing fed

  watch.cancel(flow::CancelReason::kExplicit);
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  EXPECT_TRUE(unwound.load()) << "scope cancel resolved the parked aligned()";
#endif
  ASSERT_TRUE(watch.close().ok());
  ASSERT_TRUE(root.close().ok());
}

// Pin: wait_wave_for's deadline branch fires with no satisfy().
flow::Task<void> wave_with_deadline(flow::LaneRuntime& rt,
                                    flow::ReadinessSet& set, flow::Scope scope,
                                    std::atomic<bool>& done) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::wait_wave_for(set, rt, std::chrono::milliseconds(80),
                                       scope);
  } catch (const flow::cancelled_unwind&) {
    done.store(true, std::memory_order_seq_cst);
    co_return;
  }
#endif
  co_return;
}

TEST_F(ReadinessHarness, WaitWaveForDeadlineFires) {
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          wave_with_deadline(*rt_, *set, scope, done));
  ASSERT_TRUE(s.ok());
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(done.load()) << "deadline resolved the parked wait_wave_for()";
  ASSERT_TRUE(scope.close().ok());
}

// Pin: scope.cancel() resolves a parked wait_wave(set, rt, scope).
flow::Task<void> wave_scoped_forever(flow::LaneRuntime& rt,
                                     flow::ReadinessSet& set,
                                     flow::Scope scope,
                                     std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    for (;;) {
      (void)co_await flow::wait_wave(set, rt, scope);
    }
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    throw;
  }
#else
  co_return;
#endif
}

TEST_F(ReadinessHarness, ScopeCancelResolvesAParkedWaitWave) {
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());
  flow::Scope root = flow::Scope::root();
  flow::Scope watch = root.child({.name = "wave-watch"});
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, root,
                          wave_scoped_forever(*rt_, *set, watch, unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked, no satisfy

  watch.cancel(flow::CancelReason::kExplicit);
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  EXPECT_TRUE(unwound.load())
      << "scope cancel resolved the parked wait_wave()";
#endif
  ASSERT_TRUE(watch.close().ok());
  ASSERT_TRUE(root.close().ok());
}

TEST_F(ReadinessHarness, AlignedFastPathSetAlreadyQueued) {
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());
  const uint8_t a[] = {1};
  ASSERT_EQ(ws->feed(0, 42, a, 1), AORTA_OK);  // emits BEFORE the await

  flow::Scope scope = flow::Scope::root();
  std::atomic<uint64_t> got_ts{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          consume_aligned_once(*rt_, *ws, scope, got_ts, done));
  ASSERT_TRUE(s.ok());
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load()) << "fast path returns without a notify";
  EXPECT_EQ(got_ts.load(), 42u);
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(ReadinessHarness, StopResolvesAParkedAlignedAsStopped) {
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, scope,
                          aligned_scoped_forever(*rt_, *ws, scope, unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked, nothing fed

  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok()) << "sweep resolved the parked aligned()";
#if defined(__cpp_exceptions)
  EXPECT_TRUE(unwound.load());
#endif
}

TEST_F(ReadinessHarness, DestroyWakeUnwindsAParkedAligned) {
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, scope,
                          aligned_scoped_forever(*rt_, *ws, scope, unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked

  ws->reset();  // destroy-wake: the freed window fires the parked hook

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  EXPECT_TRUE(unwound.load()) << "stale window unwinds the awaiter";
#endif
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(ReadinessHarness, SecondAlignedAwaiterIsRefused) {
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound_a{false};
  std::atomic<bool> unwound_b{false};
  // Order-free F5 contract: whichever awaiter arms SECOND is refused and
  // unwinds; the winner keeps waiting. (A fixed "A parks first" sleep was
  // a race under sanitizer CI.)
  auto a = flow::spawn_on(*rt_, scope,
                          aligned_scoped_forever(*rt_, *ws, scope, unwound_a));
  ASSERT_TRUE(a.ok());
  auto b = flow::spawn_on(*rt_, scope,
                          aligned_scoped_forever(*rt_, *ws, scope, unwound_b));
  ASSERT_TRUE(b.ok());

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound_a.load() && !unwound_b.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  std::this_thread::sleep_for(50ms);  // settle: the winner must NOT unwind
  EXPECT_NE(unwound_a.load(), unwound_b.load())
      << "F5: exactly one awaiter is refused; the hook owner keeps waiting";
#endif
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok());
}

// Awaiter whose AUX branch fails to arm: the stale scope handle makes
// `aorta_scope_arm_arbiter` return NOT_FOUND after branch 0 armed, so
// the awaiter early-returns BEFORE `aorta_window_sync_set_notify` ran.
flow::Task<void> aligned_stale_scope_arm(flow::LaneRuntime& rt,
                                         flow::WindowSync& ws,
                                         std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::detail::aligned_loop(ws.native_handle(), &rt,
                                              /*window_ns=*/0,
                                              /*scope_handle=*/0xdead'beef);
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    co_return;
  }
#else
  co_return;
#endif
}

TEST_F(ReadinessHarness, AuxArmFailureDoesNotClobberParkedAlignedHook) {
  // Regression: a second awaiter whose
  // aux-branch arm FAILS never installed the window's notify hook, so
  // its resume must NOT clear the hook — that hook belongs to the
  // first, legitimately-parked awaiter. (SecondAlignedAwaiterIsRefused
  // covers only the non-aux collision, where set_notify itself refuses.)
  auto ws = flow::WindowSync::create(1, 1ms);
  ASSERT_TRUE(ws.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<uint64_t> got_ts{0};
  std::atomic<bool> done{false};
  auto a = flow::spawn_on(*rt_, scope,
                          consume_aligned_once(*rt_, *ws, scope, got_ts, done));
  ASSERT_TRUE(a.ok());
  std::this_thread::sleep_for(100ms);  // A parks; its hook is installed

  std::atomic<bool> refused{false};
  auto b = flow::spawn_on(*rt_, scope,
                          aligned_stale_scope_arm(*rt_, *ws, refused));
  ASSERT_TRUE(b.ok());
#if defined(__cpp_exceptions)
  const auto refuse_deadline = std::chrono::steady_clock::now() + 5s;
  while (!refused.load() &&
         std::chrono::steady_clock::now() < refuse_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(refused.load())
      << "the aux-arm failure resolved the second awaiter promptly";
#endif

  const uint8_t p[] = {7};
  ASSERT_EQ(ws->feed(0, 700, p, 1), AORTA_OK);
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(done.load())
      << "the parked awaiter's hook survived the aux-arm failure";
  if (done.load()) {
    EXPECT_EQ(got_ts.load(), 700u);
  }
  // Stop first: on a regression the victim is parked forever and a bare
  // scope.close() would hang; the stop sweep resolves it either way.
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(ReadinessHarness, DestroyWakeUnwindsAParkedWave) {
  // wave() documents the same destroy-wake contract as
  // aligned() but had no test.
  auto set = flow::ReadinessSet::create(2);
  ASSERT_TRUE(set.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, scope,
                          wave_scoped_forever(*rt_, *set, scope, unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked

  set->reset();  // destroy-wake: the freed set fires the parked hook

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  EXPECT_TRUE(unwound.load()) << "stale set unwinds the awaiter";
#endif
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(ReadinessHarness, SecondWaveAwaiterIsRefused) {
  // The F5 single-awaiter contract, wave() leg —
  // order-free (whichever arms second is refused; the owner keeps
  // waiting), same shape as SecondAlignedAwaiterIsRefused.
  auto set = flow::ReadinessSet::create(2);
  ASSERT_TRUE(set.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound_a{false};
  std::atomic<bool> unwound_b{false};
  auto a = flow::spawn_on(*rt_, scope,
                          wave_scoped_forever(*rt_, *set, scope, unwound_a));
  ASSERT_TRUE(a.ok());
  auto b = flow::spawn_on(*rt_, scope,
                          wave_scoped_forever(*rt_, *set, scope, unwound_b));
  ASSERT_TRUE(b.ok());

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound_a.load() && !unwound_b.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  std::this_thread::sleep_for(50ms);  // settle: the winner must NOT unwind
  EXPECT_NE(unwound_a.load(), unwound_b.load())
      << "F5: exactly one wave() awaiter is refused";
#endif
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok());
}

// The wave() twin of aligned_stale_scope_arm: aux-branch arm failure
// early-returns before `aorta_readiness_set_set_notify` ran.
flow::Task<void> wave_stale_scope_arm(flow::LaneRuntime& rt,
                                      flow::ReadinessSet& set,
                                      std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::detail::wave_loop(set.native_handle(), &rt,
                                           /*window_ns=*/0,
                                           /*scope_handle=*/0xdead'beef);
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    co_return;
  }
#else
  co_return;
#endif
}

TEST_F(ReadinessHarness, AuxArmFailureDoesNotClobberParkedWaveHook) {
  // Regression — wave_once copy of the
  // aligned_once bug: the failed-aux early return never owned the set's
  // notify hook and must not clear the parked awaiter's hook on resume.
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<uint64_t> got_wave{0};
  std::atomic<bool> done{false};
  auto a = flow::spawn_on(*rt_, scope,
                          await_one_wave(*rt_, *set, scope, got_wave, done));
  ASSERT_TRUE(a.ok());
  std::this_thread::sleep_for(100ms);  // A parks; its hook is installed

  std::atomic<bool> refused{false};
  auto b = flow::spawn_on(*rt_, scope,
                          wave_stale_scope_arm(*rt_, *set, refused));
  ASSERT_TRUE(b.ok());
#if defined(__cpp_exceptions)
  const auto refuse_deadline = std::chrono::steady_clock::now() + 5s;
  while (!refused.load() &&
         std::chrono::steady_clock::now() < refuse_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(refused.load())
      << "the aux-arm failure resolved the second awaiter promptly";
#endif

  ASSERT_EQ(set->satisfy(0), AORTA_OK);
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(done.load())
      << "the parked awaiter's hook survived the aux-arm failure";
  if (done.load()) {
    EXPECT_EQ(got_wave.load(), 1u);
  }
  // Stop first: on a regression the victim is parked forever and a bare
  // scope.close() would hang; the stop sweep resolves it either way.
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(ReadinessHarness, WaveResumesOnCompletion) {
  auto set = flow::ReadinessSet::create(2);
  ASSERT_TRUE(set.ok());

  flow::Scope scope = flow::Scope::root();
  std::atomic<uint64_t> got_wave{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          await_one_wave(*rt_, *set, scope, got_wave, done));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked on wave()

  ASSERT_EQ(set->satisfy(0), AORTA_OK);
  std::this_thread::sleep_for(20ms);
  EXPECT_FALSE(done.load()) << "half a wave must not resume";
  ASSERT_EQ(set->satisfy(1), AORTA_OK);

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load()) << "completed wave resumes the frame";
  EXPECT_EQ(got_wave.load(), 1u);
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(ReadinessHarness, WaveBaselineIgnoresPastWaves) {
  auto set = flow::ReadinessSet::create(1);
  ASSERT_TRUE(set.ok());
  ASSERT_EQ(set->satisfy(0), AORTA_OK);  // wave 1 fires BEFORE the await
  ASSERT_EQ(set->waves(), 1u);

  flow::Scope scope = flow::Scope::root();
  std::atomic<uint64_t> got_wave{0};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          await_one_wave(*rt_, *set, scope, got_wave, done));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);
  EXPECT_FALSE(done.load()) << "a wave that fired before the call is history";

  ASSERT_EQ(set->satisfy(0), AORTA_OK);  // wave 2
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load());
  EXPECT_EQ(got_wave.load(), 2u);
  ASSERT_TRUE(scope.close().ok());
}

}  // namespace
