// Flow co_await flow::recv — value flow, lost-wakeup
// protocol, Stopped on executor stop.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "aorta/flow_subscriber.hpp"
#include "aorta/node.h"
#include "aorta/publisher.h"

namespace flow = aorta::flow;
using namespace std::chrono_literals;

namespace {

class RecvHarness : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_SINGLE, &exec_), AORTA_OK);
    rt_ = flow::LaneRuntime::SingleThread();
    attachment_ = std::make_unique<flow::FacadeAttachment>(*rt_, exec_);
    ASSERT_TRUE(attachment_->attached());
    spin_ = std::thread([this] { aorta_executor_spin(exec_); });

    auto node = aorta::Node::Create("flow_next_test");
    ASSERT_TRUE(node.ok()) << node.status().message();
    node_ = std::move(node).value();
  }

  void TearDown() override {
    (void)aorta_executor_stop(exec_);
    if (spin_.joinable()) spin_.join();
    attachment_.reset();
    ASSERT_EQ(aorta_executor_free(exec_), AORTA_OK);
    node_.reset();
  }

  uint64_t exec_ = 0;
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::unique_ptr<flow::FacadeAttachment> attachment_;
  std::thread spin_;
  std::shared_ptr<aorta::Node> node_;
};

flow::Task<void> consume_one(flow::LaneRuntime& rt, flow::Scope scope,
                             aorta::PullSubscriber& sub,
                             std::atomic<int>& got_bytes,
                             std::atomic<bool>& done) {
  co_await flow::resume_on_lane{&rt};
  aorta::PullSample s = co_await flow::recv(sub, rt, scope);
  got_bytes.store(static_cast<int>(s.Size()), std::memory_order_seq_cst);
  done.store(true, std::memory_order_seq_cst);
}

flow::Task<void> consume_forever(flow::LaneRuntime& rt, flow::Scope scope,
                                 aorta::PullSubscriber& sub,
                                 std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    for (;;) {
      (void)co_await flow::recv(sub, rt, scope);
    }
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    throw;
  }
#else
  co_return;
#endif
}

TEST_F(RecvHarness, RecvDeliversAParkedSample) {
  auto sub = node_->CreatePullSubscriber("test/flow_recv", 4);
  ASSERT_TRUE(sub.ok()) << sub.status().message();
  auto pub = node_->CreatePublisher("test/flow_recv");
  ASSERT_TRUE(pub.ok()) << pub.status().message();
  std::this_thread::sleep_for(300ms);

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> got_bytes{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          consume_one(*rt_, scope, sub.value(), got_bytes, done));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // frame parks on recv()

  const std::string payload = "hello-recv";
  ASSERT_TRUE(pub.value()->PublishBytes(reinterpret_cast<const uint8_t*>(payload.data()), payload.size()).ok());

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load()) << "parked recv() woke on publish";
  EXPECT_EQ(got_bytes.load(), static_cast<int>(payload.size()));
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(RecvHarness, FastPathSampleAlreadyQueued) {
  auto sub = node_->CreatePullSubscriber("test/flow_recv_fast", 4);
  ASSERT_TRUE(sub.ok());
  auto pub = node_->CreatePublisher("test/flow_recv_fast");
  ASSERT_TRUE(pub.ok());
  std::this_thread::sleep_for(300ms);
  const std::string payload = "pre";
  ASSERT_TRUE(pub.value()->PublishBytes(reinterpret_cast<const uint8_t*>(payload.data()), payload.size()).ok());
  std::this_thread::sleep_for(200ms);  // sample lands BEFORE the await

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> got_bytes{-1};
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(*rt_, scope,
                          consume_one(*rt_, scope, sub.value(), got_bytes, done));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load()) << "fast path returns without a notify";
  EXPECT_EQ(got_bytes.load(), 3);
  ASSERT_TRUE(scope.close().ok());
}

// AMBIENT form, no rt/scope args — {rt, scope} come from the frame's
// promise context (stamped at spawn_on). Mirrors consume_one but through
// `flow::recv(sub)` instead of the explicit `flow::recv(sub, rt, scope)`.
flow::Task<void> consume_ambient(aorta::PullSubscriber& sub,
                                 std::atomic<int>& got_bytes,
                                 std::atomic<bool>& done,
                                 std::atomic<bool>& unwound) {
#if defined(__cpp_exceptions)
  try {
    aorta::PullSample s = co_await flow::recv(sub);
    got_bytes.store(static_cast<int>(s.Size()), std::memory_order_seq_cst);
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    done.store(true, std::memory_order_seq_cst);
    throw;
  }
  done.store(true, std::memory_order_seq_cst);
#else
  co_return;
#endif
}

// Gate 2.2 regression pin: a QUEUED sample sits on the subscriber (like
// FastPathSampleAlreadyQueued above), but the spawning scope is ALREADY
// cancelled before the ambient `flow::recv(sub)` ever runs. Before the
// fix, the ambient wrapper delegated straight to the explicit form,
// whose `await_ready()` unconditionally pops a queued sample (the F1
// fast path) with no cancellation check at all — so a pre-cancelled
// frame would silently consume and return the sample instead of
// unwinding cancelled. The fix gates on `ctx.scope.is_cancelled()`
// BEFORE delegating, so the sample is left untouched for a future
// consumer.
TEST_F(RecvHarness, PreCancelledAmbientRecvDoesNotConsumeAQueuedSample) {
  auto sub = node_->CreatePullSubscriber("test/flow_recv_precancel_ambient", 4);
  ASSERT_TRUE(sub.ok()) << sub.status().message();
  auto pub = node_->CreatePublisher("test/flow_recv_precancel_ambient");
  ASSERT_TRUE(pub.ok()) << pub.status().message();
  std::this_thread::sleep_for(300ms);
  const std::string payload = "queued";
  ASSERT_TRUE(pub.value()
                  ->PublishBytes(reinterpret_cast<const uint8_t*>(payload.data()),
                                 payload.size())
                  .ok());
  std::this_thread::sleep_for(200ms);  // sample lands BEFORE the await

  flow::Scope scope = flow::Scope::root();
  scope.cancel(flow::CancelReason::kShutdown);  // cancelled BEFORE spawn

  std::atomic<int> got_bytes{-1};
  std::atomic<bool> done{false};
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(
      *rt_, scope, consume_ambient(sub.value(), got_bytes, done, unwound));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done.load()) << "a pre-cancelled ambient recv must resolve, "
                              "not hang";
  EXPECT_TRUE(unwound.load())
      << "gate 2.2: a pre-cancelled ambient flow::recv(sub) must gate "
         "BEFORE delegating to the explicit form's unconditional "
         "await_ready fast path";
  EXPECT_EQ(got_bytes.load(), -1)
      << "the queued sample must NOT have been consumed by the "
         "pre-cancelled frame";
  ASSERT_TRUE(scope.close().ok());

  // The sample must still be queued: a fresh explicit recv() on a LIVE
  // scope sees it.
  flow::Scope live_scope = flow::Scope::root();
  std::atomic<int> got_bytes2{-1};
  std::atomic<bool> done2{false};
  auto s2 = flow::spawn_on(
      *rt_, live_scope,
      consume_one(*rt_, live_scope, sub.value(), got_bytes2, done2));
  ASSERT_TRUE(s2.ok());
  deadline = std::chrono::steady_clock::now() + 5s;
  while (!done2.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(done2.load())
      << "the sample must still be queued for a fresh consumer";
  EXPECT_EQ(got_bytes2.load(), static_cast<int>(payload.size()));
  ASSERT_TRUE(live_scope.close().ok());
}

flow::Task<void> consume_in_scope(flow::LaneRuntime& rt,
                                  aorta::PullSubscriber& sub,
                                  flow::Scope watch_scope,
                                  std::atomic<bool>& unwound,
                                  std::atomic<int>& got_bytes) {
#if defined(__cpp_exceptions)
  try {
    for (;;) {
      aorta::PullSample s = co_await flow::recv(sub, rt, watch_scope);
      got_bytes.store(static_cast<int>(s.Size()), std::memory_order_seq_cst);
    }
  } catch (const flow::cancelled_unwind&) {
    unwound.store(true, std::memory_order_seq_cst);
    throw;
  }
#else
  co_return;
#endif
}

TEST_F(RecvHarness, SecondRecvAwaiterIsRefused) {
  // The F5 single-awaiter contract for recv() — documented
  // in flow_subscriber.hpp, previously untested. Order-free: whichever awaiter
  // arms second self-resolves Stopped and unwinds; the winner parks on.
  auto sub = node_->CreatePullSubscriber("test/flow_recv_second", 4);
  ASSERT_TRUE(sub.ok());
  std::this_thread::sleep_for(100ms);

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound_a{false};
  std::atomic<bool> unwound_b{false};
  auto a = flow::spawn_on(*rt_, scope,
                          consume_forever(*rt_, scope, sub.value(), unwound_a));
  ASSERT_TRUE(a.ok());
  auto b = flow::spawn_on(*rt_, scope,
                          consume_forever(*rt_, scope, sub.value(), unwound_b));
  ASSERT_TRUE(b.ok());

  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound_a.load() && !unwound_b.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  std::this_thread::sleep_for(50ms);  // settle: the winner must NOT unwind
  EXPECT_NE(unwound_a.load(), unwound_b.load())
      << "F5: exactly one recv() awaiter is refused";
#endif
  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(RecvHarness, ScopeCancelResolvesAParkedRecv) {
  // recv_in parity: cancelling the GIVEN scope —
  // not the whole executor — must resolve a parked recv(sub, rt, scope)
  // promptly and unwind the frame.
  auto sub = node_->CreatePullSubscriber("test/flow_recv_scope_cancel", 4);
  ASSERT_TRUE(sub.ok());
  std::this_thread::sleep_for(100ms);

  flow::Scope scope = flow::Scope::root();
  flow::Scope watch = flow::Scope::root({.name = "recv-watch"});
  std::atomic<bool> unwound{false};
  std::atomic<int> got_bytes{0};
  auto s = flow::spawn_on(
      *rt_, scope, consume_in_scope(*rt_, sub.value(), watch, unwound,
                                    got_bytes));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked, nothing published

  watch.cancel(flow::CancelReason::kExplicit);
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load(std::memory_order_seq_cst) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
#if defined(__cpp_exceptions)
  EXPECT_TRUE(unwound.load()) << "scope cancel resolved the parked recv()";
#endif
  ASSERT_TRUE(scope.close().ok());
  (void)watch.close();
}

TEST_F(RecvHarness, ScopedRecvStillDeliversData) {
  // The scope-taking overload's happy path: a live scope changes
  // nothing — samples deliver exactly like plain recv().
  auto sub = node_->CreatePullSubscriber("test/flow_recv_scope_data", 4);
  ASSERT_TRUE(sub.ok());
  auto pub = node_->CreatePublisher("test/flow_recv_scope_data");
  ASSERT_TRUE(pub.ok());
  std::this_thread::sleep_for(200ms);

  flow::Scope scope = flow::Scope::root();
  flow::Scope watch = flow::Scope::root({.name = "recv-live"});
  std::atomic<bool> unwound{false};
  std::atomic<int> got_bytes{0};
  auto s = flow::spawn_on(
      *rt_, scope, consume_in_scope(*rt_, sub.value(), watch, unwound,
                                    got_bytes));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked

  const std::vector<uint8_t> payload{1, 2, 3};
  ASSERT_TRUE(pub.value()->PublishBytes(payload).ok());
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (got_bytes.load(std::memory_order_seq_cst) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(got_bytes.load(), 3) << "scoped recv() delivered the sample";
  EXPECT_FALSE(unwound.load()) << "a live scope never unwinds the consumer";
  watch.cancel(flow::CancelReason::kExplicit);  // release the parked loop
  ASSERT_TRUE(scope.close().ok());
  (void)watch.close();
}

TEST_F(RecvHarness, StopResolvesAParkedRecvAsStopped) {
  auto sub = node_->CreatePullSubscriber("test/flow_recv_stop", 4);
  ASSERT_TRUE(sub.ok());
  std::this_thread::sleep_for(100ms);

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto s = flow::spawn_on(*rt_, scope,
                          consume_forever(*rt_, scope, sub.value(), unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(100ms);  // parked, nothing published

  ASSERT_EQ(aorta_executor_stop(exec_), AORTA_OK);
  spin_.join();
  ASSERT_TRUE(scope.close().ok()) << "sweep resolved the parked recv()";
#if defined(__cpp_exceptions)
  EXPECT_TRUE(unwound.load()) << "frame unwound via cancelled_unwind";
#endif
}

}  // namespace

// ---- Regression pins ---------------------------------------------------------

class MtRecvHarness : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(aorta_executor_new(AORTA_EXECUTOR_MULTI, &exec_), AORTA_OK);
    rt_ = flow::LaneRuntime::MultiThread({0, 0, 3, 0});
    attachment_ = std::make_unique<flow::FacadeAttachment>(*rt_, exec_);
    ASSERT_TRUE(attachment_->attached());
    spin_ = std::thread([this] { aorta_executor_spin(exec_); });

    auto node = aorta::Node::Create("flow_next_mt_test");
    ASSERT_TRUE(node.ok()) << node.status().message();
    node_ = std::move(node).value();
  }

  void TearDown() override {
    (void)aorta_executor_stop(exec_);
    if (spin_.joinable()) spin_.join();
    attachment_.reset();
    ASSERT_EQ(aorta_executor_free(exec_), AORTA_OK);
    node_.reset();
  }

  uint64_t exec_ = 0;
  std::unique_ptr<flow::LaneRuntime> rt_;
  std::unique_ptr<flow::FacadeAttachment> attachment_;
  std::thread spin_;
  std::shared_ptr<aorta::Node> node_;
};

flow::Task<void> consume_n(flow::LaneRuntime& rt, flow::Scope scope,
                           aorta::PullSubscriber& sub, int n,
                           std::atomic<int>& got, std::atomic<bool>& done) {
  for (int i = 0; i < n; i++) {
    (void)co_await flow::recv(sub, rt, scope);
    got.fetch_add(1, std::memory_order_seq_cst);
  }
  done.store(true, std::memory_order_seq_cst);
}

TEST_F(MtRecvHarness, NotifyRacesArmHammerOnWorkers) {
  // Pin: MT lane workers resume the frame
  // while await_suspend's post-publish steps run — hammer the window
  // with a fast publisher. Any frame-touch-after-publish is a TSan/ASan
  // hit and a lost sample is a hang here.
  auto sub = node_->CreatePullSubscriber("test/recv_mt_hammer", 8);
  ASSERT_TRUE(sub.ok());
  auto pub = node_->CreatePublisher("test/recv_mt_hammer");
  ASSERT_TRUE(pub.ok());
  std::this_thread::sleep_for(300ms);

  flow::Scope scope = flow::Scope::root();
  std::atomic<int> got{0};
  std::atomic<bool> done{false};
  constexpr int kRounds = 200;
  auto s = flow::spawn_on(*rt_, scope,
                          consume_n(*rt_, scope, sub.value(), kRounds, got, done));
  ASSERT_TRUE(s.ok());

  const std::string payload = "x";
  for (int i = 0; i < kRounds; i++) {
    // Publish slightly faster than the consumer re-arms sometimes, and
    // slower other times — both edges of the race get hit across 200
    // rounds (the ring absorbs bursts).
    ASSERT_TRUE(pub.value()
                    ->PublishBytes(
                        reinterpret_cast<const uint8_t*>(payload.data()),
                        payload.size())
                    .ok());
    if ((i & 3) == 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
  }
  auto deadline = std::chrono::steady_clock::now() + 20s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    // Keep topping up so a burst drop (ring depth 8) can't starve the
    // count — the pin is frame-safety + no-strand, not losslessness.
    ASSERT_TRUE(pub.value()
                    ->PublishBytes(
                        reinterpret_cast<const uint8_t*>(payload.data()),
                        payload.size())
                    .ok());
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(done.load()) << "no strand, no lost wakeup: " << got.load();
  ASSERT_TRUE(scope.close().ok());
}

TEST_F(RecvHarness, SubscriberDropWakesAParkedRecv) {
  // Pin (C++ side): freeing the pull subscriber while a frame
  // parks on recv() must resolve it (WakeOnDrop fires the hook).
  auto sub = node_->CreatePullSubscriber("test/recv_drop_wake", 4);
  ASSERT_TRUE(sub.ok());
  std::this_thread::sleep_for(100ms);

  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> unwound{false};
  auto held = std::make_shared<aorta::PullSubscriber>(std::move(sub).value());
  auto s = flow::spawn_on(*rt_, scope, consume_forever(*rt_, scope, *held, unwound));
  ASSERT_TRUE(s.ok());
  std::this_thread::sleep_for(150ms);  // parked

  held.reset();  // undeclare → WakeOnDrop → hook fires → branch resolves
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!unwound.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(unwound.load()) << "parked recv() resolved on subscriber drop";
  ASSERT_TRUE(scope.close().ok());
}

flow::Task<void> recv_with_deadline(flow::LaneRuntime& rt, flow::Scope scope,
                                    aorta::PullSubscriber& sub,
                                    std::atomic<bool>& done) {
#if defined(__cpp_exceptions)
  try {
    (void)co_await flow::recv_for(sub, rt, std::chrono::milliseconds(80),
                                  scope);
  } catch (const flow::cancelled_unwind&) {
    done.store(true, std::memory_order_seq_cst);
    co_return;
  }
#endif
  co_return;
}

TEST_F(RecvHarness, RecvForCompositionDeadlineFires) {
  // recv_for = timeout ∘ recv — nothing published, the
  // 80ms window unwinds the wait instead of parking forever.
  auto sub = node_->CreatePullSubscriber("test/recv_for", 4);
  ASSERT_TRUE(sub.ok());
  std::this_thread::sleep_for(100ms);
  flow::Scope scope = flow::Scope::root();
  std::atomic<bool> done{false};
  auto s = flow::spawn_on(
      *rt_, scope, recv_with_deadline(*rt_, scope, sub.value(), done));
  ASSERT_TRUE(s.ok());
  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(done.load()) << "deadline resolved the parked recv()";
  ASSERT_TRUE(scope.close().ok());
}
